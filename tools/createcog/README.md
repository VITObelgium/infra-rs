# createcog

Create a Cloud Optimized GeoTIFF (COG) from a GDAL-readable raster or selected
NetCDF data. Existing raster/TIFF inputs need no NetCDF selection flags:

```sh
createcog --input input.tif --output output.tif
```

## Conda package

The `sas-raster-tools` conda package contains `createcog`. From the repository
root, build and test it with Pixi in a normal shell outside devenv; the build
obtains its toolchain and native dependencies from conda-forge:

```sh
pixi run --locked -e packaging conda-build
pixi run --locked -e packaging conda-test
```

Packages are written to `conda/output/` relative to the repository root. Tagged
builds publish to [VITO-RMA/conda-channel](https://github.com/VITO-RMA/conda-channel),
not GitHub Release assets. After the first publication, install with:

```sh
conda install -c https://raw.githubusercontent.com/VITO-RMA/conda-channel/main \
  -c conda-forge sas-raster-tools
```

See the [conda packaging README](../../conda/README.md) for platform requirements,
channel publishing credentials, and local channel updates.

## Output type and scale/offset decoding

Use `--output-data-type TYPE` to choose the output sample
type. Names are case-insensitive: `Int8`, `UInt8` (also `Byte`), `Int16`, `UInt16`,
`Int32`, `UInt32`, `Int64`, `UInt64`, `Float32`, and `Float64`.
Rust-style aliases are also accepted: `i8`, `u8`, `i16`, `u16`, `i32`, `u32`,
`i64`, `u64`, `f32`, and `f64`.

```sh
createcog --input input.tif --output float.tif --output-data-type f32
```

**`--output-data-type` alone only casts samples.** It retains the original scale/offset
metadata and does not decode packed values. Without `--output-data-type`, `--unscale`,
or `--scale`, the existing source-type behavior is unchanged.

Use `--unscale` to apply each selected source band's scale and offset to its
pixels:

```text
physical = raw * scale + offset
```

The decoded input has no effective scaling metadata (scale 1, offset 0), so
consumers must not apply the original coefficients again. Output defaults to
`Float64` only when `--unscale` is used without `--scale` or an explicit
`--output-data-type`. An explicit `--output-data-type` selects the output type; `--scale`
selects its encoded unsigned output type.

```sh
createcog --input packed.tif --output physical.tif --unscale
createcog --input packed.tif --output physical-f32.tif --unscale --output-data-type Float32
createcog --input packed.nc --output physical.tif \
  --variable WBGT --dimensions time,y,x --unscale
```

Decoding happens after NetCDF selection, and every selected source band must
have effective scaling. If **any** selected band has scale 1 and offset 0
(including absent coefficients, which default to 1 and 0), `--unscale` fails.
Non-finite scale or offset coefficients (`NaN` or infinity) are also rejected.

Without `--scale`, an explicit integer output type can lose fractional values
and clamp values outside its representable range according to GDAL conversion rules. Choose a
floating-point output type when those losses are unacceptable; `Float32` can
also lose precision compared with the default `Float64`.

`--unscale` is an **input** operation; `--scale 8` and `--scale 16` are **output**
operations and retain their existing output-rescaling behavior. They may be
combined: input scale/offset decoding runs first, then the existing output
scaling repacks the physical values and writes new scale/offset metadata for
that encoded output (not the original input coefficients).

```sh
# Decode packed input, then repack it as UInt16 with new output scaling metadata:
createcog --input packed.tif --output repacked.tif --unscale --scale 16
```

`--scale 8` selects `UInt8` output; `--scale 16` selects `UInt16`. An explicit
`--output-data-type` may accompany `--scale` only if it matches:
`--scale 8 --output-data-type UInt8` (or `Byte`) and
`--scale 16 --output-data-type UInt16` are valid, with or without `--unscale`. Mismatched
types are rejected during option validation with an error naming the required
type. `--unscale` and `--output-data-type` may also be combined without output scaling.

## NetCDF selection

Select a variable and name its dimensions in output axis-role order, not source storage order:

```sh
createcog --input uc.nc --output wbgt.tif \
  --variable WBGT --dimensions time,y,x
```

`--variable` accepts one or more comma-delimited names and can be repeated. Within
each input file, output bands are variable-major: all slices of the first selected
variable, then all slices of the next variable. Within each variable, time/band
indices follow stored order; time coordinates are not decoded or sorted
chronologically.

```sh
createcog --input uc.nc --output weather.tif \
  --variable WBGT --variable TEMP --dimensions time,y,x

# Equivalent variable selection:
createcog --input uc.nc --output weather.tif \
  --variable WBGT,TEMP --dimensions time,y,x
```

Without `--variable`, the sole matching numeric data variable is selected
automatically. If selection is ambiguous or no variable matches, the error lists
matching candidates and provides `--variable` guidance.

`--dimensions` takes exactly one comma-delimited argument (default: `time,y,x`).
Specify exactly two names for a 2D array (`y,x`), or three names for a 3D array
(`band/time,y,x`). Each position assigns an output axis role:

- The last name identifies the horizontal dimension (x / columns).
- The second-to-last name identifies the vertical dimension (y / rows).
- For a 3D array, the first name identifies the dimension whose slices become
  output bands, in stored index order.

These are actual dimension names from the dataset, not coordinate values or
slice indices. The names do not need to be literally `time`, `y`, or `x`; their
positions assign their roles. For example:

```sh
createcog --input forecast.nc --output forecast.tif \
  --variable WBGT --dimensions forecast,latitude,longitude

createcog --input surface.nc --output surface.tif \
  --variable elevation --dimensions latitude,longitude
```

The source array can store its dimensions in any order; `createcog` automatically
transposes it into the requested output order. For example, a variable stored as
`[time, x, y]` should still use `--dimensions time,y,x` when `x` is horizontal and
`y` is vertical.

Specifying `--dimensions time,x,y` does **not** describe that source storage order.
It is accepted, but assigns `x` to rows and `y` to columns, swapping their spatial
roles. Axis roles are not inferred from dimension names.

### Combining files with a wildcard

Quote the input pattern so the shell passes it to `createcog` unchanged:

```sh
createcog --input 'weather/*.nc' --output weather.tif \
  --variable WBGT --dimensions time,y,x
```

Matching filenames are sorted lexically (alphabetically), not by creation time or
their time coordinates. Output is **file-major, then variable-major, then stored
slice order**: all selected variables and their time slices from the first file,
then all selected variables and slices from the next file. For example, selecting
`WBGT,TEMP` from `a.nc` and `b.nc`, each with two time slices, produces:

```text
a.nc: WBGT[0], WBGT[1], TEMP[0], TEMP[1]
b.nc: WBGT[0], WBGT[1], TEMP[0], TEMP[1]
```

Every matching file must contain every requested variable; a missing variable in
any file or a pattern matching no files is an error, and no output is created.
Selected variables across all files must share a compatible grid, CRS, sample
type, and nodata value (see below). Files are appended as bands, not mosaicked or
sorted by decoded dates.

All selected NetCDF band views are combined into one temporary VRT. Multiband
processing is detected from this prepared input, so `--multi-band` is unnecessary
even when each individual file contributes only one band. The explicit
`--multi-band` flag remains supported. Existing TIFF wildcard inputs also remain
supported, for example `--input 'rasters/*.tif' --multi-band`.

### Supported data and value handling

- Local NetCDF files with 2D/3D numeric arrays on regular rectilinear grids are
  supported. Spatial coordinates must be one-dimensional cell centers with at
  least two values per axis. Curvilinear, irregular, and higher-dimensional grids
  are not supported. Coordinate `scale_factor` / `add_offset` are decoded into CRS
  units before computing the grid.
- All selected variables, including selections across multiple files, must share
  a grid (spatial dimensions, extent, and resolution), CRS, sample type, and nodata
  value.
  Variables/files are not automatically promoted to a common sample type.
- NetCDF preparation uses temporary VRT slices, recovering the geotransform and
  CRS from the source. If projection metadata is missing, assign it explicitly
  with `--source-srs EPSG:4326` (or the correct source CRS).
- NetCDF preparation retains raw sample values and scale/offset metadata. Add
  `--unscale` to decode packed values using `scale_factor` / `add_offset` after
  selection. `--output-data-type` alone casts values without decoding that metadata.
  These options are separate from `--scale 8` / `--scale 16` output rescaling.
- Multiple prepared bands automatically use the multiband COG pipeline, whether
  they come from variables, time slices, or multiple files; adding `--multi-band`
  is not required, but remains supported.
- Temporary VRTs remain available during command generation or COG creation and
  are cleaned up afterward. `--gdal-cmd` is diagnostic: any temporary VRT paths it
  prints are not persistent inputs for a later command.

Use `createcog --help` for zoom, tile-size, scaling, and other output options.
