# Implementation plan

## Conclusion

Implement a two-phase pipeline in `geo`:

```text
input band manifest
    -> one warped, overview-complete COG per band
    -> Rust assembler copies compressed tiles into one TILE-interleaved COG
    -> internal validation
    -> atomic output replacement
```

GDAL must never receive the complete multiband dataset during output creation. The final merge must not decode, recompress, or rewrite raster values. The existing `tiff` encoder and the abandoned `origin/tiffwrite` branch cannot produce precompressed, IFDs-before-data COGs, so a focused COG assembler is required.

## Implementation

### 1. Normalize both input forms

Add a logical band manifest containing `(path, one-based band index, datatype, nodata, description, scale, offset)`.

- A single input file contributes all its bands.
- A glob contributes every band from every matched file, ordered by sorted path and then band index.
- Exclude the output path from glob matches.
- Reject an empty manifest.
- Preserve `--multi-band` as the explicit glob mode for compatibility.
- Use the optimized pipeline whenever the manifest contains more than one band.

### 2. Calculate one canonical target grid

Determine the common output datatype and nodata from the first logical band or explicit `CogCreationOptions`. Validate that every logical band can be represented by that output contract.

Calculate the EPSG:3857 union extent of all inputs and apply the existing zoom strategy. Create the first band as a warped VRT covering that union, then translate it to a one-band COG using the current tiling scheme, zoom, tile size, overview, compression, predictor, sparse, and alignment options.

Read the resulting grid and overview dimensions back and use them as the canonical layout for every subsequent band. This delegates exact Google Maps and `ALIGNED_LEVELS` behavior to GDAL instead of duplicating it in Rust.

### 3. Create one temporary COG per band

For each manifest entry:

- Select exactly one source band with `-srcband`.
- Create a one-band warped VRT fixed to the canonical extent, dimensions, projection, and nodata.
- Translate the VRT to a one-band COG with identical block size, datatype, compression, predictor, and overview count.
- Apply `--scale` during this per-band translation so there is no final whole-file scaling pass.
- Preserve band description, scale, offset, and statistics for final metadata assembly.

Store all staging files in an RAII temporary directory beside the destination. This guarantees cleanup and keeps the final rename on the same filesystem. Move `tempfile` from a development-only dependency to a normal dependency of the owning crate.

### 4. Add a raw TIFF index

Add a low-level parser under `crates/geo/src/geotiff/` that records, for every IFD:

- TIFF kind: Classic TIFF or BigTIFF.
- Byte order.
- IFD identity and `NewSubfileType`.
- Image and tile dimensions.
- Bits per sample, sample format, photometric interpretation, and planar configuration.
- Compression, predictor, and required codec-specific tags.
- Raw georeferencing and GDAL metadata tags.
- Tile offsets and byte counts.

Do not rely only on the normalized `GeoTiffMetadata` model because it currently collapses properties across IFDs and does not preserve every tag required for writing.

Validate that staged inputs are single-band, tiled, mask-free, and compatible at every level. Reject mismatched grids, tile sizes, overview dimensions, datatypes, compression, predictors, nodata, byte order, palettes, alpha bands, masks, unrelated IFDs, and unsupported codec-specific tags with a precise error.

### 5. Implement the constrained COG assembler

Add a serializer dedicated to staging COGs produced by this pipeline. It must:

- Precompute the complete output layout using checked arithmetic.
- Select Classic TIFF or BigTIFF from the exact compressed payload and header sizes.
- Write the TIFF header and GDAL ghost area with `INTERLEAVE=TILE`.
- Write the main IFD followed by largest-to-smallest overview IFDs.
- Mark reduced-resolution IFDs with `NewSubfileType=1`.
- Place all `TileOffsets` and `TileByteCounts` arrays before imagery.
- Store the logical tile arrays in TIFF band-major order.
- Stream physical payloads in smallest-overview-to-full-resolution order, then row-major tile order, then ascending band order.
- Preserve sparse blocks as zero offset/count entries.
- Regenerate each four-byte COG block leader and repeated-last-four-bytes trailer.
- Copy compressed payloads through a fixed-size buffer without decoding them.
- Emit `PlanarConfiguration=Separate` and repeated sample fields for all bands.
- Copy the controlled reference COG's geospatial tags.
- Rebuild GDAL XML containing tiling-scheme metadata, `INTERLEAVE=TILE`, and per-band descriptions, scale, offset, and statistics.

The logical tile-array index remains `band * tiles_per_level + tile`, while physical payload order is `tile -> band`. These two orders must not be conflated.

### 6. Validate before publishing

Add a native structural validator that verifies:

- Expected band count and overview count.
- Tiled, planar-separate storage.
- Agreement between TIFF structure, GDAL XML, and ghost-area `INTERLEAVE=TILE` declarations.
- Main and overview IFD ordering.
- Tile index arrays appearing before imagery.
- Smallest-overview-to-full-resolution data ordering.
- Row-major tile and tile-major band ordering.
- Valid sparse offset/count pairs.
- Monotonic, non-overlapping payload ranges.
- Correct block leaders and trailers.

Write to a sibling temporary output, reopen it with both GDAL and `GeoTiffMetadata`, run the validator, and only then replace the destination. Preserve any existing destination if staging, assembly, or validation fails.

### 7. Integrate GDAL 3.13 validation

Add an explicit `nixpkgs-unstable` input to `devenv.yaml` and update `devenv.lock`. Add the Nixpkgs GDAL 3.13 package, including its Python bindings, to the development environment so the official validator is available.

Keep this validator package separate from the static GDAL package used to compile and link the Rust workspace. Expose it through a wrapper or an absolute `GDAL_COG_VALIDATOR` path to avoid `PATH`, `GDAL_DATA`, `PROJ_LIB`, or dynamic-library conflicts.

The Nix integration test must run:

```bash
gdal driver cog validate --full-check=yes output.tif
```

The packaged `createcog` binary remains self-contained by using native validation at runtime. The official GDAL validator is the mandatory integration and CI oracle.

### 8. Update CLI behavior and progress

Update `tools/createcog/src/createtiles.rs` to resolve the band manifest and dispatch automatically.

- Retain existing single-band behavior for a one-band manifest.
- Support both a single multiband file and globbed datasets.
- Aggregate progress across per-band preparation, byte-based assembly, and validation.
- Update `--gdal-cmd` to report the staged pipeline instead of attempting to inspect a literal glob.
- Fix the unsupported `cog` positional argument in the `createmultibandscaledcog` recipe in `justfile`.
- Propagate glob traversal errors rather than silently discarding them.

## Primary files

| Area | Files |
| --- | --- |
| Orchestration | `crates/geo/src/cog/creation.rs`, new `crates/geo/src/cog/multiband_creation.rs` |
| Raw TIFF model | new `crates/geo/src/geotiff/raw.rs` |
| COG writer | new `crates/geo/src/geotiff/cog_assembler.rs` |
| Validation | new `crates/geo/src/geotiff/cog_validation.rs` |
| Metadata | `crates/geo/src/geotiff/decoder.rs`, `gdalmetadata.rs`, `metadata.rs` |
| Exports and dependencies | `crates/geo/src/cog.rs`, `geotiff.rs`, `crates/geo/Cargo.toml` |
| CLI | `tools/createcog/src/main.rs`, `createtiles.rs`, `justfile` |
| Validator environment | `devenv.yaml`, `devenv.lock`, `devenv.nix` |

## Testing

### Unit tests

- Manifest expansion for a single multiband file and a glob of single- and multiband files.
- Deterministic path-then-band ordering and output-path exclusion.
- Compatibility failures for every unsupported mismatch.
- Classic TIFF and BigTIFF layout planning, including overflow boundaries.
- Logical band-major tile arrays versus physical tile-major payload order.
- Sparse block handling.
- Block leader and trailer generation.
- Native validator failures for deliberately corrupted structures.

### Integration tests

- Generate compact, distinct one-band fixtures in a temporary directory.
- Create outputs from both supported input forms.
- Assert the output band count equals the manifest length.
- Assert `ChunkDataLayout::Tiled`, `Interleave::Tile`, EPSG:3857, datatype, nodata, compression, predictor, tile size, and overview dimensions.
- Compare every output band and overview with its corresponding staged band using both GDAL and `GeoTiffReader`.
- Hash every non-sparse compressed source payload and assert byte identity in the output.
- Verify physical ordering by sorting nonzero output offsets.
- Run `gdal driver cog validate --full-check=yes` in the Nix GDAL 3.13 environment.
- Cover `--scale`, absent nodata, partial edge tiles, sparse blocks, 256- and 512-pixel tiles, integer and floating-point data, no overviews, and multiple overviews.

### Performance regression

Add a named `slow_test_` case with at least 64 bands. Assert that it completes through the staged assembler and does not invoke a final multiband GDAL warp or translate. Record elapsed time and peak resident memory for diagnostics, but avoid a brittle wall-clock threshold in normal CI.

## Acceptance criteria

- Single multiband files and globbed datasets both produce deterministic band ordering.
- The output band count equals the manifest length.
- GDAL reports `INTERLEAVE=TILE`; TIFF reports planar-separate storage.
- Every output band and overview decodes identically to its corresponding staged band.
- Every non-sparse compressed payload is byte-identical to its source payload.
- `gdal driver cog validate --full-check=yes` succeeds.
- A 64-or-more-band regression case completes without a final multiband GDAL warp or translate.
- Merge memory is bounded by TIFF indexes plus one fixed copy buffer, not raster dimensions multiplied by band count.
- Classic TIFF overflow automatically selects BigTIFF before writing begins.
- Partial outputs and temporary directories are removed on failure.
- An existing destination is replaced only after all validation succeeds.

## Initial scope constraints

The first implementation assembles only the controlled, single-band staging COGs produced by this tool. It is not a general arbitrary-TIFF merger. Explicitly reject masks, alpha bands, palettes, mixed sample types, mixed nodata values, differing codecs or predictors, external overviews, and unknown IFD roles until each is deliberately supported and tested.
