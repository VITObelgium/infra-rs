use std::path::Path;

use crate::{gdalinterop, vector::io::FeatureDefinitionExtension, GeoReference};
use crate::{raster, Error, RasterNum, Result};
use gdal::{
    raster::GdalType,
    vector::{FieldValue, LayerAccess},
};

use super::{geometrytype::GeometryType, io, BurnValue};

/// Translate a GDAL vector dataset using the provided translate options
/// The options are passed as a list of strings in the form `["-option1", "value1", "-option2", "value2"]`
/// and match the options of the gdal ogr2ogr command line tool
/// The translated dataset is returned
pub fn translate_cli_opts(ds: &gdal::Dataset, options: &[String]) -> Result<gdal::Dataset> {
    let mem_ds = io::dataset::create_in_memory()?;
    let mut opts = VectorTranslateOptionsWrapper::new(options)?;

    let mut usage_error: std::ffi::c_int = 0;
    unsafe {
        gdal_sys::GDALVectorTranslate(
            std::ptr::null_mut(),
            mem_ds.c_dataset(),
            1,
            &mut ds.c_dataset(),
            opts.c_options(),
            &mut usage_error,
        );
    }

    if usage_error == gdalinterop::TRUE {
        return Err(Error::InvalidArgument("Vector translate: invalid arguments".to_string()));
    }

    Ok(mem_ds)
}

/// Translate a GDAL vector dataset to disk using the provided translate options
/// The options are passed as a list of strings in the form `["-option1", "value1", "-option2", "value2"]`
/// and match the options of the gdal ogr2ogr command line tool
/// The dataset is returned in case the user wants to continue working with it but can also be ignored
pub fn translate_ds_to_disk(ds: &gdal::Dataset, path: &Path, options: &[String]) -> Result<gdal::Dataset> {
    gdalinterop::create_output_directory_if_needed(path)?;
    let path_str = std::ffi::CString::new(path.to_string_lossy().as_ref())?;
    let mut opts = VectorTranslateOptionsWrapper::new(options)?;
    let mut usage_error: std::ffi::c_int = 0;

    let handle = unsafe {
        gdal_sys::GDALVectorTranslate(
            path_str.as_ptr(),
            std::ptr::null_mut(),
            1,
            &mut ds.c_dataset(),
            opts.c_options(),
            &mut usage_error,
        )
    };

    if usage_error == gdalinterop::TRUE {
        return Err(Error::InvalidArgument("Vector translate: invalid arguments".to_string()));
    }

    gdalinterop::check_pointer(handle, "GDALVectorTranslate")?;

    Ok(unsafe { gdal::Dataset::from_c_dataset(handle) })
}

#[derive(Debug, Default)]
pub struct RasterizeOptions<T: num::One> {
    /// the attribute field used to burn the values, or a fixed value
    pub burn_value: BurnValue<T>,
    /// If Some the raster will be initialized with this value for rasterization
    pub init_value: Option<T>,
    pub add: bool,
    pub all_touched: bool,
    pub meta: GeoReference,
    /// if none, the first layer will be used
    pub input_layer: Option<String>,
    pub target_aligned_pixels: bool,
    /// Additional cli options to pass to the rasterize command
    /// in the form `["-option1", "value1", "-option2", "value2"]`
    /// and match the options of the gdal `gdal_rasterize` command line tool
    /// Use when the provided options are not sufficient or new command line options are not supported yet
    pub cli_options: Vec<String>,
}

impl<T: num::One + ToString> From<RasterizeOptions<T>> for Vec<String> {
    fn from(options: RasterizeOptions<T>) -> Vec<String> {
        let mut options_vec = Vec::new();
        if let Some(input_layer) = options.input_layer {
            options_vec.push("-l".to_string());
            options_vec.push(input_layer);
        }

        if options.add {
            options_vec.push("-add".to_string());
        }

        if options.all_touched {
            options_vec.push("-at".to_string());
        }

        if options.target_aligned_pixels {
            options_vec.push("-tap".to_string());
        }

        match options.burn_value {
            BurnValue::Field(field_name) => {
                options_vec.push("-a".to_string());
                options_vec.push(field_name);
            }
            BurnValue::Value(value) => {
                options_vec.push("-burn".to_string());
                options_vec.push(value.to_string());
            }
        }

        if let Some(init_value) = options.init_value {
            options_vec.push("-init".to_string());
            options_vec.push(init_value.to_string());
        }

        options_vec.extend(options.cli_options);

        options_vec
    }
}

pub fn rasterize<T: RasterNum<T> + GdalType + ToString>(
    ds: &gdal::Dataset,
    meta: &GeoReference,
    options: RasterizeOptions<T>,
) -> Result<(GeoReference, Vec<T>)> {
    if options.add {
        if let Some(nodata_value) = options.meta.nodata() {
            if nodata_value.is_nan() {
                return Err(Error::InvalidArgument(
                    "Rasterize output nodata is nan, this is not compatible with the add algorithm".to_string(),
                ));
            }
        }
    }

    let cli_options: Vec<String> = options.into();
    rasterize_with_cli_options(ds, meta, &cli_options)
}

/// Rasterize a GDAL vector dataset using the provided rasterize options
/// The options are passed as a list of strings in the form `["-option1", "value1", "-option2", "value2"]`
/// and match the options of the gdal `gdal_rasterize` command line tool
/// The rasterized dataset is returned
pub fn rasterize_with_cli_options<T: RasterNum<T> + GdalType>(
    ds: &gdal::Dataset,
    meta: &GeoReference,
    options: &[String],
) -> Result<(GeoReference, Vec<T>)> {
    let gdal_options = RasterizeOptionsWrapper::new(options)?;

    let data = vec![meta.nodata_as::<T>()?.unwrap_or(T::zero()); meta.rows() * meta.columns()];
    let mut mem_ds = raster::io::dataset::create_in_memory_with_data::<T>(meta, &data)?;

    raster::io::dataset::metadata_to_dataset_band(&mut mem_ds, meta, 1)?;

    let mut usage_error: std::ffi::c_int = gdal_sys::CPLErr::CE_None as std::ffi::c_int;
    unsafe {
        gdal_sys::GDALRasterize(
            std::ptr::null_mut(),
            mem_ds.c_dataset(),
            ds.c_dataset(),
            gdal_options.c_options(),
            &mut usage_error,
        );
    }

    if usage_error == gdalinterop::TRUE {
        return Err(Error::InvalidArgument("Vector rasterize: invalid arguments".to_string()));
    }

    let meta = raster::io::dataset::read_band_metadata(&mem_ds, 1)?;
    Ok((meta, data))
}

/// Convenience function to rasterize a vector dataset to disk
/// Avoids creating an in-memory dataset that then needs to be written to disk
pub fn rasterize_to_disk_with_cli_options(ds: &gdal::Dataset, path: &Path, options: &[String]) -> Result<gdal::Dataset> {
    let gdal_options = RasterizeOptionsWrapper::new(options)?;
    let path_cstr = std::ffi::CString::new(path.to_string_lossy().to_string())?;

    let mut usage_error: std::ffi::c_int = 0;
    let handle = unsafe {
        gdal_sys::GDALRasterize(
            path_cstr.as_ptr(),
            std::ptr::null_mut(),
            ds.c_dataset(),
            gdal_options.c_options(),
            &mut usage_error,
        )
    };

    if usage_error == gdalinterop::TRUE {
        return Err(Error::InvalidArgument("Vector rasterize: invalid arguments".to_string()));
    }

    gdalinterop::check_pointer(handle, "GDALRasterize")?;

    Ok(unsafe { gdal::Dataset::from_c_dataset(handle) })
}

#[derive(Default, Debug)]
pub struct BufferOptions {
    pub distance: f64,
    pub num_quad_segments: u32,
    /// copy over the fields in the resulting dataset
    pub include_fields: bool,
    /// apply an attribute filter to the input layers;
    attribute_filter: Option<String>,
    /// override the type of the resulting geometry
    geometry_type: Option<GeometryType>,
}

pub fn buffer(ds: &gdal::Dataset, opts: &BufferOptions) -> Result<gdal::Dataset> {
    assert!(opts.distance > 0.0);

    let mut mem_ds = io::dataset::create_in_memory()?;

    for i in 0..ds.layer_count() {
        let mut src_layer = ds.layer(i)?;
        let spatial_ref = src_layer.spatial_ref();

        let mut layer_options = gdal::vector::LayerOptions {
            name: &src_layer.name(),
            srs: spatial_ref.as_ref(),
            ..Default::default()
        };

        if let Some(geometry_type) = opts.geometry_type {
            layer_options.ty = geometry_type.into();
        }

        let field_count = src_layer.defn().field_count()? as usize;
        let mut dst_layer = mem_ds.create_layer(layer_options)?;

        if opts.include_fields {
            // Take over the field definitions
            let mut names: Vec<String> = Vec::with_capacity(field_count);
            let mut types: Vec<gdal_sys::OGRFieldType::Type> = Vec::with_capacity(field_count);

            for field in src_layer.defn().fields() {
                names.push(field.name());
                types.push(field.field_type());
            }

            let definitions = names
                .iter()
                .zip(types.iter())
                .map(|(name, ty)| (name.as_ref(), *ty))
                .collect::<Vec<(&str, gdal_sys::OGRFieldType::Type)>>();

            dst_layer.create_defn_fields(&definitions)?;
        }

        if let Some(filter) = &opts.attribute_filter {
            src_layer.set_attribute_filter(filter)?;
        }

        for feature in src_layer.features() {
            if let Some(geom) = feature.geometry() {
                let geom = geom.buffer(opts.distance, opts.num_quad_segments)?;
                if opts.include_fields {
                    // Copy the geometry and the fields
                    let mut names: Vec<String> = Vec::with_capacity(field_count);
                    let mut values: Vec<FieldValue> = Vec::with_capacity(field_count);

                    for (name, value) in feature.fields() {
                        if let Some(value) = value {
                            names.push(name);
                            values.push(value);
                        }
                    }

                    dst_layer.create_feature_fields(geom, &names.iter().map(|s| s.as_ref()).collect::<Vec<&str>>(), &values)?;
                } else {
                    // Only copy the geometry
                    dst_layer.create_feature(geom)?;
                }
            }
        }
    }

    Ok(mem_ds)
}

struct RasterizeOptionsWrapper {
    options: *mut gdal_sys::GDALRasterizeOptions,
}

impl RasterizeOptionsWrapper {
    fn new(opts: &[String]) -> Result<Self> {
        let mut c_opts = gdal::cpl::CslStringList::new();
        for opt in opts {
            c_opts.add_string(opt)?;
        }

        let options = unsafe { gdal_sys::GDALRasterizeOptionsNew(c_opts.as_ptr(), std::ptr::null_mut()) };
        if options.is_null() {
            return Err(Error::InvalidArgument("Failed to create rasterize options".to_string()));
        }

        Ok(Self { options })
    }

    fn c_options(&self) -> *mut gdal_sys::GDALRasterizeOptions {
        self.options
    }
}

impl Drop for RasterizeOptionsWrapper {
    fn drop(&mut self) {
        unsafe { gdal_sys::GDALRasterizeOptionsFree(self.c_options()) };
    }
}

struct VectorTranslateOptionsWrapper {
    options: *mut gdal_sys::GDALVectorTranslateOptions,
}

impl VectorTranslateOptionsWrapper {
    fn new(opts: &[String]) -> Result<Self> {
        let mut c_opts = gdal::cpl::CslStringList::new();
        for opt in opts {
            c_opts.add_string(opt)?;
        }

        let options = unsafe { gdal_sys::GDALVectorTranslateOptionsNew(c_opts.as_ptr(), std::ptr::null_mut()) };
        if options.is_null() {
            return Err(Error::InvalidArgument("Failed to create vector translate options".to_string()));
        }

        Ok(Self { options })
    }

    fn c_options(&mut self) -> *mut gdal_sys::GDALVectorTranslateOptions {
        self.options
    }
}

impl Drop for VectorTranslateOptionsWrapper {
    fn drop(&mut self) {
        unsafe { gdal_sys::GDALVectorTranslateOptionsFree(self.c_options()) };
    }
}

#[cfg(test)]
mod tests {

    use path_macro::path;

    use crate::vector;
    use crate::Result;

    use super::*;

    fn layer_surface_area(layer: &mut gdal::vector::Layer) -> f64 {
        layer.features().map(|f| f.geometry().unwrap().area()).sum::<f64>()
    }

    #[test]
    fn test_buffer() -> Result<()> {
        let path = path!(env!("CARGO_MANIFEST_DIR") / "tests" / "data" / "boundaries.gpkg");

        let ds = vector::io::dataset::open_read_only(&path).unwrap();
        let buffered_ds = buffer(
            &ds,
            &BufferOptions {
                distance: 1000.0,
                num_quad_segments: 30,
                include_fields: false,
                ..Default::default()
            },
        )?;

        assert_eq!(buffered_ds.layer_count(), ds.layer_count());
        assert_eq!(buffered_ds.layer(0)?.defn().field_count()?, 0);
        // The buffered geometry should cover a larger surface area
        assert!(layer_surface_area(&mut buffered_ds.layer(0)?) > layer_surface_area(&mut ds.layer(0)?));

        Ok(())
    }

    #[test]
    fn test_buffer_include_fields() -> Result<()> {
        let path = path!(env!("CARGO_MANIFEST_DIR") / "tests" / "data" / "boundaries.gpkg");

        let ds = vector::io::dataset::open_read_only(&path).unwrap();
        let buffered_ds = buffer(
            &ds,
            &BufferOptions {
                distance: 1000.0,
                num_quad_segments: 10,
                include_fields: true,
                ..Default::default()
            },
        )?;

        assert_eq!(buffered_ds.layer_count(), ds.layer_count());
        assert_eq!(buffered_ds.layer(0)?.defn().field_count()?, 1);

        Ok(())
    }
}
