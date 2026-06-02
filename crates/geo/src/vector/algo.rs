use std::path::Path;

use crate::vector::gdalio::{self, FeatureDefinitionExtension as _};
use crate::{Error, Result, gdalinterop};
use gdal::vector::Feature;
use gdal::vector::LayerAccess;

use super::geometrytype::GeometryType;

mod rasterize;

pub use rasterize::{
    RasterizeOptions, rasterize, rasterize_ds, rasterize_ds_with_cli_options, rasterize_to_disk_with_cli_options,
    rasterize_with_cli_options,
};

/// Translate a GDAL vector dataset using the provided translate options
/// The options are passed as a list of strings in the form `["-option1", "value1", "-option2", "value2"]`
/// and match the options of the gdal ogr2ogr command line tool
/// The translated dataset is returned
pub fn translate_cli_opts(ds: &gdal::Dataset, options: &[String]) -> Result<gdal::Dataset> {
    let mem_ds = gdalio::dataset::create_in_memory()?;
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
    let path_str = std::ffi::CString::new(path.to_string_lossy().to_string())?;
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

    let mut mem_ds = gdalio::dataset::create_in_memory()?;

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
        let dst_layer = mem_ds.create_layer(layer_options)?;

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

        let defn = dst_layer.defn();
        for feature in src_layer.features() {
            if let Some(geom) = feature.geometry() {
                let mut ft = Feature::new(defn)?;
                let geom = geom.buffer(opts.distance, opts.num_quad_segments)?;
                ft.set_geometry(geom)?;

                if opts.include_fields {
                    // Copy the fields
                    for (name, value) in feature.fields() {
                        if let Some(value) = value {
                            ft.set_field(defn.field_index(name)?, &value)?;
                        }
                    }
                }

                ft.create(&dst_layer)?;
            }
        }
    }

    Ok(mem_ds)
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

    use crate::Result;

    use super::*;

    fn layer_surface_area(layer: &mut gdal::vector::Layer) -> f64 {
        layer.features().map(|f| f.geometry().unwrap().area()).sum::<f64>()
    }

    #[test]
    fn test_buffer() -> Result<()> {
        let path = path!(env!("CARGO_MANIFEST_DIR") / "tests" / "data" / "boundaries.gpkg");

        let ds = gdalio::dataset::open_read_only(&path).unwrap();
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

        let ds = gdalio::dataset::open_read_only(&path).unwrap();
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
