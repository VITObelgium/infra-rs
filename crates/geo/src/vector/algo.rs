use std::{
    any::Any,
    ffi::{c_char, c_int, c_void},
    panic::{AssertUnwindSafe, catch_unwind},
    path::Path,
};

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
    translate_ds_to_disk_with_progress(ds, path, options, None)
}

/// Translate a GDAL vector dataset to disk with optional GDAL progress reporting.
///
/// Options match the `ogr2ogr` command line tool, as in [`translate_ds_to_disk`].
/// The callback runs synchronously on the calling thread with completion fractions
/// from 0.0 to 1.0 and is not retained after this function returns. GDAL may disable
/// progress reporting for source layers without fast feature counts.
///
/// # Errors
/// Returns an error if translation fails or the callback panics while unwinding is
/// enabled. A callback panic interrupts translation without unwinding through GDAL;
/// a partially written output may remain on disk.
pub fn translate_ds_to_disk_with_progress(
    ds: &gdal::Dataset,
    path: &Path,
    options: &[String],
    progress: Option<&mut dyn FnMut(f64)>,
) -> Result<gdal::Dataset> {
    gdalinterop::create_output_directory_if_needed(path)?;
    let path_str = std::ffi::CString::new(path.to_string_lossy().to_string())?;
    let mut progress = progress.map(|callback| VectorTranslateProgress {
        callback,
        panic_payload: None,
    });
    let mut opts = if progress.is_some() {
        VectorTranslateOptionsWrapper::new_with_progress(options)?
    } else {
        VectorTranslateOptionsWrapper::new(options)?
    };
    let mut usage_error: c_int = 0;

    if let Some(progress) = progress.as_mut() {
        // SAFETY: GDALVectorTranslate calls back synchronously, and the context
        // stays at this address until it returns. The options do not outlive it.
        unsafe {
            gdal_sys::GDALVectorTranslateOptionsSetProgress(
                opts.c_options(),
                Some(vector_translate_progress_trampoline),
                std::ptr::from_mut(progress).cast::<c_void>(),
            );
        }
    }

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

    // Own any returned dataset before checking callback errors so it is closed
    // even if a driver returns a handle after the callback requested interruption.
    let translated_ds =
        gdalinterop::check_pointer(handle, "GDALVectorTranslate").map(|handle| unsafe { gdal::Dataset::from_c_dataset(handle) });

    if progress.as_ref().is_some_and(|progress| progress.panic_payload.is_some()) {
        return Err(Error::Runtime("Vector translate: progress callback panicked".to_string()));
    }

    if usage_error == gdalinterop::TRUE {
        return Err(Error::InvalidArgument("Vector translate: invalid arguments".to_string()));
    }

    Ok(translated_ds?)
}

struct VectorTranslateProgress<'a> {
    callback: &'a mut dyn FnMut(f64),
    // Retain the payload until GDAL returns: even its destructor may panic.
    panic_payload: Option<Box<dyn Any + Send>>,
}

unsafe extern "C" fn vector_translate_progress_trampoline(complete: f64, _message: *const c_char, user_data: *mut c_void) -> c_int {
    if user_data.is_null() {
        return gdalinterop::TRUE;
    }

    // SAFETY: Only installed with a live, exclusively borrowed context for the
    // duration of the synchronous GDALVectorTranslate call.
    let progress = unsafe { &mut *user_data.cast::<VectorTranslateProgress<'_>>() };
    if progress.panic_payload.is_some() {
        return gdalinterop::FALSE;
    }

    // The callback is never reused after a panic, so its unwind safety is not
    // required. No Rust unwind (including payload destruction) may cross C.
    match catch_unwind(AssertUnwindSafe(|| (progress.callback)(complete))) {
        Ok(()) => gdalinterop::TRUE,
        Err(payload) => {
            progress.panic_payload = Some(payload);
            gdalinterop::FALSE
        }
    }
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
    fn new(opts: impl IntoIterator<Item = impl AsRef<str>>) -> Result<Self> {
        let mut c_opts = gdal::cpl::CslStringList::new();
        for opt in opts {
            c_opts.add_string(opt.as_ref())?;
        }

        let options = unsafe { gdal_sys::GDALVectorTranslateOptionsNew(c_opts.as_ptr(), std::ptr::null_mut()) };
        if options.is_null() {
            return Err(Error::InvalidArgument("Failed to create vector translate options".to_string()));
        }

        Ok(Self { options })
    }

    fn new_with_progress(opts: &[String]) -> Result<Self> {
        // GDAL's progress setter only installs the callback; ogr2ogr also needs
        // -progress to enable feature counting and progress notifications.
        let progress_option = (!opts.iter().any(|opt| opt.eq_ignore_ascii_case("-progress"))).then_some("-progress");
        Self::new(progress_option.into_iter().chain(opts.iter().map(String::as_str)))
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

    fn point_dataset() -> Result<gdal::Dataset> {
        let mut ds = gdalio::dataset::create_in_memory()?;
        let srs = gdal::spatial_ref::SpatialRef::from_epsg(4326)?;
        let mut layer = ds.create_layer(gdal::vector::LayerOptions {
            name: "points",
            srs: Some(&srs),
            ty: gdal_sys::OGRwkbGeometryType::wkbPoint,
            ..Default::default()
        })?;
        for _ in 0..128 {
            layer.create_feature(gdal::vector::Geometry::from_wkt("POINT (4 50)")?)?;
        }
        Ok(ds)
    }

    fn reprojection_options() -> Vec<String> {
        // Small transactions also bound Arrow batch sizes, so progress remains
        // intermediate if GDAL chooses the batched translation path.
        ["-f", "GPKG", "-t_srs", "EPSG:3857", "-gt", "16"]
            .into_iter()
            .map(String::from)
            .collect()
    }

    #[test]
    fn translate_ds_to_disk_with_progress_reports_intermediate_values_and_completion() -> Result<()> {
        let ds = point_dataset()?;
        let directory = tempfile::tempdir()?;
        let mut values = Vec::new();
        let translated = translate_ds_to_disk_with_progress(
            &ds,
            &directory.path().join("translated.gpkg"),
            &reprojection_options(),
            Some(&mut |value| values.push(value)),
        )?;

        assert!(values.iter().any(|&value| value > 0.0 && value < 1.0), "{values:?}");
        assert_eq!(values.last(), Some(&1.0));
        assert!(values.iter().all(|value| (0.0..=1.0).contains(value)), "{values:?}");
        assert!(values.windows(2).all(|pair| pair[0] <= pair[1]), "{values:?}");
        let mut layer = translated.layer(0)?;
        assert_eq!(layer.feature_count(), 128);
        assert_eq!(layer.spatial_ref().unwrap().auth_code()?, 3857);
        let feature = layer.features().next().unwrap();
        let point = feature.geometry().unwrap().get_point(0);
        approx::assert_abs_diff_eq!(point.0, 445_277.963_173, epsilon = 0.001);
        approx::assert_abs_diff_eq!(point.1, 6_446_275.841_017, epsilon = 0.001);
        Ok(())
    }

    #[test]
    fn translate_ds_to_disk_with_progress_accepts_existing_progress_option() -> Result<()> {
        let ds = point_dataset()?;
        let directory = tempfile::tempdir()?;
        let mut options = reprojection_options();
        options.push("-progress".to_string());
        let mut complete = false;
        let _translated = translate_ds_to_disk_with_progress(
            &ds,
            &directory.path().join("translated.gpkg"),
            &options,
            Some(&mut |value| complete |= value == 1.0),
        )?;

        assert!(complete);
        Ok(())
    }

    #[test]
    fn translate_ds_to_disk_preserves_translation_without_progress() -> Result<()> {
        let ds = point_dataset()?;
        let directory = tempfile::tempdir()?;
        let options = reprojection_options();
        let translated = translate_ds_to_disk(&ds, &directory.path().join("legacy.gpkg"), &options)?;
        let without_progress = translate_ds_to_disk_with_progress(&ds, &directory.path().join("no_progress.gpkg"), &options, None)?;

        assert_eq!(translated.layer(0)?.feature_count(), 128);
        assert_eq!(without_progress.layer(0)?.feature_count(), 128);
        Ok(())
    }

    #[test]
    fn translate_ds_to_disk_with_progress_returns_error_when_callback_panics() -> Result<()> {
        let ds = point_dataset()?;
        let directory = tempfile::tempdir()?;
        for panic_at in [0.0, 1.0] {
            let mut panics = 0;
            let error = translate_ds_to_disk_with_progress(
                &ds,
                &directory.path().join(format!("panic_{panic_at}.gpkg")),
                &reprojection_options(),
                Some(&mut |value| {
                    if value >= panic_at {
                        panics += 1;
                        panic!("progress callback panic at {value}");
                    }
                }),
            )
            .expect_err("callback panics must be converted to an error");

            assert!(matches!(error, Error::Runtime(ref message) if message == "Vector translate: progress callback panicked"));
            assert_eq!(panics, 1, "a panicked callback must not be called again");
        }
        Ok(())
    }

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
