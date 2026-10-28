use std::{ops::Range, path::PathBuf, sync::Arc};

use gdal::{DriverManager, Metadata as _, raster::Buffer};
use geo::{Array, DenseArray, GeoReference, RasterScale, Tile, raster::algo::gdal::translate};
use raster_tile::RasterTileIO;
use tiler::{DynamicTileProvider, LayerMetadata, LayerSourceType, Result, TileFormat, TileRequest};

struct Fixture {
    _directory: tempfile::TempDir,
    path: PathBuf,
    tile: Tile,
}

impl Fixture {
    fn new(sparse_second_band: bool, compression: &str) -> Result<Self> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("multiband.tif");
        let tile = Tile { z: 8, x: 128, y: 128 };
        let dataset = fixture_dataset(tile, sparse_second_band)?;
        let options = vec![
            "-of".into(),
            "COG".into(),
            "-co".into(),
            "BLOCKSIZE=256".into(),
            "-co".into(),
            "INTERLEAVE=TILE".into(),
            "-co".into(),
            format!("COMPRESS={compression}"),
            "-co".into(),
            "SPARSE_OK=YES".into(),
            "-co".into(),
            "OVERVIEWS=NONE".into(),
        ];
        translate(&dataset, &path, &options)?;
        // The existing COG detector requires a full 16 KiB header read, even for
        // smaller compressed files. Trailing padding leaves TIFF payloads unchanged.
        let file = std::fs::OpenOptions::new().write(true).open(&path)?;
        file.set_len(file.metadata()?.len().max(16 * 1024))?;
        Ok(Self {
            _directory: directory,
            path,
            tile,
        })
    }

    fn layers(&self) -> Result<Vec<LayerMetadata>> {
        DynamicTileProvider::default().add_file(&self.path)
    }

    fn request(&self) -> TileRequest {
        TileRequest {
            tile: self.tile,
            dpi_ratio: 1,
            tile_size: 256,
            tile_format: TileFormat::RasterTile,
        }
    }
}

fn fixture_dataset(tile: Tile, sparse_second_band: bool) -> Result<gdal::Dataset> {
    let georef = GeoReference::from_tile(&tile, 256, 1);
    let mut dataset = DriverManager::get_driver_by_name("MEM")?.create_with_band_type::<u16, _>("", 256, 256, 3)?;
    dataset.set_geo_transform(&georef.geo_transform().into())?;
    dataset.set_projection("EPSG:3857")?;
    for index in 1..=3 {
        write_fixture_band(&dataset, index, sparse_second_band)?;
    }
    Ok(dataset)
}

fn write_fixture_band(dataset: &gdal::Dataset, index: usize, sparse_second_band: bool) -> Result<()> {
    let mut band = dataset.rasterband(index)?;
    band.set_no_data_value(Some(f64::from(u16::MAX)))?;
    band.set_description(&format!("Channel {index}"))?;
    band.set_scale(index as f64 * 0.5)?;
    band.set_offset(100.0)?;
    let value = if sparse_second_band && index == 2 {
        u16::MAX
    } else {
        index as u16 * 10
    };
    let mut values = vec![value; 256 * 256];
    values[..8 * 256].fill(u16::MAX);
    band.write((0, 0), (256, 256), &mut Buffer::new((256, 256), values))?;
    Ok(())
}

fn read(meta: &LayerMetadata, req: &TileRequest, bands: Range<usize>) -> Result<Vec<DenseArray<f32>>> {
    DynamicTileProvider::get_multiband_tile_for_layer(meta, req, bands)
}

#[test]
fn enumerates_bands_with_unique_ids_metadata_and_shared_reader() -> Result<()> {
    let fixture = Fixture::new(false, "NONE")?;
    let layers = fixture.layers()?;
    assert_eq!(layers.len(), 3);
    let ids: std::collections::HashSet<_> = layers.iter().map(|layer| layer.id).collect();
    assert_eq!(ids.len(), 3);
    for (index, layer) in layers.iter().enumerate() {
        assert_eq!(layer.band_nr, Some(index + 1));
        assert_eq!(layer.source_format, LayerSourceType::CloudOptimizedGeoTiff);
        assert_eq!(layer.description, format!("Channel {}", index + 1));
        assert_eq!(
            layer.scale,
            Some(RasterScale {
                scale: (index + 1) as f64 * 0.5,
                offset: 100.0
            })
        );
        assert!(Arc::ptr_eq(
            layer.tileprovider_data.as_ref().unwrap(),
            layers[0].tileprovider_data.as_ref().unwrap()
        ));
    }
    Ok(())
}

fn assert_raw_bands(compression: &str) -> Result<()> {
    let fixture = Fixture::new(false, compression)?;
    let provider = tiler::tileproviderfactory::create_single_file_tile_provider(&fixture.path, &Default::default())?;
    let layers = provider.layers();
    let request = fixture.request();
    // A subset may be requested through metadata for any band of the same file.
    let arrays = read(&layers[2], &request, 2..4)?;
    assert_eq!(arrays.len(), 2);
    for (index, array) in arrays.iter().enumerate() {
        assert_eq!(array.as_slice().len(), 256 * 256);
        assert!(array.as_slice()[..8 * 256].iter().all(|value| value.is_nan()));
        assert!(array.as_slice()[8 * 256..].iter().all(|value| *value == (index + 2) as f32 * 10.0));
        let single = provider.get_tile(layers[index + 1].id, &request)?;
        let single = DenseArray::<u16>::from_raster_tile_bytes(&single.data)?;
        assert_eq!(single.as_slice()[8 * 256], (index + 2) as u16 * 10);
    }
    Ok(())
}

#[test]
fn returns_raw_f32_subset_and_preserves_single_band_reads() -> Result<()> {
    assert_raw_bands("NONE")
}

#[test]
fn lerc_nodata_masks_become_nan_without_scaling() -> Result<()> {
    assert_raw_bands("LERC")
}

#[test]
fn sparse_bands_and_missing_coverage_preserve_band_count() -> Result<()> {
    let fixture = Fixture::new(true, "NONE")?;
    let layers = fixture.layers()?;
    let mut request = fixture.request();
    let arrays = read(&layers[0], &request, 1..4)?;
    assert_eq!(
        arrays.iter().map(|array| array.is_empty()).collect::<Vec<_>>(),
        [false, true, false]
    );
    request.tile.x += 1;
    let arrays = read(&layers[0], &request, 1..4)?;
    assert_eq!(arrays.len(), 3);
    assert!(arrays.iter().all(|array| array.is_empty()));
    request.tile.z += 1;
    assert!(read(&layers[0], &request, 2..4)?.iter().all(|array| array.is_empty()));
    Ok(())
}

#[test]
fn below_minimum_zoom_synthesizes_all_requested_bands() -> Result<()> {
    let fixture = Fixture::new(true, "NONE")?;
    let layers = fixture.layers()?;
    let mut request = fixture.request();
    request.tile = Tile { z: 7, x: 64, y: 64 };
    let arrays = read(&layers[0], &request, 1..4)?;
    assert_eq!(arrays.len(), 3);
    assert!(
        arrays[1].is_empty(),
        "Unexpected sparse band value: {:?}",
        arrays[1].as_slice().iter().find(|value| !value.is_nan())
    );
    for (index, value) in [(0, 10.0), (2, 30.0)] {
        assert_eq!(arrays[index].as_slice()[8 * 256 + 8], value);
        assert!(arrays[index].as_slice()[200 * 256 + 200].is_nan());
        assert!(arrays[index].as_slice()[0].is_nan());
    }
    let subset = read(&layers[0], &request, 3..4)?;
    assert_eq!(subset[0].as_slice()[8 * 256 + 8], 30.0);
    request.tile.x += 1;
    assert!(read(&layers[0], &request, 1..4)?.iter().all(|array| array.is_empty()));
    Ok(())
}

#[test]
fn single_band_cogs_support_raw_batch_reads() -> Result<()> {
    let fixture = Fixture::new(false, "NONE")?;
    let input = gdal::Dataset::open(&fixture.path)?;
    let path = fixture._directory.path().join("single.tif");
    translate(
        &input,
        &path,
        &[
            "-of".into(),
            "COG".into(),
            "-b".into(),
            "1".into(),
            "-co".into(),
            "COMPRESS=NONE".into(),
            "-co".into(),
            "BLOCKSIZE=256".into(),
        ],
    )?;
    let layers = DynamicTileProvider::default().add_file(&path)?;
    assert_eq!(layers.len(), 1);
    let mut request = fixture.request();
    request.tile_format = TileFormat::Png;
    let arrays = read(&layers[0], &request, 1..2)?;
    assert_eq!(arrays.len(), 1);
    assert!(arrays[0].as_slice()[0].is_nan());
    assert_eq!(arrays[0].as_slice()[8 * 256], 10.0);
    assert!(read(&layers[0], &request, 1..3).is_err());
    Ok(())
}

#[test]
fn cancellable_api_returns_typed_cancellation_without_partial_output() -> Result<()> {
    let fixture = Fixture::new(false, "NONE")?;
    let meta = fixture.layers()?.remove(0);
    let result = DynamicTileProvider::get_multiband_tile_for_layer_cancellable(&meta, &fixture.request(), 1..4, || true);
    assert!(matches!(result, Err(tiler::Error::GeoError(geo::Error::Cancelled))));
    let arrays = DynamicTileProvider::get_multiband_tile_for_layer_cancellable(&meta, &fixture.request(), 2..4, || false)?;
    assert_eq!(arrays.len(), 2);
    assert_eq!(arrays[0].as_slice()[8 * 256], 20.0);
    Ok(())
}

#[test]
fn cancellation_after_uninterruptible_gdal_fallback_discards_output() -> Result<()> {
    let fixture = Fixture::new(false, "NONE")?;
    let meta = fixture.layers()?.remove(0);
    let mut request = fixture.request();
    request.tile = Tile { z: 7, x: 64, y: 64 };
    let checks = std::cell::Cell::new(0);
    // The first checks admit the request and warp; the post-warp check cancels.
    let result = DynamicTileProvider::get_multiband_tile_for_layer_cancellable(&meta, &request, 1..4, || {
        checks.set(checks.get() + 1);
        checks.get() >= 3
    });
    assert!(matches!(result, Err(tiler::Error::GeoError(geo::Error::Cancelled))));
    assert_eq!(checks.get(), 3);
    Ok(())
}

#[test]
fn invalid_ranges_and_unsupported_requests_return_errors() -> Result<()> {
    let fixture = Fixture::new(false, "NONE")?;
    let mut meta = fixture.layers()?.remove(0);
    let mut request = fixture.request();
    for (start, end) in [(0, 2), (1, 1), (3, 2), (1, 5), (4, 5), (1, usize::MAX)] {
        assert!(read(&meta, &request, start..end).is_err(), "{start}..{end}");
    }
    request.tile_size = 512;
    assert!(read(&meta, &request, 1..4).is_err());
    request = fixture.request();
    request.dpi_ratio = 2;
    assert!(read(&meta, &request, 1..4).is_err());
    request = fixture.request();
    meta.source_format = LayerSourceType::GeoTiff;
    assert!(read(&meta, &request, 1..4).unwrap_err().to_string().contains("only for COG"));
    meta.source_format = LayerSourceType::CloudOptimizedGeoTiff;
    meta.tileprovider_data = None;
    assert!(read(&meta, &request, 1..4).is_err());
    Ok(())
}
