use geo::{Array, GeoReference, RasterScale, Tile, raster::formats};
use raster_tile::RasterTileIO;
use tiler::{
    LayerSourceType, Result, TileFormat, TileRequest,
    tileproviderfactory::{TileProviderOptions, create_single_file_tile_provider},
};

fn assert_metadata_coefficients_preserve_raw_values(driver: &str, source_format: LayerSourceType) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let tile = Tile { z: 8, x: 128, y: 128 };
    let tile_size = 256;
    let georef = GeoReference::from_tile(&tile, tile_size, 1).copy_with_nodata(Some(u16::MAX));
    let mut pixels: Vec<u16> = (0..tile_size * tile_size).map(|index| (index % 13) as u16).collect();
    pixels[0] = u16::MAX;

    for (index, (scale, offset)) in [
        (None, None),
        (Some(1.0), Some(0.0)),
        (Some(-0.5), None),
        (None, Some(100.0)),
        (Some(-0.5), Some(100.0)),
    ]
    .into_iter()
    .enumerate()
    {
        let path = directory.path().join(format!("{driver}_{index}.tif"));
        {
            let dataset = formats::gdal::create_in_memory_dataset_with_data(&georef, &pixels)?;
            let mut band = dataset.rasterband(1)?;
            // GDAL omits identity coefficients when translating. Write nonidentity values first,
            // then replace their XML text in place to preserve the TIFF/COG layout.
            let explicit_identity = (scale, offset) == (Some(1.0), Some(0.0));
            if let Some(scale) = scale {
                band.set_scale(if explicit_identity { 2.0 } else { scale })?;
            }
            if let Some(offset) = offset {
                band.set_offset(if explicit_identity { 3.0 } else { offset })?;
            }
            band.get_statistics(true, false)?.expect("Fixture must have raw statistics");

            let mut options = vec!["-of".into(), driver.into()];
            if driver == "COG" {
                options.extend(["-co".into(), "BLOCKSIZE=256".into(), "-co".into(), "COMPRESS=NONE".into()]);
            }
            geo::raster::algo::gdal::translate(&dataset, &path, &options)?;
        }

        if (scale, offset) == (Some(1.0), Some(0.0)) {
            let mut bytes = std::fs::read(&path)?;
            for (item, replacement) in [
                (b"<Item name=\"SCALE\" sample=\"0\" role=\"scale\">2</Item>".as_slice(), b'1'),
                (b"<Item name=\"OFFSET\" sample=\"0\" role=\"offset\">3</Item>".as_slice(), b'0'),
            ] {
                let position = bytes
                    .windows(item.len())
                    .position(|window| window == item)
                    .expect("Fixture must contain coefficient XML");
                bytes[position + item.len() - "</Item>".len() - 1] = replacement;
            }
            std::fs::write(&path, bytes)?;
        }

        let provider = create_single_file_tile_provider(
            &path,
            &TileProviderOptions {
                calculate_stats: true,
                ..Default::default()
            },
        )?;
        let metadata = provider.layers().remove(0);
        assert_eq!(metadata.source_format, source_format);
        let expected_scale = match (scale, offset) {
            (None, None) => None,
            (scale, offset) => Some(RasterScale {
                scale: scale.unwrap_or(1.0),
                offset: offset.unwrap_or(0.0),
            }),
        };
        assert_eq!(metadata.scale, expected_scale, "{driver} must preserve source coefficient presence");
        assert_eq!((metadata.min_value, metadata.max_value), (0.0, 12.0));
        assert_eq!(metadata.nodata, Some(f64::from(u16::MAX)));

        let tile_data = provider.get_tile(
            metadata.id,
            &TileRequest {
                tile,
                dpi_ratio: 1,
                tile_size: tile_size as u32,
                tile_format: TileFormat::RasterTile,
            },
        )?;
        let raster = geo::DenseArray::<u16>::from_raster_tile_bytes(&tile_data.data)?;
        assert_eq!(
            raster.as_slice(),
            pixels.as_slice(),
            "{driver} must not apply scale/offset to tile values"
        );
    }

    Ok(())
}

#[test]
fn gdal_metadata_coefficients_preserve_raw_values() -> Result<()> {
    assert_metadata_coefficients_preserve_raw_values("GTiff", LayerSourceType::GeoTiff)
}

#[test]
fn cog_metadata_coefficients_preserve_raw_values() -> Result<()> {
    assert_metadata_coefficients_preserve_raw_values("COG", LayerSourceType::CloudOptimizedGeoTiff)
}

#[test]
fn mbtiles_metadata_has_no_scale() -> Result<()> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("test/data/gem_limburg.mbtiles");
    let provider = create_single_file_tile_provider(&path, &TileProviderOptions::default())?;
    let metadata = provider.layers().remove(0);
    assert_eq!(metadata.scale, None);
    Ok(())
}
