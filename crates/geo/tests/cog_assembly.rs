#![cfg(all(feature = "gdal", feature = "raster-io-geotiff", feature = "deflate"))]

use std::{
    fs::{self, File},
    path::{Path, PathBuf},
};

use gdal::{Dataset, DriverManager, Metadata, cpl::CslStringList, raster::RasterCreationOptions};
use geo::{
    Array, ArrayDataType, Columns, GeoReference, Result, Rows, Tile, ZoomLevelStrategy,
    cog::{CogCreationOptions, PredictorSelection, TileSource, WebTilesReader, create_cog_tiles},
    geotiff::{BandIndex, FIRST_BAND, GeoTiffMetadata, TiffChunkLocation, assemble_band_cogs, io},
    raster::{Compression, algo::gdal::translate, formats::gdal::create_in_memory_dataset_with_data},
};

const TILE_SIZE: usize = 256;
const NODATA: u8 = 255;
const SENTINEL: &[u8] = b"do not replace this destination on validation failure\0\xff";

fn source_georeference(tiles: i32, x: i32) -> GeoReference {
    let mut georef = GeoReference::from_tile(&Tile { z: 8, x, y: 85 }, TILE_SIZE, 1);
    georef.set_rows(Rows(tiles * TILE_SIZE as i32));
    georef.set_columns(Columns(tiles * TILE_SIZE as i32));
    georef.set_nodata(Some(f64::from(NODATA)));
    georef
}

fn options(aligned_levels: i32) -> CogCreationOptions {
    CogCreationOptions {
        min_zoom: Some(6),
        zoom_level_strategy: ZoomLevelStrategy::Manual(8),
        tile_size: TILE_SIZE as u32,
        compression: Some(Compression::Deflate),
        predictor: Some(PredictorSelection::Horizontal),
        allow_sparse: true,
        output_data_type: Some(ArrayDataType::Uint8),
        aligned_levels: Some(aligned_levels),
        scale: false,
    }
}

fn single_level_options() -> CogCreationOptions {
    CogCreationOptions {
        min_zoom: Some(8),
        ..options(1)
    }
}

fn stage_band(
    directory: &Path,
    name: &str,
    georef: &GeoReference,
    pixels: &[u8],
    options: CogCreationOptions,
    edit_metadata: impl FnOnce(&mut Dataset) -> Result<()>,
) -> Result<PathBuf> {
    let source = directory.join(format!("{name}_source.tif"));
    let output = directory.join(format!("{name}_cog.tif"));
    let mut dataset = create_in_memory_dataset_with_data(georef, pixels)?;
    edit_metadata(&mut dataset)?;
    translate(&dataset, &source, &["-of".into(), "GTiff".into()])?.close()?;
    create_cog_tiles(&source, &output, options)?;
    Ok(output)
}

fn sparse_pixels(georef: &GeoReference, band: usize) -> Vec<u8> {
    let width = georef.columns().count() as usize;
    (0..georef.raster_size().cell_count())
        .map(|index| {
            let row = index / width;
            let col = index % width;
            let occupied = match band {
                0 => col < TILE_SIZE || (row >= 2 * TILE_SIZE && col >= 2 * TILE_SIZE),
                _ => row < TILE_SIZE || col >= 2 * TILE_SIZE,
            };
            if occupied {
                (1 + (row / 16 + col / 16 + band * 43) % 200) as u8
            } else {
                NODATA
            }
        })
        .collect()
}

fn web_reader(path: &Path) -> Result<WebTilesReader> {
    WebTilesReader::new(GeoTiffMetadata::from_file(path)?)
}

fn raw_chunk(file: &mut File, chunk: TiffChunkLocation) -> Result<Vec<u8>> {
    assert!(!chunk.is_sparse(), "Sparse chunks must not trigger source reads");
    let mut bytes = vec![0; usize::try_from(chunk.size).expect("Small fixture chunk")];
    io::read_chunk(&chunk, file, &mut bytes)?;
    Ok(bytes)
}

fn assert_native_payloads_and_decodes(inputs: &[PathBuf], output: &Path) -> Result<()> {
    let assembled = web_reader(output)?;
    let assembled_dataset = Dataset::open(output)?;
    let mut assembled_file = File::open(output)?;
    assert_eq!(assembled.cog_metadata().band_count as usize, inputs.len());
    assert_eq!(assembled_dataset.raster_count(), inputs.len());
    assert_eq!(assembled_dataset.metadata_item("LAYOUT", "IMAGE_STRUCTURE").as_deref(), Some("COG"));
    if inputs.len() > 1 {
        assert_eq!(
            assembled_dataset.metadata_item("INTERLEAVE", "IMAGE_STRUCTURE").as_deref(),
            Some("TILE")
        );
    }

    for (band_index, input) in inputs.iter().enumerate() {
        let source = web_reader(input)?;
        let source_dataset = Dataset::open(input)?;
        let source_band = source_dataset.rasterband(1)?;
        let output_band = assembled_dataset.rasterband(band_index + 1)?;
        let band = BandIndex::new(band_index + 1).unwrap();
        let mut source_file = File::open(input)?;
        assert_eq!(source.native_overviews().len(), assembled.native_overviews().len());
        assert_eq!(source.data_type(), assembled.data_type());
        assert_eq!(source.cog_metadata().compression, assembled.cog_metadata().compression);
        assert_eq!(source.cog_metadata().predictor, assembled.cog_metadata().predictor);
        assert_eq!(source.cog_metadata().data_layout, assembled.cog_metadata().data_layout);
        assert_eq!(source_dataset.geo_transform()?, assembled_dataset.geo_transform()?);
        assert_eq!(source_dataset.projection(), assembled_dataset.projection());
        assert_eq!(source_band.no_data_value(), output_band.no_data_value());
        assert_eq!(output_band.overview_count()? as usize + 1, assembled.native_overviews().len());
        assert_eq!(source_band.overview_count()?, output_band.overview_count()?);

        for (level_index, (expected, actual)) in source.native_overviews().iter().zip(assembled.native_overviews()).enumerate() {
            assert_eq!(expected.raster_size, actual.raster_size, "Band {band_index}, IFD {level_index}");
            assert_eq!(actual.chunk_locations.len(), expected.chunk_locations.len() * inputs.len());

            let start = band_index * expected.chunk_locations.len();
            let output_chunks = &actual.chunk_locations[start..start + expected.chunk_locations.len()];
            for (tile_index, (&source_chunk, &output_chunk)) in expected.chunk_locations.iter().zip(output_chunks).enumerate() {
                assert_eq!(
                    source_chunk.is_sparse(),
                    output_chunk.is_sparse(),
                    "Band {band_index}, IFD {level_index}, tile {tile_index}"
                );
                assert_eq!(source_chunk.size, output_chunk.size);
                if source_chunk.is_sparse() {
                    assert_eq!(output_chunk, TiffChunkLocation { offset: 0, size: 0 });
                } else {
                    assert_eq!(
                        raw_chunk(&mut source_file, source_chunk)?,
                        raw_chunk(&mut assembled_file, output_chunk)?,
                        "Compressed bytes changed for band {band_index}, IFD {level_index}, tile {tile_index}"
                    );
                }
            }

            let source_level = if level_index == 0 {
                source_dataset.rasterband(1)?
            } else {
                source_band.overview(level_index - 1)?
            };
            let output_level = if level_index == 0 {
                assembled_dataset.rasterband(band_index + 1)?
            } else {
                output_band.overview(level_index - 1)?
            };
            assert_eq!(source_level.band_type(), output_level.band_type());
            assert_eq!(source_level.block_size(), output_level.block_size());
            assert_eq!(source_level.size(), output_level.size());
            assert_eq!(
                output_level.size(),
                (actual.raster_size.cols.count() as usize, actual.raster_size.rows.count() as usize)
            );
            let expected_pixels = source_level.read_band_as::<u8>()?;
            let actual_pixels = output_level.read_band_as::<u8>()?;
            assert_eq!(actual_pixels.shape(), expected_pixels.shape());
            assert_eq!(
                actual_pixels.data(),
                expected_pixels.data(),
                "GDAL pixels for band {band_index}, IFD {level_index}"
            );
            let source_pixels = source.read_overview_as::<u8>(expected, FIRST_BAND, |chunk| raw_chunk(&mut source_file, chunk))?;
            let assembled_pixels = assembled.read_overview_as::<u8>(actual, band, |chunk| raw_chunk(&mut assembled_file, chunk))?;
            assert_eq!(
                source_pixels.as_slice(),
                expected_pixels.data(),
                "Source WebTilesReader versus GDAL, IFD {level_index}"
            );
            assert_eq!(
                assembled_pixels.as_slice(),
                actual_pixels.data(),
                "Assembled WebTilesReader versus GDAL, IFD {level_index}"
            );

            let zoom = source.tile_info().max_zoom - level_index as i32;
            let source_tiles = source.zoom_level_tile_sources(zoom).expect("Source web tile level");
            let output_tiles = assembled.zoom_level_tile_sources(zoom).expect("Output web tile level");
            assert!(!source_tiles.is_empty());
            assert_eq!(source_tiles.len(), output_tiles.len());
            for tile in source_tiles.keys() {
                assert!(output_tiles.contains_key(tile), "Missing assembled web tile {tile:?}");
                let expected = source
                    .read_tile_data_as::<u8>(tile, FIRST_BAND, &mut source_file)?
                    .expect("Mapped source tile");
                let actual = assembled
                    .read_tile_data_as::<u8>(tile, band, &mut assembled_file)?
                    .expect("Mapped output tile");
                assert_eq!(actual.size(), expected.size());
                assert_eq!(actual.as_slice(), expected.as_slice(), "Web tile {tile:?}, band {band_index}");
            }
        }
    }
    Ok(())
}

fn assert_unaligned_sparse_assembly(aligned_levels: i32) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let georef = source_georeference(3, 131);
    let mut inputs = Vec::new();
    for band in 0..2 {
        inputs.push(stage_band(
            directory.path(),
            &format!("band_{band}"),
            &georef,
            &sparse_pixels(&georef, band),
            options(aligned_levels),
            |_| Ok(()),
        )?);
    }
    let first = web_reader(&inputs[0])?;
    let second = web_reader(&inputs[1])?;
    assert_eq!(first.native_overviews().len(), 3, "Fixture must exercise both lower overviews");
    let occupancy: Vec<_> = first.native_overviews()[0]
        .chunk_locations
        .iter()
        .zip(&second.native_overviews()[0].chunk_locations)
        .map(|(a, b)| (a.is_sparse(), b.is_sparse()))
        .collect();
    assert!(occupancy.contains(&(true, false)), "Fixture needs a tile occupied only in band two");
    assert!(occupancy.contains(&(false, true)), "Fixture needs a tile occupied only in band one");
    assert!(occupancy.contains(&(true, true)), "Fixture needs a tile sparse in both bands");
    let lowest_zoom = first.tile_info().max_zoom - 2;
    let lowest_tiles = first.zoom_level_tile_sources(lowest_zoom).unwrap();
    assert!(!lowest_tiles.is_empty());
    assert!(lowest_tiles.values().all(|tile| matches!(tile, TileSource::Unaligned(_))));
    let web_chunk_references: usize = lowest_tiles
        .values()
        .map(|tile| match tile {
            TileSource::Unaligned(chunks) => chunks.len(),
            _ => unreachable!("Lowest overview is unaligned"),
        })
        .sum();
    assert!(
        web_chunk_references > first.native_overviews()[2].chunk_locations.len(),
        "Fixture must reuse native chunks across different web tile cutouts"
    );

    let output = directory.path().join("assembled.tif");
    assemble_band_cogs(&inputs, &output, &mut |_| {})?;
    let assembled = web_reader(&output)?;
    assert!(
        assembled
            .zoom_level_tile_sources(lowest_zoom)
            .unwrap()
            .values()
            .all(|tile| matches!(tile, TileSource::MultiBandUnaligned(_)))
    );
    assert_native_payloads_and_decodes(&inputs, &output)
}

#[test]
fn assembles_sparse_bands_with_only_the_base_level_aligned() -> Result<()> {
    assert_unaligned_sparse_assembly(1)
}

#[test]
fn assembles_sparse_bands_with_two_aligned_levels_and_an_unaligned_lower_overview() -> Result<()> {
    assert_unaligned_sparse_assembly(2)
}

fn sorted_metadata_domain(metadata: &impl Metadata, domain: &str) -> Vec<String> {
    let mut values = metadata.metadata_domain(domain).unwrap_or_default();
    values.sort();
    values
}

#[test]
fn retains_band_metadata_statistics_and_custom_items_with_remapped_samples() -> Result<()> {
    assert_metadata_domain_survives_assembly("")?;
    assert_metadata_domain_survives_assembly("ASSEMBLY_TEST")
}

fn assert_metadata_domain_survives_assembly(band_domain: &str) -> Result<()> {
    const STATISTICS: [&str; 5] = [
        "STATISTICS_MINIMUM",
        "STATISTICS_MAXIMUM",
        "STATISTICS_MEAN",
        "STATISTICS_STDDEV",
        "STATISTICS_VALID_PERCENT",
    ];
    let directory = tempfile::tempdir()?;
    let georef = source_georeference(1, 131);
    let descriptions = ["Red & <visible>", "Near infrared \"NIR\""];
    let scales = [0.25, -0.5];
    let offsets = [-7.0, 103.0];
    let custom = ["red\tline\nwith <entities> & quotes \"'", "infrared\nsecond\tband & <value>"];
    let global = "shared\tmetadata\nwith <xml> & quotes \"'";
    let domains = if band_domain.is_empty() { vec![""] } else { vec!["", band_domain] };
    // GDAL 3.12's COPY_SRC_MDD=YES excludes default-domain band metadata, so
    // exercise default and arbitrary domains separately. Precompute statistics:
    // COG's STATISTICS=YES creates a VRT that drops arbitrary domains.
    let creation_options = RasterCreationOptions::from_iter([
        "BLOCKSIZE=256",
        "COMPRESS=DEFLATE",
        "PREDICTOR=STANDARD",
        "OVERVIEWS=NONE",
        "STATISTICS=AUTO",
        if band_domain.is_empty() {
            "COPY_SRC_MDD=AUTO"
        } else {
            "COPY_SRC_MDD=YES"
        },
    ]);
    let driver = DriverManager::get_driver_by_name("COG")?;
    let mut inputs = Vec::new();
    for band in 0..2 {
        let pixels: Vec<_> = (0..georef.raster_size().cell_count())
            .map(|index| (1 + index % 40 + band * 80) as u8)
            .collect();
        let mut dataset = create_in_memory_dataset_with_data(&georef, &pixels)?;
        for &domain in &domains {
            dataset.set_metadata_item("CUSTOM_GLOBAL", global, domain)?;
        }
        {
            let mut raster_band = dataset.rasterband(1)?;
            raster_band.set_description(descriptions[band])?;
            raster_band.set_scale(scales[band])?;
            raster_band.set_offset(offsets[band])?;
            raster_band.set_metadata_item("CUSTOM_BAND", custom[band], band_domain)?;
            raster_band.get_statistics(true, false)?.expect("Fixture statistics");
            for name in STATISTICS {
                let value = raster_band.metadata_item(name, "").expect("Computed fixture statistics");
                raster_band.set_metadata_item(name, &value, band_domain)?;
            }
        }
        let input = directory.path().join(format!("metadata_{band}.tif"));
        dataset.create_copy(&driver, &input, &creation_options)?.close()?;
        let fixture = Dataset::open(&input)?;
        let fixture_band = fixture.rasterband(1)?;
        for &domain in &domains {
            assert_eq!(
                fixture.metadata_item("CUSTOM_GLOBAL", domain).as_deref(),
                Some(global),
                "Source fixture dataset metadata in domain {domain:?}"
            );
        }
        assert_eq!(
            fixture_band.metadata_item("CUSTOM_BAND", band_domain).as_deref(),
            Some(custom[band]),
            "Source fixture band metadata in domain {band_domain:?}"
        );
        assert_eq!(
            sorted_metadata_domain(&fixture_band, band_domain),
            sorted_metadata_domain(&dataset.rasterband(1)?, band_domain),
            "GDAL must retain all source fixture band metadata"
        );
        inputs.push(input);
    }
    let output = directory.path().join("metadata_assembled.tif");
    assemble_band_cogs(&inputs, &output, &mut |_| {})?;
    let assembled = web_reader(&output)?;
    let output_dataset = Dataset::open(&output)?;
    for &domain in &domains {
        assert_eq!(output_dataset.metadata_item("CUSTOM_GLOBAL", domain).as_deref(), Some(global));
        let globals = output_dataset.metadata_domain(domain).expect("Output global metadata");
        assert_eq!(
            globals.iter().filter(|value| value.starts_with("CUSTOM_GLOBAL=")).count(),
            1,
            "Common global keys must occur once in domain {domain:?}"
        );
    }
    let mut roundtrip_inputs = Vec::new();

    for (band, input) in inputs.iter().enumerate() {
        let source = web_reader(input)?;
        let assembled_info = assembled.tile_info();
        let source_info = source.tile_info();
        assert_eq!(
            assembled_info.band_metadata[band].description,
            source_info.band_metadata[0].description
        );
        assert_eq!(assembled_info.band_metadata[band].scale, source_info.band_metadata[0].scale);
        let source_metadata = &source.cog_metadata().band_metadata[0];
        let output_metadata = &assembled.cog_metadata().band_metadata[band];

        assert_eq!(output_metadata.description, source_metadata.description);
        assert_eq!(output_metadata.scale, source_metadata.scale);

        let source_dataset = Dataset::open(input)?;
        let source_band = source_dataset.rasterband(1)?;
        let output_band = output_dataset.rasterband(band + 1)?;
        assert_eq!(output_band.description()?, descriptions[band]);
        assert_eq!(output_band.scale(), Some(scales[band]));
        assert_eq!(output_band.offset(), Some(offsets[band]));
        assert_eq!(source_band.metadata_item("CUSTOM_BAND", band_domain).as_deref(), Some(custom[band]));
        assert_eq!(output_band.metadata_item("CUSTOM_BAND", band_domain).as_deref(), Some(custom[band]));
        for &domain in &domains {
            assert_eq!(
                sorted_metadata_domain(&output_band, domain),
                sorted_metadata_domain(&source_band, domain),
                "Per-band metadata in domain {domain:?} must survive sample remapping"
            );
            assert_eq!(
                sorted_metadata_domain(&output_dataset, domain),
                sorted_metadata_domain(&source_dataset, domain)
            );
        }
        for name in STATISTICS {
            let expected = source_band.metadata_item(name, band_domain).expect("Source statistics");
            assert_eq!(output_band.metadata_item(name, band_domain), Some(expected), "Band {band}, {name}");
        }
        let roundtrip = directory.path().join(format!("roundtrip_{band}.tif"));
        assemble_band_cogs(std::slice::from_ref(input), &roundtrip, &mut |_| {})?;
        assert_native_payloads_and_decodes(std::slice::from_ref(input), &roundtrip)?;
        roundtrip_inputs.push(roundtrip);
    }
    let roundtrip_output = directory.path().join("roundtrip_assembled.tif");
    assemble_band_cogs(&roundtrip_inputs, &roundtrip_output, &mut |_| {})?;
    let roundtrip_dataset = Dataset::open(&roundtrip_output)?;
    for &domain in &domains {
        assert_eq!(
            sorted_metadata_domain(&roundtrip_dataset, domain),
            sorted_metadata_domain(&output_dataset, domain)
        );
        for band in 1..=inputs.len() {
            let expected = output_dataset.rasterband(band)?;
            let actual = roundtrip_dataset.rasterband(band)?;
            assert_eq!(actual.description()?, expected.description()?);
            assert_eq!(actual.scale(), expected.scale());
            assert_eq!(actual.offset(), expected.offset());
            assert_eq!(sorted_metadata_domain(&actual, domain), sorted_metadata_domain(&expected, domain));
        }
    }
    assert_native_payloads_and_decodes(&inputs, &roundtrip_output)?;
    assert_native_payloads_and_decodes(&inputs, &output)
}

fn assert_rejected_without_replacing(inputs: &[PathBuf], output: &Path, message: &str) -> Result<()> {
    fs::write(output, SENTINEL)?;
    let error = assemble_band_cogs(inputs, output, &mut |_| {}).expect_err("Incompatible assembly must fail");
    assert!(error.to_string().contains(message), "Expected {message:?}, got {error}");
    assert_eq!(
        fs::read(output)?,
        SENTINEL,
        "Validation failure must preserve an existing destination"
    );
    let absent = output.with_extension("absent.tif");
    assert!(!absent.exists());
    assert!(assemble_band_cogs(inputs, &absent, &mut |_| {}).is_err());
    assert!(!absent.exists(), "Validation failure must not create a destination");
    Ok(())
}

#[test]
fn rejects_conflicting_compression_predictor_and_sample_type_before_replacing_destination() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let georef = source_georeference(1, 131);
    let pixels = vec![17; georef.raster_size().cell_count()];
    let reference = stage_band(directory.path(), "reference", &georef, &pixels, single_level_options(), |_| Ok(()))?;
    for (name, variant) in [
        (
            "compression",
            CogCreationOptions {
                compression: None,
                predictor: None,
                ..single_level_options()
            },
        ),
        (
            "predictor",
            CogCreationOptions {
                predictor: None,
                ..single_level_options()
            },
        ),
        (
            "sample_type",
            CogCreationOptions {
                output_data_type: Some(ArrayDataType::Uint16),
                ..single_level_options()
            },
        ),
    ] {
        let incompatible = stage_band(directory.path(), name, &georef, &pixels, variant, |_| Ok(()))?;
        assert_rejected_without_replacing(
            &[reference.clone(), incompatible],
            &directory.path().join(format!("{name}_output.tif")),
            "incompatible data encoding",
        )?;
    }
    Ok(())
}

#[test]
fn rejects_conflicting_georeferencing_before_replacing_destination() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let mut inputs = Vec::new();
    for x in [131, 132] {
        let georef = source_georeference(1, x);
        inputs.push(stage_band(
            directory.path(),
            &format!("origin_{x}"),
            &georef,
            &vec![17; georef.raster_size().cell_count()],
            single_level_options(),
            |_| Ok(()),
        )?);
    }
    assert_rejected_without_replacing(&inputs, &directory.path().join("output.tif"), "incompatible georeferencing")
}

#[test]
fn rejects_conflicting_global_metadata_before_replacing_destination() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let georef = source_georeference(1, 131);
    let pixels = vec![17; georef.raster_size().cell_count()];
    let mut inputs = Vec::new();
    for value in ["first", "second"] {
        inputs.push(stage_band(
            directory.path(),
            value,
            &georef,
            &pixels,
            single_level_options(),
            |dataset| {
                dataset.set_metadata_item("CUSTOM_GLOBAL", value, "")?;
                Ok(())
            },
        )?);
    }
    assert_rejected_without_replacing(
        &inputs,
        &directory.path().join("output.tif"),
        "conflicting dataset-level GDAL metadata",
    )
}

#[test]
fn rejects_duplicate_globals_even_when_the_values_are_identical() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let georef = source_georeference(1, 131);
    let pixels = vec![17; georef.raster_size().cell_count()];
    for (index, second_value) in ["first", "second"].into_iter().enumerate() {
        let dataset = create_in_memory_dataset_with_data(&georef, &pixels)?;
        let mut metadata = CslStringList::new();
        metadata.add_string("DUPLICATE_GLOBAL=first")?;
        metadata.add_string(&format!("DUPLICATE_GLOBAL={second_value}"))?;
        // SAFETY: GDAL copies the live string list; the dataset and empty domain are valid.
        let result = unsafe { gdal_sys::GDALSetMetadata(dataset.c_dataset(), metadata.as_ptr(), c"".as_ptr()) };
        assert_eq!(result, gdal_sys::CPLErr::CE_None);
        let input = directory.path().join(format!("duplicate_{index}.tif"));
        let creation_options = RasterCreationOptions::from_iter([
            "BLOCKSIZE=256",
            "COMPRESS=DEFLATE",
            "PREDICTOR=STANDARD",
            "OVERVIEWS=NONE",
            "TILING_SCHEME=GoogleMapsCompatible",
            "ZOOM_LEVEL=8",
        ]);
        dataset
            .create_copy(&DriverManager::get_driver_by_name("COG")?, &input, &creation_options)?
            .close()?;
        // GDAL can collapse duplicate keys when reopening metadata. The assembler
        // must reject the stored duplicates rather than accept the effective value.
        assert_rejected_without_replacing(
            &[input],
            &directory.path().join(format!("output_{index}.tif")),
            "duplicate GDAL metadata keys",
        )?;
    }
    Ok(())
}

#[test]
fn rejects_empty_inputs_and_excessive_band_counts_without_touching_destination() -> Result<()> {
    let directory = tempfile::tempdir()?;
    assert_rejected_without_replacing(&[], &directory.path().join("empty.tif"), "At least one input band COG")?;
    let inputs = vec![directory.path().join("not_opened.tif"); usize::from(u16::MAX) + 1];
    assert_rejected_without_replacing(&inputs, &directory.path().join("too_many.tif"), "SamplesPerPixel limit")
}

#[test]
fn rejects_missing_non_cog_and_multiband_sources_without_touching_destination() -> Result<()> {
    let directory = tempfile::tempdir()?;
    assert_rejected_without_replacing(
        &[directory.path().join("missing.tif")],
        &directory.path().join("missing_output.tif"),
        "",
    )?;
    let georef = source_georeference(1, 131);
    let pixels = vec![17; georef.raster_size().cell_count()];
    let dataset = create_in_memory_dataset_with_data(&georef, &pixels)?;
    let plain_tiff = directory.path().join("plain.tif");
    translate(&dataset, &plain_tiff, &["-of".into(), "GTiff".into()])?.close()?;
    assert_rejected_without_replacing(&[plain_tiff], &directory.path().join("plain_output.tif"), "not a compatible COG")?;
    let multiband = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/data/multiband_cog_interleave_tile_google_maps_compatible.tif");
    assert_rejected_without_replacing(&[multiband], &directory.path().join("multiband_output.tif"), "Input band 1")
}
