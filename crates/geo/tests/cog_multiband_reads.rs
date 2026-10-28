#![cfg(feature = "raster-io-geotiff")]

use std::io::{Cursor, Read, Seek, SeekFrom};

use geo::{
    Array, BandIndex, Result, Tile,
    cog::{TileSource, WebTilesReader},
    geotiff::{GeoTiffMetadata, TiffChunkLocation},
};

struct CountingReader {
    cursor: Cursor<Vec<u8>>,
    reads: usize,
    seeks: usize,
    last_start: u64,
}

impl Read for CountingReader {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.reads += 1;
        self.last_start = self.cursor.position();
        self.cursor.read(buffer)
    }
}

impl Seek for CountingReader {
    fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
        self.seeks += 1;
        self.cursor.seek(position)
    }
}

fn fixture() -> Result<(GeoTiffMetadata, CountingReader)> {
    let path =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/data/multiband_cog_interleave_tile_google_maps_compatible.tif");
    Ok((
        GeoTiffMetadata::from_file(&path)?,
        CountingReader {
            cursor: Cursor::new(std::fs::read(path)?),
            reads: 0,
            seeks: 0,
            last_start: 0,
        },
    ))
}

fn contributing_reads(source: &TileSource, bands: &std::ops::Range<usize>) -> usize {
    let selected = bands.start - 1..bands.end - 1;
    match source {
        TileSource::MultiBandAligned(locations) => usize::from(locations[selected].iter().any(|location| !location.is_sparse())),
        TileSource::MultiBandUnaligned(items) => items
            .iter()
            .filter(|(locations, _)| locations[selected.clone()].iter().any(|location| !location.is_sparse()))
            .count(),
        _ => panic!("Expected multiband source"),
    }
}

fn assert_batched_reads(zoom: i32) -> Result<()> {
    let (metadata, mut input) = fixture()?;
    let cog = WebTilesReader::new(metadata)?;
    let sources = cog.zoom_level_tile_sources(zoom).unwrap();
    let (tile, source) = sources
        .iter()
        .max_by_key(|(_, source)| contributing_reads(source, &(1..4)))
        .unwrap();
    for bands in [1..4, 2..4, 2..3] {
        input.reads = 0;
        input.seeks = 0;
        let arrays = cog.read_multi_band_tile_data(tile, bands.clone(), &mut input)?.unwrap();
        assert_eq!(arrays.len(), bands.len());
        assert_eq!(input.reads, contributing_reads(source, &bands));
        assert_eq!(input.seeks, input.reads);
        assert_matches_single_band_reads(&cog, tile, bands, arrays, &mut input)?;
    }
    Ok(())
}

fn assert_matches_single_band_reads(
    cog: &WebTilesReader,
    tile: &Tile,
    bands: std::ops::Range<usize>,
    arrays: Vec<geo::AnyDenseArray>,
    input: &mut CountingReader,
) -> Result<()> {
    for (band, array) in bands.zip(arrays) {
        let expected = cog.read_tile_data(tile, BandIndex::new(band).unwrap(), &mut *input)?.unwrap();
        assert_eq!(array, expected);
    }
    Ok(())
}

#[test]
fn aligned_tiles_read_requested_bands_in_one_range() -> Result<()> {
    assert_batched_reads(17)
}

#[test]
fn unaligned_tiles_read_once_per_contributing_native_tile() -> Result<()> {
    assert_batched_reads(15)
}

#[test]
fn sparse_bands_do_not_widen_ranges_to_offset_zero() -> Result<()> {
    let (mut metadata, mut input) = fixture()?;
    for overview in &mut metadata.overviews {
        let tiles_per_band = overview.chunk_locations.len() / metadata.band_count as usize;
        overview.chunk_locations[..tiles_per_band].fill(TiffChunkLocation { offset: 0, size: 0 });
    }
    let cog = WebTilesReader::new(metadata)?;
    let (tile, source) = cog.zoom_level_tile_sources(17).unwrap().iter().next().unwrap();
    let TileSource::MultiBandAligned(locations) = source else {
        panic!("Expected aligned source")
    };
    let arrays = cog.read_multi_band_tile_data(tile, 1..4, &mut input)?.unwrap();
    assert!(!arrays[0].cast_to::<f32>().contains_data());
    assert_eq!(input.reads, 1);
    let start = locations[1..3]
        .iter()
        .filter(|location| !location.is_sparse())
        .map(|location| location.offset)
        .min()
        .unwrap();
    let end = locations[1..3]
        .iter()
        .map(|location| location.offset + location.size)
        .max()
        .unwrap();
    assert_eq!(input.cursor.position(), end);
    assert_eq!(input.last_start, start);
    input.reads = 0;
    input.seeks = 0;
    cog.read_multi_band_tile_data(tile, 1..2, &mut input)?;
    assert_eq!((input.reads, input.seeks), (0, 0));
    let tile = cog.zoom_level_tile_sources(15).unwrap().keys().next().unwrap();
    let arrays = cog.read_multi_band_tile_data(tile, 1..2, &mut input)?.unwrap();
    assert!(!arrays[0].cast_to::<f32>().contains_data());
    assert_eq!((input.reads, input.seeks), (0, 0));
    Ok(())
}

#[test]
fn rejects_invalid_ranges_even_without_coverage() -> Result<()> {
    let (metadata, mut input) = fixture()?;
    let cog = WebTilesReader::new(metadata)?;
    let tile = Tile { z: 1, x: 0, y: 0 };
    for (start, end) in [
        (0, 2),
        (1, 1),
        (3, 2),
        (1, cog.cog_metadata().band_count as usize + 2),
        (1, usize::MAX),
    ] {
        assert!(cog.read_multi_band_tile_data(&tile, start..end, &mut input).is_err());
    }
    assert!(cog.read_multi_band_tile_data(&tile, 1..4, &mut input)?.is_none());
    assert_eq!((input.reads, input.seeks), (0, 0));
    Ok(())
}

#[test]
fn batched_reads_propagate_truncated_payloads() -> Result<()> {
    let (metadata, mut input) = fixture()?;
    let cog = WebTilesReader::new(metadata)?;
    let tile = cog.zoom_level_tile_sources(17).unwrap().keys().next().unwrap();
    input.cursor.get_mut().truncate(1);
    assert!(cog.read_multi_band_tile_data(tile, 1..4, &mut input).is_err());
    Ok(())
}
