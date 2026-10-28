use std::{cell::Cell, rc::Rc};

use super::*;
use crate::{Array as _, RasterSize};

#[derive(Default)]
struct VirtualReader {
    position: u64,
    reads: Vec<Range<u64>>,
    count: Rc<Cell<usize>>,
}

impl Read for VirtualReader {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        assert!(buffer.len() <= MAX_READ_BYTES as usize);
        buffer.fill((self.position % 251) as u8);
        let end = self.position + buffer.len() as u64;
        self.reads.push(self.position..end);
        self.position = end;
        self.count.set(self.count.get() + 1);
        Ok(buffer.len())
    }
}

impl Seek for VirtualReader {
    fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
        let SeekFrom::Start(position) = position else {
            return Err(std::io::Error::other("Expected absolute seek"));
        };
        self.position = position;
        Ok(position)
    }
}

fn distant_bands() -> Vec<TiffChunkLocation> {
    (0..20)
        .map(|index| TiffChunkLocation {
            offset: 128 + index * 1024 * 1024 * 1024,
            size: 16,
        })
        .collect()
}

#[test]
fn twenty_bands_gigabytes_apart_only_allocate_their_payloads() -> Result<()> {
    let locations = distant_bands();
    let mut reader = VirtualReader::default();
    let buffer = BandBuffer::read(&locations, &mut reader, &|| false)?;
    assert_eq!(reader.reads.len(), 20);
    assert_eq!(buffer.groups.iter().map(Vec::len).sum::<usize>(), 20 * 16);
    for (index, location) in locations.iter().enumerate() {
        assert_eq!(reader.reads[index], location.offset..location.offset + location.size);
        assert_eq!(buffer.chunk(index), vec![(location.offset % 251) as u8; 16]);
    }
    Ok(())
}

#[test]
fn nearby_chunks_coalesce_and_preserve_band_order_and_sparse_slots() -> Result<()> {
    let locations = [
        TiffChunkLocation { offset: 2064, size: 1024 },
        TiffChunkLocation { offset: 0, size: 0 },
        TiffChunkLocation { offset: 1024, size: 1024 },
    ];
    let mut reader = VirtualReader::default();
    let buffer = BandBuffer::read(&locations, &mut reader, &|| false)?;
    assert_eq!(reader.reads.len(), 1);
    assert_eq!(reader.reads[0], 1024..3088);
    assert_eq!(buffer.slices, [(0, 1040..2064), (0, 0..0), (0, 0..1024)]);
    assert!(buffer.chunk(1).is_empty());
    Ok(())
}

#[test]
fn coalescing_limits_both_gap_size_and_read_amplification() -> Result<()> {
    for (size, gap) in [(1, 4096), (1024 * 1024, MAX_GAP_BYTES + 1)] {
        let locations = [
            TiffChunkLocation { offset: 128, size },
            TiffChunkLocation {
                offset: 128 + size + gap,
                size,
            },
        ];
        let groups = read_groups(&locations)?;
        assert_eq!(groups.len(), 2);
        assert!(groups.iter().all(|group| group.range.end - group.range.start == size));
    }
    Ok(())
}

#[test]
fn contiguous_chunks_split_at_the_group_byte_cap() -> Result<()> {
    let locations = (0..20)
        .map(|index| TiffChunkLocation {
            offset: 128 + index * 1024 * 1024,
            size: 1024 * 1024,
        })
        .collect::<Vec<_>>();
    let groups = read_groups(&locations)?;
    assert_eq!(groups.len(), 3);
    assert!(groups.iter().all(|group| group.range.end - group.range.start <= MAX_READ_BYTES));
    Ok(())
}

#[test]
fn cancelled_before_reading_does_not_touch_the_reader() {
    let mut reader = VirtualReader::default();
    let result = BandBuffer::read(&distant_bands(), &mut reader, &|| true);
    assert!(matches!(result, Err(Error::Cancelled)));
    assert!(reader.reads.is_empty());
    assert_eq!(reader.position, 0);
}

#[test]
fn cancellation_between_distant_groups_stops_after_one_read() {
    let mut reader = VirtualReader::default();
    let count = Rc::clone(&reader.count);
    let result = BandBuffer::read(&distant_bands(), &mut reader, &|| count.get() >= 1);
    assert!(matches!(result, Err(Error::Cancelled)));
    assert_eq!(reader.reads.len(), 1);
    assert_eq!(reader.reads[0], 128..144);
}

#[test]
fn oversized_single_chunk_reads_in_cancellable_steps() {
    let mut reader = VirtualReader::default();
    let count = Rc::clone(&reader.count);
    let locations = [TiffChunkLocation {
        offset: 128,
        size: MAX_READ_BYTES + 32,
    }];
    let result = BandBuffer::read(&locations, &mut reader, &|| count.get() >= 1);
    assert!(matches!(result, Err(Error::Cancelled)));
    assert_eq!(reader.reads.len(), 1);
    assert_eq!(reader.reads[0], 128..128 + MAX_READ_BYTES);
}

#[test]
fn invalid_chunk_end_returns_error_without_reading() {
    let mut reader = VirtualReader::default();
    let locations = [TiffChunkLocation { offset: u64::MAX, size: 1 }];
    assert!(matches!(
        BandBuffer::read(&locations, &mut reader, &|| false),
        Err(Error::InvalidArgument(_))
    ));
    assert!(reader.reads.is_empty());
}

fn distant_band_reader_fixture() -> Result<(WebTilesReader, Tile)> {
    let path = crate::testutils::workspace_test_data_dir().join("multiband_cog_interleave_tile_google_maps_compatible.tif");
    let mut meta = crate::geotiff::GeoTiffMetadata::from_file(&path)?;
    let tile = Tile { z: 2, x: 1, y: 1 };
    meta.geo_reference = crate::GeoReference::from_tile(&tile, 256, 1).copy_with_nodata(Some(u8::MAX));
    meta.data_layout = crate::geotiff::ChunkDataLayout::Tiled(256);
    meta.compression = None;
    meta.predictor = None;
    meta.band_count = 20;
    meta.band_metadata.resize_with(20, Default::default);
    meta.overviews.truncate(1);
    meta.overviews[0].raster_size = RasterSize::square(256);
    meta.overviews[0].chunk_locations = distant_bands()
        .into_iter()
        .map(|location| TiffChunkLocation {
            size: 256 * 256,
            ..location
        })
        .collect();
    Ok((WebTilesReader::new(meta)?, tile))
}

#[test]
fn public_reader_decodes_distant_band_payloads_without_gap_reads() -> Result<()> {
    let (cog, tile) = distant_band_reader_fixture()?;
    let mut reader = VirtualReader::default();
    let arrays = cog
        .read_multi_band_tile_data_cancellable(&tile, 1..21, &mut reader, || false)?
        .unwrap();
    assert_eq!((arrays.len(), reader.reads.len()), (20, 20));
    assert_eq!(
        reader.reads.iter().map(|range| range.end - range.start).sum::<u64>(),
        20 * 256 * 256
    );
    for (array, location) in arrays.iter().zip(distant_bands()) {
        assert!(
            array
                .cast_to::<u8>()
                .as_slice()
                .iter()
                .all(|pixel| *pixel == (location.offset % 251) as u8)
        );
    }
    Ok(())
}

#[test]
fn public_reader_discards_output_when_cancelled_between_groups() -> Result<()> {
    let (cog, tile) = distant_band_reader_fixture()?;
    let mut reader = VirtualReader::default();
    let count = Rc::clone(&reader.count);
    let result = cog.read_multi_band_tile_data_cancellable(&tile, 1..21, &mut reader, || count.get() >= 1);
    assert!(matches!(result, Err(Error::Cancelled)));
    assert_eq!(reader.reads.len(), 1);
    Ok(())
}

#[test]
fn buffered_chunks_use_shared_parser_cancellation_between_band_decodes() -> Result<()> {
    let (cog, tile) = distant_band_reader_fixture()?;
    let source = cog.tile_source(&tile).unwrap();
    let pixels = 256 * 256;
    // The truncated second band must not be decoded after cancellation.
    let buffer = BandBuffer {
        groups: vec![vec![1; pixels], vec![1]],
        slices: vec![(0, 0..pixels), (1, 0..1)],
    };
    let chunks = buffer.chunks().collect::<Vec<_>>();
    let checks = Cell::new(0);
    let result = cog.parse_multi_band_tile_data_cancellable(1..3, source, &chunks, || {
        checks.set(checks.get() + 1);
        checks.get() == 3
    });
    assert!(matches!(result, Err(Error::Cancelled)));
    assert_eq!(checks.get(), 3);
    assert!(cog.parse_multi_band_tile_data(1..3, source, &chunks).is_err());
    Ok(())
}
