use std::{
    io::{Read, Seek, SeekFrom},
    ops::Range,
};

use super::webtiles::check_cancelled;
use super::{TileSource, WebTilesReader};
use crate::{AnyDenseArray, Error, Result, Tile, geotiff::TiffChunkLocation};

#[cfg(test)]
#[path = "tileio_tests.rs"]
mod tests;

const MAX_READ_BYTES: u64 = 8 * 1024 * 1024;
const MAX_GAP_BYTES: u64 = 64 * 1024;
const MAX_READ_AMPLIFICATION: u64 = 2;

struct ReadGroup {
    range: Range<u64>,
    payload_bytes: u64,
    chunks: Vec<(usize, Range<usize>)>,
}

impl ReadGroup {
    fn can_merge(&self, next: &Range<u64>) -> bool {
        let span = self.range.end.max(next.end) - self.range.start;
        let payload = self.payload_bytes.saturating_add(next.end - next.start);
        span <= MAX_READ_BYTES
            && next.start.saturating_sub(self.range.end) <= MAX_GAP_BYTES
            && span <= payload.saturating_mul(MAX_READ_AMPLIFICATION)
    }

    fn push(&mut self, index: usize, range: Range<u64>) -> Result<()> {
        let start = usize::try_from(range.start - self.range.start)
            .map_err(|_| Error::InvalidArgument("Multiband chunk offset is too large".into()))?;
        let end =
            usize::try_from(range.end - self.range.start).map_err(|_| Error::InvalidArgument("Multiband chunk end is too large".into()))?;
        self.payload_bytes = self.payload_bytes.saturating_add(range.end - range.start);
        self.range.end = self.range.end.max(range.end);
        self.chunks.push((index, start..end));
        Ok(())
    }
}

fn read_groups(locations: &[TiffChunkLocation]) -> Result<Vec<ReadGroup>> {
    let mut present = locations
        .iter()
        .enumerate()
        .filter(|(_, location)| !location.is_sparse())
        .collect::<Vec<_>>();
    present.sort_unstable_by_key(|(_, location)| location.offset);
    let mut groups: Vec<ReadGroup> = Vec::new();
    for (index, location) in present {
        let end = location
            .offset
            .checked_add(location.size)
            .ok_or_else(|| Error::InvalidArgument("Multiband chunk range overflows u64".into()))?;
        let range = location.offset..end;
        if !groups.last().is_some_and(|group| group.can_merge(&range)) {
            groups.push(ReadGroup {
                range: range.start..range.start,
                payload_bytes: 0,
                chunks: Vec::new(),
            });
        }
        if let Some(group) = groups.last_mut() {
            group.push(index, range)?;
        }
    }
    Ok(groups)
}

/// Compressed payloads for one native tile, without unbounded inter-band gaps.
struct BandBuffer {
    groups: Vec<Vec<u8>>,
    slices: Vec<(usize, Range<usize>)>,
}

impl BandBuffer {
    fn read(locations: &[TiffChunkLocation], reader: &mut (impl Read + Seek), cancelled: &impl Fn() -> bool) -> Result<Self> {
        let mut buffer = Self {
            groups: Vec::new(),
            slices: vec![(0, 0..0); locations.len()],
        };
        for group in read_groups(locations)? {
            check_cancelled(cancelled)?;
            let data = read_group(&group.range, reader, cancelled)?;
            let index = buffer.groups.len();
            for (band, range) in group.chunks {
                buffer.slices[band] = (index, range);
            }
            buffer.groups.push(data);
        }
        Ok(buffer)
    }

    fn chunks(&self) -> impl Iterator<Item = &[u8]> {
        (0..self.slices.len()).map(|band| self.chunk(band))
    }

    fn chunk(&self, band: usize) -> &[u8] {
        let (group, range) = &self.slices[band];
        if range.is_empty() {
            &[]
        } else {
            &self.groups[*group][range.clone()]
        }
    }
}

fn read_group(range: &Range<u64>, reader: &mut (impl Read + Seek), cancelled: &impl Fn() -> bool) -> Result<Vec<u8>> {
    let size = usize::try_from(range.end - range.start).map_err(|_| Error::InvalidArgument("Multiband chunk range is too large".into()))?;
    let mut data = vec![0; size];
    reader.seek(SeekFrom::Start(range.start))?;
    // An individually oversized chunk needs its own payload allocation, but even
    // its reads are bounded so cancellation is checked while fetching it.
    for block in data.chunks_mut(MAX_READ_BYTES as usize) {
        check_cancelled(cancelled)?;
        reader.read_exact(block)?;
        check_cancelled(cancelled)?;
    }
    Ok(data)
}

fn read_buffers(
    source: &TileSource,
    bands: &Range<usize>,
    reader: &mut (impl Read + Seek),
    cancelled: &impl Fn() -> bool,
) -> Result<Vec<BandBuffer>> {
    let selected = bands.start - 1..bands.end - 1;
    let mut read = |locations: &[TiffChunkLocation]| BandBuffer::read(locations, reader, cancelled);
    match source {
        TileSource::Aligned(location) => Ok(vec![read(std::slice::from_ref(location))?]),
        TileSource::Unaligned(items) => items.iter().map(|(location, _)| read(std::slice::from_ref(location))).collect(),
        TileSource::MultiBandAligned(locations) => Ok(vec![read(&locations[selected])?]),
        TileSource::MultiBandUnaligned(items) => items.iter().map(|(locations, _)| read(&locations[selected.clone()])).collect(),
    }
}

impl WebTilesReader {
    /// Read contiguous, 1-based, half-open bands in band order without scaling.
    ///
    /// Reads nearby compressed chunks in bounded groups, not an entire min..max
    /// span across distant BAND-interleaved payloads. Coalesced groups are at most
    /// 8 MiB, gaps at most 64 KiB and span at most twice requested payload bytes.
    /// An individually larger chunk is isolated and read in at most 8 MiB steps.
    /// Sparse bands require no reads and return nodata arrays. Returns `None`
    /// outside the native tile index (including zooms without native overviews).
    /// Decoding and tile assembly use the same parser as asynchronous/WASM reads.
    ///
    /// # Errors
    /// Returns an error for empty, reversed, zero-based or out-of-range band ranges,
    /// or if seeking, reading or decoding any contributing chunk fails.
    pub fn read_multi_band_tile_data(
        &self,
        tile: &Tile,
        bands: Range<usize>,
        reader: &mut (impl Read + Seek),
    ) -> Result<Option<Vec<AnyDenseArray>>> {
        self.read_multi_band_tile_data_cancellable(tile, bands, reader, || false)
    }

    /// Cancellable version of [`Self::read_multi_band_tile_data`].
    ///
    /// Checks before/after compressed read steps and individual band/native-chunk
    /// decodes. An in-progress reader call or individual codec decode cannot be
    /// interrupted. The callback should cheaply check a cancellation/generation flag.
    ///
    /// # Errors
    /// Returns [`Error::Cancelled`] when the callback returns true; partial output
    /// is discarded. Other errors match [`Self::read_multi_band_tile_data`].
    pub fn read_multi_band_tile_data_cancellable(
        &self,
        tile: &Tile,
        bands: Range<usize>,
        reader: &mut (impl Read + Seek),
        cancelled: impl Fn() -> bool,
    ) -> Result<Option<Vec<AnyDenseArray>>> {
        check_cancelled(&cancelled)?;
        let bands = self.normalize_band_range(bands)?;
        let Some(source) = self.tile_source(tile) else { return Ok(None) };
        let buffers = read_buffers(source, &bands, reader, &cancelled)?;
        let chunks = buffers.iter().flat_map(BandBuffer::chunks).collect::<Vec<_>>();
        let arrays = match source {
            TileSource::MultiBandAligned(_) | TileSource::MultiBandUnaligned(_) => {
                self.parse_multi_band_tile_data_cancellable(bands, source, &chunks, &cancelled)?
            }
            _ => vec![self.parse_tile_data_cancellable(source, crate::FIRST_BAND, &chunks, &cancelled)?],
        };
        check_cancelled(&cancelled)?;
        Ok(Some(arrays))
    }
}
