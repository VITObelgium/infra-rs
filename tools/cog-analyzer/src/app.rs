//! Application state management for the COG Analyzer.

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::PathBuf;

use anyhow::{Context, anyhow};
use geo::cog::WebTilesReader;
use geo::geotiff::{BandIndex, GeoTiffMetadata, GeoTiffReader, ParseFromBufferError};
use ratatui_image::picker::Picker;

use crate::Result;
use crate::tabs::chunks::ChunksTabState;
use crate::tabs::overview::OverviewTabState;
use crate::tabs::webtiles::WebTilesTabState;

/// The main tabs available in the application.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Tab {
    #[default]
    Overview,
    RawChunks,
    WebTiles,
}

impl Tab {
    /// Get the next tab in order.
    pub fn next(self) -> Self {
        match self {
            Tab::Overview => Tab::RawChunks,
            Tab::RawChunks => Tab::WebTiles,
            Tab::WebTiles => Tab::Overview,
        }
    }

    /// Get the previous tab in order.
    pub fn previous(self) -> Self {
        match self {
            Tab::Overview => Tab::WebTiles,
            Tab::RawChunks => Tab::Overview,
            Tab::WebTiles => Tab::RawChunks,
        }
    }

    /// Get all tabs in order.
    pub fn all() -> [Tab; 3] {
        [Tab::Overview, Tab::RawChunks, Tab::WebTiles]
    }

    /// Get the display name of the tab.
    pub fn name(&self) -> &'static str {
        match self {
            Tab::Overview => "Overview",
            Tab::RawChunks => "Raw Chunks",
            Tab::WebTiles => "Web Tiles",
        }
    }
}

/// Main application state.
pub struct App {
    /// Whether the application is running.
    pub running: bool,

    /// Currently selected tab.
    pub current_tab: Tab,

    /// Path to the COG file.
    pub file_path: PathBuf,

    /// Original path or URL supplied by the user.
    pub source: String,

    /// File size in bytes.
    pub file_size: u64,

    /// COG metadata.
    pub cog_metadata: GeoTiffMetadata,

    /// `WebTiles` reader for tile access.
    pub webtiles_reader: Option<WebTilesReader>,

    /// Currently selected band (1-based index, None means all bands or single-band COG).
    pub selected_band: Option<BandIndex>,

    /// Whether this is a multiband COG.
    pub is_multiband: bool,

    /// Total number of bands.
    pub band_count: u32,

    /// Overview tab state.
    pub overview_tab: OverviewTabState,

    /// Chunks tab state.
    pub chunks_tab: ChunksTabState,

    /// Web tiles tab state.
    pub webtiles_tab: WebTilesTabState,

    /// Error message to display (if any).
    pub error_message: Option<String>,

    /// Image picker for terminal graphics protocol.
    pub image_picker: Option<Picker>,

    source_kind: SourceKind,
}

enum SourceKind {
    Local,
    Remote { url: String, size: u64 },
}

pub trait CogReader: Read + Seek {}

impl<T: Read + Seek> CogReader for T {}

struct RemoteFile {
    url: String,
    size: u64,
    position: u64,
}

impl RemoteFile {
    fn read_range(url: &str, start: u64, end: u64) -> crate::Result<Vec<u8>> {
        let range = format!(
            "bytes={start}-{}",
            end.checked_sub(1).ok_or_else(|| anyhow!("invalid empty byte range"))?
        );
        let response = reqwest::blocking::Client::new()
            .get(url)
            .header(reqwest::header::RANGE, range)
            .send()
            .with_context(|| format!("failed to fetch byte range from {url}"))?;
        if response.status() != reqwest::StatusCode::PARTIAL_CONTENT {
            return Err(anyhow!(
                "server does not support HTTP range requests (status {})",
                response.status()
            ));
        }
        let bytes = response.bytes().context("failed to read byte range response")?;
        if bytes.len() != (end - start) as usize {
            return Err(anyhow!(
                "server returned {} bytes for requested range {}-{}",
                bytes.len(),
                start,
                end - 1
            ));
        }
        Ok(bytes.to_vec())
    }

    fn new(url: String) -> crate::Result<Self> {
        let response = reqwest::blocking::Client::new()
            .get(&url)
            .header(reqwest::header::RANGE, "bytes=0-0")
            .send()
            .with_context(|| format!("failed to request COG size from {url}"))?;
        if response.status() != reqwest::StatusCode::PARTIAL_CONTENT {
            return Err(anyhow!(
                "server does not support HTTP range requests (status {})",
                response.status()
            ));
        }
        let size = response
            .headers()
            .get(reqwest::header::CONTENT_RANGE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.rsplit_once('/'))
            .and_then(|(_, size)| size.parse().ok())
            .or_else(|| response.content_length())
            .ok_or_else(|| anyhow!("server did not provide the COG size in Content-Range or Content-Length"))?;
        Ok(Self { url, size, position: 0 })
    }

    fn metadata(&self) -> crate::Result<GeoTiffMetadata> {
        let mut size = 16 * 1024u64;
        loop {
            let end = size.min(self.size);
            match GeoTiffMetadata::from_buffer(Self::read_range(&self.url, 0, end)?) {
                Ok(metadata) => return Ok(metadata),
                Err(ParseFromBufferError::BufferTooSmall(_)) if end < self.size => {
                    size = (size * 2).min(self.size);
                }
                Err(ParseFromBufferError::BufferTooSmall(_)) => {
                    return Err(anyhow!("COG metadata exceeds the remote file size"));
                }
                Err(ParseFromBufferError::Error(error)) => return Err(error.into()),
            }
        }
    }
}

impl Read for RemoteFile {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if buffer.is_empty() || self.position >= self.size {
            return Ok(0);
        }
        let end = self.position.saturating_add(buffer.len() as u64).min(self.size);
        let bytes = Self::read_range(&self.url, self.position, end).map_err(io::Error::other)?;
        buffer[..bytes.len()].copy_from_slice(&bytes);
        self.position += bytes.len() as u64;
        Ok(bytes.len())
    }
}

impl Seek for RemoteFile {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        let new_position = match position {
            SeekFrom::Start(offset) => offset,
            SeekFrom::Current(offset) => self
                .position
                .checked_add_signed(offset)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "seek before start"))?,
            SeekFrom::End(offset) => self
                .size
                .checked_add_signed(offset)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "seek before start"))?,
        };
        self.position = new_position;
        Ok(new_position)
    }
}

impl App {
    /// Create a new application instance from a local path or HTTP(S) URL.
    pub fn new(input: PathBuf) -> Result<Self> {
        let source = input.to_string_lossy().into_owned();
        let (file_size, cog_metadata, source_kind) = if source.starts_with("http://") || source.starts_with("https://") {
            let remote = RemoteFile::new(source.clone())?;
            let metadata = remote.metadata()?;
            (
                remote.size,
                metadata,
                SourceKind::Remote {
                    url: source.clone(),
                    size: remote.size,
                },
            )
        } else {
            let file_size = std::fs::metadata(&input)?.len();
            let metadata = GeoTiffReader::from_file(&input)?.metadata().clone();
            (file_size, metadata, SourceKind::Local)
        };

        // Determine if multiband
        let band_count = cog_metadata.band_count;
        let is_multiband = band_count > 1;

        // Set initial band selection
        let selected_band = if is_multiband { Some(geo::geotiff::FIRST_BAND) } else { None };

        // Try to create WebTilesReader
        let webtiles_reader = match WebTilesReader::new(cog_metadata.clone()) {
            Ok(reader) => Some(reader),
            Err(e) => {
                log::warn!("Failed to create WebTilesReader: {}", e);
                None
            }
        };

        // Initialize tab states
        let overview_count = cog_metadata.overviews.len();
        let chunks_tab = ChunksTabState::new(overview_count);

        let webtiles_tab = if let Some(ref reader) = webtiles_reader {
            WebTilesTabState::new(reader.tile_info().min_zoom, reader.tile_info().max_zoom, band_count)
        } else {
            WebTilesTabState::default()
        };

        Ok(Self {
            running: true,
            current_tab: Tab::Overview,
            file_path: input,
            source,
            file_size,
            cog_metadata,
            webtiles_reader,
            selected_band,
            is_multiband,
            band_count,
            overview_tab: OverviewTabState::default(),
            chunks_tab,
            webtiles_tab,
            error_message: None,
            image_picker: None,
            source_kind,
        })
    }

    /// Initialize the image picker by querying the terminal.
    /// This should be called after the terminal is in raw mode.
    ///
    /// If `force_halfblocks` is true, skip protocol detection and use Unicode halfblocks.
    pub fn init_image_picker(&mut self, force_halfblocks: bool) {
        if force_halfblocks {
            self.image_picker = Some(Picker::halfblocks());
            return;
        }

        match Picker::from_query_stdio() {
            Ok(picker) => {
                self.image_picker = Some(picker);
            }
            Err(e) => {
                log::warn!("Failed to query terminal for graphics protocol: {}, falling back to halfblocks", e);
                self.image_picker = Some(Picker::halfblocks());
            }
        }
    }

    /// Switch to the next tab.
    pub fn next_tab(&mut self) {
        self.current_tab = self.current_tab.next();
    }

    /// Switch to the previous tab.
    pub fn previous_tab(&mut self) {
        self.current_tab = self.current_tab.previous();
    }

    /// Switch to the next band.
    pub fn next_band(&mut self) {
        if !self.is_multiband {
            return;
        }

        if let Some(band) = self.selected_band {
            let next = band.get() + 1;
            if next <= self.band_count as usize {
                self.selected_band = BandIndex::new(next);
                // Clear cached chunk data when band changes
                self.chunks_tab.clear_chunk_data();
                self.webtiles_tab.clear_tile_data();
            }
        }
    }

    /// Switch to the previous band.
    pub fn previous_band(&mut self) {
        if !self.is_multiband {
            return;
        }

        if let Some(band) = self.selected_band {
            let prev = band.get().saturating_sub(1);
            if prev >= 1 {
                self.selected_band = BandIndex::new(prev);
                // Clear cached chunk data when band changes
                self.chunks_tab.clear_chunk_data();
                self.webtiles_tab.clear_tile_data();
            }
        }
    }

    /// Get the GDAL description for a 1-based band index.
    pub fn band_name(&self, band: usize) -> Option<&str> {
        let sample = u32::try_from(band.checked_sub(1)?).ok()?;
        self.cog_metadata
            .band_metadata
            .iter()
            .find(|metadata| metadata.sample == sample)
            .and_then(|metadata| metadata.description.as_deref())
    }

    /// Get a display label for a 1-based band index.
    pub fn band_display(&self, band: usize) -> String {
        match self.band_name(band) {
            Some(name) => format!("Band {band} ({name})"),
            None => format!("Band {band}"),
        }
    }

    /// Get the current band index (1-based) and name, if available, for display.
    pub fn current_band_display(&self) -> String {
        match self.selected_band {
            Some(band) => format!("{} of {}", self.band_display(band.get()), self.band_count),
            None => self.band_display(1),
        }
    }

    /// Get the currently selected band index for reading data.
    pub fn get_band_index(&self) -> BandIndex {
        self.selected_band.unwrap_or(geo::geotiff::FIRST_BAND)
    }

    /// Set an error message.
    pub fn set_error(&mut self, message: String) {
        self.error_message = Some(message);
    }

    /// Clear the error message.
    pub fn clear_error(&mut self) {
        self.error_message = None;
    }

    /// Quit the application.
    pub fn quit(&mut self) {
        self.running = false;
    }

    /// Open the COG file for reading.
    pub fn open_file(&self) -> Result<Box<dyn CogReader>> {
        match &self.source_kind {
            SourceKind::Local => Ok(Box::new(File::open(&self.file_path)?)),
            SourceKind::Remote { url, size } => Ok(Box::new(RemoteFile {
                url: url.clone(),
                size: *size,
                position: 0,
            })),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::net::TcpListener;
    use std::path::PathBuf;
    use std::sync::mpsc;
    use std::thread;

    use super::App;

    #[test]
    fn loads_cog_from_http_url() {
        let cog = include_bytes!("../../../tests/data/multiband_cog.tif");
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let address = listener.local_addr().expect("get test server address");
        let (ranges, receiver) = mpsc::channel();
        let server = thread::spawn(move || {
            for _ in 0..3 {
                let (mut stream, _) = listener.accept().expect("accept test request");
                let mut request = Vec::new();
                let mut byte = [0; 1];
                while !request.ends_with(b"\r\n\r\n") {
                    stream.read_exact(&mut byte).expect("read test request");
                    request.push(byte[0]);
                }
                let request = String::from_utf8(request).expect("parse test request");
                let range = request
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("range").then(|| value.trim().strip_prefix("bytes="))?
                    })
                    .expect("range header");
                let (start, end) = range.split_once('-').expect("parse range");
                let start: usize = start.parse().expect("parse range start");
                let end: usize = end.parse().expect("parse range end");
                let body = &cog[start..=end];
                ranges.send((start, end)).expect("send requested range");
                let response = format!(
                    "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {}-{}/{}\r\nConnection: close\r\n\r\n",
                    body.len(),
                    start,
                    end,
                    cog.len()
                );
                stream.write_all(response.as_bytes()).expect("write response headers");
                stream.write_all(body).expect("write response body");
            }
        });

        let app = App::new(PathBuf::from(format!("http://{address}/fixture.tif"))).expect("load COG URL");
        let chunk = app.cog_metadata.overviews[0].chunk_locations[0];
        let mut reader = app.open_file().expect("open remote COG");
        reader.seek(SeekFrom::Start(chunk.offset)).expect("seek to chunk");
        let mut chunk_data = vec![0; chunk.size as usize];
        reader.read_exact(&mut chunk_data).expect("read remote chunk");

        assert_eq!(app.source, format!("http://{address}/fixture.tif"));
        assert_eq!(app.file_size, cog.len() as u64);
        let requested_ranges = [
            receiver.recv().expect("receive size range"),
            receiver.recv().expect("receive metadata range"),
            receiver.recv().expect("receive chunk range"),
        ];
        assert_eq!(requested_ranges[0], (0, 0));
        assert_eq!(requested_ranges[1].0, 0);
        assert_eq!(requested_ranges[1].1 - requested_ranges[1].0 + 1, 16 * 1024);
        assert_eq!(
            requested_ranges[2],
            (chunk.offset as usize, (chunk.offset + chunk.size - 1) as usize)
        );
        server.join().expect("join test server");
    }
}
