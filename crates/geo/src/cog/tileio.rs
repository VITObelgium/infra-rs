use std::io::{Read, Seek};

use inf::allocate::{self, AlignedVecUnderConstruction};
use simd_macro::simd_bounds;

use crate::{
    Array as _, ArrayInterop as _, ArrayMetadata as _, ArrayNum, Cell, Columns, DenseArray, RasterMetadata, RasterSize, Result, Rows,
    Window,
    cog::{
        Compression, HorizontalUnpredictable, Predictor, TiffChunkLocation,
        io::{parse_chunk_data_into_buffer, read_chunk},
    },
    raster::intersection::CutOut,
};

#[cfg(feature = "simd")]
const LANES: usize = inf::simd::LANES;

#[simd_bounds]
pub fn read_tile_data<T: ArrayNum + HorizontalUnpredictable>(
    cog_location: &TiffChunkLocation,
    tile_size: u32,
    nodata: Option<f64>,
    compression: Option<Compression>,
    predictor: Option<Predictor>,
    reader: &mut (impl Read + Seek),
) -> Result<DenseArray<T>> {
    if cog_location.size == 0 {
        return Ok(DenseArray::empty());
    }

    let mut cog_chunk = vec![0; cog_location.size as usize];
    read_chunk(cog_location, reader, &mut cog_chunk)?;
    parse_tile_data(tile_size, nodata, compression, predictor, None, &cog_chunk)
}

#[simd_bounds]
pub fn parse_tile_data<T: ArrayNum + HorizontalUnpredictable>(
    tile_size: u32,
    nodata: Option<f64>,
    compression: Option<Compression>,
    predictor: Option<Predictor>,
    cutout: Option<&CutOut>,
    chunk_data: &[u8],
) -> Result<DenseArray<T>> {
    let mut meta = RasterMetadata::sized_with_nodata(RasterSize::square(tile_size as i32), nodata);
    let mut tile_data = AlignedVecUnderConstruction::new(tile_size as usize * tile_size as usize);

    parse_chunk_data_into_buffer(tile_size, compression, predictor, chunk_data, unsafe { tile_data.as_slice_mut() })?;

    let mut arr = DenseArray::<T>::new_init_nodata(meta, unsafe { tile_data.assume_init() })?;

    if let Some(cutout) = cutout {
        let size = RasterSize::with_rows_cols(Rows(cutout.rows), Columns(cutout.cols));
        let window = Window::new(Cell::from_row_col(cutout.src_row_offset, cutout.src_col_offset), size);
        let cutout_data = allocate::aligned_vec_from_iter(arr.iter_window(window));

        meta = RasterMetadata::sized_with_nodata(size, nodata);
        arr = DenseArray::<T>::new(meta, cutout_data)?;
    }

    Ok(arr)
}
