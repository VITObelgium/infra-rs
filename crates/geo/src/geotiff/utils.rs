use std::fs::File;

use crate::{
    Array, ArrayNum, Cell, CellSize, Columns, GeoReference, RasterSize, RasterWindow, Result, Rows,
    densearrayiterators::{DenserRasterWindowIterator, DenserRasterWindowIteratorMut},
    geotiff::{GeoTiffMetadata, TiffChunkLocation, io, tileio},
    raster::intersection::intersect_georeference,
};

use simd_macro::simd_bounds;

#[cfg(feature = "simd")]
const LANES: usize = inf::simd::LANES;

macro_rules! impl_horizontal_unpredictable_for_int {
    ($($t:ty),*) => {
        $(
            paste::paste! {
                fn [<unpredict_horizontal_ $t>](data: &mut [$t], row_size: u32) {
                    for row in data.chunks_mut(row_size as usize) {
                        for i in 1..row.len() {
                            row[i] = row[i].wrapping_add(row[i - 1]);
                        }
                    }
                }
            }
        )*
    };
}

macro_rules! impl_horizontal_unpredictable_for_fp {
    ($($t:ty),*) => {
        $(
            paste::paste! {
                fn [<unpredict_horizontal_ $t>](data: &mut [$t], row_size: u32) {
                    for row in data.chunks_mut(row_size as usize) {
                        for i in 1..row.len() {
                            row[i] += row[i - 1];
                        }
                    }
                }
            }
        )*
    };
}

impl_horizontal_unpredictable_for_int!(u8, u16, u32, u64, i8, i16, i32, i64);
impl_horizontal_unpredictable_for_fp!(f32, f64);

pub fn unpredict_horizontal<T: ArrayNum + Copy>(data: &mut [T], row_size: u32) {
    // Macro based dispatch to avoid an extra trait bound on T which pollutes the entire call stack.
    match T::TYPE {
        crate::ArrayDataType::Uint8 => unpredict_horizontal_u8(bytemuck::cast_slice_mut(data), row_size),
        crate::ArrayDataType::Uint16 => unpredict_horizontal_u16(bytemuck::cast_slice_mut(data), row_size),
        crate::ArrayDataType::Uint32 => unpredict_horizontal_u32(bytemuck::cast_slice_mut(data), row_size),
        crate::ArrayDataType::Uint64 => unpredict_horizontal_u64(bytemuck::cast_slice_mut(data), row_size),
        crate::ArrayDataType::Int8 => unpredict_horizontal_i8(bytemuck::cast_slice_mut(data), row_size),
        crate::ArrayDataType::Int16 => unpredict_horizontal_i16(bytemuck::cast_slice_mut(data), row_size),
        crate::ArrayDataType::Int32 => unpredict_horizontal_i32(bytemuck::cast_slice_mut(data), row_size),
        crate::ArrayDataType::Int64 => unpredict_horizontal_i64(bytemuck::cast_slice_mut(data), row_size),
        crate::ArrayDataType::Float32 => unpredict_horizontal_f32(bytemuck::cast_slice_mut(data), row_size),
        crate::ArrayDataType::Float64 => unpredict_horizontal_f64(bytemuck::cast_slice_mut(data), row_size),
    }
}

fn decode_delta_bytes(data: &mut [u8], bytes_per_pixel: usize, row_size: u32) {
    unpredict_horizontal_u8(data, bytes_per_pixel as u32 * row_size);
}

pub fn unpredict_fp32(data: &mut [f32], row_size: u32) {
    let mut bytes: Vec<u8> = bytemuck::cast_slice(data).to_vec();

    debug_assert_eq!(bytes.len() % row_size as usize, 0);
    decode_delta_bytes(&mut bytes, std::mem::size_of::<f32>(), row_size);

    let tile_size = row_size as usize;
    for (row_nr, row) in bytes.chunks_mut(std::mem::size_of::<f32>() * tile_size).enumerate() {
        for i in 0..tile_size {
            data[row_nr * tile_size + i] = f32::from_be_bytes([row[i], row[tile_size + i], row[tile_size * 2 + i], row[tile_size * 3 + i]]);
        }
    }
}

pub fn unpredict_fp64(data: &mut [f64], row_size: u32) {
    let mut bytes: Vec<u8> = bytemuck::cast_slice(data).to_vec();
    debug_assert_eq!(bytes.len() % row_size as usize, 0);
    decode_delta_bytes(&mut bytes, std::mem::size_of::<f64>(), row_size);

    let tile_size = row_size as usize;
    for (row_nr, row) in bytes.chunks_mut(std::mem::size_of::<f64>() * tile_size).enumerate() {
        for i in 0..tile_size {
            data[row_nr * tile_size + i] = f64::from_be_bytes([
                row[i],
                row[tile_size + i],
                row[tile_size * 2 + i],
                row[tile_size * 3 + i],
                row[tile_size * 4 + i],
                row[tile_size * 5 + i],
                row[tile_size * 6 + i],
                row[tile_size * 7 + i],
            ]);
        }
    }
}

pub fn change_georef_cell_size(geo_reference: &GeoReference, cell_size: CellSize) -> GeoReference {
    let mut result = geo_reference.clone();
    let x_factor = cell_size.x() / geo_reference.cell_size_x();
    let y_factor = cell_size.y() / geo_reference.cell_size_y();
    result.set_cell_size(cell_size);

    let raster_size = geo_reference.raster_size();
    let new_rows = Rows((raster_size.rows.count() as f64 * y_factor).round() as i32);
    let new_cols = Columns((raster_size.cols.count() as f64 * x_factor).round() as i32);
    result.set_rows(new_rows);
    result.set_columns(new_cols);

    result
}

#[simd_bounds]
pub fn merge_tile_chunks_into_buffer<T: ArrayNum>(
    meta: &GeoTiffMetadata,
    geo_reference: &GeoReference, // The georeference of the provided buffer
    tile_size: u32,
    tile_sources: &[(TiffChunkLocation, GeoReference)],
    tiff_file: &mut File,
    buffer: &mut [T],
) -> Result<()> {
    let nodata = geo_reference.nodata();

    for (cog_location, chunk_geo_reference) in tile_sources {
        if cog_location.is_sparse() {
            continue; // Skip sparse tiles, they are already filled with nodata
        }

        let tile_cutout = tileio::read_tile_data::<T>(cog_location, tile_size, nodata, meta.compression, meta.predictor, tiff_file)?;
        let cutout = intersect_georeference(chunk_geo_reference, geo_reference)?;

        let dest_window = RasterWindow::new(
            Cell::from_row_col(cutout.dst_row_offset, cutout.dst_col_offset),
            RasterSize::with_rows_cols(Rows(cutout.rows), Columns(cutout.cols)),
        );

        let src_window = RasterWindow::new(
            Cell::from_row_col(cutout.src_row_offset, cutout.src_col_offset),
            RasterSize::with_rows_cols(Rows(cutout.rows), Columns(cutout.cols)),
        );

        let dest_iterator = DenserRasterWindowIteratorMut::from_buffer(buffer, geo_reference.clone(), dest_window);
        for (dest, source) in dest_iterator.zip(tile_cutout.iter_window(src_window)) {
            *dest = source;
        }
    }

    Ok(())
}

#[simd_bounds]
pub fn merge_strip_chunks_into_buffer<T: ArrayNum>(
    meta: &GeoTiffMetadata,
    geo_reference: &GeoReference, // The georeference of the provided buffer
    rows_per_strip: u32,
    tile_sources: &[(TiffChunkLocation, GeoReference, bool)],
    tiff_file: &mut File,
    buffer: &mut [T],
) -> Result<()> {
    let cols_in_source_raster = meta.geo_reference.raster_size().cols.count() as usize;
    let mut strip_buffer = vec![T::zero(); rows_per_strip as usize * cols_in_source_raster];

    for (chunk_location, chunk_geo_reference, final_strip_in_file) in tile_sources {
        if chunk_location.is_sparse() {
            continue; // Skip sparse chunks, they are already filled with nodata
        }

        let cutout = intersect_georeference(chunk_geo_reference, geo_reference)?;
        let strip_length = if *final_strip_in_file {
            (cutout.rows as usize) * cols_in_source_raster
        } else {
            strip_buffer.len()
        };

        if strip_length == 0 {
            continue;
        }

        io::read_chunk_data_into_buffer(
            chunk_location,
            rows_per_strip,
            geo_reference.nodata(),
            meta.compression,
            meta.predictor,
            tiff_file,
            &mut strip_buffer[..strip_length],
        )?;

        let dest_window = RasterWindow::new(
            Cell::from_row_col(cutout.dst_row_offset, cutout.dst_col_offset),
            RasterSize::with_rows_cols(Rows(cutout.rows), Columns(cutout.cols)),
        );

        let src_window = RasterWindow::new(
            Cell::from_row_col(cutout.src_row_offset, cutout.src_col_offset),
            RasterSize::with_rows_cols(Rows(cutout.rows), Columns(cutout.cols)),
        );

        let dest_iterator = DenserRasterWindowIteratorMut::from_buffer(buffer, geo_reference.clone(), dest_window);
        let src_iterator = DenserRasterWindowIterator::from_buffer(&strip_buffer, chunk_geo_reference, src_window);
        for (dest, source) in dest_iterator.zip(src_iterator) {
            *dest = source;
        }
    }

    Ok(())
}
