//! Rechunk selected samples once into band-major raw storage. GDAL's per-band
//! warps then avoid repeatedly inflating large temporal NetCDF/HDF5 chunks.
use std::{
    ffi::CString,
    fs::File,
    io::{Seek, SeekFrom, Write},
    path::Path,
};

use anyhow::{Context, Result, bail, ensure};
use bytemuck::Pod;
use gdal::{
    Dataset, DatasetOptions, GdalOpenFlags,
    raster::{GdalDataType, GdalType, MDArray},
};

const BUFFER_BYTES: usize = 128 * 1024 * 1024;

#[derive(Debug)]
pub(super) struct CachePlan {
    shape: [usize; 3], // time, north/south, east/west roles
    block: [usize; 3],
    order: [usize; 3],
    bytes: usize,
    flip_rows: bool,
}

impl CachePlan {
    fn new(sizes: &[usize], chunks: &[usize], order: &[usize], bytes: usize, flip_rows: bool) -> Result<Self> {
        ensure!(
            sizes.len() == 3 && order.len() == 3,
            "NetCDF staging needs a three-dimensional variable"
        );
        let order = [order[0], order[1], order[2]];
        let shape = order.map(|axis| sizes[axis]);
        ensure!(
            shape.iter().all(|&size| size > 0) && bytes > 0 && bytes <= BUFFER_BYTES / 2,
            "Invalid NetCDF staging dimensions or element size"
        );
        let chunk = order.map(|axis| chunks.get(axis).copied().unwrap_or(1).max(1).min(sizes[axis]));
        let mut block = [chunk[0], chunk[1], shape[2]];
        let spatial_limit = BUFFER_BYTES / block[0].saturating_add(1).saturating_mul(block[1]).saturating_mul(bytes).max(1);
        if spatial_limit < block[2] {
            block[2] = (spatial_limit / chunk[2]).max(1).saturating_mul(chunk[2]).min(shape[2]);
        }
        while working_bytes(block, bytes) > BUFFER_BYTES {
            let axis = (0..3).max_by_key(|&axis| block[axis]).context("Missing staging axis")?;
            block[axis] = (block[axis] / 2).max(1);
        }
        let time_limit = (BUFFER_BYTES / block[1].saturating_mul(block[2]).saturating_mul(bytes).max(1))
            .saturating_sub(1)
            .max(1);
        block[0] = if time_limit >= chunk[0] {
            (time_limit / chunk[0] * chunk[0]).min(shape[0])
        } else {
            block[0].min(time_limit).max(1)
        };
        if block[0] == shape[0] {
            let rows = BUFFER_BYTES / block[0].saturating_add(1).saturating_mul(block[2]).saturating_mul(bytes).max(1);
            if rows >= chunk[1] {
                block[1] = (rows / chunk[1] * chunk[1]).min(shape[1]);
            }
        }
        let plan = Self {
            shape,
            block,
            order,
            bytes,
            flip_rows,
        };
        plan.total_bytes()?;
        Ok(plan)
    }

    fn total_bytes(&self) -> Result<u64> {
        self.shape.iter().try_fold(self.bytes as u64, |total, size| {
            total.checked_mul(*size as u64).context("NetCDF staging file is too large")
        })
    }

    fn source_order(&self, values: [usize; 3]) -> Vec<usize> {
        let mut result = vec![0; 3];
        for axis in 0..3 {
            result[self.order[axis]] = values[axis];
        }
        result
    }

    fn origins(&self) -> impl Iterator<Item = [usize; 3]> + '_ {
        (0..self.shape[0]).step_by(self.block[0]).flat_map(move |time| {
            (0..self.shape[1])
                .step_by(self.block[1])
                .flat_map(move |row| (0..self.shape[2]).step_by(self.block[2]).map(move |column| [time, row, column]))
        })
    }
}

fn working_bytes(block: [usize; 3], bytes: usize) -> usize {
    block[0]
        .saturating_add(1)
        .saturating_mul(block[1])
        .saturating_mul(block[2])
        .saturating_mul(bytes)
}

pub(super) fn stage(array: &MDArray<'_>, source: &Path, variable: &str, output: &Path, order: &[usize], flip_rows: bool) -> Result<()> {
    let sizes = array.dimensions()?.iter().map(|dim| dim.size()).collect::<Vec<_>>();
    let chunks = chunk_shape(source, variable)?;
    let data_type = GdalDataType::try_from(array.datatype().numeric_datatype())?;
    ensure!(data_type != GdalDataType::Unknown, "Cannot stage unknown NetCDF sample type");
    let plan = CachePlan::new(&sizes, &chunks, order, data_type.bytes() as usize, flip_rows)?;
    match data_type {
        GdalDataType::Float32 => stage_typed::<f32>(array, output, &plan),
        GdalDataType::Float64 => stage_typed::<f64>(array, output, &plan),
        integer => stage_integer(array, output, &plan, integer),
    }
}

fn stage_integer(array: &MDArray<'_>, output: &Path, plan: &CachePlan, data_type: GdalDataType) -> Result<()> {
    match data_type {
        GdalDataType::Int8 => stage_typed::<i8>(array, output, plan),
        GdalDataType::UInt8 => stage_typed::<u8>(array, output, plan),
        GdalDataType::Int16 => stage_typed::<i16>(array, output, plan),
        GdalDataType::UInt16 => stage_typed::<u16>(array, output, plan),
        GdalDataType::Int32 => stage_typed::<i32>(array, output, plan),
        GdalDataType::UInt32 => stage_typed::<u32>(array, output, plan),
        GdalDataType::Int64 => stage_typed::<i64>(array, output, plan),
        GdalDataType::UInt64 => stage_typed::<u64>(array, output, plan),
        other => bail!("Expected integer NetCDF samples, got {other}"),
    }
}

fn stage_typed<T: GdalType + Pod + Default>(array: &MDArray<'_>, output: &Path, plan: &CachePlan) -> Result<()> {
    let mut file = File::create(output)?;
    file.set_len(plan.total_bytes()?)?;
    let mut buffer = vec![T::default(); plan.block.iter().product()];
    let mut frame = vec![T::default(); plan.block[1] * plan.block[2]];
    for origin in plan.origins() {
        cache_block(array, &mut file, plan, origin, &mut buffer, &mut frame)?;
    }
    file.flush()?;
    Ok(())
}

fn cache_block<T: GdalType + Pod>(
    array: &MDArray<'_>,
    file: &mut File,
    plan: &CachePlan,
    origin: [usize; 3],
    buffer: &mut [T],
    frame: &mut [T],
) -> Result<()> {
    let count = std::array::from_fn(|axis| plan.block[axis].min(plan.shape[axis] - origin[axis]));
    let source_count = plan.source_order(count);
    let source_start = plan.source_order(origin).into_iter().map(|value| value as u64).collect();
    array.read_into_slice(buffer, source_start, source_count.clone())?;
    let source_strides = [source_count[1] * source_count[2], source_count[2], 1];
    let strides = plan.order.map(|axis| source_strides[axis]);
    for time in 0..count[0] {
        unpack_frame(buffer, frame, time, count, strides, plan.flip_rows);
        write_frame(file, plan, origin, count, time, &frame[..count[1] * count[2]])?;
    }
    Ok(())
}

fn unpack_frame<T: Copy>(source: &[T], frame: &mut [T], time: usize, count: [usize; 3], strides: [usize; 3], flip_rows: bool) {
    for row in 0..count[1] {
        let source_row = if flip_rows { count[1] - row - 1 } else { row };
        let start = time * strides[0] + source_row * strides[1];
        let destination = &mut frame[row * count[2]..(row + 1) * count[2]];
        if strides[2] == 1 {
            destination.copy_from_slice(&source[start..start + count[2]]);
        } else {
            for (column, value) in destination.iter_mut().enumerate() {
                *value = source[start + column * strides[2]];
            }
        }
    }
}

fn write_frame<T: Pod>(file: &mut File, plan: &CachePlan, origin: [usize; 3], count: [usize; 3], time: usize, frame: &[T]) -> Result<()> {
    let row = if plan.flip_rows {
        plan.shape[1] - origin[1] - count[1]
    } else {
        origin[1]
    };
    let offset =
        (((origin[0] + time) as u64 * plan.shape[1] as u64 + row as u64) * plan.shape[2] as u64 + origin[2] as u64) * plan.bytes as u64;
    if count[2] == plan.shape[2] {
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(bytemuck::cast_slice(frame))?;
    } else {
        for row in 0..count[1] {
            file.seek(SeekFrom::Start(offset + row as u64 * plan.shape[2] as u64 * plan.bytes as u64))?;
            file.write_all(bytemuck::cast_slice(&frame[row * count[2]..(row + 1) * count[2]]))?;
        }
    }
    Ok(())
}

/// gdal-rs doesn't expose block sizes yet; query metadata only through GDAL's C API.
fn chunk_shape(path: &Path, variable: &str) -> Result<Vec<usize>> {
    let dataset = Dataset::open_ex(
        path,
        DatasetOptions {
            open_flags: GdalOpenFlags::GDAL_OF_MULTIDIM_RASTER,
            ..Default::default()
        },
    )?;
    let name = CString::new(format!("/{}", variable.trim_start_matches('/')))?;
    // SAFETY: dataset remains alive until the owned group/array handles are released.
    let group = unsafe { gdal_sys::GDALDatasetGetRootGroup(dataset.c_dataset()) };
    ensure!(!group.is_null(), "Cannot inspect NetCDF group for chunk sizes");
    let mut handles = ArrayHandles {
        _dataset: &dataset,
        group,
        array: std::ptr::null_mut(),
    };
    // SAFETY: valid owned group and NUL-terminated name; returned array is released by Drop.
    handles.array = unsafe { gdal_sys::GDALGroupOpenMDArrayFromFullname(group, name.as_ptr(), std::ptr::null_mut()) };
    ensure!(
        !handles.array.is_null(),
        "Cannot inspect NetCDF variable {variable} for chunk sizes"
    );
    let mut count = 0;
    // SAFETY: valid live array; GDAL returns an allocated array of count elements.
    let blocks = unsafe { gdal_sys::GDALMDArrayGetBlockSize(handles.array, &mut count) };
    if blocks.is_null() {
        return Ok(Vec::new());
    }
    // SAFETY: count is supplied by GDAL; copy before freeing its allocation.
    let values = unsafe { std::slice::from_raw_parts(blocks, count).to_vec() };
    // SAFETY: blocks was allocated by GDAL and must be freed with its allocator.
    unsafe { gdal_sys::VSIFree(blocks.cast()) };
    values
        .into_iter()
        .map(|value| usize::try_from(value).context("NetCDF chunk is too large"))
        .collect()
}

struct ArrayHandles<'a> {
    _dataset: &'a Dataset,
    group: gdal_sys::GDALGroupH,
    array: gdal_sys::GDALMDArrayH,
}

impl Drop for ArrayHandles<'_> {
    fn drop(&mut self) {
        // SAFETY: these handles are owned, released once, and their dataset outlives them.
        unsafe {
            if !self.array.is_null() {
                gdal_sys::GDALMDArrayRelease(self.array);
            }
            gdal_sys::GDALGroupRelease(self.group);
        }
    }
}

#[cfg(test)]
#[path = "netcdf_cache_tests.rs"]
mod tests;
