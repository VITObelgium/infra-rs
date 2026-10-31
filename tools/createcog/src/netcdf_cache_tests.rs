use super::*;

#[test]
fn lisbon_time_chunks_are_read_as_36_aligned_slabs_with_bounded_memory() -> Result<()> {
    let plan = CachePlan::new(&[8776, 301, 301], &[1463, 51, 51], &[0, 1, 2], 4, true)?;
    assert_eq!(plan.block, [1463, 51, 301]);
    assert_eq!(plan.origins().count(), 36);
    assert!(working_bytes(plan.block, plan.bytes) <= BUFFER_BYTES);
    assert_eq!(plan.total_bytes()?, 8776 * 301 * 301 * 4);
    Ok(())
}

#[test]
fn cache_roundtrips_all_axis_orders_row_directions_and_partial_blocks() -> Result<()> {
    for order in [[0, 1, 2], [0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [2, 1, 0]] {
        for flip in [false, true] {
            for block in [[1, 2, 3], [2, 3, 4]] {
                roundtrip(order, flip, block)?;
            }
        }
    }
    Ok(())
}

fn origins(plan: &CachePlan) -> Vec<[usize; 3]> {
    let mut origins = Vec::new();
    for time in (0..plan.shape[0]).step_by(plan.block[0]) {
        for row in (0..plan.shape[1]).step_by(plan.block[1]) {
            for column in (0..plan.shape[2]).step_by(plan.block[2]) {
                origins.push([time, row, column]);
            }
        }
    }
    origins
}

fn roundtrip(order: [usize; 3], flip: bool, block: [usize; 3]) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("samples.raw");
    let mut file = File::create(&path)?;
    let plan = CachePlan {
        shape: [2, 3, 4],
        block,
        order,
        bytes: 2,
        flip_rows: flip,
    };
    file.set_len(plan.total_bytes()?)?;
    let mut frame = vec![0i16; 12];
    for origin in origins(&plan) {
        let count = std::array::from_fn(|axis| block[axis].min(plan.shape[axis] - origin[axis]));
        let source = fake_block(&plan, origin, count);
        let count_source = plan.source_order(count);
        let source_strides = [count_source[1] * count_source[2], count_source[2], 1];
        let strides = order.map(|axis| source_strides[axis]);
        for time in 0..count[0] {
            unpack_frame(&source, &mut frame, time, count, strides, flip);
            write_frame(&mut file, &plan, origin, count, time, &frame[..count[1] * count[2]])?;
        }
    }
    let bytes = std::fs::read(path)?;
    assert_cached_samples(&bytes, flip);
    Ok(())
}

fn assert_cached_samples(bytes: &[u8], flip: bool) {
    let actual: Vec<_> = bytes
        .chunks_exact(2)
        .map(|value| i16::from_ne_bytes([value[0], value[1]]))
        .collect();
    for (index, value) in actual.iter().enumerate() {
        let time = index / 12;
        let row = index % 12 / 4;
        let source_row = if flip { 2 - row } else { row };
        assert_eq!(*value, (time * 100 + source_row * 10 + index % 4) as i16);
    }
}

fn fake_block(plan: &CachePlan, origin: [usize; 3], count: [usize; 3]) -> Vec<i16> {
    let source_count = plan.source_order(count);
    let strides = [source_count[1] * source_count[2], source_count[2], 1];
    let strides = plan.order.map(|axis| strides[axis]);
    let mut data = vec![0; count.iter().product()];
    for time in 0..count[0] {
        for row in 0..count[1] {
            for column in 0..count[2] {
                data[time * strides[0] + row * strides[1] + column * strides[2]] =
                    ((origin[0] + time) * 100 + (origin[1] + row) * 10 + origin[2] + column) as i16;
            }
        }
    }
    data
}
