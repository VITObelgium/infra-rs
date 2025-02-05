use num::{Bounded, NumCast, Zero};

use crate::raster::algo::clusterutils::handle_time_cell;
use crate::{array, ArrayNum};
use crate::{
    raster::{
        algo::clusterutils::{visit_neighbour_cells, visit_neighbour_diag_cells, MARK_DONE},
        DenseRaster,
    },
    Array, ArrayCopy, Cell, DenseArray, Error, GeoReference, Result,
};

use super::clusterutils::{FiLo, MARK_BORDER, MARK_TODO};
use super::nodata;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BarrierDiagonals {
    /// Allow traversal through diagonal barriers
    Include,
    /// Don't allow traversal through diagonal barriers
    Exclude,
}

fn handle_cell_closest_target(
    delta_d: f32,
    cell: Cell,
    new_cell: Cell,
    distance_to_target: &mut impl Array<Pixel = f32>,
    closest_target: &mut impl Array,
    mark: &mut impl Array<Pixel = u8>,
    border: &mut FiLo<Cell>,
) {
    if distance_to_target[new_cell] > distance_to_target[cell] + delta_d {
        distance_to_target[new_cell] = distance_to_target[cell] + delta_d;
        closest_target[new_cell] = closest_target[cell];
        if mark[new_cell] != MARK_BORDER {
            mark[new_cell] = MARK_BORDER;
            border.push_back(new_cell);
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_sum_le_time_distance_cell(
    delta_d: f32,
    cell: Cell,
    new_cell: Cell,
    distance_to_target: &mut impl Array<Pixel = f32>,
    mark: &mut impl Array<Pixel = u8>,
    travel_time: &impl Array,
    border: &mut FiLo<Cell>,
    cells: &mut Vec<Cell>,
) {
    if travel_time.cell_is_nodata(new_cell) {
        return;
    }

    let cell_travel_time: f32 = NumCast::from(travel_time[cell]).expect("Failed to cast travel time to f32");
    let new_cell_travel_time: f32 = NumCast::from(travel_time[new_cell]).expect("Failed to cast travel time to f32");

    let alternative_dist = distance_to_target[cell] + delta_d / 2.0_f32 * (cell_travel_time + new_cell_travel_time);
    let d = &mut distance_to_target[new_cell];
    if *d > alternative_dist {
        *d = alternative_dist;
        let m = &mut mark[new_cell];
        if *m != MARK_BORDER {
            if *m == MARK_TODO {
                cells.push(new_cell);
            }
            *m = MARK_BORDER;
            border.push_back(new_cell);
        }
    }
}

fn handle_cell_value_at_closest_target(
    delta_d: f32,
    cell: Cell,
    new_cell: Cell,
    distance_to_target: &mut impl Array<Pixel = f32>,
    mark: &mut impl Array<Pixel = u8>,
    value_at_closest_target: &mut impl Array,
    border: &mut FiLo<Cell>,
) {
    if distance_to_target[new_cell] > distance_to_target[cell] + delta_d {
        distance_to_target[new_cell] = distance_to_target[cell] + delta_d;
        value_at_closest_target[new_cell] = value_at_closest_target[cell];
        if mark[new_cell] != MARK_BORDER {
            mark[new_cell] = MARK_BORDER;
            border.push_back(new_cell);
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_cell_value_at_closest_travel_target(
    delta_d: f32,
    cell: Cell,
    new_cell: Cell,
    distance_to_target: &mut impl Array<Pixel = f32>,
    value_at_closest_target: &mut impl Array,
    travel_time: &impl Array,
    mark: &mut impl Array<Pixel = u8>,
    border: &mut FiLo<Cell>,
) {
    let new_cell_travel_time: f32 = NumCast::from(travel_time[new_cell]).expect("Failed to cast travel time to f32");

    let alternative_dist = distance_to_target[cell] + delta_d * new_cell_travel_time;
    if distance_to_target[new_cell] > alternative_dist {
        distance_to_target[new_cell] = alternative_dist;
        value_at_closest_target[new_cell] = value_at_closest_target[cell];
        if mark[new_cell] != MARK_BORDER {
            mark[new_cell] = MARK_BORDER;
            border.push_back(new_cell);
        }
    }
}

fn handle_cell(
    delta_d: f32,
    cell: Cell,
    new_cell: Cell,
    distance_to_target: &mut impl Array<Pixel = f32>,
    mark: &mut impl Array<Pixel = u8>,
    border: &mut FiLo<Cell>,
) {
    if distance_to_target[new_cell] > distance_to_target[cell] + delta_d {
        distance_to_target[new_cell] = distance_to_target[cell] + delta_d;
        if mark[new_cell] != MARK_BORDER {
            mark[new_cell] = MARK_BORDER;
            border.push_back(new_cell);
        }
    }
}

fn handle_cell_with_obstacles(
    delta_d: f32,
    cell: Cell,
    new_cell: Cell,
    obstacles: &impl Array<Pixel = u8>,
    distance_to_target: &mut impl Array<Pixel = f32>,
    mark: &mut impl Array<Pixel = u8>,
    border: &mut FiLo<Cell>,
) {
    if !obstacles.cell_is_nodata(new_cell) && obstacles[new_cell] == 0 && distance_to_target[new_cell] > distance_to_target[cell] + delta_d
    {
        distance_to_target[new_cell] = distance_to_target[cell] + delta_d;
        if mark[new_cell] != MARK_BORDER {
            mark[new_cell] = MARK_BORDER;
            border.push_back(new_cell);
        }
    }
}

fn handle_cell_with_obstacles_diag(
    delta_d: f32,
    cell: Cell,
    new_cell: Cell,
    obstacles: &impl Array<Pixel = u8>,
    distance_to_target: &mut impl Array<Pixel = f32>,
    mark: &mut impl Array<Pixel = u8>,
    border: &mut FiLo<Cell>,
) {
    if obstacles.cell_is_nodata(new_cell)
        || obstacles.cell_is_nodata(Cell::from_row_col(cell.row, new_cell.col))
        || obstacles.cell_is_nodata(Cell::from_row_col(new_cell.row, cell.col))
    {
        return;
    }

    if obstacles[new_cell] == 0
        && !(obstacles[Cell::from_row_col(cell.row, new_cell.col)] != 0 && obstacles[Cell::from_row_col(new_cell.row, cell.col)] != 0)
        && distance_to_target[new_cell] > distance_to_target[cell] + delta_d
    {
        distance_to_target[new_cell] = distance_to_target[cell] + delta_d;
        if mark[new_cell] != MARK_BORDER {
            mark[new_cell] = MARK_BORDER;
            border.push_back(new_cell);
        }
    }
}

pub fn distances_up_to<RasterType>(target: &RasterType, unreachable: f32) -> RasterType::WithPixelType<f32>
where
    RasterType: Array<Pixel = u8, Metadata = GeoReference>,
    RasterType::WithPixelType<f32>: ArrayCopy<f32, RasterType>,
{
    let rows = target.rows();
    let cols = target.columns();

    let mut distance_to_target = RasterType::WithPixelType::<f32>::new_with_dimensions_of(target, unreachable);
    let mut mark = DenseRaster::<u8>::new_with_dimensions_of(target, MARK_TODO);

    let mut border = FiLo::new(rows, cols);

    for r in 0..rows.count() {
        for c in 0..cols.count() {
            let cell = Cell::from_row_col(r, c);
            if target.cell_is_nodata(cell) {
                distance_to_target.set_cell_value(cell, None);
            } else if target[cell] != 0 {
                distance_to_target[cell] = 0.0;
                mark[cell] = MARK_BORDER;
                border.push_back(cell);
            }
        }
    }

    let sqrt2 = 2.0_f32.sqrt();
    while !border.is_empty() {
        let cell = border.pop_head();
        assert_eq!(mark[cell], MARK_BORDER);
        mark[cell] = MARK_DONE;

        visit_neighbour_cells(cell, rows, cols, |neighbour| {
            handle_cell(1.0, cell, neighbour, &mut distance_to_target, &mut mark, &mut border);
        });

        visit_neighbour_diag_cells(cell, rows, cols, |neighbour| {
            handle_cell(sqrt2, cell, neighbour, &mut distance_to_target, &mut mark, &mut border);
        });
    }

    distance_to_target *= target.metadata().cell_size_x() as f32;
    distance_to_target
}

pub fn distance<RasterType>(target: &RasterType) -> RasterType::WithPixelType<f32>
where
    RasterType: Array<Pixel = u8, Metadata = GeoReference>,
    RasterType::WithPixelType<f32>: ArrayCopy<f32, RasterType>,
{
    let unreachable = f32::INFINITY;
    distances_up_to(target, unreachable)
}

pub fn distance_with_obstacles<RasTarget, RasObstacles>(
    target: &RasTarget,
    obstacles: &RasObstacles,
    diagonals: BarrierDiagonals,
) -> Result<RasTarget::WithPixelType<f32>>
where
    RasTarget: Array<Metadata = GeoReference>,
    RasObstacles: Array,
    RasTarget::WithPixelType<f32>: ArrayCopy<f32, RasTarget>,
{
    array::check_dimensions(target, obstacles)?;

    let unreachable = f32::INFINITY;

    let mut distance_to_target = RasTarget::WithPixelType::<f32>::new_with_dimensions_of(target, unreachable);
    let mut mark = DenseArray::<u8, _>::new_with_dimensions_of(target, MARK_TODO);

    let mut byte_target = DenseArray::<u8, _>::new_with_dimensions_of(target, 0);
    let mut byte_obstacles = DenseArray::<u8, _>::new_with_dimensions_of(target, 0);

    let rows = target.rows();
    let cols = target.columns();
    let mut border = FiLo::new(rows, cols);

    for r in 0..rows.count() {
        for c in 0..cols.count() {
            let cell = Cell::from_row_col(r, c);
            if target.cell_has_data(cell) && target[cell] != RasTarget::Pixel::zero() {
                byte_target[cell] = 1;
            }

            if obstacles.cell_is_nodata(cell) || obstacles[cell] != RasObstacles::Pixel::zero() {
                byte_obstacles[cell] = 1;
            }
        }
    }

    for r in 0..rows.count() {
        for c in 0..cols.count() {
            let cell = Cell::from_row_col(r, c);
            if byte_target.cell_is_nodata(cell) {
                distance_to_target[cell] = 0.0;
            } else if byte_target[cell] != 0 {
                distance_to_target[cell] = 0.0;
                mark[cell] = MARK_BORDER;
                border.push_back(cell);
            }
        }
    }

    let sqrt2 = 2.0_f32.sqrt();
    while !border.is_empty() {
        let cell = border.pop_head();
        assert_eq!(mark[cell], MARK_BORDER);
        mark[cell] = MARK_DONE;

        visit_neighbour_cells(cell, rows, cols, |neighbour| {
            handle_cell_with_obstacles(
                1.0,
                cell,
                neighbour,
                &byte_obstacles,
                &mut distance_to_target,
                &mut mark,
                &mut border,
            );
        });

        match diagonals {
            BarrierDiagonals::Include => {
                visit_neighbour_diag_cells(cell, rows, cols, |neighbour| {
                    handle_cell_with_obstacles(
                        sqrt2,
                        cell,
                        neighbour,
                        &byte_obstacles,
                        &mut distance_to_target,
                        &mut mark,
                        &mut border,
                    );
                });
            }
            BarrierDiagonals::Exclude => {
                visit_neighbour_diag_cells(cell, rows, cols, |neighbour| {
                    handle_cell_with_obstacles_diag(
                        sqrt2,
                        cell,
                        neighbour,
                        &byte_obstacles,
                        &mut distance_to_target,
                        &mut mark,
                        &mut border,
                    );
                });
            }
        }
    }

    distance_to_target *= target.metadata().cell_size_x() as f32;
    Ok(distance_to_target)
}

pub fn travel_distances_up_to<TargetRaster, RasterType>(
    target: &TargetRaster,
    travel_time: &RasterType,
    unreachable: f32,
) -> Result<RasterType::WithPixelType<f32>>
where
    TargetRaster: Array<Pixel = u8>,
    RasterType: Array,
    RasterType::WithPixelType<f32>: ArrayCopy<f32, RasterType>,
{
    if target.size() != travel_time.size() {
        return Err(Error::InvalidArgument(
            "Target raster dimensions should match travel time raster dimensions".into(),
        ));
    }

    let rows = target.rows();
    let cols = target.columns();

    let mut distance_to_target = RasterType::WithPixelType::<f32>::new_with_dimensions_of(travel_time, unreachable);
    let mut mark = DenseArray::<u8, TargetRaster::Metadata>::new_with_dimensions_of(target, MARK_TODO);

    let mut border = FiLo::new(rows, cols);

    for r in 0..rows.count() {
        for c in 0..cols.count() {
            let cell = Cell::from_row_col(r, c);
            if target.cell_is_nodata(cell) || travel_time.cell_is_nodata(cell) {
                distance_to_target.set_cell_value(cell, None);
            } else if target[cell] != 0 {
                distance_to_target[cell] = 0.0;
                mark[cell] = MARK_BORDER;
                border.push_back(cell);
            }
        }
    }

    let sqrt2 = 2.0_f32.sqrt();
    while !border.is_empty() {
        let cell = border.pop_head();
        assert_eq!(mark[cell], MARK_BORDER);
        mark[cell] = MARK_DONE;

        visit_neighbour_cells(cell, rows, cols, |neighbour| {
            handle_sum_le_time_distance_cell(
                1.0,
                cell,
                neighbour,
                &mut distance_to_target,
                &mut mark,
                travel_time,
                &mut border,
                &mut Vec::new(),
            );
        });

        visit_neighbour_diag_cells(cell, rows, cols, |neighbour| {
            handle_sum_le_time_distance_cell(
                sqrt2,
                cell,
                neighbour,
                &mut distance_to_target,
                &mut mark,
                travel_time,
                &mut border,
                &mut Vec::new(),
            );
        });
    }

    Ok(distance_to_target)
}

pub fn travel_distance<TargetRaster, RasterType>(target: &TargetRaster, travel_time: &RasterType) -> Result<RasterType::WithPixelType<f32>>
where
    TargetRaster: Array<Pixel = u8>,
    RasterType: Array,
    RasterType::WithPixelType<f32>: ArrayCopy<f32, RasterType>,
{
    let unreachable = f32::INFINITY;
    travel_distances_up_to(target, travel_time, unreachable)
}

pub fn closest_target<T, RasterType>(target: &RasterType) -> RasterType::WithPixelType<T>
where
    RasterType: Array,
    RasterType::WithPixelType<f32>: ArrayCopy<f32, RasterType>,
    RasterType::WithPixelType<T>: ArrayCopy<T, RasterType>,
    T: ArrayNum<T>,
{
    let rows = target.rows();
    let cols = target.columns();
    let unreachable = f32::INFINITY;

    let mut distance_to_target = RasterType::WithPixelType::<f32>::new_with_dimensions_of(target, unreachable);
    let mut closest_target = RasterType::WithPixelType::<T>::new_with_dimensions_of(target, T::zero());
    let mut mark = DenseArray::<u8, RasterType::Metadata>::new_with_dimensions_of(target, MARK_TODO);

    let mut border = FiLo::new(rows, cols);

    for r in 0..rows.count() {
        for c in 0..cols.count() {
            let cell = Cell::from_row_col(r, c);
            if target.cell_is_nodata(cell) {
                continue;
            }

            if target[cell] != RasterType::Pixel::zero() {
                distance_to_target[cell] = 0.0;
                closest_target[cell] = NumCast::from(target[cell]).expect("Failed to cast target to T");
                mark[cell] = MARK_BORDER;
                border.push_back(cell);
            }
        }
    }

    let sqrt2 = 2.0_f32.sqrt();
    while !border.is_empty() {
        let cell = border.pop_head();
        assert_eq!(mark[cell], MARK_BORDER);
        mark[cell] = MARK_DONE;

        visit_neighbour_cells(cell, rows, cols, |neighbour| {
            handle_cell_closest_target(
                1.0,
                cell,
                neighbour,
                &mut distance_to_target,
                &mut closest_target,
                &mut mark,
                &mut border,
            );
        });

        visit_neighbour_diag_cells(cell, rows, cols, |neighbour| {
            handle_cell_closest_target(
                sqrt2,
                cell,
                neighbour,
                &mut distance_to_target,
                &mut closest_target,
                &mut mark,
                &mut border,
            );
        });
    }

    closest_target
}

pub fn value_at_closest_target<TResult, TargetRaster, ValueRaster>(
    target: &TargetRaster,
    value: &ValueRaster,
) -> Result<ValueRaster::WithPixelType<TResult>>
where
    TResult: ArrayNum<TResult>,
    ValueRaster: Array,
    TargetRaster: Array,
    ValueRaster::WithPixelType<TResult>: ArrayCopy<TResult, ValueRaster>,
    ValueRaster::WithPixelType<f32>: ArrayCopy<f32, ValueRaster>,
{
    if target.size() != value.size() {
        return Err(Error::InvalidArgument(
            "Target raster dimensions should match value raster dimensions".into(),
        ));
    }

    let rows = target.rows();
    let cols = target.columns();
    let unreachable = (rows.count() * cols.count() + 1) as f32;

    let mut value_at_closest_target = ValueRaster::WithPixelType::<TResult>::new_with_dimensions_of(value, TResult::zero());
    let mut distance_to_target = ValueRaster::WithPixelType::<f32>::new_with_dimensions_of(value, unreachable);

    let mut mark = DenseArray::<u8, TargetRaster::Metadata>::new_with_dimensions_of(target, MARK_TODO);
    let mut border = FiLo::new(rows, cols);

    for r in 0..rows.count() {
        for c in 0..cols.count() {
            let cell = Cell::from_row_col(r, c);
            if target.cell_is_nodata(cell) {
                continue;
            }

            if target[cell] != TargetRaster::Pixel::zero() {
                distance_to_target[cell] = 0.0;

                if value.cell_is_nodata(cell) {
                    value_at_closest_target.set_cell_value(cell, None);
                } else {
                    value_at_closest_target[cell] = NumCast::from(value[cell]).expect("Failed to cast value to TResult");
                }

                mark[cell] = MARK_BORDER;
                border.push_back(cell);
            }
        }
    }

    let sqrt2 = 2.0_f32.sqrt();
    while !border.is_empty() {
        let cell = border.pop_head();
        assert_eq!(mark[cell], MARK_BORDER);
        mark[cell] = MARK_DONE;

        visit_neighbour_cells(cell, rows, cols, |neighbour| {
            handle_cell_value_at_closest_target(
                1.0,
                cell,
                neighbour,
                &mut distance_to_target,
                &mut mark,
                &mut value_at_closest_target,
                &mut border,
            );
        });

        visit_neighbour_diag_cells(cell, rows, cols, |neighbour| {
            handle_cell_value_at_closest_target(
                sqrt2,
                cell,
                neighbour,
                &mut distance_to_target,
                &mut mark,
                &mut value_at_closest_target,
                &mut border,
            );
        });
    }

    Ok(value_at_closest_target)
}

pub fn value_at_closest_travel_target<TResult, TargetRaster, TravelRaster, ValueRaster>(
    target: &TargetRaster,
    travel_times: &TravelRaster,
    value: &ValueRaster,
) -> Result<ValueRaster::WithPixelType<TResult>>
where
    TResult: ArrayNum<TResult>,
    TargetRaster: Array,
    TravelRaster: Array,
    ValueRaster: Array,
    ValueRaster::WithPixelType<TResult>: ArrayCopy<TResult, ValueRaster>,
    ValueRaster::WithPixelType<f32>: ArrayCopy<f32, ValueRaster>,
{
    if target.size() != value.size() || target.size() != travel_times.size() {
        return Err(Error::InvalidArgument(
            "Target, travel times, and value map dimensions should be the same".into(),
        ));
    }

    let rows = target.rows();
    let cols = target.columns();
    let unreachable = f32::INFINITY;

    let mut value_at_closest_target = ValueRaster::WithPixelType::<TResult>::new_with_dimensions_of(value, TResult::zero());
    let mut distance_to_target = ValueRaster::WithPixelType::<f32>::new_with_dimensions_of(value, unreachable);

    let mut mark = DenseArray::<u8, TargetRaster::Metadata>::new_with_dimensions_of(target, MARK_TODO);
    let mut border = FiLo::new(rows, cols);

    for r in 0..rows.count() {
        for c in 0..cols.count() {
            let cell = Cell::from_row_col(r, c);
            if value.cell_is_nodata(cell) {
                value_at_closest_target.set_cell_value(cell, None);
                distance_to_target.set_cell_value(cell, None);
            } else if target[cell] != TargetRaster::Pixel::zero() {
                distance_to_target[cell] = 0.0;
                value_at_closest_target[cell] = NumCast::from(value[cell]).expect("Failed to cast value to TResult");
                mark[cell] = MARK_BORDER;
                border.push_back(cell);
            }
        }
    }

    let sqrt2 = 2.0_f32.sqrt();
    while !border.is_empty() {
        let cell = border.pop_head();
        assert_eq!(mark[cell], MARK_BORDER);
        mark[cell] = MARK_DONE;

        visit_neighbour_cells(cell, rows, cols, |neighbour| {
            handle_cell_value_at_closest_travel_target(
                1.0,
                cell,
                neighbour,
                &mut distance_to_target,
                &mut value_at_closest_target,
                travel_times,
                &mut mark,
                &mut border,
            );
        });

        visit_neighbour_diag_cells(cell, rows, cols, |neighbour| {
            handle_cell_value_at_closest_travel_target(
                sqrt2,
                cell,
                neighbour,
                &mut distance_to_target,
                &mut value_at_closest_target,
                travel_times,
                &mut mark,
                &mut border,
            );
        });
    }

    Ok(value_at_closest_target)
}

pub fn value_at_closest_less_than_travel_target<TResult, TargetRaster, TravelRaster, ValueRaster>(
    target: &TargetRaster,
    travel_times: &TravelRaster,
    max_travel_time: f32,
    value: &ValueRaster,
) -> Result<ValueRaster::WithPixelType<TResult>>
where
    TResult: ArrayNum<TResult>,
    TargetRaster: Array,
    TravelRaster: Array,
    ValueRaster: Array,
    ValueRaster::WithPixelType<TResult>: ArrayCopy<TResult, ValueRaster>,
    ValueRaster::WithPixelType<f32>: ArrayCopy<f32, ValueRaster>,
{
    if target.size() != value.size() || target.size() != travel_times.size() {
        return Err(Error::InvalidArgument(
            "Target, travel times, and value map dimensions should be the same".into(),
        ));
    }

    let rows = target.rows();
    let cols = target.columns();
    let unreachable = max_travel_time;

    let mut value_at_closest_target = ValueRaster::WithPixelType::<TResult>::new_with_dimensions_of(value, TResult::zero());
    let mut distance_to_target = ValueRaster::WithPixelType::<f32>::new_with_dimensions_of(value, unreachable);

    let mut mark = DenseArray::<u8, TargetRaster::Metadata>::new_with_dimensions_of(target, MARK_TODO);
    let mut border = FiLo::new(rows, cols);

    for r in 0..rows.count() {
        for c in 0..cols.count() {
            let cell = Cell::from_row_col(r, c);
            if value.cell_is_nodata(cell) {
                value_at_closest_target.set_cell_value(cell, None);
                distance_to_target.set_cell_value(cell, None);
            } else if target[cell] != TargetRaster::Pixel::zero() {
                distance_to_target[cell] = 0.0;
                value_at_closest_target[cell] = NumCast::from(value[cell]).expect("Failed to cast value to TResult");
                mark[cell] = MARK_BORDER;
                border.push_back(cell);
            }
        }
    }

    let sqrt2 = 2.0_f32.sqrt();
    while !border.is_empty() {
        let cell = border.pop_head();
        assert_eq!(mark[cell], MARK_BORDER);
        mark[cell] = MARK_DONE;

        visit_neighbour_cells(cell, rows, cols, |neighbour| {
            handle_cell_value_at_closest_travel_target(
                1.0,
                cell,
                neighbour,
                &mut distance_to_target,
                &mut value_at_closest_target,
                travel_times,
                &mut mark,
                &mut border,
            );
        });

        visit_neighbour_diag_cells(cell, rows, cols, |neighbour| {
            handle_cell_value_at_closest_travel_target(
                sqrt2,
                cell,
                neighbour,
                &mut distance_to_target,
                &mut value_at_closest_target,
                travel_times,
                &mut mark,
                &mut border,
            );
        });
    }

    Ok(value_at_closest_target)
}

// computes the sum of the valueToSum that is within the distance via lowest travelTime
#[allow(clippy::too_many_arguments)]
fn compute_sum_le_time_distance<TResult, TValue, TTravel>(
    target_cell: Cell,
    travel_time: &impl Array<Pixel = TTravel>,
    max_travel_time: f32,
    unreachable: f32,
    value_to_sum: &impl Array<Pixel = TValue>,
    incl_adjacent: bool,
    distance_to_target: &mut impl Array<Pixel = f32>,
    mark: &mut impl Array<Pixel = u8>,
    border: &mut FiLo<Cell>,
    cells: &mut Vec<Cell>,
    adjacent_cells: &mut Vec<Cell>,
) -> TResult
where
    TValue: ArrayNum<TValue>,
    TResult: ArrayNum<TResult>,
{
    let mut sum = TResult::zero();

    let rows = mark.rows();
    let cols = mark.columns();

    let cell = target_cell;
    distance_to_target[cell] = 0.0;
    if !travel_time.cell_is_nodata(cell) {
        border.push_back(cell);
        mark[cell] = MARK_BORDER;
    } else {
        mark[cell] = MARK_DONE;
    }
    cells.push(cell);

    let sqrt2 = 2.0_f32.sqrt();
    while !border.is_empty() {
        let cur_cell = border.pop_head();
        assert_eq!(mark[cur_cell], MARK_BORDER);
        mark[cur_cell] = MARK_DONE;

        visit_neighbour_cells(cur_cell, rows, cols, |neighbour| {
            handle_sum_le_time_distance_cell(1.0, cur_cell, neighbour, distance_to_target, mark, travel_time, border, cells);
        });

        visit_neighbour_diag_cells(cur_cell, rows, cols, |neighbour| {
            handle_sum_le_time_distance_cell(sqrt2, cur_cell, neighbour, distance_to_target, mark, travel_time, border, cells);
        });
    }

    for &cell in cells.iter() {
        assert!(distance_to_target[cell] <= max_travel_time);
        if !value_to_sum.cell_is_nodata(cell) {
            sum += NumCast::from(value_to_sum[cell]).expect("Failed to cast value to TResult");
        }
        assert_eq!(mark[cell], MARK_DONE);
        mark[cell] = MARK_TODO;
    }

    if incl_adjacent {
        assert!(adjacent_cells.is_empty());
        let mut handle_adjacent = |cell: Cell| {
            if cell.row >= 0
                && cell.row < rows.count()
                && cell.col >= 0
                && cell.col < cols.count()
                && distance_to_target[cell] > max_travel_time
                && mark[cell] == MARK_TODO
            {
                if !value_to_sum.cell_is_nodata(cell) {
                    sum += NumCast::from(value_to_sum[cell]).expect("Failed to cast value to TResult");
                }
                mark[cell] = MARK_DONE;
                adjacent_cells.push(cell);
            }
        };
        for cell in cells.iter() {
            handle_adjacent(cell.above());
            handle_adjacent(cell.below());
            handle_adjacent(cell.left());
            handle_adjacent(cell.right());
        }
    }

    for cell in cells.iter() {
        distance_to_target[*cell] = unreachable;
    }
    for cell in adjacent_cells.iter() {
        mark[*cell] = MARK_TODO;
    }

    cells.clear();
    adjacent_cells.clear();
    sum
}

pub fn sum_within_travel_distance<TResult, MaskRaster, ResistanceRaster, ValueRaster>(
    mask: &MaskRaster,
    resistance_map: &ResistanceRaster,
    value_map: &ValueRaster,
    max_resistance: f32,
    include_adjacent: bool,
) -> Result<impl Array<Pixel = TResult>>
where
    TResult: ArrayNum<TResult>,
    MaskRaster: Array,
    ResistanceRaster: Array,
    ValueRaster: Array,
    ValueRaster::WithPixelType<TResult>: ArrayCopy<TResult, ValueRaster>,
    ResistanceRaster::WithPixelType<f32>: ArrayCopy<f32, ResistanceRaster>,
{
    if mask.size() != resistance_map.size() || mask.size() != value_map.size() {
        return Err(Error::InvalidArgument(
            "Mask, resistance, and value map dimensions should be the same".into(),
        ));
    }

    if max_resistance <= 0.0 {
        return Err(Error::InvalidArgument("max_resistance should be positive".into()));
    }

    for i in 0..resistance_map.len() {
        if let Some(v) = resistance_map.value(i) {
            if v < ResistanceRaster::Pixel::zero() {
                return Err(Error::InvalidArgument("resistance may not be negative".into()));
            }
        }
    }

    let rows = mask.rows();
    let cols = mask.columns();

    let mut result = ValueRaster::WithPixelType::<TResult>::new_with_dimensions_of(value_map, TResult::zero());
    let unreachable = f32::INFINITY;

    let mut distance_to_target = ResistanceRaster::WithPixelType::<f32>::new_with_dimensions_of(resistance_map, unreachable);
    let mut mark = DenseArray::<u8, MaskRaster::Metadata>::new_with_dimensions_of(mask, MARK_TODO);
    let mut border = FiLo::new(rows, cols);
    let mut cells = Vec::new();
    let mut adjacent_cells = Vec::new();

    for r in 0..rows.count() {
        for c in 0..cols.count() {
            let cell = Cell::from_row_col(r, c);
            if mask.cell_has_data(cell) && mask[cell] != MaskRaster::Pixel::zero() {
                result[cell] = compute_sum_le_time_distance::<TResult, _, _>(
                    cell,
                    resistance_map,
                    max_resistance,
                    unreachable,
                    value_map,
                    include_adjacent,
                    &mut distance_to_target,
                    &mut mark,
                    &mut border,
                    &mut cells,
                    &mut adjacent_cells,
                );
            }
        }
    }

    Ok(result)
}

pub fn sum_targets_within_travel_distance<TResult, TargetRaster, ResistanceRaster>(
    targets: &TargetRaster,
    resistance_map: &ResistanceRaster,
    max_resistance: f32,
) -> Result<impl Array<Pixel = TResult>>
where
    TResult: ArrayNum<TResult>,
    TargetRaster: Array,
    ResistanceRaster: Array,
    TargetRaster::WithPixelType<TResult>: ArrayCopy<TResult, TargetRaster>,
    TargetRaster::WithPixelType<f32>: ArrayCopy<f32, TargetRaster>,
{
    if targets.size() != resistance_map.size() {
        return Err(Error::InvalidArgument(
            "Targets and resistance map dimensions should be the same".into(),
        ));
    }

    if max_resistance <= 0.0 {
        return Err(Error::InvalidArgument("maxResistance should be positive".into()));
    }

    for i in 0..resistance_map.len() {
        if let Some(v) = resistance_map.value(i) {
            if v < ResistanceRaster::Pixel::zero() {
                return Err(Error::InvalidArgument("resistance may not be negative".into()));
            }
        }
    }

    let rows = targets.rows();
    let cols = targets.columns();
    let unreachable = max_resistance + 1.0;
    let sqrt2 = 2.0_f32.sqrt();

    let mut result = TargetRaster::WithPixelType::<TResult>::new_with_dimensions_of(targets, TResult::zero());
    let mut distance_to_target = TargetRaster::WithPixelType::<f32>::new_with_dimensions_of(targets, unreachable);
    let mut mark = DenseArray::<u8, _>::new_with_dimensions_of(targets, MARK_TODO);
    let mut added = DenseArray::<u8, _>::new_with_dimensions_of(targets, 0);
    let mut border = FiLo::new(rows, cols);

    // FLT_MAX/4 allows to add 2 x sqrt(2) of them and still be less than FLT_MAX
    let resistance = nodata::replace_nodata(
        resistance_map,
        ResistanceRaster::Pixel::max_value() / NumCast::from(4.0).expect("Failed to cast 4.0"),
    );

    for r in 0..rows.count() {
        for c in 0..cols.count() {
            let cell = Cell::from_row_col(r, c);
            if targets.cell_is_nodata(cell) || targets[cell] == TargetRaster::Pixel::zero() {
                continue;
            }

            distance_to_target.fill(unreachable);
            mark.fill(MARK_TODO);
            added.fill(0);
            assert!(border.is_empty());

            distance_to_target[cell] = 0.0;
            if !resistance_map.cell_is_nodata(cell) {
                border.push_back(cell);
                mark[cell] = MARK_BORDER;
            } else {
                mark[cell] = MARK_DONE;
            }

            while !border.is_empty() {
                let cur_cell = border.pop_head();
                assert_eq!(mark[cur_cell], MARK_BORDER);
                mark[cur_cell] = MARK_DONE;
                if distance_to_target[cur_cell] <= max_resistance && added[cur_cell] == 0 {
                    // we can get here via different routes within the maxTravelTime.  But count only once.
                    result[cur_cell] += NumCast::from(targets[cell]).expect("Failed to cast target to TResult");
                    added[cur_cell] = 1;
                }

                visit_neighbour_cells(cur_cell, rows, cols, |neighbour| {
                    handle_time_cell(
                        1.0,
                        cur_cell,
                        neighbour,
                        &mut distance_to_target,
                        &mut mark,
                        &resistance,
                        &mut border,
                    );
                });

                visit_neighbour_diag_cells(cur_cell, rows, cols, |neighbour| {
                    handle_time_cell(
                        sqrt2,
                        cur_cell,
                        neighbour,
                        &mut distance_to_target,
                        &mut mark,
                        &resistance,
                        &mut border,
                    );
                });
            }
        }
    }

    Ok(result)
}

#[cfg(test)]
#[generic_tests::define]
mod unspecialized_generictests {
    use approx::{assert_abs_diff_eq, assert_relative_eq, RelativeEq};

    use crate::{
        array::{Columns, Rows},
        testutils::NOD,
        CellSize, Point, RasterSize,
    };

    use super::*;

    #[test]
    fn distance<R: Array<Pixel = u8, Metadata = GeoReference, WithPixelType<u8> = R>>()
    where
        R::WithPixelType<f32>: ArrayCopy<f32, R::WithPixelType<u8>>,
    {
        let meta = GeoReference::with_origin(
            "",
            RasterSize::with_rows_cols(Rows(5), Columns(10)),
            Point::new(0.0, 0.0),
            CellSize::square(100.0),
            Some(NOD),
        );

        #[rustfmt::skip]
        let raster = R::WithPixelType::<u8>::new(
            meta.clone(),
            vec![
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                1, 2, 0, 0, 0, 0, 0, 0, 0, 0,
                3, 0, 0, 1, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            ],
        );

        #[rustfmt::skip]
        let expected = R::WithPixelType::<f32>::new(
            meta,
            vec![
                200.0, 200.000, 241.421, 282.843, 341.421, 382.843, 424.264, 524.264, 624.264, 724.264,
                100.0, 100.000, 141.421, 200.000, 241.421, 282.843, 382.843, 482.843, 582.843, 682.843,
                  0.0,   0.000, 100.000, 100.000, 141.421, 241.421, 341.421, 441.421, 541.421, 641.421,
                  0.0, 100.000, 100.000,   0.000, 100.000, 200.000, 300.000, 400.000, 500.000, 600.000,
                100.0, 141.421, 141.421, 100.000, 141.421, 241.421, 341.421, 441.421, 541.421, 641.421,
            ]
        );

        assert_abs_diff_eq!(expected, &super::distance(&raster), epsilon = 0.001);
    }

    #[test]
    fn distance_all_ones<R: Array<Pixel = u8, Metadata = GeoReference, WithPixelType<u8> = R>>()
    where
        R::WithPixelType<f32>: ArrayCopy<f32, R::WithPixelType<u8>>,
    {
        let meta = GeoReference::with_origin(
            "",
            RasterSize::with_rows_cols(Rows(5), Columns(10)),
            Point::new(0.0, 0.0),
            CellSize::square(100.0),
            Some(NOD),
        );

        #[rustfmt::skip]
        let raster = R::WithPixelType::<u8>::new(
            meta.clone(),
            vec![
                1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            ],
        );

        #[rustfmt::skip]
        let expected = R::WithPixelType::<f32>::new(
            meta,
            vec![
                0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
            ]
        );

        assert_abs_diff_eq!(expected, &super::distance(&raster));
    }

    #[test]
    fn distance_with_obstacles<R: Array<Pixel = u8, Metadata = GeoReference, WithPixelType<u8> = R>>()
    where
        R::WithPixelType<f32>: ArrayCopy<f32, R::WithPixelType<u8>> + RelativeEq,
    {
        let meta = GeoReference::with_origin(
            "",
            RasterSize::with_rows_cols(Rows(5), Columns(10)),
            Point::new(0.0, 0.0),
            CellSize::square(100.0),
            Some(NOD),
        );

        #[rustfmt::skip]
        let targets = R::WithPixelType::<u8>::new(
            meta.clone(),
            vec![
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                1, 2, 0, 0, 0, 0, 0, 0, 0, 0,
                3, 0, 0, 1, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            ],
        );

        #[rustfmt::skip]
        let barrier = R::WithPixelType::<u8>::new(
            meta.clone(),
            vec![
                0, 0, 0, 0, 0, 0, 1, 0, 0, 0,
                1, 1, 1, 0, 0, 0, 1, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 1, 0, 0, 0,
                0, 0, 0, 1, 0, 0, 1, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            ],
        );

        const INF: f32 = f32::INFINITY;

        #[rustfmt::skip]
        let expected = R::WithPixelType::<f32>::new(
            meta,
            vec![
                541.421, 441.421, 341.421, 300.0, 341.421, 382.843,     INF, 782.843, 824.264, 865.685,
                    INF,     INF,     INF, 200.0, 241.421, 282.843,     INF, 682.843, 724.264, 765.685,
                    0.0,     0.0, 100.000, 100.0, 141.421, 241.421,     INF, 582.843, 624.264, 724.264,
                    0.0,   100.0, 100.000,   0.0, 100.000, 200.000,     INF, 482.843, 582.843, 682.843,
                  100.0, 141.421, 141.421, 100.0, 141.421, 241.421, 341.421, 441.421, 541.421, 641.421,
            ]
        );

        assert_relative_eq!(
            expected,
            &super::distance_with_obstacles(&targets, &barrier, BarrierDiagonals::Exclude).unwrap(),
            epsilon = 0.001
        );
    }

    #[test]
    fn distance_with_obstacles_only_diagonal_path<R: Array<Pixel = u8, Metadata = GeoReference, WithPixelType<u8> = R>>()
    where
        R::WithPixelType<f32>: ArrayCopy<f32, R::WithPixelType<u8>> + RelativeEq,
    {
        let meta = GeoReference::with_origin(
            "",
            RasterSize::with_rows_cols(Rows(5), Columns(10)),
            Point::new(0.0, 0.0),
            CellSize::square(100.0),
            Some(NOD),
        );

        #[rustfmt::skip]
        let targets = R::WithPixelType::<u8>::new(
            meta.clone(),
            vec![
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 1, 1, 0, 0, 0, 0,
                0, 0, 0, 0, 1, 1, 0, 0, 0, 0,
            ],
        );

        #[rustfmt::skip]
        let barrier = R::WithPixelType::<u8>::new(
            meta.clone(),
            vec![
                0, 1, 1, 1, 1, 1, 1, 1, 1, 0,
                1, 0, 1, 1, 1, 1, 1, 1, 0, 1,
                1, 1, 0, 1, 1, 1, 1, 0, 1, 1,
                1, 1, 1, 0, 1, 1, 0, 1, 1, 1,
                1, 1, 1, 1, 0, 0, 1, 1, 1, 1,
            ],
        );

        const INF: f32 = f32::INFINITY;

        {
            // Allow diagonals
            #[rustfmt::skip]
            let expected = R::WithPixelType::<f32>::new(
                meta.clone(),
                vec![
                    524.26401,       INF,       INF,   INF, INF, INF,   INF,       INF,       INF,  524.2641,
                        INF, 382.84273,       INF,   INF, INF, INF,   INF,       INF, 382.84273,       INF,
                        INF,       INF, 241.42136,   INF, INF, INF,   INF, 241.42137,       INF,       INF,
                        INF,       INF,       INF, 100.0, 0.0, 0.0, 100.0,       INF,       INF,       INF,
                        INF,       INF,       INF,   INF, 0.0, 0.0,   INF,       INF,       INF,       INF,
                ]
            );

            assert_relative_eq!(
                expected,
                &super::distance_with_obstacles(&targets, &barrier, BarrierDiagonals::Include).unwrap(),
                epsilon = 0.0001
            );
        }

        {
            // Don't allow diagonals
            #[rustfmt::skip]
            let expected = R::WithPixelType::<f32>::new(
                meta.clone(),
                vec![
                    INF, INF, INF,   INF, INF, INF,   INF, INF, INF, INF,
                    INF, INF, INF,   INF, INF, INF,   INF, INF, INF, INF,
                    INF, INF, INF,   INF, INF, INF,   INF, INF, INF, INF,
                    INF, INF, INF, 100.0, 0.0, 0.0, 100.0, INF, INF, INF,
                    INF, INF, INF,   INF, 0.0, 0.0,   INF, INF, INF, INF,
                ]
            );

            assert_relative_eq!(
                expected,
                &super::distance_with_obstacles(&targets, &barrier, BarrierDiagonals::Exclude).unwrap(),
                epsilon = 0.0001
            );
        }
    }

    #[test]
    fn distance_with_obstacles_only_diagonal_barrier<R: Array<Pixel = u8, Metadata = GeoReference, WithPixelType<u8> = R>>()
    where
        R::WithPixelType<f32>: ArrayCopy<f32, R::WithPixelType<u8>> + RelativeEq,
    {
        let meta = GeoReference::with_origin(
            "",
            RasterSize::with_rows_cols(Rows(5), Columns(10)),
            Point::new(0.0, 0.0),
            CellSize::square(100.0),
            Some(NOD),
        );

        #[rustfmt::skip]
        let targets = R::WithPixelType::<u8>::new(
            meta.clone(),
            vec![
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 1, 1, 0, 0, 0, 0,
            ],
        );

        #[rustfmt::skip]
        let barrier = R::WithPixelType::<u8>::new(
            meta.clone(),
            vec![
                1, 0, 0, 0, 0, 0, 0, 0, 0, 1,
                0, 1, 0, 0, 0, 0, 0, 0, 1, 0,
                0, 0, 1, 0, 0, 0, 0, 1, 0, 0,
                0, 0, 0, 1, 0, 0, 1, 0, 0, 0,
                0, 0, 0, 1, 0, 0, 1, 0, 0, 0,
            ],
        );

        const INF: f32 = f32::INFINITY;

        {
            // Allow diagonals
            #[rustfmt::skip]
            let expected = R::WithPixelType::<f32>::new(
                meta.clone(),
                vec![
                        INF, 524.264, 482.843, 441.421, 400.0, 400.0, 441.421, 482.843, 524.264,     INF,
                    665.685,     INF, 382.843, 341.421, 300.0, 300.0, 341.421, 382.843,     INF, 665.685,
                    624.264, 524.264,     INF, 241.421, 200.0, 200.0, 241.421,     INF, 524.264, 624.264,
                    582.843, 482.843, 382.843,     INF, 100.0, 100.0,     INF, 382.843, 482.843, 582.843,
                    624.264, 524.264, 482.843,     INF,   0.0,   0.0,     INF, 482.843, 524.264, 624.264,
                ]
            );

            assert_relative_eq!(
                expected,
                &super::distance_with_obstacles(&targets, &barrier, BarrierDiagonals::Include).unwrap(),
                epsilon = 0.001
            );
        }

        {
            // Don't allow diagonals
            #[rustfmt::skip]
            let expected = R::WithPixelType::<f32>::new(
                meta.clone(),
                vec![
                    INF, 524.264, 482.843, 441.421, 400.0, 400.0, 441.421, 482.843, 524.264, INF,
                    INF,     INF, 382.843, 341.421, 300.0, 300.0, 341.421, 382.843,     INF, INF,
                    INF,     INF,     INF, 241.421, 200.0, 200.0, 241.421,     INF,     INF, INF,
                    INF,     INF,     INF,     INF, 100.0, 100.0,     INF,     INF,     INF, INF,
                    INF,     INF,     INF,     INF,   0.0,   0.0,     INF,     INF,     INF, INF,
                ]
            );

            assert_relative_eq!(
                expected,
                &super::distance_with_obstacles(&targets, &barrier, BarrierDiagonals::Exclude).unwrap(),
                epsilon = 0.001
            );
        }
    }

    #[instantiate_tests(<DenseRaster<u8>>)]
    mod denseraster {}
}
