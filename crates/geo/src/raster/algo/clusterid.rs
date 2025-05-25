use num::Zero;

use crate::{Array, ArrayCopy, ArrayMetadata, ArrayNum, Cell, DenseArray, Error, GeoReference, Nodata, RasterSize, Result};

use super::clusterutils::{ClusterDiagonals, MARK_TODO};
use super::clusterutils::{
    FiLo, MARK_BORDER, MARK_DONE, handle_cell, insert_border_cell, insert_cell, show_warning_if_clustering_on_floats,
    visit_neighbour_cells, visit_neighbour_diag_cells,
};

pub fn cluster_id<R, T>(ras: &R, diagonals: ClusterDiagonals) -> R::WithPixelType<u32>
where
    R: Array<Pixel = T>,
    T: ArrayNum,
    R::WithPixelType<u32>: ArrayCopy<u32, R>,
{
    show_warning_if_clustering_on_floats::<T>();

    let rows = ras.rows();
    let cols = ras.columns();

    let mut result = R::WithPixelType::<u32>::new_with_dimensions_of(ras, 0);
    let mut mark = DenseArray::<u8>::filled_with(Some(MARK_TODO), ras.size());
    let mut cluster_cells = Vec::new();
    let mut border = FiLo::new(rows, cols);

    let mut cluster_id = 0;
    for r in 0..rows.count() {
        for c in 0..cols.count() {
            let cell = Cell::from_row_col(r, c);

            if ras.cell_is_nodata(cell) {
                result.set_cell_value(cell, None);
                continue;
            }

            let ras_val = ras[cell];

            if ras_val == T::zero() {
                result[cell] = 0;
            } else if ras_val > T::zero() && mark[cell] == MARK_TODO {
                cluster_id += 1;

                cluster_cells.clear();
                border.clear();

                let cluster_value = ras_val;

                // add current cell to the cluster
                insert_cell(cell, &mut cluster_cells, &mut mark, &mut border);

                while !border.is_empty() {
                    let cell = border.pop_head();

                    visit_neighbour_cells(cell, rows, cols, |neighbour| {
                        handle_cell(neighbour, cluster_value, &mut cluster_cells, &mut mark, &mut border, ras);
                    });

                    if diagonals == ClusterDiagonals::Include {
                        visit_neighbour_diag_cells(cell, rows, cols, |neighbour| {
                            handle_cell(neighbour, cluster_value, &mut cluster_cells, &mut mark, &mut border, ras);
                        });
                    }
                }

                for &cell in &cluster_cells {
                    mark[cell] = MARK_DONE;
                    result[cell] = cluster_id;
                }
            }
        }
    }

    result
}

pub fn fuzzy_cluster_id<R>(ras: &R, radius_in_meter: f32) -> R::WithPixelType<i32>
where
    R: Array<Metadata = GeoReference>,
    R::WithPixelType<i32>: ArrayCopy<i32, R>,
{
    let rows = ras.rows();
    let cols = ras.columns();

    let radius = radius_in_meter / ras.metadata().cell_size_x() as f32;
    let radius_in_cells = radius as i32;
    let radius2 = (radius * radius) as i32;

    let mut result = R::WithPixelType::<i32>::new_with_dimensions_of(ras, -9999);
    let mut mark = DenseArray::<u8>::filled_with(Some(MARK_DONE), ras.size());

    ras.iter().zip(mark.iter_mut()).zip(result.iter_mut()).for_each(|((val, m), res)| {
        if val.is_nodata() {
            *m = MARK_TODO;
            *res = i32::NODATA;
            return;
        }

        if *val > R::Pixel::zero() {
            *m = MARK_TODO;
        } else {
            *res = 0;
        }
    });

    let mut cluster_id = 0;
    let mut border = FiLo::new(rows, cols);

    for r in 0..rows.count() {
        for c in 0..cols.count() {
            let cell = Cell::from_row_col(r, c);
            if mark[cell] == MARK_TODO {
                cluster_id += 1;

                border.clear();
                border.push_back(cell);
                mark[cell] = MARK_BORDER;

                while !border.is_empty() {
                    let cell = border.pop_head();
                    mark[cell] = MARK_DONE;
                    result[cell] = cluster_id;

                    let r0 = if cell.row - radius_in_cells < 0 {
                        0
                    } else {
                        cell.row - radius_in_cells
                    };
                    let c0 = if cell.col - radius_in_cells < 0 {
                        0
                    } else {
                        cell.col - radius_in_cells
                    };
                    let r1 = if cell.row + radius_in_cells > rows.count() - 1 {
                        rows.count() - 1
                    } else {
                        cell.row + radius_in_cells
                    };
                    let c1 = if cell.col + radius_in_cells > cols.count() - 1 {
                        cols.count() - 1
                    } else {
                        cell.col + radius_in_cells
                    };

                    for rr in r0..=r1 {
                        let dr = rr - cell.row;
                        let dr2 = dr * dr;

                        for cc in c0..=c1 {
                            let neighbour = Cell::from_row_col(rr, cc);
                            if mark[neighbour] == MARK_TODO {
                                let dc = cc - cell.col;
                                if dr2 + dc * dc <= radius2 {
                                    mark[neighbour] = MARK_BORDER;
                                    border.push_back(neighbour);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    result
}

fn handle_cell_with_obstacles_straight(
    cell: Cell,
    cat_map: &impl Array<Pixel = i32>,
    cluster_value: i32,
    obstacle_map: &impl Array<Pixel = u8>,
    mark: &mut impl Array<Pixel = u8>,
    border: &mut FiLo<Cell>,
) {
    if cat_map[cell] == cluster_value && mark[cell] == MARK_TODO && obstacle_map[cell] == 0 {
        insert_border_cell(cell, mark, border);
    }
}

fn handle_cell_with_obstacles_diag(
    old_cell: Cell,
    cell: Cell,
    cat_map: &impl Array<Pixel = i32>,
    cluster_value: i32,
    obstacle_map: &impl Array<Pixel = u8>,
    mark: &mut DenseArray<u8>,
    border: &mut FiLo<Cell>,
) {
    if cat_map[cell] == cluster_value
        && mark[cell] == MARK_TODO
        && obstacle_map[cell] == 0
        && (obstacle_map[Cell::from_row_col(old_cell.row, cell.col)] == 0 || obstacle_map[Cell::from_row_col(cell.row, old_cell.col)] == 0)
    {
        insert_border_cell(cell, mark, border);
    }
}

fn compute_cluster_id_of_obstacle_cell(
    cell: Cell,
    cluster_id_map: &mut impl Array<Pixel = i32>,
    obstacle_map: &impl Array<Pixel = u8>,
    cluster_size: &mut [i32],
) {
    let rows = cluster_id_map.rows();
    let cols = cluster_id_map.columns();

    let mut count_neighbors = std::collections::HashMap::new();
    for r in cell.row - 1..=cell.row + 1 {
        for c in cell.col - 1..=cell.col + 1 {
            let cur_cell = Cell::from_row_col(r, c);
            if r >= 0
                && r < rows.count()
                && c >= 0
                && c < cols.count()
                && !obstacle_map.cell_is_nodata(cur_cell)
                && obstacle_map[cur_cell] == 0
            {
                let cluster_id = cluster_id_map[cur_cell];
                if cluster_id > 0 {
                    *count_neighbors.entry(cluster_id).or_insert(0) += 1;
                }
            }
        }
    }

    let mut cluster_id = -1;
    let mut most_count = 9;

    for (&id, &count) in &count_neighbors {
        if cluster_id == -1 || most_count < count {
            cluster_id = id;
            most_count = count;
        } else if most_count == count && cluster_size[cluster_id as usize] > cluster_size[id as usize] {
            cluster_id = id;
        }
    }

    if cluster_id > 0 {
        cluster_id_map.set_cell_value(cell, Some(cluster_id));
        cluster_size[cluster_id as usize] += 1;
    } else if !obstacle_map.cell_is_nodata(cell) && obstacle_map[cell] > 0 {
        cluster_id_map.set_cell_value(cell, Some(0));
    }
}

pub fn cluster_id_with_obstacles<R>(cat_map: &R, obstacle_map: &impl Array<Pixel = u8>) -> Result<R>
where
    R: Array<Pixel = i32> + ArrayCopy<i32, R>,
{
    if cat_map.size() != obstacle_map.size() {
        return Err(Error::InvalidArgument(
            "Raster, category and obstacle map dimensions should be the same".into(),
        ));
    }

    let rows = cat_map.rows();
    let cols = cat_map.columns();

    let mut result = R::new_with_dimensions_of(cat_map, Nodata::NODATA);
    let mut mark = DenseArray::<u8>::filled_with(Some(MARK_TODO), cat_map.size());

    let mut cluster_id = 0;
    let mut border = FiLo::new(rows, cols);

    let mut cluster_size = vec![0; rows * cols];
    for r in 0..rows.count() {
        for c in 0..cols.count() {
            let cell = Cell::from_row_col(r, c);
            if !cat_map.cell_is_nodata(cell)
                && !obstacle_map.cell_is_nodata(cell)
                && cat_map[cell] > 0
                && mark[cell] == MARK_TODO
                && obstacle_map[cell] == 0
            {
                cluster_id += 1;
                border.clear();
                let cluster_value = cat_map[cell];
                insert_border_cell(cell, &mut mark, &mut border);

                while !border.is_empty() {
                    let cell = border.pop_head();
                    mark[cell] = MARK_DONE;
                    result[cell] = cluster_id;

                    visit_neighbour_cells(cell, rows, cols, |neighbour| {
                        handle_cell_with_obstacles_straight(neighbour, cat_map, cluster_value, obstacle_map, &mut mark, &mut border);
                    });

                    visit_neighbour_diag_cells(cell, rows, cols, |neighbour| {
                        handle_cell_with_obstacles_diag(cell, neighbour, cat_map, cluster_value, obstacle_map, &mut mark, &mut border);
                    });
                }
            }
        }
    }

    for r in 0..rows.count() {
        for c in 0..cols.count() {
            let cell = Cell::from_row_col(r, c);
            if !cat_map.cell_is_nodata(cell) && !obstacle_map.cell_is_nodata(cell) && cat_map[cell] > 0 && obstacle_map[cell] > 0 {
                assert_eq!(mark[cell], MARK_TODO);
                compute_cluster_id_of_obstacle_cell(cell, &mut result, obstacle_map, &mut cluster_size);
            }
        }
    }

    Ok(result)
}

fn is_blocked<R>(from: Cell, diagonal: bool, to: Cell, obstacle: &R) -> bool
where
    R: Array<Pixel = u8>,
{
    if diagonal {
        obstacle[to] != 0 || (obstacle[Cell::from_row_col(from.row, to.col)] != 0 && obstacle[Cell::from_row_col(to.row, from.col)] != 0)
    } else {
        obstacle[to] != 0
    }
}

fn is_blocked_path<R>(from: Cell, to: Cell, obstacle: &R) -> bool
where
    R: Array<Pixel = u8>,
{
    let mut row = from.row;
    let mut col = from.col;
    while row != to.row || col != to.col {
        let mut dr = to.row - row;
        dr = dr.clamp(-1, 1);
        let mut dc = to.col - col;
        dc = dc.clamp(-1, 1);
        let diagonal = (dr.abs() + dc.abs()) > 1;

        if row + dr == to.row && col + dc == to.col {
            return is_blocked(
                Cell::from_row_col(row, col),
                diagonal,
                Cell::from_row_col(row + dr, col + dc),
                obstacle,
            );
        } else if is_blocked(
            Cell::from_row_col(row, col),
            diagonal,
            Cell::from_row_col(row + dr, col + dc),
            obstacle,
        ) {
            return true;
        }
        row += dr;
        col += dc;
    }

    false
}

#[allow(clippy::too_many_arguments)]
fn compute_fuzzy_cluster_id_with_obstacles_rc(
    cell: Cell,
    items: &impl Array<Pixel = i32>,
    background_id: &impl Array<Pixel = i32>,
    obstacles: &impl Array<Pixel = u8>,
    size: RasterSize,
    radius: f32,
    cluster_id: i32,
    mark: &mut DenseArray<u8>,
    border: &mut FiLo<Cell>,
    result: &mut impl Array<Pixel = i32>,
) {
    assert_eq!(mark[cell], MARK_TODO);
    mark[cell] = MARK_BORDER;
    assert!(border.is_empty());
    border.push_back(cell);

    while !border.is_empty() {
        let c = border.pop_head();
        assert_eq!(mark[c], MARK_BORDER);
        mark[c] = MARK_DONE;
        assert_eq!(result[c], 0);
        result[c] = cluster_id;

        let r0 = (c.row - (radius + 0.5) as i32).max(0);
        let c0 = (c.col - (radius + 0.5) as i32).max(0);
        let r1 = (c.row + (radius + 0.5) as i32).min(size.rows.count() - 1);
        let c1 = (c.col + (radius + 0.5) as i32).min(size.cols.count() - 1);

        for rr in r0..=r1 {
            for cc in c0..=c1 {
                let dr = rr - c.row;
                let dc = cc - c.col;
                if dr * dr + dc * dc <= (radius * radius) as i32 {
                    let clcl = Cell::from_row_col(rr, cc);
                    if items[clcl] == items[cell]
                        && background_id[clcl] == background_id[cell]
                        && mark[clcl] == MARK_TODO
                        && !is_blocked_path(c, clcl, obstacles)
                    {
                        assert_eq!(result[clcl], 0);
                        mark[clcl] = MARK_BORDER;
                        border.push_back(clcl);
                    }
                }
            }
        }
    }
}

pub fn fuzzy_cluster_id_with_obstacles<R>(items: &R, obstacles: &impl Array<Pixel = u8>, radius_in_meter: f32) -> Result<R>
where
    R: Array<Pixel = i32, Metadata = GeoReference> + ArrayCopy<i32, R>,
{
    let background_id = cluster_id_with_obstacles(&R::new_with_dimensions_of(items, 1), obstacles)?;

    let rows = items.rows();
    let cols = items.columns();
    let mut result = R::filled_with_nodata(items.metadata().clone());
    let radius = radius_in_meter / items.metadata().cell_size_x() as f32;

    let mut mark = DenseArray::<u8>::filled_with(Some(MARK_TODO), items.size());
    let mut border = FiLo::new(rows, cols);
    let mut cluster_id = 1;

    for r in 0..rows.count() {
        for c in 0..cols.count() {
            let cell = Cell::from_row_col(r, c);
            if items.cell_is_nodata(cell) {
                mark[cell] = MARK_DONE;
                result.set_cell_value(cell, None);
                continue;
            }

            if items[cell] > 0 && mark[cell] == MARK_TODO {
                if obstacles[cell] > 0 {
                    mark[cell] = MARK_DONE;
                    result[cell] = cluster_id;
                } else {
                    compute_fuzzy_cluster_id_with_obstacles_rc(
                        cell,
                        items,
                        &background_id,
                        obstacles,
                        items.metadata().size(),
                        radius,
                        cluster_id,
                        &mut mark,
                        &mut border,
                        &mut result,
                    );
                }
                cluster_id += 1;
            }
        }
    }

    Ok(result)
}

#[cfg(test)]
#[generic_tests::define]
mod generictests {
    use crate::{
        RasterSize,
        array::{Columns, Rows},
        testutils::create_vec,
    };

    use super::*;

    #[test]
    fn test_cluster_id<R: Array<Metadata = RasterSize>>()
    where
        R::WithPixelType<u32>: ArrayCopy<u32, R>,
    {
        let size = RasterSize::with_rows_cols(Rows(5), Columns(4));
        #[rustfmt::skip]
        let raster = R::new(
            size,
            create_vec(&[
                1.0, 1.0, 1.0, 1.0,
                1.0, 1.0, 2.0, 3.0,
                3.0, 3.0, 3.0, 3.0,
                1.0, 1.0, 5.0, 5.0,
                1.0, 1.0, 5.0, 1.0,
            ]),
        ).unwrap();

        #[rustfmt::skip]
        let expected = R::WithPixelType::<u32>::new(
            size,
            vec![
                1, 1, 1, 1,
                1, 1, 2, 3,
                3, 3, 3, 3,
                4, 4, 5, 5,
                4, 4, 5, 6
            ]
        ).unwrap();

        assert_eq!(expected, cluster_id(&raster, ClusterDiagonals::Exclude));
    }

    #[test]
    fn test_cluster_id_border_values<R: Array<Metadata = RasterSize>>()
    where
        R::WithPixelType<u32>: ArrayCopy<u32, R>,
    {
        let size = RasterSize::with_rows_cols(Rows(5), Columns(4));
        #[rustfmt::skip]
        let raster = R::new(
            size,
            create_vec(&[
                1.0, 2.0, 3.0, 4.0,
                2.0, 9.0, 9.0, 5.0,
                3.0, 9.0, 9.0, 6.0,
                4.0, 9.0, 9.0, 7.0,
                5.0, 6.0, 7.0, 8.0,
            ]),
        ).unwrap();

        #[rustfmt::skip]
        let expected = R::WithPixelType::<u32>::new(
            size,
            vec![
                 1,  2,  3,  4,
                 5,  6,  6,  7,
                 8,  6,  6,  9,
                10,  6,  6, 11,
                12, 13, 14, 15,
            ]
        ).unwrap();

        assert_eq!(expected, cluster_id(&raster, ClusterDiagonals::Exclude));
    }

    #[instantiate_tests(<DenseArray<i8>>)]
    mod denserasteri8 {}

    #[instantiate_tests(<DenseArray<u8>>)]
    mod denserasteru8 {}

    #[instantiate_tests(<DenseArray<i32>>)]
    mod denserasteri32 {}

    #[instantiate_tests(<DenseArray<u32>>)]
    mod denserasteru32 {}

    #[instantiate_tests(<DenseArray<i64>>)]
    mod denserasteri64 {}

    #[instantiate_tests(<DenseArray<u64>>)]
    mod denserasteru64 {}

    #[instantiate_tests(<DenseArray<f32>>)]
    mod denserasterf32 {}

    #[instantiate_tests(<DenseArray<f64>>)]
    mod denserasterf64 {}
}

#[cfg(test)]
#[generic_tests::define]
mod genericgeotests {
    use crate::{
        RasterSize,
        array::{Columns, Rows},
        raster::DenseRaster,
        testutils::create_vec,
    };

    use super::*;

    #[test]
    fn test_fuzzy_cluster_id<R: Array<Metadata = GeoReference>>()
    where
        R::WithPixelType<i32>: ArrayCopy<i32, R>,
    {
        let size = RasterSize::with_rows_cols(Rows(10), Columns(10));
        let mut meta = GeoReference::without_spatial_reference(size, None);
        meta.set_cell_size(100.0);

        #[rustfmt::skip]
        let raster = R::new(
            meta.clone(),
            create_vec(&[
                1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                1.0, 1.0, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0,
                1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0,
                1.0, 0.0, 1.0, 1.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0,
                1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0,
                0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0,
                1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 0.0, 0.0,
                0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
            ]),
        ).unwrap();

        #[rustfmt::skip]
        let expected = R::WithPixelType::<i32>::new(
            meta.clone(),
            vec![
                1, 1, 1, 1, 1, 0, 0, 0, 0, 0,
                1, 1, 0, 1, 0, 0, 2, 0, 2, 0,
                1, 0, 0, 1, 0, 0, 0, 2, 0, 0,
                1, 0, 1, 1, 0, 0, 2, 0, 2, 0,
                1, 1, 1, 1, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 3,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 4, 0,
                5, 0, 6, 0, 7, 0, 8, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            ]
        ).unwrap();

        assert_eq!(expected, fuzzy_cluster_id(&raster, 1.42_f32 * meta.cell_size_x() as f32));
    }

    #[instantiate_tests(<DenseRaster<i8>>)]
    mod denserasteri8 {}

    #[instantiate_tests(<DenseRaster<u8>>)]
    mod denserasteru8 {}

    #[instantiate_tests(<DenseRaster<i32>>)]
    mod denserasteri32 {}

    #[instantiate_tests(<DenseRaster<u32>>)]
    mod denserasteru32 {}

    #[instantiate_tests(<DenseRaster<i64>>)]
    mod denserasteri64 {}

    #[instantiate_tests(<DenseRaster<u64>>)]
    mod denserasteru64 {}

    #[instantiate_tests(<DenseRaster<f32>>)]
    mod denserasterf32 {}

    #[instantiate_tests(<DenseRaster<f64>>)]
    mod denserasterf64 {}
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "gdal")]
    #[test]
    fn test_cluster_id_with_obstacles() {
        use super::*;
        use crate::raster::DenseRaster;
        use crate::raster::RasterIO;
        use path_macro::path;

        let test_data_dir = path!(env!("CARGO_MANIFEST_DIR") / "tests" / "data");

        let categories = DenseRaster::<i32>::read(&test_data_dir.join("clusteridwithobstacles_categories.tif")).unwrap();
        let obstacles = DenseRaster::<u8>::read(&test_data_dir.join("clusteridwithobstacles_obstacles.tif")).unwrap();
        let expected = DenseRaster::<i32>::read(&test_data_dir.join("reference/clusteridwithobstacles.tif")).unwrap();

        let result = cluster_id_with_obstacles(&categories, &obstacles).unwrap();

        assert_eq!(expected, result);
    }
}
