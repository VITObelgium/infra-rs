use approx::relative_eq;

use crate::{Error, GeoReference, Result};

#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct CutOut {
    pub src_col_offset: i32,
    pub src_row_offset: i32,
    pub dst_col_offset: i32,
    pub dst_row_offset: i32,
    pub rows: i32,
    pub cols: i32,
}

pub fn intersect_georeference(src_meta: &GeoReference, dst_meta: &GeoReference) -> Result<CutOut> {
    // src_meta: the metadata of the raster that we are going to read as it ison disk
    // dst_meta: the metadata of the raster that will be returned to the user

    let src_cellsize = src_meta.cell_size();
    let dst_cellsize = dst_meta.cell_size();

    if !relative_eq!(src_cellsize, dst_cellsize, epsilon = 1e-10) {
        return Err(Error::InvalidArgument("Cell sizes do not match".to_string()));
    }

    if !src_cellsize.is_valid() {
        return Err(Error::InvalidArgument("Extent cellsize is zero".to_string()));
    }

    let cell_size = src_meta.cell_size();
    let src_bbox = src_meta.bounding_box();
    let dst_bbox = dst_meta.bounding_box();

    let intersect = src_bbox.intersection(&dst_bbox);

    // Calulate the cell in the source extent that corresponds to the top left cell of the intersect
    // Take the nearest one, otherwise in case of non-integer cell sizes, we might take a cell to the left,
    // because the cell index is e.g 32.99999999 due to floating point precision issues.
    let intersect_top_left_cell = src_meta.point_to_nearest_cell(intersect.top_left());

    let result = CutOut {
        src_col_offset: intersect_top_left_cell.col,
        src_row_offset: intersect_top_left_cell.row,
        rows: (intersect.height() / cell_size.y()).abs().round() as i32,
        cols: (intersect.width() / cell_size.x()).round() as i32,
        dst_col_offset: ((intersect.top_left().x() - dst_bbox.top_left().x()) / cell_size.x()).round() as i32,
        dst_row_offset: ((dst_bbox.top_left().y() - intersect.top_left().y()) / cell_size.y().abs()).round() as i32,
    };

    Ok(result)
}

#[cfg(test)]
mod tests {
    use approx::assert_relative_eq;

    use crate::{Cell, CellSize, Columns, GeoTransform, Point, RasterSize, Rows};

    use super::*;

    #[test]
    fn test_intersect_metadata() {
        let meta1 = GeoReference::with_bottom_left_origin(
            String::default(),
            RasterSize::with_rows_cols(Rows(3), Columns(5)),
            Point::new(1.0, -10.0),
            CellSize::square(4.0),
            Some(-10.0),
        );
        let meta2 = GeoReference::with_bottom_left_origin(
            String::default(),
            RasterSize::with_rows_cols(Rows(3), Columns(4)),
            Point::new(-3.0, -6.0),
            CellSize::square(4.0),
            Some(-6.0),
        );

        assert_eq!(meta2.cell_center(Cell::from_row_col(0, 0)), Point::new(-1.0, 4.0));
        assert_eq!(meta1.point_to_cell(Point::new(0.0, 4.0)), Cell::from_row_col(-1, -1));

        let cutout = intersect_georeference(&meta1, &meta2).unwrap();

        assert_eq!(cutout.rows, 2);
        assert_eq!(cutout.cols, 3);
        assert_eq!(cutout.src_col_offset, 0);
        assert_eq!(cutout.src_row_offset, 0);
        assert_eq!(cutout.dst_col_offset, 1);
        assert_eq!(cutout.dst_row_offset, 1);
    }

    #[test]
    fn intersect_meta_epsg_4326() {
        const TRANS: GeoTransform = GeoTransform::new([
            -30.000_000_763_788_11,
            0.100_000_001_697_306_9,
            0.0,
            29.999999619212282,
            0.0,
            -0.049_999_998_635_984_29,
        ]);

        let meta = GeoReference::new(
            "EPSG:4326".to_string(),
            RasterSize::with_rows_cols(Rows(840), Columns(900)),
            TRANS,
            None,
        );
        assert_relative_eq!(
            meta.cell_center(Cell::from_row_col(0, 0)),
            Point::new(
                TRANS.top_left().x() + (TRANS.cell_size_x() / 2.0),
                TRANS.top_left().y() + (TRANS.cell_size_y() / 2.0)
            ),
            epsilon = 1e-6
        );

        // Cell to point and back
        let cell = Cell::from_row_col(0, 0);
        assert_eq!(meta.point_to_cell(meta.cell_center(cell)), cell);
        assert_eq!(meta.point_to_cell(meta.top_left()), Cell::from_row_col(0, 0));

        let cutout = intersect_georeference(&meta, &meta).unwrap();
        assert_eq!(cutout.cols, 900);
        assert_eq!(cutout.rows, 840);

        assert_eq!(cutout.src_col_offset, 0);
        assert_eq!(cutout.dst_col_offset, 0);

        assert_eq!(cutout.src_row_offset, 0);
        assert_eq!(cutout.dst_row_offset, 0);
    }

    #[test]
    fn intersect_meta_epsg_3857() {
        let meta1 = GeoReference::new(
            "EPSG:3857".to_string(),
            RasterSize::with_rows_cols(Rows(256), Columns(256)),
            [547900.6187481433, 611.49622628141, 0.0, 6731350.45890576, 0.0, -611.49622628141].into(),
            None,
        );

        let meta2 = GeoReference::new(
            "EPSG:3857".to_string(),
            RasterSize::with_rows_cols(Rows(256), Columns(256)),
            [626172.1357121639, 611.49622628141, 0.0, 6731350.45890576, 0.0, -611.49622628141].into(),
            None,
        );

        assert!(meta1.intersects(&meta2).unwrap());

        let cutout = intersect_georeference(&meta1, &meta2).unwrap();
        assert_eq!(cutout.cols, 128);
        assert_eq!(cutout.rows, 256);

        assert_eq!(cutout.src_col_offset, 128);
        assert_eq!(cutout.dst_col_offset, 0);

        assert_eq!(cutout.src_row_offset, 0);
        assert_eq!(cutout.dst_row_offset, 0);
    }
}
