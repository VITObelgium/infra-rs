use approx::{AbsDiffEq, RelativeEq};
use num::{NumCast, ToPrimitive};

use crate::{cell::Cell, crs::Epsg, rect, Error, LatLonBounds, Point, Rect};

#[cfg(feature = "gdal")]
use crate::spatialreference::{projection_from_epsg, projection_to_epsg, projection_to_geo_epsg};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct RasterSize {
    pub rows: usize,
    pub cols: usize,
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct CellSize {
    x: f64,
    y: f64,
}

impl AbsDiffEq for CellSize {
    type Epsilon = <f64 as AbsDiffEq>::Epsilon;

    fn default_epsilon() -> <f64 as AbsDiffEq>::Epsilon {
        f64::default_epsilon()
    }

    fn abs_diff_eq(&self, other: &Self, epsilon: <f64 as AbsDiffEq>::Epsilon) -> bool {
        f64::abs_diff_eq(&self.x, &other.x, epsilon) && f64::abs_diff_eq(&self.y, &other.y, epsilon)
    }
}

impl RelativeEq for CellSize {
    fn default_max_relative() -> <f64 as AbsDiffEq>::Epsilon {
        f64::default_max_relative()
    }

    fn relative_eq(
        &self,
        other: &Self,
        epsilon: <f64 as AbsDiffEq>::Epsilon,
        max_relative: <f64 as AbsDiffEq>::Epsilon,
    ) -> bool {
        f64::relative_eq(&self.x, &other.x, epsilon, max_relative)
            && f64::relative_eq(&self.y, &other.y, epsilon, max_relative)
    }
}

impl CellSize {
    pub fn new(x: f64, y: f64) -> Self {
        CellSize { x, y }
    }

    pub fn square(size: f64) -> Self {
        CellSize::new(size, -size)
    }

    pub fn is_valid(&self) -> bool {
        self.x != 0.0 && self.y != 0.0
    }

    pub fn multiply(&mut self, factor: f64) {
        self.x *= factor;
        self.y *= factor;
    }

    pub fn divide(&mut self, factor: f64) {
        self.x /= factor;
        self.y /= factor;
    }

    pub fn x(&self) -> f64 {
        self.x
    }

    pub fn y(&self) -> f64 {
        self.y
    }
}

/// Represents the metadata associated with a raster so it can be georeferenced.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct GeoMetadata {
    /// The proj projection string
    projection: String,
    /// The size of the image in pixels (width, height)
    size: RasterSize,
    /// The affine transformation.
    geo_transform: [f64; 6],
    /// The nodata value.
    nodata: Option<f64>,
}

impl GeoMetadata {
    pub fn new<S: Into<String>>(projection: S, size: RasterSize, geo_transform: [f64; 6], nodata: Option<f64>) -> Self {
        GeoMetadata {
            projection: projection.into(),
            size,
            geo_transform,
            nodata,
        }
    }

    pub fn raster_size(&self) -> RasterSize {
        self.size
    }

    pub fn without_spatial_reference(size: RasterSize, nodata: Option<f64>) -> Self {
        GeoMetadata {
            size,
            nodata,
            ..Default::default()
        }
    }

    pub fn with_origin<S: Into<String>, T: NumCast>(
        projection: S,
        size: RasterSize,
        lower_left_coordintate: Point,
        cell_size: CellSize,
        nodata: Option<T>,
    ) -> Self {
        let geo_transform = [
            lower_left_coordintate.x(),
            cell_size.x(),
            0.0,
            lower_left_coordintate.y() - (cell_size.y() * size.rows as f64),
            0.0,
            cell_size.y(),
        ];

        let nodata = match nodata {
            Some(nodata) => nodata.to_f64(),
            None => None,
        };

        GeoMetadata {
            projection: projection.into(),
            size,
            geo_transform,
            nodata,
        }
    }

    pub fn set_extent(&mut self, lower_left_coordintate: Point, size: RasterSize, cell_size: CellSize) {
        self.size = size;
        self.geo_transform = [
            lower_left_coordintate.x(),
            cell_size.x(),
            0.0,
            lower_left_coordintate.y() - (cell_size.y() * self.size.rows as f64),
            0.0,
            cell_size.y(),
        ];
    }

    pub fn copy_with_nodata<T: ToPrimitive>(&self, nodata: Option<T>) -> Self {
        GeoMetadata {
            projection: self.projection.clone(),
            size: self.size,
            geo_transform: self.geo_transform,
            nodata: nodata.and_then(|x| x.to_f64()),
        }
    }

    /// The verical cell size of the image.
    pub fn cell_size(&self) -> CellSize {
        CellSize::new(self.cell_size_x(), self.cell_size_y())
    }

    /// The horizontal cell size of the image.
    pub fn cell_size_x(&self) -> f64 {
        self.geo_transform[1]
    }

    pub fn set_cell_size_x(&mut self, size: f64) {
        self.geo_transform[1] = size;
    }

    /// The verical cell size of the image.
    pub fn cell_size_y(&self) -> f64 {
        self.geo_transform[5]
    }

    pub fn is_north_up(&self) -> bool {
        self.cell_size_y() < 0.0
    }

    pub fn set_cell_size_y(&mut self, size: f64) {
        self.geo_transform[5] = size;
    }

    pub fn set_cell_size(&mut self, size: f64) {
        self.set_cell_size_x(size);
        self.set_cell_size_y(-size);
    }

    pub fn rows(&self) -> usize {
        self.size.rows
    }

    pub fn columns(&self) -> usize {
        self.size.cols
    }

    /// Translates a cell to a point in the raster.
    /// Cell (0, 0) is the top left corner of the raster.
    fn coordinate_for_cell_fraction(&self, col: f64, row: f64) -> Point<f64> {
        let x = self.geo_transform[0] + self.geo_transform[1] * col + self.geo_transform[2] * row;
        let y = self.geo_transform[3] + self.geo_transform[4] * col + self.geo_transform[5] * row;

        Point::new(x, y)
    }

    pub fn cell_lower_left(&self, cell: Cell) -> Point<f64> {
        self.coordinate_for_cell_fraction(cell.col as f64, cell.row as f64 + 1.0)
    }

    pub fn cell_center(&self, cell: Cell) -> Point<f64> {
        self.coordinate_for_cell_fraction(cell.col as f64 + 0.5, cell.row as f64 + 0.5)
    }

    pub fn center(&self) -> Point<f64> {
        self.coordinate_for_cell_fraction(self.columns() as f64 / 2.0, self.rows() as f64 / 2.0)
    }

    pub fn top_left(&self) -> Point<f64> {
        self.coordinate_for_cell_fraction(0.0, 0.0)
    }

    pub fn top_left_center(&self) -> Point<f64> {
        self.coordinate_for_cell_fraction(0.5, 0.5)
    }

    pub fn bottom_right(&self) -> Point<f64> {
        self.coordinate_for_cell_fraction(self.columns() as f64, self.rows() as f64)
    }

    pub fn top_right(&self) -> Point<f64> {
        self.coordinate_for_cell_fraction(self.columns() as f64, 0.0)
    }

    pub fn bottom_left(&self) -> Point<f64> {
        self.coordinate_for_cell_fraction(0.0, self.rows() as f64)
    }

    fn convert_x_to_col_fraction(&self, x: f64) -> f64 {
        (x - self.bottom_left().x()) / self.cell_size_x()
    }

    fn convert_y_to_row_fraction(&self, y: f64) -> f64 {
        (y - self.top_left().y()) / self.cell_size_y()
    }

    pub fn x_to_col(&self, x: f64) -> i32 {
        (self.convert_x_to_col_fraction(x)).floor() as i32
    }

    pub fn y_to_row(&self, y: f64) -> i32 {
        (self.convert_y_to_row_fraction(y)).floor() as i32
    }

    pub fn point_to_cell(&self, p: Point<f64>) -> Cell {
        Cell::new(self.y_to_row(p.y()), self.x_to_col(p.x()))
    }

    pub fn is_point_on_map(&self, p: Point<f64>) -> bool {
        self.is_cell_on_map(self.point_to_cell(p))
    }

    pub fn is_cell_on_map(&self, cell: Cell) -> bool {
        self.is_on_map(cell.row, cell.col)
    }

    pub fn is_on_map(&self, r: i32, c: i32) -> bool {
        r < self.rows() as i32 && c < self.columns() as i32 && r >= 0 && c >= 0
    }

    pub fn bounding_box(&self) -> Rect<f64> {
        Rect::<f64>::from_ne_sw(self.top_left(), self.bottom_right())
    }

    pub fn latlonbounds(&self) -> LatLonBounds {
        LatLonBounds::hull(self.top_left().into(), self.bottom_right().into())
    }

    pub fn geo_transform(&self) -> [f64; 6] {
        self.geo_transform
    }

    pub fn projection(&self) -> &str {
        &self.projection
    }

    pub fn set_projection(&mut self, projection: String) {
        self.projection = projection;
    }

    pub fn set_nodata(&mut self, nodata: Option<f64>) {
        self.nodata = nodata;
    }

    pub fn nodata(&self) -> Option<f64> {
        self.nodata
    }

    pub fn geographic_epsg(&self) -> Option<Epsg> {
        #[cfg(feature = "gdal")]
        {
            if !self.projection.is_empty() {
                return projection_to_geo_epsg(self.projection.as_str());
            }
        }

        None
    }

    pub fn projected_epsg(&self) -> Option<Epsg> {
        #[cfg(feature = "gdal")]
        {
            if !self.projection.is_empty() {
                return projection_to_epsg(self.projection.as_str());
            }
        }

        None
    }

    pub fn projection_frienly_name(&self) -> String {
        if let Some(epsg) = self.projected_epsg() {
            format!("{}", epsg)
        } else {
            String::new()
        }
    }

    pub fn set_projection_from_epsg(&mut self, #[allow(unused)] epsg: Epsg) -> Result<(), Error> {
        #[cfg(feature = "gdal")]
        {
            self.projection = projection_from_epsg(epsg)?;
            Ok(())
        }

        #[cfg(not(feature = "gdal"))]
        {
            Err(Error::Runtime(
                "GDAL feature needs to be enabled for projection API".to_string(),
            ))
        }
    }
}

pub fn metadata_intersects(meta1: &GeoMetadata, meta2: &GeoMetadata) -> Result<bool, Error> {
    if meta1.projection != meta2.projection {
        return Err(Error::InvalidArgument(
            "Cannot intersect metadata with different projections".to_string(),
        ));
    }

    if meta1.cell_size() != meta2.cell_size() && !metadata_is_aligned(meta1, meta2) {
        return Err(Error::InvalidArgument(format!(
            "Extents cellsize does not match {:?} <-> {:?}",
            meta1.cell_size(),
            meta2.cell_size()
        )));
    }

    if meta1.cell_size().x == 0.0 {
        panic!("Extents cellsize is zero");
    }

    Ok(rect::intersects(&meta1.bounding_box(), &meta2.bounding_box()))
}

pub fn is_aligned(val1: f64, val2: f64, cellsize: f64) -> bool {
    let diff = (val1 - val2).abs();
    diff % cellsize < 1e-12
}

pub fn metadata_is_aligned(meta1: &GeoMetadata, meta2: &GeoMetadata) -> bool {
    let cell_size_x1 = meta1.cell_size_x();
    let cell_size_x2 = meta2.cell_size_x();

    let cell_size_y1 = meta1.cell_size_y().abs();
    let cell_size_y2 = meta2.cell_size_y().abs();

    if cell_size_x1 != cell_size_x2 {
        let (larger, smaller) = if cell_size_x1 < cell_size_x2 {
            (cell_size_x2, cell_size_x1)
        } else {
            (cell_size_x1, cell_size_x2)
        };

        if larger % smaller != 0.0 {
            return false;
        }
    }

    if cell_size_y1 != cell_size_y2 {
        let (larger, smaller) = if cell_size_y1 < cell_size_y2 {
            (cell_size_y2, cell_size_y1)
        } else {
            (cell_size_y1, cell_size_y2)
        };

        if larger % smaller != 0.0 {
            return false;
        }
    }

    is_aligned(meta1.geo_transform[0], meta2.geo_transform[0], meta1.cell_size_x())
        && is_aligned(
            meta1.geo_transform[3],
            meta2.geo_transform[3],
            meta1.cell_size_y().abs(),
        )
}

#[cfg(test)]
mod tests {

    use crate::CellSize;
    use crate::Point;

    use super::*;
    use crate::GeoMetadata;

    #[test]
    fn bounding_box_zero_origin() {
        let meta = GeoMetadata::with_origin(
            String::new(),
            RasterSize { rows: 10, cols: 5 },
            Point::new(0.0, 0.0),
            CellSize::square(5.0),
            Option::<f64>::None,
        );

        let bbox = meta.bounding_box();
        assert_eq!(*bbox.top_left(), Point::new(0.0, 50.0));
        assert_eq!(*bbox.bottom_right(), Point::new(25.0, 0.0));
    }

    #[test]
    fn bounding_box_negative_y_origin() {
        let meta = GeoMetadata::with_origin(
            String::new(),
            RasterSize { rows: 2, cols: 2 },
            Point::new(9.0, -10.0),
            CellSize::square(4.0),
            Option::<f64>::None,
        );

        let bbox = meta.bounding_box();
        assert_eq!(*bbox.top_left(), Point::new(9.0, -2.0));
        assert_eq!(*bbox.bottom_right(), Point::new(17.0, -10.0));
    }

    #[test]
    fn bounding_box_epsg_4326() {
        const TRANS: [f64; 6] = [-30.0, 0.100, 0.0, 30.0, 0.0, -0.05];

        let meta = GeoMetadata::new(
            "EPSG:4326".to_string(),
            RasterSize { rows: 840, cols: 900 },
            TRANS,
            None,
        );
        let bbox = meta.bounding_box();

        assert_eq!(meta.top_left(), Point::new(-30.0, 30.0));
        assert_eq!(meta.bottom_right(), Point::new(60.0, -12.0));

        assert_eq!(*bbox.top_left(), meta.top_left());
        assert_eq!(*bbox.bottom_right(), meta.bottom_right());
    }

    #[test]
    fn point_calculations_zero_origin() {
        let meta = GeoMetadata::with_origin(
            String::new(),
            RasterSize { rows: 2, cols: 2 },
            Point::new(0.0, 0.0),
            CellSize::square(1.0),
            Option::<f64>::None,
        );

        assert_eq!(meta.cell_center(Cell::new(0, 0)), Point::new(0.5, 1.5));
        assert_eq!(meta.cell_center(Cell::new(1, 1)), Point::new(1.5, 0.5));

        assert_eq!(meta.cell_lower_left(Cell::new(0, 0)), Point::new(0.0, 1.0));
        assert_eq!(meta.cell_lower_left(Cell::new(2, 2)), Point::new(2.0, -1.0));

        assert_eq!(meta.top_left(), Point::new(0.0, 2.0));
        assert_eq!(meta.center(), Point::new(1.0, 1.0));
        assert_eq!(meta.bottom_right(), Point::new(2.0, 0.0));

        assert_eq!(meta.convert_x_to_col_fraction(-1.0), -1.0);
        assert_eq!(meta.convert_x_to_col_fraction(0.0), 0.0);
        assert_eq!(meta.convert_x_to_col_fraction(2.0), 2.0);
        assert_eq!(meta.convert_x_to_col_fraction(3.0), 3.0);

        assert_eq!(meta.convert_y_to_row_fraction(-1.0), 3.0);
        assert_eq!(meta.convert_y_to_row_fraction(0.0), 2.0);
        assert_eq!(meta.convert_y_to_row_fraction(2.0), 0.0);
        assert_eq!(meta.convert_y_to_row_fraction(3.0), -1.0);
    }

    #[test]
    fn point_calculations_non_negative_origin() {
        let meta = GeoMetadata::with_origin(
            String::new(),
            RasterSize { rows: 2, cols: 2 },
            Point::new(-1.0, -1.0),
            CellSize::square(1.0),
            Option::<f64>::None,
        );

        assert_eq!(meta.cell_center(Cell::new(0, 0)), Point::new(-0.5, 0.5));
        assert_eq!(meta.cell_center(Cell::new(1, 1)), Point::new(0.5, -0.5));

        assert_eq!(meta.cell_lower_left(Cell::new(0, 0)), Point::new(-1.0, 0.0));
        assert_eq!(meta.cell_lower_left(Cell::new(2, 2)), Point::new(1.0, -2.0));

        assert_eq!(meta.top_left(), Point::new(-1.0, 1.0));
        assert_eq!(meta.center(), Point::new(0.0, 0.0));
        assert_eq!(meta.bottom_right(), Point::new(1.0, -1.0));

        assert_eq!(meta.convert_x_to_col_fraction(0.0), 1.0);
        assert_eq!(meta.convert_y_to_row_fraction(0.0), 1.0);
        assert_eq!(meta.convert_x_to_col_fraction(2.0), 3.0);
        assert_eq!(meta.convert_y_to_row_fraction(2.0), -1.0);
    }

    #[test]
    fn point_calculations_non_positive_origin() {
        let meta = GeoMetadata::with_origin(
            String::new(),
            RasterSize { rows: 2, cols: 2 },
            Point::new(1.0, 1.0),
            CellSize::square(1.0),
            Option::<f64>::None,
        );

        assert_eq!(meta.cell_center(Cell::new(0, 0)), Point::new(1.5, 2.5));
        assert_eq!(meta.cell_center(Cell::new(1, 1)), Point::new(2.5, 1.5));

        assert_eq!(meta.top_left(), Point::new(1.0, 3.0));
        assert_eq!(meta.center(), Point::new(2.0, 2.0));
        assert_eq!(meta.bottom_right(), Point::new(3.0, 1.0));
    }

    #[test]
    fn test_metadata_intersects() {
        let meta_with_origin = |orig| {
            GeoMetadata::with_origin(
                String::new(),
                RasterSize { rows: 3, cols: 3 },
                orig,
                CellSize::square(5.0),
                Option::<f64>::None,
            )
        };

        let meta = meta_with_origin(Point::new(0.0, 0.0));

        assert!(metadata_intersects(&meta, &meta_with_origin(Point::new(10.0, 10.0))).unwrap());
        assert!(metadata_intersects(&meta, &meta_with_origin(Point::new(-10.0, -10.0))).unwrap());
        assert!(metadata_intersects(&meta, &meta_with_origin(Point::new(-10.0, 10.0))).unwrap());
        assert!(metadata_intersects(&meta, &meta_with_origin(Point::new(10.0, -10.0))).unwrap());

        assert!(!metadata_intersects(&meta, &meta_with_origin(Point::new(15.0, 15.0))).unwrap());
        assert!(!metadata_intersects(&meta, &meta_with_origin(Point::new(0.0, 15.0))).unwrap());
        assert!(!metadata_intersects(&meta, &meta_with_origin(Point::new(15.0, 0.0))).unwrap());
        assert!(!metadata_intersects(&meta, &meta_with_origin(Point::new(0.0, -15.0))).unwrap());
    }

    #[test]
    fn metadata_intersects_only_y_overlap() {
        let meta1 = GeoMetadata::with_origin(
            "",
            RasterSize { rows: 133, cols: 121 },
            Point::new(461_144.591_644_468_2, 6_609_204.087_706_049),
            CellSize::square(76.437_028_285_176_21),
            Option::<f64>::None,
        );

        let meta2 = GeoMetadata::with_origin(
            "",
            RasterSize { rows: 195, cols: 122 },
            Point::new(475_361.878_905_511, 6_607_216.724_970_634),
            CellSize::square(76.437_028_285_176_21),
            Option::<f64>::None,
        );

        assert!(!metadata_intersects(&meta1, &meta2).unwrap());
    }

    #[test]
    fn metadata_intersects_only_x_overlap() {
        let meta1 = GeoMetadata::with_origin(
            "",
            RasterSize { rows: 133, cols: 121 },
            Point::new(461_144.591_644_468_2, 6_609_204.087_706_049),
            CellSize::square(76.437_028_285_176_21),
            Option::<f64>::None,
        );

        let meta2 = GeoMetadata::with_origin(
            "",
            RasterSize { rows: 195, cols: 122 },
            Point::new(461_144.591_644_468_2, 6_807_216.724_970_634),
            CellSize::square(76.437_028_285_176_21),
            Option::<f64>::None,
        );

        assert!(!metadata_intersects(&meta1, &meta2).unwrap());
    }

    #[test]
    fn metadata_intersects_different_but_aligned_cellsize() {
        let meta1 = GeoMetadata::with_origin(
            "",
            RasterSize { rows: 3, cols: 3 },
            Point::new(0.0, 0.0),
            CellSize::square(10.0),
            Option::<f64>::None,
        );

        assert!(metadata_intersects(
            &meta1,
            &GeoMetadata::with_origin(
                "",
                RasterSize { rows: 4, cols: 4 },
                Point::new(10.0, 10.0),
                CellSize::square(5.0),
                Option::<f64>::None,
            )
        )
        .unwrap());

        assert!(!metadata_intersects(
            &meta1,
            &GeoMetadata::with_origin(
                "",
                RasterSize { rows: 4, cols: 4 },
                Point::new(30.0, 30.0),
                CellSize::square(5.0),
                Option::<f64>::None
            )
        )
        .unwrap());

        assert!(metadata_intersects(
            &meta1,
            &GeoMetadata::with_origin(
                String::new(),
                RasterSize { rows: 4, cols: 4 },
                Point::new(11.0, 10.0),
                CellSize::square(5.0),
                Option::<f64>::None
            )
        )
        .is_err_and(|e| e.to_string() == "Invalid argument: Extents cellsize does not match CellSize { x: 10.0, y: -10.0 } <-> CellSize { x: 5.0, y: -5.0 }"));

        assert!(metadata_intersects(
            &meta1,
            &GeoMetadata::with_origin(
                String::new(),
                RasterSize { rows: 4, cols: 4 },
                Point::new(10.0, 11.0),
                CellSize::square(5.0),
                Option::<f64>::None
            )
        )
        .is_err_and(|e| e.to_string() == "Invalid argument: Extents cellsize does not match CellSize { x: 10.0, y: -10.0 } <-> CellSize { x: 5.0, y: -5.0 }"));

        assert!(metadata_intersects(
            &GeoMetadata::with_origin(
                "",
                RasterSize { rows: 4, cols: 4 },
                Point::new(11.0, 10.0),
                CellSize::square(5.0),
                Option::<f64>::None
            ),
            &meta1,
        )
        .is_err_and(|e| e.to_string() == "Invalid argument: Extents cellsize does not match CellSize { x: 5.0, y: -5.0 } <-> CellSize { x: 10.0, y: -10.0 }"));

        assert!(metadata_intersects(
            &GeoMetadata::with_origin(
                "",
                RasterSize { rows: 4, cols: 4 },
                Point::new(10.0, 11.0),
                CellSize::square(5.0),
                Option::<f64>::None
            ),
            &meta1,
        )
        .is_err_and(|e| e.to_string() == "Invalid argument: Extents cellsize does not match CellSize { x: 5.0, y: -5.0 } <-> CellSize { x: 10.0, y: -10.0 }"));
    }

    #[test]
    fn metadata_set_bottom_left_coordinate() {
        let coord = Point::new(160000.0, 195000.0);

        let mut meta = GeoMetadata::with_origin(
            "",
            RasterSize { rows: 920, cols: 2370 },
            Point::new(22000.0, 153000.0),
            CellSize::square(100.0),
            Option::<f64>::None,
        );

        meta.set_extent(coord, RasterSize { rows: 1, cols: 1 }, CellSize::square(100.0));

        assert_eq!(meta.bottom_left(), coord);
    }
}
