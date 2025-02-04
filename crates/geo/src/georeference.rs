use crate::{array::ArrayMetadata, raster::RasterSize, tile::ZoomLevelStrategy, Cell};
use approx::{AbsDiffEq, RelativeEq};
use num::{NumCast, ToPrimitive};

use crate::{
    crs::{self, Epsg},
    Error, LatLonBounds, Point, Rect, Result, Tile,
};

#[cfg(feature = "gdal")]
use crate::spatialreference::{projection_from_epsg, projection_to_epsg, projection_to_geo_epsg};

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

    fn relative_eq(&self, other: &Self, epsilon: <f64 as AbsDiffEq>::Epsilon, max_relative: <f64 as AbsDiffEq>::Epsilon) -> bool {
        f64::relative_eq(&self.x, &other.x, epsilon, max_relative) && f64::relative_eq(&self.y, &other.y, epsilon, max_relative)
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
pub struct GeoReference {
    /// The proj projection string
    projection: String,
    /// The size of the image in pixels (width, height)
    size: RasterSize,
    /// The affine transformation.
    geo_transform: [f64; 6],
    /// The nodata value.
    nodata: Option<f64>,
}

impl GeoReference {
    pub fn new<S: Into<String>>(projection: S, size: RasterSize, geo_transform: [f64; 6], nodata: Option<f64>) -> Self {
        GeoReference {
            projection: projection.into(),
            size,
            geo_transform,
            nodata,
        }
    }

    #[cfg(feature = "gdal")]
    pub fn from_file(path: &std::path::Path) -> Result<Self> {
        crate::raster::io::dataset::read_file_metadata(path)
    }

    pub fn raster_size(&self) -> RasterSize {
        self.size
    }

    pub const fn without_spatial_reference(size: RasterSize, nodata: Option<f64>) -> Self {
        GeoReference {
            projection: String::new(),
            size,
            geo_transform: [0.0; 6],
            nodata,
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

        GeoReference {
            projection: projection.into(),
            size,
            geo_transform,
            nodata,
        }
    }

    pub fn from_tile(tile: &Tile, tile_size: usize, dpi_ratio: u8) -> Self {
        let tile_size = tile_size * dpi_ratio as usize;
        let raster_size = RasterSize::with_rows_cols(tile_size, tile_size);
        let pixel_size = Tile::pixel_size_at_zoom_level(tile.z) / dpi_ratio as f64;
        GeoReference::with_origin(
            "",
            raster_size,
            crs::lat_lon_to_web_mercator(tile.bounds().southwest()),
            CellSize::square(pixel_size),
            Some(f64::NAN),
        )
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
        GeoReference {
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

    pub fn cell_at_index(&self, index: usize) -> Cell {
        Cell::from_row_col((index / self.columns()) as i32, (index % self.columns()) as i32)
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

    pub fn cell_bounding_box(&self, cell: Cell) -> Rect<f64> {
        let ll = self.cell_lower_left(cell);

        Rect::from_ne_sw(
            Point::new(ll.x(), ll.y() - self.cell_size_y()),
            Point::new(ll.x() + self.cell_size_x(), ll.y()),
        )
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
        Cell::from_row_col(self.y_to_row(p.y()), self.x_to_col(p.x()))
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

    pub fn nodata_as<T: num::NumCast>(&self) -> Result<Option<T>> {
        match self.nodata {
            None => Ok(None),
            Some(nodata) => match num::NumCast::from(nodata) {
                Some(nodata) => Ok(Some(nodata)),
                None => Err(Error::InvalidArgument("Failed to convert nodata value".to_string())),
            },
        }
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

    #[cfg(feature = "gdal")]
    pub fn set_projection_from_epsg(&mut self, #[allow(unused)] epsg: Epsg) -> Result<()> {
        self.projection = projection_from_epsg(epsg)?;
        Ok(())
    }

    pub fn intersects(&self, other: &GeoReference) -> Result<bool> {
        if self.projection != other.projection {
            return Err(Error::InvalidArgument(
                "Cannot intersect metadata with different projections".to_string(),
            ));
        }

        if self.cell_size() != other.cell_size() && !self.is_aligned_with(other) {
            return Err(Error::InvalidArgument(format!(
                "Extents cellsize does not match {:?} <-> {:?}",
                self.cell_size(),
                other.cell_size()
            )));
        }

        if self.cell_size().x == 0.0 {
            panic!("Extents cellsize is zero");
        }

        Ok(self.bounding_box().intersects(&other.bounding_box()))
    }

    pub fn is_aligned_with(&self, other: &GeoReference) -> bool {
        let cell_size_x1 = self.cell_size_x();
        let cell_size_x2 = other.cell_size_x();

        let cell_size_y1 = self.cell_size_y().abs();
        let cell_size_y2 = other.cell_size_y().abs();

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

        let x_aligned = is_aligned(self.geo_transform[0], other.geo_transform[0], self.cell_size_x());
        let y_aligned = is_aligned(self.geo_transform[3], other.geo_transform[3], self.cell_size_y());

        x_aligned && y_aligned
    }

    #[cfg(feature = "gdal")]
    pub fn warped(&self, target_srs: &crate::SpatialReference) -> Result<Self> {
        use crate::gdalinterop;

        if self.projection.is_empty() {
            return Err(Error::InvalidArgument(
                "Cannot warp metadata without projection information".to_string(),
            ));
        }

        let target_projection = target_srs.to_wkt()?;

        let mem_driver = gdal::DriverManager::get_driver_by_name("MEM")?;
        let mut src_ds = mem_driver.create("in-mem", self.columns(), self.rows(), 0)?;
        src_ds.set_geo_transform(&self.geo_transform)?;
        src_ds.set_projection(&self.projection)?;

        // Create a transformer that maps from source pixel/line coordinates
        // to destination georeferenced coordinates (not destination pixel line).
        // We do that by omitting the destination dataset handle (setting it to nullptr).
        unsafe {
            let target_srs = std::ffi::CString::new(target_projection.clone())?;
            let transformer_arg = gdalinterop::check_pointer(
                gdal_sys::GDALCreateGenImgProjTransformer(
                    src_ds.c_dataset(),
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    target_srs.as_ptr(),
                    gdalinterop::FALSE,
                    0.0,
                    0,
                ),
                "Failed to create projection transformer",
            )?;

            let mut target_transform: gdal::GeoTransform = [0.0; 6];
            let mut rows: std::ffi::c_int = 0;
            let mut cols: std::ffi::c_int = 0;

            let warp_rc = gdal_sys::GDALSuggestedWarpOutput(
                src_ds.c_dataset(),
                Some(gdal_sys::GDALGenImgProjTransform),
                transformer_arg,
                target_transform.as_mut_ptr(),
                &mut cols,
                &mut rows,
            );

            gdal_sys::GDALDestroyGenImgProjTransformer(transformer_arg);

            match crate::gdalinterop::check_rc(warp_rc) {
                Ok(_) => Ok(GeoReference::new(
                    target_projection,
                    RasterSize {
                        rows: rows as usize,
                        cols: cols as usize,
                    },
                    target_transform,
                    self.nodata,
                )),
                Err(e) => {
                    gdal_sys::GDALDestroyGenImgProjTransformer(transformer_arg);
                    Err(e.into())
                }
            }
        }
    }

    #[cfg(feature = "gdal")]
    pub fn warped_to_epsg(&self, epsg: Epsg) -> Result<Self> {
        self.warped(&crate::SpatialReference::from_epsg(epsg)?)
    }

    #[cfg(feature = "gdal")]
    pub fn aligned_to_xyz_tiles_for_zoom_level(&self, zoom_level: i32) -> Result<GeoReference> {
        /// Create a new `GeoReference` that is aligned to the XYZ tile grid used for serving tiles.
        /// Such an aligned grid is used as a warping target for rasters from which tiles can be extracted
        /// and served as XYZ tiles.
        use crate::{crs, Tile};

        if self.projection.is_empty() {
            return Err(Error::InvalidArgument(
                "Cannot align metadata without projection information".to_string(),
            ));
        }

        let wgs84_meta = self.warped_to_epsg(crs::epsg::WGS84_WEB_MERCATOR)?;

        let top_left = crs::web_mercator_to_lat_lon(wgs84_meta.top_left());
        let bottom_right = crs::web_mercator_to_lat_lon(wgs84_meta.bottom_right());

        let top_left_tile = crate::Tile::for_coordinate(top_left, zoom_level).web_mercator_bounds();
        let bottom_right_tile = crate::Tile::for_coordinate(bottom_right, zoom_level).web_mercator_bounds();

        let cell_size = (top_left_tile.bottom_right().x() - top_left_tile.top_left().x()) / Tile::TILE_SIZE as f64;
        let raster_size = RasterSize {
            rows: ((top_left_tile.top_left().y() - bottom_right_tile.bottom_right().y()) / cell_size).ceil() as usize,
            cols: ((bottom_right_tile.bottom_right().x() - top_left_tile.top_left().x()) / cell_size).ceil() as usize,
        };

        let mut result = GeoReference::default();
        result.set_extent(top_left_tile.bottom_left(), raster_size, CellSize::square(cell_size));
        result.set_projection_from_epsg(crs::epsg::WGS84_WEB_MERCATOR)?;
        result.set_nodata(self.nodata);
        Ok(result)
    }

    #[cfg(feature = "gdal")]
    /// Create a new `GeoReference` that is aligned to the XYZ tile grid used for serving tiles.
    /// Such an aligned grid is used as a warping target for rasters from which tiles can be extracted
    /// and served as XYZ tiles.
    pub fn aligned_to_xyz_tiles_auto_detect_zoom_level(&self, strategy: ZoomLevelStrategy) -> Result<GeoReference> {
        let zoom = Tile::zoom_level_for_pixel_size(self.cell_size_x(), strategy);
        self.aligned_to_xyz_tiles_for_zoom_level(zoom)
    }

    pub fn intersection(&self, other: &GeoReference) -> Result<GeoReference> {
        if self.projection() != other.projection() {
            return Err(Error::InvalidArgument(
                "Cannot intersect georeferences with different projections".to_string(),
            ));
        }

        if self.cell_size() != other.cell_size() {
            return Err(Error::InvalidArgument(
                "Cannot intersect georeferences with different cell sizes".to_string(),
            ));
        }

        if !self.is_aligned_with(other) {
            return Err(Error::InvalidArgument(
                "Cannot intersect georeferences that are not aligned".to_string(),
            ));
        }

        let intersection = self.bounding_box().intersection(&other.bounding_box());
        if !intersection.empty() {
            let raster_size = RasterSize {
                rows: (intersection.height() / self.cell_size_y().abs()).round() as usize,
                cols: (intersection.width() / self.cell_size_x().abs()).round() as usize,
            };

            Ok(GeoReference::with_origin(
                self.projection(),
                raster_size,
                intersection.bottom_left(),
                self.cell_size(),
                self.nodata,
            ))
        } else {
            Ok(GeoReference::default())
        }
    }
}

impl ArrayMetadata for GeoReference {
    fn size(&self) -> RasterSize {
        self.size
    }

    fn with_size(size: RasterSize) -> Self {
        self::GeoReference::without_spatial_reference(size, None)
    }

    fn with_rows_cols(rows: usize, cols: usize) -> Self {
        self::GeoReference::without_spatial_reference(RasterSize::with_rows_cols(rows, cols), None)
    }
}

fn is_aligned(val1: f64, val2: f64, cellsize: f64) -> bool {
    let diff = (val1 - val2).abs();
    diff % cellsize < 1e-12
}

#[cfg(test)]
mod tests {

    use crate::CellSize;

    use super::*;

    #[test]
    fn bounding_box_zero_origin() {
        let meta = GeoReference::with_origin(
            String::new(),
            RasterSize { rows: 10, cols: 5 },
            Point::new(0.0, 0.0),
            CellSize::square(5.0),
            Option::<f64>::None,
        );

        let bbox = meta.bounding_box();
        assert_eq!(bbox.top_left(), Point::new(0.0, 50.0));
        assert_eq!(bbox.bottom_right(), Point::new(25.0, 0.0));
    }

    #[test]
    fn bounding_box_negative_y_origin() {
        let meta = GeoReference::with_origin(
            String::new(),
            RasterSize { rows: 2, cols: 2 },
            Point::new(9.0, -10.0),
            CellSize::square(4.0),
            Option::<f64>::None,
        );

        let bbox = meta.bounding_box();
        assert_eq!(bbox.top_left(), Point::new(9.0, -2.0));
        assert_eq!(bbox.bottom_right(), Point::new(17.0, -10.0));
    }

    #[test]
    fn bounding_box_epsg_4326() {
        const TRANS: [f64; 6] = [-30.0, 0.100, 0.0, 30.0, 0.0, -0.05];

        let meta = GeoReference::new("EPSG:4326".to_string(), RasterSize { rows: 840, cols: 900 }, TRANS, None);
        let bbox = meta.bounding_box();

        assert_eq!(meta.top_left(), Point::new(-30.0, 30.0));
        assert_eq!(meta.bottom_right(), Point::new(60.0, -12.0));

        assert_eq!(bbox.top_left(), meta.top_left());
        assert_eq!(bbox.bottom_right(), meta.bottom_right());
    }

    #[test]
    fn point_calculations_zero_origin() {
        let meta = GeoReference::with_origin(
            String::new(),
            RasterSize { rows: 2, cols: 2 },
            Point::new(0.0, 0.0),
            CellSize::square(1.0),
            Option::<f64>::None,
        );

        assert_eq!(meta.cell_center(Cell::from_row_col(0, 0)), Point::new(0.5, 1.5));
        assert_eq!(meta.cell_center(Cell::from_row_col(1, 1)), Point::new(1.5, 0.5));

        assert_eq!(meta.cell_lower_left(Cell::from_row_col(0, 0)), Point::new(0.0, 1.0));
        assert_eq!(meta.cell_lower_left(Cell::from_row_col(2, 2)), Point::new(2.0, -1.0));

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
        let meta = GeoReference::with_origin(
            String::new(),
            RasterSize { rows: 2, cols: 2 },
            Point::new(-1.0, -1.0),
            CellSize::square(1.0),
            Option::<f64>::None,
        );

        assert_eq!(meta.cell_center(Cell::from_row_col(0, 0)), Point::new(-0.5, 0.5));
        assert_eq!(meta.cell_center(Cell::from_row_col(1, 1)), Point::new(0.5, -0.5));

        assert_eq!(meta.cell_lower_left(Cell::from_row_col(0, 0)), Point::new(-1.0, 0.0));
        assert_eq!(meta.cell_lower_left(Cell::from_row_col(2, 2)), Point::new(1.0, -2.0));

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
        let meta = GeoReference::with_origin(
            String::new(),
            RasterSize { rows: 2, cols: 2 },
            Point::new(1.0, 1.0),
            CellSize::square(1.0),
            Option::<f64>::None,
        );

        assert_eq!(meta.cell_center(Cell::from_row_col(0, 0)), Point::new(1.5, 2.5));
        assert_eq!(meta.cell_center(Cell::from_row_col(1, 1)), Point::new(2.5, 1.5));

        assert_eq!(meta.top_left(), Point::new(1.0, 3.0));
        assert_eq!(meta.center(), Point::new(2.0, 2.0));
        assert_eq!(meta.bottom_right(), Point::new(3.0, 1.0));
    }

    #[test]
    fn test_metadata_intersects() {
        let meta_with_origin = |orig| {
            GeoReference::with_origin(
                String::new(),
                RasterSize { rows: 3, cols: 3 },
                orig,
                CellSize::square(5.0),
                Option::<f64>::None,
            )
        };

        let meta = meta_with_origin(Point::new(0.0, 0.0));

        assert!(meta.intersects(&meta_with_origin(Point::new(10.0, 10.0))).unwrap());
        assert!(meta.intersects(&meta_with_origin(Point::new(-10.0, -10.0))).unwrap());
        assert!(meta.intersects(&meta_with_origin(Point::new(-10.0, 10.0))).unwrap());
        assert!(meta.intersects(&meta_with_origin(Point::new(10.0, -10.0))).unwrap());

        assert!(!meta.intersects(&meta_with_origin(Point::new(15.0, 15.0))).unwrap());
        assert!(!meta.intersects(&meta_with_origin(Point::new(0.0, 15.0))).unwrap());
        assert!(!meta.intersects(&meta_with_origin(Point::new(15.0, 0.0))).unwrap());
        assert!(!meta.intersects(&meta_with_origin(Point::new(0.0, -15.0))).unwrap());
    }

    #[test]
    fn metadata_intersects_only_y_overlap() {
        let meta1 = GeoReference::with_origin(
            "",
            RasterSize { rows: 133, cols: 121 },
            Point::new(461_144.591_644_468_2, 6_609_204.087_706_049),
            CellSize::square(76.437_028_285_176_21),
            Option::<f64>::None,
        );

        let meta2 = GeoReference::with_origin(
            "",
            RasterSize { rows: 195, cols: 122 },
            Point::new(475_361.878_905_511, 6_607_216.724_970_634),
            CellSize::square(76.437_028_285_176_21),
            Option::<f64>::None,
        );

        assert!(!meta1.intersects(&meta2).unwrap());
    }

    #[test]
    fn metadata_intersects_only_x_overlap() {
        let meta1 = GeoReference::with_origin(
            "",
            RasterSize { rows: 133, cols: 121 },
            Point::new(461_144.591_644_468_2, 6_609_204.087_706_049),
            CellSize::square(76.437_028_285_176_21),
            Option::<f64>::None,
        );

        let meta2 = GeoReference::with_origin(
            "",
            RasterSize { rows: 195, cols: 122 },
            Point::new(461_144.591_644_468_2, 6_807_216.724_970_634),
            CellSize::square(76.437_028_285_176_21),
            Option::<f64>::None,
        );

        assert!(!meta1.intersects(&meta2).unwrap());
    }

    #[test]
    fn metadata_intersects_different_but_aligned_cellsize() {
        let meta1 = GeoReference::with_origin(
            "",
            RasterSize { rows: 3, cols: 3 },
            Point::new(0.0, 0.0),
            CellSize::square(10.0),
            Option::<f64>::None,
        );

        assert!(meta1
            .intersects(&GeoReference::with_origin(
                "",
                RasterSize { rows: 4, cols: 4 },
                Point::new(10.0, 10.0),
                CellSize::square(5.0),
                Option::<f64>::None,
            ))
            .unwrap());

        assert!(!&meta1
            .intersects(&GeoReference::with_origin(
                "",
                RasterSize { rows: 4, cols: 4 },
                Point::new(30.0, 30.0),
                CellSize::square(5.0),
                Option::<f64>::None
            ))
            .unwrap());

        assert!(meta1
            .intersects(&GeoReference::with_origin(
                String::new(),
                RasterSize { rows: 4, cols: 4 },
                Point::new(11.0, 10.0),
                CellSize::square(5.0),
                Option::<f64>::None
            ))
            .is_err_and(|e| {
                assert_eq!(
                    e.to_string(),
                    "Invalid argument: Extents cellsize does not match CellSize { x: 10.0, y: -10.0 } <-> CellSize { x: 5.0, y: -5.0 }"
                );
                true
            }));

        assert!(meta1
            .intersects(&GeoReference::with_origin(
                String::new(),
                RasterSize { rows: 4, cols: 4 },
                Point::new(10.0, 11.0),
                CellSize::square(5.0),
                Option::<f64>::None
            ))
            .is_err_and(|e| {
                assert_eq!(
                    e.to_string(),
                    "Invalid argument: Extents cellsize does not match CellSize { x: 10.0, y: -10.0 } <-> CellSize { x: 5.0, y: -5.0 }"
                );
                true
            }));

        assert!(GeoReference::with_origin(
            "",
            RasterSize { rows: 4, cols: 4 },
            Point::new(11.0, 10.0),
            CellSize::square(5.0),
            Option::<f64>::None
        )
        .intersects(&meta1)
        .is_err_and(|e| e.to_string()
            == "Invalid argument: Extents cellsize does not match CellSize { x: 5.0, y: -5.0 } <-> CellSize { x: 10.0, y: -10.0 }"));

        assert!(GeoReference::with_origin(
            "",
            RasterSize { rows: 4, cols: 4 },
            Point::new(10.0, 11.0),
            CellSize::square(5.0),
            Option::<f64>::None
        )
        .intersects(&meta1)
        .is_err_and(|e| e.to_string()
            == "Invalid argument: Extents cellsize does not match CellSize { x: 5.0, y: -5.0 } <-> CellSize { x: 10.0, y: -10.0 }"));
    }

    #[test]
    fn metadata_set_bottom_left_coordinate() {
        let coord = Point::new(160000.0, 195000.0);

        let mut meta = GeoReference::with_origin(
            "",
            RasterSize { rows: 920, cols: 2370 },
            Point::new(22000.0, 153000.0),
            CellSize::square(100.0),
            Option::<f64>::None,
        );

        meta.set_extent(coord, RasterSize { rows: 1, cols: 1 }, CellSize::square(100.0));

        assert_eq!(meta.bottom_left(), coord);
    }

    #[test]
    #[cfg(feature = "gdal")]
    fn warp_metadata() {
        use approx::assert_relative_eq;

        let meta = GeoReference::with_origin(
            "EPSG:31370",
            RasterSize { rows: 120, cols: 144 },
            Point::new(-219000.0, -100000.0),
            CellSize::square(5000.0),
            Option::<f64>::None,
        );

        let warped = meta.warped_to_epsg(4326.into()).unwrap();

        assert_eq!(warped.projected_epsg(), Some(4326.into()));
        assert_eq!(warped.raster_size(), RasterSize { rows: 89, cols: 176 });
        assert_relative_eq!(warped.cell_size(), CellSize::square(0.062023851850733745), epsilon = 1e-10);
    }

    #[test]
    #[cfg(feature = "gdal")]
    fn test_tile_for_coordinate() {
        use core::f32;

        use crate::{crs, Coordinate, SpatialReference, Tile};

        let coord = Coordinate::latlon(51.0, 4.0);
        let tile = Tile::for_coordinate(coord, 9);

        let raster_size = RasterSize::with_rows_cols(Tile::TILE_SIZE as usize, Tile::TILE_SIZE as usize);
        let pixel_size = Tile::pixel_size_at_zoom_level(tile.z);
        let srs = SpatialReference::from_epsg(crs::epsg::WGS84_WEB_MERCATOR)
            .unwrap()
            .to_wkt()
            .unwrap();

        let mut tile_meta = GeoReference::with_origin(
            srs,
            raster_size,
            tile.bounds().southwest().into(),
            CellSize::square(pixel_size),
            Some(f32::NAN),
        );

        println!("{:?} {:?}", tile_meta, coord);
        let cell = tile_meta.point_to_cell(coord.into());
        println!("{:?}", cell);
        let ll = tile_meta.cell_lower_left(cell);
        println!("{:?}", ll);
        tile_meta.set_extent(ll, RasterSize::with_rows_cols(1, 1), tile_meta.cell_size());
    }
}
