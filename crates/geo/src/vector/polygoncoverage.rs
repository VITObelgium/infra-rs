use gdal::vector::{LayerAccess, ToGdal};
use geos::Geom;
use geozero::ToGeos;
use inf::progressinfo::AsyncProgressNotification;
use inf::{duration, Rect};
use rayon::prelude::*;
use std::ffi::CString;
use std::path::Path;

use crate::raster::{Cell, CellIterator};
use crate::spatialreference::CoordinateWarpTransformer;
use crate::{Error, GeoReference, Point, RasterSize, Result, SpatialReference};

use super::coveragetools::VectorBuilder;

#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct CellInfo {
    pub compute_grid_cell: Cell, // row column index of this cell in the full output grid
    pub polygon_grid_cell: Cell, // row column index of this cell in the polygon sub grid of the spatial pattern grid
    pub coverage: f64,           // The percentage of the polygon surface in this cell
    pub cell_coverage: f64,      // The percentage of this cell covered by the polygon
}

#[derive(Debug, Default, Clone)]
pub struct PolygonCellCoverage {
    pub id: u64,
    pub name: String,
    pub value: f64,
    pub output_subgrid_extent: GeoReference, // This polygon subgrid within the output grid
    pub cells: Vec<CellInfo>,
}

/// Specifies how to handle cells intersecting the border of the polygons
#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub enum BorderHandling {
    /// No specific border logic
    #[default]
    None,
    /// The coverage is divided over the polygons in a border cell (e.g Polygon1: 30%, Polygon2: 30%, 40% No intersections -> Polygon 1 and 2 are adjusted to 50%)
    AdjustCoverage,
}

fn warp_geometry(
    geom: &geos::Geometry,
    source_projection: &SpatialReference,
    dest_projection: &SpatialReference,
) -> Result<geos::Geometry> {
    let transfomer = CoordinateWarpTransformer::new(source_projection, dest_projection)?;

    let warped = geom.transform_xy(|x, y| match transfomer.transform_point((x, y).into()) {
        Ok(coord) => Some((coord.x(), coord.y())),
        Err(_) => None,
    })?;

    Ok(warped)
}

fn create_polygon_from_rect_as<TGeom: TryFrom<geo_types::Polygon>>(rect: Rect<f64>) -> Result<TGeom> {
    let polygon = geo_types::Polygon::new(
        geo_types::LineString::from(vec![
            rect.top_left(),
            rect.top_right(),
            rect.bottom_right(),
            rect.bottom_left(),
            rect.top_left(),
        ]),
        vec![],
    );

    match TGeom::try_from(polygon) {
        Ok(poly) => Ok(poly),
        Err(_) => Err(Error::InvalidArgument("Failed to create polygon geometry".to_string())),
    }
}

fn create_geometry_extent(geom: &geos::Geometry, grid_extent: &GeoReference) -> Result<GeoReference> {
    let mut geometry_extent = grid_extent.clone();

    let env = geom.envelope()?;

    let top_left = Point::new(env.get_x_min()?, env.get_y_max()?);
    let bottom_right = Point::new(env.get_x_max()?, env.get_y_min()?);

    let top_left_cell = grid_extent.point_to_cell(top_left);
    let bottom_right_cell = grid_extent.point_to_cell(bottom_right);

    let top_left_ll = grid_extent.cell_lower_left(top_left_cell);
    let bottom_right_ll = grid_extent.cell_lower_left(bottom_right_cell);

    let geom_rect = Rect::from_ne_sw(
        Point::new(top_left_ll.x(), top_left_ll.y() - grid_extent.cell_size_y()),
        Point::new(bottom_right_ll.x() + grid_extent.cell_size_x(), bottom_right_ll.y()),
    );

    let size = RasterSize {
        cols: (bottom_right_cell.col - top_left_cell.col) as usize + 1,
        rows: (bottom_right_cell.row - top_left_cell.row) as usize + 1,
    };
    geometry_extent.set_extent(
        Point::new(geom_rect.top_left().x(), geom_rect.bottom_right().y()),
        size,
        geometry_extent.cell_size(),
    );

    Ok(geometry_extent)
}

fn create_geometry_extent_for_srs(
    geom: &geos::Geometry,
    grid_extent: &GeoReference,
    mut source_projection: SpatialReference,
) -> Result<GeoReference> {
    let mut dest_proj = SpatialReference::from_proj(grid_extent.projection())?;

    if source_projection.epsg_cs() != dest_proj.epsg_cs() {
        let warped_geom = warp_geometry(geom, &source_projection, &dest_proj)?;
        create_geometry_extent(&warped_geom, grid_extent)
    } else {
        create_geometry_extent(geom, grid_extent)
    }
}

fn create_cell_coverages(
    extent: &GeoReference,
    polygon_extent: &GeoReference,
    geom: &geos::Geometry,
) -> Result<Vec<CellInfo>> {
    let mut result: Vec<CellInfo> = Vec::new();

    let prepared_geom = geom.to_prepared_geom()?;
    let cell_size = extent.cell_size();
    let cell_area = f64::abs(cell_size.x() * cell_size.y());

    let polygon_area = geom.area()?;

    for polygon_cell in CellIterator::for_raster_with_size(polygon_extent.raster_size()) {
        let bbox = polygon_extent.cell_bounding_box(polygon_cell);

        let cell_geom = create_polygon_from_rect_as::<geos::Geometry>(bbox)?;

        // Intersect it with the polygon
        let xy_centre = polygon_extent.cell_center(polygon_cell);
        let output_cell = extent.point_to_cell(xy_centre);

        if prepared_geom.within(&cell_geom)? {
            result.push(CellInfo {
                compute_grid_cell: output_cell,
                polygon_grid_cell: polygon_cell,
                coverage: 1.0,
                cell_coverage: polygon_area / cell_area,
            });
        } else if prepared_geom.contains_properly(&cell_geom)? {
            result.push(CellInfo {
                compute_grid_cell: output_cell,
                polygon_grid_cell: polygon_cell,
                coverage: cell_area / polygon_area,
                cell_coverage: 1.0,
            });
        } else if prepared_geom.intersects(&cell_geom)? {
            let intersect_geometry = geom.intersection(&cell_geom)?;
            let intersect_area = intersect_geometry.area()?;
            if intersect_area > 0.0 {
                result.push(CellInfo {
                    compute_grid_cell: output_cell,
                    polygon_grid_cell: polygon_cell,
                    coverage: intersect_area / polygon_area,
                    cell_coverage: intersect_area / cell_area,
                });
            }
        }
    }

    Ok(result)
}

fn create_polygon_coverage(
    polygon_id: u64,
    mut geometry: geos::Geometry,
    mut geometry_projection: SpatialReference,
    output_extent: &GeoReference,
) -> Result<PolygonCellCoverage> {
    let mut cov = PolygonCellCoverage::default();

    if geometry_projection.epsg_cs() != output_extent.projected_epsg() {
        geometry = warp_geometry(
            &geometry,
            &geometry_projection,
            &SpatialReference::from_proj(output_extent.projection())?,
        )?;
    }

    cov.id = polygon_id;
    cov.output_subgrid_extent = create_geometry_extent_for_srs(&geometry, output_extent, geometry_projection)?;
    cov.cells = create_cell_coverages(output_extent, &cov.output_subgrid_extent, &geometry)?;

    Ok(cov)
}

fn process_region_borders(cell_coverages: Vec<PolygonCellCoverage>) -> Result<Vec<PolygonCellCoverage>> {
    let result: Vec<PolygonCellCoverage> = cell_coverages
        .par_iter()
        .flat_map(|cov| -> Result<PolygonCellCoverage> {
            let region = &cov.name;
            let output_extent = &cov.output_subgrid_extent;
            let cells = &cov.cells;

            let mut intersecting_coverages: Vec<PolygonCellCoverage> = Vec::new();
            for test_cov in &cell_coverages {
                if test_cov.name != *region {
                    if let Ok(true) = output_extent.intersects(&test_cov.output_subgrid_extent) {
                        intersecting_coverages.push(test_cov.clone());
                    }
                }
            }

            if intersecting_coverages.is_empty() {
                Ok(cov.clone())
            } else {
                let mut modified_coverages: Vec<CellInfo> = Vec::with_capacity(cells.len());

                for cell in cells {
                    let mut modified_coverage = *cell;

                    if cell.coverage < 1.0 && cell.coverage > 0.0 {
                        // polygon border, check if there are other polygons in this cell
                        let mut other_polygon_coverages = 0.0;

                        for test_cov in &intersecting_coverages {
                            // Locate the current cell in the coverage of the other polygon
                            if let Ok(cell_iter) = test_cov
                                .cells
                                .binary_search_by(|cov| cov.compute_grid_cell.cmp(&cell.compute_grid_cell))
                            {
                                // the other polygon covers the cell
                                other_polygon_coverages += test_cov.cells[cell_iter].coverage;
                            }
                        }

                        if other_polygon_coverages == 0.0 {
                            // This is the only polygon in the cell, so we get all the coverage
                            modified_coverage.coverage = 1.0;
                        } else {
                            modified_coverage.coverage = cell.coverage / (cell.coverage + other_polygon_coverages);
                        }
                    }

                    modified_coverages.push(modified_coverage);
                }

                Ok(PolygonCellCoverage {
                    name: region.clone(),
                    cells: modified_coverages,
                    output_subgrid_extent: output_extent.clone(),
                    ..PolygonCellCoverage::default()
                })
            }
        })
        .collect();

    Ok(result)
}

#[derive(Debug, Clone)]
pub enum BurnValue {
    Value(f64),
    Field(String),
}

impl Default for BurnValue {
    fn default() -> Self {
        BurnValue::Value(1.0)
    }
}

fn required_layer_field_index(layer: &gdal::vector::Layer, field_name: &str) -> Result<i32> {
    let field_name_c_str = CString::new(field_name)?;
    let field_index = unsafe { gdal_sys::OGR_L_FindFieldIndex(layer.c_layer(), field_name_c_str.as_ptr(), 1) };

    if field_index < 0 {
        Err(Error::Runtime(format!(
            "Field '{}' not found in layer '{}'",
            field_name,
            layer.name()
        )))
    } else {
        Ok(field_index)
    }
}

#[derive(Debug, Default, Clone)]
pub struct CoverageConfiguration {
    pub border_handling: BorderHandling,
    pub burn_value: BurnValue,
    pub attribute_filter: Option<String>,
    pub input_layer: Option<String>,
    pub name_field: Option<String>,
}

pub struct CoverageData {
    pub extent: GeoReference,
    pub polygons: Vec<PolygonCellCoverage>,
}

impl CoverageData {
    fn build_vector(&self) -> Result<VectorBuilder> {
        let mut builder = VectorBuilder::with_layer("cell coverages", self.extent.projection())?;
        builder.add_field("row", gdal::vector::OGRFieldType::OFTInteger)?;
        builder.add_field("col", gdal::vector::OGRFieldType::OFTInteger)?;
        builder.add_field("coverage", gdal::vector::OGRFieldType::OFTReal)?;
        builder.add_field("cellcoverage", gdal::vector::OGRFieldType::OFTReal)?;
        builder.add_field("name", gdal::vector::OGRFieldType::OFTString)?;

        for polygon in &self.polygons {
            for cell_info in &polygon.cells {
                let cell_bbox = polygon
                    .output_subgrid_extent
                    .cell_bounding_box(cell_info.polygon_grid_cell);

                let cell_polygon = geo_types::Polygon::from(cell_bbox);

                builder.add_named_cell_geometry_with_coverage(
                    cell_info.compute_grid_cell,
                    cell_info.coverage,
                    cell_info.cell_coverage,
                    &polygon.name,
                    cell_polygon.to_gdal()?,
                )?;
            }
        }

        Ok(builder)
    }

    pub fn to_geojson(&self) -> Result<String> {
        self.build_vector()?.into_geojson()
    }

    pub fn store(&self, path: &Path) -> Result<()> {
        self.build_vector()?.store(path)?;
        Ok(())
    }
}

pub fn create_geometries(
    vector_ds: &gdal::Dataset,
    output_extent: &GeoReference,
    config: &CoverageConfiguration,
) -> Result<Vec<(u64, f64, String, geos::Geometry)>> {
    let mut geometries: Vec<(u64, f64, String, geos::Geometry)> = Vec::new();

    for i in 0..vector_ds.layer_count() {
        let mut layer = vector_ds.layer(i)?;

        if let Some(ref input_layer) = config.input_layer {
            if layer.name() != *input_layer {
                continue;
            }
        }

        let (value_column, burn) = match config.burn_value {
            BurnValue::Value(val) => (None, val),
            BurnValue::Field(ref burn_field) => (Some(required_layer_field_index(&layer, burn_field)?), 0.0),
        };

        let name_column = match config.name_field {
            Some(ref name) => Some(required_layer_field_index(&layer, name)?),
            None => None,
        };

        assert!(!output_extent.projection().is_empty());

        if let Some(srs) = layer.spatial_ref() {
            let mut layer_srs = SpatialReference::new(srs);
            if output_extent.projected_epsg() != layer_srs.epsg_cs() {
                return Err(Error::InvalidArgument(format!(
                    "Projection mismatch between input vector and metadata grid EPSG:{:?} <-> EPSG:{:?}",
                    output_extent.projected_epsg(),
                    layer_srs.epsg_cs()
                )));
            }
        } else {
            return Err(Error::InvalidArgument(
                "Invalid input vector: No projection information available".to_string(),
            ));
        }

        let bbox = output_extent.bounding_box();
        let top_left = bbox.top_left();
        let bottom_right = bbox.bottom_right();

        layer.set_spatial_filter(&gdal::vector::Geometry::bbox(
            top_left.x(),
            bottom_right.y(),
            bottom_right.x(),
            top_left.y(),
        )?);

        if let Some(filter) = &config.attribute_filter {
            layer.set_attribute_filter(filter)?;
        }

        for feature in layer.features() {
            // Read the burn value from the field or use the fixed burn value
            let value = match value_column {
                Some(idx) => feature.field_as_double(idx)?.unwrap_or(0.0),
                None => burn,
            };

            // Add an optional name to the coverage
            let name = match name_column {
                Some(idx) => feature.field_as_string(idx)?.unwrap_or_default(),
                None => String::new(),
            };

            if let Some(geom) = feature.geometry() {
                geometries.push((feature.fid().unwrap_or(0), value, name, geom.to_geos()?));
            }
        }
    }

    Ok(geometries)
}

pub fn create_polygon_coverages(
    vector_ds: &gdal::Dataset,
    output_extent: &GeoReference,
    config: CoverageConfiguration,
    progress_cb: impl AsyncProgressNotification,
) -> Result<CoverageData> {
    let mut geometries = create_geometries(vector_ds, output_extent, &config)?;

    log::debug!("Create cell coverages");
    let rec = duration::Recorder::new();

    // sort on geometry complexity, so we always start processing the most complex geometries
    // this avoids processing the most complext geometry in the end on a single core
    geometries.sort_by(|lhs, rhs| rhs.3.get_num_points().cmp(&lhs.3.get_num_points()));

    // export to string and import in every loop instance, accessing the spatial reference
    // from multiple threads is not thread safe
    let projection = output_extent.projection().to_string();

    progress_cb.reset(geometries.len() as u64);

    let mut result: Vec<PolygonCellCoverage> = geometries
        .into_par_iter()
        .flat_map(|id_geom| -> Result<PolygonCellCoverage> {
            let mut cov = create_polygon_coverage(
                id_geom.0,
                id_geom.3,
                SpatialReference::from_proj(&projection)?,
                output_extent,
            )?;
            cov.value = id_geom.1;
            cov.name = id_geom.2.clone();
            progress_cb.tick()?;

            Ok(cov)
        })
        .collect();

    log::debug!("Create cell coverages took: {}", rec.elapsed_time_string());
    if config.border_handling == BorderHandling::AdjustCoverage {
        let rec = duration::Recorder::new();
        // Update the coverages on the polygon borders to get appropriate coverage values at the edges
        // E.g. a cell on the border that is only covered by 1 polygon for 50% should be modified to 100%
        // Because the data is only for inside the region
        result = process_region_borders(result)?;
        log::debug!("Processing polygon borders took: {}", rec.elapsed_time_string());
    }

    Ok(CoverageData {
        polygons: result,
        extent: output_extent.clone(),
    })
}
