use crate::Cell;
use crate::gdalinterop;
use gdal::vector::Feature;
use gdal::vector::LayerAccess;

use crate::Error;
use crate::Result;

use super::algo;
use super::io;
use super::io::LayerAccessExtension;

pub struct VectorBuilder {
    layer: gdal::vector::OwnedLayer,
}

impl VectorBuilder {
    pub fn with_layer(name: &str, projection: &str) -> Result<Self> {
        let mut ds = io::dataset::create_in_memory()?;
        let srs = gdal::spatial_ref::SpatialRef::from_definition(projection)?;
        ds.create_layer(gdal::vector::LayerOptions {
            name,
            ty: gdal::vector::OGRwkbGeometryType::wkbPolygon,
            srs: Some(&srs),
            ..Default::default()
        })?;

        Ok(Self { layer: ds.into_layer(0)? })
    }

    /// Add a field to the layer and return the index of the field
    pub fn add_field(&mut self, name: &str, field_type: gdal::vector::OGRFieldType::Type) -> Result<usize> {
        self.layer.create_defn_fields(&[(name, field_type)])?;
        self.layer.field_index_with_name(name)
        //io::layer_field_index(&self.layer, name)
    }

    // pub fn add_cell_geometry(&mut self, cell: Cell, geom: gdal::vector::Geometry) -> Result<()> {
    //     use gdal::vector::FieldValue;

    //     self.layer.create_feature_fields(
    //         geom,
    //         &["row", "col"],
    //         &[FieldValue::IntegerValue(cell.row), FieldValue::IntegerValue(cell.col)],
    //     )?;

    //     Ok(())
    // }

    // pub fn add_cell_geometry_with_coverage(
    //     &mut self,
    //     cell: Cell,
    //     coverage: f64,
    //     geom: gdal::vector::Geometry,
    // ) -> Result<()> {
    //     use gdal::vector::FieldValue;

    //     self.layer.create_feature_fields(
    //         geom,
    //         &["row", "col", "coverage"],
    //         &[
    //             FieldValue::IntegerValue(cell.row),
    //             FieldValue::IntegerValue(cell.col),
    //             FieldValue::RealValue(coverage),
    //         ],
    //     )?;

    //     Ok(())
    // }

    pub fn add_named_cell_geometry_with_coverage(
        &mut self,
        cell: Cell,
        coverage: f64,
        cell_coverage: f64,
        name: &str,
        geom: gdal::vector::Geometry,
    ) -> Result<()> {
        let defn = self.layer.defn();
        let mut ft = Feature::new(defn)?;
        ft.set_geometry(geom)?;
        ft.set_field_integer(defn.field_index("row")?, cell.row)?;
        ft.set_field_integer(defn.field_index("col")?, cell.col)?;
        ft.set_field_double(defn.field_index("coverage")?, coverage)?;
        ft.set_field_double(defn.field_index("cellcoverage")?, cell_coverage)?;
        ft.set_field_string(defn.field_index("name")?, name)?;

        ft.create(&self.layer)?;

        Ok(())
    }

    pub fn store(self, path: &std::path::Path) -> Result<()> {
        let ds = self.layer.into_dataset();
        algo::translate_ds_to_disk(&ds, path, &[])?;
        Ok(())
    }

    pub fn into_geojson(self) -> Result<String> {
        let ds = self.layer.into_dataset();
        let mem_file = gdalinterop::MemoryFile::empty(std::path::Path::new("/vsimem/json_serialization.geojson"))?;

        algo::translate_ds_to_disk(&ds, mem_file.path(), &[])?;

        match std::str::from_utf8(mem_file.as_slice()?) {
            Ok(json_data) => Ok(json_data.to_string()),
            Err(e) => Err(Error::Runtime(format!("Failed to convert json data to utf8 ({})", e))),
        }
    }
}
