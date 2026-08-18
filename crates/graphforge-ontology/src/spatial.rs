//! Canonical GeoArrow spatial property contract.
//!
//! GraphForge owns typed spatial values and their Arrow representation. It does
//! not own projection, map viewports, painting, or interaction.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use arrow::array::{Array, Float64Array, ListArray, StructArray};
use arrow::datatypes::{DataType, Field, Fields};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Arrow extension metadata keys defined by the Arrow extension-type contract.
pub const EXTENSION_NAME_KEY: &str = "ARROW:extension:name";
/// Arrow metadata key carrying the GeoArrow JSON metadata object.
pub const EXTENSION_METADATA_KEY: &str = "ARROW:extension:metadata";
const WEB_MERCATOR_MAX: f64 = 20_037_508.342_789_244;

/// Resource limits applied before a spatial array is accepted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpatialValidationLimits {
    /// Maximum top-level geometry rows.
    pub max_geometries: usize,
    /// Maximum total coordinate vertices.
    pub max_vertices: usize,
    /// Maximum in-memory Arrow array size.
    pub max_bytes: usize,
}

impl Default for SpatialValidationLimits {
    fn default() -> Self {
        Self {
            max_geometries: 1_000_000,
            max_vertices: 10_000_000,
            max_bytes: 256 * 1024 * 1024,
        }
    }
}

/// Stable, value-safe validation failures. Messages never contain coordinates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum SpatialValidationError {
    /// Field storage type differs from the canonical geometry layout.
    #[error("spatial field does not use the canonical GeoArrow type")]
    TypeMismatch,
    /// Extension name or metadata differs from the canonical contract.
    #[error("spatial field has missing or non-canonical GeoArrow metadata")]
    MetadataMismatch,
    /// Runtime array storage differs from its declared field.
    #[error("spatial array does not match its field")]
    ArrayMismatch,
    /// A coordinate or geometry part is null.
    #[error("spatial child values must not be null")]
    NullChild,
    /// At least one coordinate is NaN or infinite.
    #[error("spatial coordinate is non-finite")]
    NonFiniteCoordinate,
    /// At least one coordinate violates the declared CRS bounds.
    #[error("spatial coordinate is outside the declared CRS bounds")]
    CoordinateOutOfRange,
    /// A polygon ring is too short or its endpoints differ.
    #[error("polygon ring is not closed")]
    RingNotClosed,
    /// Geometry, vertex, or byte limits were exceeded.
    #[error("spatial value exceeds its resource limit")]
    ResourceLimit,
}

impl SpatialValidationError {
    /// Stable public error code that is safe to log.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::TypeMismatch => "GF_SPATIAL_TYPE_MISMATCH",
            Self::MetadataMismatch => "GF_SPATIAL_METADATA_MISMATCH",
            Self::ArrayMismatch => "GF_SPATIAL_ARRAY_MISMATCH",
            Self::NullChild => "GF_SPATIAL_NULL_CHILD",
            Self::NonFiniteCoordinate => "GF_SPATIAL_NON_FINITE_COORDINATE",
            Self::CoordinateOutOfRange => "GF_SPATIAL_COORDINATE_OUT_OF_RANGE",
            Self::RingNotClosed => "GF_SPATIAL_RING_NOT_CLOSED",
            Self::ResourceLimit => "GF_SPATIAL_RESOURCE_LIMIT",
        }
    }
}

/// The two coordinate reference systems accepted by spatial v1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SpatialCrs {
    /// WGS 84 longitude/latitude in canonical x/y order.
    #[serde(rename = "EPSG:4326")]
    Epsg4326,
    /// Web Mercator easting/northing in canonical x/y order.
    #[serde(rename = "EPSG:3857")]
    Epsg3857,
}

impl SpatialCrs {
    /// Stable authority-code spelling used in GeoArrow metadata.
    #[must_use]
    pub const fn authority_code(self) -> &'static str {
        match self {
            Self::Epsg4326 => "EPSG:4326",
            Self::Epsg3857 => "EPSG:3857",
        }
    }

    /// Canonical GeoArrow extension metadata. Authority-code CRS metadata is
    /// permitted by GeoArrow and keeps the project contract byte-stable.
    #[must_use]
    pub fn extension_metadata(self) -> String {
        format!(
            "{{\"crs\":\"{}\",\"crs_type\":\"authority_code\"}}",
            self.authority_code()
        )
    }
}

impl fmt::Display for SpatialCrs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.authority_code())
    }
}

/// Homogeneous two-dimensional geometry kinds supported by spatial v1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpatialGeometryType {
    /// A single x/y coordinate.
    Point,
    /// An ordered path of coordinate vertices.
    LineString,
    /// An exterior ring followed by optional interior rings.
    Polygon,
    /// A collection of points.
    MultiPoint,
    /// A collection of line strings.
    MultiLineString,
    /// A collection of polygons.
    MultiPolygon,
}

impl SpatialGeometryType {
    /// GeoArrow extension name for this homogeneous geometry kind.
    #[must_use]
    pub const fn extension_name(self) -> &'static str {
        match self {
            Self::Point => "geoarrow.point",
            Self::LineString => "geoarrow.linestring",
            Self::Polygon => "geoarrow.polygon",
            Self::MultiPoint => "geoarrow.multipoint",
            Self::MultiLineString => "geoarrow.multilinestring",
            Self::MultiPolygon => "geoarrow.multipolygon",
        }
    }

    /// Stable ontology spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Point => "point",
            Self::LineString => "linestring",
            Self::Polygon => "polygon",
            Self::MultiPoint => "multipoint",
            Self::MultiLineString => "multilinestring",
            Self::MultiPolygon => "multipolygon",
        }
    }
}

/// Complete homogeneous spatial property type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SpatialType {
    /// Homogeneous geometry kind.
    pub geometry: SpatialGeometryType,
    /// Homogeneous coordinate reference system.
    pub crs: SpatialCrs,
}

impl SpatialType {
    /// Canonical separated-coordinate GeoArrow storage type.
    #[must_use]
    pub fn data_type(self) -> DataType {
        let coordinate = DataType::Struct(Fields::from(vec![
            Field::new("x", DataType::Float64, false),
            Field::new("y", DataType::Float64, false),
        ]));
        let list =
            |name: &str, child: DataType| DataType::List(Arc::new(Field::new(name, child, false)));
        match self.geometry {
            SpatialGeometryType::Point => coordinate,
            SpatialGeometryType::LineString => list("vertices", coordinate),
            SpatialGeometryType::Polygon => list("rings", list("vertices", coordinate)),
            SpatialGeometryType::MultiPoint => list("points", coordinate),
            SpatialGeometryType::MultiLineString => {
                list("linestrings", list("vertices", coordinate))
            }
            SpatialGeometryType::MultiPolygon => {
                list("polygons", list("rings", list("vertices", coordinate)))
            }
        }
    }

    /// Canonical top-level GeoArrow field metadata.
    #[must_use]
    pub fn field_metadata(self) -> HashMap<String, String> {
        HashMap::from([
            (
                EXTENSION_NAME_KEY.to_owned(),
                self.geometry.extension_name().to_owned(),
            ),
            (
                EXTENSION_METADATA_KEY.to_owned(),
                self.crs.extension_metadata(),
            ),
        ])
    }

    /// Construct a canonical GeoArrow field. Extension metadata is attached
    /// only to the top-level field, as required by GeoArrow.
    #[must_use]
    pub fn field(self, name: impl Into<String>, nullable: bool) -> Field {
        Field::new(name.into(), self.data_type(), nullable).with_metadata(self.field_metadata())
    }

    /// Stable compiled-ontology representation.
    #[must_use]
    pub fn catalog_name(self) -> String {
        format!("spatial:{}:{}", self.geometry.as_str(), self.crs)
    }

    /// Parse the stable compiled-ontology representation.
    #[must_use]
    pub fn from_catalog_name(value: &str) -> Option<Self> {
        let mut parts = value.split(':');
        if parts.next()? != "spatial" {
            return None;
        }
        let geometry = match parts.next()? {
            "point" => SpatialGeometryType::Point,
            "linestring" => SpatialGeometryType::LineString,
            "polygon" => SpatialGeometryType::Polygon,
            "multipoint" => SpatialGeometryType::MultiPoint,
            "multilinestring" => SpatialGeometryType::MultiLineString,
            "multipolygon" => SpatialGeometryType::MultiPolygon,
            _ => return None,
        };
        let authority = parts.next()?;
        let code = parts.next()?;
        if parts.next().is_some() || authority != "EPSG" {
            return None;
        }
        let crs = match code {
            "4326" => SpatialCrs::Epsg4326,
            "3857" => SpatialCrs::Epsg3857,
            _ => return None,
        };
        Some(Self { geometry, crs })
    }

    /// Validate an Arrow array before mutation or publication.
    pub fn validate_array(
        self,
        field: &Field,
        array: &dyn Array,
        limits: SpatialValidationLimits,
    ) -> Result<(), SpatialValidationError> {
        if field.data_type() != &self.data_type() {
            return Err(SpatialValidationError::TypeMismatch);
        }
        if field.metadata() != &self.field_metadata() {
            return Err(SpatialValidationError::MetadataMismatch);
        }
        if array.data_type() != field.data_type() || array.len() > limits.max_geometries {
            return Err(if array.len() > limits.max_geometries {
                SpatialValidationError::ResourceLimit
            } else {
                SpatialValidationError::ArrayMismatch
            });
        }
        if array.get_array_memory_size() > limits.max_bytes {
            return Err(SpatialValidationError::ResourceLimit);
        }
        let coordinates = coordinate_array(array).ok_or(SpatialValidationError::ArrayMismatch)?;
        if coordinates.len() > limits.max_vertices {
            return Err(SpatialValidationError::ResourceLimit);
        }
        validate_coordinates(coordinates, self.crs)?;
        if matches!(self.geometry, SpatialGeometryType::Polygon) {
            validate_polygon_rings(
                array
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or(SpatialValidationError::ArrayMismatch)?,
            )?;
        } else if matches!(self.geometry, SpatialGeometryType::MultiPolygon) {
            let polygons = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or(SpatialValidationError::ArrayMismatch)?;
            reject_null_children(polygons)?;
            for index in 0..polygons.len() {
                if polygons.is_null(index) {
                    continue;
                }
                let value = polygons.value(index);
                validate_polygon_rings(
                    value
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .ok_or(SpatialValidationError::ArrayMismatch)?,
                )?;
            }
        }
        Ok(())
    }
}

fn coordinate_array(array: &dyn Array) -> Option<&StructArray> {
    if let Some(coordinates) = array.as_any().downcast_ref::<StructArray>() {
        return Some(coordinates);
    }
    let list = array.as_any().downcast_ref::<ListArray>()?;
    coordinate_array(list.values().as_ref())
}

fn reject_null_children(list: &ListArray) -> Result<(), SpatialValidationError> {
    if list.values().null_count() != 0 {
        return Err(SpatialValidationError::NullChild);
    }
    Ok(())
}

fn validate_coordinates(
    coordinates: &StructArray,
    crs: SpatialCrs,
) -> Result<(), SpatialValidationError> {
    if coordinates.null_count() != 0 {
        return Err(SpatialValidationError::NullChild);
    }
    let x = coordinates
        .column_by_name("x")
        .and_then(|values| values.as_any().downcast_ref::<Float64Array>())
        .ok_or(SpatialValidationError::ArrayMismatch)?;
    let y = coordinates
        .column_by_name("y")
        .and_then(|values| values.as_any().downcast_ref::<Float64Array>())
        .ok_or(SpatialValidationError::ArrayMismatch)?;
    if x.null_count() != 0 || y.null_count() != 0 || x.len() != y.len() {
        return Err(SpatialValidationError::NullChild);
    }
    for index in 0..x.len() {
        let (x_value, y_value) = (x.value(index), y.value(index));
        if !x_value.is_finite() || !y_value.is_finite() {
            return Err(SpatialValidationError::NonFiniteCoordinate);
        }
        let in_range = match crs {
            SpatialCrs::Epsg4326 => {
                (-180.0..=180.0).contains(&x_value) && (-90.0..=90.0).contains(&y_value)
            }
            SpatialCrs::Epsg3857 => {
                (-WEB_MERCATOR_MAX..=WEB_MERCATOR_MAX).contains(&x_value)
                    && (-WEB_MERCATOR_MAX..=WEB_MERCATOR_MAX).contains(&y_value)
            }
        };
        if !in_range {
            return Err(SpatialValidationError::CoordinateOutOfRange);
        }
    }
    Ok(())
}

fn validate_polygon_rings(polygons: &ListArray) -> Result<(), SpatialValidationError> {
    reject_null_children(polygons)?;
    for polygon_index in 0..polygons.len() {
        if polygons.is_null(polygon_index) {
            continue;
        }
        let polygon_array = polygons.value(polygon_index);
        let rings = polygon_array
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or(SpatialValidationError::ArrayMismatch)?;
        reject_null_children(rings)?;
        for ring_index in 0..rings.len() {
            let coordinate_array = rings.value(ring_index);
            let coordinates = coordinate_array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or(SpatialValidationError::ArrayMismatch)?;
            if coordinates.len() < 4 {
                return Err(SpatialValidationError::RingNotClosed);
            }
            let x = coordinates
                .column_by_name("x")
                .and_then(|values| values.as_any().downcast_ref::<Float64Array>())
                .ok_or(SpatialValidationError::ArrayMismatch)?;
            let y = coordinates
                .column_by_name("y")
                .and_then(|values| values.as_any().downcast_ref::<Float64Array>())
                .ok_or(SpatialValidationError::ArrayMismatch)?;
            let last = coordinates.len() - 1;
            if x.value(0).to_bits() != x.value(last).to_bits()
                || y.value(0).to_bits() != y.value(last).to_bits()
            {
                return Err(SpatialValidationError::RingNotClosed);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Float64Array};

    #[test]
    fn every_spatial_type_has_canonical_geoarrow_metadata() {
        for geometry in [
            SpatialGeometryType::Point,
            SpatialGeometryType::LineString,
            SpatialGeometryType::Polygon,
            SpatialGeometryType::MultiPoint,
            SpatialGeometryType::MultiLineString,
            SpatialGeometryType::MultiPolygon,
        ] {
            for crs in [SpatialCrs::Epsg4326, SpatialCrs::Epsg3857] {
                let spatial = SpatialType { geometry, crs };
                let field = spatial.field("location", true);
                assert_eq!(
                    field.metadata().get(EXTENSION_NAME_KEY).map(String::as_str),
                    Some(geometry.extension_name())
                );
                assert_eq!(
                    field.metadata().get(EXTENSION_METADATA_KEY),
                    Some(&crs.extension_metadata())
                );
                assert_eq!(
                    SpatialType::from_catalog_name(&spatial.catalog_name()),
                    Some(spatial)
                );
            }
        }
    }

    #[test]
    fn serde_requires_explicit_geometry_and_crs() {
        let spatial = SpatialType {
            geometry: SpatialGeometryType::Polygon,
            crs: SpatialCrs::Epsg4326,
        };
        let json = serde_json::to_string(&spatial).unwrap();
        assert_eq!(json, r#"{"geometry":"polygon","crs":"EPSG:4326"}"#);
        assert_eq!(serde_json::from_str::<SpatialType>(&json).unwrap(), spatial);
        assert!(serde_json::from_str::<SpatialType>(r#"{"geometry":"point"}"#).is_err());
        assert!(
            serde_json::from_str::<SpatialType>(r#"{"geometry":"point","crs":"EPSG:26915"}"#)
                .is_err()
        );
    }

    fn points(values: &[(f64, f64)]) -> StructArray {
        StructArray::from(vec![
            (
                Arc::new(Field::new("x", DataType::Float64, false)),
                Arc::new(Float64Array::from(
                    values.iter().map(|(x, _)| *x).collect::<Vec<_>>(),
                )) as ArrayRef,
            ),
            (
                Arc::new(Field::new("y", DataType::Float64, false)),
                Arc::new(Float64Array::from(
                    values.iter().map(|(_, y)| *y).collect::<Vec<_>>(),
                )) as ArrayRef,
            ),
        ])
    }

    #[test]
    fn point_validation_enforces_metadata_crs_and_limits_without_values_in_errors() {
        let spatial = SpatialType {
            geometry: SpatialGeometryType::Point,
            crs: SpatialCrs::Epsg4326,
        };
        let field = spatial.field("location", false);
        spatial
            .validate_array(&field, &points(&[(-105.0, 39.7)]), Default::default())
            .unwrap();

        let out_of_range = spatial
            .validate_array(&field, &points(&[(181.0, 0.0)]), Default::default())
            .unwrap_err();
        assert_eq!(out_of_range.code(), "GF_SPATIAL_COORDINATE_OUT_OF_RANGE");
        assert!(!out_of_range.to_string().contains("181"));

        let non_finite = spatial
            .validate_array(&field, &points(&[(f64::NAN, 0.0)]), Default::default())
            .unwrap_err();
        assert_eq!(non_finite.code(), "GF_SPATIAL_NON_FINITE_COORDINATE");
        assert!(!non_finite.to_string().contains("NaN"));

        let limit = spatial
            .validate_array(
                &field,
                &points(&[(0.0, 0.0)]),
                SpatialValidationLimits {
                    max_geometries: 0,
                    ..Default::default()
                },
            )
            .unwrap_err();
        assert_eq!(limit.code(), "GF_SPATIAL_RESOURCE_LIMIT");

        let metadata = spatial
            .validate_array(
                &Field::new("location", spatial.data_type(), false),
                &points(&[(0.0, 0.0)]),
                Default::default(),
            )
            .unwrap_err();
        assert_eq!(metadata.code(), "GF_SPATIAL_METADATA_MISMATCH");
    }
}
