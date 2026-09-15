//! Spatial point construction and distance expression lowering.
//!
//! Lowering state and dispatch remain in the parent; runtime semantics use existing adapters.

use super::{DfExpr, ExprId, ExprLowerer, IrExpr, IrLiteral, LoweringError, lit, spatial_scalar};

impl ExprLowerer<'_> {
    pub(super) fn lower_spatial_point(&self, args: &[ExprId]) -> Result<DfExpr, LoweringError> {
        let [arg] = args else {
            return Err(LoweringError::InvalidType(
                "point() expects one map argument".into(),
            ));
        };
        let value = self.const_spatial_point(*arg)?;
        Ok(DfExpr::Literal(spatial_scalar(&value), None))
    }

    fn const_spatial_point(
        &self,
        id: ExprId,
    ) -> Result<graphforge_core::SpatialValue, LoweringError> {
        use graphforge_core::{
            SpatialCoordinates, SpatialCrs, SpatialGeometryType, SpatialType, SpatialValue,
        };
        let IrExpr::MapLiteral(entries) = self.arena.get(id) else {
            return Err(LoweringError::InvalidType(
                "point() requires a literal coordinate map in the certified profile".into(),
            ));
        };
        let number = |key: &str| -> Option<f64> {
            let (_, id) = entries
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(key))?;
            match self.arena.get(*id) {
                IrExpr::Literal(IrLiteral::Int(value)) => {
                    #[allow(
                        clippy::cast_precision_loss,
                        reason = "Cypher point coordinates are represented as f64"
                    )]
                    let value = *value as f64;
                    Some(value)
                }
                IrExpr::Literal(IrLiteral::Float(value)) => Some(*value),
                _ => None,
            }
        };
        let string = |key: &str| -> Option<&str> {
            let (_, id) = entries
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(key))?;
            match self.arena.get(*id) {
                IrExpr::Literal(IrLiteral::Str(value)) => Some(value.as_str()),
                _ => None,
            }
        };
        let geographic = number("longitude").zip(number("latitude"));
        let cartesian = number("x").zip(number("y"));
        let ((x, y), default_crs) = match (cartesian, geographic) {
            (Some(value), None) => (value, SpatialCrs::Epsg3857),
            (None, Some(value)) => (value, SpatialCrs::Epsg4326),
            _ => {
                return Err(LoweringError::InvalidType(
                    "point() requires exactly x/y or longitude/latitude numeric coordinates".into(),
                ));
            }
        };
        if !x.is_finite() || !y.is_finite() {
            return Err(LoweringError::InvalidType(
                "point() coordinates must be finite".into(),
            ));
        }
        let crs = match string("crs") {
            None => default_crs,
            Some(value) if value.eq_ignore_ascii_case("EPSG:4326") => SpatialCrs::Epsg4326,
            Some(value) if value.eq_ignore_ascii_case("EPSG:3857") => SpatialCrs::Epsg3857,
            Some(value) => {
                return Err(LoweringError::InvalidType(format!(
                    "unsupported spatial CRS `{value}` for point() computation"
                )));
            }
        };
        if (geographic.is_some() && crs != SpatialCrs::Epsg4326)
            || (cartesian.is_some() && crs != SpatialCrs::Epsg3857)
        {
            return Err(LoweringError::InvalidType(
                "point() coordinate keys do not match the declared CRS".into(),
            ));
        }
        Ok(SpatialValue {
            spatial_type: SpatialType {
                geometry: SpatialGeometryType::Point,
                crs,
            },
            coordinates: SpatialCoordinates::Point([x, y]),
            extension_name: None,
            extension_metadata: None,
        })
    }

    pub(super) fn lower_spatial_distance(&self, args: &[ExprId]) -> Result<DfExpr, LoweringError> {
        use graphforge_core::{SpatialCoordinates, SpatialCrs};
        let [left, right] = args else {
            return Err(LoweringError::InvalidType(
                "distance() expects two Point values".into(),
            ));
        };
        let point = |id| match self.arena.get(id) {
            IrExpr::FunctionCall { name, args }
                if name.eq_ignore_ascii_case("point") && args.len() == 1 =>
            {
                self.const_spatial_point(args[0])
            }
            IrExpr::Literal(IrLiteral::Spatial(value)) => Ok(value.clone()),
            _ => Err(LoweringError::InvalidType(
                "distance() certified profile requires Point values".into(),
            )),
        };
        let a = point(*left)?;
        let b = point(*right)?;
        if a.spatial_type.crs != b.spatial_type.crs {
            return Err(LoweringError::InvalidType(
                "distance() does not implicitly reproject mixed CRS values".into(),
            ));
        }
        let (SpatialCoordinates::Point([ax, ay]), SpatialCoordinates::Point([bx, by])) =
            (&a.coordinates, &b.coordinates)
        else {
            return Err(LoweringError::InvalidType(
                "distance() accepts Point geometry only".into(),
            ));
        };
        let distance = match &a.spatial_type.crs {
            SpatialCrs::Epsg3857 => (bx - ax).hypot(by - ay),
            SpatialCrs::Epsg4326 => {
                const EARTH_RADIUS_METRES: f64 = 6_371_008.8;
                let (lat1, lat2) = (ay.to_radians(), by.to_radians());
                let dlat = lat2 - lat1;
                let dlon = (bx - ax).to_radians();
                let h = (dlat / 2.0).sin().powi(2)
                    + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);
                2.0 * EARTH_RADIUS_METRES * h.sqrt().asin()
            }
            SpatialCrs::Preserved(_) => {
                return Err(LoweringError::InvalidType(
                    "distance() does not compute preserved-only CRS values".into(),
                ));
            }
        };
        Ok(lit(distance))
    }
}
