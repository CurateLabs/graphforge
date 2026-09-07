//! Plan-local literal equality. Public IR equality and serialization are unchanged.
use graphforge_ir::IrLiteral;
use std::hash::{Hash, Hasher};

/// Signed zeros compare equally; NaNs are reflexive and retain their payload.
fn float_key(value: f64) -> u64 {
    if value == 0.0 { 0 } else { value.to_bits() }
}

fn sequence_eq<T>(left: &[T], right: &[T], eq: impl Fn(&T, &T) -> bool) -> bool {
    left.len() == right.len() && left.iter().zip(right).all(|(a, b)| eq(a, b))
}

fn literal_eq(left: &IrLiteral, right: &IrLiteral) -> bool {
    match (left, right) {
        (IrLiteral::Float(a), IrLiteral::Float(b)) => float_key(*a) == float_key(*b),
        (IrLiteral::List(a), IrLiteral::List(b)) => sequence_eq(a, b, literal_eq),
        (IrLiteral::Map(a), IrLiteral::Map(b)) => properties_eq(a, b),
        (IrLiteral::Spatial(a), IrLiteral::Spatial(b)) => {
            a.spatial_type == b.spatial_type
                && a.extension_name == b.extension_name
                && a.extension_metadata == b.extension_metadata
                && spatial_eq(&a.coordinates, &b.coordinates)
        }
        _ => left == right,
    }
}

fn spatial_eq(
    left: &graphforge_core::SpatialCoordinates,
    right: &graphforge_core::SpatialCoordinates,
) -> bool {
    use graphforge_core::SpatialCoordinates::{
        LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon,
    };
    let point = |a: &[f64; 2], b: &[f64; 2]| {
        float_key(a[0]) == float_key(b[0]) && float_key(a[1]) == float_key(b[1])
    };
    let line = |a: &Vec<[f64; 2]>, b: &Vec<[f64; 2]>| sequence_eq(a, b, point);
    let polygon = |a: &Vec<Vec<[f64; 2]>>, b: &Vec<Vec<[f64; 2]>>| sequence_eq(a, b, line);
    match (left, right) {
        (Point(a), Point(b)) => point(a, b),
        (LineString(a), LineString(b)) | (MultiPoint(a), MultiPoint(b)) => line(a, b),
        (Polygon(a), Polygon(b)) | (MultiLineString(a), MultiLineString(b)) => polygon(a, b),
        (MultiPolygon(a), MultiPolygon(b)) => sequence_eq(a, b, polygon),
        _ => false,
    }
}

fn properties_eq(left: &[(String, IrLiteral)], right: &[(String, IrLiteral)]) -> bool {
    sequence_eq(left, right, |(ak, av), (bk, bv)| {
        ak == bk && literal_eq(av, bv)
    })
}

/// Borrowed key avoids copying literal payloads during plan comparison/hashing.
pub(crate) struct Properties<'a>(pub &'a [(String, IrLiteral)]);
impl Eq for Properties<'_> {}
impl PartialEq for Properties<'_> {
    fn eq(&self, other: &Self) -> bool {
        properties_eq(self.0, other.0)
    }
}
impl Hash for Properties<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        hash_props(self.0, state);
    }
}

fn hash_literal<H: Hasher>(lit: &IrLiteral, state: &mut H) {
    match lit {
        IrLiteral::Null => 0u8.hash(state),
        IrLiteral::Bool(b) => {
            1u8.hash(state);
            b.hash(state);
        }
        IrLiteral::Int(i) => {
            2u8.hash(state);
            i.hash(state);
        }
        IrLiteral::Float(f) => {
            3u8.hash(state);
            float_key(*f).hash(state);
        }
        IrLiteral::Str(s) => {
            4u8.hash(state);
            s.hash(state);
        }
        IrLiteral::Uuid(uuid) => {
            14u8.hash(state);
            uuid.hash(state);
        }
        IrLiteral::Duration {
            months,
            days,
            seconds,
            nanos,
        } => {
            5u8.hash(state);
            months.hash(state);
            days.hash(state);
            seconds.hash(state);
            nanos.hash(state);
        }
        IrLiteral::DateTime(t) => {
            6u8.hash(state);
            t.hash(state);
        }
        IrLiteral::Date(d) => {
            7u8.hash(state);
            d.hash(state);
        }
        IrLiteral::LocalDateTime { days, nanos } => {
            8u8.hash(state);
            days.hash(state);
            nanos.hash(state);
        }
        IrLiteral::Time(n) => {
            9u8.hash(state);
            n.hash(state);
        }
        IrLiteral::ZonedTime { nanos, offset } => {
            10u8.hash(state);
            nanos.hash(state);
            offset.hash(state);
        }
        IrLiteral::ZonedDateTime {
            days,
            nanos,
            offset,
            zone,
        } => {
            11u8.hash(state);
            days.hash(state);
            nanos.hash(state);
            offset.hash(state);
            zone.hash(state);
        }
        IrLiteral::Spatial(value) => {
            15u8.hash(state);
            value.spatial_type.hash(state);
            value.extension_name.hash(state);
            value.extension_metadata.hash(state);
            hash_spatial_coordinates(&value.coordinates, state);
        }
        IrLiteral::List(items) => {
            12u8.hash(state);
            items.len().hash(state);
            for it in items {
                hash_literal(it, state);
            }
        }
        IrLiteral::Map(entries) => {
            13u8.hash(state);
            entries.len().hash(state);
            for (key, value) in entries {
                key.hash(state);
                hash_literal(value, state);
            }
        }
    }
}

fn hash_spatial_coordinates<H: Hasher>(
    coordinates: &graphforge_core::SpatialCoordinates,
    state: &mut H,
) {
    use graphforge_core::SpatialCoordinates;
    fn point<H: Hasher>(value: &[f64; 2], state: &mut H) {
        float_key(value[0]).hash(state);
        float_key(value[1]).hash(state);
    }
    match coordinates {
        SpatialCoordinates::Point(value) => {
            0u8.hash(state);
            point(value, state);
        }
        SpatialCoordinates::LineString(values) => {
            1u8.hash(state);
            values.len().hash(state);
            for value in values {
                point(value, state);
            }
        }
        SpatialCoordinates::Polygon(rings) => {
            2u8.hash(state);
            rings.len().hash(state);
            for ring in rings {
                ring.len().hash(state);
                for value in ring {
                    point(value, state);
                }
            }
        }
        SpatialCoordinates::MultiPoint(values) => {
            3u8.hash(state);
            values.len().hash(state);
            for value in values {
                point(value, state);
            }
        }
        SpatialCoordinates::MultiLineString(lines) => {
            4u8.hash(state);
            lines.len().hash(state);
            for line in lines {
                line.len().hash(state);
                for value in line {
                    point(value, state);
                }
            }
        }
        SpatialCoordinates::MultiPolygon(polygons) => {
            5u8.hash(state);
            polygons.len().hash(state);
            for polygon in polygons {
                polygon.len().hash(state);
                for ring in polygon {
                    ring.len().hash(state);
                    for value in ring {
                        point(value, state);
                    }
                }
            }
        }
    }
}

fn hash_props<H: Hasher>(props: &[(String, IrLiteral)], state: &mut H) {
    props.len().hash(state);
    for (k, v) in props {
        k.hash(state);
        hash_literal(v, state);
    }
}
