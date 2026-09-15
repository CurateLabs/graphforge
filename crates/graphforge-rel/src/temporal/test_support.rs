use super::{Fields, TemporalField};

pub(super) fn fields(pairs: &[(&str, TemporalField)]) -> Fields {
    pairs
        .iter()
        .map(|(k, v)| {
            (
                (*k).to_string(),
                match v {
                    TemporalField::Int(n) => TemporalField::Int(*n),
                    TemporalField::Float(x) => TemporalField::Float(*x),
                    TemporalField::Str(s) => TemporalField::Str(s.clone()),
                    TemporalField::Date(d) => TemporalField::Date(*d),
                },
            )
        })
        .collect()
}

pub(super) fn int(n: i64) -> TemporalField {
    TemporalField::Int(n)
}
