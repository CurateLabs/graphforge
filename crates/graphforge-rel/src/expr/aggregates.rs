//! Cypher aggregate implementations and their state contracts.

use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{Signature, Volatility};
use datafusion::scalar::ScalarValue;
use std::sync::LazyLock;

use super::{cypher_order, cypher_value_eq, scalar_as_f64};

/// `min`/`max` over a heterogeneous (tagged) column use Cypher orderability
/// ([`cypher_order`]) — native struct min/max would order by `__het_key` (null for
/// non-numeric elements). Returns the original tagged element so it renders right.
pub(crate) static CYPHER_MAX: LazyLock<datafusion::logical_expr::AggregateUDF> =
    LazyLock::new(|| {
        datafusion::logical_expr::AggregateUDF::new_from_impl(CypherExtreme::new(true))
    });
pub(crate) static CYPHER_MIN: LazyLock<datafusion::logical_expr::AggregateUDF> =
    LazyLock::new(|| {
        datafusion::logical_expr::AggregateUDF::new_from_impl(CypherExtreme::new(false))
    });
pub(crate) static CYPHER_COLLECT: LazyLock<datafusion::logical_expr::AggregateUDF> =
    LazyLock::new(|| {
        datafusion::logical_expr::AggregateUDF::new_from_impl(CypherCollect::new(false))
    });
pub(crate) static CYPHER_COLLECT_DISTINCT: LazyLock<datafusion::logical_expr::AggregateUDF> =
    LazyLock::new(|| {
        datafusion::logical_expr::AggregateUDF::new_from_impl(CypherCollect::new(true))
    });
pub(crate) static CYPHER_PERCENTILE_DISC: LazyLock<datafusion::logical_expr::AggregateUDF> =
    LazyLock::new(|| {
        datafusion::logical_expr::AggregateUDF::new_from_impl(CypherPercentile::new(false))
    });
pub(crate) static CYPHER_PERCENTILE_CONT: LazyLock<datafusion::logical_expr::AggregateUDF> =
    LazyLock::new(|| {
        datafusion::logical_expr::AggregateUDF::new_from_impl(CypherPercentile::new(true))
    });

#[derive(Debug)]
struct CypherExtreme {
    signature: Signature,
    is_max: bool,
}
impl CypherExtreme {
    fn new(is_max: bool) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            is_max,
        }
    }
}
impl PartialEq for CypherExtreme {
    fn eq(&self, o: &Self) -> bool {
        self.is_max == o.is_max
    }
}
impl Eq for CypherExtreme {}
impl std::hash::Hash for CypherExtreme {
    fn hash<H: std::hash::Hasher>(&self, st: &mut H) {
        self.is_max.hash(st);
    }
}
impl datafusion::logical_expr::AggregateUDFImpl for CypherExtreme {
    fn name(&self) -> &str {
        if self.is_max {
            "cypher_max"
        } else {
            "cypher_min"
        }
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(arg_types[0].clone())
    }
    fn accumulator(
        &self,
        args: datafusion::logical_expr::function::AccumulatorArgs,
    ) -> datafusion::error::Result<Box<dyn datafusion::logical_expr::Accumulator>> {
        Ok(Box::new(ExtremeAcc {
            is_max: self.is_max,
            dtype: args.return_field.data_type().clone(),
            best: None,
        }))
    }
    fn state_fields(
        &self,
        args: datafusion::logical_expr::function::StateFieldsArgs,
    ) -> datafusion::error::Result<Vec<datafusion::arrow::datatypes::FieldRef>> {
        use datafusion::arrow::datatypes::Field;
        Ok(vec![std::sync::Arc::new(Field::new(
            "best",
            args.return_field.data_type().clone(),
            true,
        ))])
    }
}

#[derive(Debug)]
struct ExtremeAcc {
    is_max: bool,
    dtype: DataType,
    best: Option<ScalarValue>,
}
impl datafusion::logical_expr::Accumulator for ExtremeAcc {
    fn update_batch(
        &mut self,
        values: &[datafusion::arrow::array::ArrayRef],
    ) -> datafusion::error::Result<()> {
        use datafusion::arrow::array::Array;
        let arr = &values[0];
        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }
            let v = ScalarValue::try_from_array(arr, i)?;
            if v.is_null() {
                continue;
            }
            let take = match &self.best {
                None => true,
                Some(b) => {
                    let ord = cypher_order(&v, b);
                    (self.is_max && ord == std::cmp::Ordering::Greater)
                        || (!self.is_max && ord == std::cmp::Ordering::Less)
                }
            };
            if take {
                self.best = Some(v);
            }
        }
        Ok(())
    }
    fn evaluate(&mut self) -> datafusion::error::Result<ScalarValue> {
        match &self.best {
            Some(b) => Ok(b.clone()),
            None => ScalarValue::try_from(&self.dtype),
        }
    }
    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.best.as_ref().map_or(0, ScalarValue::size)
    }
    fn state(&mut self) -> datafusion::error::Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }
    fn merge_batch(
        &mut self,
        states: &[datafusion::arrow::array::ArrayRef],
    ) -> datafusion::error::Result<()> {
        self.update_batch(states)
    }
}

#[derive(Debug)]
struct CypherCollect {
    signature: Signature,
    distinct: bool,
}

impl CypherCollect {
    fn new(distinct: bool) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            distinct,
        }
    }
}

impl PartialEq for CypherCollect {
    fn eq(&self, o: &Self) -> bool {
        self.distinct == o.distinct
    }
}

impl Eq for CypherCollect {}

impl std::hash::Hash for CypherCollect {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.distinct.hash(state);
    }
}

impl datafusion::logical_expr::AggregateUDFImpl for CypherCollect {
    fn name(&self) -> &str {
        if self.distinct {
            "cypher_collect_distinct"
        } else {
            "cypher_collect"
        }
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::new_list(arg_types[0].clone(), true))
    }
    fn accumulator(
        &self,
        args: datafusion::logical_expr::function::AccumulatorArgs,
    ) -> datafusion::error::Result<Box<dyn datafusion::logical_expr::Accumulator>> {
        let DataType::List(field) = args.return_field.data_type() else {
            return Err(datafusion::error::DataFusionError::Plan(
                "cypher_collect return type must be a list".into(),
            ));
        };
        Ok(Box::new(CollectAcc {
            distinct: self.distinct,
            elem_type: field.data_type().clone(),
            values: Vec::new(),
        }))
    }
    fn state_fields(
        &self,
        args: datafusion::logical_expr::function::StateFieldsArgs,
    ) -> datafusion::error::Result<Vec<datafusion::arrow::datatypes::FieldRef>> {
        use datafusion::arrow::datatypes::Field;
        Ok(vec![std::sync::Arc::new(Field::new(
            "values",
            args.return_field.data_type().clone(),
            true,
        ))])
    }
}

#[derive(Debug)]
struct CollectAcc {
    distinct: bool,
    elem_type: DataType,
    values: Vec<ScalarValue>,
}

impl CollectAcc {
    fn push_value(&mut self, v: ScalarValue) {
        if v.is_null() {
            return;
        }
        if self.distinct
            && self
                .values
                .iter()
                .any(|seen| cypher_value_eq(seen, &v) == Some(true))
        {
            return;
        }
        self.values.push(v);
    }

    fn as_list(&self) -> ScalarValue {
        ScalarValue::List(ScalarValue::new_list(&self.values, &self.elem_type, true))
    }
}

impl datafusion::logical_expr::Accumulator for CollectAcc {
    fn update_batch(
        &mut self,
        values: &[datafusion::arrow::array::ArrayRef],
    ) -> datafusion::error::Result<()> {
        use datafusion::arrow::array::Array;
        let arr = &values[0];
        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }
            self.push_value(ScalarValue::try_from_array(arr, i)?);
        }
        Ok(())
    }
    fn evaluate(&mut self) -> datafusion::error::Result<ScalarValue> {
        Ok(self.as_list())
    }
    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.values.iter().map(ScalarValue::size).sum::<usize>()
    }
    fn state(&mut self) -> datafusion::error::Result<Vec<ScalarValue>> {
        Ok(vec![self.as_list()])
    }
    fn merge_batch(
        &mut self,
        states: &[datafusion::arrow::array::ArrayRef],
    ) -> datafusion::error::Result<()> {
        use datafusion::arrow::array::{Array, ListArray};
        let arr = &states[0];
        let Some(list) = arr.as_any().downcast_ref::<ListArray>() else {
            return Err(datafusion::error::DataFusionError::Plan(
                "cypher_collect state must be a list".into(),
            ));
        };
        for row in 0..list.len() {
            if list.is_null(row) {
                continue;
            }
            let values = list.value(row);
            for i in 0..values.len() {
                if values.is_null(i) {
                    continue;
                }
                self.push_value(ScalarValue::try_from_array(&values, i)?);
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
struct CypherPercentile {
    signature: Signature,
    continuous: bool,
}

impl CypherPercentile {
    fn new(continuous: bool) -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
            continuous,
        }
    }
}

impl PartialEq for CypherPercentile {
    fn eq(&self, o: &Self) -> bool {
        self.continuous == o.continuous
    }
}

impl Eq for CypherPercentile {}

impl std::hash::Hash for CypherPercentile {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.continuous.hash(state);
    }
}

impl datafusion::logical_expr::AggregateUDFImpl for CypherPercentile {
    fn name(&self) -> &str {
        if self.continuous {
            "cypher_percentile_cont"
        } else {
            "cypher_percentile_disc"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        let Some(value_type) = arg_types.first() else {
            return Err(datafusion::error::DataFusionError::Plan(
                "percentile aggregate requires value and percentile arguments".into(),
            ));
        };
        if !is_percentile_numeric_type(value_type) {
            return Err(datafusion::error::DataFusionError::Plan(format!(
                "percentile value expression must be numeric, got {value_type}"
            )));
        }
        if self.continuous {
            Ok(DataType::Float64)
        } else {
            Ok(value_type.clone())
        }
    }

    fn accumulator(
        &self,
        args: datafusion::logical_expr::function::AccumulatorArgs,
    ) -> datafusion::error::Result<Box<dyn datafusion::logical_expr::Accumulator>> {
        let value_type = args.expr_fields.first().map_or_else(
            || args.return_field.data_type().clone(),
            |f| f.data_type().clone(),
        );
        Ok(Box::new(PercentileAcc {
            continuous: self.continuous,
            value_type,
            result_type: args.return_field.data_type().clone(),
            values: Vec::new(),
            percentile: None,
        }))
    }

    fn state_fields(
        &self,
        args: datafusion::logical_expr::function::StateFieldsArgs,
    ) -> datafusion::error::Result<Vec<datafusion::arrow::datatypes::FieldRef>> {
        use datafusion::arrow::datatypes::Field;
        let value_type = args.input_fields.first().map_or_else(
            || args.return_field.data_type().clone(),
            |f| f.data_type().clone(),
        );
        Ok(vec![
            std::sync::Arc::new(Field::new(
                "values",
                DataType::new_list(value_type, true),
                true,
            )),
            std::sync::Arc::new(Field::new("percentile", DataType::Float64, true)),
        ])
    }
}

#[derive(Debug)]
struct PercentileAcc {
    continuous: bool,
    value_type: DataType,
    result_type: DataType,
    values: Vec<ScalarValue>,
    percentile: Option<f64>,
}

impl PercentileAcc {
    fn push_value(&mut self, v: ScalarValue) -> datafusion::error::Result<()> {
        if v.is_null() {
            return Ok(());
        }
        if scalar_as_f64(&v).is_none() {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "percentile value expression must be numeric, got {}",
                v.data_type()
            )));
        }
        self.values.push(v);
        Ok(())
    }

    fn observe_percentile(&mut self, p: Option<f64>) -> datafusion::error::Result<()> {
        let Some(p) = p else {
            return Ok(());
        };
        if !p.is_finite() || !(0.0..=1.0).contains(&p) {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "percentile argument must be a finite number between 0.0 and 1.0 inclusive, got {p}"
            )));
        }
        match self.percentile {
            Some(existing) if (existing - p).abs() > f64::EPSILON => {
                Err(datafusion::error::DataFusionError::Execution(
                    "percentile argument must be constant within an aggregate group".into(),
                ))
            }
            Some(_) => Ok(()),
            None => {
                self.percentile = Some(p);
                Ok(())
            }
        }
    }

    fn null_result(&self) -> datafusion::error::Result<ScalarValue> {
        ScalarValue::try_from(&self.result_type)
    }

    fn percentile_scalar(
        values: &[datafusion::arrow::array::ArrayRef],
        row: usize,
    ) -> datafusion::error::Result<Option<f64>> {
        use datafusion::arrow::array::Array;
        let arr = &values[1];
        if arr.is_null(row) {
            return Ok(None);
        }
        let scalar = ScalarValue::try_from_array(arr, row)?;
        if scalar.is_null() {
            Ok(None)
        } else {
            scalar_as_f64(&scalar).map(Some).ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(format!(
                    "percentile argument must be numeric, got {}",
                    scalar.data_type()
                ))
            })
        }
    }
}

impl datafusion::logical_expr::Accumulator for PercentileAcc {
    fn update_batch(
        &mut self,
        values: &[datafusion::arrow::array::ArrayRef],
    ) -> datafusion::error::Result<()> {
        use datafusion::arrow::array::Array;
        let value_arr = &values[0];
        for row in 0..value_arr.len() {
            self.observe_percentile(Self::percentile_scalar(values, row)?)?;
            if value_arr.is_null(row) {
                continue;
            }
            self.push_value(ScalarValue::try_from_array(value_arr, row)?)?;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> datafusion::error::Result<ScalarValue> {
        let Some(percentile) = self.percentile else {
            return self.null_result();
        };
        if self.values.is_empty() {
            return self.null_result();
        }
        let mut values: Vec<(f64, ScalarValue)> = self
            .values
            .iter()
            .filter_map(|v| scalar_as_f64(v).map(|f| (f, v.clone())))
            .collect();
        if values.is_empty() {
            return self.null_result();
        }
        values.sort_by(|(l, _), (r, _)| l.total_cmp(r));

        if self.continuous {
            let len = values.len();
            if len == 1 {
                return Ok(ScalarValue::Float64(Some(values[0].0)));
            }
            let (lower_index, upper_index, fraction) = percentile_cont_indices(percentile, len);
            let result = if lower_index == upper_index {
                values[lower_index].0
            } else {
                let lower = values[lower_index].0;
                let upper = values[upper_index].0;
                lower + (upper - lower) * fraction
            };
            Ok(ScalarValue::Float64(Some(result)))
        } else {
            let index = percentile_disc_index(percentile, values.len());
            Ok(values[index].1.clone())
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.values.iter().map(ScalarValue::size).sum::<usize>()
    }

    fn state(&mut self) -> datafusion::error::Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::List(ScalarValue::new_list(&self.values, &self.value_type, true)),
            ScalarValue::Float64(self.percentile),
        ])
    }

    fn merge_batch(
        &mut self,
        states: &[datafusion::arrow::array::ArrayRef],
    ) -> datafusion::error::Result<()> {
        use datafusion::arrow::array::{Array, Float64Array, ListArray};
        let values = &states[0];
        let Some(lists) = values.as_any().downcast_ref::<ListArray>() else {
            return Err(datafusion::error::DataFusionError::Plan(
                "percentile state values must be a list".into(),
            ));
        };
        let Some(percentiles) = states[1].as_any().downcast_ref::<Float64Array>() else {
            return Err(datafusion::error::DataFusionError::Plan(
                "percentile state percentile must be Float64".into(),
            ));
        };
        for row in 0..lists.len() {
            self.observe_percentile(if percentiles.is_null(row) {
                None
            } else {
                Some(percentiles.value(row))
            })?;
            if lists.is_null(row) {
                continue;
            }
            let values = lists.value(row);
            for i in 0..values.len() {
                if values.is_null(i) {
                    continue;
                }
                self.push_value(ScalarValue::try_from_array(&values, i)?)?;
            }
        }
        Ok(())
    }
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    reason = "percentile ranks are defined by converting bounded [0, 1] floats into sorted indexes"
)]
fn percentile_cont_indices(percentile: f64, len: usize) -> (usize, usize, f64) {
    let index = percentile * ((len - 1) as f64);
    let lower = index.floor() as usize;
    let upper = index.ceil() as usize;
    (lower, upper, index.fract())
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    reason = "percentile ranks are defined by converting bounded [0, 1] floats into sorted indexes"
)]
fn percentile_disc_index(percentile: f64, len: usize) -> usize {
    if percentile <= f64::EPSILON {
        0
    } else {
        ((percentile * (len as f64)).ceil() as usize)
            .saturating_sub(1)
            .min(len - 1)
    }
}

fn is_percentile_numeric_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int8
            | DataType::Null
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Array;
    use datafusion::arrow::datatypes::Field;
    use std::sync::Arc;

    #[test]
    fn aggregate_accumulators_cover_update_merge_state_and_empty_contracts() {
        use datafusion::arrow::array::{
            ArrayRef, Float64Array, Int64Array, ListArray, StringArray,
        };
        use datafusion::logical_expr::Accumulator;

        let ints: ArrayRef = Arc::new(Int64Array::from(vec![Some(4), None, Some(-2), Some(9)]));
        for (is_max, expected) in [(true, 9), (false, -2)] {
            let mut acc = ExtremeAcc {
                is_max,
                dtype: DataType::Int64,
                best: None,
            };
            assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int64(None));
            acc.update_batch(std::slice::from_ref(&ints)).unwrap();
            assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int64(Some(expected)));
            assert_eq!(
                acc.state().unwrap(),
                vec![ScalarValue::Int64(Some(expected))]
            );
            assert!(acc.size() >= std::mem::size_of::<ExtremeAcc>());
            let merged: ArrayRef =
                Arc::new(Int64Array::from(vec![Some(if is_max { 12 } else { -7 })]));
            acc.merge_batch(&[merged]).unwrap();
            assert_eq!(
                acc.evaluate().unwrap(),
                ScalarValue::Int64(Some(if is_max { 12 } else { -7 }))
            );
        }

        for distinct in [false, true] {
            let mut acc = CollectAcc {
                distinct,
                elem_type: DataType::Int64,
                values: Vec::new(),
            };
            acc.update_batch(std::slice::from_ref(&ints)).unwrap();
            let merge: ArrayRef = Arc::new(ListArray::from_iter_primitive::<
                datafusion::arrow::datatypes::Int64Type,
                _,
                _,
            >([Some(vec![Some(4), Some(11)])]));
            acc.merge_batch(&[merge]).unwrap();
            let ScalarValue::List(values) = acc.evaluate().unwrap() else {
                panic!("collect must return a list")
            };
            let expected_len = if distinct { 4 } else { 5 };
            assert_eq!(values.value(0).len(), expected_len);
            assert_eq!(acc.state().unwrap().len(), 1);
            assert!(acc.size() >= std::mem::size_of::<CollectAcc>());
            let bad: ArrayRef = Arc::new(Int64Array::from(vec![1]));
            assert!(
                acc.merge_batch(&[bad])
                    .unwrap_err()
                    .to_string()
                    .contains("must be a list")
            );
        }

        for continuous in [false, true] {
            let mut acc = PercentileAcc {
                continuous,
                value_type: DataType::Int64,
                result_type: if continuous {
                    DataType::Float64
                } else {
                    DataType::Int64
                },
                values: Vec::new(),
                percentile: None,
            };
            assert!(acc.evaluate().unwrap().is_null());
            let p: ArrayRef = Arc::new(Float64Array::from(vec![Some(0.5); 4]));
            acc.update_batch(&[Arc::clone(&ints), p]).unwrap();
            assert_eq!(
                acc.evaluate().unwrap(),
                if continuous {
                    ScalarValue::Float64(Some(4.0))
                } else {
                    ScalarValue::Int64(Some(4))
                }
            );
            assert_eq!(acc.state().unwrap().len(), 2);
            assert!(acc.size() >= std::mem::size_of::<PercentileAcc>());
            assert!(acc.observe_percentile(Some(f64::NAN)).is_err());
            assert!(acc.observe_percentile(Some(0.75)).is_err());
            let bad_values: ArrayRef = Arc::new(StringArray::from(vec!["not-list"]));
            let good_p: ArrayRef = Arc::new(Float64Array::from(vec![0.5]));
            assert!(
                acc.merge_batch(&[bad_values, good_p])
                    .unwrap_err()
                    .to_string()
                    .contains("must be a list")
            );
        }
    }

    #[test]
    fn percentile_error_contract_matrix_is_exact() {
        use datafusion::logical_expr::AggregateUDFImpl;

        let continuous = CypherPercentile::new(true);
        assert_eq!(
            continuous.return_type(&[]).unwrap_err().to_string(),
            "Error during planning: percentile aggregate requires value and percentile arguments"
        );
        assert_eq!(
            continuous
                .return_type(&[DataType::Utf8, DataType::Float64])
                .unwrap_err()
                .to_string(),
            "Error during planning: percentile value expression must be numeric, got Utf8"
        );
        assert_eq!(
            continuous
                .return_type(&[DataType::Int32, DataType::Float64])
                .unwrap(),
            DataType::Float64
        );
        assert_eq!(
            CypherPercentile::new(false)
                .return_type(&[DataType::Int32, DataType::Float64])
                .unwrap(),
            DataType::Int32
        );

        let mut accumulator = PercentileAcc {
            continuous: true,
            value_type: DataType::Int64,
            result_type: DataType::Float64,
            values: vec![],
            percentile: None,
        };
        accumulator.push_value(ScalarValue::Null).unwrap();
        assert_eq!(
            accumulator
                .push_value(ScalarValue::Utf8(Some("bad".into())))
                .unwrap_err()
                .to_string(),
            "Execution error: percentile value expression must be numeric, got Utf8"
        );
        for invalid in [f64::NAN, -0.1, 1.1] {
            assert!(
                accumulator
                    .observe_percentile(Some(invalid))
                    .unwrap_err()
                    .to_string()
                    .contains("finite number between 0.0 and 1.0")
            );
        }
        accumulator.observe_percentile(Some(0.25)).unwrap();
        assert_eq!(
            accumulator
                .observe_percentile(Some(0.75))
                .unwrap_err()
                .to_string(),
            "Execution error: percentile argument must be constant within an aggregate group"
        );
    }

    #[test]
    fn percentile_state_validation_errors_are_exact() {
        use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array};
        use datafusion::logical_expr::Accumulator;
        let mut accumulator = PercentileAcc {
            continuous: true,
            value_type: DataType::Int64,
            result_type: DataType::Float64,
            values: vec![],
            percentile: None,
        };
        let wrong_values: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let percentile: ArrayRef = Arc::new(Float64Array::from(vec![0.5]));
        assert_eq!(
            accumulator
                .merge_batch(&[wrong_values, percentile])
                .unwrap_err()
                .to_string(),
            "Error during planning: percentile state values must be a list"
        );
    }

    #[test]
    fn percentile_non_numeric_argument_is_rejected() {
        use datafusion::arrow::array::{ArrayRef, Int64Array};
        use datafusion::logical_expr::Accumulator;
        let mut percentile = PercentileAcc {
            continuous: true,
            value_type: DataType::Int64,
            result_type: DataType::Float64,
            values: Vec::new(),
            percentile: None,
        };
        let values: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let bad_percentile: ArrayRef =
            Arc::new(datafusion::arrow::array::StringArray::from(vec!["half"]));
        assert!(
            percentile
                .update_batch(&[values, bad_percentile])
                .unwrap_err()
                .to_string()
                .contains("must be numeric")
        );
    }
}
