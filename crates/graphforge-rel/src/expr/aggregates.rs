//! Cypher aggregate implementations and their state contracts.

use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{Signature, Volatility};
use datafusion::scalar::ScalarValue;
use std::sync::LazyLock;

use super::value_semantics::{DistinctIndex, cypher_order, cypher_value_eq, scalar_as_f64};

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
        Ok(Box::new(CollectAcc::new(
            self.distinct,
            field.data_type().clone(),
        )))
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
    /// Hash-bucketed membership over `values`; consulted only when `distinct`.
    seen: DistinctIndex,
}

impl CollectAcc {
    fn new(distinct: bool, elem_type: DataType) -> Self {
        Self {
            distinct,
            elem_type,
            values: Vec::new(),
            seen: DistinctIndex::default(),
        }
    }

    fn push_value(&mut self, v: ScalarValue) {
        self.push_value_with(v, cypher_value_eq);
    }

    /// `push_value` with the equality used to detect a repeat injected, so a test
    /// can count how many comparisons a `DISTINCT` collection performs.
    fn push_value_with(
        &mut self,
        v: ScalarValue,
        eq: impl Fn(&ScalarValue, &ScalarValue) -> Option<bool>,
    ) {
        if v.is_null() {
            return;
        }
        if self.distinct {
            self.seen.admit(&mut self.values, v, eq);
        } else {
            self.values.push(v);
        }
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
        std::mem::size_of_val(self)
            + self.values.iter().map(ScalarValue::size).sum::<usize>()
            + self.seen.heap_size()
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
            let mut acc = CollectAcc::new(distinct, DataType::Int64);
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

    /// `collect(DISTINCT)` accumulator behaviour: Cypher DISTINCT equality, first-seen
    /// order, and work that stays linear in the number of values collected.
    mod collect_distinct {
        use super::*;
        use crate::expr::tests::values::{float, int, ints, node, relationship, text};
        use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array, ListArray};
        use datafusion::arrow::datatypes::Int64Type;
        use datafusion::logical_expr::Accumulator;
        use std::cell::Cell;

        fn collected(acc: &mut CollectAcc) -> ArrayRef {
            let ScalarValue::List(list) = acc.evaluate().unwrap() else {
                panic!("collect must return a list")
            };
            list.value(0)
        }

        fn float_bits(values: &ArrayRef) -> Vec<u64> {
            let floats = values.as_any().downcast_ref::<Float64Array>().unwrap();
            floats.values().iter().map(|f| f.to_bits()).collect()
        }

        fn float_array(values: Vec<Option<f64>>) -> ArrayRef {
            Arc::new(Float64Array::from(values))
        }

        #[test]
        fn mixed_numbers_equal_across_types_collapse_and_nan_never_does() {
            let mut acc = CollectAcc::new(true, DataType::Float64);
            for v in [
                int(3),
                float(3.0), // equals 3
                float(2.5),
                int(2), // not equal to 2.5
                float(f64::NAN),
                float(f64::NAN), // NaN equals nothing, itself included
                text("3"),       // a string is not the number
                int(0),
                float(-0.0), // equals 0
                ScalarValue::Int64(None),
                ScalarValue::Float64(None),
                float(f64::INFINITY),
                float(f64::INFINITY),
            ] {
                acc.push_value(v);
            }
            assert_eq!(
                acc.values,
                vec![
                    int(3),
                    float(2.5),
                    int(2),
                    float(f64::NAN),
                    float(f64::NAN),
                    text("3"),
                    int(0),
                    float(f64::INFINITY),
                ]
            );
        }

        #[test]
        fn nested_lists_and_maps_compare_element_wise() {
            use crate::expr::tests::values::{large_list, list, map};
            let mut acc = CollectAcc::new(true, DataType::Null);
            let floats = |items: &[f64]| {
                let items: Vec<_> = items.iter().copied().map(float).collect();
                list(&items, &DataType::Float64)
            };
            let nested = |inner: ScalarValue| list(&[inner.clone()], &inner.data_type());
            let with_null = list(&[int(1), ScalarValue::Int64(None)], &DataType::Int64);
            for v in [
                ints(&[1, 2]),
                floats(&[1.0, 2.0]), // equals [1, 2]
                ints(&[2, 1]),
                ints(&[]),
                ints(&[]),
                nested(ints(&[1])),
                nested(floats(&[1.0])), // equals [[1]]
                large_list(&[int(1), int(2)], &DataType::Int64), // a LargeList is another type
                with_null.clone(),      // [1, null] equals nothing, itself included
                with_null.clone(),
                map(vec![("a", int(1)), ("b", int(2))]),
                map(vec![("b", float(2.0)), ("a", float(1.0))]), // same map
                map(vec![("a", int(1))]),
                map(vec![("a", ScalarValue::Int64(None))]), // a null value: equals nothing
                map(vec![("a", ScalarValue::Int64(None))]),
            ] {
                acc.push_value(v);
            }
            let kept = &acc.values;
            assert_eq!(kept.len(), 11, "{kept:#?}");
            assert_eq!(kept[0], ints(&[1, 2]));
            assert_eq!(kept[1], ints(&[2, 1]));
            assert_eq!(kept[2], ints(&[]));
            assert_eq!(kept[3], nested(ints(&[1])));
            assert_eq!(kept[4], large_list(&[int(1), int(2)], &DataType::Int64));
            assert_eq!(kept[5], with_null);
            assert_eq!(kept[6], with_null);
            assert_eq!(kept[7], map(vec![("a", int(1)), ("b", int(2))]));
            assert_eq!(kept[8], map(vec![("a", int(1))]));
            // A map holding a null value equals nothing, itself included.
            assert_eq!(kept[9], map(vec![("a", ScalarValue::Int64(None))]));
            assert_eq!(kept[10], map(vec![("a", ScalarValue::Int64(None))]));
        }

        #[test]
        fn entities_collapse_by_identity_not_by_properties() {
            let mut acc = CollectAcc::new(true, DataType::Null);
            for v in [
                node(1, 5),
                node(2, 5), // same properties, different node
                node(1, 6), // same node, different properties
                relationship(1, 5),
                relationship(1, 6),
                node(3, 5),
            ] {
                acc.push_value(v);
            }
            // A node and a relationship with one identity stay distinct values.
            assert_eq!(
                acc.values,
                vec![node(1, 5), node(2, 5), relationship(1, 5), node(3, 5)]
            );
        }

        #[test]
        fn update_batch_dedups_a_float_column_keeping_first_seen_order() {
            let mut acc = CollectAcc::new(true, DataType::Float64);
            let column = float_array(vec![
                Some(1.0),
                Some(f64::NAN),
                Some(1.0),
                None,
                Some(f64::NAN),
                Some(2.0),
                Some(-0.0),
                Some(0.0),
            ]);
            acc.update_batch(&[column]).unwrap();
            assert_eq!(
                float_bits(&collected(&mut acc)),
                [
                    1.0_f64.to_bits(),
                    f64::NAN.to_bits(),
                    f64::NAN.to_bits(),
                    2.0_f64.to_bits(),
                    (-0.0_f64).to_bits(),
                ]
            );
        }

        #[test]
        fn update_batch_dedups_a_node_column_by_identity() {
            let rows = [node(1, 5), node(2, 5), node(1, 6), node(3, 5), node(2, 9)];
            let column = ScalarValue::iter_to_array(rows.clone()).unwrap();
            let mut acc = CollectAcc::new(true, column.data_type().clone());
            acc.update_batch(&[column]).unwrap();
            // Equal-property nodes stay apart; repeated identities collapse onto the first.
            assert_eq!(
                acc.values,
                vec![rows[0].clone(), rows[1].clone(), rows[3].clone()]
            );
            assert_eq!(collected(&mut acc).len(), 3);
        }

        #[test]
        fn non_distinct_collect_keeps_every_non_null_value() {
            let mut acc = CollectAcc::new(false, DataType::Float64);
            let column = float_array(vec![Some(1.0), Some(1.0), None, Some(f64::NAN), Some(1.0)]);
            acc.update_batch(&[column]).unwrap();
            assert_eq!(collected(&mut acc).len(), 4);
        }

        fn partial(values: Vec<Option<f64>>) -> CollectAcc {
            let mut acc = CollectAcc::new(true, DataType::Float64);
            acc.update_batch(&[float_array(values)]).unwrap();
            acc
        }

        fn merge_state(into: &mut CollectAcc, from: &mut CollectAcc) {
            let state = from.state().unwrap().remove(0).to_array().unwrap();
            into.merge_batch(&[state]).unwrap();
        }

        #[test]
        fn merge_batch_across_partial_states_matches_one_sequential_pass() {
            let first = vec![Some(1.0), Some(f64::NAN), Some(2.0), Some(1.0)];
            let second = vec![Some(f64::NAN), Some(2.0), Some(3.0), None, Some(1.0)];
            let third = vec![Some(3.0), Some(0.0), Some(-0.0), Some(4.0)];

            let mut sequential = CollectAcc::new(true, DataType::Float64);
            for batch in [&first, &second, &third] {
                sequential
                    .update_batch(&[float_array(batch.clone())])
                    .unwrap();
            }

            let mut merged = CollectAcc::new(true, DataType::Float64);
            for batch in [&first, &second, &third] {
                merge_state(&mut merged, &mut partial(batch.clone()));
            }
            assert_eq!(
                float_bits(&collected(&mut merged)),
                float_bits(&collected(&mut sequential))
            );
            // 1, NaN, 2, NaN, 3, 0, 4: both NaNs survive, -0.0 collapses onto 0.0.
            assert_eq!(collected(&mut merged).len(), 7);

            // A partial state that overlaps what the target already holds only adds the new.
            let mut target = partial(first.clone());
            merge_state(&mut target, &mut partial(second.clone()));
            assert_eq!(
                float_bits(&collected(&mut target)),
                [1.0, f64::NAN, 2.0, f64::NAN, 3.0].map(f64::to_bits)
            );
        }

        #[test]
        fn merge_batch_skips_null_states_and_null_elements_and_spans_rows() {
            let states: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>([
                Some(vec![Some(1), Some(2), None]),
                None,
                Some(vec![Some(2), Some(3)]),
                Some(vec![]),
                Some(vec![Some(1), Some(4)]),
            ]));
            let mut acc = CollectAcc::new(true, DataType::Int64);
            acc.merge_batch(&[states]).unwrap();
            let kept = collected(&mut acc);
            let kept = kept.as_any().downcast_ref::<Int64Array>().unwrap();
            assert_eq!(kept.values().to_vec(), vec![1, 2, 3, 4]);
        }

        #[test]
        fn distinct_collect_charges_its_membership_index() {
            let mut distinct = CollectAcc::new(true, DataType::Int64);
            let mut plain = CollectAcc::new(false, DataType::Int64);
            for n in 0..1_000 {
                distinct.push_value(int(n));
                plain.push_value(int(n));
            }
            assert_eq!(distinct.values, plain.values);
            assert!(
                distinct.size() >= plain.size() + 1_000 * std::mem::size_of::<usize>(),
                "distinct {} vs plain {}",
                distinct.size(),
                plain.size()
            );
        }

        /// Push `values` then the same values again, counting equality evaluations.
        fn comparisons_for(values: &[ScalarValue]) -> (usize, usize) {
            let mut acc = CollectAcc::new(true, DataType::Null);
            let calls = Cell::new(0_usize);
            let counting = |a: &ScalarValue, b: &ScalarValue| {
                calls.set(calls.get() + 1);
                crate::expr::value_semantics::cypher_value_eq(a, b)
            };
            for v in values.iter().chain(values) {
                acc.push_value_with(v.clone(), counting);
            }
            assert_eq!(
                acc.values.len(),
                values.len(),
                "every distinct value kept once"
            );
            (values.len(), calls.get())
        }

        #[test]
        fn distinct_collect_does_linear_equality_work() {
            // Each of n distinct values is compared against (almost) nothing on its
            // first sight and against its one twin on its second: about n comparisons
            // in all. A scan of every value seen would need n(n-1)/2 + n^2/2, which is
            // quadratic and fails the bound at both sizes.
            for n in [2_000_i64, 4_000] {
                let numbers: Vec<_> = (0..n)
                    .map(|i| if i % 2 == 0 { int(i) } else { float(i as f64) })
                    .collect();
                let nodes: Vec<_> = (0..n).map(|i| node(u64::try_from(i).unwrap(), 0)).collect();
                for (label, values) in [("numbers", numbers), ("nodes", nodes)] {
                    let (distinct, comparisons) = comparisons_for(&values);
                    assert!(
                        comparisons <= 2 * distinct,
                        "{label}: {comparisons} comparisons for {distinct} distinct values"
                    );
                }
            }
        }
    }
}
