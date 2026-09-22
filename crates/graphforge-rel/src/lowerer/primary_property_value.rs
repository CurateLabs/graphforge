//! Reconcile primary-owner values after UUID/membership filtering, without casts.
use datafusion::{
    arrow::{
        array::{Array, ArrayRef, Int8Array, MutableArrayData, make_array, new_null_array},
        buffer::NullBuffer,
        datatypes::{DataType, Field, FieldRef},
    },
    common::{DataFusionError, Result},
    logical_expr::{
        ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
        Signature, Volatility,
    },
};
use std::sync::Arc;

pub(super) fn expression(values: Vec<Expr>, expected: Option<FieldRef>) -> Expr {
    ScalarUDF::new_from_impl(PrimaryPropertyValue {
        signature: Signature::variadic_any(Volatility::Immutable),
        expected,
    })
    .call(values)
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct PrimaryPropertyValue {
    signature: Signature,
    expected: Option<FieldRef>,
}
impl ScalarUDFImpl for PrimaryPropertyValue {
    fn name(&self) -> &'static str {
        "graphforge_primary_property_value"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        Ok(self.expected.as_ref().map_or_else(
            || DataType::Struct(graphforge_value::heterogeneous::dynamic_fields(types)),
            |field| field.data_type().clone(),
        ))
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        if let Some(expected) = &self.expected {
            return Ok(expected.clone());
        }
        let types = args
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect::<Vec<_>>();
        let fields = graphforge_value::heterogeneous::dynamic_fields(&types)
            .iter()
            .enumerate()
            .map(|(index, field)| {
                if index == 0 {
                    field.clone()
                } else {
                    Arc::new(
                        field
                            .as_ref()
                            .clone()
                            .with_metadata(args.arg_fields[index - 1].metadata().clone()),
                    )
                }
            })
            .collect::<Vec<_>>();
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Struct(fields.into()),
            true,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = args
            .args
            .iter()
            .map(|value| value.to_array(args.number_rows))
            .collect::<Result<Vec<_>>>()?;
        let selected = selected_rows(&arrays, args.number_rows)?;
        let output = if let Some(expected) = &self.expected {
            typed_values(&arrays, &args.arg_fields, expected, &selected)?
        } else {
            if arrays.len() > 127 {
                return Err(invalid());
            }
            let tags = selected
                .iter()
                .map(|index| i8::try_from(index.unwrap_or(0)).map_err(|_| invalid()))
                .collect::<Result<Vec<_>>>()?;
            let encoded = graphforge_value::heterogeneous::encode_dynamic_rows(
                Int8Array::from(tags),
                arrays,
                Some(NullBuffer::from(
                    selected.iter().map(Option::is_some).collect::<Vec<_>>(),
                )),
            )
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
            let DataType::Struct(fields) = args.return_field.data_type() else {
                return Err(invalid());
            };
            Arc::new(datafusion::arrow::array::StructArray::try_new(
                fields.clone(),
                encoded.columns().to_vec(),
                encoded.nulls().cloned(),
            )?) as ArrayRef
        };
        Ok(ColumnarValue::Array(output))
    }
}
fn selected_rows(arrays: &[ArrayRef], rows: usize) -> Result<Vec<Option<usize>>> {
    (0..rows)
        .map(|row| {
            let mut selected = None;
            for (index, array) in arrays.iter().enumerate() {
                let present = if array.is_null(row) {
                    false
                } else if let Some(values) = array
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::StructArray>()
                {
                    if graphforge_value::heterogeneous::recognize(values.data_type())
                        .map_err(|error| DataFusionError::External(Box::new(error)))?
                        .is_some()
                    {
                        !matches!(
                            graphforge_value::heterogeneous::decode_row(values, row)
                                .map_err(|error| DataFusionError::External(Box::new(error)))?,
                            graphforge_value::heterogeneous::Decoded::Null
                        )
                    } else {
                        true
                    }
                } else {
                    true
                };
                if present {
                    if selected.is_some() {
                        return Err(invalid());
                    }
                    selected = Some(index);
                }
            }
            Ok(selected)
        })
        .collect()
}
fn typed_values(
    arrays: &[ArrayRef],
    fields: &[FieldRef],
    expected: &Field,
    selected: &[Option<usize>],
) -> Result<ArrayRef> {
    let compatible: Vec<_> = fields
        .iter()
        .map(|field| {
            field.data_type() == expected.data_type() && field.metadata() == expected.metadata()
        })
        .collect();
    if selected.iter().flatten().any(|index| !compatible[*index]) {
        return Err(invalid());
    }
    let data: Vec<_> = arrays
        .iter()
        .zip(&compatible)
        .map(|(array, matches)| {
            if *matches {
                array.to_data()
            } else {
                new_null_array(expected.data_type(), array.len()).to_data()
            }
        })
        .collect();
    let mut output = MutableArrayData::new(data.iter().collect(), true, selected.len());
    for (row, index) in selected.iter().enumerate() {
        if let Some(index) = index {
            output.extend(*index, row, row + 1);
        } else {
            output.extend_nulls(1);
        }
    }
    Ok(make_array(output.freeze()))
}
fn invalid() -> DataFusionError {
    DataFusionError::Execution(
        "incompatible or ambiguous property values for the matched semantic owner".into(),
    )
}
