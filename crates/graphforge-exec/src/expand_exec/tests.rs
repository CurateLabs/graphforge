use super::*;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use std::sync::Arc;

#[test]
fn unused_list_output_preserves_exact_non_nullable_child_schema() {
    let item = Arc::new(Field::new("item", DataType::UInt32, false));
    let field = Field::new("type_ids", DataType::List(Arc::clone(&item)), false);
    let column = unused_expand_column(&field, 3).unwrap();

    assert_eq!(column.data_type(), field.data_type());
    assert_eq!(column.len(), 3);
    assert_eq!(column.null_count(), 0);
}

#[test]
fn required_v4_destination_identity_fails_closed_without_admitted_session() {
    let error = require_admitted_ordinal_identity(true, false)
        .expect_err("required v4 identity must not fall back without its admitted session");
    assert!(
        error
            .to_string()
            .contains("requires admitted v4 ordinal identity")
    );
    require_admitted_ordinal_identity(true, true).expect("admitted v4 session");
    require_admitted_ordinal_identity(false, false).expect("legacy generation fallback");
}
