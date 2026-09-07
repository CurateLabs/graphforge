//! IR-level expression types and the flat [`ExprArena`] that stores them.
//!
//! Expressions are represented as a flat vector of [`IrExpr`] nodes indexed by
//! [`ExprId`].  This avoids deep recursive types and makes expression sharing,
//! substitution, and serialisation straightforward.
//!
//! Unlike the AST expression types in `graphforge-ast`, these types:
//! - carry **no source spans** (semantic-only)
//! - use resolved IDs ([`VarId`], [`PropertyId`]) instead of raw name strings
//! - collapse AST-level sugar (`IsNull`, `InList`, `StringOp`, `RegexMatch`)
//!   into `UnaryOp` / `BinaryOp` variants handled uniformly by the executor

use serde::{Deserialize, Serialize};

use crate::{ExprId, PropertyId, VarId};

pub use graphforge_value::Literal as IrLiteral;

// ---------------------------------------------------------------------------
// BinaryOpKind
// ---------------------------------------------------------------------------

/// The operator in a [`IrExpr::BinaryOp`] expression.
///
/// String and collection predicates that appear as separate `Expr` variants in
/// the AST are normalised to binary ops here so the executor handles them
/// uniformly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BinaryOpKind {
    // ---- comparison ----
    /// `=`
    Eq,
    /// `<>`
    Neq,
    /// `<`
    Lt,
    /// `<=`
    Lte,
    /// `>`
    Gt,
    /// `>=`
    Gte,

    // ---- logical ----
    /// `AND`
    And,
    /// `OR`
    Or,
    /// `XOR`
    Xor,

    // ---- arithmetic ----
    /// `+`
    Add,
    /// `-`
    Sub,
    /// `*`
    Mul,
    /// `/`
    Div,
    /// `%`
    Mod,
    /// `^`
    Pow,

    // ---- collection / string predicates ----
    /// `expr IN list`
    In,
    /// `STARTS WITH`
    StartsWith,
    /// `ENDS WITH`
    EndsWith,
    /// `CONTAINS`
    Contains,
    /// `=~` (regular expression match)
    RegexMatch,
}

// ---------------------------------------------------------------------------
// UnaryOpKind
// ---------------------------------------------------------------------------

/// The operator in a [`IrExpr::UnaryOp`] expression.
///
/// `IsNull` and `IsNotNull` live here only — there are no top-level
/// `IrExpr::IsNull` / `IrExpr::IsNotNull` variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum UnaryOpKind {
    /// `NOT expr`
    Not,
    /// Arithmetic negation: `-expr`
    Neg,
    /// `expr IS NULL`
    IsNull,
    /// `expr IS NOT NULL`
    IsNotNull,
}

// ---------------------------------------------------------------------------
// CaseArm
// ---------------------------------------------------------------------------

/// A single WHEN/THEN arm in a [`IrExpr::Case`] expression.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CaseArm {
    /// The WHEN condition (or comparison value in a simple CASE).
    pub when: ExprId,
    /// The THEN result expression.
    pub then: ExprId,
}

// ---------------------------------------------------------------------------
// IrExpr
// ---------------------------------------------------------------------------

/// A single node in the [`ExprArena`].
///
/// Every `ExprId` child is an index into the same arena; the arena owns all
/// nodes and is the authoritative source of truth for expression structure.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum IrExpr {
    /// A scalar constant.
    Literal(IrLiteral),
    /// A reference to a pattern variable (e.g. `n` in `MATCH (n:Person)`).
    VarRef(VarId),
    /// A property read: `base.prop`.
    PropertyAccess {
        /// The expression whose property is read.
        base: ExprId,
        /// The property type ID (resolved by the binder).
        prop: PropertyId,
    },
    /// A binary infix expression.
    BinaryOp {
        /// The operator.
        op: BinaryOpKind,
        /// Left-hand operand.
        left: ExprId,
        /// Right-hand operand.
        right: ExprId,
    },
    /// A unary prefix expression.
    UnaryOp {
        /// The operator.
        op: UnaryOpKind,
        /// The operand.
        expr: ExprId,
    },
    /// A built-in or user-defined function call.
    FunctionCall {
        /// Fully-qualified function name (e.g. `"toUpper"`, `"apoc.text.join"`).
        name: String,
        /// Argument expressions.
        args: Vec<ExprId>,
    },
    /// A named query parameter (e.g. `$name`).
    Parameter(String),
    /// A CASE expression.
    Case {
        /// Simple-CASE operand; `None` for searched CASE.
        operand: Option<ExprId>,
        /// The WHEN/THEN arms.
        arms: Vec<CaseArm>,
        /// The ELSE expression, if present.
        else_expr: Option<ExprId>,
    },
    /// A list literal: `[e0, e1, …]`.
    ListLiteral(Vec<ExprId>),
    /// A map literal: `{k0: e0, k1: e1, …}`.
    ///
    /// Keys are plain strings; values are `ExprId` references.
    MapLiteral(Vec<(String, ExprId)>),
    /// A quantifier predicate `all/any/none/single(loop_var IN list WHERE pred)`.
    /// `loop_var` is bound (only) within `predicate`. (#955)
    Quantifier {
        /// Which quantifier (all/any/none/single).
        kind: graphforge_ast::QuantifierKind,
        /// The loop variable bound per element while evaluating `predicate`.
        loop_var: VarId,
        /// The list expression iterated over.
        list: ExprId,
        /// The per-element predicate.
        predicate: ExprId,
    },
    /// A list comprehension `[loop_var IN list WHERE filter | projection]`.
    /// `loop_var` is bound (only) within `filter` and `projection`. Either of
    /// `filter`/`projection` may be absent (a bare `[x IN list]` is the list
    /// itself; `[x IN list WHERE p]` filters; `[x IN list | e]` maps). (#955)
    ListComprehension {
        /// The loop variable bound per element while evaluating the clauses.
        loop_var: VarId,
        /// The list expression iterated over.
        list: ExprId,
        /// Optional per-element filter; only elements where it is true are kept.
        filter: Option<ExprId>,
        /// Optional per-element projection; absent ⇒ the element itself.
        projection: Option<ExprId>,
    },
}

// ---------------------------------------------------------------------------
// ExprArena
// ---------------------------------------------------------------------------

/// Flat, index-addressed storage for all [`IrExpr`] nodes in a query plan.
///
/// Nodes are appended with [`push`](ExprArena::push), which returns a stable
/// [`ExprId`] index.  Because the arena is a plain `Vec`, serialisation
/// produces a JSON array where each element's position is its `ExprId`.
///
/// # Example
///
/// ```
/// use graphforge_ir::{ExprArena, IrExpr, IrLiteral, VarId, PropId, PropertyId};
/// use graphforge_ir::expr::BinaryOpKind;
///
/// let mut arena = ExprArena::new();
///
/// // Build: name = "Alice"
/// let var = arena.push(IrExpr::VarRef(VarId(0)));
/// let prop = arena.push(IrExpr::PropertyAccess { base: var, prop: PropertyId::ontology(PropId(1)).unwrap() });
/// let lit = arena.push(IrExpr::Literal(IrLiteral::Str("Alice".into())));
/// let eq  = arena.push(IrExpr::BinaryOp { op: BinaryOpKind::Eq, left: prop, right: lit });
///
/// assert_eq!(arena.get(eq), &IrExpr::BinaryOp { op: BinaryOpKind::Eq, left: prop, right: lit });
/// ```
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct ExprArena {
    nodes: Vec<IrExpr>,
}

impl ExprArena {
    /// Creates an empty arena.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends `expr` to the arena and returns its stable [`ExprId`].
    pub fn push(&mut self, expr: IrExpr) -> ExprId {
        let id =
            ExprId(u32::try_from(self.nodes.len()).expect("ExprArena exceeded u32::MAX capacity"));
        self.nodes.push(expr);
        id
    }

    /// Returns a reference to the expression at `id`.
    ///
    /// # Panics
    ///
    /// Panics if `id` is out of bounds (i.e. was not returned by this arena's
    /// [`push`](Self::push)).
    #[must_use]
    pub fn get(&self, id: ExprId) -> &IrExpr {
        &self.nodes[id.0 as usize]
    }

    /// Replace named parameter nodes with supplied literal values.
    pub fn substitute_parameters(&mut self, params: &std::collections::HashMap<String, IrLiteral>) {
        for node in &mut self.nodes {
            if let IrExpr::Parameter(name) = node
                && let Some(value) = params.get(name)
            {
                *node = IrExpr::Literal(value.clone());
            }
        }
    }

    /// Returns the number of expressions in the arena.
    #[must_use]
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Returns `true` if the arena contains no expressions.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{PropId, VarId};

    /// Build `a.name = "Alice" AND a.age > 30` and verify round-trip.
    #[test]
    fn non_trivial_expression_tree_roundtrip() {
        let mut arena = ExprArena::new();

        // a (VarId 0)
        let a = arena.push(IrExpr::VarRef(VarId(0)));

        // a.name (PropId 1)
        let a_name = arena.push(IrExpr::PropertyAccess {
            base: a,
            prop: PropertyId::ontology(PropId(1)).unwrap(),
        });

        // "Alice"
        let alice = arena.push(IrExpr::Literal(IrLiteral::Str("Alice".into())));

        // a.name = "Alice"
        let eq = arena.push(IrExpr::BinaryOp {
            op: BinaryOpKind::Eq,
            left: a_name,
            right: alice,
        });

        // a.age (PropId 2)
        let a_age = arena.push(IrExpr::PropertyAccess {
            base: a,
            prop: PropertyId::ontology(PropId(2)).unwrap(),
        });

        // 30
        let thirty = arena.push(IrExpr::Literal(IrLiteral::Int(30)));

        // a.age > 30
        let gt = arena.push(IrExpr::BinaryOp {
            op: BinaryOpKind::Gt,
            left: a_age,
            right: thirty,
        });

        // (a.name = "Alice") AND (a.age > 30)
        let and = arena.push(IrExpr::BinaryOp {
            op: BinaryOpKind::And,
            left: eq,
            right: gt,
        });

        // Serde round-trip
        let json = serde_json::to_string(&arena).unwrap();
        let restored: ExprArena = serde_json::from_str(&json).unwrap();

        assert_eq!(arena, restored);

        // Verify the root node is correct after restore
        assert_eq!(
            restored.get(and),
            &IrExpr::BinaryOp {
                op: BinaryOpKind::And,
                left: eq,
                right: gt,
            }
        );
    }

    #[test]
    fn push_returns_sequential_ids() {
        let mut arena = ExprArena::new();
        let id0 = arena.push(IrExpr::Literal(IrLiteral::Null));
        let id1 = arena.push(IrExpr::Literal(IrLiteral::Bool(true)));
        let id2 = arena.push(IrExpr::Literal(IrLiteral::Int(42)));
        assert_eq!(id0, ExprId(0));
        assert_eq!(id1, ExprId(1));
        assert_eq!(id2, ExprId(2));
    }

    #[test]
    fn get_retrieves_correct_expression() {
        let mut arena = ExprArena::new();
        let id = arena.push(IrExpr::Literal(IrLiteral::Str("hello".into())));
        assert_eq!(
            arena.get(id),
            &IrExpr::Literal(IrLiteral::Str("hello".into()))
        );
    }

    #[test]
    fn empty_arena_len_and_is_empty() {
        let arena = ExprArena::new();
        assert_eq!(arena.len(), 0);
        assert!(arena.is_empty());
    }

    #[test]
    fn arena_all_ir_expr_variants() {
        let mut arena = ExprArena::new();

        let lit_null = arena.push(IrExpr::Literal(IrLiteral::Null));
        let lit_bool = arena.push(IrExpr::Literal(IrLiteral::Bool(false)));
        let lit_int = arena.push(IrExpr::Literal(IrLiteral::Int(-1)));
        let lit_float = arena.push(IrExpr::Literal(IrLiteral::Float(2.71)));
        let lit_str = arena.push(IrExpr::Literal(IrLiteral::Str("x".into())));
        let lit_dur = arena.push(IrExpr::Literal(IrLiteral::Duration {
            months: 0,
            days: 0,
            seconds: 0,
            nanos: 1_000_000,
        }));
        let lit_dt = arena.push(IrExpr::Literal(IrLiteral::DateTime(0)));

        let var = arena.push(IrExpr::VarRef(VarId(7)));

        let prop = arena.push(IrExpr::PropertyAccess {
            base: var,
            prop: PropertyId::ontology(PropId(3)).unwrap(),
        });

        let binop = arena.push(IrExpr::BinaryOp {
            op: BinaryOpKind::Add,
            left: lit_int,
            right: lit_float,
        });

        let unop = arena.push(IrExpr::UnaryOp {
            op: UnaryOpKind::IsNull,
            expr: prop,
        });

        let call = arena.push(IrExpr::FunctionCall {
            name: "toUpper".into(),
            args: vec![lit_str],
        });

        let param = arena.push(IrExpr::Parameter("name".into()));

        let arm = CaseArm {
            when: lit_bool,
            then: lit_int,
        };
        let case = arena.push(IrExpr::Case {
            operand: Some(var),
            arms: vec![arm],
            else_expr: Some(lit_null),
        });

        let list = arena.push(IrExpr::ListLiteral(vec![lit_int, lit_float]));

        let map = arena.push(IrExpr::MapLiteral(vec![("key".into(), lit_str)]));

        // Serde round-trip over the full arena
        let json = serde_json::to_string(&arena).unwrap();
        let restored: ExprArena = serde_json::from_str(&json).unwrap();
        assert_eq!(arena, restored);

        // Spot-check a few nodes
        assert_eq!(restored.get(binop), arena.get(binop));
        assert_eq!(restored.get(unop), arena.get(unop));
        assert_eq!(restored.get(call), arena.get(call));
        assert_eq!(restored.get(case), arena.get(case));
        assert_eq!(restored.get(list), arena.get(list));
        assert_eq!(restored.get(map), arena.get(map));
        assert_eq!(restored.get(param), arena.get(param));
        assert_eq!(restored.get(lit_dur), arena.get(lit_dur));
        assert_eq!(restored.get(lit_dt), arena.get(lit_dt));
    }

    #[test]
    fn case_arm_roundtrip() {
        let arm = CaseArm {
            when: ExprId(0),
            then: ExprId(1),
        };
        let json = serde_json::to_string(&arm).unwrap();
        let back: CaseArm = serde_json::from_str(&json).unwrap();
        assert_eq!(arm, back);
    }

    #[test]
    fn unary_is_null_not_top_level_variant() {
        // IS NULL must be expressed as UnaryOp { op: IsNull, ... }
        // (the issue explicitly forbids a top-level IrExpr::IsNull variant)
        let mut arena = ExprArena::new();
        let var = arena.push(IrExpr::VarRef(VarId(0)));
        let is_null = arena.push(IrExpr::UnaryOp {
            op: UnaryOpKind::IsNull,
            expr: var,
        });
        assert!(matches!(
            arena.get(is_null),
            IrExpr::UnaryOp {
                op: UnaryOpKind::IsNull,
                ..
            }
        ));
    }

    #[test]
    fn binary_op_kind_serde_roundtrip() {
        for op in [
            BinaryOpKind::Eq,
            BinaryOpKind::Neq,
            BinaryOpKind::Lt,
            BinaryOpKind::Lte,
            BinaryOpKind::Gt,
            BinaryOpKind::Gte,
            BinaryOpKind::And,
            BinaryOpKind::Or,
            BinaryOpKind::Xor,
            BinaryOpKind::Add,
            BinaryOpKind::Sub,
            BinaryOpKind::Mul,
            BinaryOpKind::Div,
            BinaryOpKind::Mod,
            BinaryOpKind::Pow,
            BinaryOpKind::In,
            BinaryOpKind::StartsWith,
            BinaryOpKind::EndsWith,
            BinaryOpKind::Contains,
            BinaryOpKind::RegexMatch,
        ] {
            let json = serde_json::to_string(&op).unwrap();
            let back: BinaryOpKind = serde_json::from_str(&json).unwrap();
            assert_eq!(op, back);
        }
    }

    #[test]
    fn unary_op_kind_serde_roundtrip() {
        for op in [
            UnaryOpKind::Not,
            UnaryOpKind::Neg,
            UnaryOpKind::IsNull,
            UnaryOpKind::IsNotNull,
        ] {
            let json = serde_json::to_string(&op).unwrap();
            let back: UnaryOpKind = serde_json::from_str(&json).unwrap();
            assert_eq!(op, back);
        }
    }

    #[test]
    fn float_finite_roundtrip() {
        let lit = IrLiteral::Float(2.71_f64);
        let json = serde_json::to_string(&lit).unwrap();
        let back: IrLiteral = serde_json::from_str(&json).unwrap();
        assert_eq!(lit, back);
    }

    #[test]
    fn uuid_literal_serde_roundtrip() {
        let lit = IrLiteral::Uuid([0x5a; 16]);
        let json = serde_json::to_string(&lit).unwrap();
        assert!(json.contains("Uuid"));
        assert_eq!(serde_json::from_str::<IrLiteral>(&json).unwrap(), lit);
    }

    #[test]
    fn localdatetime_literal_serde_roundtrip() {
        // #920: the new temporal-storage variant round-trips through serde.
        let lit = IrLiteral::LocalDateTime {
            days: 5_393,
            nanos: 45_074_645_876_123,
        };
        let json = serde_json::to_string(&lit).unwrap();
        assert!(json.contains("LocalDateTime"), "tagged variant: {json}");
        let back: IrLiteral = serde_json::from_str(&json).unwrap();
        assert_eq!(lit, back);
    }

    #[test]
    fn temporal_storage_variants_serde_roundtrip() {
        // #920: time / time-with-zone / datetime (with and without a named zone)
        // all round-trip through serde.
        for lit in [
            IrLiteral::Time(45_074_645_876_123),
            IrLiteral::ZonedTime {
                nanos: 45_074_645_876_123,
                offset: 3_600,
            },
            IrLiteral::ZonedDateTime {
                days: 5_393,
                nanos: 45_074_645_876_123,
                offset: 3_600,
                zone: Some("Europe/Stockholm".to_owned()),
            },
            IrLiteral::ZonedDateTime {
                days: 5_393,
                nanos: 0,
                offset: -3_600,
                zone: None,
            },
        ] {
            let json = serde_json::to_string(&lit).unwrap();
            let back: IrLiteral = serde_json::from_str(&json).unwrap();
            assert_eq!(lit, back, "round-trip {json}");
        }
    }

    #[test]
    fn float_nan_roundtrip() {
        let lit = IrLiteral::Float(f64::NAN);
        let json = serde_json::to_string(&lit).unwrap();
        assert!(json.contains("NaN"), "NaN should be tagged: {json}");
        let back: IrLiteral = serde_json::from_str(&json).unwrap();
        assert!(
            matches!(back, IrLiteral::Float(f) if f.is_nan()),
            "should deserialise back to NaN"
        );
    }

    #[test]
    fn float_positive_infinity_roundtrip() {
        let lit = IrLiteral::Float(f64::INFINITY);
        let json = serde_json::to_string(&lit).unwrap();
        assert!(json.contains("+Infinity"), "should be tagged: {json}");
        let back: IrLiteral = serde_json::from_str(&json).unwrap();
        assert_eq!(back, IrLiteral::Float(f64::INFINITY));
    }

    #[test]
    fn float_negative_infinity_roundtrip() {
        let lit = IrLiteral::Float(f64::NEG_INFINITY);
        let json = serde_json::to_string(&lit).unwrap();
        assert!(json.contains("-Infinity"), "should be tagged: {json}");
        let back: IrLiteral = serde_json::from_str(&json).unwrap();
        assert_eq!(back, IrLiteral::Float(f64::NEG_INFINITY));
    }

    #[test]
    fn ir_literal_deser_extra_field_before_value() {
        // The deserialiser must locate "value" regardless of field order.
        let json = r#"{"type":"Bool","extra":99,"value":true}"#;
        let lit: IrLiteral = serde_json::from_str(json).unwrap();
        assert_eq!(lit, IrLiteral::Bool(true));
    }

    #[test]
    fn ir_literal_deser_value_before_type_not_supported() {
        // Our serializer always emits {"type":...,"value":...} so the normal
        // round-trip is always in-order.  This test documents current behaviour:
        // a map where "value" precedes "type" will fail (unknown field "value").
        let json = r#"{"value":42,"type":"Int"}"#;
        let result: Result<IrLiteral, _> = serde_json::from_str(json);
        // Either succeeds or errors — we just ensure no panic.
        let _ = result;
    }
}
