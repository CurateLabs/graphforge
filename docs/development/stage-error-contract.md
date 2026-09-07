# Stage error compatibility contract

Issue #1018 and ADR 0027 define the v0.6.0 boundary. Core owns the existing
`ParseError`, `BindError`, `LoweringError`, and `AlgorithmError` types; their
originating crates re-export them. `GfError` retains complete diagnostic payloads
and every supplied span. The Rust enum's source shape changes in v0.6.0.

## Conversion matrix

| Origin | Rust facade | Rust/Python code | Python exception | Node code | Span |
|---|---|---|---|---|---|
| Parser, every kind and expected/found payload | Parse with original diagnostic | GF_PARSE | ParseError | ParseError | Original; Python tuple; Node prefix |
| Binder, every diagnostic in order | Bind | GF_PARSE | ParseError | ParseError | All retained; first is public primary |
| Existing typed-UUID binder validation | BindValidation | GF_VALIDATION | ValidationError | GF_VALIDATION | Original diagnostics retained; no new binding span |
| Legacy cypher logical EXPLAIN binder rejection | BindPlan | GF_PLAN | PlanError if transported | PlanError if transported | Original diagnostics retained |
| Planning UnknownFunction/UnsupportedExpr/UnboundVar | Lowering | GF_PLAN | PlanError | PlanError | None supplied |
| Planning InvalidType | Lowering | GF_VALIDATION | ValidationError | ValidationError | None supplied |
| Runtime lowering error, or established execute coercion remap | LoweringExecution | GF_EXECUTION | ExecutionError | ExecutionError | None supplied |
| Algorithm Unavailable/DuplicateCapability | Algorithm | GF_VALIDATION | ValidationError | ValidationError | None supplied |
| Other AlgorithmError variants | Algorithm | GF_EXECUTION | ExecutionError | ExecutionError | None supplied |
| Existing GfError inside DataFusion | Original typed variant | Original code | Original mapping | Original mapping | Original |
| Foreign planner/execution error without a GraphForge source | Plan/Execution fallback | GF_PLAN/GF_EXECUTION | PlanError/ExecutionError | PlanError/ExecutionError | None supplied |

Planning `InvalidType` is the deliberate public-code correction from GF_PLAN to
GF_VALIDATION. It is a validation rejection, not a planner capability gap. Node
codes otherwise retain their established spellings; this work does not change
all Node codes into Rust's GF-prefixed codes. Existing project/API typed codes,
provider attributes, validation exceptions, and other fault domains are unchanged.

Parser message presentation is also preserved: execute uses the parser's detailed
message, while EXPLAIN uses its Display. Both retain the complete parser record.
Legacy cypher EXPLAIN methods that successfully return `bind_errors` JSON keep
that behavior; #1007 owns entry-point changes and consumes the shared binder
conversion rather than creating another policy.

The existing foreign-DataFusion coercion/placeholder compatibility classifier
continues to apply to Plan and typed UnsupportedExpr failures. It cannot override
InvalidType based on user-controlled property names. For example,
``RETURN 1.`Cannot coerce` `` still produces GF_VALIDATION.

## Executable evidence

`crates/graphforge-api/tests/stage_error_matrix.json` is shared by Rust's
`bind_error_spans` integration target, Python's `stage_errors.py`, and Node's
`stage-errors.test.mjs`. It covers parser/binder failures, two real InvalidType
paths, the quoted-property collision, foreign coercion and missing-parameter
compatibility, and an actual undefined Euler-circuit failure. Rust additionally
checks exact kinds, payloads and spans. The matrix preserves stream/execute/
EXPLAIN differences rather than assuming all entry points classify alike.

Core tests enumerate every parser, binder, lowering and algorithm kind. Multiple
real binder errors retain individual spans. Executor tests exercise nested/shared
DataFusion recovery and physical planning/stream boundaries. The TCK harness
recognizes typed planning InvalidType as compile-time; it does not accept all
GF_VALIDATION errors as compile-time failures.

Native matrix tests require rebuilding the bindings from the tested Rust source.
A debug native build is sufficient for correctness evidence; it makes no release
performance or package-certification claim.
