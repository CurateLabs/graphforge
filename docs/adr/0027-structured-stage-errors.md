# ADR 0027: Preserve stage diagnostics at public error boundaries

## Status

Accepted for the v0.6.0 implementation of #1018.

## Decision

Core owns the existing parser, binder, lowering, and algorithm diagnostic types.
Their originating crates re-export those types, keeping dependency direction
from stages to core. The facade retains the original kind, payload, message,
and every supplied span. Binder diagnostics retain their original order; the
existing public primary span remains the first span. Lowering and algorithm
errors supply no span, so conversion must not manufacture one.

Shared Rust adapters classify these diagnostics. DataFusion source recovery
continues to preserve a downcastable GraphForge error through shared and nested
wrappers. Runtime lowering diagnostics retain the execution fault domain; a
planning `LoweringError::InvalidType` is the one intended correction from
`GF_PLAN` to `GF_VALIDATION`. This produces Python `ValidationError` with code
`GF_VALIDATION` and Node code `ValidationError`, replacing `PlanError` for this
case. Other established Rust, Python, and Node codes remain unchanged; Node's
codes are not normalized to Rust's codes.

This changes the Rust source shape of `GfError`: parse/bind variants gain
structured fields and lowering/algorithm failures gain typed variants. This is
an intentional v0.6.0 source-compatibility change, not a wire or storage format
change. Existing diagnostic display and binding presentation are retained,
including EXPLAIN's parser Display text versus execute's parser message.

The existing typed-UUID binder validation exception is centralized with binder
conversion. #1007 consumes this policy rather than inventing another binder
fault domain. Legacy cypher EXPLAIN entry points that serialize binder failures
or classify them as planning errors are unchanged here; parser-entry-point
behavior remains #1007's concern.

## Consequences

Error-code tests must cover actual facade entry points and rebuilt Python/Node
bindings, not only direct enum construction. Stage unit tests pin every typed
kind and payload; multiple binder diagnostics and nested DataFusion sources
must survive without double-prefixing or loss. No string parsing is added to
recover types, and no general error registry or reverse dependency is introduced.
