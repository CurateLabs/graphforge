# Semantic committed-generation graph diff

`GraphForge::diff_committed_generations` is the Rust-owned interchange contract
for deriving graph changes between two exact committed generations. Both
endpoints are bound by generation UUID and manifest SHA-256. Resolution leases
those immutable generations directly; it does not consult `CURRENT` again and
does not expose or interpret mutation journals.

The result contains six deterministic Arrow IPC streams: added, removed, and
modified nodes, followed by the corresponding edge streams. Rows are ordered by
durable UUID. Added and modified rows contain complete target-generation values;
edge rows include their exact source and target UUIDs and relationship type.
Modified-record maps carry the canonical sorted names of properties whose value
changed. Every stream schema identifies the contract, change kind, endpoint
UUIDs, and endpoint manifest fingerprints.

## Frozen Arrow contract

All six streams use schema metadata `graphforge.contract =
semantic-generation-diff/1`. They also carry `graphforge.change_kind` plus the
source and target generation UUID and manifest SHA-256 under the corresponding
`graphforge.source_*` and `graphforge.target_*` keys. Consumers must reject an
unknown contract value or metadata that does not match the requested endpoints.

Node rows contain non-null `record_uuid: FixedSizeBinary(16)`, `labels`, and
`properties`. Edge rows contain non-null `record_uuid`, `source_uuid`, and
`target_uuid` fields of `FixedSizeBinary(16)`, `relationship_type: Utf8`, and
`properties`. The relationship field is nullable
in the Arrow schema for forward-compatible query projection, although committed
edge rows always carry a value. The property struct is the complete
typed row projected by the target for added and modified records and by the
source for removed records. Every stream appends non-null
`changed_properties: List<Utf8>`; it is empty for added and removed records and
contains sorted property names for modified records. Field additions require a
new contract version; consumers must address fields by name and may not infer
record identity from row position.

## Consumer state machine

1. Pin the locally applied generation UUID and manifest SHA-256 as the source,
   and request one exact committed target identity.
2. Accept `ready` only when both returned identities and all six stream metadata
   match the request. Validate every stream before mutating consumer state.
3. Remove rows first, then upsert complete added and modified target rows. Apply
   nodes and edges as one checkpoint; do not expose a partially applied set.
4. Commit the returned target identity and checkpoint binding only after the
   reconstructed canonical graph equals that target. A retry with the same
   endpoints and limits returns identical bytes.
5. On `reload_required`, discard the incremental attempt and full-load the exact
   target. `generation_unavailable` includes retention/compaction;
   `identity_mismatch`, `corrupt_generation`, and `incompatible_graph` are
   integrity or compatibility failures; `resource_limit` means no partial bytes
   were admitted. Cancellation is the typed `GF_CANCELLED` error and likewise
   exposes no partial result.

Version 1 is a single bounded response, not a pagination protocol. Callers set
record and output-byte limits before execution and provide cooperative
cancellation. They may walk a retained generation ladder or request a direct
source-to-target range; both must reconstruct the same exact target. If any
intermediate generation was compacted, the caller full-loads instead of guessing
across the missing range. Polling cadence, backpressure queues, rendering, and
cache eviction remain consumer-owned.

Requests have record and encoded-byte limits plus cooperative cancellation.
Generation loss, corruption, incompatible state, identity mismatch, and
resource exhaustion produce distinct typed reload-required dispositions.
Resolution, hydration, comparison, and encoding complete before a result is
returned, so failures cannot expose a partial stream set. Retrying the same
identities and limits produces identical bytes and the same checkpoint binding.

This API is consumer-neutral. Python, Node, CLI, XYG, and future synchronizers
may project these Rust-owned streams, but polling, subscriptions, caches,
rendering, and applying the changes remain consumer responsibilities.
