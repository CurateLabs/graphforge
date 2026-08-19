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

Requests have record and encoded-byte limits plus cooperative cancellation.
Generation loss, corruption, incompatible state, identity mismatch, and
resource exhaustion produce distinct typed reload-required dispositions.
Resolution, hydration, comparison, and encoding complete before a result is
returned, so failures cannot expose a partial stream set. Retrying the same
identities and limits produces identical bytes and the same checkpoint binding.

This API is consumer-neutral. Python, Node, CLI, XYG, and future synchronizers
may project these Rust-owned streams, but polling, subscriptions, caches,
rendering, and applying the changes remain consumer responsibilities.
