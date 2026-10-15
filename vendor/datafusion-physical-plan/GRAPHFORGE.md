# GraphForge vendoring notice

This directory is a vendored copy of `datafusion-physical-plan` 54.1.0 from
crates.io (excluding Cargo's `.cargo-ok` marker), under Apache License 2.0.
`LICENSE.txt` and `NOTICE.txt` are unchanged upstream files.

The root `[patch.crates-io]` selects this copy. Its explicit workspace `exclude`
means workspace fmt, clippy and tests do not cover the vendored crate directly.
Product regressions therefore live in `graphforge-exec` and `graphforge-api`.

## Spill-pool wakeup patch

Apache DataFusion [PR #24891](https://github.com/apache/datafusion/pull/24891),
commit `09a2aff72d88c16499ae3266a943a4cd9841b618`, is ported to 54.1.0 in
`src/spill/spill_pool.rs`. It fixes the two-open-file repartition deadlock
reported in [#24883](https://github.com/apache/datafusion/issues/24883).
The port adapts only the 54.1 API names: one cloneable `SpillPoolWriter` and
`channel`, rather than the later sink and separate channel APIs. CI exercises
`graphforge-exec::session::spill_pool_regression_tests`.

## Sort spill admission patch

GraphForge #2146 covers `src/spill/admission.rs`, `src/spill/credit.rs`, `src/spill/mod.rs`,
`src/spill/in_progress_spill_file.rs`, `src/spill/spill_manager.rs`,
`src/sorts/sort.rs`, `src/sorts/multi_level_merge.rs`,
`src/sorts/copy_admission.rs`, `src/sorts/copy_bounds.rs`, the
merge/cursor/builder plumbing, and Cargo
features.
It keeps Arrow's existing IPC writer, reader, codecs and record-batch layouts.

The sorter admits each completed sorted batch's actual retained backing before
collecting it, and retains that admission until writing and dropping the batch.
An unavailable source reservation returns a typed error before any older-row
flush or writer allocation. The pre-sort copy estimate is not charged again
for completed sorted output. Checked excess above all live source backing can pay write
workspace; ordinary spill callers acquire that workspace from the pool.
Encoding admits view GC, metadata, body buffers and codec workspace before
construction. Write admission shares the actual view-GC predicate. For Arrow's
single-block GC branch, encoded payload bounds use its new referenced bytes
and view descriptors; original source capacities remain separately admitted,
and GC construction keeps its full allowance. Repeated and NULL stored views
are included. Skipped GC and the multi-block branch keep the original bound.
Dictionary tracker entries retain original buffer authority
through the writer's last use. Finishing destroys the writer, including its
private dictionary tracker and compression context, before freeing its guard.

Before each read, a framing preflight saves and restores the seek position,
admits metadata before constructing its temporary Vec, and checks IPC body and
compressed-buffer length prefixes before Arrow allocates decode buffers.
Reader metadata has a separate persistent reservation. Pre-existing multi-run
admission is divided explicitly: input credit moves to the readers, while
sorted-copy credit remains independent of input credit. Numeric input-credit
tickets prevent prefetch, active workers and escaped outputs from reusing the
same bytes. For known all-IPC merges with a nonspilling sponsor and qualified
physical-owner schemas, a private synchronized bank recycles returned tickets
while readers remain active. All reader roots are constructed and registration
is sealed before prefetch starts. Workers clone the same root permit; tickets
never own a producer permit. EOF/error releases the stream's root even if the
fused stream remains retained. A started blocking worker retains its root
through actual completion. The last root retires the bank and releases unused
credit; later ticket returns shrink its authority. An escaped leaf therefore
cannot pin the retired reservoir. Control/permit storage is separately
preadmitted. Fresh deficits remain separate reservations and never enlarge the
bank. The invariant is authority = available + outstanding; ticket splits are
logical, with no pool callback. Retirement/drop may lock the existing pool but
never allocates, admits, waits for workers, or walks Arrow under the bank lock.
Spillable aggregate sponsors, mixed inputs and unqualified schemas retain their
existing reservation lifecycle: no change to spill loans, floors or policy.

When every merge source is an admitted IPC reader and the merge uses row
encoding, copy admission follows allocation phases. Direct Column keys with
stateless fixed and variable-byte encodings qualify for checked current-operation
bounds. Variable encodings measure actual valid cell lengths and Arrow's pinned
8/32-byte block padding before append, with one LengthTracker Vec shared across
variable columns; nested/dictionary key codecs retain the original envelope. Their
constructor allowance is admitted before reader prefetch starts, covers both
constructors and known future vector storage, and converts to actual resident
leases before unused allowance is released. Row conversion accounts both Vec
reallocations, the old retained Rows, encoded bytes, offsets and encoder metadata. Builder growth admits a complete new Vec while its old capacity
remains resident.

Interleave qualifies primitives, Boolean, Null, FixedSizeBinary, ordinary
32/64-bit offset byte arrays, Struct, List and LargeList recursively. A borrowed
selection traversal counts the exact stored offset spans, including cells
masked by NULL list/struct/string parents. Bounds cover output buffers, null
bitmaps, alignment, flattened child-index scratch and preadmitted metadata;
input full backing remains independently charged. Flat Utf8View/BinaryView
payloads additionally bound the complete source handle mapping table (including
unused and empty buffers), raw NULL views, descriptor/null buffers, Buffer Vec
growth and Arc conversion; source payloads keep their existing IPC tickets.
Nested views retain the original envelope. Earlier copied output
leases cannot pay for another allocation. Other operations restore the
existing full copy envelope; no unsupported-type refusal or alternate engine
is introduced. This finite proof does not replace upstream estimates for
unqualified kernels.

While waiting for input, exact resident Rows, converter and vector authority
remains. Reusable slots and current/previous cursors share each Rows lease;
converter metadata stays protected while Rows retain its configuration. A
failed mutation keeps its phase alive until its owning backing drops.

Interleave additionally admits its temporary local owner ledgers before their
allocation. Source batches remain alive while numeric owner identities are
used. Complete source owner capacities retain their existing IPC tokens;
interleave never reclaims those Bytes. Each distinct fresh output owner gets
its own fixed buffer-lifetime lease, so an escaped copied leaf retains its
whole allocation without pinning copied siblings. Previous output leases
cannot pay for a later copy phase. Empty dangling buffers receive no physical
owner claim. Mixed in-memory/IPC inputs and specialized field cursors keep the
existing fixed copy reservation.

Arrow 58.4's `pool` feature attaches fixed, already-admitted guards to original
shared `Bytes` through `RecordBatch::claim`. This is a lifetime hook, not the
infallible Arrow interface pretending to perform fallible admission. Original
concrete array types, downcasts, schemas, capacities and memory-size reports
remain intact. A temporary batch-local ledger counts each exact buffer owner
base once at its complete capacity (maximum capacity for aliases with the same
base), including null, child, view and dictionary buffers. It does not charge
the same shared IPC body once per slice. The ledger's checked table envelope is
covered before allocating it; decoded bytes never trigger late admission.
Each distinct decoded physical owner receives its own prepaid payload ticket,
sharing only the bounded batch metadata guard. Dropping one compressed sibling
can therefore release its payload while another leaf escapes. Reader-local
ArrayData roots pin prior dictionary VALUES, never keys or whole batches,
through the next decode/reconciliation; their metadata and replacement overlap
are preadmitted. Existing dictionary owner tickets are preserved without a
second payload charge. Tokens/banks never own Arrow roots, avoiding cycles.
Buffer clones and slices keep authority after stream drop. A
started blocking worker owns its admission and file while finishing; aborting
the async handle cannot free that worker's guards. Cancellation is checked at
read/decode boundaries.

The hook covers immutable engine IPC buffers. It does not claim to admit later
mutable ownership conversions: those allocations require their operator's
normal admission. In Arrow 58.4, an external successful `Buffer::into_vec`
conversion forgets `Bytes` and its attached pool token; callers using that API
outside this immutable engine path can retain a charge indefinitely. No engine
spill path uses that conversion. Null-only arrays have no data buffers and no
row-scaled backing allocation. Their constant headers receive construction
admission; this buffer hook does not retain a charge for bufferless headers
through queued output or the final standalone `NullArray` clone. Zero-column
batches likewise own no column backing. This is an explicit constant-header
limit, not a claim of universal Arrow metadata ownership. Generic merge
materialization outside the qualified all-IPC row merge keeps its existing
sorted-copy reservation. The qualified merge's new immutable output buffers use
the same lifetime hook; its constant bufferless/standalone header limitation
remains explicit. No universal Arrow header or mutable-conversion ownership is
claimed.

The direct controls are in
`crates/graphforge-exec/src/session/spill_admission_tests.rs` and
`crates/graphforge-exec/src/session/spill_copy_tests.rs`, with source-handoff
controls in `crates/graphforge-exec/src/session/spill_source_tests.rs`, and
physical-owner/dictionary controls in `crates/graphforge-exec/src/session/spill_credit_tests.rs`; the facade control is `crates/graphforge-api/tests/sort_spill_memory.rs`. Run the direct controls
through the workspace CI runner:

```sh
python3 scripts/test_environment.py -- cargo nextest run --locked --no-fail-fast \
  -p graphforge-exec -E 'test(spill_admission_)'
python3 scripts/test_environment.py -- cargo nextest run --locked --no-fail-fast \
  -p graphforge-api --test sort_spill_memory
```

These commands describe reproduction, not a claim that a particular candidate
has passed. Test results and native workload receipts belong on the issue/PR.

## Updating and removal

Compare each named patch file against registry `datafusion-physical-plan`
54.1.0 when updating this fork; preserve the two independently tested fixes.
Delete the directory, root patch entry and workspace exclude when an adopted
DataFusion release includes both fixes (or equivalent upstream behavior).
Then verify one registry copy with `cargo tree -i datafusion-physical-plan`
and rerun the product regressions. Do not remove the wakeup patch merely
because the admission patch has an upstream replacement, or conversely.

The patch applies to repository builds of the CLI and native bindings.
Published crates.io manifests do not carry the root patch and resolve stock
DataFusion.
