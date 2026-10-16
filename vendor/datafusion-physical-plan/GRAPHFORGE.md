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
`src/spill/schema_admission.rs`, `src/spill/resident_metadata.rs`,
`src/spill/spill_pool.rs`, `src/repartition/mod.rs`, `src/aggregates/row_hash.rs`,
`src/sorts/sort.rs`, `src/sorts/multi_level_merge.rs`,
`src/sorts/copy_admission.rs`, `src/sorts/copy_bounds.rs`, the
merge/cursor/builder plumbing, and Cargo features.
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
For matched flat Int64/Utf8 arrays and the actual declared flat writer schema,
the uncompressed, alignment-eight writer instead follows a checked append
sequence. Each payload and padding append accounts old/new Vec capacity overlap,
including its minimum capacity, plus validity and rebased-offset scratch during
actual use. NULL stored spans are included; source full capacities, all existing
headers and schema metadata remain independent. Other codecs, types, declared
schemas and empty schemas keep the original encoding envelope. The Arrow writer
and its growth policy are unchanged.
Dictionary tracker entries retain original buffer authority
through the writer's last use. Finishing destroys the writer, including its
private dictionary tracker and compression context, before freeing its guard.

Before each read, a framing preflight saves and restores the seek position,
admits metadata before constructing its temporary Vec, and checks IPC body and
compressed-buffer length prefixes before Arrow allocates decode buffers.
The shared buffered-stream helper reserves an existing channel slot before
polling its input. Queued and currently produced items together fit the
configured capacity, without an extra pending-send decoded batch. Capacity,
merge degree, batch sizes and pool rules are unchanged. FIFO, error items,
non-multithread pass-through and task-owned cancellation retain their existing
semantics; EOF returns its unused permit.
Reader metadata has a separate persistent reservation. Each reader parses one
heap schema; an identity-checked shared owner carries its schema reservation
through that reader and the final body token. Equal schemas from independent
readers remain distinct. Per-batch array/control metadata and per-decode scratch
stay independently funded; a nonidentical batch schema keeps its original full
schema bound. BufReader and MessageReader metadata high-water authority remain
separate after extracting the shared schema owner. This does not extend the
existing external SchemaRef/bufferless-header guarantee. Full schema parsing and
array construction grants remain live until those phases finish. Qualified
constructor provenance then permits a checked resident grant; unknown container
or target layouts retain their original fees. Schema/datatype heaps, full buffer
backing, owner ledgers and platform-specific control allocations stay separate.

The qualified private Repartition FIFO transfers one independently admitted,
nonspilling writer workspace after encoding and flush, before publication. Later
writes and reads reuse available tickets and admit fresh deficits before
allocation; they never add another donor to the bank. One logical consumer root
spans rotations, and workers retain their own permit until actual exit. Terminal
paths drop real readers before retiring unused credit. This preserves allocation
safety without claiming every simultaneous writer/read workspace fits a pool.

Pre-existing multi-run
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

A separate primitive Field capability qualifies only a nonspilling sponsor,
all admitted IPC inputs, one direct Int64 Column key, and flat Int64/Utf8 output
fields. Its constructor allowance is admitted before prefetch: the incoming
stream Vec's complete capacity coexists with a separately reserved Fuse Vec,
and the actual Field/generic-merge/BatchBuilder types and capacities are funded.
The Field lease retains its actual vector capacity. Column evaluation clones
an ArrayRef, and the primitive cursor clones its ScalarBuffer; ArrayValues
stores only the null threshold and does not allocate key payload or a bitmap.
The generic merge then uses the existing funded builder-growth and interleave
phases, including fresh output leases through escaped aliases. This explicit
capability does not use a zero row-width sentinel. Other Field keys or payload
families retain the original envelope. Dynamic Row and primitive Field modes
keep an explicit numeric floor for CopyAdmission's Arc/control allocation
through the last escaped output; the Row resident metadata transfers that
exact charge to the floor rather than charging it twice. The floor owns no
Resident and cannot form an owner cycle. In particular Utf8View key conversion
can allocate through gc() and is not covered by the primitive proof.

Other all-IPC Field dispatch keeps the complete original copy envelope as a
live production floor through key evaluation and StringView gc. On the same
original reservation, fresh interleave workspace and previous escaped outputs
are additional to that floor. Existing checked interleave bounds apply; an
unproved payload kernel refuses before allocation rather than treating the
old envelope as a universal output bound. Constructor/growth metadata is
preadmitted and kept at its actual capacity. EOF, error and drop destroy inner
cursor/builder/GC/failed-phase data before retiring the production floor;
escaped outputs retain their exact owners, metadata and Copy control afterward.
No new consumer, empty reservation or split is introduced for this handoff.
Real workspace release can still invoke the existing aggregate loan/floor
policy; there is no claim of identical pool callback sequences. The wrapper's
cached SchemaRef clones the same schema already retained by the outer
RecordBatchStreamAdapter, adding neither a heap schema nor a last-use extension;
the existing constant/external schema-header boundary remains.

For uncompressed V5 RecordBatch frames, framing admits one complete body only
for Int64, UInt64, UInt32, UUID16, Timestamp, Utf8 and recursive List/Struct
layouts. An allocation-free, depth-limited walk checks exact DFS node/buffer
counts, root/Struct row lengths, each List child's own length, null counts,
signed descriptor ranges, required lengths, 8-byte offsets, absent dictionary/
variadic/compression messages, and a valid body allocation Layout. Pinned MessageReader uses from_len_zeroed with exact body
capacity and Arrow alignment; these layouts need no alignment copy. The whole
body, including padding, stays charged through the final alias. Compressed,
unknown or unmatched layouts keep the original body/decompression allowance;
schema, metadata, ledger, header, codec and BufReader terms are unchanged.
This proves allocation reachability for the private immutable engine frame,
not general validation of arbitrary IPC values.

Interleave qualifies primitives, Boolean, Null, FixedSizeBinary, ordinary
32/64-bit offset byte arrays, Struct, List and LargeList recursively. A borrowed
selection traversal counts the exact stored offset spans, including cells
masked by NULL list/struct/string parents. Bounds cover output buffers, null
bitmaps, alignment, flattened child-index scratch and preadmitted metadata;
input full backing remains independently charged. Flat Utf8View/BinaryView
payloads additionally bound the complete source handle mapping table (including
unused and empty buffers), raw NULL views, descriptor/null buffers, Buffer Vec
growth and Arc conversion; source payloads keep their existing IPC tickets.
Merges without the controlled Row capability and mixed inputs retain the
original envelope. Within the controlled Row path, unproved payload kernels fail
with a named resource refusal before execution. Offset overflow is reported
before allocation to the existing smaller-prefix retry. Earlier copied output
leases cannot pay for another allocation. Source preparation, complete serial
kernel peak and output reconciliation transition on the same reservation while
protecting sources and earlier outputs. FixedField retains its production floor
and requires the checked interleave bound described above. No alternate engine
or universal kernel estimator is introduced.

While waiting for input, exact resident Rows, converter and vector authority
remains. Reusable slots and current/previous cursors share each Rows lease;
converter metadata stays protected while Rows retain its configuration. A
failed mutation keeps its phase alive until its owning backing drops. Controlled
merges release source batches after their final future-cursor/pending-index use;
actual Row EOF releases cached keys. Aggregate grouping drops its consumed input
handle before state admission or spilling. Genuine accumulator aliases retain
their own buffer tickets.

Interleave additionally admits its temporary local owner ledgers before their
allocation. Source batches remain alive while numeric owner identities are
used. Complete source owner capacities retain their existing IPC tokens;
interleave never reclaims those Bytes. Each distinct fresh output owner gets
its own fixed buffer-lifetime lease, so an escaped copied leaf retains its
whole allocation without pinning copied siblings. Previous output leases
cannot pay for a later copy phase. Empty dangling buffers receive no physical
owner claim. Mixed in-memory/IPC inputs keep the existing fixed copy reservation.
Other all-IPC Field cursors keep their fixed production floor and separately
fund escaped output handoff as described above.

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
  -p graphforge-exec --lib -E 'test(spill_admission_) | test(copy_phase_) | test(spill_fifo_) | test(spill_schema_preparation) | test(spill_array_preparation) | test(spill_aggregate_input_last_use)'
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
