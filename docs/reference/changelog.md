# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.5.1] - 2026-08-01

- Freeze Cargo, Python, Node/native npm, CLI, and agent-skills surfaces at
  `0.5.1` and cut the dated changelog for the coordinated M1 corrective
  release (#192).
- Replace the Python package README with a concise PyPI landing page: short
  purpose, install path, one minimal first-use example, and canonical
  `docs.graphforge.sh` links instead of a raw CLI command inventory (#304).
- Partition the immutable candidate into independently retained Python, npm,
  crates, evidence, and manifest artifacts; authorize every write from fresh
  registry truth; parallelize only the five native npm packages with verified
  main/CLI/skills fan-in; isolate registry credentials; remove polling sleeps;
  and always reconcile all 24 public nodes across partial job outcomes (#296).
- Add a deterministic pre-write release rehearsal that installs and executes
  the exact candidate through clean Python, Node/native, CLI, and agent-skills
  consumers, validates all crate packages and dependencies, and proves the
  24-node recovery graph sequentially across partial success, cancelled,
  timed-out, skipped, propagation-delayed, conflicting, indeterminate, and
  expired-artifact outcomes (#295).
- Derive publication recovery from the immutable candidate plus fresh PyPI,
  npm, and crates.io truth: normalize absence, accepted propagation, verified
  bytes, conflict, failure, and indeterminate evidence; bound visibility checks
  without sleeps or repeat writes; and schedule only dependency-valid absent
  nodes with available retained artifacts (#294).
- Replace checksum-only release records with a deterministic, partitioned
  candidate manifest that enforces the complete 24-node public package set,
  one root version, exact dependency edges, archive entrypoints/legal files,
  retention, and an explicit registry-independent publication state model
  before any write (#293).
- Adopt ADR 0017's single-version release invariant: all public Rust crates,
  Python and Node/native adapters, CLI, and agent skills must publish one exact
  GraphForge version, and partial-publication recovery may not introduce
  temporary or registry-specific version divergence (#292).
- Split native binaries out of the main `@curatelabs/graphforge` npm tarball,
  and add the explicitly authorized v0.5.0 supplemental artifact/checksum
  record so the previously unpublished main package stays below npm's payload
  limit while reusing the already-published platform packages (#287).
- Verify newly published and resumed npm packages immediately against npm's
  `dist.integrity` metadata, preserving exact-byte checks without failing while
  the registry's public tarball CDN is still replicating (#284).
- Add an explicit, maintainer-reasoned publication recovery dispatch that can
  resume the immutable `v0.5.0` tag and retained candidate after waiving only
  the tagged changelog's stale `[Unreleased]` entries; all artifact, checksum,
  identity, ordering, and fail-closed registry checks remain required (#281).
- Move all public npm packages from the unavailable `@graphforge` scope to the
  Curate Labs-owned `@curatelabs` scope, using `@curatelabs/graphforge` for the
  native binding and `@curatelabs/graphforge-*` for platform, CLI, and agent
  skills packages; update release records, verification, and publication docs
  to reject the retired candidate names (#279).

## [0.5.0] - 2026-07-31

- Preserve one exact-SHA M1 release-candidate bundle for 30 days, record and
  validate every Python/npm/crates.io checksum before publication, publish the
  recorded Python/npm bytes without rebuilding, bind crates.io packaging to the
  same record, and add a non-publishing credential preflight plus ordered
  operator runbook (#192).
- Rename the complete Cargo workspace from `gf-*` / `gf_*` to the public
  `graphforge-*` / `graphforge_*` namespace, publish the 14 Rust libraries plus
  `graphforge-cli` to crates.io in dependency order, and keep the Python/Node
  binding implementation crates private to their language registries (#196, #269).
- Freeze Cargo, Python, Node, CLI, and agent-skills release metadata at
  `0.5.0`; fail closed before registry writes on SHA/version/changelog/npm
  drift; align publication, release-record, certification, and clean-environment
  tooling with the current repository and complete crates.io surface (#192).
- Align the published homepage and root README with the v0.5 development line, remove legacy
  PyPI v0.4 badges/install claims, and route readers to current Python, Node, CLI, and VS Code
  workflows (#255).
- Integrate the GraphForge VS Code extension into the public guide by mechanically importing
  its curated `docs/published/` pages from an immutable, checksummed revision (#252).
- Add a runtime- and provider-agnostic deployment specification for Pulumi
  TypeScript/Python and Terraform, projecting digest-pinned core artifacts,
  service/worker/job/host intent, external source/secret references, and
  distinct infrastructure/runtime states without creating a service or
  depending on #215 (#227).
- Register generation-managed repository snapshots with complete-workspace
  checkpoint diff/revert validation so checkpoints created after repository
  sync can be restored and reopened deterministically (#248).
- Add offline `infra validate --target` with a versioned provider-neutral
  ontology-neutral receipt, explicit deployment ownership and
  validity/plan/connectivity/readiness/capability states, and
  first-party Pulumi TypeScript/Python plus Terraform provider/module plan
  validation over the same secret-free resolved configuration (#231).
- Stabilize Rust CLI machine output with repository-relative JSON receipts,
  bounded structured error details, schema-first checkpoint/revert JSON,
  authoritative `checkpoint show`, and non-mutating revert preview with
  required explicit `--yes` confirmation (#243).
- Package host-discoverable project-local GraphForge skills from one canonical
  source in both the `graphforge` wheel and `@curatelabs/graphforge-cli`, with
  deterministic compatibility metadata and byte-parity checks (#230).
- Preserve LF bytes for canonical and packaged project skills on Windows
  checkouts so release-candidate wheels satisfy their recorded SHA-256 digests
  (#259).
- Make tracked text checkout bytes LF-stable across platforms so Windows
  release gates compare configuration, ontology, manifest, and checksum
  fixtures without newline conversion (#263).
- Close staged lease and manifest file handles before optimistic generation
  promotion so Windows can atomically rename the durable directory (#265).
- Emit v0.5.0 artifact records with current M1, release, and legal tracker
  lineage instead of retired pre-migration issue references (#267).
- Add byte-for-byte equivalent `graphforge` wheel and `@curatelabs/graphforge-cli` npm
  entry points backed by the same Rust CLI parser, execution, structured errors,
  output, and exit codes, with packed clean-install `uvx`/offline `npx`
  acceptance for the complete repository lifecycle. Project `export`/`import`
  remains whole-generation portable interchange; #236 owns Rust runtime-catalog
  inspection, ontology suggestion/validation, and document export, while #237
  owns thin binding parity plus durable adopt/clear behavior (#226).
- Add Rust-owned deterministic runtime-catalog inspection, conservative ontology
  drafts, structured non-mutating validation, and explicit-source atomic
  YAML/JSON ontology export (#236).
- Expose the #236 Rust-owned runtime-catalog inspection, ontology suggestion,
  non-mutating validation, and explicit document export consistently in Python
  and Node, plus durable workspace inspection, adoption, and clear, while
  keeping session loads explicitly non-durable (#237).
- Add deterministic portable export of the current generation or a named
  checkpoint and validate-before-mutation atomic import into new or empty
  projects, excluding live pointers, locks, journals, caches, and trash (#229).
- Add Rust-owned repository lifecycle commands and APIs for safe `.graphforge/`
  initialization, deterministic configuration resolution, declared-input sync,
  Git provenance, and guarded state-only removal (#228), including atomic,
  generation-managed repository snapshots with non-mutating CI drift checks
  (#244).

- Define the `.graphforge/` repository integration and deployment configuration
  boundary, including closed versioned contracts, ignored data surfaces,
  secret-free IaC resolution, and cross-ecosystem ownership (#225).
- Publish the GitHub Pages documentation at `https://docs.graphforge.sh`, with
  root-relative site URLs and current `CurateLabs/graphforge` source links (#223).

- Stop rerunning the full Test Suite after an already-green pull request is
  squash-merged; exact-head pull-request CI remains the required merge gate
  ([#209](https://github.com/CurateLabs/graphforge/issues/209)).

- Minimize Blacksmith CI storage by removing speculative caches from infrequent
  gates, sharing dependency caches by lockfile identity, and deleting the
  one-run M1 certification build disk after certification (#207).
- Remove unconsumed CI artifacts and move required cross-job transfers to
  Blacksmith-backed cache storage, eliminating GitHub artifact-quota failures
  from the required CI Gate (#205).
- Use one-day GitHub artifacts only for cross-OS binding release-candidate JSON
  reports after colocated caches proved unreliable across runner platforms;
  ordinary CI artifacts and publication bytes remain prohibited (#260).
- Restore local multi-surface coverage via `make coverage`: Rust (`cargo llvm-cov` →
  `build/coverage-rust/`), Python wrapper (`pytest-cov` on
  `crates/graphforge-bindings-py/python/graphforge`), and Node (`c8` on `@curatelabs/graphforge`
  `lib/`), each with an enforceable ≥85% line threshold (#2845).
- Keep docs homepage shields.io badges inline (with wrap) under Starlight's
  block-image content styles (#2845).
- Move Datasets out of the published “Use every day” nav into Reference as a
  backlog extension (not shipped in v0.5.0), and clarify overview / landing /
  README copy accordingly (refs #2845).

- Fix getting-started / quickstart / tutorial examples against the local v0.5.0-dev
  RC surface (existing project dirs, UUIDv7 bulk ops, required `find` label, UUID
  identity predicates) and record RC execution evidence for guide + API + concurrency
  + agent-skills paths (#2789).

- Identify **v0.5.0** as the first GraphForge line under Apache-2.0 in licensing
  docs (`docs/legal/licensing.md`) (#218).
- Align architecture/status banners with shipped v0.5.0: ADR 0005 `graphforge-provenance` /
  `graphforge-knowledge` wording, consistent Shipped / Partially built / Designed framing on
  roadmap and architecture pages, and drop stale provenance-stub requirements language
  (#2792).
- Realign root `CONTRIBUTING.md` with the Rust-owned workspace and Apache-2.0
  inbound contribution terms (#218).

First public **Rust-core** GraphForge line: openCypher over DataFusion, Arrow
results, Parquet-backed project generations, thin Python (PyO3) and Node
(napi-rs) bindings, seven analyst verbs, M19 find/index, and the M20/M21
immutable knowledge/epistemic ledger. openCypher TCK posture for this line is
the authoritative runnable corpus enforced by the BDD gate.

### Breaking Changes

- **Pre-v1: no compatibility with prior lines (#2432, #2441).** GraphForge is
  pre-v1.0. v0.5.0 does **not** promise compatibility with pre-v1, prior private
  development lines, released v0.4.0 SQLite projects, or any pre-M20
  Rust/Parquet development layout. The only supported on-disk input is a valid
  v0.5 project/container. Unsupported and malformed inputs fail before mutation
  with stable `GF_UNSUPPORTED_PROJECT_FORMAT` (and related project open errors).
  There is no importer, migration command/API, in-place upgrade, read-only
  compatibility view, generation-zero interpretation, identity conversion, loss
  report, rollback workflow, or historical fixture-retention obligation. The
  abandoned v0.4 SQLite importer proposal (#2438) is closed as not planned.
- **License line changes to Apache-2.0.** GraphForge is relicensed prospectively
  from MIT to the Apache License 2.0 (`Apache-2.0`). Existing v0.4.0 and earlier
  tags/packages remain under their original MIT grants.
- **Product architecture is the Rust core, not the Python 0.4 engine.** The
  public v0.5.0 product is Rust-owned (`graphforge-cypher → graphforge-ir → graphforge-rel → graphforge-exec`)
  with Parquet project storage and Arrow results. Python and Node are thin
  bindings over `graphforge-api`, not fallback engines.
- **Graph-embedded confidence/provenance removed (#2410).** Obsolete
  graph-embedded confidence/provenance columns, storage-owned domain schemas,
  confidence-aware execution options, and relational confidence folding are
  gone. A graph property named `confidence` is ordinary domain data; immutable
  assessments and lineage live in `graphforge-knowledge` / `graphforge-provenance`.
- **Project authority is the immutable generation, not root YAML/JSON (#2476).**
  Canonical ontology, explicit ontology mode, and authoritative project
  configuration are mandatory `workspace@1` participants of every generation.
  Root YAML/JSON is adoption input; session `load_ontology` remains
  non-persistent.
- **Optional `networkx` / `igraph` / combined `analytics` packaging extras
  removed.** External graph libraries remain development-only dependencies and
  are never required for build, packaging, or runtime.

### Deferred

- **Swift and Kotlin UniFFI bindings** remain deferred to **v0.5.1** (M23).
  v0.5.0 ships Rust, Python, and Node only.
- **Peer-extension / dataset baseline packages** remain deferred to **v0.5.1**
  (M24) and are not part of the v0.5.0 core publication surface.
- **Pre-v1 project migration / import** remains out of scope for the entire
  pre-v1 series under the #2432/#2441 policy (see Breaking Changes); #2438 is
  not planned.

### Added

- Add explicit `single_writer`, bounded FIFO `queued_writer`, and
  `optimistic_multi_writer` embedded modes. Optimistic composite transactions
  rebase compatible creates, immutable-ledger rows, and disjoint property
  changes while returning stable conflict and exhausted-retry errors (#213).
- Expose the three embedded write modes through equivalent validated Python and
  Node constructor options, thread explicit selections through agent workflows,
  and verify compatible multi-agent publication, reopen, identity preservation,
  and conflict atomicity across the native concurrency matrix (#214).

- Add transaction-addressed optimistic project staging with OS-backed attempt
  leases, exact-base commit comparison, atomic generation promotion, and
  recovery that preserves live attempts while removing abandoned ones (#212).

- Add `scripts/record_release_artifacts.py` and
  `docs/development/release-artifact-record.md` for RC checksum/SBOM/contents
  inventories usable by GitHub Release and §7 clean-env checks (#2798).

- Add `scripts/publish_dry_run.py` and `make publish-dry-run` targets for local
  npm/docs/cargo-package/python publication dry-runs with JSON evidence (#2800).

- Add `scripts/verify_package_licenses.py` and `make package-license-verify` so
  Cargo/npm/Python publishable packages are checked for shipped `LICENSE` +
  `NOTICE` (Apache-2.0), and expect agent-skills `files[]` to list both (#218).


- Add third-party Rust dependency SPDX allowlisting (`deny.toml` / `cargo deny`),
  generated `legal/THIRD_PARTY_NOTICES.md` for binary redistributions, and
  package `LICENSE`/`NOTICE`/`THIRD_PARTY_NOTICES.md` packaging coverage
  (including `graphforge-knowledge` NOTICE sync) (#2783).

- Document approximate Python/Node install footprints (download + unpacked disk)
  and the separate PyArrow dependency on the installation guide, with darwin-arm64
  measurement caveats and a scale-limits cross-link (#2784).

- Record local host-native release load-matrix evidence (144/144 passed on
  `ec81fd8f…`) in [Release Load Matrix Results](docs/reference/load-matrix-results.md);
  CI `M1-Release-Load-Matrix-<sha>` upload for #2765 remains pending.

- Align release-load probe result fingerprints across bindings: Node canonicalizes
  whole floats as `1.0` (matching Python/Rust JSON), and the Rust probe uses the
  same fixed bulk operation UUIDs as Python/Node so m19 find top-k tie-breaks are
  deterministic (#2778).

- Align the Python release-load probe bulk Arrow schema with Node/Rust probes so
  required columns (including `label`) are non-nullable (#2776).

- Keep the Starlight site focused on public user and contributor documentation;
  keep `engineering/` contributor docs and ADRs under **Engineering** (not
  Understand), alongside Guide / Book / Reference / development (#2772).

- After manylinux maturin on the M1 release load sticky disk, reclaim `target/`
  ownership for the host runner so Cargo can open `.cargo-build-lock` (#2765).

- Place Architecture Decision Records under **Engineering** in the published
  Starlight sidebar (no longer under Understand → Decisions); keep
  `docs/adr/` body paths stable (#2770).

- Bump the Python `ruff` development dependency from 0.15.20 to 0.16.0 and
  exclude Markdown from `ruff format` so the new default Markdown code-block
  formatting does not rewrite docs (#2682).
- Map release load-matrix size/density scenarios to published scale-limits claims
  and add a Reference landing page for accepted same-SHA matrix evidence
  (`docs/reference/load-matrix-results.md`) (#2768).

- Keep the 144-case host-native load matrix on one sticky-disk Cargo `target/`,
  reclaim probe temps between cases, and fail closed on low free disk so dense
  L/XL cases no longer compete with a second root-disk build tree (#2765).

- Document the v0.5.0 / release-prep testing strategy in engineering lifecycle
  docs: Rust ownership, layered PR → Binding RC → publication gates, openCypher
  TCK posture (3897), platform/matrix philosophy, and what is (and is not)
  end-to-end proof (#2763).

- Drop M## milestone staging labels from published docs (book architecture, API reference, roadmap, ADRs, engineering indexes, and light testing/release-process prose), rewriting shipped capability as present-tense **v0.5.0** language; keep literal CI artifact/workflow names, schema inventory filenames, and test binary ids (`m22_m18_public_surface`, etc.) (#2761).

- Reorganize the published Starlight docs site around reader journeys (Get started,
  Use every day, Understand, Reference, Contribute & operate, Engineering,
  Community) instead of Guide/Book-first left nav; keep content paths stable
  and update the homepage / docs map accordingly (#2757).

- Deepen public engineering lifecycle docs from authoritative repository sources,
  covering testing, publishing, and observability with Starlight as the current
  docs site (#2729).

- Fix README and docs homepage badges: drop DecisionNerd / private-repo Actions
  and Codecov images that 404 anonymously, and use public shields.io badges for
  docs, openCypher TCK (3885/3885), and CurateLabs license (#2755).

- Organize Book research and use-case docs for an ex nihilo **v0.5.0** voice:
  present-tense findings, current analyst/`forge.find` APIs, Parquet project paths,
  indexes, and removal of prior-release comparison archaeology (#2742).

- Polish keeper ADR bodies (`docs/adr/0001`–`0005`) into present-tense v0.5.0
  decisions, removing Python 0.4 / `rust-core` branch transition narrative
  (#2725).

- Flesh out Guide basic-usage pages (quickstart, tutorial, Cypher, graph
  construction) for the v0.5.0 Python API: `NodeHandle.uuid`, bulk
  `operation_uuid` receipts, and `forge.execute` — so a new user can succeed
  without the Book (#2740).

- Align Book architecture docs (`docs/book/architecture/`) with shipped v0.5.0: Cypher pipeline, Parquet storage, thin Python/Node bindings, and M20/M21 epistemic status (no Python 0.4.x reference framing) (#2741).

- Sweep remaining Guide / Book / contributing docs to present-tense **v0.5.0**
  (installation, architecture overview, execution model, contributing) and
  quarantine pre-0.5 releases archaeology under `docs/development/archive/` (#2723).

- Fix Starlight in-content links after the MkDocs→Astro migrate: rewrite `.md`
  hrefs to directory-URL site paths during sync, publish linked architecture /
  release-workflow pages, retarget directory indexes and repo artifacts, and
  replace DecisionNerd absolute URLs so `pnpm docs:check-links` is clean on the
  CurateLabs GH Pages base (`/graphforge-legecy`) (#2753).

- Point the Starlight docs site `site`/`base` at CurateLabs GitHub Pages
  (`https://curatelabs.github.io/graphforge-legecy/`) so CSS/JS load on the
  published URL (#2750).

- Polish the Starlight docs site after the Guide/Book IA move: publish linked architecture/release-workflow pages in the sync allowlist and sidebar, slug-safe rename for dotted stems (e.g. `refactor-v0.5`), and document local `pnpm docs:*` / `make docs-*` run instructions (#2739).

- Reorganize documentation into DocSlime lifecycle + Guide (`docs/guide/`) + Book (`docs/book/`) information architecture, with Starlight sidebar and sync allowlist updated to match (#2737).

- Project composite graph + M20/M21 transactions through the native Python
  binding as thin request conversion onto Rust `publish_composite_transaction`,
  returning the canonical Arrow receipt (#2590).

- Replace the MkDocs Material docs site with an Astro Starlight site under
  `docs-site/` (`pnpm docs:dev` / `pnpm docs:build`), syncing allowlisted
  `docs/**` pages into the Starlight content collection and updating the docs CI
  workflow accordingly.

- Project composite graph + knowledge publication through the native Node binding
  as thin request conversion that returns the canonical Arrow IPC receipt and
  preserves Rust validation, staging, idempotency, and error codes (#2591).

- Present README and product-facing docs as an ex nihilo **v0.5.0** story (Rust
  core, Arrow results, Parquet projects, analyst verbs) without prior-release
  product narrative (#2723).

- Add engineering lifecycle documentation under `docs/`, filled from existing
  architecture, testing, and release sources, with ADR bodies still indexed from
  `docs/adr/`.

- Remove past GitHub issue numbers and historical milestone cross-references from documentation, replacing them with durable module, behavior, and process wording.

- Prove deterministic composite transaction kill-reopen recovery and idempotency
  across every accepted publication boundary, with exact reopened ordered state,
  canonical receipt retry/conflict behavior, and zero-mutation invalid requests (#2589).

- Atomically stage and publish composite graph mutations with explicit M20/M21
  participants in one project generation through `publish_composite_transaction`,
  with zero-mutation validation failures and idempotent retry (#2588).

- Add an executable atomic-recovery release workflow proving composite
  graph-plus-M20/M21 publication recovers exactly one complete generation after
  deterministic failpoint interruption, with invalid-input rejection and
  idempotent retry (#2473).

- Re-check cancellation after `list_checkpoints` storage work and align the Node
  concurrency AbortSignal peer test with cooperative race semantics so a fast
  list cannot flake CI while still proving peer isolation (#2473).


- Register the existing finance-fraud release-workflow bundle in
  `registry-v1.json` with workflow-scenario-v1 metadata so registry omission
  checks pass for closed issue #2468 (#2464).

- Add the ontology-emergence → strict-handoff release workflow: ontology-free
  bulk/incremental exploration, session-scoped advisory formalization, and
  curated mapping into a separate strict target with same-SHA Rust/Python/Node
  evidence (#2469).

- Add shared analyst-agent and developer-agent release-candidate scenarios for
  `@curatelabs/graphforge-agent-skills`, with redacted golden CI outputs, executable
  examples, and a native `npm pack` offline-install evidence runner (#2422).

- Stop forwarding neighborhood/traversal `walk_length` into native bfs/path
  invocations from `exploreGraph`; only random-walk catalog algorithms accept
  that option (#2422).

- Skip optional `valid_time` assertion-validity pages in `narrateBeliefRecords`
  when that capability is not enabled, instead of failing closed on
  `GF_CAPABILITY_DISABLED` (#2422).

- Require the short deterministic Rust/Python/Node concurrency matrix on
  applicable pull requests and add a scheduled/manual bounded-resource stress
  lane with cleanup and resource-growth checks (#2417).

- Clarify that ordinary issue close is outcome-based (acceptance criteria,
  merged work, green checks for the changed surface) and that multi-workflow
  release-certification cascades / exact-SHA citation rituals are not close
  gates for child implementation or construction issues (#2703).

- Prove Python and Node native concurrency parity against the Rust contract:
  GIL-released threaded reads, concurrent async/cancellation isolation, ordered
  Arrow/IPC equality, structured `GF_WRITER_BUSY` rejection before staging, and
  pinned-reader reopen visibility (#2416).

- Build M1 release-load fixtures through atomic `publish_bulk_nodes` /
  `publish_bulk_edges` on Rust, Python, and Node so dense L/XL cases stay inside
  the project recovery publication bound instead of exhausting disk (#2431).

- Add the manual, exact-current-main M1 release certification gate that validates
  immutable Rust and cross-platform binding evidence before executing the
  standardized 144-case host load matrix and emitting one fail-closed aggregate
  artifact (#2431).

- Preserve `graphforge-storage`'s unsafe-code prohibition on Windows by delegating the
  project-root named mutex to a safe, cross-process lock guard, including
  abandoned-owner recovery.

- Make Node `GraphForge.close()` synchronously release its native engine
  ownership so Windows project directories can be removed or reacquired
  without waiting for JavaScript garbage collection (#2619).

- Route binding release-candidate macOS and Windows execution through native
  Blacksmith runners, including verified x64 Node execution for the Intel macOS
  addon.

- Explicitly release checkpoint-revert replay locks in reverse acquisition order
  before returning, preventing immediate conflict checks from observing stale
  writer contention (#2650).

- Fix fresh-project initialization on Windows with a canonical-root named mutex
  that serializes compatible same-process initializers while preserving
  fail-closed external contention and owner directory inspection; retain Unix
  file locking semantics (#2619).

### Added

- Freeze the composite transaction Arrow receipt and pre-staging authorization
  contract: singleton schema/metadata, deterministic generation UUID, exact
  participant counts, zero-mutation validation failures, and idempotent
  retry/conflict decisions without publication (#2600).

- Prove same-SHA Rust/Python/Node Arrow bulk-construction conformance with an
  opt-in matrix covering empty/single/multi-row, mixed properties, identity,
  missing endpoints, malformed input, atomic rejection, idempotent
  retry/conflict, receipts, and reopen parity; document the bounded runner and
  activate exploratory bulk load through shipped public surfaces (#2552).
- Project Node Arrow IPC bulk construction through Rust-owned
  `publishBulkNodes` / `publishBulkEdges` with native acceptance coverage (#2551).

- Project Python bulk construction through Rust-owned `publish_bulk_nodes` /
  `publish_bulk_edges`, accepting Table/DataFrame/list containers and convenience
  `add_nodes` / `add_edges` normalization (#2550).

- Complete the Node live M18 descriptor facade: catalog-exhaustive
  `algorithmDescriptorContracts`, prepare surfaces for every analyst verb, and
  knowledge/epistemic isolation proof for registry discovery and descriptor
  preparation (#2602).

- Freeze composite pre-staging validation precedence (request/domain → ontology → identities → graph → participant) with a table-driven zero-mutation ledger (#2660).

- Aggregate composite graph and participant cross-reference validation so
  endpoint/kind checks precede M20/M21 participant-family checks (#2659).

- Aggregate composite M20/M21 participant-reference validation so provenance
  and epistemic families share one pre-staging entry point with provenance-first
  precedence (#2669).

- Add canonical Rust Arrow bulk-edge publication with complete endpoint and
  ontology preflight, one atomic graph/catalog generation, ordered receipts,
  and durable idempotent replay (#2549).

- Add canonical Rust Arrow bulk-node publication with atomic graph/catalog
  generations, deterministic receipts, and durable idempotent replay (#2537).

- Freeze the Rust-owned Arrow bulk node, edge, and receipt contracts, including
  deterministic UUIDv7 generation, structured pre-publication validation, and
  zero-write ontology and identity normalization (#2535).

- Validate composite edge endpoints, mutation targets, and declared graph-object
  reference kinds against existing and same-request identities (#2668).

- Validate composite assertion-status, supersession, hypothesis, membership,
  selection, and validity references against exact identity families (#2672).

- Validate composite provenance, lineage, assertion, confidence, evidence, and
  reasoning references against exact identity families (#2671).

- Validate composite ontology declarations and cross-family identity collisions (#2657).

- Validate the composite request envelope, graph mutation shapes, aggregate
  entry cap, and existing M20/M21 domain contracts before identity lookup (#2658).

- Add a thin agent workflow that resolves one explicit assertion or hypothesis
  question through Rust-owned pinned belief-subject evidence while preserving
  ambiguity, alternatives, source UUIDs, and native fingerprints (#2605).

- Add a thin agent workflow that narrates every scoped M20/M21 record for a
  resolved belief subject through public Node list surfaces, fails closed on
  record-budget exhaustion, and returns UUID-addressed project-level pagination
  descriptors (#2604).

- Add a thin agent workflow that dispatches one caller-prepared neutral
  recorded analysis against a resolved belief projection while preserving
  completed M20 runs when M21 attachment fails (#2606).

- Add a thin agent workflow that explores graphs through bounded public paths
  modes (neighborhood, traversal, path, reachability) with fail-closed bounds
  and graph-only capability opens (#2608).

- Add a thin agent workflow that retrieves through public M19 find modes and
  every live M18 family via descriptor/direct facades with fail-closed bounds
  and graph-only capability opens (#2603).


- Canonicalize explicitly supplied graph-reference, confidence-input, and
  hypothesis-event participants that reference existing owners without requiring
  immutable owner rows to be repeated in the same composite request (#2647).

- Expose pinned belief-subject evidence and its opaque graph projection through
  the Node binding as canonical Arrow IPC (#2634).

- Freeze Rust-owned composite request identity and pre-staging retry/conflict
  decisions over canonical graph and explicit M20/M21 participant content (#2607).

- Add a Rust-owned canonical composite graph-mutation content fingerprint with
  ordered mutations, unordered property maps, and typed property values (#2636).

- Resolve explicit belief-subject evidence and its graph projection under one
  pinned Rust generation guard, preserving alternatives and source identity
  without confidence inference (#2633).

- Add Rust-owned canonical fingerprints for hypothesis group, membership, and
  selection participants used by composite request identity (#2623).

- Freeze the bounded Rust composite transaction request vocabulary, explicit
  participant inventory, UUIDv7 identity shape, and 100,000-entry cap (#2601).

- Add Rust-owned typed UUID query parameters with native Python `uuid.UUID` and
  exact tagged Node `{ "$uuid": "..." }` projections for graph identity predicates
  (#2580).

- Add the versioned M1 release-workflow registry, static validator, deterministic
  selected/all orchestrator, ontology taxonomy, and SHA-bound evidence envelope
  scaffolding (#2582).

- Add preservation-first bootstrap and build-knowledge agent workflows over the
  shipped Node/Arrow graph, M20, and optional explicit M21 contracts (#2419).

- Harden the agent-skills shared contract against traversal, symlinks,
  subprocess requests, secret-bearing native errors, cycles, and recursively
  unbounded payloads, with offline packed-consumer security proof (#2540).

- Add versioned agent-skill manifest and input/output schemas with
  deterministic offline validation (#2539).

- Add the versioned semantics-free agent-skills adapter for safe project
  opening, capability checks, structured errors, and deterministic
  UUID/Arrow/JSON serialization (#2538).

- Preserve structured GraphForge error codes across every Node `AsyncTask`,
  including deferred plan collection/sinks and cooperative cancellation, so
  Promise rejections never degrade to napi `AbortError` (#2499).

### Changed

- Keep maturin wheel output read-only while Python release-candidate
  classification and target evidence use probed writable runner storage (#2618).

- Clear unavailable Rust compiler wrappers left by native wheel builds before
  Python release-candidate contracts launch Cargo on Linux, macOS, or Windows
  (#2596).

- Expose Rust-owned adjacency freshness inspection and rollback-safe rebuild
  receipts with deterministic source/artifact identity and thin Python/Node
  projections (#2548).
- Add a deterministic shared-directory matrix for pinned readers, complete
  generation visibility, reopen behavior, and writer contention (#2543).
- Expose Rust-owned text-index freshness inspection and atomic build receipts,
  with exact source/project identity and thin Python/Node projections (#2547).

- Harden the agent-skills package compatibility, help, nested-path CI, and
  packaged NOTICE contracts with explicit offline regression coverage (#2536).
- Add a deterministic same-process concurrency matrix for independent,
  shared-instance, cross-session, and ambient-Tokio reads (#2541).

- Synchronize pinned-checkpoint deletion with the completed recovery lock
  handoff so cleanup tests cannot race `checkpoints.lock` release (#2560).
- Add the distributable `@curatelabs/graphforge-agent-skills` NPX package shell with
  machine-readable GraphForge v0.5.0 compatibility and deterministic offline
  pack, install, and invocation validation (#2536).

- Synchronize checkpoint failpoint tests with bounded, phase-diagnostic lock
  handoffs so parent reads and replay mutations cannot race lock cleanup (#2553).
- Synchronize checkpoint replay verification with the same bounded lock handoff
  so immediate reads cannot race completed replay cleanup (#2553).
- Recover durable checkpoint intents inside the bounded writer-then-checkpoint
  lock handoff so failpoint verification cannot race production reacquisition (#2553).
- Add a deterministic knowledge-evolution release workflow proving that
  append-only evidence, confidence, reasoning, status, validity, and explicit
  hypothesis selection leave graph, ontology, search, rank, and path results
  unchanged across temporal cutoffs and reopen (#2472).

- Add an executable correction-churn release workflow proving public graph,
  ontology-validation, M20, and M21 compensations preserve append-only history
  (#2471).
- Add repository-owned Rust, Python, and Node executors for the versioned XS-XL
  release-load matrix, with same-SHA native artifact and exhaustive public-surface
  provenance bound into deterministic evidence (#2463).
- Synchronize project-recovery commit-boundary tests with writer-lock release
  so immediate verification does not race completed recovery cleanup (#2527).
- Synchronize checkpoint failpoint recovery tests with lock release so
  nonblocking reads do not race completed subprocess cleanup (#2523).
- Reject undeclared `add_node` properties before publication in strict
  ontology mode while accepting inherited declarations and preserving
  exploratory/advisory construction semantics (#2517).
- Preserve canonical edge UUIDs and deterministic adjacency results when a
  persisted exploratory graph adopts an advisory ontology and is reopened
  (#2515).

- Align the shipped Python stub with the native M20/M21 surface and preserve
  stable `GfError` codes, classes, spans, and provider metadata across every
  mapped public exception (#2500).
- Release the Python GIL around synchronous Rust-owned query, construction,
  checkpoint/history, M18/M19, and M20/M21 operations while preserving Python
  conversion and structured errors at the binding boundary (#2498).

- Harden checkpoint recovery with restart-safe pinning, transient-resource
  cleanup, populated-shape restoration, corruption fail-closed coverage, and a
  same-SHA release evidence ledger across Rust, Python, Node, and CLI (#2481).
- Treat durable graph identity fields as structural in strict ontology mode and
  correctly encode empty entity property maps used by checkpoint validation
  (#2481).

- Preserve ownership of temporary in-memory projects across checkpoint revert,
  and reject staged restoration sets with duplicate participants (#2493).

- Make canonical ontology, explicit ontology mode, and authoritative project
  configuration mandatory `workspace@1` participants of every immutable
  generation. Root YAML/JSON is now adoption input rather than project
  authority, while session `load_ontology` remains non-persistent (#2476).

- Reconcile the M21 epistemic ADR, architecture, API reference, and roadmap
  with the shipped append-only schemas, deterministic belief-resolution order,
  neutral M18 dispatch, and retryable post-run attachment contract (#2413).

- Remove obsolete graph-embedded confidence/provenance columns, storage-owned
  domain schemas, confidence-aware execution options, and relational confidence
  folding. A graph property named `confidence` is now always ordinary domain
  data; immutable assessments and lineage remain owned by `graphforge-knowledge` and
  `graphforge-provenance` (#2410).

### Fixed

- Close the public strict-property runtime-ID contract with persistent
  create/query/reopen Arrow evidence and fail-closed atomic binding (#2594).
- Retain checkpoint lock guards through non-replay revert validation and
  publication so every exit explicitly releases lock ownership (#2677).

- Explicitly release a generation read lease when its final logical reader is
  dropped so fork- or dup-inherited descriptors cannot prolong it (#2670).
- Preserve externally owned lineage provenance and prior-reasoning references
  through composite fingerprint validation for later snapshot resolution (#2674).

- Preserve exact map-literal key spans through parsing, serialization, cloning,
  and AST rewrites so downstream diagnostics can identify the key token (#2666).
- Bind strict entity and relation properties by owner kind while emitting
  runtime-catalog property IDs for reads, inline filters, SET, and REMOVE (#2662).
- Stage runtime-catalog observations per bind and publish them only on success,
  preserving deterministic label, relation, and property IDs after errors
  (#2661).
- Preserve entity/relation property owner namespaces in compiled and reloaded
  ontologies, with deterministic declaration lookup and fail-closed ambiguous
  kind-free resolution (#2654).
- Preserve checkpoint-registry corruption errors without unnecessary writer-lock escalation (#2579).
- Preserve checkpoint restoration metadata when adopting or promoting a
  workspace ontology after a revert (#2561).
- Keep runtime relation IDs disjoint from ontology relation IDs so the first
  advisory typed match cannot read a colliding known relation (#2554).

### Added

- Add an opt-in, deterministic financial-network release workflow covering
  advisory drift, strict validation, graph analysis, preserved corrections,
  non-adjudicative hypotheses, and same-SHA Python replay (#2468).

- Add a deterministic XS-XL sparse/dense dataset taxonomy, exhaustive
  Rust/Python/Node public-surface workload ledger, reproducible fixture
  generator, and fail-closed SHA-bound release-load evidence aggregator (#2463).

- Add omission-checked Python and Node projections of the Rust non-Cypher release surface,
  freshly built native parity tests, and SHA-bound wheel/addon evidence artifacts (#2501).

- Add an omission-checked Rust non-Cypher release inventory covering every
  public receiver, all 94 M18 algorithms, explicit M19 modes and policies, and
  persisted lifecycle/checkpoint conformance with SHA-bound CI evidence
  (#2429).

- Expose Rust-owned named checkpoint create/list/open/diff/revert/delete through
  thin Python, Node, and Arrow-IPC CLI surfaces with read-only pinned views,
  bounded pagination, cancellation, and stable errors (#2480).

- Add complete-workspace checkpoint revert as an atomically published child
  generation with canonical restoration provenance, deterministic idempotent
  replay, full participant validation, and immediate same-instance visibility
  (#2479).

- Add lease-pinned read-only checkpoint views, stable checkpoint listing, and
  bounded deterministic summary/logical-record diffs across graph, workspace,
  provenance, M20, and M21 participants (#2478).

- Add canonical named checkpoint registry persistence with deterministic
  UUIDv8 identity, bounded idempotent create/delete, crash-safe checksummed
  pair recovery, generation leases, and checkpoint-aware retention (#2477).

- Add the finite, SHA-bound M21 contract matrix, omission validator, manual
  Rust/Python/Node closure workflow, frozen M20 baseline comparison, and
  downloadable schema/result evidence report (#782).

- Add explicit versioned M21 belief resolution into deterministic graph-only
  projections before unchanged neutral M18 dispatch, including logical-content
  fingerprints, durable M20 run separation, and idempotent M21 interpretation
  attachments (#2004).

- Add the opt-in `valid_time@1` capability with append-only assertion validity
  events, deterministic transaction-cutoff interpretation, half-open interval
  semantics, and Arrow-equivalent Rust, Python, and Node APIs (#781).

- Add deterministic transaction-time epistemic snapshots that preserve
  statusless assertions, reasoning branches, supersession ambiguity, empty or
  unselected hypothesis groups, exact source UUIDs, canonical fingerprints,
  reopen stability, and matching Rust, Python, and Node Arrow APIs (#2423).

- Add immutable hypothesis groups, append-only membership and explicit
  selection/clear events, atomic selected-member removal, event-UUID
  transaction tie-breaking, deterministic reopen projections, and matching
  Rust, Python, and Node Arrow APIs (#779).

- Add immutable acyclic assertion-supersession relations, atomic paired
  terminal-status publication, explicit branch preservation, deterministic
  history, and matching Rust, Python, and Node Arrow APIs (#778).

- Add immutable explicit assertion-status events, deterministic current-status
  resolution, terminal supersession enforcement, atomic assertion-plus-first-
  status publication, and matching Rust, Python, and Node Arrow APIs (#777).

- Add the optional `epistemic@1` capability and immutable reasoning/amendment
  records with exact bounded content, explicit acyclic predecessor links,
  preserved branch history with deterministic current-leaf resolution, stable
  fingerprints, a generated M21 schema inventory, and matching Rust, Python,
  and Node Arrow APIs (#780).

- Add the finite M20 contract-gate ledger, omission validator, real
  absent/populated/corrupt/future knowledge-isolation acceptance for `find`,
  and a manually dispatched same-SHA Rust/Python/Node evidence workflow
  producing a checksummed closure report (#2058).

- Add the generated, SHA-256-bound M20 provenance/knowledge schema inventory
  and immutable-ledger operator documentation, with byte-for-byte registry
  drift enforcement in `graphforge-api` (#776).

- Add deterministic recorded M18 algorithm-run lifecycles with immutable
  run/event ledgers, pre-dispatch start publication, terminal result
  fingerprints, interruption reconciliation, and matching Rust, Python, and
  Node APIs (#2003).

- Add immutable evidence links with closed source/role contracts, optional
  metadata weights, canonical fingerprints, atomic assertion bundles,
  provenance publication, persistence, and matching Rust/Python/Node Arrow
  APIs (#775).

- Add immutable versioned confidence assessments with explicit and
  `conservative_min@1` policies, normalized input snapshots, canonical
  fingerprints, atomic provenance publication, persistence, and matching
  Rust/Python/Node Arrow APIs (#774).

- Add immutable, exact-text assertions with ordered UUID graph references,
  canonical fingerprints, atomic provenance publication, deterministic replay,
  persistence, and matching Rust/Python/Node Arrow APIs (#2411).

- Add immutable UUID-referenced provenance events and ordered lineage for the
  complete graph-mutation matrix, atomically published with graph generations,
  with deterministic replay/conflict handling and thin Rust/Python/Node reads
  (#773).

- Add canonical knowledge-neutral invocation descriptors for every M18
  `rank`, `cluster`, `paths`, `analyze`, and `similar` catalog entry, including
  embedding algorithms, projection-change detection, result-schema validation,
  and byte-identical Rust/Python/Node replay (#2412).

- Add the manifest-only `project_capabilities` and atomic
  `enable_capability` M20 APIs across Rust, Python, and Node, with mandatory
  `graph@1`, deterministic generation identity, future-version reporting, and
  no pre-v1 migration path (#2408).

- **Deterministic project recovery (#2414)** — interrupted generation writers
  are now classified under the kernel writer lock without PID, timestamp, or
  heartbeat lock stealing; committed journals are repaired, abandoned private
  generations are conservatively cleaned through a durable trash boundary,
  and a cross-platform subprocess failpoint matrix proves exact pre/post-commit
  visibility, resumable current-format initialization, and graph/knowledge
  isolation with no ignored recovery tests.

- **Atomic project-generation publication (#2409)** — `graphforge-storage` now stages,
  validates, flushes, and publishes complete graph/provenance/knowledge
  participant sets through one `CURRENT` replacement, with a non-blocking OS
  writer lock, canonical durable journals, exact participant checksums,
  idempotent transaction replay, and structured conflict/busy/publication
  errors.

- **Committed project-generation resolution (#2424)** — all `GraphForge` opens
  now pin the one immutable generation named by canonical `CURRENT`, validate
  its exact manifest digest and contained machine paths without enumerating
  generations or opening unrequested participant tables, and reject every
  pre-v1 root before mutation with stable binding-visible project error codes.

- **M20 immutable-ledger public API v1 (#2428)** — froze Rust method/request
  shapes, Arrow schemas and ordering, required idempotency keys, pagination and
  cancellation, composite atomicity, lossless Python/Node/CLI errors, removal
  of graph-embedded confidence options, and the additive M21 extension seam.

- **Canonical bytes and fingerprints v1 (#2427)** — froze UUID normalization,
  typed Arrow/schema/table bytes, floating and temporal normalization,
  dictionary/chunk/map/order behavior, domain-separated SHA-256 and UUIDv8
  projection, bounded failures, and shared language-neutral golden vectors.

- **Durable v0.5 project-generation protocol (#2425)** — froze the immutable
  generation layout, sole `CURRENT` linearization point, exact durability
  order, OS-backed writer/reader locks, deterministic recovery and retention,
  structured filesystem failures, and named crash-test edges without any
  pre-v1 migration or generation-zero behavior.

- **M20/M21 domain ownership contract (#2426)** — fixed provenance and knowledge
  crate/table ownership, schema registries and evolution rules, two-phase
  cross-reference validation, M21's additive extension seam, and a CI-enforced
  forbidden-dependency DAG.

- **Pre-v1 project compatibility boundary (#2432, #2441)** — v0.5 accepts only
  its own project/container contract. v0.4 SQLite and earlier Rust/Parquet
  development layouts are unsupported and fail before mutation; no importer,
  migration API, generation-zero view, or compatibility fixture obligation is
  shipped.

- Replace the universal pull-request test matrix with one change-aware `CI Gate`:
  metadata-only changes stay fast, Rust quality/tests run on isolated runners,
  bindings use same-SHA Linux artifacts, and full cross-platform certification
  remains in the release workflow.
- Relicense GraphForge prospectively from MIT to the Apache License 2.0
  (`Apache-2.0`). Existing v0.4.0 and earlier tags and artifacts remain MIT.
- Execute the agent-grounding notebook twice against each PR's freshly built
  native wheel, proving deterministic offline M18 ranking, M19
  text/vector/hybrid retrieval, atomic embedding spaces, tokenizer inspection,
  semantic queries, reranking advisories, and explicit reranking.
- Replace retired shim-era benchmarks, scripts, and examples with an audited
  installed-wheel consumer gate covering every native M18 analyst verb, M19
  text/vector/hybrid search, explicit and lazy indexing, atomic embedding
  spaces, freshness inspection, tokenizer planning, and explicit reranking.
- Reconcile the public algorithm catalog and API reference with the shipped
  Rust-only M18 registry, canonical UUID/Arrow schemas, exact verb parameters,
  read-only embedding behavior, and thin Python/Node bindings.
- `analyze(by="hashgnn")` now executes deterministic, training-free
  `hashgnn-v1` binary minhash propagation entirely in Rust, including canonical
  UUID ordering, explicit heterogeneous string/integer type tokens, atomic
  resource controls, and native Python/Node acceptance.
- `analyze(by="graphsage", directed=False)` now executes deterministic
  unsupervised `graphsage-unsupervised-v1` training entirely in Rust, with
  explicit ordered scalar-or-list node features, canonical neighborhood
  sampling, invocation-local Adam weights, UUID/FixedSizeList Arrow output,
  structured resource controls, thin native Python and Node acceptance, and
  knowledge-layer independence.
- `analyze(by="fast_random_projection")` now executes the deterministic
  `fastrp-v1` sparse-projection and weighted-adjacency propagation contract
  entirely in Rust, with ordered graph-native scalar features, canonical
  UUID/FixedSizeList Arrow output, structured resource controls, thin native
  Python and Node acceptance, and knowledge-layer independence.
- Document the whole-system positioning of GraphForge v0.5 relative to Neo4j
  with Graph Data Science, including current strengths and limitations,
  deployment and projection tradeoffs, and a detailed comparison between
  canonical Arrow results and GDS `stream`, `stats`, `mutate`, and `write`
  execution modes.
- `paths(by="gomory_hu_tree", directed=False)` now exposes the pinned classic
  parent-update Gomory-Hu forest entirely in Rust. Source-free graph-wide
  invocation, strict undirected capacity-multigraph semantics, deterministic
  UUID-only CUT rows, aggregate resource controls, structured atomic errors,
  fresh-native Python/Node acceptance, and knowledge-layer independence
  complete the capability without an external runtime backend or fallback.
- `analyze(by="count_automorphisms")` now returns the exact directed or
  undirected multigraph automorphism count entirely in Rust. Canonical
  individualization/refinement/backtracking preserves loops, parallel and
  reciprocal multiplicity while treating UUIDs as ordering rather than graph
  colors. Exact non-null `UInt64` Arrow output, structured overflow and
  resource controls, thin fresh-native Python/Node acceptance, and
  knowledge-layer independence complete the capability without an external
  runtime backend or fallback.
- `paths(by="min_steiner_tree")` and `paths(by="prize_collecting_steiner_tree")` now expose
  exact deterministic undirected Rust solvers with normalized mandatory terminals, strict
  graph-native costs and prizes, canonical UUID-only four-column Arrow results, structured
  controls and errors, thin freshly built Python/Node acceptance, and knowledge-layer
  independence.
- `paths(by="min_cost_max_flow")` and `paths(by="min_cost_max_flow_edges")` now expose one
  deterministic Rust-owned maximum-flow-then-minimum-cost solution as stable scalar and per-edge
  Arrow views through Rust, Python, and Node. Explicit graph-native capacity/cost properties,
  canonical signed edge flows, structured failures, shared controls, and knowledge-layer
  independence complete the 1213 capability without an external runtime backend or fallback.
- Dispatch explicit graph-native partitions and selected weighted topology through the Rust
  modularity kernel with a stable non-null scalar Arrow result.
- Dispatch the shared deterministic Rust minimum-cut solution into stable scalar and canonical
  per-edge Arrow views.
- Dispatch minimum-k spanning-tree analysis through the exact deterministic Rust kernel with
  canonical ranked, non-null per-edge Arrow rows.
- Dispatch graph-native partition properties and selected weighted topology through the Rust
  conductance kernel with stable per-partition Arrow rows.
- Dispatch seeded random walks through the pinned Rust kernel and stable UUID-list Arrow schema.
- Dispatch the shared Rust maximum-flow solution into stable scalar and per-edge Arrow views.

### Rust core development notes

- Native Python and Node `paths` bindings now pass explicit random-walk length
  and seed options directly to the shared Rust handler, with focused
  deterministic acceptance coverage over freshly built modules.
- `analyze(by="max_bipartite_matching")` now dispatches its selected topology
  and optional resolved partition mapping through the Rust matching kernel.
- Partition-aware kernels can now resolve strict string or integer node
  properties into the shared selected-UUID partition mapping boundary.
- Bipartite matching now has a deterministic Hopcroft-Karp kernel over validated
  UUID partition projections with canonical parallel-edge selection.
- Bipartite matching now validates exact UUID partition mappings or infers
  deterministic per-component two-colorings without knowledge-layer input.
- Partition-aware M18 kernels now share an immutable graph-layer UUID mapping
  that normalizes string and integer partition values without knowledge storage.
- `AnalyzeOptions` now carries an optional partition-property selector with
  algorithm-specific ownership and required-input validation.
- Zero-dual blossom expansion now atomically reconstructs alternating-tree
  state and validates active-blossom optimality certificates.
- Nested blossom contraction now preserves the existing contracted
  representative as the new cycle base while retaining path-edge alignment.
- Normalized weighted graphs now expose their exact matching solver through a
  crate-private boundary proven against an independent exhaustive oracle.
- `analyze(by="max_weight_matching", directed=False)` now dispatches selected
  weighted topology through the exact deterministic Rust matching kernel.
- `AnalyzeOptions` now carries a typed optional `k`; minimum-k spanning-tree
  analysis defaults it to one and rejects zero before dispatch.
- Exact matching now keeps blossom crossing paths representative-safe while
  retaining canonical leaf endpoints and checkpointing full tight-edge scans.
- The paths catalog now separates `max_flow` scalar output from the
  `max_flow_edges` per-edge assignment, with stable non-null Arrow schemas for
  both views ahead of the shared maximum-flow kernel.
- Maximum-flow execution now has one deterministic Rust solution kernel shared
  by the scalar and canonical per-edge views, including directed and
  two-direction undirected capacities, parallel edges, and self-loop handling.
- `PathsOptions` now carries optional random-walk length and seed controls;
  Rust validation normalizes their fixed defaults and rejects zero walk counts,
  target selectors, and use by other path catalog values.
- `analyze(by="is_planar", directed=False)` now dispatches to an exact,
  dependency-free Rust left-right planarity kernel with deterministic
  simple-graph normalization, canonical Boolean output, shared controls, and
  knowledge-layer independence.
- The canonical `max_cardinality_matching` result schema now contains exactly
  the non-null `edge_uuid`, `source_uuid`, and `target_uuid` columns required
  by its unweighted public contract.
- The canonical `max_bipartite_matching` result schema now contains exactly
  the non-null `edge_uuid`, `source_uuid`, and `target_uuid` columns required
  by its unweighted public contract; partition-input semantics remain deferred.
- The public contract for `analyze(by="is_planar", directed=False)` now fixes
  undirected simple-graph projection, exact one-row Boolean Arrow output,
  selection and validation behavior, multigraph normalization, deterministic
  certificate-free execution, shared controls, and Rust-only ownership ahead
  of the 1229 implementation.
- `paths(by="dfs")` now performs deterministic source-component depth-first
  preorder entirely in Rust and returns the same non-null UUID/depth/order
  Arrow result through Rust, Python, and Node. Stable neighbor ordering,
  direction and relationship selection, multigraph and singleton behavior,
  structured validation, shared limits/cancellation, native acceptance, and
  knowledge-layer independence complete the 1220 capability without an
  external runtime backend or fallback.
- `analyze(by="triad_census", directed=True)` now returns the exact global
  sixteen-class MAN census entirely in Rust. Sparse deterministic counting,
  ordered-pair presence normalization, loop exclusion, fixed zero-inclusive
  rows, checked `V choose 3` invariants, and shared limits and cancellation
  complete the 1885 kernel without an external runtime dependency or fallback.
- The Rust algorithm dispatcher now owns `analyze(by="dyad_census",
directed=True)`, returning fixed-order mutual, asymmetric, and null counts
  with checked pair arithmetic, directed endpoint-presence normalization,
  self-loop exclusion, shared controls, canonical Arrow shaping, and
  knowledge-layer independence without an external runtime or fallback.
- `analyze(by="chromatic_number", directed=False)` now returns the exact
  minimum proper-color count entirely in Rust. Deterministic DSATUR
  branch-and-bound, simple-graph UUID normalization, empty, edgeless, isolate,
  and disconnected-component semantics, self-loop rejection, shared limits
  and cancellation without approximate fallback, non-null UInt64 Arrow through
  thin Python/Node adapters, structured validation, and knowledge-layer
  independence complete the 1214 capability without an external runtime
  backend or fallback.
- M18's five Rust-owned analyst verbs now have deterministic development-only
  NetworkX/igraph parity coverage plus an explicit oracle-free native CI lane.
  The legacy `networkx`, `igraph`, and combined `analytics` packaging extras
  have been removed; external graph libraries remain development dependencies
  only and are never required for build, packaging, or runtime.
- `analyze(by="edge_coloring", directed=False)` now returns a deterministic
  greedy coloring of every selected stored edge entirely in Rust. Ascending
  edge-UUID processing with smallest-available colors, label and relationship
  filtering, parallel and reciprocal edge identity, mirrored-adjacency
  normalization, typed-empty and disconnected behavior, self-loop rejection,
  shared limits and cancellation without partial output, native Arrow through
  thin Python/Node adapters, structured validation, and knowledge-layer
  independence complete the 1212 capability without an external runtime
  backend or fallback.
- `analyze(by="euler_circuit")` and `analyze(by="euler_path")` now construct
  deterministic directed or undirected Euler trails entirely in Rust. The
  aligned non-null node/edge UUID-list schema, exact edge preservation,
  circuit and open-path start rules, canonical edge ties, parallel edges,
  loops, empty and edgeless selections, structured undefined results, shared
  limits and atomic cancellation, thin fresh-native Python/Node acceptance,
  and knowledge-layer independence complete the 1225 and 1226 capabilities
  without an external runtime backend or fallback.
- `analyze(by="node_coloring", directed=False)` now returns a deterministic
  greedy coloring of the selected undirected simple graph entirely in Rust.
  UUID-ordered smallest-available colors, label and relationship filtering,
  mirrored/parallel/reciprocal-edge normalization, isolate and typed-empty
  behavior, self-loop rejection, shared limits and cancellation without
  partial output, native Arrow through thin Python/Node adapters, structured
  validation, and knowledge-layer independence complete the 1210 capability
  without an external runtime backend or fallback.
- `analyze(by="dag_longest_path_weighted", directed=True, weight=...)` now
  returns the exact global maximum-weight path across a selected directed
  acyclic graph entirely in Rust. Strict named finite signed weights,
  deterministic complete-UUID path ties, disconnected/empty/all-negative
  behavior, maximum-weight parallel-edge normalization, cycle rejection,
  finite accumulated costs, shared limits and cancellation without partial
  output, native Arrow through thin Python/Node adapters, structured errors,
  and knowledge-layer independence complete the 1208 capability without an
  external runtime backend or fallback.
- `analyze(by="has_euler_circuit")` now returns the exact directed or
  undirected Euler-circuit predicate entirely in Rust. Active-subgraph
  connectivity, balanced directed degrees or even undirected degrees,
  empty/edgeless/isolate behavior, self-loop and stored-edge UUID multigraph
  semantics, checked arithmetic, shared limits and cancellation without
  partial output, non-null Boolean Arrow through thin Python/Node adapters,
  structured validation, and knowledge-layer independence complete the 1227
  capability without an external runtime backend or fallback.
- `analyze(by="has_euler_path")` now returns the exact directed or undirected
  Euler-path predicate entirely in Rust. Active-subgraph connectivity,
  balanced or single-start/single-end directed degrees, zero-or-two odd
  undirected degrees, empty/edgeless/isolate behavior, self-loop and
  stored-edge UUID multigraph semantics, checked arithmetic, shared limits and
  cancellation without partial output, non-null Boolean Arrow through thin
  Python/Node adapters, structured validation, and knowledge-layer
  independence complete the 1228 capability without an external runtime
  backend or fallback.
- `analyze(by="dag_longest_path", directed=True)` now returns the exact global
  longest unweighted path across a selected directed acyclic graph entirely in
  Rust. Label and relationship filtering, deterministic complete-UUID path
  ties, disconnected/empty/isolate behavior, parallel-edge normalization,
  cycle rejection, checked hop counts, shared limits and cancellation without
  partial output, native Arrow through thin Python/Node adapters, structured
  errors, and knowledge-layer independence complete the 1206 capability
  without an external runtime backend or fallback.
- `analyze(by="transitivity", directed=False)` now returns exact global
  transitivity for an undirected simple projection entirely in Rust. Label and
  relationship filtering, mirrored/parallel/reciprocal-edge normalization,
  self-loop exclusion, disconnected/empty/isolate typed-zero behavior, checked
  counting, shared limits and cancellation without partial output, native
  Arrow through thin Python/Node adapters, structured validation, and
  knowledge-layer independence complete the 1235 capability without an
  external runtime backend or fallback.
- `analyze(by="find_cycles")` now returns every deterministic canonical simple
  node cycle entirely in Rust. Directed and undirected projections, stable UUID
  rotation/orientation and row ordering, self-loop and reciprocal-cycle
  semantics, mirrored/parallel-edge normalization, typed empty results, shared
  limits and cancellation without partial output, native Arrow through thin
  Python/Node adapters, structured validation, and knowledge-layer independence
  complete the 1204 capability without an external runtime backend or fallback.
- `analyze(by="triangle_count", directed=False)` now returns the exact global
  triangle count of an undirected simple projection entirely in Rust. Label and
  relationship filtering, mirrored/parallel/reciprocal-edge normalization,
  self-loop exclusion, disconnected/empty/isolate typed-zero behavior, checked
  `UInt64` arithmetic, shared limits and cancellation without partial output,
  native Arrow through thin Python/Node adapters, structured validation, and
  knowledge-layer independence complete the 1232 capability without an
  external runtime backend or fallback.
- `analyze(by="maximum_spanning_tree", directed=False)` now returns a
  deterministic Kruskal maximum spanning forest entirely in Rust. Strict unit
  or named finite signed weights, canonical public-UUID endpoints,
  descending-weight selection with stable whole-edge ties,
  disconnected/empty/isolate/self-loop/parallel behavior, relationship and
  label filtering, shared limits and cancellation without partial output,
  native Arrow through thin Python/Node adapters, structured errors, and
  knowledge-layer independence complete the 1196 capability without an
  external runtime backend or fallback.
- `analyze(by="bridges", directed=False)` now returns deterministic cut edges
  entirely in Rust. Label and relationship projection, stored-edge UUID
  multigraph semantics, self-loop handling, canonical UUID-only Arrow rows,
  stack-safe low-link traversal, shared limits and cancellation, thin native
  bindings, and knowledge-layer independence complete the 1231 Bridges
  capability without an external runtime backend or fallback.
- Graph-native Cypher reads, variable-length traversal, and all 55 currently
  registered Rust rank, cluster, similar, paths, and analyze handlers now run
  through a CI-enforced paired persistent-project isolation gate. Bare and
  synthetic knowledge/provenance-enriched clones must return identical stable
  Arrow results; an exhaustive typed partition also fails when a new handler
  lacks a paired case. These test-only sentinels enforce the ADR 0006 boundary
  without claiming the canonical epistemic-history contract tracked by #782
  (#772).
- `analyze(by="articulation_points", directed=False)` now returns the
  deterministic cut vertices of an undirected selection entirely in Rust.
  Label and relationship projection, stored-edge UUID multigraph semantics,
  self-loop handling, stack-safe low-link traversal, shared limits and
  cancellation without partial output, UUID-only Arrow through
  Rust/Python/Node, structured errors, and knowledge-layer independence
  complete the 1205 articulation-points capability without an external runtime
  backend or fallback.
- `analyze(by="topological_sort")` now returns a deterministic directed Kahn
  ordering entirely in Rust. Label and relationship projection,
  UUID-ordered ready-node ties, parallel-edge accounting, cycle and self-loop
  rejection, contiguous zero-based positions, shared limits and cancellation
  without partial output, UUID-only Arrow through Rust/Python/Node, structured
  errors, and knowledge-layer independence complete the 1200 topological-sort
  capability without an external runtime backend or fallback.
- `paths(by="transitive_closure")` now returns the global positive-length
  reachability relation entirely in Rust. Validated-but-unscoped sources,
  direction and relationship filtering, cycle-derived self-pairs, parallel-edge
  deduplication, deterministic UUID pair ordering, shared limits and
  cancellation without partial output, UUID-only Arrow through
  Rust/Python/Node, structured errors, and knowledge-layer independence
  complete the 1224 transitive-closure capability without an external runtime
  backend or fallback.
- `paths(by="yens")` now returns up to `k` exact, deterministic, distinct
  loopless paths entirely in Rust. Required targets and positive `k`, unit or
  strict finite non-negative weights, direction and relationship filtering,
  stable parallel-edge representatives and one-based ranking, shared limits
  and cancellation without partial output, UUID-only Arrow through
  Rust/Python/Node, structured errors, and knowledge-layer independence
  complete the 1207 Yen capability without an external runtime backend or
  fallback.
- `analyze(by="minimum_spanning_tree", directed=False)` now returns a
  deterministic Kruskal minimum spanning forest entirely in Rust. Strict unit
  or named finite weights, canonical undirected endpoints and whole-edge ties,
  disconnected/empty/self-loop/parallel behavior, relationship and label
  filtering, shared limits and cancellation without partial output, UUID-only
  Arrow through Rust/Python/Node, structured errors, and knowledge-layer
  independence complete the 1194 capability without an external runtime
  backend.
- `paths(by="delta_stepping")` now computes exact deterministic non-negative
  shortest paths entirely in Rust with fixed `delta = 1.0` light/heavy bucket
  semantics. Optional targets, unit or strict weights, direction and
  relationship filtering, stable ordering and ties, shared limits and
  cancellation without partial output, UUID-only Arrow through
  Rust/Python/Node, structured errors, and knowledge-layer independence
  complete the 1202 Delta-stepping capability without an external runtime
  backend.
- `paths(by="floyd_warshall")` now computes exact deterministic shortest paths
  for every reachable ordered non-self pair entirely in Rust. Global
  negative-cycle rejection, unit or strict finite signed weights, direction
  and relationship filtering, stable complete-path and edge ties, shared
  limits and cancellation, UUID-only Arrow through Rust/Python/Node,
  structured errors, and knowledge-layer independence complete the 1203
  Floyd-Warshall capability without an external runtime backend.
- `paths(by="bellman_ford")` now computes exact deterministic negative-weight
  shortest paths entirely in Rust. Optional targets, unit or strict finite
  weights, reachable-negative-cycle rejection, direction and relationship
  filtering, stable ties and ordering, shared limits and cancellation,
  UUID-only Arrow through Rust/Python/Node, structured errors, and
  knowledge-layer independence complete the 1201 Bellman-Ford capability
  without an external runtime backend.
- `paths(by="astar")` now computes exact deterministic source-target routes
  entirely in Rust, with a zero-heuristic default or strict caller-supplied
  node estimates. Direction and relationship filtering, strict optional
  weights, stable ties, shared limits and cancellation, UUID-only Arrow through
  Rust/Python/Node, structured errors, and knowledge-layer independence
  complete the 1199 A* capability without an external runtime backend.
- `paths(by="dijkstra_all_pairs")` now computes exact deterministic shortest
  paths for every reachable ordered non-self node pair entirely in Rust.
  Strict optional weights, direction and relationship filtering, stable ties
  and ordering, invocation-wide limits and cancellation, UUID-only Arrow
  results through Rust/Python/Node, structured errors, and knowledge-layer
  independence complete the 1197 all-pairs Dijkstra capability without an
  external runtime backend.
- `similar(by="cosine")` now performs exact exhaustive all-score cosine
  similarity entirely in Rust. Deterministic top-K retains positive, zero, and
  negative scores; validated graph-native vectors, UUID-only Arrow through
  Rust/Python/Node, resource limits, cancellation, structured errors, and
  topology and knowledge-layer independence complete the 1192 cosine
  similarity capability without an external runtime backend.
- `similar(by="filtered_node_similarity")` now performs relationship-filtered
  exact Jaccard scoring entirely in Rust. Outgoing `via` neighborhood and
  candidate eligibility, deterministic top-K and cutoff rules, UUID-only Arrow
  through Rust/Python/Node, shared limits and cancellation, structured errors,
  and knowledge-layer independence complete the 1191 filtered node-similarity
  capability without an external runtime backend.
- `similar(by="filtered_knn")` now performs relationship-filtered exact cosine
  K-nearest-neighbor comparison entirely in Rust. Outgoing `via` eligibility,
  deterministic top-K and cutoff rules, validated graph-native vectors,
  UUID-only Arrow through Rust/Python/Node, resource limits, cancellation,
  structured errors, and knowledge-layer independence complete the 1190
  filtered KNN similarity capability without an external runtime backend.
- `similar(by="knn")` now performs exact exhaustive cosine K-nearest-neighbor
  comparison entirely in Rust and returns deterministic UUID-only Arrow through
  Rust, Python, and Node. Required graph-native vectors, stable top-K and cutoff
  rules, vector/output limits, cancellation, structured errors, topology and
  knowledge-layer independence, and the absence of approximate runtime controls
  complete the 1189 KNN similarity capability.
- `cluster(by="k_core_decomposition")` now returns each node's exact,
  non-renumbered core number through the shared deterministic Rust peeling
  kernel used by rank. Direction-neutral simple projection, relationship
  filtering, UUID-only Arrow results through Rust/Python/Node, boundary
  semantics, shared resource guards and cancellation, structured errors,
  knowledge-layer independence, and atomic opt-in write-back complete the 1187
  k-core-decomposition cluster capability.
- `cluster(by="biconnected")` now computes the true overlapping biconnected
  block cover with a deterministic non-recursive Hopcroft-Tarjan kernel entirely
  in Rust, then returns a topology-stable scalar primary-membership projection
  as UUID-only Arrow through Rust, Python, and Node. Direction-neutral simple
  projection, articulation and boundary semantics, shared resource guards and
  cancellation, structured errors, knowledge-layer independence, and atomic
  opt-in write-back complete the 1186 biconnected-components cluster capability.
- `cluster(by="strongly_connected")` now computes deterministic directed SCCs
  with non-recursive Tarjan entirely in Rust and returns stable UUID-only Arrow
  communities through Rust, Python, and Node. Direction and relationship
  filtering, topology-stable consecutive IDs, multigraph and boundary
  semantics, shared resource guards and cancellation, structured errors,
  knowledge-layer independence, and atomic opt-in write-back complete the 1185
  strongly-connected-components cluster capability.
- `cluster(by="approximate_max_k_cut")` now computes a deterministic,
  unweighted two-way one-exchange cut entirely in Rust and returns stable
  UUID-only Arrow communities through Rust, Python, and Node. Fixed topology
  ties and canonical IDs, multigraph and boundary semantics, bounded resources
  and cancellation, structured errors, knowledge-layer independence, and
  atomic opt-in write-back complete the 1183 approximate maximum-cut cluster
  capability.
- `cluster(by="k_means")` now performs deterministic, fixed-`k = 10`
  squared-Euclidean clustering entirely in Rust and returns stable UUID-only
  Arrow communities through Rust, Python, and Node. Graph-native vectors,
  bounded resources and cancellation, structured errors, direction-neutral
  execution, and atomic opt-in write-back use the common algorithm contracts
  (the 1182 K-means cluster capability).
- `cluster(by="hdbscan")` now performs deterministic Euclidean HDBSCAN* with
  EOM selection entirely in Rust and returns stable UUID-only Arrow clusters,
  including `-1` noise, through Rust, Python, and Node. Graph-native vector
  properties are validated and bounded before execution; relationship `via`
  is rejected; shared cancellation, structured errors, direction-independent
  results, and atomic opt-in write-back use the common algorithm contracts
  (the 1181 HDBSCAN cluster capability).
- `cluster(by="spinglass")` now anneals the positive-edge
  Reichardt-Bornholdt Potts Hamiltonian entirely in Rust and returns the
  deterministic UUID-only Arrow partition through Rust, Python, and Node,
  with a fixed SplitMix64 schedule, direction-insensitive unweighted simple
  projection, component isolation, dense resource guard, shared cancellation
  and limits, structured errors, and atomic opt-in write-back (the 1180
  Spinglass cluster capability).
- `cluster(by="walktrap")` now performs deterministic four-step random-walk
  agglomeration entirely in Rust and returns the maximum-modularity UUID-only
  Arrow partition through Rust, Python, and Node, with stable Ward-cost ties,
  direction-insensitive simple adjacency, a dense resource limit, shared
  cancellation and limits, structured errors, and atomic opt-in write-back
  (the 1179 Walktrap cluster capability).
- `cluster(by="leading_eigenvector")` now performs deterministic recursive
  Newman modularity bisection entirely in Rust and returns a stable UUID-only
  Arrow partition through Rust, Python, and Node, with a built-in Jacobi
  eigensolver, direction-insensitive simple adjacency, fixed split and tie
  rules, a dense resource limit, shared cancellation and limits, structured
  errors, and atomic opt-in write-back (the 1178 Leading Eigenvector cluster
  capability).
- `cluster(by="infomap")` now minimizes the deterministic two-level map
  equation entirely in Rust and returns the stable UUID-only Arrow partition
  through Rust, Python, and Node, with directed stationary flow, fixed
  teleportation and tie rules, `via` normalization, shared limits and
  cancellation, structured errors, and atomic opt-in write-back (the 1177
  Infomap cluster capability).
- `cluster(by="fastgreedy")` now performs deterministic, incremental
  Clauset-Newman-Moore agglomeration in Rust and returns the recorded
  maximum-modularity partition through the UUID-only Arrow cluster schema in
  Rust, Python, and Node, with stable gain and partition ties,
  direction-insensitive simple adjacency, `via` filtering, shared limits and
  cancellation, structured errors, and atomic opt-in write-back (the 1176
  Fast Greedy cluster capability).
- `cluster(by="modularity_optimization")` now performs deterministic,
  single-level local modularity moves in Rust and returns the direct UUID-only
  Arrow partition through Rust, Python, and Node, without Louvain condensation
  or hierarchy, with direction-insensitive simple adjacency, `via` filtering,
  stable consecutive community IDs, shared limits and cancellation, structured
  numeric errors, and atomic opt-in write-back (the 1175 direct modularity
  optimization cluster capability).
- `cluster(by="girvan_newman")` now performs deterministic divisive
  edge-betweenness clustering in Rust and returns the maximum-modularity
  recorded partition through the UUID-only Arrow cluster schema in Rust,
  Python, and Node, with one-edge removal and full recomputation, stable ties,
  direction-insensitive simple adjacency, `via` filtering, shared limits and
  cancellation, structured errors, and atomic opt-in write-back (the 1174
  Girvan-Newman cluster capability).
- `cluster(by="speaker_listener")` now performs deterministic fixed-stream
  classic SLPA evolution in Rust and returns its strongest surviving primary
  membership through the UUID-only Arrow cluster schema in Rust, Python, and
  Node, with 100 asynchronous sweeps, `0.05` association pruning,
  direction-insensitive simple adjacency, `via` filtering, shared limits and
  cancellation, structured errors, and atomic opt-in write-back (the 1173
  speaker-listener cluster capability).
- `cluster(by="label_propagation")` now performs deterministic, asynchronous
  classic label propagation in Rust and returns the same UUID-only Arrow
  partition through Rust, Python, and Node, with fixed-stream sweep and tie
  ordering, direction-insensitive simple adjacency, `via` filtering, stable
  consecutive community IDs, shared limits and cancellation, structured
  errors, and atomic opt-in write-back (the 1172 label-propagation cluster
  capability).
- `cluster(by="leiden")` now performs deterministic, unweighted three-phase
  Leiden community detection in Rust and returns the same UUID-only Arrow
  partition through Rust, Python, and Node, with connected refinement,
  direction-insensitive simple adjacency, `via` filtering, stable consecutive
  community IDs, shared limits and cancellation, structured numeric errors,
  and atomic opt-in write-back (the 1171 Leiden cluster capability).
- `cluster(by="louvain")` now performs deterministic, unweighted multilevel
  modularity optimization in Rust and returns the same UUID-only Arrow
  partition through Rust, Python, and Node, with direction-insensitive simple
  adjacency, `via` filtering, stable consecutive community IDs, shared limits
  and cancellation, structured numeric errors, and atomic opt-in write-back
  (the 1170 Louvain cluster capability).
- `rank(by="total_neighbors")` now computes deterministic per-node
  total-neighbors missing-link aggregates in Rust and returns the same UUID-only
  Arrow result through Rust, Python, and Node, with outgoing/symmetric union
  semantics, `via` filtering, checked exact numeric behavior, positive
  cross-component and isolated-node boundaries, shared limits, structured
  errors, and atomic opt-in write-back (the 1169 total-neighbors capability).
- `rank(by="resource_allocation")` now computes deterministic per-node
  resource-allocation missing-link aggregates in Rust and returns the same
  UUID-only Arrow result through Rust, Python, and Node, with reciprocal-degree
  discounts, directed/undirected simple adjacency, `via` filtering, finite
  compensated numeric behavior, shared limits, structured errors, and atomic
  opt-in write-back (the 1168 resource-allocation capability).
- `rank(by="common_neighbors")` now computes deterministic per-node
  common-neighbor missing-link aggregates in Rust and returns the same UUID-only
  Arrow result through Rust, Python, and Node, with directed/undirected simple
  adjacency, `via` filtering, checked exact numeric behavior, disconnected and
  empty boundaries, shared limits and structured errors, and atomic opt-in
  write-back semantics (the 1167 common-neighbors capability).
- `rank(by="adamic_adar")` now computes deterministic per-node Adamic-Adar
  missing-link aggregates in Rust and returns the same UUID-only Arrow result
  through Rust, Python, and Node, with directed/undirected simple adjacency,
  `via` filtering, compensated finite sums, disconnected and empty boundaries,
  shared limits and structured errors, and atomic opt-in write-back semantics
  (the 1166 Adamic-Adar capability).
- `rank(by="preferential_attachment")` now computes deterministic per-node
  missing-link aggregates in Rust and returns the same UUID-only Arrow result
  through Rust, Python, and Node, with directed/undirected simple adjacency,
  `via` filtering, checked exact numeric behavior, disconnected and empty
  boundaries, shared limits and structured errors, and atomic opt-in write-back
  semantics (the 1165 preferential-attachment capability).
- `rank(by="k_core")` now computes deterministic core numbers in Rust and
  returns the same UUID-only Arrow result through Rust, Python, and Node, using
  direction-insensitive simple adjacency with `via` filtering, collapsed
  parallel/reciprocal relationships, ignored self-loops, disconnected/edgeless
  behavior, shared limits and structured errors, and atomic opt-in write-back
  semantics (the 1164 k-core capability).
- `rank(by="triangles")` now counts distinct unordered three-node cliques in
  Rust and returns the same deterministic UUID-only Arrow result through Rust,
  Python, and Node, with direction-insensitive `directed` modes, `via` filtering,
  collapsed parallel/reciprocal relationships, ignored self-loops, disconnected/
  edgeless behavior, shared limits and structured errors, and atomic opt-in
  write-back semantics (#1163).
- `rank(by="clustering_coefficient")` and its
  `local_clustering_coefficient` input alias now compute deterministic unweighted
  Fagiolo directed or classic undirected local transitivity in Rust and return the
  same canonical UUID-only Arrow result through Rust, Python, and Node, with `via`,
  simplified multigraph/self-loop behavior, shared limits/errors, and atomic
  opt-in write-back semantics (#1162).
- `rank(by="celf")` now computes deterministic Independent Cascade influence
  maximization in Rust and returns full-seed-set marginal spread through Rust,
  Python, and Node, with fixed UUID-keyed simulations, directed/undirected `via`
  selection, multigraph/self-loop and disconnected/edgeless behavior, shared
  limits and structured errors, and atomic opt-in write-back semantics (#1161).
- `rank(by="hits_authority")` now returns the final authority vector from the
  shared deterministic Rust HITS recurrence through Rust, Python, and Node,
  with fixed alternating L2-normalized updates, directed/undirected `via`
  selection, multigraph/self-loop and isolated/disconnected/edgeless behavior,
  shared limits and structured errors, and atomic opt-in write-back semantics
  (#1160).
- `rank(by="hits_hub")` now computes deterministic unweighted HITS hub scores
  in Rust and returns the same UUID-only Arrow result through Rust, Python, and
  Node, with fixed alternating L2-normalized updates, directed/undirected `via`
  selection, multigraph/self-loop and isolated/disconnected/edgeless behavior,
  shared limits and structured errors, and atomic opt-in write-back semantics
  (#1159).
- `rank(by="article_rank")` now computes deterministic unweighted ArticleRank
  delta propagation in Rust and returns the same UUID-only Arrow result through
  Rust, Python, and Node, with fixed damping/convergence, average-outdegree
  penalties, directed/undirected `via` selection, multigraph/dangling/
  disconnected behavior, shared limits and structured errors, and atomic
  opt-in write-back semantics (#1158).
- `rank(by="eigenvector")` now computes deterministic unweighted incoming
  eigenvector centrality with shifted power iteration in Rust and returns the
  same UUID-only Arrow result through Rust, Python, and Node, with L2-normalized
  convergence, directed/undirected `via` selection, multigraph/disconnected,
  shared-limit, structured-error, and atomic opt-in write-back semantics (#1157).
- `rank(by="harmonic_closeness")` now computes deterministic normalized
  unweighted outward harmonic closeness in Rust and returns the same UUID-only
  Arrow result through Rust, Python, and Node, with directed/undirected `via`
  selection, disconnected, multigraph/self-loop, shared-limit,
  structured-error, and atomic opt-in write-back semantics (#1155).
- `rank(by="closeness")` now computes deterministic unweighted outward
  Wasserman-Faust closeness in Rust and returns the same UUID-only Arrow result
  through Rust, Python, and Node, with directed/undirected `via` selection,
  disconnected, multigraph/self-loop, shared-limit, structured-error, and
  atomic opt-in write-back semantics (#1154).
- `rank(by="betweenness")` now computes exact deterministic unweighted Brandes
  node betweenness in Rust and returns the same UUID-only Arrow result through
  Rust, Python, and Node, with normalized direction/`via`, multigraph,
  self-loop/disconnected, shared-limit, structured-error, and atomic opt-in
  write-back semantics (#1153).
- `rank(by="pagerank")` now computes deterministic unweighted PageRank in Rust
  and returns the same UUID-only Arrow result through Rust, Python, and Node,
  with directed/undirected selection, `via`, multigraph, dangling/disconnected,
  shared-limit, structured-error, and atomic opt-in write-back semantics (#1152).
- `analyze(by="is_dag")` now performs deterministic DAG detection in Rust and
  returns the same one-row Boolean Arrow result through Rust, Python, and Node,
  with optional label and `via` selection, explicit direction, empty/multigraph,
  structured-error, and shared resource-limit semantics (#1202).
- `paths(by="dijkstra")` now computes exact deterministic non-negative weighted
  shortest paths in Rust and returns the same UUID-only Arrow result through
  Rust, Python, and Node, with target or all-reachable mode, strict optional
  weights, `via`, direction, selectors, structured errors, shared resource
  limits, and knowledge-layer independence (#1195).
- `paths(by="bfs")` now performs deterministic unweighted traversal in Rust and
  returns the same UUID-only Arrow path result through Rust, Python, and Node,
  with optional target, `via`, direction, selector, multigraph, empty-result,
  structured-error, and shared resource-limit semantics (#1193).
- `similar(by="node_similarity")` now computes deterministic outgoing-neighbor
  Jaccard scores in Rust and returns the same UUID-only Arrow result through
  Rust, Python, and Node, with `via`, positive top-K, multigraph, empty-result,
  structured-error, and shared resource-limit semantics (#1188).
- `cluster(by="components")` now computes deterministic weakly connected
  components through the shared Rust adjacency and dispatch pipeline, returning
  UUID-only Arrow results through Rust, Python, and Node with selector,
  multigraph, NULL, and atomic opt-in write-back semantics (#1184).
- `rank(by="degree")` now runs through the Rust adjacency and typed-dispatch
  pipeline, returning deterministic UUID-only Arrow results through Rust,
  Python, and Node with directed, undirected, multigraph, `via`, and atomic
  opt-in write-back semantics (#1156).
- `EdgeHandle` now exposes stable UUID identity and relationship-type metadata
  without leaking the internal numeric edge surrogate (#1286).
- `GraphForge::paths` now has one canonical Rust boundary accepting typed
  `NodeSelector` inputs; the temporary string and migration entrypoints are gone (#1296).
- Shared Rust, Python, and Node acceptance now enforces UUID-only node handles,
  Rust readback, and typed path-selector error contracts (#1301).
- Node `paths` now accepts UUID strings, native handles, and typed property
  selectors while leaving graph lookup and validation in Rust (#1303).
- The native Node addon now delegates `addNode` to Rust and returns a
  graph-owned handle exposing UUID identity without a storage surrogate (#1302).
- Python `paths` now coerces UUID strings, native handles, and explicit typed
  property selectors into Rust-owned `NodeSelector` validation (#1299).
- The Python package and compatibility API now export the native UUID-only
  `NodeHandle` and describe its concrete `add_node` return type (#1300).
- The native Python extension now delegates `add_node` to Rust and returns a
  graph-owned handle exposing UUID identity without a storage surrogate (#1298).
- A typed Rust path entrypoint now resolves UUID, graph-owned handle, and unique
  property selectors before algorithm dispatch, enabling thin binding migration (#1295).
- `GraphForge::add_node` now creates one UUIDv7-backed node through the canonical
  atomic Rust write path and returns a graph-owned UUID handle (#1291).
- Path node selectors now resolve UUIDs, graph-owned handles, and unique typed
  label/property matches entirely in Rust with bounded deterministic errors (#1284).
- Storage can now decode one complete node-property stem into typed UUID-keyed
  rows for bounded Rust selector/materialization work (#1288).
- `NodeHandle` now exposes UUID identity and opaque graph ownership instead of
  leaking an internal numeric storage surrogate (#1283).
- Rank and cluster now share validated, atomic UUID-keyed property write-back that
  preserves runtime-catalog persistence and topology/adjacency state (#1280).
- M18 node results now join selected property tables once per invocation by UUID,
  preserving deterministic nullable schemas and rejecting duplicates (#1256).
- The TCK performance baseline now reflects the complete post-M17
  `ubuntu-latest` run after the XOR, CREATE, quantifier, and multi-label
  corrections: all 3,897 scenarios pass in 66.505 seconds of summed scenario
  time, with a 113.515 ms p99 and 975.693 ms maximum. The absolute warning
  threshold tightens from 33.3 seconds to the generated 1.3-second boundary;
  relative scenario and aggregate warnings remain unchanged and advisory
  (#1276).
- Known literal node-label predicates now lower directly against canonical
  `type_ids` membership instead of rebuilding a complete string `labels()`
  list for every secondary pattern label. Multi-label MATCH planning is linear
  in requested labels while dynamic/unmapped membership and projected
  `labels()` values retain full openCypher semantics (#1275).
- Filtered node hydration now proves the canonical dense `node_id` layout from
  Parquet row-group and page metadata and selects exact physical rows. Scattered
  fixed-hop destinations no longer decode graph-scale node ranges; gapped,
  legacy, or malformed files fail closed to the existing predicate reader with
  post-read key validation and no migration or sidecar metadata (#1271).
- Terminal demand now bounds chained fixed-hop traversal across relationship
  uniqueness and DataFusion repartitioning without pushing hard limits through
  selective or blocking operators. Query-scoped soft batches and cancellation
  eliminate graph-wide over-pull, expose aggregate per-hop diagnostics, and cut
  cached LiveJournal two-hop `LIMIT 1000` from a 1,614.5 ms median and 29.5M
  scanned edge rows to 127.7 ms and 196,608 rows with all 3,897 TCK scenarios
  retained (#1269).
- Invariant `all`/`any`/`none`/`single` predicates now fold from list
  cardinality without per-element predicate batches. Uncorrelated list
  comprehensions evaluate volatile filters once over the logical element set,
  and heterogeneous list-plus-element operations reuse Arrow buffers while
  preserving dynamic list concatenation, order, nulls, and `rand()` evaluation
  counts (#1265).
- Terminal CREATE-only statements now materialize only bindings read by later
  clauses instead of rebuilding an ever-growing DataFusion frontier for dead
  node and relationship variables. Dependent references, property expressions,
  exact side effects, and single-commit rollback semantics are preserved; a
  deterministic 10x clause gate holds full reads and rewrite commits constant
  (#1264).
- XOR chains now remain first-class Graph IR and DataFusion UDF nodes instead
  of expanding shared subexpressions into an exponential AND/OR tree.
  Three-valued boolean semantics and left associativity are unchanged, while
  the deterministic 11-to-22 operand work count grows from 10 to 21 nodes
  (2.1x). Graph IR advances to 0.3.0 for the new serialized operator (#1263).
- The Rust BDD runner now records first-pass per-scenario timings for the API
  and complete openCypher corpus, publishes distribution/slow-scenario
  artifacts, and emits warning-only TCK regression annotations against a
  reviewable CI baseline without weakening the 3,897-scenario correctness gate.
  TCK fixtures reuse one reset-isolated in-memory engine under a versioned
  serial measurement profile, preventing cucumber's default 64-way execution
  from turning runtime and temporary-storage contention into false scenario
  regressions. The existing `clear()` lifecycle method now fully resets
  in-memory graph, catalog, procedure, and adjacency state while rejecting
  persistent projects without deleting their data (#1259).
- Fixed-hop traversal now uses a streaming, provider-backed `ExpandExec` for
  typed and wildcard relationships in every ontology mode and index state.
  Terminal `LIMIT`/`SKIP + LIMIT` fetches bound emitted hop work without
  crossing filters, ordering, distinct, or aggregation boundaries; resumable
  output chunks prevent high-degree and multi-hop Arrow overflows. A
  deterministic 10x scale test gates neighborhood-proportional I/O (#1248).
- Shape typed M18 handler rows into canonical UUID-only Arrow batches with
  stable metadata, identical empty/populated schemas, and strict logical value
  validation (#1255).
- Add typed Rust-only M18 handler registration, dependency metadata,
  cooperative cancellation, resource limits, and structured convergence errors
  for algorithm dispatch (#1253).
- Add the Rust M18 `AdjacencyProvider` adapter with deterministic UUID mappings,
  label/relation/direction selection, strict optional numeric weights, and
  stale-index fallback coverage (#610).
- M18 analyst algorithms now share exhaustive Rust `by=` enums in `graphforge-core`.
  Python validates names through the same registry, every algorithm maps to a
  stable UUID-only logical Arrow schema, and the `local_clustering_coefficient`
  alias has one canonical owner.
- Projection and scope semantics now support nested aggregate expressions,
  implicit grouping, path wildcards and forwarding, sequential CREATE property
  references, shadow-safe WITH aliases, ordered collection across WITH
  projections, deleted-entity access validation, and variable-independent
  SKIP/LIMIT expressions. The remaining twenty-five TCK scenarios move the
  committed Rust baseline to **3,897 / 3,897** with zero failures and zero
  regressions.
- MATCH now preserves relationship-list bindings, mixed-direction traversal,
  relationship isomorphism across fixed and variable segments, variable-length
  property predicates, computed and null node correlation, and OPTIONAL MATCH
  row multiplicity. Seventeen MATCH, WITH, and path scenarios move the committed
  TCK baseline to **3,872 / 3,897** with zero regressions.
- Static property access over heterogeneous graph-value expressions now uses
  key-aware runtime typing, and exploratory Parquet storage preserves mixed
  scalar property types with tagged values instead of stringifying them.
  Whole-node results therefore retain the original property types. Four graph
  access and filtering scenarios move the committed TCK baseline to
  **3,855 / 3,897** with zero regressions.
- Pattern binding now rejects scalar aliases reused as nodes or relationships,
  validates direct graph-function argument kinds, and preserves runtime-polymorphic
  null and `UNWIND` values. `labels()` and `type()` decode heterogeneous graph
  values and raise runtime errors for invalid elements. Thirty-one MATCH and
  graph-function scenarios move the committed TCK baseline to **3,851 / 3,897**
  with zero regressions.
- Relationship `MERGE` now preserves every existing pattern match, evaluates
  row-dependent properties before identity matching, and carries independently
  aliased entity identities through `WITH`. Correlated list comprehensions keep
  qualified outer columns distinct and return typed empty lists without
  evaluating their projection. Eleven MERGE scenarios move the committed TCK
  baseline to **3,820 / 3,897** with zero regressions.
- `DELETE` now accepts named paths and node, relationship, or path values
  extracted from lists and maps; null path targets are no-ops, scalar targets
  remain type errors, and shared label tokens are counted only when their last
  node is deleted. Nine Delete3/Delete5 scenarios move the committed TCK
  baseline to **3,809 / 3,897** with zero regressions.
- Terminal `LIMIT 0`, `SKIP`, filtering, and aggregation now shape write
  results without suppressing side effects. Created relationship values retain
  topology and properties through terminal projections, and DELETE reports net
  label-token removal across a statement. Twenty-five CREATE, SET, REMOVE, and
  DELETE scenarios move the committed TCK baseline to **3,800 / 3,897** with
  zero regressions.
- Write statements now alternate relational and mutation clauses over one
  identity-preserving frontier. `WITH`, `UNWIND`, `MATCH`, `MERGE`, and
  terminal ordering see the correct clause-ordered rows, including nodes still
  buffered by the statement's atomic write context. Twelve CREATE, MERGE,
  UNWIND, and aggregation scenarios move the committed TCK baseline to
  **3,775 / 3,897** with zero regressions.
- `SET` now accepts parenthesized entity targets, promotes mixed numeric list
  expressions, preserves scalar property shapes in whole-entity results,
  deduplicates pending-create replacement counters, and omits removed
  relationship properties. Seven Set1/Set2 scenarios move the committed TCK
  baseline to **3,763 / 3,897** with zero regressions.
- Collected node values now retain matchable and writable identity through
  `UNWIND`; path access works over collected path structs; multi-type
  relationship patterns include every alternative; and replacing an existing
  property reports both removal and addition side effects. Twenty-three list,
  path, match, triadic, and SET scenarios move the committed TCK baseline to
  **3,756 / 3,897** with zero regressions.
- `UNWIND null` now produces zero rows, and parameterized lists of maps retain
  their struct element type through `UNWIND`, `MERGE`, and `SET`. Two Unwind1
  scenarios move the committed TCK baseline to **3,733 / 3,897** with zero
  regressions.
- Simple `CASE` expressions now compare each `WHEN` arm with openCypher
  cross-type equality instead of coercing every operand to one DataFusion type.
- Chained comparisons now evaluate as conjunctions over adjacent operands, so
  bounded numeric/string ranges and longer mixed chains follow openCypher
  semantics.
- `SET n:Label` and `REMOVE n:Label` now mutate authoritative multi-label
  membership for pending and persisted nodes, preserve routing identity, and
  report idempotent label side effects.
- Persistent graph open now removes abandoned staged-write temp files from
  graph-owned topology and property directories while preserving unrelated
  files.
- The execution fuzz target now rejects empty-scope projection wildcards and
  reports result-shaping schema mismatches as errors instead of panicking. The
  daily scheduled fuzz workflow now gates on execution-target failures.
- Confidence-enabled `UNION` now deduplicates on public result columns after
  conjunction folding and retains the complete highest-confidence derivation,
  keeping its confidence and provenance aligned. `UNION ALL` continues to
  preserve every derivation.
- `UNION ALL` now executes independently bound branch plans while preserving
  duplicates, and `UNION` applies distinct result shaping. Mixed union modes
  and incompatible output columns fail during binding. All 12 Union1-Union3
  scenarios now pass, moving the committed TCK baseline to **3,694 / 3,897**
  with zero regressions.
- Registered `CALL ... YIELD` procedures now execute through the public
  `GraphForge` facade with explicit and implicit arguments, compile-time
  signature validation, null-safe fixture lookup, output projection and
  aliases, and correlated in-query semantics. All 52 Call1-Call6 scenarios now
  pass, moving the committed TCK baseline to **3,682 / 3,897** with zero
  regressions.
- `MERGE` now executes real node and relationship match-or-create semantics,
  including row-dependent pattern properties, same-statement pending visibility,
  multi-label matching, undirected relationship matching, named paths, deleted
  entity exclusion, and row-masked `ON CREATE` / `ON MATCH` property and label
  actions. Terminal global `count(*)` / `count(expr)` now aggregate over the
  materialized write frontier. Seventy-four newly passing write and MERGE
  scenarios move the committed TCK baseline to **3,630 / 3,897** with zero
  regressions.
- SET/REMOVE follow-up coverage now proves per-row routing for untyped
  relationships and persisted list-property round trips. Invalid map-shaped
  property values are reported as deliberate runtime type errors, moving the
  committed TCK baseline to **3,556 / 3,897** with zero regressions.
- `SET entity += map` now merges runtime map values into node and relationship
  properties, while `SET entity = map` replaces the complete property set.
  Null map values remove properties, null targets are ignored, and parameter
  maps plus same-statement entity returns work end to end. Ten Set4/Set5
  scenarios move the committed TCK baseline to **3,555 / 3,897** with zero
  regressions.
- Nodes now persist an authoritative multi-label set while retaining an
  immutable primary label for property-file routing and legacy compatibility.
  Multi-label CREATE, membership MATCH, label predicates, `labels()`, optional
  matching, and legacy scalar-topology migration work end to end. Fifteen new
  scenarios move the committed TCK baseline to **3,545 / 3,897** with zero
  regressions.
- CREATE, DELETE, SET, REMOVE, and MERGE now share the clause-ordered statement driver,
  including one staged commit, parameter binding, terminal RETURN shaping, and
  a consistent six-counter Arrow summary for statements without RETURN.
  Twenty-six write-result, null-target, list, and dynamic-value scenarios move
  the committed TCK baseline to **3,530 / 3,897** with zero regressions.
- Mixed-write statements now expose accumulated SET and REMOVE property values
  to later value expressions through a statement-local frontier overlay,
  including writes to nodes created earlier in the same statement.
- Compound aggregate `WITH` projections now preserve named path values and
  their hidden node/relationship components, including renamed aliases, so
  `nodes()`, `relationships()`, and `length()` remain available downstream.
- Named paths now compose zero-hop, multi-segment fixed-hop, and mixed
  variable/fixed patterns for bare path values plus `nodes()`,
  `relationships()`, and `length()`. Twenty-four named-path and path-variable
  validation scenarios move the committed TCK baseline to **3,504 / 3,897**
  with zero regressions.
- Fixed-length MATCH now treats repeated node variables as equality constraints
  instead of duplicate scans and enforces relationship uniqueness within each
  path pattern without leaking that constraint across comma-separated patterns.
  Twenty-three MATCH, OPTIONAL MATCH, filtering, and aggregation scenarios move
  the committed TCK baseline to **3,480 / 3,897** with zero regressions.
- `ORDER BY` now preserves legal hidden source keys across projection, resolves
  projected aliases and aggregate expressions against their output columns,
  keeps DISTINCT scoped to visible results, and rejects invalid aggregate/scope
  shapes during binding. Thirty-eight ordering, projection, path-length, and
  LIMIT scenarios move the committed TCK baseline to **3,457 / 3,897** with
  zero regressions.
- Dynamic heterogeneous list expressions now preserve node, relationship, path,
  temporal, nested-list, map, and scalar payloads in a finite Arrow tagged
  struct. Twenty-two comparison, conversion, projection, grouping, and ordering
  scenarios move the committed TCK baseline to **3,419 / 3,897** with zero
  regressions.
- `collect()` now preserves an immediately preceding explicit `ORDER BY` by
  carrying the logical sort keys into DataFusion's ordered `array_agg`. The ten
  `WithOrderBy1` comparison-consistency scenarios move the committed TCK
  baseline to **3,397 / 3,897** with zero regressions.
- `ORDER BY` now compares zoned `time` and `datetime` values by their absolute
  UTC instant while preserving each value's original offset for rendering.
  Twenty `WithOrderBy1`/`WithOrderBy2` scenarios move the committed TCK
  baseline to **3,387 / 3,897** with zero regressions.
- Intermediate `WITH` projections now disambiguate scalar aliases from same-named
  properties on forwarded node and relationship values without changing the
  public `RETURN` schema. This fixes 42 `WithOrderBy1` scenarios and moves the
  committed TCK baseline to **3,367 / 3,897** with zero regressions.
- Nested existential subqueries now preserve outer and intermediate
  correlations across simple bodies, full read pipelines, and nested pattern
  predicates without leaking child-local variables or multiplying outer rows.
  `ExistentialSubquery3` is now **3 / 3**, the combined existential set is **10
  / 10**, and the committed TCK baseline moves to **3,325 / 3,897** (+3, 0
  regressions).
- Full read-only `exists { MATCH ... WITH ... WHERE ... RETURN ... }`
  subqueries now reuse the normal clause pipeline, correlate outer variables,
  keep child-local names scoped, and discard terminal projection values before
  the existence semi-join. Write clauses remain compile-time errors.
  `ExistentialSubquery2` is now **3 / 3**, and the committed TCK baseline moves
  to **3,322 / 3,897** (+2, 0 regressions).
- Aggregation in `WITH` now supports implicit scalar, node, relationship, and
  path grouping; aggregate expressions mixed with constants or projected
  grouping leaves; post-aggregate filtering, ordering, skip, and limit; and
  compile-time rejection of ambiguous grouping expressions. Empty-input and
  null-only groups retain Cypher cardinality. The TCK baseline moves to **3,320
  / 3,897** (+15, 0 regressions).
- Simple `exists { pattern WHERE predicate }` subqueries now correlate outer
  variables while keeping child variables local, filter without multiplying
  outer rows, and support relationship predicates such as `type(r)`. The four
  `ExistentialSubquery1` scenarios and two related `MatchWhere1` scenarios move
  the TCK baseline to **3,305 / 3,897** (+6, 0 regressions).
- Pattern comprehensions nested inside graph-valued list comprehensions now
  correlate once per node element, preserve lexical shadowing, element order,
  null/empty shaping, and nested confidence/provenance. `Pattern2` is now
  **11 / 11**, and the TCK baseline is **3,299 / 3,897** (+1, 0 regressions).
- Fixed-hop `nodes(path)` now returns complete endpoint values with dynamic
  labels and a shared nullable property shape, including endpoints backed by
  different label tables. The TCK renderer now emits bare path structs as
  directional openCypher path literals. Pattern2 reaches **10 / 11**, and the
  advisory baseline moves to **3,298 / 3,897** (+18, 0 regressions).
- Correlated pattern comprehensions now aggregate one projected list per outer
  row, preserve null elements and duplicate outer rows, and shape no-match
  results as typed empty lists. Opt-in confidence/provenance includes every
  matched child entity. Pattern2 node/relationship property projections and
  List6 degree/existence checks move the TCK baseline to **3,280 / 3,897** (+6,
  0 regressions); path-value hydration remains tracked by #806.
- Pattern comprehensions now bind into a correlated Graph IR child plan with
  isolated pattern-local variables, optional filtering, and a terminal value
  projection. Relational collection/execution remains the next #961 slice, so
  this IR-only change does not move the TCK baseline.
- Pattern comprehension syntax now parses into the existing AST representation,
  including named paths, anonymous patterns, optional filters, variable-length
  relationships, and projection expressions. Binding and execution remain the
  next #961 slice; this parser-only change does not move the TCK baseline.
- Pattern predicate disjunctions now close the final #961 `Pattern1` residuals:
  multi-type relationships and `OR`-composed existential patterns union only
  their correlated keys before one semi/anti join, preserving outer duplicates
  without multiplying rows when several alternatives match. The TCK advisory
  baseline is now **3,274 / 3,897** (+2, 0 regressions).
- Variable-length pattern predicate expressions now cover the #1069 slice from
  #961: typed outgoing, incoming, undirected, exact-length, and two-bound-node
  variable-length predicates lower through the existing correlated existential
  var-length traversal without multiplying outer rows. The TCK advisory baseline
  is now **3,272 / 3,897** (+7, 0 regressions).
- Fixed single-hop pattern predicate expressions now cover the #1067 slice from
  #961: `(n)-->()`, `(n)--()`, `(n)<--()`, typed relationship predicates,
  two-bound-node predicates, `NOT`, and `AND` lower as correlated existential
  filters without multiplying outer rows; invalid self-pattern truthiness is
  rejected at bind time. The TCK advisory baseline is now **3,265 / 3,897**
  (+12, 0 regressions).
- Label/type predicate expressions now cover the #1065 slice from #961:
  `n:Label` and `r:TYPE` parse and lower in `WHERE`/`RETURN`, execute against
  runtime `labels()`/`type()` values, and propagate null through optional graph
  values. The TCK advisory baseline is now **3,253 / 3,897** (+9, 0
  regressions).
- Parser literal and pattern-token handling now covers the #1063 slice from
  #961: leading-dot floats, signed minimum integer literals across decimal,
  hex, and octal forms, anonymous `<-->` relationship segments, and explicit
  unbounded var-length ranges (`*..`) parse correctly. The TCK advisory
  baseline is now **3,244 / 3,897** (+18, 0 regressions).
- Percentile aggregates now cover the current #1054 TCK slice:
  `percentileDisc()` returns the discrete ranked input value, `percentileCont()`
  interpolates continuously, and out-of-range percentile arguments fail
  deliberately at runtime. The TCK advisory baseline is now **3,226 / 3,897**
  (+12, 0 regressions).
- List validation and list-comprehension residuals now close the current #1053
  TCK slice: parameterized list lookup works without static type information,
  invalid list/container subscripts and non-list `IN` operands fail deliberately,
  `collect()` materializes graph values for list-comprehension property access,
  aggregate calls inside list-comprehension filter/projection are rejected at
  compile time, and `size(path)` is rejected in favor of `length(path)`. The TCK
  advisory baseline is now **3,214 / 3,897** (+20, 0 regressions).
- Nested graph values now render correctly for the current #1055 TCK slice:
  graph variables inside map/list literal value positions lower to Arrow
  node/relationship values instead of scalar UUID identities, so map projections
  can contain nested nodes and relationships. The TCK advisory baseline is now
  **3,194 / 3,897** (+1, 0 regressions).
- `keys()` and `properties()` now cover the current #1052 TCK slice:
  `keys()` works over literal/parameter/null maps without regressing
  node/relationship property-key semantics, `properties()` returns map values for
  nodes, relationships, maps, and null, and optional child node shapes are tracked
  so absent optional entities propagate null. The TCK advisory baseline is now
  **3,193 / 3,897** (+29, 0 regressions).
- Parameterized row counts now close the current #960 TCK slice: `SKIP $param`
  and `LIMIT $param` materialize against runtime query parameters, invalid
  negative/floating values fail at runtime, and single CREATE/DELETE/SET/REMOVE
  write paths bind query parameters before execution. The TCK advisory baseline
  is now **3,164 / 3,897** (+9, 0 regressions).
- Scalar built-in and operator lowering now closes the current #1033 TCK slice:
  Cypher built-in names resolve case-insensitively, `substring()` uses Cypher's
  zero-based start index while preserving the current Utf8 result contract, and
  string `+` chooses concatenation when a known string is combined with a
  property-backed value that is not proven non-string. The TCK advisory baseline
  is now **3,155 / 3,897** (+6, 0 regressions).
- The binder now rejects invalid row-count and numeric literal cases for #1037:
  `SKIP`/`LIMIT` require non-negative integer constants across RETURN and WITH,
  non-constant/negative/floating-point row counts fail at bind time, and
  overflowing float literals such as `1.34E999` are rejected. The TCK advisory
  baseline is now **3,149 / 3,897** (+11, 0 regressions).
- Map and property value access now follows openCypher null semantics for #1029:
  absent map keys/properties and null containers return `null`, dynamic map
  subscripts and node/relationship property lookups use runtime string keys,
  computed keys such as `n['nam' + 'e']` resolve correctly, and query parameters
  can carry map values through the Rust/Python/Node API surfaces.
- Whole graph entity values now materialize through projections for the #889
  cluster: fixed-hop `RETURN r`, `RETURN p`, `type(r)`, relationship collections,
  fixed-hop `relationships(p)` relationship properties, `labels(n)`/`labels(null)`,
  and `keys(r)` all use Arrow node/relationship/path values instead of scalar
  identity fallbacks. The TCK advisory baseline is now **3,004 / 3,897** (+14
  since the #1028 baseline, 0 regressions).
- WITH/RETURN projection scope now handles projection aliases in `ORDER BY`,
  `WITH ... WHERE` over both projected aliases and incoming variables, `WITH *`,
  and `WITH DISTINCT`; the TCK advisory baseline is now **2,990 / 3,897** (+29,
  0 regressions) for the #1028 cluster.
- `WITH n AS m` now carries whole-node aliases through the read path, and
  write-result `RETURN` can project terminal rows from `CREATE` statements that
  reuse matched/reference nodes, mint created nodes/edges, or end mixed write
  phases with frontier-only projections. Graph reads after writes still reject
  explicitly until pending-write visibility lands (#814).
- openCypher value semantics now match the TCK for the #962 cluster: boolean
  `AND`/`OR` use Kleene null logic, NaN equality/ordering and cross-type
  comparisons return the Cypher results, predicate precedence distinguishes
  `IS NULL`/`IN`/string predicates from comparison, `range()` includes negative
  endpoints correctly, explicit-null list-slice bounds return `null`, relationship
  variables compare by identity, and `collect`/`collect(DISTINCT)` filter nulls.
- Heterogeneous and nested list values now flow through list `+` and conversion
  built-ins for #1030: list append/concat/prepend uses GraphForge's tagged
  Cypher value representation instead of homogeneous Arrow array coercion, and
  `toInteger`, `toFloat`, and `toString` decode tagged list-comprehension
  elements before converting.
- **Full-range `date`** (ADR 0012 / #1011): the `date` type now spans openCypher's
  entire year range **−999,999,999 … +999,999,999**, where the old Arrow `Date32`
  (i32 days, ≈ year ±5.8M) and chrono `NaiveDate` (≈ year ±262k) both fell short —
  so `date('-999999999-01-01')` and the billion-year `duration.between`/
  `.inSeconds` cases (**Temporal10 [9]/[10]**) previously returned `null`. Two
  parts: (1) a custom proleptic-Gregorian **`calendar`** module on `i64` days
  (Hinnant's civil algorithms, ISO-week/ordinal/weekday, day-clamped month add,
  signed-year formatting) replaces chrono for all date math, and the `duration`
  model widens to `{months, days, seconds, nanos}` all `i64` in floor-canonical
  form (a ~6.3e16-second span fits `i64`); (2) a standalone `date` becomes a
  self-describing **`Struct{epoch_day: Int64}`** (a bare `Int64` is
  indistinguishable from an integer property on storage decode) and the `date`
  child of the `localdatetime`/`datetime` structs becomes `Int64` days. Parsing,
  rendering, component accessors, `truncate`, projection, map construction, and
  the `duration.between` family are all range-complete. Corpus **2,394 → 2,396**
  (the two Temporal10 large-value scenarios), 0 regressions.
- Date values now round-trip through node-property storage as a typed date, and
  `date` **component accessors** work (ADR 0009 / #920). A new `IrLiteral::Date`
  carries an Arrow `Date32` through the write/read path (`graphforge-storage`), so a
  stored date reads back and renders as a date (not a datetime). `d.year`/
  `.quarter`/`.month`/`.week`/`.weekYear`/`.day`/`.ordinalDay`/`.weekDay`/
  `.dayOfQuarter` on a `Date32`-typed value lower to component extraction
  (`cypher_date_component`, ISO-8601 semantics) — dispatched by type
  (`ExprLowerer` now sees the input schema), so non-date `.name` stays property
  access. Verified functional (inline `WITH date({…}) AS d RETURN d.year`) and
  unit-tested against `Temporal5`'s values. Corpus unchanged at **983** (the
  `Temporal5` scenarios store the date via `CREATE`, which can't yet evaluate a
  computed property value — a separate write-path gap; this is the verified
  foundation that pays off once that lands).
- `date(<value>)` now projects from a runtime temporal value (ADR 0009 / #920,
  the `Temporal3` "select" form): `date(other)` extracts the date, and
  `date({date: other, year: 28, …})` overrides the named components (calendar /
  ISO-week / ordinal / quarter mode, keeping the rest from `other`). A new
  `cypher_date_project` UDF takes the base (`Date32` or an ISO date/datetime
  string) plus eight nullable overrides and returns `Date32`. Corpus passing
  **964 → 983** (+19: `Temporal3`).
- `date` is now a typed runtime value — an Arrow `Date32` — rather than a
  canonical `Utf8` string (ADR 0009 phase 1, the first of the runtime
  temporal-value subsystem). `date(<literal>)` lowers to a `Date32` scalar and
  `date(<runtime expr>)` to `to_date(...)` (also `Date32`), so dates compare,
  sort, and round-trip as one type — e.g. `date(toString(d)) = d` is now
  `Date32 == Date32`. The TCK renderer formats a `Date32` back to the canonical
  `YYYY-MM-DD`. Corpus unchanged at **964** (a representation change, verified
  zero-regression via the baseline gate) — it's the foundation the date
  projection (`Temporal3`), accessors (`Temporal5`), and arithmetic (`Temporal8`)
  phases build on. Equality across types stays correct via `cypher_eq`.
- Cypher equality (`=`/`<>`) is now type-tolerant (`cypher_eq`, ADR 0009).
  Comparing values of **different types** is `false` (`<>` → `true`) rather than
  a DataFusion planning error, and equality is the proper three-valued Cypher
  semantics: `null` propagates; lists/maps compare structurally (a length or
  key-set mismatch is `false`, a `null` element with no definitive inequality is
  `null`); node/relationship/path values compare by identity. Same-typed scalars
  keep Arrow's vectorised kernel; cross-numeric (`1 = 1.0`) coerces. This unblocks
  typed temporal values (the next ADR-0009 phase) and fixes heterogeneous
  equality generally. Corpus passing **946 → 964** (+18: `Comparison1` equality,
  `List3` list equality).
- Temporal constructors now also build from a literal **map** —
  `date({year, month, day})`, `localtime`/`time`/`localdatetime`/`datetime`
  (with default-`Z`, offset, or IANA named-zone `timezone:` fields), and
  `duration({months, days, …})` (#599). Map fields are read at lowering time
  (`graphforge-rel::temporal`), so a map of literal/constant fields is canonicalised to
  the quoted-ISO `Utf8` the TCK renders. Supports calendar/ISO-week/ordinal/
  quarter date selection (with `dayOfWeek`/`dayOfQuarter` and `date`-anchored
  week forms), additive `millisecond`+`microsecond`+`nanosecond` subseconds,
  second-precision zone offsets (`+02:05:59`), DST-correct named zones, and
  `datetime.fromepoch`/`datetime.fromepochmillis`. This completes `Temporal1`
  (Create Temporal Values from a Map): **206** scenarios, and cascades into
  temporal comparison/ordering/rendering. Corpus passing **714 → 946** (+232).
  Operations on _runtime_ temporal values (`Temporal3` select-from-value,
  accessors, arithmetic) remain follow-ups — they need typed temporal values in
  the engine, not the lowering-time string shortcut.
- All temporal constructors now build from an ISO-8601 **string** literal —
  `localtime`, `time`, `localdatetime`, `datetime` (incl. named time zones), and
  `duration`, joining `date` (#599). As with `date`, a literal-string argument is
  parsed and canonicalised in `graphforge-rel::temporal` at lowering time and emitted as
  the quoted-ISO `Utf8` literal the TCK renders. Highlights:
  - Times render at the precision the input specified (`21:40` vs `21:40:32` vs
    `21:40:32.142`), with trailing-zero subseconds trimmed.
  - Zone offsets normalise to `±HH:MM` (`+0100` → `+01:00`, `-02` → `-02:00`),
    and a zero offset renders as `Z`.
  - `datetime('…[Europe/London]')` resolves the offset from the IANA tz database
    at that instant (on `chrono-tz`), including pre-standard-time LMT offsets
    (`Europe/Stockholm` in 1818 → `+00:53:28`).
  - `duration('P0.75M')`-style decimals spill into smaller units using
    openCypher's average-Gregorian-month seconds; the alternative
    `P2012-02-02T14:37:21.545` form is supported.
    This completes `Temporal2` (Create Temporal Values from a String): **53/53**.
    Corpus passing **672 → 714** (+42). Map/component construction
    (`Temporal1`/`Temporal3`) and accessors/arithmetic are the remaining temporal
    work.
- `date(<string>)` now accepts every ISO-8601 form, not just canonical
  `YYYY-MM-DD` (#599). Because the openCypher TCK builds temporals from constant
  arguments, `date(<literal string>)` is parsed and canonicalised at **lowering
  time** (new `graphforge-rel::temporal`, on `chrono`) and emitted as the `Utf8` literal
  the TCK renders as the quoted ISO date — no runtime UDF. Covers calendar
  (`2015-07-21`, `20150721`), year-month (`2015-07`, `201507`), year-only
  (`2015`), ISO-week (`2015-W30-2`, `2015W302`, `2015-W30`, `2015W30`), and
  ordinal (`2015-202`, `2015202`) forms. Non-literal/non-string arguments still
  fall back to DataFusion `to_date`/`to_char` (canonical only). Corpus passing
  **663 → 672** (+9: the remaining `Temporal2` parse-date-from-string forms).
  Map-argument construction and the time-bearing types
  (datetime/time/localtime/localdatetime) remain follow-ups.
- `date('YYYY-MM-DD')` from an ISO string (#599) — the first temporal constructor.
  Lowered to `to_char(to_date(s), '%Y-%m-%d')`, parsing+validating the string and
  re-rendering it canonically as the quoted ISO string the openCypher TCK uses for
  a date. Corpus passing **660 → 663** (+3: `Temporal2` parse-date-from-string,
  `Temporal4` null propagation). Map-argument construction (`date({year, month,
day})`), the other temporal types (datetime/time/localtime/localdatetime), and
  ISO week/ordinal dates are follow-ups (a long tail of per-type/format work).
- Map literals `{k: v, …}` (#599/#600) — previously rejected ("MapLiteral has no
  direct DataFusion equivalent"), now lowered to an Arrow `Struct` via
  `named_struct` (keys → field names, values → fields), the same representation
  node/relationship property bags use. So `m.key` resolves through the existing
  `get_field` path, nested maps work recursively, and the TCK harness renders a
  map `Struct` (one with no `labels` field) as `{key: value, …}` (keys sorted).
  An empty map `{}` is an empty struct. Corpus passing **637 → 660** (+23:
  `Literals8` maps incl. 40-deep nesting, `Comparison1` map equality, `Literals7`).
- `UNWIND` over a node's qualified columns no longer errors, and `keys(node)` is
  supported (#599/#600). `UnwindExec` rebuilt its `DFSchema` from the _physical_
  Arrow input schema, whose field names are unqualified (`name`), so a list
  expression referencing a qualified column (`var_0.name` — e.g. `UNWIND [n.name,
n.surname]` or `keys(n)`) failed at execution with "No field named var_0.name".
  It now plans the expression against the `UnwindNode`'s **logical** input schema
  (which keeps qualifiers), resolving to column indices that align with the batch.
  On top of that, `keys(node)` is implemented — a per-row list of the property
  names the node actually has (non-null), built from the node's shape. Corpus
  passing **634 → 637** (+3: `Graph8` keys on a single node, `Remove1`).
- Nested aggregates in a RETURN expression (#599) — `count(*) + 1`,
  `count(a) + count(b)`, `count(n) / 60 / 60` previously failed (the aggregate
  wasn't the whole item, so `count` lowered as a plain function →
  `UnknownFunction`). A RETURN whose items are aggregate expressions or constants
  (no group-by keys) now decomposes into an `Aggregate` — one `AggExpr` per
  aggregate call, each bound to a synthetic variable — followed by a `Project`
  that computes the surrounding arithmetic over those variables (the standard
  aggregate→project lowering). The `Aggregate` IR op gained optional `out_var`
  (per agg) / `group_vars` fields; a plain top-level aggregate is unchanged, so
  `RETURN count(*) AS c ORDER BY c` still resolves exactly as before. Corpus
  passing **631 → 634** (+3). Mixed group-key + nested-aggregate items
  (`RETURN n.x, count(*) + 1`) remain a follow-up.
- List literals with nulls no longer panic; heterogeneous lists degrade gracefully
  (#598/#600) — a constant list literal was const-folded via `ScalarValue::new_list`,
  which **panicked** when an element was an untyped `null` (`[1, null]` →
  "Expected Int, got NULL") or the types were mixed (~87 of the corpus's execution
  panics). Untyped nulls are now re-typed to the list's element type (so `[1, null]`
  is a correct nullable list), and a genuinely heterogeneous literal (`[1, 'abc']`)
  falls back to `make_array` — which coerces a common type (widening `[1, 2.5]` →
  Float64) or surfaces a graceful execution error instead of crashing. The TCK
  error-assertion step also now refuses to count a caught panic as a genuine
  rejection. Corpus passing **618 → 631** (+13).
- `range(start, end [, step])` list function (#598/#600) — lowered to DataFusion's
  `range`, adjusting for Cypher's INCLUSIVE end (DataFusion's is exclusive): the
  stop is shifted one past `end` (`+1` ascending, `-1` for a literal-negative
  step); `step` defaults to 1. Unblocks the `List11` range family (default/explicit
  step, and the invalid-argument negative cases — a `step` of 0 surfaces
  DataFusion's genuine argument error) plus range-driven `UNWIND`/aggregation.
  Corpus passing **575 → 618** (+43).
- Group-by keys of a mixed aggregate query are named (and materialized) correctly
  (#599) — in `RETURN n.name, count(*)` the `n.name` group-key column was named by
  its lowered expression, not the `n.name` header openCypher expects; and a
  whole-node group key (`RETURN n, count(*)`) lowered to a bare `var_N` (not a real
  column) instead of a node value. The `Aggregate` IR op now carries an optional
  per-key output alias (`group_aliases`, parallel to `group_by`), filled by the
  binder from the RETURN item's `AS` alias or source text and applied by the
  lowerer; and group keys are materialized through the same path as terminal RETURN
  items (a bare node var → `_node_struct`). Corpus passing **570 → 575** (+5).
- Un-aliased aggregate columns are named by their source text (#598/#599) — an
  aggregate `RETURN` item without `AS` (`RETURN count(*)`, `min(x)`, `sum(i)`) was
  named `agg_<n>`, failing the TCK's header-exact comparison. It now uses the
  verbatim expression text (`ReturnItem::display`, the same source as the
  non-aggregate columns in #906), falling back to `agg_<n>` only when neither an
  alias nor display text exists. Corpus passing **555 → 570** (+15). (Naming the
  group-by keys of a _mixed_ aggregate query — `RETURN n.name, sum(n.num)` — is a
  follow-up; it needs per-key aliases on the `Aggregate` op.)
- TCK harness — error-assertion step (#598) — added `Then a <Error> should be
raised at <compile time|runtime|any time>: <detail>` for the corpus's negative
  scenarios. GraphForge does not track openCypher's error _categories_
  (SyntaxError / TypeError / …), so the step asserts the _phase_ the error
  occurred in (compile → `parse`/`plan` error, which covers both true syntax
  errors and bind-time semantic ones like undefined/duplicate variables; runtime →
  `execution`/`storage`). Crucially it only counts an error that is GraphForge
  **deliberately validating** an invalid query — capability-gap errors
  (`unsupported`, `not yet`, `unknown built-in function`, DataFusion `Schema
error` / `Error during planning` / `type_coercion`, `not implemented`) are
  rejected, since a scenario that "passes" only because GraphForge can't run the
  construct is a false conformance pass. Corpus passing **331 → 555** (+224, all
  genuine rejections — parse errors, variable-scope/already-bound binder checks,
  `DELETE`-target and `MERGE`-combination validations); ~309 capability-gap false
  passes were deliberately excluded. Baseline re-blessed.
- TCK harness — ordered and empty result-assertion steps (#598/#600) — added the
  cucumber step definitions `Then the result should be, in order:` (order-sensitive
  positional comparison, for `ORDER BY` scenarios) and `Then the result should be
empty` (zero rows). Previously undefined, so every scenario using them failed on
  the missing step regardless of correctness. Corpus passing **313 → 331** (+18);
  baseline re-blessed. The remaining ordered/empty scenarios await other engine
  features (their values, not the assertion, are the blocker).
- Un-aliased `RETURN` columns are named by their source text (#598) — openCypher
  names a projection column by the expression as written (`n.prop`, `count(*)`,
  `a.x IS NULL`), but GraphForge left such columns to DataFusion's derived name
  (internal `var_N` qualifiers / substituted literals), so a huge class of
  scenarios produced the right _values_ under the wrong _header_ and failed the
  TCK's header-exact comparison. The parser now captures each un-aliased item's
  verbatim expression text (`ReturnItem::display`, sliced by span) and the binder
  uses it as the default column name when there is no explicit `AS`. Corpus
  passing **250 → 313** (+63: Boolean1–5, Comparison1–2, Match5, List, Graph6, …);
  baseline re-blessed. (`RETURN *` names each column by its variable.)
- CI now runs the Rust test suite (`cargo test --workspace`) — previously the
  `test.yml` Rust job only ran `cargo check`/`clippy`/`fmt`, so the integration
  tests (`e2e_baseline`, `facade_methods`, `logical_plan_golden`, …) compiled but
  never executed and Rust-level regressions reached `main` uncaught. Closes that
  gap — it is the gate that would have caught the two bugs fixed just below.
- Re-matching a `WITH`-forwarded node no longer double-joins its properties
  (#598/#814) — `MATCH (a:Person) WITH a MATCH (a)-[:KNOWS]->(b) RETURN *` errored
  with `Projections require unique expression names` (`var_0.age` twice): the
  bound-var `NodeScan` re-joined `a`'s property table even though `WITH` already
  forwarded those columns, so two `var_0.<prop>` columns reached the `RETURN *`
  projection. `join_node_properties` is now idempotent — it skips any property
  column already present under the var's qualifier — fixing the regression test
  `return_star_after_with_forwarded_node` (which had been failing on `main`,
  uncaught because CI did not run the Rust integration tests).
- Accessing an absent property yields `null`, not a planning error (#598, Null1)
  — `n.missing` where `missing` is not among the columns the node's scan joined
  in now lowers to a `null` literal instead of a dangling `var_N.missing` column
  reference that DataFusion rejects. The lowerer (`ExprLowerer`, `PropertyAccess`)
  consults the node var's `NodeShape::prop_names` (the authoritative joined-column
  set) using the same name resolution `resolve_prop_col` uses. Topology columns
  (`node_uuid`, `type_id`, …) are exempt — they are always present under the var
  and `node_prop_cols` excludes them from `prop_names`, so e.g. `n.node_uuid`
  still resolves to the real identity column rather than being nulled. This is the
  Cypher semantic for missing properties and unblocks queries that sort/filter on
  an optional property — e.g. `MATCH (a:A) … ORDER BY a.name` where `name` is unset.
  The rewrite fires only when the property lists are AUTHORITATIVE (read from a
  backing dataset); schema-only / `explain` lowering has no dataset, so an empty
  property list means "unknown" there, not "absent", and the access stays an
  unresolved column reference rather than being nulled.
  Corpus passing 249 → 250 (`WithSkipLimit2 [1]`). (Null1's own scenarios also need
  expression-text result-column naming, a separate follow-up.)
- TCK conformance — advisory whole-corpus run with a set-based regression gate
  (#598/#608) — replaced per-scenario `@skip-rust` un-skipping with the v0.4.0
  run-up model: the BDD harness now runs **every** scenario of the vendored
  corpus (no filter) via cucumber `run()` (which does not exit on failure), and a
  custom writer records the **set** of scenarios that pass. That set is gated
  against the committed `tests/tck/passing_baseline.txt`: any baseline scenario
  that stops passing fails CI (a regression) — even if a _different_ scenario
  newly passes, so an XPASS cannot mask a regression (a count could). Newly
  passing scenarios are surfaced as a `TCK XPASS` warning and locked in by
  re-blessing (`BLESS_TCK_BASELINE=1 cargo test -p graphforge-api --test bdd`). Running
  the whole corpus immediately surfaced **249** passing (vs 25 previously
  un-skipped) — engine improvements now auto-surface instead of needing manual
  un-skips. Removes the obsolete tag-based `tck_coverage.rs` gate.
- Node equality + unlabelled-endpoint property join (#598) — `WHERE a = b` /
  `a <> b` over node variables now compares node identity (`node_uuid`) instead
  of erroring on the bare multi-column var qualifier; and an already-bound
  UNLABELLED endpoint (the `x` in `(n)-[r]->(x)`) now gets its properties joined
  (`_untyped` in exploratory mode), so `WHERE x.p = …` / `RETURN x.p` resolves.
  Un-skips `MatchWhere3` and `WithWhere3` entirely and `MatchWhere4 [1]`; corpus
  passing 18 → 25.
- TCK un-skip batch — null/boolean scalar scenarios (#598) — un-skipped the
  verified-passing scenarios of `Null3`, `Boolean1`, `Boolean2` (per-scenario;
  the null-truth-table, `IN`, and type-error scenarios stay `@skip-rust`).
  Corpus passing 9 → 18. Each was confirmed green by running, not predicted.
- `CREATE` of an anonymous path binds its edge endpoints (#598/#601) —
  `CREATE (:A)-[:REL]->(:B)` no longer errors `CREATE edge references unbound
dst var`. `lower_create` carried the same anonymous-destination bug fixed for
  MATCH in #895: the trailing anonymous node minted a fresh var instead of
  reusing the edge's `dst`, so the edge spec pointed at an uncreated node. It now
  reuses the dst var (`pending_dst`), matching the edge to the created node. This
  unblocks the many TCK setups that build graphs via anonymous `CREATE` paths.
- `RETURN *` projects all in-scope variables (#598 read-path) — `MATCH (a)-->(b)
RETURN *` now returns a column per named variable (`a`, `b`) instead of erroring
  (`variable \`_\` used before it was introduced`). The binder expands the `_`
item to the current named vars (sorted for a deterministic plan); each is
projected as before, so node vars materialize as whole node values. Combined
with WITH whole-node forwarding (#814 slice 2), the`MATCH (a) WITH a MATCH
  (a)-->(b) RETURN *` shape now works end-to-end. Named relationship/path vars
  would expand here but still fail downstream (rel/path values unsupported);
  anonymous pattern elements are not in scope and so are not returned.
- `WITH` forwards a whole node (#814, slice 2) — `MATCH (n) WITH n …` now carries
  the node through the scope reset instead of erroring. A node variable spans
  several columns (`var_<n>.node_uuid` + properties), so the lowerer forwards all
  of them under the var's qualifier (rather than projecting a single column), and
  the binder reuses the var + re-registers it as a node so a later `RETURN n`
  still materializes a whole node value and `n.x` resolves. Still deferred:
  renaming a node through `WITH` (`WITH n AS m`, needs column re-qualification)
  and forwarding a relationship/path — both rejected with a clear error.
- Multi-pattern MATCH + an anonymous-endpoint correctness fix (#598 read-path) —
  two related fixes:
  - **Comma-separated patterns are a cross product.** `MATCH (a), (b)` now
    Cartesian-joins the two matches instead of the second scan replacing the
    first (which dropped the earlier pattern's columns — `No field named
var_0`). A fresh node scan over a non-empty input cross-joins; a WHERE over
    both disconnected variables then filters the product.
  - **Anonymous relationship endpoints bind to the expansion.** A trailing
    anonymous node (`(:Person)-[:KNOWS]->(:Person)`) now reuses the `Expand`'s
    `dst` var instead of minting a fresh, dangling one. Previously that scan
    silently _replaced_ the expansion, so such a pattern counted Persons (5),
    not KNOWS edges (4) — a latent wrong-result bug the cross-product work
    surfaced. The trailing node now hits the bound-var path (label/property
    filter on the real `dst`).
- `WITH` (read-path projection) now lowers and executes (#814, slice 1) — a
  `MATCH … WITH … RETURN` pipeline projects/renames mid-query and chains, where
  before `GraphOp::With` hit "operator not yet lowered". The binder lowers each
  `WITH` item's expression in the current scope, mints an `out_var` for its
  alias, then **resets the scope** to exactly those aliases (Cypher semantics):
  a variable not carried forward is out of scope afterwards and referencing it is
  an error, not a silently-wrong result. An optional `WITH … WHERE` filters over
  the projected aliases; `ORDER BY`/`SKIP`/`LIMIT`/`DISTINCT` after `WITH` work
  via their existing ops. The lowerer projects each item to its alias column and
  registers the `out_var` in the var-map so downstream clauses resolve.
  Deliberately deferred (rejected with a clear error rather than mishandled):
  carrying a **whole node/path** through `WITH` (`WITH n` — a node spans multiple
  columns) and **aggregation in `WITH`** (`WITH count(*) …`); write-result
  `RETURN` and mid-statement reads-after-writes remain #814's later slices.
- Unlabelled `MATCH (n) RETURN n` materializes a whole node value (#889, slice 1)
  — building on #785 (which required the label in the pattern), a bare match with
  no label now returns the node with its **real label name and properties**, not
  a uuid-only struct. Two pieces: (1) the label is recovered at read time by
  resolving the node's stored `type_id` through a new `RuntimeCatalog`
  entity-type reverse map (mirroring `relation_type_name`), surfaced via
  `GraphCatalog::label_names` and emitted as a runtime `CASE type_id WHEN … THEN
'Label'` over the node's `type_id` column — so it works in exploratory mode
  where the ontology map is empty; (2) an unlabelled node scan now joins its
  properties from `properties/_untyped.parquet` (where all exploratory writes
  land), so the node value carries readable props. Property-table _routing_ stays
  ontology-only (the label reverse-map is kept separate) so a labelled
  exploratory match still reads from `_untyped`. One label per node — multi-label
  is #799; TCK rendering of node values + scenario un-skipping is #889 slice 2.
- `startNode(r)` / `endNode(r)` return the endpoint **node value** (#753) — over
  a matched fixed-hop relationship these now return the start/end node with
  identity, labels, and readable properties (reusing the #785 node-value
  materialization), not a bare UUID. The binder records each fixed hop's
  `(src, dst)` node vars at `Expand` and rewrites the calls onto them: as a node
  reference for property access (`startNode(r).name`) and, in a terminal RETURN,
  as a whole node value (`RETURN endNode(r)`). Endpoints follow the edge's
  **direction** — an incoming `(a)<-[r]-(b)` resolves `startNode(r)` to `b` — not
  the pattern's left/right order. Deliberately deferred (fall through to the
  usual `UnknownFunction` error rather than risk a wrong answer): variable-length
  edges (the var binds to a relationship _list_), undirected edges (orientation
  is per matched row), and `OPTIONAL MATCH` edges (an unmatched `r` is null;
  honoring `startNode(null) = null` needs an edge-uuid null gate — #889). Closes
  the #743 deferral.
- Neighborhood-proportional traversal latency (#838) — variable-length and
  single-hop expansion now read only the **reached destination** node records
  (`read_nodes_filtered`, an `node_id`-filtered read mirroring the #830 edge
  read) instead of scanning the whole node table per query, and topology Parquet
  is written in 64 K-row row groups so a localized k-hop traversal prunes by
  `edge_id`/`node_id` to ~one row group. The decode footprint of a localized
  traversal is now flat as the graph grows (measured: `*1..3` 10.1 ms at 1 M
  edges vs 12.4 ms at 10 M; see `benchmarks/traversal_scaling.md`), versus ~10×
  before. A traversal whose reached set is scattered across id-space remains
  scan-bound (the columnar-pushdown limit).
- Adjacency index baseline (M15, ADR 0005) — variable-length and single-hop
  traversal now serve from a persistent CSR index under `indexes/adjacency/`
  (a `{out,in}` CSR pair per relation plus an `_all` union, with a manifest
  stamping the `topology_generation` it was built from) instead of scanning the
  edge tables on every call. One `AdjacencyProvider` backs all consumers;
  `forge.index("adjacency")` builds/refreshes it, a stale or missing index
  falls back to scan-build (identical results, only slower), and the
  adjacency-aware lowering rule routes covered single-hop patterns to it with
  no Graph-IR change. `explain` reports `adjacency=hit|miss|building`. Edge
  records are read lazily on a Hit (`edge_id`-filtered, #830) and the provider
  is shared at the `GraphForge` facade with cheap per-session revalidation
  (#832), so repeat queries don't re-load the CSR. A differential corpus pins
  adjacency-backed traversal byte-for-byte against the join path. Maintenance:
  `graphforge_storage::adjacency::validate_adjacency_index` diffs a persisted index
  against a fresh rebuild (#766).
- Incremental adjacency rebuild (#765) — a pure-append topology commit now
  records a per-commit delta segment (`indexes/adjacency/deltas/<generation>.parquet`),
  and the persistent adjacency provider serves a fresh view as _base CSR ⊎ a
  contiguous delta chain_ without a full rebuild. Newly created edges are
  traversable immediately on an indexed graph; a DELETE (or a chain longer than 64) breaks the chain and falls back to the existing full rebuild, which now
  also prunes the segments it subsumes. Results are unchanged — the overlay is
  byte-identical to a full rebuild.
- Diagnostic I/O read counters `graphforge_storage::io_stats` (#767) — process-global
  `IoSnapshot` counting full vs `edge_id`-filtered edge reads and full node
  reads across the catalog-free direct readers (`read_edges`,
  `read_edges_filtered`, `read_nodes`). Advisory instrumentation (no behavior
  change) backing the M15 T1 proof that an adjacency-index Hit issues no full
  edge scan; the filtered-read >50% fallback is counted as a full read so it
  cannot masquerade as a cheap filtered read.
- Clause-ordered mixed-write statements (#792 Step 2) — one statement may now
  combine CREATE/DELETE/SET/REMOVE clauses (same or different kinds): a single
  read prefix runs once and each write clause applies in clause order against
  that frontier, extended with the variables CREATE clauses mint so later
  clauses target created entities like matched ones. openCypher interactions
  hold: a plain DELETE sees edges created **in-statement** (error without
  DETACH; DETACH cancels the pending edge before it ever hits disk),
  created-then-deleted entities never reach disk but count in both counters,
  SET/REMOVE on a created entity edits the write buffer, writing to a deleted
  entity errors, and repeated deletes are no-ops. A value expression reading a
  property written earlier in the statement is rejected loudly (cross-clause
  property visibility is a follow-up), as is a graph read after a write
  clause. Mixed statements return a
  one-row six-counter summary (`nodes_created`/`edges_created`/
  `nodes_deleted`/`edges_deleted`/`properties_set`/`properties_removed`);
  single-write statements were unified onto this path in #817. Bonus fixes:
  multiple same-kind clauses (two SETs, two
  CREATEs) used to pass the old guard and die in lowering — they now sequence,
  and two CREATE clauses no longer mint colliding `node_id` surrogates (one
  shared writer per statement).
- All-or-nothing Parquet writes (#790) — every write in graphforge-storage stages a
  sibling temp file and atomically renames it into place, so an I/O failure
  mid-write (disk full, encode error) can never leave a truncated file. The
  multi-file rewrites (DELETE across topology/property files, SET/REMOVE
  across stems, the CREATE append-merge flush, and a #792 mixed statement's
  whole effect set) stage into one `RewriteBatch` and commit once —
  `topology/nodes.parquet` renames last on the delete path so even a (rare)
  rename-phase failure leaves at worst orphaned-but-unreferenced rows, never
  a deleted node with surviving edges. Failures while building replacement
  files leave the prior on-disk state byte-identical; durability (fsync)
  remains out of scope for this non-production engine.
- Named path variables over variable-length patterns: `nodes(p)` /
  `relationships(p)` / `length(p)` (#754, first slice) —
  `MATCH p = (a)-[:KNOWS*1..2]->(b)` now binds
  `p` and the three path functions execute end-to-end. The binder records the
  path's constituent variables and rewrites the calls at bind time (`p` itself
  never reaches the plan): `relationships(p)` is the #709 relationship-list
  column verbatim, `length(p)` is its element count (the hop count; 0-hop → 0),
  and `nodes(p)` lowers to a new `cypher_path_nodes` UDF that recovers the
  traversal node sequence per row by walking the edge list from the start
  node's uuid (edges store src/dst in **storage** orientation, but every BFS
  emission is a connected walk, so each next node is deterministically the
  edge's other endpoint — correct for `In`/`Undirected` traversals and
  self-loops). `nodes(p)` returns `List<Struct{node_uuid}>`, mirroring the
  edge-list element shape so node properties/labels can be added later without
  changing the container kind. Fixed single-hop paths
  (`MATCH p = (a)-[:KNOWS]->(b)`, including an explicit `*1..1`) compose the
  same three functions from the join's scalar columns (`length(p)` → constant
  1, UInt64 like the var-length form; `nodes(p)` → the two endpoint structs in
  traversal order; `relationships(p)` → a one-element list carrying the
  topology fields with the bind-time `rel_type` — no edge properties yet, a
  documented gap vs the var-length list). Bare `RETURN p` returns one Arrow
  `Struct{nodes, relationships}` column per path (named `p` when unaliased),
  assembled at projection from the same expressions. All path values
  null-propagate: an unmatched `OPTIONAL MATCH` row's `p`, `nodes(p)`,
  `relationships(p)`, and `length(p)` are Cypher `null` (the fixed-hop forms
  gate on the edge's `edge_uuid`; the var-length forms inherit it from the
  list column — whose registration now survives `OPTIONAL MATCH`, fixing a
  latent var-map gap). **Deferred** (rejected with a clear message):
  multi-segment path patterns; edge-property parity for the fixed-segment
  relationship struct and node property/label augmentation are follow-ups.
- Property `SET` / `REMOVE` write execution with runtime-expression values (#791)
  — `SET n.prop = <expr>` and `REMOVE n.prop` now execute end-to-end for both node
  and edge targets (previously a bind error). The value is a **full runtime
  expression** evaluated per matched row (`SET n.age = n.age + 1`, `SET a.x = b.y`),
  not a baked literal: the lowerer turns it into a DataFusion `Expr` (resolving
  against the matched-row schema — the eager property join from #784/#789 makes
  `var_<n>.<prop>` available), and the new `GraphSetExec` evaluates it per batch via
  `create_physical_expr(...).evaluate(...)` (the `UnwindExec` pattern), converting
  each row's result to an `IrLiteral` (`scalar_to_ir_literal`). Writes rewrite the
  property files in place via new `graphforge-storage` primitives (`set_node_properties` /
  `remove_node_properties` / `set_edge_properties_rewrite` / `remove_edge_properties`)
  that reuse the writer's decode → merge → re-infer cycle — SET on a propertyless
  node mints a fresh row; REMOVE of an absent key/uuid is a no-op (openCypher). Node
  file stems resolve per row from `type_id` (entity name, or `_untyped` in
  Exploratory mode); edge stems from the row's `rel_type_name`. The #792 guard
  extends to SET/REMOVE (no mixing with CREATE; one rewrite kind per query for now),
  and `execute_stream` rejects them. **Deferred** (each rejected with a clear
  message): `SET n:Label` / `REMOVE n:Label` (needs a foundational multi-label
  storage model — a follow-up issue); `SET n += {…}` / `n = {…}` map merge/replace;
  SET/REMOVE on an untyped edge; mixed/multi-rewrite ordering; non-scalar stored
  values.
- Edge properties on variable-length relationship-list elements (#755) — `MATCH
(a)-[r:KNOWS*1..2]->(b) RETURN r[0].since` now reads the edge's persisted
  properties. The var-length edge-list struct
  (`List<Struct<{edge_uuid, src_uuid, dst_uuid, rel_type}>>`) gains the relation's
  property columns: the lowerer discovers them from `edge_properties/<REL>.parquet`
  (the same dynamic schema the fixed-hop `join_edge_properties` uses) and bakes the
  field list into the node schema (the single source of truth); `VarLenExpandExec`
  materialises the values by `take`-ing each property column at the row matching
  each hop's `edge_uuid` (NULL for an edge with no property row — LEFT-join
  semantics). The access lowering needed no change — `r[i].<prop>` already composes
  to `get_field(array_element(rels, fixup(i)), "<prop>")`. A wildcard (`*`) or a
  relation with no persisted properties keeps the topology-only 4-field struct
  (byte-identical — no plan-shape change). `r[i].nonexistent_prop` remains a plan
  error (not openCypher NULL) — out of scope.
- Reject mixing CREATE/MERGE with DELETE in one statement (#792, step 1) — a query
  that combines a buffered-append write (`CREATE`/`MERGE`) with an in-place rewrite
  write (`DELETE`), e.g. `MATCH (a) CREATE (b) DELETE a`, was silently mis-handled:
  the graphforge-api write router classified the plan as a single write kind and ran only
  the DELETE side, dropping the CREATE. It now returns a clear `GfError::Validation`
  ("a single query may not mix CREATE/MERGE with DELETE yet; run them as separate
  statements") — a guard at the top of `run_plan`. Neither side runs on rejection;
  unmixed CREATE-only / MERGE-only / DELETE-only statements are unaffected. Proper
  clause-ordered sequencing of mixed read+write remains the larger follow-up (#792
  step 2).
- Inline relationship-property predicates filter in MATCH (#750) — `MATCH
(a)-[r:KNOWS {since:2020}]->(b)` now matches only edges with `since = 2020`
  (previously the inline map was silently dropped and every KNOWS edge matched).
  The binder lowers the relationship's inline property map to AND-ed equality
  predicates and emits a `GraphOp::Filter` after the `Expand` — the relationship
  analogue of the node case (#748), reusing the same variable-generic
  `lower_inline_property_filter`. The read side already materialises
  `var_<rel>.<prop>` for a fixed hop (#784's `join_edge_properties`), so this is a
  binder-only change. Guarded to fixed hops: a variable-length edge var binds to a
  _list_ column, not scalar properties, so inline props there stay a no-op (that's
  #755). No logical-plan goldens change.
- Properties on a fixed-expand destination node now resolve (#789) — `MATCH
(a:Person)-[:KNOWS]->(b:Person {name:'Bob'}) RETURN a.node_uuid` (and
  `WHERE b.age > 28`, `RETURN b.name`, `ORDER BY b.x`) previously failed to plan
  with "No field named var_N.name": the destination of a fixed single-hop expand
  was lowered as an already-bound `NodeScan` that filtered by label but never
  joined its property table, so `var_<dst>.<prop>` was never materialised (only
  the **leading** node's properties were joined, #748/#704). `join_node_properties`
  is now **append-preserving** — it passes every existing input column through (so
  the source + edge columns of the joined Expand plan survive) and appends the
  destination's re-qualified property columns — and the already-bound `NodeScan`
  arm calls it after the type filter. Fixes the whole class of destination-property
  access, not only inline `{prop:val}` filters. No logical-plan goldens shift (the
  optimizer prunes the property join when no destination property is referenced).
- DELETE / DETACH DELETE execution (#740) — `MATCH (n) DELETE n` and
  `DETACH DELETE` now run end-to-end. New `GraphDeleteExec` drains the matched
  rows, collects the target UUIDs (node vs edge by the resolved kind), and
  rewrites the affected Parquet files via the storage mutator. openCypher
  semantics are enforced: a node may be deleted **without** `DETACH` only if every
  relationship still incident to it is also deleted by the same statement — so
  `MATCH (a)-[r]->(b) DELETE r, a` is legal, while leaving an untargeted edge on a
  deleted node raises an ExecutionError ("Cannot delete node, because it still has
  relationships…"); `DETACH DELETE` folds the node's incident edges into the
  delete set and removes them too. `DELETE` of a NULL (e.g. an unmatched
  `OPTIONAL MATCH` row) is a no-op per openCypher, not an error. Wired through the
  `ExtensionPlanner` dispatch, a new `ExecutionSession::execute_delete`, and the
  graphforge-api write router (`GraphOp::Delete` → the write path; rejected on the
  streaming path like other writes). The `errors.feature` no-DETACH scenario now
  runs against a real connected graph (its `given` step built an empty forge
  before). `SET`/`REMOVE` remain deferred (still reject at bind).
- DELETE — logical node + write-gated lowering (#740) — new `GraphDeleteNode`
  (graphforge-plan) mirroring `GraphCreateNode`: an input-driven write node carrying the
  resolved `DeleteTarget`s (var + node/edge kind) and the `detach` flag, emitting
  a one-row `nodes_deleted`/`edges_deleted` summary. graphforge-rel lowers `GraphOp::Delete`
  beside `Create`, gated on the write target (`new_for_writes`) so a read-only
  session still rejects it. Each target's node-vs-edge kind is resolved from the
  input plan's schema (`var_<n>.node_uuid` vs `var_<n>.edge_uuid`), so the
  executor reads the right identity column. Physical execution follows.
- DELETE — IR + binder lowering (#740) — `DELETE`/`DETACH DELETE` now lower to a
  new `GraphOp::Delete { vars, detach }` instead of failing at bind time with
  `UnsupportedClause`. The binder resolves each delete target to a bound variable
  (node or edge); a non-variable target (`DELETE n.prop`) is a new
  `InvalidDeleteTarget` bind error and an undeclared target stays
  `UndeclaredVariable` — only `DELETE <var>` is valid per openCypher. Whether a
  target is a node or edge is resolved downstream at execution time. (`SET`/
  `REMOVE`/`CALL` still reject at bind for now.) Physical execution follows.
- DELETE — storage rewrite primitive (#740) — new `graphforge-storage::mutator` module
  with `delete_nodes`, `delete_edges`, and `incident_edge_uuids`. Unlike the
  append-only `GraphWriter`, these **rewrite** committed Parquet files in place:
  read the current rows, drop the targeted ones (by `node_uuid`/`edge_uuid` via a
  keep-mask + `filter_record_batch`), and write the survivors back; an untouched
  file is never rewritten. `delete_nodes` also drops the deleted nodes' rows from
  `properties/*.parquet` (and `delete_edges` from `edge_properties/*.parquet`).
  `incident_edge_uuids` maps target node UUIDs → surrogate `node_id`s and scans
  the edge files for edges touching them (either endpoint) — the basis for the
  openCypher "no relationships without DETACH" check and for DETACH's incident-
  edge cleanup. The binder/lowering/exec wiring lands in follow-ups.
- Edge-property persistence — write path (#784) — `CREATE (a)-[:KNOWS {since:
2020}]->(b)` now persists edge properties instead of erroring with "CREATE edge
  properties are not yet supported". `GraphWriter::set_edge_properties(edge_uuid,
rel_type, props)` mirrors the node-property path but is keyed by `edge_uuid` and
  written to a dedicated `edge_properties/REL_TYPE.parquet` directory (routed by
  relation name in every mode, so a relation type never collides with a same-named
  node label under `properties/`). The dynamic per-stem schema inference
  (first-seen column order, first-non-null type, mixed-type→Utf8 coercion,
  null-first inference) and the read-merge-rewrite append cycle are shared with
  the node path; the CREATE executor mints the edge UUID and persists the buffered
  props. On the read side, a fixed single-hop expand LEFT-joins the edge scan with
  `edge_properties/<rel>.parquet` on `edge_uuid` (new `EdgePropertyTable` provider,
  mirroring `PropertyTable`), so `MATCH (a)-[r:KNOWS]->(b) RETURN r.since` resolves
  the property to a real column — the edge analogue of inline node-property reads.
  (The variable-length edge-list var `r` in `[r:KNOWS*1..2]` still carries only
  topology, no properties — that's #755.) Two hardening guards: a CREATE edge that
  carries properties but no relation type is rejected (its props would route to
  `_untyped` where the relation-name-keyed read can never reach them); and a
  property whose name collides with a topology column (`created_at`, `src_id`, …)
  is dropped from the join rather than building a duplicate-qualified field that
  breaks the MATCH plan (the topology column stays authoritative; symmetric guard
  added to the node-property join).
- Relationship-list access on the variable-length edge var (#743) — list/
  relationship functions now work on `r` (the `List<Struct>` edge list from
  #709), lowered to DataFusion in `resolve_builtin`: **indexing** `r[i]`
  (0-based, negative-from-end, null on out-of-range — via `array_element` with a
  `CASE` 0→1-based fixup), **slicing** `r[i..j]`/`r[i..]`/`r[..j]` (0-based,
  end-exclusive, null-unbounded → DataFusion's 1-based inclusive `array_slice`,
  with the unbounded end cast to `Int64`), `head(r)`/`last(r)`, **`type(r[i])`**
  (reads the element's `rel_type`), and **`size()`** as a new polymorphic
  `cypher_size` scalar UDF that dispatches on the argument's runtime type (list
  element count _or_ string char count) rather than mis-mapping `size("str")` to
  `array_length`. These also work on plain list literals (`[10,20,30][−1]`,
  `size('hello')`). Rust-core parity with the Python reference's list ops.
  Deferred (correctness-blocked, follow-ups filed): `startNode`/`endNode`
  (#753, need node materialization), `relationships(path)`/`nodes(path)` (#754,
  need named path variables), `r[i].<prop>` (#755, need edge-property
  persistence).
- CREATE streams its input instead of materializing it (#747) — `GraphCreateExec`
  now drains its child plan batch-by-batch (via DataFusion's partition-coalescing
  `execute_stream`) and writes each batch as it arrives, instead of `collect`-ing
  the whole `MATCH`/`UNWIND` frontier first. A large mixed `MATCH … CREATE` no
  longer materializes the full frontier in memory. The single `GraphWriter` is
  opened once and flushed once, so per-row reference/mint semantics and the write
  summary are unchanged. (`VarLenExpand`/`OptionalMatch`/`Unwind` still collect —
  their kernels genuinely need the full input.)
- Inline node-property predicates filter in MATCH (#748) — `MATCH (a:Person
{name:'Alice'})` now matches only Persons named Alice (previously the inline
  map was silently dropped and every Person matched). The binder lowers the
  property map to AND-ed equality predicates (`a.k = v AND …`, keys sorted for a
  stable plan) and emits a `GraphOp::Filter` after the node scan — the same shape
  a `WHERE` clause produces. Inline **relationship** properties
  (`-[r:KNOWS {since:2020}]->`) remain deferred (#750) until edge properties are
  persisted.
- Mixed `MATCH … CREATE` runs the write per matched row (#703) — `MATCH (a:Person)
CREATE (a)-[:KNOWS]->(b:Person)` now creates one new `b` + one edge **per
  matched `a`**, referencing the matched node's identity rather than minting a
  duplicate (previously the MATCH was silently discarded and the CREATE ran
  once). `GraphCreateNode`/`GraphCreateExec` are now input-driven: the executor
  iterates input rows, reads MATCH-bound vars' `node_uuid`/`node_id` from the
  row (via `GraphWriter::register_existing_node`) and mints fresh nodes/edges
  per row; the binder marks MATCH-bound CREATE vars as references
  (`CreateNodeSpec::is_reference`) and the lowerer folds the preceding pipeline
  in as the create node's input. A standalone `CREATE` is driven by the implicit
  single unit row (creates once, unchanged); an empty MATCH creates nothing.
  RETURN-after-CREATE, edge properties, and input streaming (vs `collect`, #747)
  remain deferred.
- Variable-length edge variable binds to the relationship list (#709) — the edge
  var `r` in `(a)-[r:KNOWS*1..3]->(b)` now binds to the openCypher _list of
  relationships_ along each path, surfaced as a forward-stable Arrow
  `List<Struct<{edge_uuid, src_uuid, dst_uuid, rel_type}>>` column (public UUID
  identity only — no surrogate ids leak). `VarLenExpandExec`'s BFS now tracks the
  ordered per-path edge sequence (not just a dedup set) and emits it as the
  trailing list column; the lowerer registers the edge var and `length(r)` lowers
  to DataFusion `array_length` (so `MATCH (a)-[r:KNOWS*1..2]->(b) RETURN
length(r)` returns the hop count per path). `size()` stays deferred (string vs
  list polymorphism). Richer access (indexing `r[i]`, `type(r[i])`,
  `relationships(path)`, edge properties) is a follow-up (#743) that needs no
  column-type change.
- Reject unimplemented writes + invalid CREATE patterns (#724) — two openCypher
  semantics that previously "passed" only because `execute` was a stub now fail
  loudly. A CREATE relationship with a type disjunction
  (`CREATE (a)-[:KNOWS|LIKES]->(b)`) is rejected as a `ParseError` (a created
  relationship must name exactly one type; the disjunction is only valid in
  MATCH, which is unchanged). Unimplemented write/side-effecting clauses —
  `DELETE`, `SET`, `REMOVE`, `CALL` — now surface an explicit bind error
  (`GfError::Plan`) instead of the binder's previous silent no-op (which made
  `… DELETE n` report success while doing nothing). The binder's catch-all arm
  is now an error rather than a no-op, so any future clause must be wired in
  deliberately. Full `DELETE`/`DETACH DELETE` (and `SET`/`REMOVE`) execution
  semantics are deferred to a follow-up.
- Streaming results + cross-session catalog persistence (#725) — new
  `GraphForge::execute_stream`/`execute_stream_with_params` return a
  `SendableRecordBatchStream` (backed by `ExecutionSession::execute_plan_stream`)
  with the _same_ UUID-only output shaping + schema metadata as `execute`, so
  the schema is available before the first batch is pulled (the
  `RecordBatchReader` contract the bindings rely on, #587). Writes are rejected
  (`GfError::Validation`) — only reads stream. A `GraphForge` now owns a
  long-lived multi-thread Tokio runtime so a stream's background tasks
  (repartition/coalesce) are not cancelled when the call that built it returns;
  it shuts down in the background on drop, so dropping an instance inside an
  async context (e.g. the BDD harness) no longer panics. The write path now
  flushes the shared `RuntimeCatalog` to `topology/runtime_catalog.parquet`, so
  a later `GraphForge::new(path)` reloads observed labels/relation-types/
  properties across sessions.
- Query parameters + e2e gate complete (#584) — `$name` placeholders are now
  bound to values: `execute_with_params("… WHERE n.age > $min …", {min: 28})`
  substitutes the placeholder via DataFusion's `LogicalPlan::with_param_values`
  before physical planning. `ir_literal_to_scalar` (graphforge-rel) converts
  `IrLiteral` → `ScalarValue` (one source of truth shared with literal
  lowering); the `Parameter` placeholder id now carries the `$` so DataFusion's
  named-param binding resolves it. New `ExecutionSession::execute_plan_with_params`
  (reads only; writes carry no placeholders). A missing parameter value errors
  clearly. This was the last open scenario of the #584 end-to-end gate — the
  full parse → bind → lower → execute → Arrow pipeline is now exercised
  end-to-end through the public API, closing the #583 Execution-Baseline epic.
- Incremental writes (#733) — `GraphWriter::flush` now **appends/merges** with
  existing on-disk data instead of overwriting, so separate
  `GraphForge::execute("CREATE …")` calls accumulate (e.g. CREATE A,B then
  CREATE C → 3 Person). `open` seeds surrogate `node_id`/`edge_id` from the
  on-disk maximum (monotonic across sessions); node/edge files concat their new
  rows with the existing ones; property files are decoded back to literals and
  re-inferred so the dynamic schema evolves across flushes (new columns,
  cross-flush type widening to `Utf8`, leading-null columns) using the same rules
  as a single flush. No cross-session dedup (pure `CREATE` mints fresh UUIDs;
  MATCH…CREATE upsert is #703). The merge is not atomic (acceptable for this
  small-graph engine). Unblocks the #584 incremental-CREATE scenario.
- End-to-end execution baseline tests (#584) — a `graphforge-api` integration suite drives
  the full pipeline (parse → bind → lower → execute → Arrow) through the public
  `GraphForge::execute` against a fixture graph (5 Person, 4 KNOWS, 1 LIKES).
  Covers simple scans, `WHERE`, `count()`, single-/two-hop and variable-length
  traversal, `OPTIONAL MATCH`, `UNWIND`, `ORDER BY`/`LIMIT`, and the Arrow result
  contract (FixedSizeBinary(16) `node_uuid`, no surrogate `node_id`/`edge_id`,
  query metadata, unique `query_id`). Parameter injection (`$min`) and
  incremental `CREATE` (the writer overwrites on flush — #733) remain deferred.
  Milestone 13 read-path exit gate.
- `count()` aggregation in RETURN (#729) — `RETURN count(n)` / `count(*)` now work.
  The binder detects top-level aggregate calls (`count`/`sum`/`avg`/`min`/`max`/
  `collect`) in a RETURN and emits a `GraphOp::Aggregate` (non-aggregate items
  become group-by keys) instead of a `Project` of an unknown function call.
  `count(*)` and `count(<node/edge var>)` count bound rows (the var is always
  bound in a MATCH), avoiding a reference to the non-existent bare `var_N`
  column; `count(DISTINCT expr)` maps to a distinct count. Part of the #583
  read-path epic.
- Exploratory traversal + OPTIONAL projection (#728, #730) — relationship queries
  now work end-to-end without an ontology. `TypedEdgeScan`/`EdgeScan`/var-length
  `Expand` resolve relation-type names from the `RuntimeCatalog` (exposed via
  `GraphCatalog::rel_names()`), and `CREATE` write-lowering does the same so an
  exploratory edge is written to `_exploratory.parquet` with its real
  `rel_type_name` (previously `_UNKNOWN`, which no `MATCH ()-[:REL]->()` filter
  matched). The catalog no longer registers a typed `edges_<rel>` table for a
  runtime relation unless its Parquet file actually exists on disk, so the read
  path correctly routes exploratory edges through the `_exploratory` catch-all.
  `OPTIONAL MATCH` now registers the optional-side variables it binds, so
  `RETURN m.x` over the optional side resolves (was “unbound variable”). Part of
  the #583 read-path epic; surfaced by the #584 e2e work.
- List literals (#714) — `IrExpr::ListLiteral` now lowers instead of erroring, so
  `UNWIND [1, 2, 3] AS x` and `UNWIND [] AS x` work end-to-end. All-constant
  lists fold to a `ScalarValue::List` literal (the form `UnwindExec` consumes);
  lists with expression elements lower to a `make_array(...)` call. Also fixed
  the lowering base: a source-free pipeline (`UNWIND […]`, `RETURN 1`) now starts
  from the single relational unit row rather than a zero-row relation, so it
  produces output (a pipeline with a scan still starts empty — the scan supplies
  its own rows). Part of the #583 read-path epic.
- Public `GraphForge::execute` (#719) — the facade is wired into the real
  parse → bind → lower → execute pipeline and returns an Arrow-backed
  `ExecutionResult`. `GraphForge::new(Some(path))` loads `graphforge.yaml` +
  `ontology.yaml` (resolving the `OntologyMode`) and seeds the `RuntimeCatalog`;
  `new(None)` is an in-memory exploratory instance over a temp dir. `execute`
  routes `CREATE` to the write path and reads to `execute_plan`, sharing one
  `RuntimeCatalog` across queries. Results expose UUID identity columns
  (`node_uuid`/`edge_uuid`) — never the surrogate `node_id`/`edge_id` — and carry
  schema metadata (`graphforge.query_id`, `ir_version`, `ontology_mode`, and
  `ontology_version` when present). Added `ontology_mode()` / `runtime_catalog()`
  accessors and empty-query validation. Closes the #583 read-path epic's facade
  PR; `execute_stream` + runtime-catalog persistence are tracked in #725, and
  unimplemented write-clause validation (DELETE, multi-type CREATE) in #724.
- Property-table JOINs (#704) — `RETURN n.name` now reads real property values.
  A labelled node scan LEFT-joins its property table (`properties/<Entity>.parquet`
  in strict/advisory mode, `properties/_untyped.parquet` in exploratory) on
  `node_uuid`, projecting each property column re-qualified under the node's
  `var_N` so `PropertyAccess` resolves to the joined column. `PropertyAccess`
  IDs resolve to names via the `RuntimeCatalog` (the binder's `PropId` space),
  surfaced through `GraphCatalog::prop_names()`; the catalog now also registers
  the exploratory `properties__untyped` table with its on-disk schema. Part of
  the #583 read-path epic.
- Fixed single-hop joins (#718) — a fixed `(a)-[:R]->(b)` pattern now lowers to a
  connected relational join instead of disconnected scans. The binder emits a
  single `Expand { min_hops: 1, max_hops: Some(1) }` (the only op carrying the
  src/dst node vars) for fixed hops, which the lowerer routes to the existing
  two-join chain; variable-length hops are unchanged. A destination label
  (`(b:Label)`) is now applied as a filter on the already-bound node rather than
  silently dropped (this also fixes the var-length destination label). Surfaced
  and fixed a latent `OPTIONAL MATCH` schema bug: now that the optional child is
  a real join it re-binds shared variables, so the node excludes **all** of a
  shared variable's columns from the inner output (not just the join key),
  avoiding duplicate `var_<shared>` fields. Part of the #583 read-path epic.
- General read execution (#717) — `ExecutionSession::execute_plan` is wired
  (lower → physical plan → collect), and read scans now bind their real
  Parquet-backed catalog providers (`TopologyNodeTable`/`TypedEdgeTable`) instead
  of a schema-only source, so a `MATCH (n:Label)` reads the rows a prior `CREATE`
  wrote. (Schema-only lowering is retained for explain/golden paths that have no
  project directory.) Part of the #583 read-path epic.
- New `graphforge-api` crate (#716) — the public `GraphForge` engine facade now lives in
  a top-level `graphforge-api` crate (depends on the full pipeline: graphforge-cypher/graphforge-ir/
  graphforge-rel/graphforge-exec/graphforge-storage/graphforge-ontology). It was relocated out of `graphforge-core`,
  which is the foundation crate every other crate depends on and therefore can't
  host the facade without a dependency cycle. `graphforge-cli` and the language bindings
  now depend on `graphforge-api`; the BDD API/TCK harness moved with it. (First step of
  the #583 read-path epic; facade methods remain stubs until #717–#719.)
- `UNWIND` **execution** (#582) — `UnwindExec` evaluates the list expression per
  input row and explodes it into one output row per element (input columns + the
  element column), dropping rows whose list is null or empty (openCypher
  semantics). Routed from the `UnwindNode` Extension by the query planner. The
  full `UNWIND … RETURN` round-trip and `UNWIND [literal]` lowering await #583 /
  list-literal lowering.
- `UNWIND` schema plumbing (#582, foundation) — `UnwindNode` now declares an
  output schema that extends the input with the unwound element column (named
  after the alias, unqualified, nullable, so a bare `RETURN x` resolves). The
  lowerer resolves the element type from the list expression.
- `OPTIONAL MATCH` **execution** (#581) — `OptionalMatchExec` left-joins the
  outer input against the optional sub-plan on the shared-variable join keys,
  preserving every outer row and setting the optional-side columns to **null**
  when there is no match (openCypher null-shaping, via `take` with null indices).
  Handles fan-out (one outer → many inner), multi-key tuples, empty inner, and
  the unconditional (no-key) case; routed from the `OptionalMatchNode` Extension
  by the query planner.
- `OPTIONAL MATCH` join-key plumbing (#581, foundation) — `OptionalMatchNode`
  now carries the shared-variable `join_keys` and declares an output schema that
  extends the outer columns with the optional sub-plan's columns made
  **nullable** (openCypher null-shaping). The lowerer computes the shared
  variables (outer ∩ optional) as join keys. Physical execution lands in a
  follow-up PR; non-empty join keys for real `MATCH (a)-[:R]->(b)` queries await
  fixed single-hop join lowering (#583).
- Variable-length `Expand` **execution** (#580) — `VarLenExpandExec` performs an
  iterative BFS over the edge table for patterns like `(a)-[:KNOWS*1..3]->(b)`,
  with openCypher relationship-isomorphism (no edge reused within a path, so
  unbounded `*` terminates on cycles). Handles `Out`/`In`/`Undirected`, bounded
  and unbounded ranges, and is routed from the `VarLenExpandNode` Extension by
  the query planner. The edge variable `r` (relationship list) is not yet bound
  — deferred to a follow-up issue.
- Variable-length `Expand` lowering (#580, foundation) — `VarLenExpandNode` now
  carries the source/destination vars, direction, relation type, and baked
  project dir/mode, and declares an output schema that extends the input with
  the destination node's columns (so a downstream `RETURN b.node_id` resolves).
  Physical BFS execution and the edge-variable (relationship-list) binding land
  in follow-up PRs.
- Catalog-free edge/node readers in `graphforge-storage` (#580) — `read_edges(dir,
rel_name, mode)` and `read_nodes(dir)` read the topology Parquet files
  directly, mirroring the `GraphWriter` layout (typed vs `_exploratory`). Lets
  physical execution nodes (e.g. the upcoming `VarLenExpandExec`) read edges
  without a `GraphCatalog`, which the DataFusion `TaskContext` does not expose.
- LALRPOP parser replacing Python LALR(1) executor
- DataFusion-backed execution engine with Arrow result streams
- `execute_arrow()` returning `pyarrow.Table` via Arrow C Data Interface
- `execute_stream()` returning `pyarrow.RecordBatchReader`
- Parquet storage provider (replaces SQLite in the Rust core)
- PyO3 + maturin Python binding (`graphforge-bindings-py`)
- napi-rs Node binding with Arrow IPC (`graphforge-bindings-node`)
- See [ADR 0001](../adr/0001-rust-core.md) and [roadmap](../releases/roadmap.md)

## [0.4.0] - 2026-05-07

### Added

- **`db.gds` graph algorithm surface** (#484, PRs #514–#516) — 8 compiled algorithms dispatched to igraph (preferred) or NetworkX: `pagerank`, `betweenness_centrality`, `closeness_centrality`, `degree_centrality`, `louvain`, `connected_components`, `clustering_coefficient`, `triangle_count`. Stream mode returns `dict[node_id, scalar]`; write mode persists results via `set_node_properties()`. Optional `node_label`, `rel_type`, `directed` filters.
- **`db.search` hybrid retrieval surface** (#476, PRs #517 #533–#539) — FTS5 text search (`db.search.text()`), vector cosine similarity (`db.search.vector()`), and RRF-fused hybrid (`db.search()` / `db.search.hybrid()`). All methods return `list[SearchHit]` with `.ref`, `.score`, `.sources`, `.text_score`, `.vector_score`. Multi-space vector storage: `set_node_vector(id, vec, space="model-name")`. Bring-your-own-vectors; `space` can represent text embeddings, geo coordinates, temporal features, or any numeric feature array.
- **`SearchHit` result type** — frozen dataclass with score provenance; every `.ref.id` is directly addressable in `db.execute()`.
- **`set_node_properties()` bulk write-back** (#480, PR #515) — batch-update node properties from a `dict[node_id, dict[prop, value]]`.
- **`directed` and `node_id_property` export parameters** (#478 #479, PR #514) — `to_networkx(directed=True)`, `to_igraph(node_id_property="name")`, etc.
- **`graphforge.recipes.neighbourhood()`** (#492, PR #518) — n-hop subgraph expansion returning `list[dict]` for LLM context building.
- **Benchmark suite** (#388, PR #519) — `make benchmark` runs real-dataset benchmarks through M-tier (XS/S/M/L).
- **Integration tests for three-surface interoperability** (PR #541) — GDS write-back readable via Cypher, search results addressable via `id()`, cross-surface PageRank threshold queries.
- **Use-case documentation** (PR #542, closes #523 #524 #525 #526) — `db.gds` examples in `network-analysis.md`; `db.search` + `recipes` in `llm-workflows.md`; deduplication pattern in `knowledge-graph-construction.md`; hybrid tool selection in `agent-grounding.md`.

## [0.3.10] - 2026-05-06

### Added

- **Schema introspection API** (#469, #499) — `labels()`, `relationship_types()`, `node_count(label=None)`, `relationship_count(type=None)` on `GraphForge`; efficient set-union over storage backend
- **JSON export/import** (#470, #500) — `gf.to_json(path, metadata, indent)` serialises the full graph to a portable JSON file; `GraphForge.from_json(path)` reconstructs it; Hypothesis roundtrip property test
- **`merge_node()` safe upsert** (#471, #501) — `merge_node(labels, match_on, on_create, on_match)` with regex label validation, index-safe on_match updates, and create/match semantics matching MERGE clause behaviour
- **`add_graph_documents()` LangChain-compatible ingestion** (#472, #502) — duck-typed batch ingestion accepting any object with `.nodes`/`.relationships` attributes; idempotent edges; rel type pre-check; no `langchain-community` import required
- **Parse/plan LRU cache** (#464, #504) — `GraphForge(cache_size=N)` caches parsed+planned query pipelines; `clear_cache()` and `cache_info()` for introspection; thread-safe; default size 128

### Fixed

- **ORDER BY on aliased RETURN DISTINCT property no longer raises UndefinedVariable** (#481, #494) — planner now records `(variable, property)` paths for aliased `PropertyAccess` return items, so `RETURN DISTINCT a.year AS year ORDER BY a.year` resolves correctly
- **Variable reuse across WITH boundary no longer raises KeyError** (#482, #495) — optimizer's redundant-traversal-elimination pass now treats `Aggregate` as a scope boundary (same as `With`/`Union`/`Subquery`), preventing fresh post-WITH variable bindings from being silently dropped
- **EXISTS {} subquery crash on anonymous inner node with outer-scope property** (#474, #496) — `_plan_match` now passes `var_name` (always set, from pattern variable or generated) instead of `node_pattern.variable` (can be None) to `_properties_to_predicate`
- **`shortestPath()` / `allShortestPaths()` parse correctly and raise NotImplementedError** (#468, #497) — previously caused unhelpful `SyntaxError: Unexpected token`; now parsed into `ShortestPathExpression` AST nodes; planner raises `NotImplementedError` with BFS workaround: `MATCH path = (a)-[*1..N]-(b) RETURN length(path) ORDER BY length(path) LIMIT 1`

## [0.3.9] - 2026-05-04

### Added

- **LALR(1) parser migration** (#365, #430) — grammar refactored to LALR(1); linear-time parsing replaces Earley; unknown function names now detected at parse time; 586 new parser tests; OOM-safe local coverage via sharded `make coverage`
- **Property equality index for O(1) node lookup** (#390, #427) — in-memory `_prop_index` (prop → value → {node\_id}) on `Graph`; `get_nodes_by_property` and `get_nodes_by_label_and_property` methods; executor `_extract_equality_hints` replaces full scan with index lookup when a `ScanNodes` is followed by a simple equality `Filter`; index maintained on `SET`/`REMOVE`/`DELETE`
- **Bulk ingestion API** (#389, #444) — `create_node_bulk`, `create_relationship_bulk` (skip per-call Pydantic validation); `bulk_ingest()` context manager defers statistics rebuilds until exit via `_flush_deferred_stats()`; CSV loader uses bulk path; significant throughput improvement for dataset loading
- **Real-dataset QA and performance profiling suite** (#417, #419) — 5-tier perf suite (karate 34n, ego-facebook 4k, amazon 334k, livejournal 4M, orkut 117M edges); `scripts/perf_report.py` renders baseline/delta Markdown tables; `make test-perf`, `make test-perf-xs`, `make test-perf-large`, `make test-perf-slow` targets; L/XL tier benchmarks added; `perf_slow` marker applied consistently (#440, #441)
- **`elementId()` scalar function** (#211, #447) — GQL-spec `elementId(node|rel)` returns the element id as `CypherString`; `id()` continues to return `CypherInt`; NULL-safe; arity-checked
- **Scale limits documentation** — `docs/reference/scale-limits.md` explains LIMIT-respecting (~20M edges) vs full-scan (~1M edges) performance ceilings; README graph-size claim updated to reflect edge-count as binding constraint (#443)
- **`make pre-push-fast`** (#429, #437) — fast-fail pre-push shortcut (format, lint, type, security, docstrings; no coverage); inline coverage thresholds (85% total, 90% patch)

### Fixed

- **Fixed-length hop ignores rel predicate** (#431, #439) — fixed-length traversal now evaluates `rel_pattern.predicate` inline; parser predicate whitelist replaced with open accept
- **var-length relationship predicates** (#381, #425) — `_var_length_reachable` now evaluates inline relationship predicates (e.g. `[*1..3 {weight: 1}]`) at each hop instead of ignoring them
- **Cross-hop edge uniqueness in multi-segment patterns** (#382, #425) — `initial_used_edge_ids` seeds the visited-edge set from prior hops, preventing the same edge being traversed twice across pattern segments
- **SQLite delete persistence** (#409, #421) — deleted nodes and edges now removed from SQLite backend; previously they resurrected on reload
- **SQLite bulk I/O** (#392, #421) — `save_nodes_bulk` / `save_edges_bulk` use `executemany`; `_save_graph_to_backend` wrapped in rollback; resolves >20s hang on M-tier reload
- **`sys.setrecursionlimit` thread safety** (#433, #445) — limit set once in `CypherParser.__init__` instead of per-request get/set/restore, eliminating races in concurrent parser use
- **Transaction rollback restores ID counters** (#412) — `rollback()` restores `_next_node_id` and `_next_edge_id`, preventing ID gaps after aborted transactions
- **Closed-instance guard** (#411) — `execute()` and mutation methods raise `RuntimeError` after `close()`
- **`DatasetInfo` rejects `ftp://` URLs** (#414)
- **TCK metrics CI gate** (#410) — `tck_metrics.py` exits non-zero on TCK failures

### Performance

- **UNWIND + WITH LIMIT short-circuit** (#393, #443) — `_pipeline_limit` reverse-scan treats pass-through `With(limit_count=N, no sort, no filter)` as transparent; `_execute_unwind` respects `row_limit` and exits early; `UNWIND range(1M, 2M) AS i WITH i LIMIT 3000 RETURN sum(i)` drops from ~4.5s to <10ms
- **LIMIT short-circuit for scan and expand operators** (#422, #423) — `_pipeline_limit` look-ahead stops scan/expand at row budget; write operators and Filter block short-circuit to preserve correctness
- **SQLite PRAGMA tuning** (#392, #446) — `synchronous=NORMAL` (eliminates `fdatasync` per commit), 64 MB page cache (`cache_size=-65536`), `temp_store=MEMORY`; safe with WAL mode for analytical workloads
- **Lazy statistics timestamp** — `_stats_last_updated` stamped at read time in `get_statistics()` rather than on every add; `_defer_stats` flag eliminates per-edge stat overhead during bulk ingest

### CI / Tooling

- **Test suite zero-base renovation** (#428, #438) — fixture strategy audit, duplication removal, marker consistency, parametrization improvements across unit and integration suites
- **GHA actions updated** — `actions/upload-artifact` v7, `actions/download-artifact` v8, `codecov/codecov-action` v6, `actions/upload-pages-artifact` v5, `astral-sh/setup-uv` v8.1.0

## [0.3.8] - 2026-05-02

### Fixed - TCK Feature Completeness (3885/3885 passing)

- **Nanosecond precision in temporal types** — `CypherDateTime` and `CypherTime` now store a `_ns_residue` (0–999) for sub-microsecond precision; `_components` tuple extended to 8 elements with nanoseconds; construction, accessors, arithmetic, comparison, and serialization all updated (#224)
- **Statement clock caching** — `now()`, `datetime()`, `date()`, `time()` (without arguments) now return consistent values within a single query by caching the start time via `time.time_ns()` (#224)
- **Extreme year support** — dates/datetimes outside Python's 1–9999 range now handled via `_WideDate` / `_WideDateTime` classes; durations too large for `timedelta` use `_BigDuration`; Julian Day Number arithmetic for cross-extreme-year duration computation (#224)
- **IANA timezone name preservation** — `CypherDateTime` stores the IANA zone name (e.g. `Europe/Stockholm`) separately; `.timezone` accessor returns the zone name rather than the numeric offset; `utcoffset()` now receives a concrete datetime for DST-aware lookup (#224)
- **Aggregate detection in QuantifierExpression** — `ALL(ok IN collect(...) WHERE ok)` patterns now correctly detected as aggregate-containing; planner emits `Aggregate` instead of `Project` (#224)
- **OPTIONAL MATCH multi-WHERE placement** — multiple `WHERE` clauses no longer overwrite each other; multi-hop OPTIONAL MATCH WHERE is emitted after all hops so all variables are bound (#224)
- **`coalesce()` type inference** — return type is inferred from argument types so `MATCH (x)` after `WITH coalesce(a, b) AS x` accepts node-typed variables (#224)
- **Non-deterministic aggregate arguments** — `count(rand())` and similar expressions now raise `AmbiguousAggregationExpression` per the openCypher spec (#224)
- **Temporal accessor for extreme-year datetimes** — `year`, `month`, `day`, `hour`, `minute`, `second` accessors work on `_WideDateTime`-backed `CypherDateTime` values (#224)
- **All 23 xfail markers removed** — previously marked as expected failures; all were fixable (#224)

### Added

- **75-test unit coverage for extreme temporal classes** — `tests/unit/types/test_extreme_temporal_values.py` covers `_WideDate`, `_WideDateTime`, `_BigDuration`, extreme-year construction/accessors, IANA timezone preservation, and large duration handling

### Fixed

- **WITH clause WHERE filtering and variable scoping** (#362) — `WITH n WHERE n.age > 30` now correctly filters post-projection; chained WITH propagates bindings properly
- **Triadic OPTIONAL MATCH semantics** (#302 partial) — `OPTIONAL MATCH (a)-[r]->(b)` with pre-bound `b` now correctly filters to only edges reaching that specific destination (LEFT JOIN semantics)
- **Relationship type disjunction without colon prefix** (#363) — `[:KNOWS|FOLLOWS]` (pipe without colon on subsequent types) now parses correctly
- **Multi-CREATE variable scoping** (#364 partial) — variables created in one CREATE clause are now visible in subsequent CREATE clauses within the same query

### Performance

- **O(n²) → O(1) graph statistics updates** (#366) — `_update_statistics_after_add_edge` no longer scans all edges on every insert; replaced with incremental `_unique_sources_by_type` tracking. Eliminated 2,000+ Pydantic `model_copy()` calls per 1,000-node graph by switching to mutable counters with lazy `GraphStatistics` construction
- **Parser fast path for long CREATE sequences** (#366) — queries with many consecutive CREATE clauses (e.g. the TCK movie graph with 971 CREATEs) are pre-split into batches of 5 before Earley parsing, reducing wall-clock time from 27 minutes to ~26 seconds. See docs/development/parser-performance-analysis.md for full analysis and the LALR(1) migration plan (issue #365)

### CI

- **TCK sharding across 4 parallel runners** — TCK tests now split using `pytest-split` with duration-based balancing (~4x faster CI)
- **Coverage sharding** — coverage collection also split across 4 shards and merged in report job
- **Python 3.14 experimental matrix** — added as non-blocking `continue-on-error` entries
- **TCK performance reporting** — new `scripts/tck_perf_report.py` runs after each TCK run, reporting tests over 5s and tracking use-case tests (movie graph, school graph) for performance regression detection

### TCK Coverage

- **3,885 / 3,885 passing (100%)** — first release with zero TCK failures and zero expected failures

## [0.3.7] - 2026-04-07

### Added - Functions

- **Math functions: e, pi, exp, log, log10, trig, degrees, radians, timestamp** (#326) — full set of mathematical and trigonometric functions per openCypher spec
- **`startNode()` and `endNode()` graph functions** (#318) — extract the start/end node from a relationship
- **`properties()` and `keys()` graph functions** (#316) — inspect node/relationship property maps and key sets
- **List concatenation `+` operator** (#315) — `[1,2] + [3,4]` → `[1,2,3,4]`
- **Map subscript access `m['key']`** (#314) — dynamic map property lookup
- **Exponent float notation and leading-dot floats** (#311) — `1.5e3`, `.5` now parsed correctly

### Fixed - Sorting & Aggregation

- **ORDER BY mixed-type sort ordering** (#320) — values of different Cypher types now sort in the correct openCypher type hierarchy order
- **Aggregation in composite expressions and ORDER BY** (#329) — `count(n) + 1`, `ORDER BY count(n)` and similar patterns now evaluate correctly
- **Validate ORDER BY aggregation expressions at compile time** (#331) — non-aggregated variables in aggregating ORDER BY now raise a clear error
- **Detect non-projected ORDER BY aggregates at compile time** (#332) — aggregates in ORDER BY that aren't in the projection are caught early

### Fixed - Expression Evaluation

- **Operator precedence, null propagation, and type coercion** (#321) — `NOT`, `AND`, `OR` precedence; null propagation in arithmetic; integer/float coercion edge cases
- **Type conversion TypeError/null per openCypher spec** (#327) — `toInteger()`, `toFloat()`, `toString()` now return `null` on unconvertible input per spec
- **List equality with null uses three-valued logic** (#330) — `[1, null] = [1, null]` now correctly returns `null`
- **NaN handling in comparisons, arithmetic, and sort** (#323) — NaN propagation and sort position per openCypher spec
- **SKIP/LIMIT accept expression values** (#319) — `SKIP toInteger(n.offset)` and similar dynamic skip/limit values now work

### Fixed - Parser & Clause Gaps

- **WITH keyword position gaps** (#310) — `WITH` is now accepted after `SET`, `REMOVE`, `DELETE`, and in chained multi-part queries
- **ASCENDING/DESCENDING keywords** (#312) — `ORDER BY n.name ASCENDING` / `DESCENDING` now parse correctly; removed stale xfail markers (#325)
- **`NodeRef`/`EdgeRef` comparison methods** (#313) — nodes and relationships are now sortable and comparable by identity

### Fixed - Graph Semantics

- **Relationship uniqueness in MATCH patterns** (#333) — runtime enforcement that the same physical edge cannot fill two relationship variables in one MATCH pattern; extended to anonymous multi-hop rels and path-variable (`ExpandMultiHop`) patterns
- **Temporal select-into constructors** (#334) — cross-type temporal projection (`date({date: other})`, `localdatetime({datetime: dt})`, etc.) with component overrides; null propagation, hour-overflow normalization, and timezone conversion all correct

### TCK Coverage

- **3,235 / 3,885 passing (83.3%)** (up from ~2,507 at v0.3.6) — +728 net new passing scenarios
- Theme: ORDER BY correctness, aggregation, operator precedence, graph semantics, temporal completeness

## [0.3.6] - 2026-02-28

### Fixed - TCK Harness Correctness

- **Integer property comparison** (#252) — TCK harness was comparing integer properties as strings, causing ~423 false failures; now coerced correctly
- **Map literal value coercion** (#261) — `_parse_map_literal` in conftest now coerces numeric string values, fixing ~30 false failures

### Fixed - Temporal Correctness

- **`date({})` / `datetime({})` / `time({})` map constructors** (#253, #268) — component validation, UTC defaults, and function-as-argument parsing
- **`UnexpectedEOF` on duration/datetime literals** (#254) — parser now handles all ISO 8601 duration and datetime forms
- **Timezone offset `+HH:MM` parsing** (#255) — `+HH:MM` and `-HH:MM` offsets now parsed correctly
- **Date map constructor component validation** (#256) — out-of-range components raise `ValueError`
- **`datetime(string)` defaults to UTC** (#276) — string datetimes without timezone now default to UTC per spec
- **Map constructor omitted components default to 1** (#278) — `date({year: 2015})` → `2015-01-01`, not error
- **`TRUNCATE` units: week, weekYear, quarter, decade, century, millennium** (#269) — all truncation units now return correct values
- **Week-based and select-form datetime constructors** (#270) — `datetime({week: 30, ...})` and similar forms return correct values
- **Duration serialization, comparison, multiplication, and fractional arithmetic** (#271) — full duration correctness including `duration.between()`
- **`datetime(aDatetime)` type coercion** (#274) — coercing datetime subtypes no longer conflicts with `:E` label parser token
- **Compact ISO 8601 date/datetime string formats** (#273) — ordinal (`YYYY-DDD`, `YYYYDDD`), week date (`YYYY-Www[-D]`), year-month (`YYYY-MM`, `YYYYMM`), and year-only (`YYYY`) all parsed correctly
- **IANA timezone names** (#272) — `[Region/City]` suffix in datetime strings is parsed; IANA zone is authoritative over explicit offset; historical second-precision offsets (e.g. `+00:53:28`) handled correctly

### TCK Coverage

- **2,507 passing** (up from ~1,694 at v0.3.5) — +813 net new passing scenarios
- Theme: all temporal TCK scenarios now pass; harness accuracy fully corrected

## [0.3.5] - 2026-02-19

### Added - Math Functions (#195, #196, #197)

- **sqrt(x)** - Square root function
  - Returns `CypherFloat`; negative input returns `null`; null propagation
  - Example: `RETURN sqrt(4) AS r` → `2.0`
- **rand()** - Random float in [0.0, 1.0)
  - Takes no arguments; returns a new `CypherFloat` each call
  - Example: `RETURN rand() * 100 AS r`
- **pow(base, exponent)** - Exponentiation function
  - Consistent with `^` operator: `pow(2, 3) = 2^3 = 8`
  - Null propagation for both arguments
  - Example: `RETURN pow(2, 10) AS r` → `1024`

### Fixed - Aggregation Function Tests (#201, #202, #203, #204)

- Fixed ~24 syntax bugs (`RETURNfunc(` → `RETURN func(`) in aggregation test files
- `percentileDisc()`, `percentileCont()`, `stDev()`, `stDevP()` were already implemented; tests now pass
- Updated docs to reflect all four as COMPLETE

### Added - TCK Step Definitions (#237)

- Added 5 missing pytest-bdd step definitions unblocking ~129 previously failing TCK scenarios:
  - `executing control query:` — alias for `executing query:`
  - `the result should be (ignoring element order for lists):` — row comparison with sorted list values
  - `the result should be, in order (ignoring element order for lists):` — ordered variant
  - `parameters are:` — parses datatable, substitutes `$param` references in queries
  - `there exists a procedure` — marked `xfail` (CALL procedures tracked for v0.3.6 in #190)
- Extended `_parse_value()` to handle list literals like `['Foo', 'Bar']`
- Extended `_row_to_comparable()` to handle `CypherList` values

### Implementation Status Updates

- Math functions: `sqrt`, `rand`, `pow` now COMPLETE (was NOT_IMPLEMENTED)
- Aggregation functions: `percentileDisc`, `percentileCont`, `stDev`, `stDevP` now COMPLETE
- TCK pass rate: +129 passing scenarios, +50 xfailed (procedure CALL)

## [0.3.4] - 2026-02-18

### Added - Power Arithmetic Operator (#213)

- **^ (power/exponentiation) operator** - Full exponentiation support
  - Right-associative: `2^3^2 = 2^(3^2) = 512`
  - Highest arithmetic precedence (above `*`, `/`)
  - Type handling: `int^int` returns int if whole, else float
  - Negative exponents: `2^-1 = 0.5`
  - Fractional exponents: `4^0.5 = 2.0`
  - NULL propagation for both operands
  - 39 integration tests covering associativity, precedence, types, and edge cases

### Added - XOR Logical Operator (#212)

- **XOR operator** - Exclusive OR with proper ternary logic
  - `true XOR false = true`, `true XOR true = false`
  - Precedence: NOT > AND > XOR > OR
  - NULL propagation: any NULL operand yields NULL
  - Left-associative chaining: `a XOR b XOR c`
  - Case-insensitive keyword (`XOR`, `xor`, `Xor`)
  - 22 integration tests + 4 unit tests

### Documentation - Feature Completion Updates

#### Already-Implemented Features Now Documented (#193, #214, #215)

- **length() function** - Documented as COMPLETE for paths
  - `length(path)` returns relationship count in path
  - For strings/lists, use `size()` function instead
  - File: `src/graphforge/executor/evaluator.py:2604`
- **List slicing [start..end]** - Documented as COMPLETE
  - Example: `RETURN [1, 2, 3, 4, 5][1..3]` → `[2, 3]`
  - Supports open-ended slicing: `[..]`, `[1..]`, `[..3]`
  - File: `src/graphforge/executor/evaluator.py:1458`
- **Negative list indexing** - Documented as COMPLETE
  - Example: `RETURN [1, 2, 3][-1]` → `3` (last element)
  - Supports both index and slice operations: `[1, 2, 3][-2..-1]`
  - File: `src/graphforge/executor/evaluator.py:1458`

### Added - String Function Aliases

#### toUpper()/toLower() camelCase variants (#194)

- **toUpper(string)** - CamelCase alias for UPPER(), converts string to uppercase
  - Example: `RETURN toUpper('hello') AS result` -> `'HELLO'`
- **toLower(string)** - CamelCase alias for LOWER(), converts string to lowercase
  - Example: `RETURN toLower('HELLO') AS result` -> `'hello'`
- Both camelCase and legacy forms (UPPER/LOWER) supported
- Case-insensitive keywords: toUpper, TOUPPER, toLower, TOLOWER all work
- 18 new integration tests (7 toUpper + 7 toLower + 4 interop)

### Implementation Status Updates

- Functions: 58/72 (81%, +3%) - String functions now 13/13 (100%)
- Operators: 32/34 (94%, +6%) - All list operators now COMPLETE
- Documentation reflects actual implementation status

## [0.3.3] - 2026-02-18

### Added - Pattern & CALL Features (Feature Completion: 88%)

#### Pattern Predicates - COMPLETE (#216)

- **Inline WHERE in patterns** - Filter relationships during pattern matching
  - Example: `MATCH ()-[r:KNOWS WHERE r.since > 2020]->(f) RETURN f.name`
  - Property comparisons, complex expressions (AND, OR, NOT)
  - Function calls in predicates, NULL handling
  - Variable-length paths with predicates
  - Works with undirected relationships
  - 16 comprehensive integration tests

#### CALL Subqueries - COMPLETE (#189)

- **General CALL { } subquery support** - Execute nested queries with full openCypher syntax
  - Example: `CALL { MATCH (p:Person) RETURN p.name AS name } RETURN name`
  - **Correlated scoping:** Access outer variables from parent query
  - **UNION support:** CALL can contain UNION and UNION ALL queries
  - **Unit subqueries:** Preserve 1:1 cardinality for side-effect queries
  - **Nested CALL:** Support for CALL within CALL
  - **Multiple CALL:** Cartesian product of multiple subqueries
  - 13 comprehensive integration tests

#### Pattern Comprehension - COMPLETE (#217)

- **Pattern-based list operations** - Transform graph patterns into lists
  - Example: `[(p:Person) WHERE p.age > 18 | p.name]`
  - Simple node patterns: `[(p:Person) | p.name]`
  - Relationship patterns: `[(p)-[:KNOWS]->(f) | f.name]`
  - Optional WHERE filters for pattern results
  - Complex expressions in map clause
  - Correlated variables from outer scope
  - Nested in RETURN, WHERE, WITH clauses
  - NULL property handling
  - 15 comprehensive integration tests

### Implementation

**AST Nodes:**

- PatternComprehension (expression.py) - pattern, filter_expr, map_expr
- CallClause (clause.py) - nested query support

**Grammar (cypher.lark):**

- pattern_comprehension rule with optional WHERE
- call_clause and call_query rules
- Pattern WHERE predicates already existed, now fully documented

**Parser (parser.py):**

- pattern_comprehension transformer
- call_clause and call_query transformers
- Enhanced list_literal to handle pattern comprehension

**Planner (planner.py):**

- Call operator with UNION support
- TypeContext preservation for nested queries

**Executor (executor.py, evaluator.py):**

- _execute_call with correlated scoping and unit subquery detection
- PatternComprehension evaluation via temporary MATCH execution
- Pattern predicate evaluation in relationship matching (already existed)

### Testing

- 44 new integration tests (16 + 13 + 15)
- All tests passing with 100% coverage on new code
- TCK pass rate: ~40% (1,530 / 3,837 tests)

### Documentation

- Updated patterns.md: 100% completion (8/8 pattern types)
- Updated clauses.md: CALL marked as COMPLETE
- All features documented with examples and implementation references
- Feature completion: 85% → 88% (+3 features)

### Performance

- No performance regressions
- Pattern comprehension uses existing pattern matching infrastructure
- CALL subqueries properly isolated with TypeContext.copy()

## [0.3.2] - 2026-02-17

### Added - List Operations (100% Complete)

#### List Operation Functions (#198, #199, #200)

- **filter()** - Filter lists by predicate
  - Example: `RETURN filter(x IN [1,2,3,4,5] WHERE x > 3) AS result` → [4, 5]
  - NULL list returns NULL, NULL items excluded
  - Variable binding with proper scoping
- **extract()** - Map transformations over lists
  - Example: `RETURN extract(x IN [1,2,3] | x * 2) AS result` → [2, 4, 6]
  - NULL list returns NULL, NULL items processed normally
  - Supports complex expressions and property access
- **reduce()** - Fold/reduce with accumulator
  - Example: `RETURN reduce(sum = 0, x IN [1,2,3,4] | sum + x) AS result` → 10
  - Dual variable binding (accumulator + loop variable)
  - Empty list returns initial value

### Implementation

- Added three AST nodes: FilterExpression, ExtractExpression, ReduceExpression
- Grammar rules for all three expressions in cypher.lark
- Parser transformers with Pydantic validation
- Evaluator handlers with proper NULL handling and variable scoping
- Treated as special syntax (like list comprehensions) due to variable binding

### Testing

- 42 comprehensive integration tests
- Full coverage of edge cases (empty lists, NULL handling, variable shadowing)
- Composition and nesting tests
- All tests passing with 100% coverage on new code

### Documentation

- Updated implementation status: 58/72 functions complete (81%, +4%)
- List Functions: 8/8 (100%)

## [0.3.1] - 2026-02-17

### Added - Predicate Functions (100% Complete)

#### Quantifier Functions (#205, #206, #207, #208)

- **all()** - Tests if all elements in a list satisfy a predicate
  - Example: `RETURN all(x IN [2, 4, 6] WHERE x % 2 = 0) AS result` → true
  - Implements three-valued NULL logic per OpenCypher spec
  - Returns false if any element fails, true if all pass, NULL if indeterminate
- **any()** - Tests if any element in a list satisfies a predicate
  - Example: `RETURN any(x IN [1, 2, 3] WHERE x > 2) AS result` → true
  - Returns true if any element passes, NULL if no true but some NULL
- **none()** - Tests if no elements in a list satisfy a predicate
  - Example: `RETURN none(x IN [1, 3, 5] WHERE x % 2 = 0) AS result` → true
  - Inverse of any() with proper NULL handling
- **single()** - Tests if exactly one element satisfies a predicate
  - Example: `RETURN single(x IN [1, 2, 3] WHERE x = 2) AS result` → true
  - Returns true only if exactly one match and no NULLs
  - Returns NULL if uniqueness cannot be determined (NULLs present)

#### Property and Collection Testing (#209, #210)

- **exists()** - Tests if a property exists or expression is not NULL
  - Example: `MATCH (p:Person) WHERE exists(p.age) RETURN p.name`
  - Evaluates before NULL propagation for accurate property checking
  - Returns false for missing properties (NULL values are not stored)
- **isEmpty()** - Tests if a list, string, or map is empty
  - Example: `RETURN isEmpty([]) AS result` → true
  - Works with lists, strings, and maps
  - Returns NULL for NULL input (three-valued logic)

### Testing

- 57 comprehensive integration tests (34 quantifier + 23 exists/isEmpty)
- Parametrized tests for better maintainability
- Full coverage of NULL handling edge cases
- All tests passing with 100% coverage on new code

### Documentation

- Updated implementation status: 55/72 functions complete (76%, +2%)
- Predicate Functions: 6/6 (100%)
- Detailed function signatures and examples in docs/reference/implementation-status/functions.md

### Performance

- No performance regressions
- Efficient NULL propagation handling
- Optimized list iteration for quantifiers

## [0.3.0] - 2026-02-09

### Added - Major Cypher Features

#### OPTIONAL MATCH (Left Outer Joins)

- Left outer join semantics with NULL preservation (#104)
- Example: `MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS]->(f) RETURN p.name, f.name`
- 6 integration tests, comprehensive NULL handling

#### UNION and UNION ALL

- Combine query results with automatic deduplication (UNION) or preserve duplicates (UNION ALL) (#104)
- Example: `MATCH (p:Person) RETURN p.name UNION MATCH (c:Company) RETURN c.name`
- Tree-based operator structure for nested queries
- 9 integration tests

#### List Comprehensions

- Transform and filter lists declaratively (#104)
- Example: `RETURN [x IN [1,2,3,4,5] WHERE x > 3 | x * 2]`
- Supports WHERE filtering, map expressions, and nested comprehensions
- 12 integration tests

#### EXISTS and COUNT Subquery Expressions

- Correlated subqueries for existence checks and counting (#104)
- Example: `MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:KNOWS]->() } RETURN p.name`
- Full operator pipeline execution for nested queries
- 13 integration tests

#### Variable-Length Path Patterns

- Recursive traversal with cycle detection (#104)
- Example: `MATCH (a)-[:KNOWS*1..3]->(b) RETURN a.name, b.name`
- Depth-first search with per-path cycle prevention
- Configurable min/max hop counts
- 2 integration tests

#### IS NULL / IS NOT NULL Operators

- Boolean NULL checking (distinct from = NULL ternary logic) (#104)
- Example: `MATCH (p:Person) WHERE p.age IS NULL RETURN p.name`
- Always returns boolean (never NULL)

### Added - Dataset Integration

#### NetworkRepository Datasets (#110, #113)

- 10 new graph datasets from NetworkRepository
- GraphML loader for complex graph formats
- Comprehensive metadata with node/edge counts, categories, licenses
- Examples: Polblogs, Polbooks, Karate club, Dolphin social network, C. elegans, Les Miserables
- All datasets validated with comprehensive test suite

#### Spatial and Temporal Types

- Point type for geographic coordinates
- Distance function for spatial queries
- Date, DateTime, Time, Duration types
- Full openCypher compatibility for type system

#### Dataset Validation Infrastructure (#112, #113)

- Comprehensive validation script (scripts/validate_datasets.py)
- Validates downloads, caching, node/edge counts, query functionality
- Performance benchmarking for all datasets
- 100% validation success rate (13/13 datasets tested)

### Fixed

- make coverage-diff command now works correctly (#111)
- Dataset validation script handles missing query results (#112)
- NetworkRepository dataset URLs and metadata corrections (#113)
- Exception handling improvements in validation infrastructure (#113)
- Resource cleanup with proper try/finally blocks

### Architecture Improvements

- Tree-based operator structure for nested queries
- Dual serialization: SQLite+MessagePack (data) + Pydantic+JSON (metadata)
- Enhanced expression evaluator with recursive execution
- Operator pipeline supports nested query planning

### Testing

- 767 integration tests passing (42+ new tests for v0.3.0)
- 91.96% code coverage maintained
- Comprehensive dataset validation suite
- Property-based testing with Hypothesis

### TCK Compatibility

- Progress from 16.6% to ~29% openCypher TCK coverage
- 312+ additional scenarios passing
- Foundation for continued TCK improvements toward 39% target

### Documentation

- Complete v0.3.0 feature documentation (CHANGELOG_v0.3.0.md)
- Dataset integration guides (docs/datasets/)
- Performance benchmarks and optimization tips
- Updated openCypher compatibility matrix

### Breaking Changes

None. All changes maintain backward compatibility with v0.2.0 and v0.2.1.

### Known Limitations

- Variable-length paths: no configurable max depth limit in unbounded queries
- UNION: no post-UNION ORDER BY (must be in each branch)
- Pattern predicates (WHERE inside patterns) not yet supported

## [0.2.1] - 2026-02-03

### Added

- **Dataset loading infrastructure** (#68)
  - Automatic dataset download and caching system
  - Dataset registry with metadata (nodes, edges, size, category, license)
  - Built-in support for HTTP downloads with retry logic and timeout
  - Local cache directory (`~/.graphforge/datasets/`) with TTL-based expiration
  - Public API: `load_dataset()`, `list_datasets()`, `get_dataset_info()`, `clear_cache()`
  - `GraphForge.from_dataset()` convenience method for loading datasets
  - Example: `gf = GraphForge.from_dataset("snap-ego-facebook")`
- **CSV edge-list loader** (#69)
  - Load edge-list datasets in CSV/TSV/space-delimited formats
  - Auto-delimiter detection (tab, comma, space)
  - Gzip compression support for `.gz` files
  - Comment line handling (lines starting with `#`)
  - Weighted and unweighted edge support
  - Node deduplication via caching
  - Consecutive whitespace handling
  - Example: Load SNAP datasets with simple edge-list format
- **5 SNAP datasets available** (#69)
  - `snap-ego-facebook` - Facebook social circles (4K nodes, 88K edges, 0.5 MB)
  - `snap-email-enron` - Enron email network (37K nodes, 184K edges, 2.5 MB)
  - `snap-ca-astroph` - Astrophysics collaboration (19K nodes, 198K edges, 1.8 MB)
  - `snap-web-google` - Google web graph (876K nodes, 5.1M edges, 75 MB)
  - `snap-twitter-combined` - Twitter social circles (81K nodes, 1.8M edges, 25 MB)
  - Auto-registered on module import
  - Filterable by source, category, and size
- **MERGE ON CREATE SET syntax** (#65)
  - Conditional property setting when creating nodes: `MERGE (n:Person {id: 1}) ON CREATE SET n.created = timestamp()`
  - Parser support in Lark grammar
  - Executor tracks whether MERGE created or matched nodes
  - Comprehensive test coverage (parser, executor, integration)
- **MERGE ON MATCH SET syntax** (#66)
  - Conditional property setting when matching existing nodes: `MERGE (n:Person {id: 1}) ON MATCH SET n.updated = timestamp()`
  - Supports both ON CREATE and ON MATCH in same statement
  - OpenCypher-compliant semantics

### Fixed

- **WITH clause variable passing in aggregation** (#67)
  - Fixed variable scoping issues when using WITH after aggregation
  - Correctly passes aggregated values to subsequent clauses
  - Example: `MATCH (n) WITH count(n) AS cnt RETURN cnt` now works correctly

### Documentation

- **Complete dataset documentation** (#69)
  - New dataset overview guide with usage examples
  - SNAP dataset documentation with 5 available datasets
  - Updated quick start with dataset loading examples
  - Added dataset examples to main README
  - Performance tips for large datasets

### Known Limitations

- Only SNAP datasets available in this release (5 datasets)
- Neo4j example datasets, LDBC, and NetworkRepository planned for v0.3.0
- See [Issue #70](https://github.com/CurateLabs/graphforge-legecy/issues/70) for roadmap to 100+ SNAP datasets

## [0.2.0] - 2026-02-03

### Added

- **CASE expressions for conditional logic** (#49)
  - Full openCypher CASE expression support with WHEN/THEN/ELSE/END syntax
  - Simple CASE (`CASE expr WHEN value`) and searched CASE (`CASE WHEN condition`)
  - NULL-safe semantics following openCypher specification
  - Example: `RETURN CASE WHEN n.age < 18 THEN 'minor' ELSE 'adult' END`
- **COLLECT aggregation function** (#46, #48)
  - Aggregate values into lists with `COLLECT()` function
  - DISTINCT support: `COLLECT(DISTINCT n.name)` removes duplicates
  - Handles complex types (CypherList, CypherMap) correctly in DISTINCT mode
  - NULL filtering: NULL values excluded from collected lists
  - Example: `MATCH (n) RETURN COLLECT(n.name)`
- **Arithmetic operators** (#44)
  - Binary operators: `+`, `-`, `*`, `/`, `%` (modulo)
  - Unary minus: `-n.value`
  - NULL propagation: operations with NULL return NULL
  - Type coercion for mixed integer/float operations
  - Division by zero returns NULL (openCypher-compliant)
  - Example: `RETURN n.price * 1.1 AS price_with_tax`
- **String matching operators** (#43)
  - `STARTS WITH`: Prefix matching
  - `ENDS WITH`: Suffix matching
  - `CONTAINS`: Substring matching
  - Case-sensitive matching following openCypher specification
  - NULL handling: returns NULL if either operand is NULL
  - Example: `MATCH (n) WHERE n.email ENDS WITH '@example.com'`
- **REMOVE clause** (#42)
  - Remove properties: `REMOVE n.property`
  - Remove labels: `REMOVE n:Label`
  - Multi-target support: `REMOVE n.prop1, n.prop2, n:Label`
  - Idempotent: removing non-existent properties/labels is a no-op
  - Example: `MATCH (n:Person) REMOVE n.age, n:Temporary`
- **UNWIND clause** (#40)
  - Unwind lists into rows: `UNWIND [1, 2, 3] AS x RETURN x`
  - Supports nested lists, empty lists, NULL values
  - Can be used with MATCH, WHERE, and other clauses
  - Example: `UNWIND $ids AS id MATCH (n) WHERE n.id = id RETURN n`
- **NOT logical operator** (#30)
  - Unary negation operator for boolean expressions
  - NULL-safe semantics: `NOT NULL` returns NULL
  - Example: `MATCH (n) WHERE NOT n.active RETURN n`
- **DETACH DELETE clause** (#33)
  - OpenCypher-compliant DELETE semantics
  - `DELETE` raises error if node has relationships
  - `DETACH DELETE` removes all connected edges first, then the node
  - Example: `MATCH (n:Person) DETACH DELETE n`
- **Comprehensive MATCH-CREATE combination tests** (#41)
  - 12 integration tests for MATCH followed by CREATE patterns
  - Validates correctness of mixed read-write operations
- **Complete documentation reorganization** (#56)
  - Restructured docs into logical sections (getting-started, user-guide, reference, development)
  - New datasets documentation section with examples
  - Improved navigation and discoverability
- **Code of Conduct** - Added Contributor Covenant Code of Conduct

### Fixed

- **ORDER BY after aggregation** (#39)
  - ORDER BY now correctly finds aliased variables after aggregation
  - Example: `MATCH (n) RETURN COUNT(n) AS cnt ORDER BY cnt` now works
- **RETURN DISTINCT after projection** (#38)
  - RETURN DISTINCT now works correctly after projection expressions
  - Fixes issue where DISTINCT was applied to wrong columns

### Changed

- **Test coverage improved** (#37)
  - Coverage increased from 88.69% to 93.76% (+4.94%)
  - Added 50+ new tests across parser, planner, and executor
- **GitHub Pages deployment modernization** (#32)
  - Migrated from legacy `mkdocs gh-deploy` to GitHub Actions native deployment
  - Uses `actions/upload-pages-artifact@v3` and `actions/deploy-pages@v4`
  - Simpler, faster, more secure deployment with `id-token` authentication
- **Issue workflow documentation** (#45)
  - Updated ISSUE_WORKFLOW.md to reflect current development process

## [0.1.4] - 2026-02-02

### Added

- **Local coverage validation workflow** (#15, #16)
  - `make pre-push` now validates 85% combined line+branch coverage locally before pushing
  - `make coverage` - Run tests with coverage measurement and generate reports
  - `make check-coverage` - Validate 85% combined coverage threshold
  - `make coverage-strict` - Strict 90% threshold validation for new features
  - `make coverage-report` - Open HTML coverage report in browser
  - `make coverage-diff` - Show coverage for changed files only
  - Catches 90% of coverage issues before CI, eliminating codecov patch failures
  - Current coverage: 88.69% (92.41% line + 81.19% branch)
- **Codecov Test Analytics integration** (#17, #18)
  - JUnit XML generation for all test runs across 8,203 tests (481 unit/integration + 7,722 TCK)
  - Test performance monitoring with execution time trends
  - Flaky test detection for intermittent failures
  - Failure rate tracking and reliability pattern analysis
  - Cross-platform analytics tracking (12 OS/Python combinations)
  - `make test-analytics` - Generate JUnit XML locally for analysis
  - Analytics dashboard at https://app.codecov.io/gh/CurateLabs/graphforge-legecy
- **List and map literal support in CREATE statements** (#15)
  - CREATE now accepts complex property types: lists, maps, and nested structures
  - Proper bidirectional CypherValue ↔ Python type conversion
  - 10 new integration tests covering complex property edge cases
- Codecov integration for automated coverage tracking
  - Coverage reports uploaded from GitHub Actions
  - Component-level coverage tracking (parser, planner, executor, storage, ast, types)
  - PR comments with coverage changes
  - Branch coverage analysis
  - Configuration file (`.codecov.yml`) with 85% project target, 80% patch target

### Changed

- **Development workflow modernization** (#16)
  - Updated CONTRIBUTING.md with comprehensive make-based workflow documentation
  - Single command for all pre-push validation: `make pre-push` (format-check, lint, type-check, coverage, check-coverage)
  - Clear documentation of coverage requirements (85% project, 80% patch)
  - Complete guidance on all available make targets with examples
- Test suite significantly expanded to 479 unit/integration tests + 7,722 TCK compliance tests

### Fixed

- Codecov test analytics deprecation warning (#18)
  - Migrated from deprecated `test-results-action@v1` to `codecov-action@v5`
  - Uses `report_type: test_results` parameter for future-proof compatibility

## [0.1.3] - 2026-02-01

### Changed

- **Column naming now uses variable names for simple variable references** (openCypher TCK compliance)
  - `RETURN n` now produces column name "n" (previously "col_0")
  - `RETURN n AS alias` produces column name "alias" (unchanged)
  - `RETURN n.property` produces column name "col_0" (unchanged - complex expression)
  - This aligns GraphForge with the openCypher specification and improves Neo4j compatibility
  - Note: This is a breaking change from v0.1.2 but necessary for WITH clause correctness
  - Rationale: WITH clause requires preserving variable names through query pipeline
- Test suite expanded with WITH clause coverage (17 comprehensive test cases)

### Added

- Comprehensive WITH clause integration tests covering:
  - Basic projection and variable renaming
  - WHERE filtering on intermediate results
  - Aggregation with GROUP BY semantics
  - ORDER BY, SKIP, LIMIT on intermediate results
  - Multi-part query chaining
  - Edge cases and null handling

### Fixed

- WITH clause bugs: column naming, aggregations, and DISTINCT behavior
- CodeRabbit configuration file to use only valid schema properties
- Removed unused pytest import from WITH clause tests

## [0.1.2] - 2026-02-01

### Added

- Professional versioning and release management system
  - Comprehensive CHANGELOG.md following Keep a Changelog format
  - Automated version bumping script (`scripts/bump_version.py`)
  - Release process documentation (RELEASING.md, docs/RELEASE_PROCESS.md, docs/RELEASE_STRATEGY.md)
  - Weekly automated release check with GitHub issue reminders
  - Release tracking workflow with auto-labeling
- MkDocs Material documentation site
  - Auto-generated API documentation from docstrings
  - Complete user guide (installation, quickstart, Cypher guide)
  - Auto-deploy to GitHub Pages on every push
- CI/CD enhancements
  - CHANGELOG validation workflow (ensures PRs update changelog)
  - Automated PR labeling based on changed files
  - Labels for component tracking (parser, planner, executor)
- `.editorconfig` for consistent editor settings across IDEs

### Changed

- Updated GitHub Actions to Node.js 24 (actions/checkout v6, actions/setup-python v6, astral-sh/setup-uv v7)
- Enhanced PR guidelines to enforce small PRs and proper fixes
- Updated README badges to professional numpy-style flat badges

### Fixed

- Integration test regression from WITH clause implementation
- Column naming now correctly uses `col_N` for unnamed return items
- SKIP/LIMIT queries no longer return empty results
- TCK test collection error resolved
- API documentation now references actual modules (api, ast, parser, planner, executor, storage, types)

## [0.1.1] - 2026-01-31

### Added

- WITH clause for query chaining and subqueries
- Production-grade CI/CD infrastructure
  - Pre-commit hooks (ruff, mypy, bandit)
  - CodeRabbit integration
  - Dependabot configuration
  - PR and issue templates
- Comprehensive documentation (30+ docs)
- TCK compliance at 16.6% (638/3,837 scenarios)

### Changed

- Updated project URLs to reflect new organization
- Enhanced README with additional badges

### Fixed

- Critical integration test failures (20 tests)
- TCK test collection error

## [0.1.0] - 2026-01-30

### Added

- Initial release of GraphForge
- Core data model (nodes, edges, properties, labels)
- Python builder API (`create_node`, `create_relationship`)
- SQLite persistence with ACID transactions
- openCypher query execution
  - MATCH, WHERE, CREATE, SET, DELETE, MERGE, RETURN
  - ORDER BY, LIMIT, SKIP
  - Aggregations (COUNT, SUM, AVG, MIN, MAX)
- Parser and AST for openCypher subset
- Query planner and executor
- 351 tests (215 unit + 136 integration)
- 81% code coverage
- Multi-OS, multi-Python CI/CD (3 OS × 4 Python versions)

[Unreleased]: https://github.com/CurateLabs/graphforge/compare/v0.5.1...HEAD
[0.5.1]: https://github.com/CurateLabs/graphforge/releases/tag/v0.5.1
[0.5.0]: https://github.com/CurateLabs/graphforge/releases/tag/v0.5.0
[0.4.0]: https://github.com/CurateLabs/graphforge-legecy/compare/v0.3.10...v0.4.0
[0.3.0]: https://github.com/CurateLabs/graphforge-legecy/compare/v0.2.1...v0.3.0
[0.2.1]: https://github.com/CurateLabs/graphforge-legecy/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/CurateLabs/graphforge-legecy/compare/v0.1.4...v0.2.0
[0.1.4]: https://github.com/CurateLabs/graphforge-legecy/compare/v0.1.3...v0.1.4
[0.1.2]: https://github.com/CurateLabs/graphforge-legecy/compare/v0.1.1...v0.1.2
[0.1.0]: https://github.com/CurateLabs/graphforge-legecy/releases/tag/v0.1.0
