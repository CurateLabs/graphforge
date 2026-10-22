---
title: "ADR 0061: Research porcelain and one meaning for a ref"
adr: "0061"
status: "Accepted"
date: "2026-10-21"
superseded_by: null
revisit_when: "A porcelain verb needs behavior no native operation provides, a Hub advertises tag refs in its refs document, or a Branch needs more than one name"
---

# ADR 0061: Research porcelain and one meaning for a ref

**Status:** Accepted

**Implementation:** #2126 is the close gate. The porcelain verbs, the ref rule
and the two pieces of local state land on one branch; #2127 (agent errors,
credentials, skill) and #2128 (Python and Node facades) consume the names
decided here and add nothing to them.

**Related:** #1772 (parent), #1771 (history semantics, done), ADR 0039 (Version
publication authority), ADR 0041 (Branch state publishes through Project
CURRENT), ADR 0044 (research interchange), ADR 0052 (lineage document), ADR 0053
(Hub publish wire contract), ADR 0055 (Versions are commits).

## Context

History semantics are finished (#1771): a Version is a commit with parents,
author and committer (ADR 0055); `gf clone` installs a continuable Branch
(`GraphForge::continue_imported_research_branch`, `crates/graphforge-api/src/research_versions.rs:96`);
`gf publish` fast-forwards only, with `--rewrite` under an expected-revision
lease and `--prune` (`crates/graphforge-cli/src/hub_publish.rs:81-86`); upstream
incorporation records a two-parent Version
(`GraphForge::update_research_branch`, `crates/graphforge-api/src/research_upstream/model.rs:80`);
credit is derived and public.

What is missing is the layer an analyst or agent types. Every history operation
is reached through `gf research <domain> <verb> --file <native JSON>`
(`crates/graphforge-cli/src/research_cli/branches.rs:9-13`), which requires the
caller to supply the current generation UUID, a fresh operation UUID and a fresh
Version UUID by hand. Versions are addressable only by UUID.

`--ref` has two meanings today:

- In `gf publish`, `--ref <name>` names a local research Branch **by its
  label**; `select` filters `registry.branches` on `record.label == name` and
  refuses zero or several matches (`crates/graphforge-cli/src/hub_publish.rs:733-748`).
  The name is checked against the discovery ref grammar first
  (`check_ref_name`, `hub_publish.rs:781`) and is then published as the
  lineage Branch's `ref_name` and as the repository's `RepositoryRef.name`.
- In `gf clone`, `--ref <name>` is a **discovery ref**: it is matched against
  `lineage.branches[*].ref_name` and resolves to that Branch's head
  (`resolve_research_version_uuid`, `crates/graphforge-cli/src/hub_clone.rs:402-432`).

Because publish writes the label into `ref_name` unchanged, the two meanings
coincide whenever the label survives the grammar check. The coincidence is an
accident of the publish code path, not a rule: a local Branch may be created with
any label up to 4,096 bytes (`crates/graphforge-storage/src/research_versions/branches.rs:153`),
including spaces and colons, and only fails at publish time; two Branches may
share a label and only fail at publish time; and a clone records the remote
`ref_name` (`ResearchRemoteBranch.ref_name`,
`crates/graphforge-storage/src/research_versions/remote.rs:10-22`) while the
installed Branch carries whatever `label` the publisher had.

Nothing names a Version. `ResearchVersionRecord.label` exists
(`crates/graphforge-storage/src/research_versions.rs:186`) but is frozen into
the record at creation and is part of the identity digest, so it cannot name a
Version after the fact and cannot be moved. The lineage document carries no
Version names at all (`LineageVersion`, `crates/graphforge-discovery/src/lineage.rs:134`).

No local state records which Branch the analyst is working on. Every native
request names its `branch_uuid`.

## Decision

### 1. A ref is a Branch label, and every Branch label is a valid ref

One rule for `gf clone`, `gf publish`, `gf push` and `gf pull`:

> `--ref <name>` names a Branch. Locally it is the Branch's `label`; on a Hub it
> is the lineage Branch's `ref_name`. They are the same string.

To make the rule true rather than usually true:

- **Branch labels are validated as refs at creation.** `CreateResearchBranchRequest`
  validation (`crates/graphforge-api/src/branches/model.rs:36`) applies the
  discovery ref grammar (`validate_ref_name`,
  `crates/graphforge-discovery/src/lib.rs:1486`: non-empty, at most 4,096
  bytes, no leading or trailing `/`, no `..`, no `//`, no control bytes, none of
  `\ ? # ~ ^ : space`) and refuses a label already held by a Branch of the same
  Project. The refusal is the existing validation class with code
  `GF_RESEARCH_BRANCH_LABEL_INVALID` or `GF_RESEARCH_BRANCH_LABEL_TAKEN`.
  The storage read-side check is unchanged, so Projects holding older labels
  still open; such a Branch cannot be pushed (publish already refuses it) and
  `gf status` reports it as `unpublishable label`.
- **Publish keeps its grammar check** (`check_ref_name`) as the defensive
  boundary and keeps refusing an ambiguous label. After this ADR the ambiguity
  arm is unreachable for Branches created through the API; it stays as a guard
  for older Projects.
- **Clone installs the Branch under the remote ref name.** `continue_imported_research_branch`
  already receives the lineage Branch and its `ref_name`; the installed Branch
  record's `label` is the lineage `label`, which the publisher wrote from the same
  string. The import step asserts `label == ref_name` and refuses a lineage
  where they differ (`hub.malformed_lineage`), so a cloned Branch can always be
  pushed back under the ref it came from.
- **The default ref is the Hub's.** `gf clone <repository>` without `--ref`
  follows the repository's advertised `default_ref` (#1785, already implemented:
  `hub_listing.rs::clone_without_a_selection_follows_the_default_ref_to_its_branch_head`).
  No porcelain verb invents a default such as `main`.

`--ref` never accepts a Version UUID or a tag. Versions are named by
`--version-uuid` where a verb accepts one today (`gf clone`, `gf publish`), and
by tags through the `<version>` positional described below.

### 2. Tags are local, movable until published, and travel in the lineage

A tag is a human name for one Version. It is new persisted state and it is the
only new state that travels.

- **Local storage.** `ResearchRegistry` gains `tags: BTreeMap<String, Uuid>`
  (`crates/graphforge-storage/src/research_versions.rs:256`), beside
  `remote_branches`. A tag name obeys the ref grammar; a tag target must be in
  the permanent `identities` ledger (it may name a released Version, like a
  parent). Bound: at most `MAX_RECEIPTS` (4,096) tags, refused as a resource
  limit. The field is omitted when empty, so revision 7 registry bytes decode and
  digest unchanged; no format revision.
- **Mutation.** One new `ResearchMutation` variant, `SetTag { name, version_uuid: Option<Uuid> }`
  (`None` deletes), published through the existing registry publication (ADR
  0039/0041: atomic CURRENT replacement, receipt, exact replay under the same
  `operation_uuid`). Setting a tag that already exists with a different target
  is refused (`GF_RESEARCH_TAG_EXISTS`); the analyst deletes and re-creates.
  API: `GraphForge::set_research_tag(&SetResearchTagRequest { operation_uuid,
  expected_generation_uuid, name, version_uuid })`.
- **Travel.** The lineage document (ADR 0052) gains `tags: Vec<LineageTag { name, version_uuid }>`,
  canonically ordered by name, omitted when empty so every existing lineage
  document is byte-identical and still validates. A tag in a lineage must name a
  Version present in `versions`. Protocol version is unchanged; a reader of the
  current crate accepts both shapes, and an older client rejects a document that
  carries tags under `deny_unknown_fields`, which is the pre-v1 policy.
- **Publish.** The lineage a publish uploads carries the union of the served
  lineage's tags and the local tags whose target is in the published closure.
  Same name, different target is refused as a non-fast-forward
  (`GF_HUB_TAG_CONFLICT`) unless `--rewrite`, under which the local target wins
  and the rewrite is recorded exactly as a head rewrite is (#2124 lease). Tags
  are never removed from a Hub by this ADR; a removed local tag stays served.
- **Clone and pull.** The import step that writes `remote_branches` also writes
  every lineage tag into `tags`. On pull, a served tag that conflicts with a
  local tag of the same name refuses the pull (`GF_RESEARCH_TAG_CONFLICT`); the
  analyst deletes the local tag and pulls again.
- **Resolution.** Wherever a porcelain verb takes a `<version>` argument it
  accepts a tag name or a Version UUID, in that order of interpretation; a
  string that is both a valid UUID and a tag name resolves as a tag
  (`GF_RESEARCH_VERSION_AMBIGUOUS` is not needed: UUIDs contain no ref-illegal
  byte but tags are looked up first, deterministically). `gf clone` and
  `gf publish --version-uuid` keep accepting only a UUID; `gf clone --tag <name>`
  resolves the name through the lineage's `tags` and then follows the existing
  `--version-uuid` path.

The frozen `ResearchVersionRecord.label` is untouched. It remains the citation
label the native verbs set at creation and is not a tag.

### 3. The current Branch is local state in the registry

`ResearchRegistry` gains `current_branch: Option<Uuid>`, omitted when `None`.
It is written by `gf switch` through a `SelectBranch { branch_uuid }` mutation
(`GraphForge::select_research_branch`) and read by every porcelain verb. It is
local only: it does not appear in the lineage, in interchange archives, or in
any digest, exactly like `remote_branches`. A clone sets it to the installed
Branch.

Every porcelain verb resolves its Branch in this order and stops at the first
hit: `--branch <ref>` on the command line; `current_branch`; the only Branch in
the Project; otherwise `GF_RESEARCH_BRANCH_UNSELECTED` ("run `gf switch <ref>`").
A `current_branch` that no longer resolves (impossible through the API, which
has no Branch deletion, but possible after restore of an older checkpoint) is
reported by `gf status` and ignored by the resolver.

### 4. Top-level verbs, delegating to native operations

The porcelain verbs are **top level** (`gf commit`, not `gf research commit`).
Reasons: `gf clone`, `gf publish`, `gf hub` and `gf auth` are already top
level, and splitting the git workflow across two levels would put `clone` and
`push` on different prefixes; `gf research` stays the namespace of the native
JSON verbs, which keeps "porcelain" and "plumbing" visibly distinct in `gf --help`.
The one collision, `gf research branch` (plumbing) versus `gf branch`
(porcelain), is the git convention (`git branch` versus `git update-ref`) and is
acceptable.

Common rules for every verb below:

- `--json` is the existing global flag (`crates/graphforge-cli/src/lib.rs:247`);
  the JSON result is written by the existing `write_execution_result` /
  `serde_json` paths. Human output is text to stdout, one record per line.
- `--branch <ref>` is accepted by every verb that acts on a Branch, resolved per
  section 3.
- Verbs that publish a Version take `--operation-uuid <uuid>`; when absent the
  verb generates a `now_v7` UUID and prints it in both outputs, so a retry with
  the same request replays the original receipt exactly as the native verb does.
  The expected generation UUID is read from the open Project immediately before
  the request (`GraphForge::committed_generation_identity`), never supplied by
  the caller; a concurrent writer produces the native `WriteConflict`.
- Author and committer come from section 5.
- Hub-facing verbs take `--hub <url>`, `--token-env <var>` and
  `--protection-bypass-env <var>` exactly as `gf clone` and `gf publish` do, and
  reuse their transport, retry, and credential paths without change.

| Verb | Flags | Native operation | Output |
| --- | --- | --- | --- |
| `gf status` | `--branch` | reads only: `research_version_retention` (branches, heads, ancestry, remote_branches, tags, current_branch) | branch, head Version, upstream (`repository_url ref`), `ahead N` (local Versions not in the ancestry of the recorded remote head), tags on head, warnings (unpublishable label, dangling current_branch). JSON: `{branch_uuid, label, head_version_uuid, upstream: {repository_url, ref, head_version_uuid}?, ahead, tags: [..], warnings: [..]}` |
| `gf log` | `--branch`, `--remote [<repository>]`, `-n <count>`, `--all` | local: walk `ancestry` from the Branch head through `research_version(uuid)` records; remote: the `gf hub list` read of refs and lineage (`repository_listing.rs`), walking `LineageVersion.parents` from the ref's head | one line per Version: short UUID, tags, author, date, first line of `description`; `--json` emits `[{version_uuid, parents, author, committer, created_at, label, description, tags, kind}]`. `--all` walks every parent, default is first-parent only |
| `gf commit` | `-m <message>` (required), `--query <cypher>` or `--file <path>` (exactly one), `--branch`, `--author`, `--committer`, `--operation-uuid` | `GraphForge::execute_research_branch(&ExecuteResearchBranchRequest{...})`, the operation `gf research branch execute --file` wraps today | human: `[<label> <short uuid>] <subject>`; JSON: the native `ResearchOperationReceipt` plus `version_uuid`, `parents` |
| `gf branch` | none: list; `<name>`: create from the current head; `--from <version>`: create from a tag or UUID | list: `research_version_retention`; create: `GraphForge::create_research_branch` with `BranchSource::Version { version_uuid: <head or --from> }` (`branches/model.rs:21`), `label = <name>` | list: one line per Branch, `*` on the current one, with head and upstream; create: the native receipt |
| `gf switch` | `<ref>` | `select_research_branch` (section 3) | `Switched to branch '<ref>'` ; JSON `{branch_uuid, label}` |
| `gf tag` | none: list; `<name> [<version>]`: create (default: current head); `-d <name>`: delete | `set_research_tag` (section 2) | list: `<name> <short uuid>`; create/delete: the receipt |
| `gf push` | `[<repository>]`, `--ref`, `--rewrite`, `--prune`, `--operation-uuid`, `--staging-directory`, `--hub`, `--token-env`, `--protection-bypass-env` | `hub_publish::run_publish_with` with `PublishArgs { repository, git_ref: Some(<ref>), version_uuid: None, .. }` | the existing publish result, unchanged |
| `gf pull` | `[<repository>]`, `--ref`, `--branch`, `--operation-uuid`, `--author`, `--committer`, `--hub`, `--token-env`, `--protection-bypass-env` | (1) download the ref's head package with the clone transport and verify it with `DiscoveryResearchVersionRequest`; (2) `GraphForge::fetch_research_branch`; (3) `preview_research_upstream` with `remote: Some(binding)`; (4) `update_research_branch` with `ResearchUpstreamSelection::AllCompatible` | human: `Merged <remote short uuid> into <ref>: <new short uuid>` or `Already up to date`; JSON: receipt, `version_uuid`, `parents: [local, remote]`, `incorporated`, `conflicts` |

Defaults for `gf push` and `gf pull`: `<repository>` defaults to the
`remote_branches` record of the resolved Branch (`repository_url`), and
`--ref` defaults to that record's `ref_name`, which equals the Branch label. A
Branch with no remote record requires `<repository>` (first push). `gf push`
never takes `--version-uuid`, `--fork-of` or `--preview`; those remain on
`gf publish`, which is unchanged and stays the general publication command.

`gf pull` semantics, stated plainly because they differ from git:

- GraphForge has no working tree. A Branch Version is created by an executed
  operation (ADR 0041), so `gf commit` always carries the mutation it commits.
  There is nothing to stage and no `gf add`.
- Incorporation always records a new Version with parents `[local head, remote head]`
  (ADR 0055 table, "Upstream incorporation"). When the local head has not moved
  since the common base, the content equals the remote head but the Version is
  still new. The native API offers no pointer-only fast-forward and this ADR
  adds none; `gf push` after such a pull fast-forwards on the Hub because the
  new Version descends from the served head.
- When the remote head is already in the local ancestry, `gf pull` performs no
  operation and prints `Already up to date`.
- When the preview reports changes that `AllCompatible` cannot adopt, `gf pull`
  refuses before `update_research_branch`, prints the conflicting fields from
  the preview, and names `gf research upstream update --file` as the explicit
  resolution path. The fetch receipt is durable, so the explicit update reuses
  it.

`gf commit -m` maps the message to the Version's existing `description` field.
`ExecuteResearchBranchRequest`, `ChangeResearchBranchOntologyRequest`,
`CreateResearchBranchRequest` and `UpdateResearchBranchRequest` gain optional
`label` and `description` fields that the Version record already stores
(`research_versions.rs:186-188`, set to `None` by these requests today). The
message's first line is the subject shown by `gf log`.

### 5. Author and committer

Resolution per role, first hit wins:

| Role | Flag | Environment | User configuration |
| --- | --- | --- | --- |
| author | `--author "Name <email>"` | `GF_AUTHOR` | `[user] name`, `email`, `orcid` |
| committer | `--committer "Name <email>"` | `GF_COMMITTER` | `[committer] name`, `email`, `orcid`; absent: same as author |

The user configuration file is `graphforge/config.toml` under
`$XDG_CONFIG_HOME` (default `~/.config`) on Unix and `%APPDATA%` on Windows,
read and written by `gf config user [--name <n>] [--email <e>] [--orcid <o>]`
(no flags prints the resolved author and committer). It is a new file because
`graphforge.yaml` is checked-in repository definition
(`crates/graphforge-api/src/repository/configuration.rs:130`) and identity is
per person, and because `gf auth` keeps sessions in the OS keyring, which is
the wrong place for public credit. The flag and environment forms parse
`Name <email>`; ORCID comes only from configuration. Values are validated by the
existing `ResearchSignature` bounds (ADR 0055) at request time. A verb that
creates a Version with no resolvable author refuses with
`GF_RESEARCH_SIGNATURE_MISSING` and the two ways to set one. The native JSON
verbs keep taking explicit `author`/`committer` in the request and keep
recording none when absent.

### 6. What stays

- `gf research <domain> <verb> --file` verbs are unchanged in behavior and remain
  the documented underlying commands. Each help text gets an example or a
  pointer to the porcelain verb that wraps it (#2126 acceptance).
- `gf clone` and `gf publish` keep their names and flags; `gf publish` grows no
  flag; `gf clone` grows `--tag <name>`.
- Operation identity, replay, fast-forward, `--rewrite`, `--prune`, the
  expected-revision lease, and derived credit are the native operations' and are
  not reimplemented.

### 7. Non-goals and what the siblings consume

Non-goals: tag deletion on a Hub; pointer-only fast-forward; Branch deletion or
rename; a staging area; `gf merge` between local Branches (incorporation is
upstream-only today); Hub refs for tags (tags travel in the lineage, not in
`RefSet.refs`); `gf fetch` as a separate verb (fetch is a durable step inside
`gf pull`; `gf log --remote` covers inspection without installing anything);
Project-context captures through porcelain (they remain
`gf research version prepare` and `commit`).

#2127 consumes: the verb and flag names above for the `graphforge-hub` skill;
the error codes introduced here (`GF_RESEARCH_BRANCH_LABEL_INVALID`,
`GF_RESEARCH_BRANCH_LABEL_TAKEN`, `GF_RESEARCH_BRANCH_UNSELECTED`,
`GF_RESEARCH_TAG_EXISTS`, `GF_RESEARCH_TAG_CONFLICT`, `GF_HUB_TAG_CONFLICT`,
`GF_RESEARCH_SIGNATURE_MISSING`), all of class "fix input", to classify
alongside #1785's Hub codes; and `--token-env` as the non-interactive credential
input that `gf push` and `gf pull` share with `gf clone` and `gf publish`.

#2128 consumes: the native functions each verb maps to (the table in section 4)
so that `clone`, `push`, `pull` and `log` in Python and Node call the same Rust
operations with the same defaults, and the `--json` shapes as the typed result
fields.

## Alternatives

- **`gf research commit` and friends under the research namespace.** Rejected:
  `clone`, `publish`, `hub` and `auth` are already top level, so the workflow
  would straddle two prefixes, and the namespace is where the plumbing lives.
- **Map labels to refs with an explicit table** (a `ref -> branch_uuid` map
  instead of validating labels). Rejected: two names per Branch is the ambiguity
  #1772 complains about, and publish already treats the label as the ref.
- **Tags as the frozen Version `label`.** Rejected: the label is part of the
  identity digest, so it cannot be added after the fact or moved, and nothing
  publishes it. It stays a citation label.
- **Tags as Hub refs in `RefSet.refs`.** Rejected: that changes the Hub wire
  contract (ADR 0053) and the Hub itself; the lineage document already travels
  with every publish and clone and is the natural carrier.
- **Current Branch in a CLI-owned file in the Project directory.** Rejected:
  the storage crate owns the Project root layout; the registry already holds
  local-only durable state (`remote_branches`).
- **A pointer-only fast-forward for `gf pull`.** Deferred, not rejected: the
  native API has no head move without a new Version, and adding one is a
  history-semantics change that belongs to a #1771 follow-up, not to porcelain.
- **`gf commit` capturing the Project workspace.** Rejected: that is a
  Project-context capture, which is not a ref and cannot be pushed; Branch
  Versions are created by executed operations.

## Consequences

- An analyst runs `gf clone`, `gf commit -m ... --query ...`, `gf tag`,
  `gf push`, `gf pull`, `gf log` and `gf status` with no JSON and no UUIDs.
- A Branch label that is not a valid ref cannot be created; older Projects with
  such labels still open and are reported by `gf status`.
- The registry gains two local fields (`tags`, `current_branch`) and two
  mutation variants; the lineage gains an optional `tags` list. All are omitted
  when empty, so existing bytes are unchanged and no revision is bumped.
- Four native requests gain optional `label` and `description`.
- Every porcelain command performs one native operation (or, for `gf pull`, the
  existing fetch, preview and update sequence), so replay, conflict and credit
  behavior is the native behavior and is tested there; porcelain tests assert
  argument mapping and the end-to-end walkthrough only.

## Implementation plan

Ordered steps, each a focused commit on `feat/2126-research-porcelain`:

1. **Label rule.** `crates/graphforge-api/src/branches/model.rs` (validate the
   label with `graphforge_discovery` ref grammar and Project uniqueness);
   `crates/graphforge-api/src/research_versions.rs` (assert `label == ref_name`
   when installing a cloned Branch). Tests:
   `branches::tests::branch_label_must_be_a_valid_unique_ref`,
   `hub_clone::research_clone_tests::clone_refuses_a_lineage_whose_label_differs_from_its_ref`.
2. **Registry state.** `crates/graphforge-storage/src/research_versions.rs`
   (`tags`, `current_branch`, `SetTag`, `SelectBranch`, bounds, validation);
   `crates/graphforge-api/src/research_versions.rs` (`set_research_tag`,
   `select_research_branch`). Tests:
   `research_versions::tests::tags_are_bounded_and_name_known_identities`,
   `research_versions::tests::registry_without_tags_or_current_branch_encodes_as_revision_7`,
   `research_versions::tests::set_tag_refuses_a_moved_target_and_replays_exactly`.
3. **Lineage tags.** `crates/graphforge-discovery/src/lineage.rs`
   (`LineageTag`, validation, canonical order);
   `crates/graphforge-cli/src/hub_publish/history.rs` and `publication.rs`
   (union with served tags, `GF_HUB_TAG_CONFLICT`, `--rewrite`);
   `crates/graphforge-api/src/research_versions.rs` (install tags at clone).
   Tests: `lineage::tests::tags_are_optional_ordered_and_name_lineage_versions`,
   `hub_publish::tests::push_publishes_local_tags_and_refuses_a_moved_served_tag_without_rewrite`,
   `hub_clone::research_clone_tests::clone_installs_lineage_tags`.
4. **Message fields.** Optional `label`/`description` on the four requests;
   `research_versions.rs` records them. Test:
   `branches::tests::execute_records_label_and_description_on_the_new_version`.
5. **Identity configuration.** New `crates/graphforge-cli/src/identity.rs`
   (`gf config user`, resolution order, `Name <email>` parsing). Tests:
   `identity::tests::author_resolves_flag_then_env_then_file`,
   `identity::tests::committer_defaults_to_author`,
   `identity::tests::missing_author_refuses_with_signature_missing`.
6. **Porcelain module.** New `crates/graphforge-cli/src/porcelain.rs` with
   `status`, `log`, `commit`, `branch`, `switch`, `tag`, `push`, `pull`
   (`pull` reuses `hub_clone` download and verification helpers, which move to
   a `pub(crate)` function `download_research_version` without behavior change);
   wire into `Command` in `lib.rs`. Unit tests per verb assert the constructed
   native request (`porcelain::tests::commit_builds_an_execute_request_on_the_resolved_branch`,
   `porcelain::tests::push_defaults_repository_and_ref_from_the_remote_record`,
   `porcelain::tests::pull_refuses_before_update_when_the_preview_has_incompatible_changes`,
   `porcelain::tests::pull_is_a_no_op_when_the_remote_head_is_an_ancestor`,
   `porcelain::tests::branch_resolution_order_is_flag_current_then_only`).
7. **Walkthrough.** `crates/graphforge-cli/tests/research_porcelain_walkthrough.rs`,
   driving the built `gf` binary (`env!("CARGO_BIN_EXE_gf")`, as
   `research_journey.rs:24` does) against the reference Hub started in-process
   the way `hub_publish/tests.rs::LoopbackHub::start` wraps
   `graphforge_hub_publish::reference::ReferenceHub::with_config`
   (`crates/graphforge-hub-publish/src/reference.rs:82`), with `GF_AUTHOR` set
   in the child environment. Steps and assertions, mapping #2126 acceptance:
   - `porcelain_walkthrough_clones_commits_tags_pushes_pulls_and_logs`:
     Alice publishes a Branch `main`; Bob `gf clone --ref main`; `gf status`
     shows `main`, upstream, `ahead 0`; Bob `gf commit -m "first" --query ...`
     and `gf commit -m "second" --query ...`; `gf tag v1`; `gf push` (no
     arguments); Alice `gf pull`, then `gf commit`, then `gf push`; Bob
     `gf pull`; Bob `gf log` lists both of his commits, Alice's, and the merge
     Versions with the expected parents; `gf log --remote` agrees with the
     served lineage; `gf log v1` through the tag equals the second commit; a
     third `gf clone --tag v1` materializes the second commit's content.
   - `porcelain_walkthrough_preserves_replay_and_fast_forward`: rerunning
     `gf commit` with the printed `--operation-uuid` returns the original
     receipt and no new Version; `gf push` from a stale clone is refused
     exactly as `gf publish` is (`GF_HUB_NON_FAST_FORWARD` class), and
     `gf push --rewrite` succeeds under the lease.
   - `porcelain_walkthrough_uses_no_handwritten_json`: the test constructs no
     request body; asserted by the test containing no `--file` invocation
     (a grep in the test over its own argv lists).
8. **Help and docs.** Every `gf research` help text gains an example or a
   pointer (`command_retirement_tests.rs` gains
   `every_research_help_text_has_an_example_or_pointer`, parsing `--help`
   output); `crates/graphforge-cli/README.md` gains a "Research workflow"
   section with every porcelain verb; `docs/book/README.md` quick start and
   `docs/book/research/README.md` are updated with the implemented names;
   `python3 scripts/ci/adr-index.py generate` is rerun when this ADR moves to
   Accepted.

Step 7 is the close evidence for #2126's walkthrough criterion. Steps 1 to 6
each land with their unit tests; no step introduces a behavior the native API
does not already provide except the two state fields and the message fields
named above.

## Implementation notes

Recorded when #2126 landed; each is a refinement of the text above, not a change
of decision.

- **Registry revision.** The registry is revision 9, not 7. `tags` and
  `current_branch` are omitted when empty, so existing bytes and identity digests
  are unchanged and no revision is bumped
  (`registry_without_tags_or_current_branch_keeps_its_bytes`).
- **Error codes.** The `GF_*` codes in section 7 are stable prefixes of the
  validation message, the convention `hub.publish.*` uses, not new variants of the
  closed public `ApiErrorCode` set. Tag names are checked against the ref grammar
  at the API boundary (`set_research_tag`) and structurally in storage, which does
  not depend on the discovery crate. The `research` feature of `graphforge-api`
  now implies `discovery` for the Branch label grammar.
- **`--ref` selects a Branch.** Because the label is the ref, `gf push --ref X` and
  `gf pull --ref X` select the Branch labelled `X`; naming a different `--branch`
  is an error. `CreateResearchBranchRequest` gains `description` only (its label
  already is the Branch label); the other three requests gain `label` and
  `description`.
- **Operation identity.** A caller-supplied `--operation-uuid` must be a UUIDv7.
  The creation time derives from it. A rerun is a replay only when its complete
  request matches the original, otherwise it is refused as
  `GF_IDEMPOTENCY_CONFLICT`. `commit`, `branch` and `pull` bind a digest of the
  whole request (kind, Branch, message, query digest, credit, `--from`,
  repository) into the identity of the Version they create, so a receipt whose
  Version carries a different identity belongs to another request. `tag` and
  `switch` carry that digest in the native receipt (`intent_sha256`), like the
  other Branch operations, and the native API replays or refuses before it looks at
  CURRENT. A replay is answered from the local receipt ledger, with no network.
  A request that names its target relatively (`gf tag v1` at the head) is the
  request as resolved; pass the Version to replay it after the head moved.
- **First Branch.** `gf branch <name>` in a Project with no Branch creates it from
  the Project's current research.
- **Upstream record.** A publication the Hub confirmed (`gf push`, `gf publish
  --ref`) records the published head as the Branch's remote citation, the record
  clone and fetch maintain (`ResearchMutation::RecordPublishedBranch`, built by
  `record_research_publication` from native Branch authority and the typed Hub
  receipt, which must be for the repository and carry the Branch's ref; the
  generic operation entry refuses it). It is local tracking state, not shared
  history. A publication's intent binds `publication_content_identity`, which
  leaves out the remote citations and receipts a publication itself writes, so
  rerunning a completed `gf push --operation-uuid` replays it. A Branch's first confirmed publication therefore
  makes it track that repository and ref, and `ahead N` is `0` right after a push.
  A failed or refused publication leaves the record unchanged, and a Version
  publication records nothing.
- **Tags on clone and pull.** A lineage tag is installed with the Version it
  names: at clone for Versions the cloned history contains, after `gf pull` for the
  rest. Tags on other Versions stay on the Hub.
- **Pull conflicts.** `conflict` and `dependency_unavailable` preview rows refuse
  the pull before any update; the fetch is durable. The evidence acknowledgement is
  the union required by the adopted rows.
- **Identity file.** `config.toml` is read and edited with `toml_edit`:
  `gf config user` changes only the keys it is given and keeps comments, quoting
  and every unrelated key and table.
- **Walkthrough.** The product transport speaks only HTTPS to public addresses, so
  a child `gf` cannot reach an in-process Hub. The Hub-facing walkthrough runs the
  CLI's own parser and dispatcher in-process over the loopback transport every
  publish and clone test uses (`hub_publish::tests::porcelain`), and the built
  binary drives the verbs that need no Hub
  (`tests/research_porcelain_walkthrough.rs`).

