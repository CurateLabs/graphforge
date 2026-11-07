# Automate GraphForge with an agent

**Advanced · Assumes basic Python, database, and terminal skills.**
For a first graph and a prompt you can check, use the
[Basic agent lesson](work-with-an-agent.md).

Choose the [runtime for your task](installation.md#choose-a-runtime-for-your-task)
before running commands. The Hub research and credential examples below use
`GF_EVALUATION_CLI`, the absolute path to the standalone Rust executable from
the [pinned source build](installation.md#build-from-source), and its matching
Python environment. They do not use a registry rc.7 launcher or a moving
`next` package. Building this source is a developer route; historical
candidate qualification does not qualify your local build or a hosted write target.

## Work in VS Code

The optional [GraphForge extension](vscode-extension/) provides project and
result views. Follow its [runtime setup guide](vscode-extension/install.md).
A coding agent can also use the Python path from the editor's terminal.

Structured extension commands require an agent integration that can call
VS Code's command API. Some commands still need a human to select an item or
complete a dialog. Consult [agent interop](vscode-extension/agent-interop.md)
for that boundary; installing the extension does not connect every chat agent
automatically.

Packaged editor and agent first-use qualification for v0.6.0 is tracked in
[#1209](https://github.com/CurateLabs/graphforge/issues/1209). This shell-based
Python route does not establish that any branded agent integration is qualified.

## Add repository guidance when useful

For work in a Git repository on supported durable storage, the selected CLI
can initialize the project and install repository-local skills:

```bash
"$GF_EVALUATION_CLI" init
"$GF_EVALUATION_CLI" skills status
```

Initialization creates GraphForge repository definitions and manages data
exclusions. It is optional for the in-memory quickstart. Existing projects can
use `"$GF_EVALUATION_CLI" skills install`. See [repository integration](repository-integration.md)
for managed files and updates.

The installed skills teach repository setup, knowledge construction and the Hub
research workflow (`graphforge-hub`). Their
version compatibility must match the engine; release qualification includes
updating and testing that compatibility for v0.6.0. Skills are instructions,
not a connection or permission system for an agent.

## Continue with a specific task

Ask the agent to [save and reopen the graph](tutorial.md) when you want durable
work. Later, ask it to [record an inquiry](record-an-inquiry.md): state the
hypothesis, challenge it, show the evidence, record the bounded conclusion,
and retrieve it before a related inquiry. You can use these steps without
learning the collaboration features.

## Give an agent a Hub research task

Use the selected standalone `"$GF_EVALUATION_CLI"` from the source-build shell.
The Hub skill is installed in the **code repository** holding agent guidance;
`--project` selects the separate downloaded data Project. On a fresh repository:

```bash
"$GF_EVALUATION_CLI" --project-dir . --json skills install
```

The public CLI returns this receipt (a second install returns `changed: false`):

```json
{"bundle_version":2,"changed":true,"installed_files":3}
```

The guidance lives at `.agents/skills/graphforge-hub/SKILL.md`. Existing bundle
version 1 installations can use `"$GF_EVALUATION_CLI" skills update`; edits are reported as a
conflict for review before replacement. Skills are instructions, not execution
permissions or credentials.

Give the agent a bounded task such as:

> Read the installed graphforge-hub skill and the CLI help. Clone
> opendata/openalex into a new openalex directory and inspect status. If it
> has no research Branch, create main with my approved credited identity before
> committing. After the approved commit, show its Version and parents, then
> inspect the newest ten Versions. Report the actual returned identifiers.
> Before writing, show me the proposed Cypher mutation and credited author.
> Do not push until I have supplied a verified publish-capable Hub and its
> repository-scoped credential. Preserve JSON receipts and report any failure.

The implemented command names are:

```bash
"$GF_EVALUATION_CLI" --json clone opendata/openalex openalex
"$GF_EVALUATION_CLI" --json --project openalex status
"$GF_EVALUATION_CLI" --json config user --name Research-agent
"$GF_EVALUATION_CLI" --json --project openalex branch main
"$GF_EVALUATION_CLI" --json --project openalex commit -m Review --file changes.cypher
"$GF_EVALUATION_CLI" --json --project openalex log -n 10
"$GF_EVALUATION_CLI" --json --project openalex tag reviewed
"$GF_EVALUATION_CLI" --json --project openalex log reviewed -n 2
```

`config user` returns the credited identity, without any access credential:

```json
{"author":{"name":"Research-agent"},"committer":{"name":"Research-agent"}}
```

`commit` executes the approved mutation from `changes.cypher` on the selected
Branch and returns `operation_uuid`, `generation_uuid`, `version_uuid`, `parents`
and `replayed`. Retain an operation's UUIDv7 before running it by supplying
`--operation-uuid`; retry with the same identity and exactly the same intent.
`log` returns an array containing Version UUIDs, parent UUIDs, author, committer,
creation time, description and tags. These are research Versions and Branches;
the code repository's Git history is separate. A tag names one Version and
cannot silently move. `gf branch` lists or creates Branches; `gf switch` selects
one for subsequent operations.

For collaboration, supply the verified Hub URL and use `gf push`, `gf pull` and
`gf log --remote`. `--ref` names the research Branch's ref consistently across
clone, push, pull and remote log. A conforming **write-capable** server must serve
`graphforge-hub-publish/1`; public browse/clone availability alone does not prove
that the production Hub offers publishing. Core collaboration tests exercise the
reference Hub through the native test transport. That is not a production
publishing result. See [portable projects](portable-projects.md) for exchanging
work when no write target is qualified.

## Provision scoped credentials for an agent or CI

The explicit noninteractive path is an unexpired bearer token for exactly
`publish:<owner>/<repository>`, injected from the CI/agent secret store. For
example, provision `GF_AGENT_PUBLISH_TOKEN` for `publish:opendata/openalex` and
run the following against the **placeholder** Hub URL, replaced by the verified
target (select the local `main` Branch first):

```bash
"$GF_EVALUATION_CLI" --json --project openalex push https://hub.example/opendata/openalex --ref main --token-env GF_AGENT_PUBLISH_TOKEN
```

`--token-env` names an environment variable, never the secret value. With that
flag omitted, push and publish use `GRAPHFORGE_HUB_PUBLISH_TOKEN`. An explicit
token takes precedence over saved sessions and device flow. Tokens stay out of
argv, URLs, Project files, receipts and logs. Rust sends the publish bearer only
to the selected Hub's control origin, never to OAuth, object-read or upload URLs.
The Hub enforces repository scope and expiry on its control requests. A publish
token is not a read token; configure read-side `--token-env` separately when the
selected Hub requires authenticated clone, pull or remote log.

A token must be acquired **before** the unattended job starts. The existing
publish issuer contract gives a concrete provisioning path:

1. Fetch `GET https://<hub>/<owner>/<repository>/.gf/publish`. The
   `authorization` object declares `device_authorization_endpoint`,
   `token_endpoint`, `scope` (exactly `publish:<owner>/<repository>`) and optional
   `client_id` (default `graphforge-cli`). These must be
   credential-free public HTTPS URLs. Do not send an existing publish bearer
   to these OAuth endpoints.
2. A human provisioning client POSTs URL-encoded `client_id` and `scope` to the
   advertised device endpoint. Its response supplies `device_code`, `user_code`,
   `verification_uri`, `expires_in` and polling `interval`. The repository owner
   approves the user code at the verification URI before it expires.
3. That client polls the advertised token endpoint with URL-encoded `client_id`,
   `device_code` and `grant_type=urn:ietf:params:oauth:grant-type:device_code`.
   `authorization_pending` means continue; `slow_down` adds five seconds between
   polls. A successful response contains `access_token`, `token_type: Bearer`,
   `expires_in` (seconds) and the granted `scope`.
4. Check the exact scope and expiry, transfer the returned `access_token`
   privately into the secret store, and inject it only into the intended job.
   Schedule the job within that lifetime; re-provision when expired or revoked.
   A Hub offering longer-lived CI credentials must document its own issuer
   procedure. Core does not implement unattended identity bootstrap.

This is the existing Rust-owned
[publish issuer contract](../reference/hub-publish/v1/README.md), not a claim
that the hosted Hub has deployed a new issuer. A provisioning client handles
the secret response privately. Interactive `gf publish` consumes its device
flow token in memory and never prints it for copying or exporting to CI.

For the streaming CLI process, a saved session is another noninteractive path:
a human runs `gf auth login --hub <hub>` once, with the session stored only in
the OS credential vault. Subsequent CLI invocations refresh it as needed and
POST to the **same origin** at `/api/cli/repository-token`, with the saved access
bearer and the following request body for this example:

```json
{"owner":"opendata","name":"openalex","capability":"publish"}
```

The response must include `access_token`, `token_type: Bearer`, positive
`expires_in`, `repository_id` and `origin`. A publish grant for another origin
is refused. Rust uses the scoped token in memory; it never exports the vault
session. Captured binding invocations require an explicit token and do not
consult the vault or prompt. Newer PyPI/npm console launchers use native streams
instead; that repair is separate from the pinned standalone CLI selected here.
Do not initiate interactive login from an unattended agent. Saved-session login and private reads require their separately
qualified Hub endpoints; the explicit-token path does not require either.

## Interpret actual tool results

Pass `--json` and keep stdout and stderr separate. Success JSON is on stdout;
an argument or runtime failure writes a JSON error on stderr. For example,
`gf --json clone 'Not An Identity'` exits 2 and returns:

```json
{"error":{"code":"GF_VALIDATION","message":"validation error: hub.invalid_identity: invalid repository identity","details":{"source":"runtime","kind":"validation","semantic_code":"hub.invalid_identity"}}}
```

Use `error.details.semantic_code` first, then per-diagnostic
`error.diagnostics[].code` for multi-ontology failures, then outer `error.code`.
Never classify by message text or exit status alone. For example,
`hub.network` is **retry**; `hub.invalid_identity` and missing
`hub.publish.auth_denied` credentials are **fix input**;
`hub.publish.entitlement_denied` and an operation-identity
`GF_IDEMPOTENCY_CONFLICT` require **a human**. A diagnostic such as
`inventory.generation_conflict` can instead make a coarse outer conflict
retryable. The [CLI error contract](../reference/cli-errors.md) lists every
supported code, class and next action.

The installed-skill product tests check these receipt/identity/error examples
against the real public CLI and every skill command/flag against its help.
The existing error-contract tests produce actual retry, fix-input and
ask-a-human failures. The native credential tests check requested repository
and capability, expired/invalid grants, same-origin publish enforcement and
secret redaction. Evidence from local tests does not establish a branded agent
integration or production Hub qualification.
