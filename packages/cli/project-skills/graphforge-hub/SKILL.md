---
name: graphforge-hub
description: Clone a Hub project, inspect research history, commit changes, and push or pull through the Rust-owned GraphForge CLI.
---

# GraphForge Hub research workflow

Use the installed `gf` CLI (the Python/npm launcher also accepts `graphforge`).
Run commands with `--json`; retain stdout results and stderr errors separately.
Use supported durable storage (`ext4`, `xfs`, or `btrfs`). Ask the researcher to
choose the repository, ref, credited identity and intended mutation.

## Clone and inspect

```bash
gf --json clone opendata/openalex openalex
gf --json --project openalex status
gf --json --project openalex log -n 10
gf --json --project openalex branch
gf --json --project openalex tag
```

Clone uses the advertised default Branch. To select a different ref, pass
`--ref`; a retained Version can be selected by `--tag` or `--version-uuid`.
Do not assume every project has a Branch: inspect the result and history first.
The downloaded project is separate from the Git repository holding guidance.

## Record a change

Put the researcher-approved Cypher mutation in `changes.cypher`. Commit runs
that mutation on the selected research Branch and captures a Version. Configure
credit with `gf config user`, or give `--author` for this invocation. Credit is
public attribution, not an access credential.

```bash
gf --json --project openalex commit -m Review --file changes.cypher --author Research-agent --operation-uuid 01900000-0000-7000-8000-000000000001
gf --json --project openalex tag reviewed
gf --json --project openalex log reviewed -n 2
```

Choose a fresh UUIDv7 for each intentional operation; the sample UUID above is
illustrative. Persist that identity before invoking a mutation and reuse it
with exactly the same arguments on a retry. A new intended change needs a new
identity. A tag names one Version and cannot silently move.

To work on another Branch:

```bash
gf --json --project openalex branch review
gf --json --project openalex switch review
```

## Publish and incorporate changes

Push/pull require an explicitly verified, conforming write-capable Hub serving
`graphforge-hub-publish/1`. Public clone availability does not prove publishing
is deployed. Obtain the target URL and permission from the Hub/repository owner.
The following `hub.example` URL is a placeholder, not an available service.

```bash
gf --json --project openalex push https://hub.example/opendata/openalex --ref main --token-env GF_AGENT_PUBLISH_TOKEN
gf --json --project openalex log --remote https://hub.example/opendata/openalex --ref main
gf --json --project openalex pull https://hub.example/opendata/openalex --ref main
```

Select `main` locally before these examples if another Branch is current.
`--ref` is the research Branch's ref label across clone, push, pull and remote
log. After an upstream is recorded, repository/ref defaults can be omitted.
Pull fetches and incorporates the remote head; a divergent history needs an
explicit decision. Ask before using `--rewrite` or `--prune`.

## Noninteractive credentials and acquisition

For an agent/CI job, provision the scoped token **before** starting the job:

1. Read `GET https://<hub>/<owner>/<repository>/.gf/publish`. Its
   `authorization` names `device_authorization_endpoint`, `token_endpoint`,
   optional `client_id` (default `graphforge-cli`) and the exact
   `scope` (`publish:opendata/openalex` for this example).
2. A human provisioning client POSTs form fields `client_id` and `scope` to
   the advertised device endpoint. The response supplies `device_code`,
   `user_code`, `verification_uri`, `expires_in` and `interval`. A repository
   owner approves the user code at that URI.
3. The provisioning client polls the advertised token endpoint with form fields
   `client_id`, `device_code` and
   `grant_type=urn:ietf:params:oauth:grant-type:device_code`, respecting
   `authorization_pending` and `slow_down`. Success returns `access_token`,
   `token_type=Bearer`, `expires_in` (seconds) and `scope`. Check the granted
   repository scope and arrange the job to finish before expiry.
4. Transfer `access_token` directly into the agent/CI secret store, inject it as
   `GF_AGENT_PUBLISH_TOKEN`, and run the push example. Reprovision expired or
   revoked tokens. A Hub offering longer-lived CI tokens must document its own
   issuer procedure; Core supplies no unattended identity bootstrap.

These are the existing `graphforge-hub-publish/1` issuer contracts implemented
by Rust, not a new hosted endpoint or a `gf` token-export command. The
provisioning client must handle the secret response privately; `gf publish`'s
interactive device flow consumes its token in memory and never prints it.
`gf push --token-env` names the injected variable, never its value. Alternatively,
inject `GRAPHFORGE_HUB_PUBLISH_TOKEN`, the default for both push and publish.
An explicit token takes precedence over saved sessions and device flow.

A human can instead run `gf auth login` once for the selected Hub. The streaming
CLI process can then run without a terminal: Rust refreshes the saved OS-vault
session as needed and POSTs `/api/cli/repository-token` on that same Hub origin,
with the session bearer and JSON `{"owner":"opendata","name":"openalex","capability":"publish"}`.
The grant has `access_token`, `token_type=Bearer`, positive `expires_in`,
`repository_id` and `origin`; publishing rejects any origin other than the
selected Hub. This exchange keeps the scoped token in memory. Captured/native
binding invocations require an explicit token and never read the vault or
prompt. An agent must not initiate interactive login or export a vault token.

Never put tokens in argv, URLs, project files, receipts or transcripts. A publish
token is repository-scoped by the Hub and sent only to the selected control
origin, never to object hosts, upload capabilities or OAuth endpoints. A publish
token is not a read credential. If the configured Hub requires authenticated
reads, provision a separate read token and opt in using `--token-env` on clone,
pull or remote log. Private-read qualification is a separate deployment concern.

## Handle errors

Read `error.details.semantic_code` first, then `error.diagnostics[].code` for
multi-ontology failures, then `error.code`. Never branch on message text or use
exit status as a semantic classifier. The complete code/class/action table is
[the CLI error contract](https://github.com/CurateLabs/graphforge/blob/main/docs/reference/cli-errors.md).

- **retry:** `hub.network`, `hub.rate_limited`, `hub.unavailable`,
  `hub.publish.ref_conflict`, or `GF_WRITER_BUSY`. Respect rate limits, refresh
  when needed, and retry the same intended operation with its saved identity.
- **fix input:** `hub.invalid_identity`, `hub.publish.auth_denied`, or
  `hub.publish.non_fast_forward`. Correct the repository or supplied secret;
  fetch/pull before retrying a stale publication. Escalate denied permissions.
- **ask a human:** `hub.access_denied`, `hub.publish.entitlement_denied`,
  `hub.publish.internal`, or outer `GF_IDEMPOTENCY_CONFLICT`. Preserve the error
  and receipt; ask for access, quota, operator help or an identity decision.

Report actual returned identifiers, parents, tags and receipts. Say which
commands ran, where they ran, and which step failed. A local/reference-Hub test
is not evidence of production publication.
