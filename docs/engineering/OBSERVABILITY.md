# Observability

GraphForge is local-first and embedded: there is no multi-tenant production service with
classic uptime SLOs. “Production” means published artifacts running in user notebooks, agent
loops, and CI. Observability therefore emphasizes **correctness signals, release health, and
developer/agent outcomes**, feeding learning back into private
[experience journeys](https://github.com/CurateLabs/graphforge-nextjs/tree/main/docs/experience).

Roadmap merge gates also name product-facing diagnostic surfaces: `explain`, query IDs,
provenance IDs, and structured errors ([`../releases/roadmap.md`](../releases/roadmap.md)).

## Observable outcomes

| Outcome / requirement | Signal | Source | Expected range | Owner |
| --- | --- | --- | --- | --- |
| FR-1 / NFR-1 Cypher correctness | TCK runnable pass count | CI BDD / release gates | Full corpus green on release lineage | Maintainers |
| FR-2 / NFR-7 Surface completeness | Non-Cypher inventory + evidence digests | Contract gate artifacts | No unclassified public methods | Maintainers |
| FR-3 Project durability | Reopen / recovery tests | Lifecycle & checkpoint gates | Pass on changed surface | Maintainers |
| FR-6 Binding parity | Arrow/IPC equality, structured codes | Binding RC / concurrency suites | Parity within published surface | Maintainers |
| FR-8 Agent DX | Structured error codes vs prose-only failures | Facade + skills adapter tests | Stable codes on known failure classes | Maintainers |
| Query diagnosability | `explain` / query ID availability | Architecture observability surfaces | Present on supported paths | Maintainers |
| Clean-install success | Quickstart smokes after publish | Manual/release verification | Pass from public registries | Release operator |
| Docs usability | Docs build; link integrity | `docs.yml` / Starlight (`docs-site/`) | Green build | Docs owners |
| Scale honesty | Fixed-hop LIMIT materialization ratios | Scale-limit CI gate | ≤3× rows on 10× edges for LIMIT 1000 shape | Maintainers |

## Service health

| Service / journey | Indicator | Objective | Window |
| ----------------- | --------- | --------- | ------ |
| CI on `main` | Required workflows at merge SHA | Green after merge | Per merge |
| Release candidate | Aggregate gate artifacts for the release | All applicable gates green on one SHA | Per release |
| Local embedded use | Structured API errors vs crashes | Fail closed with codes; no silent corruption | Per user session |
| Agent skills loop | Offline smoke + compatibility JSON | Deterministic pack hash; fail-closed versions | Per skills change / RC |
| Docs site | Starlight build/deploy success | Site serves current allowlisted `docs/` | Per docs change |

Embedded library use does not define availability SLOs for a network endpoint.

## Telemetry design

- **Events:** CI gate results, release dry-run outcomes, contract inventory digests — not
  end-user product analytics by default.
- **Logs:** structured GraphForge / adapter error codes; local diagnostics suitable for
  notebooks and agents; query IDs where emitted for correlating `explain` output.
- **Metrics:** CI durations, coverage thresholds where enforced, scale-limit gate ratios
  (fixed-hop LIMIT materialization bounds), skills pack SHA equality.
- **Traces:** optional local/dev tracing only if introduced; do not invent SaaS APM here.
- **Product diagnostics (in-process):** `explain`, provenance IDs, and structured errors as
  user-visible observability — not third-party telemetry.

Cardinality and privacy: do not ship user graph contents to third-party telemetry. Prefer
aggregate CI signals.

## Dashboards and alerts

| Signal | Trigger | Severity | Owner | Response |
| --- | --- | --- | --- | --- |
| Required CI red on `main` | Workflow failure at merge SHA | High | Maintainers | Fix forward or revert; never ignore |
| TCK / contract gate regression | Fail on PR or release dispatch | High | Surface owners | Root-cause; add regression coverage |
| Binding / skills RC failure | Parity or offline smoke red | High | Binding/skills owners | Root-cause; no wrapper-test substitution |
| Docs build failure | `docs.yml` / Starlight build red | Medium | Docs owners | Fix content or site config |
| Publish dry-run failure | Registry reject | High | Release operator | Stop publication; recover per plan |
| Scale-limit shape regression | Materialization ratio gate fail | Medium | Execution owners | Investigate adjacency/fetch path; update docs if limits change |

There is no hosted ops dashboard product; CI and release artifacts are the shared “dashboard.”

## Privacy and safety

- User graphs and project files stay on the user’s machine unless they opt into external tools.
- CI logs must avoid dumping sensitive fixture contents; redacted golden outputs for skills.
- Agent-skills security contract: no private path leakage, no reflective native exception
  text, bounded recursive payloads ([`../agent-skills.md`](../agent-skills.md)).
- License/compliance automation reports metadata, not customer datasets.

## Production learning loop

1. CI and release gates surface correctness or DX failures.
2. Maintainers file bounded issues (root cause, not log-line spam) per `AGENTS.md` failure handling.
3. Update private
   [experience journeys](https://github.com/CurateLabs/graphforge-nextjs/tree/main/docs/experience)
   when a recurring user/agent friction appears.
4. Promote validated needs into private
   [`REQUIREMENTS.md`](https://github.com/CurateLabs/graphforge-nextjs/blob/main/docs/REQUIREMENTS.md)
   and tests.
5. Docs narrative and Starlight site reflect current behavior — not historical archaeology.
6. Strategy/roadmap pages stay linked summaries; do not fork a second milestone tracker here.
