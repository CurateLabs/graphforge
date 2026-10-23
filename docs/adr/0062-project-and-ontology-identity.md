---
title: "ADR 0062: Projects are named like repositories; ontologies and bridge sets have permanent IDs like gists"
adr: "0062"
status: "Accepted"
date: "2026-10-22"
superseded_by: null
revisit_when: "A second kind of item needs a permanent ID, a published ID must change, or squatting on unpublished IDs becomes a real problem"
---

# ADR 0062: Projects are named like repositories; ontologies and bridge sets have permanent IDs like gists

**Status:** Accepted

> **Shared GraphForge decision, public revision 1.** Curate Labs maintains this text in one place and publishes it unchanged to [graphforge](https://github.com/CurateLabs/graphforge/blob/main/docs/adr/0062-project-and-ontology-identity.md), and the Hub repository. Do not edit this copy: changes are made at the source and re-published to every repository.

## Context

Two kinds of things on the Hub need names, and they age differently.

- A **Project** is found, browsed and cloned by people. Its name,
  `owner/repo`, is how people find it. Core's discovery protocol already
  records a repository move (`moved_from`), so clients can follow a rename or
  transfer.
- An **ontology module** is referenced by data. Its `ontology_id` is part of
  the module identity (ADR 0023), every qualified symbol includes it, and two
  modules are the same only when the IDs match exactly. A redirect can rescue a
  URL that a person visits. Nothing rescues an identifier stored in someone
  else's data. **Bridge sets** are the same: they are versioned, owned
  independently of the modules they connect (ADR 0023), and referenced by exact
  `bridge_id`.

The Hub's first real fixture showed the cost of mixing these up. It is a
GraphForge-maintained mirror of OpenAlex, but it was published as
`openalex/openalex` with the module ID `https://openalex.org/ontology/works`.
The name implied that the OpenAlex owners publish it, and the module ID minted
an IRI in a domain GraphForge does not control. Renaming the Project is cheap.
Renaming the module ID changes every term in every dataset that uses it.

GitHub gists and Twitter/X posts handle this well. Each has an owner, but the
owner is not part of the ID: `gist.github.com/<id>` and
`x.com/<anything>/status/<id>` reach the same item, and a handle change or
transfer breaks nothing.

## Decision

### 1. Projects keep repository identity

A Project is `graphforge.sh/<owner>/<repo>`, as today. Renames and transfers
behave like GitHub repositories: Core's protocol records the move and clients
follow it. This ADR does not change Project identity.

### 2. Three kinds of identity, chosen by how a thing is used

| Used as | Identity | Examples |
|---|---|---|
| A container people browse and clone | A name, `owner/repo` | Projects |
| An independently owned, versioned thing that other data references | A permanent ID | Ontology modules, bridge sets |
| Immutable bytes | A content digest | Packages, objects, visualization previews |

Versions keep their existing identities (Version UUID, digests). A new kind of
item gets a permanent ID only if other data references it independently of the
Project that published it. Otherwise a name or a digest is enough.

### 3. Permanent IDs are typed and never contain an owner

A permanent ID is a typed, opaque token:

```text
<prefix>_<16 characters of lowercase Crockford base32>     for example ont_7k2m9x4q8r3t5v1w
```

| Prefix | Kind | Field |
|---|---|---|
| `ont_` | Ontology module | `ontology_id` |
| `brg_` | Bridge set | `bridge_id` |

- A further kind gets its own three-letter prefix by amending this ADR.
- The 16 random characters carry 80 bits, so accidental collisions are not a
  practical concern.
- Data always uses the lowercase form. Input is case-insensitive, but only
  case is folded. Crockford's `i`/`l`→`1` and `o`→`0` aliases are rejected, so
  each accepted spelling names exactly one ID. Accepting them later would be
  backward compatible.
- Core's validator in `graphforge-discovery` is the single implementation of
  this grammar. The Hub uses it through protocol-wasm and never re-implements
  it.
- An item has one ID for its whole life, and its history is a series of
  versions under that ID. Editing and saving change the item's content, and
  publishing adds an `authored_version`; neither creates a new ID. The only new
  ID comes from regenerating after the Hub refuses the old one with `id_taken`
  (section 6).
- An ID never changes after first publish and is never reused. A deleted item
  leaves a tombstone: its URL answers `410 Gone`, and its ID cannot be claimed
  again.

### 4. One canonical URI, several ways to reach it

The ID written into data is always `https://graphforge.sh/<id>`. It satisfies
ADR 0023's requirement that `ontology_id` is a globally unique URI. Qualified
symbols keep Core's `{ module, kind, local_id }` shape. The version is not part
of the ID; it stays the separate `authored_version`.

The Hub resolves every one of these forms to the same item:

| Form | Example |
|---|---|
| Bare ID (CLI, search) | `ont_7k2m9x4q8r3t5v1w` |
| Canonical | `graphforge.sh/ont_7k2m9x4q8r3t5v1w` |
| Typed | `graphforge.sh/ontology/ont_7k2m9x4q8r3t5v1w`, `graphforge.sh/bridge/brg_…` |
| Owner | `graphforge.sh/<owner>/ont_7k2m9x4q8r3t5v1w` |
| Exact version | `graphforge.sh/ont_7k2m9x4q8r3t5v1w/<authored_version>` |

- **The canonical URL answers directly** with `200`. The typed, owner,
  uppercase and outdated-owner forms permanently redirect (`301`) to it. People
  copy what the address bar shows, so it always shows the form that belongs in
  data.
- **The canonical URL describes the current version**, and
  `/<id>/<authored_version>` describes one exact version.
- **Content negotiation:** the URL answers browsers with an HTML page and tools
  with a Core-defined item descriptor, `graphforge-item/1` (media type
  `application/vnd.graphforge.item+json`), with `Vary: Accept`. The descriptor
  carries the `id`, the `kind`, the publishing `repository` and the module's
  exact identity. The client then fetches and verifies the module through the
  existing refs → manifest → module package path, so there is no new trust path,
  and module bytes never pass through the Hub's web tier. The Hub serves only
  documents Core defines (ADR 0054). If Core later defines an RDF projection of
  modules, it is served the same way. The Hub never converts formats itself.

Two naming rules keep these paths unambiguous:

- Owner handles and repository names may not have the shape of an ID
  (`^[a-z]{3}_[0-9a-hjkmnp-tv-z]{16}$`). Ordinary names, including names with
  underscores, are unaffected.
- `ontology` and `bridge` are reserved owner names.

### 5. The owner is a property of the item, not part of its ID

Every permanent ID on the Hub has exactly one owner, a user or an organization.
The owner controls who may publish new versions, is shown as the publisher on
every page, and lists the item on the owner's page. Ownership can be
transferred; the ID does not change.

### 6. Core creates the ID, and the first publish claims it

Core creates an item's ID locally when the item is created, so references are
written once, offline, before the Project is ever published. Publishing never
rewrites an item.

At publish, the Hub checks each permanent ID in the advertised `ontology`
section of each commit manifest it admits. It never looks inside packages.

- **New ID.** The publish registers it, and the publisher becomes its owner.
- **Owned by the publisher.** New versions publish normally.
- **Owned by someone else.** Including a version that is already published,
  with the same `(id, version, content_digest)`, is reuse and is allowed
  (ADR 0051). Publishing any other content under that ID is refused with a
  stable error, `id_taken`.

One publish can hit several IDs, so `id_taken` carries a sorted list, `items`,
with one entry per refused ID: `{kind, id, owner?, publish_as_owner?}`. It is an
optional member of `hub-publish/1` with no capability negotiation, because
nothing before v1 needs backward compatibility.

An entry names the current `owner` only when the publisher can already see the
item. Otherwise it says only that the ID is taken, so the error never reveals a
private item. When the publisher belongs to the owning organization,
`publish_as_owner` says so, and the error first suggests publishing as that
organization.

Otherwise the publisher regenerates the ID locally. Core allows this only for
an ID the Hub has refused for that publisher, and requires the locally stored
`id_taken` receipt for that exact ID. If the receipt says the publisher can
publish as the owning organization, Core refuses to regenerate and points there
instead. A new ID for something you own would be a fork, which this ADR does not
cover.

Core asks first, then creates a new ID, rewrites the item and every reference to
it in the Project, and records the result as a new Version. History is never
rewritten. Older Versions keep the old ID in their history. They are carried
only as lineage entries and are never advertised, so they don't conflict. The
user then publishes the new Version.

Core does not prove who created an ID before its first publish. With random IDs,
the only way to collide is to copy someone's unpublished ID. That costs the
victim a local regeneration of an unpublished module, and nothing published is
affected.

### 7. Only mint IDs in a domain you control, and keep the domain

New modules and bridge sets authored in GraphForge get GraphForge IDs. Authors who control
another domain may use an IRI in that domain instead. Nobody, including
GraphForge's own fixtures and examples, mints IRIs in a domain they do not
control. The Hub registers and protects only GraphForge IDs. Core enforces the
canonical form only for IDs on `graphforge.sh`; other IRIs, and existing short
or legacy IDs, are unchanged.

These URIs outlive any one version of the Hub. Curate Labs holds `graphforge.sh`
as a permanent namespace. If the Hub ever shuts down, `graphforge.sh/<id>` keeps
resolving, at minimum by redirecting to an archive of each item's published
versions.

## Consequences

- Data that uses a GraphForge ontology keeps working across Project renames,
  transfers and owner changes. Only display URLs redirect.
- A module ID means nothing on its own. The title, owner and pages carry the
  meaning, as with gists.
- The Hub needs a registry of permanent IDs (ID, kind, owner, versions and the
  publishing repository of each, tombstones) separate from Projects, plus the
  routes and redirects above.
- Core needs ID creation, a regenerate command, `id_taken` in its publish
  contract, the `graphforge-item/1` descriptor, and resolution of a bare ID in
  its client. The editor surfaces the error and offers Core's regenerate
  action; it implements neither (ADR 0054).
- Someone can squat an unpublished ID. The cost is a local regeneration, never a
  broken published module.
- The Hub fixture moves to `opendata/openalex` with a fresh `ont_` module ID.

## Options considered

1. **Gist-style permanent IDs created by Core and claimed at first publish
   (chosen).**
2. **Owner and repository in the module ID**, for example
   `graphforge.sh/<owner>/<repo>/ontology/works`. Readable, but every rename or
   transfer changes every term in every dataset that uses the module. npm scopes
   and Maven group IDs show how painful moving such names is. Rejected.
3. **The Hub assigns the ID at first publish.** This prevents squatting, but
   terms are written long before publish, so publish would have to rewrite the
   module and change its digests. Rejected.
4. **IDs only their creator can claim**, derived from a key that the first
   publish must prove. This prevents squatting but adds key management to every
   module to avoid a rare, cheap failure. Rejected for now; revisit if squatting
   becomes real.
5. **A third-party persistent-identifier service** such as `w3id.org`, which
   would survive the loss of `graphforge.sh`. Rejected: IDs should read as
   GraphForge's, and section 7 commits to keeping the domain instead.
6. **Gist-style display**, where the canonical URL redirects browsers to the
   owner form. Rejected: people copy the address bar, and the owner form is not
   what belongs in data.
