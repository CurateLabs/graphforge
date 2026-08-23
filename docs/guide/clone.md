# Clone a Hub repository

Clone a public GraphForge Hub repository by its short identity or canonical URL:

```console
gf clone openalex/openalex
gf clone https://graphforge.sh/openalex/openalex my-openalex
```

The short form resolves to `https://graphforge.sh/{owner}/{repository}`. The CLI
retrieves the small `/.gf/manifest` and `/.gf/refs` discovery documents, validates
the complete snapshot, then downloads only the explicitly selected immutable
package object from the ordered HTTPS locations in the manifest. The HTML Hub
page and any particular storage provider are not part of the object protocol.

Clone rejects an existing destination. It streams the object through finite
byte and time bounds, verifies its transport length and SHA-256 digest, and then
delegates portable-v2 compatibility, semantic integrity, and materialization to
the Rust storage/API authorities. An interrupted transfer leaves only a hidden,
versioned `.<destination>.graphforge-clone` checkpoint. A later invocation uses
a strong HTTP validator and a checked `Range`/`Content-Range` exchange to resume;
if the server does not support ranges or the validator is stale, it safely starts
the object again. The checkpoint is never a project and is removed after import.
The destination is published only by the atomic portable import path.

Redirects are bounded and must retain HTTPS. Credentials in URLs, loopback and
non-public resolved addresses, query/fragment redirect hops, and local hostnames
are rejected. DNS is resolved through a filtering resolver and only the admitted
public socket addresses are handed to the connector, preventing a second DNS
lookup from rebinding the request. Authentication and private repositories are
intentionally not part of this public-clone command.

## Observability

When `GRAPHFORGE_OTEL_JSONL` names an exporter-owned local JSONL handoff, a
successful clone emits only operation class, result, duration, and byte count.
Repository identities, URLs, hosts, refs, digests, paths, credentials, user data,
and graph contents are never emitted. Export failure never changes clone behavior.
