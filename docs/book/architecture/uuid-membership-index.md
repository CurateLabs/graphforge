# UUID membership index

GraphForge persists authoritative node and edge identities in Parquet. Bulk
ingest uses a derived UUID membership index so validation does not materialize
the complete graph in memory.

## Format version 1

`indexes/uuid-membership/manifest.json` records the format version, canonical
`topology_generation`, and the filename, record count, and SHA-256 digest for
the node and edge indexes. Each immutable `.uuidx` file is an ascending,
duplicate-free sequence of raw 16-byte UUID values. The file length must equal
`count * 16`.

Readers fail closed if the manifest is missing, has an unsupported version,
names a non-local file, disagrees with the topology generation, or if an index
length or checksum differs. They never fall back to a graph-wide in-memory set.
Lookups use binary search and preserve the caller's request order; metrics
contain only counts and file-seek totals, never UUIDs.

## Publication and recovery

Rebuild scans canonical Parquet in configured record batches, sorts bounded
runs, and merges them with a configured fan-in. Immutable data files are
synced before a sibling temporary manifest is atomically renamed and the index
directory is synced. A crash before that final rename leaves the prior manifest
and its immutable files authoritative. Readers that opened the prior snapshot
continue to use it while a rebuild is in progress.

Graph mutation publication rebuilds the workspace index before the graph tree
is captured, so the Parquet topology and its membership manifest enter the same
immutable project generation. Existing projects migrate through the explicit
bounded rebuild API. Corrupt or stale indexes are integrity errors and are not
silently rebuilt during reads.
