# Who GraphForge is for

GraphForge is an embedded graph and analytics engine: it runs inside your
Python or Node process (or the `gf` command) and keeps a graph in memory or in
a local project directory. There is no database server to run.

These docs target **v0.6.0**. Before v1.0.0 GraphForge is pre-v1 software:
see [Status and limits](installation.md#status-and-limits) before you choose
durable storage or build an integration.

## Audience

The core audience is a **nontechnical analyst working with an agent**. The
[Basic path](overview.md#basic) is written for them. It still needs an agent or
technical helper to install and run software. Finishing the examples does not
establish nontechnical usability; actual first-use and comprehension evidence
belongs to [#1209](https://github.com/CurateLabs/graphforge/issues/1209).

**v0.6.0 initially serves technical analysts and integrators more completely**: people
who can run Python or Node, read a query result, and (when they choose) work
through a coding agent. They want to build or load a graph, ask relationship
questions, and keep the result without operating a database.

The [public Hub launch path](portable-projects.md#public-hub-launch-path) connects
browse, clone, query, local continuation and publish-back. Its production
qualification remains blocked; the local jobs below do not depend on it.

## Two jobs you can complete today

Both use shipped public workflows and the packages described in
[Installation](installation.md).

### 1. Explore a connected dataset and keep what you learn

Build nodes and relationships from your own rows, ask questions with openCypher
or the analyst verbs (`rank`, `cluster`, `similar`, `paths`, `analyze`, `find`),
read the answer as an Arrow table, and optionally save the graph as a durable
project that you reopen later.

- Build: [Your first graph](quickstart.md), [Construct a graph](graph-construction.md)
- Ask: [Cypher](cypher-guide.md), [Analyze results](analytics-integration.md)
- Keep: [Query, analyze, and save](tutorial.md)
- Choose a surface: [an agent](work-with-an-agent.md), [a notebook](use-a-notebook.md),
  or [your own application](integrate-graphforge.md)

### 2. Load a large relationship table, reopen it, and move it by verified export

Bulk-publish nodes and edges as Arrow tables into a durable project on a
supported filesystem, reopen and query it in a new process, then move it with
portable export, full verification, and import.

- Load: [Atomic bulk construction](graph-construction.md#atomic-bulk-construction-python)
- Move: [Portable projects](portable-projects.md)
- Size it: [Scale and workload limits](../reference/scale-limits.md), which
  links the measured ladder (up to 1,073,741,824 edges on the stated host) and
  the ingest rate it was measured at

The measured ladder ran the certification harness, not the Python quickstart.
Its numbers describe that workload on that host, not your graph.

## When an embedded engine fits

- One process or one notebook owns the graph at a time.
- The data and the question live on your machine: a citation network, a
  dependency graph, evidence tied to an inquiry.
- You want results as Arrow tables you can pass to pandas, Polars, or your own
  code, and projects you can export and verify.
- A single host's memory and disk are enough. Measure your own workload first:
  construction, reopen, query, and export have different costs.

## When it does not fit

- A centrally hosted application serving many concurrent users. Evaluate an
  operational database against that workload.
- A requirement for a published universal size or latency guarantee.
  GraphForge makes none; see [Scale and workload limits](../reference/scale-limits.md).
- Durable storage on a network filesystem, a container overlay root, or any
  filesystem outside the [admitted list](installation.md#durable-storage).
  In-memory graphs still run the full engine.
- A need for an audited benchmark result, or for backward compatibility and
  migration of data between versions before v1.0.0.
- Production-hardening assumptions. GraphForge does not claim production
  maturity.

## Shipped, and not yet shipped

| Implemented in Core; candidate qualification is separate                                   | Not part of this release, or not yet qualified                                                                  |
| ------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| Python and Node bindings, `gf` CLI, openCypher queries, analyst verbs                       | A client-server database mode                                                                                   |
| In-memory graphs; durable projects on ext4, xfs, btrfs (Linux), APFS, NTFS                  | Durable projects on unadmitted filesystems                                                                      |
| Arrow results, Parquet graph storage, JSON metadata                                         | Backward compatibility and migration for earlier project formats (pre-v1)                                       |
| Portable project export, verify, import; `gf clone` and `gf publish` commands               | Production clone and publish-back qualification is still required for launch under [#1209](https://github.com/CurateLabs/graphforge/issues/1209); full hosted catalog/publishing interfaces and private-repository qualification are separate |
| Optional inquiry records, research history, and decision-workflow building blocks           | Hosted notebooks (Kaggle, Colab) as qualified durable environments                                              |
| Measured scale evidence listed in [Scale and workload limits](../reference/scale-limits.md) | A 1,000,000 edges/s ingest rate; strong multicore ingest scaling; audited benchmark results                     |

The [roadmap](../releases/roadmap.md) has delivery detail. A capability in
this table is a statement about what the code supports, not about adoption or
fitness for your use.

## Get help

See [Get help and report a problem](../community/support.md) for where to ask,
what to include in a minimal reproduction, and how maintainers triage reports.
