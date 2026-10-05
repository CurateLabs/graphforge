# GraphForge

**Build a graph, ask a question, and keep what you learn.**

GraphForge records items and their connections: for example, which paper cites
another, or which interview excerpt informed a finding. You can ask questions
about those connections and keep the evidence behind an answer. It runs on your
computer. The primary audience is a nontechnical analyst working with an agent;
technical users can also write their own code.

These docs target **v0.6.0**. [Installation](guide/installation.md) explains
release availability and setup. Before v1.0.0, only supported current-format
projects can be opened; backward compatibility and migration are not guaranteed.

## Basic: understand and use a graph

Start here if you are doing your first research project. No programming knowledge
is needed to understand the lessons. Running the examples uses a coding agent
or a technical helper for setup.

1. [Your first graph](guide/quickstart.md): connect three papers and check a citation count.
2. [Work with an agent](guide/work-with-an-agent.md): ask it to run the example and inspect the actual answer.
3. [Your first mixed-methods project](guide/first-research-project.md): connect survey responses and interview excerpts, challenge an explanation, then save and retrieve a bounded conclusion.

Basic graph use is a complete outcome. You do not have to learn collaboration,
research history, or every other feature.

## Advanced: work directly with the tools

The [Advanced introduction](guide/advanced.md) assumes basic Python, database,
and terminal skills.
It explains GraphForge-specific ideas as you need them; expert knowledge is not
a prerequisite.

| Your task                                       | Start here                                                                          |
| ----------------------------------------------- | ----------------------------------------------------------------------------------- |
| Run code and inspect results                    | [Use a notebook](guide/use-a-notebook.md)                                           |
| Save a graph and return later                   | [Query, analyze, and save](guide/tutorial.md)                                       |
| Write queries or load records                   | [Cypher](guide/cypher-guide.md) · [Graph construction](guide/graph-construction.md) |
| Retain a challenged hypothesis and its evidence | [Record and revisit an inquiry](guide/record-an-inquiry.md)                         |
| Keep an exact research state                    | [Keep research history](guide/research-journey.md)                                  |
| Move or share a saved project                   | [Portable projects](guide/portable-projects.md)                                     |
| Call GraphForge from an application             | [Integrate GraphForge](guide/integrate-graphforge.md)                               |

Choose the task you need. Advanced is a collection of optional paths, not a
sequence you must finish before using the product.

## When GraphForge fits

Use it to inspect relationships in a citation network, connect observations to
sources, analyze a dependency graph, or retain the reasoning behind an inquiry.
Start with the data you have; an ontology is not required to create a graph.

For a centrally hosted, high-throughput application serving many concurrent
users, evaluate an operational database against that workload. GraphForge's
[scale guidance](reference/scale-limits.md) explains the difference between
measured workloads and a general size promise.

Durable projects require a supported local filesystem. In-memory work ends
when the process or notebook kernel closes. Read the
[storage requirements](guide/installation.md#durable-storage) before saving
work you intend to keep.

## Reference and specialist material

- [Cypher](guide/cypher-guide.md), [graph construction](guide/graph-construction.md),
  and [analytics](guide/analytics-integration.md)
- [API reference](reference/api.md) and [algorithm catalog](book/architecture/algorithms.md)
- [Architecture](book/architecture/overview.md), [language conformance](reference/tck-compliance.md),
  and [release scope](releases/roadmap.md)
- [Contributing](development/contributing.md) and [documentation map](README.md)

Architecture and implementation contracts are specialist references, outside
the Basic and Advanced learning paths.

For help, use [GitHub Discussions](https://github.com/CurateLabs/graphforge/discussions).
For a bug, include your package version, OS, storage mode, smallest reproducing
query, expected result, and actual result in a
[GitHub issue](https://github.com/CurateLabs/graphforge/issues/new/choose).
GraphForge is available under the [Apache License 2.0](legal/licensing.md).
