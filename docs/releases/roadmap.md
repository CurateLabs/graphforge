# v0.6.0 release scope

These docs describe GraphForge v0.6.0. They replace the earlier user guides;
there is no maintained pre-v1 documentation archive or backward-compatibility
promise. [Installation](../guide/installation.md) explains artifact availability
and exact package versions. A source capability is not a claim that its release
has already been published.

## Start small

The first experience is [basic graph use](../guide/quickstart.md): create
connected data, ask a question, and inspect the answer. Choose an
[agent](../guide/work-with-an-agent.md), a [notebook](../guide/use-a-notebook.md),
or an [integration](../guide/integrate-graphforge.md).

Additional capabilities are optional journeys. Users do not need the entire
research model to use GraphForge.

| When you need it                        | v0.6.0 surface                                                                                          | Guide                                                         |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------- |
| Query and analyze connected data        | openCypher, analyst verbs, Arrow results, graph construction                                            | [Basic graph](../guide/quickstart.md)                         |
| Keep and reuse work                     | Durable projects, reusable saved queries                                                                | [Save and reopen](../guide/tutorial.md)                       |
| Remember what an inquiry established    | Graph records for hypothesis, challenge, evidence, conclusion; optional native knowledge and provenance | [Record an inquiry](../guide/record-an-inquiry.md)            |
| Explore and review independent research | Slices, Branches, immutable Versions, comparisons, Proposals, Forks                                     | [Research workspaces](../guide/research-journey.md)           |
| Evaluate externally produced decisions  | Bounded context and validated choice, rubric, probability results; caller-owned policy                  | [Decision workflows](../book/use-cases/decision-workflows.md) |
| Exchange research                       | Portable projects, Hub discovery, clone/publication, research history                                   | [Portable projects](../guide/portable-projects.md)            |
| Work at larger scale                    | Bounded ingest and interchange; workload-specific scale and benchmark qualification                     | [Scale guidance](../reference/scale-limits.md)                |

## Release readiness

[M13](https://github.com/CurateLabs/graphforge/milestone/13) owns coordinated
release readiness and publication across Rust, Python, Node, CLI, skills, and
docs. [#1096](https://github.com/CurateLabs/graphforge/issues/1096) tracks readiness;
[#1095](https://github.com/CurateLabs/graphforge/issues/1095) tracks publication.
Use their live dependencies for status rather than inferring readiness from
this feature list.

The included programs cover architecture maintenance, scale/interchange,
benchmarks, the research lifecycle, and provider-neutral decision workflows.
Research history and Hub command ergonomics continue through
[#1771](https://github.com/CurateLabs/graphforge/issues/1771) and
[#1772](https://github.com/CurateLabs/graphforge/issues/1772). Documentation must
use the implemented command surface, not planned command names.

Guide readiness is tracked in [#1208](https://github.com/CurateLabs/graphforge/issues/1208).
[#1209](https://github.com/CurateLabs/graphforge/issues/1209) qualifies clean
candidate installs, real editor/agent/notebook paths, expected outputs, durable
reopen, and independent first-use review. Technical examples do not establish
nontechnical usability or adoption.

## Core and associated applications

Core owns graph and research semantics. The VS Code extension, XYG visualization,
and website/Hub own their respective interfaces and hosting. A Core release does
not qualify every consumer feature. [The product map](https://github.com/orgs/CurateLabs/projects/2)
coordinates their work without changing implementation ownership.

Public projects support transparent participation and attributable contributions,
using familiar Git author/committer practices. Private projects provide a
non-public space for known collaborators. The hosting application enforces
project access; local graph or governance metadata is not access control.

## Beyond this release

Optional model-specific adapters, mobile bindings, and post-release community
pilots have their own delivery work. They are not prerequisites for basic graph
use. A browser-executed graph engine is not part of this release; a protocol
validation module for Hub consumers is a separate capability.

Before v1.0.0, APIs and project formats may change without migration support.
Current-version integrity, corruption refusal, recovery, and supported
interchange remain required. A v1.0 compatibility policy will be a separate
decision. See [project compatibility](../book/architecture/project-format-compatibility.md).
