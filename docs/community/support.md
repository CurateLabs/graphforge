# Get help and report a problem

GraphForge is pre-v1 open-source software maintained by Curate Labs. Support is
best-effort through public GitHub channels. There is no service-level
agreement, response-time commitment, or private support contact.

## Where to go

| You want to…                             | Use                                                                                                                                       |
| ---------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| Ask a question or discuss an approach    | [GitHub Discussions](https://github.com/CurateLabs/graphforge/discussions)                                                                |
| Report something that is broken or wrong | [Bug report](https://github.com/CurateLabs/graphforge/issues/new?template=bug_report.yml)                                                  |
| Ask for a capability                     | [Feature request](https://github.com/CurateLabs/graphforge/issues/new?template=feature_request.yml)                                       |
| Ask a question that needs an issue       | [Question](https://github.com/CurateLabs/graphforge/issues/new?template=question.yml)                                                     |
| Report a security vulnerability          | [Private vulnerability report](https://github.com/CurateLabs/graphforge/security/advisories/new); see the [security policy](security.md) |

Do not post a vulnerability, credential, token, or private data in a public
issue or discussion.

## Write a minimal reproduction

A report that a maintainer can run in a few minutes is fixed sooner. Include:

1. **Versions:** the package and version (`graphforge.__version__`, or
   `npm ls @curatelabs/graphforge`, or `gf --version`), your OS and CPU
   architecture, and Python or Node.js version.
2. **Storage mode:** in-memory (`GraphForge()`), or a durable project and its
   filesystem. For a storage error, include the error code, such as
   `GF_UNSUPPORTED_FILESYSTEM`.
3. **The smallest graph and query that show the problem.** Build the graph in a
   few lines with invented data. Do not attach a real dataset, a live
   `.graphforge/` directory, or personal or confidential content.
4. **Expected result and actual result,** including the full error text.
5. **Whether it is new:** whether the same steps worked in an earlier version
   or build.

For a performance or memory report, also give the graph size (nodes and edges),
the host's memory and disk, the exact command, and how you measured. Without a
measured workload, a maintainer cannot tell a defect from an expected limit;
see [Scale and workload limits](../reference/scale-limits.md).

## How maintainers triage

Bug reports arrive labeled `bug`, feature requests `enhancement`, and
questions `question`. An open issue with no milestone, no parent issue and
no recorded outcome has not been triaged yet. Maintainers work through new
issues in this order:

1. **Safety first.** A report involving data loss, corruption, a crash, or a
   security boundary is read first. Security reports belong in the private
   channel above; a public report of one is moved there.
2. **Reproduce.** A maintainer tries the reproduction on the current `main` or
   the latest published candidate. If it cannot be reproduced, they ask for the
   missing item from the list above.
3. **Classify.** The issue keeps its kind label (`bug`, `enhancement`,
   `documentation`, `question`), gains an area label where one fits (`core`,
   `parser`, `planner`, `executor`, `testing`, `tooling`, `ci-cd`), and is
   checked for duplicates. A duplicate is
   closed with a link to the canonical issue.
4. **Decide.** The maintainer records one outcome on the issue: accepted and
   linked to a milestone or parent issue; needs more information; out of scope
   or declined with the reason; or answered and closed. Pre-v1 policy applies:
   a request for backward compatibility, migration of older project formats, or
   a legacy API is out of scope until a v1.0 compatibility decision.
5. **Record the outcome** on the issue. Accepted work is attached to a
   milestone or made a sub-issue of the issue it serves. Any other outcome is
   a comment, and the issue is closed unless it awaits more information.

Acceptance records that the problem is real and in scope. It does not promise
a release date.

## Contribute a fix

See [Contributing](../development/contributing.md). Each change links an issue
and goes through the pull-request gate described there.
