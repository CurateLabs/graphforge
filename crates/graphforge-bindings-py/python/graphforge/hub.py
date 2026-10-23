"""Typed Hub/research verbs over the native Rust CLI, with no subprocess.

``HubProject`` opens the project afresh for each verb. Close other writable
handles before using it, then reopen ``GraphForge`` to read committed data as
Arrow. Metadata results retain the native field names and identities.
"""

import json
from os import PathLike
from pathlib import Path
from typing import Any, Literal, TypedDict, overload

from graphforge._graphforge_rs import GraphForgeError, _cli_execute


class CoreVersionWarning(TypedDict):
    """Native warning for data produced by a different Core version."""

    code: str
    producing_core_version: str
    running_core_version: str
    message: str


class _CloneRequired(TypedDict):
    contract: str
    repository: str
    destination: str
    immutable_version: str
    package_digest: str
    generation_uuid: str
    resumed_bytes: int


class CloneResult(_CloneRequired, total=False):
    """Verified, atomically installed clone and its selected research identity."""

    resolved_ref: str
    research_branch_uuid: str
    research_version_uuid: str
    research_version_kind: str
    research_version_identity: str
    moved_from: str
    moved_to: str
    warnings: list[CoreVersionWarning]


class RepositoryIdentity(TypedDict):
    """Canonical Hub repository identity."""

    owner: str
    repository: str


class _PublishRequired(TypedDict):
    format: str
    operation_uuid: str
    request_commitment: str
    repository: RepositoryIdentity
    manifest_validator: str
    refs: list[str]
    previous_revision: str | None
    revision: str


class PublishResult(_PublishRequired, total=False):
    """The Hub's confirmed publication receipt."""

    moved_from: str
    moved_to: str
    warnings: list[CoreVersionWarning]


class OperationResult(TypedDict):
    """Durable operation and exact generation/Version identities."""

    operation_uuid: str
    generation_uuid: str
    version_uuid: str | None
    replayed: bool


class CommitResult(OperationResult):
    """Captured Version and its parents, including on exact replay."""

    parents: list[str]


class BranchResult(OperationResult):
    """A new Branch and the Version it starts from."""

    label: str


class TagResult(OperationResult):
    """Tag name and the immutable Version it names."""

    name: str


class PullResult(CommitResult):
    """Native incorporation receipt; conflict refusals raise HubWorkflowError."""

    incorporated: int
    conflicts: list[str]


class UpToDateResult(TypedDict):
    """A pull that found no newer upstream head."""

    up_to_date: bool
    branch_uuid: str
    head_version_uuid: str


class _SignatureRequired(TypedDict):
    name: str


class Signature(_SignatureRequired, total=False):
    """Public credited identity, independently from authentication."""

    email: str
    orcid: str


class LogEntry(TypedDict):
    """One native history entry; control metadata, not graph rows."""

    version_uuid: str
    parents: list[str]
    author: Signature | None
    committer: Signature | None
    created_at: int | None
    label: str | None
    description: str | None
    tags: list[str]
    kind: str


class HubWorkflowError(GraphForgeError):
    """Native CLI refusal, preserving its envelope and process-compatible output.

    ``code`` selects ``details.semantic_code`` when present, otherwise the
    envelope's broad ``code``. ``native_code`` always retains that broad code.
    """

    def __init__(self, exit_code: int, stdout: bytes, stderr: bytes):
        self.envelope: dict[str, Any] = json.loads(stderr)
        error = self.envelope["error"]
        self.native_code: str = error["code"]
        self.details: dict[str, Any] = error.get("details", {})
        self.code: str = self.details.get("semantic_code", self.native_code)
        self.exit_code = exit_code
        self.stdout = stdout
        self.stderr = stderr
        super().__init__(error["message"])


def _options(**values: str | int | bool | None) -> list[str]:
    args = []
    for name, value in values.items():
        if value is True:
            args.append(f"--{name.replace('_', '-')}")
        elif value is not None and value is not False:
            args.append(f"--{name.replace('_', '-')}={value}")
    return args


class HubProject:
    """A durable project path and optional Hub credential-source names.

    All defaults, validation, authentication, retry and history behavior belong
    to Rust. No token value is stored here. The native call is synchronous.
    """

    def __init__(
        self,
        path: str | PathLike[str],
        *,
        hub: str | None = None,
        token_env: str | None = None,
        protection_bypass_env: str | None = None,
    ):
        # Preserve symlinks for the native filesystem admission checks.
        self.path = str(Path(path).absolute())
        self.warnings: list[CoreVersionWarning] = []
        self._hub = _options(
            hub=hub, token_env=token_env, protection_bypass_env=protection_bypass_env
        )

    @overload
    def _run(
        self, verb: Literal["clone"], args: list[str], *, remote: bool = False
    ) -> CloneResult: ...

    @overload
    def _run(
        self, verb: Literal["branch"], args: list[str], *, remote: bool = False
    ) -> BranchResult: ...

    @overload
    def _run(
        self, verb: Literal["commit"], args: list[str], *, remote: bool = False
    ) -> CommitResult: ...

    @overload
    def _run(self, verb: Literal["tag"], args: list[str], *, remote: bool = False) -> TagResult: ...

    @overload
    def _run(
        self, verb: Literal["push"], args: list[str], *, remote: bool = False
    ) -> PublishResult: ...

    @overload
    def _run(
        self, verb: Literal["pull"], args: list[str], *, remote: bool = False
    ) -> PullResult | UpToDateResult: ...

    @overload
    def _run(
        self, verb: Literal["log"], args: list[str], *, remote: bool = False
    ) -> list[LogEntry]: ...

    def _run(self, verb: str, args: list[str], *, remote: bool = False) -> Any:
        exit_code, stdout, stderr = _cli_execute(
            ["gf", "--json", f"--project={self.path}", verb] + (self._hub if remote else []) + args
        )
        if exit_code:
            error = HubWorkflowError(exit_code, stdout, stderr)
            self.warnings = error.envelope.get("warnings", [])
            raise error
        value = json.loads(stdout)
        self.warnings = value.get("warnings", []) if isinstance(value, dict) else []
        # The native warning envelope wraps array results; the typed log stays a list.
        return value["result"] if verb == "log" and isinstance(value, dict) else value

    def clone(
        self,
        repository: str,
        *,
        ref: str | None = None,
        tag: str | None = None,
        version_uuid: str | None = None,
    ) -> CloneResult:
        """Clone a verified repository into this new/empty project path."""
        return self._run(
            "clone",
            [*_options(ref=ref, tag=tag, version_uuid=version_uuid), "--", repository, self.path],
            remote=True,
        )

    def branch(
        self,
        name: str,
        *,
        author: str | None = None,
        committer: str | None = None,
        from_version: str | None = None,
        branch: str | None = None,
        operation_uuid: str | None = None,
    ) -> BranchResult:
        """Create a Branch from the current head or a tag/Version."""
        args = _options(
            author=author, committer=committer, branch=branch, operation_uuid=operation_uuid
        )
        if from_version is not None:
            args.append(f"--from={from_version}")
        return self._run("branch", [*args, "--", name])

    def commit(
        self,
        message: str,
        query: str,
        *,
        author: str | None = None,
        committer: str | None = None,
        branch: str | None = None,
        operation_uuid: str | None = None,
    ) -> CommitResult:
        """Run a mutation on the selected Branch and capture its Version."""
        return self._run(
            "commit",
            _options(
                message=message,
                query=query,
                author=author,
                committer=committer,
                branch=branch,
                operation_uuid=operation_uuid,
            ),
        )

    def tag(
        self,
        name: str,
        version: str | None = None,
        *,
        branch: str | None = None,
        operation_uuid: str | None = None,
    ) -> TagResult:
        """Name the current head or an exact tag/Version."""
        return self._run(
            "tag",
            _options(branch=branch, operation_uuid=operation_uuid)
            + ["--", name]
            + ([] if version is None else [version]),
        )

    def push(
        self,
        repository: str | None = None,
        *,
        ref: str | None = None,
        branch: str | None = None,
        rewrite: bool = False,
        prune: bool = False,
        operation_uuid: str | None = None,
        staging_directory: str | None = None,
    ) -> PublishResult:
        """Publish the Branch; default to its recorded upstream, fast-forward only."""
        return self._run(
            "push",
            _options(
                ref=ref,
                branch=branch,
                rewrite=rewrite,
                prune=prune,
                operation_uuid=operation_uuid,
                staging_directory=staging_directory,
            )
            + ([] if repository is None else ["--", repository]),
            remote=True,
        )

    def publish(
        self,
        repository: str | None = None,
        *,
        ref: str | None = None,
        branch: str | None = None,
        rewrite: bool = False,
        prune: bool = False,
        operation_uuid: str | None = None,
        staging_directory: str | None = None,
    ) -> PublishResult:
        """Publish the selected Branch (the same native operation as push)."""
        return self.push(
            repository,
            ref=ref,
            branch=branch,
            rewrite=rewrite,
            prune=prune,
            operation_uuid=operation_uuid,
            staging_directory=staging_directory,
        )

    def pull(
        self,
        repository: str | None = None,
        *,
        ref: str | None = None,
        branch: str | None = None,
        author: str | None = None,
        committer: str | None = None,
        operation_uuid: str | None = None,
    ) -> PullResult | UpToDateResult:
        """Fetch and incorporate compatible changes; preserve native refusals."""
        return self._run(
            "pull",
            _options(
                ref=ref,
                branch=branch,
                author=author,
                committer=committer,
                operation_uuid=operation_uuid,
            )
            + ([] if repository is None else ["--", repository]),
            remote=True,
        )

    def log(
        self,
        version: str | None = None,
        *,
        branch: str | None = None,
        remote: bool | str = False,
        ref: str | None = None,
        max_count: int | None = None,
        all: bool = False,
    ) -> list[LogEntry]:
        """Read native topology-ordered history, locally or from a Hub repository."""
        args = _options(branch=branch, ref=ref, max_count=max_count, all=all)
        if remote is not False:
            args.append(f"--remote={'' if remote is True else remote}")
        return self._run(
            "log", args + ([] if version is None else ["--", version]), remote=remote is not False
        )
