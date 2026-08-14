#!/usr/bin/env python3
"""Prerequisite-aware, resumable local validation for ``make pre-push``.

Evidence is local to a worktree and is trusted only when its content-addressed
identity, dependencies, and native artifacts still match. It deliberately does
not transmit data or record command output, tokens, user names, or absolute paths.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable, Iterable, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time

SCHEMA = "graphforge-pre-push-validation/v1"
MIN_FREE_GIB = 80
IGNORED_PARTS = {".git", ".graphforge", ".venv", "build", "target", "node_modules"}
Command = tuple[str, ...]
CommandRunner = Callable[[Command, Mapping[str, str]], None]


@dataclass(frozen=True)
class Stage:
    """A fail-closed, content-addressed unit of local validation."""

    name: str
    commands: tuple[Command, ...] = ()
    dependencies: tuple[str, ...] = ()
    inputs: tuple[str, ...] = ()
    environment: tuple[str, ...] = ()
    artifacts: tuple[str, ...] = ()
    python_extension: bool = False
    heavy: bool = False
    profile_isolation: bool = False


@dataclass
class StageResult:
    name: str
    digest: str
    proof_digest: str
    status: str
    reason: str
    elapsed_seconds: float
    artifacts: list[dict[str, str]] = field(default_factory=list)


class ValidationError(RuntimeError):
    """A validation failure with an actionable, safe remediation."""


def canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def digest(value: object) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def file_digest(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def default_runner(command: Command, environment: Mapping[str, str]) -> None:
    print("$", " ".join(command), flush=True)
    subprocess.run(
        command, cwd=environment["GF_VALIDATION_ROOT"], env=dict(environment), check=True
    )


class Coordinator:
    """Runs stages once per compatible identity and records atomic evidence."""

    def __init__(
        self,
        root: Path,
        stages: Iterable[Stage],
        *,
        runner: CommandRunner = default_runner,
        minimum_free_gib: int = MIN_FREE_GIB,
        force_clean: bool = False,
        cache_stages: Iterable[Stage] | None = None,
    ) -> None:
        self.root = root.resolve()
        selected_stages = tuple(stages)
        self.stages = {stage.name: stage for stage in selected_stages}
        self.cache_stages = tuple(cache_stages) if cache_stages is not None else selected_stages
        self.runner = runner
        self.minimum_free_gib = minimum_free_gib
        self.force_clean = force_clean
        self.evidence_root = self.root / ".graphforge" / "validation" / "v1"
        self.shared_cache_root = self.common_git_dir() / "graphforge-validation-cache"
        self.results: dict[str, StageResult] = {}
        self.started_used_bytes = shutil.disk_usage(self.root).used
        self.peak_used_bytes = self.started_used_bytes
        self.estimated_required_gib = self.minimum_free_gib

    def sample_disk(self) -> None:
        """Record the largest observed filesystem use at stage boundaries."""
        self.peak_used_bytes = max(self.peak_used_bytes, shutil.disk_usage(self.root).used)

    def common_git_dir(self) -> Path:
        try:
            value = subprocess.check_output(
                ("git", "rev-parse", "--git-common-dir"),
                cwd=self.root,
                text=True,
                stderr=subprocess.DEVNULL,
            ).strip()
        except FileNotFoundError as error:
            raise ValidationError(
                "missing prerequisite(s): git. Install git so validation can "
                "locate the shared compilation cache."
            ) from error
        except subprocess.CalledProcessError:
            # Non-repository trees (tests, unpackaged checkouts) keep a local cache.
            return self.root / ".graphforge" / "validation" / "shared-cache"
        except OSError as error:
            raise ValidationError(f"unable to resolve git common dir: {error}") from error
        path = Path(value)
        return (self.root / path).resolve() if not path.is_absolute() else path

    def source_state(self, patterns: Sequence[str]) -> list[dict[str, str]]:
        """Hash only stage-relevant checked-in inputs, including dirty content."""
        paths: set[Path] = set()
        for pattern in patterns:
            paths.update(path for path in self.root.glob(pattern) if path.is_file())
        records: list[dict[str, str]] = []
        for path in sorted(paths):
            relative = path.relative_to(self.root)
            if any(part in IGNORED_PARTS for part in relative.parts):
                continue
            records.append({"path": relative.as_posix(), "sha256": file_digest(path)})
        return records

    def command_versions(self, stage: Stage) -> dict[str, str]:
        executables = {command[0] for command in stage.commands if command}
        if stage.name == "preflight":
            executables.update({"cargo", "rustc", "rustup", "uv", "node", "pnpm"})
        versions: dict[str, str] = {}
        for executable in sorted(executables):
            try:
                completed = subprocess.run(
                    (executable, "--version"),
                    cwd=self.root,
                    capture_output=True,
                    check=False,
                    text=True,
                )
            except OSError:
                versions[executable] = "missing"
                continue
            versions[executable] = (completed.stdout or completed.stderr).strip().split("\n")[0]
        return versions

    def stage_digest(self, stage: Stage, dependencies: Mapping[str, str]) -> str:
        environment = {key: os.environ.get(key, "") for key in stage.environment}
        return digest(
            {
                "schema": SCHEMA,
                "stage": stage.name,
                "inputs": self.source_state(stage.inputs),
                "execution_contract": {
                    "commands": stage.commands,
                    "artifacts": stage.artifacts,
                    "python_extension": stage.python_extension,
                    "heavy": stage.heavy,
                    "profile_isolation": stage.profile_isolation,
                },
                "dependencies": dependencies,
                "environment": environment,
                "tool_versions": self.command_versions(stage),
            }
        )

    def evidence_path(self, stage: Stage, stage_digest: str) -> Path:
        return self.evidence_root / "stages" / stage.name / f"{stage_digest}.json"

    def read_evidence(
        self, stage: Stage, stage_digest: str, dependencies: Mapping[str, str]
    ) -> tuple[dict[str, object] | None, str]:
        path = self.evidence_path(stage, stage_digest)
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None, "miss:no-evidence"
        except (OSError, json.JSONDecodeError):
            return None, "miss:malformed-evidence"
        if not isinstance(value, dict):
            return None, "miss:malformed-evidence"
        required = {
            "schema": SCHEMA,
            "stage": stage.name,
            "digest": stage_digest,
            "dependencies": dict(dependencies),
            "outcome": "passed",
        }
        if any(value.get(key) != expected for key, expected in required.items()):
            return None, "miss:incompatible-evidence"
        if not isinstance(value.get("proof_digest"), str):
            return None, "miss:malformed-evidence"
        if "artifacts" not in value:
            return None, "miss:malformed-evidence"
        artifacts = value["artifacts"]
        if not isinstance(artifacts, list) or not self.artifacts_match(artifacts):
            return None, "miss:artifact-identity-drift"
        return value, "hit:compatible-evidence"

    def python_extension_digest(self) -> str:
        command: Command = (
            "uv",
            "run",
            "--no-sync",
            "python",
            "-c",
            "import hashlib; from pathlib import Path; "
            "from graphforge import _graphforge_rs; "
            "p=Path(_graphforge_rs.__file__).resolve(); "
            "print(hashlib.sha256(p.read_bytes()).hexdigest())",
        )
        completed = subprocess.run(
            command, cwd=self.root, capture_output=True, check=True, text=True
        )
        value = completed.stdout.strip()
        if len(value) != 64:
            raise ValidationError("installed graphforge extension did not return a SHA-256 digest")
        return value

    def artifacts_match(self, artifacts: object) -> bool:
        if not isinstance(artifacts, list):
            return False
        for artifact in artifacts:
            if not isinstance(artifact, dict):
                return False
            relative, expected = artifact.get("path"), artifact.get("sha256")
            if not isinstance(relative, str) or not isinstance(expected, str):
                return False
            if relative == "installed:graphforge-extension":
                try:
                    current = self.python_extension_digest()
                except (OSError, subprocess.CalledProcessError, ValidationError):
                    return False
                if current != expected:
                    return False
                continue
            path = self.root / relative
            if not path.is_file() or file_digest(path) != expected:
                return False
        return True

    def collect_artifacts(self, stage: Stage) -> list[dict[str, str]]:
        artifacts: list[dict[str, str]] = []
        for pattern in stage.artifacts:
            for path in sorted(path for path in self.root.glob(pattern) if path.is_file()):
                artifacts.append(
                    {"path": path.relative_to(self.root).as_posix(), "sha256": file_digest(path)}
                )
        if stage.artifacts and not artifacts:
            raise ValidationError(f"{stage.name} did not produce its required native artifact")
        if stage.python_extension:
            artifacts.append(
                {"path": "installed:graphforge-extension", "sha256": self.python_extension_digest()}
            )
        return artifacts

    def run_preflight(self, environment: Mapping[str, str]) -> None:
        missing = [
            name
            for name in ("cargo", "rustc", "rustup", "uv", "node", "pnpm", "bazelisk")
            if not shutil.which(name)
        ]
        if missing:
            raise ValidationError(
                "missing prerequisite(s): "
                + ", ".join(missing)
                + ". Install the pinned toolchain described in docs/development/contributing.md "
                "(Bazelisk: docs/development/bazel.md)."
            )
        heavy_stages = tuple(stage for stage in self.cache_stages if stage.heavy)
        warm_cache = bool(heavy_stages) and all(
            (
                cache := self.shared_cache_root / "cargo" / self.cargo_cache_digest(stage)[:24]
            ).is_dir()
            and any(cache.iterdir())
            for stage in heavy_stages
        )
        self.estimated_required_gib = (
            min(self.minimum_free_gib, 20) if warm_cache else self.minimum_free_gib
        )
        available = shutil.disk_usage(self.root).free // (1024**3)
        if available < self.estimated_required_gib:
            raise ValidationError(
                f"insufficient disk before compilation: {available} GiB available; "
                f"{self.estimated_required_gib} GiB estimated need "
                f"({'compatible cache present' if warm_cache else 'cold cache'}). "
                "Safe cleanup options: "
                "make clean-builds (stale artifacts) or make clean-builds-all (all Rust artifacts)."
            )
        llvm_cov = subprocess.run(
            ("cargo", "llvm-cov", "--version"), cwd=self.root, capture_output=True, check=False
        )
        if llvm_cov.returncode:
            raise ValidationError(
                "cargo-llvm-cov is required; install it with: cargo install cargo-llvm-cov"
            )
        components = subprocess.check_output(
            ("rustup", "component", "list", "--installed"), cwd=self.root, text=True
        )
        if not any(line.startswith("llvm-tools") for line in components.splitlines()):
            raise ValidationError(
                "llvm-tools-preview is required; install it with: "
                "rustup component add llvm-tools-preview"
            )
        for command, remediation in (
            (
                ("uv", "sync", "--locked", "--all-extras", "--dry-run"),
                "run: uv sync --locked --all-extras",
            ),
            (
                (
                    "uv",
                    "run",
                    "--no-sync",
                    "python",
                    "-c",
                    "import coverage, hypothesis, pytest, xdist",
                ),
                "run: uv sync --locked --all-extras",
            ),
            (
                (
                    "pnpm",
                    "install",
                    "--frozen-lockfile",
                    "--lockfile-only",
                    "--offline",
                    "--ignore-scripts",
                ),
                "run: pnpm install --frozen-lockfile",
            ),
            (("uv", "run", "maturin", "--version"), "run: uv sync --all-extras"),
            (
                ("pnpm", "--filter", "@curatelabs/graphforge", "exec", "napi", "--version"),
                "run: pnpm install --frozen-lockfile",
            ),
        ):
            completed = subprocess.run(command, cwd=self.root, env=dict(environment), check=False)
            if completed.returncode:
                raise ValidationError(
                    "prerequisite check failed before compilation: "
                    f"{' '.join(command)}. Remediation: {remediation}"
                )

    def stage_environment(self, stage: Stage, stage_digest: str) -> dict[str, str]:
        environment = dict(os.environ)
        environment["GF_VALIDATION_ROOT"] = str(self.root)
        environment["GF_VALIDATION_STAGE"] = stage.name
        environment["GF_VALIDATION_DIGEST"] = stage_digest
        if stage.profile_isolation:
            profile_root = self.evidence_root / "profiles" / stage.name
            profile_root.mkdir(parents=True, exist_ok=True)
            environment["LLVM_PROFILE_FILE"] = str(profile_root / "%p-%m.profraw")
        # Cargo fingerprints first-party units inside one target directory. Key only on
        # toolchain/profile/manifest inputs that make a target directory incompatible.
        if stage.heavy:
            environment["CARGO_TARGET_DIR"] = str(
                self.shared_cache_root / "cargo" / self.cargo_cache_digest(stage)[:24]
            )
        return environment

    def cargo_cache_digest(self, stage: Stage) -> str:
        profile = "coverage" if stage.name == "rust-tests-coverage-native" else "release"
        if stage.name == "rust-quality":
            profile = "debug"
        tool_stage = Stage("cargo-cache", commands=(("cargo",), ("rustc",)))
        return digest(
            {
                "schema": SCHEMA,
                "profile": profile,
                "sources": self.source_state(
                    (
                        "Cargo.toml",
                        "Cargo.lock",
                        "rust-toolchain.toml",
                        "crates/**/Cargo.toml",
                    )
                ),
                "tool_versions": self.command_versions(tool_stage),
                "environment": {
                    key: os.environ.get(key, "")
                    for key in ("CARGO_BUILD_JOBS", "CARGO_FEATURES", "RUSTFLAGS")
                },
            }
        )

    @contextmanager
    def heavy_lock(self) -> Iterable[None]:
        """Serialise heavy compilation across worktrees (stricter than the two-build cap)."""
        self.shared_cache_root.mkdir(parents=True, exist_ok=True)
        lock_path = self.shared_cache_root / "heavy-build.lock"
        with lock_path.open("w", encoding="utf-8") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)

    def write_evidence(self, stage: Stage, value: Mapping[str, object]) -> None:
        path = self.evidence_path(stage, str(value["digest"]))
        path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            "w", encoding="utf-8", dir=path.parent, delete=False
        ) as handle:
            json.dump(value, handle, sort_keys=True, indent=2)
            handle.write("\n")
            temporary = Path(handle.name)
        temporary.chmod(0o600)
        temporary.replace(path)

    def run_stage(self, stage: Stage) -> StageResult:
        # Preflight is deliberately mandatory on every invocation (disk pressure and installed
        # tools are live conditions), but a fresh preflight should not invalidate unrelated
        # successful stages. Each stage carries its own relevant toolchain and lockfile inputs.
        dependencies = {
            name: self.results[name].proof_digest
            for name in stage.dependencies
            if name != "preflight"
        }
        stage_digest = self.stage_digest(stage, dependencies)
        if stage.name == "preflight":
            evidence, reason = None, "miss:mandatory-preflight"
        else:
            evidence, reason = self.read_evidence(stage, stage_digest, dependencies)
        if evidence is not None:
            result = StageResult(
                stage.name,
                stage_digest,
                str(evidence["proof_digest"]),
                "hit",
                reason,
                0.0,
                list(evidence["artifacts"]),
            )
            self.results[stage.name] = result
            return result
        self.sample_disk()
        started = time.monotonic()
        environment = self.stage_environment(stage, stage_digest)
        try:
            if stage.name == "preflight":
                self.run_preflight(environment)
            elif stage.heavy:
                with self.heavy_lock():
                    for command in stage.commands:
                        self.runner(command, environment)
            else:
                for command in stage.commands:
                    self.runner(command, environment)
            artifacts = self.collect_artifacts(stage)
        except (OSError, subprocess.CalledProcessError, ValidationError) as error:
            raise ValidationError(f"stage {stage.name} failed: {error}") from error
        elapsed = time.monotonic() - started
        self.sample_disk()
        evidence_value: dict[str, object] = {
            "schema": SCHEMA,
            "stage": stage.name,
            "digest": stage_digest,
            "dependencies": dependencies,
            "outcome": "passed",
            "proof_digest": digest({"identity": stage_digest, "recorded_at_ns": time.time_ns()}),
            "elapsed_seconds": round(elapsed, 3),
            "artifacts": artifacts,
        }
        self.write_evidence(stage, evidence_value)
        result = StageResult(
            stage.name,
            stage_digest,
            str(evidence_value["proof_digest"]),
            "miss",
            reason,
            elapsed,
            artifacts,
        )
        self.results[stage.name] = result
        return result

    def run(self) -> list[StageResult]:
        if self.force_clean and self.evidence_root.exists():
            shutil.rmtree(self.evidence_root)
        ordered = list(self.stages.values())
        for stage in ordered:
            if any(dependency not in self.results for dependency in stage.dependencies):
                raise ValidationError(f"stage {stage.name} has an unavailable dependency")
            result = self.run_stage(stage)
            print(
                f"stage={result.name} evidence={result.status} "
                f"reason={result.reason} digest={result.digest[:12]} "
                f"elapsed={result.elapsed_seconds:.3f}s",
                flush=True,
            )
        return [self.results[stage.name] for stage in ordered]

    def write_summary(
        self,
        results: Sequence[StageResult],
        *,
        outcome: str = "passed",
        error: str | None = None,
        filename: str = "summary.json",
    ) -> Path:
        self.sample_disk()
        usage = shutil.disk_usage(self.root)
        summary = {
            "schema": SCHEMA,
            "outcome": outcome,
            "disk": {
                "available_gib": usage.free // (1024**3),
                "estimated_required_gib": self.estimated_required_gib,
                "start_used_bytes": self.started_used_bytes,
                "peak_used_bytes": self.peak_used_bytes,
            },
            "stages": [result.__dict__ for result in results],
        }
        if error is not None:
            summary["error"] = error
        self.evidence_root.mkdir(parents=True, exist_ok=True)
        path = self.evidence_root / filename
        with tempfile.NamedTemporaryFile(
            "w", encoding="utf-8", dir=path.parent, delete=False
        ) as handle:
            json.dump(summary, handle, sort_keys=True, indent=2)
            handle.write("\n")
            temporary = Path(handle.name)
        temporary.chmod(0o600)
        temporary.replace(path)
        return path


def stages() -> tuple[Stage, ...]:
    common = ("Makefile", "rust-toolchain.toml")

    def inputs(*paths: str) -> tuple[str, ...]:
        return (*common, *paths)

    return (
        Stage(
            "preflight",
            inputs=inputs("pyproject.toml", "uv.lock", "package.json", "pnpm-lock.yaml"),
        ),
        Stage(
            "policy-static",
            commands=(
                ("uv", "run", "--no-sync", "python", "scripts/ci/test-pre-push-validation.py"),
                ("make", "pre-push-fast"),
            ),
            dependencies=("preflight",),
            inputs=inputs(
                "pyproject.toml",
                "uv.lock",
                "scripts/**/*.py",
                "**/*.py",
                ".github/**/*.yml",
            ),
        ),
        Stage(
            "rust-quality",
            commands=(
                ("cargo", "fmt", "--all", "--", "--check"),
                ("cargo", "clippy", "--workspace", "--", "-D", "warnings"),
            ),
            dependencies=("preflight",),
            environment=("CARGO_BUILD_JOBS", "CARGO_FEATURES", "RUSTFLAGS"),
            inputs=inputs(
                "Cargo.toml",
                "Cargo.lock",
                "crates/**/*.rs",
                "crates/**/Cargo.toml",
                "tests/**/*.rs",
            ),
            heavy=True,
        ),
        Stage(
            "rust-tests-coverage-native",
            commands=(
                ("bash", "scripts/ci/test-coverage-rust.sh"),
                ("uv", "run", "--no-sync", "python", "scripts/ci/test-rust-coverage-ledger.py"),
                ("make", "coverage-rust"),
            ),
            dependencies=("rust-quality",),
            environment=("CARGO_BUILD_JOBS", "CARGO_FEATURES", "RUSTFLAGS"),
            inputs=inputs(
                "Cargo.toml",
                "Cargo.lock",
                "crates/**/*.rs",
                "crates/**/Cargo.toml",
                "tests/**/*.rs",
                "scripts/coverage-rust.sh",
                "scripts/ci/test-coverage-rust.sh",
                "scripts/ci/test-rust-coverage-ledger.py",
                "tests/features/node/cucumber.js",
                "tests/features/node/package.json",
                ".github/**/*.yml",
                ".github/**/*.yaml",
            ),
            artifacts=("crates/graphforge-bindings-node/*.node",),
            python_extension=True,
            heavy=True,
        ),
        Stage(
            "python-wrapper-coverage",
            commands=(("make", "coverage-python"),),
            dependencies=("rust-tests-coverage-native",),
            environment=("COVERAGE_FAIL_UNDER_PYTHON",),
            inputs=inputs(
                "pyproject.toml",
                "uv.lock",
                "crates/graphforge-bindings-py/**/*.py",
                "tests/**/*.py",
            ),
            profile_isolation=True,
        ),
        Stage(
            "node-wrapper-coverage",
            commands=(("make", "coverage-node"),),
            dependencies=("rust-tests-coverage-native",),
            environment=("COVERAGE_FAIL_UNDER_NODE",),
            inputs=inputs(
                "package.json",
                "pnpm-lock.yaml",
                "crates/graphforge-bindings-node/**/*.mjs",
                "crates/graphforge-bindings-node/package.json",
            ),
            profile_isolation=True,
        ),
        Stage(
            "api-bdd-acceptance",
            commands=(
                ("uv", "run", "--no-sync", "python", "scripts/ci/test-api-bdd-mutations.py"),
            ),
            dependencies=("rust-tests-coverage-native", "policy-static"),
            inputs=inputs(
                "tests/**/*.py",
                "tests/**/*.rs",
                "tests/**/*.feature",
                "scripts/ci/test-api-bdd-mutations.py",
            ),
            profile_isolation=True,
        ),
        Stage(
            "final-thresholds",
            commands=(("make", "check-coverage"),),
            dependencies=(
                "python-wrapper-coverage",
                "node-wrapper-coverage",
                "api-bdd-acceptance",
            ),
            inputs=inputs("scripts/check-coverage-rust.sh"),
            profile_isolation=True,
        ),
    )


def main(argv: Sequence[str] | None = None) -> int:
    raw_minimum = os.environ.get("GF_PRE_PUSH_MIN_FREE_GIB")
    try:
        default_minimum = int(raw_minimum) if raw_minimum else MIN_FREE_GIB
    except ValueError:
        print(
            f"GF_PRE_PUSH_MIN_FREE_GIB must be an integer number of GiB; got {raw_minimum!r}",
            file=sys.stderr,
        )
        return 1
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("preflight", "run"))
    parser.add_argument(
        "--force-clean",
        action="store_true",
        help="discard only local validation evidence and rerun every stage",
    )
    parser.add_argument(
        "--minimum-free-gib",
        type=int,
        default=default_minimum,
    )
    args = parser.parse_args(argv)
    all_stages = stages()
    selected_stages = all_stages[:1] if args.command == "preflight" else all_stages
    coordinator = Coordinator(
        Path.cwd(),
        selected_stages,
        force_clean=args.force_clean,
        minimum_free_gib=args.minimum_free_gib,
        cache_stages=all_stages,
    )
    filename = "preflight-summary.json" if args.command == "preflight" else "summary.json"
    try:
        results = coordinator.run()
        summary_path = coordinator.write_summary(results, filename=filename)
    except ValidationError as error:
        summary_path = coordinator.write_summary(
            list(coordinator.results.values()),
            outcome="failed",
            error=str(error),
            filename=filename,
        )
        print(f"pre-push validation failed: {error}", file=sys.stderr)
        print(f"summary={summary_path.relative_to(coordinator.root)}", file=sys.stderr)
        return 1
    print(f"pre-push validation passed; summary={summary_path.relative_to(coordinator.root)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
