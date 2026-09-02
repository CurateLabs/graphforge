#!/usr/bin/env python3
"""Static, non-provider contract tests for the progressive qualification image."""

from __future__ import annotations

import ast
import json
from pathlib import Path
import re
import shutil
import tempfile
import unittest

import tomllib

ROOT = Path(__file__).resolve().parents[2]
DOCKERFILE = Path("containers/graphforge-progressive-qualification/Dockerfile")
ENTRYPOINT = Path("containers/graphforge-progressive-qualification/run-qualification.py")
BENCHMARK_LOCK = Path("benchmarks/uv.lock")
DOCKERIGNORE = Path(".dockerignore")

FORBIDDEN_RUNTIME = re.compile(
    r"(?:^|\s)(?:EXPOSE|HEALTHCHECK)\b|"
    r"\b(?:http\.server|uvicorn|gunicorn|flyctl|pulumi)\b|"
    r"\b(?:ARG|ENV)\s+[A-Z0-9_]*(?:TOKEN|SECRET|PASSWORD|CREDENTIAL)",
    re.IGNORECASE | re.MULTILINE,
)


class ContractError(AssertionError):
    pass


def read(root: Path) -> str:
    path = root / DOCKERFILE
    if not path.is_file():
        raise ContractError(f"missing progressive qualification image: {DOCKERFILE}")
    return path.read_text(encoding="utf-8")


def require(text: str, marker: str, message: str) -> None:
    if marker not in text:
        raise ContractError(message)


def rendered_manifest(dockerfile: str) -> dict[str, object]:
    line = next(
        (
            value.strip()
            for value in dockerfile.splitlines()
            if value.strip().startswith('"{\\"schema\\":\\"graphforge-progressive-provider-build/1')
        ),
        None,
    )
    if line is None or not line.endswith('" \\'):
        raise ContractError("build manifest JSON template is missing")
    template = line.removesuffix(" \\").removeprefix('"').removesuffix('"').replace(r"\"", '"')
    replacements = {
        "${GRAPHFORGE_COMMIT}": "a" * 40,
        "${source_tree_sha256}": "b" * 64,
        "${gf_sha256}": "c" * 64,
        "${certify_sha256}": "d" * 64,
        "${generator_sha256}": "e" * 64,
        "${benchexec_python_sha256}": "f" * 64,
    }
    for old, new in replacements.items():
        template = template.replace(old, new)
    try:
        document = json.loads(template)
    except json.JSONDecodeError as error:
        raise ContractError("build manifest template must be valid JSON") from error
    return document


def bootstrap_constants(entrypoint: str) -> dict[str, object]:
    """Return literal top-level bootstrap assignments for exact contract checks."""
    try:
        tree = ast.parse(entrypoint)
    except SyntaxError as error:
        raise ContractError("startup boundary must be valid Python") from error
    constants: dict[str, object] = {}
    for statement in tree.body:
        if not isinstance(statement, ast.Assign) or len(statement.targets) != 1:
            continue
        target = statement.targets[0]
        if not isinstance(target, ast.Name):
            continue
        try:
            constants[target.id] = ast.literal_eval(statement.value)
        except (ValueError, TypeError):
            continue
    return constants


def validate_contract(root: Path) -> None:
    dockerfile = read(root)
    entrypoint = (root / ENTRYPOINT).read_text(encoding="utf-8")
    stages = re.findall(r"^FROM\s+\S+\s+AS\s+(\S+)\s*$", dockerfile, re.MULTILINE)
    if stages != ["rust-build", "python-deps", "qualification"]:
        raise ContractError("image must use isolated Rust, locked Python, and runtime stages")
    base_images = re.findall(r"^FROM\s+(\S+)\s+AS\s+\S+\s*$", dockerfile, re.MULTILINE)
    if any(re.fullmatch(r"[^@]+@sha256:[0-9a-f]{64}", image) is None for image in base_images):
        raise ContractError("every image stage must pin an immutable base digest")

    if dockerfile.count("ARG GRAPHFORGE_COMMIT") != 2:
        raise ContractError("build and runtime stages must bind the same source identity")
    require(dockerfile, "ARG TARGETARCH", "image build must receive the target architecture")
    require(dockerfile, "ARG TARGETOS", "image build must receive the target operating system")
    require(
        dockerfile,
        'test "${TARGETOS}/${TARGETARCH}" = linux/amd64',
        "image must fail closed off linux/amd64",
    )
    require(
        dockerfile,
        "grep -Eq '^[0-9a-f]{40}$'",
        "source identity must be a lowercase full Git object ID",
    )
    require(
        dockerfile,
        'LABEL org.opencontainers.image.revision="${GRAPHFORGE_COMMIT}"',
        "runtime image must publish its source revision",
    )
    require(
        dockerfile,
        "COPY --from=rust-build --chmod=0444 /graphforge-commit /opt/graphforge/commit",
        "runtime image must carry a read-only source revision file",
    )
    require(
        dockerfile,
        "tar --sort=name --mtime='UTC 1970-01-01' --owner=0 --group=0 --numeric-owner",
        "build must deterministically archive the copied source tree",
    )
    require(
        dockerfile,
        "sha256sum /tmp/source-tree.tar",
        "build must hash the deterministic copied-source archive",
    )

    require(
        dockerfile,
        "cargo build --locked --release --package graphforge-cli --bin gf",
        "Rust stage must build the locked gf CLI",
    )
    require(
        dockerfile,
        "cargo build --manifest-path benchmarks/Cargo.toml --locked --release",
        "Rust stage must use the locked benchmark workspace",
    )
    for package in (
        "--package graphforge-benchmark-certify",
        "--package graphforge-benchmark-graph500-generator",
    ):
        require(dockerfile, package, f"Rust stage must build {package.removeprefix('--package ')}")

    require(
        dockerfile,
        'python -m pip install --no-cache-dir "uv==0.11.33"',
        "Python dependency installer must use the pinned uv version",
    )
    require(
        dockerfile,
        "COPY benchmarks/pyproject.toml benchmarks/uv.lock ./",
        "Python stage must consume the benchmark lock",
    )
    require(
        dockerfile,
        "uv sync --frozen --no-dev --no-install-project",
        "Python dependencies must fail closed on lock drift",
    )
    lock = tomllib.loads((root / BENCHMARK_LOCK).read_text(encoding="utf-8"))
    benchexec = [package for package in lock["package"] if package["name"] == "benchexec"]
    if len(benchexec) != 1 or not re.fullmatch(r"\d+\.\d+(?:\.\d+)?", benchexec[0]["version"]):
        raise ContractError("benchmark lock must resolve exactly one pinned BenchExec package")

    ignored = (root / DOCKERIGNORE).read_text(encoding="utf-8").splitlines()
    for pattern in (".git", "**/.venv", "**/target", "*.env", ".env*", "*.pem", "*.key"):
        if pattern not in ignored:
            raise ContractError(f"container build context must exclude {pattern}")

    for binary in (
        "/artifacts/gf",
        "/artifacts/graphforge-benchmark-certify",
        "/artifacts/graphforge-benchmark-graph500-generator",
    ):
        require(dockerfile, binary, f"runtime must copy built binary {Path(binary).name}")
    require(
        dockerfile,
        "> /opt/graphforge/build-manifest.json",
        "runtime must contain the source and executable identity manifest",
    )
    require(
        dockerfile,
        "chmod 0444 /opt/graphforge/build-manifest.json",
        "build identity manifest must be read-only",
    )
    manifest_fields = (
        r"\"schema\":\"graphforge-progressive-provider-build/1\"",
        r"\"commit\":\"${GRAPHFORGE_COMMIT}\"",
        r"\"source_tree_sha256\":\"${source_tree_sha256}\"",
        r"\"executables\":{",
        r"\"gf_sha256\":\"${gf_sha256}\"",
        r"\"certify_sha256\":\"${certify_sha256}\"",
        r"\"generator_executable_sha256\":\"${generator_sha256}\"",
        r"\"benchexec_python_sha256\":\"${benchexec_python_sha256}\"",
    )
    for field in manifest_fields:
        require(dockerfile, field, "build manifest must bind its exact schema and identities")
    manifest = rendered_manifest(dockerfile)
    if set(manifest) != {"schema", "commit", "source_tree_sha256", "executables"}:
        raise ContractError("build manifest must have the exact provider-build root shape")
    if manifest["schema"] != "graphforge-progressive-provider-build/1":
        raise ContractError("build manifest schema is not the runner contract")
    if not re.fullmatch(r"[0-9a-f]{40}", str(manifest["commit"])):
        raise ContractError("build manifest commit is not a full lowercase object ID")
    if not re.fullmatch(r"[0-9a-f]{64}", str(manifest["source_tree_sha256"])):
        raise ContractError("build manifest source tree identity is not SHA-256")
    executables = manifest["executables"]
    expected_executables = {
        "gf_sha256",
        "certify_sha256",
        "generator_executable_sha256",
        "benchexec_python_sha256",
    }
    if not isinstance(executables, dict) or set(executables) != expected_executables:
        raise ContractError("build manifest must have the exact executable identity shape")
    if any(not re.fullmatch(r"[0-9a-f]{64}", str(value)) for value in executables.values()):
        raise ContractError("build manifest executable identities must be SHA-256")
    for fixed_path in (
        "/usr/local/bin/gf",
        "/usr/local/bin/graphforge-benchmark-certify",
        "/usr/local/bin/graphforge-benchmark-graph500-generator",
        "/opt/graphforge/benchmarks/.venv/bin/python",
    ):
        require(dockerfile, f"sha256sum {fixed_path}", f"manifest must hash {fixed_path}")
    for asset in ("harness", "profiles", "schemas", "definitions"):
        require(
            dockerfile,
            f"COPY benchmarks/{asset} /opt/graphforge/benchmarks/{asset}",
            f"runtime must contain benchmark {asset}",
        )

    require(dockerfile, 'PYTHONPATH="/opt/graphforge/benchmarks/harness"', "harness must import")
    require(dockerfile, "WORKDIR /", "startup must not enter the volume before validating it")
    require(
        dockerfile,
        "COPY --chmod=0555 containers/graphforge-progressive-qualification/run-qualification.py",
        "runtime must install its read-only startup boundary",
    )
    require(
        dockerfile,
        'ENTRYPOINT ["/usr/local/bin/run-progressive-qualification"]',
        "image entrypoint must be the privilege-dropping startup boundary",
    )
    require(
        dockerfile,
        'CMD ["--help"]',
        "default container invocation must not spend provider resources",
    )

    runtime = dockerfile.split(" AS qualification", 1)[-1]
    if re.search(r"\b(?:apt-get|apt|useradd|adduser)\b", runtime):
        raise ContractError("runtime stage may not resolve OS packages or create named users")
    require(dockerfile, "chown 10001:10001 /work", "runtime must use the fixed numeric identity")

    constants = bootstrap_constants(entrypoint)
    expected_environment = {
        "HOME": "/work",
        "LANG": "C.UTF-8",
        "PATH": "/opt/graphforge/benchmarks/.venv/bin:/usr/local/bin:/usr/bin:/bin",
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONPATH": "/opt/graphforge/benchmarks/harness",
        "PYTHONUNBUFFERED": "1",
    }
    if constants.get("WORK_ROOT") != "/work":
        raise ContractError("startup must validate and enter exact /work")
    if constants.get("RUN_UID") != 10001 or constants.get("RUN_GID") != 10001:
        raise ContractError("startup must use the fixed numeric uid and gid")
    if constants.get("EXEC_ENV") != expected_environment:
        raise ContractError("startup executor environment must match the strict allowlist")
    for marker in (
        "os.lstat(WORK_ROOT)",
        "stat.S_ISDIR(metadata.st_mode)",
        "os.path.realpath(WORK_ROOT) != WORK_ROOT",
        "os.path.ismount(WORK_ROOT)",
    ):
        require(entrypoint, marker, "startup must validate the exact /work mount root")
    require(
        entrypoint,
        "os.chown(WORK_ROOT, RUN_UID, RUN_GID, follow_symlinks=False)",
        "startup must own only the mount root",
    )
    if re.search(r"\b(?:chown|os\.chown)\b[^\n]*(?:recursive|-R)", entrypoint, re.IGNORECASE):
        raise ContractError("startup may not recursively change volume ownership")
    require(entrypoint, "os.chdir(WORK_ROOT)", "executor must run from the mounted work root")
    require(
        entrypoint,
        "libc.prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0)",
        "startup must prohibit privilege acquisition",
    )
    for marker in ("os.setgroups([])", "os.setgid(RUN_GID)", "os.setuid(RUN_UID)"):
        require(entrypoint, marker, "startup must clear groups and drop numeric privileges")
    require(
        entrypoint,
        'os.execve(PYTHON, [PYTHON, "-P", "-m", MODULE, *sys.argv[1:]], EXEC_ENV)',
        "startup must replace itself with the unprivileged executor",
    )
    require(
        entrypoint,
        'PYTHON = "/opt/graphforge/benchmarks/.venv/bin/python"',
        "Python path must be fixed",
    )
    require(
        entrypoint,
        'MODULE = "graphforge_bench.progressive_provider_run"',
        "module must be fixed",
    )
    if re.search(r"\bos\.environ\b|\bos\.getenv\b", entrypoint):
        raise ContractError("startup may not forward ambient environment variables")
    for forbidden_name in ("TOKEN", "SECRET", "PASSWORD", "CREDENTIAL", "PULUMI", "FLY_"):
        if forbidden_name in entrypoint:
            raise ContractError(f"startup allowlist may not contain {forbidden_name}")

    combined_runtime = dockerfile + "\n" + entrypoint
    match = FORBIDDEN_RUNTIME.search(combined_runtime)
    if match:
        raise ContractError(
            f"qualification image exposes a service or credential input: {match.group(0)}"
        )
    if re.search(r"\b(?:curl|wget|nc|ssh|socat)\b", entrypoint):
        raise ContractError("startup boundary may not perform network operations")

    if re.search(r"\b(?:cargo|rustc|uv sync|pip install)\b", runtime):
        raise ContractError("runtime stage may not contain build tooling or dependency resolution")


class ProgressiveQualificationImageTests(unittest.TestCase):
    def test_repository_contract(self) -> None:
        validate_contract(ROOT)

    def test_mutations_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = Path(directory)
            for relative in (DOCKERFILE, ENTRYPOINT, BENCHMARK_LOCK, DOCKERIGNORE):
                destination = fixture / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(ROOT / relative, destination)
            dockerfile_mutations = {
                "missing_commit_binding": ("ARG GRAPHFORGE_COMMIT", "ARG OMITTED_COMMIT"),
                "malformed_commit_binding": (
                    "grep -Eq '^[0-9a-f]{40}$'",
                    "grep -Eq '.*'",
                ),
                "wrong_platform": ('test "${TARGETOS}/${TARGETARCH}" = linux/amd64', "true"),
                "mutable_base": (
                    "@sha256:a339861ae23e9abb272cea45dfafde21760d2ce6577a70f8a926153677902663",
                    "",
                ),
                "mutable_python_dependencies": ("uv sync --frozen", "uv sync"),
                "mutable_uv_installer": ('"uv==0.11.33"', '"uv"'),
                "missing_generator": (
                    "--package graphforge-benchmark-graph500-generator",
                    "--package omitted-generator",
                ),
                "missing_schema": (
                    "COPY benchmarks/schemas /opt/graphforge/benchmarks/schemas",
                    "COPY benchmarks/omitted /opt/graphforge/benchmarks/omitted",
                ),
                "missing_source_tree_hash": (
                    "sha256sum /tmp/source-tree.tar",
                    "echo unhashed-source-tree",
                ),
                "missing_manifest_source_identity": (
                    r"\"source_tree_sha256\":\"${source_tree_sha256}\"",
                    r"\"source_tree_sha256\":\"omitted\"",
                ),
                "missing_manifest_binary_identity": (
                    r"\"gf_sha256\":\"${gf_sha256}\"",
                    r"\"gf_sha256\":\"omitted\"",
                ),
                "extra_manifest_field": (
                    r"\"executables\":{",
                    r"\"unexpected\":true,\"executables\":{",
                ),
                "writable_manifest": (
                    "chmod 0444 /opt/graphforge/build-manifest.json",
                    "chmod 0644 /opt/graphforge/build-manifest.json",
                ),
                "public_service": ('CMD ["--help"]', "EXPOSE 8080"),
                "credential_input": ('CMD ["--help"]', "ENV PROVIDER_TOKEN=unsafe"),
                "floating_os_dependencies": (
                    "RUN mkdir -p /opt/graphforge /work",
                    "RUN apt-get update && apt-get install -y util-linux\n"
                    "RUN mkdir -p /opt/graphforge /work",
                ),
                "named_runtime_user": (
                    "RUN mkdir -p /opt/graphforge /work",
                    "RUN useradd benchexec\nRUN mkdir -p /opt/graphforge /work",
                ),
            }
            for name, (old, new) in dockerfile_mutations.items():
                with self.subTest(name=name):
                    destination = fixture / DOCKERFILE
                    original = destination.read_text(encoding="utf-8")
                    self.assertIn(old, original)
                    destination.write_text(original.replace(old, new, 1), encoding="utf-8")
                    with self.assertRaises(ContractError):
                        validate_contract(fixture)
                    destination.write_text(original, encoding="utf-8")

            entrypoint_mutations = {
                "plain_directory_work_root": (
                    "os.path.ismount(WORK_ROOT)",
                    "os.path.isdir(WORK_ROOT)",
                ),
                "missing_privilege_drop": ("os.setuid(RUN_UID)", "pass"),
                "recursive_chown": (
                    "os.chown(WORK_ROOT, RUN_UID, RUN_GID, follow_symlinks=False)",
                    "os.chown(WORK_ROOT, RUN_UID, RUN_GID, recursive=True)",
                ),
                "network_behavior": (
                    "os.chdir(WORK_ROOT)",
                    "os.system('wget https://provider.invalid')\n        os.chdir(WORK_ROOT)",
                ),
                "ambient_environment": (
                    '"HOME": "/work",',
                    '"HOME": os.environ["HOME"],',
                ),
                "expanded_environment": (
                    '"HOME": "/work",',
                    '"UNREVIEWED": "value",\n    "HOME": "/work",',
                ),
                "wrong_numeric_identity": ("RUN_UID = 10001", "RUN_UID = 10002"),
                "unsafe_python_search_path": (
                    '[PYTHON, "-P", "-m", MODULE, *sys.argv[1:]]',
                    '[PYTHON, "-m", MODULE, *sys.argv[1:]]',
                ),
            }
            for name, (old, new) in entrypoint_mutations.items():
                with self.subTest(name=name):
                    destination = fixture / ENTRYPOINT
                    original = destination.read_text(encoding="utf-8")
                    self.assertIn(old, original)
                    destination.write_text(original.replace(old, new, 1), encoding="utf-8")
                    with self.assertRaises(ContractError):
                        validate_contract(fixture)
                    destination.write_text(original, encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
