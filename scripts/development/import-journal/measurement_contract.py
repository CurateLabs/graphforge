"""Qualification and provenance shared by the S22 measurement methods."""

import hashlib
from itertools import pairwise
import json
import os
from pathlib import Path
import resource

CONTRACT = "graphforge-import-journal-measurement/1"
BUILD_NAMES = {
    "cargo",
    "rustc",
    "rustdoc",
    "clippy-driver",
    "bazel",
    "bazelisk",
    "maturin",
    "rustfmt",
    "cc",
    "cc1",
    "gcc",
    "clang",
    "ld",
    "lld",
    "rust-lld",
}
RECEIPTS = (
    "receipt-0-begin.json",
    "receipt-1-register-nodes.json",
    "receipt-2-register-edges.json",
    "receipt-3-stage-seal.json",
    "receipt-4-commit.json",
)
RESOURCE_ENV = (
    "RAYON_NUM_THREADS",
    "TOKIO_WORKER_THREADS",
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "MKL_NUM_THREADS",
    "MALLOC_CONF",
    "GLIBC_TUNABLES",
    "LD_PRELOAD",
    "LD_LIBRARY_PATH",
    "RUST_MIN_STACK",
    "TMP",
    "TEMP",
)
BUILD_ENV = (
    "CARGO_BUILD_JOBS",
    "CARGO_INCREMENTAL",
    "RUSTFLAGS",
    "CARGO_ENCODED_RUSTFLAGS",
    "RUSTC_WRAPPER",
    "RUSTC_WORKSPACE_WRAPPER",
    "CARGO_PROFILE_DEV_DEBUG",
    "CARGO_PROFILE_DEV_OPT_LEVEL",
    "CARGO_PROFILE_DEV_LTO",
    "CARGO_PROFILE_DEV_CODEGEN_UNITS",
    "CARGO_PROFILE_DEV_PANIC",
    "CARGO_PROFILE_DEV_INCREMENTAL",
    "CARGO_PROFILE_DEV_DEBUG_ASSERTIONS",
    "CARGO_PROFILE_DEV_OVERFLOW_CHECKS",
    "CARGO_PROFILE_DEV_STRIP",
    "CARGO_PROFILE_DEV_SPLIT_DEBUGINFO",
    "CARGO_PROFILE_DEV_RPATH",
    "CARGO_PROFILE_RELEASE_DEBUG",
    "CARGO_PROFILE_RELEASE_OPT_LEVEL",
    "CARGO_PROFILE_RELEASE_LTO",
    "CARGO_PROFILE_RELEASE_CODEGEN_UNITS",
    "CARGO_PROFILE_RELEASE_PANIC",
    "CARGO_PROFILE_RELEASE_INCREMENTAL",
    "CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS",
    "CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS",
    "CARGO_PROFILE_RELEASE_STRIP",
    "CARGO_PROFILE_RELEASE_SPLIT_DEBUGINFO",
    "CARGO_PROFILE_RELEASE_RPATH",
)
BUILD_SETTINGS = (
    "profile",
    "features",
    "default_features",
    "target",
    "rustc_version",
    "cargo_version",
    "cargo_args",
    "build_environment",
)


def require_external_output(output, repository, method_root):
    """Refuse raw output inside either the source or stored-method repository."""
    roots = [Path(repository).resolve()]
    for parent in (Path(method_root).resolve(), *Path(method_root).resolve().parents):
        if (parent / ".git").exists():
            roots.append(parent)
            break
    assert not any(Path(output).resolve().is_relative_to(root) for root in roots), (
        "raw evidence must stay outside every repository",
        output,
    )


def digest(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as source:
        while chunk := source.read(1024 * 1024):
            value.update(chunk)
    return value.hexdigest()


def write_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2) + "\n")


def read_json(path):
    return json.loads(Path(path).read_text())


def build_settings(receipt):
    settings = {key: receipt[key] for key in BUILD_SETTINGS}
    assert settings["profile"] and settings["target"]
    assert isinstance(settings["default_features"], bool)
    assert isinstance(settings["features"], list) and all(
        isinstance(x, str) for x in settings["features"]
    )
    assert settings["features"] == sorted(set(settings["features"])), (
        "features must be sorted and unique"
    )
    assert settings["rustc_version"] and settings["cargo_version"]
    assert isinstance(settings["cargo_args"], list) and settings["cargo_args"]
    arguments = settings["cargo_args"]
    assert arguments[0] == "build" and "--locked" in arguments
    profile = "release" if "--release" in arguments else "dev"
    if "--profile" in arguments:
        profile = arguments[arguments.index("--profile") + 1]
    assert settings["profile"] == profile, "build profile differs from cargo arguments"
    assert settings["default_features"] == ("--no-default-features" not in arguments)
    features = []
    for index, argument in enumerate(arguments):
        value = (
            arguments[index + 1]
            if argument == "--features"
            else argument.removeprefix("--features=")
            if argument.startswith("--features=")
            else ""
        )
        features.extend(value.replace(",", " ").split())
    assert settings["features"] == sorted(set(features)), "features differ from cargo arguments"
    if "--target" in arguments:
        assert settings["target"] == arguments[arguments.index("--target") + 1]
    assert isinstance(settings["build_environment"], dict)
    assert set(settings["build_environment"]) == set(BUILD_ENV), (
        "missing build environment settings"
    )
    assert all(
        value is None or isinstance(value, str) for value in settings["build_environment"].values()
    )
    return settings


def ambient_resources():
    cgroup = next(
        (
            line.split("::", 1)[1]
            for line in Path("/proc/self/cgroup").read_text().splitlines()
            if line.startswith("0::")
        ),
        "",
    )
    root = Path("/sys/fs/cgroup") / cgroup.lstrip("/")
    names = (
        "cpu.max",
        "cpu.weight",
        "cpuset.cpus.effective",
        "memory.max",
        "memory.swap.max",
        "io.max",
    )
    limits = {
        name: (root / name).read_text().strip() if (root / name).is_file() else None
        for name in names
    }
    return {
        "environment": {key: os.environ.get(key) for key in RESOURCE_ENV},
        "temporary_directory_policy": "TMPDIR overridden with lane_root/tmp",
        "caller_affinity": sorted(os.sched_getaffinity(0)),
        "scheduler": os.sched_getscheduler(0),
        "nice": os.getpriority(os.PRIO_PROCESS, 0),
        "cgroup_limits": limits,
        "rlimits": {
            name: list(resource.getrlimit(getattr(resource, name)))
            for name in (
                "RLIMIT_AS",
                "RLIMIT_CPU",
                "RLIMIT_DATA",
                "RLIMIT_FSIZE",
                "RLIMIT_NOFILE",
                "RLIMIT_NPROC",
                "RLIMIT_STACK",
            )
        },
        "runexec_policy": ["--no-container", "--cores", "0-15"],
    }


def quiet_metrics(samples, ticks_per_second):
    assert len(samples) >= 13, "incomplete quiet window"
    assert samples[-1]["mono"] - samples[0]["mono"] >= 59.0, "quiet window shorter than 60 seconds"
    assert not any(
        p["name"] in BUILD_NAMES
        or p["name"] in ("gf", "gf.real")
        or p["name"].startswith("graphforge_")
        for s in samples
        for p in s["processes"]
    ), "quiet process overlap"
    intervals = []
    for previous, current in pairwise(samples):
        elapsed = current["mono"] - previous["mono"]
        delta = current["system_busy_ticks"] - previous["system_busy_ticks"]
        assert elapsed > 0 and delta >= 0
        intervals.append(delta / ticks_per_second / elapsed)
    return {
        "mean_busy_cores": sum(intervals) / len(intervals),
        "max_busy_cores": max(intervals),
        "mean_limit": 0.2,
        "max_limit": 0.5,
        "ticks_per_second": ticks_per_second,
    }


def runexec_result(path):
    """BenchExec reports workload status separately from its own process exit."""
    values = {}
    for line in Path(path).read_text().splitlines():
        key, separator, value = line.partition("=")
        if separator and key in ("returnvalue", "exitsignal", "terminationreason"):
            assert key not in values, ("duplicate runexec outcome", key)
            values[key] = value
    return {
        "returnvalue": int(values["returnvalue"]) if "returnvalue" in values else None,
        "exitsignal": int(values["exitsignal"]) if "exitsignal" in values else None,
        "terminationreason": values.get("terminationreason"),
    }


def successful_workload(result):
    assert type(result["returnvalue"]) is int and result["returnvalue"] == 0, (
        "failed or missing workload returnvalue",
        result,
    )
    assert result["exitsignal"] is None and result["terminationreason"] is None, (
        "terminated workload",
        result,
    )


def validate_qualification(root, identity):
    root = Path(root)
    qualification = read_json(root / "qualification.json")
    assert qualification["contract"] == CONTRACT
    assert qualification["completed"] is True and qualification["qualified"] is True, (
        "unqualified/incomplete lane"
    )
    assert (
        type(qualification["runexec_exit_code"]) is int and qualification["runexec_exit_code"] == 0
    ), "failed runexec process"
    workload = runexec_result(root / "runexec.txt")
    assert qualification["workload_result"] == workload, "workload outcome mismatch"
    successful_workload(workload)
    assert qualification["cold_cache_reset"] is True and qualification["inputs_verified"] is True
    assert qualification["quiet_window_passed"] is True
    assert (
        type(qualification["compiler_overlap_samples"]) is int
        and qualification["compiler_overlap_samples"] == 0
    )
    for key in ("source_sha", "binary_sha256", "lane", "pair", "method_sha256"):
        assert qualification[key] == identity[key], ("qualification identity mismatch", key)
    required = {
        "quiet-before.json",
        "quiet-cpu.json",
        "host-during.json",
        "build-provenance.json",
        "inputs.sha256",
        "input-verification.txt",
        "identity.json",
        "runexec.txt",
        "runexec.stderr",
        "workload.log",
        *(f"run/{name}" for name in RECEIPTS),
    }
    assert set(qualification["artifact_sha256"]) == required, "incomplete qualified artifact set"
    for name, expected in qualification["artifact_sha256"].items():
        assert digest(root / name) == expected, ("qualified artifact changed", name)
    cpu = read_json(root / "quiet-cpu.json")
    assert type(cpu["ticks_per_second"]) is int and cpu["ticks_per_second"] > 0
    assert cpu == quiet_metrics(read_json(root / "quiet-before.json"), cpu["ticks_per_second"])
    assert cpu["mean_busy_cores"] <= 0.2 and cpu["max_busy_cores"] <= 0.5, "busy quiet window"
    during = read_json(root / "host-during.json")
    assert during and len(during) == qualification["during_samples"], "incomplete during samples"
    assert not any(p["name"] in BUILD_NAMES for s in during for p in s["processes"]), (
        "compiler overlap"
    )
    build = read_json(root / "build-provenance.json")
    assert (
        build["source_sha"] == identity["source_sha"]
        and build["binary_sha256"] == identity["binary_sha256"]
    )
    assert build_settings(build) == identity["build_settings"]
    assert digest(root / "build-provenance.json") == identity["build_provenance_sha256"]
    return qualification
