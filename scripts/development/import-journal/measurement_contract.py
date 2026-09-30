"""Qualification and provenance shared by the S22 measurement methods."""

import hashlib
from itertools import pairwise
import json
import os
from pathlib import Path
import resource

if not __debug__:
    raise RuntimeError("measurement qualification requires assertions; disable Python optimization")

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


def baseline_source_publication(source):
    """Admit zero new namespace barriers only for the reviewed baseline helper.

    The brace scan only locates the pinned function bytes; unknown bodies fail
    the content pin and must be reviewed before changing this specific method.
    CLI allocation diagnostics are absent, so the reviewed helper does only
    the rename at this boundary, with no file/directory durability barrier.
    """
    start = source.index("    fn publish_source(")
    opening = source.index("{", start)
    depth = 0
    body = None
    for end in range(opening, len(source)):
        depth += (source[end] == "{") - (source[end] == "}")
        if depth == 0:
            body = source[start : end + 1]
            break
    assert body is not None
    checksum = hashlib.sha256(body.encode()).hexdigest()
    assert checksum == "30f1a0bf1ff07ab1ecfe842a3f6b57dd6cd3a165af234b7d801375d686b71637", (
        "unreviewed baseline source-publication semantics",
        checksum,
    )
    return {
        "function_body_sha256": checksum,
        "known_zero_fields": ["fsync_calls", "fsync_elapsed_ns"],
        "unavailable_cost_reason": "baseline rename has no measured source_publication leaf",
    }


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


CGROUP_FIELDS = (
    "cpu.max",
    "cpu.max.burst",
    "cpu.weight",
    "cpuset.cpus.effective",
    "cpuset.mems.effective",
    "memory.max",
    "memory.high",
    "memory.low",
    "memory.min",
    "memory.swap.max",
    "memory.swap.high",
    "memory.oom.group",
    "io.max",
    "io.weight",
    "cgroup.controllers",
    "cgroup.subtree_control",
)


def cpu_set(value):
    result = set()
    for component in value.split(","):
        if not component:
            continue
        bounds = component.split("-")
        assert len(bounds) in (1, 2)
        first, last = int(bounds[0]), int(bounds[-1])
        assert 0 <= first <= last <= 1048576, "invalid CPU-set range"
        result.update(range(first, last + 1))
    return result


def cgroup_directory_state(path):
    try:
        return path.is_dir(), None
    except OSError as error:
        return False, type(error).__name__


def read_cgroup_field(path, field):
    try:
        return (path / field).read_text().strip(), None
    except FileNotFoundError:
        return None, "not_exposed"
    except OSError as error:
        return None, type(error).__name__


def parse_cgroup_constraints(fields):
    try:
        quota = None
        if fields["cpu.max"] is not None:
            amount, period = fields["cpu.max"].split()
            assert int(period) > 0
            if amount != "max":
                assert int(amount) > 0
                quota = int(amount) / int(period)
        cpus = (
            cpu_set(fields["cpuset.cpus.effective"])
            if fields["cpuset.cpus.effective"] is not None
            else None
        )
        limits = {}
        for field in ("memory.max", "memory.high", "memory.swap.max"):
            value = fields[field]
            limits[field] = int(value) if value not in (None, "max") else None
            assert limits[field] is None or limits[field] >= 0
        return {"quota": quota, "cpus": cpus, **limits}, None
    except (AssertionError, ValueError) as error:
        return None, str(error)


def inherited_cgroup_policy(mount, leaf, ancestor_visibility_complete=True):
    """Record every visible ancestor; derived limits are observed upper bounds."""
    mount, leaf = Path(mount).resolve(), Path(leaf).resolve()
    assert leaf.is_relative_to(mount), "cgroup membership escapes mount"
    levels = []
    unavailable = []
    errors = []
    paths = [leaf, *leaf.parents]
    for path in paths:
        if not path.is_relative_to(mount):
            break
        name = str(path.relative_to(mount))
        fields = {}
        accessible, access_error = cgroup_directory_state(path)
        if not accessible:
            errors.append({"ancestor": name, "reason": access_error or "ancestor_unavailable"})
        for field in CGROUP_FIELDS:
            value, reason = read_cgroup_field(path, field)
            fields[field] = value
            if reason is not None:
                entry = {"ancestor": name, "field": field, "reason": reason}
                unavailable.append(entry)
                if reason != "not_exposed":
                    errors.append(entry)
        levels.append({"ancestor": name, "accessible": accessible, "fields": fields})
    quotas, cpus, memories, highs, swaps = [], [], [], [], []
    for level in levels:
        constraints, error = parse_cgroup_constraints(level["fields"])
        if error is not None:
            errors.append(
                {"ancestor": level["ancestor"], "reason": "invalid_control", "error": error}
            )
            continue
        if constraints["quota"] is not None:
            quotas.append(constraints["quota"])
        if constraints["cpus"] is not None:
            cpus.append(constraints["cpus"])
        if constraints["memory.max"] is not None:
            memories.append(constraints["memory.max"])
        if constraints["memory.high"] is not None:
            highs.append(constraints["memory.high"])
        if constraints["memory.swap.max"] is not None:
            swaps.append(constraints["memory.swap.max"])
    allowed = sorted(set.intersection(*cpus)) if cpus else None
    quota = min(quotas) if quotas else None
    cpu_bounds = ([quota] if quota is not None else []) + (
        [len(allowed)] if allowed is not None else []
    )
    return {
        "mount_point": str(mount),
        "resolved_leaf": str(leaf),
        "ancestor_visibility_complete": ancestor_visibility_complete,
        "ancestors": levels,
        "unavailable_fields": unavailable,
        "observation_errors": errors,
        "effective_observed_constraints": {
            "cpu_quota_cores_upper_bound": quota,
            "cpuset_cpus_upper_bound": allowed,
            "cpu_cores_upper_bound": min(cpu_bounds) if cpu_bounds else None,
            "memory_bytes_upper_bound": min(memories) if memories else None,
            "swap_bytes_upper_bound": min(swaps) if swaps else None,
            "memory_high_throttle_bytes": min(highs) if highs else None,
            "derivation": (
                "minimum observed ancestor quota/memory and intersection of observed CPU sets"
            ),
            "complete": ancestor_visibility_complete and not unavailable and not errors,
            "missing_limit_meaning": (
                "no finite observed constraint; never an assertion of unlimited resources"
            ),
        },
    }


def decode_mount_path(value):
    return (
        value.replace("\\040", " ")
        .replace("\\011", "\t")
        .replace("\\012", "\n")
        .replace("\\134", "\\")
    )


def cgroup_resources(membership, mountinfo):
    """Resolve cgroup2 membership against mountinfo rather than guessing limits."""
    paths = [line.split("::", 1)[1] for line in membership.splitlines() if line.startswith("0::")]
    mounts = []
    for line in mountinfo.splitlines():
        before, separator, after = line.partition(" - ")
        if separator and after.split()[0] == "cgroup2":
            columns = before.split()
            mounts.append(
                (Path(decode_mount_path(columns[3])), Path(decode_mount_path(columns[4])))
            )
    if len(paths) != 1:
        return {
            "membership": paths,
            "observation_errors": ["cgroup2 membership unavailable/ambiguous"],
        }
    membership_path = Path(paths[0])
    candidates = [(root, mount) for root, mount in mounts if membership_path.is_relative_to(root)]
    if len(candidates) != 1:
        return {
            "membership": paths[0],
            "observation_errors": ["cgroup2 mount unavailable/ambiguous"],
        }
    root, mount = candidates[0]
    leaf = mount / membership_path.relative_to(root)
    policy = inherited_cgroup_policy(mount, leaf, ancestor_visibility_complete=str(root) == "/")
    policy.update(membership=paths[0], mount_root=str(root))
    if root != Path("/"):
        policy["observation_errors"].append("ancestors above cgroup mount root are not visible")
    return policy


def validate_resource_policy(resources):
    assert not resources["cgroup_policy"]["observation_errors"], (
        "unqualified cgroup resource policy",
        resources["cgroup_policy"]["observation_errors"],
    )


def ambient_resources():
    policy = cgroup_resources(
        Path("/proc/self/cgroup").read_text(), Path("/proc/self/mountinfo").read_text()
    )
    return {
        "environment": {key: os.environ.get(key) for key in RESOURCE_ENV},
        "temporary_directory_policy": "TMPDIR overridden with lane_root/tmp",
        "caller_affinity": sorted(os.sched_getaffinity(0)),
        "scheduler": os.sched_getscheduler(0),
        "nice": os.getpriority(os.PRIO_PROCESS, 0),
        "cgroup_policy": policy,
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
        "resource-policy-after.json",
        *(f"run/{name}" for name in RECEIPTS),
    }
    assert set(qualification["artifact_sha256"]) == required, "incomplete qualified artifact set"
    for name, expected in qualification["artifact_sha256"].items():
        assert digest(root / name) == expected, ("qualified artifact changed", name)
    validate_resource_policy(identity["ambient_resources"])
    after_resources = read_json(root / "resource-policy-after.json")
    validate_resource_policy(after_resources)
    assert after_resources == identity["ambient_resources"], (
        "resource policy changed during measurement"
    )
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
