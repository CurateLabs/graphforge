"""Offline regression evidence for preserved operator measurement tools."""

import importlib.util
import json
import math
import os
from pathlib import Path
import subprocess

import pytest

TOOLS = Path(__file__).resolve().parents[1] / "tools"


def module(relative):
    spec = importlib.util.spec_from_file_location("measurement", TOOLS / relative)
    loaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded)
    return loaded


def executable(path, text):
    path.write_text("#!/usr/bin/env bash\n" + text)
    path.chmod(0o755)
    return str(path)


def test_perf_zero_missing_and_integer_times(tmp_path):
    parser = module("perf-stat-validate/summarize-perf.py")
    raw = tmp_path / "perf.txt"
    raw.write_text(
        "0 cache-misses\n100 cache-references\n2 seconds time elapsed\n"
        "0 seconds user\n<not counted> cycles\n"
    )
    values = parser.parse(raw)
    assert values["time elapsed"] == 2
    assert values["user"] == 0
    assert "seconds" not in values
    assert parser.ratio(values["cache-misses"], values["cache-references"]) == 0
    assert math.isnan(parser.ratio(None, 100))
    assert math.isnan(parser.ratio(1, 0))


def test_fio_job_options_override_global(tmp_path):
    parser = module("f1-fio/summarize.py")
    section = {
        "runtime": 1000,
        "bw_bytes": 0,
        "iops": 0,
        "total_ios": 1,
        "lat_ns": {"mean": 1000000000},
    }
    job = {
        "jobname": "sample",
        "job_runtime": 1000,
        "job options": {"iodepth": "1", "numjobs": "2"},
        "read": section,
        "write": section,
        "sync": section,
    }
    row = parser.row(tmp_path / "run.out", {"iodepth": "8", "numjobs": "1"}, job)
    assert "nan" not in row.split()[-4:]
    assert row.split()[-4:-1] == ["50.0", "50.0", "50.0"]
    job["job options"]["iodepth"] = "8"
    assert parser.row(tmp_path / "run.out", {}, job).split()[-4:-1] == ["nan"] * 3


@pytest.mark.parametrize("reply", ["exit 7", "echo BROKEN"])
def test_helper_errors_fail_without_waiting(tmp_path, reply):
    helper = executable(tmp_path / "helper", reply)
    result = subprocess.run(
        [
            "bash",
            "-c",
            'source "$1"; require_quiet_helper; wait_quiet',
            "_",
            str(TOOLS / "measurement-common.sh"),
        ],
        env=os.environ | {"QUIET_HELPER": helper, "OUT": str(tmp_path)},
        capture_output=True,
        check=False,
        timeout=5,
    )
    assert result.returncode != 0


@pytest.mark.parametrize("mode", ["success", "failed", "invalid", "missing"])
def test_scaling_waits_for_children_and_validates_receipts(tmp_path, mode):
    bins = tmp_path / "bin"
    bins.mkdir()
    executable(bins / "sync", "exit 0")
    executable(bins / "sudo", "cat >/dev/null")
    helper = executable(bins / "quiet", "echo QUIET")
    generator = executable(bins / "generator", "exit 0")
    gf = executable(
        bins / "gf",
        """[[ "$TMPDIR" == */results/tmp ]] || exit 32
if [[ "$*" == *'import-session validate'* ]]; then
  case "$MODE" in
    failed) exit 9 ;;
    invalid) echo '{"outcome":"rejected"}' ;;
    missing) exit 0 ;;
    *) echo '{"outcome":"validated"}' ;;
  esac
fi
""",
    )
    out = tmp_path / "results"
    env = os.environ | {
        "PATH": str(bins) + ":" + os.environ["PATH"],
        "QUIET_HELPER": helper,
        "MODE": mode,
        "TMPDIR": str(tmp_path),
    }
    command = [
        "bash",
        str(TOOLS / "perf-stat-validate/run-scaling.sh"),
        str(out),
        gf,
        generator,
        "18",
        "2",
    ]
    result = subprocess.run(command, env=env, capture_output=True, check=False, timeout=10)
    assert (result.returncode == 0) == (mode == "success"), result.stderr
    assert len(list(out.glob("scaling-*-p*.json"))) == 2
    marker = out / "preserve"
    marker.write_text("original")
    repeat = subprocess.run(command, env=env, capture_output=True, check=False, timeout=10)
    assert repeat.returncode != 0
    assert marker.read_text() == "original"


@pytest.mark.parametrize("original", ["0", "1"])
@pytest.mark.parametrize("mode", ["success", "failed", "invalid"])
def test_perf_restores_original_watchdog_and_propagates_failure(tmp_path, original, mode):
    bins = tmp_path / "bin"
    bins.mkdir()
    executable(bins / "sync", "exit 0")
    executable(
        bins / "sudo",
        """shift # -n
case "$1" in
  sysctl)
    if [[ "$2" == -n ]]; then echo "$ORIGINAL";
    else echo "$3" >> "$HOST_LOG"; fi ;;
  tee) cat >/dev/null ;;
  perf)
    [[ "$2" == --version ]] && { echo 'perf version stub'; exit; }
    [[ "$MODE" == failed ]] && exit 8
    while [[ "$1" != -- ]]; do shift; done
    shift
    exec "$@" ;;
  -u) shift 2; exec "$@" ;;
  *) exit 19 ;;
esac
""",
    )
    helper = executable(bins / "quiet", "echo QUIET")
    generator = executable(bins / "generator", "exit 0")
    gf = executable(
        bins / "gf",
        """if [[ "$MODE" == invalid ]]; then
  echo '{"outcome":"rejected"}'
else
  echo '{"outcome":"validated"}'
fi
""",
    )
    host_log = tmp_path / "host.log"
    env = os.environ | {
        "PATH": str(bins) + ":" + os.environ["PATH"],
        "QUIET_HELPER": helper,
        "MODE": mode,
        "TMPDIR": str(tmp_path),
        "ORIGINAL": original,
        "HOST_LOG": str(host_log),
        "PASSES": "A",
        "SKIP_ANCHORS": "1",
    }
    result = subprocess.run(
        [
            "bash",
            str(TOOLS / "perf-stat-validate/run-perf.sh"),
            str(tmp_path / "results"),
            gf,
            generator,
        ],
        env=env,
        capture_output=True,
        check=False,
        timeout=10,
    )
    assert (result.returncode == 0) == (mode == "success"), result.stderr
    assert host_log.read_text().splitlines() == [
        "kernel.nmi_watchdog=0",
        f"kernel.nmi_watchdog={original}",
    ]


@pytest.mark.parametrize("fail", ["0", "1"])
def test_fio_isolates_data_and_fails_truthfully(tmp_path, fail):
    bins = tmp_path / "bin"
    bins.mkdir()
    executable(bins / "sudo", "cat >/dev/null")
    executable(bins / "sync", "exit 0")
    helper = executable(bins / "quiet", "echo QUIET")
    executable(
        bins / "fio",
        """[[ "$1" == --version ]] && { echo 'fio stub'; exit; }
printf '%s\\n' "$F1_DIR" >> "$DATA_LOG"
touch "$F1_DIR/pat.test" "$F1_DIR/app.test" "$F1_DIR/ctrl.bin"
[[ "$FAIL" == 0 ]]
""",
    )
    counters = {
        "read_bytes": 4096,
        "write_bytes": 4096,
        "read_calls": 1,
        "write_calls": 1,
        "fsync_calls": 1,
    }
    rung = tmp_path / "rung.json"
    rung.write_text(
        json.dumps(
            {
                "scale": 1,
                "live_edges": 1,
                "storage_attribution": {
                    "construction": {
                        "application_io": {"totals": counters, "phases": {"test": counters}}
                    }
                },
            }
        )
    )
    data = tmp_path / "data"
    data.mkdir()
    sentinel = data / "pat.unrelated"
    sentinel.write_text("preserve")
    env = os.environ | {
        "PATH": str(bins) + ":" + os.environ["PATH"],
        "QUIET_HELPER": helper,
        "FAIL": fail,
        "DATA_LOG": str(tmp_path / "data.log"),
    }
    result = subprocess.run(
        [
            "bash",
            str(TOOLS / "f1-fio/run-f1.sh"),
            str(rung),
            str(tmp_path / "output"),
            str(data),
        ],
        env=env,
        capture_output=True,
        check=False,
        timeout=10,
    )
    assert (result.returncode == 0) == (fail == "0"), result.stderr
    assert sentinel.read_text() == "preserve"
    paths = (tmp_path / "data.log").read_text().splitlines()
    assert paths
    assert all(Path(path).parent == data and Path(path) != data for path in paths)
