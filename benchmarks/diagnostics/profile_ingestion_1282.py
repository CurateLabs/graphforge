"""Decode frozen CPU profiles and publish content-free sample counts and hashes."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil
import subprocess

from graphforge_bench.ingestion_attribution import cpu_summary
from report_ingestion_1282 import digest, summarize


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    report = summarize(args.directory)
    if (
        report["suite"] != "perf"
        or digest(Path(shutil.which("perf"))) != report["profiler"]["sha256"]
    ):
        raise ValueError("CPU profile or decoder identity mismatch")
    result = {
        "issue": 1282,
        "claim": "diagnostic_CPU_samples_not_wall_time",
        "input_summary_sha256": report["summary_sha256"],
        "analysis_script_sha256": digest(Path(__file__)),
        "profiler": report["profiler"],
        "commands": {},
    }
    for observation in report["commands"]:
        if "perf.data_sha256" not in observation:
            continue
        label = observation["label"]
        raw = args.directory / f"{label}.perf.data"
        artifacts = {}
        for suffix, switches in (("cpu-leaf", ["-G"]), ("cpu-stack", ["--no-inline"])):
            command = [
                "sudo",
                "-n",
                "perf",
                "script",
                "-f",
                *switches,
                "-i",
                str(raw),
                "-F",
                "comm,pid,tid,time,event,ip,sym,dso",
                "--demangle",
            ]
            command_path = args.directory / f"{label}.{suffix}.command.json"
            command_path.write_text(json.dumps(command) + "\n")
            output = args.directory / f"{label}.{suffix}"
            error = args.directory / f"{label}.{suffix}.stderr"
            with output.open("w") as out, error.open("w") as err:
                subprocess.run(command, stdout=out, stderr=err, check=True)
            artifacts.update({path.name: digest(path) for path in (command_path, output, error)})
        result["commands"][label] = {
            "input_perf_sha256": observation["perf.data_sha256"],
            "artifacts": artifacts,
            **cpu_summary(
                (args.directory / f"{label}.cpu-leaf").read_text().splitlines(),
                (args.directory / f"{label}.cpu-stack").read_text().splitlines(),
            ),
        }
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
