"""BenchExec tool-info module for the public GraphForge certification runner."""

import os
from pathlib import Path

from benchexec.tools.template import BaseTool2


def _certify_evidence_path() -> str:
    work = Path("/work")
    try:
        if work.is_dir() and os.path.ismount(work):
            tmp = work / "tmp"
            tmp.mkdir(exist_ok=True)
            return str(tmp / "graphforge-certify-evidence.json")
    except OSError:
        pass
    host_root = os.environ.get("GRAPHFORGE_HOST_WORK_ROOT")
    if host_root:
        try:
            tmp = Path(host_root) / "tmp"
            tmp.mkdir(parents=True, exist_ok=True)
            return str(tmp / "graphforge-certify-evidence.json")
        except OSError:
            pass
    return "evidence.json"


class Tool(BaseTool2):
    def executable(self, tool_locator):
        return tool_locator.find_executable("graphforge-benchmark-certify")

    def name(self):
        return "GraphForge public certification"

    def project_url(self):
        return "https://github.com/CurateLabs/graphforge"

    def cmdline(self, executable, options, task, rlimits):
        if options:
            raise ValueError("certification definition does not accept opaque options")
        return [executable, "run", *task.input_files_or_identifier, _certify_evidence_path()]

    def determine_result(self, run):
        if run.was_timeout:
            return "TIMEOUT"
        if run.was_terminated:
            return "KILLED"
        return "DONE" if run.exit_code is not None and run.exit_code.value == 0 else "ERROR"
