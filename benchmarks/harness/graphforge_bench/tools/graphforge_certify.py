"""BenchExec tool-info module for the public GraphForge certification runner."""

from benchexec.tools.template import BaseTool2


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
        return [executable, "run", *task.input_files_or_identifier, "evidence.json"]

    def determine_result(self, run):
        if run.was_timeout:
            return "TIMEOUT"
        if run.was_terminated:
            return "KILLED"
        return "DONE" if run.exit_code is not None and run.exit_code.value == 0 else "ERROR"
