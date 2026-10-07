"""BenchExec tool-info module for one GDC scorecard rung phase.

The tool is the staged `graphforge-gdc-phase` launcher, which runs
`graphforge_bench.gdc_phase` on one phase task document (convert, load or
query). BenchExec measures the whole process tree; the phase executor prints
its own `graphforge-gdc-phase/1` telemetry line, which the rung runner reads
from the run's log file.
"""

from benchexec.tools.template import BaseTool2

EXECUTABLE = "graphforge-gdc-phase"


class Tool(BaseTool2):
    def executable(self, tool_locator):
        return tool_locator.find_executable(EXECUTABLE)

    def name(self):
        return "GraphForge GDC scorecard rung phase"

    def project_url(self):
        return "https://github.com/CurateLabs/graphforge"

    def cmdline(self, executable, options, task, rlimits):
        if options:
            raise ValueError("the GDC phase definition does not accept opaque options")
        return [executable, "run", *task.input_files_or_identifier]

    def determine_result(self, run):
        if run.was_timeout:
            return "TIMEOUT"
        if run.was_terminated:
            return "KILLED"
        return "DONE" if run.exit_code is not None and run.exit_code.value == 0 else "ERROR"
