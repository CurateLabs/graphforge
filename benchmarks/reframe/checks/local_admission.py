import reframe as rfm
from reframe.core.builtins import sanity_function
import reframe.utility.sanity as sn


@rfm.simple_test
class LocalBenchExecAdmission(rfm.RunOnlyRegressionTest):
    valid_systems = ["graphforge-local:local"]  # noqa: RUF012
    valid_prog_environs = ["builtin"]  # noqa: RUF012
    # The system interpreter owns the official Ubuntu BenchExec package and
    # its package-managed cgroup configuration. ReFrame itself remains locked
    # in the benchmark workspace environment.
    executable = "/usr/bin/python3"
    executable_opts = ["-m", "graphforge_bench.local_admission"]  # noqa: RUF012

    @sanity_function
    def validate_typed_result(self):
        return sn.assert_found(r'"result": "passed"', self.stdout)
