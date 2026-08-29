import reframe as rfm
from reframe.core.builtins import sanity_function
import reframe.utility.sanity as sn


@rfm.simple_test
class LocalBenchExecAdmission(rfm.RunOnlyRegressionTest):
    valid_systems = ["graphforge-local:local"]  # noqa: RUF012
    valid_prog_environs = ["builtin"]  # noqa: RUF012
    executable = "python"
    executable_opts = ["-m", "graphforge_bench.local_admission"]  # noqa: RUF012

    @sanity_function
    def validate_typed_result(self):
        return sn.assert_found(r'"result": "(passed|disqualified)"', self.stdout)
