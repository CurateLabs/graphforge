import json

import reframe as rfm
import reframe.utility.sanity as sn


@rfm.simple_test
class LocalBenchExecAdmission(rfm.RunOnlyRegressionTest):
    valid_systems = ["graphforge-local:local"]
    valid_prog_environs = ["builtin"]
    executable = "python"
    executable_opts = ["-m", "graphforge_bench.local_admission"]

    @sanity_function
    def validate_typed_result(self):
        return sn.assert_found(r'"result": "(passed|disqualified)"', self.stdout)
