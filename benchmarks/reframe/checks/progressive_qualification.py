from pathlib import Path

import reframe as rfm
from reframe.core.builtins import parameter, run_after, sanity_function
import reframe.utility.sanity as sn


@rfm.simple_test
class Graph500ProgressiveQualificationProfile(rfm.RunOnlyRegressionTest):
    """Manual-only execution of an ordinary GraphForge qualification profile."""

    profile_id = parameter(
        [
            "graph500-s18-local",
            "graph500-s19-local",
            "graph500-s20-provider",
            "graph500-s22-provider",
            "graph500-s26-provider",
        ]
    )
    valid_prog_environs = ["builtin"]  # noqa: RUF012
    tags = {"manual", "graph500-progressive"}  # noqa: RUF012
    executable = "cargo"

    @run_after("init")
    def configure_profile(self):
        root = Path(__file__).resolve().parents[2]
        execution = "local" if self.profile_id.endswith("-local") else "provider"
        self.valid_systems = (  # unavailable provider profiles cannot pass on local
            ["graphforge-local:local"] if execution == "local" else ["graphforge-provider:provider"]
        )
        self.executable_opts = [
            "run",
            "--locked",
            "--manifest-path",
            str(root / "Cargo.toml"),
            "-p",
            "graphforge-benchmark-certify",
            "--",
            "run",
            str(root / "profiles/graph500" / f"{self.profile_id.removeprefix('graph500-')}.json"),
            "evidence.json",
        ]

    @sanity_function
    def validate_completed_lifecycle(self):
        return sn.all(
            [
                sn.assert_eq(self.exitcode, 0),
                sn.assert_found(r'"phase":"reopen_proof"', self.stdout),
                sn.assert_found(r'"status":"passed"', self.stdout),
            ]
        )
