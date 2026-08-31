from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from graphforge_bench.progressive_ladder_qualification import (
    QualificationError,
    _load_provider_capacity,
    main,
    parser,
)
from tests.test_progressive_esc import projected_environment


class ProgressiveLadderQualificationTests(unittest.TestCase):
    def test_parser_requires_progressive_paths(self) -> None:
        with self.assertRaises(SystemExit):
            parser().parse_args(
                [
                    "--expected-sha",
                    "a" * 40,
                    "--execute",
                    "--confirm-disposable",
                ]
            )

    def test_missing_confirmation_writes_refused_result(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            result_path = Path(temporary) / "result.json"
            code = main(
                [
                    "--expected-sha",
                    "a" * 40,
                    "--output-dir",
                    temporary,
                    "--ledger",
                    str(Path(temporary) / "ledger.json"),
                    "--result-out",
                    str(result_path),
                    "--execute",
                ]
            )
            self.assertEqual(code, 1)
            document = json.loads(result_path.read_text())
            self.assertEqual(document["failure"], "authorization_refused")

    def test_missing_esc_projections_write_refused_result(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            result_path = Path(temporary) / "result.json"
            with patch.dict("os.environ", {}, clear=True):
                code = main(
                    [
                        "--expected-sha",
                        "a" * 40,
                        "--output-dir",
                        temporary,
                        "--ledger",
                        str(Path(temporary) / "ledger.json"),
                        "--result-out",
                        str(result_path),
                        "--execute",
                        "--confirm-disposable",
                    ]
                )
            self.assertEqual(code, 1)
            document = json.loads(result_path.read_text())
            self.assertEqual(document["failure"], "authorization_refused")

    def test_commit_mismatch_writes_refused_result(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            result_path = Path(temporary) / "result.json"
            with patch.dict("os.environ", projected_environment(), clear=True):
                code = main(
                    [
                        "--expected-sha",
                        "b" * 40,
                        "--output-dir",
                        temporary,
                        "--ledger",
                        str(Path(temporary) / "ledger.json"),
                        "--result-out",
                        str(result_path),
                        "--execute",
                        "--confirm-disposable",
                    ]
                )
            self.assertEqual(code, 1)
            document = json.loads(result_path.read_text())
            self.assertEqual(document["failure"], "authorization_refused")

    def test_malformed_provider_capacity_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            capacity_path = Path(temporary) / "capacity.json"
            capacity_path.write_text("{", encoding="utf-8")
            with self.assertRaisesRegex(QualificationError, "malformed"):
                _load_provider_capacity(capacity_path)


if __name__ == "__main__":
    unittest.main()
