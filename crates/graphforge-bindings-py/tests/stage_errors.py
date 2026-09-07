"""Execute the shared stage-error matrix through the rebuilt native binding."""

import json
from pathlib import Path

import graphforge


def check_case(forge: graphforge.GraphForge, case: dict, mode: str) -> None:
    try:
        if mode == "analyze":
            forge.execute(case["query"])
            forge.analyze(by="euler_circuit", directed=False)
        elif mode == "execute":
            forge.execute(case["query"])
        elif mode == "params":
            forge.execute(case["query"], params={})
        elif mode == "stream":
            list(forge.execute_stream(case["query"]))
        else:
            forge.explain(case["query"])
    except graphforge.GraphForgeError as error:
        prefix = "explain_" if mode == "explain" else "stream_" if mode == "stream" else ""
        assert type(error).__name__ == case.get(prefix + "python", case["python"]), (
            mode,
            case["id"],
            error,
        )
        assert error.code == case.get(prefix + "rust", case["rust"]), (
            mode,
            case["id"],
            error,
        )
        assert str(error) == case.get(prefix + "message", case["message"]), (
            mode,
            case["id"],
            error,
        )
        if "span" in case:
            assert error.span == tuple(case["span"])
        else:
            assert not hasattr(error, "span")
    else:
        raise AssertionError((mode, case["id"], "unexpected success"))


def main() -> None:
    matrix = json.loads(
        (
            Path(__file__).resolve().parents[2] / "graphforge-api/tests/stage_error_matrix.json"
        ).read_text()
    )
    forge = graphforge.GraphForge()
    for case in matrix:
        modes = (
            ("analyze",)
            if case.get("operation") == "analyze"
            else ("execute", "params", "stream", "explain")
        )
        for mode in modes:
            check_case(forge, case, mode)
    print(f"stage error matrix: {(len(matrix) - 1) * 4 + 1} native Python cases passed")


if __name__ == "__main__":
    main()
