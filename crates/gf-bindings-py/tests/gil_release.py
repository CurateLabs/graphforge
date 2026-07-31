"""Deterministic acceptance for synchronous native GIL release (#2498)."""

from __future__ import annotations

from pathlib import Path
import re
import threading

from graphforge import _graphforge_rs as native

SOURCE = Path(__file__).resolve().parents[1] / "src" / "lib.rs"


def _detached_bodies(source: str) -> list[str]:
    """Extract balanced ``detach`` closures for source-level safety checks."""
    bodies: list[str] = []
    cursor = 0
    while (opening := source.find("detach(||", cursor)) >= 0:
        opening = source.index("(", opening)
        depth = 1
        quote: str | None = None
        escaped = False
        for closing in range(opening + 1, len(source)):
            char = source[closing]
            if quote is not None:
                if escaped:
                    escaped = False
                elif char == "\\":
                    escaped = True
                elif char == quote:
                    quote = None
            elif char == '"':
                quote = char
            elif char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
                if depth == 0:
                    bodies.append(source[opening + 1 : closing])
                    cursor = closing + 1
                    break
        else:
            raise AssertionError("unbalanced Python::detach call")
    return bodies


def _borrowed_parameter_captures(source: str) -> list[str]:
    """Find borrowed PyO3 inputs used by detach without an owned shadow."""
    lines = source.splitlines()
    offsets: list[int] = []
    offset = 0
    for line in lines:
        offsets.append(offset)
        offset += len(line) + 1

    captures: list[str] = []
    for line_index, line in enumerate(lines):
        if "detach(||" not in line:
            continue
        signature_start = line_index
        while signature_start >= 0 and not re.match(
            r" {0,4}(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?fn "
            r"[A-Za-z_][A-Za-z0-9_]*",
            lines[signature_start],
        ):
            signature_start -= 1
        assert signature_start >= 0, "detach closure is not inside a function"
        signature_end = signature_start
        signature_lines: list[str] = []
        while signature_end < len(lines):
            signature_lines.append(lines[signature_end])
            if "{" in lines[signature_end]:
                break
            signature_end += 1
        signature = "\n".join(signature_lines)
        method = re.search(r"fn ([A-Za-z_][A-Za-z0-9_]*)", signature)
        assert method
        borrowed = {
            match.group(1)
            for match in re.finditer(r"([A-Za-z_][A-Za-z0-9_]*)\s*:\s*([^,\n\)]+)", signature)
            if "&str" in match.group(2) or "&[u8]" in match.group(2)
        }

        absolute = offsets[line_index] + line.index("detach(||")
        opening = source.index("(", absolute)
        depth = 0
        for closing in range(opening, len(source)):
            if source[closing] == "(":
                depth += 1
            elif source[closing] == ")":
                depth -= 1
                if depth == 0:
                    break
        else:
            raise AssertionError("unbalanced detach closure")
        body = source[opening:closing]
        attached_prefix = "\n".join(lines[signature_end + 1 : line_index])
        for parameter in borrowed:
            used_detached = re.search(rf"\b{parameter}\b", body)
            owned_shadow = any(
                (binding := re.search(r"\blet\s+(.+?)\s*=", line))
                and re.search(rf"\b{parameter}\b", binding.group(1))
                for line in attached_prefix.splitlines()
            )
            if used_detached and not owned_shadow:
                captures.append(f"{method.group(1)}:{line_index + 1}:{parameter}")
    return captures


def check_native_call_inventory() -> None:
    source = SOURCE.read_text(encoding="utf-8")
    bodies = _detached_bodies(source)
    detached = "\n".join(bodies)

    # Every release-contract family has at least one explicit native boundary.
    required = {
        "execute",  # query
        "add_node",  # construction
        "checkpoint",  # checkpoint
        "list_provenance_history",  # history
        "rank",  # M18
        "find_with_diagnostics",  # M19
        "create_assertion",  # M20
        "resolve_belief_projection",  # M21
        "project_capabilities",  # release capability contract
        "inspect_runtime_catalog",  # ontology runtime-catalog inspection
        "suggest_ontology",  # ontology draft suggestion
        "validate_ontology",  # ontology validation
        "export_ontology",  # ontology export
        "workspace_ontology",  # adopted ontology inspection
        "adopt_ontology",  # ontology adoption
        "clear_ontology",  # ontology removal
    }
    missing = sorted(name for name in required if f".{name}(" not in detached)
    assert not missing, f"native calls missing GIL-release boundary: {missing}"

    # Python-owned values and error construction must stay outside detached work.
    forbidden = ("Bound<'_", "to_pyerr(py", "py_operation_id(", "caller_embedding_rows(")
    violations = [token for token in forbidden if token in detached]
    assert not violations, f"Python access inside detached native work: {violations}"
    borrowed_captures = _borrowed_parameter_captures(source)
    assert not borrowed_captures, (
        f"borrowed Python buffers captured by detached native work: {borrowed_captures}"
    )
    assert len(bodies) >= 100, "native-call coverage unexpectedly shrank"

    mutated = source.replace("let query = query.to_owned();", "", 1)
    assert any(capture.endswith(":query") for capture in _borrowed_parameter_captures(mutated)), (
        "borrowed-input mutation was not rejected"
    )


def check_python_progresses_during_native_work() -> None:
    worker = threading.Thread(target=native._test_gil_release_probe)
    worker.start()
    try:
        native._test_gil_release_probe_wait()

        # The worker is still blocked inside native code. Reaching and executing
        # this Python bytecode proves it released the GIL; no timing is involved.
        progress = 0
        for _ in range(1_000):
            progress += 1
        assert progress == 1_000
        assert worker.is_alive()
    finally:
        native._test_gil_release_probe_signal()
    worker.join(timeout=5)
    assert not worker.is_alive()


def main() -> None:
    check_native_call_inventory()
    check_python_progresses_during_native_work()
    print("gil release acceptance passed")


if __name__ == "__main__":
    main()
