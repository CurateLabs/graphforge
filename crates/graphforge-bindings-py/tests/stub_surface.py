"""Checked two-way contract between the PyO3 surface and its shipped type stub."""

from __future__ import annotations

import ast
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / "crates/graphforge-bindings-py/src/lib.rs"
STUB = ROOT / "crates/graphforge-bindings-py/python/graphforge/_graphforge_rs.pyi"

RECEIVERS = {
    "GraphForge": "GraphForge",
    "PyCancellationToken": "CancellationToken",
    "PyCheckpointView": "CheckpointView",
    "PyEdgeHandle": "EdgeHandle",
    "PyGraphScaleIndexProfile": "GraphScaleIndexProfile",
    "PyInvocationDescriptor": "InvocationDescriptor",
    "PyNodeHandle": "NodeHandle",
    "PyRecordedAlgorithmResult": "RecordedAlgorithmResult",
    "PyResolvedBeliefProjection": "ResolvedBeliefProjection",
    "PyResolvedRecordedAlgorithmResult": "ResolvedRecordedAlgorithmResult",
}

MISSING_EPISTEMIC_SHAPES = {
    "apply_valid_time": "(*, transaction_cutoff, valid_time)",
    "assertion_status": "(assertion_uuid)",
    "create_assertion_with_status": (
        "(*, operation_uuid, assertion_uuid, claim, graph_refs, status_event_uuid, status, "
        "actor_uuid=None)"
    ),
    "create_hypothesis_group": (
        "(*, operation_uuid, group_uuid, question_key, provenance_uuid, actor_uuid=None)"
    ),
    "epistemic_snapshot": "(*, transaction_cutoff)",
    "hypothesis_members": "(group_uuid)",
    "hypothesis_selection": "(group_uuid)",
    "list_assertion_status": "(*, assertion_uuid=None, limit=100, after=None, cancellation=None)",
    "list_assertion_supersessions": (
        "(*, prior_assertion_uuid=None, replacement_assertion_uuid=None, limit=100, "
        "after=None, cancellation=None)"
    ),
    "list_assertion_validity": "(*, assertion_uuid=None, limit=100, after=None, cancellation=None)",
    "list_hypothesis_groups": "(*, question_key=None, limit=100, after=None, cancellation=None)",
    "list_hypothesis_membership": (
        "(*, group_uuid=None, assertion_uuid=None, limit=100, after=None, cancellation=None)"
    ),
    "list_hypothesis_selection": "(*, group_uuid=None, limit=100, after=None, cancellation=None)",
    "list_reasoning": "(*, assertion_uuid=None, limit=100, after=None, cancellation=None)",
    "reasoning": "(reasoning_uuid, *, cancellation=None)",
    "record_assertion_status": (
        "(*, operation_uuid, status_event_uuid, assertion_uuid, status, provenance_uuid, "
        "confidence_uuid=None, reasoning_uuid=None, actor_uuid=None)"
    ),
    "record_assertion_validity": (
        "(*, operation_uuid, validity_event_uuid, assertion_uuid, provenance_uuid, "
        "valid_from=None, valid_to=None, reasoning_uuid=None, actor_uuid=None)"
    ),
    "record_hypothesis_membership": (
        "(*, operation_uuid, membership_event_uuid, group_uuid, assertion_uuid, action, "
        "reasoning_uuid, provenance_uuid, actor_uuid=None)"
    ),
    "record_hypothesis_selection": (
        "(*, operation_uuid, selection_event_uuid, group_uuid, reasoning_uuid, "
        "provenance_uuid, selected_assertion_uuid=None, actor_uuid=None)"
    ),
    "record_reasoning": (
        "(*, operation_uuid, reasoning_uuid, assertion_uuid, kind, content_format, content, "
        "provenance_uuid, supersedes_reasoning_uuid=None, actor_uuid=None)"
    ),
    "remove_hypothesis_member": (
        "(*, operation_uuid, membership_event_uuid, selection_event_uuid, group_uuid, "
        "assertion_uuid, reasoning_uuid, provenance_uuid, selected_assertion_uuid=None, "
        "actor_uuid=None)"
    ),
    "supersede_assertion": (
        "(*, operation_uuid, supersession_uuid, prior_assertion_uuid, "
        "replacement_assertion_uuid, status_event_uuid, reasoning_uuid, provenance_uuid, "
        "actor_uuid=None)"
    ),
}


def _matching_brace(text: str, opening: int) -> int:
    depth = 0
    for index in range(opening, len(text)):
        if text[index] == "{":
            depth += 1
        elif text[index] == "}":
            depth -= 1
            if depth == 0:
                return index
    raise AssertionError("unbalanced Rust source")


def _native_members(source: str) -> dict[str, set[str]]:
    members = {public: set() for public in RECEIVERS.values()}
    for rust, public in RECEIVERS.items():
        pattern = re.compile(rf"#\[pymethods\]\s*impl\s+{rust}\s*\{{")
        matches = list(pattern.finditer(source))
        assert matches, f"missing PyO3 receiver {rust}"
        for match in matches:
            body = source[match.end() : _matching_brace(source, match.end() - 1)]
            for name in re.findall(r"^\s*fn\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(", body, re.M):
                members[public].add("__init__" if name == "new" else name)
    return members


def _stub_classes(stub: str) -> tuple[ast.Module, dict[str, ast.ClassDef]]:
    tree = ast.parse(stub)
    return tree, {node.name: node for node in tree.body if isinstance(node, ast.ClassDef)}


def _stub_members(classes: dict[str, ast.ClassDef]) -> dict[str, set[str]]:
    return {
        name: {node.name for node in classes[name].body if isinstance(node, ast.FunctionDef)}
        for name in RECEIVERS.values()
    }


def _default(value: ast.expr | None) -> str | None:
    return None if value is None else ast.unparse(value)


def _stub_shape(method: ast.FunctionDef) -> str:
    positional = list(method.args.posonlyargs) + list(method.args.args)
    assert positional and positional[0].arg == "self"
    positional = positional[1:]
    defaults = [None] * (len(positional) - len(method.args.defaults)) + list(method.args.defaults)
    parts = [
        arg.arg + (f"={_default(default)}" if default is not None else "")
        for arg, default in zip(positional, defaults)
    ]
    if method.args.kwonlyargs:
        parts.append("*")
        parts.extend(
            arg.arg + (f"={_default(default)}" if default is not None else "")
            for arg, default in zip(method.args.kwonlyargs, method.args.kw_defaults)
        )
    return f"({', '.join(parts)})"


def validate(source: str, stub: str) -> None:
    tree, classes = _stub_classes(stub)
    missing_classes = sorted(set(RECEIVERS.values()) - set(classes))
    assert not missing_classes, f"native classes missing from stub: {missing_classes}"
    native = _native_members(source)
    declared = _stub_members(classes)
    for receiver in sorted(native):
        missing = sorted(native[receiver] - declared[receiver])
        extra = sorted(declared[receiver] - native[receiver])
        assert not missing, f"{receiver} native members missing from stub: {missing}"
        assert not extra, f"{receiver} stub members absent from native surface: {extra}"

    graphforge = classes["GraphForge"]
    methods = {node.name: node for node in graphforge.body if isinstance(node, ast.FunctionDef)}
    for name, shape in MISSING_EPISTEMIC_SHAPES.items():
        method = methods[name]
        assert _stub_shape(method) == shape, (
            f"GraphForge.{name} shape drift: {_stub_shape(method)} != {shape}"
        )
        assert ast.unparse(method.returns) == "pyarrow.Table", (
            f"GraphForge.{name} must return pyarrow.Table"
        )
        if shape not in {"(assertion_uuid)", "(group_uuid)"}:
            escaped = re.escape(shape)
            assert re.search(
                rf"#\[pyo3\(signature\s*=\s*{escaped}\)\]\s*(?:#\[[^\n]+\]\s*)*fn\s+{name}\(",
                source,
            ), f"GraphForge.{name} no longer has the frozen PyO3 signature"

    assert any(isinstance(node, ast.FunctionDef) and node.name == "version" for node in tree.body)
    assert "#[pyfunction]" in source and "fn version(" in source


def check_mutation_sensitivity(source: str, stub: str) -> None:
    removed = stub.replace(
        "    def assertion_status(self, assertion_uuid: str) -> pyarrow.Table: ...\n", "", 1
    )
    try:
        validate(source, removed)
    except AssertionError as error:
        assert "missing from stub" in str(error)
    else:
        raise AssertionError("stub omission mutation was not rejected")

    start = stub.index("    def list_assertion_status(")
    limit = stub.index("limit: int = 100,", start)
    changed = stub[:limit] + stub[limit:].replace("limit: int = 100,", "limit: int = 101,", 1)
    try:
        validate(source, changed)
    except AssertionError as error:
        assert "shape drift" in str(error)
    else:
        raise AssertionError("stub signature mutation was not rejected")


def main() -> None:
    source = SOURCE.read_text()
    stub = STUB.read_text()
    validate(source, stub)
    check_mutation_sensitivity(source, stub)


if __name__ == "__main__":
    main()
