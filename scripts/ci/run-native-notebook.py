#!/usr/bin/env python3
"""Execute the agent-grounding notebook twice against one installed native wheel."""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import sys
import tempfile
import time
from typing import Any

from nbclient import NotebookClient
import nbformat

import graphforge

NOTEBOOK = Path("examples/agent_grounding/ecommerce_agent.ipynb")
RESULT_PREFIX = "GRAPHFORGE_NOTEBOOK_RESULT="
KERNEL_NAME = "graphforge-native"
REQUIRED_SURFACES = (
    "forge.rank(",
    "forge.index(",
    "forge.find(",
    "forge.publish_caller_embeddings(",
    "forge.inspect_embedding_space_freshness(",
    "forge.inspect_provider_embedding_plan(",
    "forge.publish_provider_embeddings(",
    "semantic_query=",
    "rerank=",
)
FORBIDDEN = (
    "sys.path.insert",
    "PYTHONPATH",
    "CypherValue",
    "db.search.",
    "create_node(",
    "create_relationship(",
    "requests.",
    "urllib.",
    "api.openrouter.ai",
)


def _prepare_kernel(jupyter_data: Path) -> None:
    kernel = jupyter_data / "kernels" / KERNEL_NAME
    kernel.mkdir(parents=True)
    specification = {
        "argv": [
            sys.executable,
            "-m",
            "ipykernel_launcher",
            "--log-level=ERROR",
            "-f",
            "{connection_file}",
        ],
        "display_name": "GraphForge native CI",
        "language": "python",
    }
    (kernel / "kernel.json").write_text(
        json.dumps(specification, sort_keys=True),
        encoding="utf-8",
    )


def _extract_evidence(notebook: Any) -> dict[str, Any]:
    records: list[str] = []
    for cell in notebook.cells:
        for output in cell.get("outputs", []):
            if output.get("output_type") != "stream":
                continue
            text = output.get("text", "")
            if isinstance(text, list):
                text = "".join(text)
            records.extend(
                line.removeprefix(RESULT_PREFIX)
                for line in text.splitlines()
                if line.startswith(RESULT_PREFIX)
            )
    if len(records) != 1:
        raise AssertionError(f"notebook emitted {len(records)} evidence records")
    return json.loads(records[0])


def _execute_once(root: Path, project: Path) -> tuple[dict[str, Any], int]:
    notebook_path = root / NOTEBOOK
    notebook = nbformat.read(notebook_path, as_version=4)
    project.mkdir()
    client = NotebookClient(
        notebook,
        timeout=180,
        kernel_name=KERNEL_NAME,
        resources={"metadata": {"path": str(root)}},
        allow_errors=False,
    )

    os.environ["GRAPHFORGE_NOTEBOOK_PROJECT"] = str(project)
    executed = 0
    with client.setup_kernel():
        client.reset_execution_trackers()
        for index, cell in enumerate(notebook.cells):
            if cell.cell_type == "code":
                executed += 1
            try:
                client.execute_cell(cell, index)
            except Exception as error:
                raise RuntimeError(f"{NOTEBOOK}: cell {index + 1} failed") from error
    return _extract_evidence(notebook), executed


def main() -> None:
    """Validate, execute twice in clean projects, and print stable evidence."""
    root = Path(__file__).resolve().parents[2]
    notebook_path = root / NOTEBOOK
    notebook = nbformat.read(notebook_path, as_version=4)
    nbformat.validate(notebook)

    source = "\n".join(
        "".join(cell.get("source", "")) for cell in notebook.cells if cell.cell_type == "code"
    )
    for required in REQUIRED_SURFACES:
        if required not in source:
            raise AssertionError(f"notebook does not exercise {required}")
    for forbidden in FORBIDDEN:
        if forbidden in source:
            raise AssertionError(f"notebook retains forbidden API or network target {forbidden!r}")
    if re.search(r"\.value\b", source):
        raise AssertionError("notebook retains retired row-wrapper access")
    for index, cell in enumerate(notebook.cells):
        if cell.cell_type != "code":
            continue
        if cell.get("execution_count") is not None or cell.get("outputs"):
            raise AssertionError(f"committed notebook cell {index + 1} retains execution state")

    installed = Path(graphforge.__file__).resolve()
    source_package = root / "crates/gf-bindings-py/python/graphforge"
    if installed.is_relative_to(source_package):
        raise AssertionError(f"repository source shadowed the installed wheel: {installed}")

    for key in (
        "OPENAI_API_KEY",
        "OPENROUTER_API_KEY",
        "ANTHROPIC_API_KEY",
    ):
        os.environ.pop(key, None)
    dead_proxy = "http://127.0.0.1:9"
    for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
        os.environ[key] = dead_proxy
    os.environ["NO_PROXY"] = "127.0.0.1,localhost"
    os.environ["no_proxy"] = "127.0.0.1,localhost"
    os.environ["GRAPHFORGE_REPOSITORY_ROOT"] = str(root)

    started = time.perf_counter()
    with tempfile.TemporaryDirectory() as temporary:
        temporary_path = Path(temporary)
        jupyter_data = temporary_path / "jupyter"
        _prepare_kernel(jupyter_data)
        original_jupyter_path = os.environ.get("JUPYTER_PATH")
        os.environ["JUPYTER_PATH"] = str(jupyter_data)
        try:
            first, first_cells = _execute_once(root, temporary_path / "run-1")
            second, second_cells = _execute_once(root, temporary_path / "run-2")
        finally:
            if original_jupyter_path is None:
                os.environ.pop("JUPYTER_PATH", None)
            else:
                os.environ["JUPYTER_PATH"] = original_jupyter_path

    if first != second:
        raise AssertionError(f"clean notebook executions differed: {first!r} != {second!r}")
    if first_cells != second_cells:
        raise AssertionError("clean notebook executions ran different cell counts")

    elapsed = time.perf_counter() - started
    sha = os.environ.get("GRAPHFORGE_WHEEL_SHA", "local")
    print("Native agent-grounding notebook")
    print(f"  notebook: {NOTEBOOK}")
    print(f"  wheel:    graphforge {graphforge.__version__} ({sha})")
    print(f"  cells:    {first_cells} code cells x 2 clean runs")
    print(f"  elapsed:  {elapsed:.2f}s")
    print(f"  evidence: {json.dumps(first, sort_keys=True)}")


if __name__ == "__main__":
    main()
