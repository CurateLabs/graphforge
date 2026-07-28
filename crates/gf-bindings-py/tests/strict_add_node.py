"""Native-wheel acceptance for strict ``add_node`` validation (#2517)."""

from __future__ import annotations

import hashlib
from pathlib import Path
import subprocess
import sys
import tempfile
import uuid

import graphforge as g


def project_digest(root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(path for path in root.rglob("*") if path.is_file()):
        digest.update(path.relative_to(root).as_posix().encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()


def check_strict_add_node(project: Path) -> None:
    forge = g.GraphForge(str(project))
    assert forge.ontology_mode == "strict", forge.ontology_mode

    # Both a directly declared property and an inherited declaration are accepted.
    existing = forge.execute("MATCH (n:Host) RETURN n.node_uuid AS id").column("id").to_pylist()
    valid = forge.add_node("Host", name="Gateway", hostname="gw-1")
    accepted = forge.execute("MATCH (n:Host) RETURN n.node_uuid AS id").column("id").to_pylist()
    assert len(accepted) == len(existing) + 1, accepted
    assert uuid.UUID(valid.uuid).bytes in accepted, accepted
    before = project_digest(project)

    try:
        forge.add_node("Host", unknown_field="must fail")
    except g.ValidationError as exc:
        assert exc.code == "GF_VALIDATION", exc.code
    else:
        raise SystemExit("expected strict undeclared-property ValidationError")

    assert project_digest(project) == before, "rejected add_node changed committed project state"
    after = forge.execute("MATCH (n:Host) RETURN n.node_uuid AS id").column("id").to_pylist()
    assert after == accepted, after


if len(sys.argv) > 1:
    check_strict_add_node(Path(sys.argv[1]))
else:
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        project = root / "project"
        ontology = root / "ontology.yaml"
        ontology.write_text(
            """ontology_id: construction
version: "1"
entity_types:
  - name: Asset
    abstract: false
  - name: Host
    abstract: false
    parent: Asset
properties:
  - owner: Asset
    name: name
    type: utf8
    nullable: false
  - owner: Host
    name: hostname
    type: utf8
    nullable: false
""",
            encoding="utf-8",
        )
        subprocess.run(
            [
                "cargo",
                "run",
                "--quiet",
                "-p",
                "gf-api",
                "--example",
                "strict_add_node_fixture",
                "--",
                str(project),
                str(ontology),
            ],
            cwd=Path(__file__).parents[3],
            check=True,
        )
        check_strict_add_node(project)
