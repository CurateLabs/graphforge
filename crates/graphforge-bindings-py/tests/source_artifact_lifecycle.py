"""Rust-authoritative Source/Artifact lifecycle until binding parity (#1349)."""

from __future__ import annotations

import graphforge


def check_rust_authoritative_surface() -> None:
    graph = graphforge.GraphForge()
    for name in (
        "register_source",
        "register_artifact",
        "source",
        "artifact",
        "list_sources",
        "list_artifacts",
    ):
        assert not hasattr(graph, name), f"unexpected Python binding for {name}"


def main() -> None:
    check_rust_authoritative_surface()


if __name__ == "__main__":
    main()
