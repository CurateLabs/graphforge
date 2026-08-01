#!/usr/bin/env python3
"""Deterministic checks for release-note extraction."""

from __future__ import annotations

import importlib.util
from pathlib import Path

SCRIPT = Path(__file__).with_name("release-notes.py")
SPEC = importlib.util.spec_from_file_location("release_notes", SCRIPT)
assert SPEC and SPEC.loader
release_notes = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release_notes)

sample = """# Changelog

## [Unreleased]

_Nothing yet._

## [0.5.0] - 2026-07-31

### Added

- Public release.

## [0.4.0] - 2026-01-01

- Previous.

[0.5.0]: https://example.invalid/v0.5.0
"""
assert release_notes.extract(sample, "0.5.0") == "### Added\n\n- Public release.\n"
try:
    release_notes.extract(sample, "0.6.0")
except ValueError as error:
    assert "lacks a dated" in str(error)
else:
    raise AssertionError("missing release section should fail")

print("release notes tests passed")
