"""Discover declared production Rust owners for binding contract checks."""

from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
_SPEC = importlib.util.spec_from_file_location(
    "binding_source_lexer", ROOT / "scripts/coverage_rust_ledger.py"
)
assert _SPEC and _SPEC.loader
_LEXER = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_LEXER)


def _modules(source: str, path: Path) -> list[str]:
    tokens = [token for token, _ in _LEXER._rust_tokens(source, str(path))]
    pairs = {"(": ")", "[": "]", "{": "}"}

    def closing(start: int) -> int:
        stack = [pairs[tokens[start]]]
        for index in range(start + 1, len(tokens)):
            token = tokens[index]
            if token in pairs:
                stack.append(pairs[token])
            elif token in pairs.values():
                if token != stack.pop():
                    raise ValueError(f"unbalanced module source: {path}")
                if not stack:
                    return index
        raise ValueError(f"unclosed module source: {path}")

    modules = []
    attributes: list[list[str]] = []
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token == "#" and index + 1 < len(tokens):
            opening = index + 1
            inner = tokens[opening] == "!"
            opening += int(inner)
            if opening < len(tokens) and tokens[opening] == "[":
                end = closing(opening)
                if not inner:
                    attributes.append(tokens[opening + 1 : end])
                index = end + 1
                continue
        if token == "pub":
            index += 1
            if index < len(tokens) and tokens[index] == "(":
                index = closing(index) + 1
            continue
        if token == "mod" and index + 2 < len(tokens) and tokens[index + 2] == ";":
            name = tokens[index + 1]
            if ["cfg", "(", "test", ")"] not in attributes:
                if any(attr and attr[0] in {"cfg", "cfg_attr", "path"} for attr in attributes):
                    raise ValueError(f"unsupported conditional/path module {name} in {path}")
                modules.append(name)
            index += 3
        elif token in pairs:
            index = closing(index) + 1
        else:
            index += 1
        attributes = []
    return modules


def production_sources(entry: Path) -> list[tuple[Path, str]]:
    """Read only declared production modules, rejecting missing/ambiguous ownership."""
    authority = entry.resolve().parent
    visited: set[Path] = set()
    result: list[tuple[Path, str]] = []

    def visit(path: Path) -> None:
        resolved = path.resolve()
        if not resolved.is_relative_to(authority):
            raise ValueError(f"module escapes source authority: {path}")
        if resolved in visited:
            raise ValueError(f"duplicate module source: {path}")
        if not path.is_file():
            raise ValueError(f"missing module source: {path}")
        visited.add(resolved)
        source = path.read_text(encoding="utf-8")
        result.append((path, source))
        parent = path.parent if path.name in {"lib.rs", "mod.rs"} else path.with_suffix("")
        for name in _modules(source, path):
            candidates = [parent / f"{name}.rs", parent / name / "mod.rs"]
            existing = [candidate for candidate in candidates if candidate.is_file()]
            if len(existing) != 1:
                raise ValueError(f"missing or ambiguous module {name} declared in {path}")
            visit(existing[0])

    visit(entry)
    return result


def production_source(entry: Path) -> str:
    """Join complete owners without fusing tokens across file boundaries."""
    return "\n".join(source for _, source in production_sources(entry))
