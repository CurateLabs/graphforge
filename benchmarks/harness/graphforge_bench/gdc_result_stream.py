"""Read a written ``graphforge-gdc-query-result/1`` file row by row (#1914).

A Graphalytics result has one row per vertex, millions at the larger rungs, so a
result file is about 100 MB. Loading it whole costs a Python list and several
strings for every row, and the correctness check used to load every file twice.
`StreamedResult` reads only the members before ``rows`` up front and hands the rows
out as they are decoded, so a check holds one row at a time.

The driver writes one JSON object per file with ``rows`` after every member the
check needs to start (``binding_id``, ``columns``, ``ordered``, ``query_id`` and
``result_sha256``); ``schema`` may follow the rows and is checked once they have
been read. A file that lists its rows before those members is refused rather than
loaded whole.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
import json
from pathlib import Path
import re
from typing import IO, Any

from graphforge_bench.gdc_rung_error import RungInputError

QUERY_RESULT_SCHEMA = "graphforge-gdc-query-result/1"
# What the check needs before the first row.
HEAD_MEMBERS = ("binding_id", "columns", "ordered", "query_id", "result_sha256")
CHUNK_CHARS = 1 << 20
_WHITESPACE = re.compile(r"[ \t\n\r]*")


class _Text:
    """A forward-only JSON text reader over a file, refilled a chunk at a time."""

    def __init__(self, stream: IO[str], chunk: int, label: str) -> None:
        self._stream = stream
        self._chunk = chunk
        self._label = label
        self.buffer = ""
        self.position = 0
        self.exhausted = False
        self.decoder = json.JSONDecoder()

    def fail(self, message: str) -> RungInputError:
        return RungInputError("invalid_document", f"{self._label}: {message}")

    def fill(self) -> bool:
        """Append the next chunk, dropping what was consumed; False at end of file."""
        if self.exhausted:
            return False
        more = self._stream.read(self._chunk)
        if not more:
            self.exhausted = True
            return False
        self.buffer = self.buffer[self.position :] + more
        self.position = 0
        return True

    def peek(self) -> str:
        """The next non-blank character, or "" at the end of the file."""
        while True:
            self.position = _WHITESPACE.match(self.buffer, self.position).end()  # type: ignore[union-attr]
            if self.position < len(self.buffer):
                return self.buffer[self.position]
            if not self.fill():
                return ""

    def expect(self, character: str) -> None:
        if self.peek() != character:
            raise self.fail(f"expected {character!r}")
        self.position += 1

    def value(self) -> Any:
        """Decode one JSON value, reading on until it is complete."""
        self.peek()
        while True:
            try:
                decoded, end = self.decoder.raw_decode(self.buffer, self.position)
            except json.JSONDecodeError as error:
                # A value cut off by the end of the chunk fails here; more text
                # completes it. At the end of the file the failure is the file's.
                if not self.fill():
                    raise self.fail(str(error)) from error
                continue
            if end == len(self.buffer) and not self.exhausted:
                # A number can end exactly at the chunk edge with digits still to come.
                if self.fill():
                    continue
            self.position = end
            return decoded


class StreamedResult(Mapping[str, Any]):
    """One written result, with its rows read from the file as they are consumed.

    ``result["rows"]`` returns a new single-pass iterator over the file each time.
    The other members are loaded when the result is opened.
    """

    def __init__(self, path: Path, head: Mapping[str, Any], chunk: int) -> None:
        self._path = path
        self._head = dict(head)
        self._chunk = chunk

    @classmethod
    def open(cls, path: Path, *, chunk: int = CHUNK_CHARS) -> StreamedResult:
        try:
            with path.open(encoding="utf-8") as stream:
                text = _Text(stream, chunk, path.name)
                head = _read_head(text)
        except (OSError, UnicodeError) as error:
            raise RungInputError("invalid_document", f"{path}: {error}") from error
        return cls(path, head, chunk)

    def __getitem__(self, key: str) -> Any:
        if key == "rows":
            return self._rows()
        return self._head[key]

    def __iter__(self) -> Iterator[str]:
        return iter([*self._head, "rows"])

    def __len__(self) -> int:
        return len(self._head) + 1

    def _rows(self) -> Iterator[list[Any]]:
        try:
            with self._path.open(encoding="utf-8") as stream:
                text = _Text(stream, self._chunk, self._path.name)
                seen = _read_head(text, keep_schema=True)
                yield from _read_rows(text)
                _read_tail(text, seen)
        except (OSError, UnicodeError) as error:
            raise RungInputError("invalid_document", f"{self._path}: {error}") from error


def _read_head(text: _Text, *, keep_schema: bool = False) -> dict[str, Any]:
    """Every member before ``rows``, leaving the reader just inside the rows array."""
    text.expect("{")
    members: dict[str, Any] = {}
    while True:
        if text.peek() != '"':
            raise text.fail("expected a member name")
        name = text.value()
        text.expect(":")
        if name == "rows":
            text.expect("[")
            missing = [member for member in HEAD_MEMBERS if member not in members]
            if missing:
                raise text.fail(f"lists its rows before {', '.join(missing)}")
            if "schema" in members and members["schema"] != QUERY_RESULT_SCHEMA:
                raise text.fail("is not a query result")
            if not isinstance(members["columns"], list):
                raise text.fail("columns are malformed")
            if not keep_schema:
                members.pop("schema", None)
            return members
        if name in members:
            raise text.fail(f"member {name!r} repeats")
        members[name] = text.value()
        if text.peek() == ",":
            text.position += 1
        else:
            raise text.fail("has no rows")


def _read_rows(text: _Text) -> Iterator[list[Any]]:
    """The rows of the array the reader is inside, up to and including its ``]``.

    The inner loop runs on the reader's buffer directly: it is the hot path, once
    per row, and the reader's methods would triple its cost.
    """
    decode = text.decoder.raw_decode
    skip = _WHITESPACE.match
    first = need_row = True
    while True:
        buffer, position = text.buffer, text.position
        end = len(buffer)
        while True:
            position = skip(buffer, position).end()  # type: ignore[union-attr]
            if position == end:
                break
            character = buffer[position]
            if need_row:
                if character == "]" and first:
                    text.position = position + 1
                    return
                try:
                    row, position = decode(buffer, position)
                except json.JSONDecodeError:
                    break  # cut off by the end of the buffer: read on and decode again
                if not isinstance(row, list):
                    raise text.fail("a row is not an array")
                first = need_row = False
                yield row
            elif character == "]":
                text.position = position + 1
                return
            elif character == ",":
                position += 1
                need_row = True
            else:
                raise text.fail("expected ',' between rows")
        text.position = position
        if not text.fill():
            try:
                decode(text.buffer, text.position)  # the real syntax error, if there is one
            except json.JSONDecodeError as error:
                raise text.fail(str(error)) from error
            raise text.fail("ends inside the rows")


def _read_tail(text: _Text, head: dict[str, Any]) -> None:
    """The members after ``rows``; the file must be a query result and end there."""
    members = dict(head)
    while text.peek() == ",":
        text.position += 1
        if text.peek() != '"':
            raise text.fail("expected a member name")
        name = text.value()
        text.expect(":")
        if name in members or name == "rows":
            raise text.fail(f"member {name!r} repeats")
        members[name] = text.value()
    text.expect("}")
    if text.peek() != "":
        raise text.fail("has text after the result")
    if members.get("schema") != QUERY_RESULT_SCHEMA:
        raise text.fail("is not a query result")
