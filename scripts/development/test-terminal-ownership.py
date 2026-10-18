#!/usr/bin/env python3
"""Bounded capture transport and ownership-assessment regressions."""

import copy
import json
from pathlib import Path
import tempfile
import unittest

import terminal_ownership as collector


class BoundedInputTests(unittest.TestCase):
    def test_json_strings_do_not_consume_container_depth(self):
        value = {"text": '[{\\"}]' * 20}
        self.assertEqual(collector.decode_record(json.dumps(value).encode(), 2), value)

    def test_duplicate_keys_are_not_silently_overwritten(self):
        with self.assertRaisesRegex(ValueError, "duplicate"):
            collector.decode_record(b'{"held":1,"held":2}', 4)

    def test_nested_input_is_rejected_before_json_decoder_recursion(self):
        with self.assertRaisesRegex(ValueError, "depth"):
            collector.decode_record(b'{"x":' + b"[" * 1000 + b"0" + b"]" * 1000 + b"}", 8)

    def test_numbers_must_be_unsigned_integers(self):
        for raw in [b"-1", b"1.0", b"1e6", b"NaN", b"Infinity", b"18446744073709551616"]:
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                collector.decode_record(b'{"n":' + raw + b"}", 4)

    def test_capture_records_are_objects(self):
        for raw in [b"[]", b"null", b"1", b'"text"']:
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                collector.decode_record(raw, 4)

    def test_boolean_is_not_an_integer(self):
        with self.assertRaisesRegex(ValueError, "integer"):
            collector.uint(True, "held")

    def test_symlink_capture_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "real").write_bytes(b"{}\n")
            (root / "link").symlink_to(root / "real")
            with self.assertRaises(ValueError):
                list(
                    collector.read_records(
                        root / "link", line_bytes=8, byte_limit=16, record_limit=2
                    )
                )

    def test_partial_last_record_is_not_a_successful_capture(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "capture"
            path.write_bytes(b"{}\n{}")
            with self.assertRaisesRegex(ValueError, "partial"):
                list(collector.read_records(path, line_bytes=8, byte_limit=16, record_limit=2))

    def test_line_and_record_limits_include_newline(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "capture"
            path.write_bytes(b"{}\n{}\n")
            self.assertEqual(len(list(collector.read_records(path, 3, 6, 2))), 2)
            for limits in [(2, 6, 2), (3, 5, 2), (3, 6, 1)]:
                with self.subTest(limits=limits), self.assertRaises(ValueError):
                    list(collector.read_records(path, *limits))


def header_fixture():
    return {
        "kind": "session",
        "protocol": "terminal-ownership/1",
        "calls": 2,
        "slots": 4,
        "descriptors": 8,
        "edges": 4,
        "columns": 4,
        "nodes": 8,
        "depth": 4,
        "buffers": 8,
        "line_bytes": 1024,
        "record_limit": 92,
        "byte_limit": 94208,
        "observer_bytes": 10304,
        "owner_bytes": 112,
        "piece_bytes": 80,
        "backing_bytes": 112,
        "edge_bytes": 48,
        "fixed_bytes": 1024,
    }


def batch_fixture(nulls=0, temporal=False):
    owner = {
        "kind": "owner",
        "call": 1,
        "id": 1,
        "owner_kind": 5,
        "parent": 0,
        "piece": 2,
        "object": 1000,
        "bytes": 0,
        "held": 8,
        "a": 1,
        "b": 1,
        "c": 0,
        "d": 0,
        "flags": 0,
    }
    array = {
        "kind": "backing",
        "call": 1,
        "owner": 1,
        "node": 1,
        "parent_node": 0,
        "column": 0,
        "type": (17 | (4 << 16)) if temporal else 6,
        "object": 1000,
        "base": 0,
        "capacity": 0,
        "offset": 0,
        "length": 1,
        "nulls": nulls,
        "flags": 1,
        "children": 0,
        "buffers": 2 if nulls else 1,
    }
    values = {
        **array,
        "object": 1001,
        "base": 2000,
        "capacity": 8,
        "length": 8,
        "flags": 4,
        "children": 0,
        "buffers": 0,
    }
    backings = [array, values]
    if nulls:
        backings.append(
            {**values, "object": 1002, "base": 3000, "capacity": 1, "length": 1, "flags": 34}
        )
    return {1: owner}, backings


class EnvelopeTests(unittest.TestCase):
    def test_matching_envelope_is_accepted(self):
        collector.validate_header(header_fixture())

    def test_forged_derived_budgets_are_rejected(self):
        for field in ("record_limit", "byte_limit", "observer_bytes"):
            header = header_fixture()
            header[field] -= 1
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, "derivation"):
                collector.validate_header(header)

    def test_arithmetic_overflow_and_bool_are_rejected(self):
        for value in (True, collector.U64_MAX):
            header = header_fixture()
            header["slots"] = value
            with self.subTest(value=value), self.assertRaises(ValueError):
                collector.validate_header(header)


class ArrayEvidenceTests(unittest.TestCase):
    def test_all_null_nanosecond_array_still_owns_values(self):
        owners, rows = batch_fixture(nulls=1, temporal=True)
        capacities = collector.validate_arrays(rows, owners, header_fixture())
        self.assertEqual(capacities, {2000: 8, 3000: 1})
        rows.pop(1)
        rows[0]["buffers"] = 1
        with self.assertRaisesRegex(ValueError, "backing"):
            collector.validate_arrays(rows, owners, header_fixture())

    def test_shared_backing_is_counted_once_within_live_cut(self):
        owners, rows = batch_fixture()
        owners[1]["b"] = 2
        alias = [{**r, "node": 2, "column": 1} for r in rows]
        self.assertEqual(
            collector.validate_arrays(rows + alias, owners, header_fixture()), {2000: 8}
        )

    def test_retained_slice_capacity_exceeds_visible_bytes(self):
        owners, rows = batch_fixture()
        rows[1].update(capacity=128, offset=64)
        self.assertEqual(collector.validate_arrays(rows, owners, header_fixture()), {2000: 128})

    def test_missing_column_or_buffer_does_not_pass(self):
        owners, rows = batch_fixture()
        for altered in (rows[:1], []):
            with self.subTest(altered=altered), self.assertRaises(ValueError):
                collector.validate_arrays(altered, owners, header_fixture())

    def test_invalid_temporal_units_are_rejected(self):
        owners, rows = batch_fixture(temporal=True)
        for row in rows:
            row["type"] = 17 | (1 << 16)
        with self.assertRaisesRegex(ValueError, "temporal"):
            collector.validate_arrays(rows, owners, header_fixture())

    def test_conflicting_live_capacity_is_rejected(self):
        owners, rows = batch_fixture()
        owners[1]["b"] = 2
        alias = copy.deepcopy(rows)
        for row in alias:
            row.update(node=2, column=1)
        alias[1]["capacity"] = 16
        with self.assertRaisesRegex(ValueError, "capacity"):
            collector.validate_arrays(rows + alias, owners, header_fixture())

    def test_dangling_parent_and_overflow_span_are_rejected(self):
        owners, rows = batch_fixture()
        rows[0]["parent_node"] = 2
        with self.assertRaises(ValueError):
            collector.validate_arrays(rows, owners, header_fixture())
        owners, rows = batch_fixture()
        rows[1]["offset"] = collector.U64_MAX
        with self.assertRaises(ValueError):
            collector.validate_arrays(rows, owners, header_fixture())


def capture_fixture():
    rows = [header_fixture()]
    for call in (1, 2):
        owners, backings = batch_fixture()
        owner = owners[1]
        identity, piece = 2 * call - 1, 2 * call
        owner.update(call=call, id=identity, piece=piece)
        for backing in backings:
            backing.update(call=call, owner=identity)
        begin = {
            "kind": "snapshot_begin",
            "call": call,
            "snapshot": 1,
            "outcome": 0,
            "epoch": 0,
            "in_flight": 0,
            "poison": 0,
            "gaps": 0,
            "constructed": call,
            "retired": call - 1,
            "operations": 0,
        }
        authority = {
            "kind": "piece",
            "call": call,
            "id": piece,
            "root": piece,
            "holder": identity,
            "consumer": 7,
            "held": 8,
            "split_from": 0,
            "split_bytes": 0,
            "flags": 0,
        }
        end = {
            "kind": "snapshot_end",
            "call": call,
            "snapshot": 1,
            "owners": 1,
            "pieces": 1,
            "backings": 2,
            "edges": 0,
            "complete": True,
        }
        rows += [begin, owner, authority, *backings, end]
        rows += [
            {**begin, "snapshot": 2, "retired": call},
            {**end, "snapshot": 2, "owners": 0, "pieces": 0, "backings": 0},
            {"kind": "run_end", "call": call, "outcome": 0, "snapshots": 2, "poison": 0},
        ]
    return rows


def encode_capture(rows):
    raw = b"".join(json.dumps(row, separators=(",", ":")).encode() + b"\n" for row in rows)
    footer = {
        "kind": "session_end",
        "calls": rows[0]["calls"],
        "records": len(rows),
        "bytes": len(raw),
        "checksum": collector.checksum(raw),
        "complete": True,
    }
    return raw + json.dumps(footer, separators=(",", ":")).encode() + b"\n"


class SessionAssessmentTests(unittest.TestCase):
    def assess(self, raw):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "capture.jsonl"
            path.write_bytes(raw)
            return collector.assess_capture(path)

    def test_two_calls_have_complete_transport_direct_and_funding_evidence(self):
        result = self.assess(encode_capture(capture_fixture()))
        self.assertTrue(result["capture_complete"], result)
        self.assertEqual(result["completed_calls"], 2)
        self.assertEqual(len(result["snapshots"]), 4)

    def test_missing_footer_after_external_termination_denies_global_completeness(self):
        raw = encode_capture(capture_fixture())
        result = self.assess(raw[: raw.rfind(b'{"kind":"session_end"')])
        self.assertFalse(result["transport_complete"])
        self.assertFalse(result["capture_complete"])
        self.assertIn("missing final", result["transport_errors"][0])

    def test_checksum_and_postfooter_suffix_are_rejected(self):
        raw = encode_capture(capture_fixture())
        for altered in [raw.replace(b'"consumer":7', b'"consumer":8', 1), raw + b"{}\n"]:
            with self.subTest(altered=altered[-100:]):
                self.assertFalse(self.assess(altered)["transport_complete"])

    def test_missing_actual_sponsor_is_independent_from_valid_transport(self):
        rows = capture_fixture()
        next(row for row in rows if row["kind"] == "owner")["piece"] = 999
        result = self.assess(encode_capture(rows))
        self.assertTrue(result["transport_complete"], result)
        self.assertTrue(result["direct_ownership_complete"], result)
        self.assertFalse(result["funding_complete"])

    def test_same_consumer_id_does_not_substitute_for_root_certificate(self):
        rows = capture_fixture()
        piece = next(row for row in rows if row["kind"] == "piece")
        piece.update(root=99, split_from=99, split_bytes=8, flags=1)
        result = self.assess(encode_capture(rows))
        self.assertTrue(result["transport_complete"], result)
        self.assertFalse(result["funding_complete"])

    def test_explicit_row_layout_gap_does_not_make_transport_invalid(self):
        rows = capture_fixture()
        rows[1]["gaps"] = 4
        next(row for row in rows if row["kind"] == "snapshot_end")["complete"] = False
        result = self.assess(encode_capture(rows))
        self.assertTrue(result["transport_complete"], result)
        self.assertFalse(result["direct_ownership_complete"])
        self.assertFalse(result["funding_complete"])

    def test_inflight_snapshot_cannot_claim_a_consistent_cut(self):
        rows = capture_fixture()
        rows[1].update(epoch=1, in_flight=1, operations=1)
        for row in rows[2:]:
            if row["kind"] == "snapshot_begin":
                row.update(epoch=2, operations=1)
        next(row for row in rows if row["kind"] == "snapshot_end")["complete"] = False
        result = self.assess(encode_capture(rows))
        self.assertTrue(result["transport_complete"], result)
        self.assertFalse(result["capture_complete"])

    def test_pointer_reuse_after_retirement_is_not_generation_reuse(self):
        rows = capture_fixture()
        # Both calls intentionally use the same object/base addresses.
        self.assertTrue(self.assess(encode_capture(rows))["capture_complete"])
        for row in rows:
            if row.get("call") == 2 and row["kind"] == "owner":
                row["id"] = 1
        self.assertFalse(self.assess(encode_capture(rows))["capture_complete"])

    def test_missing_second_call_cannot_be_hidden_by_a_valid_footer(self):
        rows = capture_fixture()
        rows = [row for row in rows if row.get("call") != 2]
        result = self.assess(encode_capture(rows))
        self.assertFalse(result["transport_complete"])

    def test_missing_required_type_backing_is_rejected_even_with_resealed_footer(self):
        rows = capture_fixture()
        rows = [row for i, row in enumerate(rows) if i != 5]
        result = self.assess(encode_capture(rows))
        self.assertFalse(result["capture_complete"])


if __name__ == "__main__":
    unittest.main()
