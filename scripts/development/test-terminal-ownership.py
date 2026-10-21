#!/usr/bin/env python3
"""Bounded capture transport and ownership-assessment regressions."""

import copy
import json
from pathlib import Path
import tempfile
import unittest

import terminal_ownership as collector

EMPTY_ERROR = {"error_ptr": 0, "error_len": 0, "error_hash": 0, "error_truncated": False}
EMPTY_ANCHOR = {
    **EMPTY_ERROR,
    "attempt": 0,
    "stage": 0,
    "failed_piece": 0,
    "consumer": 0,
    "request": 0,
}


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
        "observer_bytes": 10688,
        "owner_bytes": 112,
        "piece_bytes": 80,
        "backing_bytes": 112,
        "edge_bytes": 48,
        "fixed_bytes": 1024,
        "metadata_per_slot": 64,
        "tls_bytes": 128,
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
        "coverage": 0,
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


class ZeroWidthBinaryTests(unittest.TestCase):
    def test_zero_width_binary_keeps_rows_and_validity_without_payload_bytes(self):
        owners, rows = batch_fixture(nulls=1)
        owners[1]["a"] = 3
        for row in rows:
            row["type"] = 21
        rows[0].update(length=3, nulls=3)
        rows[1].update(base=0, capacity=0, length=0)
        rows[2].update(length=3)
        try:
            width = collector.type_width(21)
            capacities = collector.validate_arrays(rows, owners, header_fixture())
        except ValueError as error:
            self.fail(f"valid writer layout rejected: {error}")
        self.assertEqual(width, (21, 0))
        self.assertEqual(capacities, {3000: 1})

    def test_fixed_binary_width_cannot_exceed_the_writers_nonnegative_i32(self):
        self.assertEqual(collector.type_width(21 | (((1 << 31) - 1) << 16)), (21, (1 << 31) - 1))
        for width in [1 << 31, (1 << 32) - 1, 1 << 32]:
            with self.subTest(width=width), self.assertRaises(ValueError):
                collector.type_width(21 | (width << 16))


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
            **EMPTY_ANCHOR,
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
            {
                "kind": "run_end",
                **EMPTY_ERROR,
                "call": call,
                "outcome": 0,
                "snapshots": 2,
                "poison": 0,
            },
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

    def test_nonscalar_kind_is_rejected_as_incomplete_without_parser_crash(self):
        for kind in [[], {}, None, 1, True]:
            rows = capture_fixture()
            rows[2]["kind"] = kind
            with self.subTest(kind=kind):
                result = self.assess(encode_capture(rows))
                self.assertFalse(result["transport_complete"])
                self.assertIn("kind must be a string", result["transport_errors"][0])

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


class RefusalAttributionTests(unittest.TestCase):
    assess = SessionAssessmentTests.assess

    def refusal(self):
        rows = capture_fixture()
        error = {**EMPTY_ERROR, "error_ptr": 4000, "error_len": 19, "error_hash": 123}
        rows[1].update(
            **error, outcome=1, attempt=20, stage=1, failed_piece=2, consumer=7, request=99
        )
        next(r for r in rows if r["kind"] == "run_end").update(**error, outcome=1)
        return rows

    def test_exact_returned_root_matches_scoped_failed_attempt(self):
        result = self.assess(encode_capture(self.refusal()))
        self.assertTrue(result["refusal_attribution_complete"], result)
        self.assertTrue(result["calls"][0]["api_error_attributed"])
        self.assertTrue(result["capture_complete"])

    def test_pointer_alone_or_truncated_hash_cannot_attribute_api_error(self):
        for field, value in [("error_len", 20), ("error_hash", 124), ("error_truncated", True)]:
            rows = self.refusal()
            next(r for r in rows if r["kind"] == "run_end")[field] = value
            result = self.assess(encode_capture(rows))
            with self.subTest(field=field):
                self.assertTrue(result["transport_complete"], result)
                self.assertFalse(result["refusal_attribution_complete"])
                self.assertFalse(result["capture_complete"])

    def test_missing_failed_piece_or_consumer_disagrees_with_actual_authority(self):
        for field, value in [("failed_piece", 90), ("consumer", 8)]:
            rows = self.refusal()
            rows[1][field] = value
            result = self.assess(encode_capture(rows))
            with self.subTest(field=field):
                self.assertTrue(result["transport_complete"], result)
                self.assertFalse(result["refusal_attribution_complete"])

    def test_selected_refusal_cannot_be_attributed_to_a_successful_return(self):
        rows = self.refusal()
        next(r for r in rows if r["kind"] == "run_end")["outcome"] = 0
        result = self.assess(encode_capture(rows))
        self.assertTrue(result["transport_complete"], result)
        self.assertFalse(result["refusal_attribution_complete"])
        self.assertFalse(result["capture_complete"])

    def test_unanchored_explicit_refusal_does_not_attribute_api_error(self):
        rows = capture_fixture()
        next(r for r in rows if r["kind"] == "run_end")["outcome"] = 1
        result = self.assess(encode_capture(rows))
        self.assertTrue(result["transport_complete"], result)
        self.assertFalse(result["refusal_attribution_complete"])

    def test_partial_anchor_or_reused_attempt_fails_closed(self):
        rows = self.refusal()
        rows[1]["stage"] = 0
        self.assertFalse(self.assess(encode_capture(rows))["transport_complete"])
        rows = self.refusal()
        rows[1]["attempt"] = 2
        self.assertFalse(self.assess(encode_capture(rows))["transport_complete"])
        rows = self.refusal()
        begin2 = next(r for r in rows if r["kind"] == "snapshot_begin" and r["call"] == 2)
        begin2.update(**{key: rows[1][key] for key in collector.ANCHOR_FIELDS}, outcome=1)
        self.assertFalse(self.assess(encode_capture(rows))["transport_complete"])

    def test_pending_unadmitted_owner_cannot_claim_funding(self):
        rows = capture_fixture()
        next(r for r in rows if r["kind"] == "owner").update(flags=16, piece=0)
        result = self.assess(encode_capture(rows))
        self.assertTrue(result["transport_complete"], result)
        self.assertTrue(result["direct_ownership_complete"], result)
        self.assertFalse(result["funding_complete"])
        self.assertIn("not admitted", result["snapshots"][0]["funding_problems"][0])

    def test_retired_creation_parent_does_not_retire_live_owner_or_funding_need(self):
        rows = capture_fixture()
        owner = next(r for r in rows if r["kind"] == "owner")
        owner.update(flags=2, bytes=8)
        piece = next(r for r in rows if r["kind"] == "piece")
        piece["consumer"] = 0
        self.assertTrue(self.assess(encode_capture(rows))["capture_complete"])
        owner.update(piece=0)
        result = self.assess(encode_capture(rows))
        self.assertTrue(result["transport_complete"], result)
        self.assertFalse(result["funding_complete"])


class FundingRegressionTests(unittest.TestCase):
    def selected(self, mutate):
        rows = capture_fixture()
        stop = next(i for i, row in enumerate(rows) if row["kind"] == "snapshot_end")
        begin, records, end = rows[1], rows[2:stop], rows[stop]
        mutate(begin, records, end)
        try:
            return collector.validate_snapshot(begin, records, end, rows[0])["funding_complete"]
        except ValueError:
            return False

    def test_metadata_cannot_spend_unreserved_authority(self):
        def mutate(_begin, records, _end):
            owner = next(r for r in records if r["kind"] == "owner")
            owner.update(bytes=1024, flags=4)

        self.assertFalse(self.selected(mutate))

    def test_dangling_parent_is_not_a_complete_origin(self):
        def mutate(_begin, records, _end):
            next(r for r in records if r["kind"] == "owner")["parent"] = 999

        self.assertFalse(self.selected(mutate))

    def test_missing_evaluated_key_storage_blocks_funding(self):
        def mutate(_begin, records, end):
            next(r for r in records if r["kind"] == "owner")["owner_kind"] = 8
            records[:] = [r for r in records if r["kind"] != "backing"]
            end["backings"] = 0

        self.assertFalse(self.selected(mutate))

    def test_claim_cannot_exceed_its_resident(self):
        def mutate(begin, records, end):
            prototype = next(r for r in records if r["kind"] == "owner")
            resident = {**prototype, "id": 3, "owner_kind": 10, "a": 0, "b": 0, "bytes": 1}
            claim = {**prototype, "id": 4, "owner_kind": 11, "piece": 0, "a": 0, "b": 0}
            records += [
                resident,
                claim,
                {"kind": "edge", "call": 1, "from": 4, "to": 3, "relation": 2, "bytes": 1024},
            ]
            begin["constructed"] = 3
            end.update(owners=3, edges=1)

        self.assertFalse(self.selected(mutate))


def metadata_snapshot():
    prototype = batch_fixture()[0][1]
    copy_owner = {
        **prototype,
        "id": 1,
        "owner_kind": 9,
        "object": 0,
        "bytes": 16,
        "a": 32,
        "b": 0,
        "c": 0,
        "d": 2,
        "flags": 4,
    }
    resident = {
        **prototype,
        "id": 3,
        "owner_kind": 10,
        "parent": 1,
        "bytes": 32,
        "a": 0,
        "b": 0,
        "c": 0,
        "d": 0,
    }
    field = {
        **prototype,
        "id": 4,
        "owner_kind": 6,
        "parent": 1,
        "piece": 0,
        "bytes": 24,
        "a": 0,
        "b": 0,
        "c": 0,
        "d": 0,
        "flags": 4,
    }
    piece = {
        "kind": "piece",
        "call": 1,
        "id": 2,
        "root": 2,
        "holder": 1,
        "consumer": 7,
        "held": 48,
        "split_from": 0,
        "split_bytes": 0,
        "flags": 0,
    }
    edge = {"kind": "edge", "call": 1, "from": 4, "to": 3, "relation": 2, "bytes": 24}
    begin = {
        "kind": "snapshot_begin",
        **EMPTY_ANCHOR,
        "call": 1,
        "snapshot": 1,
        "outcome": 0,
        "epoch": 0,
        "in_flight": 0,
        "poison": 0,
        "gaps": 0,
        "constructed": 3,
        "retired": 0,
        "operations": 0,
    }
    end = {
        "kind": "snapshot_end",
        "call": 1,
        "snapshot": 1,
        "owners": 3,
        "pieces": 1,
        "backings": 0,
        "edges": 1,
        "complete": True,
    }
    return begin, [copy_owner, resident, field, piece, edge], end


class ResidentConservationTests(unittest.TestCase):
    def test_metadata_delegation_has_a_positive_supported_proof(self):
        result = collector.validate_snapshot(*metadata_snapshot(), header_fixture())
        self.assertTrue(result["funding_complete"], result)

    def test_metadata_delegation_cannot_exceed_resident_budget(self):
        begin, records, end = metadata_snapshot()
        records[2]["bytes"] = 64
        records[-1]["bytes"] = 64
        result = collector.validate_snapshot(begin, records, end, header_fixture())
        self.assertFalse(result["funding_complete"])
        self.assertIn("delegated claim exceeds its Resident", result["funding_problems"])

    def test_copy_subtotals_and_idle_floor_must_equal_actual_authority(self):
        for index, field, value in [(0, "a", 33), (3, "held", 49)]:
            begin, records, end = metadata_snapshot()
            records[index][field] = value
            with self.subTest(field=field):
                self.assertFalse(
                    collector.validate_snapshot(begin, records, end, header_fixture())[
                        "funding_complete"
                    ]
                )

    def test_parent_cycles_and_contradictory_split_flags_are_rejected(self):
        for index, field, value in [(0, "parent", 4), (3, "flags", 1)]:
            begin, records, end = metadata_snapshot()
            records[index][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                collector.validate_snapshot(begin, records, end, header_fixture())

    def test_declared_dimensions_cannot_expand_tiny_input_into_an_unbounded_set(self):
        header = header_fixture()
        header["columns"] = 10**12
        with self.assertRaisesRegex(ValueError, "dimensions"):
            collector.validate_header(header)

    def test_traversal_limit_is_per_owner_and_primitives_cannot_have_children(self):
        owners, rows = batch_fixture()
        owners[1]["b"] = 2
        alias = [{**r, "node": 2, "column": 1} for r in rows]
        header = header_fixture()
        header.update(nodes=1, buffers=1)
        with self.assertRaisesRegex(ValueError, "per-owner"):
            collector.validate_arrays(rows + alias, owners, header)
        header = header_fixture()
        rows[0]["children"] = 1
        alias[0].update(parent_node=1, column=0)
        alias[1].update(parent_node=1, column=0)
        with self.assertRaisesRegex(ValueError, "primitive"):
            collector.validate_arrays(rows + alias, owners, header)


def credit_snapshot():
    base = batch_fixture()[0][1]
    credit = {
        **base,
        "id": 1,
        "owner_kind": 16,
        "piece": 2,
        "bytes": 16,
        "held": 32,
        "a": 0,
        "b": 32,
        "c": 1,
        "d": 0,
        "flags": 4,
    }
    budget = {
        **base,
        "id": 3,
        "owner_kind": 15,
        "piece": 4,
        "bytes": 0,
        "held": 32,
        "a": 0,
        "b": 32,
        "c": 1,
        "d": 0,
    }
    permit = {
        **base,
        "id": 5,
        "owner_kind": 17,
        "parent": 1,
        "piece": 0,
        "bytes": 0,
        "held": 0,
        "a": 0,
        "b": 0,
        "c": 0,
        "d": 0,
    }
    control = {
        **base,
        "id": 6,
        "owner_kind": 18,
        "parent": 1,
        "piece": 7,
        "bytes": 16,
        "held": 16,
        "a": 3,
        "b": 0,
        "c": 0,
        "d": 0,
        "flags": 4,
    }
    metadata = {
        **base,
        "id": 8,
        "owner_kind": 18,
        "parent": 3,
        "piece": 0,
        "bytes": 24,
        "held": 0,
        "a": 1,
        "b": 0,
        "c": 0,
        "d": 0,
        "flags": 4,
    }
    owners = [credit, budget, permit, control, metadata]
    pieces = [
        {
            "kind": "piece",
            "call": 1,
            "id": identity,
            "root": identity,
            "holder": holder,
            "consumer": 7,
            "held": held,
            "split_from": 0,
            "split_bytes": 0,
            "flags": 0,
        }
        for identity, holder, held in [(2, 1, 32), (4, 3, 0), (7, 6, 16)]
    ]
    edges = [
        {
            "kind": "edge",
            "call": 1,
            "from": source,
            "to": target,
            "relation": relation,
            "bytes": amount,
        }
        for source, target, relation, amount in [
            (3, 1, 8, 32),
            (5, 1, 9, 0),
            (1, 6, 7, 16),
            (8, 3, 10, 24),
        ]
    ]
    begin = {
        "kind": "snapshot_begin",
        **EMPTY_ANCHOR,
        "call": 1,
        "snapshot": 1,
        "outcome": 0,
        "epoch": 0,
        "in_flight": 0,
        "poison": 0,
        "gaps": 0,
        "constructed": 5,
        "retired": 0,
        "operations": 0,
    }
    end = {
        "kind": "snapshot_end",
        "call": 1,
        "snapshot": 1,
        "owners": 5,
        "pieces": 3,
        "backings": 0,
        "edges": 4,
        "complete": True,
    }
    header = header_fixture()
    header.update(slots=8, edges=8, record_limit=140, byte_limit=143360, observer_bytes=15744)
    return begin, owners + pieces + edges, end, header


def queued_claim_snapshot():
    begin, records, end, header = credit_snapshot()
    prototype = batch_fixture()[0][1]
    claim = {
        **prototype,
        "id": 9,
        "owner_kind": 11,
        "parent": 3,
        "piece": 0,
        "bytes": 8,
        "held": 0,
        "a": 8,
        "b": 0,
    }
    batch = {**prototype, "id": 10, "parent": 11, "piece": 0, "c": 1, "d": 1}
    queue = {
        **prototype,
        "id": 11,
        "owner_kind": 12,
        "piece": 0,
        "held": 0,
        "a": 0,
        "b": 0,
        "flags": 4,
    }
    _, backing = batch_fixture()
    backing = [{**row, "owner": 10} for row in backing]
    raw = {
        **backing[1],
        "owner": 9,
        "node": 1,
        "type": 35,
        "object": 0,
        "offset": 0,
    }
    records += [claim, batch, queue, *backing, raw]
    for source, target, relation, amount in [(9, 3, 10, 8), (9, 8, 7, 0), (10, 9, 3, 8)]:
        records.append(
            {
                "kind": "edge",
                "call": 1,
                "from": source,
                "to": target,
                "relation": relation,
                "bytes": amount,
            }
        )
    begin["constructed"] = 8
    end.update(owners=8, backings=3, edges=7)
    return begin, records, end, header


class QueuedClaimTests(unittest.TestCase):
    def check(self, snapshot):
        return collector.validate_snapshot(*snapshot)

    def test_queued_alias_uses_the_validated_claims_borrowed_budget(self):
        result = self.check(queued_claim_snapshot())
        self.assertTrue(result["funding_complete"], result)

    def test_shared_claim_can_use_validated_copy_resident_authority(self):
        begin, records, end = metadata_snapshot()
        prototype = batch_fixture()[0][1]
        claim = {
            **prototype,
            "id": 5,
            "owner_kind": 11,
            "parent": 3,
            "piece": 0,
            "bytes": 8,
            "a": 8,
            "b": 0,
        }
        batch = {**prototype, "id": 6, "piece": 0}
        _, backings = batch_fixture()
        backings = [{**row, "owner": 6} for row in backings]
        raw = {**backings[1], "owner": 5, "type": 35, "object": 0}
        records.extend([claim, batch, *backings, raw])
        for source, target, relation, amount in [(5, 3, 2, 8), (5, 3, 7, 0), (6, 5, 3, 8)]:
            records.append(
                {
                    "kind": "edge",
                    "call": 1,
                    "from": source,
                    "to": target,
                    "relation": relation,
                    "bytes": amount,
                }
            )
        begin["constructed"] = 5
        end.update(owners=5, backings=3, edges=4)
        result = self.check((begin, records, end, credit_snapshot()[3]))
        self.assertTrue(result["funding_complete"], result)

    def test_slices_share_one_claim_without_spending_it_twice(self):
        snapshot = queued_claim_snapshot()
        node, values = [r for r in snapshot[1] if r["kind"] == "backing" and r["owner"] == 10]
        snapshot[1].extend([{**node, "node": 2, "column": 1}, {**values, "node": 2, "column": 1}])
        next(r for r in snapshot[1] if r["kind"] == "owner" and r["id"] == 10)["b"] = 2
        snapshot[2]["backings"] = 5
        self.assertTrue(self.check(snapshot)["funding_complete"])

    def test_alias_requires_the_claims_separate_funded_ipc_metadata(self):
        for target in [1, 3, 6, None]:
            snapshot = queued_claim_snapshot()
            header_edge = next(
                r
                for r in snapshot[1]
                if r["kind"] == "edge" and r["from"] == 9 and r["relation"] == 7
            )
            if target is None:
                snapshot[1].remove(header_edge)
                snapshot[2]["edges"] -= 1
            else:
                header_edge["to"] = target
            with self.subTest(target=target):
                self.assertFalse(self.check(snapshot)["funding_complete"])

    def test_ipc_metadata_cannot_replace_its_budget_with_a_tiny_resident(self):
        snapshot = queued_claim_snapshot()
        prototype = batch_fixture()[0][1]
        copy_owner = {
            **prototype,
            "id": 12,
            "owner_kind": 9,
            "piece": 14,
            "object": 0,
            "bytes": 16,
            "a": 1,
            "b": 0,
            "d": 2,
            "flags": 4,
        }
        resident = {
            **prototype,
            "id": 13,
            "owner_kind": 10,
            "piece": 14,
            "parent": 12,
            "bytes": 1,
            "a": 0,
            "b": 0,
        }
        piece = {
            **next(r for r in snapshot[1] if r["kind"] == "piece"),
            "id": 14,
            "root": 14,
            "holder": 12,
            "held": 17,
        }
        snapshot[1].extend([copy_owner, resident, piece])
        header_edge = next(r for r in snapshot[1] if r["kind"] == "edge" and r["from"] == 8)
        header_edge.update(to=13, relation=2, bytes=0)
        snapshot[0]["constructed"] = 10
        snapshot[2].update(owners=10, pieces=4)
        snapshot[3].update(slots=12, record_limit=172, byte_limit=176128, observer_bytes=19840)
        collector.validate_header(snapshot[3])
        self.assertFalse(self.check(snapshot)["funding_complete"])

    def test_alias_matching_is_per_backing_and_requires_exact_claim_amount(self):
        for field, value in [("to", 8), ("bytes", 7)]:
            snapshot = queued_claim_snapshot()
            snapshot[1][-1][field] = value
            with self.subTest(field=field):
                self.assertFalse(self.check(snapshot)["funding_complete"])
        snapshot = queued_claim_snapshot()
        raw = next(r for r in snapshot[1] if r["kind"] == "backing" and r["type"] == 35)
        raw["base"] = 4000
        self.assertFalse(self.check(snapshot)["funding_complete"])

    def test_valid_alias_does_not_hide_another_unfunded_backing(self):
        snapshot = queued_claim_snapshot()
        node, values = [r for r in snapshot[1] if r["kind"] == "backing" and r["owner"] == 10]
        snapshot[1].extend(
            [{**node, "node": 2, "column": 1}, {**values, "node": 2, "column": 1, "base": 4000}]
        )
        next(r for r in snapshot[1] if r["kind"] == "owner" and r["id"] == 10)["b"] = 2
        snapshot[2]["backings"] = 5
        self.assertFalse(self.check(snapshot)["funding_complete"])

    def test_alias_cannot_hide_broken_claim_funding_or_local_admission(self):
        for kind, identity, field, value in [
            ("owner", 10, "bytes", 8),
            ("owner", 10, "flags", 16),
            ("piece", 2, "held", 0),
            ("piece", 4, "holder", 5),
        ]:
            snapshot = queued_claim_snapshot()
            next(r for r in snapshot[1] if r["kind"] == kind and r["id"] == identity)[field] = value
            with self.subTest(kind=kind, identity=identity, field=field):
                self.assertFalse(self.check(snapshot)["funding_complete"])
        snapshot = queued_claim_snapshot()
        next(
            r for r in snapshot[1] if r["kind"] == "edge" and r["from"] == 9 and r["relation"] == 10
        )["bytes"] = 7
        self.assertFalse(self.check(snapshot)["funding_complete"])


class AuthorityForgeryTests(unittest.TestCase):
    def funded(self, snapshot):
        try:
            return collector.validate_snapshot(*snapshot)["funding_complete"]
        except ValueError:
            return False

    def test_batch_admission_cannot_exceed_its_actual_piece(self):
        rows = capture_fixture()
        stop = next(i for i, r in enumerate(rows) if r["kind"] == "snapshot_end")
        rows[2]["bytes"] = 1024
        self.assertFalse(self.funded((rows[1], rows[2:stop], rows[stop], rows[0])))

    def test_row_cannot_erase_required_converter_layout_by_zeroing_count(self):
        begin, records, end = metadata_snapshot()
        records[2].update(owner_kind=7, b=0)
        self.assertFalse(self.funded((begin, records, end, header_fixture())))

    def test_credit_cannot_erase_control_floor_and_control_piece(self):
        begin, records, end, header = credit_snapshot()
        records[0]["bytes"] = 0
        records = [
            r
            for r in records
            if not (
                (r["kind"] == "owner" and r["id"] == 6)
                or (r["kind"] == "piece" and r["id"] == 7)
                or (r["kind"] == "edge" and r["from"] == 1)
            )
        ]
        begin["constructed"] = 4
        end.update(owners=4, pieces=2, edges=3)
        self.assertFalse(self.funded((begin, records, end, header)))

    def test_credit_control_cannot_reuse_the_bank_piece(self):
        begin, records, end, header = credit_snapshot()
        next(r for r in records if r["kind"] == "owner" and r["id"] == 6)["piece"] = 2
        records = [r for r in records if r["kind"] != "piece" or r["id"] != 7]
        end["pieces"] = 2
        self.assertFalse(self.funded((begin, records, end, header)))

    def test_actual_authority_kinds_must_hold_their_own_piece(self):
        begin, records, end = metadata_snapshot()
        next(r for r in records if r["kind"] == "piece")["holder"] = 4
        self.assertFalse(self.funded((begin, records, end, header_fixture())))
        for piece in (2, 4, 7):
            begin, records, end, header = credit_snapshot()
            next(r for r in records if r["kind"] == "piece" and r["id"] == piece)["holder"] = 5
            with self.subTest(piece=piece):
                self.assertFalse(self.funded((begin, records, end, header)))

    def budgeted_data_snapshot(self, owner_kind=5):
        rows = capture_fixture()
        stop = next(i for i, r in enumerate(rows) if r["kind"] == "snapshot_end")
        begin, records, end = rows[1], rows[2:stop], rows[stop]
        owner, piece = records[:2]
        owner["piece"] = 0
        budget = {**owner, "id": 3, "owner_kind": 15, "piece": 2, "held": 0, "a": 0, "b": 0}
        piece.update(holder=3, held=0)
        records += [
            budget,
            {"kind": "edge", "call": 1, "from": 1, "to": 3, "relation": 10, "bytes": 0},
        ]
        begin["constructed"] = 2
        end.update(owners=2, edges=1)
        owner.update(owner_kind=owner_kind, flags=4 if owner_kind == 18 else 0)
        return begin, records, end, rows[0]

    def test_untyped_budget_edge_cannot_fund_batch_with_zero_authority(self):
        self.assertFalse(self.funded(self.budgeted_data_snapshot()))

    def test_metadata_cannot_hide_unfunded_data_backings_under_valid_budget_edge(self):
        self.assertFalse(self.funded(self.budgeted_data_snapshot(18)))


class RawRowsEvidenceTests(unittest.TestCase):
    def test_raw_rows_are_complete_only_with_all_concrete_components(self):
        rows = capture_fixture()
        stop = next(i for i, r in enumerate(rows) if r["kind"] == "snapshot_end")
        begin, owner, piece = rows[1:4]
        owner.update(owner_kind=8, flags=8, bytes=32, a=1, b=3)
        piece["held"] = 32
        prototype = rows[5]
        backings = [
            {
                **prototype,
                "node": node,
                "column": node - 1,
                "type": kind,
                "base": 1000 + node,
                "object": 1000 + node,
                "capacity": size,
                "length": size,
            }
            for node, kind, size in [(1, 36, 8), (2, 37, 16), (3, 38, 8)]
        ]
        end = {**rows[stop], "backings": 3}
        result = collector.validate_snapshot(begin, [owner, piece, *backings], end, rows[0])
        self.assertTrue(result["funding_complete"], result)
        with self.assertRaisesRegex(ValueError, "Rows key storage"):
            collector.validate_snapshot(
                begin, [owner, piece, *backings[:2]], {**end, "backings": 2}, rows[0]
            )

    def test_row_converter_cannot_omit_its_published_components(self):
        begin, records, end = metadata_snapshot()
        records[2].update(owner_kind=7, b=2)
        with self.assertRaisesRegex(ValueError, "RowConverter metadata"):
            collector.validate_snapshot(begin, records, end, header_fixture())


class IpcCreditTests(unittest.TestCase):
    def test_borrowed_budget_has_actual_bank_and_separate_control_authority(self):
        snapshot = credit_snapshot()
        collector.validate_header(snapshot[3])
        result = collector.validate_snapshot(*snapshot)
        self.assertTrue(result["funding_complete"], result)

    def test_consumer_equality_cannot_replace_the_actual_credit_edge(self):
        begin, records, end, header = credit_snapshot()
        records[-4]["bytes"] = 31
        result = collector.validate_snapshot(begin, records, end, header)
        self.assertFalse(result["funding_complete"])

    def test_credit_outstanding_and_permit_retirement_must_reconcile(self):
        for field, value in [("b", 33), ("c", 0), ("d", 2)]:
            begin, records, end, header = credit_snapshot()
            records[0][field] = value
            try:
                result = collector.validate_snapshot(begin, records, end, header)
                self.assertFalse(result["funding_complete"], field)
            except ValueError:
                pass

    def test_batch_metadata_cannot_spend_beyond_borrowed_budget(self):
        begin, records, end, header = credit_snapshot()
        records[4]["bytes"] = 64
        records[-1]["bytes"] = 64
        result = collector.validate_snapshot(begin, records, end, header)
        self.assertFalse(result["funding_complete"])
        self.assertIn("IPC Budget authority is spent more than once", result["funding_problems"])


if __name__ == "__main__":
    unittest.main()
