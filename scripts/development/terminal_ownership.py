"""Read-only assessment of bounded Rust terminal ownership captures.

Capture data is untrusted. Transport validity, direct ownership, funding and
native readiness are separate claims. Numeric observations never authenticate
an input file or prove coverage of an uninstrumented operator.
"""

import json
import os
from pathlib import Path
import stat

U64_MAX = (1 << 64) - 1
MAX_LINE_BYTES = 16384
MAX_CAPTURE_BYTES = 64 * 1024 * 1024
MAX_RECORDS = 100000
MAX_DEPTH = 16
FNV_OFFSET = 0xCBF29CE484222325
FNV_PRIME = 0x100000001B3


def uint(value, name):
    if type(value) is not int or not 0 <= value <= U64_MAX:
        raise ValueError(name + " must be an unsigned 64-bit integer")
    return value


def checked_add(*values):
    total = 0
    for value in values:
        total += uint(value, "operand")
        uint(total, "sum")
    return total


def checked_mul(*values):
    total = 1
    for value in values:
        total *= uint(value, "operand")
        uint(total, "product")
    return total


def checksum(raw, value=FNV_OFFSET):
    for byte in raw:
        value = ((value ^ byte) * FNV_PRIME) & U64_MAX
    return value


def decode_record(raw, depth=MAX_DEPTH):
    # Scan the bounded raw bytes before json.loads can recurse or allocate a
    # nested tree. Quotes and escaped backslashes do not change JSON depth.
    nesting = 0
    quoted = False
    escaped = False
    for byte in raw:
        if quoted:
            if escaped:
                escaped = False
            elif byte == 92:
                escaped = True
            elif byte == 34:
                quoted = False
        elif byte == 34:
            quoted = True
        elif byte in (91, 123):
            nesting += 1
            if nesting > depth:
                raise ValueError("JSON nesting depth exceeded")
        elif byte in (93, 125):
            nesting -= 1
            if nesting < 0:
                raise ValueError("unbalanced JSON container")

    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate JSON key: " + key)
            result[key] = value
        return result

    def integer(text):
        if len(text) > 20:
            raise ValueError("integer length exceeded")
        return uint(int(text), "JSON integer")

    def noninteger(text):
        raise ValueError("noninteger JSON number: " + text)

    try:
        value = json.loads(
            raw,
            object_pairs_hook=pairs,
            parse_int=integer,
            parse_float=noninteger,
            parse_constant=noninteger,
        )
    except (UnicodeError, RecursionError) as error:
        raise ValueError("invalid JSON encoding or depth") from error
    if not isinstance(value, dict):
        raise ValueError("capture record must be a JSON object")
    return value


def read_records(path, line_bytes, byte_limit, record_limit):
    """Yield bounded (object, exact LF-terminated bytes) from one regular file."""
    if not 0 < uint(line_bytes, "line_bytes") <= MAX_LINE_BYTES:
        raise ValueError("unsupported line bound")
    if not 0 < uint(byte_limit, "byte_limit") <= MAX_CAPTURE_BYTES:
        raise ValueError("unsupported byte bound")
    if not 0 < uint(record_limit, "record_limit") <= MAX_RECORDS:
        raise ValueError("unsupported record bound")
    try:
        descriptor = os.open(Path(path), os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError as error:
        raise ValueError("capture must be a readable regular file without symlinks") from error
    with os.fdopen(descriptor, "rb") as source:
        if not stat.S_ISREG(os.fstat(source.fileno()).st_mode):
            raise ValueError("capture must be a regular file")
        count = 0
        total = 0
        while raw := source.readline(line_bytes + 1):
            count += 1
            total += len(raw)
            if len(raw) > line_bytes or total > byte_limit or count > record_limit:
                raise ValueError("capture line, byte or record limit exceeded")
            if not raw.endswith(b"\n"):
                raise ValueError("partial last capture record")
            yield decode_record(raw), raw


HEADER_FIELDS = {
    "kind",
    "protocol",
    "calls",
    "slots",
    "descriptors",
    "edges",
    "columns",
    "nodes",
    "depth",
    "buffers",
    "line_bytes",
    "record_limit",
    "byte_limit",
    "observer_bytes",
    "owner_bytes",
    "piece_bytes",
    "backing_bytes",
    "edge_bytes",
    "fixed_bytes",
}


def validate_header(header):
    if set(header) != HEADER_FIELDS or header.get("kind") != "session":
        raise ValueError("unknown or incomplete session header")
    if header.get("protocol") != "terminal-ownership/1":
        raise ValueError("unsupported ownership protocol")
    for name in HEADER_FIELDS - {"kind", "protocol"}:
        uint(header[name], name)
        if name != "edges" and header[name] == 0:
            raise ValueError("zero session bound: " + name)
    if header["line_bytes"] != 1024 or header["calls"] > 64:
        raise ValueError("unsupported session line or call bound")
    per_cut = checked_add(
        2, checked_mul(2, header["slots"]), header["descriptors"], header["edges"]
    )
    records = checked_add(2, checked_mul(header["calls"], checked_add(1, checked_mul(2, per_cut))))
    byte_limit = checked_mul(records, header["line_bytes"])
    arena = checked_add(
        checked_mul(header["slots"], checked_add(header["owner_bytes"], header["piece_bytes"])),
        checked_mul(header["descriptors"], header["backing_bytes"]),
        checked_mul(header["edges"], header["edge_bytes"]),
    )
    observer = checked_add(
        checked_mul(checked_add(1, checked_mul(2, header["calls"])), arena), header["fixed_bytes"]
    )
    if (header["record_limit"], header["byte_limit"], header["observer_bytes"]) != (
        records,
        byte_limit,
        observer,
    ):
        raise ValueError("writer/parser budget derivation disagrees")
    if records > MAX_RECORDS or byte_limit > MAX_CAPTURE_BYTES:
        raise ValueError("capture envelope exceeds parser ceiling")
    if header["depth"] > MAX_DEPTH:
        raise ValueError("capture traversal depth exceeds parser ceiling")
    return header


OWNER_FIELDS = {
    "kind",
    "call",
    "id",
    "owner_kind",
    "parent",
    "piece",
    "object",
    "bytes",
    "held",
    "a",
    "b",
    "c",
    "d",
    "flags",
}
PIECE_FIELDS = {
    "kind",
    "call",
    "id",
    "root",
    "holder",
    "consumer",
    "held",
    "split_from",
    "split_bytes",
    "flags",
}
BACKING_FIELDS = {
    "kind",
    "call",
    "owner",
    "node",
    "parent_node",
    "column",
    "type",
    "object",
    "base",
    "capacity",
    "offset",
    "length",
    "nulls",
    "flags",
    "children",
    "buffers",
}
EDGE_FIELDS = {"kind", "call", "from", "to", "relation", "bytes"}
BEGIN_FIELDS = {
    "kind",
    "call",
    "snapshot",
    "outcome",
    "epoch",
    "in_flight",
    "poison",
    "gaps",
    "constructed",
    "retired",
    "operations",
}
END_FIELDS = {"kind", "call", "snapshot", "owners", "pieces", "backings", "edges", "complete"}
RUN_FIELDS = {"kind", "call", "outcome", "snapshots", "poison"}
FOOTER_FIELDS = {"kind", "calls", "records", "bytes", "checksum", "complete"}


def numeric_record(record, fields, boolean_fields=()):
    if set(record) != fields:
        raise ValueError("unknown or missing fields in " + str(record.get("kind")))
    for key in fields - {"kind"} - set(boolean_fields):
        uint(record[key], key)
    for key in boolean_fields:
        if type(record[key]) is not bool:
            raise ValueError(key + " must be boolean")


def type_width(code):
    """Validate the emitter's concrete layout encoding; return type and width."""
    low, high = code & 65535, code >> 16
    widths = {3: 1, 4: 2, 5: 4, 6: 8, 7: 1, 8: 2, 9: 4, 10: 8, 11: 2, 12: 4, 13: 8, 14: 4, 15: 8}
    if not 1 <= low <= 34:
        raise ValueError("unsupported array type")
    if 16 <= low <= 19:
        units = {16: {1, 2}, 17: {3, 4}, 18: {1, 2, 3, 4}, 19: {1, 2, 3, 4}}
        if high not in units[low]:
            raise ValueError("invalid temporal unit")
        return low, 4 if low == 16 else 8
    if low == 20:
        if high not in {1, 2, 3}:
            raise ValueError("invalid interval unit")
        return low, {1: 4, 2: 8, 3: 16}[high]
    if low == 21:
        if not 0 < high < (1 << 32):
            raise ValueError("invalid fixed binary width")
        return low, high
    if low >= 31:
        if high >> 16 or not 0 < (high & 255) <= {31: 9, 32: 18, 33: 38, 34: 76}[low]:
            raise ValueError("invalid decimal precision/scale encoding")
        return low, {31: 4, 32: 8, 33: 16, 34: 32}[low]
    if high:
        raise ValueError("unknown array type bits")
    return low, widths.get(low)


def validate_arrays(backings, owners, header):
    """Check concrete forest shape and every required backing, including NULLs."""
    nodes = {}
    buffers = {}
    children = {}
    roots = {}
    capacities = {}
    for row in backings:
        numeric_record(row, BACKING_FIELDS)
        owner, node = row["owner"], row["node"]
        if owner not in owners or not node or row["column"] >= header["columns"]:
            raise ValueError("backing has no live owner/node or exceeds columns")
        type_width(row["type"])
        if row["flags"] == 1:
            key = owner, node
            if key in nodes or row["base"] or row["capacity"] or row["nulls"] > row["length"]:
                raise ValueError("duplicate or invalid array node")
            if row["children"] > header["columns"] or row["buffers"] > header["buffers"]:
                raise ValueError("array multiplicity exceeds traversal envelope")
            checked_add(row["offset"], row["length"])
            nodes[key] = row
        else:
            flags = row["flags"]
            role = flags & ~32
            if role not in {2, 4, 8, 16} or flags & ~62:
                raise ValueError("unknown backing role/flags")
            if flags & 32 and role not in {2, 4}:
                raise ValueError("invalid bit-span role")
            extent = checked_mul(row["capacity"], 8) if flags & 32 else row["capacity"]
            if checked_add(row["offset"], row["length"]) > extent:
                raise ValueError("buffer span exceeds backing capacity")
            if row["capacity"] and not row["base"]:
                raise ValueError("positive backing capacity lacks allocation base")
            if row["base"] and row["capacity"]:
                previous = capacities.setdefault(row["base"], row["capacity"])
                if previous != row["capacity"]:
                    raise ValueError("conflicting simultaneously live backing capacity")
            buffers.setdefault((owner, node), []).append(row)
    if len(nodes) > checked_mul(header["slots"], header["nodes"]):
        raise ValueError("array node envelope exceeded")
    for key in buffers:
        if key not in nodes:
            raise ValueError("buffer lacks its array node")
    for key, node in nodes.items():
        owner, ordinal = key
        parent = node["parent_node"]
        if parent:
            if parent >= ordinal or (owner, parent) not in nodes:
                raise ValueError("array parent missing or cyclic")
            children.setdefault((owner, parent), []).append(node)
        else:
            roots.setdefault(owner, []).append(node)
        depth = 1
        ancestor = parent
        while ancestor:
            depth += 1
            if depth > header["depth"]:
                raise ValueError("array forest exceeds depth")
            ancestor = nodes[owner, ancestor]["parent_node"]
    for key, node in nodes.items():
        child_nodes = children.get(key, [])
        storage = buffers.get(key, [])
        if len(child_nodes) != node["children"] or len(storage) != node["buffers"]:
            raise ValueError("array child/buffer cardinality incomplete")
        if any(row["column"] != node["column"] for row in child_nodes + storage):
            raise ValueError("array child/buffer column mismatch")
        low, width = type_width(node["type"])
        roles = [row["flags"] & ~32 for row in storage]
        validity = [row for row in storage if row["flags"] & ~32 == 2]
        if len(validity) > 1 or (node["nulls"] and low != 1 and not validity):
            raise ValueError("missing or duplicate validity buffer")
        for row in validity:
            if row["flags"] != 34 or row["length"] != node["length"]:
                raise ValueError("invalid validity bit span")
        required = []
        if low == 1:
            if storage or child_nodes:
                raise ValueError("Null array unexpectedly has backing/children")
        elif low == 30:
            pass
        elif low in {28, 29}:
            required = [8]
            if len(child_nodes) != 1:
                raise ValueError("list child incomplete")
        elif low in {22, 23, 24, 25}:
            required = [8, 4]
        elif low in {26, 27}:
            required = [16]
        else:
            required = [4]
        nonnull_roles = [role for role in roles if role != 2]
        if low not in {26, 27} and sorted(nonnull_roles) != sorted(required):
            raise ValueError("missing or extra type backing")
        if low in {26, 27} and (
            nonnull_roles.count(16) != 1 or any(role not in {4, 16} for role in nonnull_roles)
        ):
            raise ValueError("invalid view backing")
        for row in storage:
            if row["type"] != node["type"] or row["parent_node"] != node["parent_node"]:
                raise ValueError("buffer type/parent mismatch")
            role = row["flags"] & ~32
            if row["flags"] & 32 and role == 4 and low != 2:
                raise ValueError("non-Boolean value buffer uses bit units")
            minimum = 0
            if role == 4 and width is not None:
                minimum = checked_mul(node["length"], width)
            elif role == 4 and low == 2:
                minimum = node["length"]
                if row["flags"] != 36:
                    raise ValueError("Boolean values require a bit span")
            elif role == 8:
                minimum = checked_mul(
                    checked_add(node["length"], 1), 8 if low in {23, 25, 29} else 4
                )
            elif role == 16:
                minimum = checked_mul(node["length"], 16)
            if row["length"] < minimum:
                raise ValueError("backing does not cover logical array, including all-null values")
    for owner, row in owners.items():
        if row["owner_kind"] not in {5, 14} or row["flags"] & 1:
            continue
        arrays = roots.get(owner, [])
        if (
            row["b"] > header["columns"]
            or {n["column"] for n in arrays} != set(range(row["b"]))
            or len(arrays) != row["b"]
        ):
            raise ValueError("batch root column coverage incomplete")
        if any(n["length"] != row["a"] for n in arrays):
            raise ValueError("batch root row count disagrees")
    return capacities


def validate_snapshot(begin, records, end, header):
    numeric_record(begin, BEGIN_FIELDS)
    numeric_record(end, END_FIELDS, {"complete"})
    if begin["call"] != end["call"] or begin["snapshot"] != end["snapshot"]:
        raise ValueError("snapshot boundary mismatch")
    if begin["poison"] & ~15 or begin["gaps"] & ~31 or begin["outcome"] > 2:
        raise ValueError("unknown snapshot flags/outcome")
    if begin["retired"] > begin["constructed"] or begin["in_flight"] > begin["operations"]:
        raise ValueError("invalid lifecycle counters")
    if begin["epoch"] != checked_add(
        checked_mul(2, begin["operations"] - begin["in_flight"]), begin["in_flight"]
    ):
        raise ValueError("transition epoch disagrees with open operations")
    owners = {}
    pieces = {}
    backings = []
    edges = []
    for row in records:
        kind = row.get("kind")
        if kind not in {"owner", "piece", "backing", "edge"}:
            raise ValueError("unknown snapshot record")
        if kind == "owner":
            numeric_record(row, OWNER_FIELDS)
            if (
                not row["id"]
                or row["id"] in owners
                or not 1 <= row["owner_kind"] <= 14
                or row["flags"] & ~7
            ):
                raise ValueError("duplicate or unsupported owner identity/kind/flags")
            owners[row["id"]] = row
        elif kind == "piece":
            numeric_record(row, PIECE_FIELDS)
            if not row["id"] or row["id"] in pieces or row["flags"] & ~7:
                raise ValueError("duplicate or unsupported reservation piece")
            pieces[row["id"]] = row
        elif kind == "backing":
            backings.append(row)
        else:
            numeric_record(row, EDGE_FIELDS)
            if not 1 <= row["relation"] <= 7:
                raise ValueError("unknown ownership relation")
            edges.append(row)
        if not 1 <= row["call"] <= begin["call"]:
            raise ValueError("record belongs to an invalid/future call")
    counts = len(owners), len(pieces), len(backings), len(edges)
    if counts != tuple(end[k] for k in ("owners", "pieces", "backings", "edges")):
        raise ValueError("snapshot record counts disagree")
    if any(
        actual > limit
        for actual, limit in zip(
            counts,
            (header["slots"], header["slots"], header["descriptors"], header["edges"]),
            strict=True,
        )
    ):
        raise ValueError("snapshot exceeds retained-state envelope")
    if owners.keys() & pieces.keys():
        raise ValueError("owner and reservation piece reuse a generation")
    if begin["constructed"] - begin["retired"] != len(owners):
        raise ValueError("live owner count does not reconcile with lifecycle counters")
    capacities = validate_arrays(backings, owners, header)
    direct_problems = []
    funding_problems = []
    if begin["in_flight"]:
        direct_problems.append("product transition in flight; cut is ambiguous")
    if begin["poison"]:
        direct_problems.append("recorder poisoned")
    if begin["gaps"]:
        direct_problems.append("explicit instrumented-scope coverage gap")
    if any(o["flags"] & 1 for o in owners.values()):
        direct_problems.append("opaque owner")
    for piece in pieces.values():
        if not piece["root"] or not piece["consumer"]:
            raise ValueError("reservation lacks root/consumer generation")
        if piece["flags"] & 4:
            if piece["holder"] or piece["held"] or piece["root"] != piece["id"]:
                raise ValueError("invalid retired-root certificate")
        elif piece["holder"] not in owners:
            raise ValueError("reservation lacks live holder")
        if piece["root"] not in pieces:
            funding_problems.append("missing reservation origin certificate")
        elif pieces[piece["root"]]["consumer"] != piece["consumer"]:
            raise ValueError("split reservation changed consumer")
        if piece["split_from"] and (piece["split_from"] != piece["root"] or not piece["flags"] & 1):
            raise ValueError("unknown split ancestry")
    relations = {}
    graph = {owner: [] for owner in owners}
    indegree = dict.fromkeys(owners, 0)
    seen_edges = set()
    for edge in edges:
        source, target, relation = edge["from"], edge["to"], edge["relation"]
        key = source, target, relation
        if key in seen_edges or source not in owners or target not in owners:
            raise ValueError("duplicate or dangling ownership edge")
        seen_edges.add(key)
        relations.setdefault((source, relation), []).append(edge)
        if relation in {1, 2}:
            graph[source].append(target)
            indegree[target] += 1
        if relation == 2 and owners[target]["owner_kind"] != 10:
            raise ValueError("funding edge does not identify a Resident")
    pending = [owner for owner, degree in indegree.items() if degree == 0]
    count = 0
    while pending:
        owner = pending.pop()
        count += 1
        for target in graph[owner]:
            indegree[target] -= 1
            if indegree[target] == 0:
                pending.append(target)
    if count != len(owners):
        raise ValueError("cyclic retention/funding graph")
    used = {}
    for owner, row in owners.items():
        piece = row["piece"]
        sponsors = relations.get((owner, 2), [])
        if piece and piece not in pieces:
            funding_problems.append("owner lacks its actual reservation piece")
        if sponsors:
            targets = {owners[e["to"]]["piece"] for e in sponsors}
            if len(targets) != 1 or 0 in targets:
                funding_problems.append("ambiguous Resident sponsor")
            else:
                piece = targets.pop()
        owned_backings = {
            (b["base"], b["capacity"])
            for b in backings
            if b["owner"] == owner and b["flags"] != 1 and b["capacity"]
        }
        if owned_backings:
            if piece not in pieces or pieces[piece]["flags"] & 4:
                funding_problems.append("retained backing has no live sponsoring authority")
            else:
                used.setdefault(piece, set()).update(owned_backings)
    for piece, allocations in used.items():
        if checked_add(*(capacity for _, capacity in allocations)) > pieces[piece]["held"]:
            funding_problems.append("live backing capacity exceeds its sponsoring authority")
    direct = not direct_problems
    funding = direct and not funding_problems
    if end["complete"] and not direct:
        raise ValueError("writer claims completeness despite explicit coverage gaps")
    if not end["complete"]:
        direct_problems.append("writer did not commit snapshot completeness")
        direct = funding = False
    return {
        "call": begin["call"],
        "snapshot": begin["snapshot"],
        "outcome": begin["outcome"],
        "direct_complete": direct,
        "funding_complete": funding,
        "direct_problems": direct_problems,
        "funding_problems": funding_problems,
        "owners": len(owners),
        "pieces": len(pieces),
        "unique_backing_bytes": checked_add(*capacities.values()),
        "generations": {
            identity: ("owner", o["call"], o["owner_kind"]) for identity, o in owners.items()
        }
        | {
            identity: ("piece", p["call"], p["root"], p["consumer"])
            for identity, p in pieces.items()
        },
    }


def assess_capture(path):
    summaries = []
    errors = []
    header = None
    begin = None
    records = []
    generations = {}
    active_ids = set()
    retired_ids = set()
    prior_constructed = prior_retired = prior_operations = 0
    calls = cuts = count = total = 0
    digest = FNV_OFFSET
    footer = False
    try:
        for row, raw in read_records(path, MAX_LINE_BYTES, MAX_CAPTURE_BYTES, MAX_RECORDS):
            kind = row.get("kind")
            if footer:
                raise ValueError("records after final session commit")
            if header is None:
                header = validate_header(row)
            elif kind == "snapshot_begin":
                numeric_record(row, BEGIN_FIELDS)
                if (
                    begin is not None
                    or row["call"] != calls + 1
                    or row["snapshot"] != cuts + 1
                    or cuts >= 2
                ):
                    raise ValueError("missing, duplicated or reordered snapshot boundary")
                if any(
                    row[k] < previous
                    for k, previous in (
                        ("constructed", prior_constructed),
                        ("retired", prior_retired),
                        ("operations", prior_operations),
                    )
                ):
                    raise ValueError("lifecycle counters regressed across calls")
                prior_constructed, prior_retired, prior_operations = (
                    row[k] for k in ("constructed", "retired", "operations")
                )
                begin = row
                records = []
            elif kind == "snapshot_end":
                if begin is None:
                    raise ValueError("snapshot end without beginning")
                summary = validate_snapshot(begin, records, row, header)
                current = summary.pop("generations")
                if retired_ids & current.keys():
                    raise ValueError("retired generation reused")
                for identity, origin in current.items():
                    if identity in generations and generations[identity] != origin:
                        raise ValueError("generation changed origin/type")
                retired_ids.update(active_ids - current.keys())
                active_ids = set(current)
                generations.update(current)
                summaries.append(summary)
                cuts += 1
                begin = None
                records = []
            elif kind == "run_end":
                numeric_record(row, RUN_FIELDS)
                if (
                    begin is not None
                    or row["call"] != calls + 1
                    or cuts != 2
                    or row["snapshots"] != 2
                    or row["outcome"] > 2
                    or row["poison"]
                ):
                    raise ValueError("call incomplete or recorder poisoned")
                calls += 1
                cuts = 0
            elif kind == "session_end":
                numeric_record(row, FOOTER_FIELDS, {"complete"})
                if begin is not None or cuts or calls != header["calls"] or row["calls"] != calls:
                    raise ValueError("session lacks every declared call and closure")
                if (row["records"], row["bytes"], row["checksum"]) != (count, total, digest):
                    raise ValueError("session byte/count/checksum commit disagrees")
                if not row["complete"]:
                    raise ValueError("writer did not commit session completeness")
                footer = True
            elif begin is not None and kind in {"owner", "piece", "backing", "edge"}:
                if len(records) >= checked_add(
                    checked_mul(header["slots"], 2), header["descriptors"], header["edges"]
                ):
                    raise ValueError("retained snapshot record bound exceeded")
                records.append(row)
            else:
                raise ValueError("unexpected record outside snapshot")
            count += 1
            total += len(raw)
            if (
                len(raw) > header["line_bytes"]
                or count > header["record_limit"]
                or total > header["byte_limit"]
            ):
                raise ValueError("capture exceeds declared writer/parser limits")
            digest = checksum(raw, digest)
        if header is None or not footer:
            raise ValueError(
                "missing final session commit; external termination remains incomplete"
            )
    except (OSError, ValueError) as error:
        errors.append(str(error))
    transport = footer and not errors
    direct = bool(summaries) and all(s["direct_complete"] for s in summaries)
    funding = bool(summaries) and all(s["funding_complete"] for s in summaries)
    return {
        "schema": "terminal-ownership-assessment/1",
        "capture": str(Path(path)),
        "transport_complete": transport,
        "transport_errors": errors,
        "direct_ownership_complete": direct,
        "funding_complete": funding,
        "capture_complete": transport and direct and funding,
        "completed_calls": calls,
        "snapshots": summaries,
        "qualification": (
            "Selected instrumented sort ownership only; checksum is transport integrity, "
            "not authenticity or historical funding replay. No query completion, "
            "query-wide ownership, RSS or native workload-fit claim."
        ),
    }
