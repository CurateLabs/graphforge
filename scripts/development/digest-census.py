#!/usr/bin/env python3
"""Structural SHA producer/delegate census; source digests bind every entry."""

import argparse
import collections
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys

METHOD_SOURCE = Path(__file__).read_bytes()
PARSER = argparse.ArgumentParser(description=__doc__)
PARSER.add_argument("--repo", type=Path, required=True, help="Repository to census; read only.")
PARSER.add_argument(
    "--output",
    type=Path,
    required=True,
    help="Output directory for generated evidence; never committed.",
)
PARSER.add_argument(
    "--overrides", type=Path, default=Path(__file__).with_name("digest-census-overrides.json")
)
PARSER.add_argument(
    "--allow-incomplete",
    action="store_true",
    help=(
        "Write explicit review gaps without a failing exit status; never claim exhaustive evidence."
    ),
)
ARGS = PARSER.parse_args()
ROOT = ARGS.repo.resolve()
OUTPUT = ARGS.output.resolve()
if OUTPUT.is_relative_to(ROOT):
    PARSER.error("--output must be outside --repo; raw evidence is never committed")
OUTPUT.mkdir(parents=True, exist_ok=True)
OVERRIDE_SOURCE = ARGS.overrides.read_bytes()
OVERRIDES = json.loads(OVERRIDE_SOURCE)
ROLE_CODES = {
    "durable_artifact_or_trust_boundary": "a",
    "durable_artifact": "a",
    "contract_identity": "b",
    "optional_evidence": "c",
    "control_authentication": "control_authentication",
    "portable_authentication": "portable_authentication",
    "caller_selected": "caller_selected",
    "primitive_implementation": "primitive_implementation",
    "producer_delegate": "producer_delegate",
}
REVIEW = {(x["path"], x["function"]): x for x in OVERRIDES["function_overrides"]}
NONPRODUCERS = {x["name"]: x for x in OVERRIDES["nonproducer_symbols"]}
LIMITATIONS = [
    "Structural source inventory, not compiler AST or runtime hash-pass count.",
    (
        "cfg(test) items, external test-module descendants and conditional"
        " test statement blocks are excluded; feature-gated production "
        "code remains represented."
    ),
    (
        "Macro bodies are source sites; expansion multiplicity and dynamic"
        " execution frequency are not inferred."
    ),
    (
        "Generic/type-inferred crypto producers and unknown algorithms are"
        " emitted as residual review gaps rather than silently classified."
    ),
    (
        "A semantic override pins the original UTF-8 source from each production "
        "fn keyword through its closing brace, in source order. Missing or changed "
        "body pins fail closed; reviewers check actual input bytes and consumers "
        "before updating pins. Parent attributes and dependencies are covered by "
        "whole-source hashes, but not by this function-level semantic staleness guard."
    ),
]
FILES = sorted(ROOT.glob("crates/*/src/**/*.rs"))
# Mask literals/comments while preserving offsets/newlines for bounded source ranges.
LEXRAW = re.compile(r'(?:br|r)(\#*)"')
LEXCHAR = re.compile(r"(?:b)?'(?:\\(?:u\{[0-9A-Fa-f_]+\}|x[0-9A-Fa-f]{2}|.)|[^'\\\n])'")


def mask(text):
    out = list(text)
    i = 0

    def blank(start, end):
        for n in range(start, end):
            if out[n] != "\n":
                out[n] = " "

    while i < len(text):
        start = i
        if text.startswith("//", i):
            end = text.find("\n", i)
            i = len(text) if end < 0 else end
            blank(start, i)
            continue
        if text.startswith("/*", i):
            depth = 1
            i += 2
            while i < len(text) and depth:
                if text.startswith("/*", i):
                    depth += 1
                    i += 2
                elif text.startswith("*/", i):
                    depth -= 1
                    i += 2
                else:
                    i += 1
            blank(start, i)
            continue
        raw = LEXRAW.match(text, i)
        if raw:
            ending = '"' + raw.group(1)
            end = text.find(ending, raw.end())
            i = len(text) if end < 0 else end + len(ending)
            blank(start, i)
            continue
        if text[i] == '"' or text.startswith('b"', i):
            i += 2 if text.startswith('b"', i) else 1
            while i < len(text):
                if text[i] == "\\":
                    i += 2
                elif text[i] == '"':
                    i += 1
                    break
                else:
                    i += 1
            blank(start, min(i, len(text)))
            continue
        char = LEXCHAR.match(text, i)
        if char:
            i = char.end()
            blank(start, i)
            continue
        i += 1
    return "".join(out)


def braces(text):
    stack = []
    pairs = {}
    for i, c in enumerate(text):
        if c == "{":
            stack.append(i)
        elif c == "}" and stack:
            pairs[stack.pop()] = i + 1
    return pairs


def attr_before(text, pos):
    prefix = text[max(0, pos - 4096) : pos]
    m = re.search(
        r"((?:\s*#\[[^\]]*\])+\s*(?:(?:pub(?:\([^)]*\))?|unsafe|async|const)\s+)*)$", prefix
    )
    return m.group(1) if m else ""


def only_test(attr):
    if re.search(r"#\[\s*(?:test|rstest(?:::rstest)?)\b", attr):
        return True

    # Evaluate cfg expressions with test=false; unknown features/targets remain
    # both true/false. Exclude only if no production configuration can satisfy it.
    def split_args(body):
        result = []
        start = depth = 0
        for i, c in enumerate(body):
            if c == "(":
                depth += 1
            elif c == ")":
                depth -= 1
            elif c == "," and depth == 0:
                result.append(body[start:i].strip())
                start = i + 1
        result.append(body[start:].strip())
        return [x for x in result if x]

    def cfg_values(expr):
        expr = expr.strip()
        if expr == "test":
            return {False}
        m = re.fullmatch(r"(all|any|not)\s*\((.*)\)", expr, re.S)
        if not m:
            return {True, False}
        values = [cfg_values(x) for x in split_args(m.group(2))]
        if m.group(1) == "not":
            return {not x for x in values[0]} if len(values) == 1 else {True, False}
        result = {m.group(1) == "all"}
        for value in values:
            result = {a and b if m.group(1) == "all" else a or b for a in result for b in value}
        return result

    for m in re.finditer(r"\bcfg\s*\(", attr):
        start = m.end()
        depth = 1
        end = start
        while end < len(attr) and depth:
            if attr[end] == "(":
                depth += 1
            elif attr[end] == ")":
                depth -= 1
            end += 1
        if depth == 0 and cfg_values(attr[start : end - 1]) == {False}:
            return True
    return False


parsed = {}
test_roots = set()
for path in FILES:
    original = path.read_bytes().decode("utf-8")
    code = mask(original)
    pairs = braces(code)
    functions = []
    excluded = []
    # Also remove conditional test statement blocks, not only test items.
    for m in re.finditer(r"#\[\s*cfg\s*\(\s*test\s*\)\s*\]\s*\{", code):
        b = m.end() - 1
        excluded.append((m.start(), pairs.get(b, len(code))))
    # Test-only item blocks can wrap impls, structs or thread_local macros.
    for m in re.finditer(r"#\[\s*cfg\s*\([^\]]*\)\s*\]", code):
        if not only_test(m.group()):
            continue
        tail = code[m.end() :]
        if not re.match(
            (
                "\\s*(?:pub(?:\\([^)]*\\))?\\s+)?(?:unsafe\\s+)?(?:fn|impl|trait|mod|st"
                "ruct|enum|const|static|type|thread_local!)\\b"
            ),
            tail,
        ):
            continue
        depth_paren = depth_bracket = 0
        for n, c in enumerate(tail, m.end()):
            if c == "(":
                depth_paren += 1
            elif c == ")":
                depth_paren -= 1
            elif c == "[":
                depth_bracket += 1
            elif c == "]":
                depth_bracket -= 1
            elif not depth_paren and not depth_bracket and c in ";{":
                excluded.append((m.start(), pairs.get(n, n + 1) if c == "{" else n + 1))
                break
    # cfg(test) external modules mark their out-of-file descendants.
    for m in re.finditer(r"\bmod\s+(\w+)\s*([;{])", code):
        attr = attr_before(code, m.start())
        if only_test(attr):
            if m.group(2) == "{":
                b = m.end() - 1
                excluded.append((m.start(), pairs.get(b, len(code))))
            else:
                stem = (
                    path.parent if path.stem in ("lib", "main", "mod") else path.parent / path.stem
                )
                test_roots.add(stem / (m.group(1) + ".rs"))
                test_roots.add(stem / m.group(1))
    for m in re.finditer(r"\bfn\s+(\w+)\b", code):
        depth_paren = depth_bracket = 0
        b = None
        for offset, c in enumerate(code[m.end() :], m.end()):
            if c == "(":
                depth_paren += 1
            elif c == ")":
                depth_paren -= 1
            elif c == "[":
                depth_bracket += 1
            elif c == "]":
                depth_bracket -= 1
            elif not depth_paren and not depth_bracket and c in ";{":
                b = offset
                break
        if b is None or code[b] != "{":
            continue
        end = pairs.get(b, len(code))
        attrs = attr_before(code, m.start())
        functions.append((m.start(), end, m.group(1)))
        if only_test(attrs):
            excluded.append((m.start(), end))
    parsed[path] = (original, code, functions, excluded)
entries = []
test_entries = []
source_digests = {}
producer_functions = set()
for path, (original, code, functions, excluded) in parsed.items():
    relative = str(path.relative_to(ROOT))
    source_digests[relative] = hashlib.sha256(original.encode()).hexdigest()
    external_test = any(path == root or root in path.parents for root in test_roots) or bool(
        re.search(r"#!\[\s*cfg\s*\(\s*test\s*\)", code)
    )
    aliases = {
        m.group(2): m.group(1)
        for m in re.finditer(
            (
                "\\b(ArtifactSha256|ContractSha256|ControlSha256|EvidenceSha256|PortableSha256|Pay"
                "loadSha256|ObservedSha256)\\s+as\\s+(\\w+)"
            ),
            code,
        )
    }
    crypto_aliases = set(re.findall(r"\b\w*Sha(?:256|512|384|224|1)\s+as\s+(\w+)", code))
    local_producer_re = re.compile(
        r"\b(?P<name>"
        + ("|".join(sorted(crypto_aliases)) + "|" if crypto_aliases else "")
        + (
            "[A-Za-z_][A-Za-z_0-"
            "9]*Sha256|Sha256)\\s*::\\s*(?P<method>new|digest|default|for_domain)"
            "\\b(?P<invoked>\\s*\\()?"
        )
    )
    for m in local_producer_re.finditer(code):
        function = next(
            (name for start, end, name in reversed(functions) if start <= m.start() < end), "<item>"
        )
        symbol = m.group("name")
        primitive = aliases.get(symbol, symbol)
        role = {
            "ArtifactSha256": "durable_artifact",
            "PayloadSha256": "durable_artifact",
            "ContractSha256": "contract_identity",
            "ControlSha256": "control_authentication",
            "PortableSha256": "portable_authentication",
            "EvidenceSha256": "optional_evidence",
        }.get(primitive, "needs_input_review")
        if (
            primitive in ("ObservedSha256", "PayloadSha256")
            and m.group("method") == "for_domain"
            and m.group("invoked") is not None
            and re.match(
                r"\s*(?:\w+\s*::\s*)*HashDomain\s*::\s*PortableAuthentication\s*,?\s*\)",
                code[code.index("(", m.start()) + 1 :],
            )
        ):
            role = "portable_authentication"
        line = original.count("\n", 0, m.start()) + 1
        record = {
            "path": relative,
            "line": line,
            "function": function,
            "symbol": symbol,
            "resolved_alias": primitive,
            "form": m.group("method"),
            "constructor_reference": m.group("invoked") is None,
            "role_by_explicit_type": role,
            "text": original.splitlines()[line - 1].strip(),
            "offset": m.start(),
        }
        if external_test or any(start <= m.start() < end for start, end in excluded):
            test_entries.append(record)
        else:
            entries.append(record)
            producer_functions.add(function)
# Helpers that hold producers have a distinct delegation population. Never sum
# delegates with constructors to infer runtime hash passes.
delegates = []
for path, (original, code, functions, excluded) in parsed.items():
    external_test = any(path == root or root in path.parents for root in test_roots) or bool(
        re.search(r"#!\[\s*cfg\s*\(\s*test\s*\)", code)
    )
    if external_test:
        continue
    for m in re.finditer(r"\b([A-Za-z_][A-Za-z_0-9]*)\s*\(", code):
        name = m.group(1)
        if (
            name not in producer_functions
            or code[max(0, m.start() - 4) : m.start()].strip() == "fn"
        ):
            continue
        if any(start <= m.start() < end for start, end in excluded):
            continue
        line = original.count("\n", 0, m.start()) + 1
        caller = next(
            (name for start, end, name in reversed(functions) if start <= m.start() < end), "<item>"
        )
        if name in ("new", "default"):
            continue
        if (
            not re.search(r"(hash|digest|sha256|fingerprint|checksum)", name, re.I)
            and name != "shard_set_identity"
        ):
            continue
        delegates.append(
            {
                "path": str(path.relative_to(ROOT)),
                "line": line,
                "caller": caller,
                "delegate": name,
                "text": original.splitlines()[line - 1].strip(),
                "offset": m.start(),
            }
        )

# Bind stored input/consumer decisions to the exact function source reviewed.
# Hash original text, including literal digest inputs that the lexer masks. Match
# every production definition for same-name methods rather than picking one.
reviewed_functions = collections.defaultdict(list)
for path, (original, code, functions, excluded) in parsed.items():
    external_test = any(path == root or root in path.parents for root in test_roots) or bool(
        re.search(r"#!\[\s*cfg\s*\(\s*test\s*\)", code)
    )
    if external_test:
        continue
    for start, end, fn in functions:
        if any(a <= start < b for a, b in excluded):
            continue
        key = (str(path.relative_to(ROOT)), fn)
        reviewed_functions[key].append(
            {
                "line": original.count("\n", 0, start) + 1,
                "sha256": hashlib.sha256(original[start:end].encode()).hexdigest(),
            }
        )
gaps = []
for key, item in REVIEW.items():
    actual = [body["sha256"] for body in reviewed_functions.get(key, [])]
    expected = item.get("function_bodies_sha256")
    if not actual:
        kind = "stale_override"
    elif (
        not isinstance(expected, list)
        or not expected
        or any(
            not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value)
            for value in expected
        )
    ):
        kind = "reviewed_input_unpinned"
    elif actual != expected:
        kind = "reviewed_input_changed"
    else:
        continue
    gaps.append(
        {
            "kind": kind,
            "path": item["path"],
            "function": item["function"],
            "reviewed_function_bodies_sha256": expected,
            "current_function_bodies_sha256": actual,
        }
    )
revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
diff_sha = hashlib.sha256(subprocess.check_output(["git", "diff", "HEAD"], cwd=ROOT)).hexdigest()
method_sha = hashlib.sha256(METHOD_SOURCE).hexdigest()
overrides_sha = hashlib.sha256(OVERRIDE_SOURCE).hexdigest()
d = {
    "source_revision": revision,
    "working_diff_sha256": diff_sha,
    "method_sha256": method_sha,
    "files": source_digests,
    "production_constructors": entries,
    "excluded_tests": test_entries,
    "delegate_candidates": delegates,
}
roles = {key: (x["role"], x["input_contract"]) for key, x in REVIEW.items()}
rows = []
for x in entries:
    path = x["path"]
    fn = x["function"]
    k = (path, fn)
    role, reason = roles.get(
        k, ("unresolved", "No stored semantic review of actual input bytes and consumer.")
    )
    if role == "unresolved":
        gaps.append(
            {"kind": "unclassified_producer", "path": path, "line": x["line"], "function": fn}
        )
    # A producer helper can accept different bytes at separate invocations.
    if (
        path.startswith("crates/graphforge-api/src/")
        and x["form"] == "digest"
        and fn
        in (
            "generation_uuid",
            "mutation_generation_uuid",
            "assertion_generation_uuid",
            "knowledge_generation_uuid",
            "repository_sync_generation_uuid",
            "research_metadata_generation_uuid",
            "workspace_generation_uuid",
        )
    ):
        role = "durable_artifact_or_trust_boundary"
        reason = (
            "Inner participant.bytes digest supplies required durable content "
            "commitment for the outer generation identity."
        )
    if (
        path.endswith("/project_fault_oracle.rs")
        and fn == "child_manifest_bytes"
        and "graph/nodes" in x["text"]
    ):
        role = "contract_identity"
        reason = "Constant graph/nodes schema descriptor defines schema fingerprint."
    if path.endswith("/project_portable_v2.rs") and fn == "verify_bundle" and x["form"] == "digest":
        role = "contract_identity"
        reason = "PAX path text derives canonical header suffix; does not authenticate payload."
    if (
        path.endswith("/project_portable_v2_export/transport.rs")
        and fn == "expanded"
        and x["form"] == "new"
    ):
        role = "contract_identity"
        reason = (
            "Ordered already-computed entry path/length/digest metadata and "
            "tagmanifest define transport identity."
        )
    if (
        path.endswith("/project_publication/participants.rs")
        and fn == "request_metadata_with_payloads"
        and "participant.bytes" in x["text"]
    ):
        role = "durable_artifact_or_trust_boundary"
        reason = (
            "Complete participant bytes supply mandatory artifact content "
            "commitment; outer request hash uses metadata."
        )
    original = parsed[ROOT / path][0]
    lines = original.splitlines()
    line = x["line"] - 1
    row = dict(x)
    row.update(
        semantic_role=role,
        input_contract=reason,
        source_sha256=source_digests[path],
        context=lines[max(0, line - 2) : min(len(lines), line + 14)],
    )
    rows.append(row)
semantic_result = {"producers": rows}

by = {}
for x in semantic_result["producers"]:
    by.setdefault((x["path"], x["function"]), []).append(x)
rows = []
for c in d["delegate_candidates"]:
    p = c["path"]
    name = c["delegate"]
    text = c["text"]
    fn = c["caller"]
    x = dict(c)
    target = None
    role = None
    kind = None
    reason = None
    # Exclude calls to the constructor itself: already represented as application site.
    if name == "digest" and re.search(r"\b\w*Sha256\s*::\s*digest\s*\(", text):
        kind = "constructor_already_counted"
        reason = "Static Digest::digest is already in producer inventory."
    elif name == "checksum" and "corruption_checksum::checksum" in text:
        kind = "noncryptographic_checksum"
        reason = "XXH64 checksum implementation, not SHA-256."
    elif name == "checksum":
        kind = "stored_digest_getter"
        reason = (
            "Ontology handle checksum returns recorded ontology checksum "
            "string; no SHA producer invocation."
        )
    elif name == "projection_fingerprint" and p.startswith("crates/graphforge-api/"):
        kind = "stored_digest_getter"
        reason = (
            "Prepared algorithm descriptor projection_fingerprint accessor "
            "returns recorded projection identity."
        )
    elif name == "publication_fingerprint" and "manifest.publication_fingerprint()" in text:
        kind = "stored_digest_getter"
        reason = "Embedding manifest accessor returns recorded publication fingerprint."
    elif name == "fingerprint" and ".fingerprint()" in text:
        if "/branches/claim_fields.rs" in p or "/composite_transaction.rs" in p:
            target = ("crates/graphforge-core/src/canonical.rs", "fingerprint")
            role = "contract_identity"
            kind = "transitive_contract_helper"
            reason = (
                "Knowledge assertion/event/row fingerprint methods delegate "
                "canonical semantic contract identity."
            )
        else:
            kind = "stored_digest_getter"
            reason = (
                "Recorded algorithm descriptor, embedding source, composition or "
                "semantic context fingerprint accessor; no fresh SHA call."
            )
    elif name == "projection_fingerprint" and "/graphforge-bindings-" in p:
        kind = "stored_digest_getter"
        reason = "Thin binding getter returns stored analyst descriptor projection fingerprint."
    elif (name == "sha256_hex" and p.endswith("/project_generation.rs")) or (
        name == "digest_hex" and p.endswith("/project_recovery.rs")
    ):
        kind = "digest_hex_formatter"
        reason = "Helper formats already computed 32-byte digest as lowercase hex; no SHA producer."
    elif name == "hash_bytes" and p.endswith("/search_manifest.rs"):
        kind = "noncryptographic_identity_hash"
        reason = "Helper updates four FNV-style u64 states with XOR/wrapping multiply, not SHA-256."
    elif name == "digest" and "/repository/" in p:
        kind = "digest_syntax_validation"
        reason = (
            "repository::digest validates textual declared SHA syntax, does not compute digest."
        )
    elif name == "digest" and "/research_proposals/" in p:
        kind = "digest_syntax_validation"
        reason = (
            "selection::digest parses optional recorded hexadecimal baseline "
            "identity, does not compute digest."
        )
    elif (p, name) in by:
        target = (p, name)
        kind = "same_file_producer_function"
        reason = "Actual producer function is defined in the caller source module."
    elif name == "fingerprint":
        if "/project_portable_v2_subset.rs" in p:
            target = (
                "crates/graphforge-storage/src/project_portable_v2_selection.rs",
                "fingerprint",
            )
        else:
            target = ("crates/graphforge-core/src/canonical.rs", "fingerprint")
        kind = "imported_contract_helper"
        role = "contract_identity"
        reason = (
            "Imported canonical fingerprint helper computes domain-separated "
            "canonical semantic identity; knowledge macros are delegates."
        )
    elif name == "hex_sha256" and "/uuid_membership/" in p:
        target = ("crates/graphforge-storage/src/uuid_membership/topology_delta.rs", "hex_sha256")
        kind = "parent_imported_producer"
        role = "control_authentication"
        reason = "Parent topology_delta helper authenticates manifest or receipt control body."
        if fn in ("finish_block", "finish_v4_tombstone_block", "install_construction_bytes"):
            role = "durable_artifact_or_trust_boundary"
            reason = "Same helper receives ordinal artifact/block payload bytes on this write edge."
        elif "topology_delta_sha256:" in text:
            role = "contract_identity"
            reason = "Same helper receives constant rebuild/migration contract marker on this edge."
    elif name == "sha256" and "/graph_construction/" in p:
        target = ("crates/graphforge-storage/src/graph_construction.rs", "sha256")
        kind = "parent_imported_producer"
        role = "control_authentication"
        reason = "Parent SHA helper authenticates canonical construction control/receipt body."
        if fn in ("chunk_key_name", "new", "shape_canonical_inner", "shape_receipt_name"):
            role = "contract_identity"
            reason = (
                "Same helper receives logical authority/name/key text to derive "
                "stable namespace on this edge."
            )
    elif name == "digest_sha256":
        target = ("crates/graphforge-portable-oci/src/lib.rs", "digest_sha256")
        kind = "imported_trust_boundary_helper"
        role = "durable_artifact_or_trust_boundary"
        reason = (
            "Explicit OCI publication/import authenticates complete "
            "layer/config/manifest transport bytes."
        )
    elif name == "read_graph_object_by_digest_file_counted_in_domain":
        target = ("crates/graphforge-storage/src/graph_object_store.rs", name)
        kind = "qualified_or_parent_imported_producer"
        original, code, _, _ = parsed[ROOT / p]
        opening = code.index("(", c["offset"])
        depth = 1
        end = opening + 1
        while end < len(code) and depth:
            depth += (code[end] == "(") - (code[end] == ")")
            end += 1
        call = code[opening:end]
        if re.search(r"HashDomain\s*::\s*ControlAuthentication\b", call):
            role = "control_authentication"
            reason = "Caller explicitly selects bounded manifest/route control authority bytes."
        elif re.search(r"HashDomain\s*::\s*PortableAuthentication\b", call):
            role = "portable_authentication"
            reason = "Caller explicitly selects portable archive/member/payload authentication."
        elif re.search(r"HashDomain\s*::\s*ArtifactPayload\b", call):
            role = "durable_artifact_or_trust_boundary"
            reason = "Caller explicitly selects artifact payload authentication."
        else:
            role = "caller_selected"
            reason = "Caller forwards its domain; producer role remains caller selected."
    elif name == "descriptor_projection_fingerprint":
        target = (
            "crates/graphforge-exec/src/algorithm_graph.rs",
            "descriptor_projection_fingerprint",
        )
        kind = "resolved_algorithm_method"
        role = "contract_identity"
        reason = (
            "Algorithm graph method computes descriptor projection identity "
            "from topology/property vector commitments."
        )
    else:
        targets = {
            "shard_set_identity": (
                "crates/graphforge-storage/src/adjacency/codec.rs",
                "shard_set_identity",
            ),
            "shape_authority_sha256": (
                "crates/graphforge-storage/src/graph_construction.rs",
                "shape_authority_sha256",
            ),
            "inventory_authority_sha256": (
                "crates/graphforge-storage/src/graph_construction_encoding.rs",
                "inventory_authority_sha256",
            ),
            "logical_path_digest": (
                "crates/graphforge-storage/src/graph_manifest.rs",
                "logical_path_digest",
            ),
            "identity_digest": (
                "crates/graphforge-storage/src/research_versions.rs",
                "identity_digest",
            ),
            "identity_map_authority_sha256": (
                "crates/graphforge-storage/src/storage_attribution.rs",
                "identity_map_authority_sha256",
            ),
            "canonical_digest": ("crates/graphforge-discovery/src/lib.rs", "canonical_digest"),
            "domain_digest": (
                "crates/graphforge-ontology/src/composition/canonical.rs",
                "domain_digest",
            ),
            "participant_content_sha256": (
                "crates/graphforge-api/src/knowledge/ledger.rs",
                "participant_content_sha256",
            ),
            "category_map_authority_sha256": (
                "crates/graphforge-storage/src/storage_attribution.rs",
                "category_map_authority_sha256",
            ),
            "hash_regular_file": (
                "crates/graphforge-storage/src/graph_object_store.rs",
                "hash_regular_file",
            ),
            "delete_request_digest_values": (
                "crates/graphforge-storage/src/project_checkpoints.rs",
                "delete_request_digest_values",
            ),
            "revert_request_digest": (
                "crates/graphforge-storage/src/project_checkpoints/restoration.rs",
                "revert_request_digest",
            ),
            "digest_definition_tree": (
                "crates/graphforge-api/src/repository/configuration.rs",
                "digest_definition_tree",
            ),
            "analyze_projection_fingerprint": (
                "crates/graphforge-exec/src/algorithm_analyze.rs",
                "analyze_projection_fingerprint",
            ),
            "paths_projection_fingerprint": (
                "crates/graphforge-exec/src/algorithm_paths.rs",
                "paths_projection_fingerprint",
            ),
            "publication_fingerprint": (
                "crates/graphforge-storage/src/research_versions/branches.rs",
                "publication_fingerprint",
            ),
        }
        target = targets.get(name)
        if target:
            kind = "qualified_or_parent_imported_producer"
            reason = "Verified source-module helper reference with exact producer target."
    if target:
        ps = by.get(target, [])
        if not role:
            rr = {a["semantic_role"] for a in ps}
            role = next(iter(rr)) if len(rr) == 1 else "caller_selected"
        x.update(
            target_function=target[0] + "::" + target[1],
            target_producers=[f"{a['path']}:{a['line']}" for a in ps],
            semantic_role=role,
        )
    x.update(
        resolution=kind or "unresolved",
        input_contract=reason or "Requires further source name resolution.",
        count_as_application_producer=False,
    )
    rows.append(x)

semantic_edges = rows
for row in semantic_edges:
    if row["resolution"] == "unresolved":
        gaps.append(
            {
                "kind": "unresolved_helper",
                "path": row["path"],
                "line": row["line"],
                "callee": row["delegate"],
            }
        )
producers = semantic_result["producers"]
for row in producers:
    row["class"] = ROLE_CODES.get(row["semantic_role"], row["semantic_role"])
for row in semantic_edges:
    if "semantic_role" in row:
        row["class"] = ROLE_CODES.get(row["semantic_role"], row["semantic_role"])

# Residual audit is broader than the original constructor query. It lists every
# crypto/type token and every hash/digest/checksum/fingerprint invocation, including
# helper callbacks. Unknown constructors/algorithms are explicit close blockers.
function_roles = {}
for x in producers:
    function_roles.setdefault((x["path"], x["function"]), set()).add(x["class"])
for edge in semantic_edges:
    if "class" in edge:
        function_roles.setdefault((edge["path"], edge["caller"]), set()).add(edge["class"])
function_sources = {}
for path, (_original, code, functions, excluded) in parsed.items():
    relative = str(path.relative_to(ROOT))
    external_test = any(path == root or root in path.parents for root in test_roots) or bool(
        re.search(r"#!\[\s*cfg\s*\(\s*test\s*\)", code)
    )
    if external_test:
        continue
    for start, end, fn in functions:
        if not any(a <= start < b for a, b in excluded):
            function_sources.setdefault((relative, fn), []).append(code[start:end])
by_name = collections.defaultdict(list)
for key in function_sources:
    by_name[key[1]].append(key)
helper_pattern = re.compile(
    r"\b(\w*(?:hash|digest|fingerprint|checksum)\w*|shard_set_identity)\s*\(", re.I
)
# Propagate known producer contracts through named helpers; preserve mixed roles.
for _ in range(len(function_sources) + 1):
    changed = False
    for key, bodies in function_sources.items():
        found = set()
        for body in bodies:
            for call in helper_pattern.finditer(body):
                name = call.group(1)
                if re.search(r"\bfn\s*$", body[: call.start()]):
                    continue
                local = (key[0], name)
                targets = [local] if local in function_sources else by_name.get(name, [])
                for target in targets:
                    found.update(function_roles.get(target, set()))
        if found and not found.issubset(function_roles.get(key, set())):
            function_roles.setdefault(key, set()).update(found)
            changed = True
    if not changed:
        break

edge_index = {(x["path"], x["line"], x["delegate"]): x for x in semantic_edges}
producer_index = {(x["path"], x["line"], x["symbol"], x["form"]) for x in producers}
extra_aliases = {
    a
    for _, code, _, _ in parsed.values()
    for a in re.findall(r"\b\w*Sha(?:256|512|384|224|1)\s+as\s+(\w+)", code)
}
crypto_pattern = re.compile(
    (
        "\\b(?:\\w*Sha(?:1|224|256|384|512)\\w*|sha[123]|Digest|\\w*Blake[23]\\"
        "w*|blake[23]|Md5|md5|Hmac|hmac|openssl|ring"
    )
    + ("|" + "|".join(sorted(extra_aliases)) if extra_aliases else "")
    + r")\b"
)
residual = []
typed_callbacks = []
broad_calls = []
for path, (original, code, functions, excluded) in parsed.items():
    relative = str(path.relative_to(ROOT))
    external_test = any(path == root or root in path.parents for root in test_roots) or bool(
        re.search(r"#!\[\s*cfg\s*\(\s*test\s*\)", code)
    )
    lines = original.splitlines()

    def function_at(pos, functions=functions):
        return next((n for a, b, n in reversed(functions) if a <= pos < b), "<item>")

    def test_at(pos, external_test=external_test, excluded=excluded):
        return external_test or any(a <= pos < b for a, b in excluded)

    # Type-state references account update/finalize callback owners independently
    # from producer constructors. A callback does not create another hash pass.
    for token in crypto_pattern.finditer(code):
        pos = token.start()
        name = token.group()
        line = original.count("\n", 0, pos) + 1
        fn = function_at(pos)
        after = code[token.end() :]
        kind = "type_or_recorded_identity_reference"
        if test_at(pos):
            kind = "excluded_test_reference"
        elif name in (
            "sha1",
            "sha3",
            "Blake2",
            "Blake3",
            "blake2",
            "blake3",
            "Md5",
            "md5",
            "Hmac",
            "hmac",
            "openssl",
            "ring",
        ) and (name not in ("ring", "openssl") or re.match(r"\s*::", after)):
            kind = "unreviewed_crypto_algorithm"
            gaps.append({"kind": kind, "path": relative, "line": line, "symbol": name})
        elif re.match(
            r"\s*::\s*(?:new|digest|default|for_domain)\b(?!\s*::)|\s*::\s*\w+\s*\(", after
        ):
            method = re.match(r"\s*::\s*(\w+)\b", after).group(1)
            if (relative, line, name, method) in producer_index:
                kind = "producer_already_classified"
            else:
                kind = "unmatched_crypto_invocation"
                gaps.append(
                    {"kind": kind, "path": relative, "line": line, "symbol": name, "method": method}
                )
        elif re.search(r"&\s*mut\s*$", code[max(0, pos - 20) : pos]) or re.search(
            r"\b(?:dyn|impl)\b[^;{}]*$", code[max(0, pos - 80) : pos]
        ):
            kind = "typed_crypto_state_or_callback"
            typed_callbacks.append(
                {
                    "path": relative,
                    "line": line,
                    "function": fn,
                    "symbol": name,
                    "source_sha256": source_digests[relative],
                }
            )
        residual.append(
            {
                "path": relative,
                "line": line,
                "function": fn,
                "symbol": name,
                "classification": kind,
                "text": lines[line - 1].strip(),
            }
        )
    for call in helper_pattern.finditer(code):
        pos = call.start()
        name = call.group(1)
        line = original.count("\n", 0, pos) + 1
        fn = function_at(pos)
        item = {
            "path": relative,
            "line": line,
            "caller": fn,
            "callee": name,
            "source_sha256": source_digests[relative],
            "text": lines[line - 1].strip(),
        }
        if test_at(pos):
            item["classification"] = "excluded_test_invocation"
        elif (qualified := re.search(r"(\w+)\s*::\s*$", code[max(0, pos - 80) : pos])) and (
            relative,
            line,
            qualified.group(1),
            name,
        ) in producer_index:
            item["classification"] = "constructor_already_counted"
        elif name in NONPRODUCERS:
            item["classification"] = NONPRODUCERS[name]["classification"]
            item["input_contract"] = NONPRODUCERS[name]["input_contract"]
        elif re.search(r"\bfn\s*$", code[max(0, pos - 8) : pos]):
            item["classification"] = "function_declaration"
        elif (relative, line, name) in edge_index:
            edge = edge_index[(relative, line, name)]
            item["classification"] = edge["resolution"]
            item["classes"] = [edge["class"]] if "class" in edge else []
        else:
            local = (relative, name)
            targets = [local] if local in function_sources else by_name.get(name, [])
            known_roles = (
                set().union(*(function_roles.get(t, set()) for t in targets)) if targets else set()
            )
            if known_roles:
                item["classification"] = "transitive_source_helper"
                item["classes"] = sorted(known_roles)
                item["candidate_definitions"] = [f"{p}::{f}" for p, f in targets]
            elif targets:
                # These known source bodies contain no recognized SHA producer or edge.
                # Keep them visible for independent residual review, never infer a runtime pass.
                item["classification"] = "source_helper_without_sha_reachability"
                item["candidate_definitions"] = [f"{p}::{f}" for p, f in targets]
            else:
                item["classification"] = "unmatched_helper"
                gaps.append(
                    {"kind": "unmatched_helper", "path": relative, "line": line, "callee": name}
                )
        broad_calls.append(item)

# Thin Python/Node production bindings are independently audited for crypto
# calls. Recorded SHA fields and Rust pass-through references are evidence,
# never fallback hash engines or extra Rust producer sites.
binding_residual = []
binding_sources = {}
for relative_root in (
    "crates/graphforge-bindings-py/python",
    "crates/graphforge-bindings-node/lib",
):
    source_root = ROOT / relative_root
    if not source_root.exists():
        continue
    for source in sorted(source_root.rglob("*")):
        if source.suffix not in (".py", ".js", ".ts", ".mjs", ".cjs"):
            continue
        relative = str(source.relative_to(ROOT))
        content = source.read_bytes()
        binding_sources[relative] = hashlib.sha256(content).hexdigest()
        for line, text in enumerate(content.decode().splitlines(), 1):
            if not re.search(
                r"hash|hashlib|sha256|createHash|crypto\.subtle|md5|blake", text, re.I
            ):
                continue
            invocation = re.search(
                r"(?:hashlib\.\w+|createHash|crypto\.subtle\.digest|sha256|md5|blake\w*)\s*\(",
                text,
                re.I,
            )
            classification = (
                "unreviewed_binding_crypto_invocation"
                if invocation
                else "recorded_identity_or_rust_passthrough_reference"
            )
            if invocation:
                gaps.append({"kind": classification, "path": relative, "line": line})
            binding_residual.append(
                {
                    "path": relative,
                    "line": line,
                    "classification": classification,
                    "text": text.strip(),
                    "source_sha256": binding_sources[relative],
                }
            )

# Audit manifests for crypto implementations outside the SHA type spelling.
dependencies = []
manifest_sources = {}
for manifest in [*sorted(ROOT.glob("crates/*/Cargo.toml")), ROOT / "Cargo.toml"]:
    if not manifest.exists():
        continue
    content = manifest.read_bytes()
    manifest_sources[str(manifest.relative_to(ROOT))] = hashlib.sha256(content).hexdigest()
    for line, text in enumerate(content.decode().splitlines(), 1):
        if re.match(r"\s*(?:sha[123]|blake[23]|md5|hmac|ring|openssl|digest)\s*=", text):
            dependencies.append(
                {"path": str(manifest.relative_to(ROOT)), "line": line, "text": text.strip()}
            )
            if not re.match(r"\s*sha2\s*=", text):
                gaps.append(
                    {
                        "kind": "unreviewed_crypto_dependency",
                        "path": str(manifest.relative_to(ROOT)),
                        "line": line,
                    }
                )
if Path(__file__).read_bytes() != METHOD_SOURCE:
    gaps.append({"kind": "method_changed_during_census"})
if ARGS.overrides.read_bytes() != OVERRIDE_SOURCE:
    gaps.append({"kind": "semantic_overrides_changed_during_census"})
changed_sources = [
    path
    for path, digest in source_digests.items()
    if hashlib.sha256((ROOT / path).read_bytes()).hexdigest() != digest
]
for path in changed_sources:
    gaps.append({"kind": "source_changed_during_census", "path": path})
for path, digest in binding_sources.items():
    if hashlib.sha256((ROOT / path).read_bytes()).hexdigest() != digest:
        gaps.append({"kind": "binding_source_changed_during_census", "path": path})
for path, digest in manifest_sources.items():
    if hashlib.sha256((ROOT / path).read_bytes()).hexdigest() != digest:
        gaps.append({"kind": "manifest_source_changed_during_census", "path": path})
identity = {
    "source_revision": revision,
    "working_diff_sha256": diff_sha,
    "method_sha256": method_sha,
    "overrides_sha256": overrides_sha,
    "source_files": source_digests,
    "rust_source_inventory_sha256": hashlib.sha256(
        json.dumps(source_digests, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest(),
    "limitations": LIMITATIONS,
    "static_sites_are_runtime_passes": False,
    "thin_binding_source_files": binding_sources,
    "dependency_manifest_source_files": manifest_sources,
    "review_gap_count": len(gaps),
}
summary = {
    "source_files": len(FILES),
    "application_producer_sites": sum(
        x["class"] not in ("primitive_implementation", "producer_delegate") for x in producers
    ),
    "producer_classes": dict(collections.Counter(x["class"] for x in producers)),
    "excluded_test_constructors": len(test_entries),
    "helper_edges": dict(collections.Counter(x["resolution"] for x in semantic_edges)),
    "residual_crypto_tokens": dict(collections.Counter(x["classification"] for x in residual)),
    "broad_helper_invocations": dict(collections.Counter(x["classification"] for x in broad_calls)),
    "review_gaps": dict(collections.Counter(x["kind"] for x in gaps)),
}
outputs = {
    "producers.json": {
        "identity": identity,
        "producers": producers,
        "excluded_test_constructors": test_entries,
    },
    "delegates.json": {"identity": identity, "edges": semantic_edges},
    "residual-audit.json": {
        "identity": identity,
        "crypto_tokens": residual,
        "typed_callbacks": typed_callbacks,
        "helper_invocations": broad_calls,
        "crypto_dependencies": dependencies,
    },
    "review-gaps.json": {"identity": identity, "gaps": gaps},
    "binding-residual.json": {"identity": identity, "references": binding_residual},
    "reviewed-functions.json": {
        "identity": identity,
        "functions": [
            {"path": path, "function": fn, "bodies": bodies}
            for (path, fn), bodies in sorted(reviewed_functions.items())
            if (path, fn) in REVIEW
        ],
    },
    "summary.json": {"identity": identity, "summary": summary},
}
for name, value in outputs.items():
    (OUTPUT / name).write_text(json.dumps(value, indent=2) + "\n")
print(json.dumps(summary, indent=2))
if gaps and not ARGS.allow_incomplete:
    sys.exit(1)
