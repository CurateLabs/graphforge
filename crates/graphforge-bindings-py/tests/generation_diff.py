"""Real PyO3 execution for the Rust-owned semantic generation diff (#804)."""

from __future__ import annotations

import io

from pyarrow import ipc


def check_generation_diff() -> None:
    import graphforge as gf

    forge = gf.GraphForge()
    first = forge.add_node("Person", name="Grace")
    source = forge.committed_generation_identity()
    second = forge.add_node("Person", name="Ada")
    forge.add_edge(first, "KNOWS", second, since=2026)
    target = forge.committed_generation_identity()

    request = {
        "source_generation_uuid": source["generation_uuid"],
        "source_manifest_sha256": source["manifest_sha256"],
        "target_generation_uuid": target["generation_uuid"],
        "target_manifest_sha256": target["manifest_sha256"],
    }
    result = forge.diff_committed_generations(**request)
    retry = forge.diff_committed_generations(**request)
    assert result["kind"] == "ready"
    assert result["source"] == source
    assert result["target"] == target
    assert result["checkpoint_binding"] == retry["checkpoint_binding"]
    for name in (
        "added_nodes",
        "removed_nodes",
        "modified_nodes",
        "added_edges",
        "removed_edges",
        "modified_edges",
    ):
        assert result[name]["ipc"] == retry[name]["ipc"]
        table = ipc.open_stream(io.BytesIO(result[name]["ipc"])).read_all()
        assert table.num_rows == result[name]["row_count"]
    assert result["added_nodes"]["row_count"] == 1
    assert result["added_edges"]["row_count"] == 1

    forge.execute("MATCH (n) SET n.active = true")
    final_target = forge.committed_generation_identity()
    ladder = forge.diff_committed_generations(
        source_generation_uuid=target["generation_uuid"],
        source_manifest_sha256=target["manifest_sha256"],
        target_generation_uuid=final_target["generation_uuid"],
        target_manifest_sha256=final_target["manifest_sha256"],
    )
    direct = forge.diff_committed_generations(
        source_generation_uuid=source["generation_uuid"],
        source_manifest_sha256=source["manifest_sha256"],
        target_generation_uuid=final_target["generation_uuid"],
        target_manifest_sha256=final_target["manifest_sha256"],
    )
    assert ladder["kind"] == "ready"
    assert direct["kind"] == "ready"
    assert ladder["source"] == target
    assert ladder["target"] == final_target
    assert direct["source"] == source
    assert direct["target"] == final_target
    assert ladder["modified_nodes"]["row_count"] == 2
    assert direct["added_nodes"]["row_count"] == 1
    assert direct["modified_nodes"]["row_count"] == 1

    wrong_manifest = bytearray(source["manifest_sha256"])
    wrong_manifest[0] ^= 0xFF
    reload = forge.diff_committed_generations(
        **{**request, "source_manifest_sha256": bytes(wrong_manifest)}
    )
    assert reload == {"kind": "reload_required", "reason": "identity_mismatch"}
    bounded = forge.diff_committed_generations(**request, max_records_per_generation=0)
    assert bounded == {"kind": "reload_required", "reason": "resource_limit"}
    byte_bounded = forge.diff_committed_generations(**request, max_output_bytes=1)
    assert byte_bounded == {"kind": "reload_required", "reason": "resource_limit"}

    cancellation = gf.CancellationToken()
    cancellation.cancel()
    try:
        forge.diff_committed_generations(**request, cancellation=cancellation)
    except gf.GraphForgeError as error:
        assert error.code == "GF_CANCELLED"
    else:
        raise AssertionError("pre-cancelled generation diff must fail explicitly")


if __name__ == "__main__":
    check_generation_diff()
