"""Cross-language neutral invocation descriptor acceptance."""

import graphforge


def test_rank_descriptor_golden_and_dispatch() -> None:
    graph = graphforge.GraphForge()
    descriptor = graph.prepare_rank_invocation("Person", by="degree", via="KNOWS", directed=True)

    assert descriptor.verb == "rank"
    assert descriptor.algorithm == "degree"
    assert isinstance(descriptor.canonical_bytes, bytes)
    assert len(descriptor.projection_fingerprint) == 64
    assert (
        descriptor.fingerprint == "61be156b4aea627fd2cdbf75e18bcc5d0cfc1df53de51ceec5ab9c98f5e19992"
    )

    result = graph.invoke_descriptor(descriptor)
    assert result.column_names == ["node_uuid", "score"]
    assert result.num_rows == 0
    replayed = graph.invoke_descriptor_bytes(descriptor.canonical_bytes)
    assert replayed.equals(result)
