use crate::GraphForge;
use std::collections::HashMap;

#[test]
fn runtime_guard_blocks_inside_and_outside_an_ambient_runtime() {
    let graph = GraphForge::new(None).unwrap();
    let (_, _, guard) = graph
        .execute_stream_owned("RETURN 1 AS value", &HashMap::new())
        .unwrap();
    assert_eq!(guard.block_on(async { 41 + 1 }), 42);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async move {
        assert_eq!(guard.block_on(async { 20 + 22 }), 42);
    });
}
