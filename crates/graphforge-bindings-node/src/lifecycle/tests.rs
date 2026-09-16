use super::*;
use crate::Arc;
use crate::Buffer;
use crate::CapabilityId;
use crate::GraphForge;
use crate::WriteContext;
use crate::canonical_operation_id;
use crate::ipc_to_record_batch;

#[test]
fn capability_tasks_return_rust_owned_arrow_ipc() {
    let graph = GraphForge::new(None, None).unwrap();
    let mut inspect = ProjectCapabilitiesTask {
        engine: Arc::clone(&graph.inner),
    };
    let initial = inspect.compute().unwrap().unwrap();
    let initial = ipc_to_record_batch(&Buffer::from(initial)).unwrap();
    assert_eq!(initial.num_rows(), 2);

    let mut enable = EnableCapabilityTask {
        engine: Arc::clone(&graph.inner),
        request: graphforge_api::EnableCapabilityRequest {
            context: WriteContext {
                operation_uuid: canonical_operation_id("018f0f4e-7b8c-7000-8000-000000000001")
                    .unwrap(),
                actor_uuid: None,
            },
            capability_id: CapabilityId::Knowledge,
            capability_version: 1,
        },
    };
    let enabled = enable.compute().unwrap().unwrap();
    let enabled = ipc_to_record_batch(&Buffer::from(enabled)).unwrap();
    assert_eq!(enabled.num_rows(), 3);
    assert_eq!(
        enabled
            .column_by_name("capability_id")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap()
            .value(1),
        "knowledge"
    );
}
