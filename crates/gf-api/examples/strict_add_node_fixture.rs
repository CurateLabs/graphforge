//! Prepare the strict project consumed by the native-wheel acceptance test.

use std::path::PathBuf;

use gf_api::{AdoptOntologyRequest, GraphForge, OntologyMode, OperationId, WriteContext};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let project = PathBuf::from(std::env::args_os().nth(1).ok_or("missing project path")?);
    let ontology = PathBuf::from(std::env::args_os().nth(2).ok_or("missing ontology path")?);
    std::fs::create_dir_all(&project)?;
    let mut graph = GraphForge::new(Some(project.to_str().ok_or("project path is not UTF-8")?))?;
    graph.adopt_ontology(AdoptOntologyRequest {
        context: WriteContext {
            operation_uuid: OperationId(uuid::Uuid::from_u128(2_517)),
            actor_uuid: None,
        },
        path: ontology,
        mode: OntologyMode::Strict,
    })?;
    Ok(())
}
