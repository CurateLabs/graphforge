use std::path::Path;
use std::sync::{Arc, Mutex};

use graphforge_core::OntologyMode;
use graphforge_core::uuid::new_v7;
use graphforge_exec::ExecutionSession;
use graphforge_ir::{Binder, GraphPlan, RuntimeCatalog};
use graphforge_storage::{GraphCatalog, GraphWriter};
use graphforge_value::EntityTypeId;
use tempfile::TempDir;

const TS: i64 = 1_700_000_000_000_000;
const PERSON: EntityTypeId = match EntityTypeId::decode(0) {
    Ok(id) => id,
    Err(_) => panic!("valid person fixture identity"),
};

fn seed_filler_nodes(dir: &Path, count: usize) {
    let mut writer = GraphWriter::open_at(dir, OntologyMode::Exploratory, TS).unwrap();
    for _ in 0..count {
        writer.create_node(new_v7(), PERSON).unwrap();
    }
    writer.flush().unwrap();
}

fn bind(query: &str, runtime_catalog: &Arc<Mutex<RuntimeCatalog>>) -> GraphPlan {
    let binder = Binder::new(None, Arc::clone(runtime_catalog), OntologyMode::Exploratory);
    let ast = graphforge_cypher::parse(query).expect("parse");
    binder
        .bind(&ast)
        .unwrap_or_else(|error| panic!("bind {query:?}: {error:?}"))
}

pub struct FreshMergeFixture {
    dir: TempDir,
    runtime_catalog: Arc<Mutex<RuntimeCatalog>>,
    runtime: tokio::runtime::Runtime,
}

impl FreshMergeFixture {
    pub fn with_filler_nodes(count: usize) -> Self {
        let dir = TempDir::new().unwrap();
        seed_filler_nodes(dir.path(), count);
        Self {
            dir,
            runtime_catalog: Arc::new(Mutex::new(RuntimeCatalog::new())),
            runtime: tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
        }
    }

    pub fn merge_rows(&self, rows: usize) {
        let statement =
            format!("UNWIND range(1, {rows}) AS i MERGE (m:Merged {{uid: i}}) RETURN count(m)");
        self.runtime
            .block_on(self.execute(&statement))
            .expect("merge execution");
    }

    async fn execute(&self, statement: &str) -> Result<(), String> {
        let plan = bind(statement, &self.runtime_catalog);
        let catalog =
            GraphCatalog::open(self.dir.path(), None, &self.runtime_catalog.lock().unwrap())
                .map_err(|error| error.to_string())?;
        let session = ExecutionSession::new_with_target(
            catalog,
            None,
            self.dir.path().to_path_buf(),
            OntologyMode::Exploratory,
        )
        .map_err(|error| error.to_string())?;
        session
            .execute_write_statement(&plan)
            .await
            .map_err(|error| error.to_string())?;
        Ok(())
    }
}
