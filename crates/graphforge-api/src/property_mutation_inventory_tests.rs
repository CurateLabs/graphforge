use graphforge_storage::{StorageIoPhase, lifecycle_io};

use crate::GraphForge;

fn seed_project(noise_rows: usize) -> (tempfile::TempDir, GraphForge) {
    let project = tempfile::Builder::new()
        .prefix("gf-property-mutation-inventory-")
        .tempdir_in(std::env::current_dir().unwrap())
        .unwrap();
    let project_path = project.path().join("project");
    let graph = GraphForge::new(project_path.to_str()).unwrap();
    let mut statement = String::from("CREATE (:Target {value: 1})");
    for _ in 0..noise_rows {
        statement.push_str(" CREATE (:Noise)");
    }
    graph.execute(&statement).unwrap();
    (project, graph)
}

fn inventory_read_bytes(graph: &GraphForge, statement: &str) -> (u64, u64, u64, u64) {
    let workspace = graph.dir();
    let expected_route_bytes = target_route_payload_bytes(&workspace.dir);
    let expected_control_bytes = std::fs::metadata(workspace.dir.join("semantic-routes.json"))
        .map_or(0, |metadata| metadata.len().saturating_mul(2));
    let _capture = lifecycle_io::CaptureScope::install();
    let before = lifecycle_io::snapshot().unwrap();
    graph.execute(statement).unwrap();
    let region = lifecycle_io::snapshot().unwrap().since(&before).unwrap();
    region.validate_for_qualification().unwrap();
    (
        region.phases[&StorageIoPhase::PropertyMutationInventory].read_bytes,
        region.phases[&StorageIoPhase::PropertyMutationRouteAuthority].read_bytes,
        expected_route_bytes,
        expected_control_bytes,
    )
}

fn target_route_payload_bytes(root: &std::path::Path) -> u64 {
    let control_path = root.join("semantic-routes.json");
    let component = std::fs::read(&control_path)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
        .and_then(|table| table.get("entries")?.as_array().cloned())
        .and_then(|entries| {
            entries.into_iter().find_map(|entry| {
                (entry.get("route")?.as_str()? == "_untyped")
                    .then(|| entry.get("component")?.as_str().map(str::to_owned))
                    .flatten()
            })
        });
    let flat_names = component.map_or_else(
        || vec!["_untyped.parquet".to_owned(), "_untyped".to_owned()],
        |component| vec![format!("{component}.parquet"), component],
    );
    fn collect(path: &std::path::Path, names: &[String], bytes: &mut u64) {
        let Ok(entries) = std::fs::read_dir(path) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().into_owned();
            if path.is_dir() {
                if names.contains(&name) {
                    collect(&path, &[], bytes);
                } else {
                    collect(&path, names, bytes);
                }
            } else if names.contains(&name) || names.is_empty() {
                *bytes =
                    bytes.saturating_add(entry.metadata().map_or(0, |metadata| metadata.len()));
            }
        }
    }
    let mut bytes = 0;
    collect(&root.join("properties"), &flat_names, &mut bytes);
    bytes
}

fn target_value(graph: &GraphForge) -> String {
    let batches = graph
        .execute("MATCH (n:Target) RETURN n.value AS value")
        .unwrap()
        .batches;
    let mut result = None;
    for batch in batches {
        let column = batch.column_by_name("value").unwrap();
        for row in 0..batch.num_rows() {
            assert!(result.is_none(), "fixture has a single target node");
            result = Some(arrow::util::display::array_value_to_string(column, row).unwrap());
        }
    }
    result.unwrap_or_else(|| "NO ROW".to_owned())
}

fn run_fixture(noise_rows: usize) -> ((u64, u64, u64, u64), (u64, u64, u64, u64)) {
    let (project, graph) = seed_project(noise_rows);
    let set_reads = inventory_read_bytes(&graph, "MATCH (n:Target) SET n.value = 2");
    drop(graph);
    let project_path = project.path().join("project");
    let reopened = GraphForge::new(project_path.to_str()).unwrap();
    assert_eq!(target_value(&reopened), "2");

    let remove_reads = inventory_read_bytes(&reopened, "MATCH (n:Target) REMOVE n.value");
    drop(reopened);
    let after_remove = GraphForge::new(project_path.to_str()).unwrap();
    assert_eq!(target_value(&after_remove), "");
    (set_reads, remove_reads)
}

#[test]
fn persisted_set_and_remove_inventory_reads_ignore_unrelated_growth() {
    let small = run_fixture(4);
    let large = run_fixture(64);
    println!(
        "property mutation inventory bytes: noise_nodes=4 set_payload={} set_route_control={} remove_payload={} remove_route_control={}; noise_nodes=64 set_payload={} set_route_control={} remove_payload={} remove_route_control={}",
        small.0.0, small.0.1, small.1.0, small.1.1, large.0.0, large.0.1, large.1.0, large.1.1,
    );
    assert!(small.0.0 > 0, "SET must authenticate its selected route");
    assert!(small.1.0 > 0, "REMOVE must authenticate its selected route");
    assert_eq!(small.0.0, small.0.2, "SET selected-route byte attribution");
    assert_eq!(small.0.1, small.0.3, "SET control-table byte attribution");
    assert_eq!(
        small.1.0, small.1.2,
        "REMOVE selected-route byte attribution"
    );
    assert_eq!(
        small.1.1, small.1.3,
        "REMOVE control-table byte attribution"
    );
    assert_eq!(large.0.0, large.0.2, "large SET selected-route attribution");
    assert_eq!(large.0.1, large.0.3, "large SET control-table attribution");
    assert_eq!(
        large.1.0, large.1.2,
        "large REMOVE selected-route attribution"
    );
    assert_eq!(
        large.1.1, large.1.3,
        "large REMOVE control-table attribution"
    );
    assert_eq!(
        large.0.0, small.0.0,
        "SET inventory work grew with unrelated payloads: small={small:?}, large={large:?}"
    );
    assert_eq!(
        large.1.0, small.1.0,
        "REMOVE inventory work grew with unrelated payloads: small={small:?}, large={large:?}"
    );
    assert_eq!(
        large.0.1, small.0.1,
        "SET route-control work grew for fixed schema: small={small:?}, large={large:?}"
    );
    assert_eq!(
        large.1.1, small.1.1,
        "REMOVE route-control work grew for fixed schema: small={small:?}, large={large:?}"
    );
}
