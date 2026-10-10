use super::{SchemaTopologyFacts, preflight};
use crate::import_session::inventory_budget::InventoryBudget;
use crate::import_session::parquet_footer_counts::preflight as footer_preflight;
use graphforge_core::{ApiErrorCode, GfError, ProjectErrorCode};

#[derive(Default)]
struct Wire(Vec<u8>);

#[allow(clippy::cast_possible_truncation)]
impl Wire {
    fn byte(&mut self, value: u8) {
        self.0.push(value);
    }
    fn vlq(&mut self, mut value: u64) {
        while value >= 0x80 {
            self.byte((value as u8 & 0x7f) | 0x80);
            value >>= 7;
        }
        self.byte(value as u8);
    }
    fn signed(&mut self, value: i64) {
        self.vlq((value.wrapping_shl(1) ^ (value >> 63)) as u64);
    }
    fn text(&mut self, value: &[u8]) {
        self.vlq(value.len() as u64);
        self.0.extend_from_slice(value);
    }
    fn list_header(&mut self, count: usize, kind: u8) {
        if count < 15 {
            self.byte(((count as u8) << 4) | kind);
        } else {
            self.byte(0xfc | kind);
            self.vlq(count as u64);
        }
    }
}

#[derive(Clone)]
struct Element {
    name: Vec<u8>,
    children: Option<i32>,
    physical: bool,
    repetition: Option<i32>,
    geometry_crs: Option<Vec<u8>>,
}

fn root(children: Option<i32>) -> Element {
    Element {
        name: b"schema".to_vec(),
        children,
        physical: false,
        repetition: Some(0),
        geometry_crs: None,
    }
}
fn group(name: &[u8], children: i32) -> Element {
    Element {
        name: name.to_vec(),
        children: Some(children),
        physical: false,
        repetition: Some(0),
        geometry_crs: None,
    }
}
fn leaf(name: &[u8], repetition: i32) -> Element {
    Element {
        name: name.to_vec(),
        children: Some(0),
        physical: true,
        repetition: Some(repetition),
        geometry_crs: None,
    }
}

fn element(wire: &mut Wire, value: &Element) {
    let mut previous = 0;
    if value.physical {
        wire.byte(0x15);
        wire.signed(1);
        previous = 1;
    }
    if let Some(repetition) = value.repetition {
        let delta = 3 - previous;
        wire.byte(((delta as u8) << 4) | 5);
        wire.signed(i64::from(repetition));
        previous = 3;
    }
    let delta = 4 - previous;
    wire.byte(((delta as u8) << 4) | 8);
    wire.text(&value.name);
    previous = 4;
    if let Some(children) = value.children {
        let delta = 5 - previous;
        wire.byte(((delta as u8) << 4) | 5);
        wire.signed(i64::from(children));
        previous = 5;
    }
    if let Some(crs) = &value.geometry_crs {
        let delta = 10 - previous;
        wire.byte(((delta as u8) << 4) | 12); // SchemaElement.logicalType
        wire.byte(0x0c); // LogicalType.geometry, explicit field id follows
        wire.signed(17);
        wire.byte(0x18); // GeometryType.crs
        wire.text(crs);
        wire.byte(0); // GeometryType stop
        wire.byte(0); // LogicalType stop
    }
    wire.byte(0);
}

fn footer(elements: &[Element]) -> Vec<u8> {
    let mut wire = Wire::default();
    wire.byte(0x15);
    wire.signed(1); // version
    wire.byte(0x19);
    wire.list_header(elements.len(), 12);
    for value in elements {
        element(&mut wire, value);
    }
    wire.byte(0x16);
    wire.signed(0); // num_rows
    wire.byte(0x19);
    wire.byte(0); // row_groups
    wire.byte(0); // FileMetaData stop
    wire.0
}

fn topology(elements: &[Element]) -> Result<SchemaTopologyFacts, GfError> {
    let bytes = footer(elements);
    let footer_facts = footer_preflight(&bytes, u64::MAX, None)?;
    preflight(
        &bytes,
        &footer_facts,
        &mut InventoryBudget::new(u64::MAX),
        None,
    )
}

fn storage_error(error: GfError) -> bool {
    matches!(error, GfError::Storage(_))
}
fn resource_error(error: GfError) -> bool {
    matches!(
        error,
        GfError::Project {
            code: ProjectErrorCode::ResourceLimit,
            ..
        }
    )
}

#[test]
fn flat_siblings_charge_minimum_four_path_slots_per_leaf() {
    let mut elements = vec![root(Some(40))];
    elements.extend((0..40).map(|_| leaf(b"x", 0)));
    let facts = topology(&elements).unwrap();
    assert_eq!(facts.nodes, 41);
    assert_eq!(facts.physical_leaves, 40);
    assert_eq!(facts.group_child_slots, 40);
    assert_eq!(facts.path_string_slots, 160);
    assert_eq!(facts.path_name_bytes, 40);
    assert_eq!(facts.max_visited_depth, 1);
}

#[test]
fn a_shared_group_name_is_multiplied_by_its_descendant_leaves() {
    let mut elements = vec![root(Some(1)), group(b"shared-long-group-name", 3)];
    elements.extend((0..3).map(|_| leaf(b"leaf", 0)));
    let facts = topology(&elements).unwrap();
    assert_eq!(
        facts.path_name_bytes,
        3 * (u64::try_from(b"shared-long-group-name".len()).unwrap() + 4)
    );
    assert_eq!(facts.max_visited_depth, 2);
}

#[test]
fn required_depth_changes_path_depth_without_changing_levels() {
    for depth in [16usize, 17, 32, 33] {
        let mut elements = vec![root(Some(1))];
        for _ in 0..depth {
            elements.push(group(b"g", 1));
        }
        elements.push(leaf(b"x", 0));
        let facts = topology(&elements).unwrap();
        assert_eq!(facts.max_visited_depth, u64::try_from(depth + 1).unwrap());
        assert_eq!(facts.max_definition_level, 0);
        assert_eq!(facts.max_repetition_level, 0);
        assert_eq!(
            facts.path_string_slots,
            u64::try_from((depth + 1).max(4)).unwrap()
        );
    }
}

#[test]
fn inner_child_count_is_checked_before_any_native_constructor() {
    let elements = [root(Some(1)), group(b"g", i32::MAX)];
    assert!(storage_error(topology(&elements).unwrap_err()));
}

#[test]
fn missing_descendants_extra_root_and_missing_repetition_are_refused() {
    assert!(storage_error(
        topology(&[root(Some(2)), leaf(b"x", 0)]).unwrap_err()
    ));
    assert!(storage_error(
        topology(&[root(Some(1)), leaf(b"x", 0), leaf(b"y", 0)]).unwrap_err()
    ));
    let mut missing_rep = leaf(b"x", 0);
    missing_rep.repetition = None;
    assert!(storage_error(
        topology(&[root(Some(1)), missing_rep]).unwrap_err()
    ));
    assert!(storage_error(
        topology(&[root(Some(1)), group(b"g", -1)]).unwrap_err()
    ));
    assert!(storage_error(topology(&[]).unwrap_err()));
}

#[test]
fn empty_root_and_native_empty_nonroot_group_have_distinct_topology() {
    let mut ignored_root = root(None);
    ignored_root.physical = true;
    ignored_root.repetition = None;
    let empty_root = topology(&[ignored_root]).unwrap();
    assert_eq!(empty_root.nodes, 1);
    assert_eq!(empty_root.physical_leaves, 0);
    assert_eq!(topology(&[root(Some(0))]).unwrap().nodes, 1);
    let mut empty = group(b"empty", 0);
    empty.physical = false;
    let facts = topology(&[root(Some(1)), empty]).unwrap();
    assert_eq!(facts.nodes, 2);
    assert_eq!(facts.physical_leaves, 0);
    assert_eq!(facts.max_visited_depth, 1);
}

#[test]
fn a_physical_type_on_a_positive_child_group_is_ignored() {
    let mut physical_group = group(b"g", 1);
    physical_group.physical = true;
    let facts = topology(&[root(Some(1)), physical_group, leaf(b"x", 0)]).unwrap();
    assert_eq!(facts.physical_leaves, 1);
    assert_eq!(facts.group_child_slots, 2);
}

#[test]
fn geometry_crs_clones_are_charged_for_primitive_and_all_group_kinds() {
    let crs = b"EPSG:4326";
    let mut primitive = leaf(b"x", 0);
    primitive.geometry_crs = Some(crs.to_vec());
    let facts = topology(&[root(Some(1)), primitive]).unwrap();
    assert_eq!(
        facts.primitive_crs_clone_bytes,
        u64::try_from(crs.len()).unwrap()
    );
    assert_eq!(facts.group_crs_clone_bytes, 0);

    let mut empty_group = group(b"empty", 0);
    empty_group.geometry_crs = Some(crs.to_vec());
    let facts = topology(&[root(Some(1)), empty_group]).unwrap();
    assert_eq!(
        facts.group_crs_clone_bytes,
        u64::try_from(crs.len()).unwrap()
    );
    assert_eq!(facts.physical_leaves, 0);

    let mut nonempty_group = group(b"group", 1);
    nonempty_group.geometry_crs = Some(crs.to_vec());
    let facts = topology(&[root(Some(1)), nonempty_group, leaf(b"x", 0)]).unwrap();
    assert_eq!(
        facts.group_crs_clone_bytes,
        u64::try_from(crs.len()).unwrap()
    );
}

#[test]
fn checked_levels_refuse_a_deep_optional_chain() {
    let mut elements = vec![root(Some(1))];
    for _ in 0..i16::MAX {
        let mut node = group(b"g", 1);
        node.repetition = Some(1);
        elements.push(node);
    }
    let mut final_leaf = leaf(b"x", 1);
    final_leaf.repetition = Some(1);
    elements.push(final_leaf);
    assert!(storage_error(topology(&elements).unwrap_err()));
}

#[test]
fn stack_budget_refusal_is_a_resource_error() {
    let mut elements = vec![root(Some(1))];
    for _ in 0..33 {
        elements.push(group(b"g", 1));
    }
    elements.push(leaf(b"x", 0));
    let bytes = footer(&elements);
    let footer_facts = footer_preflight(&bytes, u64::MAX, None).unwrap();
    let error = preflight(&bytes, &footer_facts, &mut InventoryBudget::new(1), None).unwrap_err();
    assert!(resource_error(error));
}

#[test]
fn cancellation_is_reported_during_topology_replay() {
    use crate::CancellationToken;
    let token = CancellationToken::new();
    token.cancel();
    let bytes = footer(&[root(Some(1)), leaf(b"x", 0)]);
    let footer_facts = footer_preflight(&bytes, u64::MAX, None).unwrap();
    let error = preflight(
        &bytes,
        &footer_facts,
        &mut InventoryBudget::new(u64::MAX),
        Some(&token),
    )
    .unwrap_err();
    assert!(matches!(
        error,
        GfError::Api {
            code: ApiErrorCode::Cancelled,
            ..
        }
    ));
}
