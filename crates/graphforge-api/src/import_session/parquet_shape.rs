//! Immutable structural mapping between the admitted Arrow schema and the
//! physical Parquet schema. This records tree shape and leaf levels only; it
//! does not inspect page events or estimate native-reader memory.

use std::sync::Arc;

use arrow::datatypes::{DataType, FieldRef};
use graphforge_core::GfError;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::basic::{ConvertedType, Repetition, Type as PhysicalType};
use parquet::schema::types::TypePtr;

use crate::CancellationToken;

use super::inventory_budget::{InventoryBudget, reserve};
use super::{cancelled, storage};

#[allow(dead_code)] // Consumed by the source admission and materialization path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum NodeKind {
    Primitive,
    Struct,
    List,
    LargeList,
    FixedSizeList,
    Map,
}

/// One visible Arrow node, linked by indices so a wide schema does not create
/// a heap allocation per node.
#[allow(dead_code)] // Immutable facts are consumed by the bounded native planner.
#[derive(Debug)]
pub(super) struct Node {
    pub(super) field: FieldRef,
    pub(super) kind: NodeKind,
    pub(super) parent: Option<usize>,
    pub(super) first_child: Option<usize>,
    pub(super) next_sibling: Option<usize>,
    pub(super) definition: i16,
    pub(super) repetition: i16,
    pub(super) nullable: bool,
    pub(super) owner_leaf: Option<usize>,
    /// Physical leaf ordinal for primitive nodes; synthetic list/struct nodes
    /// have no direct leaf.
    pub(super) column_index: Option<usize>,
}

#[allow(dead_code)] // Immutable facts are consumed by the bounded native planner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Leaf {
    pub(super) column_index: usize,
    pub(super) source_node: usize,
    pub(super) max_definition: i16,
    pub(super) max_repetition: i16,
    pub(super) physical_type: PhysicalType,
    pub(super) type_length: i32,
}

#[allow(dead_code)] // Built here and wired by the parent source reader separately.
#[derive(Debug)]
pub(super) struct SchemaShape {
    pub(super) nodes: Vec<Node>,
    pub(super) leaves: Vec<Leaf>,
    pub(super) root_children: Option<usize>,
    node_charge: u64,
    leaf_charge: u64,
}

struct VisitTask {
    physical_node: usize,
    parquet: TypePtr,
    field: FieldRef,
    parent: Option<usize>,
    definition: i16,
    repetition: i16,
    nullable_override: Option<bool>,
}

impl SchemaShape {
    pub(super) fn build(
        metadata: &ArrowReaderMetadata,
        budget: &mut InventoryBudget,
        cancellation: Option<&CancellationToken>,
    ) -> Result<Self, GfError> {
        let initial_charge = budget.live_bytes();
        match Self::build_inner(metadata, budget, cancellation) {
            Ok(shape) => Ok(shape),
            Err(error) => {
                budget.release(budget.live_bytes().saturating_sub(initial_charge));
                Err(error)
            }
        }
    }

    fn build_inner(
        metadata: &ArrowReaderMetadata,
        budget: &mut InventoryBudget,
        cancellation: Option<&CancellationToken>,
    ) -> Result<Self, GfError> {
        check_cancelled(cancellation)?;
        let arrow_fields = metadata.schema().fields();
        let physical = physical_topology(
            metadata.parquet_schema().root_schema_ptr(),
            budget,
            cancellation,
        )?;
        if physical[0].leaf_end != metadata.parquet_schema().columns().len() {
            return Err(storage(
                "physical schema leaf count differs from descriptors",
            ));
        }

        for node in &physical {
            check_cancelled(cancellation)?;
            if node.parquet.is_primitive() {
                let descriptor = &metadata.parquet_schema().columns()[node.first_leaf];
                if !Arc::ptr_eq(&node.parquet, &descriptor.self_type_ptr()) {
                    return Err(storage("physical leaf identity differs from descriptor"));
                }
            }
        }

        let mut nodes = Vec::new();
        let mut leaves = Vec::new();
        let mut tasks = Vec::new();
        let mut root_children = None;
        let mut root_tail = None;
        let mut tail_by_parent: Vec<Option<usize>> = Vec::new();

        // The root has no visible Arrow node; its fields are pushed in reverse
        // so the explicit stack visits them in physical DFS order.
        let mut arrow_index = arrow_fields.len();
        for physical_node in reverse_children(&physical, 0) {
            check_cancelled(cancellation)?;
            let parquet = &physical[physical_node].parquet;
            if !physical[physical_node].visible {
                continue;
            }
            arrow_index = arrow_index
                .checked_sub(1)
                .ok_or_else(|| storage("admitted Arrow schema is missing a Parquet field"))?;
            let field = &arrow_fields[arrow_index];
            check_cancelled(cancellation)?;
            if parquet.name() != field.name() {
                return Err(storage(
                    "admitted Arrow and Parquet root field names differ",
                ));
            }
            reserve(&mut tasks, 1, budget, "schema visitor stack")?;
            tasks.push(VisitTask {
                physical_node,
                parquet: Arc::clone(parquet),
                field: Arc::clone(field),
                parent: None,
                definition: 0,
                repetition: 0,
                nullable_override: None,
            });
        }
        if arrow_index != 0 {
            return Err(storage("admitted Arrow schema has unmatched root fields"));
        }

        while let Some(task) = tasks.pop() {
            check_cancelled(cancellation)?;
            let (definition, repetition, physical_nullable) =
                advance_levels(&task.parquet, task.definition, task.repetition)?;
            let nullable = task.nullable_override.unwrap_or(physical_nullable);

            let is_primitive = task.parquet.is_primitive();
            let map_annotation = !is_primitive
                && matches!(
                    task.parquet.get_basic_info().converted_type(),
                    ConvertedType::MAP | ConvertedType::MAP_KEY_VALUE
                );
            let map_as_list = map_annotation && {
                let fields = task.parquet.get_fields();
                fields.len() == 1 && !fields[0].is_primitive() && fields[0].get_fields().len() == 1
            };
            let is_list_group = !is_primitive
                && (task.parquet.get_basic_info().converted_type() == ConvertedType::LIST
                    || map_as_list);
            let is_map_group = map_annotation && !map_as_list;

            if is_primitive {
                if matches!(
                    task.parquet.get_basic_info().repetition(),
                    Repetition::REPEATED
                ) {
                    let element = list_element(&task.field)?;
                    let kind = list_kind(task.field.data_type())?;
                    let list_idx = append_node(
                        &mut nodes,
                        &mut tail_by_parent,
                        &mut root_children,
                        &mut root_tail,
                        budget,
                        task.field.clone(),
                        kind,
                        task.parent,
                        definition,
                        repetition,
                        false,
                        None,
                    )?;
                    append_leaf(
                        &mut nodes,
                        &mut tail_by_parent,
                        &mut root_children,
                        &mut root_tail,
                        &mut leaves,
                        budget,
                        &task.parquet,
                        physical[task.physical_node].first_leaf,
                        element.clone(),
                        Some(list_idx),
                        definition,
                        repetition,
                    )?;
                    continue;
                }

                if task.nullable_override.is_none() && task.field.is_nullable() != nullable {
                    return Err(storage(
                        "admitted Arrow leaf nullability differs from Parquet structure",
                    ));
                }

                if matches!(
                    task.field.data_type(),
                    DataType::Struct(_)
                        | DataType::List(_)
                        | DataType::LargeList(_)
                        | DataType::FixedSizeList(_, _)
                        | DataType::ListView(_)
                        | DataType::LargeListView(_)
                        | DataType::Map(_, _)
                ) {
                    return Err(storage(
                        "Parquet primitive does not match admitted Arrow leaf field",
                    ));
                }

                let _node_idx = append_leaf(
                    &mut nodes,
                    &mut tail_by_parent,
                    &mut root_children,
                    &mut root_tail,
                    &mut leaves,
                    budget,
                    &task.parquet,
                    physical[task.physical_node].first_leaf,
                    task.field,
                    task.parent,
                    definition,
                    repetition,
                )?;
                continue;
            }

            if is_map_group {
                let (entry_field, _) = match task.field.data_type() {
                    DataType::Map(field, sorted) => (Arc::clone(field), *sorted),
                    _ => return Err(storage("Parquet MAP does not match admitted Arrow field")),
                };
                if task.field.is_nullable() != nullable {
                    return Err(storage(
                        "admitted Arrow MAP nullability differs from Parquet structure",
                    ));
                }
                let source_fields = task.parquet.get_fields();
                if source_fields.len() != 1
                    || source_fields[0].get_basic_info().repetition() != Repetition::REPEATED
                    || source_fields[0].get_fields().len() != 2
                {
                    return Err(storage("invalid Parquet MAP key/value structure"));
                }
                let (entry_fields, entry_kind) = match entry_field.data_type() {
                    DataType::Struct(fields) if fields.len() == 2 => (fields, NodeKind::Struct),
                    _ => {
                        return Err(storage(
                            "admitted Arrow MAP entry is not a key/value Struct",
                        ));
                    }
                };
                let map_definition = task
                    .definition
                    .checked_add(
                        if task.parquet.get_basic_info().repetition() == Repetition::OPTIONAL {
                            2
                        } else {
                            1
                        },
                    )
                    .ok_or_else(|| storage("Parquet MAP definition level overflow"))?;
                let map_repetition = task
                    .repetition
                    .checked_add(1)
                    .ok_or_else(|| storage("Parquet MAP repetition level overflow"))?;
                let map_idx = append_node(
                    &mut nodes,
                    &mut tail_by_parent,
                    &mut root_children,
                    &mut root_tail,
                    budget,
                    task.field.clone(),
                    NodeKind::Map,
                    task.parent,
                    map_definition,
                    map_repetition,
                    nullable,
                    None,
                )?;
                let entry_idx = append_node(
                    &mut nodes,
                    &mut tail_by_parent,
                    &mut root_children,
                    &mut root_tail,
                    budget,
                    Arc::clone(&entry_field),
                    entry_kind,
                    Some(map_idx),
                    map_definition,
                    map_repetition,
                    false,
                    None,
                )?;
                let entry_physical = physical[task.physical_node]
                    .first_child
                    .ok_or_else(|| storage("MAP is missing its physical entry"))?;
                for (ordinal, physical_node) in
                    reverse_children(&physical, entry_physical).enumerate()
                {
                    check_cancelled(cancellation)?;
                    let physical_child = &physical[physical_node].parquet;
                    let arrow_child = &entry_fields[1 - ordinal];
                    let ordinal = 1 - ordinal;
                    if physical_child.get_basic_info().repetition() == Repetition::REPEATED {
                        return Err(storage("Parquet MAP key/value fields cannot be repeated"));
                    }
                    if physical_child.name() != arrow_child.name() {
                        return Err(storage(
                            "admitted Arrow MAP field names differ from Parquet",
                        ));
                    }
                    reserve(&mut tasks, 1, budget, "schema visitor stack")?;
                    tasks.push(VisitTask {
                        physical_node,
                        parquet: Arc::clone(physical_child),
                        field: Arc::clone(arrow_child),
                        parent: Some(entry_idx),
                        definition: map_definition,
                        repetition: map_repetition,
                        nullable_override: (ordinal == 0).then_some(false),
                    });
                }
                continue;
            }

            if is_list_group {
                let fields = task.parquet.get_fields();
                if fields.len() != 1
                    || fields[0].get_basic_info().repetition() != Repetition::REPEATED
                    || task.parquet.get_basic_info().repetition() == Repetition::REPEATED
                {
                    return Err(storage("invalid Parquet LIST structural wrapper"));
                }
                let outer_definition =
                    if task.parquet.get_basic_info().repetition() == Repetition::OPTIONAL {
                        task.definition
                            .checked_add(1)
                            .ok_or_else(|| storage("Parquet definition level overflow"))?
                    } else {
                        task.definition
                    };
                if task.field.is_nullable()
                    != (task.parquet.get_basic_info().repetition() == Repetition::OPTIONAL)
                {
                    return Err(storage(
                        "admitted Arrow LIST nullability differs from Parquet structure",
                    ));
                }
                let list_definition = outer_definition
                    .checked_add(1)
                    .ok_or_else(|| storage("Parquet definition level overflow"))?;
                let list_repetition = task
                    .repetition
                    .checked_add(1)
                    .ok_or_else(|| storage("Parquet repetition level overflow"))?;
                let element = list_element(&task.field)?;
                let list_idx = append_node(
                    &mut nodes,
                    &mut tail_by_parent,
                    &mut root_children,
                    &mut root_tail,
                    budget,
                    task.field.clone(),
                    list_kind(task.field.data_type())?,
                    task.parent,
                    list_definition,
                    list_repetition,
                    task.parquet.get_basic_info().repetition() == Repetition::OPTIONAL,
                    None,
                )?;

                let repeated = &fields[0];
                let repeated_physical = physical[task.physical_node]
                    .first_child
                    .ok_or_else(|| storage("LIST is missing its physical element"))?;
                let preserve_struct = preserve_list_struct(&task.parquet, repeated);
                if repeated.is_primitive() {
                    if element.is_nullable() {
                        return Err(storage(
                            "legacy repeated primitive Arrow element must be non-nullable",
                        ));
                    }
                    append_leaf(
                        &mut nodes,
                        &mut tail_by_parent,
                        &mut root_children,
                        &mut root_tail,
                        &mut leaves,
                        budget,
                        repeated,
                        physical[repeated_physical].first_leaf,
                        element,
                        Some(list_idx),
                        list_definition,
                        list_repetition,
                    )?;
                } else if preserve_struct {
                    let item_fields = match element.data_type() {
                        DataType::Struct(fields) => fields,
                        _ => {
                            return Err(storage(
                                "legacy LIST element is not an admitted Arrow Struct",
                            ));
                        }
                    };
                    let struct_idx = append_node(
                        &mut nodes,
                        &mut tail_by_parent,
                        &mut root_children,
                        &mut root_tail,
                        budget,
                        Arc::clone(&element),
                        NodeKind::Struct,
                        Some(list_idx),
                        list_definition,
                        list_repetition,
                        false,
                        None,
                    )?;
                    let mut arrow_index = item_fields.len();
                    for physical_node in reverse_children(&physical, repeated_physical) {
                        check_cancelled(cancellation)?;
                        let physical_child = &physical[physical_node].parquet;
                        if !physical[physical_node].visible {
                            continue;
                        }
                        arrow_index = arrow_index.checked_sub(1).ok_or_else(|| {
                            storage("legacy LIST Struct is missing a Parquet child")
                        })?;
                        let arrow_child = &item_fields[arrow_index];
                        if physical_child.name() != arrow_child.name() {
                            return Err(storage("legacy LIST Struct names differ from Parquet"));
                        }
                        reserve(&mut tasks, 1, budget, "schema visitor stack")?;
                        tasks.push(VisitTask {
                            physical_node,
                            parquet: Arc::clone(physical_child),
                            field: Arc::clone(arrow_child),
                            parent: Some(struct_idx),
                            definition: list_definition,
                            repetition: list_repetition,
                            nullable_override: None,
                        });
                    }
                    if arrow_index != 0 {
                        return Err(storage("legacy LIST Struct has unmatched fields"));
                    }
                } else {
                    let child_type = repeated.get_fields()[0].clone();
                    reserve(&mut tasks, 1, budget, "schema visitor stack")?;
                    tasks.push(VisitTask {
                        physical_node: physical[repeated_physical]
                            .first_child
                            .ok_or_else(|| storage("LIST wrapper is missing its item"))?,
                        parquet: child_type,
                        field: element,
                        parent: Some(list_idx),
                        definition: list_definition,
                        repetition: list_repetition,
                        nullable_override: None,
                    });
                }
                continue;
            }

            let (struct_field, repeated_list) =
                if task.parquet.get_basic_info().repetition() == Repetition::REPEATED {
                    let item = list_element(&task.field)?;
                    if !matches!(item.data_type(), DataType::Struct(_)) {
                        return Err(storage(
                            "repeated Parquet group is not an admitted Struct list",
                        ));
                    }
                    (item, true)
                } else {
                    (task.field.clone(), false)
                };
            let (struct_fields, kind) = match struct_field.data_type() {
                DataType::Struct(fields) => (fields, NodeKind::Struct),
                _ => {
                    return Err(storage(
                        "Parquet group does not match admitted Arrow Struct",
                    ));
                }
            };
            if !repeated_list && struct_field.is_nullable() != nullable {
                return Err(storage(
                    "admitted Arrow Struct nullability differs from Parquet structure",
                ));
            }

            let parent_idx = if repeated_list {
                let list_kind = list_kind(task.field.data_type())?;
                let list_idx = append_node(
                    &mut nodes,
                    &mut tail_by_parent,
                    &mut root_children,
                    &mut root_tail,
                    budget,
                    task.field.clone(),
                    list_kind,
                    task.parent,
                    definition,
                    repetition,
                    false,
                    None,
                )?;
                // List-item Struct has the same levels as its repeated group.
                let struct_idx = append_node(
                    &mut nodes,
                    &mut tail_by_parent,
                    &mut root_children,
                    &mut root_tail,
                    budget,
                    struct_field.clone(),
                    kind,
                    Some(list_idx),
                    definition,
                    repetition,
                    false,
                    None,
                )?;
                Some(struct_idx)
            } else {
                Some(append_node(
                    &mut nodes,
                    &mut tail_by_parent,
                    &mut root_children,
                    &mut root_tail,
                    budget,
                    struct_field.clone(),
                    kind,
                    task.parent,
                    definition,
                    repetition,
                    nullable,
                    None,
                )?)
            };

            let mut arrow_index = struct_fields.len();
            for physical_node in reverse_children(&physical, task.physical_node) {
                check_cancelled(cancellation)?;
                let physical_child = &physical[physical_node].parquet;
                if !physical[physical_node].visible {
                    continue;
                }
                arrow_index = arrow_index
                    .checked_sub(1)
                    .ok_or_else(|| storage("admitted Arrow Struct is missing a Parquet child"))?;
                let arrow_child = &struct_fields[arrow_index];
                if physical_child.name() != arrow_child.name() {
                    return Err(storage(
                        "admitted Arrow and Parquet Struct field names differ",
                    ));
                }
                reserve(&mut tasks, 1, budget, "schema visitor stack")?;
                tasks.push(VisitTask {
                    physical_node,
                    parquet: Arc::clone(physical_child),
                    field: Arc::clone(arrow_child),
                    parent: parent_idx,
                    definition,
                    repetition,
                    nullable_override: None,
                });
            }
            if arrow_index != 0 {
                return Err(storage("admitted Arrow Struct has unmatched fields"));
            }
        }

        let stack_charge = capacity_bytes::<VisitTask>(tasks.capacity())?;
        drop(tasks);
        budget.release(stack_charge);
        let tail_charge = capacity_bytes::<Option<usize>>(tail_by_parent.capacity())?;
        drop(tail_by_parent);
        budget.release(tail_charge);

        // Resolve the first actual descendant leaf from the already-built
        // links. Reverse node order guarantees children are complete first.
        for idx in (0..nodes.len()).rev() {
            check_cancelled(cancellation)?;
            if nodes[idx].owner_leaf.is_none() {
                nodes[idx].owner_leaf = nodes[idx]
                    .first_child
                    .and_then(|child| nodes[child].owner_leaf);
            }
        }

        let descriptors = metadata.parquet_schema().columns();
        let mut previous_column = None;
        for leaf in &leaves {
            check_cancelled(cancellation)?;
            let descriptor = descriptors
                .get(leaf.column_index)
                .ok_or_else(|| storage("visible leaf has no physical descriptor"))?;
            if previous_column.is_some_and(|previous| previous >= leaf.column_index)
                || leaf.max_definition != descriptor.max_def_level()
                || leaf.max_repetition != descriptor.max_rep_level()
                || leaf.physical_type != descriptor.physical_type()
                || leaf.type_length != descriptor.type_length()
            {
                return Err(storage(
                    "Parquet structural levels differ from leaf descriptor",
                ));
            }
            previous_column = Some(leaf.column_index);
        }
        let physical_charge = capacity_bytes::<PhysicalNode>(physical.capacity())?;
        drop(physical);
        budget.release(physical_charge);

        check_cancelled(cancellation)?;

        Ok(Self {
            node_charge: capacity_bytes::<Node>(nodes.capacity())?,
            leaf_charge: capacity_bytes::<Leaf>(leaves.capacity())?,
            nodes,
            leaves,
            root_children,
        })
    }

    /// Actual retained vector capacity after all visitor temporaries have
    /// dropped. Arrow fields are shared references to the admitted schema.
    pub(super) fn inventory_bytes(&self) -> Result<u64, GfError> {
        self.node_charge
            .checked_add(self.leaf_charge)
            .ok_or_else(|| storage("Parquet schema inventory size overflow"))
    }

    pub(super) fn release(self, budget: &mut InventoryBudget) {
        budget.release(self.node_charge);
        budget.release(self.leaf_charge);
    }
}

fn append_node(
    nodes: &mut Vec<Node>,
    tails: &mut Vec<Option<usize>>,
    roots: &mut Option<usize>,
    root_tail: &mut Option<usize>,
    budget: &mut InventoryBudget,
    field: FieldRef,
    kind: NodeKind,
    parent: Option<usize>,
    definition: i16,
    repetition: i16,
    nullable: bool,
    column_index: Option<usize>,
) -> Result<usize, GfError> {
    reserve(nodes, 1, budget, "schema nodes")?;
    reserve(tails, 1, budget, "schema sibling links")?;
    let index = nodes.len();
    nodes.push(Node {
        field,
        kind,
        parent,
        first_child: None,
        next_sibling: None,
        definition,
        repetition,
        nullable,
        owner_leaf: None,
        column_index,
    });
    tails.push(None);
    attach_node(nodes, tails, roots, root_tail, index);
    Ok(index)
}

fn attach_node(
    nodes: &mut [Node],
    tails: &mut [Option<usize>],
    roots: &mut Option<usize>,
    root_tail: &mut Option<usize>,
    index: usize,
) {
    let parent = nodes[index].parent;
    if let Some(parent) = parent {
        if let Some(tail) = tails[parent] {
            nodes[tail].next_sibling = Some(index);
        } else {
            nodes[parent].first_child = Some(index);
        }
        tails[parent] = Some(index);
    } else {
        if let Some(tail) = *root_tail {
            nodes[tail].next_sibling = Some(index);
        } else {
            *roots = Some(index);
        }
        *root_tail = Some(index);
    }
}

fn append_leaf(
    nodes: &mut Vec<Node>,
    tails: &mut Vec<Option<usize>>,
    roots: &mut Option<usize>,
    root_tail: &mut Option<usize>,
    leaves: &mut Vec<Leaf>,
    budget: &mut InventoryBudget,
    parquet: &TypePtr,
    descriptor_index: usize,
    field: FieldRef,
    parent: Option<usize>,
    definition: i16,
    repetition: i16,
) -> Result<usize, GfError> {
    let index = nodes.len();
    let owner_index = leaves.len();
    let nullable = field.is_nullable();
    reserve(nodes, 1, budget, "schema nodes")?;
    reserve(tails, 1, budget, "schema sibling links")?;
    nodes.push(Node {
        field,
        kind: NodeKind::Primitive,
        parent,
        first_child: None,
        next_sibling: None,
        definition,
        repetition,
        nullable,
        owner_leaf: Some(owner_index),
        column_index: Some(descriptor_index),
    });
    tails.push(None);
    attach_node(nodes, tails, roots, root_tail, index);
    reserve(leaves, 1, budget, "schema leaves")?;
    leaves.push(Leaf {
        column_index: descriptor_index,
        source_node: index,
        max_definition: definition,
        max_repetition: repetition,
        physical_type: parquet.get_physical_type(),
        type_length: match parquet.as_ref() {
            parquet::schema::types::Type::PrimitiveType { type_length, .. } => *type_length,
            parquet::schema::types::Type::GroupType { .. } => {
                return Err(storage("Parquet leaf inventory received a group type"));
            }
        },
    });
    Ok(index)
}

fn advance_levels(
    parquet: &TypePtr,
    definition: i16,
    repetition: i16,
) -> Result<(i16, i16, bool), GfError> {
    let (d, r, nullable) = match parquet.get_basic_info().repetition() {
        Repetition::REQUIRED => (definition, repetition, false),
        Repetition::OPTIONAL => (
            definition
                .checked_add(1)
                .ok_or_else(|| storage("definition level overflow"))?,
            repetition,
            true,
        ),
        Repetition::REPEATED => (
            definition
                .checked_add(1)
                .ok_or_else(|| storage("definition level overflow"))?,
            repetition
                .checked_add(1)
                .ok_or_else(|| storage("repetition level overflow"))?,
            false,
        ),
    };
    if d < 0 || r < 0 {
        return Err(storage("negative Parquet schema level"));
    }
    Ok((d, r, nullable))
}

fn list_element(field: &FieldRef) -> Result<FieldRef, GfError> {
    match field.data_type() {
        DataType::List(child)
        | DataType::LargeList(child)
        | DataType::FixedSizeList(child, _)
        | DataType::ListView(child)
        | DataType::LargeListView(child) => Ok(Arc::clone(child)),
        _ => Err(storage(
            "Parquet repeated field does not match admitted Arrow list",
        )),
    }
}

fn list_kind(data_type: &DataType) -> Result<NodeKind, GfError> {
    match data_type {
        DataType::List(_) | DataType::ListView(_) => Ok(NodeKind::List),
        DataType::LargeList(_) | DataType::LargeListView(_) => Ok(NodeKind::LargeList),
        DataType::FixedSizeList(_, _) => Ok(NodeKind::FixedSizeList),
        _ => Err(storage("Parquet list does not match admitted Arrow list")),
    }
}

fn is_list_annotation(parquet: &TypePtr) -> bool {
    let info = parquet.get_basic_info();
    match info.logical_type_ref() {
        Some(logical) => matches!(logical, parquet::basic::LogicalType::List),
        None => info.converted_type() == ConvertedType::LIST,
    }
}

fn has_single_repeated_child(parquet: &TypePtr) -> bool {
    let fields = parquet.get_fields();
    fields.len() == 1
        && fields[0].get_basic_info().has_repetition()
        && fields[0].get_basic_info().repetition() == Repetition::REPEATED
}

/// Physical ordinals include columns hidden by semantic inference. These facts
/// are temporary and independently admitted before the visible tree is built.
struct PhysicalNode {
    parquet: TypePtr,
    first_child: Option<usize>,
    last_child: Option<usize>,
    previous_sibling: Option<usize>,
    first_leaf: usize,
    leaf_end: usize,
    visible: bool,
}

struct ReverseChildren<'a> {
    nodes: &'a [PhysicalNode],
    next: Option<usize>,
}

impl Iterator for ReverseChildren<'_> {
    type Item = usize;
    fn next(&mut self) -> Option<usize> {
        let current = self.next?;
        self.next = self.nodes[current].previous_sibling;
        Some(current)
    }
}

fn reverse_children(nodes: &[PhysicalNode], parent: usize) -> ReverseChildren<'_> {
    ReverseChildren {
        nodes,
        next: nodes[parent].last_child,
    }
}

fn preserve_list_struct(outer: &TypePtr, repeated: &TypePtr) -> bool {
    !repeated.is_primitive()
        && (repeated.get_fields().len() != 1
            || (!is_list_annotation(repeated)
                && !has_single_repeated_child(repeated)
                && (repeated.name() == "array"
                    || repeated.name().strip_suffix("_tuple") == Some(outer.name()))))
}

fn physical_topology(
    root: TypePtr,
    budget: &mut InventoryBudget,
    cancellation: Option<&CancellationToken>,
) -> Result<Vec<PhysicalNode>, GfError> {
    let mut nodes = Vec::new();
    let mut frames: Vec<(usize, usize)> = Vec::new();
    reserve(&mut nodes, 1, budget, "physical schema topology")?;
    nodes.push(PhysicalNode {
        parquet: root,
        first_child: None,
        last_child: None,
        previous_sibling: None,
        first_leaf: 0,
        leaf_end: 0,
        visible: false,
    });
    reserve(&mut frames, 1, budget, "physical schema traversal stack")?;
    frames.push((0, 0));
    let mut next_leaf = 0usize;
    while let Some(&(index, next_child)) = frames.last() {
        check_cancelled(cancellation)?;
        let parquet = &nodes[index].parquet;
        if parquet.is_primitive() {
            next_leaf = next_leaf
                .checked_add(1)
                .ok_or_else(|| storage("physical schema leaf count overflow"))?;
            nodes[index].leaf_end = next_leaf;
            nodes[index].visible = true;
            frames.pop();
            continue;
        }
        let fields = parquet.get_fields();
        if let Some(child) = fields.get(next_child) {
            let child = Arc::clone(child);
            frames.last_mut().expect("schema frame exists").1 += 1;
            reserve(&mut nodes, 1, budget, "physical schema topology")?;
            let child_index = nodes.len();
            let previous_sibling = nodes[index].last_child;
            nodes.push(PhysicalNode {
                parquet: child,
                first_child: None,
                last_child: None,
                previous_sibling,
                first_leaf: next_leaf,
                leaf_end: next_leaf,
                visible: false,
            });
            if nodes[index].first_child.is_none() {
                nodes[index].first_child = Some(child_index);
            }
            nodes[index].last_child = Some(child_index);
            reserve(&mut frames, 1, budget, "physical schema traversal stack")?;
            frames.push((child_index, 0));
            continue;
        }
        nodes[index].leaf_end = next_leaf;
        nodes[index].visible = semantic_visibility(&nodes, index, cancellation)?;
        frames.pop();
    }
    let charge = capacity_bytes::<(usize, usize)>(frames.capacity())?;
    drop(frames);
    budget.release(charge);
    Ok(nodes)
}

/// Match the pinned all-column inference dispatch, including a LIST's direct
/// visit_struct branch (which bypasses the repeated group's own annotation).
fn semantic_visibility(
    nodes: &[PhysicalNode],
    index: usize,
    cancellation: Option<&CancellationToken>,
) -> Result<bool, GfError> {
    let node = &nodes[index];
    let parquet = &node.parquet;
    let converted = parquet.get_basic_info().converted_type();
    let map = matches!(converted, ConvertedType::MAP | ConvertedType::MAP_KEY_VALUE);
    let entry = node.first_child;
    let map_as_list = map
        && parquet.get_fields().len() == 1
        && entry.is_some_and(|entry| {
            !nodes[entry].parquet.is_primitive() && nodes[entry].parquet.get_fields().len() == 1
        });
    if index != 0 && (converted == ConvertedType::LIST || map_as_list) {
        // An annotated repeated group may be consumed directly by visit_struct
        // rather than dispatched. Its unused dispatch shape need not be valid.
        if parquet.get_fields().len() != 1 {
            return Ok(false);
        }
        let Some(repeated) = entry else {
            return Ok(false);
        };
        if nodes[repeated].parquet.is_primitive() {
            return Ok(true);
        }
        if preserve_list_struct(parquet, &nodes[repeated].parquet) {
            return any_visible_child(nodes, repeated, cancellation);
        }
        return Ok(nodes[repeated]
            .first_child
            .is_some_and(|item| nodes[item].visible));
    }
    if index != 0 && map {
        if parquet.get_fields().len() != 1 {
            return Ok(false);
        }
        let Some(entry) = entry else { return Ok(false) };
        if nodes[entry].parquet.is_primitive() || nodes[entry].parquet.get_fields().len() != 2 {
            return Ok(false);
        }
        for child in reverse_children(nodes, entry) {
            check_cancelled(cancellation)?;
            if !nodes[child].visible {
                return Ok(false);
            }
        }
        return Ok(true);
    }
    any_visible_child(nodes, index, cancellation)
}

fn any_visible_child(
    nodes: &[PhysicalNode],
    parent: usize,
    cancellation: Option<&CancellationToken>,
) -> Result<bool, GfError> {
    for child in reverse_children(nodes, parent) {
        check_cancelled(cancellation)?;
        if nodes[child].visible {
            return Ok(true);
        }
    }
    Ok(false)
}

fn check_cancelled(cancellation: Option<&CancellationToken>) -> Result<(), GfError> {
    if cancellation.is_some_and(CancellationToken::is_cancelled) {
        Err(cancelled())
    } else {
        Ok(())
    }
}

fn capacity_bytes<T>(capacity: usize) -> Result<u64, GfError> {
    let cap = u64::try_from(capacity).map_err(storage)?;
    let size = u64::try_from(std::mem::size_of::<T>()).map_err(storage)?;
    cap.checked_mul(size)
        .ok_or_else(|| storage("Parquet schema inventory size overflow"))
}

#[cfg(test)]
#[path = "parquet_shape/tests.rs"]
mod tests;
