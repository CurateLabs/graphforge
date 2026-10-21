//! Owner-qualified property inspection for composition-bound research.
use super::fields::{self, FieldSink, Objects, Scan};
use crate::{CancellationToken, GfError, GraphForge, IrLiteral};
use arrow::array::FixedSizeBinaryArray;
use std::collections::{BTreeMap, HashMap};
use uuid::Uuid;

/// One hash partition of object identities, `hash(id) % modulus == residue`.
/// Owner conflicts are per object, so a partition holds every emission of the
/// objects it owns and can be checked without seeing the rest of the graph.
#[derive(Clone, Copy)]
struct Partition {
    modulus: u64,
    residue: u64,
}
impl Partition {
    fn holds(self, id: Uuid) -> bool {
        use std::hash::{Hash, Hasher};
        self.modulus == 1 || {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            id.hash(&mut hasher);
            hasher.finish() % self.modulus == self.residue
        }
    }
}
const MAX_PARTITIONS: u64 = 1 << 20;

/// Receives one owner-qualified property emission of an owner scan.
type Emit<'a> = &'a mut dyn FnMut(fields::Key, [u8; 32]) -> Result<(), GfError>;

pub(super) fn read(
    graph: &GraphForge,
    selected: Option<&Objects>,
    budgeted: bool,
    sink: &mut dyn FieldSink,
    cancellation: &CancellationToken,
) -> Result<(), GfError> {
    let bindings = graph
        .semantic_storage_bindings
        .lock()
        .expect("semantic bindings")
        .clone();
    let Some(bindings) = bindings else {
        return Ok(());
    };
    refuse_owner_conflicts(sink, |sink, part, emit| {
        scan_bindings(
            graph,
            &bindings,
            selected,
            budgeted,
            cancellation,
            sink,
            part,
            emit,
        )
    })
}

/// Emit every owner-qualified property the scans produce to `sink`, refusing a
/// key whose owner scans disagree. A sink with a conflict bound is served
/// partition by partition: a partition whose keys exceed the bound is split in
/// two and rescanned, so memory stays within the bound while the check still
/// sees every key of every object, requested or not.
fn refuse_owner_conflicts(
    sink: &mut dyn FieldSink,
    mut scan: impl FnMut(&mut dyn FieldSink, Partition, Emit<'_>) -> Result<(), GfError>,
) -> Result<(), GfError> {
    let bound = sink.conflict_keys();
    let mut pending = vec![Partition {
        modulus: 1,
        residue: 0,
    }];
    while let Some(part) = pending.pop() {
        let mut semantic = BTreeMap::new();
        let mut overflowed = false;
        let scanned = scan(sink, part, &mut |key, digest| {
            // A single native field cannot represent two distinct owner values.
            if semantic.get(&key).is_some_and(|prior| *prior != digest) {
                return Err(invalid());
            }
            semantic.insert(key, digest);
            if bound.is_some_and(|bound| semantic.len() > bound) {
                overflowed = true;
                return Err(fields::limit());
            }
            Ok(())
        });
        match scanned {
            Err(_) if overflowed => {
                if part.modulus >= MAX_PARTITIONS {
                    return Err(fields::limit());
                }
                let modulus = part.modulus * 2;
                pending.push(Partition {
                    modulus,
                    residue: part.residue + part.modulus,
                });
                pending.push(Partition {
                    modulus,
                    residue: part.residue,
                });
            }
            other => {
                other?;
                for (key, digest) in semantic {
                    sink.field(key, digest)?;
                }
            }
        }
    }
    Ok(())
}

/// Stream every binding's owner scan for the objects of one partition.
#[allow(clippy::too_many_arguments)]
fn scan_bindings(
    graph: &GraphForge,
    bindings: &graphforge_storage::SemanticStorageBindings,
    selected: Option<&Objects>,
    budgeted: bool,
    cancellation: &CancellationToken,
    sink: &mut dyn FieldSink,
    part: Partition,
    emit: Emit<'_>,
) -> Result<(), GfError> {
    for binding in &bindings.bindings {
        let (kind, object, identity, pattern) = match binding.route_kind {
            graphforge_storage::SemanticRouteKind::Entity => (
                "node",
                "n",
                "node_uuid",
                format!(
                    "(n:`{}`)",
                    binding.symbol.ambiguity_candidate().replace('`', "``")
                ),
            ),
            graphforge_storage::SemanticRouteKind::Relation => (
                "edge",
                "r",
                "edge_uuid",
                format!(
                    "()-[r:`{}`]->()",
                    binding.symbol.ambiguity_candidate().replace('`', "``")
                ),
            ),
            _ => continue,
        };
        cancellation.checkpoint()?;
        let identities: Vec<_> = selected.map_or_else(
            || vec![None],
            |set| {
                set.iter()
                    .filter(|(k, _)| k == kind)
                    .map(|(_, id)| Some(*id))
                    .collect()
            },
        );
        for selected_id in identities {
            let params = selected_id.map_or_else(HashMap::new, |id| {
                HashMap::from([("id".into(), IrLiteral::Uuid(*id.as_bytes()))])
            });
            let filter = if selected_id.is_some() {
                format!(" WHERE {object}.{identity} = $id")
            } else {
                String::new()
            };
            let query = format!(
                "MATCH {pattern}{filter} RETURN {object}.{identity} AS object_uuid, properties({object}) AS properties"
            );
            let consume = |batch: &arrow::record_batch::RecordBatch| {
                sink.scanned(Scan::Semantic, batch.get_array_memory_size())?;
                let ids = batch
                    .column_by_name("object_uuid")
                    .and_then(|a| a.as_any().downcast_ref::<FixedSizeBinaryArray>())
                    .ok_or_else(invalid)?;
                for row in 0..batch.num_rows() {
                    cancellation.checkpoint()?;
                    let id = Uuid::from_slice(ids.value(row)).map_err(|_| invalid())?;
                    if !part.holds(id) {
                        continue;
                    }
                    for (property, digest) in fields::properties(
                        batch
                            .column_by_name("properties")
                            .ok_or_else(invalid)?
                            .as_ref(),
                        row,
                    )? {
                        emit((kind.into(), id, format!("property:{property}")), digest)?;
                    }
                }
                Ok(())
            };
            if budgeted {
                crate::slices::stream_branch_params(graph, &query, &params, cancellation, consume)?;
            } else {
                crate::slices::stream_branch_unbudgeted(
                    graph,
                    &query,
                    &params,
                    cancellation,
                    consume,
                )?;
            }
        }
    }
    Ok(())
}

pub(crate) fn property_pattern(
    graph: &GraphForge,
    kind: &str,
    id: Uuid,
    property: &str,
    cancellation: &CancellationToken,
) -> Result<String, GfError> {
    let (plain, object, identity, projection) = if kind == "node" {
        ("(n)", "n", "node_uuid", "labels(n)")
    } else {
        ("()-[r]->()", "r", "edge_uuid", "type(r)")
    };
    let Some(context) = graph.default_composition_snapshot() else {
        return Ok(plain.into());
    };
    let bindings = graph
        .semantic_storage_bindings
        .lock()
        .expect("semantic bindings")
        .clone();
    let Some(bindings) = bindings else {
        return Ok(plain.into());
    };
    let mut names = Vec::new();
    crate::slices::stream_branch_params(
        graph,
        &format!("MATCH {plain} WHERE {object}.{identity}=$id RETURN {projection}"),
        &HashMap::from([("id".into(), IrLiteral::Uuid(*id.as_bytes()))]),
        cancellation,
        |batch| {
            for row in 0..batch.num_rows() {
                let value = graphforge_storage::decode_property_value(
                    batch.column(0),
                    batch.schema().field(0),
                    row,
                )?;
                match value {
                    IrLiteral::Str(name) => names.push(name),
                    IrLiteral::List(values) => {
                        for value in values {
                            if let IrLiteral::Str(name) = value {
                                names.push(name);
                            }
                        }
                    }
                    _ => return Err(invalid()),
                }
            }
            Ok(())
        },
    )?;
    let route = if kind == "node" {
        graphforge_storage::SemanticRouteKind::Entity
    } else {
        graphforge_storage::SemanticRouteKind::Relation
    };
    let mut found = None;
    for binding in bindings.bindings.iter().filter(|b| {
        b.route_kind == route
            && names.iter().any(|n| {
                *n == b.symbol.display() || *n == b.route || *n == b.symbol.ambiguity_candidate()
            })
    }) {
        let name = binding.symbol.ambiguity_candidate();
        if let Ok((symbol, _)) =
            context.resolve_owned_property(binding.symbol.kind, &name, property)
        {
            if found.as_ref().is_some_and(|(prior, _)| *prior != symbol) {
                return Err(invalid());
            }
            found = Some((symbol, name));
        }
    }
    Ok(found.map_or_else(
        || plain.into(),
        |(_, name)| {
            if kind == "node" {
                format!("(n:`{}`)", name.replace('`', "``"))
            } else {
                format!("()-[r:`{}`]->()", name.replace('`', "``"))
            }
        },
    ))
}
fn invalid() -> GfError {
    GfError::Validation("research property has ambiguous or invalid semantic owner context".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::branches::fields::{Key, KeySink};
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Collects what the semantic scan emits, with a chosen conflict bound.
    struct Recorder {
        bound: Option<usize>,
        fields: BTreeMap<Key, [u8; 32]>,
    }
    impl Recorder {
        fn new(bound: Option<usize>) -> Self {
            Self {
                bound,
                fields: BTreeMap::new(),
            }
        }
    }
    impl FieldSink for Recorder {
        fn field(&mut self, key: Key, digest: [u8; 32]) -> Result<(), GfError> {
            self.fields.insert(key, digest);
            Ok(())
        }
        fn scanned(&mut self, _scan: Scan, _bytes: usize) -> Result<(), GfError> {
            Ok(())
        }
        fn len(&self) -> usize {
            self.fields.len()
        }
        fn conflict_keys(&self) -> Option<usize> {
            self.bound
        }
    }

    fn key(object: u128, property: &str) -> Key {
        (
            "node".into(),
            Uuid::from_u128(object),
            format!("property:{property}"),
        )
    }

    /// Two binding-major owner scans over `objects` nodes with properties `p`
    /// and `q`. The second owner disagrees on `p` of `conflict_at`. Returns the
    /// most keys any single pass emitted, which is what the driver buffers.
    fn owner_scans(
        objects: u128,
        conflict_at: Option<u128>,
        most_emitted: &AtomicUsize,
    ) -> impl FnMut(&mut dyn FieldSink, Partition, Emit<'_>) -> Result<(), GfError> + '_ {
        move |_sink, part, emit| {
            let mut emitted = 0;
            for owner in 0..2_u8 {
                for object in (0..objects).filter(|object| part.holds(Uuid::from_u128(*object))) {
                    for property in ["p", "q"] {
                        let differs = owner == 1 && property == "p" && conflict_at == Some(object);
                        let digest = [if differs { 2 } else { 1 }; 32];
                        let new_key = owner == 0;
                        emit(key(object, property), digest)?;
                        emitted += usize::from(new_key);
                        most_emitted.fetch_max(emitted, Ordering::Relaxed);
                    }
                }
            }
            Ok(())
        }
    }

    fn refused(error: &GfError) -> bool {
        error
            .to_string()
            .contains("ambiguous or invalid semantic owner context")
    }

    #[test]
    fn an_owner_conflict_on_an_unrequested_key_is_refused() {
        let wanted: BTreeSet<Key> = [key(0, "q")].into();
        let mut sink = KeySink {
            wanted: &wanted,
            seen: BTreeMap::new(),
        };
        let most = AtomicUsize::new(0);
        let error = refuse_owner_conflicts(&mut sink, owner_scans(50, Some(7), &most)).unwrap_err();
        assert!(refused(&error), "{error}");
    }

    #[test]
    fn an_owner_conflict_on_a_requested_key_is_refused() {
        let wanted: BTreeSet<Key> = [key(7, "p")].into();
        let mut sink = KeySink {
            wanted: &wanted,
            seen: BTreeMap::new(),
        };
        let most = AtomicUsize::new(0);
        let error = refuse_owner_conflicts(&mut sink, owner_scans(50, Some(7), &most)).unwrap_err();
        assert!(refused(&error), "{error}");
    }

    #[test]
    fn agreeing_owners_pass_and_hand_over_only_the_requested_keys() {
        let wanted: BTreeSet<Key> = [key(7, "p"), key(9, "q")].into();
        let mut sink = KeySink {
            wanted: &wanted,
            seen: BTreeMap::new(),
        };
        let most = AtomicUsize::new(0);
        refuse_owner_conflicts(&mut sink, owner_scans(50, None, &most)).unwrap();
        assert_eq!(
            sink.seen,
            BTreeMap::from([(key(7, "p"), [1; 32]), (key(9, "q"), [1; 32])])
        );
    }

    #[test]
    fn a_conflict_bound_caps_the_buffer_without_hiding_a_distant_conflict() {
        const BOUND: usize = 8;
        // 500 objects x 2 properties are far above the bound, and the second
        // owner's disagreeing emission comes a whole scan after the first's.
        let most = AtomicUsize::new(0);
        let mut agreeing = Recorder::new(Some(BOUND));
        refuse_owner_conflicts(&mut agreeing, owner_scans(500, None, &most)).unwrap();
        assert_eq!(agreeing.fields.len(), 1000);
        assert!(most.load(Ordering::Relaxed) <= BOUND + 1);

        let most = AtomicUsize::new(0);
        let error = refuse_owner_conflicts(
            &mut Recorder::new(Some(BOUND)),
            owner_scans(500, Some(499), &most),
        )
        .unwrap_err();
        assert!(refused(&error), "{error}");
    }

    /// `people` nodes carrying both composed owners' labels.
    fn composed_graph(people: usize) -> (tempfile::TempDir, GraphForge) {
        use crate::composition_binding_tests::{composed_fixture, install_composition_authority};
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("project");
        let path_text = path.to_str().unwrap();
        let first = GraphForge::new(Some(path_text)).unwrap();
        let (context, _, _) = composed_fixture(graphforge_ontology::ActivationMode::Strict);
        install_composition_authority(&first, &context);
        drop(first);
        let forge = GraphForge::new(Some(path_text)).unwrap();
        forge
            .execute_with_composition(
                &format!(
                    "UNWIND range(1, {people}) AS i CREATE (n:`research:Person`:`genealogy:Person` {{name: 'Ada'}})"
                ),
                context.clone(),
            )
            .unwrap();
        drop(forge);
        let graph = GraphForge::new(Some(path_text)).unwrap();
        graph
            .install_generation_composition_context(&context)
            .unwrap();
        (dir, graph)
    }

    #[test]
    fn partitioned_engine_scan_emits_what_the_unbounded_scan_emits() {
        let (_dir, graph) = composed_graph(40);
        let cancel = CancellationToken::new();
        let mut whole = Recorder::new(None);
        read(&graph, None, true, &mut whole, &cancel).unwrap();
        assert_eq!(
            whole
                .fields
                .keys()
                .filter(|key| key.2 == "property:name")
                .count(),
            40
        );
        // A bound of 3 keys forces repeated splitting of the 40 objects.
        let mut split = Recorder::new(Some(3));
        read(&graph, None, false, &mut split, &cancel).unwrap();
        assert_eq!(split.fields, whole.fields);
    }
}
