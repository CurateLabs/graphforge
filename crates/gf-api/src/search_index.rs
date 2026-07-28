//! Typed explicit search and adjacency indexing on the public Rust facade.

use std::time::{SystemTime, UNIX_EPOCH};

use gf_search::{
    LazyTextRequest, SearchIndexLimits, SearchIndexRequest, TextIndexFreshnessReason,
    TextIndexFreshnessState, inspect_text_index_freshness, prepare_search_index,
};

use super::{CancellationToken, GfError, GraphForge, NodeSelector};

/// Statically distinct text-build and vector-upsert options.
#[derive(Clone, Debug, PartialEq)]
pub enum SearchIndexOptions {
    /// Build, reuse, or atomically replace a Tantivy text index.
    Text {
        /// Explicit string properties, or `None` for stable discovery of all
        /// currently observed string properties on the required label.
        properties: Option<Vec<String>>,
        /// Replace even an exactly matching fresh index when `true`.
        rebuild: bool,
    },
    /// Insert or atomically replace one caller-supplied UUID vector.
    Vector {
        /// Graph-owned selector resolved to UUID before entering the backend.
        node: NodeSelector,
        /// Finite, non-zero vector whose dimension is fixed per label/space.
        vector: Vec<f32>,
        /// Required caller-defined vector space.
        space: String,
    },
}

/// Canonical public text-index build receipt and freshness inspection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TextIndexInspection {
    /// Project generation containing the inspected graph/index snapshot.
    pub project_generation_uuid: uuid::Uuid,
    /// Canonically ordered indexed properties.
    pub properties: Vec<String>,
    /// Current committed search-source generation.
    pub source_generation: u64,
    /// Current committed-source fingerprint.
    pub source_fingerprint: String,
    /// Immutable artifact publication generation, when readable.
    pub artifact_generation: Option<String>,
    /// Artifact-recorded source generation, when readable.
    pub artifact_source_generation: Option<u64>,
    /// Artifact-recorded source fingerprint, when readable.
    pub artifact_source_fingerprint: Option<String>,
    /// Bounded artifact state.
    pub state: TextIndexFreshnessState,
    /// Bounded reason for a non-current state.
    pub reason: Option<TextIndexFreshnessReason>,
}

/// Canonical adjacency build receipt and read-only freshness inspection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdjacencyInspection {
    /// Project generation containing the inspected graph/index snapshot.
    pub project_generation_uuid: uuid::Uuid,
    /// Current canonical topology generation.
    pub source_topology_generation: u64,
    /// SHA-256 identity of canonical source topology tuples.
    pub source_topology_fingerprint: String,
    /// Base topology generation recorded by the artifact manifest.
    pub artifact_source_generation: Option<u64>,
    /// Effective source generation after applying a complete delta chain.
    pub artifact_effective_generation: Option<u64>,
    /// SHA-256 identity of effective canonical artifact tuples.
    pub artifact_fingerprint: Option<String>,
    /// Bounded artifact state.
    pub state: gf_storage::adjacency::AdjacencyFreshnessState,
    /// Bounded reason for a non-current state.
    pub reason: Option<gf_storage::adjacency::AdjacencyFreshnessReason>,
}

impl SearchIndexOptions {
    /// Convert binding field presence into one statically distinct search-index variant.
    ///
    /// The outer `properties` option records whether the caller supplied the
    /// text keyword; its inner option preserves explicit `None` for stable
    /// default-property discovery. Python and Node use this one Rust-owned
    /// boundary so mixed, missing, and incomplete variants cannot drift.
    ///
    /// # Errors
    /// Returns [`GfError::Validation`] when both variants, neither variant, or
    /// an incomplete vector variant is supplied.
    pub fn from_binding_fields(
        properties: Option<Option<Vec<String>>>,
        rebuild: Option<bool>,
        node: Option<NodeSelector>,
        vector: Option<Vec<f32>>,
        space: Option<String>,
    ) -> Result<Self, GfError> {
        let has_text = properties.is_some() || rebuild.is_some();
        let has_vector = node.is_some() || vector.is_some() || space.is_some();
        match (has_text, has_vector) {
            (true, true) => Err(validation(
                "text index fields (properties/rebuild) cannot be combined with vector index fields (node/vector/space)",
            )),
            (false, false) => Err(validation(
                "search index requires text fields (properties/rebuild) or vector fields (node/vector/space)",
            )),
            (true, false) => Ok(Self::Text {
                properties: properties.unwrap_or(None),
                rebuild: rebuild.unwrap_or(false),
            }),
            (false, true) => Ok(Self::Vector {
                node: node.ok_or_else(|| validation("vector index requires node"))?,
                vector: vector.ok_or_else(|| validation("vector index requires vector"))?,
                space: space.ok_or_else(|| validation("vector index requires space"))?,
            }),
        }
    }
}

impl GraphForge {
    /// Explicitly build or update one typed graph-native search artifact.
    ///
    /// Text property discovery and validation, vector membership/dimension
    /// checks, freshness, locking, bounded mutation retry, and atomic
    /// publication remain owned by `gf-search`. Public and persisted identity
    /// is UUID-only; this facade never projects knowledge fields.
    ///
    /// # Errors
    /// Returns [`GfError::Validation`] for an invalid or unknown label, an
    /// unsupported selector, invalid text properties, vector, or space.
    /// Structured storage, execution, and lifecycle errors preserve backend
    /// cancellation, limits, corruption, locking, and concurrent mutation.
    pub fn index_search(
        &self,
        label: &str,
        options: SearchIndexOptions,
    ) -> Result<Option<TextIndexInspection>, GfError> {
        let label_id = self.search_label_id(label)?;
        let mut text_properties = None;
        match options {
            SearchIndexOptions::Text {
                properties,
                rebuild,
            } => {
                prepare_search_index(
                    &self.dir,
                    SearchIndexRequest::Text {
                        label,
                        label_id,
                        properties: properties.as_deref(),
                        rebuild,
                    },
                    SearchIndexLimits::default(),
                    || Ok(()),
                )?;
                text_properties = Some(properties);
            }
            SearchIndexOptions::Vector {
                node,
                vector,
                space,
            } => {
                let node_uuid = self.resolve_node_selector(&node)?;
                prepare_search_index(
                    &self.dir,
                    SearchIndexRequest::Vector {
                        label,
                        label_id,
                        node_uuid: *node_uuid.as_bytes(),
                        vector: &vector,
                        space: &space,
                        updated_at_micros: transaction_time_micros(),
                    },
                    SearchIndexLimits::default(),
                    || Ok(()),
                )?;
            }
        }
        self.publish_workspace_update()?;
        text_properties
            .map(|properties| self.inspect_text_index(label, properties.as_deref()))
            .transpose()
    }

    /// Inspect an explicit or default-discovered text index without building it.
    ///
    /// `None` discovers the same stable string-property projection used by a
    /// default text build. Stale and incompatible artifacts return bounded
    /// state/reason values and are never labeled current.
    ///
    /// # Errors
    /// Returns structured selector, source, corruption, resource, cancellation,
    /// or repeated-concurrent-mutation errors.
    pub fn inspect_text_index(
        &self,
        label: &str,
        properties: Option<&[String]>,
    ) -> Result<TextIndexInspection, GfError> {
        let label_id = self.search_label_id(label)?;
        let inspection = inspect_text_index_freshness(
            &self.dir,
            LazyTextRequest { label, label_id },
            properties,
            gf_search::TextLifecycleLimits::default(),
            || Ok(()),
        )?;
        Ok(TextIndexInspection {
            project_generation_uuid: *self
                .current_generation_uuid
                .lock()
                .expect("generation UUID lock poisoned"),
            properties: inspection.properties,
            source_generation: inspection.source_generation,
            source_fingerprint: inspection.source_fingerprint,
            artifact_generation: inspection.artifact_generation,
            artifact_source_generation: inspection.artifact_source_generation,
            artifact_source_fingerprint: inspection.artifact_source_fingerprint,
            state: inspection.state,
            reason: inspection.reason,
        })
    }

    /// Explicitly build the full derived CSR adjacency index.
    ///
    /// This operation is separate from label-keyed search artifacts, so a
    /// graph label cannot collide with adjacency capability selection.
    ///
    /// # Errors
    /// Returns [`GfError::Storage`] if the derived adjacency build fails.
    pub fn index_adjacency(&self) -> Result<AdjacencyInspection, GfError> {
        self.rebuild_adjacency(None)
    }

    /// Rebuild adjacency with cooperative cancellation and rollback-safe publication.
    pub fn rebuild_adjacency(
        &self,
        cancellation: Option<CancellationToken>,
    ) -> Result<AdjacencyInspection, GfError> {
        let visibility = self
            .graph_visibility
            .lock()
            .expect("graph visibility lock poisoned");
        let adjacency_visibility = self
            .adjacency_visibility
            .write()
            .expect("adjacency visibility lock poisoned");
        let token = cancellation.unwrap_or_default();
        token.checkpoint()?;
        let adjacency = gf_storage::adjacency::adjacency_dir(&self.dir);
        let workspace_parent = self.dir.parent().ok_or_else(|| {
            GfError::Storage("graph workspace has no same-filesystem parent".into())
        })?;
        let staged = tempfile::Builder::new()
            .prefix(".graphforge-adjacency-stage.")
            .tempdir_in(workspace_parent)
            .map_err(|error| GfError::Storage(format!("cannot stage adjacency: {error}")))?;
        gf_storage::adjacency::build_adjacency_index_into(
            &self.dir,
            staged.path(),
            transaction_time_micros(),
            || token.checkpoint(),
        )?;
        let issues =
            gf_storage::adjacency::validate_adjacency_index_against(&self.dir, staged.path())?;
        if !issues.is_empty() {
            return Err(GfError::Validation(format!(
                "staged adjacency failed validation: {issues:?}"
            )));
        }
        #[cfg(test)]
        crate::adjacency_rebuild_barrier::hit(staged.path())?;
        token.checkpoint()?;

        // Keep rollback bytes beside (not inside) the private workspace. The
        // operation-wide visibility lock makes the two directory renames an
        // atomic observation boundary for every reader of this workspace.
        let backup = self.dir.with_file_name(format!(
            ".graphforge-adjacency-backup.{}",
            uuid::Uuid::new_v4().simple()
        ));
        let adjacency_parent = adjacency
            .parent()
            .ok_or_else(|| GfError::Storage("adjacency path has no workspace parent".into()))?;
        std::fs::create_dir_all(adjacency_parent).map_err(|error| {
            GfError::Storage(format!("cannot prepare adjacency destination: {error}"))
        })?;
        let had_prior = adjacency.exists();
        if had_prior {
            std::fs::rename(&adjacency, &backup).map_err(|error| {
                GfError::Storage(format!("cannot preserve prior adjacency artifact: {error}"))
            })?;
        }
        let staged_adjacency = gf_storage::adjacency::adjacency_dir(staged.path());
        if let Err(error) = std::fs::rename(&staged_adjacency, &adjacency) {
            self.adjacency_provider.invalidate();
            let restore = had_prior
                .then(|| std::fs::rename(&backup, &adjacency))
                .transpose()
                .err()
                .map(|restore| restore.to_string());
            return Err(adjacency_swap_error(&error.to_string(), restore.as_deref()));
        }
        self.adjacency_provider.invalidate();
        let prior_generation = *self
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned");
        let publication = self.publish_workspace_update();
        let observed_generation = *self
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned");
        if let Err(error) =
            reconcile_adjacency_publication(prior_generation, observed_generation, publication)
        {
            self.adjacency_provider.invalidate();
            std::fs::remove_dir_all(&adjacency).map_err(|restore| {
                GfError::Storage(format!(
                    "adjacency publication failed ({error}); rollback cleanup failed: {restore}"
                ))
            })?;
            if had_prior {
                std::fs::rename(&backup, &adjacency).map_err(|restore| {
                    GfError::Storage(format!(
                        "adjacency publication failed ({error}); rollback restore failed: {restore}"
                    ))
                })?;
            }
            self.adjacency_provider.invalidate();
            return Err(error);
        }
        self.adjacency_provider.invalidate();
        if had_prior {
            // Publication is already authoritative. A best-effort cleanup
            // failure must not turn a committed rebuild into a false failure.
            let _ = std::fs::remove_dir_all(&backup);
        }
        drop(adjacency_visibility);
        drop(visibility);
        self.inspect_adjacency()
    }

    /// Inspect the derived adjacency artifact without mutating it.
    ///
    /// # Errors
    /// Returns [`GfError::Storage`] when canonical topology cannot be read.
    pub fn inspect_adjacency(&self) -> Result<AdjacencyInspection, GfError> {
        let _visibility = self
            .graph_visibility
            .lock()
            .expect("graph visibility lock poisoned");
        let _adjacency_visibility = self
            .adjacency_visibility
            .read()
            .expect("adjacency visibility lock poisoned");
        let inspection = gf_storage::adjacency::inspect_adjacency_index(&self.dir)?;
        Ok(AdjacencyInspection {
            project_generation_uuid: *self
                .current_generation_uuid
                .lock()
                .expect("generation UUID lock poisoned"),
            source_topology_generation: inspection.source_generation,
            source_topology_fingerprint: inspection.source_fingerprint,
            artifact_source_generation: inspection.artifact_generation,
            artifact_effective_generation: inspection.artifact_effective_generation,
            artifact_fingerprint: inspection.artifact_fingerprint,
            state: inspection.state,
            reason: inspection.reason,
        })
    }

    pub(crate) fn search_label_id(&self, label: &str) -> Result<u32, GfError> {
        validate_label(label)?;
        self.ontology
            .as_ref()
            .and_then(|ontology| ontology.entity_type_id(label).map(|id| id.0))
            .or_else(|| {
                self.runtime_catalog
                    .lock()
                    .expect("runtime catalog poisoned")
                    .entity_type_names_with_ids()
                    .find_map(|(id, name)| (name == label).then_some(id.0))
            })
            .ok_or_else(|| validation(format!("unknown search label {label:?}")))
    }
}

fn reconcile_adjacency_publication(
    prior: uuid::Uuid,
    observed: uuid::Uuid,
    publication: Result<(), GfError>,
) -> Result<(), GfError> {
    match publication {
        Ok(()) => Ok(()),
        Err(_) if observed != prior => Ok(()),
        Err(error) => Err(error),
    }
}

fn adjacency_swap_error(publish: &str, restore: Option<&str>) -> GfError {
    let message = match restore {
        Some(restore) => format!(
            "cannot publish staged adjacency into workspace: {publish}; \
             rollback restore failed: {restore}"
        ),
        None => format!("cannot publish staged adjacency into workspace: {publish}"),
    };
    GfError::Storage(message)
}

fn transaction_time_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            i64::try_from(duration.as_micros()).unwrap_or(i64::MAX)
        })
}

fn validate_label(label: &str) -> Result<(), GfError> {
    if label.is_empty() || label.trim() != label || label.chars().any(char::is_control) {
        Err(validation(format!("invalid search label {label:?}")))
    } else {
        Ok(())
    }
}

fn validation(message: impl Into<String>) -> GfError {
    GfError::Validation(message.into())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use gf_core::uuid::Uuid;
    use gf_storage::{
        SearchArtifactKey, VectorStoreLimits, current_search_artifact, read_vector_snapshot,
    };

    use super::*;
    use crate::{NodeHandle, PropValue};

    fn text(properties: Option<Vec<&str>>, rebuild: bool) -> SearchIndexOptions {
        SearchIndexOptions::Text {
            properties: properties.map(|values| {
                values
                    .into_iter()
                    .map(ToOwned::to_owned)
                    .collect::<Vec<_>>()
            }),
            rebuild,
        }
    }

    fn vector(node: NodeSelector, values: &[f32]) -> SearchIndexOptions {
        SearchIndexOptions::Vector {
            node,
            vector: values.to_vec(),
            space: "semantic".to_owned(),
        }
    }

    fn assert_validation<T>(result: Result<T, GfError>) {
        assert!(matches!(result, Err(GfError::Validation(_))));
    }

    #[test]
    fn binding_fields_select_exactly_one_complete_typed_variant() {
        assert_eq!(
            SearchIndexOptions::from_binding_fields(Some(None), None, None, None, None,).unwrap(),
            text(None, false)
        );
        assert_eq!(
            SearchIndexOptions::from_binding_fields(None, Some(true), None, None, None,).unwrap(),
            text(None, true)
        );

        let node = NodeSelector::Uuid(Uuid::now_v7());
        assert_eq!(
            SearchIndexOptions::from_binding_fields(
                None,
                None,
                Some(node.clone()),
                Some(vec![1.0]),
                Some("semantic".to_owned()),
            )
            .unwrap(),
            SearchIndexOptions::Vector {
                node,
                vector: vec![1.0],
                space: "semantic".to_owned(),
            }
        );

        assert_validation(SearchIndexOptions::from_binding_fields(
            None, None, None, None, None,
        ));
        assert_validation(SearchIndexOptions::from_binding_fields(
            Some(None),
            None,
            Some(NodeSelector::Uuid(Uuid::now_v7())),
            Some(vec![1.0]),
            Some("semantic".to_owned()),
        ));
        assert_validation(SearchIndexOptions::from_binding_fields(
            None,
            None,
            Some(NodeSelector::Uuid(Uuid::now_v7())),
            None,
            Some("semantic".to_owned()),
        ));
    }

    #[test]
    fn text_defaults_validate_explicit_properties_reuse_replace_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let graph = GraphForge::new(Some(path)).unwrap();
        graph
            .execute(
                "CREATE (:Person {summary:'Graph search', age:30, name:'Alice'}), \
                 (:NoText {age:1})",
            )
            .unwrap();

        graph.index_search("Person", text(None, false)).unwrap();
        let key = SearchArtifactKey::text("Person", ["name", "summary"]).unwrap();
        let first = current_search_artifact(&graph.dir, &key).unwrap().unwrap();
        assert_eq!(
            first.manifest.properties.as_deref().unwrap(),
            ["name", "summary"]
        );

        graph.index_search("Person", text(None, false)).unwrap();
        let reused = current_search_artifact(&graph.dir, &key).unwrap().unwrap();
        assert_eq!(reused.path, first.path);

        graph.index_search("Person", text(None, true)).unwrap();
        let replaced = current_search_artifact(&graph.dir, &key).unwrap().unwrap();
        assert_ne!(replaced.path, first.path);

        assert_validation(graph.index_search("Person", text(Some(vec![]), false)));
        assert_validation(graph.index_search("Person", text(Some(vec!["name", "name"]), false)));
        assert_validation(graph.index_search("Person", text(Some(vec!["age"]), false)));
        assert_validation(graph.index_search("Person", text(Some(vec!["missing"]), false)));
        assert_validation(graph.index_search("Missing", text(None, false)));
        assert_validation(graph.index_search(" Person", text(None, false)));

        graph.index_search("NoText", text(None, false)).unwrap();
        let no_text_probe = SearchArtifactKey::text("NoText", ["_"]).unwrap();
        let no_text_label_root = no_text_probe.artifact_root(&graph.dir);
        let no_text_label_root = no_text_label_root
            .parent()
            .expect("text key has a property component");
        assert!(!no_text_label_root.exists());

        drop(graph);
        let reopened = GraphForge::new(Some(path)).unwrap();
        reopened
            .index_search("Person", text(Some(vec!["summary", "name"]), false))
            .unwrap();
        let after_reopen = current_search_artifact(&reopened.dir, &key)
            .unwrap()
            .unwrap();
        assert_eq!(
            after_reopen.path.file_name(),
            replaced.path.file_name(),
            "the immutable artifact identity survives workspace rehydration"
        );
    }

    #[test]
    fn text_receipt_tracks_stale_rebuild_incompatible_and_reopen_transitions() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let graph = GraphForge::new(Some(path)).unwrap();
        graph
            .execute("CREATE (:Person {name:'Alpha', summary:'first'})")
            .unwrap();

        let missing = graph
            .inspect_text_index("Person", Some(&["name".to_owned()]))
            .unwrap();
        assert_eq!(missing.properties, ["name"]);
        assert_eq!(missing.state, TextIndexFreshnessState::Missing);
        assert_eq!(missing.reason, Some(TextIndexFreshnessReason::NotBuilt));
        assert!(missing.artifact_generation.is_none());

        let built = graph
            .index_search("Person", text(Some(vec!["name"]), false))
            .unwrap()
            .unwrap();
        assert_eq!(built.state, TextIndexFreshnessState::Current);
        assert_eq!(built.reason, None);
        assert_eq!(
            built.artifact_source_generation,
            Some(built.source_generation)
        );
        assert_eq!(
            built.artifact_source_fingerprint.as_deref(),
            Some(built.source_fingerprint.as_str())
        );
        let first_artifact = built.artifact_generation.clone().unwrap();

        graph
            .execute("CREATE (:Person {name:'Beta', summary:'second'})")
            .unwrap();
        let stale = graph
            .inspect_text_index("Person", Some(&["name".to_owned()]))
            .unwrap();
        assert_eq!(stale.state, TextIndexFreshnessState::Stale);
        assert_eq!(
            stale.reason,
            Some(TextIndexFreshnessReason::SourceGenerationChanged)
        );
        assert_eq!(
            stale.artifact_generation.as_deref(),
            Some(first_artifact.as_str())
        );

        let rebuilt = graph
            .index_search("Person", text(Some(vec!["name"]), true))
            .unwrap()
            .unwrap();
        assert_eq!(rebuilt.state, TextIndexFreshnessState::Current);
        assert_ne!(
            rebuilt.artifact_generation.as_deref(),
            Some(first_artifact.as_str())
        );
        assert_ne!(
            rebuilt.project_generation_uuid,
            built.project_generation_uuid
        );

        let key = SearchArtifactKey::text("Person", ["name"]).unwrap();
        let artifact = current_search_artifact(&graph.dir, &key).unwrap().unwrap();
        let manifest_path = artifact.path.join("manifest.json");
        let mut manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
        manifest["backend_version"] = serde_json::Value::from("future-backend-v9");
        std::fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
        let incompatible = graph
            .inspect_text_index("Person", Some(&["name".to_owned()]))
            .unwrap();
        assert_eq!(incompatible.state, TextIndexFreshnessState::Incompatible);
        assert_eq!(
            incompatible.reason,
            Some(TextIndexFreshnessReason::BackendVersion)
        );

        let repaired = graph
            .index_search("Person", text(Some(vec!["name"]), true))
            .unwrap()
            .unwrap();
        assert_eq!(repaired.state, TextIndexFreshnessState::Current);
        drop(graph);

        let reopened = GraphForge::new(Some(path)).unwrap();
        let reopened_inspection = reopened
            .inspect_text_index("Person", Some(&["name".to_owned()]))
            .unwrap();
        assert_eq!(reopened_inspection, repaired);
    }

    #[test]
    fn vector_selectors_membership_idempotence_replacement_and_reopen_are_uuid_owned() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let graph = GraphForge::new(Some(path)).unwrap();
        let person = graph
            .add_node(
                "Person",
                &HashMap::from([("name".to_owned(), PropValue::Str("Alice".to_owned()))]),
            )
            .unwrap();
        let animal = graph.add_node("Animal", &HashMap::new()).unwrap();

        graph
            .index_search(
                "Person",
                vector(NodeSelector::Handle(person.clone()), &[1.0, 0.0]),
            )
            .unwrap();
        let key = SearchArtifactKey::vector("Person", "semantic").unwrap();
        let first = current_search_artifact(&graph.dir, &key).unwrap().unwrap();

        graph
            .index_search(
                "Person",
                vector(NodeSelector::Uuid(person.uuid), &[1.0, 0.0]),
            )
            .unwrap();
        let repeated = current_search_artifact(&graph.dir, &key).unwrap().unwrap();
        assert_eq!(repeated.path, first.path);

        graph
            .index_search(
                "Person",
                vector(NodeSelector::Uuid(person.uuid), &[0.0, 1.0]),
            )
            .unwrap();
        let replaced = current_search_artifact(&graph.dir, &key).unwrap().unwrap();
        assert_ne!(replaced.path, first.path);
        let rows = read_vector_snapshot(&replaced.path, 2, VectorStoreLimits::default(), || Ok(()))
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].node_uuid, *person.uuid.as_bytes());
        assert_eq!(rows[0].vector, [0.0, 1.0]);

        assert_validation(graph.index_search(
            "Person",
            vector(NodeSelector::Uuid(Uuid::now_v7()), &[1.0, 0.0]),
        ));
        assert_validation(graph.index_search(
            "Person",
            vector(NodeSelector::Uuid(animal.uuid), &[1.0, 0.0]),
        ));
        assert_validation(graph.index_search(
            "Person",
            SearchIndexOptions::Vector {
                node: NodeSelector::Uuid(person.uuid),
                vector: vec![1.0, 0.0],
                space: String::new(),
            },
        ));

        drop(graph);
        let reopened = GraphForge::new(Some(path)).unwrap();
        reopened
            .index_search(
                "Person",
                vector(NodeSelector::Uuid(person.uuid), &[0.0, 1.0]),
            )
            .unwrap();
        let after_reopen = current_search_artifact(&reopened.dir, &key)
            .unwrap()
            .unwrap();
        assert_eq!(
            after_reopen.path.file_name(),
            replaced.path.file_name(),
            "the immutable artifact identity survives workspace rehydration"
        );
    }

    #[test]
    fn adjacency_is_explicit_and_never_treats_a_search_label_as_magic() {
        let graph = GraphForge::new(None).unwrap();
        graph
            .execute("CREATE (:adjacency {name:'search'}), (:Person)")
            .unwrap();

        let receipt = graph.index_adjacency().unwrap();
        assert_eq!(
            receipt.state,
            gf_storage::adjacency::AdjacencyFreshnessState::Current
        );
        assert_eq!(
            receipt.artifact_fingerprint.as_deref(),
            Some(receipt.source_topology_fingerprint.as_str())
        );
        assert!(
            gf_storage::adjacency::validate_adjacency_index(&graph.dir)
                .unwrap()
                .is_empty()
        );
        graph.index("adjacency").unwrap();

        assert!(matches!(
            graph.index("Person"),
            Err(GfError::NotImplemented("index"))
        ));
        graph.index_search("adjacency", text(None, false)).unwrap();
        let key = SearchArtifactKey::text("adjacency", ["name"]).unwrap();
        assert!(current_search_artifact(&graph.dir, &key).unwrap().is_some());
    }

    #[test]
    fn adjacency_rebuild_cancellation_preserves_prior_authoritative_artifact() {
        let graph = GraphForge::new(None).unwrap();
        graph
            .execute("CREATE (a:Person)-[:KNOWS]->(b:Person)")
            .unwrap();
        let prior = graph.index_adjacency().unwrap();
        let prior_manifest =
            std::fs::read(gf_storage::adjacency::manifest_path(&graph.dir)).unwrap();
        let token = CancellationToken::new();
        token.cancel();

        let error = graph.rebuild_adjacency(Some(token)).unwrap_err();
        assert_eq!(error.code(), "GF_CANCELLED");
        assert_eq!(
            std::fs::read(gf_storage::adjacency::manifest_path(&graph.dir)).unwrap(),
            prior_manifest
        );
        assert_eq!(graph.inspect_adjacency().unwrap(), prior);
    }

    #[test]
    fn adjacency_barrier_cancellation_and_follow_on_rebuild_are_deterministic() {
        use std::sync::{Arc, mpsc};
        use std::time::Duration;
        let _serial = crate::adjacency_rebuild_barrier::serial_test_guard();
        let directory = tempfile::tempdir().unwrap();
        let project = directory.path().join("project");
        std::fs::create_dir(&project).unwrap();
        let path = project.to_str().unwrap();
        let graph = Arc::new(GraphForge::new(Some(path)).unwrap());
        graph
            .execute("CREATE (a:Person)-[:KNOWS]->(b:Person)")
            .unwrap();
        graph.index_adjacency().unwrap();
        let manifest = gf_storage::adjacency::manifest_path(&graph.dir);
        let prior_manifest = std::fs::read(&manifest).unwrap();
        graph
            .execute("CREATE (c:Person)-[:KNOWS]->(d:Person)")
            .unwrap();
        let before_cancel = graph.inspect_adjacency().unwrap();
        let generation_before_cancel = before_cancel.project_generation_uuid;
        let controller = crate::adjacency_rebuild_barrier::Controller::arm().unwrap();
        let cookie = controller.cookie();
        let cancellation = CancellationToken::new();
        let worker_token = cancellation.clone();
        let worker_graph = Arc::clone(&graph);
        let (worker_sent, worker_received) = mpsc::sync_channel(1);
        let worker = std::thread::spawn(move || {
            let _cookie = crate::adjacency_rebuild_barrier::present(cookie);
            let result = worker_graph.rebuild_adjacency(Some(worker_token));
            worker_sent
                .try_send(result)
                .expect("controller keeps the bounded worker channel open");
        });
        let cancelled_stage = controller.wait().unwrap();
        cancellation.cancel();
        controller.release().unwrap();
        let error = worker_received
            .recv_timeout(Duration::from_secs(5))
            .expect(
                "phase=refresh-begun-before-publication cookie_state=matched worker=result_timeout",
            )
            .unwrap_err();
        worker.join().unwrap();
        assert_eq!(error.code(), "GF_CANCELLED");
        assert_eq!(std::fs::read(&manifest).unwrap(), prior_manifest);
        assert_eq!(graph.inspect_adjacency().unwrap(), before_cancel);
        assert!(!cancelled_stage.exists());
        drop(controller);
        drop(graph);
        let reopened = Arc::new(GraphForge::new(Some(path)).unwrap());
        assert_eq!(reopened.inspect_adjacency().unwrap(), before_cancel);
        assert_eq!(
            std::fs::read(gf_storage::adjacency::manifest_path(&reopened.dir)).unwrap(),
            prior_manifest
        );
        let (rebuilt, rebuilt_stage) = rebuild_through_barrier(Arc::clone(&reopened));
        assert_eq!(
            rebuilt.state,
            gf_storage::adjacency::AdjacencyFreshnessState::Current
        );
        assert_eq!(
            rebuilt.artifact_fingerprint.as_deref(),
            Some(rebuilt.source_topology_fingerprint.as_str())
        );
        assert_ne!(rebuilt.project_generation_uuid, generation_before_cancel);
        assert!(!rebuilt_stage.exists());
        drop(reopened);
        let final_reopen = GraphForge::new(Some(path)).unwrap();
        assert_eq!(final_reopen.inspect_adjacency().unwrap(), rebuilt);
        assert!(!rebuilt_stage.exists());
    }

    fn rebuild_through_barrier(
        graph: std::sync::Arc<GraphForge>,
    ) -> (AdjacencyInspection, std::path::PathBuf) {
        let controller = crate::adjacency_rebuild_barrier::Controller::arm().unwrap();
        let cookie = controller.cookie();
        let (sent, received) = std::sync::mpsc::sync_channel(1);
        let worker = std::thread::spawn(move || {
            let _cookie = crate::adjacency_rebuild_barrier::present(cookie);
            sent.try_send(graph.rebuild_adjacency(None)).unwrap();
        });
        let staged = controller.wait().unwrap();
        controller.release().unwrap();
        let rebuilt = received
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap()
            .unwrap();
        worker.join().unwrap();
        (rebuilt, staged)
    }

    #[test]
    fn adjacency_reader_and_rebuild_share_the_visibility_boundary() {
        use std::sync::{Arc, mpsc};
        use std::time::Duration;

        let graph = Arc::new(GraphForge::new(None).unwrap());
        graph
            .execute("CREATE (a:Person)-[:KNOWS]->(b:Person)")
            .unwrap();
        graph.index_adjacency().unwrap();
        let held_write = graph
            .adjacency_visibility
            .write()
            .expect("adjacency visibility lock poisoned");
        let reader = Arc::clone(&graph);
        let (reader_sent, reader_received) = mpsc::channel();
        let reader_join = std::thread::spawn(move || {
            reader_sent
                .send(reader.rank("Person", crate::RankOptions::default()))
                .unwrap();
        });
        assert!(
            reader_received
                .recv_timeout(Duration::from_millis(50))
                .is_err()
        );
        drop(held_write);
        reader_received
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        reader_join.join().unwrap();

        let held_read = graph
            .adjacency_visibility
            .read()
            .expect("adjacency visibility lock poisoned");
        let worker = Arc::clone(&graph);
        let (sent, received) = mpsc::channel();
        let join = std::thread::spawn(move || {
            sent.send(worker.rebuild_adjacency(None)).unwrap();
        });
        assert!(received.recv_timeout(Duration::from_millis(50)).is_err());
        drop(held_read);
        assert_eq!(
            received
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap()
                .state,
            gf_storage::adjacency::AdjacencyFreshnessState::Current
        );
        join.join().unwrap();
    }

    #[test]
    fn confirmed_current_reconciles_a_post_commit_publication_error() {
        let prior = Uuid::now_v7();
        let committed = Uuid::now_v7();
        let injected = GfError::Storage("post-CURRENT failure".into());
        assert!(reconcile_adjacency_publication(prior, committed, Err(injected)).is_ok());
        let uncommitted = GfError::Storage("pre-CURRENT failure".into());
        assert_eq!(
            reconcile_adjacency_publication(prior, prior, Err(uncommitted))
                .unwrap_err()
                .to_string(),
            "storage error: pre-CURRENT failure"
        );
    }

    #[test]
    fn adjacency_swap_failure_reports_publish_and_restore_causes() {
        assert_eq!(
            adjacency_swap_error("publish-cause", None).to_string(),
            "storage error: cannot publish staged adjacency into workspace: publish-cause"
        );
        assert_eq!(
            adjacency_swap_error("publish-cause", Some("restore-cause")).to_string(),
            "storage error: cannot publish staged adjacency into workspace: publish-cause; \
             rollback restore failed: restore-cause"
        );
    }

    #[test]
    fn cross_facade_rebuild_conflict_restores_the_losing_workspace() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().to_str().unwrap();
        let first = GraphForge::new(Some(path)).unwrap();
        first
            .execute("CREATE (a:Person)-[:KNOWS]->(b:Person)")
            .unwrap();
        let winner = GraphForge::new(Some(path)).unwrap();
        let loser = GraphForge::new(Some(path)).unwrap();
        assert_eq!(
            loser.inspect_adjacency().unwrap().state,
            gf_storage::adjacency::AdjacencyFreshnessState::Missing
        );
        let committed = winner.rebuild_adjacency(None).unwrap();
        assert_eq!(
            loser.rebuild_adjacency(None).unwrap_err().code(),
            "GF_VALIDATION"
        );
        assert_eq!(
            loser.inspect_adjacency().unwrap().state,
            gf_storage::adjacency::AdjacencyFreshnessState::Missing
        );
        drop((first, winner, loser));
        assert_eq!(
            GraphForge::new(Some(path))
                .unwrap()
                .inspect_adjacency()
                .unwrap(),
            committed
        );
    }

    #[test]
    fn cross_graph_handles_are_rejected_before_search_storage_is_touched() {
        let graph = GraphForge::new(None).unwrap();
        graph.execute("CREATE (:Person)").unwrap();
        let other = GraphForge::new(None).unwrap();
        other.execute("CREATE (:Person)").unwrap();
        let foreign = NodeHandle::new(Uuid::now_v7(), "Person", graph.identity.clone());

        assert_validation(
            other.index_search("Person", vector(NodeSelector::Handle(foreign), &[1.0])),
        );
        assert!(!other.dir.join("embeddings").exists());
    }
}
