//! Encoding publication for graph construction.

use super::{
    Checkpoint, ConstructionPublicationIntent, ConstructionPublicationReceipt,
    ConstructionPublicationState, ConstructionShape, DetailCodec, Digest, GfError,
    GraphConstructionEncoding, GraphConstructionSession, GraphConstructionState, OsStr,
    PRIVATE_ROOT, PUBLICATION_INTENT, PUBLICATION_RECEIPT, Path, PathBuf, Sha256, StableDirectory,
    Uuid, account_encoding_cache_release, checked_evidence_sum, construction_failpoint,
    control_sha256, current_parent_generation, decode_bounded, hex, install_control,
    ordinal_publication_tombstones, read_completed_shape, read_completed_shape_outputs,
    record_encoded_active_artifacts, record_encoding_io_evidence, reject_cancelled,
    replace_checkpoint_control, shape_authority_sha256, storage, supersession, validate_sha256,
};

impl GraphConstructionSession {
    /// Authenticate an already committed publication without preparing private
    /// encoding, using its exact immutable generation after `CURRENT` advances.
    /// Returns `None` if the transaction has not committed yet.
    ///
    /// # Errors
    /// Refuses changed session, transaction, generation or successor payload authority.
    pub fn replay_committed_publication(
        &mut self,
        target_generation_uuid: Uuid,
        transaction_uuid: Uuid,
        mut cancelled: impl FnMut() -> bool,
    ) -> Result<Option<crate::ProjectPublicationReceipt>, GfError> {
        reject_cancelled(&mut cancelled)?;
        self.revalidate_authority()?;
        if !matches!(
            self.checkpoint.publication_state,
            Some(
                ConstructionPublicationState::Publishing | ConstructionPublicationState::Published
            )
        ) {
            return Ok(None);
        }
        let Some(published) =
            crate::published_project_transaction(&self.project_path, transaction_uuid)?
        else {
            if self.publication_committed() {
                return Err(storage("published construction transaction is absent"));
            }
            return Ok(None);
        };
        if published.generation_uuid != target_generation_uuid {
            return Err(storage("published construction target changed"));
        }
        self.finish_publication_cancellable(
            target_generation_uuid,
            &hex(&published.generation_manifest_sha256),
            &mut cancelled,
        )?;
        Ok(Some(published))
    }

    /// Durably bind the sealed private inventory to the one target that the
    /// existing project publisher will stage. This records replay authority;
    /// it does not install objects, stage a generation, or mutate `CURRENT`.
    #[allow(
        dead_code,
        reason = "consumed by the next #932 publisher integration slice"
    )]
    pub(crate) fn begin_publication(
        &mut self,
        target_generation_uuid: Uuid,
        transaction_uuid: Uuid,
    ) -> Result<ConstructionPublicationIntent, GfError> {
        self.revalidate_authority()?;
        recover_publication(&self.project_path, &self.root, &mut self.checkpoint)?;
        if self.checkpoint.publication_state == Some(ConstructionPublicationState::Publishing) {
            let intent = read_publication_intent(&self.root, &self.checkpoint)?;
            if intent.target_generation_uuid == target_generation_uuid
                && intent.transaction_uuid == transaction_uuid
            {
                return Ok(intent);
            }
            return Err(storage("publication replay target changed"));
        }
        if self.checkpoint.state != GraphConstructionState::Sealed
            || self.checkpoint.publication_state != Some(ConstructionPublicationState::Sealed)
        {
            return Err(storage("only a sealed session can begin publication"));
        }
        let shape_authority_sha256 = self
            .checkpoint
            .shape_authority_sha256
            .clone()
            .ok_or_else(|| storage("publication requires completed shape authority"))?;
        let encoding_inventory_sha256 = self
            .checkpoint
            .encoding_inventory_sha256
            .clone()
            .ok_or_else(|| storage("publication requires encoded inventory authority"))?;
        let intent = ConstructionPublicationIntent {
            format_version: self.checkpoint.format_version,
            operation_uuid: self.checkpoint.operation_uuid,
            project_identity: self.checkpoint.project_identity.clone(),
            session_identity: self.checkpoint.session_identity.clone(),
            parent_generation_uuid: self.checkpoint.parent_generation_uuid,
            parent_generation_manifest_sha256: self
                .checkpoint
                .parent_generation_manifest_sha256
                .clone(),
            target_generation_uuid,
            transaction_uuid,
            shape_authority_sha256,
            encoding_inventory_sha256,
        };
        validate_publication_intent(&intent, &self.checkpoint)?;
        install_control(&self.root, PUBLICATION_INTENT, &intent)?;
        self.checkpoint.publication_state = Some(ConstructionPublicationState::Publishing);
        replace_checkpoint_control(&self.root, &self.checkpoint)?;
        Ok(intent)
    }

    /// Record the sole project publisher's exact durable result. Replay must
    /// supply the same target generation and manifest digest.
    #[allow(
        dead_code,
        reason = "consumed by the next #932 publisher integration slice"
    )]
    pub(crate) fn finish_publication(
        &mut self,
        target_generation_uuid: Uuid,
        target_generation_manifest_sha256: &str,
    ) -> Result<ConstructionPublicationReceipt, GfError> {
        self.finish_publication_cancellable(
            target_generation_uuid,
            target_generation_manifest_sha256,
            &mut || false,
        )
    }

    fn finish_publication_cancellable(
        &mut self,
        target_generation_uuid: Uuid,
        target_generation_manifest_sha256: &str,
        cancelled: &mut impl FnMut() -> bool,
    ) -> Result<ConstructionPublicationReceipt, GfError> {
        recover_publication_cancellable(
            &self.project_path,
            &self.root,
            &mut self.checkpoint,
            cancelled,
        )?;
        if self.checkpoint.publication_state == Some(ConstructionPublicationState::Published) {
            let receipt = read_publication_receipt(&self.root, &self.checkpoint)?;
            authenticate_published_target(
                &self.project_path,
                &mut self.checkpoint,
                &receipt,
                cancelled,
            )?;
            if receipt.target_generation_uuid == target_generation_uuid
                && receipt.target_generation_manifest_sha256 == target_generation_manifest_sha256
            {
                return Ok(receipt);
            }
            return Err(storage("published replay result changed"));
        }
        if self.checkpoint.publication_state != Some(ConstructionPublicationState::Publishing) {
            return Err(storage("publication has no durable intent"));
        }
        validate_sha256(
            target_generation_manifest_sha256,
            "target generation manifest",
        )?;
        let intent = read_publication_intent(&self.root, &self.checkpoint)?;
        if intent.target_generation_uuid != target_generation_uuid {
            return Err(storage("published target differs from durable intent"));
        }
        let provisional = ConstructionPublicationReceipt {
            operation_uuid: self.checkpoint.operation_uuid,
            project_identity: self.checkpoint.project_identity.clone(),
            session_identity: self.checkpoint.session_identity.clone(),
            intent_sha256: control_sha256(&intent)?,
            transaction_uuid: intent.transaction_uuid,
            target_generation_uuid,
            target_generation_manifest_sha256: target_generation_manifest_sha256.to_owned(),
        };
        authenticate_published_target(
            &self.project_path,
            &mut self.checkpoint,
            &provisional,
            cancelled,
        )?;
        let receipt = provisional;
        install_control(&self.root, PUBLICATION_RECEIPT, &receipt)?;
        self.checkpoint.publication_state = Some(ConstructionPublicationState::Published);
        replace_checkpoint_control(&self.root, &self.checkpoint)?;
        Ok(receipt)
    }

    /// Encode a completed canonical shape into private, ordinary GraphForge
    /// graph/index artifacts. This does not publish either topology generation
    /// or project `CURRENT`; the generation-last publisher consumes the sealed
    /// inventory later.
    pub fn encode_canonical(
        &mut self,
        shape: &ConstructionShape,
        generation: u64,
    ) -> Result<GraphConstructionEncoding, GfError> {
        self.encode_canonical_with_cancellation(shape, generation, || false)
    }

    /// Cancellation-aware form of [`Self::encode_canonical`]. Installed files
    /// remain private and are deterministically regenerated on resume; only the
    /// final inventory is authoritative.
    pub fn encode_canonical_with_cancellation(
        &mut self,
        shape: &ConstructionShape,
        generation: u64,
        mut cancelled: impl FnMut() -> bool,
    ) -> Result<GraphConstructionEncoding, GfError> {
        self.revalidate_authority()?;
        self.reclaim_superseded_payloads_cancellable(&mut cancelled)?;
        if self.checkpoint.state != GraphConstructionState::Sealed
            || self.checkpoint.publication_state != Some(ConstructionPublicationState::Sealed)
        {
            return Err(storage("only a sealed session can be encoded"));
        }
        let completed = read_completed_shape(&self.root, &self.checkpoint, false)?
            .map(|(shape, _)| shape)
            .ok_or_else(|| storage("canonical shape is not complete"))?;
        if &completed != shape {
            return Err(storage("encoder input differs from completed shape"));
        }
        let shape_outputs = read_completed_shape_outputs(&self.root, &self.checkpoint)?;
        let shape_authority = shape_authority_sha256(shape, &shape_outputs)?;
        if self.checkpoint.shape_authority_sha256.as_deref() != Some(&shape_authority) {
            return Err(storage("encoder shape authority differs from checkpoint"));
        }
        let (parent_uuid, parent_digest, parent) = current_parent_generation(&self.project_path)?;
        if parent_uuid != self.checkpoint.parent_generation_uuid
            || parent_digest != self.checkpoint.parent_generation_manifest_sha256
        {
            return Err(storage("construction ordinal parent is no longer CURRENT"));
        }
        let encoded = crate::graph_construction_encoding::encode(
            &self.root,
            DetailCodec::from_version(self.checkpoint.format_version).map_err(storage)?,
            shape,
            generation,
            self.checkpoint.ontology_mode,
            self.base_snapshot.as_ref(),
            parent.as_ref(),
            self.semantic_authority.as_ref(),
            &shape_outputs,
            &shape_authority,
            self.checkpoint.encoding_inventory_sha256.as_deref(),
            self.checkpoint.budgets,
            &mut cancelled,
        )?;
        record_encoded_active_artifacts(&self.root, &encoded, &mut self.checkpoint.evidence)?;
        record_encoding_io_evidence(&mut self.checkpoint.evidence, &encoded)?;
        account_encoding_cache_release(
            &encoded.invocation.evidence,
            &mut self.checkpoint.evidence,
        )?;
        let inventory_authority =
            crate::graph_construction_encoding::inventory_authority_sha256(&encoded)?;
        match self.checkpoint.encoding_inventory_sha256.as_deref() {
            Some(expected) if expected != inventory_authority => {
                return Err(storage(
                    "encoded inventory differs from checkpoint authority",
                ));
            }
            Some(_) => {}
            None => {
                self.checkpoint.encoding_inventory_sha256 = Some(inventory_authority);
                replace_checkpoint_control(&self.root, &self.checkpoint)?;
            }
        }
        self.reclaim_superseded_payloads_cancellable(&mut cancelled)?;
        Ok(encoded)
    }

    /// Install the authenticated canonical inventory into the project CAS and
    /// publish exactly one project generation from the session's pinned parent.
    /// The CAS lease remains held through the sole `CURRENT` replacement.
    #[allow(clippy::too_many_lines)]
    pub fn publish_canonical(
        &mut self,
        encoding: &GraphConstructionEncoding,
        target_generation_uuid: Uuid,
        transaction_uuid: Uuid,
    ) -> Result<crate::ProjectPublicationReceipt, GfError> {
        self.publish_canonical_with_cancellation(
            encoding,
            target_generation_uuid,
            transaction_uuid,
            || false,
            None,
        )
    }

    /// Publish while polling cancellation through the final pre-`CURRENT`
    /// boundary. When `preparation` is `Some`, it runs against the durable,
    /// lease-verified candidate generation immediately before `CURRENT`;
    /// preparation failure leaves `CURRENT` unchanged. Replay paths never
    /// invoke the callback because `CURRENT` already names the target.
    #[allow(clippy::too_many_lines)]
    pub fn publish_canonical_with_cancellation(
        &mut self,
        encoding: &GraphConstructionEncoding,
        target_generation_uuid: Uuid,
        transaction_uuid: Uuid,
        mut cancelled: impl FnMut() -> bool,
        mut preparation: Option<crate::project_publication::ReaderPreparation<'_>>,
    ) -> Result<crate::ProjectPublicationReceipt, GfError> {
        reject_cancelled(&mut cancelled)?;
        if self.checkpoint.publication_state == Some(ConstructionPublicationState::Published) {
            let published =
                crate::published_project_transaction(&self.project_path, transaction_uuid)?
                    .ok_or_else(|| storage("published construction transaction is absent"))?;
            if published.generation_uuid != target_generation_uuid {
                return Err(storage("published construction target changed"));
            }
            self.finish_publication_cancellable(
                target_generation_uuid,
                &hex(&published.generation_manifest_sha256),
                &mut cancelled,
            )?;
            return Ok(published);
        }
        if self.checkpoint.publication_state == Some(ConstructionPublicationState::Publishing) {
            self.begin_publication(target_generation_uuid, transaction_uuid)?;
            if let Some(published) =
                crate::published_project_transaction(&self.project_path, transaction_uuid)?
            {
                if published.generation_uuid != target_generation_uuid {
                    return Err(storage("published construction target changed"));
                }
                self.finish_publication_cancellable(
                    target_generation_uuid,
                    &hex(&published.generation_manifest_sha256),
                    &mut cancelled,
                )?;
                return Ok(published);
            }
        }
        let authentication =
            crate::concurrency_attribution::RegionScope::named("publication_authentication");
        let expected_inventory = self
            .checkpoint
            .encoding_inventory_sha256
            .as_deref()
            .ok_or_else(|| storage("construction publication requires encoded inventory"))?;
        if crate::graph_construction_encoding::inventory_authority_sha256(encoding)?
            != expected_inventory
        {
            return Err(storage("publication encoding authority changed"));
        }
        if encoding.generation
            != self
                .checkpoint
                .parent_topology_generation
                .checked_add(1)
                .ok_or_else(|| storage("publication topology generation overflows"))?
        {
            return Err(storage("publication topology generation changed"));
        }
        let inventory_control =
            crate::graph_construction_encoding::authenticate_inventory_control_for_publication(
                &self.root, encoding,
            )?;
        self.checkpoint.evidence.publication_application_read_bytes = self
            .checkpoint
            .evidence
            .publication_application_read_bytes
            .checked_add(inventory_control.read_bytes)
            .ok_or_else(|| storage("publication read byte count overflows"))?;
        self.checkpoint
            .evidence
            .publication_application_read_operations = self
            .checkpoint
            .evidence
            .publication_application_read_operations
            .checked_add(inventory_control.read_calls)
            .ok_or_else(|| storage("publication read operation count overflows"))?;
        let admission = crate::filesystem_admission::admit_project_lifecycle(
            &self.project_path,
            self.checkpoint.lifecycle_mode,
            crate::filesystem_admission::ProjectRootRequirement::Existing,
        )?;
        admission.revalidate_identity()?;
        let parent = crate::resolve_project_generation(admission.root())?;
        if parent.generation_uuid() != self.checkpoint.parent_generation_uuid
            || hex(&parent.manifest_sha256()) != self.checkpoint.parent_generation_manifest_sha256
        {
            return Err(storage("construction parent is no longer CURRENT"));
        }

        let mut lease = crate::begin_graph_object_publication(admission.root())?;
        lease.set_allocation_operation(self.root.allocation().cloned());
        let (mut manifest_state, manifest_read_bytes, manifest_read_calls) =
            match parent.declared_graph_files_participant()? {
                Some(crate::GraphFilesParticipant::V2(root)) => {
                    let (state, evidence) = crate::graph_object_store::GraphManifestState::open(
                        &lease,
                        root,
                        crate::GraphManifestLimits::default(),
                    )?;
                    (
                        state,
                        evidence
                            .decoded_bytes
                            .checked_add(evidence.authority_read_bytes)
                            .ok_or_else(|| storage("manifest authority read bytes overflow"))?,
                        evidence.application_read_calls,
                    )
                }
                Some(crate::GraphFilesParticipant::V1(inventory)) if inventory.file_count == 0 => {
                    (crate::graph_object_store::GraphManifestState::empty(), 0, 0)
                }
                Some(crate::GraphFilesParticipant::V1(_)) => {
                    return Err(storage(
                        "nonempty construction parent requires compact graph root",
                    ));
                }
                None => (crate::graph_object_store::GraphManifestState::empty(), 0, 0),
            };
        self.checkpoint.evidence.publication_application_read_bytes = self
            .checkpoint
            .evidence
            .publication_application_read_bytes
            .checked_add(manifest_read_bytes)
            .ok_or_else(|| storage("publication manifest read byte count overflows"))?;
        self.checkpoint
            .evidence
            .publication_application_read_operations = self
            .checkpoint
            .evidence
            .publication_application_read_operations
            .checked_add(manifest_read_calls)
            .ok_or_else(|| storage("publication manifest read call count overflows"))?;
        for retained in &encoding.retained_artifacts {
            let entry = manifest_state
                .entries()
                .find(|entry| entry.relative_path == retained.target_path)
                .ok_or_else(|| storage("retained construction object is absent from parent"))?;
            if entry.byte_length != retained.bytes || entry.content_sha256 != retained.sha256 {
                return Err(storage("retained construction object authority changed"));
            }
        }

        let workspace = self
            .project_path
            .join(PRIVATE_ROOT)
            .join(self.checkpoint.operation_uuid.simple().to_string())
            .join(&encoding.root)
            .join("graph");
        let workspace_identity =
            graphforge_filesystem::path_identity(&workspace).map_err(storage)?;
        let encoded_directory = self
            .root
            .open_child_directory(OsStr::new(&encoding.root))
            .map_err(storage)?
            .open_child_directory(OsStr::new("graph"))
            .map_err(storage)?;
        if workspace_identity != encoded_directory.identity() {
            return Err(storage("encoded workspace path identity changed"));
        }
        let sealed_files = encoding
            .artifacts
            .iter()
            .map(
                |artifact| crate::graph_object_store::AuthenticatedGraphFile {
                    relative_path: PathBuf::from(&artifact.path),
                    byte_length: artifact.bytes,
                    content_sha256: artifact.sha256.clone(),
                },
            )
            .collect::<Vec<_>>();
        let (ordinal_tombstones, ordinal_io) =
            ordinal_publication_tombstones(&parent, &manifest_state, &encoded_directory, encoding)?;
        self.checkpoint.evidence.publication_application_read_bytes = self
            .checkpoint
            .evidence
            .publication_application_read_bytes
            .checked_add(ordinal_io.read_bytes)
            .ok_or_else(|| storage("ordinal publication read bytes overflow"))?;
        self.checkpoint
            .evidence
            .publication_application_read_operations = self
            .checkpoint
            .evidence
            .publication_application_read_operations
            .checked_add(ordinal_io.read_calls)
            .ok_or_else(|| storage("ordinal publication read calls overflow"))?;
        drop(authentication);
        let cas_install = crate::concurrency_attribution::RegionScope::named("cas_install");
        let (graph_root, cas_evidence) = {
            let artifact = encoding
                .artifacts
                .iter()
                .find(|artifact| artifact.path == crate::route_component::TABLE_FILE)
                .ok_or_else(|| storage("mapped encoding lacks route authority"))?;
            let entry = crate::GraphFileEntry {
                relative_path: artifact.path.clone(),
                byte_length: artifact.bytes,
                content_sha256: artifact.sha256.clone(),
                role: crate::GraphFileRole::Other,
            };
            let (bytes, calls) = crate::graph_files::read_route_table_counted(&workspace, &entry)?;
            if bytes.len() as u64 != artifact.bytes
                || hex(&Sha256::digest(&bytes)) != artifact.sha256
            {
                return Err(storage("mapped encoding route authority changed"));
            }
            let routes =
                crate::route_component::RouteTable::decode(&bytes, 64 * 1024 * 1024, 100_000)?;
            self.checkpoint.evidence.publication_application_read_bytes = checked_evidence_sum(
                "route publication read bytes",
                self.checkpoint.evidence.publication_application_read_bytes,
                &[bytes.len() as u64],
            )?;
            self.checkpoint
                .evidence
                .publication_application_read_operations = checked_evidence_sum(
                "route publication read calls",
                self.checkpoint
                    .evidence
                    .publication_application_read_operations,
                &[calls],
            )?;
            crate::graph_object_store::append_authenticated_mapped_graph_files(
                &lease,
                &workspace,
                &mut manifest_state,
                &sealed_files,
                &ordinal_tombstones,
                &routes,
            )?
        };
        self.checkpoint.evidence.cas_application_read_bytes = self
            .checkpoint
            .evidence
            .cas_application_read_bytes
            .checked_add(cas_evidence.publication_io.totals()?.read_bytes)
            .ok_or_else(|| storage("CAS read byte count overflows"))?;
        self.checkpoint.evidence.cas_application_read_operations = self
            .checkpoint
            .evidence
            .cas_application_read_operations
            .checked_add(cas_evidence.read_calls)
            .ok_or_else(|| storage("CAS read operation count overflows"))?;
        self.checkpoint.evidence.cas_application_write_bytes = self
            .checkpoint
            .evidence
            .cas_application_write_bytes
            .checked_add(cas_evidence.write_bytes)
            .ok_or_else(|| storage("CAS write byte count overflows"))?;
        self.checkpoint.evidence.cas_application_write_operations = self
            .checkpoint
            .evidence
            .cas_application_write_operations
            .checked_add(cas_evidence.write_calls)
            .ok_or_else(|| storage("CAS write operation count overflows"))?;
        self.checkpoint.evidence.cas_fsync_operations = self
            .checkpoint
            .evidence
            .cas_fsync_operations
            .checked_add(cas_evidence.fsync_calls)
            .ok_or_else(|| storage("CAS fsync count overflows"))?;
        self.checkpoint
            .evidence
            .cas_publication_io
            .checked_add_assign(&cas_evidence.publication_io)?;
        if graphforge_filesystem::path_identity(&workspace).map_err(storage)?
            != encoded_directory.identity()
        {
            return Err(storage(
                "encoded workspace identity changed during CAS install",
            ));
        }
        for artifact in &encoding.artifacts {
            let entry = manifest_state
                .entries()
                .find(|entry| entry.relative_path == artifact.path)
                .ok_or_else(|| storage("installed construction artifact is absent"))?;
            if entry.byte_length != artifact.bytes || entry.content_sha256 != artifact.sha256 {
                return Err(storage("installed construction artifact authority changed"));
            }
        }

        drop(cas_install);
        // CAS installation is private and unreachable until the publication
        // intent and generation are committed. Authenticate all source bytes
        // first so corruption leaves the session sealed and retryable.
        let intent = crate::concurrency_attribution::RegionScope::named("publication_intent");
        self.begin_publication(target_generation_uuid, transaction_uuid)?;
        drop(intent);

        let generation_commit =
            crate::concurrency_attribution::RegionScope::named("generation_commit");
        let graph_participant = crate::graph_files::graph_files_root_participant(&graph_root)?;
        let capabilities = parent
            .capabilities()
            .into_iter()
            .map(|capability| crate::ProjectCapability {
                capability_id: capability.capability_id,
                capability_version: capability.capability_version,
            })
            .collect();
        let mut participants = parent
            .participant_snapshots()?
            .into_iter()
            .filter(|snapshot| {
                snapshot.capability_id != crate::GRAPH_CAPABILITY_ID
                    || snapshot.record_family_id != crate::GRAPH_FILES_FAMILY
            })
            .map(|snapshot| {
                let encoding = match snapshot.encoding.as_str() {
                    "parquet" => crate::ProjectParticipantEncoding::Parquet,
                    "arrow" => crate::ProjectParticipantEncoding::Arrow,
                    "json" => crate::ProjectParticipantEncoding::Json,
                    _ => return Err(storage("parent participant encoding is unsupported")),
                };
                Ok(crate::ProjectParticipant {
                    capability_id: snapshot.capability_id,
                    capability_version: snapshot.capability_version,
                    record_family_id: snapshot.record_family_id,
                    record_version: snapshot.record_version,
                    encoding,
                    schema_fingerprint: snapshot.schema_fingerprint,
                    row_count: snapshot.row_count,
                    bytes: snapshot.bytes,
                })
            })
            .collect::<Result<Vec<_>, GfError>>()?;
        participants.push(graph_participant);
        let request = crate::ProjectGenerationRequest {
            transaction_uuid,
            generation_uuid: target_generation_uuid,
            capabilities,
            participants,
        };
        let publication =
            match crate::project_publication::stage_project_generation_from_admitted_parent(
                admission,
                parent,
                &request,
                None,
                self.root.allocation(),
            )? {
                crate::ProjectStageOutcome::Staged(staged) => {
                    let staged = staged.validate(|_| Ok(()), |_, _| Ok(()))?;
                    match preparation.take() {
                        Some(prepare) => staged
                            .publish_with_graph_objects_preparation_cancellable(
                                &lease,
                                &mut cancelled,
                                prepare,
                            )?,
                        None => staged.publish_with_graph_objects_cancellable(&lease, &mut cancelled)?,
                    }
                }
                crate::ProjectStageOutcome::AlreadyPublished(receipt) => receipt,
            };
        drop(generation_commit);
        construction_failpoint("publication.after_current_before_receipt");
        let receipt = crate::concurrency_attribution::RegionScope::named("publication_receipt");
        self.finish_publication_cancellable(
            target_generation_uuid,
            &hex(&publication.generation_manifest_sha256),
            // CURRENT already committed; finish recording its durable receipt.
            &mut || false,
        )?;
        drop(receipt);
        Ok(publication)
    }

    /// Reopen or create the canonical encoded inventory for a sealed session.
    pub fn prepare_canonical_encoding(
        &mut self,
        generation: u64,
    ) -> Result<GraphConstructionEncoding, GfError> {
        self.prepare_canonical_encoding_with_cancellation(generation, || false)
    }

    /// Reopen or create canonical encoding while polling cooperative cancellation.
    pub fn prepare_canonical_encoding_with_cancellation(
        &mut self,
        generation: u64,
        mut cancelled: impl FnMut() -> bool,
    ) -> Result<GraphConstructionEncoding, GfError> {
        reject_cancelled(&mut cancelled)?;
        self.revalidate_authority()?;
        if self.checkpoint.state != GraphConstructionState::Sealed {
            return Err(storage("only a sealed session can be prepared"));
        }
        if self.checkpoint.encoding_inventory_sha256.is_some() {
            self.reclaim_superseded_payloads_cancellable(&mut cancelled)?;
            let encoded = self
                .root
                .open_child_directory(OsStr::new("encoded-v1"))
                .map_err(storage)?;
            let inventory = crate::graph_construction_encoding::read_inventory(&encoded)?
                .ok_or_else(|| storage("encoded inventory is absent"))?;
            if inventory.generation != generation {
                return Err(storage("encoded inventory generation changed"));
            }
            return Ok(inventory);
        }
        let shape = self.shape_canonical_inner(&mut cancelled)?;
        self.encode_canonical_with_cancellation(&shape, generation, cancelled)
    }

    /// Seal receipt authority and immediately prepare canonical encoding with
    /// one input-authentication pass. If the process stops after the sealed
    /// checkpoint, resume performs that authentication before consuming data.
    #[doc(hidden)]
    pub fn seal_and_prepare_canonical_encoding_with_cancellation(
        &mut self,
        generation: u64,
        mut cancelled: impl FnMut() -> bool,
    ) -> Result<GraphConstructionEncoding, GfError> {
        self.seal_inner(false)?;
        let shape = self.shape_canonical_inner(&mut cancelled)?;
        self.encode_canonical_with_cancellation(&shape, generation, cancelled)
    }
}

fn validate_publication_intent(
    intent: &ConstructionPublicationIntent,
    checkpoint: &Checkpoint,
) -> Result<(), GfError> {
    validate_sha256(
        &intent.parent_generation_manifest_sha256,
        "parent generation manifest",
    )?;
    validate_sha256(&intent.shape_authority_sha256, "shape authority")?;
    validate_sha256(&intent.encoding_inventory_sha256, "encoding inventory")?;
    if intent.format_version != checkpoint.format_version
        || intent.operation_uuid != checkpoint.operation_uuid
        || intent.project_identity != checkpoint.project_identity
        || intent.session_identity != checkpoint.session_identity
        || intent.parent_generation_uuid != checkpoint.parent_generation_uuid
        || intent.parent_generation_manifest_sha256 != checkpoint.parent_generation_manifest_sha256
        || intent.shape_authority_sha256
            != checkpoint
                .shape_authority_sha256
                .clone()
                .unwrap_or_default()
        || intent.encoding_inventory_sha256
            != checkpoint
                .encoding_inventory_sha256
                .clone()
                .unwrap_or_default()
        || intent.target_generation_uuid.is_nil()
        || intent.transaction_uuid.is_nil()
    {
        return Err(storage("publication intent authority changed"));
    }
    Ok(())
}

fn read_publication_intent(
    root: &StableDirectory,
    checkpoint: &Checkpoint,
) -> Result<ConstructionPublicationIntent, GfError> {
    let mut file = root
        .open_child_file(OsStr::new(PUBLICATION_INTENT))
        .map_err(storage)?;
    let intent = decode_bounded(&mut file)?;
    validate_publication_intent(&intent, checkpoint)?;
    Ok(intent)
}

fn read_publication_receipt(
    root: &StableDirectory,
    checkpoint: &Checkpoint,
) -> Result<ConstructionPublicationReceipt, GfError> {
    let intent = read_publication_intent(root, checkpoint)?;
    let mut file = root
        .open_child_file(OsStr::new(PUBLICATION_RECEIPT))
        .map_err(storage)?;
    let receipt: ConstructionPublicationReceipt = decode_bounded(&mut file)?;
    validate_sha256(
        &receipt.target_generation_manifest_sha256,
        "target generation manifest",
    )?;
    if receipt.intent_sha256 != control_sha256(&intent)?
        || receipt.operation_uuid != checkpoint.operation_uuid
        || receipt.project_identity != checkpoint.project_identity
        || receipt.session_identity != checkpoint.session_identity
        || receipt.transaction_uuid != intent.transaction_uuid
        || receipt.target_generation_uuid != intent.target_generation_uuid
    {
        return Err(storage("publication receipt differs from durable intent"));
    }
    Ok(receipt)
}

pub(super) fn recover_publication(
    project_dir: &Path,
    root: &StableDirectory,
    checkpoint: &mut Checkpoint,
) -> Result<(), GfError> {
    recover_publication_cancellable(project_dir, root, checkpoint, &mut || false)
}

fn authenticate_published_target(
    project_dir: &Path,
    checkpoint: &mut Checkpoint,
    receipt: &ConstructionPublicationReceipt,
    cancelled: &mut impl FnMut() -> bool,
) -> Result<(), GfError> {
    let target = crate::resolve_generation_by_uuid(project_dir, receipt.target_generation_uuid)
        .map_err(|error| storage(format!("published target cannot be authenticated: {error}")))?;
    let actual_manifest = hex(&target.manifest_sha256());
    if actual_manifest != receipt.target_generation_manifest_sha256 {
        return Err(storage(
            "published target manifest differs from durable receipt",
        ));
    }
    if target.parent_generation_uuid() != Some(checkpoint.parent_generation_uuid) {
        return Err(storage(
            "published target is not a child of the pinned parent generation",
        ));
    }
    let journal = crate::published_project_transaction(project_dir, receipt.transaction_uuid)
        .map_err(|error| storage(format!("project publication journal is invalid: {error}")))?
        .ok_or_else(|| storage("project publication journal is not published"))?;
    if journal.transaction_uuid != receipt.transaction_uuid
        || journal.generation_uuid != receipt.target_generation_uuid
        || journal.generation_manifest_sha256 != target.manifest_sha256()
    {
        return Err(storage(
            "project publication journal differs from construction publication authority",
        ));
    }
    supersession::authenticate_public_successor(&target, &mut checkpoint.evidence, cancelled)?;
    Ok(())
}

fn recover_publication_cancellable(
    project_dir: &Path,
    root: &StableDirectory,
    checkpoint: &mut Checkpoint,
    cancelled: &mut impl FnMut() -> bool,
) -> Result<(), GfError> {
    let intent = match root.open_child_file(OsStr::new(PUBLICATION_INTENT)) {
        Ok(mut file) => {
            let intent: ConstructionPublicationIntent = decode_bounded(&mut file)?;
            validate_publication_intent(&intent, checkpoint)?;
            Some(intent)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(storage(error)),
    };
    if intent.is_some()
        && checkpoint.publication_state == Some(ConstructionPublicationState::Sealed)
    {
        checkpoint.publication_state = Some(ConstructionPublicationState::Publishing);
        replace_checkpoint_control(root, checkpoint)?;
    }
    let receipt_exists = match root.open_child_file(OsStr::new(PUBLICATION_RECEIPT)) {
        Ok(file) => {
            drop(file);
            true
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => return Err(storage(error)),
    };
    if receipt_exists {
        let receipt = read_publication_receipt(root, checkpoint)?;
        authenticate_published_target(project_dir, checkpoint, &receipt, cancelled)?;
        if checkpoint.publication_state == Some(ConstructionPublicationState::Publishing) {
            checkpoint.publication_state = Some(ConstructionPublicationState::Published);
            replace_checkpoint_control(root, checkpoint)?;
        }
    }
    match checkpoint.publication_state {
        Some(ConstructionPublicationState::Publishing) if intent.is_none() => {
            Err(storage("publishing checkpoint lacks durable intent"))
        }
        Some(ConstructionPublicationState::Published) if !receipt_exists => {
            Err(storage("published checkpoint lacks durable receipt"))
        }
        None | Some(ConstructionPublicationState::Sealed) if intent.is_some() || receipt_exists => {
            Err(storage(
                "publication metadata is inconsistent with session state",
            ))
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests;
