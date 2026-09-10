//! Version-nine predecessor removal under existing durable successor authority.
//! Receipts remain installed so interruption needs no second cleanup journal.
use super::{
    ArtifactReceipt, BLOCK_BYTES, Digest, GfError, GraphConstructionEncoding,
    GraphConstructionEvidence, GraphConstructionSession, OsStr, Read, ReadWork, Sha256,
    StableDirectory, account_cache_release, account_encoding_cache_release,
    canonical_artifact_target, checked_category_remove, compact_parent_inventory,
    construction_failpoint, control_sha256, decode_bounded, file_identity, file_link_count, hex,
    is_shape_artifact_name, read_completed_shape, read_completed_shape_outputs,
    record_active_identity_remove, replace_checkpoint_control, shape_receipt_name, storage,
};

impl GraphConstructionSession {
    pub(super) fn has_encoding_successor(&self) -> bool {
        self.checkpoint.format_version >= 9 && self.checkpoint.encoding_inventory_sha256.is_some()
    }

    pub(super) fn reclaim_superseded_payloads(&mut self) -> Result<(), GfError> {
        self.reclaim_superseded_payloads_cancellable(&mut || false)
    }

    pub(super) fn reclaim_superseded_payloads_cancellable(
        &mut self,
        cancelled: &mut impl FnMut() -> bool,
    ) -> Result<(), GfError> {
        super::reject_cancelled(cancelled)?;
        if self.checkpoint.format_version < 9 || self.checkpoint.shape_authority_sha256.is_none() {
            return Ok(());
        }
        // Retained parent payloads are authenticated through counted CAS reads
        // below. Do not add an uncounted snapshot revalidation pass here.
        self.project.revalidate_named().map_err(storage)?;
        self.root.revalidate_named().map_err(storage)?;
        if !self
            .checkpoint
            .project_identity
            .matches(self.project.identity())
            || !self
                .checkpoint
                .session_identity
                .matches(self.root.identity())
        {
            return Err(storage("supersession private authority changed"));
        }
        let _retained_successor_leases = if self.has_encoding_successor() {
            let output = self
                .root
                .open_child_directory(OsStr::new("encoded-v1"))
                .map_err(storage)?;
            let inventory = crate::graph_construction_encoding::read_inventory(&output)?
                .ok_or_else(|| storage("supersession encoding inventory is absent"))?;
            if Some(crate::graph_construction_encoding::inventory_authority_sha256(&inventory)?)
                != self.checkpoint.encoding_inventory_sha256
                || Some(&inventory.shape_authority_sha256)
                    != self.checkpoint.shape_authority_sha256.as_ref()
            {
                return Err(storage("supersession encoding authority changed"));
            }
            let work = crate::graph_construction_encoding::authenticate_inventory_payloads(
                &output, &inventory, cancelled,
            )?;
            self.record_supersession_reads(work.input_read_bytes, work.input_read_operations)?;
            account_encoding_cache_release(&work, &mut self.checkpoint.evidence)?;
            self.authenticate_retained_successors(&inventory, cancelled)?
        } else {
            read_completed_shape(&self.root, &self.checkpoint, false)?
                .ok_or_else(|| storage("supersession shape is absent"))?;
            for receipt in read_completed_shape_outputs(&self.root, &self.checkpoint)? {
                let work = authenticate_payload(&self.root, &receipt, cancelled)?;
                self.record_supersession_reads(work.bytes, work.operations)?;
                account_cache_release(work.cache_release, &mut self.checkpoint.evidence)?;
            }
            None
        };

        // Authenticate the complete immutable receipt chain BEFORE any removal.
        let mut previous = None;
        for sequence in 0..self.checkpoint.next_sequence {
            super::reject_cancelled(cancelled)?;
            let receipt = self.read_receipt(sequence)?;
            if receipt.prior_receipt_sha256 != previous {
                return Err(storage("supersession receipt chain changed"));
            }
            previous = Some(control_sha256(&receipt)?);
        }
        if previous != self.checkpoint.last_receipt_sha256 {
            return Err(storage("supersession receipt tail changed"));
        }
        supersession_boundary(if self.has_encoding_successor() {
            "supersession.encoded_authenticated"
        } else {
            "supersession.shape_authenticated"
        })?;
        for sequence in 0..self.checkpoint.next_sequence {
            super::reject_cancelled(cancelled)?;
            let receipt = self.read_receipt(sequence)?;
            for artifact in [&receipt.parquet, &receipt.identities, &receipt.details]
                .into_iter()
                .chain(receipt.endpoints.iter())
            {
                self.retire_payload(artifact, false, self.checkpoint.inputs_retired, cancelled)?;
            }
        }
        self.checkpoint.inputs_retired = true;
        supersession_boundary("supersession.before_inputs_checkpoint")?;
        self.checkpoint_supersession()?;
        if self.has_encoding_successor() {
            self.retire_shape_payloads(cancelled)?;
            self.checkpoint.shape_retired = true;
            supersession_boundary("supersession.before_shape_checkpoint")?;
        }
        self.checkpoint_supersession()?;
        supersession_boundary("supersession.after_checkpoint")?;
        Ok(())
    }

    fn retire_shape_payloads(
        &mut self,
        cancelled: &mut impl FnMut() -> bool,
    ) -> Result<(), GfError> {
        for expected in read_completed_shape_outputs(&self.root, &self.checkpoint)? {
            super::reject_cancelled(cancelled)?;
            let mut file = self
                .root
                .open_child_file(OsStr::new(&shape_receipt_name(&expected.name)))
                .map_err(storage)?;
            let actual: ArtifactReceipt = decode_bounded(&mut file)?;
            if actual != expected {
                return Err(storage("supersession completed output receipt changed"));
            }
        }
        // Every surviving shape output has a writer receipt. Keep these
        // controls, including obsolete-run receipts, as retry authority.
        for name in self.root.child_names().map_err(storage)? {
            super::reject_cancelled(cancelled)?;
            let Some(name) = name.to_str() else { continue };
            if !name.starts_with("shape-receipt-") {
                continue;
            }
            let mut file = self
                .root
                .open_child_file(OsStr::new(name))
                .map_err(storage)?;
            if file_link_count(&file).map_err(storage)? != 1 {
                return Err(storage("supersession writer receipt has extra links"));
            }
            let receipt: ArtifactReceipt = decode_bounded(&mut file)?;
            if shape_receipt_name(&receipt.name) != name {
                return Err(storage("supersession writer receipt ownership changed"));
            }
            if !is_shape_artifact_name(&receipt.name) && canonical_artifact_target(&receipt.name) {
                continue;
            }
            if !is_shape_artifact_name(&receipt.name) {
                return Err(storage("supersession writer receipt artifact changed"));
            }
            self.retire_payload(&receipt, true, self.checkpoint.shape_retired, cancelled)?;
        }
        if self
            .checkpoint
            .evidence
            .current_merge_temporary_allocated_bytes
            != 0
        {
            return Err(storage("supersession shape inventory is incomplete"));
        }
        Ok(())
    }

    fn authenticate_retained_successors(
        &mut self,
        encoding: &GraphConstructionEncoding,
        cancelled: &mut impl FnMut() -> bool,
    ) -> Result<
        Option<(
            crate::ResolvedProjectGeneration,
            crate::graph_object_store::GraphObjectReadLease,
        )>,
        GfError,
    > {
        if encoding.retained_artifacts.is_empty() {
            if encoding.evidence.retained_index_runs != 0 {
                return Err(storage("retained-index evidence lacks references"));
            }
            return Ok(None);
        }
        let parent = crate::resolve_generation_by_uuid(
            &self.project_path,
            self.checkpoint.parent_generation_uuid,
        )?;
        if hex(&parent.manifest_sha256()) != self.checkpoint.parent_generation_manifest_sha256 {
            return Err(storage("supersession parent manifest changed"));
        }
        let lease = crate::graph_object_store::begin_graph_object_read(&self.project_path)?;
        let (inventory, work) = compact_parent_inventory(&parent)?;
        self.record_supersession_reads(work.bytes, work.operations)?;
        let inventory =
            inventory.ok_or_else(|| storage("supersession compact parent is absent"))?;
        let manifest = inventory
            .files
            .iter()
            .find(|entry| entry.relative_path == "topology/uuid-membership/manifest.json")
            .ok_or_else(|| storage("supersession parent UUID manifest is absent"))?;
        let (_manifest, work, released) = lease.open_for_construction(
            &manifest.content_sha256,
            manifest.byte_length,
            cancelled,
        )?;
        self.record_supersession_reads(work.read_bytes, work.read_calls)?;
        account_cache_release(released, &mut self.checkpoint.evidence)?;
        let mut previous = None;
        for retained in &encoding.retained_artifacts {
            if previous.is_some_and(|name: &str| name >= retained.target_path.as_str())
                || !retained
                    .target_path
                    .starts_with("topology/uuid-membership/")
                || retained.source_root != self.project_path.to_string_lossy()
                || retained.source_root_volume != self.project.identity().volume_serial
                || retained.source_root_file_id != hex(&self.project.identity().file_id)
                || retained.parent_manifest_sha256 != manifest.content_sha256
            {
                return Err(storage("supersession retained parent authority changed"));
            }
            previous = Some(retained.target_path.as_str());
            let entry = inventory
                .files
                .iter()
                .find(|entry| entry.relative_path == retained.target_path)
                .ok_or_else(|| storage("supersession retained parent artifact is absent"))?;
            let source = crate::graph_object_path(&self.project_path, &entry.content_sha256)?;
            if entry.content_sha256 != retained.sha256
                || entry.byte_length != retained.bytes
                || source
                    .strip_prefix(&self.project_path)
                    .map_err(storage)?
                    .to_string_lossy()
                    != retained.source_path
            {
                return Err(storage("supersession retained parent artifact changed"));
            }
            let (file, work, released) =
                lease.open_for_construction(&entry.content_sha256, entry.byte_length, cancelled)?;
            let identity = file_identity(file.as_ref()).map_err(storage)?;
            if identity.volume_serial != retained.source_volume
                || hex(&identity.file_id) != retained.source_file_id
            {
                return Err(storage("supersession retained parent identity changed"));
            }
            self.record_supersession_reads(work.read_bytes, work.read_calls)?;
            account_cache_release(released, &mut self.checkpoint.evidence)?;
        }
        Ok(Some((parent, lease)))
    }

    fn checkpoint_supersession(&mut self) -> Result<(), GfError> {
        let mut next = self.checkpoint.clone();
        next.evidence.recovery_checkpoint_fsync_operations = next
            .evidence
            .recovery_checkpoint_fsync_operations
            .checked_add(3)
            .ok_or_else(|| storage("supersession checkpoint synchronization count overflow"))?;
        replace_checkpoint_control(&self.root, &next)?;
        self.checkpoint = next;
        Ok(())
    }

    fn record_supersession_reads(&mut self, bytes: u64, operations: u64) -> Result<(), GfError> {
        self.checkpoint.evidence.recovery_application_read_bytes = self
            .checkpoint
            .evidence
            .recovery_application_read_bytes
            .checked_add(bytes)
            .ok_or_else(|| storage("supersession read bytes overflow"))?;
        self.checkpoint
            .evidence
            .recovery_application_read_operations = self
            .checkpoint
            .evidence
            .recovery_application_read_operations
            .checked_add(operations)
            .ok_or_else(|| storage("supersession read operations overflow"))?;
        Ok(())
    }

    fn retire_payload(
        &mut self,
        receipt: &ArtifactReceipt,
        shaped: bool,
        completed: bool,
        cancelled: &mut impl FnMut() -> bool,
    ) -> Result<(), GfError> {
        super::reject_cancelled(cancelled)?;
        let key = format!(
            "{:016x}:{}",
            receipt.identity.volume_serial, receipt.identity.file_id
        );
        let active = self
            .checkpoint
            .evidence
            .storage_active_identity_allocated_bytes
            .get(&key)
            .copied();
        match self.root.open_child_file(OsStr::new(&receipt.name)) {
            Ok(file) => {
                if completed {
                    return Err(storage("supersession retired predecessor reappeared"));
                }
                let identity = file_identity(&file).map_err(storage)?;
                if !receipt.identity.matches(identity)
                    || file_link_count(&file).map_err(storage)? != 1
                {
                    return Err(storage("supersession predecessor identity changed"));
                }
                if active != Some(receipt.allocated_bytes) {
                    return Err(storage(
                        "supersession predecessor allocation authority changed",
                    ));
                }
                drop(file);
                let work = authenticate_payload(&self.root, receipt, cancelled)?;
                self.record_supersession_reads(work.bytes, work.operations)?;
                account_cache_release(work.cache_release, &mut self.checkpoint.evidence)?;
                supersession_boundary("supersession.before_unlink")?;
                self.root
                    .unlink_child_if_identity(OsStr::new(&receipt.name), identity)
                    .map_err(storage)?;
                supersession_boundary("supersession.after_unlink")?;
                if shaped {
                    supersession_boundary("supersession.shape_after_unlink")?;
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if completed {
                    return Ok(());
                }
            }
            Err(error) => return Err(storage(error)),
        }
        // Even a previously missing name must cross the directory durability
        // barrier before a retry releases its still-persisted allocation.
        supersession_boundary("supersession.before_sync")?;
        self.root.sync().map_err(storage)?;
        self.checkpoint
            .evidence
            .recovery_checkpoint_fsync_operations = self
            .checkpoint
            .evidence
            .recovery_checkpoint_fsync_operations
            .checked_add(1)
            .ok_or_else(|| storage("supersession directory synchronization count overflow"))?;
        supersession_boundary("supersession.after_sync")?;
        if shaped {
            supersession_boundary("supersession.shape_after_sync")?;
        }
        if let Some(allocated) = active {
            if allocated != receipt.allocated_bytes {
                return Err(storage(
                    "supersession missing predecessor allocation changed",
                ));
            }
            let category = crate::ArtifactCategory::ConstructionStaging;
            let evidence = &self.checkpoint.evidence;
            let reported = checked_category_remove(
                &evidence.storage_current[&category],
                receipt.bytes,
                allocated,
            )?;
            let authority = checked_category_remove(
                &evidence.storage_receipt_category_authorities[&category],
                receipt.bytes,
                allocated,
            )?;
            let merge_bytes = if shaped {
                evidence
                    .current_merge_temporary_allocated_bytes
                    .checked_sub(allocated)
                    .ok_or_else(|| storage("supersession merge allocation underflow"))?
            } else {
                evidence.current_merge_temporary_allocated_bytes
            };
            let evidence = &mut self.checkpoint.evidence;
            record_active_identity_remove(evidence, &key)?;
            evidence.storage_current.insert(category, reported);
            evidence
                .storage_receipt_category_authorities
                .insert(category, authority);
            evidence.current_merge_temporary_allocated_bytes = merge_bytes;
        }
        Ok(())
    }
}

#[allow(clippy::unnecessary_wraps)] // Test builds inject returned I/O failures at these same boundaries.
fn supersession_boundary(name: &str) -> Result<(), GfError> {
    construction_failpoint(name);
    #[cfg(test)]
    if RETURNED_FAILURE.with(|point| point.borrow().as_deref() == Some(name)) {
        return Err(storage(format!("injected returned failure at {name}")));
    }
    Ok(())
}

pub(super) fn authenticate_public_successor(
    target: &crate::ResolvedProjectGeneration,
    evidence: &mut GraphConstructionEvidence,
    cancelled: &mut impl FnMut() -> bool,
) -> Result<(), GfError> {
    let (inventory, mut work) = compact_parent_inventory(target)?;
    let inventory =
        inventory.ok_or_else(|| storage("published supersession successor is not compact"))?;
    let lease = crate::graph_object_store::begin_graph_object_read(target.container_root())?;
    for entry in &inventory.files {
        let (_file, io, released) =
            lease.open_for_construction(&entry.content_sha256, entry.byte_length, cancelled)?;
        work.bytes = work
            .bytes
            .checked_add(io.read_bytes)
            .ok_or_else(|| storage("successor read bytes overflow"))?;
        work.operations = work
            .operations
            .checked_add(io.read_calls)
            .ok_or_else(|| storage("successor read calls overflow"))?;
        account_cache_release(released, evidence)?;
    }
    evidence.recovery_application_read_bytes = evidence
        .recovery_application_read_bytes
        .checked_add(work.bytes)
        .ok_or_else(|| storage("successor authentication bytes overflow"))?;
    evidence.recovery_application_read_operations = evidence
        .recovery_application_read_operations
        .checked_add(work.operations)
        .ok_or_else(|| storage("successor authentication calls overflow"))?;
    Ok(())
}

#[cfg(test)]
thread_local! {
    static RETURNED_FAILURE: std::cell::RefCell<Option<String>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
pub(super) fn set_returned_failure(point: Option<&str>) {
    RETURNED_FAILURE.with(|current| *current.borrow_mut() = point.map(str::to_owned));
}

fn authenticate_payload(
    root: &StableDirectory,
    receipt: &ArtifactReceipt,
    cancelled: &mut impl FnMut() -> bool,
) -> Result<ReadWork, GfError> {
    let file = root
        .open_child_file(OsStr::new(&receipt.name))
        .map_err(storage)?;
    if !receipt
        .identity
        .matches(file_identity(&file).map_err(storage)?)
        || file_link_count(&file).map_err(storage)? != 1
    {
        return Err(storage("supersession payload identity changed"));
    }
    let mut reader = graphforge_filesystem::FileCacheReleasingReader::new(file).map_err(storage)?;
    let mut work = ReadWork::default();
    let mut digest = Sha256::new();
    let mut block = vec![0; BLOCK_BYTES];
    let result = (|| {
        loop {
            super::reject_cancelled(cancelled)?;
            let count = reader.read(&mut block).map_err(storage)?;
            if count == 0 {
                break;
            }
            digest.update(&block[..count]);
            work.bytes = work
                .bytes
                .checked_add(count as u64)
                .ok_or_else(|| storage("supersession payload size overflow"))?;
            work.operations += 1;
        }
        if work.bytes != receipt.bytes || hex(&digest.finalize()) != receipt.sha256 {
            return Err(storage("supersession payload digest changed"));
        }
        Ok(())
    })();
    let released = reader.finish().map_err(storage);
    match (result, released) {
        (Ok(()), Ok(released)) => {
            work.cache_release = released;
            Ok(work)
        }
        (Err(primary), Ok(_)) => Err(primary),
        (Ok(()), Err(error)) => Err(error),
        (Err(primary), Err(secondary)) => Err(storage(format!(
            "{primary}; cache release failed: {secondary}"
        ))),
    }
}
