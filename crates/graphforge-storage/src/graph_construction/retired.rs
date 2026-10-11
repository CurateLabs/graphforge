//! Construction sessions an earlier build recorded in a retired format.
//!
//! Initial builds run on the bulk builder, so the staged initial-build pipeline
//! (ADR 0058) and the node-index bound that shaped its stage controls are gone.
//! A session recorded by a build that still had them cannot resume: its finish
//! stage controls hash a budgets record that this build serializes differently,
//! and its routes may name the replay the build no longer has. It is refused
//! with a typed error before anything of it runs, and it stays discardable
//! without being opened: [`discard_retired_construction`] removes its tree.

use super::controls::read_bounded_limit;
use super::recovery::remove_owned_directory_tree;
use super::spool::{ChunkRoute, SealRoute};
use super::{
    CHECKPOINT, Checkpoint, ConstructionPublicationState, GfError, MAX_CONTROL_BYTES, OsStr,
    PRIVATE_ROOT, ProcessReservation, SESSION_LOCK, StableDirectory, Uuid, file_link_count, hex,
    storage,
};
use std::path::Path;

const MESSAGE: &str = "this construction session was recorded by an earlier build in a format that is retired and cannot resume; restart the import";

/// The budget field a retired build recorded in every stage control it hashed.
const RETIRED_BUDGET_FIELD: &str = "max_node_index_bytes";

/// The refusal for a session in a retired format.
pub(super) fn refusal() -> GfError {
    GfError::Validation(MESSAGE.to_owned())
}

/// Whether the checkpoint `body` was recorded by a build whose staged initial
/// builds are retired: its budgets name the retired bound, or it recorded a
/// route that only that build had.
pub(super) fn is_retired(body: &[u8], checkpoint: &Checkpoint) -> bool {
    checkpoint.chunk_route == ChunkRoute::Staged
        || checkpoint.seal_route == Some(SealRoute::ReplayStaged)
        || serde_json::from_slice::<serde_json::Value>(body)
            .ok()
            .is_some_and(|value| value["budgets"].get(RETIRED_BUDGET_FIELD).is_some())
}

/// Remove the session `operation_uuid` of the project at `project_dir` if it
/// was recorded in a retired format, without opening it. Returns whether it
/// did; `false` leaves the session to the ordinary discard.
///
/// # Errors
/// Refuses a session another process holds, one that is publishing or
/// published, and returns storage errors.
pub fn discard_retired_construction(
    project_dir: &Path,
    operation_uuid: Uuid,
) -> Result<bool, GfError> {
    let project = StableDirectory::open(project_dir).map_err(storage)?;
    let identity = project.identity();
    let _reservation = ProcessReservation::acquire(format!(
        "{}:{}:{}",
        identity.volume_serial,
        hex(&identity.file_id),
        operation_uuid.simple()
    ))?;
    let absent = |error: &std::io::Error| error.kind() == std::io::ErrorKind::NotFound;
    let private = match project.open_child_directory(OsStr::new(PRIVATE_ROOT)) {
        Ok(private) => private,
        Err(error) if absent(&error) => return Ok(false),
        Err(error) => return Err(storage(error)),
    };
    let name = operation_uuid.simple().to_string();
    let root = match private.open_child_directory(OsStr::new(&name)) {
        Ok(root) => root,
        Err(error) if absent(&error) => return Ok(false),
        Err(error) => return Err(storage(error)),
    };
    let lock = match root.open_child_file(OsStr::new(SESSION_LOCK)) {
        Ok(lock) => lock,
        Err(error) if absent(&error) => return Ok(false),
        Err(error) => return Err(storage(error)),
    };
    if file_link_count(&lock).map_err(storage)? != 1 {
        return Err(storage("construction session lock has unexpected links"));
    }
    if !crate::file_lock::try_lock_exclusive(&lock).map_err(storage)? {
        return Err(storage(
            "construction operation is locked by another process",
        ));
    }
    let mut file = match root.open_child_file(OsStr::new(CHECKPOINT)) {
        Ok(file) => file,
        Err(error) if absent(&error) => return Ok(false),
        Err(error) => return Err(storage(error)),
    };
    let body = read_bounded_limit(&mut file, MAX_CONTROL_BYTES)?;
    drop(file);
    let checkpoint: Checkpoint = serde_json::from_slice(&body).map_err(storage)?;
    if !is_retired(&body, &checkpoint) {
        return Ok(false);
    }
    if matches!(
        checkpoint.publication_state,
        Some(ConstructionPublicationState::Publishing | ConstructionPublicationState::Published)
    ) {
        return Err(storage(
            "published construction belongs to generation recovery",
        ));
    }
    let session_identity = root.identity();
    // The same traversal bound the ordinary discard derives from the budgets.
    let mut remaining = checkpoint
        .budgets
        .max_chunks
        .checked_mul(16)
        .and_then(|limit| {
            u64::try_from(checkpoint.budgets.max_schema_groups)
                .ok()
                .and_then(|groups| limit.checked_add(groups))
        })
        .and_then(|limit| limit.checked_add(4_096))
        .ok_or_else(|| storage("construction discard traversal bound overflow"))?;
    crate::file_lock::unlock(&lock).map_err(storage)?;
    drop(lock);
    drop(root);
    remove_owned_directory_tree(
        &private,
        OsStr::new(&name),
        session_identity,
        &mut remaining,
    )?;
    Ok(true)
}
