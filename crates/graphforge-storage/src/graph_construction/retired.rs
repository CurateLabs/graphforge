//! Construction sessions an earlier build recorded in a retired format.
//!
//! Initial builds run on the bulk builder, so the staged initial-build pipeline
//! (ADR 0058) and the node-index bound that shaped its stage controls are gone.
//! A session recorded by a build that still had them cannot resume: its finish
//! stage controls hash a budgets record that this build serializes differently,
//! and its routes may name the replay the build no longer has. It is refused
//! with a typed error before anything of it runs, and it stays discardable
//! without being opened: [`discard_retired_construction`] removes its tree.

use super::controls::{read_bounded_limit, replace_control};
use super::recovery::{remove_owned_directory_tree, remove_owned_directory_tree_last};
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
/// route that only that build had. A `body` that is not JSON is corrupt and
/// refused, not taken for a current record.
///
/// With `staged_initial`, a session that staged the chunks of an initial build
/// is retired too, whatever else it recorded: an initial build no longer
/// stages, and the shaping, finish-stage and encoding state such a build left
/// can only be the staged pipeline's. The storage engine still stages on an
/// empty project for its own tests, so opening passes `false`; the facade and
/// every discard pass `true`.
pub(super) fn is_retired(
    body: &[u8],
    checkpoint: &Checkpoint,
    staged_initial: bool,
) -> Result<bool, GfError> {
    let value: serde_json::Value = serde_json::from_slice(body).map_err(storage)?;
    Ok(checkpoint.chunk_route == ChunkRoute::Staged
        || checkpoint.seal_route == Some(SealRoute::ReplayStaged)
        || value["budgets"].get(RETIRED_BUDGET_FIELD).is_some()
        || (staged_initial
            && checkpoint.parent_topology_generation == 0
            && checkpoint.chunk_route != ChunkRoute::Spool
            && checkpoint.next_sequence > 0))
}

/// Refuse, with the typed restart error, a construction session recorded in a
/// retired format, reading nothing but its checkpoint: no recovery runs, no
/// intermediate is authenticated or unlinked. A session that is absent or
/// current passes.
///
/// # Errors
/// [`GfError::Validation`] for a retired session, and storage errors.
pub fn refuse_retired_construction(
    project_dir: &Path,
    operation_uuid: Uuid,
) -> Result<(), GfError> {
    let absent = |error: &std::io::Error| error.kind() == std::io::ErrorKind::NotFound;
    let project = StableDirectory::open(project_dir).map_err(storage)?;
    let private = match project.open_child_directory(OsStr::new(PRIVATE_ROOT)) {
        Ok(private) => private,
        Err(error) if absent(&error) => return Ok(()),
        Err(error) => return Err(storage(error)),
    };
    let root = match private.open_child_directory(OsStr::new(&operation_uuid.simple().to_string()))
    {
        Ok(root) => root,
        Err(error) if absent(&error) => return Ok(()),
        Err(error) => return Err(storage(error)),
    };
    let mut file = match root.open_child_file(OsStr::new(CHECKPOINT)) {
        Ok(file) => file,
        Err(error) if absent(&error) => return Ok(()),
        Err(error) => return Err(storage(error)),
    };
    let body = read_bounded_limit(&mut file, MAX_CONTROL_BYTES)?;
    let checkpoint: Checkpoint = serde_json::from_slice(&body).map_err(storage)?;
    if is_retired(&body, &checkpoint, true)? {
        return Err(refusal());
    }
    Ok(())
}

/// Remove the directory `name` of `parent`, entry by entry, each unlinked only
/// if it is still the file that was listed: the owned removal a construction
/// discard performs, for a private session directory outside the construction
/// root.
///
/// `last` names the entry removed after every other, the record that keeps an
/// interrupted removal readable.
///
/// # Errors
/// Refuses a name that is not one path component and a directory whose
/// identity changes while it is removed.
pub fn remove_owned_directory(parent: &Path, name: &str, last: &str) -> Result<(), GfError> {
    if name.is_empty() || name.contains(['/', '\\']) || name == "." || name == ".." {
        return Err(storage("owned directory name is not one path component"));
    }
    let parent = StableDirectory::open(parent).map_err(storage)?;
    let child = parent
        .open_child_directory(OsStr::new(name))
        .map_err(storage)?;
    let identity = child.identity();
    drop(child);
    let mut remaining = 1 << 22;
    remove_owned_directory_tree_last(
        &parent,
        OsStr::new(name),
        identity,
        &mut remaining,
        OsStr::new(last),
    )
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
    // A removal interrupted after it unlinked the lock leaves the record; the
    // retry takes a fresh lock, as opening a session does.
    let lock = root
        .open_or_create_child_file(OsStr::new(SESSION_LOCK))
        .map_err(storage)?;
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
    if !is_retired(&body, &checkpoint, true)? {
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
    // The record says so first: the checkpoint becomes aborted, durably, before
    // anything is unlinked, as an ordinary discard does. Another process that
    // takes the lock once it is released finds an aborted session, which can
    // only be discarded; it can neither resume nor publish what is being
    // removed.
    let mut aborted: serde_json::Value = serde_json::from_slice(&body).map_err(storage)?;
    aborted["state"] = serde_json::json!("aborted");
    aborted
        .as_object_mut()
        .ok_or_else(|| storage("checkpoint is not a JSON object"))?
        .remove("publication_state");
    replace_control(&root, CHECKPOINT, &aborted)?;
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
    // The ordinary discard's protocol: release the lock and close its handle
    // (a handle held open blocks deleting the file on Windows), then remove the
    // tree with the checkpoint last, so an interruption at any point leaves an
    // aborted record that a retry converges on.
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
