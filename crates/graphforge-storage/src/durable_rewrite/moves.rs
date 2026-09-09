//! Version-two source retirement, subordinate to the existing rewrite intent.

use super::*;
use std::path::PathBuf;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SourceRetirement {
    components: Vec<String>,
    root_volume: u64,
    root_file: String,
    parent_volume: u64,
    parent_file: String,
    original: AuthenticatedFile,
}

impl SourceRetirement {
    pub(crate) fn is_reserved_authority(&self) -> bool {
        let relative: PathBuf = self.components.iter().collect();
        crate::staging::is_reserved_authority(&relative)
    }
}

fn components(root: &Path, source: &Path) -> Result<Vec<String>, GfError> {
    if !root.is_absolute() || !source.is_absolute() {
        return Err(storage("rewrite move requires absolute contained paths"));
    }
    let relative = source.strip_prefix(root).map_err(storage)?;
    if relative.as_os_str().len() as u64 > MAX_JOURNAL_BYTES {
        return Err(storage("rewrite move source exceeds journal bound"));
    }
    let mut result = Vec::new();
    for component in relative.components() {
        let Component::Normal(name) = component else {
            return Err(storage("rewrite move source is not normalized"));
        };
        result.push(
            name.to_str()
                .ok_or_else(|| storage("rewrite move source is not UTF-8"))?
                .to_owned(),
        );
    }
    validate_components(&result)?;
    if protected_source(&result) {
        return Err(storage("rewrite move cannot retire protected authority"));
    }
    let reconstructed: PathBuf = result.iter().collect();
    if root.join(reconstructed).as_os_str() != source.as_os_str() {
        return Err(storage("rewrite move source is not normalized"));
    }
    Ok(result)
}

fn protected_source(parts: &[String]) -> bool {
    matches!(
        parts.first().map(String::as_str),
        Some(
            "CURRENT"
                | "FORMAT"
                | "catalog.json"
                | "manifest.json"
                | "locks"
                | "transactions"
                | "generations"
                | "graph-objects"
                | ".graphforge-rewrite-v1.json"
                | ".graphforge-rewrite.lock"
        )
    ) || matches!(
        parts
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
            .as_slice(),
        [
            "topology",
            "generation.json" | "runtime_entity_label_encoding.json"
        ] | ["topology", "uuid-membership", ..]
    ) || parts.iter().any(|part| {
        part == ".lock" || Path::new(part).extension() == Some(std::ffi::OsStr::new("lock"))
    })
}

fn validate_components(parts: &[String]) -> Result<(), GfError> {
    if parts.is_empty()
        || parts.len() > 256
        || parts.iter().any(|part| {
            part.is_empty()
                || part == "."
                || part == ".."
                || part.contains('/')
                || part.contains('\0')
                || (cfg!(windows) && part.contains('\\'))
        })
    {
        return Err(storage("invalid rewrite move source components"));
    }
    Ok(())
}

fn parent(
    root: &StableDirectory,
    source: &SourceRetirement,
) -> Result<(StableDirectory, std::ffi::OsString), GfError> {
    validate_components(&source.components)?;
    let id = root.identity();
    if id.volume_serial != source.root_volume || hex(&id.file_id) != source.root_file {
        return Err(storage("rewrite move root identity changed"));
    }
    root.revalidate_named().map_err(storage)?;
    let mut directory = root.try_clone().map_err(storage)?;
    for part in &source.components[..source.components.len() - 1] {
        directory = directory
            .open_child_directory(std::ffi::OsStr::new(part))
            .map_err(storage)?;
    }
    let id = directory.identity();
    if id.volume_serial != source.parent_volume || hex(&id.file_id) != source.parent_file {
        return Err(storage("rewrite move source parent identity changed"));
    }
    Ok((
        directory,
        source.components.last().expect("validated nonempty").into(),
    ))
}

fn authenticate(file: &File, expected: &AuthenticatedFile) -> Result<(), GfError> {
    if graphforge_filesystem::file_link_count(file).map_err(storage)? != 1
        || authenticated_file(file)? != *expected
    {
        return Err(storage("rewrite move source identity or content changed"));
    }
    Ok(())
}

pub(crate) fn stage(
    root_path: &Path,
    source_path: &Path,
    destination: &Path,
) -> Result<(tempfile::NamedTempFile, SourceRetirement), GfError> {
    let parts = components(root_path, source_path)?;
    let destination_relative = canonical_relative(root_path, destination)?;
    if parts.join("/") == destination_relative {
        return Err(storage("rewrite move source equals destination"));
    }
    with_rewrite_lock(root_path, |root| {
        let mut source_parent = root.try_clone().map_err(storage)?;
        for part in &parts[..parts.len() - 1] {
            source_parent = source_parent
                .open_child_directory(std::ffi::OsStr::new(part))
                .map_err(storage)?;
        }
        let source = source_parent
            .open_child_file(std::ffi::OsStr::new(parts.last().unwrap()))
            .map_err(storage)?;
        let original = authenticated_file(&source)?;
        authenticate(&source, &original)?;
        let (destination_parent, name) =
            retained_parent_at(root_path, root, &destination_relative)?;
        match destination_parent.open_child_file(&name) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(storage(error)),
            Ok(_) => return Err(storage("rewrite move destination already exists")),
        }
        let mut temporary = tempfile::Builder::new()
            .prefix("move.")
            .suffix(".tmp")
            .tempfile_in(
                destination
                    .parent()
                    .ok_or_else(|| storage("move destination has no parent"))?,
            )
            .map_err(storage)?;
        let mut input = source.try_clone().map_err(storage)?;
        input.rewind().map_err(storage)?;
        std::io::copy(&mut input, &mut temporary).map_err(storage)?;
        temporary.as_file().sync_all().map_err(storage)?;
        authenticate(&source, &original)?;
        let (bytes, digest) = hash_reader(temporary.as_file().try_clone().map_err(storage)?)?;
        if bytes != original.bytes || digest != original.sha256 {
            return Err(storage("rewrite move staged copy differs from source"));
        }
        source_parent.revalidate_named().map_err(storage)?;
        destination_parent.revalidate_named().map_err(storage)?;
        let root_id = root.identity();
        let parent_id = source_parent.identity();
        Ok((
            temporary,
            SourceRetirement {
                components: parts.clone(),
                root_volume: root_id.volume_serial,
                root_file: hex(&root_id.file_id),
                parent_volume: parent_id.volume_serial,
                parent_file: hex(&parent_id.file_id),
                original,
            },
        ))
    })
}

pub(super) fn validate(intent: &Intent) -> Result<(), GfError> {
    let moves = intent
        .entries
        .iter()
        .filter_map(|entry| entry.source.as_ref())
        .collect::<Vec<_>>();
    if !matches!(intent.version, 1 | 2)
        || (intent.version == 1 && !moves.is_empty())
        || (intent.version == 2 && moves.is_empty())
    {
        return Err(storage(
            "rewrite intent version does not match move semantics",
        ));
    }
    let mut paths = std::collections::HashSet::new();
    let mut identities = std::collections::HashSet::new();
    for entry in &intent.entries {
        let Some(source) = &entry.source else {
            continue;
        };
        validate_components(&source.components)?;
        let path = source.components.join("/");
        if protected_source(&source.components)
            || entry.class != EntryClass::Data
            || entry.prior_destination.is_some()
            || source.root_volume != intent.root_volume
            || source.root_file != intent.root_file
            || source.original.bytes != entry.bytes
            || source.original.sha256 != entry.sha256
            || !paths.insert(path.clone())
            || !identities.insert((source.original.volume, &source.original.file))
            || path == JOURNAL
            || path == LOCK
            || path == "topology/generation.json"
            || intent
                .entries
                .iter()
                .any(|other| other.destination == path || other.temporary == path)
        {
            return Err(storage("invalid or colliding rewrite move"));
        }
    }
    Ok(())
}

pub(super) fn check(
    root_path: &Path,
    root: &StableDirectory,
    entry: &Entry,
    replay: bool,
) -> Result<(), GfError> {
    let Some(source) = &entry.source else {
        return Ok(());
    };
    let (directory, name) = parent(root, source)?;
    match directory.open_child_file(&name) {
        Ok(file) => authenticate(&file, &source.original),
        Err(error) if replay && error.kind() == std::io::ErrorKind::NotFound => {
            let (destination_parent, target) =
                retained_parent_at(root_path, root, &entry.destination)?;
            authenticate_installed_destination(&destination_parent, &target, entry)
        }
        Err(error) => Err(storage(error)),
    }
}

pub(super) fn install_and_retire(
    root_path: &Path,
    root: &StableDirectory,
    entry: &Entry,
) -> Result<(), GfError> {
    install(root_path, root, entry)?;
    if entry.source.is_some() {
        crate::project_failpoint::hit(
            "rewrite.after_move_install",
            None,
            None,
            "REWRITE_MOVE",
            false,
        )?;
        retire(root_path, root, entry)?;
        crate::project_failpoint::hit(
            "rewrite.after_source_retirement",
            None,
            None,
            "REWRITE_MOVE",
            false,
        )?;
    }
    Ok(())
}

pub(super) fn retire(
    root_path: &Path,
    root: &StableDirectory,
    entry: &Entry,
) -> Result<(), GfError> {
    let Some(source) = &entry.source else {
        return Ok(());
    };
    let (destination_parent, target) = retained_parent_at(root_path, root, &entry.destination)?;
    authenticate_installed_destination(&destination_parent, &target, entry)?;
    let (directory, name) = parent(root, source)?;
    match directory.open_child_file(&name) {
        Ok(file) => {
            authenticate(&file, &source.original)?;
            let identity = graphforge_filesystem::file_identity(&file).map_err(storage)?;
            drop(file);
            directory
                .unlink_child_if_identity(&name, identity)
                .map_err(storage)?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(storage(error)),
    }
    directory.sync().map_err(storage)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> tempfile::TempDir {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("properties")).unwrap();
        std::fs::write(root.path().join("properties/old"), b"exact source bytes").unwrap();
        root
    }

    #[test]
    fn move_and_control_commit_preserve_bytes_and_retire_exact_source() {
        let root = fixture();
        let source = root.path().join("properties/old");
        let destination = root.path().join("properties/new");
        let old = graphforge_filesystem::file_identity(&File::open(&source).unwrap()).unwrap();
        let mut batch = RewriteBatch::new();
        batch
            .stage_move(root.path(), &source, &destination)
            .unwrap();
        batch
            .stage_bytes(
                &root.path().join("route-table.json"),
                b"authenticated control",
            )
            .unwrap();
        batch.commit_at(root.path()).unwrap();
        assert!(!source.exists());
        assert_eq!(std::fs::read(&destination).unwrap(), b"exact source bytes");
        assert_ne!(
            graphforge_filesystem::file_identity(&File::open(&destination).unwrap()).unwrap(),
            old
        );
        assert_eq!(
            std::fs::read(root.path().join("route-table.json")).unwrap(),
            b"authenticated control"
        );
        recover(root.path()).unwrap();
        assert!(!source.exists());
        assert!(!root.path().join(JOURNAL).exists());
    }

    #[test]
    fn substituted_source_and_duplicate_source_refuse_before_install() {
        for substitute in [false, true] {
            let root = fixture();
            let source = root.path().join("properties/old");
            let destination = root.path().join("properties/new");
            let mut batch = RewriteBatch::new();
            batch
                .stage_move(root.path(), &source, &destination)
                .unwrap();
            if substitute {
                std::fs::rename(&source, root.path().join("original")).unwrap();
                std::fs::write(&source, b"exact source bytes").unwrap();
            } else {
                batch
                    .stage_move(root.path(), &source, &root.path().join("properties/other"))
                    .unwrap();
            }
            assert!(matches!(
                batch.commit_at(root.path()),
                Err(GfError::Storage(_))
            ));
            assert_eq!(std::fs::read(source).unwrap(), b"exact source bytes");
            assert!(!destination.exists());
            assert!(!root.path().join(JOURNAL).exists());
        }
    }

    #[test]
    fn move_subprocess_crash_replays_install_retirement_and_control_together() {
        const CHILD: &str = "GRAPHFORGE_MOVE_CHILD_ROOT";
        if let Ok(root) = std::env::var(CHILD) {
            let root = Path::new(&root);
            let mut batch = RewriteBatch::new();
            batch
                .stage_move(
                    root,
                    &root.join("properties/old"),
                    &root.join("properties/new"),
                )
                .unwrap();
            batch
                .stage_bytes(&root.join("route-table.json"), b"control-v2")
                .unwrap();
            let _ = batch.commit_at(root);
            panic!("failpoint did not terminate child");
        }
        for (phase, durable) in [
            ("rewrite.before_intent", false),
            ("rewrite.after_preparing_disarm", false),
            ("rewrite.after_durable_intent", true),
            ("rewrite.after_move_install", true),
            ("rewrite.after_source_retirement", true),
            ("rewrite.before_generation_authority", true),
            ("rewrite.after_generation_authority", true),
        ] {
            let root = fixture();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "durable_rewrite::moves::tests::move_subprocess_crash_replays_install_retirement_and_control_together", "--nocapture"])
                .env(CHILD, root.path())
                .env("GRAPHFORGE_PROJECT_FAILPOINTS", "graphforge-internal-subprocess-v1")
                .env("GRAPHFORGE_PROJECT_FAILPOINT", phase)
                .status().unwrap();
            assert_eq!(
                status.code(),
                Some(crate::project_failpoint::exit_code()),
                "{phase}"
            );
            recover(root.path()).unwrap();
            recover(root.path()).unwrap();
            assert_eq!(
                root.path().join("properties/old").exists(),
                !durable,
                "{phase}"
            );
            assert_eq!(
                root.path().join("properties/new").exists(),
                durable,
                "{phase}"
            );
            assert_eq!(
                root.path().join("route-table.json").exists(),
                durable,
                "{phase}"
            );
            let payload = if durable {
                "properties/new"
            } else {
                "properties/old"
            };
            assert_eq!(
                std::fs::read(root.path().join(payload)).unwrap(),
                b"exact source bytes",
                "{phase}"
            );
            if durable {
                assert_eq!(
                    std::fs::read(root.path().join("route-table.json")).unwrap(),
                    b"control-v2"
                );
            }
            assert!(!root.path().join(JOURNAL).exists(), "{phase}");
        }
    }
    #[test]
    fn reserved_sources_and_replacement_of_move_refuse() {
        for relative in [
            "topology/nodes.parquet",
            "topology/surrogate_tails.parquet",
            "topology/edges/X.parquet",
        ] {
            let root = fixture();
            let source = root.path().join(relative);
            std::fs::create_dir_all(source.parent().unwrap()).unwrap();
            std::fs::write(&source, b"authority").unwrap();
            let mut batch = RewriteBatch::new();
            batch
                .stage_move(root.path(), &source, &root.path().join("properties/new"))
                .unwrap();
            assert!(matches!(
                batch.commit_at(root.path()),
                Err(GfError::Storage(_))
            ));
            assert_eq!(std::fs::read(source).unwrap(), b"authority");
            assert!(!root.path().join("properties/new").exists());
        }
        for relative in [
            "CURRENT",
            "FORMAT",
            "catalog.json",
            "locks/writer.lock",
            "topology/generation.json",
            "topology/uuid-membership/ordinal-v4-receipt.json",
        ] {
            let root = fixture();
            let source = root.path().join(relative);
            std::fs::create_dir_all(source.parent().unwrap()).unwrap();
            std::fs::write(&source, b"protected").unwrap();
            let mut batch = RewriteBatch::new();
            assert!(matches!(
                batch.stage_move(root.path(), &source, &root.path().join("properties/new")),
                Err(GfError::Storage(_))
            ));
            assert_eq!(std::fs::read(source).unwrap(), b"protected");
        }
        let root = fixture();
        let mut batch = RewriteBatch::new();
        let destination = root.path().join("properties/new");
        batch
            .stage_move(
                root.path(),
                &root.path().join("properties/old"),
                &destination,
            )
            .unwrap();
        assert!(matches!(
            batch.stage_bytes(&destination, b"different"),
            Err(GfError::Storage(_))
        ));
        batch.commit_at(root.path()).unwrap();
        assert_eq!(std::fs::read(destination).unwrap(), b"exact source bytes");
    }

    #[test]
    fn installed_destination_does_not_authorize_deleting_substituted_source() {
        let root = fixture();
        let source = root.path().join("properties/old");
        let destination = root.path().join("properties/new");
        let mut batch = RewriteBatch::new();
        batch
            .stage_move(root.path(), &source, &destination)
            .unwrap();
        FAIL_AFTER_DURABLE_INTENT.set(true);
        assert!(batch.commit_at(root.path()).is_err());
        {
            let guard = acquire(root.path()).unwrap();
            let (bytes, _) = read_journal(&guard.directory).unwrap().unwrap();
            let intent: Intent = serde_json::from_slice(&bytes).unwrap();
            install(root.path(), &guard.directory, &intent.entries[0]).unwrap();
        }
        std::fs::rename(&source, root.path().join("saved-original")).unwrap();
        std::fs::write(&source, b"different owner").unwrap();
        assert!(matches!(recover(root.path()), Err(GfError::Storage(_))));
        assert_eq!(std::fs::read(&source).unwrap(), b"different owner");
        assert_eq!(std::fs::read(&destination).unwrap(), b"exact source bytes");
        assert!(root.path().join(JOURNAL).exists());
    }
    #[test]
    fn genuine_v1_wire_checksum_and_recovery_remain_unchanged() {
        // Independent copy of the pre-move wire schema: deliberately do not
        // serialize the current Intent/Entry types to create legacy evidence.
        #[derive(Serialize)]
        struct LegacyFile<'a> {
            volume: u64,
            file: &'a str,
            bytes: u64,
            sha256: &'a str,
        }
        #[derive(Serialize)]
        struct LegacyEntry<'a> {
            class: &'a str,
            destination: &'a str,
            temporary: &'a str,
            parent_volume: u64,
            parent_file: &'a str,
            bytes: u64,
            sha256: &'a str,
            temporary_volume: u64,
            temporary_file: &'a str,
            prior_destination: Option<LegacyFile<'a>>,
        }
        #[derive(Serialize)]
        struct LegacyGeneration {
            topology: u64,
            search: u64,
            property: u64,
        }
        #[derive(Serialize)]
        struct LegacyIntent<'a> {
            version: u8,
            state: &'a str,
            transaction: &'a str,
            root_volume: u64,
            root_file: &'a str,
            prior: LegacyGeneration,
            next: LegacyGeneration,
            auxiliary: Option<()>,
            entries: Vec<LegacyEntry<'a>>,
            checksum: String,
        }
        let root = fixture();
        std::fs::write(
            root.path().join("legacy-second.json"),
            b"prior legacy bytes",
        )
        .unwrap();
        FAIL_AFTER_DURABLE_INTENT.set(true);
        let mut batch = RewriteBatch::new();
        batch
            .stage_bytes(
                &root.path().join("legacy-second.json"),
                b"legacy second bytes",
            )
            .unwrap();
        assert!(commit(batch, root.path(), false, false, false, None).is_err());
        let bytes = std::fs::read(root.path().join(JOURNAL)).unwrap();
        let captured: Intent = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(captured.version, 1);
        let mut legacy = LegacyIntent {
            version: 1,
            state: "durable",
            transaction: &captured.transaction,
            root_volume: captured.root_volume,
            root_file: &captured.root_file,
            prior: LegacyGeneration {
                topology: 0,
                search: 0,
                property: 0,
            },
            next: LegacyGeneration {
                topology: 0,
                search: 0,
                property: 0,
            },
            auxiliary: None,
            entries: captured
                .entries
                .iter()
                .map(|entry| LegacyEntry {
                    class: if entry.class == EntryClass::Data {
                        "data"
                    } else {
                        "generation_authority"
                    },
                    destination: &entry.destination,
                    temporary: &entry.temporary,
                    parent_volume: entry.parent_volume,
                    parent_file: &entry.parent_file,
                    bytes: entry.bytes,
                    sha256: &entry.sha256,
                    temporary_volume: entry.temporary_volume,
                    temporary_file: &entry.temporary_file,
                    prior_destination: entry.prior_destination.as_ref().map(|prior| LegacyFile {
                        volume: prior.volume,
                        file: &prior.file,
                        bytes: prior.bytes,
                        sha256: &prior.sha256,
                    }),
                })
                .collect(),
            checksum: String::new(),
        };
        legacy.checksum = hex(&Sha256::digest(serde_json::to_vec(&legacy).unwrap()));
        assert_eq!(legacy.checksum, captured.checksum);
        let original_wire = serde_json::to_vec(&legacy).unwrap();
        assert_eq!(original_wire, bytes);
        std::fs::write(root.path().join(JOURNAL), original_wire).unwrap();
        recover(root.path()).unwrap();
        recover(root.path()).unwrap();
        assert_eq!(
            std::fs::read(root.path().join("legacy-second.json")).unwrap(),
            b"legacy second bytes"
        );
        assert!(!root.path().join(JOURNAL).exists());
    }
    #[test]
    fn malformed_move_metadata_and_cross_entry_paths_fail_closed() {
        let root = fixture();
        let mut batch = RewriteBatch::new();
        batch
            .stage_move(
                root.path(),
                &root.path().join("properties/old"),
                &root.path().join("properties/new"),
            )
            .unwrap();
        FAIL_AFTER_DURABLE_INTENT.set(true);
        assert!(batch.commit_at(root.path()).is_err());
        let bytes = std::fs::read(root.path().join(JOURNAL)).unwrap();
        for case in 0..7 {
            let mut intent: Intent = serde_json::from_slice(&bytes).unwrap();
            match case {
                0 => intent.version = 3,
                1 => intent.version = 1,
                2 => intent.entries[0].source = None,
                3 => intent.entries[0]
                    .source
                    .as_mut()
                    .unwrap()
                    .components
                    .clear(),
                4 => {
                    intent.entries[0].source.as_mut().unwrap().components =
                        vec!["..".into(), "old".into()]
                }
                5 => {
                    intent.entries[0].source.as_mut().unwrap().components =
                        vec!["properties".into(), "new".into()]
                }
                6 => intent.entries[0].source.as_mut().unwrap().components = vec!["CURRENT".into()],
                _ => unreachable!(),
            }
            // Recompute the checksum so this proves semantic admission, not
            // merely rejection of a corrupted checksum.
            intent.checksum = checksum(&intent).unwrap();
            std::fs::write(root.path().join(JOURNAL), intent_bytes(&intent).unwrap()).unwrap();
            assert!(
                matches!(recover(root.path()), Err(GfError::Storage(_))),
                "case {case}"
            );
            assert_eq!(
                std::fs::read(root.path().join("properties/old")).unwrap(),
                b"exact source bytes"
            );
            assert!(!root.path().join("properties/new").exists());
        }
        std::fs::write(root.path().join(JOURNAL), bytes).unwrap();
        recover(root.path()).unwrap();
    }

    #[test]
    fn hardlink_source_alias_is_not_an_authenticated_single_owner() {
        let root = fixture();
        let source = root.path().join("properties/old");
        std::fs::hard_link(&source, root.path().join("alias")).unwrap();
        let mut batch = RewriteBatch::new();
        assert!(matches!(
            batch.stage_move(root.path(), &source, &root.path().join("properties/new")),
            Err(GfError::Storage(_))
        ));
        assert_eq!(std::fs::read(source).unwrap(), b"exact source bytes");
        assert_eq!(
            std::fs::read(root.path().join("alias")).unwrap(),
            b"exact source bytes"
        );
        assert!(!root.path().join("properties/new").exists());
    }
    #[cfg(unix)]
    #[test]
    fn literal_backslash_source_replays_without_normalizing_sibling() {
        let root = fixture();
        let source = root.path().join(r"properties/A\B");
        let sibling = root.path().join("properties/A/B");
        std::fs::create_dir_all(sibling.parent().unwrap()).unwrap();
        std::fs::write(&source, b"literal backslash route").unwrap();
        std::fs::write(&sibling, b"different nested route").unwrap();
        let mut batch = RewriteBatch::new();
        batch
            .stage_move(root.path(), &source, &root.path().join("properties/mapped"))
            .unwrap();
        FAIL_AFTER_DURABLE_INTENT.set(true);
        assert!(batch.commit_at(root.path()).is_err());
        recover(root.path()).unwrap();
        recover(root.path()).unwrap();
        assert!(!source.exists());
        assert_eq!(
            std::fs::read(root.path().join("properties/mapped")).unwrap(),
            b"literal backslash route"
        );
        assert_eq!(std::fs::read(sibling).unwrap(), b"different nested route");
    }

    #[test]
    fn changed_source_parent_refuses_before_install() {
        let root = fixture();
        std::fs::create_dir(root.path().join("destination")).unwrap();
        let source = root.path().join("properties/old");
        let mut batch = RewriteBatch::new();
        batch
            .stage_move(root.path(), &source, &root.path().join("destination/new"))
            .unwrap();
        std::fs::rename(
            root.path().join("properties"),
            root.path().join("saved-parent"),
        )
        .unwrap();
        std::fs::create_dir(root.path().join("properties")).unwrap();
        std::fs::write(&source, b"exact source bytes").unwrap();
        assert!(matches!(
            batch.commit_at(root.path()),
            Err(GfError::Storage(_))
        ));
        assert_eq!(std::fs::read(&source).unwrap(), b"exact source bytes");
        assert_eq!(
            std::fs::read(root.path().join("saved-parent/old")).unwrap(),
            b"exact source bytes"
        );
        assert!(!root.path().join("destination/new").exists());
    }

    #[cfg(unix)]
    #[test]
    fn symlink_source_parent_is_not_followed() {
        let root = fixture();
        std::os::unix::fs::symlink(root.path().join("properties"), root.path().join("linked"))
            .unwrap();
        let mut batch = RewriteBatch::new();
        assert!(matches!(
            batch.stage_move(
                root.path(),
                &root.path().join("linked/old"),
                &root.path().join("properties/new")
            ),
            Err(GfError::Storage(_))
        ));
        assert_eq!(
            std::fs::read(root.path().join("properties/old")).unwrap(),
            b"exact source bytes"
        );
        assert!(!root.path().join("properties/new").exists());
    }
}
