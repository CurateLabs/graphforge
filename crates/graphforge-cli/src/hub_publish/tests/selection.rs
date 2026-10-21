//! Shared native publication selection fixtures.
use super::*;

pub(super) fn args(repository: &str) -> PublishArgs {
    PublishArgs {
        rewrite: false,
        prune: false,
        check_fork_origin: None,
        repository: repository_url(repository),
        git_ref: None,
        version_uuid: None,
        fork_of: None,
        operation_uuid: None,
        protection_bypass_env: None,
        staging_directory: None,
        previews: Vec::new(),
    }
}

pub(super) fn on_ref(repository: &str, name: &str) -> PublishArgs {
    PublishArgs {
        git_ref: Some(name.into()),
        ..args(repository)
    }
}
