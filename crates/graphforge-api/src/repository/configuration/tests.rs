use super::super::*;
use super::*;
use tempfile::tempdir;

fn assert_validation_message(result: Result<(), GfError>, expected: &str) {
    match result.unwrap_err() {
        GfError::Validation(message) => assert_eq!(message, expected),
        error => panic!("expected validation error, got {error}"),
    }
}

#[test]
fn definition_documents_reject_every_malformed_contract_shape() {
    assert_validation_message(
        validate_definition_document(&[0xff], "json"),
        "definition file must be canonical UTF-8 text",
    );
    assert_validation_message(
        validate_definition_document(b"{\0}", "json"),
        "definition file contains binary data",
    );
    assert_validation_message(
        validate_definition_document(b"{", "json"),
        "JSON definition is malformed",
    );
    assert_validation_message(
        validate_definition_document(b"[]", "json"),
        "JSON definition must be an object",
    );
    assert_validation_message(
        validate_definition_document(b"[", "yaml"),
        "YAML definition is malformed",
    );
    assert_validation_message(
        validate_definition_document(b"value", "yml"),
        "YAML definition must be a mapping",
    );
    assert_validation_message(
        validate_definition_document(b" \n\t", "cypher"),
        "Cypher migration definition is empty",
    );
    assert_validation_message(
        validate_definition_document(b"{}", "toml"),
        "unregistered definition file type",
    );

    assert!(validate_definition_document(b"{}", "json").is_ok());
    assert!(validate_definition_document(b"key: value", "yaml").is_ok());
    assert!(validate_definition_document(b"RETURN 1", "cypher").is_ok());
}

#[test]
fn definition_tree_enforces_containment_types_and_size() {
    let root = tempdir().unwrap();
    fs::create_dir(root.path().join("nested")).unwrap();
    fs::write(root.path().join("nested/valid.JSON"), "{}").unwrap();
    let digest = digest_definition_tree(root.path(), DefinitionKind::Schemas).unwrap();
    assert_eq!(digest.len(), 64);

    fs::write(root.path().join("missing-extension"), "{}").unwrap();
    assert_eq!(
        digest_definition_tree(root.path(), DefinitionKind::Schemas)
            .unwrap_err()
            .to_string(),
        "validation error: definition files require a registered extension"
    );
    fs::remove_file(root.path().join("missing-extension")).unwrap();

    fs::write(root.path().join("unsupported.cypher"), "RETURN 1").unwrap();
    assert_eq!(
        digest_definition_tree(root.path(), DefinitionKind::Ontology)
            .unwrap_err()
            .to_string(),
        "validation error: ontology definitions do not allow .cypher files"
    );
    assert!(digest_definition_tree(root.path(), DefinitionKind::Migrations).is_ok());
    fs::remove_file(root.path().join("unsupported.cypher")).unwrap();

    fs::write(
        root.path().join("oversized.json"),
        vec![b' '; 1024 * 1024 + 1],
    )
    .unwrap();
    assert_eq!(
        digest_definition_tree(root.path(), DefinitionKind::Schemas)
            .unwrap_err()
            .to_string(),
        "validation error: definition file exceeds byte bound"
    );

    #[cfg(unix)]
    {
        fs::remove_file(root.path().join("oversized.json")).unwrap();
        std::os::unix::fs::symlink(
            root.path().join("nested/valid.JSON"),
            root.path().join("linked.json"),
        )
        .unwrap();
        assert_eq!(
            digest_definition_tree(root.path(), DefinitionKind::Schemas)
                .unwrap_err()
                .to_string(),
            "validation error: symlinks are not allowed in definitions"
        );
    }
}

#[test]
fn repository_scalar_contracts_cover_boundaries_and_credentials() {
    for valid in ["a", "a0", "alpha-beta", "alpha_beta"] {
        assert!(stable_id(valid).is_ok(), "{valid}");
    }
    for invalid in ["", "A", "0alpha", "alpha-", "alpha__beta", &"a".repeat(65)] {
        assert_eq!(
            stable_id(invalid).unwrap_err().to_string(),
            "validation error: invalid stable id"
        );
    }

    let digest_value = "ab".repeat(32);
    assert!(digest(&digest_value).is_ok());
    for invalid in ["a".repeat(63), "A".repeat(64), "g".repeat(64)] {
        assert_eq!(
            digest(&invalid).unwrap_err().to_string(),
            "validation error: invalid sha256 digest"
        );
    }

    assert!(bounded("a", 1, 2, "value").is_ok());
    assert!(bounded("ab", 1, 2, "value").is_ok());
    assert_eq!(
        bounded("", 1, 2, "value").unwrap_err().to_string(),
        "validation error: value exceeds contract bounds"
    );
    assert!(uri_has_inline_credentials("https://user@example.com/path"));
    assert!(!uri_has_inline_credentials(
        "https://example.com/user@example"
    ));
    assert!(!uri_has_inline_credentials("relative/user@example"));
}

#[test]
fn checked_in_fixture_resolves_to_the_contract_golden() {
    let root = tempdir().unwrap();
    fs::create_dir_all(root.path().join(".graphforge")).unwrap();
    fs::write(
        root.path().join(CONFIG),
        include_str!("../../../../../docs/contracts/examples/graphforge-v1.yaml"),
    )
    .unwrap();
    let context = RepositoryContext::discover(root.path()).unwrap();
    assert_eq!(context.load_config().unwrap().schema_version, 1);
    let actual = context.resolve_config().unwrap();
    let expected: Value = serde_json::from_str(include_str!(
        "../../../../../docs/contracts/examples/graphforge-resolved-v1.json"
    ))
    .unwrap();
    assert_eq!(actual, expected);
    let canonical = serde_json::to_string(&actual).unwrap() + "\n";
    assert_eq!(
        canonical,
        include_str!("../../../../../docs/contracts/examples/graphforge-resolved-v1.json")
    );
    let validation = context.validate_infra_target("production").unwrap();
    let canonical_validation =
        serde_json::to_string(&serde_json::to_value(&validation).unwrap()).unwrap() + "\n";
    assert_eq!(
        canonical_validation,
        include_str!(
            "../../../../../docs/contracts/examples/graphforge-infra-validation-production-v1.json"
        )
    );
    assert_eq!(validation.static_validity.status, "valid");
    assert_eq!(validation.planned_infrastructure.mutation, "none");
    assert_eq!(validation.connectivity.status, "not_checked");
    assert_eq!(validation.readiness.status, "not_checked");
    assert_eq!(
        validation.capability_compatibility.status,
        "requirements_declared"
    );
    assert!(!root.path().join(".graphforge/state").exists());
    assert_eq!(
        validation,
        context.validate_infra_target("production").unwrap()
    );
    assert_eq!(
        context.validate_infra_target("missing").unwrap_err().code(),
        "GF_VALIDATION"
    );
}

#[test]
fn stable_ids_match_the_published_separator_grammar() {
    for valid in ["a", "a1", "a-b", "a_b2", "alpha-beta_gamma9"] {
        stable_id(valid).unwrap();
    }
    for invalid in [
        "", "1a", "A", "a-", "a_", "a--b", "a__b", "a-_b", "a_-b", "a.b",
    ] {
        assert_eq!(stable_id(invalid).unwrap_err().code(), "GF_VALIDATION");
    }
}

#[test]
fn target_semantics_fail_closed_before_state_or_secret_materialization() {
    let root = tempdir().unwrap();
    fs::create_dir_all(root.path().join(".graphforge")).unwrap();
    let context = RepositoryContext::discover(root.path()).unwrap();
    let mut config: ProjectConfig = serde_yaml::from_str(DEFAULT_CONFIG).unwrap();
    let target = config.targets.get_mut("local").unwrap();
    target.ownership = Some(TargetOwnership::External);
    assert_eq!(
        config.validate(&context).unwrap_err().code(),
        "GF_VALIDATION"
    );

    let base: ProjectConfig = serde_yaml::from_str(DEFAULT_CONFIG).unwrap();
    let base_target = base.targets["local"].clone();
    let semantics_error = |target: Target, expected: &str| {
        assert_validation_message(target.validate_semantics(), expected);
    };

    let mut target = base_target.clone();
    target.topology = Some(Topology {
        execution: Some(ExecutionKind::Process),
        scheduling: Some(SchedulingKind::OnDemand),
        replicas: Some(1),
    });
    semantics_error(
        target,
        "embedded targets require one long-running process, local storage, and no network exposure",
    );

    let mut target = base_target.clone();
    target.kind = TargetKind::Host;
    target.ownership = Some(TargetOwnership::External);
    target.topology = Some(Topology {
        execution: Some(ExecutionKind::Process),
        scheduling: Some(SchedulingKind::LongRunning),
        replicas: Some(1),
    });
    semantics_error(target, "host targets require host execution");

    let mut target = base_target.clone();
    target.kind = TargetKind::Service;
    target.ownership = Some(TargetOwnership::External);
    target.topology = Some(Topology {
        execution: Some(ExecutionKind::Host),
        scheduling: Some(SchedulingKind::LongRunning),
        replicas: Some(1),
    });
    semantics_error(target, "host execution requires a host target");

    let mut target = base_target.clone();
    target.kind = TargetKind::Job;
    target.ownership = Some(TargetOwnership::External);
    target.topology = Some(Topology {
        execution: Some(ExecutionKind::Process),
        scheduling: Some(SchedulingKind::LongRunning),
        replicas: Some(1),
    });
    semantics_error(
        target,
        "job targets are on-demand and other targets are long-running",
    );

    let mut target = base_target;
    target.kind = TargetKind::Service;
    target.ownership = Some(TargetOwnership::External);
    target.topology = Some(Topology {
        execution: Some(ExecutionKind::Process),
        scheduling: Some(SchedulingKind::LongRunning),
        replicas: Some(1),
    });
    target.network = None;
    semantics_error(target, "service targets require a network port");

    let mut config: ProjectConfig = serde_yaml::from_str(DEFAULT_CONFIG).unwrap();
    config
        .targets
        .get_mut("local")
        .unwrap()
        .storage
        .capacity_bytes = Some(MAX_PORTABLE_INTEGER + 1);
    assert_eq!(
        config.validate(&context).unwrap_err().code(),
        "GF_VALIDATION"
    );

    let mut config: ProjectConfig = serde_yaml::from_str(DEFAULT_CONFIG).unwrap();
    config.sources.push(Source {
        id: "input".into(),
        uri: "https://user@example.invalid/data.parquet".into(),
        sha256: "a".repeat(64),
        media_type: None,
    });
    assert_eq!(
        config.validate(&context).unwrap_err().code(),
        "GF_VALIDATION"
    );

    let sentinel = ["GRAPHFORGE_SECRET", "SENTINEL_231"].join("_");
    fs::write(
        root.path().join(CONFIG),
        format!(
            "{DEFAULT_CONFIG}secrets:\n  - id: token\n    source: environment\n    value: {sentinel}\n"
        ),
    )
    .unwrap();
    let error = context.resolve_config().unwrap_err();
    assert_eq!(error.code(), "GF_VALIDATION");
    assert!(!error.to_string().contains(&sentinel));
    assert!(!root.path().join(".graphforge/state").exists());
}

#[test]
fn project_and_target_validation_rejects_every_cross_field_conflict() {
    let root = tempdir().unwrap();
    let context = RepositoryContext::discover(root.path()).unwrap();
    let base: ProjectConfig = serde_yaml::from_str(DEFAULT_CONFIG).unwrap();
    let invalid = |config: ProjectConfig| {
        assert_eq!(
            config.validate(&context).unwrap_err().code(),
            "GF_VALIDATION"
        );
    };

    let mut config = base.clone();
    config.schema_version = 2;
    invalid(config);
    let mut config = base.clone();
    config.targets.clear();
    invalid(config);
    let mut config = base.clone();
    config.sources = (0..257)
        .map(|index| Source {
            id: format!("source-{index}"),
            uri: format!("https://example.invalid/{index}"),
            sha256: "a".repeat(64),
            media_type: None,
        })
        .collect();
    invalid(config);
    let mut config = base.clone();
    config.project.ontology = "ontology\\schema.yaml".into();
    invalid(config);
    let mut config = base.clone();
    config.project.schemas = "../outside".into();
    invalid(config);

    let source = Source {
        id: "input".into(),
        uri: "https://example.invalid/data.parquet".into(),
        sha256: "a".repeat(64),
        media_type: Some("application/vnd.apache.parquet".into()),
    };
    let mut config = base.clone();
    config.sources = vec![source.clone(), source.clone()];
    invalid(config);
    let mut config = base.clone();
    config.sources = vec![Source {
        sha256: "not-a-digest".into(),
        ..source.clone()
    }];
    invalid(config);
    let mut config = base.clone();
    config.sources = vec![source];
    config.targets.get_mut("local").unwrap().source_ids = vec!["missing".into()];
    invalid(config);

    let secret = SecretReference {
        id: "token".into(),
        source: SecretSource::Environment,
    };
    let mut config = base.clone();
    config.secrets = vec![secret.clone(), secret];
    invalid(config);
    let mut config = base.clone();
    config.targets.get_mut("local").unwrap().secret_ids = vec!["missing".into()];
    invalid(config);

    let mut config = base.clone();
    let target = config.targets.get_mut("local").unwrap();
    target.capabilities = vec![
        CapabilityRequirement {
            id: "search".into(),
            version: 1,
        },
        CapabilityRequirement {
            id: "search".into(),
            version: 2,
        },
    ];
    invalid(config);
    let mut config = base.clone();
    config.targets.get_mut("local").unwrap().capabilities = vec![CapabilityRequirement {
        id: "search".into(),
        version: 0,
    }];
    invalid(config);
    let mut config = base.clone();
    config.targets.get_mut("local").unwrap().source_ids =
        (0..257).map(|index| format!("source-{index}")).collect();
    invalid(config);
    let mut config = base.clone();
    config.targets.get_mut("local").unwrap().capabilities = (0..65)
        .map(|index| CapabilityRequirement {
            id: format!("capability-{index}"),
            version: 1,
        })
        .collect();
    invalid(config);

    let mut config = base.clone();
    let target = config.targets.get_mut("local").unwrap();
    target.write.mode = WriteMode::Single;
    target.write.queue_capacity = Some(1);
    invalid(config);
    let mut config = base.clone();
    let target = config.targets.get_mut("local").unwrap();
    target.write.mode = WriteMode::Queued;
    target.write.queue_capacity = None;
    invalid(config);
    let mut config = base.clone();
    let target = config.targets.get_mut("local").unwrap();
    target.write.mode = WriteMode::OptimisticMulti;
    target.write.max_rebase_attempts = None;
    invalid(config);

    let mut config = base.clone();
    let target = config.targets.get_mut("local").unwrap();
    target.kind = TargetKind::Service;
    target.ownership = Some(TargetOwnership::External);
    target.topology = Some(Topology {
        execution: Some(ExecutionKind::Container),
        scheduling: Some(SchedulingKind::LongRunning),
        replicas: Some(1),
    });
    target.storage.kind = StorageKind::Volume;
    target.network = Some(Network {
        exposure: Some(Exposure::Public),
        port: Some(443),
        tls_required: Some(false),
    });
    invalid(config);
    let mut config = base.clone();
    let target = config.targets.get_mut("local").unwrap();
    target.backup = Some(Backup {
        checkpoints: Some(false),
        retention_count: Some(2),
    });
    invalid(config);
}
