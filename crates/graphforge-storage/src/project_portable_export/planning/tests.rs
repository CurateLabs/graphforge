use super::super::{PortableOutput, export_complete_portable};
use super::*;
use crate::PortableErrorCode;
use crate::open_or_initialize_project;
use std::fs;
use std::sync::atomic::AtomicBool;

#[test]
fn selection_preview_is_stable_exact_and_consumed_by_export_plan() {
    let project = tempfile::tempdir().unwrap();
    let generation = open_or_initialize_project(project.path()).unwrap();
    let limits = PortableExportLimits::default();
    let request = PortableSelectionRequest {
        profile: PortableSelectionProfile::Settings,
        strict: true,
    };
    let first = preview_portable_selection(&generation, &request, limits).unwrap();
    let second = preview_portable_selection(&generation, &request, limits).unwrap();
    assert_eq!(first, second);
    assert_eq!(first.package_class, "component-selective");
    assert!(first.selection_fingerprint.starts_with("sha256:"));
    assert!(first.included.iter().all(|entry| entry.kind == "settings"));

    let plan = plan_selected_portable(&generation, &first, limits).unwrap();
    assert_eq!(plan.selection_fingerprint, first.selection_fingerprint);
    assert_eq!(plan.package_class, PortablePackageClass::ComponentSelective);
    let out = tempfile::tempdir().unwrap();
    let receipt = export_complete_portable(
        &plan,
        out.path().join("settings.gfpb"),
        PortableOutput::Bundle,
        limits,
        &AtomicBool::new(false),
        |_| {},
    )
    .unwrap();
    assert_eq!(receipt.selection_fingerprint, first.selection_fingerprint);
    let mut tampered = first.clone();
    tampered.redactions.push("invented-after-preview".into());
    let error = plan_selected_portable(&generation, &tampered, limits).unwrap_err();
    assert_eq!(error.code, PortableErrorCode::Incompatible);

    let exact = PortableSelectionRequest {
        profile: PortableSelectionProfile::Custom(
            first
                .included
                .iter()
                .map(|entry| entry.identity.clone())
                .collect(),
        ),
        strict: true,
    };
    let exact = preview_portable_selection(&generation, &exact, limits).unwrap();
    assert_eq!(
        first
            .included
            .iter()
            .map(|entry| &entry.identity)
            .collect::<Vec<_>>(),
        exact
            .included
            .iter()
            .map(|entry| &entry.identity)
            .collect::<Vec<_>>()
    );
}

#[test]
fn selection_rejects_ambiguous_identity_and_resource_overflow() {
    let project = tempfile::tempdir().unwrap();
    let generation = open_or_initialize_project(project.path()).unwrap();
    let limits = PortableExportLimits::default();
    let complete = preview_portable_selection(
        &generation,
        &PortableSelectionRequest {
            profile: PortableSelectionProfile::Complete,
            strict: false,
        },
        limits,
    )
    .unwrap();
    let identity = complete.included[0].identity.clone();
    let error = preview_portable_selection(
        &generation,
        &PortableSelectionRequest {
            profile: PortableSelectionProfile::Custom(vec![identity.clone(), identity]),
            strict: false,
        },
        limits,
    )
    .unwrap_err();
    assert_eq!(error.code, PortableErrorCode::Incompatible);
    let error = preview_portable_selection(
        &generation,
        &PortableSelectionRequest {
            profile: PortableSelectionProfile::Complete,
            strict: false,
        },
        PortableExportLimits {
            max_components: 1,
            ..limits
        },
    )
    .unwrap_err();
    assert_eq!(error.code, PortableErrorCode::LimitExceeded);
}

#[test]
fn selection_rejects_secret_and_host_path_settings_without_leaking_values() {
    for unsafe_settings in [
        serde_json::json!({"api_token": "do-not-export"}),
        serde_json::json!({"cache": {"directory": "/Users/example/private"}}),
    ] {
        let project = tempfile::tempdir().unwrap();
        let generation = open_or_initialize_project(project.path()).unwrap();
        let path = generation
            .participant_path(
                crate::WORKSPACE_CAPABILITY_ID,
                crate::WORKSPACE_CONFIGURATION_FAMILY,
            )
            .unwrap();
        fs::write(path, serde_json::to_vec(&unsafe_settings).unwrap()).unwrap();
        let error = preview_portable_selection(
            &generation,
            &PortableSelectionRequest {
                profile: PortableSelectionProfile::Settings,
                strict: true,
            },
            PortableExportLimits::default(),
        )
        .unwrap_err();
        assert_eq!(error.code, PortableErrorCode::Incompatible);
        assert!(!error.to_string().contains("do-not-export"));
        assert!(!error.to_string().contains("/Users/example"));
    }
}

#[test]
fn built_in_profiles_select_only_their_canonical_component_classes() {
    let project = tempfile::tempdir().unwrap();
    let generation = open_or_initialize_project(project.path()).unwrap();
    let limits = PortableExportLimits::default();
    for (profile, allowed) in [
        (
            PortableSelectionProfile::OntologyOnly,
            &["ontology", "schema"][..],
        ),
        (
            PortableSelectionProfile::DataComponents,
            &["graph-data", "schema"][..],
        ),
        (
            PortableSelectionProfile::Artifacts,
            &["derived-artifact", "schema"][..],
        ),
        (PortableSelectionProfile::Settings, &["settings"][..]),
    ] {
        let plan = preview_portable_selection(
            &generation,
            &PortableSelectionRequest {
                profile,
                strict: false,
            },
            limits,
        )
        .unwrap();
        assert!(
            plan.included
                .iter()
                .all(|entry| allowed.contains(&entry.kind.as_str()))
        );
        plan_selected_portable(&generation, &plan, limits).unwrap();
    }
}

#[test]
fn plan_debug_is_content_and_host_path_free() {
    let project = tempfile::tempdir().unwrap();
    let generation = open_or_initialize_project(project.path()).unwrap();
    let plan = plan_complete_portable(&generation, PortableExportLimits::default()).unwrap();
    let debug = format!("{plan:?}");
    assert!(!debug.contains(project.path().to_string_lossy().as_ref()));
    assert!(!debug.contains("manifest"));
    assert!(debug.contains("entry_count"));
}
