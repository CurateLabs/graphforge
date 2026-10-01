use super::*;

/// Full verification reads composition metadata from the package itself
/// and never materializes its payload (#1405).
#[cfg(target_os = "linux")]
#[test]
fn full_verification_of_a_composition_package_copies_no_payload() {
    let (_package_parent, package) = super::composition_package();
    let written = || {
        std::fs::read_to_string("/proc/thread-self/io")
            .unwrap()
            .lines()
            .find_map(|line| line.strip_prefix("wchar:"))
            .unwrap()
            .trim()
            .parse::<u64>()
            .unwrap()
    };
    let before = written();
    let report = crate::verify_portable_v2(
        &package,
        crate::PortableV2Mode::Full,
        crate::PortableV2Limits::default(),
        None,
    )
    .unwrap();
    assert!(report.ontology_composition.is_some());
    assert!(!report.ontology_composition_entries.is_empty());
    assert_eq!(written() - before, 0, "verification wrote payload bytes");
}
