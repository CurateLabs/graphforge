//! Deterministic destructive history cases through the same public corpus mapping.
use super::*;
use std::collections::BTreeSet;
fn read(publication: &Publication) -> ResearchLineage {
    ResearchLineage::from_json(
        &publication
            .objects
            .iter()
            .find(|o| o.media_type == RESEARCH_LINEAGE_MEDIA_TYPE)
            .unwrap()
            .bytes,
        DiscoveryLimits::default(),
    )
    .unwrap()
}
fn replace(publication: &mut Publication, lineage: ResearchLineage) {
    publication
        .objects
        .iter_mut()
        .find(|o| o.media_type == RESEARCH_LINEAGE_MEDIA_TYPE)
        .unwrap()
        .bytes = lineage.to_canonical_json().unwrap();
    publication.objects.sort_by_key(Object::digest);
}
fn next(previous: &Publication, operation: u128, head: u128) -> Publication {
    let old = read(previous);
    let mut publication = research_publication(v1(operation), 3100, 3101, head, 'd', None);
    let mut lineage = read(&publication);
    let head = lineage.versions[0].version_uuid.clone();
    lineage.branches[0] = old.branches[0].clone();
    lineage.branches[0].head_version_uuid = head;
    lineage.versions[0].parents = vec![old.branches[0].base_version_uuid.clone()];
    let base = old
        .versions
        .iter()
        .find(|v| v.version_uuid == old.branches[0].base_version_uuid)
        .unwrap()
        .clone();
    // A retained Version keeps its published package, and the manifest keeps its bytes.
    let package = base.package.as_ref().unwrap().object_digest.0.clone();
    publication.objects.push(
        previous
            .objects
            .iter()
            .find(|o| o.digest() == package)
            .unwrap()
            .clone(),
    );
    lineage.versions.push(base);
    lineage
        .versions
        .sort_by(|a, b| a.version_uuid.cmp(&b.version_uuid));
    replace(&mut publication, lineage);
    publication
}
pub(super) fn cases() -> Vec<Case> {
    let mut cases = inventory_cases();
    for (name, stale, dependent, proposal) in [
        ("explicit-rewrite-prune-grace", false, false, false),
        ("rewrite-stale-lease-refused", true, false, false),
        ("prune-branch-origin-refused", false, true, false),
        ("prune-proposal-source-refused", false, false, true),
    ] {
        let base = research_publication(v1(1100), 3100, 3101, 3102, 'c', None);
        let mut model = RepoModel::default();
        let mut steps = vec![token(&base.repository, "token")];
        steps.extend(publish(&base, "token", "base", &mut model));
        let mut old = next(&base, 1101, 3103);
        old.expected_revision = Some(model.revision(&base.repository));
        steps.extend(retained_open(&old, "old", &[&base]));
        model.advance(&old);
        steps.push(commit(
            &old,
            "token",
            "old",
            complete(&old, &model.revision(&old.repository)),
        ));
        let orphan = read(&old)
            .version(&operation(3103).to_string())
            .unwrap()
            .package
            .as_ref()
            .unwrap()
            .object_digest
            .0
            .clone();
        let mut replacement = next(&old, 1102, 3104);
        if dependent {
            let mut lineage = read(&replacement);
            lineage
                .versions
                .retain(|v| v.version_uuid == operation(3104).to_string());
            lineage.versions[0].parents.clear();
            replace(&mut replacement, lineage);
        }
        if proposal {
            // A Proposal whose frozen source is the replaced head survives the
            // rewrite, and its payload projection stays a listed Version; the
            // pruned head remains their dependency.
            let mut lineage = read(&replacement);
            let payload = Object::package("research Version package fixture: proposal payload");
            let package = PortablePackageReference {
                format: PORTABLE_FORMAT.into(),
                package_digest: Sha256Digest(digest_of('e')),
                producing_core_version: "0.6.0".into(),
                object_digest: Sha256Digest(payload.digest()),
            };
            lineage.versions.push(LineageVersion {
                parents: vec![],
                version_uuid: operation(3105).to_string(),
                identity_digest: Sha256Digest(digest_of('9')),
                kind: "projection".into(),
                branch_uuid: lineage.branches[0].branch_uuid.clone(),
                source_version_uuid: Some(operation(3103).to_string()),
                author: None,
                committer: None,
                package: Some(package.clone()),
            });
            lineage
                .versions
                .sort_by(|a, b| a.version_uuid.cmp(&b.version_uuid));
            lineage.proposals.push(LineageProposal {
                proposal_uuid: operation(3106).to_string(),
                source_branch_uuid: lineage.branches[0].branch_uuid.clone(),
                source_version_uuid: operation(3103).to_string(),
                payload_version_uuid: operation(3105).to_string(),
                package,
            });
            replacement.objects.push(payload);
            replace(&mut replacement, lineage);
        }
        replacement.rewrite = true;
        replacement.prune = true;
        replacement.expected_revision = Some(if stale {
            digest_of('f')
        } else {
            model.revision(&base.repository)
        });
        steps.extend(retained_open(&replacement, "replacement", &[&base, &old]));
        let expect = if stale {
            error(412, "ref_conflict")
        } else if dependent || proposal {
            error(400, "invalid_input")
        } else {
            model.advance(&replacement);
            complete(&replacement, &model.revision(&base.repository))
        };
        steps.push(commit(&replacement, "token", "replacement", expect));
        steps.push(get_refs(&base.repository, &model));
        if !stale && !dependent && !proposal {
            let url = format!("{}/.gf/objects/{orphan}", repo_url(&base.repository));
            let bytes = old.objects.iter().find(|o| o.digest() == orphan).unwrap();
            steps.push(send(
                request(HubMethod::Get, &url),
                status(200).text(String::from_utf8(bytes.bytes.clone()).unwrap()),
            ));
            steps.push(act(Action::AdvanceClock { seconds: 900 }));
            steps.push(send(
                request(HubMethod::Get, &url),
                error(404, "invalid_input"),
            ));
            // The Branch base/origin stays a retained Version, so its published
            // package stays reachable after the replacement's grace.
            let kept = read(&base).versions[0]
                .package
                .as_ref()
                .unwrap()
                .object_digest
                .0
                .clone();
            let bytes = base.objects.iter().find(|o| o.digest() == kept).unwrap();
            steps.push(send(
                request(
                    HubMethod::Get,
                    format!("{}/.gf/objects/{kept}", repo_url(&base.repository)),
                ),
                status(200).text(String::from_utf8(bytes.bytes.clone()).unwrap()),
            ));
        }
        cases.push(case(name,"Explicit committed history intent preserves the revision lease and native dependencies; unreachable bytes have a fixed 900-second reference grace.",steps));
    }
    cases.push(package_strip_case());
    cases
}

/// A retained Version never loses its published package: retention only
/// treats lineage-referenced packages as reachable.
fn package_strip_case() -> Case {
    let base = research_publication(v1(1300), 3100, 3101, 3102, 'c', None);
    let mut model = RepoModel::default();
    let mut steps = vec![token(&base.repository, "token")];
    steps.extend(publish(&base, "token", "base", &mut model));
    let mut stripped = next(&base, 1301, 3103);
    let mut lineage = read(&stripped);
    let retained = lineage.branches[0].base_version_uuid.clone();
    let version = lineage
        .versions
        .iter_mut()
        .find(|v| v.version_uuid == retained)
        .unwrap();
    let package = version.package.take().unwrap().object_digest.0;
    replace(&mut stripped, lineage);
    stripped.objects.retain(|o| o.digest() != package);
    stripped.expected_revision = Some(model.revision(&base.repository));
    steps.extend(retained_open(&stripped, "stripped", &[&base]));
    steps.push(commit(
        &stripped,
        "token",
        "stripped",
        error(400, "invalid_input"),
    ));
    steps.push(get_refs(&base.repository, &model));
    case(
        "retained-version-package-strip-refused",
        "A publication that drops the published package of a Version it retains is refused before any ref or retirement change.",
        steps,
    )
}

fn inventory_cases() -> Vec<Case> {
    [false, true].into_iter().map(|retained| {
        let base = research_publication(v1(1200), 3100, 3101, 3102, 'c', None);
        let mut model = RepoModel::default();
        let mut steps = vec![token(&base.repository, "token")];
        steps.extend(publish(&base, "token", "base", &mut model));
        let original = read(&base).versions[0].clone();
        let digest = original.package.as_ref().unwrap().object_digest.0.clone();
        let bytes = base.objects.iter().find(|o| o.digest() == digest).unwrap();
        let mut candidate = next(&base, 1201, 3103);
        let mut lineage = read(&candidate);
        let retained_version = lineage.versions.iter_mut().find(|v| v.version_uuid == original.version_uuid).unwrap();
        *retained_version = original;
        replace(&mut candidate, lineage);
        if !retained {
            candidate.objects.retain(|o| o.digest() != digest);
        }
        candidate.prune = true;
        candidate.expected_revision = Some(model.revision(&base.repository));
        steps.extend(retained_open(&candidate, "candidate", &[&base]));
        let expected = if retained {
            model.advance(&candidate);
            complete(&candidate, &model.revision(&base.repository))
        } else { error(400, "invalid_input") };
        steps.push(commit(&candidate, "token", "candidate", expected));
        steps.push(get_refs(&base.repository, &model));
        steps.push(act(Action::AdvanceClock { seconds: 900 }));
        steps.push(send(request(HubMethod::Get, format!("{}/.gf/objects/{}", repo_url(&base.repository), bytes.digest())), status(200).text(String::from_utf8(bytes.bytes.clone()).unwrap())));
        if !retained {
            let new_package = read(&candidate).version(&operation(3103).to_string()).unwrap().package.as_ref().unwrap().object_digest.0.clone();
            steps.push(send(request(HubMethod::Get, format!("{}/.gf/objects/{new_package}",repo_url(&base.repository))), error(404,"invalid_input")));
        }
        case(if retained { "retained-native-package-survives-prune" } else { "missing-native-package-inventory-refused" }, "Every advertised Version package must bind the exact authenticated manifest/session inventory before ref mutation or retirement.", steps)
    }).collect()
}

pub(super) fn retained_open(
    publication: &Publication,
    prefix: &str,
    previous: &[&Publication],
) -> Vec<Step> {
    let retained: BTreeSet<_> = previous
        .iter()
        .flat_map(|p| p.objects.iter().map(Object::digest))
        .collect();
    let mut opened = open_new(publication, "token", prefix);
    let Step::Request { expect, .. } = &mut opened else {
        unreachable!()
    };
    for (index, object) in publication.objects.iter().enumerate() {
        if retained.contains(&object.digest()) {
            expect.json.as_mut().unwrap()["uploads"][index]["received"] = json!(object.bytes.len());
        }
    }
    let mut steps = vec![opened];
    steps.extend(
        publication
            .objects
            .iter()
            .enumerate()
            .filter(|(_, object)| !retained.contains(&object.digest()))
            .map(|(index, object)| put_chunk(publication, prefix, index, 0, object.bytes.len())),
    );
    steps
}

#[path = "history/current_refs.rs"]
mod current_refs;
#[path = "history/move_retention.rs"]
mod move_retention;
