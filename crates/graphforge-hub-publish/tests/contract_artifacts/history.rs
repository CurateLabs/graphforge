//! Deterministic destructive history cases through the same public corpus mapping.
use super::*;
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
    let mut base = old
        .versions
        .iter()
        .find(|v| v.version_uuid == old.branches[0].base_version_uuid)
        .unwrap()
        .clone();
    base.package = None;
    lineage.versions.push(base);
    lineage
        .versions
        .sort_by(|a, b| a.version_uuid.cmp(&b.version_uuid));
    replace(&mut publication, lineage);
    publication
}
pub(super) fn cases() -> Vec<Case> {
    let mut cases = Vec::new();
    for (name, stale, dependent) in [
        ("explicit-rewrite-prune-grace", false, false),
        ("rewrite-stale-lease-refused", true, false),
        ("prune-branch-origin-refused", false, true),
    ] {
        let base = research_publication(v1(1100), 3100, 3101, 3102, 'c', None);
        let mut model = RepoModel::default();
        let mut steps = vec![token(&base.repository, "token")];
        steps.extend(publish(&base, "token", "base", &mut model));
        let mut old = next(&base, 1101, 3103);
        old.expected_revision = Some(model.revision(&base.repository));
        steps.extend(publish(&old, "token", "old", &mut model));
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
        replacement.rewrite = true;
        replacement.prune = true;
        replacement.expected_revision = Some(if stale {
            digest_of('f')
        } else {
            model.revision(&base.repository)
        });
        steps.push(open_new(&replacement, "token", "replacement"));
        steps.extend(upload_all(&replacement, "replacement"));
        let expect = if stale {
            error(412, "ref_conflict")
        } else if dependent {
            error(400, "invalid_input")
        } else {
            model.advance(&replacement);
            complete(&replacement, &model.revision(&base.repository))
        };
        steps.push(commit(&replacement, "token", "replacement", expect));
        steps.push(get_refs(&base.repository, &model));
        if !stale && !dependent {
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
        }
        cases.push(case(name,"Explicit committed history intent preserves the revision lease and native dependencies; unreachable bytes have a fixed 900-second reference grace.",steps));
    }
    cases
}
