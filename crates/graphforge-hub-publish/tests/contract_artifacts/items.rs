use super::*;

fn publication(owner: &str, repository: &str, n: u128, digest: char) -> Publication {
    let mut publication = Publication::new(
        &identity(owner, repository),
        operation(n),
        vec![Object::package("permanent item fixture")],
    );
    publication.ontology = Some(json!({
        "composition_digest": digest_of('c'),
        "modules": [{"id": "https://graphforge.sh/ont_aaaaaaaaaaaaaaaa", "version": "1", "content_digest": digest_of(digest)}],
        "bridge_sets": []
    }));
    publication
}

fn refused(publication: &Publication, visible: bool, organization: bool) -> Step {
    let mut item = json!({"kind": "ontology_module", "id": "ont_aaaaaaaaaaaaaaaa"});
    if visible {
        item["owner"] = json!("organization");
    }
    if organization {
        item["publish_as_owner"] = json!(true);
    }
    commit(publication, "publisher_token", "changed", status(409).json(json!({
        "code": "id_taken", "message": graphforge_hub_publish::ID_TAKEN_MESSAGE, "items": [item],
    })))
}

pub(super) fn cases() -> Vec<Case> {
    // Keep every ownership outcome in one fresh Hub. Repeated permission
    // refusals reuse the same session; no claim or operation receipt commits.
    let first = publication("organization", "items", 5100, 'a');
    let mut model = RepoModel::default();
    let mut steps = vec![token(&first.repository, "owner_token")];
    steps.extend(publish(&first, "owner_token", "first", &mut model));

    let reuse = publication("alice", "reuse", 5101, 'a');
    let mut model = RepoModel::default();
    steps.push(token(&reuse.repository, "reuse_token"));
    steps.extend(publish(&reuse, "reuse_token", "reuse", &mut model));

    let changed = publication("alice", "changed", 5102, 'b');
    steps.push(token(&changed.repository, "publisher_token"));
    steps.push(open_new(&changed, "publisher_token", "changed"));
    steps.extend(upload_all(&changed, "changed"));
    steps.push(refused(&changed, false, false));
    steps.push(act(Action::SetItemVisibility {
        publisher: "alice".into(),
        id: "ont_aaaaaaaaaaaaaaaa".into(),
        visible: true,
    }));
    steps.push(refused(&changed, true, false));
    steps.push(act(Action::SetOrganizationPublishPermission {
        publisher: "alice".into(),
        organization: "organization".into(),
        allowed: true,
    }));
    steps.push(refused(&changed, true, true));
    steps.push(refs_absent(&changed.repository));
    vec![case(
        "permanent-id-ownership",
        "Claim a new ID, reuse its exact tuple without transferring ownership, then refuse new content with hidden owner, visible owner, and authorized organization hint.",
        steps,
    )]
}
