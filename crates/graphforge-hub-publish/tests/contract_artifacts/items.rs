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

fn bind(namespace: &str, account: &str, handle: &str) -> Step {
    act(Action::SetPublicationAccount {
        namespace: namespace.into(),
        account: account.into(),
        canonical_handle: handle.into(),
    })
}

fn stable_accounts() -> Case {
    let first = publication("organization", "original", 5200, 'a');
    let mut steps = vec![
        bind(
            "organization",
            "reference-account-0000000000000001",
            "organization",
        ),
        token(&first.repository, "old_token"),
    ];
    steps.extend(publish(
        &first,
        "old_token",
        "first",
        &mut RepoModel::default(),
    ));
    let collision = publication("new-publisher", "collision", 5205, 'd');
    steps.push(token(&collision.repository, "collision_token"));
    steps.push(open_new(&collision, "collision_token", "collision"));
    steps.extend(upload_all(&collision, "collision"));
    steps.push(commit(
        &collision,
        "collision_token",
        "collision",
        status(409).json(json!({"code":"id_taken", "message":graphforge_hub_publish::ID_TAKEN_MESSAGE, "items":[{"kind":"ontology_module", "id":"ont_aaaaaaaaaaaaaaaa"}]})),
    ));
    steps.push(refs_absent(&collision.repository));
    // A new canonical handle and repository alias still represent account A.
    let renamed = publication("renamed-owner", "renamed", 5201, 'b');
    steps.push(bind(
        "renamed-owner",
        "reference-account-0000000000000001",
        "renamed-owner",
    ));
    steps.push(token(&renamed.repository, "renamed_token"));
    steps.extend(publish(
        &renamed,
        "renamed_token",
        "renamed",
        &mut RepoModel::default(),
    ));
    // Handle reuse binds B. Existing grants cannot turn into B's grants, and B
    // cannot take A's ownership even though it now holds A's former handle.
    steps.push(bind("organization", "opaque-account-B", "organization"));
    let fresh = publication("organization", "original", 5202, 'c');
    steps.push(send(
        request(
            HubMethod::Post,
            format!("{}/.gf/publish/sessions", repo_url(&fresh.repository)),
        )
        .bearer("old_token")
        .json(fresh.open_body()),
        error(403, "auth_denied"),
    ));
    steps.push(token(&first.repository, "new_token"));
    // Completed operation replay is account-bound too.
    steps.push(send(
        request(
            HubMethod::Post,
            format!("{}/.gf/publish/sessions", repo_url(&first.repository)),
        )
        .bearer("new_token")
        .json(first.open_body()),
        error(403, "auth_denied"),
    ));
    let reuse = publication("organization", "reuse", 5203, 'a');
    steps.push(token(&reuse.repository, "reuse_token"));
    steps.extend(publish(
        &reuse,
        "reuse_token",
        "reuse",
        &mut RepoModel::default(),
    ));
    let changed = publication("organization", "changed", 5204, 'c');
    steps.push(token(&changed.repository, "changed_token"));
    steps.push(open_new(&changed, "changed_token", "changed"));
    steps.extend(upload_all(&changed, "changed"));
    steps.push(act(Action::SetItemVisibility {
        publisher: "organization".into(),
        id: "ont_aaaaaaaaaaaaaaaa".into(),
        visible: true,
    }));
    steps.push(commit(&changed, "changed_token", "changed", status(409).json(json!({
        "code":"id_taken", "message":graphforge_hub_publish::ID_TAKEN_MESSAGE,
        "items":[{"kind":"ontology_module", "id":"ont_aaaaaaaaaaaaaaaa", "owner":"renamed-owner"}]
    }))));
    steps.push(refs_absent(&changed.repository));
    case(
        "permanent-id-stable-accounts",
        "Stable account ownership survives handle changes and repository aliases; reassigned handles invalidate old grants and cannot replay another account's operation or acquire its ID.",
        steps,
    )
}

#[test]
fn stable_account_grants_and_handle_reuse_preserve_ownership() {
    let case = stable_accounts();
    replay(&ReferenceHub::default(), &case.name, &case.steps);
}
