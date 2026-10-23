//! Permanent-ID ownership, exact semantic reuse, and privacy-filtered refusals.
use super::*;

const MODULE: &str = "ont_7k2m9x4q8r3t5v1w";
const BRIDGE: &str = "brg_7k2m9x4q8r3t5v1w";

fn inventory(version: &str, digest: char) -> Value {
    json!({"composition_digest":digest_of('a'),
        "modules":[{"id":format!("https://graphforge.sh/{MODULE}"),"version":version,"content_digest":digest_of(digest)}],
        "bridge_sets":[{"id":format!("https://graphforge.sh/{BRIDGE}"),"version":version,"content_digest":digest_of(digest)}]})
}

pub(super) fn cases() -> Vec<Case> {
    let owner = identity("org", "source");
    let other = identity("alice", "copy");
    let mut first = Publication::new(
        &owner,
        operation(3000),
        vec![Object::package("owner package")],
    );
    first.ontology = Some(inventory("1", 'c'));
    let mut cases = Vec::new();
    let setup = || {
        let mut model = RepoModel::default();
        let mut steps = vec![token(&owner, "owner_token"), token(&other, "other_token")];
        steps.extend(publish(&first, "owner_token", "first", &mut model));
        (steps, model)
    };
    let (steps, _) = setup();
    cases.push(case("claim-new-permanent-ids", "The first committed advertised module and bridge IDs are claimed by the publisher, without opening package contents.", steps));
    let (mut steps, _) = setup();
    let mut reused = Publication::new(
        &other,
        operation(3001),
        vec![Object::package("different transporting project package")],
    );
    reused.ontology = first.ontology.clone();
    steps.extend(publish(
        &reused,
        "other_token",
        "reused",
        &mut RepoModel::default(),
    ));
    // Exact reuse did not transfer the claim to Alice.
    let mut next = first.clone();
    next.operation = operation(3002);
    next.ontology = Some(inventory("2", 'd'));
    let mut model = RepoModel::default();
    model.advance(&first);
    next.expected_revision = Some(model.revision(&owner));
    steps.extend(publish(&next, "owner_token", "next", &mut model));
    // All previously published authored versions remain reusable permanently.
    let third = identity("bob", "reuse");
    steps.push(token(&third, "third_token"));
    let mut reused_next = next.clone();
    reused_next.repository = third;
    reused_next.operation = operation(3003);
    reused_next.expected_revision = None;
    steps.extend(publish(
        &reused_next,
        "third_token",
        "reused_next",
        &mut RepoModel::default(),
    ));
    let fourth = identity("carol", "old-reuse");
    steps.push(token(&fourth, "fourth_token"));
    let mut reused_old = first.clone();
    reused_old.repository = fourth;
    reused_old.operation = operation(3004);
    steps.extend(publish(
        &reused_old,
        "fourth_token",
        "reused_old",
        &mut RepoModel::default(),
    ));
    cases.push(case("exact-permanent-id-reuse", "An exact (ID, authored version, canonical digest) is reusable by another publisher even in a different package. Ownership stays with the first publisher, who may publish new content.", steps));

    for (name, visible, publish_as) in [
        ("permanent-id-taken-owner-hidden", false, false),
        ("permanent-id-taken-owner-visible", true, false),
        ("permanent-id-taken-publish-as-organization", true, true),
    ] {
        let (mut steps, _) = setup();
        for id in [MODULE, BRIDGE] {
            steps.push(act(Action::SetItemPermissions {
                token: "${other_token}".into(),
                id: id.into(),
                visible,
                publish_as_owner: publish_as,
            }));
        }
        let mut attempt = reused.clone();
        attempt.operation = operation(3010);
        attempt.ontology = Some(inventory("1", 'd'));
        steps.push(open_new(&attempt, "other_token", "attempt"));
        steps.extend(upload_all(&attempt, "attempt"));
        let items: Vec<_> = [("bridge_set", BRIDGE), ("ontology_module", MODULE)]
            .into_iter()
            .map(|(kind, id)| {
                let mut item = json!({"kind":kind,"id":id});
                if visible {
                    item["owner"] = json!("org");
                }
                if publish_as {
                    item["publish_as_owner"] = json!(true);
                }
                item
            })
            .collect();
        steps.push(commit(&attempt, "other_token", "attempt", status(409).json(json!({"code":"id_taken","message":"permanent item ID is owned by another publisher","items":items}))));
        // The same digest at another authored version is also foreign content.
        let mut wrong_version = attempt.clone();
        wrong_version.operation = operation(3011);
        wrong_version.ontology = Some(inventory("2", 'c'));
        steps.push(open_new(&wrong_version, "other_token", "wrong_version"));
        steps.extend(upload_all(&wrong_version, "wrong_version"));
        steps.push(commit(&wrong_version, "other_token", "wrong_version", status(409).json(json!({"code":"id_taken","message":"permanent item ID is owned by another publisher","items":items}))));
        // No repository revision was installed by either refusal.
        steps.push(send(
            request(HubMethod::Get, format!("{}/.gf/refs", repo_url(&other))),
            error(404, "invalid_input"),
        ));
        cases.push(case(name, "Foreign new content is refused at commit with a fixed message and sorted per-ID hints. Hidden ownership stays absent; organization publishing permission is explicit.", steps));
    }
    let (mut steps, _) = setup();
    let fresh = "ont_aaaaaaaaaaaaaaaa";
    let mut rejected = reused.clone();
    rejected.operation = operation(3020);
    rejected.ontology = Some(inventory("2", 'd'));
    rejected.ontology.as_mut().unwrap()["modules"].as_array_mut().unwrap().push(json!({"id":format!("https://graphforge.sh/{fresh}"),"version":"1","content_digest":digest_of('e')}));
    steps.push(open_new(&rejected, "other_token", "rejected"));
    steps.extend(upload_all(&rejected, "rejected"));
    steps.push(commit(&rejected, "other_token", "rejected", status(409).json(json!({"code":"id_taken","message":"permanent item ID is owned by another publisher","items":[{"kind":"bridge_set","id":BRIDGE},{"kind":"ontology_module","id":MODULE}]}))));
    let third = identity("bob", "claim");
    steps.push(token(&third, "third_token"));
    let mut claim_fresh = Publication::new(
        &third,
        operation(3021),
        vec![Object::package("third publisher")],
    );
    claim_fresh.ontology = Some(
        json!({"composition_digest":digest_of('a'),"modules":[{"id":format!("https://graphforge.sh/{fresh}"),"version":"2","content_digest":digest_of('f')}],"bridge_sets":[]}),
    );
    steps.extend(publish(
        &claim_fresh,
        "third_token",
        "fresh",
        &mut RepoModel::default(),
    ));
    cases.push(case("permanent-id-refusal-is-atomic", "A rejected multi-ID commit claims nothing: another publisher can claim its previously unowned ID with different content.", steps));
    let embedded = Publication::new(
        &owner,
        operation(3030),
        vec![Object::package(&format!(
            "ontology_id=https://graphforge.sh/{MODULE}; private unadvertised content"
        ))],
    );
    let mut steps = vec![token(&owner, "owner_token"), token(&other, "other_token")];
    steps.extend(publish(
        &embedded,
        "owner_token",
        "embedded",
        &mut RepoModel::default(),
    ));
    let mut advertised = reused;
    advertised.operation = operation(3031);
    advertised.ontology = Some(inventory("3", 'e'));
    steps.extend(publish(
        &advertised,
        "other_token",
        "advertised",
        &mut RepoModel::default(),
    ));
    cases.push(case("only-advertised-permanent-ids-are-claimed","IDs embedded in package bytes but absent from the advertised ontology inventory are never inspected or claimed; another publisher can claim them with new content.",steps));
    cases
}
