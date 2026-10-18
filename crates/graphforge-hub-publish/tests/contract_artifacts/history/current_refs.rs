//! Actual authenticated reference commits, with independent current ref snapshots.
use super::*;

fn execute(hub: &ReferenceHub, steps: Vec<Step>, variables: &mut BTreeMap<String, String>) {
    for (index, step) in steps.into_iter().enumerate() {
        match step {
            Step::Request { request, expect } => {
                check(
                    &hub.handle(&build(&request, variables, "current refs")),
                    &expect,
                    variables,
                    &format!("current refs {index}"),
                );
            }
            Step::Action {
                action:
                    Action::IssueToken {
                        scope,
                        ttl_seconds,
                        capture,
                    },
            } => {
                variables.insert(capture, hub.issue_token(&scope, ttl_seconds));
            }
            Step::Action { .. } => panic!("unexpected action"),
        }
    }
}
fn advance(previous: &Publication, branch: usize, id: u128) -> Publication {
    let mut next = previous.clone();
    next.operation = operation(id);
    let mut lineage = read(previous);
    let old_head = &lineage.branches[branch].head_version_uuid;
    let mut version = lineage.version(old_head).unwrap().clone();
    version.parents = vec![old_head.clone()];
    version.version_uuid = operation(id).to_string();
    version.identity_digest = Sha256Digest(sha256_digest(version.version_uuid.as_bytes()));
    let object = Object::package(&format!("native ref fixture {id}"));
    version.package.as_mut().unwrap().object_digest = Sha256Digest(object.digest());
    lineage.branches[branch]
        .head_version_uuid
        .clone_from(&version.version_uuid);
    next.refs = vec![lineage.branches[branch].ref_name.clone()];
    next.resolved_ref = next.refs[0].clone();
    lineage.versions.push(version);
    lineage
        .versions
        .sort_by(|a, b| a.version_uuid.cmp(&b.version_uuid));
    next.objects.push(object);
    replace(&mut next, lineage);
    next
}
fn commit_update(
    hub: &ReferenceHub,
    next: &mut Publication,
    previous: &Publication,
    model: &mut RepoModel,
    vars: &mut BTreeMap<String, String>,
    expected_error: Option<(u16, &str)>,
) {
    next.expected_revision = Some(model.revision(&next.repository));
    let mut steps = retained_open(next, "candidate", &[previous]);
    let expected = if let Some((status, code)) = expected_error {
        error(status, code)
    } else {
        model.advance(next);
        complete(next, &model.revision(&next.repository))
    };
    steps.push(commit(next, "token", "candidate", expected));
    steps.push(get_refs(&next.repository, model));
    execute(hub, steps, vars);
}
#[test]
fn alternating_current_refs_ignore_shadow_heads_but_reject_forgery_non_ff_and_prune() {
    let hub = ReferenceHub::new();
    let mut base = research_publication(v1(1300), 3100, 3101, 3102, 'c', None);
    let side = research_publication(v1(1301), 3100, 3111, 3112, 'd', None);
    let mut lineage = read(&base);
    let mut branch = read(&side).branches.remove(0);
    branch.ref_name = "side".into();
    lineage.branches.push(branch);
    let version = read(&side).versions.remove(0);
    let digest = &version.package.as_ref().unwrap().object_digest.0;
    base.objects.push(
        side.objects
            .iter()
            .find(|o| o.digest() == *digest)
            .unwrap()
            .clone(),
    );
    lineage.versions.push(version);
    replace(&mut base, lineage);
    base.refs = vec!["main".into(), "side".into()];
    let mut model = RepoModel::default();
    let mut vars = BTreeMap::new();
    let mut steps = vec![token(&base.repository, "token")];
    steps.extend(publish(&base, "token", "base", &mut model));
    execute(&hub, steps, &mut vars);
    let mut a = advance(&base, 0, 3103);
    commit_update(&hub, &mut a, &base, &mut model, &mut vars, None);
    let mut b = advance(&a, 1, 3113);
    commit_update(&hub, &mut b, &a, &mut model, &mut vars, None);
    let mut c = advance(&b, 0, 3104);
    commit_update(&hub, &mut c, &b, &mut model, &mut vars, None);
    for (id, prune, changed_side, code) in [
        (1400, false, true, "invalid_input"),
        (1401, false, false, "non_fast_forward"),
        (1402, true, true, "invalid_input"),
        (1403, false, true, "invalid_input"),
    ] {
        let mut bad = c.clone();
        bad.operation = operation(id);
        bad.prune = prune;
        bad.rewrite = changed_side;
        let mut lineage = read(&bad);
        let branch = usize::from(changed_side);
        if id == 1403 {
            lineage.branches[branch].label = "forged historical creation".into();
        } else {
            lineage.branches[branch].head_version_uuid =
                operation(if changed_side { 3112 } else { 3102 }).to_string();
        }
        if prune {
            lineage
                .versions
                .retain(|v| v.version_uuid != operation(3113).to_string());
        }
        replace(&mut bad, lineage);
        commit_update(
            &hub,
            &mut bad,
            &c,
            &mut model,
            &mut vars,
            Some((if changed_side { 400 } else { 409 }, code)),
        );
        let control = bad
            .objects
            .iter()
            .find(|o| o.media_type == RESEARCH_LINEAGE_MEDIA_TYPE)
            .unwrap();
        execute(
            &hub,
            vec![send(
                request(
                    HubMethod::Get,
                    format!(
                        "{}/.gf/objects/{}",
                        repo_url(&bad.repository),
                        control.digest()
                    ),
                ),
                error(404, "invalid_input"),
            )],
            &mut vars,
        );
    }
}
