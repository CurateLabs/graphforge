//! A repository move regenerates control documents; retirement scheduled by an
//! earlier prune must never delete what the moved refs reach.
use super::current_refs::execute;
use super::*;

const SCRATCH: &str = "application/octet-stream";

struct Moved {
    hub: ReferenceHub,
    base: Publication,
    new: RepositoryIdentity,
    /// Bytes equal to the lineage a move to `new` will regenerate.
    rehomed: Object,
    unreachable: Object,
    vars: BTreeMap<String, String>,
}

fn get(hub: &ReferenceHub, url: &str) -> HubResponse {
    hub.handle(&HubRequest::new(HubMethod::Get, url))
}

fn object_url(repository: &RepositoryIdentity, digest: &str) -> String {
    format!("{}/.gf/objects/{digest}", repo_url(repository))
}

/// The regenerated lineage is deterministic: a twin Hub moved the same way
/// yields exactly the bytes the real Hub will insert at its move.
fn rehomed_lineage(base: &Publication, new: &RepositoryIdentity) -> Object {
    let twin = ReferenceHub::new();
    let mut model = RepoModel::default();
    let mut steps = vec![token(&base.repository, "token")];
    steps.extend(publish(base, "token", "base", &mut model));
    execute(&twin, steps, &mut BTreeMap::new());
    twin.move_repository(&base.repository, new).unwrap();
    let manifest = get(&twin, &format!("{}/.gf/manifest", repo_url(new)));
    let manifest =
        DiscoveryManifest::from_json(&manifest.body, DiscoveryLimits::default()).unwrap();
    let digest = manifest.lineage.unwrap().object_digest.0;
    Object {
        media_type: SCRATCH,
        bytes: get(&twin, &object_url(new, &digest)).body,
    }
}

/// Publish the base, plant the future rehomed lineage (and one genuinely
/// unreferenced object) in the inventory, then prune both from it.
fn setup() -> Moved {
    let new = identity("research", "renamed");
    let base = research_publication(v1(1400), 3100, 3101, 3102, 'c', None);
    let rehomed = rehomed_lineage(&base, &new);
    let unreachable = Object {
        media_type: SCRATCH,
        bytes: b"genuinely unreachable bytes".to_vec(),
    };
    let hub = ReferenceHub::new();
    let mut model = RepoModel::default();
    let mut steps = vec![token(&base.repository, "token")];
    steps.extend(publish(&base, "token", "base", &mut model));
    let mut planted = base.clone();
    planted.operation = operation(1401);
    planted.objects.push(rehomed.clone());
    planted.objects.push(unreachable.clone());
    planted.objects.sort_by_key(Object::digest);
    planted.expected_revision = Some(model.revision(&base.repository));
    steps.extend(retained_open(&planted, "planted", &[&base]));
    model.advance(&planted);
    steps.push(commit(
        &planted,
        "token",
        "planted",
        complete(&planted, &model.revision(&base.repository)),
    ));
    let mut pruned = base.clone();
    pruned.operation = operation(1402);
    pruned.prune = true;
    pruned.expected_revision = Some(model.revision(&base.repository));
    steps.extend(retained_open(&pruned, "pruned", &[&base, &planted]));
    model.advance(&pruned);
    steps.push(commit(
        &pruned,
        "token",
        "pruned",
        complete(&pruned, &model.revision(&base.repository)),
    ));
    let mut vars = BTreeMap::new();
    execute(&hub, steps, &mut vars);
    for object in [&rehomed, &unreachable] {
        assert_eq!(
            get(&hub, &object_url(&base.repository, &object.digest())).status,
            200,
            "pruned bytes keep serving through the grace"
        );
    }
    Moved {
        hub,
        base,
        new,
        rehomed,
        unreachable,
        vars,
    }
}

#[test]
fn move_after_prune_keeps_the_regenerated_lineage_through_the_grace() {
    let Moved {
        hub,
        base,
        new,
        rehomed,
        unreachable,
        ..
    } = setup();
    hub.advance_clock(100);
    hub.move_repository(&base.repository, &new).unwrap();
    hub.advance_clock(800);
    let url = object_url(&new, &rehomed.digest());
    let served = get(&hub, &url);
    assert_eq!(served.status, 200, "the current lineage was collected");
    assert_eq!(served.body, rehomed.bytes);
    // The current manifest still selects that lineage, and it binds.
    let manifest = get(&hub, &format!("{}/.gf/manifest", repo_url(&new)));
    let manifest =
        DiscoveryManifest::from_json(&manifest.body, DiscoveryLimits::default()).unwrap();
    let selected = manifest.lineage.unwrap();
    assert_eq!(selected.object_digest.0, rehomed.digest());
    let lineage = ResearchLineage::from_json(&served.body, DiscoveryLimits::default()).unwrap();
    assert_eq!(selected.lineage_digest, lineage.canonical_digest().unwrap());
    assert_eq!(lineage.repository, new);
    // The retained Branch base/origin package is still served.
    let package = &lineage.versions[0]
        .package
        .as_ref()
        .unwrap()
        .object_digest
        .0;
    assert_eq!(get(&hub, &object_url(&new, package)).status, 200);
    // Pruning is not disabled: an object no ref reaches is gone after its grace.
    assert_eq!(
        get(&hub, &object_url(&base.repository, &unreachable.digest())).status,
        404
    );
}

#[test]
fn move_restarts_the_grace_of_bytes_it_makes_live_then_unreachable_again() {
    let Moved {
        hub,
        base,
        new,
        rehomed,
        mut vars,
        ..
    } = setup();
    hub.advance_clock(100);
    hub.move_repository(&base.repository, &new).unwrap();
    hub.advance_clock(400);
    // A prune on the moved repository now retires the regenerated lineage.
    let refs = get(&hub, &format!("{}/.gf/refs", repo_url(&new)));
    let mut successor = {
        let mut lineage = read(&base);
        lineage.repository = new.clone();
        let mut moved = base.clone();
        moved.repository = new.clone();
        replace(&mut moved, lineage);
        next(&moved, 1403, 3103)
    };
    successor.repository = new.clone();
    let mut lineage = read(&successor);
    lineage.repository = new.clone();
    replace(&mut successor, lineage);
    successor.prune = true;
    successor.expected_revision = Some(sha256_digest(&refs.body));
    let mut model = RepoModel::default();
    model.advance(&successor);
    let mut steps = vec![token(&new, "token")];
    steps.extend(retained_open(&successor, "successor", &[&base]));
    steps.push(commit(
        &successor,
        "token",
        "successor",
        complete(&successor, &model.revision(&new)),
    ));
    execute(&hub, steps, &mut vars);
    // Its fixed 900 s grace runs from the prune, not from the earlier one.
    hub.advance_clock(499);
    let url = object_url(&new, &rehomed.digest());
    assert_eq!(get(&hub, &url).status, 200, "grace was cut short");
    hub.advance_clock(400);
    assert_eq!(get(&hub, &url).status, 200, "grace was cut short");
    hub.advance_clock(1);
    assert_eq!(
        get(&hub, &url).status,
        404,
        "unreachable bytes are collected"
    );
}
