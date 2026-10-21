//! Two actual reference origins; native exports and publish behavior remain real.
use super::*;
struct Hubs {
    origin: ReferenceHub,
    destination: ReferenceHub,
    calls: RefCell<Vec<(String, String)>>,
}
impl Transport for Hubs {
    fn get(
        &self,
        _: &Url,
        _: Option<u64>,
        _: Option<&str>,
        _: u64,
    ) -> Result<HttpResponse, GfError> {
        panic!("publication uses bounded exchange");
    }
    fn exchange(&self, request: &HubRequest, limit: usize) -> Result<HubResponse, GfError> {
        let url = Url::parse(&request.url).unwrap();
        let host = url.host_str().unwrap();
        self.calls
            .borrow_mut()
            .push((host.into(), url.path().into()));
        let hub = match host {
            "origin.example" => &self.origin,
            "destination.example" => &self.destination,
            _ => return Err(network("unexpected test origin")),
        };
        let response = hub.handle(request);
        assert!(response.body.len() <= limit);
        Ok(response)
    }
}
fn run(hubs: &Hubs, graph: &GraphForge, args: &PublishArgs, hub: &ReferenceHub) -> PublishOutcome {
    let (_, base) = parse_input(&args.repository).unwrap();
    let id = RepositoryIdentity::parse(base.path().trim_start_matches('/')).unwrap();
    let mut notices = Vec::new();
    let mut environment = PublishEnvironment {
        bypass: None,
        credential: Credential::Token(PublishToken::new(hub.issue_token(&publish_scope(&id), 900))),
        wait: &|_| panic!("no device flow"),
        notices: &mut notices,
        resolution: None,
    };
    publish(hubs, graph, args, &mut environment).unwrap()
}
#[test]
fn ordinary_established_cross_hub_fork_update_does_not_guess_or_probe_origin_host() {
    let origin = history();
    let hubs = Hubs {
        origin: ReferenceHub::with_config(ReferenceHubConfig {
            base_url: "https://origin.example".into(),
            ..ReferenceHubConfig::default()
        })
        .unwrap(),
        destination: ReferenceHub::with_config(ReferenceHubConfig {
            base_url: "https://destination.example".into(),
            ..ReferenceHubConfig::default()
        })
        .unwrap(),
        calls: RefCell::new(Vec::new()),
    };
    let mut original = on_ref("curate/claims", "main");
    original.repository = "https://origin.example/curate/claims".into();
    run(&hubs, &origin.graph, &original, &hubs.origin);
    let (mut fork, branch) = independent_fork(&origin, origin.head);
    let mut initial = on_ref("curate/fork", "main");
    initial.repository = "https://destination.example/curate/fork".into();
    initial.fork_of = Some(original.repository);
    run(&hubs, &fork, &initial, &hubs.destination);
    edit_by(&mut fork, branch, 8, "Independent author");
    let mut update = initial;
    update.fork_of = None;
    let before = hubs.calls.borrow().len();
    run(&hubs, &fork, &update, &hubs.destination);
    assert!(
        hubs.calls.borrow()[before..]
            .iter()
            .all(|(host, _)| host == "destination.example")
    );
    let root = origin.root.path().join("independent-fork");
    drop(fork);
    let reopened = GraphForge::new(root.to_str()).unwrap();
    let query = reopened
        .open_research_branch(branch)
        .unwrap()
        .graph()
        .execute("MATCH (n:Item) RETURN sum(n.score)")
        .unwrap();
    assert_eq!(
        query.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        8
    );
}
