//! Command-path publication round trips, with supplemental native loopback HTTP proof.
use super::*;

#[test]
fn existing_selected_branch_publication_advertises_exact_triple_and_clones() {
    let hub = PublicationHub(
        graphforge_hub_publish::ReferenceHub::with_config(
            graphforge_hub_publish::ReferenceHubConfig {
                base_url: "https://hub.test".into(),
                ..Default::default()
            },
        )
        .unwrap(),
    );
    publication_round_trip(&hub, &hub.0);
}

#[cfg(feature = "hub-test-support")]
mod http {
    use super::*;
    use graphforge_hub_publish::{HubMethod, HubRequest, ReferenceHub, ReferenceHubConfig};
    use std::io::{BufRead, BufReader, Read};
    use std::net::{TcpListener, TcpStream};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    struct Server {
        port: u16,
        stop: Arc<AtomicBool>,
        thread: Option<std::thread::JoinHandle<()>>,
        hub: Arc<ReferenceHub>,
    }
    impl Server {
        fn new() -> Self {
            let hub = Arc::new(
                ReferenceHub::with_config(ReferenceHubConfig {
                    base_url: "https://hub.test".into(),
                    ..Default::default()
                })
                .unwrap(),
            );
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let port = listener.local_addr().unwrap().port();
            let stop = Arc::new(AtomicBool::new(false));
            let serving = hub.clone();
            let stopping = stop.clone();
            let thread = std::thread::spawn(move || {
                loop {
                    let (mut stream, _) = listener.accept().unwrap();
                    if stopping.load(Ordering::Relaxed) {
                        break;
                    }
                    stream
                        .set_read_timeout(Some(std::time::Duration::from_secs(10)))
                        .unwrap();
                    let mut reader = BufReader::new(stream.try_clone().unwrap());
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    let mut parts = line.split_whitespace();
                    let method = match parts.next().unwrap() {
                        "GET" => HubMethod::Get,
                        "HEAD" => HubMethod::Head,
                        "POST" => HubMethod::Post,
                        "PUT" => HubMethod::Put,
                        other => panic!("fixture method {other}"),
                    };
                    let url = format!("https://hub.test{}", parts.next().unwrap());
                    let mut request = HubRequest::new(method, &url);
                    let mut length = 0;
                    loop {
                        let mut line = String::new();
                        assert_ne!(reader.read_line(&mut line).unwrap(), 0);
                        if line == "\r\n" {
                            break;
                        }
                        let (name, value) = line.trim_end().split_once(':').unwrap();
                        if name.eq_ignore_ascii_case("content-length") {
                            length = value.trim().parse().unwrap();
                        }
                        assert!(
                            !name.eq_ignore_ascii_case("transfer-encoding"),
                            "fixture expects bounded content-length requests"
                        );
                        request = request.with_header(name, value.trim());
                    }
                    let mut body = vec![0; length];
                    reader.read_exact(&mut body).unwrap();
                    request = request.with_body(body);
                    let mut response = serving.handle(&request);
                    // This is the same visibility advertisement used by PublicationHub;
                    // the actual native admission and package validation are unchanged.
                    if method == HubMethod::Get && url.ends_with("/.gf/publish") {
                        let mut body: Value = serde_json::from_slice(&response.body).unwrap();
                        body["capabilities"]
                            .as_array_mut()
                            .unwrap()
                            .push(json!({"capability":"publish-visibility", "major":1}));
                        response.body = serde_json::to_vec(&body).unwrap();
                    }
                    write!(
                        stream,
                        "HTTP/1.1 {} Fixture\r\nContent-Length: {}\r\nConnection: close\r\n",
                        response.status,
                        response.body.len()
                    )
                    .unwrap();
                    for (name, value) in response.headers {
                        if !name.eq_ignore_ascii_case("content-length")
                            && !name.eq_ignore_ascii_case("connection")
                        {
                            write!(stream, "{name}: {value}\r\n").unwrap();
                        }
                    }
                    stream.write_all(b"\r\n").unwrap();
                    stream.write_all(&response.body).unwrap();
                }
            });
            Self {
                port,
                stop,
                thread: Some(thread),
                hub,
            }
        }
    }
    impl Drop for Server {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Relaxed);
            let _ = TcpStream::connect(("127.0.0.1", self.port));
            self.thread.take().unwrap().join().unwrap();
        }
    }

    #[test]
    fn existing_selected_branch_adopts_then_publicly_publishes_exact_triple_and_clones_over_http() {
        let server = Server::new();
        crate::hub_test_support::launcher::start(server.port);
        publication_round_trip(&HttpTransport::new(), &server.hub);
    }
}

fn publication_round_trip(transport: &dyn Transport, hub: &graphforge_hub_publish::ReferenceHub) {
    let published = Published::new();
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    run(
        &published.transport(),
        &published.command("start_from", &source),
    )
    .unwrap();
    let mut origin = GraphForge::new(source.to_str()).unwrap();
    publish_public_with(transport, hub, &mut origin, "opendata/openalex");
    let path = root.path().join("consumer");
    let mut graph = GraphForge::new(path.to_str()).unwrap();
    graph.execute("CREATE (:Retained {value:42})").unwrap();
    let branch = super::selected_branch::selected_main(&mut graph);
    let before = graph.research_version_retention().unwrap();
    drop(graph);
    let mut command = published.command("adopt", &path);
    command.extend(["--hub".into(), "https://hub.test".into()]);
    let receipt = run(transport, &command).unwrap();
    let mut graph = GraphForge::new(path.to_str()).unwrap();
    let after = graph.research_version_retention().unwrap();
    assert_ne!(after.heads[&branch], before.heads[&branch]);
    assert_eq!(receipt["adoption"]["module"], json!(published.module));
    assert_eq!(retained_row(&graph).1, 42);
    let metadata = graph.research_project_metadata().unwrap();
    let composition = graph.workspace_ontology_composition().unwrap();
    // Parse the ordinary public command, resolving the selected head without any
    // hand-authored Branch ontology request or publication composition bypass.
    publish_public_with(transport, hub, &mut graph, "curate/adoption");
    let response = transport
        .get(
            &Url::parse("https://hub.test/curate/adoption/.gf/manifest").unwrap(),
            None,
            None,
            16 * 1024 * 1024,
        )
        .unwrap();
    assert_eq!(response.status, 200);
    let manifest: Value = serde_json::from_reader(response.body).unwrap();
    assert!(
        manifest["ontology"]["modules"]
            .as_array()
            .unwrap()
            .iter()
            .any(|module| module["id"] == published.module.ontology_id
                && module["version"] == published.module.authored_version
                && module["content_digest"]
                    == format!("sha256:{}", published.module.canonical_digest))
    );
    let cloned = root.path().join("cloned");
    let cli = crate::Cli::try_parse_from([
        "gf",
        "clone",
        "curate/adoption",
        cloned.to_str().unwrap(),
        "--hub",
        "https://hub.test",
    ])
    .unwrap();
    let crate::Command::Clone(args) = cli.command.unwrap() else {
        panic!("clone command")
    };
    crate::hub_clone::run_clone_with(transport, args, true, &mut Vec::new()).unwrap();
    let clone = GraphForge::new(cloned.to_str()).unwrap();
    assert_eq!(clone.workspace_ontology_composition().unwrap(), composition);
    assert_eq!(clone.research_project_metadata().unwrap(), metadata);
    assert_eq!(retained_row(&clone).1, 42);
    let cloned_head = clone
        .open_research_branch(
            clone
                .research_version_retention()
                .unwrap()
                .current_branch
                .unwrap(),
        )
        .unwrap();
    assert_eq!(
        cloned_head
            .graph()
            .workspace_ontology_composition()
            .unwrap(),
        composition
    );
    assert_eq!(
        cloned_head.graph().research_project_metadata().unwrap(),
        metadata
    );
}
