//! Device approval links through the native publish credential path.
use super::*;
use crate::hub_http::HttpResponse;
use graphforge_hub_publish::{ReferenceHub, ReferenceHubConfig};
use std::cell::RefCell;

const ORIGIN: &str = "https://hub.test";
const PLAIN: &str = "https://auth.example/device";
const COMPLETE: &str = "https://auth.example/device?user_code=ABCD-EFGH";

struct DeviceHub {
    hub: ReferenceHub,
    /// Replaces only the approval-page fields of the reference response.
    links: Option<(String, Option<String>)>,
    device: RefCell<Option<DeviceAuthorizationResponse>>,
    requests: RefCell<Vec<HubRequest>>,
}
impl DeviceHub {
    fn new(links: Option<(String, Option<String>)>) -> Self {
        Self {
            hub: ReferenceHub::with_config(ReferenceHubConfig {
                base_url: ORIGIN.into(),
                ..ReferenceHubConfig::default()
            })
            .unwrap(),
            links,
            device: RefCell::new(None),
            requests: RefCell::new(Vec::new()),
        }
    }
    fn capabilities(&self) -> PublishCapabilities {
        self.hub.capabilities(&RepositoryIdentity {
            owner: "curate".into(),
            repository: "claims".into(),
        })
    }
    fn client(&self) -> Hub<'_> {
        Hub {
            refusal: None,
            bypass: None,
            transport: self,
            base: Url::parse(ORIGIN).unwrap(),
        }
    }
    fn approve(&self) {
        let device = self.device.borrow();
        assert!(self.hub.approve_device(&device.as_ref().unwrap().user_code));
        self.hub.advance_clock(5);
    }
}
impl Transport for DeviceHub {
    fn get(
        &self,
        _: &Url,
        _: Option<u64>,
        _: Option<&str>,
        _: u64,
    ) -> Result<HttpResponse, GfError> {
        panic!("publish uses bounded exchanges")
    }
    fn exchange(&self, request: &HubRequest, _: usize) -> Result<HubResponse, GfError> {
        self.requests.borrow_mut().push(request.clone());
        let mut response = self.hub.handle(request);
        if request.url.ends_with("/device_authorization") {
            let mut device: DeviceAuthorizationResponse =
                serde_json::from_slice(&response.body).unwrap();
            if let Some((plain, complete)) = &self.links {
                device.verification_uri = plain.clone();
                device.verification_uri_complete = complete.clone();
            }
            response.body = serde_json::to_vec(&device).unwrap();
            *self.device.borrow_mut() = Some(device);
        }
        Ok(response)
    }
}

#[test]
fn device_link_selection_opens_the_displayed_complete_link_or_plain_fallback() {
    for complete in [Some(COMPLETE), None] {
        let transport = DeviceHub::new(Some((PLAIN.into(), complete.map(str::to_owned))));
        let opened = RefCell::new(Vec::new());
        let mut notices = Vec::new();
        let token = obtain_token_with_opener(
            &transport.client(),
            &transport.capabilities(),
            &mut PublishEnvironment {
                bypass: None,
                credential: Credential::DeviceFlow,
                wait: &|_| transport.approve(),
                notices: &mut notices,
                resolution: None,
            },
            &|url| opened.borrow_mut().push(url.to_string()),
        )
        .unwrap();
        let device = transport.device.borrow();
        let device = device.as_ref().unwrap();
        let selected = complete.unwrap_or(PLAIN);
        assert_eq!(*opened.borrow(), [selected]);
        let notices = String::from_utf8(notices).unwrap();
        assert_eq!(
            notices,
            format!(
                "To publish curate/claims, open {selected} and enter code {}\n",
                device.user_code
            )
        );
        assert!(!notices.contains(&device.device_code));
        assert!(!notices.contains(token.expose()));
        assert_eq!(transport.requests.borrow().len(), 2);
    }
}

#[test]
fn unsafe_device_links_are_refused_before_display_open_or_poll() {
    let oversized = format!("{COMPLETE}{}", "x".repeat(4096));
    for complete in [
        "",
        "not a URL",
        "javascript:alert(1)",
        "file:///tmp/approval",
        "http://auth.example/device",
        "https://user:password@auth.example/device",
        "https://auth.example/device#fragment",
        "https://other.example/device",
        "https://auth.example:8443/device",
        "https://auth.example/device\nforged instruction",
        "https://auth.example/device\u{85}forged instruction",
        oversized.as_str(),
    ] {
        let transport = DeviceHub::new(Some((PLAIN.into(), Some(complete.into()))));
        let mut notices = Vec::new();
        let result = obtain_token_with_opener(
            &transport.client(),
            &transport.capabilities(),
            &mut PublishEnvironment {
                bypass: None,
                credential: Credential::DeviceFlow,
                wait: &|_| panic!("unsafe link must not poll"),
                notices: &mut notices,
                resolution: None,
            },
            &|_| panic!("unsafe link must not open"),
        );
        let error = result.unwrap_err();
        assert_eq!(error.code(), "GF_VALIDATION", "{error}");
        assert!(error.to_string().contains("hub.publish.invalid_input"));
        assert!(notices.is_empty());
        assert_eq!(transport.requests.borrow().len(), 1);
    }
    // A safe complete link cannot bypass validation of the plain link.
    assert!(crate::auth::verification_link("http://auth.example/device", Some(COMPLETE)).is_err());
    assert!(crate::auth::verification_link("https://127.0.0.1/device", None).is_err());
}

#[test]
fn device_link_is_shown_before_open_and_denial_still_stops_polling() {
    let transport = DeviceHub::new(Some((PLAIN.into(), Some(COMPLETE.into()))));
    let notices = RefCell::new(Vec::new());
    let pending = RefCell::new(Vec::new());
    struct Writer<'a>(&'a RefCell<Vec<u8>>, &'a RefCell<Vec<u8>>);
    impl Write for Writer<'_> {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.1.borrow_mut().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            self.0.borrow_mut().append(&mut self.1.borrow_mut());
            Ok(())
        }
    }
    let error = obtain_token_with_opener(
        &transport.client(),
        &transport.capabilities(),
        &mut PublishEnvironment {
            bypass: None,
            credential: Credential::DeviceFlow,
            wait: &|_| {
                let device = transport.device.borrow();
                assert!(
                    transport
                        .hub
                        .deny_device(&device.as_ref().unwrap().user_code)
                );
                transport.hub.advance_clock(5);
            },
            notices: &mut Writer(&notices, &pending),
            resolution: None,
        },
        &|url| {
            assert_eq!(url.as_str(), COMPLETE);
            assert!(String::from_utf8_lossy(&notices.borrow()).contains(COMPLETE));
            assert_eq!(transport.requests.borrow().len(), 1);
        },
    )
    .unwrap_err();
    assert!(error.to_string().contains("hub.publish.auth_denied"));
    assert_eq!(transport.requests.borrow().len(), 2);
}

#[test]
fn native_publish_uses_the_prefilled_notice_through_the_real_device_grant() {
    use graphforge_api::{BranchSource, CreateResearchBranchRequest};
    let root = tempfile::tempdir().unwrap();
    let mut graph = GraphForge::new(root.path().join("project").to_str()).unwrap();
    graph.execute("CREATE (:Item {score:1})").unwrap();
    let version_uuid = Uuid::now_v7();
    graph
        .create_research_branch(
            &CreateResearchBranchRequest {
                description: None,
                author: None,
                committer: None,
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: graph
                    .committed_generation_identity()
                    .unwrap()
                    .generation_uuid,
                branch_uuid: Uuid::now_v7(),
                version_uuid,
                source: BranchSource::Current {
                    origin_version_uuid: Uuid::now_v7(),
                    context_uuid: Uuid::now_v7(),
                },
                creator_uuid: Uuid::now_v7(),
                created_at: 1,
                label: "main".into(),
            },
            &CancellationToken::new(),
        )
        .unwrap();
    let transport = DeviceHub::new(None);
    let args = PublishArgs {
        visibility: VisibilityArgs::default(),
        rewrite: false,
        prune: false,
        check_fork_origin: None,
        repository: Some(format!("{ORIGIN}/curate/claims")),
        git_ref: Some("main".into()),
        version_uuid: None,
        fork_of: None,
        operation_uuid: None,
        staging_directory: None,
        protection_bypass_env: None,
        previews: Vec::new(),
    };
    let mut notices = Vec::new();
    let outcome = publish(
        &transport,
        &graph,
        &args,
        &mut PublishEnvironment {
            bypass: None,
            credential: Credential::DeviceFlow,
            wait: &|_| transport.approve(),
            notices: &mut notices,
            resolution: None,
        },
    )
    .unwrap();
    assert!(!outcome.replayed);
    let device = transport.device.borrow();
    let device = device.as_ref().unwrap();
    let notices = String::from_utf8(notices).unwrap();
    let complete = device.verification_uri_complete.as_deref().unwrap();
    assert!(complete.ends_with(&format!("?user_code={}", device.user_code)));
    assert!(notices.contains(complete), "{notices}");
    assert!(notices.contains(&format!("enter code {}", device.user_code)));
    assert!(!notices.contains(&device.device_code));
    assert!(transport.requests.borrow().iter().any(|request| {
        request.url.ends_with("/commit") && request.header("authorization").is_some()
    }));
}
