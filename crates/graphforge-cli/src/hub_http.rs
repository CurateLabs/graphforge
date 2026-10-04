//! Shared HTTPS plumbing for Hub clone and publish.
//!
//! HTTPS only, public addresses only, no proxies, manually bounded redirects
//! for public reads, no redirects for writes, and bounded response bodies.

use graphforge_discovery::RepositoryIdentity;
use graphforge_hub_publish::{HubMethod, HubRequest, HubResponse};
use sha2::{Digest as _, Sha256};
use std::fmt::Write as _;
use std::io::Read;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::time::Duration;
use ureq::unversioned::resolver::{DefaultResolver, ResolvedSocketAddrs, Resolver};
use ureq::unversioned::transport::{
    Buffers, ConnectionDetails, Connector, DefaultConnector, NextTimeout,
};
use url::Url;

pub(crate) const DEFAULT_HUB: &str = "https://graphforge.sh";
pub(crate) const MAX_METADATA_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const MAX_REDIRECTS: usize = 4;

#[derive(Debug)]
struct PublicResolver(DefaultResolver);

impl Resolver for PublicResolver {
    fn resolve(
        &self,
        uri: &ureq::http::Uri,
        config: &ureq::config::Config,
        timeout: ureq::unversioned::transport::NextTimeout,
    ) -> Result<ResolvedSocketAddrs, ureq::Error> {
        let addresses = self.0.resolve(uri, config, timeout)?;
        let mut approved = self.empty();
        for address in addresses
            .iter()
            .copied()
            .filter(|address| public_ip(address.ip()))
        {
            approved.push(address);
        }
        if approved.is_empty() {
            Err(ureq::Error::HostNotFound)
        } else {
            Ok(approved)
        }
    }
}

pub(crate) fn public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => public_v4(ip),
        IpAddr::V6(ip) => {
            if let Some(v4) = ip.to_ipv4_mapped() {
                return public_v4(v4);
            }
            !(ip.is_unspecified()
                || ip.is_loopback()
                || ip.is_multicast()
                || in_v6(ip, "fc00::".parse().unwrap(), 7)
                || in_v6(ip, "fe80::".parse().unwrap(), 10)
                || in_v6(ip, "2001:db8::".parse().unwrap(), 32))
        }
    }
}

fn public_v4(ip: Ipv4Addr) -> bool {
    let value = u32::from(ip);
    let blocked = [
        ("0.0.0.0", 8),
        ("10.0.0.0", 8),
        ("100.64.0.0", 10),
        ("127.0.0.0", 8),
        ("169.254.0.0", 16),
        ("172.16.0.0", 12),
        ("192.0.0.0", 24),
        ("192.0.2.0", 24),
        ("192.168.0.0", 16),
        ("198.18.0.0", 15),
        ("198.51.100.0", 24),
        ("203.0.113.0", 24),
        ("224.0.0.0", 4),
        ("240.0.0.0", 4),
    ];
    !blocked.iter().any(|(base, bits)| {
        let base = u32::from(base.parse::<Ipv4Addr>().unwrap());
        let mask = u32::MAX.checked_shl(32 - bits).unwrap_or(0);
        value & mask == base & mask
    })
}

fn in_v6(ip: Ipv6Addr, base: Ipv6Addr, bits: u32) -> bool {
    let mask = u128::MAX.checked_shl(128 - bits).unwrap_or(0);
    u128::from(ip) & mask == u128::from(base) & mask
}

pub(crate) struct HttpResponse {
    pub(crate) status: u16,
    pub(crate) location: Option<String>,
    pub(crate) content_range: Option<String>,
    pub(crate) etag: Option<String>,
    pub(crate) body: Box<dyn Read + Send>,
}

pub(crate) trait Transport {
    fn validate(&self, url: &Url) -> Result<(), graphforge_api::GfError> {
        validate_url(url)
    }

    fn get(
        &self,
        url: &Url,
        range: Option<u64>,
        if_range: Option<&str>,
        limit: u64,
    ) -> Result<HttpResponse, graphforge_api::GfError>;

    /// Send one request without following redirects and read at most `limit`
    /// response body bytes. Writes (`POST`, `PUT`) and `HEAD` use this path.
    ///
    /// The caller validates `request.url` first: control-plane URLs with
    /// [`validate_url`], data-plane upload URLs with [`validate_upload_url`].
    fn exchange(
        &self,
        _request: &HubRequest,
        _limit: usize,
    ) -> Result<HubResponse, graphforge_api::GfError> {
        Err(network("transport does not send Hub writes"))
    }
}

pub(crate) struct HttpTransport {
    pub(crate) agent: ureq::Agent,
}

impl HttpTransport {
    pub(crate) fn new() -> Self {
        Self {
            agent: read_agent(
                true,
                &READ_TIMEOUTS,
                PublicResolver(DefaultResolver::default()),
            ),
        }
    }
}

/// Bounds for Hub reads (`GET`): discovery documents and package objects.
///
/// A whole-request deadline cut every object larger than it could transfer,
/// and resume could only restart the same cut. Reads therefore bound each
/// phase instead, and the body by inactivity: every wait for response bytes
/// may last at most `idle`, however long the whole transfer takes.
pub(crate) struct ReadTimeouts {
    /// Opening the connection.
    pub(crate) connect: Duration,
    /// Sending the request head and receiving the response head.
    pub(crate) response: Duration,
    /// Longest wait for the next response bytes.
    pub(crate) idle: Duration,
}

/// Production read bounds.
pub(crate) const READ_TIMEOUTS: ReadTimeouts = ReadTimeouts {
    connect: Duration::from_secs(10),
    response: Duration::from_mins(1),
    idle: Duration::from_mins(1),
};

/// Agent for Hub reads: per-phase bounds and an idle bound on every socket
/// wait, no global or whole-body deadline.
pub(crate) fn read_agent(
    https_only: bool,
    timeouts: &ReadTimeouts,
    resolver: impl Resolver,
) -> ureq::Agent {
    let config = ureq::Agent::config_builder()
        .https_only(https_only)
        .http_status_as_error(false)
        .max_redirects(0)
        .proxy(None)
        .timeout_global(None)
        .timeout_connect(Some(timeouts.connect))
        .timeout_send_request(Some(timeouts.response))
        .timeout_recv_response(Some(timeouts.response))
        .timeout_recv_body(None)
        .build();
    ureq::Agent::with_parts(
        config,
        IdleConnector {
            inner: DefaultConnector::new(),
            idle: timeouts.idle,
        },
        resolver,
    )
}

/// Connector whose transports never wait longer than `idle` for one socket
/// read or write, so a stalled connection fails while a slow but steadily
/// progressing one never does.
#[derive(Debug)]
struct IdleConnector {
    inner: DefaultConnector,
    idle: Duration,
}

impl Connector<()> for IdleConnector {
    type Out = IdleTransport;

    fn connect(
        &self,
        details: &ConnectionDetails,
        chained: Option<()>,
    ) -> Result<Option<Self::Out>, ureq::Error> {
        Ok(self
            .inner
            .connect(details, chained)?
            .map(|inner| IdleTransport {
                inner,
                idle: self.idle.into(),
            }))
    }
}

#[derive(Debug)]
struct IdleTransport {
    inner: Box<dyn ureq::unversioned::transport::Transport>,
    idle: ureq::unversioned::transport::time::Duration,
}

impl IdleTransport {
    fn bound(&self, timeout: NextTimeout) -> NextTimeout {
        NextTimeout {
            after: timeout.after.min(self.idle),
            reason: timeout.reason,
        }
    }
}

impl ureq::unversioned::transport::Transport for IdleTransport {
    fn buffers(&mut self) -> &mut dyn Buffers {
        self.inner.buffers()
    }

    fn transmit_output(&mut self, amount: usize, timeout: NextTimeout) -> Result<(), ureq::Error> {
        let timeout = self.bound(timeout);
        self.inner.transmit_output(amount, timeout)
    }

    fn await_input(&mut self, timeout: NextTimeout) -> Result<bool, ureq::Error> {
        let timeout = self.bound(timeout);
        self.inner.await_input(timeout)
    }

    fn is_open(&mut self) -> bool {
        self.inner.is_open()
    }

    fn is_tls(&self) -> bool {
        self.inner.is_tls()
    }
}

impl Transport for HttpTransport {
    fn get(
        &self,
        url: &Url,
        range: Option<u64>,
        if_range: Option<&str>,
        limit: u64,
    ) -> Result<HttpResponse, graphforge_api::GfError> {
        let mut request = self.agent.get(url.as_str());
        if let Some(offset) = range {
            request = request.header("Range", format!("bytes={offset}-"));
        }
        if let Some(validator) = if_range {
            request = request.header("If-Range", validator);
        }
        let response = request.call().map_err(|_| network("request failed"))?;
        let status = response.status().as_u16();
        let location = response
            .headers()
            .get("location")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let content_range = response
            .headers()
            .get("content-range")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let etag = response
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let (_, body) = response.into_parts();
        Ok(HttpResponse {
            status,
            location,
            content_range,
            etag,
            body: Box::new(body.into_reader().take(limit.saturating_add(1))),
        })
    }
    fn exchange(
        &self,
        request: &HubRequest,
        limit: usize,
    ) -> Result<HubResponse, graphforge_api::GfError> {
        send_with_agent(&self.agent, request, limit)
    }
}

/// Bounds for Hub writes (`POST`, `PUT`).
///
/// A whole-request deadline would fail every upload chunk on a slow uplink no
/// matter how steadily it progresses, and resume could never help. Writes
/// therefore bound each phase instead: the body of one chunk may take
/// `send_body`, and every other phase (request head, response head, response
/// body) `response`.
pub(crate) struct WriteTimeouts {
    /// Sending one request body (one upload chunk or one commit document).
    pub(crate) send_body: Duration,
    /// Each other request and response phase.
    pub(crate) response: Duration,
}

/// Production write bounds. With 1 MiB upload chunks, five minutes per chunk
/// body admits any uplink of at least about 3.5 KB/s.
pub(crate) const WRITE_TIMEOUTS: WriteTimeouts = WriteTimeouts {
    send_body: Duration::from_mins(5),
    response: Duration::from_mins(1),
};

/// Agent configuration for Hub writes: per-phase bounds, no global deadline.
pub(crate) fn write_agent_config(
    https_only: bool,
    timeouts: &WriteTimeouts,
) -> ureq::config::Config {
    ureq::Agent::config_builder()
        .https_only(https_only)
        .http_status_as_error(false)
        .max_redirects(0)
        .proxy(None)
        .timeout_global(None)
        .timeout_connect(Some(Duration::from_secs(10)))
        .timeout_send_request(Some(timeouts.response))
        .timeout_send_body(Some(timeouts.send_body))
        .timeout_recv_response(Some(timeouts.response))
        .timeout_recv_body(Some(timeouts.response))
        .build()
}

/// Transport for `gf publish`: reads use clone's [`read_agent`], while writes
/// use [`write_agent_config`].
pub(crate) struct HubTransport {
    read: HttpTransport,
    write: ureq::Agent,
}

impl HubTransport {
    pub(crate) fn new() -> Self {
        Self::with_agents(
            HttpTransport::new(),
            ureq::Agent::with_parts(
                write_agent_config(true, &WRITE_TIMEOUTS),
                DefaultConnector::new(),
                PublicResolver(DefaultResolver::default()),
            ),
        )
    }

    pub(crate) fn with_agents(read: HttpTransport, write: ureq::Agent) -> Self {
        Self { read, write }
    }
}

impl Transport for HubTransport {
    fn get(
        &self,
        url: &Url,
        range: Option<u64>,
        if_range: Option<&str>,
        limit: u64,
    ) -> Result<HttpResponse, graphforge_api::GfError> {
        self.read.get(url, range, if_range, limit)
    }

    fn exchange(
        &self,
        request: &HubRequest,
        limit: usize,
    ) -> Result<HubResponse, graphforge_api::GfError> {
        match request.method {
            HubMethod::Post | HubMethod::Put => send_with_agent(&self.write, request, limit),
            HubMethod::Get | HubMethod::Head => self.read.exchange(request, limit),
        }
    }
}

/// Send one request through `agent`; redirects are returned, never followed.
pub(crate) fn send_with_agent(
    agent: &ureq::Agent,
    request: &HubRequest,
    limit: usize,
) -> Result<HubResponse, graphforge_api::GfError> {
    let method = match request.method {
        HubMethod::Get => "GET",
        HubMethod::Head => "HEAD",
        HubMethod::Post => "POST",
        HubMethod::Put => "PUT",
    };
    let mut builder = ureq::http::Request::builder()
        .method(method)
        .uri(request.url.as_str());
    for (name, value) in &request.headers {
        builder = builder.header(name.as_str(), value.as_str());
    }
    let result = if matches!(request.method, HubMethod::Get | HubMethod::Head) {
        let built = builder
            .body(())
            .map_err(|_| validation("hub.invalid_input", "request is invalid"))?;
        agent.run(built)
    } else {
        let built = builder
            .body(request.body.as_slice())
            .map_err(|_| validation("hub.invalid_input", "request is invalid"))?;
        agent.run(built)
    };
    let response = result.map_err(|_| network("request failed"))?;
    let status = response.status().as_u16();
    let headers = response
        .headers()
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.as_str().to_ascii_lowercase(), value.to_owned()))
        })
        .collect();
    let (_, body) = response.into_parts();
    let mut bytes = Vec::new();
    body.into_reader()
        .take(limit as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| network("response read failed"))?;
    if bytes.len() > limit {
        return Err(limit_error("response exceeds byte bound"));
    }
    Ok(HubResponse {
        status,
        headers,
        body: bytes,
    })
}

/// Validate a data-plane upload URL a Hub issued.
///
/// Upload URLs are capabilities, so unlike [`validate_url`] a query string is
/// allowed; scheme, credentials, fragment, and host rules are the same.
pub(crate) fn validate_upload_url(url: &Url) -> Result<(), graphforge_api::GfError> {
    if url.scheme() != "https"
        || url.username() != ""
        || url.password().is_some()
        || url.host_str().is_none()
        || url.fragment().is_some()
    {
        return Err(validation(
            "hub.unsafe_location",
            "upload URL must be credential-free HTTPS",
        ));
    }
    if let Some(host) = url.host_str()
        && host.parse::<IpAddr>().is_ok_and(|ip| !public_ip(ip))
    {
        return Err(validation("hub.unsafe_location", "URL host is not public"));
    }
    Ok(())
}

pub(crate) fn validate_url(url: &Url) -> Result<(), graphforge_api::GfError> {
    if url.scheme() != "https"
        || url.username() != ""
        || url.password().is_some()
        || url.host_str().is_none()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(validation(
            "hub.unsafe_location",
            "URL must be credential-free HTTPS",
        ));
    }
    if let Some(host) = url.host_str()
        && host.parse::<IpAddr>().is_ok_and(|ip| !public_ip(ip))
    {
        return Err(validation("hub.unsafe_location", "URL host is not public"));
    }
    Ok(())
}

/// Bounded in-process retry for transient read failures.
///
/// A retry follows a failed connection, a failed or truncated response body,
/// or a `408`, `429`, or `5xx` status. Only consecutive failures without
/// progress count toward `attempts`; a body read that advances the durable
/// download offset resets the count, so a long transfer survives any number of
/// separated interruptions while a dead one fails promptly.
#[derive(Clone, Copy, Debug)]
pub(crate) struct RetryPolicy {
    /// Requests allowed for one step without progress.
    pub(crate) attempts: u32,
    /// Wait before the first retry; each later wait doubles.
    pub(crate) initial_backoff: Duration,
    /// Upper bound on one wait.
    pub(crate) max_backoff: Duration,
}

/// Production retry policy: five attempts, waiting 1, 2, 4, then 8 seconds.
pub(crate) const RETRY_POLICY: RetryPolicy = RetryPolicy {
    attempts: 5,
    initial_backoff: Duration::from_secs(1),
    max_backoff: Duration::from_secs(30),
};

impl RetryPolicy {
    /// The wait before retry number `failures` (one-based).
    pub(crate) fn backoff(&self, failures: u32) -> Duration {
        let doublings = failures.saturating_sub(1).min(16);
        self.initial_backoff
            .saturating_mul(1_u32 << doublings)
            .min(self.max_backoff)
    }
}

/// Whether an HTTP status is worth retrying.
fn transient_status(status: u16) -> bool {
    matches!(status, 408 | 429 | 500..=599)
}

/// One request failure; `transient` marks a failure a retry may cure, and
/// `status` is the HTTP status of an unsuccessful response.
pub(crate) struct FetchFailure {
    pub(crate) error: graphforge_api::GfError,
    pub(crate) transient: bool,
    pub(crate) status: Option<u16>,
}

impl From<graphforge_api::GfError> for FetchFailure {
    fn from(error: graphforge_api::GfError) -> Self {
        Self {
            error,
            transient: false,
            status: None,
        }
    }
}

#[cfg(test)]
pub(crate) fn fetch(
    transport: &dyn Transport,
    start: &Url,
    range: Option<u64>,
    if_range: Option<&str>,
    limit: u64,
) -> Result<HttpResponse, graphforge_api::GfError> {
    let mut attempts = 0;
    fetch_once(transport, start, range, if_range, limit, &mut attempts).map_err(|f| f.error)
}

/// Fetch `start`, retrying transient failures under `policy`.
pub(crate) fn fetch_with_attempts(
    transport: &dyn Transport,
    start: &Url,
    range: Option<u64>,
    if_range: Option<&str>,
    limit: u64,
    attempts: &mut u32,
    policy: &RetryPolicy,
) -> Result<HttpResponse, graphforge_api::GfError> {
    let mut failures = 0;
    loop {
        match fetch_once(transport, start, range, if_range, limit, attempts) {
            Ok(response) => return Ok(response),
            Err(failure) => {
                failures += 1;
                if !failure.transient || failures >= policy.attempts {
                    return Err(failure.error);
                }
                std::thread::sleep(policy.backoff(failures));
            }
        }
    }
}

/// Fetch `start` once, following at most [`MAX_REDIRECTS`] public redirects.
pub(crate) fn fetch_once(
    transport: &dyn Transport,
    start: &Url,
    range: Option<u64>,
    if_range: Option<&str>,
    limit: u64,
    attempts: &mut u32,
) -> Result<HttpResponse, FetchFailure> {
    let mut url = start.clone();
    for hop in 0..=MAX_REDIRECTS {
        transport.validate(&url)?;
        *attempts = attempts.saturating_add(1);
        let response = transport
            .get(&url, range, if_range, limit)
            .map_err(|error| FetchFailure {
                transient: is_network(&error),
                error,
                status: None,
            })?;
        if matches!(response.status, 301 | 302 | 303 | 307 | 308) {
            if hop == MAX_REDIRECTS {
                return Err(network("redirect limit exceeded").into());
            }
            let location = response
                .location
                .as_deref()
                .ok_or_else(|| network("redirect is missing Location"))?;
            url = url
                .join(location)
                .map_err(|_| validation("hub.unsafe_location", "invalid redirect URL"))?;
            continue;
        }
        if !(200..300).contains(&response.status) {
            return Err(FetchFailure {
                error: network(&format!("Hub returned HTTP status {}", response.status)),
                transient: transient_status(response.status),
                status: Some(response.status),
            });
        }
        return Ok(response);
    }
    unreachable!()
}

/// Whether `error` is a `hub.network` failure.
pub(crate) fn is_network(error: &graphforge_api::GfError) -> bool {
    matches!(error, graphforge_api::GfError::Storage(detail) if detail.starts_with("hub.network:"))
}

pub(crate) fn read_bounded(
    mut response: HttpResponse,
    limit: usize,
) -> Result<Vec<u8>, graphforge_api::GfError> {
    let mut bytes = Vec::new();
    response
        .body
        .read_to_end(&mut bytes)
        .map_err(|_| network("response read failed"))?;
    if bytes.len() > limit {
        return Err(limit_error("response exceeds byte bound"));
    }
    Ok(bytes)
}

pub(crate) fn parse_input(
    value: &str,
) -> Result<(RepositoryIdentity, Url), graphforge_api::GfError> {
    parse_input_at(value, None)
}

/// Resolve a repository argument against `hub`, which only applies to the
/// `owner/repository` form; an explicit repository URL names its own Hub.
pub(crate) fn parse_input_at(
    value: &str,
    hub: Option<&str>,
) -> Result<(RepositoryIdentity, Url), graphforge_api::GfError> {
    if !value.contains("://") {
        let identity = RepositoryIdentity::parse(value)
            .map_err(|_| validation("hub.invalid_identity", "invalid repository identity"))?;
        let hub = Url::parse(hub.unwrap_or(DEFAULT_HUB))
            .map_err(|_| validation("hub.invalid_identity", "invalid Hub URL"))?;
        validate_url(&hub)?;
        let mut base = hub.clone();
        base.set_path(&format!(
            "{}/{}/{}",
            hub.path().trim_end_matches('/'),
            identity.owner,
            identity.repository
        ));
        return Ok((identity, base));
    }
    if hub.is_some() {
        return Err(validation(
            "hub.invalid_identity",
            "--hub applies only to an owner/repository name",
        ));
    }
    let base = Url::parse(value)
        .map_err(|_| validation("hub.invalid_identity", "invalid repository URL"))?;
    validate_url(&base)?;
    if base.query().is_some() || base.path().ends_with('/') {
        return Err(validation(
            "hub.invalid_identity",
            "repository URL must end in owner/repository",
        ));
    }
    let segments: Vec<_> = base.path_segments().into_iter().flatten().collect();
    if segments.len() != 2 {
        return Err(validation(
            "hub.invalid_identity",
            "repository URL must end in owner/repository",
        ));
    }
    let identity = RepositoryIdentity::parse(&format!("{}/{}", segments[0], segments[1]))
        .map_err(|_| validation("hub.invalid_identity", "invalid repository identity"))?;
    Ok((identity, base))
}

pub(crate) fn endpoint(base: &Url, name: &str) -> Url {
    let mut endpoint = base.clone();
    endpoint.set_path(&format!("{}/.gf/{name}", base.path().trim_end_matches('/')));
    endpoint
}

pub(crate) fn validation(code: &str, message: &str) -> graphforge_api::GfError {
    graphforge_api::GfError::Validation(format!("{code}: {message}"))
}

pub(crate) fn network(message: &str) -> graphforge_api::GfError {
    graphforge_api::GfError::Storage(format!("hub.network: {message}"))
}
pub(crate) fn limit_error(message: &str) -> graphforge_api::GfError {
    validation("hub.limit_exceeded", message)
}
pub(crate) fn storage(error: impl std::fmt::Display) -> graphforge_api::GfError {
    graphforge_api::GfError::Storage(error.to_string())
}
pub(crate) fn hash_reader(reader: &mut impl Read) -> Result<String, graphforge_api::GfError> {
    let mut hash = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
    loop {
        let read = reader.read(&mut buffer).map_err(storage)?;
        if read == 0 {
            break;
        }
        hash.update(&buffer[..read]);
    }
    let digest = hash.finalize();
    let hex = digest
        .iter()
        .fold(String::with_capacity(64), |mut hex, byte| {
            write!(hex, "{byte:02x}").expect("writing to a string cannot fail");
            hex
        });
    Ok(format!("sha256:{hex}"))
}
