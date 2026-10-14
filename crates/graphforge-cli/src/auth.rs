//! Browser sign-in with secrets held exclusively in the operating system vault.
use crate::hub_http::{DEFAULT_HUB, HubTransport, Transport, validation};
use clap::{Args, Subcommand};
use graphforge_api::GfError;
use graphforge_hub_publish::{HubMethod, HubRequest, HubResponse};
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use url::Url;

#[derive(Args)]
pub(crate) struct AuthArgs {
    /// Read a Vercel preview bypass secret from this environment variable.
    #[arg(long, global = true)]
    protection_bypass_env: Option<String>,
    #[command(subcommand)]
    command: AuthCommand,
}
#[derive(Subcommand)]
enum AuthCommand {
    /// Sign in using a browser or a verification link on another device.
    Login {
        #[arg(long, default_value = DEFAULT_HUB)]
        hub: String,
        #[arg(long)]
        no_browser: bool,
    },
    /// Verify the saved session with the Hub.
    Status {
        #[arg(long, default_value = DEFAULT_HUB)]
        hub: String,
    },
    /// Revoke the saved session and remove local credentials.
    Logout {
        #[arg(long, default_value = DEFAULT_HUB)]
        hub: String,
    },
}
#[derive(Serialize, Deserialize)]
struct Session {
    access_token: String,
    refresh_token: String,
    expires_at: u64,
}
#[derive(Deserialize)]
struct Tokens {
    access_token: String,
    refresh_token: String,
    token_type: String,
    expires_in: u64,
}
#[derive(Deserialize)]
struct DeviceAuthorization {
    device_code: String,
    user_code: String,
    verification_uri: String,
    verification_uri_complete: Option<String>,
    expires_in: u64,
    #[serde(default = "poll_interval")]
    interval: u64,
}
fn poll_interval() -> u64 {
    5
}
trait Store {
    fn read(&self, origin: &str) -> Result<Option<String>, GfError>;
    fn write(&self, origin: &str, value: &str) -> Result<(), GfError>;
    fn delete(&self, origin: &str) -> Result<(), GfError>;
}
struct Vault;
fn vault_error() -> GfError {
    denied("OS credential vault unavailable; unlock or configure your system credential service")
}
impl Store for Vault {
    fn read(&self, origin: &str) -> Result<Option<String>, GfError> {
        match keyring::Entry::new("graphforge-cli", origin)
            .map_err(|_| vault_error())?
            .get_password()
        {
            Ok(value) => Ok(Some(value)),
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(_) => Err(vault_error()),
        }
    }
    fn write(&self, origin: &str, value: &str) -> Result<(), GfError> {
        keyring::Entry::new("graphforge-cli", origin)
            .map_err(|_| vault_error())?
            .set_password(value)
            .map_err(|_| vault_error())
    }
    fn delete(&self, origin: &str) -> Result<(), GfError> {
        match keyring::Entry::new("graphforge-cli", origin)
            .map_err(|_| vault_error())?
            .delete_credential()
        {
            Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
            Err(_) => Err(vault_error()),
        }
    }
}
fn denied(message: &str) -> GfError {
    validation("hub.auth_denied", message)
}
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
fn origin(value: &str) -> Result<Url, GfError> {
    let url = Url::parse(value).map_err(|_| denied("invalid Hub origin"))?;
    crate::hub_http::validate_url(&url)?;
    if url.path() != "/" || url.query().is_some() || url.fragment().is_some() {
        return Err(denied("--hub must be a bare HTTPS origin"));
    }
    Ok(url)
}
fn request(
    transport: &dyn Transport,
    hub: &Url,
    path: &str,
    method: HubMethod,
    token: Option<&str>,
    form: &[(&str, &str)],
    body: Option<Vec<u8>>,
) -> Result<HubResponse, GfError> {
    let url = hub
        .join(path)
        .map_err(|_| denied("invalid authentication endpoint"))?;
    transport.validate(&url)?;
    let mut request = HubRequest::new(method, url.as_str());
    if let Some(token) = token {
        validate_secret(token)?;
        request = request.with_header("authorization", format!("Bearer {token}"));
    }
    if let Some(body) = body {
        request = request
            .with_header("content-type", "application/json")
            .with_body(body);
    } else if !form.is_empty() {
        request = request
            .with_header("content-type", "application/x-www-form-urlencoded")
            .with_body(
                url::form_urlencoded::Serializer::new(String::new())
                    .extend_pairs(form)
                    .finish()
                    .into_bytes(),
            );
    }
    transport.exchange(&request, 64 * 1024)
}
fn validate_secret(value: &str) -> Result<(), GfError> {
    if value.is_empty() || value.len() > 32768 || !value.bytes().all(|b| b.is_ascii_graphic()) {
        return Err(denied("invalid authentication response"));
    }
    Ok(())
}
fn save(store: &dyn Store, hub: &Url, response: HubResponse, now: u64) -> Result<Session, GfError> {
    let HubResponse { status, body, .. } = response;
    if status != 200 {
        return Err(denied("sign-in was refused; run gf auth login"));
    }
    let tokens: Tokens =
        serde_json::from_slice(&body).map_err(|_| denied("invalid authentication response"))?;
    validate_secret(&tokens.access_token)?;
    validate_secret(&tokens.refresh_token)?;
    if !tokens.token_type.eq_ignore_ascii_case("Bearer") || tokens.expires_in == 0 {
        return Err(denied("invalid authentication response"));
    }
    let session = Session {
        access_token: tokens.access_token,
        refresh_token: tokens.refresh_token,
        expires_at: now.saturating_add(tokens.expires_in),
    };
    // One vault value replaces the access/refresh pair together, including rotation.
    store.write(
        &hub.origin().ascii_serialization(),
        &serde_json::to_string(&session).map_err(|_| denied("cannot encode session"))?,
    )?;
    Ok(session)
}
fn session(
    store: &dyn Store,
    transport: &dyn Transport,
    hub: &Url,
    now: u64,
) -> Result<Option<Session>, GfError> {
    let Some(value) = store.read(&hub.origin().ascii_serialization())? else {
        return Ok(None);
    };
    let session: Session = serde_json::from_str(&value)
        .map_err(|_| denied("saved session is invalid; run gf auth logout"))?;
    if session.expires_at > now.saturating_add(60) {
        validate_secret(&session.access_token)?;
        return Ok(Some(session));
    }
    validate_secret(&session.refresh_token)?;
    let response = request(
        transport,
        hub,
        "/api/cli/token",
        HubMethod::Post,
        None,
        &[
            ("client_id", "graphforge-cli"),
            ("grant_type", "refresh_token"),
            ("refresh_token", &session.refresh_token),
        ],
        None,
    )?;
    if serde_json::from_slice::<serde_json::Value>(&response.body)
        .ok()
        .and_then(|value| {
            value
                .get("error")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
        .as_deref()
        == Some("invalid_grant")
    {
        store.delete(&hub.origin().ascii_serialization())?;
    }
    save(store, hub, response, now).map(Some)
}
fn verification_url(value: &str) -> Result<Url, GfError> {
    if value.len() > 4096 || value.chars().any(char::is_control) {
        return Err(denied("invalid verification link"));
    }
    let url = Url::parse(value).map_err(|_| denied("invalid verification link"))?;
    let mut checked = url.clone();
    checked.set_query(None);
    crate::hub_http::validate_url(&checked)?;
    Ok(url)
}
fn login(
    store: &dyn Store,
    transport: &dyn Transport,
    hub: &Url,
    output: &mut dyn Write,
    wait: &dyn Fn(Duration),
    clock: &dyn Fn() -> u64,
    open: &dyn Fn(&Url),
) -> Result<(), GfError> {
    let response = request(
        transport,
        hub,
        "/api/cli/device",
        HubMethod::Post,
        None,
        &[("client_id", "graphforge-cli")],
        None,
    )?;
    if response.status != 200 {
        return Err(denied("device authorization was refused"));
    }
    let device: DeviceAuthorization = serde_json::from_slice(&response.body)
        .map_err(|_| denied("invalid device authorization response"))?;
    validate_secret(&device.device_code)?;
    if device.user_code.is_empty()
        || device.user_code.len() > 128
        || device.user_code.chars().any(char::is_control)
    {
        return Err(denied("invalid verification code"));
    }
    let link = verification_url(&device.verification_uri)?;
    let complete = device
        .verification_uri_complete
        .as_deref()
        .map(verification_url)
        .transpose()?;
    if complete
        .as_ref()
        .is_some_and(|complete| complete.origin() != link.origin())
    {
        return Err(denied("verification links must share an origin"));
    }
    writeln!(output, "Open {link} and enter code {}", device.user_code)
        .map_err(|_| denied("cannot write verification instructions"))?;
    output
        .flush()
        .map_err(|_| denied("cannot write verification instructions"))?;
    open(complete.as_ref().unwrap_or(&link));
    let deadline = clock().saturating_add(device.expires_in.min(900));
    let mut interval = device.interval.clamp(1, 60);
    loop {
        if clock().saturating_add(interval) >= deadline {
            return Err(denied("device authorization expired"));
        }
        wait(Duration::from_secs(interval));
        let response = request(
            transport,
            hub,
            "/api/cli/token",
            HubMethod::Post,
            None,
            &[
                ("client_id", "graphforge-cli"),
                ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
                ("device_code", &device.device_code),
            ],
            None,
        )?;
        if response.status == 200 {
            save(store, hub, response, clock())?;
            return Ok(());
        }
        let error: serde_json::Value = serde_json::from_slice(&response.body).unwrap_or_default();
        match error.get("error").and_then(serde_json::Value::as_str) {
            Some("authorization_pending") => {}
            Some("slow_down") => interval = interval.saturating_add(5).min(60),
            _ => return Err(denied("device authorization was denied or expired")),
        }
    }
}
fn browser(url: &Url) {
    #[cfg(target_os = "macos")]
    let _ = std::process::Command::new("open").arg(url.as_str()).spawn();
    #[cfg(target_os = "windows")]
    let _ = std::process::Command::new("rundll32")
        .args(["url.dll,FileProtocolHandler", url.as_str()])
        .spawn();
    #[cfg(all(unix, not(target_os = "macos")))]
    let _ = std::process::Command::new("xdg-open")
        .arg(url.as_str())
        .spawn();
}
fn logout(
    store: &dyn Store,
    transport: &dyn Transport,
    hub: &Url,
    now: u64,
) -> Result<(), GfError> {
    let saved = session(store, transport, hub, now);
    let revoked = match saved {
        Ok(Some(session)) => request(
            transport,
            hub,
            "/api/cli/logout",
            HubMethod::Post,
            Some(&session.access_token),
            &[],
            None,
        )
        .and_then(|response| {
            if response.status == 200 || response.status == 204 || response.status == 401 {
                Ok(())
            } else {
                Err(denied(
                    "remote revocation failed; local credentials removed",
                ))
            }
        }),
        Ok(None) => Ok(()),
        Err(error) => Err(error),
    };
    store.delete(&hub.origin().ascii_serialization())?;
    revoked
}
#[derive(Deserialize)]
struct SessionStatus {
    authenticated: bool,
    principal_id: String,
    expires_at: u64,
}
fn verify_status(
    transport: &dyn Transport,
    hub: &Url,
    bearer: &str,
    now: u64,
) -> Result<SessionStatus, GfError> {
    let response = request(
        transport,
        hub,
        "/api/cli/session",
        HubMethod::Get,
        Some(bearer),
        &[],
        None,
    )?;
    if response.status != 200 {
        return Err(denied("session is no longer valid; run gf auth login"));
    }
    let status: SessionStatus =
        serde_json::from_slice(&response.body).map_err(|_| denied("invalid session response"))?;
    if !status.authenticated || status.expires_at <= now {
        return Err(denied("session is expired or revoked; run gf auth login"));
    }
    validate_secret(&status.principal_id)?;
    Ok(status)
}
fn run_inner(args: AuthArgs, json: bool, output: &mut dyn Write) -> Result<i32, GfError> {
    match args.command {
        AuthCommand::Login { hub, no_browser } => {
            let hub = origin(&hub)?;
            let transport = PreviewTransport::new(&hub, args.protection_bypass_env.as_deref())?;
            login(
                &Vault,
                &transport,
                &hub,
                &mut std::io::stderr(),
                &std::thread::sleep,
                &now,
                &|url| {
                    if !no_browser {
                        browser(url);
                    }
                },
            )?;
            writeln!(
                output,
                "{}",
                if json {
                    "{\"authenticated\":true}"
                } else {
                    "Signed in."
                }
            )
            .map_err(|_| denied("cannot write authentication status"))?;
        }
        AuthCommand::Status { hub } => {
            let hub = origin(&hub)?;
            let transport = PreviewTransport::new(&hub, args.protection_bypass_env.as_deref())?;
            let Some(session) = session(&Vault, &transport, &hub, now())? else {
                writeln!(
                    output,
                    "{}",
                    if json {
                        "{\"authenticated\":false}"
                    } else {
                        "Not signed in. Run gf auth login."
                    }
                )
                .map_err(|_| denied("cannot write authentication status"))?;
                return Ok(1);
            };
            let status = verify_status(&transport, &hub, &session.access_token, now())?;
            let principal = &status.principal_id;
            if json {
                writeln!(
                    output,
                    "{}",
                    serde_json::json!({"authenticated":true,"principal_id":principal,"expires_at":status.expires_at})
                ).map_err(|_| denied("cannot write authentication status"))?;
            } else {
                writeln!(
                    output,
                    "Signed in as {principal} at {}",
                    hub.origin().ascii_serialization()
                )
                .map_err(|_| denied("cannot write authentication status"))?;
            }
        }
        AuthCommand::Logout { hub } => {
            let hub = origin(&hub)?;
            let transport = PreviewTransport::new(&hub, args.protection_bypass_env.as_deref())?;
            logout(&Vault, &transport, &hub, now())?;
            writeln!(
                output,
                "{}",
                if json {
                    "{\"authenticated\":false}"
                } else {
                    "Signed out."
                }
            )
            .map_err(|_| denied("cannot write authentication status"))?;
        }
    }
    Ok(0)
}

pub(crate) struct RepositoryToken {
    pub token: String,
    pub origin: Url,
}
pub(crate) fn repository_token(
    hub: &Url,
    owner: &str,
    name: &str,
    capability: &str,
) -> Result<Option<RepositoryToken>, GfError> {
    let hub = origin(&hub.origin().ascii_serialization())?;
    let transport = HubTransport::new();
    let Some(session) = session(&Vault, &transport, &hub, now())? else {
        return Ok(None);
    };
    exchange_repository_token(
        &transport,
        &hub,
        &session.access_token,
        owner,
        name,
        capability,
    )
    .map(Some)
}
fn exchange_repository_token(
    transport: &dyn Transport,
    hub: &Url,
    bearer: &str,
    owner: &str,
    name: &str,
    capability: &str,
) -> Result<RepositoryToken, GfError> {
    #[derive(Deserialize)]
    struct Grant {
        access_token: String,
        origin: String,
        token_type: String,
        expires_in: u64,
        repository_id: String,
    }
    let response = request(
        transport,
        hub,
        "/api/cli/repository-token",
        HubMethod::Post,
        Some(bearer),
        &[],
        Some(
            serde_json::to_vec(
                &serde_json::json!({"owner":owner,"name":name,"capability":capability}),
            )
            .map_err(|_| denied("invalid repository request"))?,
        ),
    )?;
    if response.status != 200 {
        return Err(denied("repository access denied"));
    }
    let grant: Grant = serde_json::from_slice(&response.body)
        .map_err(|_| denied("invalid repository credential"))?;
    validate_secret(&grant.access_token)?;
    if !grant.token_type.eq_ignore_ascii_case("Bearer")
        || grant.expires_in == 0
        || grant.repository_id.is_empty()
    {
        return Err(denied("invalid repository credential"));
    }
    Ok(RepositoryToken {
        token: grant.access_token,
        origin: origin(&grant.origin)?,
    })
}

/// Metadata uses identity credentials only at the explicit Hub. Object reads
/// use a different repository capability only at its Hub-approved origin.
pub(crate) struct CloneTransport<T = crate::hub_http::HttpTransport> {
    inner: T,
    hub: Url,
    metadata: [Url; 2],
    discovery_acquire: Box<dyn Fn() -> Result<Option<String>, GfError>>,
    discovery_token: std::cell::RefCell<Option<String>>,
    acquire: Box<dyn Fn() -> Result<Option<RepositoryToken>, GfError>>,
    token: std::cell::RefCell<Option<RepositoryToken>>,
}
struct PreviewTransport<T = HubTransport> {
    inner: T,
    origin: url::Origin,
    bypass: Option<String>,
}
impl PreviewTransport<HubTransport> {
    fn new(hub: &Url, bypass_env: Option<&str>) -> Result<Self, GfError> {
        let bypass = bypass_env
            .map(|name| {
                std::env::var(name).map_err(|_| {
                    denied("preview bypass environment variable is missing or not Unicode")
                })
            })
            .transpose()?;
        if let Some(bypass) = &bypass {
            validate_secret(bypass)?;
        }
        Ok(Self {
            inner: HubTransport::new(),
            origin: hub.origin(),
            bypass,
        })
    }
}
impl<T: Transport> Transport for PreviewTransport<T> {
    fn get(
        &self,
        _: &Url,
        _: Option<u64>,
        _: Option<&str>,
        _: u64,
    ) -> Result<crate::hub_http::HttpResponse, GfError> {
        Err(denied("authentication transport cannot fetch objects"))
    }
    fn exchange(&self, request: &HubRequest, limit: usize) -> Result<HubResponse, GfError> {
        let url =
            Url::parse(&request.url).map_err(|_| denied("invalid authentication endpoint"))?;
        if url.origin() != self.origin {
            return Err(denied("authentication must use the explicit Hub origin"));
        }
        let mut request = request.clone();
        if let Some(bypass) = &self.bypass {
            request = request.with_header("x-vercel-protection-bypass", bypass);
        }
        self.inner.exchange(&request, limit)
    }
}
fn saved_bearer(
    hub: &Url,
    token_env: Option<&str>,
    bypass_env: Option<&str>,
) -> Result<Option<String>, GfError> {
    if let Some(name) = token_env {
        let bearer = std::env::var(name).map_err(|_| {
            denied("clone credential environment variable is missing or not Unicode")
        })?;
        validate_secret(&bearer)?;
        Ok(Some(bearer))
    } else {
        Ok(
            session(&Vault, &PreviewTransport::new(hub, bypass_env)?, hub, now())?
                .map(|session| session.access_token),
        )
    }
}
impl CloneTransport<crate::hub_http::HttpTransport> {
    pub(crate) fn new(
        repository: &str,
        hub: Option<&str>,
        token_env: Option<&str>,
        bypass_env: Option<&str>,
    ) -> Result<Self, GfError> {
        let (identity, base) = crate::hub_http::parse_input_at(repository, hub)?;
        let hub = origin(&base.origin().ascii_serialization())?;
        let auth_hub = hub.clone();
        let metadata_hub = hub.clone();
        let token_env = token_env.map(str::to_owned);
        let metadata_env = token_env.clone();
        let bypass_env = bypass_env.map(str::to_owned);
        let metadata_bypass = bypass_env.clone();
        Ok(Self {
            inner: crate::hub_http::HttpTransport::new(),
            hub,
            metadata: [
                crate::hub_http::endpoint(&base, "refs"),
                crate::hub_http::endpoint(&base, "manifest"),
            ],
            discovery_acquire: Box::new(move || {
                saved_bearer(
                    &metadata_hub,
                    metadata_env.as_deref(),
                    metadata_bypass.as_deref(),
                )
            }),
            discovery_token: std::cell::RefCell::new(None),
            acquire: Box::new(move || {
                let Some(bearer) =
                    saved_bearer(&auth_hub, token_env.as_deref(), bypass_env.as_deref())?
                else {
                    return Ok(None);
                };
                exchange_repository_token(
                    &PreviewTransport::new(&auth_hub, bypass_env.as_deref())?,
                    &auth_hub,
                    &bearer,
                    &identity.owner,
                    &identity.repository,
                    "read",
                )
                .map(Some)
            }),
            token: std::cell::RefCell::new(None),
        })
    }
}
impl<T: Transport> CloneTransport<T> {
    fn send_authorized(
        &self,
        url: &Url,
        range: Option<u64>,
        if_range: Option<&str>,
        limit: u64,
        bearer: &str,
    ) -> Result<crate::hub_http::HttpResponse, GfError> {
        validate_secret(bearer)?;
        let mut header = ureq::http::HeaderValue::from_str(&format!("Bearer {bearer}"))
            .map_err(|_| denied("invalid repository credential"))?;
        header.set_sensitive(true);
        let response =
            self.inner
                .get_authorized(url, range, if_range, limit, &[("authorization", header)])?;
        if matches!(response.status, 301 | 302 | 303 | 307 | 308) {
            return Err(denied("authenticated repository downloads cannot redirect"));
        }
        Ok(response)
    }
    fn authenticated(
        &self,
        url: &Url,
        range: Option<u64>,
        if_range: Option<&str>,
        limit: u64,
    ) -> Result<crate::hub_http::HttpResponse, GfError> {
        let token = self.token.borrow();
        let token = token
            .as_ref()
            .ok_or_else(|| denied("sign in with gf auth login to access this repository"))?;
        if url.origin() != token.origin.origin() {
            return Err(denied(
                "repository credential does not authorize this object origin",
            ));
        }
        self.send_authorized(url, range, if_range, limit, &token.token)
    }
    fn discovery(&self, url: &Url, limit: u64) -> Result<crate::hub_http::HttpResponse, GfError> {
        let response = if let Some(bearer) = self.discovery_token.borrow().as_ref() {
            self.send_authorized(url, None, None, limit, bearer)?
        } else {
            self.inner.get(url, None, None, limit)?
        };
        if matches!(response.status, 401 | 403 | 404)
            && let Some(bearer) = (self.discovery_acquire)()?
        {
            let response = self.send_authorized(url, None, None, limit, &bearer)?;
            self.discovery_token.replace(Some(bearer));
            return Ok(response);
        }
        Ok(response)
    }
}
impl<T: Transport> Transport for CloneTransport<T> {
    fn get(
        &self,
        url: &Url,
        range: Option<u64>,
        if_range: Option<&str>,
        limit: u64,
    ) -> Result<crate::hub_http::HttpResponse, GfError> {
        if self.metadata.contains(url) {
            return self.discovery(url, limit);
        }
        let response = if self.token.borrow().is_some() {
            self.authenticated(url, range, if_range, limit)?
        } else {
            self.inner.get(url, range, if_range, limit)?
        };
        if matches!(response.status, 401 | 403 | 404)
            && let Some(token) = (self.acquire)()?
        {
            self.token.replace(Some(token));
            return self.authenticated(url, range, if_range, limit);
        }
        Ok(response)
    }
    fn get_discovery(
        &self,
        url: &Url,
        headers: &[(&str, ureq::http::HeaderValue)],
        limit: u64,
    ) -> Result<crate::hub_http::HttpResponse, GfError> {
        if url.origin() != self.hub.origin() || !self.metadata.contains(url) {
            return Err(denied("discovery credential origin mismatch"));
        }
        // Explicit Hub bearers take precedence. A preview bypass on its own
        // can be combined with the saved identity after anonymous denial.
        let has_bearer = headers.iter().any(|(name, _)| *name == "authorization");
        let response = self.inner.get_discovery(url, headers, limit)?;
        if !has_bearer
            && matches!(response.status, 401 | 403 | 404)
            && let Some(bearer) = (self.discovery_acquire)()?
        {
            validate_secret(&bearer)?;
            let mut authorization = ureq::http::HeaderValue::from_str(&format!("Bearer {bearer}"))
                .map_err(|_| denied("invalid discovery credential"))?;
            authorization.set_sensitive(true);
            let mut combined = headers.to_vec();
            combined.push(("authorization", authorization));
            let response = self.inner.get_discovery(url, &combined, limit)?;
            if matches!(response.status, 301 | 302 | 303 | 307 | 308) {
                return Err(denied("authenticated discovery cannot redirect"));
            }
            self.discovery_token.replace(Some(bearer));
            return Ok(response);
        }
        Ok(response)
    }
}

pub(crate) fn run(
    args: AuthArgs,
    json: bool,
    output: &mut dyn Write,
) -> Result<i32, crate::CliRuntimeError> {
    run_inner(args, json, output).map_err(Into::into)
}

#[cfg(test)]
mod tests;
