//! Browser sign-in with secrets held exclusively in the operating system vault.
use crate::CliRuntimeError;
use crate::hub_http::{DEFAULT_HUB, HubTransport, Transport, validation};
use clap::{Args, Subcommand};
use graphforge_api::GfError;
use graphforge_hub_publish::{HubMethod, HubRequest, HubResponse};
use serde::{Deserialize, Serialize};
use std::io::{Read as _, Write};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use url::Url;

mod control;
pub(crate) use control::exchange_control;

/// Longest client wait, independently of the server code lifetime.
pub(crate) const MAX_DEVICE_WAIT_SECONDS: u64 = 900;
/// Bound each pause even when the server advertises a larger interval.
pub(crate) const MAX_POLL_INTERVAL_SECONDS: u64 = 60;

/// A terminal device-flow outcome, separate from malformed inputs and denial.
#[derive(Debug)]
pub(crate) struct DeviceAuthorizationEnd {
    expires_in: u64,
    wait_limit: bool,
}
impl DeviceAuthorizationEnd {
    pub(crate) fn expired(expires_in: u64) -> CliRuntimeError {
        CliRuntimeError::Authentication(Self {
            expires_in,
            wait_limit: false,
        })
    }
    pub(crate) fn deadline(expires_in: u64) -> CliRuntimeError {
        CliRuntimeError::Authentication(Self {
            expires_in,
            wait_limit: expires_in > MAX_DEVICE_WAIT_SECONDS,
        })
    }
    pub(crate) fn semantic_code(&self) -> &'static str {
        if self.wait_limit {
            "hub.auth_wait_limit"
        } else {
            "hub.auth_expired"
        }
    }
}
impl std::fmt::Display for DeviceAuthorizationEnd {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.wait_limit {
            write!(
                formatter,
                "device authorization stopped at the client wait limit of {MAX_DEVICE_WAIT_SECONDS} seconds"
            )?;
        } else {
            formatter.write_str("device authorization expired")?;
        }
        write!(
            formatter,
            " (server-advertised code lifetime: {} seconds); run the same command again to request a new code",
            self.expires_in
        )
    }
}

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
    /// Save a machine session for Hub access and publishing (primary sign-in path).
    /// Uses the OS credential vault.
    Login {
        #[arg(long, default_value = DEFAULT_HUB)]
        hub: String,
        /// Print a verification link for approval on another device instead of opening a browser.
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
trait StoreLock {}
impl StoreLock for () {}

trait Store {
    fn lock(&self, origin: &str) -> Result<Box<dyn StoreLock + '_>, GfError>;
    fn read(&self, origin: &str) -> Result<Option<String>, SavedCredentialError>;
    fn write(&self, origin: &str, value: &str) -> Result<(), GfError>;
    fn delete(&self, origin: &str) -> Result<(), GfError>;
}
struct Vault;
/// Advisory metadata only; per-origin session records remain authoritative.
const LAST_LOGIN_ORIGIN: &str = "last-successful-login-origin";

/// Read only the advisory origin value, never a session or token record.
pub(crate) fn last_login_origin() -> Result<Option<Url>, GfError> {
    last_login_origin_from(&Vault)
}
fn last_login_origin_from(store: &dyn Store) -> Result<Option<Url>, GfError> {
    let _guard = store.lock(LAST_LOGIN_ORIGIN)?;
    store
        .read(LAST_LOGIN_ORIGIN)?
        .map(|value| {
            if value.len() > 2048 {
                return Err(denied("invalid saved login origin"));
            }
            origin(&value)
        })
        .transpose()
}
fn record_login_origin(store: &dyn Store, hub: &Url) -> Result<(), GfError> {
    let _guard = store.lock(LAST_LOGIN_ORIGIN)?;
    store.write(LAST_LOGIN_ORIGIN, &hub.origin().ascii_serialization())
}
fn clear_login_origin(store: &dyn Store, hub: &Url) -> Result<(), GfError> {
    let _guard = store.lock(LAST_LOGIN_ORIGIN)?;
    if store.read(LAST_LOGIN_ORIGIN)?.as_deref() == Some(&hub.origin().ascii_serialization()) {
        store.delete(LAST_LOGIN_ORIGIN)?;
    }
    Ok(())
}

/// Only a failed read of an unavailable vault may defer to anonymous 404.
/// Invalid stored data, lock failures and refresh failures remain strict.
#[derive(Debug)]
enum SavedCredentialError {
    Unavailable,
    Other(GfError),
}
impl From<GfError> for SavedCredentialError {
    fn from(error: GfError) -> Self {
        Self::Other(error)
    }
}
impl From<SavedCredentialError> for GfError {
    fn from(error: SavedCredentialError) -> Self {
        match error {
            SavedCredentialError::Unavailable => vault_error(),
            SavedCredentialError::Other(error) => error,
        }
    }
}
fn vault_read_error(error: &keyring::Error) -> SavedCredentialError {
    match error {
        keyring::Error::PlatformFailure(_) | keyring::Error::NoStorageAccess(_) => {
            SavedCredentialError::Unavailable
        }
        // Encoding, attributes, ambiguity and future error kinds fail closed.
        _ => SavedCredentialError::Other(vault_error()),
    }
}
fn vault_error() -> GfError {
    denied("OS credential vault unavailable; unlock or configure your system credential service")
}
impl Store for Vault {
    fn lock(&self, origin: &str) -> Result<Box<dyn StoreLock + '_>, GfError> {
        #[cfg(feature = "hub-test-support")]
        if crate::hub_test_support::launcher::active() {
            return Ok(Box::new(()));
        }
        #[cfg(windows)]
        let parent = std::env::var_os("LOCALAPPDATA");
        #[cfg(not(windows))]
        let parent = std::env::var_os("HOME");
        let parent = parent.ok_or_else(lock_error)?;
        Ok(Box::new(lock_in(std::path::Path::new(&parent), origin)?))
    }

    fn read(&self, origin: &str) -> Result<Option<String>, SavedCredentialError> {
        #[cfg(feature = "hub-test-support")]
        if crate::hub_test_support::launcher::active() {
            return crate::hub_test_support::launcher::vault(origin, None).map_err(Into::into);
        }
        match keyring::Entry::new("graphforge-cli", origin)
            .map_err(|error| vault_read_error(&error))?
            .get_password()
        {
            Ok(value) => Ok(Some(value)),
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(error) => Err(vault_read_error(&error)),
        }
    }
    fn write(&self, origin: &str, value: &str) -> Result<(), GfError> {
        #[cfg(feature = "hub-test-support")]
        if crate::hub_test_support::launcher::active() {
            return crate::hub_test_support::launcher::vault(origin, Some(value)).map(|_| ());
        }
        keyring::Entry::new("graphforge-cli", origin)
            .map_err(|_| vault_error())?
            .set_password(value)
            .map_err(|_| vault_error())
    }
    fn delete(&self, origin: &str) -> Result<(), GfError> {
        #[cfg(feature = "hub-test-support")]
        if crate::hub_test_support::launcher::active() {
            return crate::hub_test_support::launcher::vault(origin, Some("")).map(|_| ());
        }
        match keyring::Entry::new("graphforge-cli", origin)
            .map_err(|_| vault_error())?
            .delete_credential()
        {
            Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
            Err(_) => Err(vault_error()),
        }
    }
}
/// These stable empty files coordinate processes; credentials never touch disk.
struct CredentialLock {
    _file: std::fs::File,
    _directory: graphforge_filesystem::StableDirectory,
}
impl StoreLock for CredentialLock {}
fn lock_error() -> GfError {
    denied("cannot secure the per-Hub credential lock directory")
}
fn lock_name(origin: &str) -> String {
    format!(
        "{}.lock",
        graphforge_hub_publish::sha256_digest(origin.as_bytes()).replace(':', "-")
    )
}
fn lock_file_in(
    parent: &std::path::Path,
    origin: &str,
) -> Result<(graphforge_filesystem::StableDirectory, std::fs::File), GfError> {
    let parent_handle =
        graphforge_filesystem::StableDirectory::open(parent).map_err(|_| lock_error())?;
    let path = parent.join(".graphforge-cli-auth-locks");
    match graphforge_filesystem::create_private_directory(&path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(_) => return Err(lock_error()),
    }
    let directory =
        graphforge_filesystem::StableDirectory::open(&path).map_err(|_| lock_error())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        let parent_owner = parent_handle
            .as_file()
            .metadata()
            .map_err(|_| lock_error())?
            .uid();
        let metadata = directory.as_file().metadata().map_err(|_| lock_error())?;
        if metadata.uid() != parent_owner || metadata.mode() & 0o777 != 0o700 {
            return Err(lock_error());
        }
    }
    parent_handle.revalidate_named().map_err(|_| lock_error())?;
    let name = lock_name(origin);
    let file = directory
        .open_or_create_child_file(std::ffi::OsStr::new(&name))
        .map_err(|_| lock_error())?;
    if graphforge_filesystem::file_link_count(&file).map_err(|_| lock_error())? != 1 {
        return Err(lock_error());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        let metadata = file.metadata().map_err(|_| lock_error())?;
        let owner = directory
            .as_file()
            .metadata()
            .map_err(|_| lock_error())?
            .uid();
        if metadata.uid() != owner || metadata.mode() & 0o777 != 0o600 {
            return Err(lock_error());
        }
    }
    Ok((directory, file))
}
fn lock_in(parent: &std::path::Path, origin: &str) -> Result<CredentialLock, GfError> {
    let (directory, file) = lock_file_in(parent, origin)?;
    fs4::FileExt::lock(&file).map_err(|_| lock_error())?;
    directory.revalidate_named().map_err(|_| lock_error())?;
    // Never unlink this file: replacing its inode would split the lock domain.
    Ok(CredentialLock {
        _file: file,
        _directory: directory,
    })
}
fn denied(message: &str) -> GfError {
    validation("hub.auth_denied", message)
}
/// A remote refusal names only the origin we contacted, never a response URL
/// or credential. Formatting requires no credential-store access.
fn denied_at(hub: &Url, message: &str) -> GfError {
    denied(&format!(
        "{message} (Hub origin: {})",
        hub.origin().ascii_serialization()
    ))
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
    let _guard = store.lock(&hub.origin().ascii_serialization())?;
    let session = save_unlocked(store, hub, response, now)?;
    // The authoritative session is committed. A failed advisory hint must not
    // turn that successful login into an authentication failure.
    let _ = record_login_origin(store, hub);
    Ok(session)
}
fn save_unlocked(
    store: &dyn Store,
    hub: &Url,
    response: HubResponse,
    now: u64,
) -> Result<Session, GfError> {
    let HubResponse { status, body, .. } = response;
    if status != 200 {
        return Err(denied_at(hub, "sign-in was refused; run gf auth login"));
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
    session_lookup(store, transport, hub, now).map_err(Into::into)
}
fn session_lookup(
    store: &dyn Store,
    transport: &dyn Transport,
    hub: &Url,
    now: u64,
) -> Result<Option<Session>, SavedCredentialError> {
    let _guard = store.lock(&hub.origin().ascii_serialization())?;
    session_unlocked(store, transport, hub, now)
}
fn session_unlocked(
    store: &dyn Store,
    transport: &dyn Transport,
    hub: &Url,
    now: u64,
) -> Result<Option<Session>, SavedCredentialError> {
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
    save_unlocked(store, hub, response, now)
        .map(Some)
        .map_err(Into::into)
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
/// Select a bounded, display-safe HTTPS approval link. A complete link must
/// share the plain link's origin; authorization providers may differ from Hub.
pub(crate) fn verification_link(plain: &str, complete: Option<&str>) -> Result<Url, GfError> {
    let link = verification_url(plain)?;
    let complete = complete.map(verification_url).transpose()?;
    if complete
        .as_ref()
        .is_some_and(|complete| complete.origin() != link.origin())
    {
        return Err(denied("verification links must share an origin"));
    }
    Ok(complete.unwrap_or(link))
}
fn login(
    store: &dyn Store,
    transport: &dyn Transport,
    hub: &Url,
    output: &mut dyn Write,
    wait: &dyn Fn(Duration),
    clock: &dyn Fn() -> u64,
    open: &dyn Fn(&Url),
) -> Result<(), CliRuntimeError> {
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
        return Err(denied_at(hub, "device authorization was refused").into());
    }
    let device: DeviceAuthorization = serde_json::from_slice(&response.body)
        .map_err(|_| denied("invalid device authorization response"))?;
    validate_secret(&device.device_code)?;
    if device.expires_in == 0
        || device.user_code.is_empty()
        || device.user_code.len() > 128
        || device.user_code.chars().any(char::is_control)
    {
        return Err(denied("invalid device authorization response").into());
    }
    let link = verification_link(
        &device.verification_uri,
        device.verification_uri_complete.as_deref(),
    )?;
    writeln!(
        output,
        "Open {link} and enter code {} (code lifetime: {} seconds)",
        device.user_code, device.expires_in
    )
    .map_err(|_| denied("cannot write verification instructions"))?;
    output
        .flush()
        .map_err(|_| denied("cannot write verification instructions"))?;
    open(&link);
    let deadline = clock().saturating_add(device.expires_in.min(MAX_DEVICE_WAIT_SECONDS));
    let mut interval = device.interval.clamp(1, MAX_POLL_INTERVAL_SECONDS);
    loop {
        let remaining = deadline.saturating_sub(clock());
        if remaining == 0 {
            return Err(DeviceAuthorizationEnd::deadline(device.expires_in));
        }
        wait(Duration::from_secs(interval.min(remaining)));
        if clock() >= deadline {
            return Err(DeviceAuthorizationEnd::deadline(device.expires_in));
        }
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
            Some("slow_down") => {
                interval = interval.saturating_add(5).min(MAX_POLL_INTERVAL_SECONDS);
            }
            Some("expired_token") => {
                return Err(DeviceAuthorizationEnd::expired(device.expires_in));
            }
            _ => return Err(denied_at(hub, "device authorization was denied").into()),
        }
    }
}
pub(crate) fn browser(url: &Url) {
    #[cfg(feature = "hub-test-support")]
    if crate::hub_test_support::launcher::active() {
        return;
    }
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
    let _guard = store.lock(&hub.origin().ascii_serialization())?;
    let saved = session_unlocked(store, transport, hub, now);
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
                Err(denied_at(
                    hub,
                    "remote revocation failed; local credentials removed",
                ))
            }
        }),
        Ok(None) => Ok(()),
        Err(error) => Err(error.into()),
    };
    store.delete(&hub.origin().ascii_serialization())?;
    // Credentials are removed; an advisory hint cannot change that outcome.
    let _ = clear_login_origin(store, hub);
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
        return Err(denied_at(
            hub,
            "session is no longer valid; run gf auth login",
        ));
    }
    let status: SessionStatus =
        serde_json::from_slice(&response.body).map_err(|_| denied("invalid session response"))?;
    if !status.authenticated || status.expires_at <= now {
        return Err(denied_at(
            hub,
            "session is expired or revoked; run gf auth login",
        ));
    }
    validate_secret(&status.principal_id)?;
    Ok(status)
}
fn run_inner(args: AuthArgs, json: bool, output: &mut dyn Write) -> Result<i32, CliRuntimeError> {
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
impl RepositoryToken {
    /// Publish capabilities must stay on the selected Hub control origin.
    pub(crate) fn into_publish_token(
        self,
        hub: &Url,
    ) -> Result<graphforge_hub_publish::PublishToken, GfError> {
        if self.origin.origin() != hub.origin() {
            return Err(validation(
                "hub.auth_denied",
                "publish capability must match the Hub origin",
            ));
        }
        Ok(graphforge_hub_publish::PublishToken::new(self.token))
    }
}
pub(crate) fn repository_token(
    hub: &Url,
    owner: &str,
    name: &str,
    capability: &str,
    bypass_env: Option<&str>,
) -> Result<Option<RepositoryToken>, GfError> {
    let hub = origin(&hub.origin().ascii_serialization())?;
    let transport = PreviewTransport::new(&hub, bypass_env)?;
    repository_token_from(&Vault, &transport, &hub, owner, name, capability, now())
}
fn repository_token_from(
    store: &dyn Store,
    transport: &dyn Transport,
    hub: &Url,
    owner: &str,
    name: &str,
    capability: &str,
    now: u64,
) -> Result<Option<RepositoryToken>, GfError> {
    let Some(session) = session(store, transport, hub, now)? else {
        return Ok(None);
    };
    exchange_repository_token(
        transport,
        hub,
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
        return Err(denied_at(hub, "repository access denied"));
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
    metadata: std::cell::RefCell<Vec<Url>>,
    descriptor: Option<Url>,
    // Item discovery may live below a deployment prefix. Vault/grant identity
    // stays on `hub`'s bare origin; retained metadata uses this selected base.
    item_base: Option<Url>,
    identity: std::rc::Rc<std::cell::RefCell<Option<graphforge_discovery::RepositoryIdentity>>>,
    discovery_acquire: Box<dyn Fn() -> Result<Option<String>, SavedCredentialError>>,
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
) -> Result<Option<String>, SavedCredentialError> {
    if let Some(name) = token_env {
        let bearer = std::env::var(name).map_err(|_| {
            denied("clone credential environment variable is missing or not Unicode")
        })?;
        validate_secret(&bearer)?;
        Ok(Some(bearer))
    } else {
        Ok(
            session_lookup(&Vault, &PreviewTransport::new(hub, bypass_env)?, hub, now())?
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
        Self::for_selection(Some(identity), &base, None, token_env, bypass_env)
    }
    pub(crate) fn for_item(
        hub: &Url,
        descriptor: Url,
        bypass_env: Option<&str>,
    ) -> Result<Self, GfError> {
        crate::hub_http::validate_url(hub)?;
        if descriptor.origin() != hub.origin()
            || !descriptor
                .path()
                .starts_with(&format!("{}/", hub.path().trim_end_matches('/')))
        {
            return Err(denied("item descriptor selection mismatch"));
        }
        Self::for_selection(None, hub, Some(descriptor), None, bypass_env)
    }
    fn for_selection(
        selected: Option<graphforge_discovery::RepositoryIdentity>,
        base: &Url,
        descriptor: Option<Url>,
        token_env: Option<&str>,
        bypass_env: Option<&str>,
    ) -> Result<Self, GfError> {
        let metadata = if selected.is_some() {
            vec![
                crate::hub_http::endpoint(base, "refs"),
                crate::hub_http::endpoint(base, "manifest"),
            ]
        } else {
            vec![
                descriptor
                    .clone()
                    .ok_or_else(|| denied("missing item selection"))?,
            ]
        };
        let identity = std::rc::Rc::new(std::cell::RefCell::new(selected));
        let grant_identity = identity.clone();
        let hub = origin(&base.origin().ascii_serialization())?;
        let auth_hub = hub.clone();
        let metadata_hub = hub.clone();
        let token_env = token_env.map(str::to_owned);
        let metadata_env = token_env.clone();
        let bypass_env = bypass_env.map(str::to_owned);
        let metadata_bypass = bypass_env.clone();
        let item_base = descriptor.as_ref().map(|_| base.clone());
        Ok(Self {
            inner: crate::hub_http::HttpTransport::new(),
            hub,
            metadata: std::cell::RefCell::new(metadata),
            descriptor,
            item_base,
            identity,
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
                let selected = grant_identity.borrow();
                let selected = selected.as_ref().ok_or_else(|| {
                    denied("item descriptor must be verified before object access")
                })?;
                exchange_repository_token(
                    &PreviewTransport::new(&auth_hub, bypass_env.as_deref())?,
                    &auth_hub,
                    &bearer,
                    &selected.owner,
                    &selected.repository,
                    "read",
                )
                .map(Some)
            }),
            token: std::cell::RefCell::new(None),
        })
    }
}
impl<T: Transport> CloneTransport<T> {
    fn discovery_authorized(
        &self,
        url: &Url,
        limit: u64,
        bearer: &str,
    ) -> Result<crate::hub_http::HttpResponse, GfError> {
        validate_secret(bearer)?;
        let mut header = ureq::http::HeaderValue::from_str(&format!("Bearer {bearer}"))
            .map_err(|_| denied("invalid discovery credential"))?;
        header.set_sensitive(true);
        let response =
            self.inner
                .get_authorized(url, None, None, limit, &[("authorization", header)])?;
        self.discovery_response(url, response)
    }
    fn discovery_response(
        &self,
        url: &Url,
        mut response: crate::hub_http::HttpResponse,
    ) -> Result<crate::hub_http::HttpResponse, GfError> {
        if matches!(response.status, 301 | 302 | 303 | 307 | 308) {
            if self.descriptor.as_ref() == Some(url) || response.status != 308 {
                return Err(denied("authenticated discovery cannot redirect"));
            }
            let mut bytes = Vec::new();
            response
                .body
                .take(4097)
                .read_to_end(&mut bytes)
                .map_err(|_| denied("move response read failed"))?;
            if bytes.len() > 4096 {
                return Err(denied("move response exceeds bound"));
            }
            let moved = graphforge_discovery::RepositoryMoved::from_json(
                &bytes,
                graphforge_discovery::DiscoveryLimits {
                    max_response_bytes: 4096,
                    ..Default::default()
                },
            )
            .map_err(|_| denied("authenticated discovery move is invalid"))?;
            if Some(&moved.moved_from) != self.identity.borrow().as_ref() {
                return Err(denied("authenticated discovery move identity mismatch"));
            }
            moved
                .destination(
                    url,
                    response
                        .location
                        .as_deref()
                        .ok_or_else(|| denied("move destination is absent"))?,
                )
                .map_err(|_| denied("authenticated discovery move destination is invalid"))?;
            // Only expose the bounded marker to the outer agreement checker.
            // No authenticated redirect is followed here.
            response.body = Box::new(std::io::Cursor::new(bytes));
        }
        Ok(response)
    }
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
        let anonymous = self.discovery_token.borrow().is_none();
        let response = if let Some(bearer) = self.discovery_token.borrow().as_ref() {
            self.discovery_authorized(url, limit, bearer)?
        } else {
            self.inner.get(url, None, None, limit)?
        };
        if matches!(response.status, 401 | 403 | 404)
            && let Some(bearer) = self.acquire_discovery(response.status, anonymous)?
        {
            let response = self.discovery_authorized(url, limit, &bearer)?;
            self.discovery_token.replace(Some(bearer));
            return Ok(response);
        }
        Ok(response)
    }
    fn acquire_discovery(&self, status: u16, anonymous: bool) -> Result<Option<String>, GfError> {
        match (self.discovery_acquire)() {
            // A masked private 404 can still retry when saved credentials exist.
            // Only an initially anonymous 404 may outlive an unavailable implicit
            // vault read; explicit credentials and all other errors stay strict.
            Err(SavedCredentialError::Unavailable) if anonymous && status == 404 => Ok(None),
            result => result.map_err(Into::into),
        }
    }
}
impl<T: Transport> Transport for CloneTransport<T> {
    fn item_descriptor_verified(
        &self,
        url: &Url,
        descriptor: &graphforge_discovery::ItemDescriptor,
    ) -> Result<(), GfError> {
        if self.descriptor.as_ref() != Some(url) || url.origin() != self.hub.origin() {
            return Err(denied("item descriptor selection mismatch"));
        }
        descriptor
            .validate(graphforge_discovery::DiscoveryLimits::default())
            .map_err(|_| denied("invalid item descriptor"))?;
        let (_, base) = crate::hub_http::parse_input_at(
            &descriptor.repository.canonical_name(),
            Some(
                self.item_base
                    .as_ref()
                    .ok_or_else(|| denied("missing item selection"))?
                    .as_str(),
            ),
        )?;
        let mut refs = crate::hub_http::endpoint(&base, "refs");
        refs.query_pairs_mut()
            .append_pair("revision", &descriptor.discovery.refs_revision.0);
        let mut manifest = crate::hub_http::endpoint(&base, "manifest");
        manifest
            .query_pairs_mut()
            .append_pair("validator", &descriptor.discovery.manifest_validator.0);
        self.metadata.replace(vec![refs, manifest]);
        self.identity.replace(Some(descriptor.repository.clone()));
        self.token.replace(None);
        Ok(())
    }
    fn repository_moved(
        &self,
        moved: &graphforge_discovery::RepositoryMoved,
        current: &Url,
    ) -> Result<(), GfError> {
        moved
            .validate()
            .map_err(|_| denied("repository move identity is invalid"))?;
        let mut expected = self.hub.clone();
        expected.set_path(&format!("/{}", moved.moved_to.canonical_name()));
        if Some(&moved.moved_from) != self.identity.borrow().as_ref() || current != &expected {
            return Err(denied("repository authentication rebind is invalid"));
        }
        self.metadata.replace(vec![
            crate::hub_http::endpoint(current, "refs"),
            crate::hub_http::endpoint(current, "manifest"),
        ]);
        self.identity.replace(Some(moved.moved_to.clone()));
        self.token.replace(None);
        // The cached WorkOS bearer remains bound to exactly the same Hub
        // origin. A new repository grant must be acquired under the new name.
        Ok(())
    }
    fn get(
        &self,
        url: &Url,
        range: Option<u64>,
        if_range: Option<&str>,
        limit: u64,
    ) -> Result<crate::hub_http::HttpResponse, GfError> {
        if self.metadata.borrow().contains(url) {
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
        if url.origin() != self.hub.origin() || !self.metadata.borrow().contains(url) {
            return Err(denied("discovery credential origin mismatch"));
        }
        // Explicit Hub bearers take precedence. A preview bypass on its own
        // can be combined with the saved identity after anonymous denial.
        let has_bearer = headers.iter().any(|(name, _)| *name == "authorization");
        let anonymous = !has_bearer && self.discovery_token.borrow().is_none();
        let mut combined = headers.to_vec();
        if !has_bearer && let Some(bearer) = self.discovery_token.borrow().as_ref() {
            let mut value = ureq::http::HeaderValue::from_str(&format!("Bearer {bearer}"))
                .map_err(|_| denied("invalid discovery credential"))?;
            value.set_sensitive(true);
            combined.push(("authorization", value));
        }
        let response = self.inner.get_discovery(url, &combined, limit)?;
        let response = if combined.iter().any(|(name, _)| *name == "authorization") {
            self.discovery_response(url, response)?
        } else {
            response
        };
        if !has_bearer
            && matches!(response.status, 401 | 403 | 404)
            && let Some(bearer) = self.acquire_discovery(response.status, anonymous)?
        {
            validate_secret(&bearer)?;
            let mut authorization = ureq::http::HeaderValue::from_str(&format!("Bearer {bearer}"))
                .map_err(|_| denied("invalid discovery credential"))?;
            authorization.set_sensitive(true);
            combined.retain(|(name, _)| *name != "authorization");
            combined.push(("authorization", authorization));
            let response =
                self.discovery_response(url, self.inner.get_discovery(url, &combined, limit)?)?;
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
    run_inner(args, json, output)
}

#[cfg(test)]
pub(crate) mod tests;

#[cfg(test)]
mod lock_tests;
