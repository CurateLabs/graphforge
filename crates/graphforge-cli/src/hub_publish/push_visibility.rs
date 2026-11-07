//! Push create intent, decided before publication identity or mutable operations.
use super::{
    CliRuntimeError, Credential, GfError, Hub, HubPublishErrorCode, PublicationContext,
    PublishArgs, PublishCapabilities, PublishEnvironment, PublishToken, Transport, VisibilityArgs,
    obtain_token, protocol, publish_error, publish_with_resolution, read_private_context,
    resolve_publication,
};

use graphforge_hub_publish::PublishVisibility;

/// A credential never supplies visibility intent for a noninteractive push.
pub(crate) fn require_push_visibility(visibility: &VisibilityArgs) -> Result<(), GfError> {
    if !visibility.public && !visibility.private && !crate::process::interactive_process() {
        return Err(push_visibility_required(false));
    }
    Ok(())
}

fn push_visibility_required(cancelled: bool) -> GfError {
    publish_error(
        HubPublishErrorCode::InvalidInput,
        &format!(
            "{}create it with --public or --private. Private repositories need a plan.",
            if cancelled {
                "cancelled; nothing was created. "
            } else {
                "push requires a visibility choice; "
            },
        ),
    )
}

/// Push adds a create decision to the existing publication flow. Explicit flags
/// retain publish's credential, operation notice and replay ordering.
pub(crate) fn push_with_credentials(
    transport: &dyn Transport,
    args: &PublishArgs,
    load_token: impl FnOnce() -> Result<Option<PublishToken>, GfError>,
    bypass: Option<&crate::hub_http::DiscoveryCredentials>,
    publish_with: impl FnOnce(&PublishArgs, &mut PublishEnvironment<'_>) -> Result<(), CliRuntimeError>,
) -> Result<(), CliRuntimeError> {
    require_push_visibility(&args.visibility)?;
    let (identity, base) = args.destination()?;
    let resolution = resolve_publication(transport, &identity, &base, bypass)?;
    graphforge_hub_publish::require_visibility_capability(
        &resolution.capabilities,
        args.visibility()?,
    )
    .map_err(|error| protocol(&error))?;
    if args.visibility()?.is_none() {
        require_push_context(&resolution.capabilities)?;
    }
    publish_with_resolution(
        transport,
        args,
        load_token()?,
        bypass,
        Some(resolution),
        |environment| {
            let mut chosen = args.clone();
            choose_push_visibility(transport, &mut chosen, environment)?;
            publish_with(&chosen, environment)
        },
    )
}

fn require_push_context(capabilities: &PublishCapabilities) -> Result<(), GfError> {
    graphforge_hub_publish::require_visibility_capability(
        capabilities,
        Some(PublishVisibility::Public),
    )
    .map_err(|error| protocol(&error))?;
    if !capabilities
        .capabilities
        .iter()
        .any(|c| c.capability == "publish-read-context" && c.major == 1)
    {
        return Err(publish_error(
            HubPublishErrorCode::UnsupportedFuture,
            "interactive push requires the Hub's publish-read-context capability; create it with --public or --private",
        ));
    }
    Ok(())
}

pub(super) fn choose_push_visibility(
    transport: &dyn Transport,
    args: &mut PublishArgs,
    environment: &mut PublishEnvironment<'_>,
) -> Result<(), CliRuntimeError> {
    let io = |error: std::io::Error| GfError::Execution(error.to_string());
    if args.visibility()?.is_some() {
        return Ok(());
    }
    if environment.terminal.is_none() {
        return Err(push_visibility_required(false).into());
    }
    let mut resolution = environment
        .resolution
        .take()
        .expect("push resolved before credentials");
    require_push_context(&resolution.capabilities)?;
    let hub = Hub {
        refusal: None,
        bypass: environment.bypass,
        transport,
        base: resolution.base.clone(),
    };
    let token = obtain_token(&hub, &resolution.capabilities, environment)?;
    let published = read_private_context(&hub, &resolution.identity, &token)?;
    environment.credential = Credential::Token(token);
    if published.is_none() {
        // All-null means create-if-absent, including an unpublished website record.
        write!(environment.notices,
            "{} doesn't exist on {}. Create it as (P)ublic — anyone can read and clone it — or p(R)ivate — needs a plan? [p/r/cancel] ",
            resolution.identity.canonical_name(), resolution.base.origin().ascii_serialization()).map_err(io)?;
        environment.notices.flush().map_err(io)?;
        let mut answer = String::new();
        let read = environment
            .terminal
            .as_mut()
            .unwrap()
            .read_line(&mut answer);
        writeln!(environment.notices).map_err(io)?;
        args.visibility = match read {
            Ok(n) if n > 0 => match answer.trim().to_ascii_lowercase().as_str() {
                "p" | "public" => VisibilityArgs {
                    public: true,
                    private: false,
                },
                "r" | "private" => VisibilityArgs {
                    public: false,
                    private: true,
                },
                _ => return Err(push_visibility_required(true).into()),
            },
            _ => return Err(push_visibility_required(true).into()),
        };
    }
    resolution.context = match published {
        Some(published) => PublicationContext::Published(Box::new(published)),
        None => PublicationContext::Unpublished,
    };
    environment.resolution = Some(resolution);
    Ok(())
}
