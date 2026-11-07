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

/// A prompt answer is part of the native operation identity. After a remote
/// commit, fresh argv has no answer and the target is no longer absent. Recover
/// only an authenticated status matching this exact selection and one visibility
/// intent; no local receipt or current repository visibility is substituted.
pub(super) fn recover_operation(
    hub: &Hub<'_>,
    token: &PublishToken,
    identity: &super::RepositoryIdentity,
    selection: &super::Selection,
    args: &PublishArgs,
    content: &str,
    intent: &str,
) -> Result<(PublishArgs, uuid::Uuid, Option<super::OperationStatus>), GfError> {
    let given = args.operation_uuid.as_deref().map(|value| {
        uuid::Uuid::parse_str(value).expect("operation UUID validated before recovery")
    });
    let operation = |candidate: &PublishArgs| {
        given.unwrap_or_else(|| {
            graphforge_api::hub_publish_operation(
                &identity.canonical_name(),
                selection.git_ref.as_deref(),
                &selection.version_uuid.to_string(),
                &super::history::bind_flags(content.to_owned(), candidate),
            )
        })
    };
    let initial_uuid = operation(args);
    let initial = super::read_operation_status(hub, token, identity, initial_uuid)?;
    let alternatives = [
        VisibilityArgs {
            public: true,
            private: false,
        },
        VisibilityArgs {
            public: false,
            private: true,
        },
    ];
    if given.is_some() || initial.is_some() {
        if let Some(status) = &initial
            && status.intent_digest != super::history::bind_flags(intent.to_owned(), args)
            && given.is_some()
        {
            for visibility in alternatives {
                let mut candidate = args.clone();
                candidate.visibility = visibility;
                if status.intent_digest == super::history::bind_flags(intent.to_owned(), &candidate)
                {
                    return Ok((candidate, initial_uuid, initial));
                }
            }
        }
        return Ok((args.clone(), initial_uuid, initial));
    }
    let mut recovered = None;
    for visibility in alternatives {
        let mut candidate = args.clone();
        candidate.visibility = visibility;
        let uuid = operation(&candidate);
        if let Some(status) = super::read_operation_status(hub, token, identity, uuid)? {
            if status.intent_digest != super::history::bind_flags(intent.to_owned(), &candidate)
                || recovered.is_some()
            {
                return Err(publish_error(
                    HubPublishErrorCode::IdempotencyConflict,
                    "earlier publication intent is ambiguous or differs; retry with the original --public or --private choice",
                ));
            }
            recovered = Some((candidate, uuid, Some(status)));
        }
    }
    Ok(recovered.unwrap_or_else(|| (args.clone(), initial_uuid, None)))
}
