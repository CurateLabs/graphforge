//! Resolve permanent IDs to retained metadata, then use the existing verifier.

use super::{
    DiscoveryLimits, DiscoveryManifest, ItemId, MAX_METADATA_BYTES, MAX_MODULE_PACKAGE_BYTES,
    ModuleFetchArgs, ModuleFetchResult, RefSet, RetryPolicy, Sha256Digest, Transport, Url,
    acquire_staging, digest_pin, endpoint, ensure_output_available, fetch_and_publish_module,
    fetch_document, normalized_id, parse_input_at, protocol_error, release_staging, validation,
};
use graphforge_discovery::{
    ITEM_DESCRIPTOR_MEDIA_TYPE, ITEM_RESOLUTION_ERROR_MAX_BYTES, ItemDescriptor, ItemKind,
    has_item_id_shape, parse_item_resolution_refusal,
};
use std::io::Read;

pub(super) struct ItemReference {
    id: ItemId,
    version: Option<String>,
}

pub(super) fn parse_reference(
    value: &str,
) -> Result<Option<ItemReference>, graphforge_api::GfError> {
    if let Ok(id) = ItemId::parse_reference(value) {
        return Ok(Some(ItemReference { id, version: None }));
    }
    if value.contains("://") {
        let url = Url::parse(value).map_err(|_| invalid("invalid item or repository URL"))?;
        let first = url
            .path()
            .trim_start_matches('/')
            .split('/')
            .next()
            .unwrap_or_default();
        if has_item_id_shape(&first.to_ascii_lowercase()) {
            return Err(invalid("item URLs must be canonical GraphForge item URIs"));
        }
        return Ok(None);
    }
    let (first, version) = value
        .split_once('/')
        .map_or((value, None), |(id, version)| (id, Some(version)));
    if !has_item_id_shape(&first.to_ascii_lowercase()) {
        return Ok(None);
    }
    let id = ItemId::parse(first).map_err(|_| invalid("unknown or malformed permanent item ID"))?;
    if version.is_some_and(str::is_empty) {
        return Err(invalid("authored version must not be empty"));
    }
    Ok(Some(ItemReference {
        id,
        version: version.map(str::to_owned),
    }))
}

fn invalid(message: &str) -> graphforge_api::GfError {
    validation("hub.invalid_identity", message)
}

fn mismatch(message: &str) -> graphforge_api::GfError {
    validation("hub.integrity_failure", message)
}

/// Only the negotiated descriptor request carries Accept; packages use their
/// existing credential-free transport path.
struct DescriptorTransport<'a>(&'a dyn Transport);

impl Transport for DescriptorTransport<'_> {
    fn validate(&self, url: &Url) -> Result<(), graphforge_api::GfError> {
        self.0.validate(url)
    }

    fn get(
        &self,
        url: &Url,
        _range: Option<u64>,
        _if_range: Option<&str>,
        limit: u64,
    ) -> Result<super::super::HttpResponse, graphforge_api::GfError> {
        let mut response = self.0.get_discovery(
            url,
            &[(
                "accept",
                ureq::http::HeaderValue::from_static(ITEM_DESCRIPTOR_MEDIA_TYPE),
            )],
            limit,
        )?;
        if response.status == 409 {
            let mut bytes = Vec::new();
            response
                .body
                .by_ref()
                .take((ITEM_RESOLUTION_ERROR_MAX_BYTES + 1) as u64)
                .read_to_end(&mut bytes)
                .map_err(|_| {
                    validation(
                        "hub.malformed_response",
                        "item resolution refusal is unreadable",
                    )
                })?;
            let error = parse_item_resolution_refusal(409, &bytes).unwrap_or_else(|error| error);
            return Err(protocol_error(&error));
        }
        Ok(response)
    }
}

/// Permit only the exact fixed snapshot selector. The ordinary URL guard
/// remains strict for every other request, including package objects.
struct SnapshotTransport<'a> {
    inner: &'a dyn Transport,
    expected: Url,
}

impl Transport for SnapshotTransport<'_> {
    fn validate(&self, url: &Url) -> Result<(), graphforge_api::GfError> {
        if url != &self.expected {
            return Err(mismatch("retained discovery endpoint changed"));
        }
        let mut public = url.clone();
        public.set_query(None);
        self.inner.validate(&public)
    }

    fn get(
        &self,
        url: &Url,
        range: Option<u64>,
        if_range: Option<&str>,
        limit: u64,
    ) -> Result<super::super::HttpResponse, graphforge_api::GfError> {
        self.inner.get(url, range, if_range, limit)
    }
}

fn snapshot_document(
    transport: &dyn Transport,
    base: &Url,
    name: &str,
    selector: &str,
    digest: &Sha256Digest,
    retry: &RetryPolicy,
) -> Result<Vec<u8>, graphforge_api::GfError> {
    let mut url = endpoint(base, name);
    url.query_pairs_mut().append_pair(selector, &digest.0);
    let scoped = SnapshotTransport {
        inner: transport,
        expected: url.clone(),
    };
    fetch_document(&scoped, &url, retry)
}

fn item_url(hub: &Url, id: &ItemId, version: Option<&str>) -> Result<Url, graphforge_api::GfError> {
    let mut url = hub.clone();
    url.path_segments_mut()
        .map_err(|()| invalid("invalid Hub URL"))?
        .pop_if_empty()
        .push(id.as_str());
    if let Some(version) = version {
        let prefix = format!("{}/", url.path());
        url.path_segments_mut()
            .map_err(|()| invalid("invalid Hub URL"))?
            .push(version);
        if !url.path().starts_with(&prefix) {
            return Err(invalid(
                "authored version cannot be represented as an item URL segment",
            ));
        }
    }
    Ok(url)
}

pub(super) fn fetch(
    transport: &dyn Transport,
    args: &ModuleFetchArgs,
    reference: &ItemReference,
    retry: &RetryPolicy,
) -> Result<ModuleFetchResult, graphforge_api::GfError> {
    if reference.id.kind() != ItemKind::OntologyModule {
        return Err(invalid(
            "ontology module fetch does not fetch bridge packages",
        ));
    }
    if args
        .ontology_id
        .as_deref()
        .is_some_and(|id| normalized_id(id) != reference.id.to_uri())
    {
        return Err(invalid("--ontology-id disagrees with the item reference"));
    }
    let version = match (reference.version.as_deref(), args.version.as_deref()) {
        (Some(from_ref), Some(from_flag)) if from_ref != from_flag => {
            return Err(invalid("--version disagrees with the item reference"));
        }
        (from_ref, from_flag) => from_ref.or(from_flag),
    };
    if version.is_some_and(|version| {
        version.is_empty() || version.len() > DiscoveryLimits::default().max_string_bytes
    }) {
        return Err(invalid(
            "authored version is empty or exceeds its byte bound",
        ));
    }
    let pin = digest_pin(args)?;
    let hub = crate::hub_http::hub_base(args.hub.as_deref())?;
    ensure_output_available(&args.output)?;
    let url = item_url(&hub, &reference.id, version)?;
    let limits = DiscoveryLimits {
        max_response_bytes: MAX_METADATA_BYTES,
        max_module_package_bytes: MAX_MODULE_PACKAGE_BYTES,
        ..DiscoveryLimits::default()
    };
    let descriptor = ItemDescriptor::from_json(
        &fetch_document(&DescriptorTransport(transport), &url, retry)?,
        limits,
    )
    .map_err(|error| protocol_error(&error))?;
    if descriptor.id != reference.id.as_str() || descriptor.kind != ItemKind::OntologyModule {
        return Err(mismatch("descriptor names a different item"));
    }
    if version.is_some_and(|version| descriptor.module.version != version) {
        return Err(mismatch("descriptor names a different authored version"));
    }
    if pin
        .as_ref()
        .is_some_and(|pin| pin != &descriptor.module.content_digest)
    {
        return Err(mismatch("descriptor disagrees with --digest pin"));
    }
    let (_, base) = parse_input_at(&descriptor.repository.canonical_name(), Some(hub.as_str()))?;
    let refs_bytes = snapshot_document(
        transport,
        &base,
        "refs",
        "revision",
        &descriptor.discovery.refs_revision,
        retry,
    )?;
    let manifest_bytes = snapshot_document(
        transport,
        &base,
        "manifest",
        "validator",
        &descriptor.discovery.manifest_validator,
        retry,
    )?;
    let refs = RefSet::from_json(&refs_bytes, limits).map_err(|error| protocol_error(&error))?;
    let manifest = DiscoveryManifest::from_json(&manifest_bytes, limits)
        .map_err(|error| protocol_error(&error))?;
    descriptor
        .bind_snapshot(&refs, &manifest)
        .map_err(|error| protocol_error(&error))?;
    let (_, _, object) = manifest
        .ontology_module_selection(&descriptor.module)
        .map_err(|error| protocol_error(&error))?;
    let staging = acquire_staging(&args.output)?;
    let result = fetch_and_publish_module(
        transport,
        args,
        &descriptor.repository,
        &descriptor.module,
        &manifest_bytes,
        &refs_bytes,
        object,
        limits,
        &staging,
        retry,
    );
    release_staging(staging);
    result
}
