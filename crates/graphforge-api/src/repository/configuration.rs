//! Repository configuration ownership.

use super::{
    BTreeMap, BTreeSet, Deserialize, Digest, GfError, InfraCapabilityCompatibility,
    InfraNotChecked, InfraPlan, InfraStaticValidity, InfraValidationResult, MAX_PORTABLE_INTEGER,
    Path, RepositoryContext, Serialize, Sha256, Value, bounded, digest, encode_hex, fs, json,
    stable_id, uri_has_inline_credentials, validation,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DefinitionKind {
    Migrations,
    Ontology,
    Schemas,
    Seeds,
}

impl DefinitionKind {
    pub(super) const fn id(self) -> &'static str {
        match self {
            Self::Migrations => "migrations",
            Self::Ontology => "ontology",
            Self::Schemas => "schemas",
            Self::Seeds => "seeds",
        }
    }

    fn allows_extension(self, extension: &str) -> bool {
        matches!(extension, "json" | "yaml" | "yml")
            || matches!(self, Self::Migrations) && matches!(extension, "cypher")
    }
}

pub(super) fn digest_definition_tree(
    root: &Path,
    definition_kind: DefinitionKind,
) -> Result<String, GfError> {
    let mut pending = vec![root.to_path_buf()];
    let mut files = Vec::new();
    let mut total = 0_u64;
    while let Some(directory) = pending.pop() {
        for entry in
            fs::read_dir(&directory).map_err(|error| GfError::Storage(error.to_string()))?
        {
            let entry = entry.map_err(|error| GfError::Storage(error.to_string()))?;
            let kind = entry
                .file_type()
                .map_err(|error| GfError::Storage(error.to_string()))?;
            if kind.is_symlink() {
                return Err(validation("symlinks are not allowed in definitions"));
            }
            if kind.is_dir() {
                pending.push(entry.path());
            } else if kind.is_file() {
                files.push(entry.path());
            }
            if files.len() + pending.len() > 10_000 {
                return Err(validation("definition tree exceeds file bound"));
            }
        }
    }
    files.sort();
    let mut hash = Sha256::new();
    for path in files {
        let relative = path
            .strip_prefix(root)
            .map_err(|_| validation("definition path escaped its root"))?;
        let extension = path
            .extension()
            .and_then(|value| value.to_str())
            .map(str::to_ascii_lowercase)
            .ok_or_else(|| validation("definition files require a registered extension"))?;
        if !definition_kind.allows_extension(&extension) {
            return Err(validation(format!(
                "{} definitions do not allow .{extension} files",
                definition_kind.id()
            )));
        }
        let bytes = fs::read(&path).map_err(|error| GfError::Storage(error.to_string()))?;
        if bytes.len() > 1024 * 1024 {
            return Err(validation("definition file exceeds byte bound"));
        }
        total = total.saturating_add(bytes.len() as u64);
        if total > 16 * 1024 * 1024 {
            return Err(validation("definition tree exceeds byte bound"));
        }
        validate_definition_document(&bytes, &extension)?;
        hash.update(relative.to_string_lossy().replace('\\', "/").as_bytes());
        hash.update([0]);
        hash.update((bytes.len() as u64).to_be_bytes());
        hash.update(&bytes);
    }
    Ok(encode_hex(&hash.finalize()))
}

fn validate_definition_document(bytes: &[u8], extension: &str) -> Result<(), GfError> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| validation("definition file must be canonical UTF-8 text"))?;
    if text.contains('\0') {
        return Err(validation("definition file contains binary data"));
    }
    match extension {
        "json" => {
            let value: Value = serde_json::from_str(text)
                .map_err(|_| validation("JSON definition is malformed"))?;
            if !value.is_object() {
                return Err(validation("JSON definition must be an object"));
            }
        }
        "yaml" | "yml" => {
            let value: serde_yaml::Value = serde_yaml::from_str(text)
                .map_err(|_| validation("YAML definition is malformed"))?;
            if !value.is_mapping() {
                return Err(validation("YAML definition must be a mapping"));
            }
        }
        "cypher" => {
            if text.trim().is_empty() {
                return Err(validation("Cypher migration definition is empty"));
            }
        }
        _ => return Err(validation("unregistered definition file type")),
    }
    Ok(())
}

/// Closed repository configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectConfig {
    schema_version: u32,
    pub(super) project: DefinitionPaths,
    #[serde(default)]
    pub(super) sources: Vec<Source>,
    #[serde(default)]
    secrets: Vec<SecretReference>,
    targets: BTreeMap<String, Target>,
}

impl ProjectConfig {
    pub(super) fn validate(&self, context: &RepositoryContext) -> Result<(), GfError> {
        if self.schema_version != 1 {
            return Err(validation("unsupported schema_version"));
        }
        if self.sources.len() > 256 || self.secrets.len() > 128 || self.targets.len() > 64 {
            return Err(validation(
                "configuration collection exceeds contract bounds",
            ));
        }
        let mut source_ids = BTreeSet::new();
        for source in &self.sources {
            stable_id(&source.id)?;
            if !source_ids.insert(&source.id) {
                return Err(validation("duplicate source id"));
            }
            digest(&source.sha256)?;
            if uri_has_inline_credentials(&source.uri) {
                return Err(validation("source URI must not contain inline credentials"));
            }
            bounded(&source.uri, 1, 2048, "source URI")?;
            if let Some(media_type) = &source.media_type {
                bounded(media_type, 1, 128, "media type")?;
            }
        }
        let mut secret_ids = BTreeSet::new();
        for secret in &self.secrets {
            stable_id(&secret.id)?;
            if !secret_ids.insert(&secret.id) {
                return Err(validation("duplicate secret id"));
            }
        }
        if self.targets.is_empty() {
            return Err(validation("at least one target is required"));
        }
        for (id, target) in &self.targets {
            stable_id(id)?;
            digest(&target.artifact.sha256)?;
            bounded(&target.artifact.version, 1, 128, "artifact version")?;
            if target.source_ids.len() > 256 || target.secret_ids.len() > 128 {
                return Err(validation(
                    "target reference collection exceeds contract bounds",
                ));
            }
            if target.capabilities.len() > 64 {
                return Err(validation(
                    "target capability collection exceeds contract bounds",
                ));
            }
            if target.source_ids.iter().collect::<BTreeSet<_>>().len() != target.source_ids.len()
                || target.secret_ids.iter().collect::<BTreeSet<_>>().len()
                    != target.secret_ids.len()
            {
                return Err(validation("target references must be unique"));
            }
            let mut capability_ids = BTreeSet::new();
            for capability in &target.capabilities {
                stable_id(&capability.id)?;
                if capability.version == 0 || !capability_ids.insert(&capability.id) {
                    return Err(validation(
                        "target capability requirements must have unique ids and positive versions",
                    ));
                }
            }
            target.validate_bounds()?;
            target.validate_semantics()?;
            if target.source_ids.iter().any(|id| !source_ids.contains(id)) {
                return Err(validation("target references an unknown source"));
            }
            if target.secret_ids.iter().any(|id| !secret_ids.contains(id)) {
                return Err(validation("target references an unknown secret"));
            }
        }
        for path in self.project.paths() {
            bounded(path, 1, 1024, "definition path")?;
            if path.contains('\\') {
                return Err(validation("definition paths must use '/' separators"));
            }
            context.contained_path(path)?;
        }
        Ok(())
    }

    pub(super) fn resolve(&self) -> Result<Value, GfError> {
        let mut targets = Vec::new();
        for (id, target) in &self.targets {
            let mut value =
                serde_json::to_value(target).map_err(|error| validation(error.to_string()))?;
            let object = value.as_object_mut().expect("target serializes as object");
            object.insert("id".into(), json!(id));
            object.insert(
                "ownership".into(),
                serde_json::to_value(target.effective_ownership())
                    .expect("target ownership serializes"),
            );
            object.insert(
                "topology".into(),
                serde_json::to_value(target.effective_topology())
                    .expect("target topology serializes"),
            );
            let mut capabilities = target.capabilities.clone();
            capabilities
                .sort_by(|left, right| (&left.id, left.version).cmp(&(&right.id, right.version)));
            object.insert(
                "capabilities".into(),
                serde_json::to_value(capabilities).expect("target capabilities serialize"),
            );
            object.entry("resources").or_insert_with(|| json!({}));
            object
                .entry("network")
                .or_insert_with(|| json!({"exposure":"none","tls_required":false}));
            object
                .entry("health")
                .or_insert_with(|| json!({"timeout_seconds":30}));
            object
                .entry("observability")
                .or_insert_with(|| json!({"logs":true,"metrics":false,"traces":false}));
            object
                .entry("backup")
                .or_insert_with(|| json!({"checkpoints":false}));
            fill_target_defaults(object);
            targets.push(value);
        }
        let mut sources = self.sources.clone();
        sources.sort_by(|left, right| left.id.cmp(&right.id));
        let mut secrets = self.secrets.clone();
        secrets.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(json!({
            "contract": "graphforge-resolved-config/1",
            "project": {
                "integration_root": ".graphforge", "state": ".graphforge/state",
                "imports": ".graphforge/imports", "exports": ".graphforge/exports",
                "ontology": self.project.ontology, "schemas": self.project.schemas,
                "seeds": self.project.seeds, "migrations": self.project.migrations
            },
            "sources": sources, "secrets": secrets, "targets": targets
        }))
    }
}

fn fill_target_defaults(object: &mut serde_json::Map<String, Value>) {
    insert_defaults(
        object,
        "network",
        &[("exposure", json!("none")), ("tls_required", json!(false))],
    );
    insert_defaults(
        object,
        "observability",
        &[
            ("logs", json!(true)),
            ("metrics", json!(false)),
            ("traces", json!(false)),
        ],
    );
    insert_defaults(object, "backup", &[("checkpoints", json!(false))]);
}

fn insert_defaults(
    object: &mut serde_json::Map<String, Value>,
    section: &str,
    fields: &[(&str, Value)],
) {
    if let Some(Value::Object(values)) = object.get_mut(section) {
        for (name, default) in fields {
            values
                .entry((*name).to_owned())
                .or_insert_with(|| default.clone());
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DefinitionPaths {
    ontology: String,
    schemas: String,
    seeds: String,
    migrations: String,
}
impl DefinitionPaths {
    pub(super) fn entries(&self) -> [(DefinitionKind, &str); 4] {
        [
            (DefinitionKind::Migrations, &self.migrations),
            (DefinitionKind::Ontology, &self.ontology),
            (DefinitionKind::Schemas, &self.schemas),
            (DefinitionKind::Seeds, &self.seeds),
        ]
    }

    pub(super) fn paths(&self) -> impl Iterator<Item = &str> {
        [
            &*self.ontology,
            &*self.schemas,
            &*self.seeds,
            &*self.migrations,
        ]
        .into_iter()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Source {
    pub(super) id: String,
    uri: String,
    pub(super) sha256: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    media_type: Option<String>,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SecretReference {
    id: String,
    source: SecretSource,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum SecretSource {
    Environment,
    Pulumi,
    Terraform,
    SecretManager,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Artifact {
    kind: ArtifactKind,
    version: String,
    sha256: String,
}
#[derive(Debug, Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ArtifactKind {
    PythonWheel,
    NodePackage,
    NativeBinary,
    OciImage,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Target {
    kind: TargetKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    ownership: Option<TargetOwnership>,
    artifact: Artifact,
    #[serde(skip_serializing_if = "Option::is_none")]
    topology: Option<Topology>,
    #[serde(default)]
    capabilities: Vec<CapabilityRequirement>,
    write: WriteConfig,
    storage: StorageConfig,
    #[serde(skip_serializing_if = "Option::is_none")]
    resources: Option<Resources>,
    #[serde(skip_serializing_if = "Option::is_none")]
    network: Option<Network>,
    #[serde(skip_serializing_if = "Option::is_none")]
    health: Option<Health>,
    #[serde(skip_serializing_if = "Option::is_none")]
    observability: Option<Observability>,
    #[serde(skip_serializing_if = "Option::is_none")]
    backup: Option<Backup>,
    #[serde(default)]
    source_ids: Vec<String>,
    #[serde(default)]
    secret_ids: Vec<String>,
}
impl Target {
    fn validate_bounds(&self) -> Result<(), GfError> {
        if self.write.queue_capacity == Some(0)
            || self
                .write
                .queue_capacity
                .is_some_and(|value| value > 65_536)
            || self
                .write
                .max_rebase_attempts
                .is_some_and(|value| value > 64)
            || self.storage.capacity_bytes == Some(0)
            || self
                .storage
                .capacity_bytes
                .is_some_and(|value| value > MAX_PORTABLE_INTEGER)
            || self.resources.as_ref().is_some_and(|value| {
                value
                    .cpu_millis
                    .is_some_and(|amount| amount == 0 || amount > MAX_PORTABLE_INTEGER)
                    || value
                        .memory_bytes
                        .is_some_and(|amount| amount == 0 || amount > MAX_PORTABLE_INTEGER)
            })
            || self
                .health
                .as_ref()
                .is_some_and(|value| value.timeout_seconds == 0 || value.timeout_seconds > 300)
            || self
                .backup
                .as_ref()
                .and_then(|value| value.retention_count)
                .is_some_and(|value| value == 0 || value > 1024)
            || self
                .topology
                .as_ref()
                .and_then(|value| value.replicas)
                .is_some_and(|value| value == 0 || value > 1024)
        {
            return Err(validation("target value exceeds contract bounds"));
        }
        if let Some(class) = &self.storage.class {
            bounded(class, 1, 128, "storage class")?;
        }
        Ok(())
    }

    fn effective_ownership(&self) -> TargetOwnership {
        self.ownership.unwrap_or(match self.kind {
            TargetKind::Embedded => TargetOwnership::Embedded,
            _ => TargetOwnership::External,
        })
    }

    fn effective_topology(&self) -> ResolvedTopology {
        let topology = self.topology.as_ref();
        ResolvedTopology {
            execution: topology.and_then(|value| value.execution).unwrap_or(
                match (self.kind, self.artifact.kind) {
                    (TargetKind::Host, _) => ExecutionKind::Host,
                    (_, ArtifactKind::OciImage) => ExecutionKind::Container,
                    _ => ExecutionKind::Process,
                },
            ),
            scheduling: topology
                .and_then(|value| value.scheduling)
                .unwrap_or(match self.kind {
                    TargetKind::Job => SchedulingKind::OnDemand,
                    _ => SchedulingKind::LongRunning,
                }),
            replicas: topology.and_then(|value| value.replicas).unwrap_or(1),
        }
    }

    fn validate_semantics(&self) -> Result<(), GfError> {
        let ownership = self.effective_ownership();
        let topology = self.effective_topology();
        if (self.kind == TargetKind::Embedded) != (ownership == TargetOwnership::Embedded) {
            return Err(validation(
                "embedded ownership is valid only for an embedded target",
            ));
        }
        if self.kind == TargetKind::Embedded
            && (topology.execution != ExecutionKind::Process
                || topology.scheduling != SchedulingKind::LongRunning
                || topology.replicas != 1
                || self.storage.kind != StorageKind::Local
                || self
                    .network
                    .as_ref()
                    .and_then(|value| value.exposure)
                    .is_some_and(|value| value != Exposure::None))
        {
            return Err(validation(
                "embedded targets require one long-running process, local storage, and no network exposure",
            ));
        }
        if self.kind == TargetKind::Host && topology.execution != ExecutionKind::Host {
            return Err(validation("host targets require host execution"));
        }
        if self.kind != TargetKind::Host && topology.execution == ExecutionKind::Host {
            return Err(validation("host execution requires a host target"));
        }
        if (self.kind == TargetKind::Job) != (topology.scheduling == SchedulingKind::OnDemand) {
            return Err(validation(
                "job targets are on-demand and other targets are long-running",
            ));
        }
        if self.kind == TargetKind::Service
            && self.network.as_ref().and_then(|value| value.port).is_none()
        {
            return Err(validation("service targets require a network port"));
        }
        if self
            .network
            .as_ref()
            .and_then(|value| value.exposure)
            .is_some_and(|value| value == Exposure::Public)
            && !self
                .network
                .as_ref()
                .and_then(|value| value.tls_required)
                .unwrap_or(false)
        {
            return Err(validation("public targets require TLS"));
        }
        match self.write.mode {
            WriteMode::Single
                if self.write.queue_capacity.is_some()
                    || self.write.max_rebase_attempts.is_some() =>
            {
                return Err(validation(
                    "single_writer does not accept queue or rebase settings",
                ));
            }
            WriteMode::Queued if self.write.queue_capacity.is_none() => {
                return Err(validation("queued_writer requires queue_capacity"));
            }
            WriteMode::OptimisticMulti if self.write.max_rebase_attempts.is_none() => {
                return Err(validation(
                    "optimistic_multi_writer requires max_rebase_attempts",
                ));
            }
            _ => {}
        }
        if self
            .backup
            .as_ref()
            .and_then(|value| value.retention_count)
            .is_some()
            && !self
                .backup
                .as_ref()
                .and_then(|value| value.checkpoints)
                .unwrap_or(false)
        {
            return Err(validation("backup retention requires checkpoint backups"));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum TargetKind {
    Embedded,
    Service,
    Worker,
    Job,
    Host,
}

#[derive(Debug, Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum TargetOwnership {
    Embedded,
    Local,
    External,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CapabilityRequirement {
    id: String,
    version: u16,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Topology {
    #[serde(skip_serializing_if = "Option::is_none")]
    execution: Option<ExecutionKind>,
    #[serde(skip_serializing_if = "Option::is_none")]
    scheduling: Option<SchedulingKind>,
    #[serde(skip_serializing_if = "Option::is_none")]
    replicas: Option<u16>,
}

#[derive(Debug, Clone, Serialize)]
struct ResolvedTopology {
    execution: ExecutionKind,
    scheduling: SchedulingKind,
    replicas: u16,
}

#[derive(Debug, Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ExecutionKind {
    Process,
    Container,
    Host,
}

#[derive(Debug, Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum SchedulingKind {
    LongRunning,
    OnDemand,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WriteConfig {
    mode: WriteMode,
    #[serde(skip_serializing_if = "Option::is_none")]
    queue_capacity: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_rebase_attempts: Option<u8>,
}
#[derive(Debug, Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
enum WriteMode {
    #[serde(rename = "single_writer")]
    Single,
    #[serde(rename = "queued_writer")]
    Queued,
    #[serde(rename = "optimistic_multi_writer")]
    OptimisticMulti,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StorageConfig {
    kind: StorageKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    persistent: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    class: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    capacity_bytes: Option<u64>,
}
#[derive(Debug, Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum StorageKind {
    Local,
    Volume,
    Object,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Resources {
    #[serde(skip_serializing_if = "Option::is_none")]
    cpu_millis: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    memory_bytes: Option<u64>,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Network {
    #[serde(skip_serializing_if = "Option::is_none")]
    exposure: Option<Exposure>,
    #[serde(skip_serializing_if = "Option::is_none")]
    port: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tls_required: Option<bool>,
}
#[derive(Debug, Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Exposure {
    None,
    Private,
    Public,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Health {
    timeout_seconds: u16,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Observability {
    #[serde(skip_serializing_if = "Option::is_none")]
    logs: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    metrics: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    traces: Option<bool>,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Backup {
    #[serde(skip_serializing_if = "Option::is_none")]
    checkpoints: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    retention_count: Option<u16>,
}

impl RepositoryContext {
    /// Parse and semantically validate the closed project configuration.
    pub fn load_config(&self) -> Result<ProjectConfig, GfError> {
        let bytes = fs::read(&self.config_path).map_err(|error| {
            GfError::Storage(format!(
                "cannot read {}: {error}",
                self.config_path.display()
            ))
        })?;
        let config: ProjectConfig = serde_yaml::from_slice(&bytes)
            .map_err(|error| validation(format!("invalid graphforge.yaml: {error}")))?;
        config.validate(self)?;
        Ok(config)
    }

    /// Resolve the config to deterministic, secret-free JSON with explicit defaults.
    pub fn resolve_config(&self) -> Result<Value, GfError> {
        self.load_config()?.resolve()
    }

    /// Validate one declared infrastructure target without network access or mutation.
    ///
    /// This reads only the tracked project configuration. It never opens live
    /// GraphForge state, resolves secret values, reads definition contents, or
    /// attempts connectivity/readiness checks.
    pub fn validate_infra_target(&self, target_id: &str) -> Result<InfraValidationResult, GfError> {
        stable_id(target_id)?;
        let resolved = self.resolve_config()?;
        let encoded = serde_json::to_vec(&resolved)
            .map_err(|error| validation(format!("cannot encode resolved config: {error}")))?;
        let resolved_config_sha256 = encode_hex(&Sha256::digest(encoded));
        let target = resolved
            .get("targets")
            .and_then(Value::as_array)
            .and_then(|targets| {
                targets
                    .iter()
                    .find(|target| target.get("id").and_then(Value::as_str) == Some(target_id))
            })
            .cloned()
            .ok_or_else(|| validation(format!("unknown infrastructure target: {target_id}")))?;
        let target_object = target
            .as_object()
            .ok_or_else(|| validation("resolved target must be an object"))?;
        let text = |name: &str| {
            target_object
                .get(name)
                .and_then(Value::as_str)
                .ok_or_else(|| validation(format!("resolved target is missing {name}")))
        };
        let topology = target_object
            .get("topology")
            .and_then(Value::as_object)
            .ok_or_else(|| validation("resolved target is missing topology"))?;
        let topology_text = |name: &str| {
            topology
                .get(name)
                .and_then(Value::as_str)
                .ok_or_else(|| validation(format!("resolved topology is missing {name}")))
        };
        let replicas = topology
            .get("replicas")
            .and_then(Value::as_u64)
            .ok_or_else(|| validation("resolved topology is missing replicas"))?;
        let artifact = target_object
            .get("artifact")
            .cloned()
            .ok_or_else(|| validation("resolved target is missing artifact"))?;
        let requirements = target_object
            .get("capabilities")
            .and_then(Value::as_array)
            .cloned()
            .ok_or_else(|| validation("resolved target is missing capabilities"))?;
        let ownership = text("ownership")?.to_owned();
        let kind = text("kind")?.to_owned();
        let execution = topology_text("execution")?.to_owned();
        let scheduling = topology_text("scheduling")?.to_owned();
        Ok(InfraValidationResult {
            contract: "graphforge-infra-validation/1",
            resolved_config_sha256,
            target,
            static_validity: InfraStaticValidity { status: "valid" },
            planned_infrastructure: InfraPlan {
                status: "validated",
                mutation: "none",
                ownership,
                kind,
                execution,
                scheduling,
                replicas,
                artifact,
            },
            connectivity: InfraNotChecked {
                status: "not_checked",
            },
            readiness: InfraNotChecked {
                status: "not_checked",
            },
            capability_compatibility: InfraCapabilityCompatibility {
                status: "requirements_declared",
                requirements,
            },
        })
    }
}

#[cfg(test)]
mod tests;
