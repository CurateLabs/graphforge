//! Generated cross-domain knowledge schema inventory.

use std::collections::BTreeMap;

use arrow::datatypes::{DataType, Field, SchemaRef, TimeUnit};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const INVENTORY_FORMAT: &str = "graphforge-knowledge-schema-inventory/1";

#[derive(Debug)]
struct InventoryEntry {
    capability_id: &'static str,
    capability_version: u32,
    record_family: &'static str,
    record_version: u32,
    schema: SchemaRef,
    schema_fingerprint: [u8; 32],
    enum_registry_versions: &'static [(&'static str, u32)],
    sort_key: &'static [&'static str],
    fingerprint_domain: &'static str,
    owner: &'static str,
    implementation_issue: u64,
    max_rows: usize,
}

fn inventory_json() -> Vec<u8> {
    let mut entries = Vec::new();
    entries.extend(
        graphforge_provenance::schema_registry()
            .into_iter()
            .map(|entry| InventoryEntry {
                capability_id: entry.capability_id,
                capability_version: entry.capability_version,
                record_family: entry.record_family,
                record_version: entry.record_version,
                schema: entry.schema,
                schema_fingerprint: entry.schema_fingerprint,
                enum_registry_versions: entry.enum_registry_versions,
                sort_key: entry.sort_key,
                fingerprint_domain: entry.fingerprint_domain.as_str(),
                owner: entry.owner,
                implementation_issue: entry.implementation_issue,
                max_rows: entry.max_rows,
            }),
    );
    entries.extend(
        graphforge_knowledge::schema_registry()
            .into_iter()
            .filter(|entry| !matches!(entry.capability_id, "epistemic" | "valid_time"))
            .map(|entry| InventoryEntry {
                capability_id: entry.capability_id,
                capability_version: entry.capability_version,
                record_family: entry.record_family,
                record_version: entry.record_version,
                schema: entry.schema,
                schema_fingerprint: entry.schema_fingerprint,
                enum_registry_versions: entry.enum_registry_versions,
                sort_key: entry.sort_key,
                fingerprint_domain: entry.fingerprint_domain.as_str(),
                owner: entry.owner,
                implementation_issue: entry.implementation_issue,
                max_rows: entry.max_rows,
            }),
    );
    entries.sort_by_key(|entry| {
        (
            entry.capability_id,
            entry.record_family,
            entry.record_version,
        )
    });

    let records = entries.iter().map(entry_json).collect::<Vec<_>>();
    let document = json!({
        "inventory_format": INVENTORY_FORMAT,
        "container_contracts": container_contracts(),
        "records": records,
    });
    let mut bytes = serde_json::to_vec_pretty(&document).expect("inventory is JSON-serializable");
    bytes.push(b'\n');
    bytes
}

fn epistemic_inventory_json() -> Vec<u8> {
    let mut entries = graphforge_knowledge::schema_registry()
        .into_iter()
        .filter(|entry| matches!(entry.capability_id, "epistemic" | "valid_time"))
        .map(|entry| InventoryEntry {
            capability_id: entry.capability_id,
            capability_version: entry.capability_version,
            record_family: entry.record_family,
            record_version: entry.record_version,
            schema: entry.schema,
            schema_fingerprint: entry.schema_fingerprint,
            enum_registry_versions: entry.enum_registry_versions,
            sort_key: entry.sort_key,
            fingerprint_domain: entry.fingerprint_domain.as_str(),
            owner: entry.owner,
            implementation_issue: entry.implementation_issue,
            max_rows: entry.max_rows,
        })
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| (entry.record_family, entry.record_version));
    let document = json!({
        "inventory_format": "graphforge-epistemic-schema-inventory/1",
        "knowledge_baseline": "docs/reference/knowledge-schema-inventory.sha256",
        "records": entries.iter().map(entry_json).collect::<Vec<_>>(),
    });
    let mut bytes = serde_json::to_vec_pretty(&document).expect("inventory is JSON-serializable");
    bytes.push(b'\n');
    bytes
}

fn container_contracts() -> Value {
    json!([
        {
            "record_family": "project_format_marker",
            "owner": "graphforge-storage",
            "format": String::from_utf8_lossy(graphforge_storage::PROJECT_FORMAT_BYTES),
            "version": 1,
            "fields": [],
        },
        {
            "record_family": "current",
            "owner": "graphforge-storage",
            "format": "graphforge-project",
            "version": 1,
            "fields": [
                ["format", "Utf8", "required"],
                ["format_version", "UInt32", "required"],
                ["generation_uuid", "Uuid", "required"],
                ["generation_manifest_sha256", "Sha256Hex", "required"],
            ],
        },
        {
            "record_family": "generation_manifest",
            "owner": "graphforge-storage",
            "format": "graphforge-generation",
            "version": 1,
            "fields": [
                ["format", "Utf8", "required"],
                ["format_version", "UInt32", "required"],
                ["generation_uuid", "Uuid", "required"],
                ["parent_generation_uuid", "Uuid", "nullable"],
                ["transaction_uuid", "Uuid", "required"],
                ["capabilities", "CapabilityDescriptor[]", "required"],
                ["participants", "ParticipantDescriptor[]", "required"],
            ],
        },
        {
            "record_family": "transaction_journal",
            "owner": "graphforge-storage",
            "format": "graphforge-transaction",
            "version": 1,
            "fields": [
                ["format", "Utf8", "required"],
                ["format_version", "UInt32", "required"],
                ["transaction_uuid", "Uuid", "required"],
                ["generation_uuid", "Uuid", "required"],
                ["parent_generation_uuid", "Uuid", "nullable"],
                ["phase", "JournalPhase", "required"],
                ["request_fingerprint", "Sha256Hex", "required"],
                ["participant_paths", "Utf8[]", "required"],
                ["generation_manifest_sha256", "Sha256Hex", "nullable"],
            ],
        },
    ])
}

fn entry_json(entry: &InventoryEntry) -> Value {
    let enums = entry
        .enum_registry_versions
        .iter()
        .map(|(name, version)| ((*name).to_owned(), Value::from(*version)))
        .collect::<BTreeMap<_, _>>();
    json!({
        "capability_id": entry.capability_id,
        "capability_version": entry.capability_version,
        "record_family": entry.record_family,
        "record_version": entry.record_version,
        "owner": entry.owner,
        "implementation_issue": entry.implementation_issue,
        "max_rows": entry.max_rows,
        "fingerprint_domain": entry.fingerprint_domain,
        "schema_fingerprint_sha256": encode_hex(&entry.schema_fingerprint),
        "sort_key": entry.sort_key,
        "enum_registry_versions": enums,
        "schema_metadata": sorted_metadata(entry.schema.metadata()),
        "fields": entry.schema.fields().iter().map(|field| field_json(field)).collect::<Vec<_>>(),
    })
}

fn field_json(field: &Field) -> Value {
    json!({
        "name": field.name(),
        "data_type": data_type_name(field.data_type()),
        "nullable": field.is_nullable(),
        "metadata": sorted_metadata(field.metadata()),
    })
}

fn sorted_metadata(metadata: &std::collections::HashMap<String, String>) -> BTreeMap<&str, &str> {
    metadata
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect()
}

fn data_type_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Utf8 => "Utf8".to_owned(),
        DataType::Binary => "Binary".to_owned(),
        DataType::UInt32 => "UInt32".to_owned(),
        DataType::Float64 => "Float64".to_owned(),
        DataType::FixedSizeBinary(width) => format!("FixedSizeBinary({width})"),
        DataType::List(item) => format!("List({})", data_type_name(item.data_type())),
        DataType::Timestamp(unit, timezone) => {
            let unit = match unit {
                TimeUnit::Second => "Second",
                TimeUnit::Millisecond => "Millisecond",
                TimeUnit::Microsecond => "Microsecond",
                TimeUnit::Nanosecond => "Nanosecond",
            };
            let timezone = timezone.as_deref().unwrap_or("");
            format!("Timestamp({unit},{timezone})")
        }
        other => panic!("unclassified knowledge inventory data type: {other:?}"),
    }
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn inventory_sha256(bytes: &[u8]) -> String {
    encode_hex(&Sha256::digest(bytes))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use super::{epistemic_inventory_json, inventory_json, inventory_sha256};

    fn repository_path(relative: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(relative)
    }

    #[test]
    fn knowledge_schema_inventory_matches_checked_contract() {
        let inventory = inventory_json();
        let digest = format!(
            "{}  knowledge-schema-inventory.json\n",
            inventory_sha256(&inventory)
        );
        let inventory_path = repository_path("docs/reference/knowledge-schema-inventory.json");
        let digest_path = repository_path("docs/reference/knowledge-schema-inventory.sha256");

        if std::env::var_os("UPDATE_KNOWLEDGE_SCHEMA_INVENTORY").is_some() {
            fs::write(&inventory_path, &inventory).expect("write generated knowledge inventory");
            fs::write(&digest_path, &digest).expect("write generated knowledge inventory digest");
        }

        assert_eq!(
            fs::read(&inventory_path).expect("checked knowledge inventory"),
            inventory,
            "knowledge schema registry drifted; review it, then run \
             UPDATE_KNOWLEDGE_SCHEMA_INVENTORY=1 cargo test -p graphforge-api \
             knowledge_schema_inventory_matches_checked_contract"
        );
        assert_eq!(
            fs::read_to_string(&digest_path).expect("checked knowledge inventory digest"),
            digest,
            "knowledge schema inventory digest is stale"
        );
    }

    #[test]
    fn epistemic_schema_inventory_matches_checked_contract() {
        let inventory = epistemic_inventory_json();
        let digest = format!(
            "{}  epistemic-schema-inventory.json\n",
            inventory_sha256(&inventory)
        );
        let inventory_path = repository_path("docs/reference/epistemic-schema-inventory.json");
        let digest_path = repository_path("docs/reference/epistemic-schema-inventory.sha256");

        if std::env::var_os("UPDATE_EPISTEMIC_SCHEMA_INVENTORY").is_some() {
            fs::write(&inventory_path, &inventory).expect("write generated epistemic inventory");
            fs::write(&digest_path, &digest).expect("write generated epistemic inventory digest");
        }

        assert_eq!(
            fs::read(&inventory_path).expect("checked epistemic inventory"),
            inventory,
            "epistemic schema registry drifted; review it, then run \
             UPDATE_EPISTEMIC_SCHEMA_INVENTORY=1 cargo test -p graphforge-api \
             epistemic_schema_inventory_matches_checked_contract"
        );
        assert_eq!(
            fs::read_to_string(&digest_path).expect("checked epistemic inventory digest"),
            digest,
            "epistemic schema inventory digest is stale"
        );
    }
}
