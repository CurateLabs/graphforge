//! Authored IDs retain their domain; GraphForge IDs use the shared grammar.

use graphforge_discovery::{ItemId, ItemKind};

pub(crate) fn validate_item_identity(value: &str, kind: ItemKind) -> Result<(), &'static str> {
    if value.trim().is_empty() {
        return Err(match kind {
            ItemKind::OntologyModule => "missing ontology_id; run ontology module create first",
            ItemKind::BridgeSet => "missing bridge_id; run ontology bridge create first",
        });
    }
    if let Ok(url) = url::Url::parse(value)
        && url.scheme() == "https"
        && url.host_str() == Some("graphforge.sh")
    {
        let canonical = ItemId::parse_kind(value, kind).is_ok_and(|id| id.to_uri() == value);
        if !canonical {
            return Err(match kind {
                ItemKind::OntologyModule => {
                    "ontology_id must be a canonical lowercase https://graphforge.sh/ont_ URI"
                }
                ItemKind::BridgeSet => {
                    "bridge_id must be a canonical lowercase https://graphforge.sh/brg_ URI"
                }
            });
        }
    }
    Ok(())
}
