//! Query-owned reads of the existing persisted edge-property index.

use crate::GfError;
use graphforge_storage::AuthenticatedPropertyInventory;
use graphforge_storage::edge_property_index::{
    EdgeIdMap, PrepaidEdgePropertyLookup, lookup_edge_properties_admitted,
    lookup_edge_properties_by_id_admitted,
};
use graphforge_storage::read_memory::{PrepaidBatch, ReadAdmission};
use std::collections::BTreeSet;
use std::path::Path;

fn duplicate_owner() -> GfError {
    GfError::Project {
        code: graphforge_core::ProjectErrorCode::ProjectCorrupt,
        message: "edge properties have multiple authenticated owners".into(),
    }
}

pub(crate) fn indexed_edge_property_batches(
    inventory: &AuthenticatedPropertyInventory,
    dir: &Path,
    route: &str,
    targets: &BTreeSet<[u8; 16]>,
    property_names: &[String],
    owners: &mut BTreeSet<[u8; 16]>,
    admission: &dyn ReadAdmission,
) -> Result<Option<Vec<PrepaidBatch>>, GfError> {
    let rows = match lookup_edge_properties_admitted(
        dir,
        inventory,
        route,
        targets,
        property_names,
        admission,
    )? {
        PrepaidEdgePropertyLookup::Absent => return Ok(Some(Vec::new())),
        PrepaidEdgePropertyLookup::NotIndexed => return Ok(None),
        PrepaidEdgePropertyLookup::Indexed(rows) => rows,
    };
    let keys = rows
        .batch()
        .column_by_name("edge_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
        .unwrap();
    if keys
        .iter()
        .flatten()
        .any(|uuid| !owners.insert(uuid.try_into().expect("stored UUID width")))
    {
        return Err(duplicate_owner());
    }
    Ok(Some(vec![rows]))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn indexed_edge_property_batches_by_id(
    inventory: &AuthenticatedPropertyInventory,
    dir: &Path,
    route: &str,
    targets: &BTreeSet<u64>,
    property_names: &[String],
    map: Option<&EdgeIdMap>,
    owners: &mut BTreeSet<u64>,
    admission: &dyn ReadAdmission,
) -> Result<Option<Vec<PrepaidBatch>>, GfError> {
    let rows = match lookup_edge_properties_by_id_admitted(
        dir,
        inventory,
        route,
        targets,
        property_names,
        map,
        admission,
    )? {
        PrepaidEdgePropertyLookup::Absent => return Ok(Some(Vec::new())),
        PrepaidEdgePropertyLookup::NotIndexed => return Ok(None),
        PrepaidEdgePropertyLookup::Indexed(rows) => rows,
    };
    let keys = rows
        .batch()
        .column_by_name("edge_id")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .unwrap();
    if keys.values().iter().any(|id| !owners.insert(*id)) {
        return Err(duplicate_owner());
    }
    Ok(Some(vec![rows]))
}
