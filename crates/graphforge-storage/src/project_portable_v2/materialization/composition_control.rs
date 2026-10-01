//! Private captures from the actual typed composition-control writers.
use super::{MaterializedCapture, Path, PortableV2Error, PortableV2ErrorCode, fs};
use crate::project_publication::{ProjectFileParticipant, ProjectParticipant};
use graphforge_core::hash_observation::ControlSha256;
use graphforge_filesystem::ObservedSync as _;
use sha2::Digest;
use std::io::Write as _;

pub(crate) struct CapturedCompositionControl {
    pub(crate) participant: ProjectFileParticipant,
    pub(crate) capture: MaterializedCapture,
}

pub(crate) fn persist_staged_composition(
    stage: &Path,
    staged: &crate::WorkspacePortableOntologyStaging,
    max_bytes: u64,
    allocation: Option<&crate::StorageAllocationOperation>,
) -> Result<CapturedCompositionControl, PortableV2Error> {
    let participant = staged
        .to_project_participant()
        .map_err(|_| failed("cannot encode composition candidate"))?;
    write_control(
        stage,
        "portable-ontology-staging.json",
        participant,
        max_bytes,
        allocation,
    )
}

pub(crate) fn persist_composition_authority(
    stage: &Path,
    composition: &crate::WorkspaceOntologyComposition,
    max_bytes: u64,
    allocation: Option<&crate::StorageAllocationOperation>,
) -> Result<CapturedCompositionControl, PortableV2Error> {
    let participant = composition
        .to_project_participant()
        .map_err(|_| failed("cannot encode composition authority"))?;
    write_control(
        stage,
        "ontology-composition-authority.json",
        participant,
        max_bytes,
        allocation,
    )
}

// Only the two typed encoders above can reach this private byte writer. Public
// metadata/digest tuples do not mint capture authority.
fn write_control(
    stage: &Path,
    name: &str,
    mut participant: ProjectParticipant,
    max_bytes: u64,
    allocation: Option<&crate::StorageAllocationOperation>,
) -> Result<CapturedCompositionControl, PortableV2Error> {
    let bytes = std::mem::take(&mut participant.bytes);
    if bytes.len() as u64 > max_bytes {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::LimitExceeded,
            "generated composition control exceeds manifest bound",
        ));
    }
    let digest = ControlSha256::digest(&bytes).into();
    let checksum = crate::corruption_checksum::checksum(&bytes);
    let source = stage.join(name);
    let mut output = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&source)
        .map_err(|_| failed("cannot stage composition control"))?;
    let identity = graphforge_filesystem::file_identity(&output)
        .map_err(|_| failed("cannot identify composition control writer"))?;
    let written = output
        .write_all(&bytes)
        .and_then(|()| output.observed_sync_all())
        .map_err(|_| failed("cannot write or sync composition control"));
    // Account a partial write even when it fails. Preserve the write error as
    // primary; only a fully completed writer can produce a private capture.
    let observed = allocation.map_or(Ok(()), |allocation| {
        allocation
            .replace_file_at(&source, &output)
            .map_err(|_| failed("cannot account composition control writer"))
    });
    drop(output);
    written?;
    observed?;
    let file = crate::project_portable_v2_export::open_source_no_follow(&source)?;
    if graphforge_filesystem::file_identity(&file)
        .map_err(|_| failed("cannot identify completed composition control"))?
        != identity
    {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::ConcurrentMutation,
            "composition control identity changed after writer close",
        ));
    }
    let capture = MaterializedCapture {
        identity,
        length: bytes.len() as u64,
        digest,
        checksum,
        allocated_bytes: graphforge_filesystem::file_space_usage(&file)
            .map_err(|_| failed("cannot inspect completed composition control allocation"))?
            .allocated_bytes,
    };
    if let Some(allocation) = allocation {
        allocation
            .replace_file_at(&source, &file)
            .map_err(|_| failed("cannot account completed composition control"))?;
    }
    drop(file);
    capture.authenticate(&source, None)?;
    capture.open_source(&source)?;
    Ok(CapturedCompositionControl {
        participant: ProjectFileParticipant {
            participant,
            source,
            byte_length: bytes.len() as u64,
            content_sha256: digest,
        },
        capture,
    })
}

fn failed(detail: &'static str) -> PortableV2Error {
    PortableV2Error::new(PortableV2ErrorCode::Io, detail)
}
