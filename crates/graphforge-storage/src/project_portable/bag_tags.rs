//! The authenticated BagIt tags; another GraphForge producer revision is incompatible, not tampered.
use super::{BTreeMap, ControlSha256, Digest, Entry, PortableError, PortableErrorCode};

/// The `Bag-Software-Agent` value of a `bag-info.txt`: the remainder of its first line.
fn bag_agent(bytes: &[u8]) -> Option<&[u8]> {
    let line = bytes.split(|byte| *byte == b'\n').next()?;
    line.strip_prefix(b"Bag-Software-Agent: ")
}

/// Another GraphForge producer revision wrote the package when its agent is exactly the
/// current agent name followed by `-<token>` (non-empty, no whitespace): refuse it as
/// incompatible before the exact tag comparison can call it tampered. Any other
/// deviation, including stray whitespace or a carriage return, is left to that comparison.
pub(super) fn refuse_other_producer_revision(
    map: &BTreeMap<&str, &Entry>,
    path: &str,
    current: &[u8],
) -> Result<(), PortableError> {
    let Some(current) = bag_agent(current) else {
        return Ok(());
    };
    let Some(agent) = map
        .get(path)
        .and_then(|entry| entry.bytes.as_deref())
        .and_then(bag_agent)
    else {
        return Ok(());
    };
    let revision = agent
        .strip_prefix(current)
        .and_then(|rest| rest.strip_prefix(b"-"));
    if revision.is_some_and(|token| !token.is_empty() && !token.iter().any(u8::is_ascii_whitespace))
    {
        return Err(PortableError::at(
            PortableErrorCode::Incompatible,
            path,
            "package was written by a different GraphForge producer revision",
        ));
    }
    Ok(())
}

pub(super) fn require_bag_tags(
    map: &BTreeMap<&str, &Entry>,
    bagit: &[u8],
    bag_info: &[u8],
) -> Result<(), PortableError> {
    require_exact(map, "bagit.txt", bagit)?;
    refuse_other_producer_revision(map, "bag-info.txt", bag_info)?;
    require_exact(map, "bag-info.txt", bag_info)
}

fn require_exact(
    map: &BTreeMap<&str, &Entry>,
    path: &str,
    expected: &[u8],
) -> Result<(), PortableError> {
    let e = map.get(path).ok_or_else(|| {
        PortableError::at(PortableErrorCode::InvalidStructure, path, "missing tag")
    })?;
    if e.length != expected.len() as u64 || e.digest != ControlSha256::digest(expected)[..] {
        return Err(PortableError::at(
            PortableErrorCode::DigestMismatch,
            path,
            "tag bytes",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::tests::package;
    use super::super::*;

    #[test]
    fn package_from_another_producer_revision_is_incompatible_not_tampered() {
        let root = package();
        let current = String::from_utf8(BAG_INFO.to_vec()).unwrap();
        let older = current.replace(
            "GraphForge portable\n",
            &format!("GraphForge portable-{}\n", "v2"),
        );
        let unrelated = current.replace("GraphForge portable\n", "GraphForge portable-x\n");
        for tag in [older, unrelated] {
            for mode in [PortableMode::Full, PortableMode::StructureOnly] {
                fs::write(root.path().join("bag-info.txt"), &tag).unwrap();
                let error = verify_portable(root.path(), mode, PortableLimits::default(), None)
                    .unwrap_err();
                assert_eq!(error.code, PortableErrorCode::Incompatible, "{tag:?}");
            }
        }
        // Any other deviation from the current tag stays a digest mismatch.
        for tag in [
            "Bag-Software-Agent: GraphForge portable\r\nBagging-Date: 1970-01-01\n",
            "Bag-Software-Agent: GraphForge portable \nBagging-Date: 1970-01-01\n",
            "Bag-Software-Agent: GraphForge portable-x \nBagging-Date: 1970-01-01\n",
            "Bag-Software-Agent: GraphForge portable-\nBagging-Date: 1970-01-01\n",
            "Bag-Software-Agent: GraphForge portable-x\r\nBagging-Date: 1970-01-01\n",
            "Bag-Software-Agent: GraphForge portables\nBagging-Date: 1970-01-01\n",
            "Bag-Software-Agent: other\nBagging-Date: 1970-01-01\n",
            "Bag-Software-Agent: GraphForge portable\nBagging-Date: 1971-01-01\n",
        ] {
            fs::write(root.path().join("bag-info.txt"), tag).unwrap();
            let error = verify_portable(
                root.path(),
                PortableMode::StructureOnly,
                PortableLimits::default(),
                None,
            )
            .unwrap_err();
            assert_eq!(error.code, PortableErrorCode::DigestMismatch, "{tag:?}");
        }
    }
}
