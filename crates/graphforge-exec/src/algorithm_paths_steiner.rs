//! Shared normalization for exact Steiner path algorithms.

use graphforge_core::PathsOptions;
use graphforge_core::algorithms::PathAlgorithm;

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};

/// Closed normalization contract for the two Steiner algorithms.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SteinerKind {
    MinimumTree,
    PrizeCollecting,
}

impl SteinerKind {
    const fn algorithm(self) -> PathAlgorithm {
        match self {
            Self::MinimumTree => PathAlgorithm::MinSteinerTree,
            Self::PrizeCollecting => PathAlgorithm::PrizeCollectingSteinerTree,
        }
    }

    const fn name(self) -> &'static str {
        self.algorithm().as_str()
    }

    const fn minimum_terminals(self) -> usize {
        match self {
            Self::MinimumTree => 2,
            Self::PrizeCollecting => 1,
        }
    }
}

/// Canonical mandatory terminals consumed by a later Steiner kernel.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct NormalizedSteinerInvocation {
    terminal_uuids: Vec<[u8; 16]>,
}

impl NormalizedSteinerInvocation {
    pub(crate) fn terminal_uuids(&self) -> &[[u8; 16]] {
        &self.terminal_uuids
    }
}

/// Normalize and validate one graph-layer Steiner invocation atomically.
pub(crate) fn normalize_steiner_invocation(
    kind: SteinerKind,
    source: Option<[u8; 16]>,
    target: Option<[u8; 16]>,
    options: &PathsOptions,
    selected_nodes: &[[u8; 16]],
    control: &AlgorithmControl,
) -> Result<NormalizedSteinerInvocation, AlgorithmError> {
    validate_closed_options(kind, source, target, options)?;
    control.check_cancelled()?;
    let raw_count =
        u64::try_from(options.terminal_uuids.len()).map_err(|_| AlgorithmError::StateOverflow)?;
    // Raw input is bounded by the shared exact-solver budget, but normalization
    // does not retain solver search states and therefore must not consume it.
    control.check_states(raw_count)?;
    terminal_payload_bytes(options.terminal_uuids.len())?;

    let mut terminal_uuids = Vec::new();
    terminal_uuids
        .try_reserve_exact(options.terminal_uuids.len())
        .map_err(|_| execution("Steiner terminal allocation exceeds available memory"))?;
    for &uuid in &options.terminal_uuids {
        control.check_cancelled()?;
        terminal_uuids.push(uuid);
    }
    control.check_cancelled()?;
    terminal_uuids.sort_unstable();
    terminal_uuids.dedup();
    control.check_cancelled()?;

    let required = kind.minimum_terminals();
    if terminal_uuids.len() < required {
        return Err(AlgorithmError::SteinerTerminalCardinality {
            algorithm: kind.name(),
            observed: terminal_uuids.len(),
            required,
        });
    }
    for &uuid in &terminal_uuids {
        control.check_cancelled()?;
        if !selected_nodes.contains(&uuid) {
            return Err(AlgorithmError::SteinerTerminalOutsideProjection { uuid });
        }
    }

    Ok(NormalizedSteinerInvocation { terminal_uuids })
}

fn validate_closed_options(
    kind: SteinerKind,
    source: Option<[u8; 16]>,
    target: Option<[u8; 16]>,
    options: &PathsOptions,
) -> Result<(), AlgorithmError> {
    let invalid = |option, reason| AlgorithmError::SteinerOption {
        algorithm: kind.name(),
        option,
        reason,
    };
    if options.by != kind.algorithm() {
        return Err(invalid("by", "does not match the selected Steiner kind"));
    }
    if source.is_some() || target.is_some() {
        return Err(invalid(
            "source/target",
            "positional endpoints are not accepted",
        ));
    }
    if options.directed {
        return Err(invalid("directed", "must be false"));
    }
    if options.k != 1 {
        return Err(invalid("k", "must retain the default value 1"));
    }
    for (name, supplied) in [
        ("capacity_property", options.capacity_property.is_some()),
        ("cost_property", options.cost_property.is_some()),
        ("heuristic", options.heuristic.is_some()),
        ("walk_length", options.walk_length.is_some()),
        ("seed", options.seed.is_some()),
    ] {
        if supplied {
            return Err(invalid(name, "belongs to another path algorithm"));
        }
    }
    match (kind, options.prize_property.is_some()) {
        (SteinerKind::MinimumTree, true) => Err(invalid(
            "prize_property",
            "belongs only to prize_collecting_steiner_tree",
        )),
        (SteinerKind::PrizeCollecting, false) => Err(invalid(
            "prize_property",
            "is required for prize_collecting_steiner_tree",
        )),
        _ => Ok(()),
    }
}

fn terminal_payload_bytes(terminal_count: usize) -> Result<usize, AlgorithmError> {
    terminal_count
        .checked_mul(std::mem::size_of::<[u8; 16]>())
        .ok_or_else(|| execution("Steiner terminal allocation size overflowed"))
}

fn execution(message: impl Into<String>) -> AlgorithmError {
    AlgorithmError::Execution {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algorithm_dispatch::{AlgorithmCancellation, AlgorithmLimits};
    use graphforge_core::algorithms::AlgorithmFieldType;

    fn uuid(value: u8) -> [u8; 16] {
        [value; 16]
    }

    fn options(kind: SteinerKind, terminals: &[u8]) -> PathsOptions {
        PathsOptions {
            by: kind.algorithm(),
            directed: false,
            terminal_uuids: terminals.iter().copied().map(uuid).collect(),
            prize_property: matches!(kind, SteinerKind::PrizeCollecting).then(|| "prize".into()),
            ..PathsOptions::default()
        }
    }

    fn control(states: u64) -> AlgorithmControl {
        AlgorithmControl::new(
            AlgorithmLimits {
                states,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        )
    }

    #[test]
    fn raw_terminals_are_fallibly_bounded_sorted_deduplicated_and_membership_checked() {
        let normalization_control = control(4);
        let normalized = normalize_steiner_invocation(
            SteinerKind::MinimumTree,
            None,
            None,
            &options(SteinerKind::MinimumTree, &[3, 1, 3, 2]),
            &[uuid(1), uuid(2), uuid(3)],
            &normalization_control,
        )
        .unwrap();
        assert_eq!(normalized.terminal_uuids(), &[uuid(1), uuid(2), uuid(3)]);
        assert_eq!(normalization_control.consume_states(4), Ok(4));

        assert!(matches!(
            normalize_steiner_invocation(
                SteinerKind::MinimumTree,
                None,
                None,
                &options(SteinerKind::MinimumTree, &[1, 2, 3]),
                &[uuid(1), uuid(2), uuid(3)],
                &control(2),
            ),
            Err(AlgorithmError::StateLimit {
                observed: 3,
                limit: 2
            })
        ));
        assert_eq!(terminal_payload_bytes(3), Ok(48));
        assert!(terminal_payload_bytes(usize::MAX).is_err());
    }

    #[test]
    fn cardinality_membership_and_cancellation_are_structured_and_atomic() {
        assert!(matches!(
            normalize_steiner_invocation(
                SteinerKind::MinimumTree,
                None,
                None,
                &options(SteinerKind::MinimumTree, &[1, 1]),
                &[uuid(1)],
                &control(2),
            ),
            Err(AlgorithmError::SteinerTerminalCardinality {
                observed: 1,
                required: 2,
                ..
            })
        ));
        assert!(matches!(
            normalize_steiner_invocation(
                SteinerKind::PrizeCollecting,
                None,
                None,
                &options(SteinerKind::PrizeCollecting, &[]),
                &[],
                &control(0),
            ),
            Err(AlgorithmError::SteinerTerminalCardinality {
                observed: 0,
                required: 1,
                ..
            })
        ));
        assert_eq!(
            normalize_steiner_invocation(
                SteinerKind::PrizeCollecting,
                None,
                None,
                &options(SteinerKind::PrizeCollecting, &[2]),
                &[uuid(1)],
                &control(1),
            ),
            Err(AlgorithmError::SteinerTerminalOutsideProjection { uuid: uuid(2) })
        );
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            normalize_steiner_invocation(
                SteinerKind::PrizeCollecting,
                None,
                None,
                &options(SteinerKind::PrizeCollecting, &[1]),
                &[uuid(1)],
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
            ),
            Err(AlgorithmError::Cancelled)
        );
    }

    #[test]
    fn closed_boundary_rejects_endpoints_direction_k_prize_and_cross_options() {
        let valid = options(SteinerKind::MinimumTree, &[1, 2]);
        let cases = [
            (Some(uuid(1)), None, valid.clone()),
            (None, Some(uuid(2)), valid.clone()),
            (
                None,
                None,
                PathsOptions {
                    directed: true,
                    ..valid.clone()
                },
            ),
            (
                None,
                None,
                PathsOptions {
                    k: 2,
                    ..valid.clone()
                },
            ),
            (
                None,
                None,
                PathsOptions {
                    prize_property: Some("prize".into()),
                    ..valid.clone()
                },
            ),
            (
                None,
                None,
                PathsOptions {
                    seed: Some(7),
                    ..valid.clone()
                },
            ),
            (
                None,
                None,
                PathsOptions {
                    capacity_property: Some("capacity".into()),
                    ..valid.clone()
                },
            ),
            (
                None,
                None,
                PathsOptions {
                    cost_property: Some("cost".into()),
                    ..valid.clone()
                },
            ),
            (
                None,
                None,
                PathsOptions {
                    heuristic: Some("estimate".into()),
                    ..valid.clone()
                },
            ),
            (
                None,
                None,
                PathsOptions {
                    walk_length: Some(4),
                    ..valid.clone()
                },
            ),
            (
                None,
                None,
                PathsOptions {
                    by: PathAlgorithm::PrizeCollectingSteinerTree,
                    ..valid.clone()
                },
            ),
        ];
        for (source, target, options) in cases {
            assert!(matches!(
                normalize_steiner_invocation(
                    SteinerKind::MinimumTree,
                    source,
                    target,
                    &options,
                    &[uuid(1), uuid(2)],
                    &control(2),
                ),
                Err(AlgorithmError::SteinerOption { .. })
            ));
        }
        let mut pcst = options(SteinerKind::PrizeCollecting, &[1]);
        pcst.prize_property = None;
        assert!(matches!(
            normalize_steiner_invocation(
                SteinerKind::PrizeCollecting,
                None,
                None,
                &pcst,
                &[uuid(1)],
                &control(1),
            ),
            Err(AlgorithmError::SteinerOption {
                option: "prize_property",
                ..
            })
        ));
    }

    #[test]
    fn schema_is_exact_non_null_single_tree_shape() {
        for kind in [SteinerKind::MinimumTree, SteinerKind::PrizeCollecting] {
            let schema =
                graphforge_core::algorithms::Algorithm::Paths(kind.algorithm()).result_schema();
            assert_eq!(
                schema
                    .fields
                    .iter()
                    .map(|field| (field.name, field.data_type, field.nullable))
                    .collect::<Vec<_>>(),
                [
                    ("edge_uuid", AlgorithmFieldType::Uuid, false),
                    ("source_uuid", AlgorithmFieldType::Uuid, false),
                    ("target_uuid", AlgorithmFieldType::Uuid, false),
                    ("weight", AlgorithmFieldType::Float64, false),
                ]
            );
            assert!(!schema.includes_node_properties);
            assert!(schema.fields.iter().all(|field| field.name != "tree_id"));
        }
    }
}
