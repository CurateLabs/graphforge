//! Checked aggregate resource controls shared by embedding-v1 kernels.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};

pub(crate) const DEFAULT_EMBEDDING_MEMORY_BYTES: u64 = 1024 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct EmbeddingResourceLimits {
    pub(crate) memory_bytes: u64,
    pub(crate) work: u64,
}

impl Default for EmbeddingResourceLimits {
    fn default() -> Self {
        Self {
            memory_bytes: DEFAULT_EMBEDDING_MEMORY_BYTES,
            work: u64::MAX,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct EmbeddingResourceEstimate {
    pub(crate) topology_bytes: u64,
    pub(crate) output_bytes: u64,
    pub(crate) working_bytes: u64,
    pub(crate) optimizer_bytes: u64,
    pub(crate) scratch_bytes: u64,
    pub(crate) work: u64,
}

impl EmbeddingResourceEstimate {
    pub(crate) fn memory_bytes(self) -> Result<u64, EmbeddingResourceError> {
        [
            self.topology_bytes,
            self.output_bytes,
            self.working_bytes,
            self.optimizer_bytes,
            self.scratch_bytes,
        ]
        .into_iter()
        .try_fold(0_u64, |total, bytes| {
            total
                .checked_add(bytes)
                .ok_or(EmbeddingResourceError::Overflow)
        })
    }

    pub(crate) fn node2vec(input: Node2VecResources) -> Result<Self, EmbeddingResourceError> {
        let topology_bytes = input.topology.bytes()?;
        let output_bytes = product(&[input.topology.nodes, input.dimensions, 4])?;
        let working_bytes = product(&[input.topology.nodes, input.dimensions, 4, 2])?;
        let walk_tokens = input.walk_length.checked_add(1).ok_or(overflow())?;
        let optimizer_bytes = sum(&[
            product(&[input.topology.nodes, 8])?,
            product(&[walk_tokens, 8])?,
        ])?;
        let walks = product(&[input.topology.nodes, input.walks_per_node])?;
        let transition_work = product(&[
            walks,
            input.walk_length,
            input.epochs.checked_add(1).ok_or(overflow())?,
        ])?;
        let corpus_tokens = product(&[walks, walk_tokens])?;
        let contexts = product(&[
            corpus_tokens,
            input.window_size.checked_mul(2).ok_or(overflow())?,
        ])?;
        let samples = product(&[
            contexts,
            input.negative_samples.checked_add(1).ok_or(overflow())?,
        ])?;
        Ok(Self {
            topology_bytes,
            output_bytes,
            working_bytes,
            optimizer_bytes,
            scratch_bytes: input.scratch_bytes,
            work: sum(&[transition_work, contexts, samples, input.topology.nodes])?,
        })
    }

    pub(crate) fn graphsage(input: GraphSageResources<'_>) -> Result<Self, EmbeddingResourceError> {
        if input.sample_sizes.len() != input.layer_widths.len() {
            return Err(EmbeddingResourceError::InvalidShape);
        }
        let topology_bytes = input.topology.bytes()?;
        let output_bytes = product(&[input.topology.nodes, input.dimensions, 4])?;
        let features = product(&[input.topology.nodes, input.feature_width, 8])?;
        let mut sampled_nodes = 1_u64;
        let mut fanout_product = 1_u64;
        for fanout in input.sample_sizes {
            fanout_product = fanout_product.checked_mul(*fanout).ok_or(overflow())?;
            sampled_nodes = sampled_nodes
                .checked_add(fanout_product)
                .ok_or(overflow())?;
        }
        let working_bytes = sum(&[
            features,
            product(&[sampled_nodes, 8])?,
            product(&[input.topology.nodes, input.dimensions, 8])?,
        ])?;
        let mut prior_width = input.feature_width;
        let mut parameter_coordinates = 0_u64;
        let mut prior_width_sum = 0_u64;
        let mut inference_coordinates = 0_u64;
        for width in input.layer_widths {
            prior_width_sum = prior_width_sum.checked_add(prior_width).ok_or(overflow())?;
            let fan_in = prior_width.checked_mul(2).ok_or(overflow())?;
            parameter_coordinates = parameter_coordinates
                .checked_add(product(&[*width, fan_in])?)
                .ok_or(overflow())?;
            inference_coordinates = inference_coordinates
                .checked_add(product(&[
                    *width,
                    fan_in.checked_add(1).ok_or(overflow())?,
                ])?)
                .ok_or(overflow())?;
            prior_width = *width;
        }
        let optimizer_bytes = product(&[parameter_coordinates, 8, 4])?;
        let max_pairs = product(&[input.topology.nodes, 50, 5])?;
        let roots = product(&[
            input.epochs,
            max_pairs,
            input.negative_samples.checked_add(2).ok_or(overflow())?,
        ])?;
        let inference_work = sum(&[
            product(&[input.topology.adjacency_entries, prior_width_sum])?,
            product(&[input.topology.nodes, inference_coordinates])?,
        ])?;
        let work = sum(&[
            max_pairs,
            product(&[roots, sampled_nodes])?,
            product(&[input.epochs, max_pairs, parameter_coordinates])?,
            inference_work,
        ])?;
        Ok(Self {
            topology_bytes,
            output_bytes,
            working_bytes,
            optimizer_bytes,
            scratch_bytes: input.scratch_bytes,
            work,
        })
    }

    pub(crate) fn fastrp(input: FastRpResources) -> Result<Self, EmbeddingResourceError> {
        let topology_bytes = input.topology.bytes()?;
        let output_bytes = product(&[input.topology.nodes, input.dimensions, 4])?;
        let matrix_bytes = product(&[input.topology.nodes, input.dimensions, 8])?;
        let working_bytes = sum(&[
            product(&[matrix_bytes, 3])?,
            product(&[input.topology.nodes, 8])?,
        ])?;
        let optimizer_bytes = product(&[input.properties, input.dimensions, 8])?;
        let propagated_iterations = input.iteration_weights.saturating_sub(1);
        let work = sum(&[
            product(&[
                input.topology.adjacency_entries,
                propagated_iterations,
                input.dimensions,
            ])?,
            product(&[input.topology.nodes, input.dimensions])?,
            product(&[
                input.topology.nodes,
                input.iteration_weights,
                input.dimensions,
            ])?,
            product(&[input.properties, input.dimensions])?,
        ])?;
        Ok(Self {
            topology_bytes,
            output_bytes,
            working_bytes,
            optimizer_bytes,
            scratch_bytes: input.scratch_bytes,
            work,
        })
    }

    pub(crate) fn hashgnn(input: HashGnnResources) -> Result<Self, EmbeddingResourceError> {
        let topology_bytes = input.topology.bytes()?;
        let output_bytes = product(&[input.topology.nodes, input.dimensions, 4])?;
        let packed_words = input.dimensions.checked_add(63).ok_or(overflow())? / 64;
        let working_bytes = product(&[input.topology.nodes, packed_words, 8, 3])?;
        let work = product(&[
            input.iterations,
            input.active_bits,
            input.active_bits,
            input
                .topology
                .nodes
                .checked_add(input.topology.adjacency_entries)
                .ok_or(overflow())?,
        ])?;
        Ok(Self {
            topology_bytes,
            output_bytes,
            working_bytes,
            optimizer_bytes: 0,
            scratch_bytes: input.scratch_bytes,
            work,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TopologyResources {
    pub(crate) nodes: u64,
    pub(crate) adjacency_entries: u64,
    pub(crate) bytes_per_node: u64,
    pub(crate) bytes_per_adjacency_entry: u64,
}

impl TopologyResources {
    fn bytes(self) -> Result<u64, EmbeddingResourceError> {
        sum(&[
            product(&[self.nodes, self.bytes_per_node])?,
            product(&[self.adjacency_entries, self.bytes_per_adjacency_entry])?,
        ])
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Node2VecResources {
    pub(crate) topology: TopologyResources,
    pub(crate) dimensions: u64,
    pub(crate) walks_per_node: u64,
    pub(crate) walk_length: u64,
    pub(crate) window_size: u64,
    pub(crate) negative_samples: u64,
    pub(crate) epochs: u64,
    pub(crate) scratch_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct GraphSageResources<'a> {
    pub(crate) topology: TopologyResources,
    pub(crate) dimensions: u64,
    pub(crate) feature_width: u64,
    pub(crate) sample_sizes: &'a [u64],
    pub(crate) layer_widths: &'a [u64],
    pub(crate) epochs: u64,
    pub(crate) negative_samples: u64,
    pub(crate) scratch_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FastRpResources {
    pub(crate) topology: TopologyResources,
    pub(crate) dimensions: u64,
    pub(crate) iteration_weights: u64,
    pub(crate) properties: u64,
    pub(crate) scratch_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct HashGnnResources {
    pub(crate) topology: TopologyResources,
    pub(crate) dimensions: u64,
    pub(crate) iterations: u64,
    pub(crate) active_bits: u64,
    pub(crate) scratch_bytes: u64,
}

fn product(factors: &[u64]) -> Result<u64, EmbeddingResourceError> {
    factors.iter().try_fold(1_u64, |value, factor| {
        value.checked_mul(*factor).ok_or(overflow())
    })
}

fn sum(terms: &[u64]) -> Result<u64, EmbeddingResourceError> {
    terms.iter().try_fold(0_u64, |value, term| {
        value.checked_add(*term).ok_or(overflow())
    })
}

const fn overflow() -> EmbeddingResourceError {
    EmbeddingResourceError::Overflow
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum EmbeddingResourceError {
    #[error("embedding resource accounting exceeds UInt64 range")]
    Overflow,
    #[error("embedding resource shape is inconsistent")]
    InvalidShape,
    #[error("embedding memory limit exceeded: observed {observed}, limit {limit}")]
    MemoryLimit { observed: u64, limit: u64 },
    #[error("embedding work limit exceeded: observed {observed}, limit {limit}")]
    WorkLimit { observed: u64, limit: u64 },
    #[error(transparent)]
    Algorithm(#[from] AlgorithmError),
}

pub(crate) struct EmbeddingControl<'a> {
    algorithm: &'a AlgorithmControl,
    limits: EmbeddingResourceLimits,
    work: AtomicU64,
}

impl<'a> EmbeddingControl<'a> {
    pub(crate) fn new(algorithm: &'a AlgorithmControl, limits: EmbeddingResourceLimits) -> Self {
        Self {
            algorithm,
            limits,
            work: AtomicU64::new(0),
        }
    }

    /// Validate the complete invocation estimate before any allocation or output mutation.
    pub(crate) fn preflight(
        &self,
        estimate: EmbeddingResourceEstimate,
    ) -> Result<(), EmbeddingResourceError> {
        self.algorithm.check_cancelled()?;
        let observed = estimate.memory_bytes()?;
        if observed > self.limits.memory_bytes {
            return Err(EmbeddingResourceError::MemoryLimit {
                observed,
                limit: self.limits.memory_bytes,
            });
        }
        if estimate.work > self.limits.work {
            return Err(EmbeddingResourceError::WorkLimit {
                observed: estimate.work,
                limit: self.limits.work,
            });
        }
        Ok(())
    }

    /// Check cancellation and atomically charge deterministic aggregate work.
    pub(crate) fn checkpoint(&self, charge: u64) -> Result<u64, EmbeddingResourceError> {
        self.algorithm.check_cancelled()?;
        let updated = self
            .work
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current
                    .checked_add(charge)
                    .filter(|next| *next <= self.limits.work)
            });
        match updated {
            Ok(previous) => Ok(previous + charge),
            Err(previous) => match previous.checked_add(charge) {
                Some(observed) => Err(EmbeddingResourceError::WorkLimit {
                    observed,
                    limit: self.limits.work,
                }),
                None => Err(EmbeddingResourceError::Overflow),
            },
        }
    }

    /// Consume one algorithm-level iteration while preserving the separate work budget.
    pub(crate) fn iteration_checkpoint(&self) -> Result<u64, EmbeddingResourceError> {
        self.algorithm.checkpoint().map_err(Into::into)
    }

    pub(crate) fn before_publish(&self) -> Result<(), EmbeddingResourceError> {
        self.algorithm.check_cancelled()?;
        Ok(())
    }

    /// Declared compute-thread budget for parallel embedding kernels (#344).
    pub(crate) fn compute_threads(&self) -> usize {
        self.algorithm.compute_threads()
    }

    /// Borrow the instance-owned private compute pool when attached (#344).
    pub(crate) fn compute_pool(&self) -> Option<&crate::ComputePool> {
        self.algorithm.compute_pool()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algorithm_dispatch::{AlgorithmCancellation, AlgorithmLimits};

    fn algorithm(cancellation: AlgorithmCancellation) -> AlgorithmControl {
        AlgorithmControl::new(AlgorithmLimits::default(), cancellation)
    }

    fn topology(nodes: u64, adjacency_entries: u64) -> TopologyResources {
        TopologyResources {
            nodes,
            adjacency_entries,
            bytes_per_node: 16,
            bytes_per_adjacency_entry: 32,
        }
    }

    #[test]
    fn default_is_one_gib_and_all_components_are_aggregate() {
        assert_eq!(EmbeddingResourceLimits::default().memory_bytes, 1 << 30);
        let estimate = EmbeddingResourceEstimate::node2vec(Node2VecResources {
            topology: topology(2, 3),
            dimensions: 4,
            walks_per_node: 2,
            walk_length: 3,
            window_size: 2,
            negative_samples: 1,
            epochs: 1,
            scratch_bytes: 5,
        })
        .unwrap();
        assert_eq!(estimate.topology_bytes, 128);
        assert_eq!(estimate.memory_bytes(), Ok(277));
    }

    #[test]
    fn preflight_rejects_overflow_and_limits_atomically() {
        let algorithm = algorithm(AlgorithmCancellation::default());
        let control = EmbeddingControl::new(
            &algorithm,
            EmbeddingResourceLimits {
                memory_bytes: 9,
                work: 7,
            },
        );
        let estimate = EmbeddingResourceEstimate::fastrp(FastRpResources {
            topology: topology(1, 0),
            dimensions: 1,
            iteration_weights: 1,
            properties: 0,
            scratch_bytes: 0,
        })
        .unwrap();
        assert!(matches!(
            control.preflight(estimate),
            Err(EmbeddingResourceError::MemoryLimit { .. })
        ));
        let estimate = EmbeddingResourceEstimate::hashgnn(HashGnnResources {
            topology: topology(1, 0),
            dimensions: 1,
            iterations: 8,
            active_bits: 1,
            scratch_bytes: 0,
        })
        .unwrap();
        let control = EmbeddingControl::new(
            &algorithm,
            EmbeddingResourceLimits {
                memory_bytes: u64::MAX,
                work: 7,
            },
        );
        assert_eq!(
            control.preflight(estimate),
            Err(EmbeddingResourceError::WorkLimit {
                observed: 8,
                limit: 7
            })
        );
        assert_eq!(
            EmbeddingResourceEstimate {
                topology_bytes: u64::MAX,
                output_bytes: 1,
                ..Default::default()
            }
            .memory_bytes(),
            Err(EmbeddingResourceError::Overflow)
        );
    }

    #[test]
    fn work_checkpoints_are_non_resetting_deterministic_and_bounded() {
        let first_algorithm = algorithm(AlgorithmCancellation::default());
        let replay_algorithm = algorithm(AlgorithmCancellation::default());
        let limits = EmbeddingResourceLimits {
            memory_bytes: u64::MAX,
            work: 3,
        };
        let control = EmbeddingControl::new(&first_algorithm, limits);
        let replay = EmbeddingControl::new(&replay_algorithm, limits);
        for charge in [1, 2] {
            assert_eq!(control.checkpoint(charge), replay.checkpoint(charge));
        }
        assert_eq!(
            control.checkpoint(1),
            Err(EmbeddingResourceError::WorkLimit {
                observed: 4,
                limit: 3
            })
        );
        assert_eq!(control.checkpoint(0), Ok(3));
    }

    #[test]
    fn cancellation_is_checked_before_preflight_work_and_publication() {
        let cancellation = AlgorithmCancellation::default();
        let algorithm = algorithm(cancellation.clone());
        let control = EmbeddingControl::new(&algorithm, EmbeddingResourceLimits::default());
        cancellation.cancel();
        let cancelled = Err(EmbeddingResourceError::Algorithm(AlgorithmError::Cancelled));
        assert_eq!(
            control.preflight(EmbeddingResourceEstimate::default()),
            cancelled
        );
        assert_eq!(
            control.checkpoint(1),
            Err(EmbeddingResourceError::Algorithm(AlgorithmError::Cancelled))
        );
        assert_eq!(
            control.before_publish(),
            Err(EmbeddingResourceError::Algorithm(AlgorithmError::Cancelled))
        );
    }

    #[test]
    fn every_algorithm_estimator_computes_from_primitive_counts() {
        let node2vec = EmbeddingResourceEstimate::node2vec(Node2VecResources {
            topology: topology(2, 3),
            dimensions: 4,
            walks_per_node: 2,
            walk_length: 3,
            window_size: 2,
            negative_samples: 1,
            epochs: 1,
            scratch_bytes: 0,
        })
        .unwrap();
        let graphsage = EmbeddingResourceEstimate::graphsage(GraphSageResources {
            topology: topology(2, 3),
            dimensions: 4,
            feature_width: 3,
            sample_sizes: &[2, 1],
            layer_widths: &[5, 4],
            epochs: 1,
            negative_samples: 2,
            scratch_bytes: 0,
        })
        .unwrap();
        let fastrp = EmbeddingResourceEstimate::fastrp(FastRpResources {
            topology: topology(2, 3),
            dimensions: 4,
            iteration_weights: 3,
            properties: 2,
            scratch_bytes: 0,
        })
        .unwrap();
        let hashgnn = EmbeddingResourceEstimate::hashgnn(HashGnnResources {
            topology: topology(2, 3),
            dimensions: 65,
            iterations: 2,
            active_bits: 4,
            scratch_bytes: 0,
        })
        .unwrap();
        assert_eq!(node2vec.work, 218);
        assert_eq!(graphsage.work, 45_682);
        assert_eq!(fastrp.work, 64);
        assert_eq!(hashgnn.work, 160);
        assert_eq!(hashgnn.working_bytes, 96);
    }

    #[test]
    fn every_algorithm_estimator_rejects_internal_multiplication_overflow() {
        assert_eq!(
            EmbeddingResourceEstimate::node2vec(Node2VecResources {
                topology: topology(1, 0),
                dimensions: u64::MAX,
                walks_per_node: 1,
                walk_length: 1,
                window_size: 1,
                negative_samples: 1,
                epochs: 1,
                scratch_bytes: 0,
            }),
            Err(EmbeddingResourceError::Overflow)
        );
        assert_eq!(
            EmbeddingResourceEstimate::graphsage(GraphSageResources {
                topology: topology(1, 0),
                dimensions: 1,
                feature_width: 1,
                sample_sizes: &[u64::MAX, 2],
                layer_widths: &[1, 1],
                epochs: 1,
                negative_samples: 1,
                scratch_bytes: 0,
            }),
            Err(EmbeddingResourceError::Overflow)
        );
        assert_eq!(
            EmbeddingResourceEstimate::fastrp(FastRpResources {
                topology: topology(u64::MAX, 0),
                dimensions: 2,
                iteration_weights: 1,
                properties: 0,
                scratch_bytes: 0,
            }),
            Err(EmbeddingResourceError::Overflow)
        );
        assert_eq!(
            EmbeddingResourceEstimate::hashgnn(HashGnnResources {
                topology: topology(1, 0),
                dimensions: 1,
                iterations: u64::MAX,
                active_bits: 2,
                scratch_bytes: 0,
            }),
            Err(EmbeddingResourceError::Overflow)
        );
    }
}
