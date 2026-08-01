//! Boolean Euler-path predicate over the shared canonical Euler projection.

pub(crate) use crate::algorithm_analyze_euler::EulerEdge as EulerPathEdge;
use crate::algorithm_analyze_euler::EulerProjection;
use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};

/// Decide whether the selected directed or undirected multigraph has an Euler trail.
pub(crate) fn has_euler_path(
    nodes: &[[u8; 16]],
    edges: &[EulerPathEdge],
    directed: bool,
    control: &AlgorithmControl,
) -> Result<bool, AlgorithmError> {
    control.check_output_rows(1)?;
    EulerProjection::new(nodes, edges, directed, control)?.has_path(control)
}
