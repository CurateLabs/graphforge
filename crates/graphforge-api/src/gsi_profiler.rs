//! Graph Scale Index (GSI) profiler over live workspace counts.

use graphforge_core::GfError;
use graphforge_storage::GraphDirectedness;

use crate::GraphForge;

/// Directedness reported by a GSI grade.
///
/// Configuration absence yields [`Self::Unknown`] (`Gx`) even when density uses
/// the directed formula.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GsiDirectedness {
    /// Project configured `graph_directedness=directed` (`GD`).
    Directed,
    /// Project configured `graph_directedness=undirected` (`GU`).
    Undirected,
    /// Project left directedness unset (`Gx`).
    Unknown,
}

impl GsiDirectedness {
    /// Canonical token for structured results and bindings.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Directed => "directed",
            Self::Undirected => "undirected",
            Self::Unknown => "unknown",
        }
    }

    const fn prefix(self) -> &'static str {
        match self {
            Self::Directed => "GD",
            Self::Undirected => "GU",
            Self::Unknown => "Gx",
        }
    }
}

impl From<Option<GraphDirectedness>> for GsiDirectedness {
    fn from(value: Option<GraphDirectedness>) -> Self {
        match value {
            Some(GraphDirectedness::Directed) => Self::Directed,
            Some(GraphDirectedness::Undirected) => Self::Undirected,
            None => Self::Unknown,
        }
    }
}

/// Structured Graph Scale Index grade for one opened workspace.
#[derive(Debug, Clone, PartialEq)]
pub struct GraphScaleIndexProfile {
    /// Full GSI identifier (`[GD|GU|Gx]-[Scale]-[Size]-D[Density]`).
    pub gsi: String,
    /// Configured or unknown directedness.
    pub directedness: GsiDirectedness,
    /// Live node count `V` (excludes deleted facts).
    pub node_count: u64,
    /// Live edge count `E` (excludes deleted facts).
    pub edge_count: u64,
    /// Raw clamped density in `[0.0, 1.0]`.
    pub density: f64,
    /// Two-character Scale Code (`00`–`12`, or `**`).
    pub scale_code: String,
    /// Size Tag tied to the Scale Code band.
    pub size_tag: String,
    /// Integer density percent in `0..=100`.
    pub density_integer: u32,
}

impl GraphForge {
    /// Grade the live graph in this opened workspace to a Graph Scale Index.
    ///
    /// Empty and tiny graphs succeed without error. Density uses the undirected
    /// formula for `GU`, and the directed formula for `GD` and `Gx`.
    ///
    /// # Errors
    /// Returns structured project, execution, or schema errors when live counts
    /// or workspace configuration cannot be read.
    pub fn profile_gsi(&self) -> Result<GraphScaleIndexProfile, GfError> {
        let inspection = self.inspect_graph()?;
        let node_count = inspection.node_count("");
        let edge_count = inspection.edge_count()?;
        let directedness =
            GsiDirectedness::from(self.workspace_configuration()?.graph_directedness);
        Ok(grade_gsi(node_count, edge_count, directedness))
    }
}

/// Pure GSI grading used by [`GraphForge::profile_gsi`] and unit tests.
#[must_use]
pub fn grade_gsi(
    node_count: u64,
    edge_count: u64,
    directedness: GsiDirectedness,
) -> GraphScaleIndexProfile {
    let (scale_code, size_tag) = scale_and_size(node_count);
    let density = raw_density(node_count, edge_count, directedness);
    let density_integer = density_integer(density);
    let gsi = format!(
        "{}-{}-{}-D{}",
        directedness.prefix(),
        scale_code,
        size_tag,
        format_density_integer(density_integer)
    );
    GraphScaleIndexProfile {
        gsi,
        directedness,
        node_count,
        edge_count,
        density,
        scale_code: scale_code.to_owned(),
        size_tag: size_tag.to_owned(),
        density_integer,
    }
}

fn scale_and_size(node_count: u64) -> (&'static str, &'static str) {
    match node_count {
        0 => ("00", "XS"),
        1..=99 => ("01", "XS"),
        100..=999 => ("02", "XS"),
        1_000..=9_999 => ("03", "XS"),
        10_000..=99_999 => ("04", "XS"),
        100_000..=999_999 => ("05", "SM"),
        1_000_000..=9_999_999 => ("06", "MD"),
        10_000_000..=99_999_999 => ("07", "LG"),
        100_000_000..=999_999_999 => ("08", "XL"),
        1_000_000_000..=9_999_999_999 => ("09", "2XL"),
        10_000_000_000..=99_999_999_999 => ("10", "3XL"),
        100_000_000_000..=999_999_999_999 => ("11", "4XL"),
        1_000_000_000_000..=9_999_999_999_999 => ("12", "5XL"),
        _ => ("**", "BIG"),
    }
}

fn raw_density(node_count: u64, edge_count: u64, directedness: GsiDirectedness) -> f64 {
    if node_count < 2 {
        return 0.0;
    }
    let denominator = (node_count as f64) * ((node_count - 1) as f64);
    if denominator == 0.0 {
        return 0.0;
    }
    let numerator = match directedness {
        GsiDirectedness::Undirected => 2.0 * (edge_count as f64),
        GsiDirectedness::Directed | GsiDirectedness::Unknown => edge_count as f64,
    };
    let density = numerator / denominator;
    density.clamp(0.0, 1.0)
}

fn density_integer(density: f64) -> u32 {
    let rounded = (density * 100.0).round();
    if !rounded.is_finite() || rounded <= 0.0 {
        0
    } else if rounded >= 100.0 {
        100
    } else {
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        {
            rounded as u32
        }
    }
}

fn format_density_integer(density_integer: u32) -> String {
    if density_integer >= 100 {
        "100".to_owned()
    } else {
        format!("{density_integer:02}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{OperationId, WriteContext};
    use uuid::Uuid;

    fn write_context(seed: u128) -> WriteContext {
        WriteContext {
            operation_uuid: OperationId(Uuid::from_u128(seed)),
            actor_uuid: None,
        }
    }

    #[test]
    fn pure_grades_match_golden_strings() {
        assert_eq!(
            grade_gsi(0, 0, GsiDirectedness::Unknown).gsi,
            "Gx-00-XS-D00"
        );
        assert_eq!(
            grade_gsi(0, 0, GsiDirectedness::Directed).gsi,
            "GD-00-XS-D00"
        );
        assert_eq!(
            grade_gsi(1, 0, GsiDirectedness::Unknown).gsi,
            "Gx-01-XS-D00"
        );
        assert_eq!(
            grade_gsi(3, 3, GsiDirectedness::Unknown).gsi,
            "Gx-01-XS-D50"
        );
        assert_eq!(
            grade_gsi(3, 3, GsiDirectedness::Undirected).gsi,
            "GU-01-XS-D100"
        );
        assert_eq!(
            grade_gsi(3, 6, GsiDirectedness::Directed).gsi,
            "GD-01-XS-D100"
        );
        assert_eq!(
            grade_gsi(4, 1, GsiDirectedness::Directed).gsi,
            "GD-01-XS-D08"
        );
        let unknown = grade_gsi(4, 1, GsiDirectedness::Unknown);
        assert_eq!(unknown.gsi, "Gx-01-XS-D08");
        assert_eq!(unknown.directedness, GsiDirectedness::Unknown);
        assert!((unknown.density - (1.0 / 12.0)).abs() < 1e-12);
    }

    #[test]
    fn empty_workspace_profiles_without_error() {
        let graph = GraphForge::new(None).unwrap();
        let profile = graph.profile_gsi().unwrap();
        assert_eq!(profile.gsi, "Gx-00-XS-D00");
        assert_eq!(profile.directedness, GsiDirectedness::Unknown);
        assert_eq!(profile.node_count, 0);
        assert_eq!(profile.edge_count, 0);
        assert_eq!(profile.density_integer, 0);
        assert_eq!(profile.scale_code, "00");
        assert_eq!(profile.size_tag, "XS");
    }

    #[test]
    fn singleton_and_configured_directedness_grade() {
        let mut graph = GraphForge::new(None).unwrap();
        graph.execute("CREATE (a:Person {name:'Alice'})").unwrap();
        let tiny = graph.profile_gsi().unwrap();
        assert_eq!(tiny.gsi, "Gx-01-XS-D00");
        assert_eq!(tiny.node_count, 1);
        assert_eq!(tiny.density_integer, 0);

        graph
            .set_graph_directedness(write_context(1), Some(GraphDirectedness::Directed))
            .unwrap();
        assert_eq!(
            graph.graph_directedness().unwrap(),
            Some(GraphDirectedness::Directed)
        );
        assert_eq!(graph.profile_gsi().unwrap().gsi, "GD-01-XS-D00");
    }

    #[test]
    fn triangle_grades_gd_gu_and_gx() {
        let mut graph = GraphForge::new(None).unwrap();
        graph
            .execute(
                "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), \
                 (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), (c)-[:KNOWS]->(a)",
            )
            .unwrap();
        let unknown = graph.profile_gsi().unwrap();
        assert_eq!(unknown.gsi, "Gx-01-XS-D50");
        assert_eq!(unknown.directedness.as_str(), "unknown");
        assert_eq!(unknown.node_count, 3);
        assert_eq!(unknown.edge_count, 3);

        graph
            .set_graph_directedness(write_context(2), Some(GraphDirectedness::Undirected))
            .unwrap();
        let undirected = graph.profile_gsi().unwrap();
        assert_eq!(undirected.gsi, "GU-01-XS-D100");
        assert_eq!(undirected.directedness, GsiDirectedness::Undirected);

        graph
            .set_graph_directedness(write_context(3), Some(GraphDirectedness::Directed))
            .unwrap();
        let directed = graph.profile_gsi().unwrap();
        assert_eq!(directed.gsi, "GD-01-XS-D50");
        assert_eq!(directed.directedness, GsiDirectedness::Directed);

        graph
            .set_graph_directedness(write_context(4), None)
            .unwrap();
        assert_eq!(graph.graph_directedness().unwrap(), None);
        assert_eq!(graph.profile_gsi().unwrap().gsi, "Gx-01-XS-D50");
    }
}
