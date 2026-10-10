use super::*;

impl Scratch {
    /// Live reserved occupancy, for occupancy assertions in tests anywhere
    /// below `bulk`.
    pub(in crate::graph_construction_encoding::bulk) fn occupied_bytes(&self) -> u64 {
        self.occupied.load(Ordering::Relaxed)
    }
}
