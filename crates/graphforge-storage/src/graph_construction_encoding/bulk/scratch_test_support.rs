use super::*;

impl Scratch {
    pub(super) fn occupied_bytes(&self) -> u64 {
        self.occupied.load(Ordering::Relaxed)
    }
}
