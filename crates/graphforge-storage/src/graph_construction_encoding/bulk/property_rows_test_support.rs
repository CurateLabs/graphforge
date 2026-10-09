//! Run sizing controls for small property scratch fixtures.

thread_local! { pub(super) static FORCED_FRAME_BYTES: std::cell::Cell<Option<usize>> = const {std::cell::Cell::new(None)}; }
pub(crate) struct ForcedPropertyFrames;
impl ForcedPropertyFrames {
    pub(crate) fn set(bytes: usize) -> Self {
        FORCED_FRAME_BYTES.with(|forced| forced.set(Some(bytes.max(1))));
        Self
    }
}
impl Drop for ForcedPropertyFrames {
    fn drop(&mut self) {
        FORCED_FRAME_BYTES.with(|forced| forced.set(None));
    }
}
thread_local! { pub(super) static FORCED_SIZING: std::cell::Cell<Option<(usize, usize)>> = const {std::cell::Cell::new(None)}; }
/// Forces the run size and merge fan-in of the builds the current test thread
/// runs, until dropped, so a small input spans many runs and merge levels.
pub(crate) struct ForcedPropertySizing;
impl ForcedPropertySizing {
    pub(crate) fn set(run_bytes: usize, fan_in: usize) -> Self {
        FORCED_SIZING.with(|forced| forced.set(Some((run_bytes.max(1), fan_in.max(2)))));
        Self
    }
}
impl Drop for ForcedPropertySizing {
    fn drop(&mut self) {
        FORCED_SIZING.with(|forced| forced.set(None));
    }
}
