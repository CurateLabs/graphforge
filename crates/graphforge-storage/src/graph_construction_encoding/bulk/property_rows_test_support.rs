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
