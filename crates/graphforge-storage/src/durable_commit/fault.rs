//! Operation-local primitive faults, compiled only into unit tests.
use std::cell::Cell;
use std::fs::File;
use std::io::{self, Write};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Point {
    PartialWrite,
    FileFence,
    BeforeVisible,
    NativeUnknown,
    ParentFence,
}
thread_local! { static NEXT: Cell<Option<Point>> = const { Cell::new(None) }; }
pub(super) fn arm(point: Point) {
    NEXT.with(|next| next.set(Some(point)));
}
pub(super) fn hit(point: Point) -> io::Result<()> {
    if NEXT.with(|next| {
        if next.get() == Some(point) {
            next.set(None);
            true
        } else {
            false
        }
    }) {
        Err(io::Error::other(format!(
            "durable commit injected {point:?}"
        )))
    } else {
        Ok(())
    }
}
pub(super) fn write(file: &mut File, bytes: &[u8]) -> io::Result<()> {
    if NEXT.with(|next| next.get() == Some(Point::PartialWrite)) {
        file.write_all(&bytes[..bytes.len().min(2)])?;
        hit(Point::PartialWrite)
    } else {
        file.write_all(bytes)
    }
}
