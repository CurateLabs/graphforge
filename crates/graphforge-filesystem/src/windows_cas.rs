//! Exclusive construction and sealed-read capabilities for Windows CAS files.

use std::fs::File;
use std::io;

use super::FileIdentity;

/// Exclusive Windows capability used while constructing one CAS object.
#[cfg(windows)]
#[derive(Debug)]
pub struct WindowsCasWriter {
    pub(super) file: File,
    pub(super) identity: FileIdentity,
}

#[cfg(windows)]
impl io::Write for WindowsCasWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        io::Write::write(&mut self.file, buffer)
    }

    fn flush(&mut self) -> io::Result<()> {
        io::Write::flush(&mut self.file)
    }
}

#[cfg(windows)]
impl io::Read for WindowsCasWriter {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        io::Read::read(&mut self.file, buffer)
    }
}

#[cfg(windows)]
impl io::Seek for WindowsCasWriter {
    fn seek(&mut self, position: io::SeekFrom) -> io::Result<u64> {
        io::Seek::seek(&mut self.file, position)
    }
}

#[cfg(windows)]
impl WindowsCasWriter {
    /// Borrow the owned writer for first-party metadata accounting.
    #[doc(hidden)]
    #[must_use]
    pub fn as_file(&self) -> &File {
        &self.file
    }

    /// Flush the exact retained writer.
    pub fn sync_all(&self) -> io::Result<()> {
        self.file.sync_all()
    }

    /// Return the immutable identity captured at exclusive creation.
    #[must_use]
    pub fn identity(&self) -> FileIdentity {
        self.identity
    }
}

/// Read-only Windows capability for a canonically sealed CAS object.
#[cfg(windows)]
#[derive(Debug)]
pub struct WindowsSealedCasFile(pub(super) File);

#[cfg(windows)]
impl WindowsSealedCasFile {
    /// Consume the sealed capability as a standard read-only file handle.
    #[must_use]
    pub fn into_file(self) -> File {
        self.0
    }
}

/// Exclusive retained handle for authenticating a released legacy Windows CAS object.
#[cfg(windows)]
#[derive(Debug)]
pub struct WindowsLegacyCasAdopter {
    pub(super) file: File,
    pub(super) identity: FileIdentity,
}

#[cfg(windows)]
impl io::Read for WindowsLegacyCasAdopter {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        io::Read::read(&mut self.file, buffer)
    }
}

#[cfg(windows)]
impl io::Seek for WindowsLegacyCasAdopter {
    fn seek(&mut self, position: io::SeekFrom) -> io::Result<u64> {
        io::Seek::seek(&mut self.file, position)
    }
}

#[cfg(test)]
mod tests;
