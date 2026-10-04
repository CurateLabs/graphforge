//! Capture-owned proc descriptors; each observation rewinds and reads anew.
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

#[derive(Debug)]
pub(super) struct ProcReader {
    file: Option<File>,
    text: String,
}

impl ProcReader {
    pub(super) fn open(path: &str) -> Self {
        Self {
            file: File::open(path).ok(),
            text: String::new(),
        }
    }

    pub(super) fn read(&mut self) -> Option<&str> {
        let file = self.file.as_mut()?;
        file.seek(SeekFrom::Start(0)).ok()?;
        self.text.clear();
        file.read_to_string(&mut self.text).ok()?;
        Some(&self.text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retained_proc_reader_observes_changed_and_truncated_values() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("counter");
        std::fs::write(&path, "12345\n").unwrap();
        let mut reader = ProcReader::open(path.to_str().unwrap());
        assert_eq!(reader.read(), Some("12345\n"));
        std::fs::write(&path, "0\n").unwrap();
        assert_eq!(reader.read(), Some("0\n"));
        std::fs::write(&path, "7\n").unwrap();
        assert_eq!(reader.read(), Some("7\n"));
        let mut missing = ProcReader::open(directory.path().join("absent").to_str().unwrap());
        assert_eq!(missing.read(), None);
    }
}
