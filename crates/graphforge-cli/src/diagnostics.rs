//! Core version warnings raised while a command runs (#1936).
//!
//! Text mode prints `warning <code>: <message>` on standard error as soon as a
//! command raises it. `--json` keeps the one-document contract: nothing extra
//! is printed; the warnings are collected and placed in a `warnings` array
//! inside the single document the command emits, either its result on standard
//! output ([`WarningOutput`]) or its error document on standard error.

use std::cell::RefCell;
use std::io::{self, Write};
use std::rc::Rc;

use graphforge_api::CoreVersionWarning;
use serde_json::Value;

type Collected = Rc<RefCell<Vec<CoreVersionWarning>>>;

/// Warnings a command has raised so far, shared by every project-opening path.
pub(crate) struct Diagnostics<'a> {
    json: bool,
    sink: &'a mut dyn Write,
    collected: Collected,
}

impl<'a> Diagnostics<'a> {
    pub(crate) fn new(json: bool, sink: &'a mut dyn Write) -> Self {
        Self {
            json,
            sink,
            collected: Collected::default(),
        }
    }

    pub(crate) fn warn(&mut self, warnings: &[CoreVersionWarning]) {
        self.collected.borrow_mut().extend_from_slice(warnings);
        if !self.json {
            for warning in warnings {
                // Best effort: a closed standard error never fails the command.
                let _ = writeln!(self.sink, "warning {}: {}", warning.code, warning.message);
            }
        }
    }

    /// Everything raised so far, for the error document of a failed command.
    pub(crate) fn collected(&self) -> Vec<CoreVersionWarning> {
        self.collected.borrow().clone()
    }

    fn handle(&self) -> Collected {
        Rc::clone(&self.collected)
    }
}

/// A command's standard output. Under `--json`, once a warning exists, output is
/// held so [`Self::finish`] can add `warnings` to the command's one document.
pub(crate) struct WarningOutput<'a> {
    inner: &'a mut dyn Write,
    json: bool,
    collected: Collected,
    held: Vec<u8>,
}

impl<'a> WarningOutput<'a> {
    pub(crate) fn new(inner: &'a mut dyn Write, json: bool, diagnostics: &Diagnostics<'_>) -> Self {
        Self {
            inner,
            json,
            collected: diagnostics.handle(),
            held: Vec::new(),
        }
    }

    fn holding(&self) -> bool {
        self.json && !self.collected.borrow().is_empty()
    }

    /// Write out what was held. A successful command gets its document stamped
    /// with the warnings; a failed one keeps its output as written, because its
    /// error document carries them.
    pub(crate) fn finish(
        self,
        succeeded: bool,
        diagnostics: &mut Diagnostics<'_>,
    ) -> io::Result<()> {
        if self.held.is_empty() {
            return self.inner.flush();
        }
        let warnings = self.collected.borrow().clone();
        let stamped = succeeded
            .then(|| stamp_warnings(&self.held, &warnings))
            .flatten();
        if let Some(bytes) = stamped {
            self.inner.write_all(&bytes)?;
        } else {
            self.inner.write_all(&self.held)?;
            if succeeded {
                // Output that is not a JSON document cannot carry them.
                for warning in &warnings {
                    let _ = writeln!(
                        diagnostics.sink,
                        "warning {}: {}",
                        warning.code, warning.message
                    );
                }
            }
        }
        self.inner.flush()
    }
}

impl Write for WarningOutput<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.holding() {
            self.held.extend_from_slice(buf);
            Ok(buf.len())
        } else {
            self.inner.write(buf)
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.holding() {
            Ok(())
        } else {
            self.inner.flush()
        }
    }
}

/// Add `warnings` to the command's document. One object (or a stream of lines
/// whose first is an object) gets the key unless it already has one, as import
/// and clone results do; another single JSON value is wrapped as `result`.
fn stamp_warnings(held: &[u8], warnings: &[CoreVersionWarning]) -> Option<Vec<u8>> {
    let list = serde_json::to_value(warnings).ok()?;
    if let Ok(document) = serde_json::from_slice::<Value>(held) {
        return encode(&stamped(document, &list));
    }
    let text = std::str::from_utf8(held).ok()?;
    let (first, rest) = text.split_once('\n')?;
    let first = serde_json::from_str::<Value>(first).ok()?;
    if !first.is_object() {
        return None;
    }
    let mut bytes = encode(&stamped(first, &list))?;
    bytes.extend_from_slice(rest.as_bytes());
    Some(bytes)
}

fn stamped(document: Value, list: &Value) -> Value {
    match document {
        Value::Object(mut object) => {
            object.entry("warnings").or_insert_with(|| list.clone());
            Value::Object(object)
        }
        other => serde_json::json!({ "result": other, "warnings": list }),
    }
}

fn encode(document: &Value) -> Option<Vec<u8>> {
    let mut bytes = serde_json::to_vec(&document).ok()?;
    bytes.push(b'\n');
    Some(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn warning() -> CoreVersionWarning {
        graphforge_api::check_core_version(Some("0.0.1")).unwrap()
    }

    fn stamped_text(held: &str) -> Option<String> {
        stamp_warnings(held.as_bytes(), &[warning()]).map(|b| String::from_utf8(b).unwrap())
    }

    #[test]
    fn one_object_gets_warnings_and_keeps_an_existing_list() {
        let text = stamped_text("{\"a\":1}\n").unwrap();
        let value: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(value["a"], 1);
        assert_eq!(value["warnings"][0]["code"], "GF_CORE_VERSION_MISMATCH");
        let kept = stamped_text("{\"warnings\":[]}\n").unwrap();
        assert_eq!(kept.trim_end(), "{\"warnings\":[]}");
    }

    #[test]
    fn a_stream_is_stamped_on_its_first_document_and_other_output_is_not() {
        let text = stamped_text("{\"n\":1}\n{\"n\":2}\n").unwrap();
        let lines: Vec<_> = text.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("warnings"));
        assert_eq!(lines[1], "{\"n\":2}");
        assert_eq!(
            serde_json::from_str::<Value>(&stamped_text("[1,2]\n").unwrap()).unwrap()["result"],
            serde_json::json!([1, 2])
        );
        assert!(stamped_text("not json\nmore\n").is_none());
    }
}
