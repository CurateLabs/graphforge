//! A sanitized rendering of the error behind a portable-v2 I/O failure.
//!
//! Portable errors keep a stable, path-free `detail`. When the cause of an I/O
//! failure matters for diagnosis (the platform error kind, and which file of the
//! project it concerned), it travels in a separate field, with every absolute
//! host path reduced to its last two components so no user or temporary
//! directory name leaves the process.

/// Render `message` with each absolute path reduced to `<path>/parent/name`.
pub(crate) fn sanitized_cause(message: &str) -> String {
    let mut out = String::with_capacity(message.len().min(512));
    for (index, token) in message.split_whitespace().enumerate() {
        if index > 0 {
            out.push(' ');
        }
        out.push_str(&sanitize_token(token));
        if out.len() >= 480 {
            out.truncate(480);
            break;
        }
    }
    out
}

fn is_absolute_path(token: &str) -> bool {
    let token = token.trim_matches(|c| matches!(c, '"' | '\'' | '(' | ')' | ','));
    let bytes = token.as_bytes();
    token.starts_with('/')
        || token.starts_with("\\\\")
        || (bytes.len() > 2 && bytes[1] == b':' && matches!(bytes[2], b'\\' | b'/'))
}

fn sanitize_token(token: &str) -> String {
    // A path inside `...: <path>:` or `at <path>,` keeps its punctuation.
    let trimmed = token.trim_end_matches([':', ',', ';', '.']);
    let suffix = &token[trimmed.len()..];
    if !is_absolute_path(trimmed) {
        return token.to_owned();
    }
    let unquoted = trimmed.trim_matches(|c| matches!(c, '"' | '\'' | '(' | ')'));
    let parts = unquoted
        .split(['/', '\\'])
        .filter(|part| !part.is_empty() && !part.ends_with(':') && *part != "?")
        .collect::<Vec<_>>();
    let tail = &parts[parts.len().saturating_sub(2)..];
    format!("<path>/{}{suffix}", tail.join("/"))
}

#[cfg(test)]
mod tests {
    use super::sanitized_cause;

    #[test]
    fn absolute_paths_keep_only_their_last_two_components() {
        assert_eq!(
            sanitized_cause(
                "storage error: replace /home/alice/project/graph/topology/nodes.parquet: Access is denied. (os error 5)"
            ),
            "storage error: replace <path>/topology/nodes.parquet: Access is denied. (os error 5)"
        );
        assert_eq!(
            sanitized_cause(r"copy \\?\C:\Users\bob\AppData\Local\Temp\x\copy.json failed"),
            "copy <path>/x/copy.json failed"
        );
        assert_eq!(
            sanitized_cause("a: b/c relative stays"),
            "a: b/c relative stays"
        );
    }
}
