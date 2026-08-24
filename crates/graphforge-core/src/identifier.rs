//! Canonical graph identifier contract shared by every Rust surface.

/// Maximum encoded UTF-8 bytes in a graph label, relationship, or property name.
pub const MAX_GRAPH_IDENTIFIER_BYTES: usize = 255;

/// Whether `value` is a canonical bounded Unicode graph identifier.
#[must_use]
pub fn is_graph_identifier(value: &str) -> bool {
    if value.len() > MAX_GRAPH_IDENTIFIER_BYTES {
        return false;
    }
    let mut characters = value.chars();
    characters
        .next()
        .is_some_and(|first| first == '_' || first.is_alphabetic())
        && characters.all(|character| character == '_' || character.is_alphanumeric())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unicode_identifier_limit_is_encoded_bytes() {
        let at_limit = format!("A{}", "é".repeat(127));
        let over_limit = format!("A{}x", "é".repeat(127));
        assert_eq!(at_limit.len(), 255);
        assert_eq!(over_limit.len(), 256);
        assert!(is_graph_identifier(&at_limit));
        assert!(!is_graph_identifier(&over_limit));
    }
}
