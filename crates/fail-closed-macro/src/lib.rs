//! `#[fail_closed(threat = "...")]` marks a function as fail-closed for a named threat
//! model (§53.6: "每个 fail-closed 必须有明确 threat model"). This is §53.6's fail-closed
//! generation source for `cargo xtask direction-table` (G80-9) — that gate reads workspace
//! source text directly (like every other xtask gate in this repo; see
//! `xtask/src/architecture_check.rs`), not this macro's expanded output, so the only thing
//! this crate must guarantee is that the attribute cannot compile without a `threat`
//! string: an annotation with no threat model would be a fail-closed function nobody could
//! ever ask "closed against what?" — exactly what §53.6 forbids.

use proc_macro::TokenStream;

/// See module doc. Requires `threat = "..."`; returns the item unchanged when present, a
/// `compile_error!` when the argument is missing or not a string literal.
#[proc_macro_attribute]
pub fn fail_closed(attr: TokenStream, item: TokenStream) -> TokenStream {
    if has_threat_string(&attr.to_string()) {
        item
    } else {
        let msg = "#[fail_closed] requires threat = \"...\" \
                    (§53.6: 每个 fail-closed 必须有明确 threat model)";
        format!("compile_error!({msg:?});")
            .parse()
            .expect("compile_error! literal is always valid Rust")
    }
}

/// Pure string check, kept separate from [`fail_closed`] so it's unit-testable without a
/// procedural-macro invocation context: `text` (the attribute argument list, stringified)
/// must contain a word-bounded `threat`, then (modulo whitespace) `=`, then a `"`-opening
/// string literal. Text-level rather than token-level, matching this repo's other
/// source-scanning gates (e.g. `xtask/src/architecture_check.rs`'s many `find`-based
/// scans) — no `syn`/`quote` dependency exists in this crate, and one line of pure text
/// isn't a case for adding one.
fn has_threat_string(text: &str) -> bool {
    let bytes = text.as_bytes();
    let mut start = 0usize;
    while let Some(rel) = text[start..].find("threat") {
        let idx = start + rel;
        let before_ok = idx == 0 || !is_ident_byte(bytes[idx - 1]);
        let after_idx = idx + "threat".len();
        let after_ok = after_idx >= bytes.len() || !is_ident_byte(bytes[after_idx]);
        if before_ok && after_ok {
            let after = text[after_idx..].trim_start();
            if let Some(after_eq) = after.strip_prefix('=')
                && after_eq.trim_start().starts_with('"')
            {
                return true;
            }
        }
        start = idx + "threat".len();
    }
    false
}

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_threat_with_string_literal() {
        assert!(has_threat_string("threat = \"example\""));
        assert!(has_threat_string("threat=\"tight\""));
    }

    #[test]
    fn rejects_missing_threat() {
        assert!(!has_threat_string(""));
        assert!(!has_threat_string("other = \"x\""));
    }

    #[test]
    fn rejects_threat_without_string_literal() {
        assert!(!has_threat_string("threat = 5"));
        assert!(!has_threat_string("threat ="));
        assert!(!has_threat_string("threat"));
    }

    #[test]
    fn word_boundary_does_not_match_substring() {
        // `other_threat = "x"` must not count as this fn's own `threat` key.
        assert!(!has_threat_string("other_threat = \"x\""));
    }
}
