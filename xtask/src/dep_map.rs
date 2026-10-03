//! `xtask::dep_map` — `cargo xtask dep-map`: checks every module header, `// dep:` tag and Cargo `# why:` line against the code, and generates the three dependency maps.
//! Depends-on: crates=[humaux-testkit, postgres, toml]; services=[PostgreSQL(owner) r=[ops.schema_migrations]];
//!   env=[CARGO_MANIFEST_DIR, HUMAUX_TEST_PG_DSN]; modules=[xtask::rls_check]
//! Called-by: [xtask::gate_truth, xtask::main]
//! Invariants: [reads the repo and the catalog only — the catalog inside a READ ONLY transaction that is rolled
//!   back; HUMAUX_TEST_PG_DSN unset or unreachable → exit 2 (db_objects.md cannot be checked, a skip is not a pass);
//!   generated docs are pure functions of the tree and the catalog (sorted, repo-relative, no timestamps)]
//! Spec: Baseline §58; §6.2.2; §78; §79.2; ADR-0050; ADR-0051
//! dep-map: allow table-undeclared — unit-test fixtures name schema objects in strings
//! dep-map: allow table-write — unit-test fixtures spell write statements in strings
//! dep-map: allow env-undeclared — fixtures and test_target_services name HUMAUX_* variables they never read
//!
//! The convention and every rule id live in `docs/architecture/maintainability.md`; why each
//! decision was taken is ADR-0051. One pass per file: a masking lexer (comments and string
//! contents blanked, line numbers kept) feeds header parsing, computed facts (crates, env
//! literals, `schema.object` tokens, call sites, import edges) and the cross-checks. No parser
//! dependency: `toml` validates Cargo, `postgres` reads the catalog.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use postgres::{Client, NoTls};

use crate::rls_check::SCHEMAS;

const DSN_ENV: &str = "HUMAUX_TEST_PG_DSN";
const DOC_DEP_MAP: &str = "docs/architecture/dependency_map.md";
const DOC_DB: &str = "docs/architecture/db_objects.md";
const DOC_ENV: &str = "docs/architecture/env_vars.md";
const FIELDS: [&str; 4] = ["Depends-on", "Called-by", "Invariants", "Spec"];

/// Every rule id (maintainability.md §9).
const RULES: &[&str] = &[
    "header-field",
    "header-path",
    "header-purpose",
    "header-invariants",
    "spec-ref",
    "service-vocab",
    "crates-undeclared",
    "crates-unwitnessed",
    "modules-drift",
    "calledby-phantom",
    "calledby-missing",
    "env-undeclared",
    "env-unregistered",
    "table-undeclared",
    "table-write",
    "table-unwitnessed",
    "callsite-untagged",
    "tag-grammar",
    "tag-mismatch",
    "service-undeclared",
    "service-unwitnessed",
    "cargo-why",
    "cargo-usedby",
    "cargo-parse",
    "exemption-grammar",
    "exemption-unused",
    "db-not-at-head",
    "doc-drift",
];

/// `Invariants:` text that defers to prose instead of stating the invariant.
const INVARIANT_POINTERS: [&str; 5] = [
    "see prose",
    "see body",
    "see module prose",
    "see existing prose",
    "described in the prose",
];

/// Identical `Invariants:` text in this many files is boilerplate, not a per-module fact.
const INVARIANT_COPIES: usize = 3;

/// Rules an exemption may never name.
const UNEXEMPTABLE: &[&str] = &[
    "header-field",
    "doc-drift",
    "exemption-grammar",
    "exemption-unused",
    "db-not-at-head",
];

// ============================================================================
// Lexer
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommentKind {
    Line,
    InnerDoc,
    OuterDoc,
    Block,
}

#[derive(Debug, Clone)]
struct Comment {
    line: usize,
    kind: CommentKind,
    text: String,
}

/// A string literal: `start` is the byte of its first token char (prefix included), `text`
/// its raw content between the quotes (escapes not processed).
#[derive(Debug, Clone)]
struct Lit {
    start: usize,
    line: usize,
    text: String,
}

#[derive(Debug, Default)]
struct Lexed {
    masked: String,
    lits: Vec<Lit>,
    comments: Vec<Comment>,
    line_starts: Vec<usize>,
}

impl Lexed {
    fn line_of(&self, byte: usize) -> usize {
        match self.line_starts.binary_search(&byte) {
            Ok(i) => i + 1,
            Err(i) => i,
        }
    }

    fn lines(&self) -> usize {
        self.line_starts.len()
    }

    /// Masked text of 1-based `line`, without its newline.
    fn masked_line(&self, line: usize) -> &str {
        let start = self.line_starts[line - 1];
        let end = self
            .line_starts
            .get(line)
            .map_or(self.masked.len(), |next| next - 1);
        &self.masked[start..end.max(start)]
    }

    fn lit_at(&self, byte: usize) -> Option<&Lit> {
        self.lits
            .binary_search_by_key(&byte, |l| l.start)
            .ok()
            .map(|i| &self.lits[i])
    }
}

fn is_ident(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b >= 0x80
}

fn blank(m: &mut [u8], from: usize, to: usize) {
    let to = to.min(m.len());
    for x in &mut m[from..to] {
        if *x != b'\n' {
            *x = b' ';
        }
    }
}

/// One pass: comments and string/char contents blanked (newlines kept), literals and
/// comments collected.
#[allow(clippy::too_many_lines)] // one byte loop: every token form in one place keeps the masking auditable
fn lex(src: &str) -> Lexed {
    let b = src.as_bytes();
    let mut m = b.to_vec();
    let mut lits = Vec::new();
    let mut comments = Vec::new();
    let line_starts: Vec<usize> = std::iter::once(0)
        .chain(
            b.iter()
                .enumerate()
                .filter(|(_, c)| **c == b'\n')
                .map(|(i, _)| i + 1),
        )
        .collect();
    let line_of = |byte: usize| match line_starts.binary_search(&byte) {
        Ok(i) => i + 1,
        Err(i) => i,
    };
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if c == b'/' && b.get(i + 1) == Some(&b'/') {
            let end = b[i..]
                .iter()
                .position(|x| *x == b'\n')
                .map_or(b.len(), |n| i + n);
            let body = &src[i..end];
            let (kind, text) = if let Some(t) = body
                .strip_prefix("///")
                .filter(|_| !body.starts_with("////"))
            {
                (CommentKind::OuterDoc, t)
            } else if let Some(t) = body.strip_prefix("//!") {
                (CommentKind::InnerDoc, t)
            } else {
                (CommentKind::Line, &body[2..])
            };
            comments.push(Comment {
                line: line_of(i),
                kind,
                text: text.to_string(),
            });
            blank(&mut m, i, end);
            i = end;
            continue;
        }
        if c == b'/' && b.get(i + 1) == Some(&b'*') {
            let start = i;
            let mut depth = 0usize;
            while i < b.len() {
                if b[i..].starts_with(b"/*") {
                    depth += 1;
                    i += 2;
                } else if b[i..].starts_with(b"*/") {
                    depth -= 1;
                    i += 2;
                    if depth == 0 {
                        break;
                    }
                } else {
                    i += 1;
                }
            }
            comments.push(Comment {
                line: line_of(start),
                kind: CommentKind::Block,
                text: String::from_utf8_lossy(&b[start..i.min(b.len())]).into_owned(),
            });
            blank(&mut m, start, i);
            continue;
        }
        if c == b'"' {
            i = cooked_string(b, &mut m, &mut lits, i, i, &line_of);
            continue;
        }
        if c == b'\'' {
            if b.get(i + 1) == Some(&b'\\') {
                let close = b
                    .get(i + 3..(i + 14).min(b.len()))
                    .and_then(|w| w.iter().position(|x| *x == b'\''))
                    .map(|n| i + 3 + n);
                if let Some(close) = close {
                    blank(&mut m, i + 1, close);
                    i = close + 1;
                    continue;
                }
            } else if let Some(&lead) = b.get(i + 1) {
                let len = match lead {
                    0x00..=0x7f => 1,
                    0xc0..=0xdf => 2,
                    0xe0..=0xef => 3,
                    _ => 4,
                };
                if b.get(i + 1 + len) == Some(&b'\'') {
                    blank(&mut m, i + 1, i + 1 + len);
                    i += 2 + len;
                    continue;
                }
            }
            i += 1; // lifetime or label
            continue;
        }
        if is_ident(c) {
            let start = i;
            while i < b.len() && is_ident(b[i]) {
                i += 1;
            }
            let word = &b[start..i];
            if matches!(word, b"r" | b"br" | b"cr") {
                let mut j = i;
                while j < b.len() && b[j] == b'#' {
                    j += 1;
                }
                if j < b.len() && b[j] == b'"' {
                    let hashes = j - i;
                    let mut close = vec![b'"'];
                    close.extend(std::iter::repeat_n(b'#', hashes));
                    let end = b[j + 1..]
                        .windows(close.len())
                        .position(|w| w == close.as_slice())
                        .map_or(b.len(), |n| j + 1 + n);
                    lits.push(Lit {
                        start,
                        line: line_of(start),
                        text: String::from_utf8_lossy(&b[j + 1..end]).into_owned(),
                    });
                    blank(&mut m, j + 1, end);
                    i = (end + close.len()).min(b.len());
                }
            } else if matches!(word, b"b" | b"c") && b.get(i) == Some(&b'"') {
                i = cooked_string(b, &mut m, &mut lits, start, i, &line_of);
            }
            continue;
        }
        i += 1;
    }
    Lexed {
        masked: String::from_utf8(m).unwrap_or_default(),
        lits,
        comments,
        line_starts,
    }
}

/// `"…"` with `\` escapes (incl. line continuations); `quote` is the opening `"`.
fn cooked_string(
    b: &[u8],
    m: &mut [u8],
    lits: &mut Vec<Lit>,
    start: usize,
    quote: usize,
    line_of: &dyn Fn(usize) -> usize,
) -> usize {
    let mut j = quote + 1;
    while j < b.len() && b[j] != b'"' {
        j += if b[j] == b'\\' { 2 } else { 1 };
    }
    let end = j.min(b.len());
    lits.push(Lit {
        start,
        line: line_of(start),
        text: String::from_utf8_lossy(&b[quote + 1..end]).into_owned(),
    });
    blank(m, quote + 1, end);
    end + 1
}

/// Index of the bracket matching the opener at `open` (masked code).
fn matching(b: &[u8], open: usize) -> usize {
    let (o, c) = match b[open] {
        b'(' => (b'(', b')'),
        b'[' => (b'[', b']'),
        _ => (b'{', b'}'),
    };
    let mut depth = 0usize;
    for (k, x) in b.iter().enumerate().skip(open) {
        if *x == o {
            depth += 1;
        } else if *x == c {
            depth -= 1;
            if depth == 0 {
                return k;
            }
        }
    }
    b.len().saturating_sub(1)
}

/// Positions of `word` in masked code at identifier boundaries.
fn word_positions(masked: &str, word: &str) -> Vec<usize> {
    let b = masked.as_bytes();
    masked
        .match_indices(word)
        .map(|(p, _)| p)
        .filter(|&p| {
            (p == 0 || !is_ident(b[p - 1])) && b.get(p + word.len()).is_none_or(|x| !is_ident(*x))
        })
        .collect()
}

fn skip_ws(b: &[u8], mut i: usize) -> usize {
    while i < b.len() && b[i].is_ascii_whitespace() {
        i += 1;
    }
    i
}

fn read_ident(b: &[u8], i: usize) -> Option<(String, usize)> {
    let mut j = i;
    while j < b.len() && is_ident(b[j]) {
        j += 1;
    }
    (j > i && !b[i].is_ascii_digit()).then(|| (String::from_utf8_lossy(&b[i..j]).into_owned(), j))
}

/// End (exclusive) of the item starting at `from`: skip attributes, then up to the first
/// depth-0 `;` or the `}` matching the first depth-0 `{`.
fn item_end(b: &[u8], from: usize) -> usize {
    let mut i = skip_ws(b, from);
    while b.get(i) == Some(&b'#') {
        let open = skip_ws(b, i + 1);
        let open = if b.get(open) == Some(&b'!') {
            skip_ws(b, open + 1)
        } else {
            open
        };
        if b.get(open) != Some(&b'[') {
            break;
        }
        i = skip_ws(b, matching(b, open) + 1);
    }
    let mut depth = 0i32;
    while i < b.len() {
        match b[i] {
            b'(' | b'[' => depth += 1,
            b')' | b']' => depth -= 1,
            b';' if depth <= 0 => return i + 1,
            b'{' if depth <= 0 => return matching(b, i) + 1,
            _ => {}
        }
        i += 1;
    }
    b.len()
}

/// `#[cfg(test)]`-like attribute content.
fn cfg_is_test(content: &str) -> bool {
    let c: String = content.chars().filter(|c| !c.is_whitespace()).collect();
    let has_test = c
        .split(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_'))
        .any(|w| w == "test");
    c == "test"
        || ((c.starts_with("any(") || c.starts_with("all(")) && has_test && !c.contains("not("))
}

/// Byte ranges of `#[cfg(test)]` items, and whether `#![cfg(test)]` covers the whole file.
fn test_ranges(masked: &str) -> (Vec<(usize, usize)>, bool) {
    let b = masked.as_bytes();
    let mut out = Vec::new();
    let mut whole = false;
    for (p, _) in masked.match_indices("cfg(") {
        let head = masked[..p].trim_end();
        let inner = head.ends_with("#![");
        if !(head.ends_with("#[") || inner) {
            continue;
        }
        let open = p + 3;
        let close = matching(b, open);
        if !cfg_is_test(&masked[open + 1..close]) {
            continue;
        }
        if inner {
            whole = true;
            continue;
        }
        let attr_start = head.rfind('#').unwrap_or(p);
        let attr_end = b[close..]
            .iter()
            .position(|x| *x == b']')
            .map_or(b.len(), |n| close + n + 1);
        out.push((attr_start, item_end(b, attr_end)));
    }
    (out, whole)
}

/// Inline `mod name { … }` blocks: `(open, close, name)`.
fn inline_mods(masked: &str) -> Vec<(usize, usize, String)> {
    let b = masked.as_bytes();
    let mut out = Vec::new();
    for p in word_positions(masked, "mod") {
        let i = skip_ws(b, p + 3);
        let Some((name, j)) = read_ident(b, i) else {
            continue;
        };
        let k = skip_ws(b, j);
        if b.get(k) == Some(&b'{') {
            out.push((k, matching(b, k), name));
        }
    }
    out
}

/// `mod name;` declarations: `(name, pos, #[path] value)`.
fn mod_decls(lex: &Lexed) -> Vec<(String, usize, Option<String>)> {
    let masked = &lex.masked;
    let b = masked.as_bytes();
    let mut out = Vec::new();
    for p in word_positions(masked, "mod") {
        let i = skip_ws(b, p + 3);
        let Some((name, j)) = read_ident(b, i) else {
            continue;
        };
        if b.get(skip_ws(b, j)) != Some(&b';') {
            continue;
        }
        // Walk back over visibility and attributes looking for `#[path = "…"]`.
        let mut head = masked[..p].trim_end();
        if head.ends_with(')') {
            if let Some(v) = head.rfind("pub(") {
                head = head[..v].trim_end();
            }
        } else if let Some(h) = head.strip_suffix("pub") {
            head = h.trim_end();
        }
        let mut path = None;
        while head.ends_with(']') {
            let close = head.len() - 1;
            let mut depth = 0i32;
            let mut open = None;
            for k in (0..=close).rev() {
                match b[k] {
                    b']' => depth += 1,
                    b'[' => {
                        depth -= 1;
                        if depth == 0 {
                            open = Some(k);
                            break;
                        }
                    }
                    _ => {}
                }
            }
            let Some(open) = open else { break };
            let attr = &masked[open + 1..close];
            if attr.trim_start().starts_with("path")
                && let Some(q) = attr.find('"')
            {
                path = lex.lit_at(open + 1 + q).map(|l| l.text.clone());
            }
            head = masked[..open].trim_end().trim_end_matches('#').trim_end();
        }
        out.push((name, p, path));
    }
    out
}

// ============================================================================
// `use` trees and inline paths
// ============================================================================

#[derive(Debug, Clone)]
struct UseLeaf {
    path: Vec<String>,
    glob: bool,
    alias: Option<String>,
}

#[derive(Debug, Clone)]
struct UseStmt {
    pos: usize,
    end: usize,
    /// exactly `pub use` (a re-export, not an edge)
    is_pub: bool,
    leaves: Vec<UseLeaf>,
}

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Ident(String),
    Sep,
    Open,
    Close,
    Comma,
    Star,
}

fn use_tokens(s: &str) -> Vec<Tok> {
    let b = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b':' if b.get(i + 1) == Some(&b':') => {
                out.push(Tok::Sep);
                i += 2;
            }
            b'{' => {
                out.push(Tok::Open);
                i += 1;
            }
            b'}' => {
                out.push(Tok::Close);
                i += 1;
            }
            b',' => {
                out.push(Tok::Comma);
                i += 1;
            }
            b'*' => {
                out.push(Tok::Star);
                i += 1;
            }
            x if is_ident(x) => {
                let (w, j) = read_ident(b, i).unwrap_or_else(|| {
                    let mut j = i;
                    while j < b.len() && is_ident(b[j]) {
                        j += 1;
                    }
                    (s[i..j].to_string(), j)
                });
                out.push(Tok::Ident(w));
                i = j;
            }
            _ => i += 1,
        }
    }
    out
}

fn parse_use_tree(t: &[Tok], mut i: usize, prefix: &[String], out: &mut Vec<UseLeaf>) -> usize {
    let mut path = prefix.to_vec();
    while i < t.len() {
        match &t[i] {
            Tok::Sep => i += 1,
            Tok::Ident(s) => {
                path.push(s.clone());
                i += 1;
                if t.get(i) == Some(&Tok::Sep) {
                    i += 1;
                    continue;
                }
                let mut alias = None;
                if t.get(i) == Some(&Tok::Ident("as".into())) {
                    if let Some(Tok::Ident(a)) = t.get(i + 1) {
                        alias = Some(a.clone());
                    }
                    i += 2;
                }
                if path.len() > 1 && path.last().is_some_and(|l| l == "self") {
                    path.pop();
                }
                out.push(UseLeaf {
                    path,
                    glob: false,
                    alias,
                });
                return i;
            }
            Tok::Star => {
                out.push(UseLeaf {
                    path,
                    glob: true,
                    alias: None,
                });
                return i + 1;
            }
            Tok::Open => {
                i += 1;
                while i < t.len() && t[i] != Tok::Close {
                    if t[i] == Tok::Comma {
                        i += 1;
                        continue;
                    }
                    i = parse_use_tree(t, i, &path, out);
                }
                return i + 1;
            }
            Tok::Close | Tok::Comma => return i,
        }
    }
    i
}

fn use_stmts(masked: &str) -> Vec<UseStmt> {
    let b = masked.as_bytes();
    let mut out = Vec::new();
    for p in word_positions(masked, "use") {
        let head = masked[..p].trim_end();
        let is_pub = head.ends_with("pub") && head.len() >= 3 && {
            let before = head.len() - 3;
            before == 0 || !is_ident(b[before - 1])
        };
        let Some(end) = masked[p..].find(';').map(|n| p + n) else {
            continue;
        };
        let toks = use_tokens(&masked[p + 3..end]);
        let mut leaves = Vec::new();
        let mut i = 0;
        while i < toks.len() {
            let next = parse_use_tree(&toks, i, &[], &mut leaves);
            i = if next == i { i + 1 } else { next };
        }
        out.push(UseStmt {
            pos: p,
            end,
            is_pub,
            leaves,
        });
    }
    out
}

/// Inline `a::b::c` paths outside `use` statements: `(pos, segments)`; only paths whose
/// first segment is followed by `::`.
fn inline_paths(masked: &str, skip: &[(usize, usize)]) -> Vec<(usize, Vec<String>)> {
    let b = masked.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    let mut skip_iter = skip.iter().peekable();
    while i < b.len() {
        while let Some(&&(s, e)) = skip_iter.peek() {
            if e < i {
                skip_iter.next();
            } else {
                if s <= i && i <= e {
                    i = e + 1;
                }
                break;
            }
        }
        if i >= b.len() {
            break;
        }
        if !is_ident(b[i]) {
            i += 1;
            continue;
        }
        let prev_ok = i == 0 || (!is_ident(b[i - 1]) && b[i - 1] != b':' && b[i - 1] != b'.');
        let Some((first, mut j)) = read_ident(b, i) else {
            while i < b.len() && is_ident(b[i]) {
                i += 1;
            }
            continue;
        };
        if !prev_ok {
            i = j;
            continue;
        }
        let start = i;
        let mut segs = vec![first];
        loop {
            let k = skip_ws(b, j);
            if !(b.get(k) == Some(&b':') && b.get(k + 1) == Some(&b':')) {
                break;
            }
            let n = skip_ws(b, k + 2);
            match read_ident(b, n) {
                Some((seg, e)) => {
                    segs.push(seg);
                    j = e;
                }
                None => break,
            }
        }
        if segs.len() > 1
            || (b.get(skip_ws(b, j)) == Some(&b':') && b.get(skip_ws(b, j) + 1) == Some(&b':'))
        {
            out.push((start, segs));
        }
        i = j;
    }
    out
}

// ============================================================================
// Header
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Service {
    PostgreSQL,
    Qdrant,
    MiniMax,
    DashScope,
    Uds,
    Subprocess,
    Http,
    Fs,
}

impl Service {
    const ALL: [Service; 8] = [
        Service::PostgreSQL,
        Service::Qdrant,
        Service::MiniMax,
        Service::DashScope,
        Service::Uds,
        Service::Subprocess,
        Service::Http,
        Service::Fs,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Service::PostgreSQL => "PostgreSQL",
            Service::Qdrant => "Qdrant",
            Service::MiniMax => "MiniMax",
            Service::DashScope => "DashScope",
            Service::Uds => "UDS",
            Service::Subprocess => "subprocess",
            Service::Http => "HTTP",
            Service::Fs => "fs",
        }
    }

    fn parse(s: &str) -> Option<Service> {
        Service::ALL.into_iter().find(|x| x.name() == s)
    }
}

#[derive(Debug, Clone, Default)]
struct ServiceDecl {
    kind: Option<Service>,
    detail: Option<String>,
    r: Vec<String>,
    w: Vec<String>,
    x: Vec<String>,
}

#[derive(Debug, Clone, Default)]
struct DependsOn {
    crates: Vec<String>,
    services: Vec<ServiceDecl>,
    env: BTreeSet<String>,
    /// `env=[…, refused:NAME]` — a key the module names only to refuse it at boot (a removed
    /// configuration key, ADR-0060 E7); `env_vars.md` lists it as `refused-at-boot`, never as config.
    refused_env: BTreeSet<String>,
    modules: Vec<String>,
}

#[derive(Debug, Clone)]
struct Exemption {
    path: String,
    line: usize,
    /// `None` = whole file; `Some(l)` = findings on line `l`.
    target: Option<usize>,
    rule: String,
    reason: String,
    used: usize,
}

impl Exemption {
    /// Known, exemptable rule id and a real reason (maintainability.md §6).
    fn valid(&self) -> bool {
        RULES.contains(&self.rule.as_str())
            && !UNEXEMPTABLE.contains(&self.rule.as_str())
            && !self.reason.is_empty()
            && self.reason != "TODO"
    }
}

#[derive(Debug, Default)]
struct Header {
    start_line: usize,
    purpose: Option<(usize, String)>,
    /// Field 1's sentence, continuation lines joined.
    sentence: String,
    /// `§x.y` / `ADR-nnnn` references cited by the prose below the fields.
    prose_refs: Vec<String>,
    depends: Option<DependsOn>,
    called_by: Option<Vec<String>>,
    invariants: Option<String>,
    spec: Option<(usize, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Finding {
    path: String,
    line: usize,
    rule: &'static str,
    msg: String,
}

fn finding(path: &str, line: usize, rule: &'static str, msg: impl Into<String>) -> Finding {
    Finding {
        path: path.to_string(),
        line,
        rule,
        msg: msg.into(),
    }
}

/// Split on `sep` at bracket depth 0 (`[]`, `()`, `{}`).
fn split_top(s: &str, sep: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut cur = String::new();
    let mut i = 0;
    while i < s.len() {
        let rest = &s[i..];
        if depth == 0 && rest.starts_with(sep) {
            out.push(std::mem::take(&mut cur));
            i += sep.len();
            continue;
        }
        let ch = rest.chars().next().unwrap_or(' ');
        match ch {
            '[' | '(' | '{' => depth += 1,
            ']' | ')' | '}' => depth -= 1,
            _ => {}
        }
        cur.push(ch);
        i += ch.len_utf8();
    }
    out.push(cur);
    out
}

/// `[a, b]` → items (`[]` → empty); `None` if not bracketed.
fn bracket_list(s: &str) -> Option<Vec<String>> {
    let inner = s.trim().strip_prefix('[')?.strip_suffix(']')?;
    if inner.trim().is_empty() {
        return Some(Vec::new());
    }
    Some(
        split_top(inner, ",")
            .into_iter()
            .map(|x| x.trim().to_string())
            .filter(|x| !x.is_empty())
            .collect(),
    )
}

/// `A_{X,Y}_{P,Q}` → cartesian expansion.
fn brace_expand(s: &str) -> Vec<String> {
    let Some(open) = s.find('{') else {
        return vec![s.to_string()];
    };
    let Some(close) = s[open..].find('}').map(|n| open + n) else {
        return vec![s.to_string()];
    };
    let (pre, alts, post) = (&s[..open], &s[open + 1..close], &s[close + 1..]);
    alts.split(',')
        .flat_map(|alt| brace_expand(&format!("{pre}{}{post}", alt.trim())))
        .collect()
}

fn parse_service(item: &str) -> Result<ServiceDecl, String> {
    let item = item.trim();
    let name_end = item.find(['(', ' ']).unwrap_or(item.len());
    let kind = Service::parse(&item[..name_end])
        .ok_or_else(|| format!("unknown service `{}`", &item[..name_end]))?;
    let mut decl = ServiceDecl {
        kind: Some(kind),
        ..ServiceDecl::default()
    };
    let mut rest = &item[name_end..];
    if rest.starts_with('(') {
        let close = rest
            .find(')')
            .ok_or_else(|| format!("unclosed `(` in `{item}`"))?;
        decl.detail = Some(rest[1..close].trim().to_string());
        rest = &rest[close + 1..];
    }
    let mut rest = rest.trim();
    while !rest.is_empty() {
        let Some((key, tail)) = rest.split_once('=') else {
            return Err(format!("unexpected `{rest}` in `{item}`"));
        };
        let key = key.trim();
        if kind != Service::PostgreSQL || !matches!(key, "r" | "w" | "x") {
            return Err(format!("`{key}=` is not allowed on {}", kind.name()));
        }
        let tail = tail.trim_start();
        if !tail.starts_with('[') {
            return Err(format!("`{key}=` needs a [list] in `{item}`"));
        }
        let close = matching(tail.as_bytes(), 0);
        let list = bracket_list(&tail[..=close]).unwrap_or_default();
        match key {
            "r" => decl.r = list,
            "w" => decl.w = list,
            _ => decl.x = list,
        }
        rest = tail[close + 1..].trim();
    }
    Ok(decl)
}

/// Prefix of an `env=[…]` item naming a key that is refused at boot, not read (see [`DependsOn`]).
const REFUSED_ENV: &str = "refused:";

fn parse_depends(value: &str) -> Result<(DependsOn, Vec<String>), String> {
    let parts = split_top(value, "; ");
    let keys = ["crates=", "services=", "env=", "modules="];
    if parts.len() != 4 {
        return Err(format!(
            "Depends-on needs exactly `crates=[…]; services=[…]; env=[…]; modules=[…]` (found {} parts)",
            parts.len()
        ));
    }
    let mut lists = Vec::new();
    for (part, key) in parts.iter().zip(keys) {
        let body = part
            .trim()
            .strip_prefix(key)
            .ok_or_else(|| format!("expected `{key}[…]`, found `{}`", part.trim()))?;
        lists.push(bracket_list(body).ok_or_else(|| format!("`{key}` value must be a [list]"))?);
    }
    let mut vocab_errors = Vec::new();
    let services = lists[1]
        .iter()
        .filter_map(|s| match parse_service(s) {
            Ok(d) => Some(d),
            Err(e) => {
                vocab_errors.push(e);
                None
            }
        })
        .collect();
    let (refused, env): (Vec<String>, Vec<String>) = lists[2]
        .iter()
        .flat_map(|e| brace_expand(e))
        .partition(|e| e.starts_with(REFUSED_ENV));
    Ok((
        DependsOn {
            crates: lists[0].clone(),
            services,
            env: env.into_iter().collect(),
            refused_env: refused
                .iter()
                .map(|e| e[REFUSED_ENV.len()..].to_string())
                .collect(),
            modules: lists[3].clone(),
        },
        vocab_errors,
    ))
}

/// Baseline heading numbers (`^#+ N(.N)*[ .]`).
fn baseline_sections(text: &str) -> BTreeSet<String> {
    text.lines()
        .filter_map(|l| {
            let rest = l.trim_start_matches('#');
            if rest.len() == l.len() {
                return None;
            }
            let rest = rest.strip_prefix(' ')?;
            let num: String = rest
                .chars()
                .take_while(|c| c.is_ascii_digit() || *c == '.')
                .collect();
            let after = rest[num.len()..].chars().next();
            let num = num.trim_end_matches('.').to_string();
            (!num.is_empty() && (after == Some(' ') || rest[..].starts_with(&format!("{num}."))))
                .then_some(num)
        })
        .collect()
}

struct SpecIndex {
    sections: BTreeSet<String>,
    adrs: BTreeSet<String>,
}

/// Every `ADR-nnnn` and `§x.y` token of `text`, in order.
fn spec_refs(text: &str) -> Vec<String> {
    let mut out: Vec<(usize, String)> = Vec::new();
    for (p, _) in text.match_indices("ADR-") {
        let num: String = text[p + 4..]
            .chars()
            .take_while(char::is_ascii_digit)
            .collect();
        if num.len() == 4 {
            out.push((p, format!("ADR-{num}")));
        }
    }
    for (p, _) in text.match_indices('§') {
        let num: String = text[p + '§'.len_utf8()..]
            .chars()
            .take_while(|c| c.is_ascii_digit() || *c == '.')
            .collect();
        let num = num.trim_end_matches('.');
        if !num.is_empty() {
            out.push((p, format!("§{num}")));
        }
    }
    out.sort();
    out.into_iter().map(|(_, r)| r).collect()
}

impl SpecIndex {
    /// `ADR-nnnn` has a docs/adr file, `§x.y` is a Baseline heading.
    fn resolves(&self, r: &str) -> bool {
        match (r.strip_prefix("ADR-"), r.strip_prefix('§')) {
            (Some(n), _) => self.adrs.contains(n),
            (_, Some(n)) => self.sections.contains(n),
            _ => false,
        }
    }
}

fn check_spec(value: &str, idx: &SpecIndex) -> Vec<String> {
    let v = value.trim();
    if v == "none" {
        return Vec::new();
    }
    let mut bad = Vec::new();
    let mut found = 0;
    for (p, _) in v.match_indices("ADR-") {
        let num: String = v[p + 4..]
            .chars()
            .take_while(char::is_ascii_digit)
            .collect();
        if num.len() == 4 {
            found += 1;
            if !idx.adrs.contains(&num) {
                bad.push(format!("ADR-{num} has no docs/adr/{num}-*.md"));
            }
        }
    }
    for (p, _) in v.match_indices('§') {
        let num: String = v[p + '§'.len_utf8()..]
            .chars()
            .take_while(|c| c.is_ascii_digit() || *c == '.')
            .collect();
        let num = num.trim_end_matches('.');
        if num.is_empty() {
            continue;
        }
        found += 1;
        if !idx.sections.contains(num) {
            bad.push(format!("§{num} is not a Baseline_2.9.md heading"));
        }
    }
    if found == 0 {
        bad.push("no `ADR-nnnn` / `§x.y` reference (write `none` if there is none)".into());
    }
    bad
}

/// Parse the header block; returns the header, header findings and file-scoped exemptions.
#[allow(clippy::too_many_lines)] // the five fields in grammar order, one pass
fn parse_header(path: &str, text: &str) -> (Header, Vec<Finding>, Vec<Exemption>) {
    let lines: Vec<&str> = text.lines().collect();
    let mut findings = Vec::new();
    let mut exemptions = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        let t = lines[i].trim_start();
        if t.is_empty() || (t.starts_with("//") && !t.starts_with("///") && !t.starts_with("//!")) {
            i += 1;
            continue;
        }
        break;
    }
    let start = i;
    let mut end = start;
    while end < lines.len() && lines[end].starts_with("//!") {
        end += 1;
    }
    let mut header = Header {
        start_line: if end > start { start + 1 } else { 1 },
        ..Header::default()
    };
    let block = &lines[start..end];
    for (k, l) in block.iter().enumerate() {
        if let Some(rest) = l.strip_prefix("//!").map(str::trim_start)
            && let Some(body) = rest.strip_prefix("dep-map: allow ")
        {
            let (rule, reason) = body.split_once(" — ").unwrap_or((body, ""));
            exemptions.push(Exemption {
                path: path.to_string(),
                line: start + k + 1,
                target: None,
                rule: rule.trim().to_string(),
                reason: reason.trim().to_string(),
                used: 0,
            });
        }
    }
    let field_of = |l: &str| -> Option<(usize, String)> {
        let rest = l.strip_prefix("//! ")?;
        FIELDS.iter().enumerate().find_map(|(n, f)| {
            rest.strip_prefix(f)
                .and_then(|r| r.strip_prefix(": "))
                .map(|v| (n, v.trim().to_string()))
        })
    };
    let mut k = 0;
    if let Some(first) = block.first() {
        let purpose = first.strip_prefix("//! `").and_then(|r| {
            let (p, rest) = r.split_once('`')?;
            let sentence = rest.strip_prefix(" — ")?;
            (!p.is_empty() && !sentence.trim().is_empty())
                .then(|| (p.to_string(), sentence.trim().to_string()))
        });
        match purpose {
            Some((p, mut sentence)) => {
                header.purpose = Some((start + 1, p));
                k = 1;
                while let Some(cont) = block.get(k).and_then(|l| l.strip_prefix("//!  ")) {
                    if field_of(block[k]).is_some() {
                        break;
                    }
                    sentence.push(' ');
                    sentence.push_str(cont.trim());
                    k += 1;
                }
                header.sentence = sentence;
            }
            None => {
                findings.push(finding(
                    path,
                    header.start_line,
                    "header-path",
                    "first header line must be ``//! `<canonical path>` — <sentence>``",
                ));
                if field_of(first).is_none() {
                    k = 1;
                }
            }
        }
    } else {
        findings.push(finding(
            path,
            1,
            "header-path",
            "no `//!` header block (first line must be ``//! `<canonical path>` — <sentence>``)",
        ));
    }
    let mut values: [Option<(usize, String)>; 4] = Default::default();
    for (n, name) in FIELDS.iter().enumerate() {
        match block.get(k).and_then(|l| field_of(l)) {
            Some((m, mut v)) if m == n => {
                let line = start + k + 1;
                k += 1;
                while let Some(cont) = block.get(k).and_then(|l| l.strip_prefix("//!  ")) {
                    if field_of(block[k]).is_some() {
                        break;
                    }
                    v.push(' ');
                    v.push_str(cont.trim());
                    k += 1;
                }
                values[n] = Some((line, v));
            }
            _ => {
                let elsewhere = block
                    .iter()
                    .any(|l| field_of(l).is_some_and(|(m, _)| m == n));
                let what = if elsewhere { "out of order" } else { "missing" };
                findings.push(finding(
                    path,
                    header.start_line,
                    "header-field",
                    format!(
                        "`{name}:` {what} (order: purpose, Depends-on, Called-by, Invariants, Spec)"
                    ),
                ));
            }
        }
    }
    if let Some((line, v)) = &values[0] {
        match parse_depends(v) {
            Ok((d, vocab)) => {
                for e in vocab {
                    findings.push(finding(path, *line, "service-vocab", e));
                }
                header.depends = Some(d);
            }
            Err(e) => findings.push(finding(path, *line, "header-field", e)),
        }
    }
    if let Some((line, v)) = &values[1] {
        match bracket_list(v) {
            Some(list) => header.called_by = Some(list),
            None => findings.push(finding(
                path,
                *line,
                "header-field",
                "`Called-by:` must be a [list]",
            )),
        }
    }
    if let Some((line, v)) = &values[2] {
        if v.starts_with('[') && v.ends_with(']') {
            header.invariants = Some(v[1..v.len() - 1].trim().to_string());
        } else {
            findings.push(finding(
                path,
                *line,
                "header-field",
                "`Invariants:` must be [free text]",
            ));
        }
    }
    header.spec = values[3].clone();
    header.prose_refs = block[k.min(block.len())..]
        .iter()
        .flat_map(|l| spec_refs(l))
        .collect();
    (header, findings, exemptions)
}

// ============================================================================
// Call sites and tags
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Class {
    PgConnect,
    PgTxn,
    PgPoolExec,
    PgRoleSwitch,
    QdrantRequest,
    HttpSend,
    EgressCall,
    Uds,
    Tcp,
    Subprocess,
}

impl Class {
    fn id(self) -> &'static str {
        match self {
            Class::PgConnect => "pg-connect",
            Class::PgTxn => "pg-txn",
            Class::PgPoolExec => "pg-pool-exec",
            Class::PgRoleSwitch => "pg-role-switch",
            Class::QdrantRequest => "qdrant-request",
            Class::HttpSend => "http-send",
            Class::EgressCall => "egress-call",
            Class::Uds => "uds",
            Class::Tcp => "tcp",
            Class::Subprocess => "subprocess",
        }
    }

    fn compatible(self) -> &'static [Service] {
        use Service as S;
        match self {
            Class::PgConnect | Class::PgTxn | Class::PgPoolExec | Class::PgRoleSwitch => {
                &[S::PostgreSQL]
            }
            Class::QdrantRequest => &[S::Qdrant],
            Class::HttpSend => &[S::Http, S::Qdrant, S::MiniMax, S::DashScope],
            Class::EgressCall => &[S::MiniMax, S::DashScope, S::Http],
            Class::Uds => &[S::Uds],
            Class::Tcp => &[S::PostgreSQL, S::Qdrant, S::Http],
            Class::Subprocess => &[S::Subprocess],
        }
    }

    /// The service a match proves without a tag (test-target facts); `tcp` proves none.
    fn implied(self) -> Option<Service> {
        match self {
            Class::Tcp => None,
            Class::EgressCall | Class::HttpSend => Some(Service::Http),
            c => c.compatible().first().copied(),
        }
    }
}

/// Text of the first call argument after the `(` at `open` (depth-0 `,` or `)`).
fn first_arg(b: &[u8], open: usize) -> &[u8] {
    let mut depth = 0i32;
    for k in open + 1..b.len() {
        match b[k] {
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' if depth == 0 => return &b[open + 1..k],
            b')' | b']' | b'}' => depth -= 1,
            b',' if depth == 0 => return &b[open + 1..k],
            _ => {}
        }
    }
    &b[open + 1..]
}

fn is_role_switch(lit: &str) -> bool {
    let words: Vec<String> = lit
        .split_whitespace()
        .map(str::to_ascii_uppercase)
        .collect();
    words.windows(2).any(|w| w[0] == "SET" && w[1] == "ROLE")
        || words
            .windows(3)
            .any(|w| w[0] == "SET" && w[1] == "LOCAL" && w[2] == "ROLE")
}

/// Every call-site match: `(line, class)`.
fn call_sites(lex: &Lexed) -> Vec<(usize, Class)> {
    let m = &lex.masked;
    let b = m.as_bytes();
    let mut out = Vec::new();
    let fixed: [(&str, Class); 10] = [
        ("Client::connect(", Class::PgConnect),
        ("DbPool::connect(", Class::PgConnect),
        ("PgPoolOptions::new(", Class::PgConnect),
        (".begin()", Class::PgTxn),
        (".build_transaction()", Class::PgTxn),
        ("IntraCellRequest {", Class::QdrantRequest),
        (".send()", Class::HttpSend),
        ("UnixStream::connect", Class::Uds),
        ("UnixListener::bind", Class::Uds),
        ("TcpStream::connect", Class::Tcp),
    ];
    for (pat, class) in fixed {
        for (p, _) in m.match_indices(pat) {
            out.push((lex.line_of(p), class));
        }
    }
    for (p, _) in m.match_indices("Command::new(") {
        if p > 0 && is_ident(b[p - 1]) {
            continue; // `StartManualContributionCommand::new(` is not a spawn
        }
        out.push((lex.line_of(p), Class::Subprocess));
    }
    for pat in [
        ".execute(",
        ".fetch_one(",
        ".fetch_all(",
        ".fetch_optional(",
        ".fetch(",
    ] {
        for (p, _) in m.match_indices(pat) {
            let arg = first_arg(b, p + pat.len() - 1);
            if String::from_utf8_lossy(arg).contains("pool") {
                out.push((lex.line_of(p), Class::PgPoolExec));
            }
        }
    }
    for (p, _) in m.match_indices(".call(") {
        if String::from_utf8_lossy(first_arg(b, p + 5)).contains("permit") {
            out.push((lex.line_of(p), Class::EgressCall));
        }
    }
    for lit in &lex.lits {
        if is_role_switch(&lit.text) {
            out.push((lit.line, Class::PgRoleSwitch));
        }
    }
    out.sort();
    out.dedup();
    out
}

/// Typed pool → the role its connection must report (`adapters::postgres::ROLE_*`).
const POOL_ROLES: [(&str, &str); 8] = [
    ("Runtime", "role_gateway"),
    ("BatchIssuer", "role_batch_issuer"),
    ("Consolidation", "role_consolidation_worker"),
    ("PrivateWorker", "role_private_worker"),
    ("RetrievalWorker", "role_retrieval_worker"),
    ("Maintenance", "role_maintenance"),
    ("PublicWorker", "role_public_worker"),
    ("Admin", "role_admin"),
];

/// Tag detail the code itself fixes at a call site: `(line, class, detail)`; a detail of
/// `!serve` means "anything but `serve`" (a UDS client names its peer).
fn site_details(lex: &Lexed) -> Vec<(usize, Class, String)> {
    let m = &lex.masked;
    let b = m.as_bytes();
    let mut out = Vec::new();
    for (p, _) in m.match_indices("DbPool::connect(") {
        let mut s = p;
        while s > 0 && is_ident(b[s - 1]) {
            s -= 1;
        }
        if let Some((_, role)) = POOL_ROLES.iter().find(|(t, _)| *t == &m[s..p]) {
            out.push((lex.line_of(p), Class::PgConnect, (*role).to_string()));
        }
    }
    for lit in &lex.lits {
        let words: Vec<&str> = lit.text.split_whitespace().collect();
        for (i, w) in words.iter().enumerate() {
            if w.eq_ignore_ascii_case("role")
                && i > 0
                && (words[i - 1].eq_ignore_ascii_case("set")
                    || words[i - 1].eq_ignore_ascii_case("local"))
                && let Some(next) = words.get(i + 1)
            {
                let role = next.trim_matches(|c: char| c == '"' || c == ';' || c == '\\');
                if role.starts_with("role_") && role.bytes().all(is_ident) {
                    out.push((lit.line, Class::PgRoleSwitch, role.to_string()));
                }
            }
        }
    }
    for (p, _) in m.match_indices("UnixListener::bind") {
        out.push((lex.line_of(p), Class::Uds, "serve".into()));
    }
    for (p, _) in m.match_indices("UnixStream::connect") {
        out.push((lex.line_of(p), Class::Uds, "!serve".into()));
    }
    for (p, _) in m.match_indices("Command::new(") {
        if p > 0 && is_ident(b[p - 1]) {
            continue;
        }
        let at = skip_ws(b, p + "Command::new(".len());
        if let Some(lit) = lex.lit_at(at) {
            let base = lit.text.rsplit('/').next().unwrap_or(&lit.text);
            out.push((lex.line_of(p), Class::Subprocess, base.to_string()));
        }
    }
    out
}

/// Generic words that do not name a program (`subprocess(<program>)`).
const GENERIC_PROGRAMS: [&str; 8] = [
    "proc", "process", "binary", "cmd", "command", "exec", "program", "child",
];

/// Closed detail vocabulary per service (maintainability.md §1.2); `processes` are the
/// workspace binaries' short names (UDS peers).
fn detail_error(
    kind: Service,
    detail: Option<&str>,
    processes: &BTreeSet<String>,
) -> Option<String> {
    let d = detail.map(str::trim).filter(|d| !d.is_empty());
    let name = kind.name();
    match (kind, d) {
        (Service::MiniMax | Service::DashScope, None) => None,
        (Service::MiniMax | Service::DashScope, Some(d)) => {
            Some(format!("`{name}({d})`: {name} takes no detail"))
        }
        (_, None) => Some(format!(
            "`{name}` needs a `({})` detail",
            match kind {
                Service::PostgreSQL => "<role>",
                Service::Qdrant => "<collection or *>",
                Service::Uds => "serve|<peer process>",
                Service::Subprocess => "<program>",
                Service::Http => "<peer>",
                _ => "<what>",
            }
        )),
        (Service::PostgreSQL, Some(d)) => {
            let known = d == "any"
                || d == "owner"
                || crate::rls_check::RUNTIME_ROLES.contains(&d)
                || crate::rls_check::NON_RUNTIME_ROLES.contains(&d);
            (!known).then(|| format!("`PostgreSQL({d})`: not a role (role_*, `owner` or `any`)"))
        }
        (Service::Uds, Some(d)) => (d != "serve" && !processes.contains(d)).then(|| {
            format!(
                "`UDS({d})`: a server writes `UDS(serve)`, a client names its peer {}",
                fmt_set(processes)
            )
        }),
        (Service::Subprocess, Some(d)) => GENERIC_PROGRAMS
            .contains(&d)
            .then(|| format!("`subprocess({d})`: name the program that is spawned")),
        (Service::Http, Some(d)) => d.starts_with("humaux-").then(|| {
            format!(
                "`HTTP({d})`: write the process short name `{}`",
                &d["humaux-".len()..]
            )
        }),
        _ => None,
    }
}

#[derive(Debug, Clone)]
struct Tag {
    line: usize,
    parsed: Option<(Service, Option<String>)>,
}

/// `// dep: <Service>[(<detail>)] — <why>` on a line of its own.
fn parse_tag(line: &str) -> Option<(Service, Option<String>)> {
    let rest = line.trim_start().strip_prefix("// dep: ")?;
    let name_end = rest.find(['(', ' ']).unwrap_or(rest.len());
    let service = Service::parse(&rest[..name_end])?;
    let mut tail = &rest[name_end..];
    let mut detail = None;
    if let Some(r) = tail.strip_prefix('(') {
        let close = r.find(')')?;
        detail = Some(r[..close].to_string());
        tail = &r[close + 1..];
    }
    let why = tail.strip_prefix(" — ")?;
    why.starts_with(|c: char| !c.is_whitespace())
        .then_some((service, detail))
}

fn tags(text: &str, lex: &Lexed) -> Vec<Tag> {
    lex.comments
        .iter()
        .filter(|c| c.kind == CommentKind::Line && c.text.starts_with(" dep:"))
        .filter(|c| lex.masked_line(c.line).trim().is_empty())
        .map(|c| Tag {
            line: c.line,
            parsed: parse_tag(text.lines().nth(c.line - 1).unwrap_or("")),
        })
        .collect()
}

/// First line of the statement containing `line` (walk up while the previous code line
/// does not end in `;`, `{`, `}`).
fn stmt_start(lex: &Lexed, line: usize) -> usize {
    let mut s = line;
    loop {
        let mut p = s - 1;
        while p >= 1 && lex.masked_line(p).trim().is_empty() {
            p -= 1;
        }
        if p == 0 {
            return s;
        }
        let t = lex.masked_line(p).trim_end();
        if t.ends_with(';') || t.ends_with('{') || t.ends_with('}') {
            return s;
        }
        s = p;
    }
}

// ============================================================================
// Computed facts: env, tables
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum EnvKind {
    Build,
    Child,
    Runtime,
}

impl EnvKind {
    fn name(self) -> &'static str {
        match self {
            EnvKind::Build => "build-time",
            EnvKind::Child => "child",
            EnvKind::Runtime => "runtime",
        }
    }
}

fn is_humaux_name(s: &str) -> bool {
    s.strip_prefix("HUMAUX_").is_some_and(|rest| {
        rest.bytes()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == b'_')
    }) && s
        .bytes()
        .last()
        .is_some_and(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
}

/// name → (first line, kinds)
fn env_found(lex: &Lexed) -> BTreeMap<String, (usize, BTreeSet<EnvKind>)> {
    let m = &lex.masked;
    let b = m.as_bytes();
    let mut out: BTreeMap<String, (usize, BTreeSet<EnvKind>)> = BTreeMap::new();
    let mut claimed = BTreeSet::new();
    let pats: [(&str, EnvKind); 8] = [
        ("option_env!(", EnvKind::Build),
        ("env!(", EnvKind::Build),
        ("env::var_os(", EnvKind::Runtime),
        ("env::var(", EnvKind::Runtime),
        ("var_os(", EnvKind::Runtime),
        ("env::set_var(", EnvKind::Runtime),
        ("env::remove_var(", EnvKind::Runtime),
        (".env(", EnvKind::Child),
    ];
    for (pat, kind) in pats {
        for (p, _) in m.match_indices(pat) {
            if pat == "env!(" && p > 0 && is_ident(b[p - 1]) {
                continue; // option_env!
            }
            if pat == "var_os(" && p > 0 && (is_ident(b[p - 1]) || b[p - 1] == b':') {
                continue; // env::var_os( counted above
            }
            let at = skip_ws(b, p + pat.len());
            if let Some(lit) = lex.lit_at(at)
                && claimed.insert(lit.start)
            {
                let e = out
                    .entry(lit.text.clone())
                    .or_insert((lit.line, BTreeSet::new()));
                e.0 = e.0.min(lit.line);
                e.1.insert(kind);
            }
        }
    }
    for lit in &lex.lits {
        if !claimed.contains(&lit.start) && is_humaux_name(&lit.text) {
            let e = out
                .entry(lit.text.clone())
                .or_insert((lit.line, BTreeSet::new()));
            e.0 = e.0.min(lit.line);
            e.1.insert(EnvKind::Runtime);
        }
    }
    out
}

#[derive(Debug, Default, Clone)]
struct DbTokens {
    relations: BTreeMap<String, usize>,
    functions: BTreeMap<String, usize>,
    writes: BTreeMap<String, usize>,
}

/// `schema.name` tokens of one string (schema ∈ [`SCHEMAS`]): `(token, is_function, pos)`.
fn schema_tokens(s: &str) -> Vec<(String, bool, usize)> {
    let b = s.as_bytes();
    let mut out = Vec::new();
    for schema in SCHEMAS {
        for (p, _) in s.match_indices(&format!("{schema}.")) {
            if p > 0 && (is_ident(b[p - 1]) || b[p - 1] == b'.') {
                continue;
            }
            let n = p + schema.len() + 1;
            if !b.get(n).is_some_and(u8::is_ascii_lowercase) {
                continue;
            }
            let mut e = n;
            while e < b.len()
                && (b[e].is_ascii_lowercase() || b[e].is_ascii_digit() || b[e] == b'_')
            {
                e += 1;
            }
            if b.get(e).is_some_and(|c| is_ident(*c)) {
                continue;
            }
            let is_fn = b.get(e) == Some(&b'(') && !relation_context(&s[..p]);
            out.push((s[p..e].to_string(), is_fn, p));
        }
    }
    out
}

/// Keywords after which `schema.name(` is a relation with a column list, not a call:
/// `INSERT INTO t(cols)`, `REFERENCES t(id)`, `CREATE TABLE [IF NOT EXISTS] t(…)`,
/// `CREATE INDEX … ON t(col)`, `COPY t(cols) FROM`, `MERGE INTO t`.
fn relation_context(before: &str) -> bool {
    let prev = before
        .trim_end()
        .rsplit(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    matches!(
        prev.as_str(),
        "into" | "references" | "table" | "exists" | "on" | "copy" | "update" | "only"
    )
}

fn db_tokens(lex: &Lexed) -> DbTokens {
    let mut t = DbTokens::default();
    for lit in &lex.lits {
        let norm = lit
            .text
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_ascii_lowercase();
        for (tok, is_fn, _) in schema_tokens(&norm) {
            let map = if is_fn {
                &mut t.functions
            } else {
                &mut t.relations
            };
            map.entry(tok).or_insert(lit.line);
        }
        for verb in [
            "insert into ",
            "update ",
            "delete from ",
            "merge into ",
            "truncate table ",
            "truncate ",
            "copy ",
        ] {
            for (p, _) in norm.match_indices(verb) {
                if p > 0 && is_ident(norm.as_bytes()[p - 1]) {
                    continue;
                }
                let after = &norm[p + verb.len()..];
                // Directly after a write verb a token is the written relation even when a
                // column list follows (`INSERT INTO t(cols)`).
                let Some((tok, _, 0)) = schema_tokens(after).into_iter().min_by_key(|x| x.2) else {
                    continue;
                };
                if verb == "copy " && !after[tok.len()..].contains(" from") {
                    continue;
                }
                t.writes.entry(tok).or_insert(lit.line);
            }
        }
    }
    t
}

// ============================================================================
// Inventory
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum DepKind {
    Normal,
    Dev,
    Build,
}

#[derive(Debug, Clone)]
struct Dep {
    key: String,
    ident: String,
    pkg: String,
}

#[derive(Debug, Clone)]
struct CargoEntry {
    kind: DepKind,
    key: String,
    line: usize,
    why: Option<(String, Vec<String>)>,
}

#[derive(Debug)]
struct Package {
    name: String,
    manifest_rel: String,
    deps: Vec<Dep>,
    entries: Vec<CargoEntry>,
    toml_keys: BTreeSet<(DepKind, String)>,
    exemptions: Vec<Exemption>,
    grammar: Vec<Finding>,
    lib: Option<usize>,
    main: Option<usize>,
    build: Option<usize>,
}

impl Package {
    fn short(&self) -> &str {
        self.name.strip_prefix("humaux-").unwrap_or(&self.name)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    Lib,
    Main,
    Build,
    TestTop,
    TestSub,
    Src,
}

#[derive(Debug)]
struct SrcFile {
    rel: String,
    pkg: usize,
    canonical: String,
    role: Role,
    text: String,
    lex: Lexed,
    test_ranges: Vec<(usize, usize)>,
    all_test: bool,
    is_test: bool,
    inline_mods: Vec<(usize, usize, String)>,
    mod_decls: Vec<(String, usize, Option<String>)>,
    uses: Vec<UseStmt>,
    root: Option<usize>,
    mod_path: Vec<String>,
    header: Header,
    header_findings: Vec<Finding>,
    exemptions: Vec<Exemption>,
}

impl SrcFile {
    fn in_test(&self, pos: usize) -> bool {
        self.all_test || self.is_test || self.test_ranges.iter().any(|&(s, e)| s <= pos && pos < e)
    }
}

/// Everything `--check` / `--write` / `--suggest` needs, computed once.
struct Analysis {
    root: PathBuf,
    packages: Vec<Package>,
    files: Vec<SrcFile>,
    fixtures: Vec<String>,
    /// file → target → (non-test edge, test edge)
    edges: Vec<BTreeMap<usize, (bool, bool)>>,
    /// file → package name → (non-test, test)
    crates: Vec<BTreeMap<String, (bool, bool)>>,
    env: Vec<BTreeMap<String, (usize, BTreeSet<EnvKind>)>>,
    db: Vec<DbTokens>,
    sites: Vec<Vec<(usize, Class)>>,
    tags: Vec<Vec<Tag>>,
    /// process name → reachable files (root included)
    processes: BTreeMap<String, BTreeSet<usize>>,
    spec: SpecIndex,
}

fn rel(root: &Path, p: &Path) -> String {
    p.strip_prefix(root)
        .unwrap_or(p)
        .to_string_lossy()
        .replace('\\', "/")
}

fn rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    let mut entries: Vec<PathBuf> = entries.flatten().map(|e| e.path()).collect();
    entries.sort();
    for p in entries {
        let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if p.is_dir() {
            if name != "target" && !name.starts_with('.') {
                rs_files(&p, out);
            }
        } else if name.ends_with(".rs") {
            out.push(p);
        }
    }
}

fn members(root: &Path) -> Result<Vec<PathBuf>, String> {
    let text =
        fs::read_to_string(root.join("Cargo.toml")).map_err(|e| format!("Cargo.toml: {e}"))?;
    let v: toml::Value = toml::from_str(&text).map_err(|e| format!("Cargo.toml: {e}"))?;
    let globs = v
        .get("workspace")
        .and_then(|w| w.get("members"))
        .and_then(|m| m.as_array())
        .ok_or("Cargo.toml: no [workspace] members")?;
    let mut out = Vec::new();
    for g in globs.iter().filter_map(|g| g.as_str()) {
        if let Some(dir) = g.strip_suffix("/*") {
            let mut dirs: Vec<PathBuf> = fs::read_dir(root.join(dir))
                .into_iter()
                .flatten()
                .flatten()
                .map(|e| e.path())
                .filter(|p| p.join("Cargo.toml").is_file())
                .collect();
            dirs.sort();
            out.extend(dirs);
        } else if root.join(g).join("Cargo.toml").is_file() {
            out.push(root.join(g));
        }
    }
    Ok(out)
}

fn dep_kind_of_section(name: &str) -> Option<DepKind> {
    let tail = match name.strip_prefix("target.") {
        Some(rest) => rest.rsplit_once('.').map(|(_, t)| t)?,
        None => name,
    };
    match tail {
        "dependencies" => Some(DepKind::Normal),
        "dev-dependencies" => Some(DepKind::Dev),
        "build-dependencies" => Some(DepKind::Build),
        _ => None,
    }
}

fn parse_why(line: &str) -> Option<(String, Vec<String>)> {
    let rest = line.trim().strip_prefix("# why: ")?;
    let (reason, list) = rest.split_once("; used-by: ")?;
    let list = bracket_list(list)?;
    (!reason.trim().is_empty() && !list.is_empty()).then(|| (reason.trim().to_string(), list))
}

#[allow(clippy::too_many_lines)] // toml parse and line scan side by side (cargo-parse compares them)
fn parse_package(root: &Path, dir: &Path) -> Result<Package, String> {
    let manifest = dir.join("Cargo.toml");
    let manifest_rel = rel(root, &manifest);
    let text = fs::read_to_string(&manifest).map_err(|e| format!("{manifest_rel}: {e}"))?;
    let v: toml::Value = toml::from_str(&text).map_err(|e| format!("{manifest_rel}: {e}"))?;
    let name = v
        .get("package")
        .and_then(|p| p.get("name"))
        .and_then(|n| n.as_str())
        .ok_or_else(|| format!("{manifest_rel}: no package.name"))?
        .to_string();
    let mut deps = Vec::new();
    let mut toml_keys = BTreeSet::new();
    let mut tables: Vec<(DepKind, &toml::Value)> = Vec::new();
    for (sec, kind) in [
        ("dependencies", DepKind::Normal),
        ("dev-dependencies", DepKind::Dev),
        ("build-dependencies", DepKind::Build),
    ] {
        if let Some(t) = v.get(sec) {
            tables.push((kind, t));
        }
        if let Some(targets) = v.get("target").and_then(|t| t.as_table()) {
            for t in targets.values() {
                if let Some(t) = t.get(sec) {
                    tables.push((kind, t));
                }
            }
        }
    }
    for (kind, t) in tables {
        for (key, val) in t.as_table().into_iter().flatten() {
            let pkg = val
                .get("package")
                .and_then(|p| p.as_str())
                .unwrap_or(key)
                .to_string();
            toml_keys.insert((kind, key.clone()));
            deps.push(Dep {
                key: key.clone(),
                ident: key.replace('-', "_"),
                pkg,
            });
        }
    }
    let mut entries = Vec::new();
    let mut exemptions = Vec::new();
    let mut grammar = Vec::new();
    let mut section: Option<DepKind> = None;
    let lines: Vec<&str> = text.lines().collect();
    let mut pending_exemptions: Vec<usize> = Vec::new();
    for (n, l) in lines.iter().enumerate() {
        let t = l.trim();
        if t.starts_with('[') {
            let name = t
                .trim_start_matches('[')
                .trim_end_matches(']')
                .replace(['"', '\''], "");
            section = dep_kind_of_section(&name);
            continue;
        }
        if let Some(body) = t.strip_prefix("# dep-map: allow ") {
            let (rule, reason) = body.split_once(" — ").unwrap_or((body, ""));
            exemptions.push(Exemption {
                path: manifest_rel.clone(),
                line: n + 1,
                target: None,
                rule: rule.trim().to_string(),
                reason: reason.trim().to_string(),
                used: 0,
            });
            pending_exemptions.push(exemptions.len() - 1);
            continue;
        }
        let Some(kind) = section else { continue };
        if t.starts_with('#') || t.is_empty() {
            continue;
        }
        let key_end = t
            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '-'))
            .unwrap_or(t.len());
        let after = t[key_end..].trim_start();
        let after = after
            .strip_prefix('.')
            .map(|a| {
                a.trim_start_matches(|c: char| c.is_ascii_alphanumeric() || c == '-' || c == '_')
                    .trim_start()
            })
            .unwrap_or(after);
        if key_end == 0 || !after.starts_with('=') {
            continue;
        }
        let why = n.checked_sub(1).and_then(|p| parse_why(lines[p]));
        for e in pending_exemptions.drain(..) {
            exemptions[e].target = Some(n + 1);
        }
        entries.push(CargoEntry {
            kind,
            key: t[..key_end].to_string(),
            line: n + 1,
            why,
        });
    }
    for e in pending_exemptions {
        grammar.push(finding(
            &manifest_rel,
            exemptions[e].line,
            "exemption-grammar",
            "`# dep-map: allow` is not followed by a dependency entry",
        ));
    }
    Ok(Package {
        name,
        manifest_rel,
        deps,
        entries,
        toml_keys,
        exemptions,
        grammar,
        lib: None,
        main: None,
        build: None,
    })
}

fn canonical_of(pkg: &Package, in_pkg: &str) -> (String, Role) {
    let short = pkg.short();
    let segs = |rest: &str| -> Vec<String> {
        let mut s: Vec<String> = rest
            .trim_end_matches(".rs")
            .split('/')
            .map(String::from)
            .collect();
        if s.len() > 1 && s.last().is_some_and(|l| l == "mod") {
            s.pop();
        }
        s
    };
    match in_pkg {
        "src/lib.rs" => (pkg.name.clone(), Role::Lib),
        "src/main.rs" => (format!("{short}::main"), Role::Main),
        "build.rs" => (format!("{short}::build"), Role::Build),
        _ => {
            if let Some(rest) = in_pkg.strip_prefix("src/") {
                (format!("{short}::{}", segs(rest).join("::")), Role::Src)
            } else if let Some(rest) = in_pkg.strip_prefix("tests/") {
                let role = if rest.contains('/') {
                    Role::TestSub
                } else {
                    Role::TestTop
                };
                (format!("{short}::tests::{}", segs(rest).join("::")), role)
            } else {
                (format!("{short}::{}", segs(in_pkg).join("::")), Role::Src)
            }
        }
    }
}

fn normalize(p: &str) -> String {
    let mut out: Vec<&str> = Vec::new();
    for c in p.split('/') {
        match c {
            "" | "." => {}
            ".." => {
                out.pop();
            }
            c => out.push(c),
        }
    }
    out.join("/")
}

impl Analysis {
    fn new(root: &Path) -> Result<Analysis, String> {
        let root = root.to_path_buf();
        let mut packages = Vec::new();
        let mut files: Vec<SrcFile> = Vec::new();
        let mut fixtures = Vec::new();
        for dir in members(&root)? {
            let mut pkg = parse_package(&root, &dir)?;
            let pi = packages.len();
            let mut paths = Vec::new();
            rs_files(&dir, &mut paths);
            for p in paths {
                let r = rel(&root, &p);
                let in_pkg = rel(&dir, &p);
                if in_pkg.starts_with("tests/ui/") {
                    fixtures.push(r);
                    continue;
                }
                let text = fs::read_to_string(&p).map_err(|e| format!("{r}: {e}"))?;
                let (canonical, role) = canonical_of(&pkg, &in_pkg);
                let lex = lex(&text);
                let (test_ranges, all_test) = test_ranges(&lex.masked);
                let (header, header_findings, mut exemptions) = parse_header(&r, &text);
                exemptions.extend(line_exemptions(&r, &lex));
                let idx = files.len();
                match role {
                    Role::Lib => pkg.lib = Some(idx),
                    Role::Main => pkg.main = Some(idx),
                    Role::Build => pkg.build = Some(idx),
                    _ => {}
                }
                files.push(SrcFile {
                    is_test: matches!(role, Role::TestTop | Role::TestSub),
                    inline_mods: inline_mods(&lex.masked),
                    mod_decls: mod_decls(&lex),
                    uses: use_stmts(&lex.masked),
                    rel: r,
                    pkg: pi,
                    canonical,
                    role,
                    text,
                    lex,
                    test_ranges,
                    all_test,
                    root: None,
                    mod_path: Vec::new(),
                    header,
                    header_findings,
                    exemptions,
                });
            }
            packages.push(pkg);
        }
        let spec = SpecIndex {
            sections: baseline_sections(
                &fs::read_to_string(root.join("docs/architecture/Baseline_2.9.md"))
                    .unwrap_or_default(),
            ),
            adrs: fs::read_dir(root.join("docs/adr"))
                .into_iter()
                .flatten()
                .flatten()
                .filter_map(|e| {
                    let n = e.file_name().to_string_lossy().into_owned();
                    let num = n.get(..4)?;
                    (n.ends_with(".md")
                        && num.bytes().all(|c| c.is_ascii_digit())
                        && n.as_bytes().get(4) == Some(&b'-'))
                    .then(|| num.to_string())
                })
                .collect(),
        };
        let n = files.len();
        let mut an = Analysis {
            root,
            packages,
            files,
            fixtures,
            edges: vec![BTreeMap::new(); n],
            crates: vec![BTreeMap::new(); n],
            env: Vec::new(),
            db: Vec::new(),
            sites: Vec::new(),
            tags: Vec::new(),
            processes: BTreeMap::new(),
            spec,
        };
        let ns = an.walk_modules();
        an.compute_edges(&ns);
        an.compute_crates();
        an.env = an.files.iter().map(|f| env_found(&f.lex)).collect();
        an.db = an.files.iter().map(|f| db_tokens(&f.lex)).collect();
        an.sites = an.files.iter().map(|f| call_sites(&f.lex)).collect();
        an.tags = an.files.iter().map(|f| tags(&f.text, &f.lex)).collect();
        an.compute_processes();
        Ok(an)
    }

    fn file_by_rel(&self) -> HashMap<&str, usize> {
        self.files
            .iter()
            .enumerate()
            .map(|(i, f)| (f.rel.as_str(), i))
            .collect()
    }

    /// Module tree from every crate root; returns `(root, mod path) → file`.
    fn walk_modules(&mut self) -> HashMap<(usize, Vec<String>), usize> {
        let by_rel: HashMap<String, usize> = self
            .files
            .iter()
            .enumerate()
            .map(|(i, f)| (f.rel.clone(), i))
            .collect();
        let mut ns = HashMap::new();
        let roots: Vec<usize> = (0..self.files.len())
            .filter(|&i| {
                matches!(
                    self.files[i].role,
                    Role::Lib | Role::Main | Role::Build | Role::TestTop
                )
            })
            .collect();
        for r in roots {
            let test = self.files[r].is_test;
            let mut stack = vec![(r, Vec::<String>::new(), test)];
            let mut seen = BTreeSet::new();
            while let Some((f, path, test)) = stack.pop() {
                if !seen.insert(f) {
                    continue;
                }
                ns.insert((r, path.clone()), f);
                if self.files[f].root.is_none() {
                    self.files[f].root = Some(r);
                    self.files[f].mod_path = path.clone();
                }
                self.files[f].is_test |= test;
                let file = &self.files[f];
                let dir = file.rel.rsplit_once('/').map_or("", |(d, _)| d).to_string();
                let owner = matches!(
                    file.role,
                    Role::Lib | Role::Main | Role::Build | Role::TestTop
                ) || file.rel.ends_with("/mod.rs");
                let stem = file.rel.trim_end_matches(".rs").to_string();
                let decls: Vec<(String, usize, Option<String>, bool)> = file
                    .mod_decls
                    .iter()
                    .map(|(name, pos, p)| (name.clone(), *pos, p.clone(), file.in_test(*pos)))
                    .collect();
                for (name, _, attr, in_test) in decls {
                    let candidates = match attr {
                        Some(p) => vec![normalize(&format!("{dir}/{p}"))],
                        None if owner => {
                            vec![format!("{dir}/{name}.rs"), format!("{dir}/{name}/mod.rs")]
                        }
                        None => vec![format!("{stem}/{name}.rs"), format!("{stem}/{name}/mod.rs")],
                    };
                    if let Some(&child) = candidates.iter().find_map(|c| by_rel.get(c)) {
                        let mut p = path.clone();
                        p.push(name);
                        stack.push((child, p, test || in_test));
                    }
                }
            }
        }
        // Files no root reaches: resolve against their package's lib (or main).
        for f in 0..self.files.len() {
            if self.files[f].root.is_none() {
                let pkg = &self.packages[self.files[f].pkg];
                self.files[f].root = pkg.lib.or(pkg.main);
            }
        }
        ns
    }

    fn lib_of_ident(&self, ident: &str) -> Option<usize> {
        self.packages
            .iter()
            .find(|p| p.name.replace('-', "_") == ident)
            .and_then(|p| p.lib)
    }

    /// Resolve `segs` (as written at `pos` in file `f`) to the longest module-file prefix,
    /// returning `(target, remaining segments)`.
    fn resolve_raw(
        &self,
        ns: &HashMap<(usize, Vec<String>), usize>,
        f: usize,
        pos: usize,
        segs: &[String],
    ) -> Option<(usize, usize, Vec<String>)> {
        let file = &self.files[f];
        let first = segs.first()?;
        let here = || -> Vec<String> {
            let mut p = file.mod_path.clone();
            let mut mods: Vec<&(usize, usize, String)> = file
                .inline_mods
                .iter()
                .filter(|(o, c, _)| *o < pos && pos < *c)
                .collect();
            mods.sort();
            p.extend(mods.into_iter().map(|(_, _, n)| n.clone()));
            p
        };
        let (root, full) = match first.as_str() {
            "crate" => (file.root?, segs[1..].to_vec()),
            "self" => {
                let mut p = here();
                p.extend_from_slice(&segs[1..]);
                (file.root?, p)
            }
            "super" => {
                let mut p = here();
                let mut k = 0;
                while segs.get(k).is_some_and(|s| s == "super") {
                    p.pop();
                    k += 1;
                }
                p.extend_from_slice(&segs[k..]);
                (file.root?, p)
            }
            s if s.starts_with("humaux_") => (self.lib_of_ident(s)?, segs[1..].to_vec()),
            s if file.mod_decls.iter().any(|(n, _, _)| n == s) => {
                let mut p = file.mod_path.clone();
                p.extend_from_slice(segs);
                (file.root?, p)
            }
            _ => return None,
        };
        (0..=full.len()).rev().find_map(|i| {
            ns.get(&(root, full[..i].to_vec()))
                .map(|&t| (t, root, full[i..].to_vec()))
        })
    }

    fn compute_edges(&mut self, ns: &HashMap<(usize, Vec<String>), usize>) {
        // Crate-root re-exports: name → leaf, and glob modules.
        let mut named: HashMap<usize, HashMap<String, usize>> = HashMap::new();
        let mut globs: HashMap<usize, Vec<usize>> = HashMap::new();
        let pub_names: Vec<BTreeSet<String>> = self
            .files
            .iter()
            .map(|f| pub_items(&f.lex.masked, &f.uses))
            .collect();
        for (i, f) in self.files.iter().enumerate() {
            if f.role != Role::Lib {
                continue;
            }
            for u in f.uses.iter().filter(|u| u.is_pub) {
                for leaf in &u.leaves {
                    let Some((t, _, _)) = self.resolve_raw(ns, i, u.pos, &leaf.path) else {
                        continue;
                    };
                    if t == i {
                        continue;
                    }
                    if leaf.glob {
                        globs.entry(i).or_default().push(t);
                    } else if let Some(name) =
                        leaf.alias.clone().or_else(|| leaf.path.last().cloned())
                    {
                        named.entry(i).or_default().insert(name, t);
                    }
                }
            }
        }
        let resolve = |f: usize, pos: usize, segs: &[String]| -> Option<usize> {
            let (t, root, rest) = self.resolve_raw(ns, f, pos, segs)?;
            if t == root && self.files[t].role == Role::Lib && !rest.is_empty() {
                if let Some(&leaf) = named.get(&t).and_then(|m| m.get(&rest[0])) {
                    return Some(leaf);
                }
                if let Some(&leaf) = globs
                    .get(&t)
                    .and_then(|g| g.iter().find(|m| pub_names[**m].contains(&rest[0])))
                {
                    return Some(leaf);
                }
            }
            Some(t)
        };
        let mut all = Vec::new();
        for (f, file) in self.files.iter().enumerate() {
            let mut out: BTreeMap<usize, (bool, bool)> = BTreeMap::new();
            let mut add = |t: usize, pos: usize| {
                if t != f {
                    let e = out.entry(t).or_default();
                    if file.in_test(pos) {
                        e.1 = true;
                    } else {
                        e.0 = true;
                    }
                }
            };
            for u in file.uses.iter().filter(|u| !u.is_pub) {
                for leaf in &u.leaves {
                    if let Some(t) = resolve(f, u.pos, &leaf.path) {
                        add(t, u.pos);
                    }
                }
            }
            let skip: Vec<(usize, usize)> = file.uses.iter().map(|u| (u.pos, u.end)).collect();
            for (pos, segs) in inline_paths(&file.lex.masked, &skip) {
                if let Some(t) = resolve(f, pos, &segs) {
                    add(t, pos);
                }
            }
            let dir = file.rel.rsplit_once('/').map_or("", |(d, _)| d);
            let by_rel = self.file_by_rel();
            for (_, pos, attr) in &file.mod_decls {
                if let Some(p) = attr
                    && let Some(&t) = by_rel.get(normalize(&format!("{dir}/{p}")).as_str())
                {
                    add(t, *pos);
                }
            }
            all.push(out);
        }
        self.edges = all;
    }

    fn compute_crates(&mut self) {
        for (f, file) in self.files.iter().enumerate() {
            let pkg = &self.packages[file.pkg];
            let deps: HashMap<&str, &Dep> =
                pkg.deps.iter().map(|d| (d.ident.as_str(), d)).collect();
            let mut out: BTreeMap<String, (bool, bool)> = BTreeMap::new();
            let mut note = |ident: &str, pos: usize| {
                if let Some(d) = deps.get(ident) {
                    let e = out.entry(d.pkg.clone()).or_default();
                    if file.in_test(pos) {
                        e.1 = true;
                    } else {
                        e.0 = true;
                    }
                }
            };
            for u in &file.uses {
                for leaf in &u.leaves {
                    let first = leaf.path.iter().find(|s| !s.is_empty());
                    if let Some(first) = first {
                        note(first, u.pos);
                    }
                }
            }
            let skip: Vec<(usize, usize)> = file.uses.iter().map(|u| (u.pos, u.end)).collect();
            for (pos, segs) in inline_paths(&file.lex.masked, &skip) {
                note(&segs[0], pos);
            }
            let b = file.lex.masked.as_bytes();
            for p in word_positions(&file.lex.masked, "extern") {
                let i = skip_ws(b, p + 6);
                if file.lex.masked[i..].starts_with("crate")
                    && let Some((ident, _)) = read_ident(b, skip_ws(b, i + 5))
                {
                    note(&ident, p);
                }
            }
            self.crates[f] = out;
        }
    }

    fn compute_processes(&mut self) {
        for pkg in &self.packages {
            let Some(main) = pkg.main else { continue };
            let name = if pkg.name == "xtask" {
                "process(cargo-xtask)".to_string()
            } else {
                format!("process({})", pkg.name)
            };
            let mut seen = BTreeSet::new();
            let mut stack = vec![main];
            while let Some(f) = stack.pop() {
                if seen.insert(f) {
                    stack.extend(self.edges[f].iter().filter(|(_, e)| e.0).map(|(t, _)| *t));
                }
            }
            self.processes.insert(name, seen);
        }
    }

    fn processes_of(&self, f: usize) -> BTreeSet<&str> {
        self.processes
            .iter()
            .filter(|(_, s)| s.contains(&f))
            .map(|(p, _)| p.as_str())
            .collect()
    }

    /// Expected `Called-by:` of file `t`.
    fn expected_called_by(&self, t: usize) -> BTreeSet<String> {
        let file = &self.files[t];
        let pkg = &self.packages[file.pkg];
        match file.role {
            Role::Main if pkg.name == "xtask" => {
                return ["process(cargo-xtask)".to_string()].into();
            }
            Role::Main => return [format!("process({})", pkg.name)].into(),
            Role::TestTop => return ["cargo-test".to_string()].into(),
            Role::Build => return ["cargo-build".to_string()].into(),
            _ => {}
        }
        let mut out = BTreeSet::new();
        if file.role == Role::Lib {
            for p in &self.packages {
                if p.name != pkg.name && p.deps.iter().any(|d| d.pkg == pkg.name) {
                    out.insert(format!("crate({})", p.name));
                }
            }
        }
        for (i, edges) in self.edges.iter().enumerate() {
            let Some(&(non_test, test)) = edges.get(&t) else {
                continue;
            };
            let importer = &self.files[i];
            if file.role == Role::Lib && importer.pkg != file.pkg {
                continue;
            }
            if file.is_test {
                out.insert(importer.canonical.clone());
            } else {
                if non_test && !importer.is_test {
                    out.insert(importer.canonical.clone());
                }
                if test || importer.is_test {
                    out.insert("tests".to_string());
                }
            }
        }
        out
    }

    /// Outgoing module edges that `modules=` must list.
    fn computed_modules(&self, f: usize) -> BTreeSet<String> {
        let test_file = self.files[f].is_test;
        self.edges[f]
            .iter()
            .filter(|(_, e)| e.0 || (test_file && e.1))
            .map(|(t, _)| self.files[*t].canonical.clone())
            .collect()
    }

    fn file_named(&self, canonical: &str) -> Option<usize> {
        self.files.iter().position(|f| f.canonical == canonical)
    }
}

fn pub_items(masked: &str, uses: &[UseStmt]) -> BTreeSet<String> {
    let b = masked.as_bytes();
    let mut out = BTreeSet::new();
    for p in word_positions(masked, "pub") {
        let mut i = skip_ws(b, p + 3);
        while let Some((w, j)) = read_ident(b, i) {
            match w.as_str() {
                "async" | "unsafe" => i = skip_ws(b, j),
                // `extern "C"`: the ABI string is masked to `"  "`; skip past its closing quote.
                "extern" => {
                    i = skip_ws(b, j);
                    if b.get(i) == Some(&b'"') {
                        let close = b[i + 1..]
                            .iter()
                            .position(|c| *c == b'"')
                            .map_or(b.len(), |n| i + 1 + n);
                        i = skip_ws(b, close + 1);
                    }
                }
                "const" if read_ident(b, skip_ws(b, j)).is_some_and(|(n, _)| n == "fn") => {
                    i = skip_ws(b, j);
                }
                "fn" | "struct" | "enum" | "trait" | "type" | "const" | "static" | "mod"
                | "union" => {
                    if let Some((name, _)) = read_ident(b, skip_ws(b, j)) {
                        out.insert(name);
                    }
                    break;
                }
                _ => break,
            }
        }
    }
    for u in uses.iter().filter(|u| u.is_pub) {
        for l in &u.leaves {
            if let Some(n) = l.alias.clone().or_else(|| l.path.last().cloned()) {
                out.insert(n);
            }
        }
    }
    out
}

/// `// dep-map: allow <rule> — <reason>` exemptions, each targeting the next code line.
fn line_exemptions(path: &str, lex: &Lexed) -> Vec<Exemption> {
    let mut out = Vec::new();
    for c in &lex.comments {
        if c.kind != CommentKind::Line {
            continue;
        }
        let Some(body) = c.text.trim_start().strip_prefix("dep-map: allow ") else {
            continue;
        };
        let (rule, reason) = body.split_once(" — ").unwrap_or((body, ""));
        let target = (c.line + 1..=lex.lines()).find(|&l| !lex.masked_line(l).trim().is_empty());
        out.push(Exemption {
            path: path.to_string(),
            line: c.line,
            target: Some(target.unwrap_or(c.line + 1)),
            rule: rule.trim().to_string(),
            reason: reason.trim().to_string(),
            used: 0,
        });
    }
    out
}

// ============================================================================
// Cross-checks
// ============================================================================

fn declared_pg(d: &DependsOn) -> (BTreeSet<&str>, BTreeSet<&str>, BTreeSet<&str>) {
    let mut r = BTreeSet::new();
    let mut w = BTreeSet::new();
    let mut x = BTreeSet::new();
    for s in d
        .services
        .iter()
        .filter(|s| s.kind == Some(Service::PostgreSQL))
    {
        r.extend(s.r.iter().map(String::as_str));
        w.extend(s.w.iter().map(String::as_str));
        x.extend(s.x.iter().map(String::as_str));
    }
    (r, w, x)
}

fn fmt_set<S: AsRef<str>>(it: impl IntoIterator<Item = S>) -> String {
    let v: Vec<String> = it.into_iter().map(|s| s.as_ref().to_string()).collect();
    format!("[{}]", v.join(", "))
}

impl Analysis {
    #[allow(clippy::too_many_lines)] // one block per rule id, in maintainability.md §9 order
    fn file_findings(&self, f: usize) -> Vec<Finding> {
        let file = &self.files[f];
        let path = file.rel.as_str();
        let h = &file.header;
        let mut out = file.header_findings.clone();
        let hl = h.start_line;
        if let Some((line, p)) = &h.purpose
            && *p != file.canonical
        {
            out.push(finding(
                path,
                *line,
                "header-path",
                format!("header names `{p}` (computed: `{}`)", file.canonical),
            ));
        }
        if let Some((line, _)) = &h.purpose
            && !h.sentence.ends_with(['.', '。'])
        {
            out.push(finding(
                path,
                *line,
                "header-purpose",
                "field 1 must be one whole sentence ending in `.` (continue it on `//!  ` lines; \
                 the remaining prose goes below the fields)",
            ));
        }
        let empty = DependsOn::default();
        let decl = h.depends.as_ref().unwrap_or(&empty);
        if let Some(inv) = &h.invariants {
            let low = inv.to_lowercase();
            if let Some(ptr) = INVARIANT_POINTERS.iter().find(|p| low.contains(*p)) {
                out.push(finding(
                    path,
                    hl,
                    "header-invariants",
                    format!("`Invariants:` points elsewhere (`{ptr}`) — state the invariant and the failure behaviour here"),
                ));
            }
        }
        if h.invariants.as_deref() == Some("")
            && (!decl.services.is_empty() || !decl.env.is_empty() || !decl.refused_env.is_empty())
        {
            out.push(finding(
                path,
                hl,
                "header-invariants",
                "`Invariants: []` is only legal with services=[] and env=[]",
            ));
        }
        if let Some((line, v)) = &h.spec {
            for e in check_spec(v, &self.spec) {
                out.push(finding(path, *line, "spec-ref", e));
            }
            let cited: BTreeSet<&String> = h
                .prose_refs
                .iter()
                .filter(|r| self.spec.resolves(r))
                .collect();
            if v.trim() == "none" && !cited.is_empty() {
                out.push(finding(
                    path,
                    *line,
                    "spec-ref",
                    format!("`Spec: none` but the prose cites {}", fmt_set(&cited)),
                ));
            } else if !cited.is_empty() && !spec_refs(v).iter().any(|r| cited.contains(r)) {
                out.push(finding(
                    path,
                    *line,
                    "spec-ref",
                    format!(
                        "`Spec:` cites none of the references the prose cites {}",
                        fmt_set(&cited)
                    ),
                ));
            }
        }
        // crates
        let declared_crates: BTreeSet<&str> = decl.crates.iter().map(String::as_str).collect();
        let missing: Vec<&String> = self.crates[f]
            .keys()
            .filter(|c| !declared_crates.contains(c.as_str()))
            .collect();
        if !missing.is_empty() {
            out.push(finding(
                path,
                hl,
                "crates-undeclared",
                format!(
                    "crates= misses {} (computed: {})",
                    fmt_set(&missing),
                    fmt_set(self.crates[f].keys())
                ),
            ));
        }
        let unwitnessed: Vec<&str> = declared_crates
            .iter()
            .copied()
            .filter(|c| !self.crates[f].contains_key(*c))
            .collect();
        if !unwitnessed.is_empty() {
            out.push(finding(
                path,
                hl,
                "crates-unwitnessed",
                format!(
                    "crates= names {} but the code names no such dependency (computed: {})",
                    fmt_set(&unwitnessed),
                    fmt_set(self.crates[f].keys())
                ),
            ));
        }
        // modules
        let computed = self.computed_modules(f);
        let declared: BTreeSet<String> = decl.modules.iter().cloned().collect();
        if computed != declared {
            out.push(finding(
                path,
                hl,
                "modules-drift",
                format!(
                    "modules= extra {} missing {} (computed: {})",
                    fmt_set(declared.difference(&computed)),
                    fmt_set(computed.difference(&declared)),
                    fmt_set(&computed)
                ),
            ));
        }
        // called-by
        let expected = self.expected_called_by(f);
        let declared: BTreeSet<String> = h
            .called_by
            .clone()
            .unwrap_or_default()
            .into_iter()
            .collect();
        let phantom: Vec<&String> = declared.difference(&expected).collect();
        let missing: Vec<&String> = expected.difference(&declared).collect();
        if !phantom.is_empty() {
            out.push(finding(
                path,
                hl,
                "calledby-phantom",
                format!(
                    "{} import nothing here (computed: {})",
                    fmt_set(&phantom),
                    fmt_set(&expected)
                ),
            ));
        }
        if !missing.is_empty() {
            out.push(finding(
                path,
                hl,
                "calledby-missing",
                format!(
                    "{} not listed (computed: {})",
                    fmt_set(&missing),
                    fmt_set(&expected)
                ),
            ));
        }
        // env
        for (name, (line, _)) in &self.env[f] {
            if !decl.env.contains(name) && !decl.refused_env.contains(name) {
                out.push(finding(
                    path,
                    *line,
                    "env-undeclared",
                    format!(
                        "`{name}` is not in env= (computed: {})",
                        fmt_set(self.env[f].keys())
                    ),
                ));
            }
        }
        // tables
        let (r, w, x) = declared_pg(decl);
        for (tok, line) in &self.db[f].relations {
            if !r.contains(tok.as_str()) && !w.contains(tok.as_str()) {
                out.push(finding(
                    path,
                    *line,
                    "table-undeclared",
                    format!("`{tok}` is in no r=/w= list"),
                ));
            }
        }
        for (tok, line) in &self.db[f].functions {
            if !x.contains(tok.as_str()) {
                out.push(finding(
                    path,
                    *line,
                    "table-undeclared",
                    format!("`{tok}(` is in no x= list"),
                ));
            }
        }
        for (tok, line) in &self.db[f].writes {
            if !w.contains(tok.as_str()) {
                out.push(finding(
                    path,
                    *line,
                    "table-write",
                    format!("`{tok}` is written here but in no w= list"),
                ));
            }
        }
        let db = &self.db[f];
        let lists = [
            ("r", &r, &db.relations),
            ("w", &w, &db.writes),
            ("x", &x, &db.functions),
        ];
        for (key, declared, found) in lists {
            let extra: Vec<&&str> = declared
                .iter()
                .filter(|t| !found.contains_key(**t))
                .collect();
            if !extra.is_empty() {
                out.push(finding(
                    path,
                    hl,
                    "table-unwitnessed",
                    format!(
                        "{key}= names {} but no string literal here {} it (computed: {})",
                        fmt_set(&extra),
                        match key {
                            "r" => "names",
                            "w" => "writes",
                            _ => "calls",
                        },
                        fmt_set(found.keys())
                    ),
                ));
            }
        }
        // services: vocabulary and witnesses
        let processes: BTreeSet<String> = self
            .packages
            .iter()
            .filter(|p| p.main.is_some())
            .map(|p| p.short().to_string())
            .collect();
        let tags = &self.tags[f];
        let evidence = code_evidence(&file.lex, &self.env[f]);
        let roles = pool_roles(&file.lex);
        for d in &decl.services {
            let Some(kind) = d.kind else { continue };
            if let Some(e) = detail_error(kind, d.detail.as_deref(), &processes) {
                out.push(finding(path, hl, "service-vocab", e));
                continue;
            }
            let tagged = tags.iter().any(|t| {
                t.parsed.as_ref().is_some_and(|(svc, detail)| {
                    *svc == kind
                        && (*detail == d.detail
                            || (kind == Service::PostgreSQL
                                && (detail.as_deref() == Some("any")
                                    || d.detail.as_deref() == Some("any"))))
                })
            });
            let evidenced = match (kind, d.detail.as_deref()) {
                (Service::PostgreSQL, Some(role @ ("any" | "owner"))) => {
                    evidence.contains(&kind) || (role == "any" && !roles.is_empty())
                }
                (Service::PostgreSQL, Some(role)) => roles.contains(role),
                _ => evidence.contains(&kind),
            };
            // A PostgreSQL item is witnessed by a schema token its lists name (misfiled or
            // not — `table-*` report the list), or, with no lists, by any token in the file.
            let found = |t: &String| {
                db.relations.contains_key(t)
                    || db.writes.contains_key(t)
                    || db.functions.contains_key(t)
            };
            let listed: Vec<&String> = d.r.iter().chain(&d.w).chain(&d.x).collect();
            let tables = kind == Service::PostgreSQL
                && if listed.is_empty() {
                    !(db.relations.is_empty() && db.writes.is_empty() && db.functions.is_empty())
                } else {
                    listed.into_iter().any(found)
                };
            if !tagged && !tables && !evidenced {
                out.push(finding(
                    path,
                    hl,
                    "service-unwitnessed",
                    format!(
                        "`{}{}` is witnessed by nothing in this file: no `// dep:` tag, {}dependency env literal or testkit marker",
                        kind.name(),
                        d.detail.as_ref().map(|x| format!("({x})")).unwrap_or_default(),
                        if kind == Service::PostgreSQL { "typed pool, schema token, " } else { "" }
                    ),
                ));
            }
        }
        // tags and call sites
        for t in tags {
            match &t.parsed {
                None => out.push(finding(
                    path,
                    t.line,
                    "tag-grammar",
                    "tag must be `// dep: <PostgreSQL|Qdrant|MiniMax|DashScope|UDS|subprocess|HTTP|fs>[(detail)] — <why>`",
                )),
                Some((svc, detail)) => {
                    if let Some(e) = detail_error(*svc, detail.as_deref(), &processes) {
                        out.push(finding(path, t.line, "tag-grammar", e));
                        continue;
                    }
                    let ok = decl.services.iter().any(|d| {
                        d.kind == Some(*svc)
                            && (d.detail == *detail
                                || (*svc == Service::PostgreSQL && detail.as_deref() == Some("any")))
                    });
                    if !ok {
                        out.push(finding(
                            path,
                            t.line,
                            "service-undeclared",
                            format!(
                                "tag service `{}{}` is not declared in services=",
                                svc.name(),
                                detail.as_ref().map(|d| format!("({d})")).unwrap_or_default()
                            ),
                        ));
                    }
                }
            }
        }
        let details = site_details(&file.lex);
        let mut by_line: BTreeMap<usize, Vec<Class>> = BTreeMap::new();
        for (line, class) in &self.sites[f] {
            by_line.entry(*line).or_default().push(*class);
        }
        for (line, classes) in by_line {
            let s = stmt_start(&file.lex, line);
            let window = |l: usize| (l.saturating_sub(3).max(1)..l).collect::<Vec<_>>();
            let near: Vec<&Tag> = tags
                .iter()
                .filter(|t| window(line).contains(&t.line) || window(s).contains(&t.line))
                .collect();
            let bad: Vec<&str> = classes
                .iter()
                .filter(|c| {
                    !near.iter().any(|t| {
                        t.parsed
                            .as_ref()
                            .is_some_and(|(svc, _)| c.compatible().contains(svc))
                    })
                })
                .map(|c| c.id())
                .collect();
            for (_, class, want) in details
                .iter()
                .filter(|(l, c, _)| *l == line && classes.contains(c))
            {
                let tag = near
                    .iter()
                    .filter_map(|t| t.parsed.as_ref().map(|p| (t.line, p)))
                    .filter(|(_, (svc, _))| class.compatible().contains(svc))
                    .max_by_key(|(l, _)| *l);
                let Some((_, (svc, got))) = tag else { continue };
                let got = got.as_deref().unwrap_or("");
                let ok = match want.strip_prefix('!') {
                    Some(not) => got != not,
                    None => got == want,
                };
                if !ok {
                    out.push(finding(
                        path,
                        line,
                        "tag-mismatch",
                        format!(
                            "{} site fixes `{}` but the tag says `{}({got})`",
                            class.id(),
                            match want.strip_prefix('!') {
                                Some(not) => format!("not {not}"),
                                None => want.clone(),
                            },
                            svc.name()
                        ),
                    ));
                }
            }
            if !bad.is_empty() {
                out.push(finding(
                    path,
                    line,
                    "callsite-untagged",
                    format!(
                        "{} needs a `// dep:` tag within 3 lines above",
                        bad.join(", ")
                    ),
                ));
            }
        }
        out
    }

    fn global_findings(&self) -> Vec<Finding> {
        let mut out = Vec::new();
        let mut same: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
        for (f, file) in self.files.iter().enumerate() {
            if let Some(inv) = file.header.invariants.as_deref().filter(|i| !i.is_empty()) {
                same.entry(inv).or_default().push(f);
            }
        }
        for files in same.values().filter(|v| v.len() >= INVARIANT_COPIES) {
            for &f in files {
                out.push(finding(
                    &self.files[f].rel,
                    self.files[f].header.start_line,
                    "header-invariants",
                    format!(
                        "`Invariants:` text is shared verbatim by {} files — state this module's own invariant",
                        files.len()
                    ),
                ));
            }
        }
        // env-unregistered: gateway-reachable modules vs gateway::bootstrap's declared env.
        if let Some(reach) = self.processes.get("process(humaux-gateway)") {
            let registry: BTreeSet<String> = self
                .file_named("gateway::bootstrap")
                .and_then(|b| self.files[b].header.depends.as_ref())
                .map(|d| d.env.clone())
                .unwrap_or_default();
            for &f in reach {
                for (name, (line, _)) in &self.env[f] {
                    if name.starts_with("HUMAUX_GATEWAY_") && !registry.contains(name) {
                        out.push(finding(
                            &self.files[f].rel,
                            *line,
                            "env-unregistered",
                            format!("`{name}` is read by the gateway but not declared by gateway::bootstrap"),
                        ));
                    }
                }
            }
        }
        // Cargo
        for (pi, pkg) in self.packages.iter().enumerate() {
            out.extend(pkg.grammar.iter().cloned());
            let scanned: BTreeSet<(DepKind, String)> = pkg
                .entries
                .iter()
                .map(|e| (e.kind, e.key.clone()))
                .collect();
            for (kind, key) in scanned.symmetric_difference(&pkg.toml_keys) {
                out.push(finding(
                    &pkg.manifest_rel,
                    1,
                    "cargo-parse",
                    format!("dependency `{key}` ({kind:?}) is seen by only one of the line scan and the toml parse"),
                ));
            }
            for e in &pkg.entries {
                let Some((_, used_by)) = &e.why else {
                    out.push(finding(
                        &pkg.manifest_rel,
                        e.line,
                        "cargo-why",
                        format!(
                            "`{}` needs `# why: <reason>; used-by: [..]` on the line above (computed: {})",
                            e.key,
                            fmt_set(self.cargo_users(pi, &e.key))
                        ),
                    ));
                    continue;
                };
                let users = self.cargo_users(pi, &e.key);
                for u in used_by {
                    if !users.contains(u) {
                        out.push(finding(
                            &pkg.manifest_rel,
                            e.line,
                            "cargo-usedby",
                            format!(
                                "`{u}` does not name `{}` (computed: {})",
                                e.key,
                                fmt_set(&users)
                            ),
                        ));
                    }
                }
            }
        }
        out
    }

    /// Modules of package `pi` naming dependency `key`: canonical paths, `tests`, `build`.
    fn cargo_users(&self, pi: usize, key: &str) -> BTreeSet<String> {
        let pkg = &self.packages[pi];
        let Some(dep) = pkg.deps.iter().find(|d| d.key == key) else {
            return BTreeSet::new();
        };
        let mut out = BTreeSet::new();
        for (f, file) in self.files.iter().enumerate().filter(|(_, x)| x.pkg == pi) {
            let Some(&(non_test, test)) = self.crates[f].get(&dep.pkg) else {
                continue;
            };
            out.insert(file.canonical.clone());
            if file.role == Role::Build {
                out.insert("build".into());
            }
            if test || (file.is_test && non_test) {
                out.insert("tests".into());
            }
        }
        out
    }

    fn exemptions(&self) -> Vec<Exemption> {
        self.files
            .iter()
            .flat_map(|f| f.exemptions.iter().cloned())
            .chain(
                self.packages
                    .iter()
                    .flat_map(|p| p.exemptions.iter().cloned()),
            )
            .collect()
    }

    /// All rule findings except doc-drift / db-not-at-head, after exemptions; returns
    /// `(violations, exemptions with use counts)`.
    fn violations(&self) -> (Vec<Finding>, Vec<Exemption>) {
        let mut raw: Vec<Finding> = (0..self.files.len())
            .flat_map(|f| self.file_findings(f))
            .collect();
        raw.extend(self.global_findings());
        let mut exemptions = self.exemptions();
        let mut out = Vec::new();
        for e in &exemptions {
            if !e.valid() {
                out.push(finding(
                    &e.path,
                    e.line,
                    "exemption-grammar",
                    format!(
                        "`dep-map: allow {}` needs an exemptable rule id and a reason (`— <why>`)",
                        e.rule
                    ),
                ));
            }
        }
        for f in raw {
            let hit = exemptions.iter_mut().find(|e| {
                e.valid()
                    && e.path == f.path
                    && e.rule == f.rule
                    && e.target.is_none_or(|t| t == f.line)
            });
            match hit {
                Some(e) => e.used += 1,
                None => out.push(f),
            }
        }
        for e in &exemptions {
            if e.used == 0 && e.valid() {
                out.push(finding(
                    &e.path,
                    e.line,
                    "exemption-unused",
                    format!("`{}` exemption suppresses nothing", e.rule),
                ));
            }
        }
        out.sort();
        out.dedup();
        (out, exemptions)
    }
}

// ============================================================================
// Test targets (gate-truth's DB-bound set)
// ============================================================================

const TEST_MARKERS: [(&str, Service); 6] = [
    ("run_db_fixture", Service::PostgreSQL),
    ("DbIntegrationFixture", Service::PostgreSQL),
    ("ExternalDep::Postgres", Service::PostgreSQL),
    ("ExternalDep::Qdrant", Service::Qdrant),
    ("ExternalDep::DashScope", Service::DashScope),
    ("ExternalDep::MiniMax", Service::MiniMax),
];

/// Services a file's code reaches without a call-site pattern: a dependency's env literal
/// (`HUMAUX_TEST_PG_DSN`, `HUMAUX_TEST_QDRANT_PORT`, `DASHSCOPE_API_KEY`, `MINIMAX_API_KEY`)
/// or a testkit fixture marker. Witnesses a declared service with no tag (ADR-0051 D-M).
fn code_evidence(
    lex: &Lexed,
    env: &BTreeMap<String, (usize, BTreeSet<EnvKind>)>,
) -> BTreeSet<Service> {
    let mut out = BTreeSet::new();
    for name in env.keys() {
        match name.as_str() {
            "HUMAUX_TEST_PG_DSN" => out.insert(Service::PostgreSQL),
            "HUMAUX_TEST_QDRANT_PORT" => out.insert(Service::Qdrant),
            "DASHSCOPE_API_KEY" => out.insert(Service::DashScope),
            "MINIMAX_API_KEY" => out.insert(Service::MiniMax),
            _ => false,
        };
    }
    for (marker, svc) in TEST_MARKERS {
        if lex.masked.contains(marker) {
            out.insert(svc);
        }
    }
    out
}

/// Roles fixed by the typed pools a file names in code (`RuntimeDbPool` → `role_gateway`).
fn pool_roles(lex: &Lexed) -> BTreeSet<&'static str> {
    POOL_ROLES
        .iter()
        .filter(|(t, _)| !word_positions(&lex.masked, &format!("{t}DbPool")).is_empty())
        .map(|(_, r)| *r)
        .collect()
}

fn file_services(path: &Path, visited: &mut BTreeSet<PathBuf>, out: &mut BTreeSet<Service>) {
    if !visited.insert(path.to_path_buf()) {
        return;
    }
    let Ok(src) = fs::read_to_string(path) else {
        return;
    };
    let lex = lex(&src);
    out.extend(
        call_sites(&lex)
            .into_iter()
            .filter_map(|(_, c)| c.implied()),
    );
    out.extend(code_evidence(&lex, &env_found(&lex)));
    let dir = path.parent().unwrap_or(Path::new("."));
    for (_, _, attr) in mod_decls(&lex) {
        if let Some(p) = attr {
            file_services(&dir.join(p), visited, out);
        }
    }
}

/// Attribute texts directly before the item keyword at `p` (visibility, `async`, `unsafe`
/// and `const` qualifiers skipped).
fn attrs_before(m: &str, p: usize) -> Vec<&str> {
    let b = m.as_bytes();
    let mut head = m[..p].trim_end();
    loop {
        let trimmed = ["async", "unsafe", "const", "pub(crate)", "pub"]
            .iter()
            .find_map(|q| {
                head.strip_suffix(q)
                    .filter(|h| h.is_empty() || !is_ident(h.as_bytes()[h.len() - 1]))
            });
        match trimmed {
            Some(h) => head = h.trim_end(),
            None => break,
        }
    }
    let mut out = Vec::new();
    while head.ends_with(']') {
        let close = head.len() - 1;
        let mut depth = 0i32;
        let mut open = None;
        for k in (0..=close).rev() {
            match b[k] {
                b']' => depth += 1,
                b'[' => {
                    depth -= 1;
                    if depth == 0 {
                        open = Some(k);
                        break;
                    }
                }
                _ => {}
            }
        }
        let Some(open) = open else { break };
        if !m[..open].trim_end().ends_with('#') {
            break;
        }
        out.push(m[open + 1..close].trim());
        head = m[..open].trim_end().trim_end_matches('#').trim_end();
    }
    out
}

/// `#[path]` modules of a test target: `(module name, file, idents it uses from them)`.
fn path_modules(lex: &Lexed, path: &Path) -> Vec<(String, PathBuf, Vec<String>)> {
    let m = lex.masked.as_str();
    let b = m.as_bytes();
    let dir = path.parent().unwrap_or(Path::new("."));
    mod_decls(lex)
        .into_iter()
        .filter_map(|(name, _, attr)| {
            let file = dir.join(attr?);
            let used: Vec<String> = word_positions(m, "use")
                .into_iter()
                .filter(|&u| m[skip_ws(b, u + 3)..].starts_with(&format!("{name}::")))
                .flat_map(|u| {
                    let end = m[u..].find(';').map_or(m.len(), |e| u + e);
                    m[u..end]
                        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                        .filter(|w| !w.is_empty() && *w != "use" && *w != name)
                        .map(str::to_string)
                        .collect::<Vec<_>>()
                })
                .collect();
            Some((name, file, used))
        })
        .collect()
}

/// Services the **default** run (`cargo test`, no `--ignored`) of the test target at `path`
/// can reach (ADR-0051 D-K): the evidence inside its non-`#[ignore]` `#[test]` functions and
/// everything they name — local `fn`/`const`/`static`/`macro_rules!` items (by name, so a
/// method name matches every item of that name: over-inclusion, never under-) and `#[path]`
/// modules (by module name or an ident it `use`s from them). A target with no `#[ignore]` is
/// judged whole. The `#[ignore]`d tests run in the serial lane, not in this run.
fn default_run_services(path: &Path, all: &BTreeSet<Service>) -> BTreeSet<Service> {
    let Ok(src) = fs::read_to_string(path) else {
        return all.clone();
    };
    let lex = lex(&src);
    let m = lex.masked.as_str();
    if !m.contains("#[ignore") {
        return all.clone();
    }
    let b = m.as_bytes();
    // (name, start, end, root)
    let mut items: Vec<(String, usize, usize, bool)> = Vec::new();
    for kw in ["fn", "const", "static", "macro_rules!"] {
        let hits: Vec<usize> = if kw == "macro_rules!" {
            m.match_indices(kw).map(|(p, _)| p).collect()
        } else {
            word_positions(m, kw)
        };
        for p in hits {
            let Some((name, _)) = read_ident(b, skip_ws(b, p + kw.len())) else {
                continue;
            };
            if name == "fn" {
                continue; // `const fn` is found by the `fn` pass
            }
            let attrs = attrs_before(m, p);
            let is_test = kw == "fn"
                && attrs
                    .iter()
                    .any(|a| *a == "test" || a.ends_with("::test") || a.starts_with("tokio::test"));
            let ignored = attrs.iter().any(|a| a.starts_with("ignore"));
            items.push((name, p, item_end(b, p), is_test && !ignored));
        }
    }
    let modules = path_modules(&lex, path);
    let mut reached: Vec<usize> = (0..items.len()).filter(|&i| items[i].3).collect();
    let mut seen: BTreeSet<usize> = reached.iter().copied().collect();
    let mut k = 0;
    while k < reached.len() {
        let (_, s, e, _) = items[reached[k]];
        let body = &m[s..e];
        for (j, it) in items.iter().enumerate() {
            if !seen.contains(&j) && !word_positions(body, &it.0).is_empty() {
                seen.insert(j);
                reached.push(j);
            }
        }
        k += 1;
    }
    let ranges: Vec<(usize, usize)> = reached.iter().map(|&i| (items[i].1, items[i].2)).collect();
    let inside = |pos: usize| ranges.iter().any(|&(s, e)| s <= pos && pos < e);
    let mut out = BTreeSet::new();
    for (line, class) in call_sites(&lex) {
        let start = lex.line_starts[line - 1];
        let end = lex.line_starts.get(line).copied().unwrap_or(m.len());
        if (start..end).any(inside) {
            out.extend(class.implied());
        }
    }
    let env: BTreeMap<String, (usize, BTreeSet<EnvKind>)> = lex
        .lits
        .iter()
        .filter(|l| inside(l.start))
        .map(|l| (l.text.clone(), (l.line, BTreeSet::new())))
        .collect();
    out.extend(code_evidence(&lex, &env).into_iter().filter(|svc| {
        TEST_MARKERS
            .iter()
            .filter(|(_, x)| x == svc)
            .any(|(marker, _)| ranges.iter().any(|&(s, e)| m[s..e].contains(marker)))
            || env.keys().any(|n| {
                matches!(
                    (n.as_str(), svc),
                    ("HUMAUX_TEST_PG_DSN", Service::PostgreSQL)
                        | ("HUMAUX_TEST_QDRANT_PORT", Service::Qdrant)
                        | ("DASHSCOPE_API_KEY", Service::DashScope)
                        | ("MINIMAX_API_KEY", Service::MiniMax)
                )
            })
    }));
    for (name, file, used) in &modules {
        let named = ranges.iter().any(|&(s, e)| {
            let body = &m[s..e];
            !word_positions(body, name).is_empty()
                || used.iter().any(|u| !word_positions(body, u).is_empty())
        });
        if named {
            file_services(file, &mut BTreeSet::new(), &mut out);
        }
    }
    out
}

/// Repo-relative path of every integration-test target → `(services it reaches, services its
/// default run reaches)`, computed from code only: call-site patterns, dependency env
/// literals, testkit markers, `#[path]` includes.
fn test_targets(root: &Path) -> BTreeMap<String, (BTreeSet<Service>, BTreeSet<Service>)> {
    let mut dirs: Vec<PathBuf> = ["crates", "bins"]
        .iter()
        .filter_map(|g| fs::read_dir(root.join(g)).ok())
        .flat_map(|es| es.flatten().map(|e| e.path().join("tests")))
        .collect();
    dirs.push(root.join("xtask/tests"));
    let mut out = BTreeMap::new();
    for dir in dirs {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for p in entries.flatten().map(|e| e.path()) {
            if p.extension().and_then(|e| e.to_str()) != Some("rs") {
                continue;
            }
            let mut services = BTreeSet::new();
            file_services(&p, &mut BTreeSet::new(), &mut services);
            let default = default_run_services(&p, &services);
            out.insert(rel(root, &p), (services, default));
        }
    }
    out
}

/// Integration-test stem → services its **default run** reaches; a stem in two packages gets
/// the union. Consumed by `gate_truth` (ADR-0051 D-K): one source of truth for "which test
/// binaries need a database in the chain's `cargo test`".
pub fn test_target_services(root: &Path) -> BTreeMap<String, BTreeSet<Service>> {
    let mut out: BTreeMap<String, BTreeSet<Service>> = BTreeMap::new();
    for (path, (_, services)) in test_targets(root) {
        let stem = Path::new(&path)
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        out.entry(stem).or_default().extend(services);
    }
    out
}

// ============================================================================
// Database catalog (db_objects.md)
// ============================================================================

#[derive(Debug, Default)]
struct DbObject {
    kind: String,
    owner: String,
    rls: Option<(bool, bool)>,
    grants: BTreeMap<String, BTreeSet<String>>,
}

struct Catalog {
    objects: BTreeMap<String, DbObject>,
    created_in: BTreeMap<String, String>,
}

fn migration_stems(root: &Path) -> Result<Vec<(String, String)>, String> {
    let dir = root.join("migrations");
    let mut out: Vec<(String, String)> = fs::read_dir(&dir)
        .map_err(|e| format!("migrations/: {e}"))?
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("sql"))
        .filter_map(|p| {
            let stem = p.file_stem()?.to_string_lossy().into_owned();
            Some((stem, fs::read_to_string(&p).ok()?))
        })
        .collect();
    out.sort();
    Ok(out)
}

/// First migration creating each `schema.name` (renames followed).
fn created_in(migrations: &[(String, String)]) -> BTreeMap<String, String> {
    let mut out: BTreeMap<String, String> = BTreeMap::new();
    for (stem, sql) in migrations {
        let norm = sql
            .lines()
            .map(|l| l.split("--").next().unwrap_or(""))
            .collect::<Vec<_>>()
            .join(" ")
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_ascii_lowercase();
        let words: Vec<&str> = norm.split(' ').collect();
        for (i, w) in words.iter().enumerate() {
            if *w == "create" {
                let mut j = i + 1;
                if words.get(j) == Some(&"or") && words.get(j + 1) == Some(&"replace") {
                    j += 2;
                }
                if words.get(j) == Some(&"unlogged") {
                    j += 1;
                }
                match words.get(j) {
                    Some(&"materialized") if words.get(j + 1) == Some(&"view") => j += 2,
                    Some(&("table" | "view" | "function" | "procedure" | "sequence")) => j += 1,
                    _ => continue,
                }
                if words.get(j) == Some(&"if") {
                    j += 3;
                }
                if let Some(name) = words
                    .get(j)
                    .and_then(|n| schema_tokens(n).into_iter().find(|t| t.2 == 0))
                {
                    out.entry(name.0).or_insert_with(|| stem.clone());
                }
            }
            if *w == "rename"
                && words.get(i + 1) == Some(&"to")
                && let Some(new) = words.get(i + 2)
            {
                let back = words[..i]
                    .iter()
                    .rev()
                    .take(6)
                    .find_map(|x| schema_tokens(x).into_iter().find(|t| t.2 == 0));
                if let Some((old, _, _)) = back
                    && let Some(first) = out.get(&old).cloned()
                {
                    let schema = old.split('.').next().unwrap_or("");
                    let new = new.trim_end_matches(';');
                    let new = if new.contains('.') {
                        new.to_string()
                    } else {
                        format!("{schema}.{new}")
                    };
                    out.entry(new).or_insert(first);
                }
            }
        }
    }
    out
}

fn read_catalog(root: &Path, findings: &mut Vec<Finding>) -> Result<Option<Catalog>, String> {
    let dsn = std::env::var(DSN_ENV)
        .map_err(|_| format!("{DSN_ENV} is not set: db_objects.md cannot be checked"))?;
    // dep: PostgreSQL(owner) — read-only catalog read for db_objects.md (never writes).
    let mut client =
        Client::connect(&dsn, NoTls).map_err(|e| format!("cannot reach ${DSN_ENV}: {e}"))?;
    let migrations = migration_stems(root)?;
    // dep: PostgreSQL(owner) — READ ONLY transaction, rolled back at the end.
    let mut tx = client
        .build_transaction()
        .read_only(true)
        .start()
        .map_err(|e| format!("read-only transaction: {e}"))?;
    let q = |tx: &mut postgres::Transaction, sql: &str, schemas: &Vec<&str>| {
        tx.query(sql, &[schemas])
            .map_err(|e| format!("catalog query: {e}"))
    };
    let applied: BTreeSet<String> = tx
        .query("SELECT migration_id FROM ops.schema_migrations", &[])
        .map_err(|e| format!("ops.schema_migrations: {e}"))?
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect();
    let repo: BTreeSet<String> = migrations.iter().map(|(s, _)| s.clone()).collect();
    if applied != repo {
        let missing: Vec<&String> = repo.difference(&applied).collect();
        let extra: Vec<&String> = applied.difference(&repo).collect();
        findings.push(finding(
            "migrations",
            1,
            "db-not-at-head",
            format!(
                "ops.schema_migrations ≠ migrations/*.sql: not applied {}, unknown {}",
                fmt_set(missing.iter().take(5)),
                fmt_set(extra.iter().take(5))
            ),
        ));
        tx.rollback().map_err(|e| format!("rollback: {e}"))?;
        return Ok(None);
    }
    let schemas: Vec<&str> = SCHEMAS.to_vec();
    let mut objects: BTreeMap<String, DbObject> = BTreeMap::new();
    for r in q(
        &mut tx,
        "SELECT n.nspname || '.' || c.relname, c.relkind::text, pg_get_userbyid(c.relowner), \
         c.relrowsecurity, c.relforcerowsecurity \
         FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = ANY($1) AND c.relkind IN ('r','p','v','m','S','f')",
        &schemas,
    )? {
        let kind = match r.get::<_, String>(1).as_str() {
            "r" => "table",
            "p" => "partitioned table",
            "v" => "view",
            "m" => "materialized view",
            "S" => "sequence",
            _ => "foreign table",
        };
        let rls = matches!(kind, "table" | "partitioned table").then(|| (r.get(3), r.get(4)));
        objects.insert(
            r.get(0),
            DbObject {
                kind: kind.to_string(),
                owner: r.get(2),
                rls,
                grants: BTreeMap::new(),
            },
        );
    }
    for r in q(
        &mut tx,
        "SELECT n.nspname || '.' || c.relname, coalesce(g.rolname, 'PUBLIC'), a.privilege_type \
         FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         CROSS JOIN LATERAL aclexplode(c.relacl) a LEFT JOIN pg_roles g ON g.oid = a.grantee \
         WHERE n.nspname = ANY($1) AND (g.rolname LIKE 'role\\_%' OR a.grantee = 0)",
        &schemas,
    )? {
        if let Some(o) = objects.get_mut(&r.get::<_, String>(0)) {
            o.grants.entry(r.get(1)).or_default().insert(r.get(2));
        }
    }
    let mut cols: BTreeMap<(String, String, String), BTreeSet<String>> = BTreeMap::new();
    for r in q(
        &mut tx,
        "SELECT n.nspname || '.' || c.relname, coalesce(g.rolname, 'PUBLIC'), a.privilege_type, att.attname \
         FROM pg_attribute att JOIN pg_class c ON c.oid = att.attrelid \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         CROSS JOIN LATERAL aclexplode(att.attacl) a LEFT JOIN pg_roles g ON g.oid = a.grantee \
         WHERE n.nspname = ANY($1) AND att.attacl IS NOT NULL AND NOT att.attisdropped \
         AND (g.rolname LIKE 'role\\_%' OR a.grantee = 0)",
        &schemas,
    )? {
        cols.entry((r.get(0), r.get(1), r.get(2)))
            .or_default()
            .insert(r.get(3));
    }
    for ((obj, role, privilege), columns) in cols {
        if let Some(o) = objects.get_mut(&obj) {
            o.grants.entry(role).or_default().insert(format!(
                "{privilege}({})",
                columns.into_iter().collect::<Vec<_>>().join(",")
            ));
        }
    }
    for r in q(
        &mut tx,
        "SELECT n.nspname || '.' || p.proname, p.prokind::text, p.prosecdef, pg_get_userbyid(p.proowner), \
         coalesce(g.rolname, CASE WHEN a.grantee = 0 THEN 'PUBLIC' END), p.proacl IS NULL \
         FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace \
         LEFT JOIN LATERAL aclexplode(p.proacl) a ON true LEFT JOIN pg_roles g ON g.oid = a.grantee \
         WHERE n.nspname = ANY($1)",
        &schemas,
    )? {
        let name: String = r.get(0);
        let secdef: bool = r.get(2);
        let kind = match r.get::<_, String>(1).as_str() {
            "p" => "procedure",
            _ if secdef => "function (SECURITY DEFINER)",
            _ => "function",
        };
        let o = objects.entry(name).or_insert_with(|| DbObject {
            kind: kind.to_string(),
            owner: r.get(3),
            rls: None,
            grants: BTreeMap::new(),
        });
        if r.get::<_, bool>(5) {
            o.grants
                .entry("PUBLIC (default)".into())
                .or_default()
                .insert("EXECUTE".into());
        } else if let Some(role) = r.get::<_, Option<String>>(4)
            && (role == "PUBLIC" || role.starts_with("role_"))
        {
            o.grants.entry(role).or_default().insert("EXECUTE".into());
        }
    }
    tx.rollback().map_err(|e| format!("rollback: {e}"))?;
    let created = created_in(&migrations);
    objects.retain(|k, _| created.contains_key(k));
    Ok(Some(Catalog {
        objects,
        created_in: created,
    }))
}

// ============================================================================
// Rendering
// ============================================================================

impl Analysis {
    /// object → role column ("r"/"w"/"x") → process → modules, from declared headers.
    fn db_users(&self) -> BTreeMap<String, [BTreeMap<String, BTreeSet<String>>; 3]> {
        let mut out: BTreeMap<String, [BTreeMap<String, BTreeSet<String>>; 3]> = BTreeMap::new();
        for (f, file) in self.files.iter().enumerate() {
            let Some(d) = &file.header.depends else {
                continue;
            };
            let procs = self.processes_of(f);
            let procs: Vec<String> = if procs.is_empty() {
                vec!["(no process)".into()]
            } else {
                procs.into_iter().map(String::from).collect()
            };
            for s in d
                .services
                .iter()
                .filter(|s| s.kind == Some(Service::PostgreSQL))
            {
                for (col, list) in [&s.r, &s.w, &s.x].into_iter().enumerate() {
                    for obj in list {
                        for p in &procs {
                            out.entry(obj.clone()).or_default()[col]
                                .entry(p.clone())
                                .or_default()
                                .insert(file.canonical.clone());
                        }
                    }
                }
            }
        }
        out
    }

    #[allow(clippy::too_many_lines)] // one block per section of the generated doc
    fn render_dependency_map(&self, exemptions: &[Exemption]) -> String {
        let mut s = String::new();
        let manifests = self.packages.len();
        let _ = writeln!(s, "# Dependency map (generated — do not edit)\n");
        let _ = writeln!(
            s,
            "> Generated by `cargo xtask dep-map --write` from module headers and computed facts \
             (docs/architecture/maintainability.md). `--check` fails on any hand edit.\n"
        );
        let _ = writeln!(
            s,
            "{} files, {} Cargo manifests, {} processes, {} exemptions, {} excluded fixtures.\n",
            self.files.len(),
            manifests,
            self.processes.len(),
            exemptions.len(),
            self.fixtures.len()
        );
        let _ = writeln!(s, "## Processes\n");
        for (proc_name, reach) in &self.processes {
            let _ = writeln!(s, "### {proc_name}\n");
            let _ = writeln!(s, "| module | tables r / w / x | env | services |");
            let _ = writeln!(s, "|---|---|---|---|");
            let mut mods: Vec<usize> = reach.iter().copied().collect();
            mods.sort_by(|a, b| self.files[*a].canonical.cmp(&self.files[*b].canonical));
            for f in mods {
                let file = &self.files[f];
                let (tables, env, services) = match &file.header.depends {
                    Some(d) => {
                        let (r, w, x) = declared_pg(d);
                        let t = format!("r={} w={} x={}", fmt_set(r), fmt_set(w), fmt_set(x));
                        let sv: Vec<String> = d
                            .services
                            .iter()
                            .filter_map(|x| {
                                x.kind.map(|k| {
                                    format!(
                                        "{}{}",
                                        k.name(),
                                        x.detail
                                            .as_ref()
                                            .map(|d| format!("({d})"))
                                            .unwrap_or_default()
                                    )
                                })
                            })
                            .collect();
                        (t, fmt_set(&d.env), fmt_set(sv))
                    }
                    None => (
                        "(no header)".into(),
                        "(no header)".into(),
                        "(no header)".into(),
                    ),
                };
                let _ = writeln!(
                    s,
                    "| `{}` | {tables} | {env} | {services} |",
                    file.canonical
                );
            }
            let _ = writeln!(s);
        }
        let _ = writeln!(
            s,
            "## Reverse index: database object → readers / writers / executors\n"
        );
        let _ = writeln!(s, "| object | read by | written by | executed by |");
        let _ = writeln!(s, "|---|---|---|---|");
        let fmt_users = |m: &BTreeMap<String, BTreeSet<String>>| -> String {
            if m.is_empty() {
                return "—".into();
            }
            m.iter()
                .map(|(p, mods)| {
                    format!(
                        "{p}: {}",
                        mods.iter().cloned().collect::<Vec<_>>().join(", ")
                    )
                })
                .collect::<Vec<_>>()
                .join("; ")
        };
        for (obj, cols) in self.db_users() {
            let _ = writeln!(
                s,
                "| `{obj}` | {} | {} | {} |",
                fmt_users(&cols[0]),
                fmt_users(&cols[1]),
                fmt_users(&cols[2])
            );
        }
        let _ = writeln!(s, "\n## Services → modules\n");
        let mut by_service: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for file in &self.files {
            for d in file.header.depends.iter().flat_map(|d| d.services.iter()) {
                let Some(k) = d.kind else { continue };
                let key = match (k, d.detail.as_deref()) {
                    (Service::Uds, Some("serve")) => "UDS (serve)".to_string(),
                    (Service::Uds, _) => "UDS (clients)".to_string(),
                    (k, Some(detail)) => format!("{}({detail})", k.name()),
                    (k, None) => k.name().to_string(),
                };
                by_service
                    .entry(key)
                    .or_default()
                    .insert(file.canonical.clone());
            }
        }
        for (svc, mods) in &by_service {
            let _ = writeln!(
                s,
                "- **{svc}**: {}",
                mods.iter()
                    .map(|m| format!("`{m}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
        let _ = writeln!(s, "\n## Cargo dependencies\n");
        let _ = writeln!(
            s,
            "| package | dependency | kind | why | modules naming it |"
        );
        let _ = writeln!(s, "|---|---|---|---|---|");
        for (pi, pkg) in self.packages.iter().enumerate() {
            for e in &pkg.entries {
                let why = e
                    .why
                    .as_ref()
                    .map_or("(missing)".to_string(), |(w, _)| w.replace('|', "\\|"));
                let kind = match e.kind {
                    DepKind::Normal => "normal",
                    DepKind::Dev => "dev",
                    DepKind::Build => "build",
                };
                let _ = writeln!(
                    s,
                    "| {} | `{}` | {kind} | {why} | {} |",
                    pkg.name,
                    e.key,
                    fmt_set(self.cargo_users(pi, &e.key))
                );
            }
        }
        let _ = writeln!(s, "\n## Test binaries → external dependencies\n");
        let _ = writeln!(
            s,
            "`default run` = what the non-`#[ignore]` tests reach (gate-truth's DB-bound set); \
             the rest runs in the serial lane.\n"
        );
        let _ = writeln!(
            s,
            "| test target | services (computed) | default run | db-bound (default run) |"
        );
        let _ = writeln!(s, "|---|---|---|---|");
        for (path, (services, default)) in test_targets(&self.root) {
            let db = default.contains(&Service::PostgreSQL) || default.contains(&Service::Qdrant);
            let names = |x: &BTreeSet<Service>| fmt_set(x.iter().map(|x| x.name()));
            let _ = writeln!(
                s,
                "| `{path}` | {} | {} | {} |",
                names(&services),
                names(&default),
                if db { "yes" } else { "no" }
            );
        }
        let _ = writeln!(s, "\n## Excluded fixtures ({})\n", self.fixtures.len());
        for f in &self.fixtures {
            let _ = writeln!(s, "- `{f}`");
        }
        let _ = writeln!(s, "\n## Exemptions ({})\n", exemptions.len());
        let mut ex: Vec<&Exemption> = exemptions.iter().collect();
        ex.sort_by(|a, b| (&a.path, a.line).cmp(&(&b.path, b.line)));
        for e in ex {
            let scope = if e.target.is_none() { "file" } else { "line" };
            let _ = writeln!(
                s,
                "- `{}:{}` [{}] ({scope}) — {}",
                e.path, e.line, e.rule, e.reason
            );
        }
        s
    }

    fn render_env_vars(&self) -> String {
        let registry: BTreeSet<String> = ["gateway::bootstrap", "contracts::retrieval_config"]
            .iter()
            .filter_map(|c| self.file_named(c))
            .filter_map(|f| self.files[f].header.depends.as_ref())
            .flat_map(|d| d.env.iter().cloned())
            .collect();
        #[derive(Default)]
        struct Row {
            kinds: BTreeSet<EnvKind>,
            modules: BTreeSet<String>,
            processes: BTreeSet<String>,
            literal: bool,
            refused: bool,
        }
        let mut rows: BTreeMap<String, Row> = BTreeMap::new();
        for (f, file) in self.files.iter().enumerate() {
            let declared = file
                .header
                .depends
                .iter()
                .flat_map(|d| d.env.iter().chain(&d.refused_env));
            let found = self.env[f].keys();
            for name in declared.chain(found) {
                let row = rows.entry(name.clone()).or_default();
                row.refused |= file
                    .header
                    .depends
                    .as_ref()
                    .is_some_and(|d| d.refused_env.contains(name));
                row.modules.insert(file.canonical.clone());
                row.processes
                    .extend(self.processes_of(f).into_iter().map(String::from));
                if let Some((_, kinds)) = self.env[f].get(name) {
                    row.literal = true;
                    row.kinds.extend(kinds.iter().copied());
                }
            }
        }
        let mut s = String::new();
        let _ = writeln!(s, "# Environment variables (generated — do not edit)\n");
        let _ = writeln!(
            s,
            "> Generated by `cargo xtask dep-map --write`. `registry: gateway` = declared by the §50/§78 typed \
             registry (`gateway::bootstrap`, `contracts::retrieval_config`); `raw` = read directly. \
             `declared-only` = named in a header but built at runtime (no literal). `refused-at-boot` = a \
             removed key a module names only to refuse it (`env=[refused:NAME]`) — never set it.\n"
        );
        let _ = writeln!(s, "{} variables.\n", rows.len());
        let _ = writeln!(
            s,
            "| variable | kind | processes | modules | witness | registry |"
        );
        let _ = writeln!(s, "|---|---|---|---|---|---|");
        for (name, r) in &rows {
            let kind = if r.refused {
                "refused-at-boot".to_string()
            } else if r.kinds.is_empty() {
                "runtime".to_string()
            } else {
                r.kinds
                    .iter()
                    .map(|k| k.name())
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            let _ = writeln!(
                s,
                "| `{name}` | {kind} | {} | {} | {} | {} |",
                if r.processes.is_empty() {
                    "—".to_string()
                } else {
                    r.processes.iter().cloned().collect::<Vec<_>>().join(", ")
                },
                r.modules
                    .iter()
                    .map(|m| format!("`{m}`"))
                    .collect::<Vec<_>>()
                    .join(", "),
                if r.literal {
                    "literal"
                } else {
                    "declared-only"
                },
                if registry.contains(name) {
                    "gateway"
                } else {
                    "raw"
                }
            );
        }
        s
    }

    fn render_db_objects(&self, cat: &Catalog) -> String {
        let users = self.db_users();
        let mut s = String::new();
        let _ = writeln!(s, "# Database objects (generated — do not edit)\n");
        let _ = writeln!(
            s,
            "> Generated by `cargo xtask dep-map --write` from `pg_catalog` on `HUMAUX_TEST_PG_DSN` (READ ONLY \
             transaction, at migration head) + a `migrations/*.sql` scan. Only objects some migration creates. \
             Grants agree with Baseline §6.2.2 through `xtask rls-check` on the same catalog.\n"
        );
        let _ = writeln!(s, "{} objects.\n", cat.objects.len());
        let _ = writeln!(
            s,
            "| object | kind | owner | RLS | FORCE | grants (runtime roles) | created in | read by | written by | executed by |"
        );
        let _ = writeln!(s, "|---|---|---|---|---|---|---|---|---|---|");
        let empty: [BTreeMap<String, BTreeSet<String>>; 3] = Default::default();
        let fmt_users = |m: &BTreeMap<String, BTreeSet<String>>| -> String {
            if m.is_empty() {
                return "—".into();
            }
            m.iter()
                .map(|(p, mods)| {
                    format!(
                        "{p}: {}",
                        mods.iter().cloned().collect::<Vec<_>>().join(", ")
                    )
                })
                .collect::<Vec<_>>()
                .join("; ")
        };
        for (name, o) in &cat.objects {
            let grants = if o.grants.is_empty() {
                "—".to_string()
            } else {
                o.grants
                    .iter()
                    .map(|(role, privs)| {
                        format!(
                            "{role}: {}",
                            privs.iter().cloned().collect::<Vec<_>>().join(", ")
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("; ")
            };
            let (rls, force) = match o.rls {
                Some((r, f)) => (if r { "yes" } else { "no" }, if f { "yes" } else { "no" }),
                None => ("—", "—"),
            };
            let u = users.get(name).unwrap_or(&empty);
            let _ = writeln!(
                s,
                "| `{name}` | {} | {} | {rls} | {force} | {grants} | {} | {} | {} | {} |",
                o.kind,
                o.owner,
                cat.created_in.get(name).map_or("—", String::as_str),
                fmt_users(&u[0]),
                fmt_users(&u[1]),
                fmt_users(&u[2])
            );
        }
        s
    }

    fn suggest(&self, rel_path: &str) -> Option<String> {
        let f = self.files.iter().position(|x| x.rel == rel_path)?;
        let file = &self.files[f];
        let mut services: BTreeSet<String> = self.tags[f]
            .iter()
            .filter_map(|t| t.parsed.as_ref())
            .map(|(s, d)| {
                format!(
                    "{}{}",
                    s.name(),
                    d.as_ref().map(|d| format!("({d})")).unwrap_or_default()
                )
            })
            .collect();
        for (_, c) in &self.sites[f] {
            if let Some(svc) = c.implied() {
                services.insert(svc.name().to_string());
            }
        }
        let db = &self.db[f];
        let r: BTreeSet<&String> = db
            .relations
            .keys()
            .filter(|k| !db.writes.contains_key(*k))
            .collect();
        let mut s = String::new();
        let _ = writeln!(s, "== {rel_path}");
        let _ = writeln!(s, "//! `{}` — <one sentence purpose>.", file.canonical);
        let _ = writeln!(
            s,
            "//! Depends-on: crates={}; services={}; env={}; modules={}",
            fmt_set(self.crates[f].keys()),
            fmt_set(&services),
            fmt_set(self.env[f].keys()),
            fmt_set(self.computed_modules(f))
        );
        let _ = writeln!(s, "//! Called-by: {}", fmt_set(self.expected_called_by(f)));
        let _ = writeln!(
            s,
            "//! Invariants: [<what must hold; behaviour when a dependency is down>]"
        );
        let _ = writeln!(s, "//! Spec: <Baseline §x.y; ADR-00nn | none>");
        let _ = writeln!(
            s,
            "   evidence: {}",
            fmt_set(
                code_evidence(&file.lex, &self.env[f])
                    .iter()
                    .map(|x| x.name().to_string())
                    .chain(
                        pool_roles(&file.lex)
                            .iter()
                            .map(|r| format!("PostgreSQL({r})"))
                    )
            )
        );
        let _ = writeln!(
            s,
            "   tables found: r={} w={} x={}",
            fmt_set(r),
            fmt_set(db.writes.keys()),
            fmt_set(db.functions.keys())
        );
        Some(s)
    }
}

// ============================================================================
// CLI
// ============================================================================

struct Outcome {
    violations: Vec<Finding>,
    exemptions: usize,
    drift: Vec<&'static str>,
    docs: Vec<(&'static str, Option<String>)>,
}

fn first_diff_line(a: &str, b: &str) -> usize {
    a.lines()
        .zip(b.lines())
        .position(|(x, y)| x != y)
        .unwrap_or_else(|| a.lines().count().min(b.lines().count()))
        + 1
}

/// Full analysis + regeneration in memory; `catalog` errors are returned as `Err`.
fn evaluate(
    an: &Analysis,
    catalog: Result<Option<Catalog>, String>,
    db_findings: Vec<Finding>,
) -> (Outcome, Option<String>) {
    let (mut violations, exemptions) = an.violations();
    violations.extend(db_findings);
    let (db_doc, db_err) = match catalog {
        Ok(Some(cat)) => (Some(an.render_db_objects(&cat)), None),
        Ok(None) => (None, None),
        Err(e) => (None, Some(e)),
    };
    let docs = vec![
        (DOC_DEP_MAP, Some(an.render_dependency_map(&exemptions))),
        (DOC_DB, db_doc),
        (DOC_ENV, Some(an.render_env_vars())),
    ];
    let mut drift = Vec::new();
    for (path, doc) in &docs {
        let committed = fs::read_to_string(an.root.join(path)).ok();
        match (doc, &committed) {
            (Some(d), Some(c)) if d == c => {}
            (Some(d), Some(c)) => {
                drift.push(*path);
                violations.push(finding(
                    path,
                    first_diff_line(d, c),
                    "doc-drift",
                    "committed file differs from the regenerated one (run `cargo xtask dep-map --write`)",
                ));
            }
            (Some(_), None) => {
                drift.push(*path);
                violations.push(finding(
                    path,
                    1,
                    "doc-drift",
                    "missing (run `cargo xtask dep-map --write`)",
                ));
            }
            (None, _) => drift.push(*path),
        }
    }
    violations.sort();
    (
        Outcome {
            violations,
            exemptions: exemptions.len(),
            drift,
            docs,
        },
        db_err,
    )
}

pub fn run(args: &[String]) -> i32 {
    let started = Instant::now();
    let mode = args.first().map(String::as_str).unwrap_or("--check");
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let root = root.canonicalize().unwrap_or(root);
    if !matches!(mode, "--check" | "--write" | "--suggest")
        || (mode != "--suggest" && args.len() > 1)
    {
        eprintln!("usage: cargo xtask dep-map [--check | --write | --suggest <file.rs>...]");
        return 2;
    }
    let an = match Analysis::new(&root) {
        Ok(an) => an,
        Err(e) => {
            eprintln!("dep-map: error: {e}");
            return 2;
        }
    };
    if mode == "--suggest" {
        for a in &args[1..] {
            let p = Path::new(a);
            let r = if p.is_absolute() {
                rel(&root, p)
            } else {
                a.trim_start_matches("./").to_string()
            };
            match an.suggest(&r) {
                Some(s) => print!("{s}"),
                None => eprintln!("dep-map: {r} is not an in-scope file"),
            }
        }
        return 0;
    }
    let mut db_findings = Vec::new();
    let catalog = read_catalog(&root, &mut db_findings);
    let (out, db_err) = evaluate(&an, catalog, db_findings);
    if mode == "--write" {
        for (path, doc) in &out.docs {
            if let Some(doc) = doc
                && let Err(e) = fs::write(root.join(path), doc)
            {
                eprintln!("dep-map: cannot write {path}: {e}");
                return 2;
            }
        }
    }
    let shown: Vec<&Finding> = out
        .violations
        .iter()
        .filter(|f| mode == "--check" || f.rule != "doc-drift")
        .collect();
    for f in &shown {
        println!("{}:{}: [{}] {}", f.path, f.line, f.rule, f.msg);
    }
    let docs = if mode == "--write" {
        if db_err.is_some() {
            format!("written except {DOC_DB}")
        } else {
            "written".to_string()
        }
    } else if out.drift.is_empty() {
        "in sync".to_string()
    } else {
        format!("drift: {}", out.drift.join(", "))
    };
    println!(
        "dep-map: {} files, {} Cargo.toml, {} violations, {} exemptions, docs {docs}, {:.2}s",
        an.files.len(),
        an.packages.len(),
        shown.len(),
        out.exemptions,
        started.elapsed().as_secs_f64()
    );
    if let Some(e) = db_err {
        eprintln!("dep-map: error: {e}");
        return 2;
    }
    let ok = shown.is_empty() && (mode == "--write" || out.drift.is_empty());
    i32::from(!ok)
}

#[cfg(test)]
mod tests {
    use super::*;
    use humaux_testkit::{ExternalDep, skip_or_fail};

    const NODEP: &str = "crates=[]; services=[]; env=[]; modules=[]";

    fn hdr(path: &str, dep: &str, called: &str, inv: &str) -> String {
        format!(
            "//! `{path}` — fixture.\n//! Depends-on: {dep}\n//! Called-by: {called}\n//! Invariants: {inv}\n//! Spec: none\n"
        )
    }

    /// A throwaway workspace: `crates/*`, `bins/*`, `xtask`, one ADR, a Baseline stub.
    struct Fx {
        root: PathBuf,
    }

    impl Fx {
        fn new(name: &str) -> Fx {
            let root = std::env::temp_dir().join(format!("dep_map_{name}_{}", std::process::id()));
            let _ = fs::remove_dir_all(&root);
            fs::create_dir_all(root.join("docs/adr")).unwrap();
            fs::create_dir_all(root.join("docs/architecture")).unwrap();
            fs::write(
                root.join("Cargo.toml"),
                "[workspace]\nmembers = [\"crates/*\", \"bins/*\", \"xtask\"]\n",
            )
            .unwrap();
            fs::write(
                root.join("docs/adr/0051-maintainability-convention.md"),
                "x",
            )
            .unwrap();
            fs::write(
                root.join("docs/architecture/Baseline_2.9.md"),
                "# 17. Qdrant\n## 17.4 Visible\n### 6.2.2 Grants\n",
            )
            .unwrap();
            Fx { root }
        }

        fn pkg(&self, dir: &str, name: &str, deps: &str) -> &Self {
            self.file(
                &format!("{dir}/Cargo.toml"),
                &format!("[package]\nname = \"{name}\"\n\n[dependencies]\n{deps}"),
            )
        }

        fn file(&self, rel: &str, text: &str) -> &Self {
            let p = self.root.join(rel);
            fs::create_dir_all(p.parent().unwrap()).unwrap();
            fs::write(p, text).unwrap();
            self
        }

        fn an(&self) -> Analysis {
            Analysis::new(&self.root).expect("fixture analysis")
        }

        /// `(line, rule, message)` of every violation in `path`.
        fn found(&self, path: &str) -> Vec<(usize, &'static str, String)> {
            self.an()
                .violations()
                .0
                .into_iter()
                .filter(|f| f.path == path)
                .map(|f| (f.line, f.rule, f.msg))
                .collect()
        }

        fn rules(&self, path: &str) -> Vec<&'static str> {
            self.found(path).into_iter().map(|(_, r, _)| r).collect()
        }
    }

    impl Drop for Fx {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    fn called_by(an: &Analysis, canonical: &str) -> Vec<String> {
        an.expected_called_by(
            an.file_named(canonical)
                .unwrap_or_else(|| panic!("{canonical}")),
        )
        .into_iter()
        .collect()
    }

    fn strs(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    // ---- lexer ---------------------------------------------------------------------------

    #[test]
    fn lexer_masks_line_block_nested_comments_and_all_string_forms() {
        let src = "fn f<'a>(x: &'a str) -> char { // line ops.jobs\n\
                   /* outer /* nested \"q\" */ still */ let s = \"a\\\"b\\\\\";\n\
                   let r = r#\"raw \"inner\" \"#; let b = b\"by\"; let br = br#\"x\"#; let c = c\"cs\";\n\
                   let q = '\\''; let u = '\\u{1F600}'; let z = 'x'; 'outer: loop { break 'outer; }\n\
                   let cont = \"one \\\n two\"; /// doc\n}\n";
        let lex = lex(src);
        assert_eq!(lex.masked.len(), src.len(), "byte positions preserved");
        assert_eq!(
            lex.masked.lines().count(),
            src.lines().count(),
            "line numbers preserved"
        );
        for gone in [
            "line ops.jobs",
            "nested",
            "still",
            "inner",
            "by",
            "cs",
            "one",
            "doc",
        ] {
            assert!(
                !lex.masked.contains(gone),
                "{gone} must be masked: {}",
                lex.masked
            );
        }
        for kept in [
            "fn f<'a>(x: &'a str)",
            "'outer: loop",
            "break 'outer;",
            "let z = ' ';",
        ] {
            assert!(
                lex.masked.contains(kept),
                "{kept} must survive: {}",
                lex.masked
            );
        }
        let texts: Vec<&str> = lex.lits.iter().map(|l| l.text.as_str()).collect();
        assert_eq!(
            texts,
            vec![
                "a\\\"b\\\\",
                "raw \"inner\" ",
                "by",
                "x",
                "cs",
                "one \\\n two"
            ]
        );
        assert_eq!(lex.lits[5].line, 5);
        let kinds: Vec<CommentKind> = lex.comments.iter().map(|c| c.kind).collect();
        assert_eq!(
            kinds,
            vec![CommentKind::Line, CommentKind::Block, CommentKind::OuterDoc]
        );
    }

    // ---- header --------------------------------------------------------------------------

    #[test]
    fn header_grammar_accepts_five_fields_with_continuation_and_prose_below() {
        let text = "//! `a::x` — purpose.\n\
                    //! Depends-on: crates=[serde]; services=[PostgreSQL(role_gateway) r=[ops.jobs, ops.x] w=[ops.jobs]];\n\
                    //!   env=[HUMAUX_A]; modules=[a::y]\n\
                    //! Called-by: [a::z, tests]\n\
                    //! Invariants: [down → error]\n\
                    //! Spec: Baseline §17.4; ADR-0051\n\
                    //!\n\
                    //! Prose kept.\n\
                    //!   indented prose\n\
                    fn f() {}\n";
        let (h, findings, _) = parse_header("a.rs", text);
        assert!(findings.is_empty(), "{findings:?}");
        assert_eq!(h.purpose, Some((1, "a::x".to_string())));
        let d = h.depends.expect("depends");
        assert_eq!(d.crates, strs(&["serde"]));
        assert_eq!(d.services.len(), 1);
        assert_eq!(d.services[0].detail.as_deref(), Some("role_gateway"));
        assert_eq!(d.services[0].r, strs(&["ops.jobs", "ops.x"]));
        assert_eq!(d.services[0].w, strs(&["ops.jobs"]));
        assert!(d.env.contains("HUMAUX_A"));
        assert_eq!(d.modules, strs(&["a::y"]));
        assert_eq!(h.called_by, Some(strs(&["a::z", "tests"])));
        assert_eq!(h.invariants.as_deref(), Some("down → error"));
        assert_eq!(
            h.spec.map(|s| s.1).as_deref(),
            Some("Baseline §17.4; ADR-0051")
        );
    }

    #[test]
    fn header_grammar_rejects_missing_or_reordered_field_and_omitted_sublist() {
        let rules = |text: &str| -> Vec<String> {
            parse_header("a.rs", text)
                .1
                .into_iter()
                .map(|f| format!("{} {}", f.rule, f.msg))
                .collect()
        };
        let missing = rules(&format!(
            "//! `a::x` — p.\n//! Depends-on: {NODEP}\n//! Invariants: []\n//! Spec: none\n"
        ));
        assert!(
            missing
                .iter()
                .any(|m| m.starts_with("header-field `Called-by:` missing")),
            "{missing:?}"
        );
        let reordered = rules(&format!(
            "//! `a::x` — p.\n//! Depends-on: {NODEP}\n//! Invariants: []\n//! Called-by: []\n//! Spec: none\n"
        ));
        assert!(
            reordered
                .iter()
                .any(|m| m.contains("`Called-by:` out of order")),
            "{reordered:?}"
        );
        let omitted = rules(
            "//! `a::x` — p.\n//! Depends-on: crates=[]; services=[]; env=[]\n//! Called-by: []\n\
             //! Invariants: []\n//! Spec: none\n",
        );
        assert!(
            omitted
                .iter()
                .any(|m| m.starts_with("header-field Depends-on needs exactly")),
            "{omitted:?}"
        );
        let vocab = rules(
            "//! `a::x` — p.\n//! Depends-on: crates=[]; services=[Postgres(role_x)]; env=[]; modules=[]\n\
             //! Called-by: []\n//! Invariants: [x]\n//! Spec: none\n",
        );
        assert!(
            vocab.iter().any(|m| m.starts_with("service-vocab")),
            "{vocab:?}"
        );
        let no_path = rules("//! Prose only.\nfn f() {}\n");
        assert!(
            no_path.iter().any(|m| m.starts_with("header-path")),
            "{no_path:?}"
        );
    }

    #[test]
    fn header_allows_leading_plain_comment_witness_line() {
        let text = format!(
            "// witness: degrade_total\n\n{}",
            hdr("a::x", NODEP, "[]", "[]")
        );
        let (h, findings, _) = parse_header("a.rs", &text);
        assert!(findings.is_empty(), "{findings:?}");
        assert_eq!(h.start_line, 3);
        assert_eq!(h.purpose.map(|p| p.0), Some(3));
        // A `///` outer doc comment before the header is not allowed to precede it.
        let (h, findings, _) = parse_header(
            "a.rs",
            &format!("/// item doc\n{}", hdr("a::x", NODEP, "[]", "[]")),
        );
        assert!(h.purpose.is_none() && !findings.is_empty());
    }

    #[test]
    fn header_path_canonical_for_lib_main_mod_nested_tests_build() {
        let pkg = |name: &str| Package {
            name: name.to_string(),
            manifest_rel: String::new(),
            deps: Vec::new(),
            entries: Vec::new(),
            toml_keys: BTreeSet::new(),
            exemptions: Vec::new(),
            grammar: Vec::new(),
            lib: None,
            main: None,
            build: None,
        };
        let a = pkg("humaux-infra-cell");
        let cases = [
            ("src/lib.rs", "humaux-infra-cell", Role::Lib),
            ("src/main.rs", "infra-cell::main", Role::Main),
            ("src/permit.rs", "infra-cell::permit", Role::Src),
            ("src/a/mod.rs", "infra-cell::a", Role::Src),
            ("src/a/b.rs", "infra-cell::a::b", Role::Src),
            ("tests/x.rs", "infra-cell::tests::x", Role::TestTop),
            (
                "tests/support/y.rs",
                "infra-cell::tests::support::y",
                Role::TestSub,
            ),
            (
                "tests/fault/mod.rs",
                "infra-cell::tests::fault",
                Role::TestSub,
            ),
            ("build.rs", "infra-cell::build", Role::Build),
        ];
        for (file, canonical, role) in cases {
            assert_eq!(
                canonical_of(&a, file),
                (canonical.to_string(), role),
                "{file}"
            );
        }
        assert_eq!(canonical_of(&pkg("xtask"), "src/soak.rs").0, "xtask::soak");
        let fx = Fx::new("header_path");
        fx.pkg("crates/infra-cell", "humaux-infra-cell", "").file(
            "crates/infra-cell/src/lib.rs",
            &hdr("infra-cell", NODEP, "[]", "[]"),
        );
        let found = fx.found("crates/infra-cell/src/lib.rs");
        assert!(
            found.iter().any(|(_, r, m)| *r == "header-path" && m.contains("computed: `humaux-infra-cell`")),
            "{found:?}"
        );
    }

    #[test]
    fn invariants_empty_only_without_services_and_env() {
        let fx = Fx::new("invariants");
        fx.pkg("crates/a", "humaux-a", "").file(
            "crates/a/src/lib.rs",
            &format!(
                "{}pub fn f() {{ let _ = std::env::var(\"HUMAUX_A\"); }}\n",
                hdr(
                    "humaux-a",
                    "crates=[]; services=[]; env=[HUMAUX_A]; modules=[]",
                    "[]",
                    "[]"
                )
            ),
        );
        assert_eq!(fx.rules("crates/a/src/lib.rs"), vec!["header-invariants"]);
        fx.file("crates/a/src/lib.rs", &hdr("humaux-a", NODEP, "[]", "[]"));
        assert!(fx.rules("crates/a/src/lib.rs").is_empty());
    }

    #[test]
    fn spec_refs_must_exist() {
        let idx = SpecIndex {
            sections: ["17".to_string(), "17.4".to_string()].into(),
            adrs: ["0051".to_string()].into(),
        };
        assert!(check_spec("Baseline §17.4; §17; ADR-0051", &idx).is_empty());
        assert!(check_spec("none", &idx).is_empty());
        assert_eq!(check_spec("Baseline §99.9", &idx).len(), 1);
        assert_eq!(check_spec("ADR-9999", &idx).len(), 1);
        assert_eq!(
            check_spec("see the code", &idx).len(),
            1,
            "no reference at all"
        );
        assert_eq!(
            baseline_sections("# 17. Qdrant\n## 17.4 Visible\n#### 6.2.2 x\nno # 5.1 heading\n"),
            ["17", "17.4", "6.2.2"]
                .iter()
                .map(|s| s.to_string())
                .collect()
        );
    }

    // ---- env -----------------------------------------------------------------------------

    #[test]
    fn env_literal_extraction_var_var_os_std_env_set_var_env_macro_and_command_env() {
        let src = "fn f(cmd: &mut Command) {\n\
                   std::env::var(\"A_VAR\"); env::var_os(\"B\"); var_os(\"C\");\n\
                   std::env::set_var(\"D\", \"1\"); env::remove_var(\"E\");\n\
                   env!(\"F\"); option_env!(\"G\"); cmd.env(\"H\", \"1\");\n\
                   // std::env::var(\"COMMENTED\")\n}\n";
        let found = env_found(&lex(src));
        let kinds: BTreeMap<&str, Vec<EnvKind>> = found
            .iter()
            .map(|(k, (_, v))| (k.as_str(), v.iter().copied().collect()))
            .collect();
        use EnvKind::{Build, Child, Runtime};
        let expected: BTreeMap<&str, Vec<EnvKind>> = [
            ("A_VAR", vec![Runtime]),
            ("B", vec![Runtime]),
            ("C", vec![Runtime]),
            ("D", vec![Runtime]),
            ("E", vec![Runtime]),
            ("F", vec![Build]),
            ("G", vec![Build]),
            ("H", vec![Child]),
        ]
        .into();
        assert_eq!(kinds, expected);
        assert_eq!(found["A_VAR"].0, 2);
    }

    #[test]
    fn env_humaux_literal_via_helper_and_const_counts_prefix_with_trailing_underscore_does_not() {
        let src = "const K: &str = \"HUMAUX_Z9\";\nfn f() { required(\"HUMAUX_X_Y\"); \
                   let p = \"HUMAUX_PREFIX_\"; let t = \"set HUMAUX_X in text\"; }\n";
        let found: Vec<String> = env_found(&lex(src)).into_keys().collect();
        assert_eq!(found, strs(&["HUMAUX_X_Y", "HUMAUX_Z9"]));
    }

    #[test]
    fn env_brace_expansion_and_declared_only_witness() {
        let (d, _) =
            parse_depends("crates=[]; services=[]; env=[HUMAUX_R_{A,B}_{C,D}, X]; modules=[]")
                .unwrap();
        assert_eq!(
            d.env.iter().cloned().collect::<Vec<_>>(),
            strs(&[
                "HUMAUX_R_A_C",
                "HUMAUX_R_A_D",
                "HUMAUX_R_B_C",
                "HUMAUX_R_B_D",
                "X"
            ])
        );
        let fx = Fx::new("env_witness");
        fx.pkg("crates/a", "humaux-a", "").file(
            "crates/a/src/lib.rs",
            &format!(
                "{}pub fn f() {{ let _ = std::env::var(\"HUMAUX_LIT\"); }}\n",
                hdr(
                    "humaux-a",
                    "crates=[]; services=[]; env=[HUMAUX_LIT, HUMAUX_BUILT_{A,B}]; modules=[]",
                    "[]",
                    "[reads config]"
                )
            ),
        );
        let an = fx.an();
        assert!(an.violations().0.is_empty(), "{:?}", an.violations().0);
        let doc = an.render_env_vars();
        assert!(
            doc.contains("| `HUMAUX_LIT` | runtime | — | `humaux-a` | literal | raw |"),
            "{doc}"
        );
        assert!(
            doc.contains("| `HUMAUX_BUILT_B` | runtime | — | `humaux-a` | declared-only | raw |"),
            "{doc}"
        );
    }

    /// Card 33b review pass 2 (findings 1, 11): a removed key named only to be refused is declared
    /// `refused:` and rendered `refused-at-boot`. Faults: drop the prefix handling → the literal is
    /// `env-undeclared`; drop the render arm → the row reads `runtime`.
    #[test]
    fn env_refused_key_is_declared_and_rendered_refused_at_boot() {
        let fx = Fx::new("env_refused");
        fx.pkg("crates/a", "humaux-a", "").file(
            "crates/a/src/lib.rs",
            &format!(
                "{}pub const GONE: &str = \"HUMAUX_A_GONE\";\npub fn f() {{ let _ = std::env::var(\"HUMAUX_A_LIVE\"); }}\n",
                hdr(
                    "humaux-a",
                    "crates=[]; services=[]; env=[HUMAUX_A_LIVE, refused:HUMAUX_A_{GONE}]; modules=[]",
                    "[]",
                    "[reads config]"
                )
            ),
        );
        let an = fx.an();
        assert!(an.violations().0.is_empty(), "{:?}", an.violations().0);
        let doc = an.render_env_vars();
        assert!(
            doc.contains("| `HUMAUX_A_GONE` | refused-at-boot | — | `humaux-a` | literal | raw |"),
            "{doc}"
        );
        assert!(
            doc.contains("| `HUMAUX_A_LIVE` | runtime | — | `humaux-a` | literal | raw |"),
            "{doc}"
        );
    }

    #[test]
    fn env_unregistered_fires_for_gateway_module_only() {
        let fx = Fx::new("env_unregistered");
        let dep = |env: &str| format!("crates=[]; services=[]; env=[{env}]; modules=[]");
        fx.pkg("bins/gateway", "humaux-gateway", "")
            .file(
                "bins/gateway/src/main.rs",
                "mod bootstrap;\nmod other;\nfn main() { bootstrap::run(); other::go(); }\n",
            )
            .file(
                "bins/gateway/src/bootstrap.rs",
                &format!(
                    "{}pub fn run() {{ std::env::var(\"HUMAUX_GATEWAY_A\"); }}\n",
                    hdr(
                        "gateway::bootstrap",
                        &dep("HUMAUX_GATEWAY_A"),
                        "[gateway::main]",
                        "[reads HUMAUX_GATEWAY_A]"
                    )
                ),
            )
            .file(
                "bins/gateway/src/other.rs",
                &format!(
                    "{}pub fn go() {{ std::env::var(\"HUMAUX_GATEWAY_B\"); }}\n",
                    hdr(
                        "gateway::other",
                        &dep("HUMAUX_GATEWAY_B"),
                        "[gateway::main]",
                        "[reads HUMAUX_GATEWAY_B]"
                    )
                ),
            );
        fx.pkg("bins/worker", "humaux-worker", "").file(
            "bins/worker/src/main.rs",
            &format!(
                "{}fn main() {{ std::env::var(\"HUMAUX_GATEWAY_C\"); }}\n",
                hdr(
                    "worker::main",
                    &dep("HUMAUX_GATEWAY_C"),
                    "[process(humaux-worker)]",
                    "[reads HUMAUX_GATEWAY_C]"
                )
            ),
        );
        assert_eq!(
            fx.rules("bins/gateway/src/other.rs"),
            vec!["env-unregistered"]
        );
        assert!(fx.rules("bins/gateway/src/bootstrap.rs").is_empty());
        assert!(fx.rules("bins/worker/src/main.rs").is_empty());
    }

    // ---- tables --------------------------------------------------------------------------

    fn table_fixture(name: &str, services: &str, body: &str) -> Vec<(usize, &'static str, String)> {
        let fx = Fx::new(name);
        fx.pkg("crates/a", "humaux-a", "").file(
            "crates/a/src/lib.rs",
            &format!(
                "{}{body}",
                hdr(
                    "humaux-a",
                    &format!("crates=[]; services=[{services}]; env=[]; modules=[]"),
                    "[]",
                    "[x]"
                )
            ),
        );
        fx.found("crates/a/src/lib.rs")
    }

    #[test]
    fn table_tokens_from_string_literals_ignore_comments_and_docs() {
        let body = "/// Reads ops.docs_only.\n// ops.comment_only\n\
                    pub fn f() -> &'static str { \"select 1 from ops.jobs where humaux.tenant_id = x\" }\n";
        let found = table_fixture("tables_lit", "PostgreSQL(role_gateway)", body);
        assert_eq!(
            found
                .iter()
                .map(|f| (f.1, f.2.as_str()))
                .collect::<Vec<_>>(),
            vec![("table-undeclared", "`ops.jobs` is in no r=/w= list")]
        );
        assert!(
            table_fixture(
                "tables_lit_ok",
                "PostgreSQL(role_gateway) r=[ops.jobs]",
                body
            )
            .is_empty()
        );
    }

    #[test]
    fn table_function_token_requires_x_list() {
        let body = "pub fn f() -> &'static str { \"select ops.claim_next(1)\" }\n";
        let found = table_fixture(
            "tables_fn",
            "PostgreSQL(role_gateway) r=[ops.claim_next]",
            body,
        );
        assert_eq!(
            found.iter().map(|f| f.1).collect::<Vec<_>>(),
            vec!["table-unwitnessed", "table-undeclared"],
            "{found:?}"
        );
        assert!(found[1].2.contains("`ops.claim_next(` is in no x= list"));
        assert!(
            table_fixture(
                "tables_fn_ok",
                "PostgreSQL(role_gateway) x=[ops.claim_next]",
                body
            )
            .is_empty()
        );
    }

    #[test]
    fn table_write_verbs_require_w_list() {
        let body = "pub fn f() -> [&'static str; 3] { [\"INSERT  INTO\n  ops.jobs (a) values (1)\", \
                    \"update ops.jobs set a = 1\", \"on conflict do update set a = 1\"] }\n";
        let found = table_fixture("tables_w", "PostgreSQL(role_gateway) r=[ops.jobs]", body);
        assert_eq!(
            found.iter().map(|f| f.1).collect::<Vec<_>>(),
            vec!["table-write"]
        );
        assert!(
            table_fixture("tables_w_ok", "PostgreSQL(role_gateway) w=[ops.jobs]", body).is_empty()
        );
        let t = db_tokens(&lex(
            "fn f() { \"copy staging.x from stdin\"; \"delete from control.y\"; \"truncate table ops.z\"; }",
        ));
        assert_eq!(
            t.writes.keys().cloned().collect::<Vec<_>>(),
            strs(&["control.y", "ops.z", "staging.x"])
        );
    }

    // ---- Cargo ---------------------------------------------------------------------------

    #[test]
    fn cargo_comment_grammar_line_above_dependency_with_prose_block() {
        let fx = Fx::new("cargo_why");
        fx.pkg(
            "crates/a",
            "humaux-a",
            "# §6.2.3 prose block, kept as is.\n# why: parse things; used-by: [humaux-a]\nserde = \"1\"\n\
             # prose only\nnowhy = \"1\"\n# why: empty list; used-by: []\nempty = \"1\"\n",
        )
        .file("crates/a/src/lib.rs", "use serde::X;\n");
        let found: Vec<(usize, &str)> = fx
            .found("crates/a/Cargo.toml")
            .into_iter()
            .map(|(l, r, _)| (l, r))
            .collect();
        assert_eq!(found, vec![(9, "cargo-why"), (11, "cargo-why")]);
        assert_eq!(
            parse_why("# why: r; used-by: [a::b, tests]"),
            Some(("r".into(), strs(&["a::b", "tests"])))
        );
        assert_eq!(parse_why("# why: ; used-by: [a]"), None);
    }

    #[test]
    fn cargo_used_by_must_name_ident_incl_package_rename() {
        let fx = Fx::new("cargo_usedby");
        fx.file(
            "crates/a/Cargo.toml",
            "[package]\nname = \"humaux-a\"\n\n[dependencies]\n\
             # why: renamed; used-by: [a::x, tests]\nren = { package = \"real-name\", version = \"1\" }\n\n\
             [dev-dependencies]\n# why: dev; used-by: [tests]\ndevdep = \"1\"\n",
        )
        .file("crates/a/src/lib.rs", "pub mod x;\n")
        .file("crates/a/src/x.rs", "pub fn f() { ren::go(); real_name::nope(); }\n")
        .file("crates/a/tests/t.rs", "use devdep::Y;\n");
        let found = fx.found("crates/a/Cargo.toml");
        assert_eq!(found.len(), 1, "{found:?}");
        assert_eq!(found[0].1, "cargo-usedby");
        assert!(
            found[0]
                .2
                .starts_with("`tests` does not name `ren` (computed: [a::x])"),
            "{found:?}"
        );
    }

    #[test]
    fn cargo_parse_crosscheck_catches_multiline_table() {
        let fx = Fx::new("cargo_parse");
        fx.pkg(
            "crates/a",
            "humaux-a",
            "# why: w; used-by: [humaux-a]\nserde = \"1\"\n\n[dependencies.hidden]\nversion = \"1\"\n",
        )
        .file("crates/a/src/lib.rs", "use serde::X;\n");
        let found = fx.found("crates/a/Cargo.toml");
        assert_eq!(
            found.iter().map(|f| f.1).collect::<Vec<_>>(),
            vec!["cargo-parse"],
            "{found:?}"
        );
        assert!(found[0].2.contains("`hidden`"));
    }

    // ---- Called-by / modules -------------------------------------------------------------

    #[test]
    fn called_by_use_tree_inline_path_super_self_child_mod() {
        let fx = Fx::new("calledby_forms");
        fx.pkg("crates/a", "humaux-a", "")
            .file("crates/a/src/lib.rs", "pub mod x;\npub mod y;\npub mod z;\npub mod w;\npub mod p;\n")
            .file("crates/a/src/x.rs", "pub fn fx() {}\n")
            .file("crates/a/src/z.rs", "pub struct Zed;\n")
            .file("crates/a/src/p.rs", "pub fn g() {}\n")
            .file("crates/a/src/y.rs", "use crate::{x, z::Zed as Z};\n")
            .file(
                "crates/a/src/w.rs",
                "mod inner;\npub fn f() { crate::x::fx(); super::p::g(); self::inner::h(); inner::h(); }\n",
            )
            .file("crates/a/src/w/inner.rs", "pub fn h() {}\n");
        let an = fx.an();
        assert_eq!(called_by(&an, "a::x"), strs(&["a::w", "a::y"]));
        assert_eq!(called_by(&an, "a::z"), strs(&["a::y"]));
        assert_eq!(called_by(&an, "a::p"), strs(&["a::w"]));
        assert_eq!(called_by(&an, "a::w::inner"), strs(&["a::w"]));
        assert!(called_by(&an, "a::w").is_empty(), "`mod w;` is not an edge");
    }

    #[test]
    fn called_by_longest_prefix_resolution_nested_module() {
        let fx = Fx::new("calledby_prefix");
        fx.pkg("crates/a", "humaux-a", "")
            .file("crates/a/src/lib.rs", "pub mod n;\npub mod u;\n")
            .file("crates/a/src/n.rs", "pub mod m;\npub struct Top;\n")
            .file("crates/a/src/n/m.rs", "pub struct S;\n")
            .file(
                "crates/a/src/u.rs",
                "use crate::n::m::S;\nuse crate::n::Top;\n",
            );
        let an = fx.an();
        assert_eq!(
            an.computed_modules(an.file_named("a::u").unwrap()),
            ["a::n", "a::n::m"].iter().map(|s| s.to_string()).collect()
        );
        assert_eq!(called_by(&an, "a::n::m"), strs(&["a::u"]));
    }

    #[test]
    fn called_by_pub_use_reexport_resolves_to_leaf_one_hop_incl_glob() {
        let fx = Fx::new("calledby_reexport");
        fx.pkg("crates/a", "humaux-a", "")
            .file(
                "crates/a/src/lib.rs",
                "mod inner;\nmod g;\npub use inner::Thing;\npub use g::*;\npub struct Local;\n",
            )
            .file("crates/a/src/inner.rs", "pub struct Thing;\n")
            .file("crates/a/src/g.rs", "pub fn helper() {}\n");
        fx.pkg("crates/b", "humaux-b", "humaux-a = { path = \"../a\" }\n")
            .file("crates/b/src/lib.rs", "use humaux_a::Thing;\npub fn f() { humaux_a::helper(); let _ = humaux_a::Local; }\n");
        let an = fx.an();
        assert_eq!(called_by(&an, "a::inner"), strs(&["humaux-b"]));
        assert_eq!(called_by(&an, "a::g"), strs(&["humaux-b"]));
        assert_eq!(called_by(&an, "humaux-a"), strs(&["crate(humaux-b)"]));
        assert!(
            an.edges[an.file_named("humaux-a").unwrap()].is_empty(),
            "pub use / mod lines are not edges"
        );
        assert_eq!(
            an.computed_modules(an.file_named("humaux-b").unwrap()),
            ["a::g", "a::inner", "humaux-a"]
                .iter()
                .map(|s| s.to_string())
                .collect()
        );
    }

    #[test]
    fn called_by_cfg_test_importers_collapse_to_tests() {
        let fx = Fx::new("calledby_cfgtest");
        fx.pkg("crates/a", "humaux-a", "")
            .file("crates/a/src/lib.rs", "pub mod x;\npub mod y;\n")
            .file("crates/a/src/x.rs", "pub fn f() {}\n")
            .file("crates/a/src/y.rs", "pub fn g() {}\n#[cfg(test)]\nmod tests {\n    use crate::x::f;\n    use super::*;\n}\n")
            .file("crates/a/tests/it.rs", "use humaux_a::x::f;\n");
        let an = fx.an();
        assert_eq!(called_by(&an, "a::x"), strs(&["tests"]));
        assert!(
            an.computed_modules(an.file_named("a::y").unwrap())
                .is_empty()
        );
        assert!(
            called_by(&an, "a::y").is_empty(),
            "`use super::*` in its own tests is a self edge"
        );
    }

    #[test]
    fn called_by_path_attr_support_files_list_individual_tests() {
        let fx = Fx::new("calledby_path");
        fx.pkg("crates/a", "humaux-a", "")
            .file("crates/a/src/lib.rs", "")
            .file(
                "crates/a/tests/one.rs",
                "#[path = \"support/fx.rs\"]\nmod fx;\n",
            )
            .file(
                "crates/a/tests/two.rs",
                "#[path = \"support/fx.rs\"]\nmod fx;\nfn t() { fx::go(); }\n",
            )
            .file("crates/a/tests/support/fx.rs", "pub fn go() {}\n");
        let an = fx.an();
        assert_eq!(
            called_by(&an, "a::tests::support::fx"),
            strs(&["a::tests::one", "a::tests::two"])
        );
        assert_eq!(called_by(&an, "a::tests::one"), strs(&["cargo-test"]));
        assert_eq!(
            an.computed_modules(an.file_named("a::tests::two").unwrap()),
            ["a::tests::support::fx".to_string()].into()
        );
    }

    #[test]
    fn called_by_roots_process_crate_cargo_test_cargo_build() {
        let fx = Fx::new("calledby_roots");
        fx.pkg("crates/a", "humaux-a", "")
            .file("crates/a/src/lib.rs", "");
        fx.pkg(
            "bins/g",
            "humaux-g",
            "humaux-a = { path = \"../../crates/a\" }\n",
        )
        .file("bins/g/src/main.rs", "fn main() {}\n")
        .file("bins/g/build.rs", "fn main() {}\n")
        .file("bins/g/tests/t.rs", "");
        fx.pkg("xtask", "xtask", "")
            .file("xtask/src/main.rs", "fn main() {}\n");
        let an = fx.an();
        assert_eq!(called_by(&an, "g::main"), strs(&["process(humaux-g)"]));
        assert_eq!(
            called_by(&an, "xtask::main"),
            strs(&["process(cargo-xtask)"])
        );
        assert_eq!(called_by(&an, "humaux-a"), strs(&["crate(humaux-g)"]));
        assert_eq!(called_by(&an, "g::build"), strs(&["cargo-build"]));
        assert_eq!(called_by(&an, "g::tests::t"), strs(&["cargo-test"]));
        assert_eq!(
            an.processes.keys().cloned().collect::<Vec<_>>(),
            strs(&["process(cargo-xtask)", "process(humaux-g)"])
        );
    }

    #[test]
    fn called_by_phantom_and_missing_both_reported_with_computed_list() {
        let fx = Fx::new("calledby_diff");
        fx.pkg("crates/a", "humaux-a", "")
            .file("crates/a/src/lib.rs", "pub mod x;\npub mod y;\n")
            .file(
                "crates/a/src/x.rs",
                &hdr("a::x", NODEP, "[a::nobody]", "[]"),
            )
            .file("crates/a/src/y.rs", "use crate::x;\n");
        let found = fx.found("crates/a/src/x.rs");
        let msgs: Vec<String> = found.iter().map(|(_, r, m)| format!("{r} {m}")).collect();
        assert_eq!(
            msgs,
            vec![
                "calledby-missing [a::y] not listed (computed: [a::y])".to_string(),
                "calledby-phantom [a::nobody] import nothing here (computed: [a::y])".to_string(),
            ]
        );
    }

    #[test]
    fn modules_drift_set_equality_non_test_edges() {
        let fx = Fx::new("modules_drift");
        let y = |modules: &str| {
            format!(
                "{}use crate::x;\nuse crate::z;\n#[cfg(test)]\nmod tests {{ use crate::w; }}\n",
                hdr(
                    "a::y",
                    &format!("crates=[]; services=[]; env=[]; modules=[{modules}]"),
                    "[]",
                    "[]"
                )
            )
        };
        fx.pkg("crates/a", "humaux-a", "")
            .file(
                "crates/a/src/lib.rs",
                "pub mod x;\npub mod y;\npub mod z;\npub mod w;\n",
            )
            .file("crates/a/src/x.rs", "")
            .file("crates/a/src/z.rs", "")
            .file("crates/a/src/w.rs", "")
            .file("crates/a/src/y.rs", &y("a::x"));
        let found = fx.found("crates/a/src/y.rs");
        assert_eq!(found.len(), 1, "{found:?}");
        assert_eq!(
            found[0].2,
            "modules= extra [] missing [a::z] (computed: [a::x, a::z])"
        );
        fx.file("crates/a/src/y.rs", &y("a::x, a::z"));
        assert!(fx.found("crates/a/src/y.rs").is_empty());
    }

    // ---- call sites and tags -------------------------------------------------------------

    #[test]
    fn callsite_patterns_each_class_matches_and_channel_send_does_not() {
        let role = format!("\"SET LOCAL {} role_x\"", "ROLE");
        let src = format!(
            "fn f() {{\n\
             Client::connect(&dsn, NoTls);\n\
             RuntimeDbPool::connect(&cfg);\n\
             PgPoolOptions::new();\n\
             c.begin();\n\
             c.build_transaction();\n\
             q.execute(&self.pool);\n\
             sqlx::query(x).fetch_one(pool);\n\
             let _ = {role};\n\
             IntraCellRequest {{ a }};\n\
             req.send();\n\
             t.call(permit, r);\n\
             UnixStream::connect(p);\n\
             UnixListener::bind(p);\n\
             TcpStream::connect(a);\n\
             std::process::Command::new(\"sh\");\n\
             tx.send(msg); q.execute(&mut tx); t.call(req); // Client::connect(\n\
             }}\n"
        );
        let classes: Vec<(usize, &str)> = call_sites(&lex(&src))
            .into_iter()
            .map(|(l, c)| (l, c.id()))
            .collect();
        assert_eq!(
            classes,
            vec![
                (2, "pg-connect"),
                (3, "pg-connect"),
                (4, "pg-connect"),
                (5, "pg-txn"),
                (6, "pg-txn"),
                (7, "pg-pool-exec"),
                (8, "pg-pool-exec"),
                (9, "pg-role-switch"),
                (10, "qdrant-request"),
                (11, "http-send"),
                (12, "egress-call"),
                (13, "uds"),
                (14, "uds"),
                (15, "tcp"),
                (16, "subprocess"),
            ]
        );
    }

    fn tagged(name: &str, services: &str, body: &str) -> Vec<(usize, &'static str, String)> {
        let fx = Fx::new(name);
        fx.pkg("crates/a", "humaux-a", "").file(
            "crates/a/src/lib.rs",
            &format!(
                "{}{body}",
                hdr(
                    "humaux-a",
                    &format!("crates=[]; services=[{services}]; env=[]; modules=[]"),
                    "[]",
                    "[x]"
                )
            ),
        );
        fx.found("crates/a/src/lib.rs")
    }

    #[test]
    fn callsite_tag_window_3_lines_above_match_or_statement_start() {
        // header = 5 lines; body starts on line 6.
        let chain = "pub fn f() {\n\
                     // dep: Qdrant(*) — one tag above the statement covers the chain\n\
                     let r = transport\n\
                     .execute(\n\
                     permit,\n\
                     IntraCellRequest {\n\
                     a,\n\
                     },\n\
                     );\n\
                     }\n";
        assert!(tagged("window_stmt", "Qdrant(*)", chain).is_empty());
        let far = "pub fn f() {\n\
                   // dep: Qdrant(*) — too far away\n\
                   let a = 1;\n\
                   let b = 2;\n\
                   let c = 3;\n\
                   let r = IntraCellRequest { a };\n\
                   }\n";
        let found = tagged("window_far", "Qdrant(*)", far);
        assert_eq!(
            found.iter().map(|f| (f.0, f.1)).collect::<Vec<_>>(),
            vec![(11, "callsite-untagged")]
        );
        let wrong = "pub fn f() {\n// dep: Qdrant(*) — wrong service for a subprocess\nCommand::new(\"sh\");\n}\n";
        let found = tagged("window_wrong", "Qdrant(*)", wrong);
        assert_eq!(
            found.iter().map(|f| f.1).collect::<Vec<_>>(),
            vec!["callsite-untagged"]
        );
    }

    #[test]
    fn tag_grammar_rejects_legacy_postgres_spelling() {
        assert!(parse_tag("// dep: Postgres (role_x, DSN) — legacy").is_none());
        assert!(parse_tag("// dep: PostgreSQL(role_x) - hyphen").is_none());
        assert!(parse_tag("// dep: sh — lowercase program name").is_none());
        assert_eq!(
            parse_tag("    // dep: PostgreSQL(role_x) — ok"),
            Some((Service::PostgreSQL, Some("role_x".into())))
        );
        assert_eq!(
            parse_tag("// dep: subprocess(ps) — ok"),
            Some((Service::Subprocess, Some("ps".into())))
        );
        let found = tagged(
            "tag_legacy",
            "PostgreSQL(owner)",
            "pub fn f() {\n// dep: Postgres (role_x, DSN) — legacy\nClient::connect(&d, NoTls);\n}\n",
        );
        assert_eq!(
            found.iter().map(|f| f.1).collect::<Vec<_>>(),
            vec!["service-unwitnessed", "tag-grammar", "callsite-untagged"]
        );
    }

    #[test]
    fn tag_service_must_be_declared_and_role_match() {
        let body = |tag: &str| {
            format!("pub fn f() {{\n// dep: {tag} — why\nClient::connect(&d, NoTls);\n}}\n")
        };
        let rules = |name: &str, services: &str, tag: &str| -> Vec<&'static str> {
            tagged(name, services, &body(tag))
                .into_iter()
                .map(|f| f.1)
                .collect()
        };
        assert!(
            rules(
                "tag_ok",
                "PostgreSQL(role_gateway)",
                "PostgreSQL(role_gateway)"
            )
            .is_empty()
        );
        assert_eq!(
            rules(
                "tag_role",
                "PostgreSQL(role_gateway)",
                "PostgreSQL(role_private_worker)"
            ),
            vec!["service-unwitnessed", "service-undeclared"]
        );
        assert!(rules("tag_any", "PostgreSQL(role_gateway)", "PostgreSQL(any)").is_empty());
        assert_eq!(
            rules("tag_svc", "Qdrant(*)", "PostgreSQL(role_gateway)"),
            vec!["service-unwitnessed", "service-undeclared"]
        );
        assert_eq!(
            rules(
                "tag_vocab",
                "PostgreSQL(role_gateway)",
                "PostgreSQL(role_worker)"
            ),
            vec!["service-unwitnessed", "tag-grammar"],
            "`role_worker` is not a role"
        );
    }

    // ---- card 26 review: truth of what a header claims ----------------------------------

    /// P0: `INSERT INTO t(cols)` is a write of relation `t`, not an EXECUTE of function `t`.
    #[test]
    fn table_insert_with_column_list_is_a_write_not_a_function() {
        let t = db_tokens(&lex(
            "fn f() { \"INSERT INTO ops.jobs(kind) VALUES ('x')\"; \"insert into ops.outbox (a) values (1)\"; \
             \"create table if not exists ops.t2(id int references control.users(id))\"; \
             \"select ops.claim_next(1)\"; \"select * from ops.rows_of(1)\"; }",
        ));
        assert_eq!(
            t.writes.keys().cloned().collect::<Vec<_>>(),
            strs(&["ops.jobs", "ops.outbox"])
        );
        assert_eq!(
            t.functions.keys().cloned().collect::<Vec<_>>(),
            strs(&["ops.claim_next", "ops.rows_of"])
        );
        assert!(t.relations.contains_key("control.users") && t.relations.contains_key("ops.t2"));
        // The reviewer's scratch fault: declaring the table under x= no longer silences it.
        let body = "pub fn f() -> &'static str { \"INSERT INTO ops.jobs(kind) VALUES ('x')\" }\n";
        let found = table_fixture("insert_cols", "PostgreSQL(role_gateway) x=[ops.jobs]", body);
        assert_eq!(
            found.iter().map(|f| f.1).collect::<Vec<_>>(),
            vec!["table-unwitnessed", "table-undeclared", "table-write"],
            "{found:?}"
        );
        assert!(
            table_fixture(
                "insert_cols_ok",
                "PostgreSQL(role_gateway) w=[ops.jobs]",
                body
            )
            .is_empty()
        );
    }

    /// P1: crates=, services= and r/w/x= entries need a witness in the file's code.
    #[test]
    fn overdeclared_crates_services_and_tables_are_errors() {
        let fx = Fx::new("overdeclared");
        fx.pkg("crates/a", "humaux-a", "# why: w; used-by: [humaux-a]\nserde = \"1\"\n")
            .file(
                "crates/a/src/lib.rs",
                &format!(
                    "{}use serde::X;\n",
                    hdr(
                        "humaux-a",
                        "crates=[rand, serde]; services=[PostgreSQL(role_gateway) x=[ops.jobs], MiniMax, \
                         UDS(serve)]; env=[]; modules=[]",
                        "[]",
                        "[x]"
                    )
                ),
            );
        let found = fx.found("crates/a/src/lib.rs");
        let rules: Vec<&str> = found.iter().map(|f| f.1).collect();
        assert_eq!(
            rules,
            vec![
                "crates-unwitnessed",
                "service-unwitnessed",
                "service-unwitnessed",
                "service-unwitnessed",
                "table-unwitnessed"
            ],
            "{found:?}"
        );
        assert!(found[0].2.contains("[rand]"));
    }

    /// A declared service is witnessed by its tag, a typed pool (role), a dependency env
    /// literal or a testkit marker — code facts, never the header itself.
    #[test]
    fn service_witnessed_by_typed_pool_env_literal_or_marker() {
        let ok = tagged(
            "witness_ok",
            "PostgreSQL(role_private_worker), MiniMax",
            "pub fn f(p: &PrivateWorkerDbPool) { let _ = std::env::var(\"MINIMAX_API_KEY\"); }\n",
        );
        assert_eq!(
            ok.iter().map(|f| f.1).collect::<Vec<_>>(),
            vec!["env-undeclared"],
            "{ok:?}"
        );
        let wrong_role = tagged(
            "witness_role",
            "PostgreSQL(role_gateway)",
            "pub fn f(p: &PrivateWorkerDbPool) {}\n",
        );
        assert_eq!(
            wrong_role.iter().map(|f| f.1).collect::<Vec<_>>(),
            vec!["service-unwitnessed"]
        );
    }

    /// P1: the detail vocabulary is closed per service.
    #[test]
    fn service_detail_vocabulary_is_closed() {
        let procs: BTreeSet<String> = ["gateway", "private-worker", "retrieval-worker"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let bad = [
            (Service::PostgreSQL, Some("PRIMARY")),
            (Service::PostgreSQL, None),
            (Service::Qdrant, None),
            (Service::Uds, None),
            (Service::Uds, Some("peer")),
            (Service::Subprocess, Some("proc")),
            (Service::Subprocess, None),
            (Service::Http, Some("humaux-gateway")),
            (Service::Http, None),
            (Service::MiniMax, Some("x")),
        ];
        for (svc, d) in bad {
            assert!(detail_error(svc, d, &procs).is_some(), "{svc:?}({d:?})");
        }
        let good = [
            (Service::PostgreSQL, Some("role_retrieval_worker")),
            (Service::PostgreSQL, Some("any")),
            (Service::PostgreSQL, Some("owner")),
            (Service::Qdrant, Some("*")),
            (Service::Uds, Some("serve")),
            (Service::Uds, Some("private-worker")),
            (Service::Subprocess, Some("gitleaks")),
            (Service::Http, Some("gateway")),
            (Service::DashScope, None),
        ];
        for (svc, d) in good {
            assert!(detail_error(svc, d, &procs).is_none(), "{svc:?}({d:?})");
        }
    }

    /// P1: where the code fixes the detail (typed pool, `SET ROLE`, UDS side, program
    /// literal) the tag must say it; `XCommand::new(` is not a spawn.
    #[test]
    fn tag_mismatch_pool_role_role_switch_uds_side_and_program() {
        let body = "pub async fn f() {\n\
                    // dep: PostgreSQL(role_private_worker) — wrong role for this pool\n\
                    let p = RetrievalWorkerDbPool::connect(&d).await;\n\
                    // dep: UDS(retrieval-worker) — a bind is the server side\n\
                    let l = UnixListener::bind(&x);\n\
                    // dep: subprocess(git) — wrong program\n\
                    let c = Command::new(\"/usr/bin/gitleaks\");\n\
                    let n = StartManualContributionCommand::new(&y);\n\
                    // dep: PostgreSQL(role_gateway) — wrong role for the switch\n\
                    let s = \"SET LOCAL ROLE role_maintenance\";\n}\n";
        let found = tagged(
            "tag_mismatch",
            "PostgreSQL(role_private_worker), PostgreSQL(role_gateway), UDS(retrieval-worker), subprocess(git)",
            body,
        );
        let mism: Vec<(usize, String)> = found
            .iter()
            .filter(|f| f.1 == "tag-mismatch")
            .map(|f| (f.0, f.2.clone()))
            .collect();
        assert_eq!(mism.len(), 4, "{found:?}");
        assert!(mism[0].1.contains("fixes `role_retrieval_worker`"));
        assert!(mism[1].1.contains("fixes `serve`"));
        assert!(mism[2].1.contains("fixes `gitleaks`"));
        assert!(mism[3].1.contains("fixes `role_maintenance`"));
        assert!(
            !found.iter().any(|f| f.1 == "callsite-untagged"),
            "{found:?}"
        );
    }

    fn header_fixture(name: &str, header: &str) -> Vec<&'static str> {
        let fx = Fx::new(name);
        fx.pkg("crates/a", "humaux-a", "").file(
            "crates/a/src/lib.rs",
            &format!("{header}\npub fn f() {{}}\n"),
        );
        fx.rules("crates/a/src/lib.rs")
    }

    /// P1: field 1 is one whole sentence; a half sentence continues on `//!  ` lines.
    #[test]
    fn header_purpose_must_be_a_whole_sentence() {
        let fields = "//! Depends-on: crates=[]; services=[]; env=[]; modules=[]\n//! Called-by: []\n\
                      //! Invariants: []\n//! Spec: none";
        assert_eq!(
            header_fixture(
                "purpose_half",
                &format!("//! `humaux-a` — the pure half of T3.3's\n{fields}")
            ),
            vec!["header-purpose"]
        );
        assert!(
            header_fixture(
                "purpose_cont",
                &format!(
                    "//! `humaux-a` — the pure half of T3.3's\n//!   three-way check.\n{fields}"
                )
            )
            .is_empty()
        );
    }

    /// P1: `Spec:` must reflect the references the module's own prose cites.
    #[test]
    fn spec_must_cite_what_the_prose_cites() {
        let h = |spec: &str| {
            format!(
                "//! `humaux-a` — x.\n//! Depends-on: crates=[]; services=[]; env=[]; modules=[]\n\
                 //! Called-by: []\n//! Invariants: []\n//! Spec: {spec}\n//!\n//! Implements §17.4 visibility."
            )
        };
        assert_eq!(header_fixture("spec_none", &h("none")), vec!["spec-ref"]);
        assert_eq!(
            header_fixture("spec_other", &h("Baseline §17")),
            vec!["spec-ref"]
        );
        assert!(header_fixture("spec_ok", &h("Baseline §17.4")).is_empty());
    }

    /// P1: `Invariants:` states this module's invariant — no pointer to prose, and no text
    /// shared verbatim by `INVARIANT_COPIES` files.
    #[test]
    fn invariants_pointer_and_boilerplate_are_errors() {
        let dep = "crates=[]; services=[]; env=[HUMAUX_X]; modules=[]";
        let body = "pub fn f() { std::env::var(\"HUMAUX_X\"); }\n";
        let fx = Fx::new("inv_boiler");
        fx.pkg("crates/a", "humaux-a", "");
        for (i, m) in ["a", "b", "c"].iter().enumerate() {
            fx.file(
                &format!("crates/a/src/{m}.rs"),
                &format!(
                    "{}{body}",
                    hdr(&format!("a::{m}"), dep, "[]", "[same text]")
                ),
            );
            let _ = i;
        }
        fx.file(
            "crates/a/src/lib.rs",
            &format!(
                "{}pub mod a;\npub mod b;\npub mod c;\n",
                hdr("humaux-a", NODEP, "[]", "[see prose below for behaviour]")
            ),
        );
        for m in ["a", "b", "c"] {
            assert_eq!(
                fx.rules(&format!("crates/a/src/{m}.rs")),
                vec!["header-invariants"]
            );
        }
        assert_eq!(fx.rules("crates/a/src/lib.rs"), vec!["header-invariants"]);
    }

    // ---- exemptions ----------------------------------------------------------------------

    #[test]
    fn exemption_line_and_file_scope_suppress_and_are_counted() {
        let fx = Fx::new("exempt_ok");
        let head = hdr("humaux-a", NODEP, "[]", "[]");
        fx.pkg("crates/a", "humaux-a", "").file(
            "crates/a/src/lib.rs",
            &format!(
                "{head}//! dep-map: allow table-undeclared — transcribed matrix fixture\n\
                 pub fn f() {{\n    // dep-map: allow env-undeclared — fixture name, never read\n    \
                 let _ = std::env::var(\"HUMAUX_A\");\n    let _ = \"select 1 from ops.jobs, ops.more\";\n}}\n"
            ),
        );
        let an = fx.an();
        let (v, ex) = an.violations();
        assert!(v.is_empty(), "{v:?}");
        let used: Vec<(&str, usize, bool)> = ex
            .iter()
            .map(|e| (e.rule.as_str(), e.used, e.target.is_none()))
            .collect();
        assert_eq!(
            used,
            vec![("table-undeclared", 2, true), ("env-undeclared", 1, false)]
        );
        let doc = an.render_dependency_map(&ex);
        assert!(doc.contains("## Exemptions (2)"), "{doc}");
        assert!(
            doc.contains(
                "- `crates/a/src/lib.rs:6` [table-undeclared] (file) — transcribed matrix fixture"
            ),
            "{doc}"
        );
    }

    #[test]
    fn exemption_unused_and_bad_grammar_are_violations() {
        let fx = Fx::new("exempt_bad");
        let head = hdr("humaux-a", NODEP, "[]", "[]");
        fx.pkg("crates/a", "humaux-a", "").file(
            "crates/a/src/lib.rs",
            &format!(
                "{head}pub fn f() {{\n    // dep-map: allow header-field — cannot exempt this\n    let a = 1;\n    \
                 // dep-map: allow env-undeclared — TODO\n    let b = 2;\n    \
                 // dep-map: allow env-undeclared — nothing to suppress\n    let c = 3;\n}}\n"
            ),
        );
        let found: Vec<(usize, &str)> = fx
            .found("crates/a/src/lib.rs")
            .into_iter()
            .map(|(l, r, _)| (l, r))
            .collect();
        assert_eq!(
            found,
            vec![
                (7, "exemption-grammar"),
                (9, "exemption-grammar"),
                (11, "exemption-unused")
            ]
        );
    }

    // ---- generated docs ------------------------------------------------------------------

    fn docs_fixture(name: &str) -> Fx {
        let fx = Fx::new(name);
        fx.pkg("crates/a", "humaux-a", "# why: w; used-by: [humaux-a]\nserde = \"1\"\n")
            .file(
                "crates/a/src/lib.rs",
                &format!(
                    "{}use serde::X;\npub fn f() {{ let _ = std::env::var(\"HUMAUX_A\"); let _ = \"select 1 from ops.jobs\"; }}\n",
                    hdr("humaux-a", "crates=[serde]; services=[PostgreSQL(role_gateway) r=[ops.jobs]]; env=[HUMAUX_A]; modules=[]", "[]", "[x]")
                ),
            );
        fx
    }

    #[test]
    fn generated_docs_deterministic_twice_byte_identical_no_timestamp() {
        let fx = docs_fixture("docs_det");
        let (a, _) = evaluate(&fx.an(), Ok(None), Vec::new());
        let (b, _) = evaluate(&fx.an(), Ok(None), Vec::new());
        assert_eq!(a.docs, b.docs);
        let root = fx.root.to_string_lossy().into_owned();
        for (path, doc) in a
            .docs
            .iter()
            .filter_map(|(p, d)| d.as_ref().map(|d| (p, d)))
        {
            assert!(!doc.contains(&root), "{path} must use repo-relative paths");
            assert!(
                !doc.contains(" UTC") && !doc.contains("202"),
                "{path} has a timestamp-like token"
            );
        }
        let map = a.docs[0].1.as_ref().unwrap();
        assert!(
            map.contains("| `ops.jobs` | (no process): humaux-a | — | — |"),
            "{map}"
        );
        assert!(
            map.contains("| humaux-a | `serde` | normal | w | [humaux-a] |"),
            "{map}"
        );
    }

    #[test]
    fn check_reports_doc_drift_on_hand_edit() {
        let fx = docs_fixture("docs_drift");
        let (out, _) = evaluate(&fx.an(), Ok(None), Vec::new());
        for (path, doc) in &out.docs {
            if let Some(doc) = doc {
                fx.file(path, doc);
            }
        }
        let (clean, _) = evaluate(&fx.an(), Ok(None), Vec::new());
        assert_eq!(
            clean.drift,
            vec![DOC_DB],
            "only the DB doc (not rendered without a catalog)"
        );
        assert!(clean.violations.is_empty(), "{:?}", clean.violations);
        let env = fs::read_to_string(fx.root.join(DOC_ENV)).unwrap();
        fx.file(DOC_ENV, &env.replace("literal", "hand-edited"));
        let (dirty, _) = evaluate(&fx.an(), Ok(None), Vec::new());
        assert_eq!(dirty.drift, vec![DOC_DB, DOC_ENV]);
        assert!(
            dirty
                .violations
                .iter()
                .any(|f| f.rule == "doc-drift" && f.path == DOC_ENV && f.line > 1),
            "{:?}",
            dirty.violations
        );
    }

    // ---- test targets --------------------------------------------------------------------

    #[test]
    fn test_target_services_raw_dsn_binary_is_db_bound() {
        let fx = Fx::new("targets");
        fx.file(
            "crates/a/tests/raw.rs",
            "fn t() { let d = std::env::var(\"HUMAUX_TEST_PG_DSN\").unwrap(); }\n",
        )
        .file(
            "crates/a/tests/qd.rs",
            "#[path = \"support/q.rs\"]\nmod q;\n",
        )
        .file(
            "crates/a/tests/support/q.rs",
            "fn t() { let _ = IntraCellRequest { a }; }\n",
        )
        .file(
            "crates/a/tests/sub.rs",
            "fn t() { std::process::Command::new(\"sh\"); }\n",
        )
        .file("bins/b/tests/raw.rs", "fn t() {}\n");
        let t = test_target_services(&fx.root);
        assert_eq!(t["raw"], [Service::PostgreSQL].into());
        assert_eq!(t["qd"], [Service::Qdrant].into());
        assert_eq!(t["sub"], [Service::Subprocess].into());
        assert!(!t.contains_key("q"), "support files are not targets");
    }

    // ---- the real tree -------------------------------------------------------------------

    fn repo_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("..")
    }

    /// The two compliant sample headers (this file and gate_truth.rs) pass every rule on the
    /// real tree, before the backfill.
    #[test]
    fn real_tree_sample_headers_pass() {
        let an = Analysis::new(&repo_root()).expect("repo analysis");
        let (v, _) = an.violations();
        let bad: Vec<&Finding> = v
            .iter()
            .filter(|f| f.path == "xtask/src/dep_map.rs" || f.path == "xtask/src/gate_truth.rs")
            .collect();
        assert!(bad.is_empty(), "{bad:?}");
        assert_eq!(an.fixtures.len(), 25, "trybuild fixtures excluded by path");
    }

    // ---- DB ------------------------------------------------------------------------------

    #[test]
    fn db_objects_head_mismatch_refuses() {
        const TEST: &str = "db_objects_head_mismatch_refuses";
        if std::env::var(DSN_ENV).is_err() {
            skip_or_fail(
                TEST,
                "missing object: HUMAUX_TEST_PG_DSN",
                ExternalDep::Postgres,
            );
            return;
        }
        let fx = Fx::new("db_head");
        fx.file(
            "migrations/0001_not_applied.sql",
            "create table ops.nothing (a int);\n",
        );
        let mut findings = Vec::new();
        let cat = read_catalog(&fx.root, &mut findings).expect("reachable");
        assert!(cat.is_none(), "a catalog not at head is not rendered");
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].rule, "db-not-at-head");
        assert!(findings[0].msg.contains("0001_not_applied"), "{findings:?}");
    }

    #[test]
    fn db_objects_renders_owner_rls_force_grants_created_in() {
        const TEST: &str = "db_objects_renders_owner_rls_force_grants_created_in";
        if std::env::var(DSN_ENV).is_err() {
            skip_or_fail(
                TEST,
                "missing object: HUMAUX_TEST_PG_DSN",
                ExternalDep::Postgres,
            );
            return;
        }
        let root = repo_root();
        let mut findings = Vec::new();
        let cat = read_catalog(&root, &mut findings)
            .expect("reachable")
            .unwrap_or_else(|| panic!("shared DB must be at migration head: {findings:?}"));
        let an = Analysis::new(&root).expect("repo analysis");
        let doc = an.render_db_objects(&cat);
        let row = doc
            .lines()
            .find(|l| l.starts_with("| `control.memberships` |"))
            .unwrap_or_else(|| panic!("no control.memberships row"));
        let cols: Vec<&str> = row.split(" | ").collect();
        assert_eq!(cols[1], "table", "{row}");
        assert!(!cols[2].is_empty() && cols[2] != "—", "owner: {row}");
        assert!(
            matches!(cols[3], "yes" | "no") && matches!(cols[4], "yes" | "no"),
            "{row}"
        );
        assert!(cols[5].contains("role_"), "grants per runtime role: {row}");
        assert_eq!(cols[6], "0003_control_core", "{row}");
        assert!(
            !doc.contains("ops.schema_migrations"),
            "bootstrap table is created by no migration"
        );
        let again = an.render_db_objects(&read_catalog(&root, &mut Vec::new()).unwrap().unwrap());
        assert_eq!(doc, again, "catalog rendering is deterministic");
    }
}
