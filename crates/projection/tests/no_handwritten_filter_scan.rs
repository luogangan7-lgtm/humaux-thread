//! `projection::tests::no_handwritten_filter_scan` — §17.1 architecture-check 风格扫描："所有 private query adapter 必须自动注入
//!   tenant + AuthorizationScope visibility filter；业务层不得手写可选 filter。
//! Depends-on: crates=[]; services=[]; env=[CARGO_MANIFEST_DIR]; modules=[]
//! Called-by: [cargo-test]
//! Invariants: [source scan: outside dense.rs no code builds a Condition tree or a DenseQueryFilter literal; an
//!   unlocatable workspace root prints SKIP rather than passing silently]
//! Spec: none
//!
//! "
//!
//! `dense::Condition`/`dense::DenseQueryFilter` 的私有字段已经是编译期的类型级证明——
//! `DenseQueryFilter` 的唯一构造点是 `dense::build_dense_filter`（见该模块的 rustdoc）；本文件
//! 补的是这条防线的可见性：扫描整个 workspace 源码，断言除了 `dense.rs`（唯一允许拼
//! `Condition::And`/`Or` 树的地方）之外，没有第二处直接构造 `Condition` 的变体，也没有第二处
//! 直接写 `DenseQueryFilter(`（tuple-struct literal，唯一的字段本来就是私有的，这里只是把「没
//! 有绕过」钉成一条会随源码演进持续跑的断言，而不是只在当下这次审查成立）。
//!
//! 三态：workspace 根目录（`CARGO_MANIFEST_DIR/../..`）找不到就 `eprintln!` 缺失对象名后
//! `return`（skip），不静默 pass；扫描本身在 CI 环境下总能找到 workspace 根，所以这条通常总
//! 是跑到断言阶段。

use std::path::{Path, PathBuf};

fn workspace_root() -> Option<PathBuf> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")); // crates/projection
    let root = manifest_dir.parent()?.parent()?; // repo root
    root.join("crates").is_dir().then(|| root.to_path_buf())
}

/// 递归收集 `dir` 下所有 `.rs` 文件，跳过 `target/`。
fn collect_rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if path.file_name().and_then(|n| n.to_str()) == Some("target") {
                continue;
            }
            collect_rs_files(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

/// 判别「构造」与「模式匹配」：`Condition::And(x)` 在 `match` 里是**消费**（把已建好的
/// filter 翻译成 provider 线格式，例如 `adapters::qdrant`），在表达式位置才是**构造**。
/// 纯 `contains` 两者都命中——那会把合法的翻译层误报成绕过注入（本闸上线当天即如此误报）。
///
/// 判据：从命中处的定界符（`(` 或 `{`）起按深度扫到它的配对符，跳过空白后若紧跟 `=>`，
/// 这就是一条 match 分支的模式，不是构造。多行分支同样成立（判据看的是配对符之后的下一个
/// 记号，不是同一行）。
fn occurrence_is_pattern_not_construction(source: &str, at: usize, pattern: &str) -> bool {
    let open_rel = match pattern.chars().last() {
        Some(c @ ('(' | '{')) => c,
        _ => return false,
    };
    let close = if open_rel == '(' { ')' } else { '}' };
    let bytes = source.as_bytes();
    let mut i = at + pattern.len();
    let mut depth = 1usize;
    while i < bytes.len() && depth > 0 {
        let c = bytes[i] as char;
        if c == open_rel {
            depth += 1;
        } else if c == close {
            depth -= 1;
        }
        i += 1;
    }
    if depth != 0 {
        return false; // 不配对：当作构造处理（宁可误报也不放过）
    }
    let rest = source[i..].trim_start();
    rest.starts_with("=>")
}

/// 收集 `source` 里 `pattern` 的**构造**位置（排除 match 分支模式）。
fn construction_hits(source: &str, pattern: &str) -> usize {
    let mut n = 0usize;
    let mut from = 0usize;
    while let Some(rel) = source[from..].find(pattern) {
        let at = from + rel;
        if !occurrence_is_pattern_not_construction(source, at, pattern) {
            n += 1;
        }
        from = at + pattern.len();
    }
    n
}

#[test]
fn no_source_file_outside_dense_rs_constructs_condition_directly() {
    let Some(root) = workspace_root() else {
        eprintln!(
            "SKIP no_source_file_outside_dense_rs_constructs_condition_directly: workspace root not found from CARGO_MANIFEST_DIR"
        );
        return;
    };

    let allowed_file = root.join("crates/projection/src/dense.rs");
    let this_file = root.join("crates/projection/tests/no_handwritten_filter_scan.rs");

    let banned_patterns = [
        "Condition::And(",
        "Condition::Or(",
        "Condition::Eq {",
        "Condition::In {",
        "Condition::Range {",
        "DenseQueryFilter(",
    ];

    let mut rs_files = Vec::new();
    for top in ["crates", "bins", "xtask"] {
        collect_rs_files(&root.join(top), &mut rs_files);
    }
    assert!(
        !rs_files.is_empty(),
        "scan found zero .rs files under {root:?} — the walker is broken, not that the workspace is empty"
    );

    let mut offenders = Vec::new();
    for path in &rs_files {
        if path == &allowed_file || path == &this_file {
            continue;
        }
        let Ok(source) = std::fs::read_to_string(path) else {
            continue;
        };
        for pattern in banned_patterns {
            let hits = construction_hits(&source, pattern);
            if hits > 0 {
                offenders.push(format!(
                    "{}: constructs {pattern:?} ({hits} site(s))",
                    path.display()
                ));
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "§17.1: only projection::dense::build_dense_filter may construct a Condition/DenseQueryFilter \
         (tenant+visibility injection is not optional) — found hand-written construction outside it:\n{}",
        offenders.join("\n")
    );
}

/// 活哨兵（§53.3 规则3 同款）：上面那道闸把 `dense.rs` 整个排除在扫描域外，因此**构造检测器
/// 本身失灵时它会永远绿**。这条断言反过来对 `dense.rs` 跑同一个检测器，要求它至少找到一处真
/// 构造——检测器一旦被改成恒返回「这是模式」，这里立刻红。
#[test]
fn construction_detector_still_finds_the_real_constructions_in_dense_rs() {
    let Some(root) = workspace_root() else {
        eprintln!(
            "SKIP construction_detector_still_finds_the_real_constructions_in_dense_rs: workspace root not found"
        );
        return;
    };
    let dense = root.join("crates/projection/src/dense.rs");
    let Ok(source) = std::fs::read_to_string(&dense) else {
        panic!("missing object: {} (§17.1 注入点本体)", dense.display());
    };
    let total: usize = [
        "Condition::And(",
        "Condition::Or(",
        "Condition::Eq {",
        "Condition::In {",
        "Condition::Range {",
    ]
    .iter()
    .map(|p| construction_hits(&source, p))
    .sum();
    assert!(
        total > 0,
        "构造检测器在 dense.rs 上找到 0 处构造——检测器失明（而不是 dense.rs 不再注入）"
    );
}

/// 反向对照：`match` 分支必须被判成模式，否则翻译层（adapters::qdrant 把 Condition 翻成
/// Qdrant JSON）会被误报为绕过注入——这正是本闸上线首日的误报。
#[test]
fn match_arms_are_not_counted_as_construction() {
    let arm = "match c { Condition::And(clauses) => translate(clauses), Condition::Eq { field, value } => emit(field, value) }";
    assert_eq!(construction_hits(arm, "Condition::And("), 0);
    assert_eq!(construction_hits(arm, "Condition::Eq {"), 0);
    let built = "let c = Condition::And(vec![Condition::Eq { field: f, value: v }]);";
    assert_eq!(construction_hits(built, "Condition::And("), 1);
    assert_eq!(construction_hits(built, "Condition::Eq {"), 1);
}
