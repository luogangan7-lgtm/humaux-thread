//! xtask `threshold-shape` — G80-15 threshold-shape-check.
//! 判据以 §69 Continuation Gate 冻结块的 CI 两条为唯一真源（spec 行 11048–11052，
//! `threshold-shape-check` 段）：①逐行扫 §55 与 §69，命中裸比例且同行无量纲词 ⇒ 红；
//! ②逐行扫全文「不劣于」，该行须同时含否定/作废标记之一，否则 ⇒ 红。本文件不复述判据散文，
//! 只引用 § 号（仓库硬边界）。

use std::fs;

const SPEC_PATH: &str = "docs/architecture/Baseline_2.8.md";

/// §69 判据①「量纲词」清单（题 / 条 / n=），逐字取自 spec 行 11049。
const DIMENSION_WORDS: [&str; 3] = ["题", "条", "n="];
/// §69 判据②「否定/作废标记」清单，逐字取自 spec 行 11050。
const NEGATION_MARKERS: [&str; 5] = ["不可断言", "不得表述", "作废", "覆盖", "升级为"];
const NOT_WORSE_THAN: &str = "不劣于";

/// 一条判据违规：命中行号（1-based，对应 spec 原文行号）与说明。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    pub line_no: usize,
    pub message: String,
}

/// 顶层章节标题（`# N. ...`）的章节号；`## ` 二级标题不命中。
fn parse_top_level_chapter(line: &str) -> Option<u32> {
    let rest = line.strip_prefix("# ")?;
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    if digits.is_empty() {
        return None;
    }
    digits.parse().ok()
}

/// 定位一个顶层章节覆盖的 0-based 半开行区间 `[start, end)`。不硬编码行号 ——
/// 章节边界随 spec 改版漂移时本函数自动跟随（§69 判据①依赖的就是这个动态边界）。
fn chapter_line_range(spec_md: &str, chapter: u32) -> Option<(usize, usize)> {
    let lines: Vec<&str> = spec_md.lines().collect();
    let mut start = None;
    let mut end = lines.len();
    for (i, line) in lines.iter().enumerate() {
        if let Some(n) = parse_top_level_chapter(line) {
            if start.is_some() {
                end = i;
                break;
            }
            if n == chapter {
                start = Some(i);
            }
        }
    }
    start.map(|s| (s, end))
}

/// 手写扫描 `0\.\d{2,}`（xtask/Cargo.toml 不许新增依赖，故不引 regex crate；纯 ASCII 字节扫描
/// 对混排 UTF-8 中文安全，因为多字节序列的续字节全部 >= 0x80，不会被误判成数字/`.`）。
fn contains_bare_ratio(line: &str) -> bool {
    let bytes = line.as_bytes();
    let mut i = 0;
    while i + 1 < bytes.len() {
        if bytes[i] == b'0' && bytes[i + 1] == b'.' {
            let mut digits = 0usize;
            let mut j = i + 2;
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                digits += 1;
                j += 1;
            }
            if digits >= 2 {
                return true;
            }
        }
        i += 1;
    }
    false
}

/// 判据①：仅扫描 `ranges`（§55 ∪ §69）内的行。
pub fn scan_bare_ratio(spec_md: &str, ranges: &[(usize, usize)]) -> Vec<Violation> {
    let lines: Vec<&str> = spec_md.lines().collect();
    let mut out = Vec::new();
    for &(start, end) in ranges {
        for (i, line) in lines.iter().enumerate().take(end).skip(start) {
            if contains_bare_ratio(line) && !DIMENSION_WORDS.iter().any(|w| line.contains(w)) {
                out.push(Violation {
                    line_no: i + 1,
                    message: format!("G80-15① 裸比例判定线无量纲词（题/条/n=）: {}", line.trim()),
                });
            }
        }
    }
    out
}

/// 判据②：扫描**全文**（区别于判据①的 §55/§69 限定域）。
pub fn scan_not_worse_than(spec_md: &str) -> Vec<Violation> {
    let mut out = Vec::new();
    for (i, line) in spec_md.lines().enumerate() {
        if line.contains(NOT_WORSE_THAN) && !NEGATION_MARKERS.iter().any(|m| line.contains(m)) {
            out.push(Violation {
                line_no: i + 1,
                message: format!("G80-15② 「不劣于」未伴随否定/作废标记: {}", line.trim()),
            });
        }
    }
    out
}

/// 判据②（全文扫描，零依赖章节边界）无条件先跑；判据①仅在 §55/§69 都能定位时叠加。
/// 章节缺失只让①落 not_applicable，不得连②一起跳过——否则改一下 `# 55.` 的写法就能让
/// 整闸静默变绿（含本来会红的行），见 spec 11050②「逐行扫全文」与 §57.1 三态要求。
fn run_on(spec_md: &str) -> i32 {
    let ch55 = chapter_line_range(spec_md, 55);
    let ch69 = chapter_line_range(spec_md, 69);

    let mut violations = scan_not_worse_than(spec_md);
    if let (Some(a), Some(b)) = (ch55, ch69) {
        violations.extend(scan_bare_ratio(spec_md, &[a, b]));
    }

    if violations.is_empty() {
        let mut missing = Vec::new();
        if ch55.is_none() {
            missing.push("§55 chapter heading (`# 55.`)");
        }
        if ch69.is_none() {
            missing.push("§69 chapter heading (`# 69.`)");
        }
        if !missing.is_empty() {
            eprintln!(
                "threshold-shape: not_applicable (missing object: {} not found in {SPEC_PATH})",
                missing.join(", ")
            );
            return 0;
        }
        println!("threshold-shape: pass");
        return 0;
    }

    violations.sort_by_key(|v| v.line_no);
    for v in &violations {
        eprintln!("threshold-shape: fail — line {}: {}", v.line_no, v.message);
    }
    1
}

pub fn run(args: &[String]) -> i32 {
    let _ = args;
    let spec_md = match fs::read_to_string(SPEC_PATH) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("threshold-shape: fail — cannot read {SPEC_PATH}: {e}");
            return 1;
        }
    };
    run_on(&spec_md)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn real_spec_md() -> String {
        let path =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../docs/architecture/Baseline_2.8.md");
        fs::read_to_string(path).expect("spec must be readable in test env")
    }

    /// 把（可能被修改过的）spec 文本写进 tempdir 副本再读回，模拟注错测试所需的
    /// 「fixture spec 副本」——真实仓库文件全程不被触碰（硬规则③）。
    fn fixture_copy(content: &str) -> String {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!(
            "xtask-threshold-shape-fixture-{}-{n}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).expect("tempdir create");
        let path = dir.join("Baseline_2.8.md");
        fs::write(&path, content).expect("write fixture");
        fs::read_to_string(&path).expect("read fixture back")
    }

    fn domain_ranges(spec_md: &str) -> Vec<(usize, usize)> {
        vec![
            chapter_line_range(spec_md, 55).expect("§55 must exist"),
            chapter_line_range(spec_md, 69).expect("§69 must exist"),
        ]
    }

    #[test]
    fn chapter_line_range_locates_55_and_69() {
        let spec = real_spec_md();
        let (s55, e55) = chapter_line_range(&spec, 55).unwrap();
        let (s69, e69) = chapter_line_range(&spec, 69).unwrap();
        let lines: Vec<&str> = spec.lines().collect();
        assert!(lines[s55].starts_with("# 55."));
        assert!(lines[e55].starts_with("# 56."));
        assert!(lines[s69].starts_with("# 69."));
        assert!(lines[e69].starts_with("# 70."));
    }

    /// 判据①在真实 spec 上的 §55/§69 限定域内当前无违规（两处 `0\.\d{2,}` 命中均带量纲词，
    /// 与既有调研记录一致）。
    #[test]
    fn baseline_real_spec_bare_ratio_domain_is_clean() {
        let spec = real_spec_md();
        let ranges = domain_ranges(&spec);
        assert!(scan_bare_ratio(&spec, &ranges).is_empty());
    }

    /// 判据②在真实 spec 全文扫描：spec 行 3127（§16.3 切换判据示例）已经是判据②描述的
    /// 那种反例本身 —— 该行含「不劣于」但不含任何否定/作废标记，是本节冻结前遗留、
    /// 未被判据②扫描覆盖过的真实缺口，不是本 checker 的误判。据实报告，不在本任务范围内改 spec。
    #[test]
    fn baseline_real_spec_finds_pre_existing_not_worse_than_gap() {
        let spec = real_spec_md();
        let violations = scan_not_worse_than(&spec);
        let lines: Vec<usize> = violations.iter().map(|v| v.line_no).collect();
        assert_eq!(lines, vec![3127], "unexpected §69②违规集合: {lines:?}");
    }

    /// 注错 A：把 PASS 行改回 `pass_rate >= 0.95`（裸比例、无量纲词）⇒ 判据①红。
    #[test]
    fn fault_pass_line_reverted_to_bare_ratio_is_red() {
        let raw = real_spec_md();
        let bad = raw.replacen(
            "PASS:         量具自检过 且 state 层 new_min >= baseline_min + Δ",
            "PASS:         pass_rate >= 0.95",
            1,
        );
        assert_ne!(bad, raw, "fixture must actually mutate the PASS line");
        let fixture = fixture_copy(&bad);
        let ranges = domain_ranges(&fixture);
        let violations = scan_bare_ratio(&fixture, &ranges);
        assert!(
            !violations.is_empty(),
            "reverting PASS line to a bare ratio must turn ① red"
        );
    }

    /// 注错 B：在任一章插入 `AND benchmark(shadow) 不劣于 benchmark(serving)`（无否定/作废标记）
    /// ⇒ 判据②新增恰好一条红，且红在插入行本身。断言用 delta（注入前后违规数之差）而不是单纯
    /// 「结果里含 benchmark(shadow)」——spec 行 3127 本身逐字就是这句话，未注入时已经产生同文案的
    /// 违规，只断言「存在」测不出注入是否真的进了判定路径（回归：删掉注入步骤测试照样绿）。
    #[test]
    fn fault_injected_not_worse_than_line_is_red() {
        let raw = real_spec_md();
        let before = scan_not_worse_than(&raw);
        let marker = "## 55.1 量具与生产同源 —— 拓扑约束，不是一段文字\n";
        assert!(raw.contains(marker));
        let inserted_line = "AND benchmark(shadow) 不劣于 benchmark(serving)";
        let bad = raw.replacen(marker, &format!("{marker}\n{inserted_line}\n"), 1);
        assert_ne!(bad, raw);
        let fixture = fixture_copy(&bad);
        let after = scan_not_worse_than(&fixture);
        assert_eq!(
            after.len(),
            before.len() + 1,
            "injecting one un-negated 不劣于 line must add exactly one violation: before={before:?} after={after:?}"
        );
        let inserted_line_no = fixture
            .lines()
            .position(|l| l == inserted_line)
            .map(|i| i + 1)
            .expect("inserted line must appear verbatim in fixture");
        assert!(
            after
                .iter()
                .any(|v| v.line_no == inserted_line_no && v.message.contains("benchmark(shadow)")),
            "expected the new violation to be pinned to the inserted line {inserted_line_no}: {after:?}"
        );
    }

    /// 回归（判据②恒绿逃逸）：§55/§69 章节标题双缺失时，判据②仍必须对全文生效——
    /// 此前 run() 在章节缺失分支里直接 return 0，②连跑都不跑。
    #[test]
    fn not_worse_than_violation_still_reds_even_when_chapters_missing() {
        let fixture =
            fixture_copy("# 1. only chapter\nAND benchmark(shadow) 不劣于 benchmark(serving)\n");
        assert!(chapter_line_range(&fixture, 55).is_none());
        assert!(chapter_line_range(&fixture, 69).is_none());
        assert_eq!(
            run_on(&fixture),
            1,
            "② must fire and fail the gate even without §55/§69 headings"
        );
    }

    /// 章节缺失且全文确无②违规 ⇒ 仍然 not_applicable（不能因为②无条件跑了就误判 pass/fail）。
    #[test]
    fn run_on_not_applicable_when_chapters_missing_and_no_violation() {
        let fixture = fixture_copy("# 1. only chapter\nnothing else here\n");
        assert_eq!(run_on(&fixture), 0);
    }

    #[test]
    fn not_applicable_when_chapter_heading_missing() {
        let fixture = fixture_copy("# 1. only chapter\nnothing else here\n");
        assert!(chapter_line_range(&fixture, 55).is_none());
        assert!(chapter_line_range(&fixture, 69).is_none());
    }
}
