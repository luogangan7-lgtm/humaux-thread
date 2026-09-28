//! `xtask::mechanism_registry` — G0-G2 static-side gate for §1.14 mechanism registry.
//! Depends-on: crates=[humaux-adapters, humaux-contracts, tokio]; services=[PostgreSQL(role_admin)
//!   r=[ops.mechanism_observations]]; env=[CARGO_MANIFEST_DIR, HUMAUX_ADMIN_PG_DSN];
//!   modules=[adapters::mechanism_observation, adapters::postgres, contracts::mechanism_registry]
//! Called-by: [xtask::main]
//! Invariants: [static-only invocations report missing target configuration explicitly, never assert an unqueried table is absent]
//! Spec: Baseline §1.14; §1.14.1; §80.1
//!
//! xtask `mechanism-registry` — G0–G2 静态侧闸（§1.14 全文 + §1.14.1；对应 §80.1 G80-10 的
//! 静态侧，见 extract_digest.md p0-gov）。
//!
//! G3/G4/G5 read actual target-scoped observations and E2E evidence (§1.14.1).
//! Static-only invocations explicitly report missing target configuration, never
//! assert that an unqueried database table is absent.
//!
//! 解析目标是 canonical md 本身（`docs/architecture/Baseline_2.9.md`），禁止引入
//! 第二个数据文件（§1.14 本章冻结："围栏块是文档内唯一副本"）。

use humaux_adapters::{
    mechanism_observation::{RuntimeObservations, read_target},
    postgres::AdminDbPool,
};
use humaux_contracts::mechanism_registry::{
    ActivationKind, MechanismSpec, MechanismStatus, extract_fences, parse_registry,
    parse_target_args,
};
use std::collections::BTreeSet;

/// spec 唯一真源，相对本 crate manifest 目录解析（§1.14 冻结：不得另建镜像文件）。
/// 用 `CARGO_MANIFEST_DIR` 而非相对 cwd，避免 `cargo xtask` 从非 workspace-root 目录调用时找错文件。
const SPEC_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../docs/architecture/Baseline_2.9.md"
);

/// §1.14 固定列序：`ch | mechanism | activation_kind | min_denominator | probe |
/// bootstrap_value | bootstrap_measured_at | note`。
const EXPECTED_COLUMNS: usize = 8;

/// §1.14：`activation_kind` 的闭集三值。
const ALLOWED_ACTIVATION_KINDS: [&str; 3] = ["NO_MECHANISM", "ALWAYS", "DENOMINATOR_GATED"];

/// 闸的三态判定（§57.1：所有闸三态 pass/fail/not_applicable）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateStatus {
    Pass,
    Fail,
    NotApplicable,
}

/// 单个闸的判定结果。`detail` 在 fail/not_applicable 时必须给出可观察的差集或缺失对象名
/// （§57.1 第2条），不得只写「失败」二字。
#[derive(Debug, Clone)]
pub struct GateResult {
    pub gate: &'static str,
    pub status: GateStatus,
    pub detail: String,
}

/// 一行 mechanism-registry 静态记录中 G0/G1/G2 需要的字段（其余 5 列本轮不参与静态判定）。
#[derive(Debug, Clone)]
struct Row {
    ch: u32,
    activation_kind: String,
    /// 该行原始列数，用于 G0 的 schema 校验（期望恰为 [`EXPECTED_COLUMNS`]）。
    column_count: usize,
}

/// 解析单个围栏正文为行记录。列数错误的行仍尽量取出 `ch`（首列）供 G1/G2 使用——
/// 一行 schema 违规不该连带让覆盖率/锚点判定失真，两类问题必须能分开报（§1.14 坑5）。
fn parse_rows(body: &[&str]) -> Vec<Row> {
    body.iter()
        .map(|l| l.trim())
        .filter(|l| !l.is_empty())
        .filter_map(|line| {
            let fields: Vec<&str> = line.split('|').map(str::trim).collect();
            let ch = fields.first()?.parse::<u32>().ok()?;
            let activation_kind = fields.get(2).copied().unwrap_or("").to_string();
            Some(Row {
                ch,
                activation_kind,
                column_count: fields.len(),
            })
        })
        .collect()
}

/// 全文顶级章标题（`^# N. `）的章号集合 —— G1/G2 比对的另一侧。
fn heading_numbers(text: &str) -> BTreeSet<u32> {
    text.lines()
        .filter_map(|line| {
            let rest = line.strip_prefix("# ")?;
            let dot = rest.find(". ")?;
            rest[..dot].parse::<u32>().ok()
        })
        .collect()
}

/// G0 唯一副本：围栏恰 1 个、每行恰 8 列、`activation_kind` 只允许三枚举值（§1.14 G0）。
/// 列数不等于 8 天然覆盖「runtime status 列被加回」——多一列即列数≠8。
fn check_g0(text: &str, rows: &[Row]) -> GateResult {
    let fences = extract_fences(text);
    if fences.len() != 1 {
        return GateResult {
            gate: "G0",
            status: GateStatus::Fail,
            detail: format!(
                "mechanism-registry 围栏个数 = {}，期望恰 1 个",
                fences.len()
            ),
        };
    }
    let mut violations = Vec::new();
    for row in rows {
        if row.column_count != EXPECTED_COLUMNS {
            violations.push(format!(
                "ch={} 列数={}（期望 {EXPECTED_COLUMNS}）",
                row.ch, row.column_count
            ));
        } else if !ALLOWED_ACTIVATION_KINDS.contains(&row.activation_kind.as_str()) {
            violations.push(format!(
                "ch={} activation_kind={:?} 不在闭集内",
                row.ch, row.activation_kind
            ));
        }
    }
    if violations.is_empty() {
        GateResult {
            gate: "G0",
            status: GateStatus::Pass,
            detail: format!("围栏 1 个，{} 行均 8 列且 activation_kind 合法", rows.len()),
        }
    } else {
        GateResult {
            gate: "G0",
            status: GateStatus::Fail,
            detail: format!("schema 违规: {}", violations.join("; ")),
        }
    }
}

/// G1 覆盖：注册表 `ch` 去重集合必须等于全文 `^# N.` 章号集合（§1.14 G1）。
fn check_g1(text: &str, rows: &[Row]) -> GateResult {
    let ch_set: BTreeSet<u32> = rows.iter().map(|r| r.ch).collect();
    let heading_set = heading_numbers(text);
    if ch_set == heading_set {
        return GateResult {
            gate: "G1",
            status: GateStatus::Pass,
            detail: format!("{} 章与注册行一一对应", heading_set.len()),
        };
    }
    let heading_without_row: Vec<u32> = heading_set.difference(&ch_set).copied().collect();
    let row_without_heading: Vec<u32> = ch_set.difference(&heading_set).copied().collect();
    GateResult {
        gate: "G1",
        status: GateStatus::Fail,
        detail: format!(
            "有标题无注册行={heading_without_row:?}；有注册行无标题={row_without_heading:?}"
        ),
    }
}

/// G2 锚可解析：每行 `ch` 必须能定位到实际标题行（§1.14 G2）。
fn check_g2(text: &str, rows: &[Row]) -> GateResult {
    let heading_set = heading_numbers(text);
    let unresolved: Vec<u32> = rows
        .iter()
        .map(|r| r.ch)
        .filter(|ch| !heading_set.contains(ch))
        .collect();
    if unresolved.is_empty() {
        GateResult {
            gate: "G2",
            status: GateStatus::Pass,
            detail: format!("{} 行锚点均可定位到标题行", rows.len()),
        }
    } else {
        GateResult {
            gate: "G2",
            status: GateStatus::Fail,
            detail: format!("无锚定标题行: ch={unresolved:?}"),
        }
    }
}

/// Static-only call: runtime truth was not requested, not checked and not inferred.
fn missing_runtime_target() -> Vec<GateResult> {
    ["G3", "G4", "G5"].into_iter().map(|gate| GateResult {
        gate, status: GateStatus::NotApplicable,
        detail: "missing runtime target (--deployment UUID --cell UUID) and HUMAUX_ADMIN_PG_DSN; ops.mechanism_observations not queried".into(),
    }).collect()
}

fn runtime_results(specs: &[MechanismSpec], observations: &RuntimeObservations) -> Vec<GateResult> {
    ["G3", "G4", "G5"]
        .into_iter()
        .map(|gate| {
            let failures: Vec<String> = specs
                .iter()
                .filter_map(|spec| {
                    let derived = observations.status(spec);
                    let obs = observations.latest.get(&spec.id());
                    let failed = match gate {
                        "G3" => derived.status == Some(MechanismStatus::Stale),
                        "G4" => matches!(
                            derived.reason,
                            "no_data" | "wrong_target" | "invalid_or_stale_observation"
                        ),
                        _ => {
                            spec.activation_kind == ActivationKind::DenominatorGated
                                && (derived.status == Some(MechanismStatus::Stale)
                                    || obs
                                        .is_some_and(|o| derived.status != Some(o.recorded_status)))
                        }
                    };
                    failed.then(|| format!("{}:{}", spec.id(), derived.reason))
                })
                .collect();
            GateResult {
                gate,
                status: if failures.is_empty() {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: if failures.is_empty() {
                    format!(
                        "target {}/{}: live evidence evaluated",
                        observations.target.deployment_id, observations.target.cell_id
                    )
                } else {
                    format!("cannot_establish: {}", failures.join(", "))
                },
            }
        })
        .collect()
}

fn read_runtime(text: &str, args: &[String]) -> Result<Vec<GateResult>, String> {
    let target = parse_target_args(args)?;
    let specs = parse_registry(text)?;
    let dsn = std::env::var("HUMAUX_ADMIN_PG_DSN").map_err(|_| "missing HUMAUX_ADMIN_PG_DSN")?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|_| "cannot start database runtime")?;
    let observations = runtime.block_on(async {
        // dep: PostgreSQL(role_admin) — HUMAUX_ADMIN_PG_DSN, mechanism registry read
        let pool = AdminDbPool::connect(&dsn)
            .await
            .map_err(|_| "cannot connect as role_admin")?;
        read_target(&pool, &target)
            .await
            .map_err(|_| "cannot read runtime observation evidence")
    })?;
    Ok(runtime_results(&specs, &observations))
}

/// 对给定 canonical md 全文跑 G0–G5。G0 的围栏/schema 判定与 G1/G2 的覆盖/锚点判定
/// 各自独立（见 [`check_g0`] 文档），因此即便 G0 fail，G1/G2 仍照跑并给出真实结果。
pub fn check_all(text: &str) -> Vec<GateResult> {
    let fences = extract_fences(text);
    let rows: Vec<Row> = fences.first().map(|f| parse_rows(f)).unwrap_or_default();
    let mut results = vec![
        check_g0(text, &rows),
        check_g1(text, &rows),
        check_g2(text, &rows),
    ];
    results.extend(missing_runtime_target());
    results
}

/// 打印每个闸的判定并返回进程退出码：任一 `Fail` ⇒ 1；否则（`Pass`/`NotApplicable`）⇒ 0
/// （§57.1 三态；repo CLAUDE.md「一道闸没有注错红转绿记录就不算存在」由 tests 模块覆盖）。
fn report(results: &[GateResult]) -> i32 {
    let mut failed = false;
    for r in results {
        let tag = match r.status {
            GateStatus::Pass => "pass",
            GateStatus::Fail => {
                failed = true;
                "fail"
            }
            GateStatus::NotApplicable => "not_applicable",
        };
        eprintln!("mechanism-registry {}: {tag} — {}", r.gate, r.detail);
    }
    i32::from(failed)
}

pub fn run(args: &[String]) -> i32 {
    let text = match std::fs::read_to_string(SPEC_PATH) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("mechanism-registry G0: fail — cannot read spec at {SPEC_PATH}: {e}");
            return 1;
        }
    };
    let mut results = check_all(&text);
    if !args.is_empty() {
        results.truncate(3);
        match read_runtime(&text, args) {
            Ok(runtime) => results.extend(runtime),
            Err(error) => results.extend(["G3", "G4", "G5"].map(|gate| GateResult {
                gate,
                status: GateStatus::Fail,
                detail: format!("cannot_establish: {error}"),
            })),
        }
    }
    report(&results)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 最小合规 fixture：3 章，各 1 行注册记录，8 列，`activation_kind` 合法。
    /// 独立于真实 15074 行 spec，专供本模块的红转绿断言用（§1.14「禁止第二个数据文件」
    /// 约束的是生产解析目标，不约束测试 fixture）。
    const VALID_FIXTURE: &str = "\
# 1. Chapter One

body

# 2. Chapter Two

body

# 3. Chapter Three

body

## 1.14 test section

```text
ch | mechanism | activation_kind | min_denominator | probe | bootstrap_value | bootstrap_measured_at | note
```

```mechanism-registry
1 | - | NO_MECHANISM | - | - | - | - | -
2 | mech-two | ALWAYS | 1 | metric:foo | - | 2026-08-24 | note
3 | mech-three | DENOMINATOR_GATED | 2 | sql:bar | 0 | 2026-08-24 | note
```
";

    fn gate<'a>(results: &'a [GateResult], name: &str) -> &'a GateResult {
        results.iter().find(|r| r.gate == name).unwrap()
    }

    #[test]
    fn valid_fixture_g0_g1_g2_pass_g3_g4_g5_not_applicable() {
        let results = check_all(VALID_FIXTURE);
        assert_eq!(gate(&results, "G0").status, GateStatus::Pass);
        assert_eq!(gate(&results, "G1").status, GateStatus::Pass);
        assert_eq!(gate(&results, "G2").status, GateStatus::Pass);
        for g in ["G3", "G4", "G5"] {
            let r = gate(&results, g);
            assert_eq!(r.status, GateStatus::NotApplicable);
            assert!(r.detail.contains("ops.mechanism_observations"));
        }
    }

    /// 注错「加 status 列」：向一行追加第 9 列（runtime status 值），列数变为 9 ⇒ G0 红；
    /// G1/G2 不受影响，因为 `ch` 仍能从首列取出。红转绿：删掉多余列即回到 [`VALID_FIXTURE`]。
    #[test]
    fn g0_red_on_extra_status_column_then_green() {
        let mutated = VALID_FIXTURE.replace(
            "2 | mech-two | ALWAYS | 1 | metric:foo | - | 2026-08-24 | note",
            "2 | mech-two | ALWAYS | 1 | metric:foo | - | 2026-08-24 | note | ACTIVE",
        );
        assert_ne!(mutated, VALID_FIXTURE);
        let red = check_all(&mutated);
        assert_eq!(gate(&red, "G0").status, GateStatus::Fail);
        assert!(gate(&red, "G0").detail.contains("列数=9"));
        assert_eq!(gate(&red, "G1").status, GateStatus::Pass);
        assert_eq!(gate(&red, "G2").status, GateStatus::Pass);

        let green = check_all(VALID_FIXTURE);
        assert_eq!(gate(&green, "G0").status, GateStatus::Pass);
    }

    /// 注错「删围栏」：整个 mechanism-registry 围栏消失（1→0）⇒ G0 红。红转绿：围栏恢复。
    #[test]
    fn g0_red_on_fence_deleted_then_green() {
        let fence_start = VALID_FIXTURE.find("```mechanism-registry").unwrap();
        let mutated = &VALID_FIXTURE[..fence_start];
        let red = check_all(mutated);
        assert_eq!(gate(&red, "G0").status, GateStatus::Fail);
        assert!(gate(&red, "G0").detail.contains("围栏个数 = 0"));

        let green = check_all(VALID_FIXTURE);
        assert_eq!(gate(&green, "G0").status, GateStatus::Pass);
    }

    /// 注错「章号缺行」：标题 `# 3.` 保留，但对应注册行被删 ⇒ ch 去重集合 {1,2} != 标题集合
    /// {1,2,3} ⇒ G1 红。G0/G2 不受影响。红转绿：行恢复。
    #[test]
    fn g1_red_on_missing_row_for_existing_heading_then_green() {
        let mutated = VALID_FIXTURE.replace(
            "3 | mech-three | DENOMINATOR_GATED | 2 | sql:bar | 0 | 2026-08-24 | note\n",
            "",
        );
        assert_ne!(mutated, VALID_FIXTURE);
        let red = check_all(&mutated);
        assert_eq!(gate(&red, "G1").status, GateStatus::Fail);
        assert!(gate(&red, "G1").detail.contains('3'));
        assert_eq!(gate(&red, "G0").status, GateStatus::Pass);
        assert_eq!(gate(&red, "G2").status, GateStatus::Pass);

        let green = check_all(VALID_FIXTURE);
        assert_eq!(gate(&green, "G1").status, GateStatus::Pass);
    }

    /// 注错「章号改掉」（§1.14 G2 官方注入用例：把 §67 的章号改掉）：标题 `# 3.` 被重编号为
    /// `# 30.`，注册行仍写 `ch=3` ⇒ ch=3 无锚 ⇒ G2 红。红转绿：标题号恢复。
    #[test]
    fn g2_red_on_heading_renumbered_then_green() {
        let mutated = VALID_FIXTURE.replace("# 3. Chapter Three", "# 30. Chapter Three");
        assert_ne!(mutated, VALID_FIXTURE);
        let red = check_all(&mutated);
        assert_eq!(gate(&red, "G2").status, GateStatus::Fail);
        assert!(gate(&red, "G2").detail.contains("ch=[3]"));
        assert_eq!(gate(&red, "G0").status, GateStatus::Pass);

        let green = check_all(VALID_FIXTURE);
        assert_eq!(gate(&green, "G2").status, GateStatus::Pass);
    }

    /// 对真实 canonical spec 跑一遍：G0/G1/G2 必须全绿——这是本闸存在的意义，
    /// spec 本身已冻结为合规状态（§1.14）。
    #[test]
    fn real_spec_g0_g1_g2_pass() {
        let text = std::fs::read_to_string(SPEC_PATH).expect("spec must be readable");
        let results = check_all(&text);
        for g in ["G0", "G1", "G2"] {
            let r = gate(&results, g);
            assert_eq!(
                r.status,
                GateStatus::Pass,
                "{g} unexpectedly failed: {}",
                r.detail
            );
        }
    }
}
