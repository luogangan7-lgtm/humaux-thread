//! 「当前相位」的唯一真源：仓库根的 `PHASE` 文件（单行整数）。
//!
//! **为什么要有这个文件**：G80-16 / G80-33 都把「当前 Phase」当判定输入，但先前它只是
//! 两个子命令各自的 `--phase` CLI 参数、**默认 0**，而 CI 从不传——于是所有 phase>0 的
//! DoD 与 benchmark 到期判定被静默豁免，仓库交付到 Phase 7 时 `dod-check` 仍报「一切
//! 不适用」。带参数的闸，默认值取了最松的那一档，闸就是装饰品（审计结论，2026-08-27）。
//!
//! 修法不是把默认值改成一个写死的 7（那会立刻变成第二个会腐烂的常量），而是：
//! - 真源放进**仓库本身**（`PHASE` 文件随交付一起提交、一起 review）；
//! - 两个消费者（`dod-check` / `benchset-declaration`）共用同一个读取函数；
//! - 配一道**过期闸**（`cargo xtask phase-check`）：下一相位的交付物 marker 已经不再是
//!   占位、而 `PHASE` 还停在旧值时变红——「当前状态」型的常量必须配一道会红的过期闸，
//!   否则写进静态文件的那天就开始腐烂。

use std::fs;
use std::path::Path;

/// `PHASE` 文件相对仓库根的路径。
pub const PHASE_FILE: &str = "PHASE";

/// 解析 `--phase N` / `--phase=N` 覆盖；没有覆盖时读 `PHASE` 文件。
///
/// CLI 覆盖**保留**是刻意的：它让「假设我们在相位 N」的 what-if 检查（例如提前看下一相位
/// 会红哪些）不需要改文件。但它不再是默认路径——不传参数时读的是真源，不是 0。
///
/// # Panics
/// `PHASE` 文件缺失或不是整数时直接退出：该文件随仓库提交，缺了等于树坏了。静默退回
/// 任何默认值都会重演「默认 0 豁免一切」或它的反面。
#[must_use]
pub fn current_phase(args: &[String]) -> u32 {
    let mut it = args.iter();
    while let Some(a) = it.next() {
        if a == "--phase" {
            if let Some(v) = it.next()
                && let Ok(n) = v.parse()
            {
                return n;
            }
        } else if let Some(v) = a.strip_prefix("--phase=")
            && let Ok(n) = v.parse()
        {
            return n;
        }
    }
    read_phase_file(&locate_phase_file())
}

/// 定位 `PHASE`：优先 cwd（`cargo xtask` 从仓库根跑），退回 `CARGO_MANIFEST_DIR/..`
/// （`cargo test -p xtask` 的 cwd 是 crate 根 `xtask/`，不是仓库根——实测踩过）。
/// 两处都没有再交给 [`read_phase_file`] 用第一个候选去 panic 点名。
fn locate_phase_file() -> std::path::PathBuf {
    let cwd = std::path::PathBuf::from(PHASE_FILE);
    if cwd.exists() {
        return cwd;
    }
    let from_manifest = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join(PHASE_FILE);
    if from_manifest.exists() {
        from_manifest
    } else {
        cwd
    }
}

/// 读 `PHASE` 文件本体。独立出来供 `phase-check` 与测试复用。
///
/// # Panics
/// 见 [`current_phase`]。
#[must_use]
pub fn read_phase_file(path: &Path) -> u32 {
    let raw = fs::read_to_string(path).unwrap_or_else(|e| {
        panic!(
            "missing object: {} —— 当前相位的唯一真源（随仓库提交，缺失 = 树坏了）: {e}",
            path.display()
        )
    });
    raw.trim().parse().unwrap_or_else(|_| {
        panic!(
            "{} 内容不是整数（实得 {:?}）——单行相位号，别放别的",
            path.display(),
            raw.trim()
        )
    })
}

/// 下一相位的交付物 marker：`(相位号, 文件, 该文件仍是占位时的特征串)`。
///
/// `phase-check` 的判据：若 `PHASE = N` 而表里某个 `相位号 <= N+1` 的 marker 文件
/// **已经不再包含**占位特征串（= 那个相位的交付已经开始/完成），且 `相位号 > N`，
/// 则 `PHASE` 落后于交付，红。
///
/// 只登记**下一个**相位的 marker 就够了：`PHASE` 每前进一步，下一行才变得相关；
/// 一次性预登记全部 17 个相位的 marker 是在猜未来交付物的文件名，猜错的行永远不红。
const NEXT_PHASE_MARKERS: [(u32, &str, &str); 3] = [
    (8, "crates/application/src/continuity.rs", "占位模块"),
    // phase 9 = §12 Public Contribution; implementation has started, not an acceptance claim.
    (9, "crates/application/src/contribute.rs", "占位模块"),
    // Phase 9 shares this process with future resident evolution, but only wires run_once.
    // Removing the explicit disabled marker means Phase 10 must be acknowledged separately.
    (
        10,
        "bins/public-worker/src/main.rs",
        "Phase10 resident evolution is not enabled",
    ),
];

/// `cargo xtask phase-check` — `PHASE` 的过期闸。
///
/// 三态：marker 文件不存在 ⇒ `not_applicable` 并点名（交付物连占位都还没有，谈不上落后）；
/// marker 仍是占位 ⇒ pass；marker 已非占位而 `PHASE` 未前进 ⇒ fail。
pub fn run(_args: &[String]) -> i32 {
    let phase = read_phase_file(Path::new(PHASE_FILE));
    let mut failed = false;

    for (marker_phase, file, placeholder_needle) in NEXT_PHASE_MARKERS {
        if marker_phase <= phase {
            continue; // 已经承认的相位，不再看它的 marker。
        }
        match fs::read_to_string(file) {
            Err(_) => {
                println!(
                    "phase-check: not_applicable — phase {marker_phase} 的 marker 缺失 \
                     (missing object: {file})"
                );
            }
            Ok(src) if src.contains(placeholder_needle) => {
                println!(
                    "phase-check: pass — PHASE={phase}，phase {marker_phase} 的交付物仍是占位"
                );
            }
            Ok(_) => {
                eprintln!(
                    "phase-check: fail — {file} 已不再是占位（phase {marker_phase} 的交付已经\
                     开始），但 PHASE 仍是 {phase}。把 PHASE 提到 {marker_phase} 并在 \
                     NEXT_PHASE_MARKERS 里登记 phase {} 的 marker。",
                    marker_phase + 1
                );
                failed = true;
            }
        }
    }

    i32::from(failed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_file(content: &str) -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!(
            "phase-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::write(&p, content).unwrap();
        p
    }

    /// 真源可读，CLI 覆盖生效，两种写法都认。
    #[test]
    fn cli_override_wins_over_the_file() {
        assert_eq!(current_phase(&["--phase".into(), "3".into()]), 3);
        assert_eq!(current_phase(&["--phase=12".into()]), 12);
    }

    /// 不传参数时读真源——**不再默认 0**。真仓的 PHASE 当前是 7；这条断言同时钉住
    /// 「文件在」与「值 >= 7」（相位只进不退，交付新相位时这里的下限跟着提）。
    #[test]
    fn no_args_reads_the_repo_phase_file_not_zero() {
        let n = current_phase(&[]);
        assert!(
            n >= 8,
            "PHASE 真源读出 {n}，低于已交付的相位 8——相位只进不退"
        );
    }

    /// 注错：文件内容不是整数 ⇒ panic（不是静默退回任何默认值）。
    #[test]
    #[should_panic(expected = "不是整数")]
    fn a_garbage_phase_file_panics_instead_of_defaulting() {
        let p = tmp_file("seven\n");
        let _ = read_phase_file(&p);
    }

    /// 注错：文件缺失 ⇒ panic 并点名缺失对象。
    #[test]
    #[should_panic(expected = "missing object")]
    fn a_missing_phase_file_panics_naming_the_object() {
        let _ = read_phase_file(Path::new("/nonexistent/PHASE"));
    }
}
