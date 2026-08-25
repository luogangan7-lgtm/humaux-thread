//! DoD verifier registry (G80-33, §69 DoD Verifier Contract). Wave3: Phase 0 only.
//!
//! §69's `#[dod(id = "DOD-xxx", phase = N, fault = "...", kind = "...")]` is written as a
//! sketch of what a Rust attribute *would* look like; this repo does not build a proc-macro
//! for it (`crates/fail-closed-macro` is a separate, unrelated macro). Per §69 G80-33, the
//! execution body for that sketch is this static registry table: `xtask dod-check` reads it
//! as data (source-text scan, not a compiled dependency — xtask does not depend on this
//! crate) and cross-checks it against §69's canonical checkbox list. A DOD id counts as
//! having a verifier iff it has exactly one [`DodVerifier`] entry here.
//!
//! `xtask/src/dod_check.rs` parses this file's `DodVerifier { ... }` literals as text, so
//! **new entries must follow the exact one-field-per-line layout used below** (each of
//! `id` / `phase` / `fault` / `kind` / `verifier_ref` on its own `    field: value,` line).

/// One `DOD-xxx`'s verifier binding (§69 DoD Verifier Contract).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DodVerifier {
    /// `DOD-xxx`, must match §69 canonical md verbatim (G80-33 rule 2: unique, contiguous).
    pub id: &'static str,
    /// Owner phase, from §69's `[phase=N]` tag on the same checkbox line.
    pub phase: u32,
    /// Name of an existing fault-injection test that proves this verifier's red->green
    /// transition (G80-33 rule 6: empty ⇒ NOT_ADMITTED — no production DoD may be checked
    /// off on review discipline alone).
    pub fault: &'static str,
    /// `test | gate | benchmark | probe`, mirrors §69's `#[dod(kind = ...)]` sketch.
    pub kind: &'static str,
    /// Real execution body this verifier resolves to. `"architecture-check::<name>"` names
    /// one of `xtask architecture-check`'s own sub-checks verbatim (the `name` string in
    /// that subcommand's `checks` vec) — `dod_check.rs` runs that subcommand and matches its
    /// per-check report line against `<name>` to get pass/fail (G80-33 rule 5).
    pub verifier_ref: &'static str,
}

/// Phase 0 registrations — §69 DOD-001..005 (Architecture / Domain section).
pub const PHASE_0: &[DodVerifier] = &[
    DodVerifier {
        id: "DOD-001",
        phase: 0,
        fault: "g52_1_fails_on_second_enum_found_in_bins_dir",
        kind: "gate",
        verifier_ref: "architecture-check::§52.4 G52-1 (ErrorCode sole at error_code position)",
    },
    DodVerifier {
        id: "DOD-002",
        phase: 0,
        // DOD-002 = "Scope 使用 typed IDs；Tenant/User/Workspace/Task/Run/Agent 关系明确"
        // (§69)。执行体 = domain::ids 的真实测试族（newtype 往返/uuidv7/垃圾拒收），
        // fault = parse_rejects_garbage（注入垃圾必须被观察到拒绝，§49 fail-loud）。
        // 早先误配 G80-2(build_request, §55.1) 属量错对象——那是 Phase 6 的交付物，
        // 与 typed IDs 无关（ADR-0001 同批更正）。
        fault: "parse_rejects_garbage",
        kind: "test",
        verifier_ref: "test::humaux-domain::ids",
    },
    DodVerifier {
        id: "DOD-003",
        phase: 0,
        fault: "g59_3_fails_on_second_hit_in_real_evals_dir",
        kind: "gate",
        verifier_ref: "architecture-check::§59.1 G59-3 (Authority::new sole construction point)",
    },
    DodVerifier {
        id: "DOD-004",
        phase: 0,
        fault: "dependency_rule_fails_when_domain_depends_on_sqlx",
        kind: "gate",
        verifier_ref: "architecture-check::§78.3 Workspace Dependency Rule",
    },
    DodVerifier {
        id: "DOD-005",
        phase: 0,
        fault: "rule3_fault_renamed_sentinels_dir_is_red",
        kind: "gate",
        verifier_ref: "architecture-check::§53.3 规则3 (sentinels positive control)",
    },
];
