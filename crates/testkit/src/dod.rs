//! `testkit::dod` — DoD verifier registry (G80-33, §69 DoD Verifier Contract).
//! Depends-on: crates=[]; services=[]; env=[]; modules=[]
//! Called-by: []
//! Invariants: []
//! Spec: Baseline §69
//!
//! Wave3: Phase 0 only.
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
    /// `test::<package>::<filter>` executes ordinary tests; `test-ignored::` explicitly
    /// executes ignored integration tests with the same package/filter format. Missing
    /// required resources or zero executed tests cannot yield a passing verifier.
    /// `test-ignored-bin::<package>::<binary>` runs all ignored tests in one integration
    /// binary when a contract requires several complementary positive/negative cases.
    /// `test-bin::<package>::<binary>::<filter>` and `test-lib::<package>::<filter>`
    /// select one Cargo target without changing its filter or acceptance conditions.
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
        verifier_ref: "test-lib::humaux-domain::ids",
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

/// Phase 3 registrations — §69 Scheduler / Job plane。
///
/// **只有一条。** 这不是「先登记一条以后再补」，是对抗验证的实际结果：Phase 3 的 14 条 DoD
/// 里，其余 13 条要么在仓里找不到能证明它的执行体，要么候选执行体量的不是同一个对象
/// （详见 §69 缺口清单）。宁可空着让 `dod-check` 报 fail，也不硬凑一个错的映射——
/// 错的映射会让这条 DoD 永远打着勾却什么都没测，比空着危险得多。
pub const PHASE_3: &[DodVerifier] = &[DodVerifier {
    id: "DOD-090",
    phase: 3,
    // 判据链逐环核过：§69 ≡ §57.1 Phase 3 行 ≡ §80.1 G80-38 → §32.1#G32-1 ≡ §32.1 五步判据
    // ≡ scheduler_exactly_once.rs:192。fault 走 §32.1 点名的第一条注错
    // （DROP UNIQUE ⇒ ON CONFLICT 报 42P10，:360）。
    //
    // 这是 `kind: test` + 真库依赖，因此**必须与 DSN 同 job 且 HUMAUX_REQUIRE_DB=1**
    // （ci.yml 已在 job 级设好，见 ADR-0005）：否则三态 skip 会被 cargo 记成 passed，
    // dod-check 跟着判 Pass——一条空跑的绿。
    fault: "g32_1_fault_injection_unique_constraint_is_the_final_arbiter",
    kind: "test",
    verifier_ref: "test-bin::humaux-adapters::scheduler_exactly_once::g32_1_exactly_once_enqueue_survives_concurrent_replicas_and_leader_failover",
}];

/// Phase 4 registrations —— **空 slice 是真值，不是占位**：本轮 2 条 phase 4 的 DoD
/// 无一通过对抗验证。
pub const PHASE_4: &[DodVerifier] = &[];

/// Phase 6 registrations。其余仍是真缺口（集中在 §55 量具面与 Envelope 出口层），宁可让
/// dod-check 报 fail 也不硬凑错映射。
pub const PHASE_6: &[DodVerifier] = &[
    DodVerifier {
        id: "DOD-012",
        phase: 6,
        // 判据链：§69「snapshot-bound selection 无漏/重」≡ §11.7 单事务 snapshot 配方
        // ≡ §20.4 禁「活集合上的 OFFSET 分页」。
        //
        // verifier = 真实现的免疫证明（60 并发高排序插入下 snapshot 不漏不重）；
        // fault = **可执行的坏变体**：offset_pagination_positive_control.rs 把 §20.4 禁的
        // 形态逐字实现（跨事务 OFFSET 分页、页数按开始前 COUNT 预算），在页间确定性插行，
        // 断言它**必然**同时漏与重（实测 duplicates=40 missed=40，0.3s，零竞态）。
        // 此前这个注错只活在两个测试文件的模块注释里（「本地做过红转绿，坏变体刻意不
        // 提交」）——只活在注释里的注错无人能复跑，历史见证不是判据。
        fault: "banned_cross_txn_offset_pagination_exhibits_both_duplicates_and_misses",
        kind: "test",
        verifier_ref: "test-bin::humaux-adapters::consolidate_snapshot::snapshot_survives_concurrent_higher_ranked_inserts",
    },
    DodVerifier {
        id: "DOD-010",
        phase: 6,
        // §69「EXACT 查询的 total 来自权威 SQL census，而非召回结果」。
        //
        // verifier = exact_completeness_eval 的 e2e 真 census 电池：secret 扣减案
        // (total=5 来自 count(*)、returned=4、excluded=1 ⇒ coverage 4/5) 逐字段证明 total
        // 是 SQL count(*) 而非召回条数 returned——若 total=returned 则 coverage 恒 1.0、
        // secret 扣减永不可见，该案必红。需 DB env（同 DOD-093，CI 声明 HUMAUX_REQUIRE_DB）。
        // fault = completeness.rs 的构造层注错：`ExactEnumeration::new` 拒绝
        // returned+excluded>total（召回条数超过自己的分母 = §22.1「分母内生」），从类型上
        // 封死「用召回冒充 total」。
        fault: "fault_recall_count_cannot_overrun_the_denominator",
        kind: "test",
        verifier_ref: "test-bin::humaux-adapters::exact_completeness_eval::all_cases_match_expected_readouts",
    },
];

/// Phase 8 registrations — §25 Context Products。
pub const PHASE_8: &[DodVerifier] = &[DodVerifier {
    id: "DOD-093",
    phase: 8,
    // 判据链：§69「Mandatory/Pinned 不得静默消费 RECHECK_REQUIRED/UNRESOLVED/
    // CANNOT_ESTABLISH；必须 fail-loud 并输出 needs_verification[]」≡ §11.10 注错四
    // （Context 忽略 GroundingState ⇒ G 红）≡ grounding.rs 夹具 G 的真断言。
    //
    // 结构保证：`MandatoryRow::from_selector` 收 RowGrounding 返回
    // Admitted{Row|NeedsVerification}——被撤销资格的行铸不出 row，「静默消费」没有
    // 可写的形态；needs_verification 经 lane 到 handoff 顶层块（字节域测试钉住
    // 「不进字节域的披露等于没披露」）。
    //
    // fault 的红转绿已实录（2026-08-27）：把铸造门改成忽略 grounding 恒 CURRENT，
    // domain 侧 recheck_required_rows_are_diverted_and_named_not_minted 与 DB e2e 侧
    // a_live_unversioned_constraint_is_diverted_and_named_not_consumed 同时红，
    // 复原后绿。
    fault: "recheck_required_rows_are_diverted_and_named_not_minted",
    kind: "test",
    verifier_ref: "test-bin::humaux-adapters::mandatory_context_lane::a_live_unversioned_constraint_is_diverted_and_named_not_consumed",
}];

/// Phase 7 registrations — §69 Retrieval 量具面。
pub const PHASE_7: &[DodVerifier] = &[
    DodVerifier {
        id: "DOD-020",
        phase: 7,
        // 判据链：§69「Mandatory/Pinned Context 不参与 semantic 淘汰；mandatory overflow
        // 只能 cannot_establish，不能静默截断」≡ §25.4 步骤 2-5 不经 RRF/reranker ≡
        // §25.5 溢出走 Err(MandatoryOverflow)。
        //
        // verifier = G25-1 本体（compiler.rs：1 条 Mandatory + 200 条高分噪声 + 预算压满，
        // Mandatory 必须在 Context 里）；fault = 仓内可执行坏变体
        // compile_dropping_mandatory 与真 compile 的观测值必须不同——注错不活在文档里。
        //
        // 曾有 spec 级 BLOCKER（2026-08-25）：「该 reason 经 classify() 构造不出来」。
        // 解法不是改 classify() 的冻结四参签名，而是 completeness::overflow_class 单独
        // 映射（IndexCountUnavailable 的同款先例：context 侧的 cannot-establish 由
        // envelope 侧观测，classify() 看不见也不该看见）。
        fault: "g25_1_fault_a_compile_that_drops_mandatory_is_visible_in_the_observable",
        kind: "test",
        verifier_ref: "test-lib::humaux-retrieval::one_mandatory_survives_two_hundred_higher_scoring_supplementals",
    },
    DodVerifier {
        id: "DOD-017",
        phase: 7,
        // 判据链：§69 ≡ §55.1（bins/*、evals/* 出现第二处构造点即红）≡ §80.1 G80-2。
        // 该闸在真仓当前是 not_applicable 并点名了缺失对象（retrieval::build_request 尚未
        // 交付）——登记它是为了让这条 DoD 的欠账**可见**：dod-check 会照实报出来，而不是
        // 因为「没登记」被混在 40 条无差别的 fail 里。
        fault: "g80_2_fails_with_second_construction_site_in_real_evals_dir",
        kind: "gate",
        verifier_ref: "architecture-check::§55.1 G80-2 (build_request sole construction point)",
    },
];

/// Phase 9 local release/trust verifiers. Deployment applicability/recursive evolution
/// remains a separate obligation; these registrations do not activate Phase 10.
pub const PHASE_9: &[DodVerifier] = &[
    DodVerifier {
        id: "DOD-050",
        phase: 9,
        // Public facts enter through a source-backed release boundary. The negative control
        // attempts the empty-source shape and must be rejected before any public LLM capability
        // can turn it into a fact; this is a runnable fault probe, not a vocabulary grep.
        fault: "release_rejects_empty_sources",
        kind: "test",
        verifier_ref: "test-lib::humaux-domain::release_rejects_empty_sources",
    },
    DodVerifier {
        id: "DOD-051",
        phase: 9,
        fault: "scan_failure_or_changed_payload_creates_no_candidate_or_release",
        kind: "test",
        verifier_ref: "test-ignored-bin::humaux-adapters::contribution_pipeline",
    },
    DodVerifier {
        id: "DOD-052",
        phase: 9,
        // Removing either SUT grouping predicate made 100 copied sources count as100.
        // Restored source returned to 9/9. Known-link groups do not prove epistemic
        // independence; missing identities remain explicitly incomplete (§12.6).
        fault: "copied_content_does_not_gain_support_from_distinct_publisher_ids",
        kind: "test",
        verifier_ref: "test-lib::humaux-application::public_evolve::tests",
    },
    DodVerifier {
        id: "DOD-053",
        phase: 9,
        // Widening the real eligible_objects view to include quarantine killed this
        // serving assertion; restoring its exact definition/owner/ACL returned green.
        fault: "quarantine_is_durable_but_not_retrievable_and_revoked_or_disabled_moderator_cannot_mutate",
        kind: "test",
        verifier_ref: "test-ignored-bin::humaux-adapters::public_trust",
    },
    DodVerifier {
        id: "DOD-054",
        phase: 9,
        // The assessed global-queue path proves the active anonymous root/lifecycle, identity-safe
        // receipt surface, and ACL reverse reachability: evaluate carries the exact lifecycle
        // revision, stale projection is rejected, and revoke removes both receipt eligibility
        // and serving eligibility. It does not claim coverage beyond this verifier's cases.
        fault: "assessed_anonymous_lifecycle_tracks_supported_revision_and_revocation_fails_closed",
        kind: "test",
        verifier_ref: "test-ignored-bin::humaux-adapters::public_runtime::assessed_anonymous_lifecycle_tracks_supported_revision_and_revocation_fails_closed",
    },
    DodVerifier {
        id: "DOD-055",
        phase: 9,
        // A live observation with an inflated markdown bootstrap value is the negative control:
        // without the required denominator/e2e evidence it remains NOT_APPLICABLE_YET and does
        // not start resident evolution.
        fault: "real_observation_freshness_and_denominator_never_use_bootstrap",
        kind: "test",
        verifier_ref: "test-ignored-bin::humaux-adapters::mechanism_observation::real_observation_freshness_and_denominator_never_use_bootstrap",
    },
];
