//! `retrieval::handoff` — §25.3 continuity handoff 的**字节域**（G80-31）。
//! Depends-on: crates=[humaux-domain, serde, serde_json, sha2]; services=[];
//!   env=[]; modules=[domain::context, domain::grounding, retrieval::compiler]
//! Called-by: [adapters::context_repo, adapters::continuity_read, application::continuity, gateway::context, tests]
//! Invariants: []
//! Spec: §25.3; §57.1
//!
//! §57.1 Phase 8 出场判据前半：「同一 `context_snapshot_seq` 两次装配 handoff 逐字节相同」。
//! 本模块的策略是**类型即比较域**——凡是会让两次装配字节不同的东西，[`Handoff`] 在结构上
//! 就装不下（一个都不许靠"注意不要"）：
//!
//! - 无 supplemental 字段（Qdrant top-k / rerank 非确定）；
//! - 无时间戳（`state_age_seconds` 之类的挂钟量在类型上进不来）；
//! - 无 f32/f64（`fusion_score` 没 derive Serialize，`completeness_ratio` 是 Qdrant 活读）；
//! - 无 HashMap（迭代序随机）；所有 Vec 由 lane 构造器按 `(selector, memory_id)` 预排序；
//! - 内容体不入域（jsonb::text 的渲染跨 PG 版本会漂）——v1 是 **ids + counts 的 manifest**。
//!
//! 序列化唯一入口是 [`Handoff::canonical_bytes`]；快照身份的语义（seq 必要非充分、
//! token 充分）见 [`FrozenReads`] 的 doc。

use humaux_domain::context::{
    ContextBudget, FrozenReads, MandatoryOverflow, NeedsVerification, SelectorId,
};
use humaux_domain::grounding::GroundingStateKind;
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::compiler::{ContextItem, compile};

/// 字节域里的一条 lane 项：id + 来源 selector + authority 线值。**没有分数、没有内容。**
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HandoffItem {
    /// memory id（uuid 文本）。
    pub memory_id: String,
    /// 哪个 selector 选进来的。
    pub selector: String,
    /// authority 线值（PascalCase 变体名逐字）。
    pub authority: String,
}

/// DOD-093 的 `needs_verification[]` 线格式。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NeedsVerificationWire {
    /// memory id。
    pub memory_id: String,
    /// 哪个 selector 想选它。
    pub selector: String,
    /// 被撤销资格时的 grounding 状态（snake_case）。
    pub state: String,
}

/// 计数块。全 u64/bool——冻结 SQL 的整数，无浮点。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HandoffCounts {
    /// mandatory 应有（各可用 selector 独立授权候选集合的并集基数）。
    pub mandatory_expected: u64,
    /// mandatory 实际进入。
    pub mandatory_returned: u64,
    /// 差额。
    pub mandatory_missing: u64,
    /// pinned 应有（独立 COUNT，外部 oracle）。
    pub pinned_expected: u64,
    /// pinned 实际进入。
    pub pinned_returned: u64,
    /// pinned 被排除（低 authority、铸造门分流，或已由 mandatory 吸收），具名清单在顶层块。
    pub pinned_excluded: u64,
    /// §25.5 溢出。true 时 mandatory/pinned 为空、`overflow_manifest` 携带全量 id。
    pub overflow: bool,
}

/// §25.3 handoff——**类型即字节域**（见模块 doc）。
///
/// 字段序即 serde 输出序（struct 按声明序序列化）；改字段顺序就是改字节域，
/// 与改字段一样要过 review。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Handoff {
    /// `pg_snapshot_xmin`——fingerprint 轴（§1.2.1/§16.1），必要非充分。
    pub context_snapshot_seq: i64,
    /// `SHA256(pg_current_snapshot()::text)`——快照身份，「同快照 ⇒ 同字节」的充分条件。
    pub snapshot_token_sha256: String,
    /// §25.4 步骤 2 的产物（已过铸造门 + compile）。
    pub mandatory: Vec<HandoffItem>,
    /// §25.4 步骤 5 的产物。
    pub pinned: Vec<HandoffItem>,
    /// DOD-093：被铸造门分流的行，具名。
    pub needs_verification: Vec<NeedsVerificationWire>,
    /// 快照内不可裁的行（NotJudged——「没判」不许伪装成「判过且通过」）。
    pub not_judged: Vec<String>,
    /// partial-lane：不可用的 selector 与探测出的缺失对象。
    pub unavailable_selectors: Vec<(String, String)>,
    /// §25.5 溢出时的全量 manifest（非溢出为空）。
    pub overflow_manifest: Vec<String>,
    /// 计数。
    pub counts: HandoffCounts,
}

/// Wire name of a selector = its §25.4 **registered name**, read from the registry.
///
/// card 22c review debt: this used to be a second hand-written table that still said
/// `task_explicit_context_v1` on every handoff surface (`mandatory[].selector`,
/// `pinned[].selector`, `needs_verification[].selector`, `unavailable_selectors`) while
/// ADR-0046 had already retired that registration in favour of `task_explicit_context_v2`.
/// Two tables of the same names is exactly how a wire name drifts from the registry, so
/// there is now one table — [`humaux_domain::context::REGISTRY`] — and this is a read of it.
/// [`SelectorId`]'s variant names stay slot identifiers (see `SelectorSpec::registered_name`'s
/// own doc); the registered name is the only thing that reaches the wire.
const fn selector_wire(id: SelectorId) -> &'static str {
    humaux_domain::context::spec(id).registered_name
}

fn state_wire(k: GroundingStateKind) -> &'static str {
    match k {
        GroundingStateKind::Current => "current",
        GroundingStateKind::RecheckRequired => "recheck_required",
        GroundingStateKind::Unresolved => "unresolved",
        GroundingStateKind::CannotEstablish => "cannot_establish",
    }
}

fn nv_wire(nv: &NeedsVerification) -> NeedsVerificationWire {
    NeedsVerificationWire {
        memory_id: nv.memory_id.0.to_string(),
        selector: selector_wire(nv.selector).to_string(),
        state: state_wire(nv.state).to_string(),
    }
}

/// §25.4 步骤 4–7 → handoff。**单一 compile 真源**：mandatory/pinned 的最终集合来自
/// [`compile`]（G25-1 的被测对象），不是从 lane 直接抄——抄会绕开 G25-1 看守的那一层。
///
/// 溢出（§25.5）不产出条目：`counts.overflow = true`、`overflow_manifest` 全量具名、
/// mandatory/pinned 为空——`MandatoryOverflow` 里没有 Context 可返回，这里也没有。
#[must_use]
pub fn assemble_with_context(
    frozen: FrozenReads,
    budget: ContextBudget,
) -> (Handoff, crate::compiler::ContextOutcome) {
    let FrozenReads {
        mandatory,
        pinned,
        context_snapshot_seq,
        snapshot_token_sha256,
    } = frozen;
    let pinned = pinned.excluding_mandatory(&mandatory);
    let mandatory_expected = mandatory.expected();
    let pinned_expected = pinned.expected();
    let needs_verification = mandatory.needs_verification().iter().map(nv_wire).collect();
    let unavailable_selectors = mandatory
        .unavailable()
        .iter()
        .map(|(id, missing)| (selector_wire(*id).to_string(), missing.clone()))
        .collect();
    let pinned_excluded = pinned.excluded().len() as u64;
    let not_judged = not_judged_ids(&mandatory, &pinned);
    let outcome = match budget.reserve(&mandatory, &pinned) {
        Err(overflow) => crate::compiler::ContextOutcome::Overflow(overflow),
        Ok(supplemental_budget) => crate::compiler::ContextOutcome::Compiled(compile(
            mandatory,
            pinned,
            supplemental_budget,
            Vec::new(),
        )),
    };
    let handoff = match &outcome {
        crate::compiler::ContextOutcome::Overflow(overflow) => Handoff {
            context_snapshot_seq,
            snapshot_token_sha256,
            mandatory: Vec::new(),
            pinned: Vec::new(),
            needs_verification,
            not_judged,
            unavailable_selectors,
            overflow_manifest: overflow_manifest(overflow),
            counts: HandoffCounts {
                mandatory_expected,
                mandatory_returned: 0,
                mandatory_missing: mandatory_expected,
                pinned_expected,
                pinned_returned: 0,
                pinned_excluded,
                overflow: true,
            },
        },
        crate::compiler::ContextOutcome::Compiled(compiled) => {
            let mut mandatory = Vec::new();
            let mut pinned = Vec::new();
            for item in compiled.items() {
                match item {
                    ContextItem::Mandatory(row) => {
                        mandatory.push(HandoffItem {
                            memory_id: row.memory_id().0.to_string(),
                            selector: selector_wire(row.selector()).to_string(),
                            authority: format!("{:?}", row.authority()),
                        });
                    }
                    ContextItem::Pinned(row) => {
                        pinned.push(HandoffItem {
                            memory_id: row.memory_id().0.to_string(),
                            selector: selector_wire(row.selector()).to_string(),
                            authority: format!("{:?}", row.authority()),
                        });
                    }
                    ContextItem::Supplemental(_) => {
                        unreachable!("handoff 的字节域不含 supplemental")
                    }
                }
            }
            let mandatory_returned = mandatory.len() as u64;
            let pinned_returned = pinned.len() as u64;
            Handoff {
                context_snapshot_seq,
                snapshot_token_sha256,
                mandatory,
                pinned,
                needs_verification,
                not_judged,
                unavailable_selectors,
                overflow_manifest: Vec::new(),
                counts: HandoffCounts {
                    mandatory_expected,
                    mandatory_returned,
                    mandatory_missing: mandatory_expected.saturating_sub(mandatory_returned),
                    pinned_expected,
                    pinned_returned,
                    pinned_excluded,
                    overflow: false,
                },
            }
        }
    };
    (handoff, outcome)
}
#[must_use]
pub fn assemble(frozen: FrozenReads, budget: ContextBudget) -> Handoff {
    assemble_with_context(frozen, budget).0
}

fn not_judged_ids(
    mandatory: &humaux_domain::context::MandatoryLane,
    pinned: &humaux_domain::context::PinnedLane,
) -> Vec<String> {
    let mut ids = mandatory
        .rows()
        .iter()
        .chain(pinned.rows())
        .filter(|row| row.not_judged())
        .map(|row| row.memory_id().0.to_string())
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids.dedup();
    ids
}

fn overflow_manifest(o: &MandatoryOverflow) -> Vec<String> {
    let mut v: Vec<String> = o.manifest().iter().map(|m| m.0.to_string()).collect();
    v.sort_unstable();
    v
}

impl Handoff {
    /// **唯一**序列化入口（G80-31 的比较域）。serde_json 对 struct 按声明序输出，
    /// 本类型无 map、无浮点、Vec 全部预排序——字节由值唯一决定。
    ///
    /// # Panics
    /// serde_json 对本类型不可能失败（无 map key 非串、无 NaN）；失败即内存损坏级别。
    #[must_use]
    pub fn canonical_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("Handoff 的字节域按构造可序列化")
    }

    /// 字节指纹。
    #[must_use]
    pub fn sha256(&self) -> [u8; 32] {
        Sha256::digest(self.canonical_bytes()).into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use humaux_domain::authority::MemoryId;
    use humaux_domain::context::{
        Admitted, MandatoryLane, MandatoryRow, PinnedLane, SelectorOutcome, spec,
    };
    use humaux_domain::grounding::{GroundingInputs, RowGrounding, derive_grounding_state};

    fn current() -> RowGrounding {
        RowGrounding::Judged(derive_grounding_state(GroundingInputs::Edges(&[])))
    }

    fn row_with(id: MemoryId, tokens: u32) -> MandatoryRow {
        row_with_grounding(id, tokens, current())
    }

    fn row_with_grounding(id: MemoryId, tokens: u32, grounding: RowGrounding) -> MandatoryRow {
        let s = spec(SelectorId::ProjectActiveConstraintsV1);
        match MandatoryRow::from_selector(s, id, s.min_authority, tokens, grounding)
            .expect("参数合法")
        {
            Admitted::Row(r) => r,
            Admitted::NeedsVerification(nv) => panic!("row 不该被分流: {nv:?}"),
        }
    }

    fn lane_of(ids: &[MemoryId]) -> MandatoryLane {
        lane_with_rows(ids.iter().map(|id| row_with(*id, 10)).collect())
    }

    fn lane_with_rows(rows: Vec<MandatoryRow>) -> MandatoryLane {
        MandatoryLane::from_selectors([
            SelectorOutcome::Ran {
                id: SelectorId::TaskExplicitContextV1,
                candidate_ids: vec![],
                rows: vec![],
                needs_verification: vec![],
            },
            SelectorOutcome::Ran {
                id: SelectorId::ProjectActiveConstraintsV1,
                candidate_ids: rows.iter().map(|row| row.memory_id()).collect(),
                rows,
                needs_verification: vec![],
            },
            SelectorOutcome::Ran {
                id: SelectorId::UserConfirmedCorrectionsV1,
                candidate_ids: vec![],
                rows: vec![],
                needs_verification: vec![],
            },
            SelectorOutcome::Ran {
                id: SelectorId::RequiredCurrentStateFacetsV1,
                candidate_ids: vec![],
                rows: vec![],
                needs_verification: vec![],
            },
            SelectorOutcome::Ran {
                id: SelectorId::ExplicitMandatoryBindingsV1,
                candidate_ids: vec![],
                rows: vec![],
                needs_verification: vec![],
            },
        ])
        .expect("selector outcomes")
    }

    fn frozen_of(ids: &[MemoryId]) -> FrozenReads {
        FrozenReads {
            mandatory: lane_of(ids),
            pinned: PinnedLane::new(0, vec![], vec![]),
            context_snapshot_seq: 12345,
            snapshot_token_sha256: "deadbeef".repeat(8),
        }
    }

    /// **G80-31 前半的纯函数形态**：同值 FrozenReads ⇒ 逐字节相同。
    /// 行的**喂入顺序不同**也必须同字节——顺序是 lane 构造器排出来的，不是调用方碰巧的。
    /// 注错：把 from_selectors 里的 sort 删掉 ⇒ 本条红。
    #[test]
    fn equal_frozen_reads_assemble_to_identical_bytes_regardless_of_feed_order() {
        let a = MemoryId::new();
        let b = MemoryId::new();
        let budget = || ContextBudget::new(1000, 500).expect("budget");

        let h1 = assemble(frozen_of(&[a, b]), budget());
        let h2 = assemble(frozen_of(&[b, a]), budget()); // 反序喂入
        assert_eq!(
            h1.canonical_bytes(),
            h2.canonical_bytes(),
            "同值冻结读数必须逐字节相同——顺序由构造器保证，不靠调用方"
        );
        assert_eq!(h1.sha256(), h2.sha256());
    }

    /// 不同快照身份 ⇒ 字节必不同（身份真的在字节域里，不是装饰字段）。
    #[test]
    fn a_different_snapshot_token_changes_the_bytes() {
        let a = MemoryId::new();
        let budget = || ContextBudget::new(1000, 500).expect("budget");
        let h1 = assemble(frozen_of(&[a]), budget());
        let mut f2 = frozen_of(&[a]);
        f2.snapshot_token_sha256 = "feedface".repeat(8);
        let h2 = assemble(f2, budget());
        assert_ne!(h1.canonical_bytes(), h2.canonical_bytes());
    }

    /// §25.5 溢出：不产出条目、manifest 全量具名、counts.overflow=true。
    /// 「截一半发出去」在这条路径上没有值可发。
    #[test]
    fn overflow_produces_a_manifest_not_a_truncated_handoff() {
        let ids = [MemoryId::new(), MemoryId::new()];
        let mut f = frozen_of(&ids);
        f.pinned = PinnedLane::new(0, vec![], vec![]);
        // 每行 10 token，上限 5：必溢出。
        let h = assemble(f, ContextBudget::new(100, 5).expect("budget"));
        assert!(h.counts.overflow);
        assert!(h.mandatory.is_empty(), "溢出路径一条都不交付");
        assert_eq!(h.overflow_manifest.len(), 2, "manifest 必须全量");
        assert_eq!(h.counts.mandatory_returned, 0);
    }

    /// Overflow returns no body-bearing rows, but keeps the snapshot's existing NotJudged
    /// diagnostic instead of silently relabeling it as judged.
    #[test]
    fn overflow_keeps_not_judged_diagnostics() {
        let id = MemoryId::new();
        let frozen = FrozenReads {
            mandatory: lane_with_rows(vec![row_with_grounding(id, 10, RowGrounding::NotJudged)]),
            pinned: PinnedLane::new(0, vec![], vec![]),
            context_snapshot_seq: 12345,
            snapshot_token_sha256: "deadbeef".repeat(8),
        };

        let handoff = assemble(frozen, ContextBudget::new(100, 5).expect("budget"));

        assert!(handoff.counts.overflow);
        assert!(handoff.mandatory.is_empty());
        assert!(handoff.pinned.is_empty());
        assert_eq!(handoff.not_judged, vec![id.0.to_string()]);
    }

    /// Cross-lane overlap is mandatory-precedence before reservation: one physical item,
    /// no duplicate Pinned count, and no false mandatory overflow from charging it twice.
    #[test]
    fn mandatory_precedence_deduplicates_pinned_before_budget_reservation() {
        let id = MemoryId::new();
        let frozen = FrozenReads {
            mandatory: lane_of(&[id]),
            pinned: PinnedLane::new(1, vec![row_with(id, 10)], vec![]),
            context_snapshot_seq: 12345,
            snapshot_token_sha256: "deadbeef".repeat(8),
        };
        let (handoff, outcome) = assemble_with_context(
            frozen,
            ContextBudget::new(15, 10).expect("one mandatory item fits"),
        );

        assert!(matches!(
            outcome,
            crate::compiler::ContextOutcome::Compiled(_)
        ));
        assert_eq!(handoff.mandatory.len(), 1);
        assert!(handoff.pinned.is_empty());
        assert_eq!(handoff.counts.pinned_expected, 1);
        assert_eq!(handoff.counts.pinned_returned, 0);
        assert_eq!(handoff.counts.pinned_excluded, 1);
    }

    /// needs_verification 与 unavailable 都在字节域里——它们变，字节就变。
    /// 「fail-loud」如果不进字节域，两次装配一个有告警一个没有也会"逐字节相同"，
    /// 那是把 DOD-093 的披露从判据里洗掉。
    #[test]
    fn needs_verification_and_unavailable_are_part_of_the_byte_domain() {
        let a = MemoryId::new();
        let budget = || ContextBudget::new(1000, 500).expect("budget");
        let h1 = assemble(frozen_of(&[a]), budget());

        let mut lane_outcomes = frozen_of(&[a]);
        // 同一行数据，但带上一条 unavailable。
        lane_outcomes.mandatory = MandatoryLane::from_selectors([
            SelectorOutcome::Unavailable {
                id: SelectorId::TaskExplicitContextV1,
                // dep-map: allow table-undeclared — selector metadata names the missing column; SQL runs in adapters
                missing_object: "private.memory_records.task_id".into(),
            },
            SelectorOutcome::Ran {
                id: SelectorId::ProjectActiveConstraintsV1,
                candidate_ids: vec![a],
                rows: vec![row_with(a, 10)],
                needs_verification: vec![],
            },
            SelectorOutcome::Ran {
                id: SelectorId::UserConfirmedCorrectionsV1,
                candidate_ids: vec![],
                rows: vec![],
                needs_verification: vec![],
            },
            SelectorOutcome::Ran {
                id: SelectorId::RequiredCurrentStateFacetsV1,
                candidate_ids: vec![],
                rows: vec![],
                needs_verification: vec![],
            },
            SelectorOutcome::Ran {
                id: SelectorId::ExplicitMandatoryBindingsV1,
                candidate_ids: vec![],
                rows: vec![],
                needs_verification: vec![],
            },
        ])
        .expect("selector outcomes");
        let h2 = assemble(lane_outcomes, budget());
        assert_ne!(
            h1.canonical_bytes(),
            h2.canonical_bytes(),
            "unavailable 必须改变字节——否则 partial 与 full 不可区分"
        );
        assert_eq!(h2.unavailable_selectors.len(), 1);
    }

    /// The retired registration `task_explicit_context_v1` (ADR-0046) must never reappear on
    /// any handoff surface, and every emitted selector name must be a *registered* one.
    ///
    /// Fault injection: put the literal back in `selector_wire` (or point it at
    /// `RETIRED_SELECTORS`) and this goes red on both halves.
    #[test]
    fn retired_selector_v1_never_reaches_the_wire() {
        let registered: Vec<&'static str> = humaux_domain::context::REGISTRY
            .iter()
            .map(|s| s.registered_name)
            .collect();
        for id in [
            SelectorId::TaskExplicitContextV1,
            SelectorId::ProjectActiveConstraintsV1,
            SelectorId::UserConfirmedCorrectionsV1,
            SelectorId::RequiredCurrentStateFacetsV1,
            SelectorId::ExplicitMandatoryBindingsV1,
        ] {
            let wire = selector_wire(id);
            assert_ne!(
                wire, "task_explicit_context_v1",
                "{id:?} still emits the retired v1 registration on the wire (ADR-0046)"
            );
            assert!(
                registered.contains(&wire),
                "{id:?} emits {wire}, which is not a §25.4 registered name"
            );
        }
        assert_eq!(
            selector_wire(SelectorId::TaskExplicitContextV1),
            "task_explicit_context_v2"
        );
    }
}
