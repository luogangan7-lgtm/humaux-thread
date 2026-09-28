//! `retrieval::compiler` — §25.4 冻结装配的步骤 6+7。
//! Depends-on: crates=[humaux-domain]; services=[];
//!   env=[]; modules=[domain::authority, domain::context, retrieval::candidate]
//! Called-by: [adapters::context_repo, gateway::context, retrieval::envelope, retrieval::handoff, tests]
//! Invariants: []
//! Spec: §25.4
//!
//! **这是 G25-1 真正的被测对象**：全仓唯一同时握着 Mandatory/Pinned 两条 lane 与
//! semantic 候选池的地方，也因此是唯一能把「Mandatory 被 rerank 淘汰」这个注错写进去的
//! 地方。断言写在只握一条 lane 的函数上会是同义反复。
//!
//! 步骤 2–5 已在 domain 侧完成，本模块**结构上拿不到**绕过它们的路径：
//! - 拿不到未经 reserve 的预算——[`SupplementalBudget`] 没有第二构造点；
//! - 拿不到 Mandatory 行的分数——[`MandatoryRow`] 没有那个字段，而
//!   `Candidate::new` 要一个 `f32`，`MandatoryRow` 交不出来。
//!
//! 所以「把 mandatory 混进排序」不是一条要靠评审拦住的写法，是写不出来的表达式。

use humaux_domain::authority::MemoryId;
use humaux_domain::context::{
    MandatoryLane, MandatoryOverflow, MandatoryRow, PinnedLane, SupplementalBudget,
};

use crate::candidate::Candidate;

/// 装配后的一项。三档来源在类型上分开，不是一个带 `kind` 字段的结构体——
/// 分开之后「把 Supplemental 当成 Mandatory 上报」需要显式改一个变体名，
/// 而不是改一个字段值。
#[derive(Debug)]
pub enum ContextItem {
    /// §25.4 步骤 2：确定性 selector 选出的必带项。
    Mandatory(MandatoryRow),
    /// §25.4 步骤 5：用户/管理员显式钉住的。
    Pinned(MandatoryRow),
    /// §25.4 步骤 6：补充位，已过 RRF/rerank。
    Supplemental(Candidate),
}

/// 装配结果。
#[derive(Debug)]
pub struct CompiledContext {
    ordered: Vec<ContextItem>,
    dropped_supplemental: Vec<String>,
}

impl CompiledContext {
    /// 最终 Context，按 §25.4 的冻结顺序：Mandatory → Pinned → Supplemental。
    #[must_use]
    pub fn items(&self) -> &[ContextItem] {
        &self.ordered
    }

    /// 实际进入 Context 的 Mandatory id。
    ///
    /// **从 `ordered` 里筛，不是把输入字段照抄回来。** 这一条把 G25-1 从同义反复变成真
    /// 断言：照抄的话，即便装配阶段把 mandatory 全丢了，这里也照样报得出它们。
    #[must_use]
    pub fn mandatory_ids(&self) -> Vec<MemoryId> {
        self.ordered
            .iter()
            .filter_map(|i| match i {
                ContextItem::Mandatory(r) => Some(r.memory_id()),
                _ => None,
            })
            .collect()
    }

    /// 实际进入 Context 的 Mandatory 条数。同样是从 `ordered` 数出来的。
    #[must_use]
    pub fn mandatory_returned(&self) -> u64 {
        self.mandatory_ids().len() as u64
    }

    /// Pinned id，同上。
    #[must_use]
    pub fn pinned_ids(&self) -> Vec<MemoryId> {
        self.ordered
            .iter()
            .filter_map(|i| match i {
                ContextItem::Pinned(r) => Some(r.memory_id()),
                _ => None,
            })
            .collect()
    }

    /// 因补充位预算不足而被丢掉的候选 id。
    ///
    /// **补充位可以被丢，且丢了必须说。** 这与 Mandatory 的溢出是两件事：后者根本不产出
    /// Context（走 `Err` 臂），前者产出 Context 但把丢掉的列出来。
    #[must_use]
    pub fn dropped_supplemental(&self) -> &[String] {
        &self.dropped_supplemental
    }
}

/// §25.4 步骤 6+7。
///
/// 两条 lane 按值消费（[`MandatoryRow`] 不实现 `Clone`），所以装完之后调用方手里没有第二份
/// 可以再塞进别处。`ranked` 是**已经过 RRF/rerank 的**候选池——本函数不排序、不打分，
/// 它只按冻结顺序拼接并按预算截补充位。
///
/// 步骤 2–5 不经过 rerank：Mandatory 与 Pinned 在这里是**原样拼进去**的，中间没有任何
/// 依赖 `ranked` 的判断。
#[must_use]
pub fn compile(
    mandatory: MandatoryLane,
    pinned: PinnedLane,
    budget: SupplementalBudget,
    ranked: Vec<Candidate>,
) -> CompiledContext {
    let pinned = pinned.excluding_mandatory(&mandatory);
    let (_expected, mandatory_rows) = mandatory.into_parts();
    let pinned_rows = pinned.into_rows();

    let mut ordered: Vec<ContextItem> =
        Vec::with_capacity(mandatory_rows.len() + pinned_rows.len() + ranked.len());
    ordered.extend(mandatory_rows.into_iter().map(ContextItem::Mandatory));
    ordered.extend(pinned_rows.into_iter().map(ContextItem::Pinned));

    // 补充位按预算贪心装，装不下的记名丢弃。
    let mut remaining = budget.tokens();
    let mut dropped = Vec::new();
    for c in ranked {
        let cost = c.estimated_rerank_tokens();
        if cost <= remaining {
            remaining -= cost;
            ordered.push(ContextItem::Supplemental(c));
        } else {
            dropped.push(c.id().to_string());
        }
    }

    CompiledContext {
        ordered,
        dropped_supplemental: dropped,
    }
}

/// §25.4 步骤 4–7 的完整结果。
///
/// 溢出是**与 Context 并列的一个变体**，不是 `CompiledContext` 里的一个 flag：
/// 后者会给出「带着 overflow 标记但仍然返回一份 Context」这条静默通道，而 §25.5 要的正是
/// 那条路走不通。
#[derive(Debug)]
pub enum ContextOutcome {
    /// 装配成功。
    Compiled(CompiledContext),
    /// §25.5 溢出。**这个变体里没有 Context**。
    Overflow(MandatoryOverflow),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::candidate::Facet;
    use humaux_domain::context::{ContextBudget, SelectorId, SelectorOutcome, spec};

    fn m_row(tokens: u32) -> MandatoryRow {
        m_row_with_id(MemoryId::new(), tokens)
    }

    fn m_row_with_id(memory_id: MemoryId, tokens: u32) -> MandatoryRow {
        let s = spec(SelectorId::ProjectActiveConstraintsV1);
        match MandatoryRow::from_selector(
            s,
            memory_id,
            s.min_authority,
            tokens,
            humaux_domain::grounding::RowGrounding::Judged(
                humaux_domain::grounding::derive_grounding_state(
                    humaux_domain::grounding::GroundingInputs::Edges(&[]),
                ),
            ),
        )
        .expect("min_authority 恰好达标")
        {
            humaux_domain::context::Admitted::Row(r) => r,
            humaux_domain::context::Admitted::NeedsVerification(nv) => {
                panic!("CURRENT 行不该被分流: {nv:?}")
            }
        }
    }

    fn lane(rows: Vec<MandatoryRow>) -> MandatoryLane {
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

    /// **G25-1 本体**：1 条 Mandatory + 200 条高分补充候选，无论补充位怎么排，
    /// Mandatory 必须在 Context 里。
    ///
    /// 注错：把 `compile` 里 mandatory 的 `extend` 删掉 ⇒ 本条红。
    /// 这条断言之所以不是同义反复，是因为 `mandatory_ids()` **从 ordered 里筛**——
    /// 它读的是装配结果，不是输入。
    #[test]
    fn one_mandatory_survives_two_hundred_higher_scoring_supplementals() {
        let row = m_row(10);
        let target = row.memory_id();
        let m = lane(vec![row]);
        let p = PinnedLane::new(0, vec![], vec![]);

        // 200 条分数远高于任何 mandatory 的候选（mandatory 根本没有分数——这正是重点）。
        let noise: Vec<Candidate> = (0..200)
            .map(|i| Candidate::new(format!("noise-{i}"), Facet::State, 0.999, 1))
            .collect();

        // 预算只够装一小部分补充位：淘汰压力拉满。
        let budget = ContextBudget::new(60, 50)
            .expect("budget")
            .reserve(&m, &p)
            .expect("10 <= 50，不该溢出");

        let compiled = compile(m, p, budget, noise);
        assert!(
            compiled.mandatory_ids().contains(&target),
            "无论补充位怎么排，Mandatory 必须出现在 Context（§25.4 步骤 2-5 不经淘汰）"
        );
        assert_eq!(compiled.mandatory_returned(), 1);
        assert!(
            !compiled.dropped_supplemental().is_empty(),
            "预算显然装不下 200 条，必须有被丢弃的补充位——否则这条测试没有制造出淘汰压力"
        );
    }

    /// **故意坏掉的装配**：把 mandatory 丢掉，其余与 [`compile`] 逐字相同。
    ///
    /// 只存在于测试。它的存在理由是：G25-1 的注错若只活在文档里（「把 mandatory 的 extend
    /// 删掉就会红」），那句话就没有人验证过。有一个可执行的坏变体，观测点看不看得见这种
    /// 坏法就成了一条能跑的断言。
    fn compile_dropping_mandatory(
        mandatory: MandatoryLane,
        pinned: PinnedLane,
        budget: SupplementalBudget,
        ranked: Vec<Candidate>,
    ) -> CompiledContext {
        let (_expected, _dropped_on_purpose) = mandatory.into_parts();
        let pinned_rows = pinned.into_rows();
        let mut ordered: Vec<ContextItem> = Vec::new();
        ordered.extend(pinned_rows.into_iter().map(ContextItem::Pinned));
        let mut remaining = budget.tokens();
        let mut dropped = Vec::new();
        for c in ranked {
            let cost = c.estimated_rerank_tokens();
            if cost <= remaining {
                remaining -= cost;
                ordered.push(ContextItem::Supplemental(c));
            } else {
                dropped.push(c.id().to_string());
            }
        }
        CompiledContext {
            ordered,
            dropped_supplemental: dropped,
        }
    }

    /// **G25-1 的具名注错**（G80-33 规则6：没有具名注错的 verifier 不算数）。
    ///
    /// 同一条 mandatory、同一份输入，走真 `compile` 时观测点报 1 条，走
    /// [`compile_dropping_mandatory`] 时报 0 条。**两个数不同**，这就是「`mandatory_ids()`
    /// 读的是装配结果而不是输入」的证据——若它照抄输入，两边都会报 1 条，本条立刻红。
    ///
    /// 先前这条写成「空 lane 装配后必须报空」，那是**假的**：空 lane 下照抄输入也返回空，
    /// 两种实现不可区分，注错改坏了它也照样绿（实测确认过）。
    #[test]
    fn g25_1_fault_a_compile_that_drops_mandatory_is_visible_in_the_observable() {
        let make = || {
            let row = m_row(10);
            let id = row.memory_id();
            (lane(vec![row]), id)
        };

        let (m_good, id) = make();
        let p = PinnedLane::new(0, vec![], vec![]);
        let b = ContextBudget::new(100, 50)
            .expect("budget")
            .reserve(&m_good, &p)
            .expect("no overflow");
        let good = compile(m_good, p, b, vec![]);

        let (m_bad, _) = make();
        let p2 = PinnedLane::new(0, vec![], vec![]);
        let b2 = ContextBudget::new(100, 50)
            .expect("budget")
            .reserve(&m_bad, &p2)
            .expect("no overflow");
        let bad = compile_dropping_mandatory(m_bad, p2, b2, vec![]);

        assert_eq!(good.mandatory_ids(), vec![id], "真 compile 必须带上它");
        assert!(
            bad.mandatory_ids().is_empty(),
            "坏变体丢掉了 mandatory，观测点必须如实报空——报得出 id 就说明它读的是输入"
        );
        assert_ne!(
            good.mandatory_returned(),
            bad.mandatory_returned(),
            "两种装配的观测值必须不同；相同就意味着这个观测点分辨不出 mandatory 有没有进 Context"
        );
    }

    /// The public compiler also accepts raw lanes, so mandatory precedence must hold even
    /// when no handoff assembly normalized the Pinned lane first.
    #[test]
    fn compile_keeps_a_raw_cross_lane_memory_id_once() {
        let memory_id = MemoryId::new();
        let m = lane(vec![m_row_with_id(memory_id, 5)]);
        let p = PinnedLane::new(1, vec![m_row_with_id(memory_id, 5)], vec![]);
        let budget = ContextBudget::new(20, 10)
            .expect("budget")
            .reserve(&m, &p)
            .expect("raw overlap must not overflow");

        let compiled = compile(m, p, budget, vec![]);
        let mut ids = compiled
            .mandatory_ids()
            .into_iter()
            .chain(compiled.pinned_ids())
            .collect::<Vec<_>>();
        let before = ids.len();
        ids.sort_unstable();
        ids.dedup();

        assert_eq!(before, 1);
        assert_eq!(ids, vec![memory_id]);
    }

    /// 冻结顺序：Mandatory 在 Pinned 之前，Pinned 在 Supplemental 之前。
    #[test]
    fn frozen_assembly_order_is_mandatory_then_pinned_then_supplemental() {
        let m = lane(vec![m_row(5)]);
        let p = PinnedLane::new(1, vec![m_row(5)], vec![]);
        let budget = ContextBudget::new(100, 50)
            .expect("budget")
            .reserve(&m, &p)
            .expect("no overflow");
        let compiled = compile(
            m,
            p,
            budget,
            vec![Candidate::new("s-1", Facet::State, 0.5, 1)],
        );
        let kinds: Vec<&str> = compiled
            .items()
            .iter()
            .map(|i| match i {
                ContextItem::Mandatory(_) => "M",
                ContextItem::Pinned(_) => "P",
                ContextItem::Supplemental(_) => "S",
            })
            .collect();
        assert_eq!(kinds, vec!["M", "P", "S"]);
    }

    /// 补充位丢弃必须记名。丢了不说 = 静默截断，只是发生在补充位上。
    #[test]
    fn dropped_supplementals_are_named_not_just_counted() {
        let m = lane(vec![]);
        let p = PinnedLane::new(0, vec![], vec![]);
        let budget = ContextBudget::new(10, 5)
            .expect("budget")
            .reserve(&m, &p)
            .expect("no overflow");
        let compiled = compile(
            m,
            p,
            budget,
            vec![
                Candidate::new("fits", Facet::State, 0.9, 3),
                Candidate::new("too-big", Facet::State, 0.8, 999),
            ],
        );
        assert_eq!(compiled.dropped_supplemental(), &["too-big".to_string()]);
    }
}
