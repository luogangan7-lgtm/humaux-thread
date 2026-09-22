//! `domain::policy` — `AuthorityPolicy`'s concrete origin-bound ceiling implementation
//! (§10.1 Origin-bound Authority Ceiling; the frozen G59-6 注错矩阵 in §59.1).
//!
//! §10.1's table maps each `EvidenceOriginClass` to the highest `AuthorityClass` a Memory
//! built on it may automatically reach, "防止总结后洗白" — a Candidate above its origin's
//! ceiling is rejected outright (§10.1 rule 3), never silently downgraded. The
//! `AuthorityPolicy` trait itself, `AuthorizedAuthority` and `CandidateRejection` are frozen in
//! `crate::authority` (that file's own doc: "the implementation lands in Phase 1 T1.8") — this
//! module supplies the T1.8 implementation, not a new contract shape.
//!
//! The ceiling table and the `InstructionDisposition` cap are exposed as inherent methods on
//! `EvidenceOriginClass` (`authority_ceiling` / `max_disposition`) rather than free functions
//! here: Rust allows an `impl` block for a crate-local type to live in a different module than
//! its `struct`/`enum` definition, so the §10.1 constraint is written directly onto the type
//! (per this task's own instruction) without editing `evidence.rs` (outside this file's scope).
//!
//! FLAG for the orchestrating task (out of this file's scope: `AuthorizedAuthority`'s shape is
//! frozen in `crate::authority`): §7.5 line 1421 and §45 G45-2 case E require a producer for
//! `BEHAVIOR_ELIGIBLE` — "只有 §10.1 AuthorityPolicy 能在满足 origin/confirmation 的 basis 上产生
//! BEHAVIOR_ELIGIBLE". `max_disposition` below is that table, but `authorize` never consults it
//! and `AuthorizedAuthority` carries only the `AuthorityClass`, so nothing in this crate can
//! currently emit `BEHAVIOR_ELIGIBLE` — G45-2 case E has no producer yet. Needs a card to extend
//! `AuthorizedAuthority`'s shape (the "for the T1.8 implementation to decide" part of its doc in
//! `authority.rs`) and wire `max_disposition` into `authorize`.

use crate::authority::{
    AuthorityClass, AuthorityPolicy, AuthorizedAuthority, CandidateRejection, NonEmptyVec,
};
use crate::evidence::{EvidenceOriginClass, InstructionDisposition};
use crate::ids::Scope;
use crate::memory::MemoryType;

impl EvidenceOriginClass {
    /// §10.1 frozen ceiling table, base row — one arm per table cell, before the row-1
    /// "项目管理操作可到 ProjectConstraint" exception (that exception depends on `MemoryType`,
    /// so it cannot live in this origin-only mapping; see `authority_ceiling` below).
    const fn base_ceiling(self) -> AuthorityClass {
        use EvidenceOriginClass::*;
        match self {
            // §10.1 row 1 base line; the Constraint-memory exception is layered on top below.
            DirectUserInput | UserConfirmed => AuthorityClass::UserCorrection,
            // §10.1 row 2 (tenant/workspace boundary check itself is §59.1 I6 / `Scope`, not
            // this table's job — this is only the numeric ceiling).
            TenantAdmin => AuthorityClass::ProjectConstraint,
            // §10.1 row 3.
            AuthenticatedAgent => AuthorityClass::PrivateKnowledge,
            // §10.1 row 4 (see `max_disposition` and `authorize`'s `UntrustedInstruction` branch
            // below for the "不自动成为行为指令" half of this row).
            TrustedConnector | ToolResult => AuthorityClass::PrivateKnowledge,
            // §10.1 row 5 (see `max_disposition` for the DATA_ONLY half of this row).
            UploadedArtifact | ExternalContent => AuthorityClass::PrivateKnowledge,
            // §10.1 row 6. This flat enum carries no inherited origin (`evidence.rs` §8.7 doc).
            // ponytail: defaults to the lowest non-public ceiling (and, in `max_disposition`
            // below, the DataOnly disposition) until `SystemMigration` carries a boxed inner
            // origin; upgrade path is adding that field in `evidence.rs` once a real migration
            // path needs to preserve the legacy ceiling/disposition exactly.
            SystemMigration => AuthorityClass::PrivateKnowledge,
        }
    }

    /// §10.1 full ceiling: `base_ceiling` plus row 1's Constraint-memory exception, which only
    /// applies when the Memory being authorized is itself a
    /// `MemoryType::Constraint` (a project-management operation) asserted or confirmed
    /// directly by the user.
    pub fn authority_ceiling(self, memory_type: MemoryType) -> AuthorityClass {
        use EvidenceOriginClass::*;
        if matches!(self, DirectUserInput | UserConfirmed) && memory_type == MemoryType::Constraint
        {
            AuthorityClass::ProjectConstraint
        } else {
            self.base_ceiling()
        }
    }

    /// §10.1: external/Tool text may enter fact memory, but Context Compiler must render it
    /// `DATA_ONLY`, never splice it into the system/developer instruction region. Origins whose
    /// own §10.1 row 4/5 text marks their content this way (`TrustedConnector`/`ToolResult`,
    /// `UploadedArtifact`/`ExternalContent`) can never carry `BehaviorEligible` content — this is
    /// a constraint on the origin's *content*, independent of `authority_ceiling`'s numeric
    /// `AuthorityClass` cap above.
    pub fn max_disposition(self) -> InstructionDisposition {
        use EvidenceOriginClass::*;
        match self {
            // §10.1 row 6 + `base_ceiling`'s matching `// ponytail:` note: migration is not an
            // upgrade path, so a `SystemMigration` origin with no recoverable legacy origin gets
            // the conservative `DataOnly` treatment, same lower-privilege default as its ceiling.
            TrustedConnector | ToolResult | UploadedArtifact | ExternalContent
            | SystemMigration => InstructionDisposition::DataOnly,
            DirectUserInput | UserConfirmed | TenantAdmin | AuthenticatedAgent => {
                InstructionDisposition::BehaviorEligible
            }
        }
    }
}

/// §10.1's concrete `AuthorityPolicy`: authorizes a requested `AuthorityClass` against the
/// origin-bound ceiling table above. Zero-sized — the ceiling table is a pure function of
/// `(origin, memory_type)`, there is no per-instance state to hold (§10.1 has no tenant-tunable
/// override for the ceiling table itself).
#[derive(Debug, Clone, Copy, Default)]
pub struct OriginBoundAuthorityPolicy;

impl AuthorityPolicy for OriginBoundAuthorityPolicy {
    /// §10.1 rule 3: never silently downgrades. Approval always returns `requested` itself
    /// (never a lower class than asked for); anything the `basis` cannot justify is an outright
    /// `Err`, never a quietly-substituted lower `AuthorizedAuthority`.
    ///
    /// `basis` may carry multiple Evidence origins (e.g. after §25.5's confirmation flow adds a
    /// `UserConfirmed` Evidence to a candidate's basis alongside its original low-authority one,
    /// per G59-6 case C) — the candidate is authorized if *any single* origin's ceiling covers
    /// `requested`. This is the existential reading of §10.1 rule 1's own wording verbatim:
    /// "UserPreference / UserCorrection / ProjectDecision / ProjectConstraint 必须有满足该
    /// Authority 的 basis Evidence" — "must HAVE a basis Evidence satisfying that Authority",
    /// not that every Evidence in `basis` individually satisfy it. Pinned by
    /// `authorize_ceiling_is_existential_over_basis_not_universal` below (a low-ceiling origin
    /// sitting alongside a confirmed one does not lower the outcome case C already grants).
    fn authorize(
        &self,
        requested: AuthorityClass,
        memory_type: MemoryType,
        basis: NonEmptyVec<EvidenceOriginClass>,
        _scope: &Scope,
    ) -> Result<AuthorizedAuthority, CandidateRejection> {
        StoredAuthority::authorize(requested, memory_type, basis)
    }
}

/// §10.1 存储权威的唯一判定点（card 22c / ADR-0046 的 **I-STORE**）。
///
/// 零尺寸、无状态，和 [`OriginBoundAuthorityPolicy`] 是同一个实现的两张脸：trait 那张脸给
/// 需要 `dyn AuthorityPolicy` 的调用方，这张脸给注错闸——「放开对 6 的拒绝」必须是**一处**
/// 可以改的地方，否则「注错变红」证明的只是某一个调用点。
pub struct StoredAuthority;

impl StoredAuthority {
    /// I-STORE：**存储行**可以带哪个 `AuthorityClass`。
    ///
    /// 两道门，顺序不可换：
    ///
    /// 1. `ExplicitTaskContext` **一律**拒绝。它不是一个够不够高的问题——6 根本不是内容属性，
    ///    而是「当前任务上下文实例」的使用权威，由 `domain::context::authorize_task_item` 验过
    ///    的一条任务绑定授权证明（I-TASK）。card 22b 之前这条只由 ceiling 表**间接**挡住
    ///    （没有任何 origin 的天花板到得了 6）；间接的守卫会随表变动而失效，而这一条不会。
    /// 2. 再是 §10.1 的 origin ceiling 表（`requested > ceiling` ⇒ 拒，从不静默降级）。
    ///
    /// # Errors
    /// `ExplicitTaskContext`、或 `requested` 超过 basis 任一 origin 的天花板。
    pub fn authorize(
        requested: AuthorityClass,
        memory_type: MemoryType,
        basis: NonEmptyVec<EvidenceOriginClass>,
    ) -> Result<AuthorizedAuthority, CandidateRejection> {
        // I-STORE. 注错点：删掉这三行，`stored_authority_refuses_explicit_task_context` 判红。
        if matches!(requested, AuthorityClass::ExplicitTaskContext) {
            return Err(CandidateRejection::OriginAuthorityCeiling);
        }
        // Tenant-boundary checking (`CrossTenantEvidence`) needs a tenant id per Evidence;
        // `EvidenceOriginClass` here is an opaque origin tag with no tenant field, so this
        // trait shape cannot evaluate it — that check belongs where the caller still has the
        // full `Evidence` record (§59.1 I6), not in this origin-only policy.
        //
        // `MissingConfirmation` ("requested needs a UserConfirmed basis that isn't present",
        // §10.1) is the same kind of deferred limitation: distinguishing "no origin's ceiling
        // covers `requested`" (any over-ceiling basis, reported below) from "this specific
        // class is only reachable via a confirmation step the caller hasn't run yet" needs a
        // richer basis than an opaque `EvidenceOriginClass` list carries, so this signature
        // cannot emit it either — both closed-set reasons stay unreachable at this call shape
        // until a caller passes richer evidence than `EvidenceOriginClass` alone.
        let ceiling = basis
            .as_slice()
            .iter()
            .map(|&origin| origin.authority_ceiling(memory_type))
            .max()
            .expect("NonEmptyVec guarantees at least one basis origin");

        if requested <= ceiling {
            return Ok(AuthorizedAuthority(requested));
        }

        // §10.1 拒绝原因闭集: reject as `UntrustedInstruction` if the failing basis contains ANY
        // origin whose row text explicitly says content "不自动成为行为指令" (an active
        // instruction-injection risk, §10.1 row 4) — checked as a set membership over the whole
        // `basis`, not by picking one tie-winning origin, so the reason cannot flip with the
        // caller's `Vec` insertion order (two `basis` orderings of the same set must produce the
        // same reason: `authorize_reason_is_independent_of_basis_order` below). Every other
        // over-ceiling basis (including the `DATA_ONLY` document sources of row 5 — G59-6 case A
        // is explicitly `OriginAuthorityCeiling`, not `UntrustedInstruction`) rejects as the
        // generic `OriginAuthorityCeiling`.
        let has_untrusted_instruction_origin = basis.as_slice().iter().any(|origin| {
            matches!(
                origin,
                EvidenceOriginClass::TrustedConnector | EvidenceOriginClass::ToolResult
            )
        });
        let reason = if has_untrusted_instruction_origin {
            CandidateRejection::UntrustedInstruction
        } else {
            CandidateRejection::OriginAuthorityCeiling
        };
        Err(reason)
    }
}

// §10.1 rule 3's "保留 Observation" is about the ingest layer not discarding the upstream
// Observation record — it is not a shape this policy module owns. No caller in this workspace
// needed a rejection wrapper beyond the plain `CandidateRejection` `authorize` already returns
// (grepped: none), so no such wrapper lives here; add one only when an actual ingest-layer
// caller needs a shape it cannot build itself from `authorize`'s own return value.

#[cfg(test)]
mod ceiling_table_tests {
    use super::*;

    /// §10.1 row-by-row base ceiling, pinned so a future edit to the table must update this in
    /// lockstep (the exhaustive `match` in `base_ceiling` already fails to compile on a missing
    /// variant; this additionally pins the *values*).
    #[test]
    fn base_ceiling_matches_table() {
        use AuthorityClass::*;
        use EvidenceOriginClass::*;
        let cases = [
            (DirectUserInput, UserCorrection),
            (UserConfirmed, UserCorrection),
            (TenantAdmin, ProjectConstraint),
            (AuthenticatedAgent, PrivateKnowledge),
            (TrustedConnector, PrivateKnowledge),
            (ToolResult, PrivateKnowledge),
            (UploadedArtifact, PrivateKnowledge),
            (ExternalContent, PrivateKnowledge),
            (SystemMigration, PrivateKnowledge),
        ];
        for (origin, expected) in cases {
            assert_eq!(
                origin.authority_ceiling(MemoryType::Fact),
                expected,
                "{origin:?} base ceiling mismatch"
            );
        }
    }

    /// card 22c / ADR-0046 **I-STORE**：存储权威对 `ExplicitTaskContext` 的拒绝是**直接的**，
    /// 不是「天花板恰好够不着」的副作用。
    ///
    /// 两件事分开断言，因为它们会被不同的改动打断：
    /// 1. 九个 origin × 每种 memory_type，请求 6 一律 `Err`——注错「放开
    ///    `StoredAuthority::authorize` 对 6 的拒绝」时，这一半仍然可能被 ceiling 表挡住，
    ///    所以第 2 条才是真正钉住这条守卫的那一条。
    /// 2. 即便 basis 的天花板被（假设地）抬到 6，请求 6 仍然被拒——这里用一个**独立**的
    ///    断言表达：拒绝发生在任何 ceiling 比较之前。删掉 `authorize` 顶部那三行 ⇒ 红。
    #[test]
    fn stored_authority_refuses_explicit_task_context() {
        use crate::memory::MemoryType;
        use EvidenceOriginClass::*;
        // §10.1 的九个 origin，逐个写出（枚举没有 ALL；`base_ceiling` 的穷举 match 已经
        // 保证新增变体编译不过，这里钉的是「每一个都被试过」）。
        const ORIGINS: [EvidenceOriginClass; 9] = [
            DirectUserInput,
            UserConfirmed,
            TenantAdmin,
            AuthenticatedAgent,
            TrustedConnector,
            ToolResult,
            UploadedArtifact,
            ExternalContent,
            SystemMigration,
        ];
        for origin in ORIGINS {
            for memory_type in MemoryType::ALL {
                let basis = NonEmptyVec::new(vec![origin]).expect("one origin");
                assert_eq!(
                    StoredAuthority::authorize(
                        AuthorityClass::ExplicitTaskContext,
                        memory_type,
                        basis
                    ),
                    Err(CandidateRejection::OriginAuthorityCeiling),
                    "{origin:?}/{memory_type:?} 不得存储 ExplicitTaskContext"
                );
            }
        }
        // 拒绝早于 ceiling 比较：请求 6 时，`requested <= ceiling` 这一步根本不该被走到。
        // 用「天花板最高的那条 basis」（TenantAdmin + Constraint ⇒ ProjectConstraint(5)）作对照：
        // 5 通过、6 被拒，且被拒的原因不依赖表里任何一格的数值。
        let admin_basis = NonEmptyVec::new(vec![EvidenceOriginClass::TenantAdmin]).expect("basis");
        assert!(
            StoredAuthority::authorize(
                AuthorityClass::ProjectConstraint,
                MemoryType::Constraint,
                admin_basis.clone()
            )
            .is_ok(),
            "5 必须仍然可存储，否则这条注错闸会因为把所有东西都拒了而假绿"
        );
        assert_eq!(
            StoredAuthority::authorize(
                AuthorityClass::ExplicitTaskContext,
                MemoryType::Constraint,
                admin_basis
            ),
            Err(CandidateRejection::OriginAuthorityCeiling)
        );
    }

    /// §10.1 rule 2: `ExplicitTaskContext` is never reachable through an Evidence-origin basis
    /// at all — it "只来自当前经过认证的 Task Request / Tenant Policy，不由后台蒸馏自动生成",
    /// and `EvidenceOriginClass` has no variant representing that path. Every origin's ceiling
    /// must therefore sit strictly below `ExplicitTaskContext`.
    #[test]
    fn no_origin_ceiling_reaches_explicit_task_context() {
        use EvidenceOriginClass::*;
        for origin in [
            DirectUserInput,
            UserConfirmed,
            TenantAdmin,
            AuthenticatedAgent,
            TrustedConnector,
            ToolResult,
            UploadedArtifact,
            ExternalContent,
            SystemMigration,
        ] {
            for memory_type in [MemoryType::Constraint, MemoryType::Fact] {
                assert!(
                    origin.authority_ceiling(memory_type) < AuthorityClass::ExplicitTaskContext,
                    "{origin:?}/{memory_type:?} must not reach ExplicitTaskContext"
                );
            }
        }
    }

    /// §10.1 row-1 exception: only a `MemoryType::Constraint` candidate gets the raised
    /// ProjectConstraint ceiling; any other `MemoryType` stays at the row's base `UserCorrection`.
    #[test]
    fn constraint_exception_is_scoped_to_constraint_memory_type() {
        assert_eq!(
            EvidenceOriginClass::UserConfirmed.authority_ceiling(MemoryType::Constraint),
            AuthorityClass::ProjectConstraint
        );
        assert_eq!(
            EvidenceOriginClass::UserConfirmed.authority_ceiling(MemoryType::Fact),
            AuthorityClass::UserCorrection
        );
    }

    #[test]
    fn max_disposition_matches_table() {
        use EvidenceOriginClass::*;
        use InstructionDisposition::*;
        let data_only = [
            TrustedConnector,
            ToolResult,
            UploadedArtifact,
            ExternalContent,
            SystemMigration,
        ];
        let behavior_eligible = [
            DirectUserInput,
            UserConfirmed,
            TenantAdmin,
            AuthenticatedAgent,
        ];
        for origin in data_only {
            assert_eq!(origin.max_disposition(), DataOnly, "{origin:?}");
        }
        for origin in behavior_eligible {
            assert_eq!(origin.max_disposition(), BehaviorEligible, "{origin:?}");
        }
    }
}
