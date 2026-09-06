//! `projection::dense` — dense lane 查询构造：自动注入 `tenant + §6.1 AuthorizationScope
//! visibility filter`（§17.1）。
//!
//! §17.1 冻结："所有 private query adapter 必须自动注入 tenant + AuthorizationScope
//! visibility filter；业务层不得手写可选 filter。" 本模块的落实方式是类型级的，不是靠约定：
//! [`DenseQueryFilter`] 的字段是私有的，唯一构造点是 [`build_dense_filter`]，其 `scope`
//! 参数类型是 `&AuthorizationScope`（不是 `Option<&AuthorizationScope>`）——workspace 里没有
//! 第二条能产出 `DenseQueryFilter` 却跳过 tenant/visibility 注入的路径；
//! `tests/no_handwritten_filter_scan.rs` 用 architecture-check 风格的源码扫描把这一点钉死到
//! 「本模块之外没有任何地方直接拼 [`Condition`] 树」。
//!
//! 可见性析取条件（[`visibility_disjunction`]）是 `domain::identity::can_read`（§6.1.1）三个
//! match 分支的 Qdrant-filter 镜像，不是第二套可见性策略——`identity` 模块头本身就警告过
//! "业务 Adapter 不允许各自维护一套'差不多相同'的可见性条件"；两者保持同步靠
//! `visibility_disjunction_matches_can_read` 这条穷举一致性测试，不是靠人工审阅。
//!
//! 本 crate 不 import HTTP / serde_json / Qdrant SDK（§3/§78.3）：[`Condition`] 是
//! adapter-中立的条件树，序列化成 Qdrant wire filter 是 `adapters::qdrant`（HTTP 层）的职责。

use humaux_domain::affect::{AffectFilter, BasisPointRange, BasisPoints};
use humaux_domain::identity::AuthorizationScope;
use humaux_domain::subject::SubjectId;

/// §6.1.3 / ADR-0029: the Qdrant payload field carrying a point's linked subject ids
/// (`adapters::qdrant::IndexablePayload::with_subject_ids` writes it, [`DenseQueryFilter::
/// about_any_of`] filters on it). One spelling, shared by writer and reader — a drift here would
/// make subject-scoped recall silently return nothing.
pub const SUBJECT_IDS_FIELD: &str = "subject_ids";

/// §8.5.1 / ADR-0030 D-D: the six FLAT array payload fields carrying a point's affect
/// annotations (`adapters::qdrant::IndexablePayload::with_affects` writes them,
/// [`DenseQueryFilter::with_affect`] filters on them). One annotation contributes one element
/// to each array at the same index; Qdrant evaluates an array field as "any element matches",
/// so a multi-clause filter is an OVER-approximation across annotations (clause A may hit one
/// annotation, clause B another). That is by design: Qdrant is a prefilter only — the PG hydrate
/// gate re-checks per annotation (`application::affect::memories_matching`). Same one-spelling
/// rule as [`SUBJECT_IDS_FIELD`].
pub const AFFECT_KINDS_FIELD: &str = "affect_kinds";
pub const AFFECT_LABELS_FIELD: &str = "affect_labels";
pub const AFFECT_VALENCE_FIELD: &str = "affect_valence_bp";
pub const AFFECT_AROUSAL_FIELD: &str = "affect_arousal_bp";
pub const AFFECT_DOMINANCE_FIELD: &str = "affect_dominance_bp";
pub const AFFECT_INTENSITY_FIELD: &str = "affect_intensity_bp";

/// Adapter-中立的 payload 条件树。`pub`：`adapters::qdrant` 需要遍历它来生成 Qdrant 的 wire
/// JSON filter；但业务层不应该把它当"随手拼一个 filter"的入口——真正进入检索调用的值类型是
/// [`DenseQueryFilter`]，它的字段私有，只能经 [`build_dense_filter`] 产出。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Condition {
    /// `field == value`（字符串比较——payload 字段本身就是 §17 里列出的 keyword/uuid 文本）。
    Eq { field: &'static str, value: String },
    /// `field IN values`（Qdrant `MatchAny`，用于 `WORKSPACE_SHARED` 的多 workspace 成员测试）。
    In {
        field: &'static str,
        values: Vec<String>,
    },
    /// `gte <= field <= lte`（Qdrant `range`，整数 payload；ADR-0030 的 affect basis-point 轴）。
    Range {
        field: &'static str,
        gte: i64,
        lte: i64,
    },
    /// 全部子条件为真。
    And(Vec<Condition>),
    /// 任一子条件为真。
    Or(Vec<Condition>),
}

/// 调用方对已授权 universe 的进一步缩小（例如按 year/type/task，§17.2 "Query retrieval
/// filter 可以在这个授权 universe 上继续按 year/type/task 缩小"）。[`build_dense_filter`]
/// 的这个参数是 `&[FieldMatch]` 而不是 `Option<...>`：空切片就是"不再缩小"的表达方式，
/// 所以不存在绕过 tenant/visibility 注入的第二个更底层入口。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldMatch {
    pub field: &'static str,
    pub value: String,
}

/// Dense lane 的查询 filter。唯一构造点是 [`build_dense_filter`]——字段私有，模块外没有任何
/// 代码能凭空拼一个跳过 tenant/visibility 注入的实例（§17.1 的类型级落实，见模块头）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DenseQueryFilter(Condition);

impl DenseQueryFilter {
    /// 读出拼装好的条件树，供 adapter 层序列化成 Qdrant wire filter（本 crate 自己不碰
    /// HTTP/Qdrant SDK，§3/§78.3）。
    pub fn as_condition(&self) -> &Condition {
        &self.0
    }

    /// §6.1.3 / ADR-0029 D-A: narrow an already-built filter to points linked to *any* of
    /// `subject_ids` (the aboutness axis), ANDed on top of the tenant clause and §6.1.2
    /// visibility disjunction — never in place of them. Consumes `self`, so the only way to
    /// obtain a subject-scoped filter is still through [`build_dense_filter`] first (§17.1's
    /// single construction point is unchanged). An empty slice returns the filter untouched.
    ///
    /// The clause is an `Or` of `Eq` terms on the `subject_ids` *array* payload field: Qdrant's
    /// `match.value` on an array field means "array contains value", so each `Eq` is one
    /// array-contains test and the `Or` is the any-of. `In` (`match.any`) is deliberately not
    /// used — it is the multi-workspace membership shape and would read as a scalar test.
    pub fn about_any_of(self, subject_ids: &[SubjectId]) -> Self {
        if subject_ids.is_empty() {
            return self;
        }
        let arms = subject_ids
            .iter()
            .map(|s| Condition::Eq {
                field: SUBJECT_IDS_FIELD,
                value: s.0.to_string(),
            })
            .collect();
        let mut clauses = match self.0 {
            Condition::And(clauses) => clauses,
            other => vec![other],
        };
        clauses.push(Condition::Or(arms));
        DenseQueryFilter(Condition::And(clauses))
    }

    /// §8.5.1 / ADR-0030 D-D: narrow an already-built filter by an explicit affect query,
    /// ANDed on top of tenant + visibility (+ subject) — never in place of them. Same consuming
    /// shape as [`Self::about_any_of`], so §17.1's single construction point stays
    /// [`build_dense_filter`]. `None` / an empty filter returns the filter untouched.
    ///
    /// Clause mapping onto the flat affect arrays ([`AFFECT_KINDS_FIELD`] …): `kinds` /
    /// `labels_any` → `Or(Eq …)` (array-contains any), each VAD interval → `Range`,
    /// `min_effective_intensity` → `Range` on the RAW intensity (effective ≤ raw for every row,
    /// so the raw bound is a superset — the exact effective test is PG's).
    pub fn with_affect(self, filter: Option<&AffectFilter>) -> Self {
        let Some(filter) = filter.filter(|f| !f.is_empty()) else {
            return self;
        };
        let mut clauses = match self.0 {
            Condition::And(clauses) => clauses,
            other => vec![other],
        };
        let any_of = |field: &'static str, values: Vec<String>| {
            Condition::Or(
                values
                    .into_iter()
                    .map(|value| Condition::Eq { field, value })
                    .collect(),
            )
        };
        let range = |field: &'static str, r: BasisPointRange| Condition::Range {
            field,
            gte: i64::from(r.lo.get()),
            lte: i64::from(r.hi.get()),
        };
        if !filter.kinds.is_empty() {
            clauses.push(any_of(
                AFFECT_KINDS_FIELD,
                filter.kinds.iter().map(|k| k.as_str().to_owned()).collect(),
            ));
        }
        if !filter.labels_any.is_empty() {
            clauses.push(any_of(
                AFFECT_LABELS_FIELD,
                filter
                    .labels_any
                    .iter()
                    .map(|l| l.as_str().to_owned())
                    .collect(),
            ));
        }
        for (field, interval) in [
            (AFFECT_VALENCE_FIELD, filter.valence),
            (AFFECT_AROUSAL_FIELD, filter.arousal),
            (AFFECT_DOMINANCE_FIELD, filter.dominance),
        ] {
            if let Some(interval) = interval {
                clauses.push(range(field, interval));
            }
        }
        if let Some(min) = filter.min_effective_intensity {
            clauses.push(Condition::Range {
                field: AFFECT_INTENSITY_FIELD,
                gte: i64::from(min.get()),
                lte: i64::from(BasisPoints::MAX.get()),
            });
        }
        DenseQueryFilter(Condition::And(clauses))
    }
}

/// 构造 dense lane 的查询 filter：`tenant_id == scope.tenant_id()` AND
/// [`visibility_disjunction`] AND 每一条 `narrow_by`（§17.1/§17.2）。
///
/// `scope` 是 `&AuthorizationScope`，不是 `Option<&AuthorizationScope>`——§17.1 "业务层不得
/// 手写可选 filter" 在这里没有例外分支。
pub fn build_dense_filter(
    scope: &AuthorizationScope,
    narrow_by: &[FieldMatch],
) -> DenseQueryFilter {
    let mut clauses = vec![tenant_clause(scope), visibility_disjunction(scope)];
    clauses.extend(narrow_by.iter().map(|m| Condition::Eq {
        field: m.field,
        value: m.value.clone(),
    }));
    DenseQueryFilter(Condition::And(clauses))
}

/// §6.1.2 三段独立 AND 谓词里的第一段："tenant filter"。`identity::can_read` 本身从不检查
/// tenant match（见其 rustdoc），调用方必须自己 AND 上这一条——这就是那一条。
fn tenant_clause(scope: &AuthorizationScope) -> Condition {
    Condition::Eq {
        field: "tenant_id",
        value: scope.tenant_id().0.to_string(),
    }
}

/// §6.1.2 三段独立 AND 谓词里的第二段："authorized visibility disjunction"——`can_read`
/// (§6.1.1) 三个 match 分支的 Qdrant-filter 镜像：
/// - `TenantShared` 恒真（`can_read` 对它也恒真）；
/// - `UserPrivate`：`scope.user_id()` 存在时才有对应分支（`can_read` 在 `scope.user_id()` 为
///   `None` 时对 `UserPrivate` 恒假，所以这里不加任何 `UserPrivate` 分支，效果一致）；
/// - `WorkspaceShared`：`scope.allowed_workspace_ids()` 非空时才有对应分支，用 `In` 枚举全部
///   allowed workspace id（`can_read` 用 `BoundedSet::contains`，语义等价于这里的 `IN`）。
fn visibility_disjunction(scope: &AuthorizationScope) -> Condition {
    let mut arms = vec![Condition::Eq {
        field: "visibility_class",
        value: "TENANT_SHARED".to_string(),
    }];

    if let Some(uid) = scope.user_id() {
        arms.push(Condition::And(vec![
            Condition::Eq {
                field: "visibility_class",
                value: "USER_PRIVATE".to_string(),
            },
            Condition::Eq {
                field: "visibility_user_id",
                value: uid.0.to_string(),
            },
        ]));
    }

    let workspace_ids: Vec<String> = scope
        .allowed_workspace_ids()
        .iter()
        .map(|w| w.0.to_string())
        .collect();
    if !workspace_ids.is_empty() {
        arms.push(Condition::And(vec![
            Condition::Eq {
                field: "visibility_class",
                value: "WORKSPACE_SHARED".to_string(),
            },
            Condition::In {
                field: "visibility_workspace_id",
                values: workspace_ids,
            },
        ]));
    }

    Condition::Or(arms)
}

#[cfg(test)]
mod tests {
    use super::*;
    use humaux_domain::identity::{
        BoundedSet, PrincipalId, VisibilityClass, VisibilityDescriptor, can_read,
    };
    use humaux_domain::ids::{TenantId, UserId, WorkspaceId};

    fn scope(user_id: Option<UserId>, workspaces: &[WorkspaceId]) -> AuthorizationScope {
        AuthorizationScope::new(
            TenantId::new(),
            PrincipalId::new(),
            user_id,
            BoundedSet::new(workspaces.iter().copied()).unwrap(),
        )
    }

    /// 独立求值 [`Condition`] 树，只用于测试断言——不是给生产用的第二个 evaluator。
    fn eval(cond: &Condition, fields: &std::collections::HashMap<&str, String>) -> bool {
        match cond {
            Condition::Eq { field, value } => fields.get(field) == Some(value),
            Condition::In { field, values } => {
                fields.get(field).is_some_and(|v| values.contains(v))
            }
            Condition::And(cs) => cs.iter().all(|c| eval(c, fields)),
            Condition::Or(cs) => cs.iter().any(|c| eval(c, fields)),
            Condition::Range { field, gte, lte } => fields
                .get(field)
                .and_then(|raw| raw.parse::<i64>().ok())
                .is_some_and(|v| v >= *gte && v <= *lte),
        }
    }

    fn descriptor_fields(
        desc: &VisibilityDescriptor,
    ) -> std::collections::HashMap<&'static str, String> {
        let mut m = std::collections::HashMap::new();
        m.insert(
            "visibility_class",
            match desc.class {
                VisibilityClass::UserPrivate => "USER_PRIVATE",
                VisibilityClass::WorkspaceShared => "WORKSPACE_SHARED",
                VisibilityClass::TenantShared => "TENANT_SHARED",
            }
            .to_string(),
        );
        if let Some(uid) = desc.user_id {
            m.insert("visibility_user_id", uid.0.to_string());
        }
        if let Some(wid) = desc.workspace_id {
            m.insert("visibility_workspace_id", wid.0.to_string());
        }
        m
    }

    // ---- §17.1: tenant clause is always present ----

    #[test]
    fn build_dense_filter_always_ands_tenant_clause() {
        let s = scope(None, &[]);
        let filter = build_dense_filter(&s, &[]);
        match filter.as_condition() {
            Condition::And(clauses) => {
                assert_eq!(
                    clauses[0],
                    Condition::Eq {
                        field: "tenant_id",
                        value: s.tenant_id().0.to_string()
                    }
                );
            }
            other => panic!("expected top-level And, got {other:?}"),
        }
    }

    #[test]
    fn narrow_by_is_anded_in_without_replacing_tenant_visibility() {
        let s = scope(None, &[]);
        let filter = build_dense_filter(
            &s,
            &[FieldMatch {
                field: "memory_type",
                value: "fact".into(),
            }],
        );
        let Condition::And(clauses) = filter.as_condition() else {
            panic!("expected And")
        };
        // tenant clause + visibility disjunction + 1 narrow_by term.
        assert_eq!(clauses.len(), 3);
        assert_eq!(
            clauses[2],
            Condition::Eq {
                field: "memory_type",
                value: "fact".into()
            }
        );
    }

    // ---- §6.1.3 / ADR-0029: subject any-of is ANDed after tenant + visibility ----

    #[test]
    fn about_any_of_ands_an_or_of_array_contains_terms_after_tenant_and_visibility() {
        let s = scope(None, &[]);
        let a = SubjectId::new();
        let b = SubjectId::new();
        let filter = build_dense_filter(&s, &[]).about_any_of(&[a, b]);
        let Condition::And(clauses) = filter.as_condition() else {
            panic!("expected And")
        };
        assert_eq!(clauses.len(), 3, "tenant + visibility + subject clause");
        assert_eq!(
            clauses[0],
            Condition::Eq {
                field: "tenant_id",
                value: s.tenant_id().0.to_string()
            }
        );
        assert_eq!(
            clauses[2],
            Condition::Or(vec![
                Condition::Eq {
                    field: SUBJECT_IDS_FIELD,
                    value: a.0.to_string()
                },
                Condition::Eq {
                    field: SUBJECT_IDS_FIELD,
                    value: b.0.to_string()
                },
            ])
        );
        // Empty = untouched (no vacuous `Or([])`, which Qdrant would treat as match-nothing).
        assert_eq!(
            build_dense_filter(&s, &[]).about_any_of(&[]),
            build_dense_filter(&s, &[])
        );
    }

    // ---- §8.5.1 / ADR-0030: affect clauses are ANDed after tenant + visibility ----

    #[test]
    fn with_affect_ands_any_of_and_ranges_after_tenant_and_visibility() {
        use humaux_domain::affect::{AffectKind, EmotionLabel};
        let s = scope(None, &[]);
        let filter = AffectFilter {
            kinds: vec![AffectKind::Emotion],
            labels_any: vec![EmotionLabel::Frustration, EmotionLabel::Anger],
            valence: Some(
                BasisPointRange::new(
                    BasisPoints::signed(-10_000).expect("bp"),
                    BasisPoints::signed(-1).expect("bp"),
                )
                .expect("range"),
            ),
            arousal: None,
            dominance: None,
            min_effective_intensity: Some(BasisPoints::unit(5_000).expect("bp")),
        };
        let built = build_dense_filter(&s, &[]).with_affect(Some(&filter));
        let Condition::And(clauses) = built.as_condition() else {
            panic!("expected And")
        };
        assert_eq!(
            clauses.len(),
            6,
            "tenant + visibility + kinds + labels + valence + min"
        );
        assert_eq!(
            clauses[2],
            Condition::Or(vec![Condition::Eq {
                field: AFFECT_KINDS_FIELD,
                value: "EMOTION".to_owned()
            }])
        );
        assert_eq!(
            clauses[4],
            Condition::Range {
                field: AFFECT_VALENCE_FIELD,
                gte: -10_000,
                lte: -1
            }
        );
        assert_eq!(
            clauses[5],
            Condition::Range {
                field: AFFECT_INTENSITY_FIELD,
                gte: 5_000,
                lte: 10_000
            }
        );
        // None / empty = untouched.
        assert_eq!(
            build_dense_filter(&s, &[]).with_affect(None),
            build_dense_filter(&s, &[])
        );
        assert_eq!(
            build_dense_filter(&s, &[]).with_affect(Some(&AffectFilter::default())),
            build_dense_filter(&s, &[])
        );
    }

    // ---- §6.1.1: visibility_disjunction must agree with can_read on every combination ----

    #[test]
    fn visibility_disjunction_matches_can_read() {
        let user_a = UserId::new();
        let user_b = UserId::new();
        let ws_a = WorkspaceId::new();
        let ws_b = WorkspaceId::new();

        let scopes = [
            scope(None, &[]),                   // service scope, no user, no workspace
            scope(Some(user_a), &[]),           // on-behalf-of user_a, no workspace
            scope(Some(user_a), &[ws_a]),       // user_a + workspace ws_a
            scope(Some(user_a), &[ws_a, ws_b]), // user_a + two workspaces
        ];

        let descriptors = [
            VisibilityDescriptor {
                class: VisibilityClass::TenantShared,
                user_id: None,
                workspace_id: None,
            },
            VisibilityDescriptor {
                class: VisibilityClass::UserPrivate,
                user_id: Some(user_a),
                workspace_id: None,
            },
            VisibilityDescriptor {
                class: VisibilityClass::UserPrivate,
                user_id: Some(user_b),
                workspace_id: None,
            },
            VisibilityDescriptor {
                class: VisibilityClass::WorkspaceShared,
                user_id: None,
                workspace_id: Some(ws_a),
            },
            VisibilityDescriptor {
                class: VisibilityClass::WorkspaceShared,
                user_id: None,
                workspace_id: Some(ws_b),
            },
        ];

        for s in &scopes {
            let disjunction = visibility_disjunction(s);
            for d in &descriptors {
                let expected = can_read(s, d);
                let got = eval(&disjunction, &descriptor_fields(d));
                assert_eq!(
                    got, expected,
                    "scope={s:?} descriptor={d:?}: filter said {got}, can_read said {expected}"
                );
            }
        }
    }
}
