//! `projection::sparse` — 本地 BM25 lane 的查询构造（§17.6 标准路径：Qdrant Cluster 内置
//! `qdrant/bm25`，Data Cell 内处理，不是外部 Managed Retrieval Provider）+
//! tenant-scoped IDF corpus（§17.2）。
//!
//! §17.2 冻结："IDF corpus 不得比 AuthorizationScope 更宽，否则虽然结果行被过滤，其他用户
//! 私人词频仍会影响当前用户排名。" 授权可见 corpus 的判定是
//! `object.tenant_id == scope.tenant_id() AND can_read(scope, &object.visibility)`
//! （[`in_authorized_corpus`]）——直接复用 `domain::identity::can_read`（§6.1.1），不另写一套
//! USER_PRIVATE / WORKSPACE_SHARED / TENANT_SHARED 判定；`tenant_scoped_document_frequency`
//! 在统计 df 之前先过滤掉 corpus 里不满足这条谓词的文档，所以别的 tenant、或本 tenant 内当前
//! principal 无权读取的 user/workspace 私有语料，从一开始就不会进入计数——不是"结果行被过滤
//! 但词频已经混入"。
//!
//! §17.6 冻结："如果 tenant policy 连 Data Cell 内的 Qdrant text processing 都不允许（极端
//! policy），Sparse lane 标 SKIPPED_BY_POLICY；不要回退到外部 provider。"
//! [`build_sparse_lane`] 在 `local_text_processing_allowed == false` 时直接返回
//! [`SparseLane::SkippedByPolicy`]，函数体里没有第二条路径可以走到某个外部 provider——不存在
//! 可以回退的代码。
//!
//! 本 crate 不 import HTTP / serde_json / Qdrant SDK（§3/§78.3）：真正把 query text 喂给
//! Qdrant 的 `qdrant/bm25` FastEmbed 模型是 `adapters::qdrant`（HTTP 层）的职责；本模块只产出
//! retrieval filter（复用 [`crate::dense::build_dense_filter`]，§17.6 "retrieval filter ->
//! tenant_id + AuthorizationScope visibility + query filters" 与 dense lane 是同一个谓词）和
//! tenant-scoped 的 IDF 权重。

use crate::dense::{DenseQueryFilter, FieldMatch, build_dense_filter};
use humaux_domain::identity::{AuthorizationScope, VisibilityDescriptor, can_read};
use humaux_domain::ids::TenantId;
use std::collections::{HashMap, HashSet};

/// Corpus 里的一份文档，按 Qdrant payload 的同一套字段打标（§17：`tenant_id` /
/// `visibility_class` / `visibility_user_id` / `visibility_workspace_id`），外加已经分词好的
/// `terms`（分词本身——喂给 Qdrant `qdrant/bm25` FastEmbed，§17.6——不在本任务范围内，这里只
/// 承接分词结果用于 df 统计）。
#[derive(Debug, Clone)]
pub struct CorpusDocument {
    pub tenant_id: TenantId,
    pub visibility: VisibilityDescriptor,
    pub terms: Vec<String>,
}

/// §17.2 的 `idf.corpus` 谓词：`tenant_id = T AND (USER_PRIVATE OR WORKSPACE_SHARED OR
/// TENANT_SHARED)`，即 `doc.tenant_id == scope.tenant_id() AND can_read(scope,
/// &doc.visibility)`（§6.1.2 的 tenant filter AND visibility disjunction 两段；IDF corpus
/// 本身不做 §6.1.2 第三段"query-specific narrower filter"——那是 retrieval filter 的事，见
/// [`build_sparse_lane`] 对 `narrow_by` 的处理）。
fn in_authorized_corpus(scope: &AuthorizationScope, doc: &CorpusDocument) -> bool {
    doc.tenant_id == scope.tenant_id() && can_read(scope, &doc.visibility)
}

/// 每个词的文档频率（df），只在 [`in_authorized_corpus`] 为真的文档上统计——这就是
/// "tenant-scoped IDF" 的落实点：其它 tenant、或本 tenant 内当前 principal 无权读取的私有
/// 语料，根本不会被计入这个 map。
pub fn tenant_scoped_document_frequency(
    scope: &AuthorizationScope,
    corpus: &[CorpusDocument],
) -> HashMap<String, u64> {
    let mut df: HashMap<String, u64> = HashMap::new();
    for doc in corpus.iter().filter(|d| in_authorized_corpus(scope, d)) {
        let mut seen: HashSet<&str> = HashSet::new();
        for term in &doc.terms {
            if seen.insert(term.as_str()) {
                *df.entry(term.clone()).or_insert(0) += 1;
            }
        }
    }
    df
}

/// 授权可见语料的文档总数（`N`）——[`bm25_idf`] 分母里的同一个 `N`，同样只数
/// [`in_authorized_corpus`] 为真的文档。
pub fn tenant_scoped_corpus_size(scope: &AuthorizationScope, corpus: &[CorpusDocument]) -> u64 {
    corpus
        .iter()
        .filter(|d| in_authorized_corpus(scope, d))
        .count() as u64
}

/// BM25 标准 IDF 公式（Robertson/Sparck-Jones，`+1` 版本）：
/// `ln(1 + (N - df + 0.5) / (df + 0.5))`。`+1` 保证 `df == N` 时仍非负（旧式
/// `ln((N-df+0.5)/(df+0.5))` 在这种情况下会给出负权重）。`document_frequency`/`corpus_size`
/// 必须已经是 tenant-scoped 的（[`tenant_scoped_document_frequency`] /
/// [`tenant_scoped_corpus_size`] 的输出），这个函数本身不做任何授权判定。
pub fn bm25_idf(document_frequency: u64, corpus_size: u64) -> f64 {
    let df = document_frequency as f64;
    let n = corpus_size as f64;
    (1.0 + (n - df + 0.5) / (df + 0.5)).ln()
}

/// 一次 sparse lane 构造的结果（§17.6）。不是 `ErrorCode`/`DegradeCode` 的第三个变体——lane
/// 被 policy 跳过既不是终止错误也不是质量降级，是这条 lane 根本没有运行；仓库 CLAUDE.md
/// "禁止新开第三个错误枚举" 约束的是 `ErrorCode`/`DegradeCode` 这一对，不覆盖这种查询时的
/// 执行状态描述符。
#[derive(Debug, Clone, PartialEq)]
pub enum SparseLane {
    /// Lane 实际运行：`filter` 是检索 filter（tenant + visibility + `narrow_by`，直接复用
    /// [`build_dense_filter`]——§17.6 "retrieval filter -> tenant_id + AuthorizationScope
    /// visibility + query filters" 与 dense lane 是同一个谓词，不是另写一套）；`idf` 是每个
    /// query term 的 tenant-scoped 权重（§17.2），只从 `corpus` 里 [`in_authorized_corpus`]
    /// 为真的文档算出。
    Executed {
        filter: DenseQueryFilter,
        idf: HashMap<String, f64>,
    },
    /// §17.6："如果 tenant policy 连 Data Cell 内的 Qdrant text processing 都不允许（极端
    /// policy），Sparse lane 标 SKIPPED_BY_POLICY；不要回退到外部 provider。"
    /// [`build_sparse_lane`] 里没有第二条能落到某个外部 provider 的分支。
    SkippedByPolicy,
}

impl SparseLane {
    /// 供遥测/日志使用的定长字符串——不是 `ErrorCode`/`DegradeCode::as_str()` 那条注册表的
    /// 一部分（本类型不是那两个枚举之一，见类型上的说明）。
    pub fn as_str(&self) -> &'static str {
        match self {
            SparseLane::Executed { .. } => "EXECUTED",
            SparseLane::SkippedByPolicy => "SKIPPED_BY_POLICY",
        }
    }
}

/// 构造本地 BM25 lane（§17.6 标准路径）。`scope` 是 `&AuthorizationScope`，不是
/// `Option<&AuthorizationScope>`——和 [`build_dense_filter`] 一样，没有跳过 tenant/visibility
/// 注入的入口（§17.1）。
///
/// `local_text_processing_allowed` 为 `false` 时直接返回 [`SparseLane::SkippedByPolicy`]，
/// 不读 `corpus`、不读 `query_terms`——§17.6 "不要回退到外部 provider" 在这里是"没有那条分支"
/// 而不是"有分支但被拦下"。
pub fn build_sparse_lane(
    scope: &AuthorizationScope,
    narrow_by: &[FieldMatch],
    corpus: &[CorpusDocument],
    query_terms: &[String],
    local_text_processing_allowed: bool,
) -> SparseLane {
    if !local_text_processing_allowed {
        return SparseLane::SkippedByPolicy;
    }

    let filter = build_dense_filter(scope, narrow_by);
    let df = tenant_scoped_document_frequency(scope, corpus);
    let n = tenant_scoped_corpus_size(scope, corpus);
    let idf = query_terms
        .iter()
        .map(|term| {
            let weight = bm25_idf(*df.get(term).unwrap_or(&0), n);
            (term.clone(), weight)
        })
        .collect();

    SparseLane::Executed { filter, idf }
}

#[cfg(test)]
mod tests {
    use super::*;
    use humaux_domain::identity::{BoundedSet, PrincipalId, VisibilityClass};
    use humaux_domain::ids::{UserId, WorkspaceId};

    fn scope(
        tenant: TenantId,
        user_id: Option<UserId>,
        workspaces: &[WorkspaceId],
    ) -> AuthorizationScope {
        AuthorizationScope::new(
            tenant,
            PrincipalId::new(),
            user_id,
            BoundedSet::new(workspaces.iter().copied()).unwrap(),
        )
    }

    fn tenant_shared_doc(tenant: TenantId, terms: &[&str]) -> CorpusDocument {
        CorpusDocument {
            tenant_id: tenant,
            visibility: VisibilityDescriptor {
                class: VisibilityClass::TenantShared,
                user_id: None,
                workspace_id: None,
            },
            terms: terms.iter().map(|s| s.to_string()).collect(),
        }
    }

    fn user_private_doc(tenant: TenantId, user_id: UserId, terms: &[&str]) -> CorpusDocument {
        CorpusDocument {
            tenant_id: tenant,
            visibility: VisibilityDescriptor {
                class: VisibilityClass::UserPrivate,
                user_id: Some(user_id),
                workspace_id: None,
            },
            terms: terms.iter().map(|s| s.to_string()).collect(),
        }
    }

    // ---- §17.2 core acceptance: cross-tenant corpus must not move the numbers ----

    #[test]
    fn cross_tenant_documents_do_not_change_document_frequency() {
        let tenant_a = TenantId::new();
        let tenant_b = TenantId::new();
        let scope_a = scope(tenant_a, None, &[]);

        // Tenant A alone: "shared" (df=1 of 1), "rare" absent.
        let corpus_a_only = vec![tenant_shared_doc(tenant_a, &["shared", "alpha"])];
        let df_a_only = tenant_scoped_document_frequency(&scope_a, &corpus_a_only);
        let n_a_only = tenant_scoped_corpus_size(&scope_a, &corpus_a_only);
        assert_eq!(n_a_only, 1);
        assert_eq!(df_a_only.get("shared"), Some(&1));
        assert_eq!(df_a_only.get("rare"), None);

        // Same tenant-A scope, but the corpus now also carries 20 tenant-B documents that
        // all repeat "shared" and introduce "rare" 20 times — if IDF corpus were shard-wide
        // instead of tenant-scoped, both numbers below would move.
        let mut corpus_mixed = corpus_a_only.clone();
        for _ in 0..20 {
            corpus_mixed.push(tenant_shared_doc(tenant_b, &["shared", "rare"]));
        }
        let df_mixed = tenant_scoped_document_frequency(&scope_a, &corpus_mixed);
        let n_mixed = tenant_scoped_corpus_size(&scope_a, &corpus_mixed);

        assert_eq!(
            n_mixed, n_a_only,
            "tenant B documents must not enter tenant A's corpus size"
        );
        assert_eq!(
            df_mixed, df_a_only,
            "tenant B documents must not enter tenant A's df counts"
        );

        // Observable at the IDF-value level too, not just the raw counts.
        let idf_a_only = bm25_idf(*df_a_only.get("shared").unwrap(), n_a_only);
        let idf_mixed = bm25_idf(*df_mixed.get("shared").unwrap(), n_mixed);
        assert_eq!(idf_a_only, idf_mixed);
        assert!(
            !df_mixed.contains_key("rare"),
            "a term that only appears in tenant B's documents must not surface in tenant A's df at all"
        );
    }

    #[test]
    fn same_tenant_other_users_private_documents_do_not_change_document_frequency() {
        // §17.2 is not only cross-tenant: within one tenant, a different user's
        // USER_PRIVATE documents must also stay out of the current principal's IDF corpus.
        let tenant = TenantId::new();
        let user_1 = UserId::new();
        let user_2 = UserId::new();
        let scope_1 = scope(tenant, Some(user_1), &[]);

        let corpus_user1_only = vec![user_private_doc(tenant, user_1, &["secret"])];
        let df_solo = tenant_scoped_document_frequency(&scope_1, &corpus_user1_only);
        let n_solo = tenant_scoped_corpus_size(&scope_1, &corpus_user1_only);

        let mut corpus_with_user2 = corpus_user1_only.clone();
        for _ in 0..10 {
            corpus_with_user2.push(user_private_doc(tenant, user_2, &["secret", "leak"]));
        }
        let df_with_user2 = tenant_scoped_document_frequency(&scope_1, &corpus_with_user2);
        let n_with_user2 = tenant_scoped_corpus_size(&scope_1, &corpus_with_user2);

        assert_eq!(n_with_user2, n_solo);
        assert_eq!(df_with_user2, df_solo);
        assert!(!df_with_user2.contains_key("leak"));
    }

    // ---- §17.6: policy skip has no fallback branch ----

    #[test]
    fn build_sparse_lane_skipped_by_policy_when_local_text_processing_disallowed() {
        let s = scope(TenantId::new(), None, &[]);
        let lane = build_sparse_lane(&s, &[], &[], &[], false);
        assert_eq!(lane, SparseLane::SkippedByPolicy);
        assert_eq!(lane.as_str(), "SKIPPED_BY_POLICY");
    }

    #[test]
    fn build_sparse_lane_executes_and_reuses_dense_filter_shape() {
        let tenant = TenantId::new();
        let s = scope(tenant, None, &[]);
        let corpus = vec![tenant_shared_doc(tenant, &["hello", "world"])];
        let lane = build_sparse_lane(
            &s,
            &[],
            &corpus,
            &["hello".to_string(), "absent".to_string()],
            true,
        );
        match lane {
            SparseLane::Executed { filter, idf } => {
                // Same shape build_dense_filter produces directly (tenant + visibility AND).
                assert_eq!(
                    filter.as_condition(),
                    build_dense_filter(&s, &[]).as_condition()
                );
                assert!(
                    idf["hello"] < idf["absent"],
                    "a term present in the corpus must score a lower IDF than one absent from it"
                );
            }
            SparseLane::SkippedByPolicy => panic!("expected Executed"),
        }
    }
}
