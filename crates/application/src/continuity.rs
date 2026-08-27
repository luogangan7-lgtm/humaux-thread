//! `application::continuity` — §25.3 continuity 产品的装配入口（G80-31 的被测调用路径）。
//!
//! 端口反转的原因是依赖方向：`adapters` 依赖本 crate（不能反过来 import
//! `adapters::context_repo`），所以取数以 [`ContextReadPort`] 注入——`adapters` 侧实现它、
//! 委托 `context_repo::fetch_frozen`。先例：[`crate::consolidate::PrivateReasoningPort`]。
//!
//! 装配本体是纯函数（[`humaux_retrieval::handoff::assemble`]）：本函数只做「取冻结读数 →
//! 装配」两步，**收不到时钟、收不到连接**——G80-31「同一快照两次装配逐字节相同」的
//! application 侧保证就是这个签名形状。

use humaux_domain::context::{ContextBudget, FrozenReads};
use humaux_domain::error::ErrorCode;
use humaux_domain::ids::Scope;
use humaux_retrieval::handoff::{Handoff, assemble};

/// §25.4 冻结读数的取数端口。实现方：`adapters`（`context_repo::fetch_frozen`，
/// 单 REPEATABLE READ 事务）。
#[async_trait::async_trait]
pub trait ContextReadPort: Send + Sync {
    /// 单快照取齐 §25.4 步骤 2/5 的全部读数与快照身份。
    async fn fetch_frozen(&self, scope: &Scope) -> Result<FrozenReads, ErrorCode>;
}

/// §25.3：装配一份 continuity handoff。
///
/// `budget` 由调用方从上层配置传入（§78.1：domain/application 不内置默认值——
/// 默认值就是第二真源）。
///
/// # Errors
/// 取数失败原样上抛；装配本体不失败（溢出是 [`Handoff`] 里的一个如实状态，不是错误）。
pub async fn assemble_handoff(
    port: &dyn ContextReadPort,
    scope: &Scope,
    budget: ContextBudget,
) -> Result<Handoff, ErrorCode> {
    let frozen = port.fetch_frozen(scope).await?;
    Ok(assemble(frozen, budget))
}
