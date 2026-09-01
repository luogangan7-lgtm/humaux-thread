//! Native `continuity.get` application handler.

use std::sync::Arc;

use humaux_adapters::{continuity_read::PostgresContinuityReadPort, postgres::RuntimeDbPool};
use humaux_application::continuity::{ContinuityResult, read_project_continuity};
use humaux_domain::{
    context::ContextBudget, continuity::ProjectId, error::ErrorCode, identity::AuthorizationScope,
    ids::WorkspaceId,
};

pub async fn get<T>(
    pool: Arc<RuntimeDbPool>,
    authorization: AuthorizationScope,
    project_id: ProjectId,
    requested_workspace: Option<WorkspaceId>,
    budget: ContextBudget,
    accept: impl FnOnce(ContinuityResult) -> Result<T, ErrorCode>,
) -> Result<T, ErrorCode> {
    let adapter = PostgresContinuityReadPort::new(pool);
    let result = read_project_continuity(
        &adapter,
        &authorization,
        project_id,
        requested_workspace,
        budget,
    )
    .await?;
    accept(result)
}
