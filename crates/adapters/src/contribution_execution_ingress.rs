//! Transactional production core for an explicit MANUAL contribution start command.
//!
//! External authentication and transport stay deployment-owned. This adapter accepts only an
//! already-verified authorization scope plus the ID-free preparation request and caller
//! idempotency key. PostgreSQL remains the authority for policy, rights, source hashes, route
//! admission, execution identities, and the durable job chain.

use humaux_application::{
    contribute::{ContributionPreparationInput, PreparedAssessedContribution},
    contribution_execution::{ContributionExecutionEnqueueInput, ContributionPromptContract},
};
use humaux_domain::error::ErrorCode;

use crate::{
    contribution_entry_repo::load_execution_preparation_in_txn,
    contribution_execution_repo::{
        ContributionExecutionRepo, ContributionExecutionRepoError, EnqueuedContributionExecution,
    },
    postgres::PrivateWorkerDbPool,
};

#[derive(Debug)]
pub enum ContributionExecutionIngressError {
    Domain(ErrorCode),
    Repository(ContributionExecutionRepoError),
}

impl std::fmt::Display for ContributionExecutionIngressError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Domain(error) => write!(f, "contribution ingress contract failed: {error:?}"),
            Self::Repository(error) => write!(f, "contribution ingress repository failed: {error}"),
        }
    }
}

impl std::error::Error for ContributionExecutionIngressError {}

impl ContributionExecutionIngressError {
    /// Stable handler mapping for migration-0131's same-key semantic drift rejection.
    pub fn is_idempotency_conflict(&self) -> bool {
        matches!(
            self,
            Self::Repository(ContributionExecutionRepoError::Db(sqlx::Error::Database(error)))
                if error.code().as_deref() == Some("23505")
                    && error.message() == "contribution enqueue idempotency fingerprint conflict"
        )
    }
}

impl From<ErrorCode> for ContributionExecutionIngressError {
    fn from(value: ErrorCode) -> Self {
        Self::Domain(value)
    }
}

impl From<ContributionExecutionRepoError> for ContributionExecutionIngressError {
    fn from(value: ContributionExecutionRepoError) -> Self {
        Self::Repository(value)
    }
}

impl From<sqlx::Error> for ContributionExecutionIngressError {
    fn from(value: sqlx::Error) -> Self {
        Self::Repository(value.into())
    }
}

/// Private-worker-owned command boundary. It has no provider, scanner, public-pool, or Gateway
/// database capability and cannot accept caller-minted execution identities or fingerprints.
pub struct ContributionExecutionIngress<'a> {
    pool: &'a PrivateWorkerDbPool,
}

impl<'a> ContributionExecutionIngress<'a> {
    pub const fn new(pool: &'a PrivateWorkerDbPool) -> Self {
        Self { pool }
    }

    /// Freezes trusted preparation and migration-0131 enqueue in one consistent transaction.
    /// The commit completes before the existing runner can make either provider call.
    pub async fn start_manual(
        &self,
        request: ContributionPreparationInput,
        idempotency_key: String,
        coverage_contract: ContributionPromptContract,
        assessment_contract: ContributionPromptContract,
    ) -> Result<EnqueuedContributionExecution, ContributionExecutionIngressError> {
        if idempotency_key.trim().is_empty() {
            return Err(ErrorCode::InvalidInput.into());
        }

        let mut txn = self.pool.pool().begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            .execute(&mut *txn)
            .await?;

        // READ COMMITTED is deliberate: a waiting retry must take its preparation snapshot only
        // after an earlier same-key creator commits. The following contribution-inputs lock then
        // prevents every guarded authorization/policy/source input from changing until enqueue.
        ContributionExecutionRepo::lock_enqueue_key_in_txn(&mut txn, &idempotency_key).await?;
        let preparation = load_execution_preparation_in_txn(&mut txn, &request).await?;
        let prepared = PreparedAssessedContribution::try_from_current(request, preparation)?;
        let input = ContributionExecutionEnqueueInput::try_new(
            idempotency_key,
            coverage_contract,
            assessment_contract,
            prepared,
        )?;

        let execution_repo = ContributionExecutionRepo::new(self.pool);
        let enqueued = execution_repo
            .enqueue_compatible_in_txn(&mut txn, &input)
            .await?;
        txn.commit().await?;
        Ok(enqueued)
    }
}
