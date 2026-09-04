//! `application::supersede` — §36 `memory.supersede` use case, pure half (ADR-0018).
//!
//! Two things live here and nowhere else:
//! - the RNG contract for a confirm token (D-A: 32 random bytes; the domain type has no RNG,
//!   the adapter only ever sees the sha256);
//! - the supersede pair rule (D-C): target and successor must be two different, visible,
//!   `active` Memories. Visibility is the adapter's job (RLS + `can_read`); the status rule
//!   is pure and unit-tested here.

use humaux_domain::{
    authority::{AuthorityStatus, MemoryId},
    confirm::{CONFIRM_TOKEN_BYTES, ConfirmToken},
    error::ErrorCode,
};

/// Mints one fresh confirmation nonce (D-A: 32 bytes from the OS-seeded CSPRNG).
pub fn mint_confirm_token() -> ConfirmToken {
    ConfirmToken::from_bytes(rand::random::<[u8; CONFIRM_TOKEN_BYTES]>())
}

/// D-C: `successor` must exist, be visible (checked by the caller), be `active` (not itself
/// superseded/revoked/expired), and differ from `target`. A self-supersede is malformed
/// input; a non-active successor is the existing `CONFLICT` (§52.1: no new variant).
///
/// The *target's* status is deliberately not an input: its only arbiter is the adapter's
/// `UPDATE ... WHERE status = 'active'` (0 rows -> Conflict), which PostgreSQL re-evaluates
/// under the row lock. A pre-read here would be a second, racy judge that shadows the real
/// one (the double-supersede race window between SELECT and UPDATE).
pub fn check_pair(
    target: MemoryId,
    successor: MemoryId,
    successor_status: AuthorityStatus,
) -> Result<(), ErrorCode> {
    if target == successor {
        return Err(ErrorCode::InvalidInput);
    }
    if successor_status != AuthorityStatus::Active {
        return Err(ErrorCode::Conflict);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn minted_tokens_are_distinct_and_wire_round_trip() {
        let a = mint_confirm_token();
        let b = mint_confirm_token();
        assert_ne!(a, b);
        assert_eq!(ConfirmToken::decode(&a.encode()).unwrap(), a);
    }

    #[test]
    fn pair_rule_rejects_self_and_non_active_successor() {
        let target = MemoryId::new();
        let successor = MemoryId::new();
        assert_eq!(
            check_pair(target, successor, AuthorityStatus::Active),
            Ok(())
        );
        assert_eq!(
            check_pair(target, target, AuthorityStatus::Active),
            Err(ErrorCode::InvalidInput)
        );
        for status in [
            AuthorityStatus::Superseded,
            AuthorityStatus::Revoked,
            AuthorityStatus::Expired,
        ] {
            assert_eq!(
                check_pair(target, successor, status),
                Err(ErrorCode::Conflict)
            );
        }
    }
}
