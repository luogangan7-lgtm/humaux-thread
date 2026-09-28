//! `domain::selection` — §20.4 Stable Selection / Pagination Contract (T6.3, G20-1/G80-32).
//! Depends-on: crates=[sha2, uuid]; services=[]; env=[]; modules=[]
//! Called-by: [adapters::context_repo, adapters::selection_repo, gateway::guard, retrieval::request, tests]
//! Invariants: []
//! Spec: Baseline §3; §15.5; §20.1
//!
//! Pure types only (§3/§78.3: Domain never imports SQLx/HTTP/ENV) — the DB-side snapshot
//! materialization and page reads live in `adapters::selection_repo`; this module holds the
//! cursor shape, its MAC signing scheme, and every pure validity check a cursor must pass
//! before an adapter is allowed to read the manifest it names.
//!
//! §20.4 freezes two allowed modes. Mode A (single background Job — Private Consolidation
//! Input Selection / Export Builder / Projection Rebuild Selection) is already a worked
//! example: `adapters::consolidate_repo::select_and_materialize_inputs` runs one
//! `REPEATABLE READ READ WRITE` transaction that both selects and materializes, exactly the
//! recipe this section freezes. Mode B (`memory.enumerate` / EXACT / Export browser — cannot
//! hold a DB transaction open across a client round-trip) is what this module exists for:
//! [`Cursor`] is the "cursor only carries snapshot_id/query_fingerprint/last_sort_tuple/
//! expiry/MAC" contract, verbatim.
//!
//! §20.4's ban list, and how `Cursor` closes each one:
//!
//! - **OFFSET pagination over a live mutable set** — `adapters::selection_repo` materializes
//!   the full manifest into `ops.selection_snapshot_items` inside one transaction before any
//!   page is served; every page after that reads the now-immutable manifest by `ordinal`
//!   (this module's `last_sort_tuple`), never the live source table again.
//! - **cursor 不绑定 query/scope** — `mac` covers `query_fingerprint` and `tenant_id`
//!   together with the pagination position, so a cursor minted for one predicate/tenant
//!   cannot be replayed against another.
//! - **客户端可伪造 snapshot_upper_bound** — `last_ordinal` is inside the MAC; changing it
//!   without the server's key invalidates the token ([`Cursor::validate`]'s `InvalidMac`).
//! - **跨 tenant 复用 cursor** — checked explicitly and first, *before* MAC verification:
//!   a cursor validly signed for tenant A is not tampered when replayed under tenant B's
//!   session, so the MAC alone would still pass; [`Cursor::validate`] compares the cursor's
//!   own claimed `tenant_id` against the caller's actual request scope and rejects the
//!   mismatch on its own.
//!
//! `adapters::retrieve::issue_consistency_token`'s doc comment explicitly ponytails away a
//! signature ("§15.5 states outright this is 只提供 read-your-writes 约束,不是认证
//! token...upgrade path: HMAC-sign the field list with a server-held key") because that
//! token's real boundary is RLS, not itself. §20.4 draws the opposite line for EXACT/Export/
//! audit pagination ("不能使用 best-effort") — this module is that upgrade, applied to the
//! one call site that actually needs it, not retrofitted onto the read-your-writes token.

use sha2::{Digest, Sha256};
use uuid::Uuid;

const FIELD_SEP: char = '\u{1}';
const HMAC_BLOCK_SIZE: usize = 64;

/// Authorized Gateway enumeration, distinct from the worker's tenant-shared predicate.
pub const AUTHORIZED_MEMORY_ENUMERATION_V1: &str = "authorized_memory_enumeration_v1";

/// Derives a pagination-only key from bootstrap secret material. The fixed purpose label
/// prevents a cursor MAC from reusing the raw credential pepper as its signing key.
pub fn cursor_mac_key(master_key: &[u8]) -> [u8; 32] {
    hmac_sha256(master_key, b"humaux.memory.enumerate.cursor.v1")
}

/// HMAC-SHA256, hand-rolled from `sha2::Sha256` (already a `domain` dependency for
/// `evidence::payload_sha256`) rather than adding the `hmac` crate for this one call site
/// (ladder rung 5: an already-installed dependency does the hashing; the keying wrapper
/// around it is ~15 lines, not a new crate).
fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    let mut key_block = [0u8; HMAC_BLOCK_SIZE];
    if key.len() > HMAC_BLOCK_SIZE {
        let hashed = Sha256::digest(key);
        key_block[..32].copy_from_slice(&hashed);
    } else {
        key_block[..key.len()].copy_from_slice(key);
    }
    let mut ipad = [0x36u8; HMAC_BLOCK_SIZE];
    let mut opad = [0x5cu8; HMAC_BLOCK_SIZE];
    for i in 0..HMAC_BLOCK_SIZE {
        ipad[i] ^= key_block[i];
        opad[i] ^= key_block[i];
    }
    let mut inner = Sha256::new();
    inner.update(ipad);
    inner.update(message);
    let inner_digest = inner.finalize();

    let mut outer = Sha256::new();
    outer.update(opad);
    outer.update(inner_digest);
    outer.finalize().into()
}

/// Not a timing-attack-hardened compare (would need the `subtle` crate). §20.4 asks for
/// "客户端可伪造 snapshot_upper_bound" to be structurally impossible — the MAC itself provides
/// that; this only needs to not early-return on the *first* byte so a trivially short-circuit
/// isn't sitting right next to the thing that actually matters.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn from_hex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    let chars: Vec<char> = s.chars().collect();
    chars
        .chunks(2)
        .map(|pair| u8::from_str_radix(&pair.iter().collect::<String>(), 16).ok())
        .collect()
}

/// §20.1's `predicate_id` bound into a stable hex fingerprint together with the tenant it was
/// resolved for. [`Cursor::mac`] covers this string as an opaque field, so two different
/// predicates — or the same predicate resolved for a different tenant — can never validate
/// against each other's snapshot (§20.4 "cursor 不绑定 query/scope" ban).
pub fn query_fingerprint(predicate_id: &str, tenant_id: Uuid) -> String {
    let mut hasher = Sha256::new();
    hasher.update(predicate_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(tenant_id.as_bytes());
    to_hex(&hasher.finalize())
}

/// Why a [`Cursor`] failed [`Cursor::validate`] or [`Cursor::decode`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorError {
    /// Not valid hex, wrong field count, or an unparsable field — never trust client input
    /// enough to panic on it.
    Malformed(&'static str),
    /// §20.4 "跨 tenant 复用 cursor" ban: the cursor's own claimed `tenant_id` does not match
    /// the tenant the caller is actually scoped as for this request. Checked before MAC
    /// verification — see this module's doc comment for why the MAC alone cannot catch this
    /// case (the cursor is not tampered, only misdirected).
    CrossTenant,
    /// §20.4 "客户端可伪造 snapshot_upper_bound" ban: some field — most importantly
    /// `last_ordinal`, the client-visible pagination position — was altered after signing, or
    /// the token was never signed with this server's key at all.
    InvalidMac,
    Expired,
}

/// The §20.4 Mode B cursor: `snapshot_id` / `query_fingerprint` / `last_sort_tuple` (here,
/// `last_ordinal` — the position within the immutable manifest `adapters::selection_repo`
/// materializes once at page 1) / `expiry` / `MAC`, verbatim.
///
/// `mac` is private — the sole ways to obtain a `Cursor` with a `mac` that will pass
/// [`Cursor::validate`] are [`Cursor::sign`] (server-side, holds the key) and
/// [`Cursor::decode`] (parses whatever bytes came back from a client, verified separately).
/// There is no `pub` constructor that lets a caller supply an arbitrary `mac`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cursor {
    pub snapshot_id: Uuid,
    pub tenant_id: Uuid,
    pub query_fingerprint: String,
    pub last_ordinal: i64,
    pub expires_at_unix: i64,
    mac: [u8; 32],
}

impl Cursor {
    fn compute_mac(
        snapshot_id: Uuid,
        tenant_id: Uuid,
        query_fingerprint: &str,
        last_ordinal: i64,
        expires_at_unix: i64,
        mac_key: &[u8],
    ) -> [u8; 32] {
        let mut msg = Vec::with_capacity(16 + 16 + query_fingerprint.len() + 1 + 8 + 8);
        msg.extend_from_slice(snapshot_id.as_bytes());
        msg.extend_from_slice(tenant_id.as_bytes());
        msg.extend_from_slice(query_fingerprint.as_bytes());
        msg.push(0); // separator: query_fingerprint is always fixed-length hex in practice,
        // but nothing here assumes that — an explicit terminator keeps the encoding
        // unambiguous regardless.
        msg.extend_from_slice(&last_ordinal.to_be_bytes());
        msg.extend_from_slice(&expires_at_unix.to_be_bytes());
        hmac_sha256(mac_key, &msg)
    }

    /// Server-side mint — the only way to produce a `Cursor` guaranteed to pass
    /// [`Cursor::validate`] against the same `mac_key`.
    pub fn sign(
        snapshot_id: Uuid,
        tenant_id: Uuid,
        query_fingerprint: String,
        last_ordinal: i64,
        expires_at_unix: i64,
        mac_key: &[u8],
    ) -> Self {
        let mac = Self::compute_mac(
            snapshot_id,
            tenant_id,
            &query_fingerprint,
            last_ordinal,
            expires_at_unix,
            mac_key,
        );
        Self {
            snapshot_id,
            tenant_id,
            query_fingerprint,
            last_ordinal,
            expires_at_unix,
            mac,
        }
    }

    /// §20.4 checks, in order: cross-tenant reuse (cheap, and the one class of forgery a
    /// bit-for-bit-valid MAC cannot itself catch — see the module doc), MAC integrity
    /// (`InvalidMac` covers every other field, including a tampered `last_ordinal`), then
    /// expiry (a field the MAC already protects, so this is a business-rule check, not an
    /// integrity one).
    pub fn validate(
        &self,
        requested_tenant_id: Uuid,
        mac_key: &[u8],
        now_unix: i64,
    ) -> Result<(), CursorError> {
        if self.tenant_id != requested_tenant_id {
            return Err(CursorError::CrossTenant);
        }
        let expected = Self::compute_mac(
            self.snapshot_id,
            self.tenant_id,
            &self.query_fingerprint,
            self.last_ordinal,
            self.expires_at_unix,
            mac_key,
        );
        if !ct_eq(&expected, &self.mac) {
            return Err(CursorError::InvalidMac);
        }
        if now_unix >= self.expires_at_unix {
            return Err(CursorError::Expired);
        }
        Ok(())
    }

    /// Opaque wire encoding — same field-list-then-hex technique as
    /// `adapters::retrieve::issue_consistency_token`, with the MAC that module's doc comment
    /// names as the upgrade path appended as a final field.
    pub fn encode(&self) -> String {
        let fields = [
            self.snapshot_id.to_string(),
            self.tenant_id.to_string(),
            self.query_fingerprint.clone(),
            self.last_ordinal.to_string(),
            self.expires_at_unix.to_string(),
            to_hex(&self.mac),
        ];
        let plain = fields.join(&FIELD_SEP.to_string());
        to_hex(plain.as_bytes())
    }

    /// Inverse of [`Cursor::encode`]. Every failure mode returns [`CursorError::Malformed`] —
    /// never panics on attacker/client-controlled input. Does **not** verify the MAC or
    /// expiry — call [`Cursor::validate`] after decoding.
    pub fn decode(token: &str) -> Result<Self, CursorError> {
        let bytes = from_hex(token).ok_or(CursorError::Malformed("not hex"))?;
        let plain = String::from_utf8(bytes).map_err(|_| CursorError::Malformed("not utf8"))?;
        let parts: Vec<&str> = plain.split(FIELD_SEP).collect();
        let [
            snapshot_id,
            tenant_id,
            query_fingerprint,
            last_ordinal,
            expires_at_unix,
            mac_hex,
        ] = parts.as_slice()
        else {
            return Err(CursorError::Malformed("wrong field count"));
        };
        let snapshot_id =
            Uuid::parse_str(snapshot_id).map_err(|_| CursorError::Malformed("bad snapshot_id"))?;
        let tenant_id =
            Uuid::parse_str(tenant_id).map_err(|_| CursorError::Malformed("bad tenant_id"))?;
        let last_ordinal = last_ordinal
            .parse::<i64>()
            .map_err(|_| CursorError::Malformed("bad last_ordinal"))?;
        let expires_at_unix = expires_at_unix
            .parse::<i64>()
            .map_err(|_| CursorError::Malformed("bad expires_at"))?;
        let mac_vec = from_hex(mac_hex).ok_or(CursorError::Malformed("bad mac"))?;
        let mac: [u8; 32] = mac_vec
            .try_into()
            .map_err(|_| CursorError::Malformed("bad mac length"))?;
        Ok(Self {
            snapshot_id,
            tenant_id,
            query_fingerprint: (*query_fingerprint).to_string(),
            last_ordinal,
            expires_at_unix,
            mac,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &[u8] = b"test-mac-key-do-not-use-in-prod";

    fn sample() -> Cursor {
        Cursor::sign(
            Uuid::from_u128(1),
            Uuid::from_u128(2),
            query_fingerprint("enumerate_active_memory_records_v1", Uuid::from_u128(2)),
            40,
            9_999_999_999,
            KEY,
        )
    }

    #[test]
    fn round_trips_through_encode_decode() {
        let c = sample();
        let decoded = Cursor::decode(&c.encode()).expect("decode");
        assert_eq!(c, decoded);
        assert!(decoded.validate(Uuid::from_u128(2), KEY, 0).is_ok());
    }

    #[test]
    fn rejects_cross_tenant_reuse() {
        let c = sample();
        // Same, unmodified, validly-signed cursor — just presented under a different tenant's
        // session. MAC alone cannot catch this (nothing was tampered).
        assert_eq!(
            c.validate(Uuid::from_u128(999), KEY, 0),
            Err(CursorError::CrossTenant)
        );
    }

    #[test]
    fn rejects_forged_last_ordinal() {
        let c = sample();
        let mut forged = c.clone();
        forged.last_ordinal = 1_000_000; // client trying to jump past what it paid for
        assert_eq!(
            forged.validate(Uuid::from_u128(2), KEY, 0),
            Err(CursorError::InvalidMac)
        );
    }

    #[test]
    fn rejects_wrong_signing_key() {
        let c = sample();
        assert_eq!(
            c.validate(Uuid::from_u128(2), b"a-different-key-entirely", 0),
            Err(CursorError::InvalidMac)
        );
    }

    #[test]
    fn hmac_sha256_matches_rfc4231_short_and_long_key_vectors() {
        // Independent expected values: RFC 4231 sections 4.2 and 4.7.
        // https://www.rfc-editor.org/rfc/rfc4231
        assert_eq!(
            to_hex(&hmac_sha256(&[0x0b; 20], b"Hi There")),
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7",
        );
        assert_eq!(
            to_hex(&hmac_sha256(
                &[0xaa; 131],
                b"Test Using Larger Than Block-Size Key - Hash Key First"
            )),
            "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54",
        );
    }

    #[test]
    fn cursor_key_is_separated_from_credentials_and_other_bootstraps() {
        let key = cursor_mac_key(KEY);
        let cursor = Cursor::sign(
            Uuid::from_u128(1),
            Uuid::from_u128(2),
            "fp".to_owned(),
            0,
            100,
            &key,
        );
        assert!(
            cursor
                .validate(Uuid::from_u128(2), &cursor_mac_key(KEY), 0)
                .is_ok()
        );
        for other in [KEY, cursor_mac_key(b"other-bootstrap-secret").as_slice()] {
            assert_eq!(
                cursor.validate(Uuid::from_u128(2), other, 0),
                Err(CursorError::InvalidMac)
            );
        }
    }

    #[test]
    fn rejects_expired_cursor() {
        let c = Cursor::sign(
            Uuid::from_u128(1),
            Uuid::from_u128(2),
            "fp".to_string(),
            0,
            100,
            KEY,
        );
        assert_eq!(
            c.validate(Uuid::from_u128(2), KEY, 100),
            Err(CursorError::Expired)
        );
    }

    #[test]
    fn different_predicate_or_tenant_yields_different_fingerprint() {
        let t = Uuid::from_u128(2);
        let a = query_fingerprint("predicate_a", t);
        let b = query_fingerprint("predicate_b", t);
        let c = query_fingerprint("predicate_a", Uuid::from_u128(3));
        assert_ne!(a, b);
        assert_ne!(a, c);
    }
}
