//! `projection::card` — T5.7/T5.8 RetrievalCard deterministic assembly (§18).
//! Depends-on: crates=[humaux-domain, sha2, uuid]; services=[]; env=[]; modules=[domain::authority,
//!   domain::dataclass, domain::ids, domain::memory]
//! Called-by: [adapters::projection_worker, adapters::qdrant, humaux-local-secret-scan, tests, xtask::card_version]
//! Invariants: [build_card is a pure, non-async function with no client/key/network handle (§18.1), so identical
//!   source_hash inputs always produce identical card bytes]
//! Spec: Baseline §18.1; §16; §3; §78.3; §18.4; §18.2; §22; §22.1
//!
//! ## §18.1: assembly, not generation
//!
//! §18.1 freezes the production method: "纯确定性拼装（Rust 纯函数，零外部模型调用）" — an
//! LLM-authored card would make `source_hash`-identical writes produce different card bytes
//! (breaking §16 projection replay) and would tie card existence to a BYOK key's liveness
//! (turning a knowledge gap into a *retrieval* gap, §18.1). [`build_card`] enforces this by
//! *type*, not by discipline: it is not `async`, its signature takes no client/key/network
//! handle of any kind, and this module imports nothing HTTP/SQLx/Qdrant/provider-SDK-shaped
//! (§3/§78.3 — nothing in `humaux-projection`'s dependency graph is HTTP/SQLx/Qdrant/
//! provider-SDK-shaped; besides `humaux-domain` its `Cargo.toml` only adds `uuid`/`sha2`, both
//! plain hashing/id-formatting libraries with no network or provider surface). A caller cannot
//! route this function through a model call; there is no parameter to hand one to.
//!
//! ## §18.4: one version axis
//!
//! [`CARD_BUILDER_VERSION`] is the *only* version axis (§18.4 frozen ruling: the second-axis
//! name §18.4 retired — spelled out there, deliberately not reproduced here so this doc comment
//! itself doesn't trip the scan below — does not appear anywhere in this module's own source,
//! continuously enforced by `xtask card-version`'s dedicated retired-name scan, T5.8 review).
//! [`CARD_TEMPLATE_HASH`] is not a second axis; it is this builder version's own template
//! content fingerprint — §18.4: "同 `card_builder_version` 必同 `card_template_hash`". Bump
//! both together whenever [`assemble`]'s separator/placeholder shape changes; `xtask
//! card-version` fails the build the moment two different hashes show up under one version, and
//! also fails the moment [`CARD_TEMPLATE_HASH`] stops matching [`template_fingerprint`]'s
//! recomputation of `assemble`'s real current output (T5.8 review: previously nothing tied the
//! literal to `assemble`'s actual bytes).
//!
//! ## §18.2: SECRET_MATERIAL never becomes a card
//!
//! [`build_card`] refuses at the top, before touching any other field, to build a card for
//! [`DataClass::SecretMaterial`] input — §18.2: "`data_class=SECRET_MATERIAL` 不生成卡、不进
//! PLATFORM_RETRIEVAL 索引，只走本地 literal lane". [`RetrievalCard::data_class`] is
//! therefore never observed to hold [`DataClass::SecretMaterial`] on any value this module
//! actually constructs — [`CardBuildOutcome::ExcludedSecret`] is the only outcome for that
//! input, carrying no card at all. §18.2 also requires this exclusion be visible, not silent
//! ("并在 §22 的 coverage 里显式扣除并报 `excluded_secret=N`，不许静默少给") — that
//! `excluded_secret=N` is §22.1's `count(*) ... where data_class='SECRET_MATERIAL'` read in the
//! same snapshot as `total`, which is a SQL adapter concern this IO-free crate does not own
//! (§3/§78.3). A prior version of this module carried a `count_excluded_secret` helper here
//! that counted an in-memory `&[CardBuildOutcome]` batch instead — a different population than
//! §22.1's one-query snapshot, unusable by any correct coverage implementation, so it has been
//! removed rather than kept as scaffolding shaped wrong for the interface it named.
//!
//! ## §18.4: card-build failure is a processing gap, not a silent hole
//!
//! §18.4: "禁止「卡失败 ⇒ 记忆静默不可检索」：`projection.retrieval_cards` 缺行必须计入 §15
//! processing gap，并把该查询的 completeness 拉到 `cannot_establish`". [`build_card`] surfaces
//! that failure as [`CardBuildOutcome::Unbuildable`] — the one outcome that, like
//! `ExcludedSecret`, carries no [`RetrievalCard`] to persist. The two are deliberately *not*
//! the same variant: `ExcludedSecret` is a policy exclusion (tracked via `excluded_secret`,
//! §18.2), `Unbuildable` is a processing failure (tracked via `projection::stream`'s existing
//! per-seq gap machinery, §15) — conflating them would make a `SECRET_MATERIAL` memory look
//! like a completeness failure it structurally is not.
//!
//! What "wiring to `projection::stream`'s gap interface" concretely means, and what is out of
//! this task's file scope (reported honestly per this task's own instruction, not forced): a
//! `projection.retrieval_cards` writer that receives [`CardBuildOutcome::Unbuildable`] for a
//! given `stream_seq` must transition that seq's `projection.stream_log` row to `state =
//! 'FAILED'` — the same state transition any other per-seq processing failure already uses
//! (`migrations/0007_projection.sql`'s 9-state `stream_log` CHECK, `projection::stream`'s
//! module doc). That transition alone is sufficient: `projection.processing_gaps` is a *view*
//! over `FAILED`/`LOST` rows (no second gap table to keep in sync, §15.2), and
//! [`crate::stream::advance_prefix`]'s three-way identity already refuses to let a
//! checkpoint's `projection_highwater` cross a `FAILED` seq — see
//! `stream::tests::advance_prefix_stops_at_the_gap_not_past_it` for that behavior against the
//! real function (a prior version of this module carried a same-file test that claimed to
//! demonstrate this but built its `StreamLedgerSnapshot` by hand rather than from a
//! `CardBuildOutcome::Unbuildable`, so it exercised nothing this module's own code produces;
//! removed rather than kept as false coverage, T5.8 review). What this crate does *not*
//! contain — because it does not exist yet
//! anywhere in the workspace, and inventing it here would be exactly the "如实报告不硬凑" this
//! task warns against — is (a) the adapter that actually performs that `UPDATE
//! projection.stream_log SET state = 'FAILED'` write (SQLx-shaped, belongs in
//! `humaux-adapters`, not this IO-free crate) and (b) the `CompletenessClass::CannotEstablish`
//! classifier itself (`projection::stream`'s own module doc already flags this as "out of this
//! task's scope — see `retrieval::completeness`'s module doc", a module that does not exist in
//! this crate). `Unbuildable` is the domain-level signal both of those future pieces consume;
//! no new interface was needed on the `projection::stream` side because a card-build failure
//! is not a new *kind* of gap, it is an ordinary one.

use std::time::SystemTime;

use humaux_domain::authority::MemoryId;
use humaux_domain::dataclass::DataClass;
use humaux_domain::ids::WorkspaceId;
use humaux_domain::memory::MemoryType;

/// §18.4 frozen single version axis. Bump together with [`CARD_TEMPLATE_HASH`] any time
/// [`assemble`]'s layout changes — see module doc.
pub const CARD_BUILDER_VERSION: &str = "v1";

/// Content fingerprint of the `v1` template ([`assemble`]'s separator/placeholder shape), not
/// a cryptographic digest — nothing in this workspace needs this value to resist forgery, only
/// to change whenever the template does. Derived, not hand-picked: it is
/// `{CARD_BUILDER_VERSION}-{sha256 hex of assemble()'s output over a fixed golden input}` —
/// [`template_fingerprint`] recomputes that from `assemble`'s *actual current* output, and this
/// module's own `card_template_hash_matches_computed_fingerprint` test (plus `xtask
/// card-version`, T5.8) assert the two stay equal. Editing `assemble`'s separator/placeholder
/// shape without updating this literal to match now fails the build instead of staying silently
/// green — a hand-written descriptive string with nothing tying it to `assemble`'s actual bytes
/// used to let exactly that fault injection pass, T5.8 review.
// ponytail: still a hand-pasted literal, not a `const`-evaluated one — `sha2::Sha256::digest`
// is not a `const fn` on stable Rust, so there is no way to make the compiler itself recompute
// this at the definition site. `template_fingerprint()` plus the test/xtask assertions are the
// upgrade path chosen instead: run `template_fingerprint()`, paste its output here. Move to a
// true compile-time digest only if a const-eval-friendly SHA-256 ever lands in `sha2`/std.
pub const CARD_TEMPLATE_HASH: &str =
    "v1-ea55e89d6b9673b7113957816c36d347c4c071766189854609e8731f24dac52b";

/// Fixed, arbitrary golden input for [`CARD_TEMPLATE_HASH`]'s fingerprint. Content is
/// meaningless — never touches [`build_card`] — only its stability across time matters; do not
/// "clean up" these values without also recomputing and re-pasting [`CARD_TEMPLATE_HASH`].
const TEMPLATE_FINGERPRINT_TITLE: &str = "GOLDEN_TITLE";
const TEMPLATE_FINGERPRINT_KEY_CLAIM: &str = "GOLDEN_KEY_CLAIM";
const TEMPLATE_FINGERPRINT_ENTITIES: [&str; 2] = ["golden_entity_a", "golden_entity_b"];
const TEMPLATE_FINGERPRINT_EVIDENCE_EXCERPT: &str = "GOLDEN_EVIDENCE_EXCERPT";

/// Recomputes what [`CARD_TEMPLATE_HASH`] *should* be from [`assemble`]'s real current output —
/// the one mechanical tie this module's doc used to only promise in prose (T5.8 review: a
/// fault-injected separator change in `assemble` used to leave every gate green). `xtask
/// card-version` calls this directly instead of re-deriving the hash itself, so there is
/// exactly one hashing implementation to keep in sync with `assemble`.
pub fn template_fingerprint() -> String {
    use sha2::{Digest, Sha256};
    let golden = assemble(
        TEMPLATE_FINGERPRINT_TITLE,
        TEMPLATE_FINGERPRINT_KEY_CLAIM,
        &TEMPLATE_FINGERPRINT_ENTITIES.map(str::to_string),
        TEMPLATE_FINGERPRINT_EVIDENCE_EXCERPT,
    );
    let digest = Sha256::digest(golden.as_bytes());
    let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
    format!("{CARD_BUILDER_VERSION}-{hex}")
}

const MISSING_KEY_CLAIM_PLACEHOLDER: &str = "(no key claim recorded)";
const MISSING_EVIDENCE_EXCERPT_PLACEHOLDER: &str = "(no evidence excerpt recorded)";

/// §18.2's `egress_disposition` field — a card-level classification of whether this card is
/// currently allowed to leave the Data Cell, distinct from [`humaux_domain::egress::EgressPermit`]
/// (which is the machinery that actually *grants* one out-bound call, §7.3). §18.2: "没有
/// [`data_class`]/`egress_disposition`，`SECRET_MATERIAL` 在出境这一侧不可判定" — this field is
/// what a Planner reads before ever attempting to mint a permit, not a permit itself.
///
/// [`data_class`]: DataClass
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EgressDisposition {
    /// May be routed to an external Dense/Rerank provider without further gating (§18.4
    /// "信任域" — Planner still needs a policy-allowed route, but this card itself imposes no
    /// additional block).
    Allowed,
    /// Tenant/workspace policy must be consulted before this card may egress (§7.2's
    /// `private_retrieval_external_allowed` switch is exactly this kind of gate).
    PolicyGated,
    /// Must never leave the Data Cell via the external Dense/Rerank route; local BM25/EXACT/
    /// LITERAL lane only (§18.4).
    Forbidden,
}

impl EgressDisposition {
    /// The frozen `SCREAMING_SNAKE` wire form (§18.2's own three literals, `ALLOWED |
    /// POLICY_GATED | FORBIDDEN`) — mirrors `projection.retrieval_cards.egress_disposition`'s
    /// CHECK constraint (migration 0074), same split `DataClass::as_str` uses.
    pub const fn as_str(self) -> &'static str {
        match self {
            EgressDisposition::Allowed => "ALLOWED",
            EgressDisposition::PolicyGated => "POLICY_GATED",
            EgressDisposition::Forbidden => "FORBIDDEN",
        }
    }
}

/// §18.4's failure model: "拼装是纯函数，只因源字段缺失而降级". `Complete`/`Partial` are the
/// only two values a persisted [`RetrievalCard`] can ever carry — an `Unbuildable` source never
/// reaches a `RetrievalCard` at all (see [`CardBuildOutcome::Unbuildable`] and this module's
/// doc), so there is no third variant here to construct on a real card. The wire forms below
/// bind straight into `projection.retrieval_cards.status`
/// (`migrations/0063_retrieval_cards_projection_record_fields.sql`, T5.1's own column for this
/// same §18.4 field — its CHECK's three lowercase literals, `complete | partial |
/// unbuildable`, are §18.4's own casing; migration 0074 deliberately does not add a second
/// `card_status` column, see that migration's own comment).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CardStatus {
    /// Every source field ([`title`](RetrievalCard::title), `key_claim`, `evidence_excerpt`)
    /// was present in the input; no placeholder substitution happened.
    Complete,
    /// At least one of `key_claim`/`evidence_excerpt` was missing from the input and was
    /// filled with a fixed placeholder (§18.4: "缺字段用固定占位，不放弃整卡").
    Partial,
}

impl CardStatus {
    /// The frozen lowercase wire form — §18.4's own prose casing (`complete | partial |
    /// unbuildable`), matching `projection.retrieval_cards.status`'s CHECK constraint
    /// (migration 0063) rather than this workspace's more common `SCREAMING_SNAKE` convention
    /// (`DataClass::as_str`, [`EgressDisposition::as_str`]) — the field this module binds
    /// into already froze that casing before this module existed.
    pub const fn as_str(self) -> &'static str {
        match self {
            CardStatus::Complete => "complete",
            CardStatus::Partial => "partial",
        }
    }
}

/// `memory_type`'s wire form for this card — the same `SCREAMING_SNAKE` convention
/// `DataClass::as_str`/`error::ErrorCode`'s `as_str` use elsewhere in this workspace, kept
/// local to this module rather than added to `domain::memory::MemoryType` (out of this task's
/// file scope — see CLAUDE.md "只动你拥有的文件"; `domain::memory`'s own module doc already
/// says the `MemoryRecord` struct body, and by extension its wire mapping, is future work).
/// Exhaustive `match` — a 13th `MemoryType` variant fails this file to *compile*, not a
/// silently-defaulted case (§8.5's 12-variant closed set).
const fn memory_type_wire(t: MemoryType) -> &'static str {
    match t {
        MemoryType::Fact => "FACT",
        MemoryType::Preference => "PREFERENCE",
        MemoryType::Decision => "DECISION",
        MemoryType::Rejection => "REJECTION",
        MemoryType::State => "STATE",
        MemoryType::Issue => "ISSUE",
        MemoryType::Lesson => "LESSON",
        MemoryType::Constraint => "CONSTRAINT",
        MemoryType::Procedure => "PROCEDURE",
        MemoryType::Outcome => "OUTCOME",
        MemoryType::Reference => "REFERENCE",
        MemoryType::Note => "NOTE",
    }
}

/// The source fields [`build_card`] assembles a card from — everything §18.2's structure needs
/// that is not itself computed by the builder (`schema_version`/`card_builder_version`/
/// `card_template_hash`/`card_status`/`card_text` are outputs, not inputs). `memory_type`
/// belongs to the source and is caried through as data — the note under [`memory_type_wire`]
/// explains why it does not live on `domain::memory::MemoryType` itself yet.
///
/// `key_claim`/`evidence_excerpt` are `Option<String>`: `None` (or `Some("")`) is a source
/// field genuinely missing, distinct from `entities` being legitimately empty (most memory
/// types carry no named entities at all — an empty `Vec` there is not a degradation, §18.2's
/// field list does not mark it "缺字段").
///
/// `entities` carries no canonical-order contract of its own — [`build_card`] sorts and dedups
/// it before assembly, so a caller populating it from a `HashSet`/`HashMap` iteration still
/// gets byte-identical `card_text` for the same `source_hash` (§18.1's replay requirement: this
/// is the one field [`crate::fingerprint::source_hash`]'s own canonical-sort discipline did not
/// already cover).
#[derive(Debug, Clone)]
pub struct CardInput {
    pub memory_id: MemoryId,
    pub memory_type: MemoryType,
    pub data_class: DataClass,
    pub egress_disposition: EgressDisposition,
    pub workspace_id: Option<WorkspaceId>,
    pub topic: Option<String>,
    pub effective_from: SystemTime,
    pub title: String,
    pub key_claim: Option<String>,
    pub entities: Vec<String>,
    pub evidence_excerpt: Option<String>,
}

/// §18.2's char-budget ceiling for the assembled/indexed text (`card_text`, [`assemble`]'s
/// output). §18.4's own closing line: "RetrievalCard 最终长度必须由 §55 benchmark 实测，而非
/// 固定" — so this is a caller-supplied config, not a hardcoded constant in this module
/// (§78.1 bans hardcoded budgets/thresholds the same way it bans hardcoded TTLs/quotas).
// ponytail: `max_chars` is a `char`-count proxy for "80–150 tokens" (§18), not a real
// tokenizer — no tokenizer dependency exists anywhere in this workspace yet. Swap the unit for
// a real token count once §55's benchmark picks the embedding/rerank tokenizer; until then this
// is deliberately conservative (a token is rarely more than ~4 chars for the languages this
// workspace's cards are written in, so a char budget under-fills before it ever over-fills).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CardBudget {
    pub max_chars: usize,
}

impl Default for CardBudget {
    /// 150 tokens (§18's upper experimental bound) × 4 chars/token.
    fn default() -> Self {
        Self { max_chars: 600 }
    }
}

/// §18.2's fixed field list, plus the builder's own output metadata (`schema_version`/
/// `card_builder_version`/`card_template_hash`/`card_status`/`card_text`). The only way to
/// obtain one is [`build_card`] — there is no `pub` constructor here, so a value with, say, a
/// `data_class` of [`DataClass::SecretMaterial`] or a `card_text` that does not contain `title`
/// cannot be hand-assembled by a caller that skips [`build_card`]'s checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetrievalCard {
    pub schema_version: u32,
    pub card_builder_version: &'static str,
    pub card_template_hash: &'static str,
    pub memory_id: MemoryId,
    pub memory_type: MemoryType,
    /// Never [`DataClass::SecretMaterial`] on a value this module constructs — see module doc.
    pub data_class: DataClass,
    pub egress_disposition: EgressDisposition,
    pub workspace_id: Option<WorkspaceId>,
    pub topic: Option<String>,
    pub effective_from: SystemTime,
    /// §18.3: "永不截断" — [`build_card`] never shortens this field, regardless of
    /// [`CardBudget`]. Always the caller's original `title` byte-for-byte.
    pub title: String,
    pub key_claim: String,
    pub entities: Vec<String>,
    pub evidence_excerpt: String,
    pub card_status: CardStatus,
    /// §18.3's "被索引文本" — [`title`](Self::title) joined with `key_claim`/`entities`/
    /// `evidence_excerpt` per [`assemble`]. [`build_card`]'s `debug_assert!` (§18.3's own
    /// verbatim shape) guarantees this always contains [`title`](Self::title) in full.
    pub card_text: String,
}

impl RetrievalCard {
    /// This schema's fixed version — bump alongside a §18.2 field-list change (a change to
    /// what `build_card` reads, not how it joins the text — that is [`CARD_BUILDER_VERSION`]'s
    /// job).
    pub const SCHEMA_VERSION: u32 = 1;

    /// `memory_type`'s wire form for the `projection.retrieval_cards.memory_type` column
    /// (migration 0074's CHECK constraint mirrors exactly [`memory_type_wire`]'s 12 literals)
    /// — the write-path counterpart to [`data_class`](Self::data_class)`.as_str()` and
    /// [`egress_disposition`](Self::egress_disposition)`.as_str()`, which are already methods
    /// on their own types.
    pub const fn memory_type_wire(&self) -> &'static str {
        memory_type_wire(self.memory_type)
    }
}

/// [`build_card`]'s outcome — three states, only one of which carries a card to persist. See
/// this module's doc for why `ExcludedSecret` and `Unbuildable` are two different variants
/// rather than one "no card" case.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CardBuildOutcome {
    /// A row for `projection.retrieval_cards` (migration 0074) — `card.card_status` tells the
    /// caller whether every source field was present. Boxed: `RetrievalCard` (~240 bytes) is
    /// far larger than the other two zero-sized variants, and `clippy::large_enum_variant`
    /// (workspace default, `Cargo.toml`) flags the unboxed shape.
    Card(Box<RetrievalCard>),
    /// `data_class == SECRET_MATERIAL` (§18.2). No card, no `projection.retrieval_cards` row,
    /// no `PLATFORM_RETRIEVAL` index entry — count it in §22.1's `excluded_secret=N` coverage
    /// query (see module doc), never treat it as a processing failure.
    ExcludedSecret,
    /// `key_claim` and `evidence_excerpt` were *both* missing from the input — §18.4: "正文为
    /// 空" — there is no substantive content to build even a `Partial` card from. No card, no
    /// row; the caller must register this seq as a §15 processing gap (see module doc) rather
    /// than silently doing nothing.
    Unbuildable,
}

/// §18.1's sole entry point: deterministic assembly of `input` into a [`RetrievalCard`], zero
/// external calls, zero randomness, zero wall-clock reads (`effective_from` comes from `input`,
/// never from `SystemTime::now()` inside this function) — identical `(input, budget)` produces
/// byte-identical output on every call (see this module's `assembly_is_deterministic` test).
///
/// # SECRET_MATERIAL (§18.2)
///
/// Checked first, before any other field is read: `input.data_class ==
/// DataClass::SecretMaterial` always returns [`CardBuildOutcome::ExcludedSecret`], regardless
/// of how complete every other field is.
///
/// # Truncation order (§18.2)
///
/// When the assembled text exceeds `budget.max_chars`, fields are shortened in this fixed
/// order — `evidence_excerpt` first (down to empty), then `entities` (dropped one at a time
/// from the end), then `key_claim` (down to empty) — and `title` is never touched. Budget
/// truncation does not affect [`CardStatus`]; only missing-source-field placeholder
/// substitution does (§18.4 ties `card_status` to "源字段缺失", a different concept from a
/// budget ceiling).
pub fn build_card(input: CardInput, budget: CardBudget) -> CardBuildOutcome {
    if input.data_class == DataClass::SecretMaterial {
        return CardBuildOutcome::ExcludedSecret;
    }

    let key_claim_missing = input
        .key_claim
        .as_deref()
        .map(str::is_empty)
        .unwrap_or(true);
    let evidence_missing = input
        .evidence_excerpt
        .as_deref()
        .map(str::is_empty)
        .unwrap_or(true);

    if key_claim_missing && evidence_missing {
        return CardBuildOutcome::Unbuildable;
    }

    let card_status = if key_claim_missing || evidence_missing {
        CardStatus::Partial
    } else {
        CardStatus::Complete
    };

    let key_claim = input
        .key_claim
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| MISSING_KEY_CLAIM_PLACEHOLDER.to_string());
    let evidence_excerpt = input
        .evidence_excerpt
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| MISSING_EVIDENCE_EXCERPT_PLACEHOLDER.to_string());

    // §18.1 replay: canonicalize before assembly so a caller populating `entities` from a
    // HashSet/HashMap iteration still produces byte-identical `card_text` for the same
    // `source_hash` (see `CardInput::entities` doc).
    let mut entities = input.entities;
    entities.sort_unstable();
    entities.dedup();

    let (key_claim, entities, evidence_excerpt) = fit_budget(
        &input.title,
        key_claim,
        entities,
        evidence_excerpt,
        budget.max_chars,
        key_claim_missing,
        evidence_missing,
    );

    let card_text = assemble(&input.title, &key_claim, &entities, &evidence_excerpt);
    // §18.3 verbatim: the write path's sole pre-write assertion. `title` is `assemble`'s first
    // segment and is never truncated (see doc above), so this can only ever fail if a future
    // edit to `assemble` stops putting `title` first — exactly the class of regression this
    // line exists to catch before it reaches `projection.retrieval_cards`.
    debug_assert!(
        card_text.contains(&input.title),
        "§18.3: card_text must contain title"
    );

    CardBuildOutcome::Card(Box::new(RetrievalCard {
        schema_version: RetrievalCard::SCHEMA_VERSION,
        card_builder_version: CARD_BUILDER_VERSION,
        card_template_hash: CARD_TEMPLATE_HASH,
        memory_id: input.memory_id,
        memory_type: input.memory_type,
        data_class: input.data_class,
        egress_disposition: input.egress_disposition,
        workspace_id: input.workspace_id,
        topic: input.topic,
        effective_from: input.effective_from,
        title: input.title,
        key_claim,
        entities,
        evidence_excerpt,
        card_status,
        card_text,
    }))
}

/// The template: `title` first (§18.3 — must always be present in the indexed text), then
/// `key_claim`, then `entities` (comma-joined), then `evidence_excerpt`, newline-separated.
/// [`CARD_TEMPLATE_HASH`] fingerprints exactly this shape via [`template_fingerprint`], which
/// calls this same function over a fixed golden input — so an edit to this function's literal
/// layout that isn't matched by a re-pasted [`CARD_TEMPLATE_HASH`] now fails both this module's
/// own test and `xtask card-version` (previously neither observed this function's body at all).
fn assemble(title: &str, key_claim: &str, entities: &[String], evidence_excerpt: &str) -> String {
    format!(
        "{title}\n{key_claim}\n{}\n{evidence_excerpt}",
        entities.join(", ")
    )
}

/// §18.2's fixed truncation order applied until `assemble(title, ..)`'s char count is within
/// `max_chars`, or every truncatable field has been emptied — whichever comes first (`title`
/// alone may still exceed `max_chars`; that is an accepted, documented outcome, not a bug —
/// see [`build_card`]'s doc). `key_claim_is_placeholder`/`evidence_excerpt_is_placeholder` (set
/// by [`build_card`] from the same missing-field check that chose [`CardStatus`]) make a
/// placeholder field drop *whole* the moment it doesn't fit, rather than being popped down to
/// a meaningless fragment like `"(no evidence excerpt reco"` — §18.4's placeholders are meant
/// to be fixed literals, either fully present or fully absent from `card_text`.
// ponytail: pops one `char` at a time and recomputes the full assembled length on every
// iteration — O(budget × field length) on card-sized text (hundreds of chars), not a binary
// search. Upgrade to a byte-offset search only if `CardBudget::max_chars` ever grows into a
// regime where that becomes measurable.
fn fit_budget(
    title: &str,
    mut key_claim: String,
    mut entities: Vec<String>,
    mut evidence_excerpt: String,
    max_chars: usize,
    key_claim_is_placeholder: bool,
    evidence_excerpt_is_placeholder: bool,
) -> (String, Vec<String>, String) {
    let over_budget = |kc: &str, es: &[String], ee: &str| -> bool {
        assemble(title, kc, es, ee).chars().count() > max_chars
    };

    if evidence_excerpt_is_placeholder {
        if over_budget(&key_claim, &entities, &evidence_excerpt) {
            evidence_excerpt.clear();
        }
    } else {
        while over_budget(&key_claim, &entities, &evidence_excerpt) && !evidence_excerpt.is_empty()
        {
            evidence_excerpt.pop();
        }
    }
    while over_budget(&key_claim, &entities, &evidence_excerpt) && !entities.is_empty() {
        entities.pop();
    }
    if key_claim_is_placeholder {
        if over_budget(&key_claim, &entities, &evidence_excerpt) {
            key_claim.clear();
        }
    } else {
        while over_budget(&key_claim, &entities, &evidence_excerpt) && !key_claim.is_empty() {
            key_claim.pop();
        }
    }

    (key_claim, entities, evidence_excerpt)
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    /// Fixed, not [`MemoryId::new`]/[`Uuid::now_v7`] — the determinism tests below need two
    /// separately-constructed-but-content-equal inputs, which a fresh-random id would defeat.
    fn base_input() -> CardInput {
        CardInput {
            memory_id: MemoryId(Uuid::from_u128(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef)),
            memory_type: MemoryType::Fact,
            data_class: DataClass::Private,
            egress_disposition: EgressDisposition::Allowed,
            workspace_id: Some(WorkspaceId(Uuid::from_u128(
                0xfedc_ba98_7654_3210_fedc_ba98_7654_3210,
            ))),
            topic: Some("onboarding".to_string()),
            effective_from: SystemTime::UNIX_EPOCH,
            title: "Users must verify email before first login".to_string(),
            key_claim: Some("Email verification blocks first login".to_string()),
            entities: vec!["email_verification".to_string(), "login_flow".to_string()],
            evidence_excerpt: Some("2026-08-01: added a hard gate in AuthService".to_string()),
        }
    }

    // -- §18.2 SECRET_MATERIAL exclusion ---------------------------------------------------

    #[test]
    fn secret_material_never_builds_a_card() {
        let mut input = base_input();
        input.data_class = DataClass::SecretMaterial;
        let outcome = build_card(input, CardBudget::default());
        assert_eq!(outcome, CardBuildOutcome::ExcludedSecret);
    }

    #[test]
    fn secret_material_exclusion_holds_even_with_every_other_field_present() {
        // Not just an empty-input edge case — a fully-populated SECRET_MATERIAL input is
        // still excluded, proving the check runs before (and independent of) the missing-
        // field/placeholder logic below it.
        let mut input = base_input();
        input.data_class = DataClass::SecretMaterial;
        input.entities = vec!["a".into(), "b".into(), "c".into()];
        assert_eq!(
            build_card(input, CardBudget::default()),
            CardBuildOutcome::ExcludedSecret
        );
    }

    // -- §18.4 unbuildable / gap ------------------------------------------------------------

    #[test]
    fn empty_body_is_unbuildable_not_a_silent_default_card() {
        let mut input = base_input();
        input.key_claim = None;
        input.evidence_excerpt = None;
        assert_eq!(
            build_card(input, CardBudget::default()),
            CardBuildOutcome::Unbuildable
        );
    }

    #[test]
    fn empty_string_fields_count_as_missing_same_as_none() {
        let mut input = base_input();
        input.key_claim = Some(String::new());
        input.evidence_excerpt = Some(String::new());
        assert_eq!(
            build_card(input, CardBudget::default()),
            CardBuildOutcome::Unbuildable
        );
    }

    #[test]
    fn one_present_field_is_enough_to_avoid_unbuildable() {
        let mut input = base_input();
        input.evidence_excerpt = None;
        let outcome = build_card(input, CardBudget::default());
        assert!(matches!(outcome, CardBuildOutcome::Card(_)));
    }

    // -- §18.4 card_status: missing-field driven, not budget driven -------------------------

    #[test]
    fn complete_when_every_field_present() {
        let outcome = build_card(base_input(), CardBudget::default());
        let CardBuildOutcome::Card(card) = outcome else {
            panic!("expected a card")
        };
        assert_eq!(card.card_status, CardStatus::Complete);
        assert_eq!(card.key_claim, "Email verification blocks first login");
    }

    #[test]
    fn partial_when_key_claim_missing_uses_placeholder() {
        let mut input = base_input();
        input.key_claim = None;
        let CardBuildOutcome::Card(card) = build_card(input, CardBudget::default()) else {
            panic!("expected a card")
        };
        assert_eq!(card.card_status, CardStatus::Partial);
        assert_eq!(card.key_claim, MISSING_KEY_CLAIM_PLACEHOLDER);
    }

    #[test]
    fn partial_when_evidence_excerpt_missing_uses_placeholder() {
        let mut input = base_input();
        input.evidence_excerpt = None;
        let CardBuildOutcome::Card(card) = build_card(input, CardBudget::default()) else {
            panic!("expected a card")
        };
        assert_eq!(card.card_status, CardStatus::Partial);
        assert_eq!(card.evidence_excerpt, MISSING_EVIDENCE_EXCERPT_PLACEHOLDER);
    }

    #[test]
    fn empty_entities_is_not_a_partial_trigger() {
        let mut input = base_input();
        input.entities = vec![];
        let CardBuildOutcome::Card(card) = build_card(input, CardBudget::default()) else {
            panic!("expected a card")
        };
        assert_eq!(card.card_status, CardStatus::Complete);
        assert!(card.entities.is_empty());
    }

    // -- §18.2 truncation order: evidence_excerpt -> entities -> key_claim, title untouched -

    #[test]
    fn title_is_never_truncated_even_when_it_alone_exceeds_budget() {
        let mut input = base_input();
        input.title = "x".repeat(1000);
        let budget = CardBudget { max_chars: 50 };
        let CardBuildOutcome::Card(card) = build_card(input.clone(), budget) else {
            panic!("expected a card")
        };
        assert_eq!(card.title, input.title, "title must survive untouched");
        assert!(card.card_text.contains(&card.title));
    }

    #[test]
    fn evidence_excerpt_is_shortened_before_entities_or_key_claim() {
        let mut input = base_input();
        input.title = "T".to_string();
        input.key_claim = Some("KEEP-KEY-CLAIM".to_string());
        input.entities = vec!["KEEP-ENTITY".to_string()];
        input.evidence_excerpt = Some("X".repeat(200));
        // Budget large enough for title+key_claim+entities+separators but not the full
        // evidence_excerpt.
        let budget = CardBudget { max_chars: 40 };
        let CardBuildOutcome::Card(card) = build_card(input, budget) else {
            panic!("expected a card")
        };
        assert_eq!(
            card.key_claim, "KEEP-KEY-CLAIM",
            "key_claim must survive intact"
        );
        assert_eq!(
            card.entities,
            vec!["KEEP-ENTITY".to_string()],
            "entities must survive intact"
        );
        assert!(
            card.evidence_excerpt.len() < 200,
            "evidence_excerpt must have been shortened: {:?}",
            card.evidence_excerpt
        );
    }

    #[test]
    fn entities_are_dropped_before_key_claim_once_evidence_excerpt_is_empty() {
        let mut input = base_input();
        input.title = "T".to_string();
        input.key_claim = Some("KEEP-KEY-CLAIM".to_string());
        input.entities = vec!["a".repeat(50), "b".repeat(50), "c".repeat(50)];
        input.evidence_excerpt = Some("also long enough to need trimming ".repeat(5));
        // Tight enough that evidence_excerpt empties out and entities must also shrink, but
        // still fits key_claim in full.
        let budget = CardBudget { max_chars: 30 };
        let CardBuildOutcome::Card(card) = build_card(input, budget) else {
            panic!("expected a card")
        };
        assert_eq!(
            card.key_claim, "KEEP-KEY-CLAIM",
            "key_claim is the last resort, must survive"
        );
        assert!(
            card.evidence_excerpt.is_empty(),
            "evidence_excerpt must be emptied first"
        );
        assert!(card.entities.len() < 3, "entities must have been dropped");
    }

    #[test]
    fn key_claim_is_truncated_only_as_last_resort() {
        let mut input = base_input();
        input.title = "T".to_string();
        input.key_claim = Some("K".repeat(200));
        input.entities = vec!["e".repeat(50)];
        input.evidence_excerpt = Some("v".repeat(200));
        let budget = CardBudget { max_chars: 20 };
        let CardBuildOutcome::Card(card) = build_card(input, budget) else {
            panic!("expected a card")
        };
        assert!(card.evidence_excerpt.is_empty());
        assert!(card.entities.is_empty());
        assert!(
            card.key_claim.len() < 200,
            "key_claim must also have been shortened"
        );
    }

    // -- §18.4 placeholders are fixed literals: dropped whole, never char-chopped -----------

    #[test]
    fn missing_evidence_excerpt_placeholder_is_dropped_whole_not_chopped() {
        let mut input = base_input();
        input.title = "T".to_string();
        input.key_claim = Some("K".to_string());
        input.entities = vec![];
        input.evidence_excerpt = None; // -> MISSING_EVIDENCE_EXCERPT_PLACEHOLDER (30 chars)
        // Big enough for "T\nK\n\n" (5 chars) but far too small for the 30-char placeholder —
        // the old char-popping behavior would leave a fragment like "(no evidence ex".
        let budget = CardBudget { max_chars: 20 };
        let CardBuildOutcome::Card(card) = build_card(input, budget) else {
            panic!("expected a card")
        };
        assert_eq!(
            card.evidence_excerpt, "",
            "placeholder must be dropped whole, not chopped into a fragment"
        );
    }

    #[test]
    fn missing_key_claim_placeholder_is_dropped_whole_not_chopped() {
        let mut input = base_input();
        input.title = "T".to_string();
        input.key_claim = None; // -> MISSING_KEY_CLAIM_PLACEHOLDER (24 chars)
        input.entities = vec![];
        input.evidence_excerpt = Some("EEEEE".to_string()); // real, short, gets popped first
        let budget = CardBudget { max_chars: 10 };
        let CardBuildOutcome::Card(card) = build_card(input, budget) else {
            panic!("expected a card")
        };
        assert_eq!(
            card.key_claim, "",
            "placeholder must be dropped whole, not chopped into a fragment"
        );
    }

    // -- §18.1 replay: entities canonicalized before assembly -------------------------------

    #[test]
    fn entities_are_sorted_and_deduped_regardless_of_input_order() {
        let mut input_a = base_input();
        input_a.entities = vec![
            "zebra".to_string(),
            "apple".to_string(),
            "apple".to_string(),
        ];
        let mut input_b = base_input();
        input_b.entities = vec!["apple".to_string(), "zebra".to_string()];

        let CardBuildOutcome::Card(card_a) = build_card(input_a, CardBudget::default()) else {
            panic!("expected a card")
        };
        let CardBuildOutcome::Card(card_b) = build_card(input_b, CardBudget::default()) else {
            panic!("expected a card")
        };
        assert_eq!(
            card_a.entities,
            vec!["apple".to_string(), "zebra".to_string()]
        );
        assert_eq!(
            card_a.card_text, card_b.card_text,
            "differently-ordered-with-duplicates input must still assemble byte-identically"
        );
    }

    // -- §18.3 title front-loading -----------------------------------------------------------

    #[test]
    fn card_text_always_contains_title_verbatim() {
        let outcome = build_card(base_input(), CardBudget::default());
        let CardBuildOutcome::Card(card) = outcome else {
            panic!("expected a card")
        };
        assert!(card.card_text.contains(&card.title));
        assert!(card.card_text.starts_with(&card.title));
    }

    // -- determinism --------------------------------------------------------------------------

    #[test]
    fn assembly_is_deterministic_byte_for_byte() {
        let input_a = base_input();
        let input_b = base_input();
        // Distinct-but-equal inputs (not the same owned value moved twice) — proves this is
        // input-content determinism, not just "moving the same struct gives the same result".
        let CardBuildOutcome::Card(card_a) = build_card(input_a, CardBudget::default()) else {
            panic!("expected a card")
        };
        let CardBuildOutcome::Card(card_b) = build_card(input_b, CardBudget::default()) else {
            panic!("expected a card")
        };
        assert_eq!(card_a.card_text.as_bytes(), card_b.card_text.as_bytes());
        assert_eq!(card_a, card_b);
    }

    #[test]
    fn assembly_is_deterministic_across_many_repeated_calls() {
        let first = {
            let CardBuildOutcome::Card(c) = build_card(base_input(), CardBudget::default()) else {
                panic!("expected a card")
            };
            c.card_text
        };
        for _ in 0..25 {
            let CardBuildOutcome::Card(c) = build_card(base_input(), CardBudget::default()) else {
                panic!("expected a card")
            };
            assert_eq!(c.card_text, first);
        }
    }

    // -- wire forms -----------------------------------------------------------------------

    #[test]
    fn egress_disposition_wire_forms_are_the_three_frozen_literals() {
        assert_eq!(EgressDisposition::Allowed.as_str(), "ALLOWED");
        assert_eq!(EgressDisposition::PolicyGated.as_str(), "POLICY_GATED");
        assert_eq!(EgressDisposition::Forbidden.as_str(), "FORBIDDEN");
    }

    #[test]
    fn card_status_wire_forms_are_the_two_persistable_literals() {
        assert_eq!(CardStatus::Complete.as_str(), "complete");
        assert_eq!(CardStatus::Partial.as_str(), "partial");
    }

    #[test]
    fn memory_type_wire_covers_all_twelve_variants_distinctly() {
        let all = [
            MemoryType::Fact,
            MemoryType::Preference,
            MemoryType::Decision,
            MemoryType::Rejection,
            MemoryType::State,
            MemoryType::Issue,
            MemoryType::Lesson,
            MemoryType::Constraint,
            MemoryType::Procedure,
            MemoryType::Outcome,
            MemoryType::Reference,
            MemoryType::Note,
        ];
        let wires: Vec<&str> = all.iter().copied().map(memory_type_wire).collect();
        let mut sorted = wires.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(
            sorted.len(),
            12,
            "all 12 wire forms must be distinct: {wires:?}"
        );
    }

    #[test]
    fn card_builder_version_and_template_hash_are_stable_literals() {
        // Pins the exact values `xtask card-version` (T5.8) scans for — a drift here without
        // a matching bump on the other side of that gate is exactly the fault it exists to
        // catch; this test only pins that *this* file's own pair is self-consistent.
        assert_eq!(CARD_BUILDER_VERSION, "v1");
        assert!(!CARD_TEMPLATE_HASH.is_empty());
    }

    /// §18.4 / T5.8 review (blocker): `CARD_TEMPLATE_HASH` must be derived from `assemble`'s
    /// real output, not a free-floating hand-written literal. Fault injection this test now
    /// catches (previously silent): change `assemble`'s separator from `\n` to anything else
    /// — `template_fingerprint()` immediately returns a different digest and this assertion
    /// goes red, without needing a second producer anywhere else in the workspace.
    #[test]
    fn card_template_hash_matches_computed_fingerprint_of_assemble() {
        assert_eq!(CARD_TEMPLATE_HASH, template_fingerprint());
    }
}
