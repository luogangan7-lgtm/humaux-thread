# ADR-0056 — Private retrieval egress seal = secret/credential classification only; one query seal per recall; pinned-binary hash verified once

- Status: Accepted (card 30b; design pass and implementation 2026-10-01, on HEAD `c472838` = card 30).
- Amends: ADR-0055 D-E (the `scan` stage no longer measures a gateway scan, see D-C) and ADR-0055
  §Measurements filed items 1–3 (status, see D-F). Keeps ADR-0052 D-E verbatim.
- Spec: Baseline §7.5 (entry B `seal_card()` = strictest source DataClass; entry C `seal_query()` =
  "secret/credential classification"), §7.4, §12 / §12.1.1 (contribution: "有版本的确定性隐私规则 +
  Gitleaks"), §41.2 / §41.4 (`egress_chars_total` sites = `seal_card` / `seal_query`; type gate:
  `Embedder`/`Reranker` accept only `Sealed*`), §15.4 / §16 (projection terminals), ADR-0012
  (gateway → retrieval-worker RPC carries raw query text), ADR-0052 D-E.

## Context (read on `c472838`)

1. `crates/local-secret-scan/src/lib.rs`: `scan_outcome()` (:225) runs `privacy_rejection()`
   (:430, `contains_email_like || contains_phone_like`, also over decoded JSON) before the pinned
   gitleaks run, and `seal_query()` (:371) and `seal_card()` (:393) both go through `scan()` (:244).
   `LOCAL_SECRET_RULES_VERSION = "contribution-privacy-v2"` (:413). So the §12 contribution privacy
   rules also run on the §7.5 B/C private retrieval egress seal.
2. `contains_phone_like` (:479) fires on 7 digits across ` - ( ) . +`: `2026-10-15`, a UUID tail,
   a 12-digit order number, an amount. Write side: `projection_worker.rs:871` turns the `Forbidden`
   into `(Failed, "secret_scan_rejected")`, so such a memory is never projected. Read side: the
   gateway's `seal_query` (`recall.rs:431`) refuses the query `FORBIDDEN`. ADR-0055 §Known limits
   (:416-424) recorded the UUID/date case and filed "not the contribution-privacy phone heuristic's
   `FORBIDDEN`". ADR-0052 (:373) recorded a distilled `3831887` failing a ticket on the phone rule.
3. **Provenance of the rule on the seal path.** The whole crate, rules and seal functions came in
   together with `b65c88d` (2026-09-02). That commit adopted Codex's uncommitted Phase 9 code in bulk
   and fixed five unrelated blockers. Its message and the ADRs it added (0008–0011) talk only about
   contribution scanning. No ADR and no Baseline section asks for e-mail or phone rejection on
   private retrieval egress. §7.5 C asks for "secret/credential classification". §12.1.1 is where the
   deterministic privacy rules are specified, for contribution. ADR-0052 D-E (:202) defines
   `secret_scan_rejected` as "gitleaks finding (`Forbidden`) or unsealable card (`InvalidInput`)".
   The phone rule is not in that definition. Nothing chose to put the privacy rules on the seal
   path. They got there because `seal_*` reused `scan()`.
4. **Two query seals per recall.** The gateway calls `seal_query` (`recall.rs:431`). Then the
   worker calls it again on the same text (`bins/retrieval-worker/src/rpc.rs:305`) before
   `embed_queries`. The worker must seal: the §41.4 type gate makes `Embedder::embed_queries` accept
   only `SealedRetrievalQuery`, whose fields are private and minted only by `seal_query`. An opaque
   sealed value cannot cross the UDS, and per ADR-0012 raw text crosses. The gateway seal is
   documented as "defense in depth" (`recall.rs:428-430`). The UDS peer is the same entity on the
   same host, and §7.4 says a same-entity resource is not a disclosure. It is not an egress point.
5. **`egress_chars_total` is not emitted anywhere today.** No `EGRESS_CHARS_TOTAL` in any `src/`.
   The only occurrences are the §80.2 fixture in `xtask/src/metrics_registry.rs:1023-1031`.
   `metrics-registry` scores the family `NotApplicable` (no C, no W, Phase 9 < 14). So there is no
   double count today. There is no count at all. The runtime fact is that the recall path sealed
   the query twice.
6. **Cost.** Each `scan_outcome` calls `verify_executable` twice (:236, :238). Each call is an
   `fs::read` plus a SHA-256 of the 21.3 MB binary, ≈ 60 ms. They run around one `gitleaks stdin`
   spawn of ≈ 240 ms. Card 30 release measurement, n = 351: gateway `scan` p50 247.5 ms; `embed` p50
   604.2 ms, of which the provider takes 245.5 ms. Recall p50 934.6 ms.
7. **Where the scanner identity goes.** `attestation_fingerprint()` (:178) folds
   `privacy_rules_version` + `privacy_rules_digest` (= SHA-256 of `lib.rs` itself, :275) + gitleaks
   version + binary SHA. It is exposed only as `SealedRetrievalQuery::classifier_revision()`, and its
   only consumer is `adapters::retrieval_query_source`. There it is compared inside one batch
   (:252, every query in a batch must agree) and written to
   `private.retrieval_query_sources.classifier_revision` (0115, metadata-only,
   `expires_at = now + 300 s`). No cache key, profile fingerprint (`ProfileFingerprint` comes from the
   retrieval profile, not the scanner), projection family fingerprint (§16.2) or Qdrant payload
   carries it. `SealedRetrievalCard`'s receipt is never persisted. Contribution receipts (`receipt_json`,
   `contribution_entry_repo.rs:469/591/891`, `public_repo.rs:267`) come only from `scan_outcome`.
   They are stored once and later compared stored-to-stored (0131:1193), never re-derived.
   `privacy_rules_digest` already changes on every edit of `lib.rs` (card 26 did this, and so will
   this card), so the stored rows already tolerate identity churn.

## Decisions

### D-A — Seal path = size/DataClass checks + pinned gitleaks only; contribution path unchanged

`seal_query` / `seal_card` stop calling `scan()` and call a private `scan_secrets_only(bytes)` in
the same file. It does four things:
- It keeps the existing empty / `max_payload_bytes` check (`InvalidInput`).
- It runs `verify_executable` → `run_gitleaks` → `verify_executable` (D-D).
- On a clean exit it returns a receipt stamped with the seal rule set (D-B).
- On a finding it returns `Err(Forbidden)`, the same code today's seal path returns.

It does not run `privacy_rejection` and does not JSON-decode the input. gitleaks keeps
`--max-decode-depth=5`, so an encoded secret is still decoded and caught (see
`contribution_scan::real_gitleaks_decodes_base64_secret_before_matching`).

The seal-specific checks stay as they are:
- `seal_query`: `chars ≤ RETRIEVAL_QUERY_MAX_CHARS`, `bytes ≤ SEALED_RETRIEVAL_MAX_BYTES`.
- `seal_card`: `DataClass::SecretMaterial ⇒ InvalidInput` (§7.5 B / §18.2, "SECRET_MATERIAL never
  leaves"), `bytes ≤ SEALED_RETRIEVAL_MAX_BYTES`.

Failure stays fail-closed. A spawn failure, a timeout, a non-{0, finding} exit, or a binary mismatch
is `DependencyUnavailable`, never a pass.

The contribution path keeps its behaviour, its signatures and its tests: `scan()`,
`scan_outcome()`, `privacy_rejection`, `LOCAL_SECRET_RULES_VERSION`, `receipt_json`,
`ContributionScanner`. Its only change is the cheaper executable check (D-D). In code, the
gitleaks run shared by both paths is factored into one private `fn run_checked(&self, bytes) ->
Result<ScanExit, ErrorCode>`. `scan_outcome` = privacy check, then `run_checked`, then
`outcome_for_exit`, exactly as today. `outcome_for_exit(bytes, exit)` keeps its signature, so the
existing unit tests compile unmodified.

No new trait, no config knob, no public rule-set parameter. Two private functions and one const.

### D-B — The seal receipt says which rule set ran

`receipt()` takes the rules version as a parameter. The seal path stamps
`RETRIEVAL_SEAL_RULES_VERSION = "retrieval-seal-secrets-v1"`, and the contribution path keeps
`"contribution-privacy-v2"`. The accessor keeps its name (`privacy_rules_version()`). Its doc changes
to "version of the deterministic rule set this scan applied; `retrieval-seal-secrets-v1` = none
beyond gitleaks". A seal receipt can never carry `rejection_stage = DETERMINISTIC_PRIVACY`, because
the seal path returns no `Reject` receipt at all, only `Err(Forbidden)`.

Because the fingerprint folds the version, `classifier_revision` on the seal path now differs from
any contribution identity. So a `retrieval_query_sources` row says which rule set sealed that query.
The receipt JSON shape (`contribution-scan-receipt-v1`) is untouched because seal receipts are
never serialized.

**Transition (fingerprint trace, Context 7).** Two identities change: `classifier_revision` for
sealed queries (new version string, and the new `lib.rs` digest) and `privacy_rules_digest` for new
contribution receipts (the `lib.rs` digest; any edit changes it). Neither is compared across time:
- `retrieval_query_sources` compares within one batch minted by one scanner, and its rows expire
  after 300 s.
- Contribution rows are compared stored-to-stored.
- Nothing keys a cache, a profile, a Qdrant payload or a §16.2 family fingerprint on either value.

No migration, no re-fingerprinting, no dual-write. A worker restarted mid-batch cannot mix
identities because a batch is sealed by one process.

### D-C — One query seal per recall: the worker's, at the real egress boundary

Keep `bins/retrieval-worker/src/rpc.rs:305`. It is the process that holds the provider credential
and calls `Embedder::embed_queries`, the only place a sealed query can exist (§41.4 type gate), and
the egress boundary §41.2 names ("封口是唯一出境口").

Delete the gateway seal:
- `recall.rs:431` goes, along with the `scanner` field and the `SemanticRecallRuntime::new`
  parameter. `embedding_input.query` becomes `trusted_query.text()`.
- `bootstrap.rs` stops building a scanner and drops the three `HUMAUX_GATEWAY_GITLEAKS_*` registry
  entries and their `required()` reads. An unused key would otherwise have to stay required.
- `humaux-local-secret-scan` moves from gateway `[dependencies]` to `[dev-dependencies]` (tests
  still build the in-process worker).

After this, gateway production code cannot mint a `SealedRetrievalQuery`. Re-adding a gateway seal
means re-adding a Cargo dependency and a constructor parameter, and every `SemanticRecallRuntime::new`
call site would fail to compile.

**FORBIDDEN still reaches the caller.** Today every seal error in the worker becomes
`"SCAN_REJECTED"`, including a broken scanner. The worker now splits them:
- `Err(Forbidden)` ⇒ `"SCAN_REJECTED"`.
- `Err(InvalidInput)` ⇒ `"INVALID_QUERY"`.
- Anything else ⇒ `"SCANNER_UNAVAILABLE"`.

The gateway `match` in `recall.rs` gains one arm:
`Unavailable { reason } if reason == "SCAN_REJECTED" ⇒ Err(ErrorCode::Forbidden)` (operator line
`query_scan_rejected`, no query text). A genuine secret-bearing query therefore still ends as §52
`FORBIDDEN`, and a scanner outage stays `DEPENDENCY_UNAVAILABLE`. The idempotent replay path
(`envelope_from_stored`) returns the stored `response_failure_code`, so a retried call gets the same
`FORBIDDEN`. The query is sealed before `embed_queries`, so a refused query reserves no disclosure,
writes no `retrieval_query_sources` row and spends no provider budget.

**Stage timing.** The gateway `scan` lap stays and now spans only `retrieval.trusted_query()`, so it
reads ≈ 0 ms. The worker's seal is inside `embed`, as before. The key is kept so that ADR-0055 D-E's
eight-key partition and `xtask/src/soak.rs::RECALL_STAGES` (which drops a recall missing any
key) stay valid without touching xtask. `// ponytail: scan key kept at ~0 ms after ADR-0056; drop it
with soak.rs RECALL_STAGES when a card owns xtask.`

**`egress_chars_total`.** It has zero emits today (Context 5), so it cannot be double-counted. It
stays unimplemented in this card; see Rejected. "One seal per recall" is pinned instead by:
1. The gateway has no production path to a seal (the Cargo and constructor fence above).
2. The worker has exactly one `seal_query` call on the RPC path (gate `worker_single_query_seal`,
   count `== 1`).
3. The real-infra FORBIDDEN fixture (Tests).
4. The rehearsal `recall.stage.scan` p50 ≈ 0.

When G80-6 turns strict in Phase 14 and a card wires the family with its witness, the two §7.5 sites
already exist inside `seal_card` / `seal_query`, so every seal is counted exactly once by
construction.

### D-D — Hash the pinned binary once; re-verify by stat tuple

`LocalSecretScanner` gains `verified: Mutex<Option<ExeStamp>>`, where
`ExeStamp = (dev, ino, len, mtime_s, mtime_ns, ctime_s, ctime_ns)` from `std::os::unix::fs::MetadataExt`.

`new()` does the following:
1. Takes a stamp (`fs::metadata`, which follows symlinks).
2. Reads and hashes the file. A mismatch is `Conflict`, as today.
3. Takes a second stamp. If it differs from the first, the result is `DependencyUnavailable`. The
   file changed under the hash, so no stamp is cached.
4. Runs `version`.
5. Stores the stamp.

`verify_executable(&self)` takes a stamp. If it equals the cached stamp it returns `Ok`, costing
microseconds. Otherwise it re-reads and re-hashes:
- On a mismatch it returns `DependencyUnavailable` and keeps the old stamp, so every later scan
  re-hashes and fails again. There is no "remember bad".
- On a match it re-stamps, compares with the pre-hash stamp, and caches the new stamp.

It still runs once before and once after each spawn. The two full hashes per scan collapse into two
`stat` calls.

A swap by content (in place, same length, `mtime` restored with `File::set_modified`) changes
`ctime`. A swap by path (`rename` or a new symlink target) changes `ino`, and possibly `dev`. Both
force a re-hash, which fails. `#[cfg(not(unix))]` stamps are `None`, so every call re-hashes as
today.

**Tamper window, honestly compared.**
- Today and after this change alike, a swap → exec → swap-back inside one spawn is invisible (a
  TOCTOU between the check and `exec`). Neither design hashes the bytes the kernel actually maps.
- Today detects an in-place content change that leaves the stat tuple intact, and this design does
  not. Such a change needs one of:
  (a) root: set the clock back or write the raw block device;
  (b) a write through a shared `mmap`, whose `mtime`/`ctime` update POSIX lets the OS defer until
      `msync`/`munmap`;
  (c) a filesystem with coarse timestamps (HFS+ 1 s, FAT 2 s) where a same-length rewrite lands in
      the same second as the last verified change.
- APFS and ext4/xfs keep nanosecond `ctime`, which userspace cannot set.

An attacker who can write the pinned binary already runs as the service uid or root and can read the
provider credential, so this check detects tampering but does not prevent it. The runbook states the
real control: the binary is root-owned, mode 0555, on a read-only path.

### D-E — Already-failed rows: none needed before first deploy

There is no FAILED→ISSUED reissue edge. §15.2 transitions (0011:422, 0167:119) have no
`FAILED → ISSUED`, and card 37 owns DONE→ISSUED. The only operator act on a FAILED ticket is
`cargo xtask projection-serve --retire-failed secret_scan_rejected` (0167 `RETIRED_FAILED`, audited).
It unpins the §15.4 prefix but leaves the record permanently unindexed, so it is not a reissue.

Production has no data (fresh deploy; cutover import is card 51), so nothing needs reissuing. On a
dev DB the existing `secret_scan_rejected` rows (2 of 5 339) stay FAILED. Retire them with the
command above if a stream prefix is pinned, or rebuild the dev DB. No mechanism is built here.

### D-F — ADR-0055 filed items

1. Hash once plus a stat tuple: **done** (D-D; `ctime` added to the filed tuple).
2. Drop the duplicate gateway seal: **done** (D-C). The ADR-0055 wording "keep the deterministic
   privacy rules" is superseded by D-A for the reason in Context 3.
3. `spawn_blocking`: **done in the worker.** The gateway no longer scans, so the blocking scan sits
   only in the worker's `async fn embed`. There the request is owned
   (`build_request(intent, &profile)` → `request`), so
   `tokio::task::spawn_blocking(move || { request.trusted_query().ok_or(InvalidInput)?; scanner.seal_query(..) })`
   with `scanner: Arc<LocalSecretScanner>` needed no new owned query type (`RetrievalRequest` is
   `Send + 'static`). A `JoinError` maps to `SCANNER_UNAVAILABLE`.

## Known limits / upgrade signals

- **The contribution path still rejects dates, UUID tails, order numbers and amounts**
  (`contains_phone_like`, 7 digits across separators). That is fail-closed by design for public
  release (§12). It is kept unchanged in this card and is a known false-positive source for
  contribution candidates. Upgrade signal: contribution `REJECTED_SAFETY` rate on real traffic.
- The resident gitleaks process (≈ 240 ms spawn per seal) is out of scope. Filed with the
  re-measured `embed` stage: 435.5 ms p50, of which the provider is 176.0 ms, so ≈ 260 ms is the
  worker's one spawn plus the UDS RPC and ledger reserve/settle (§Measurements).
- The retrieval RPC registration row (`ops.retrieval_embedding_rpc_calls.query_sha256`, 0141) now
  exists for a query the worker later refuses. Before, the gateway refused it before registration.
  It is an unsalted SHA-256 of a gitleaks-shaped (high-entropy) string, with no text, on a
  role-restricted table.
- `seal_card` rejection is still per Evidence: one secret-bearing memory fails the whole ticket
  (ADR-0052 D-E, unchanged).

## Rejected

- *Relax `contains_phone_like` for everyone.* It weakens §12 public release and is out of scope.
- *A query-specific privacy rule.* §7.5 C specifies secret/credential classification only. Inventing
  a PII policy for private egress is a DataClass policy change (out of scope).
- *Keep the gateway seal, drop the worker's.* It is impossible: the §41.4 type gate needs a
  `SealedRetrievalQuery` in the process that calls `Embedder`, and it cannot be serialized across the
  UDS.
- *A seal-path rule-set enum or config knob.* That is one caller per path. A private function plus a
  const suffices (ponytail).
- *Implement `egress_chars_total` now.* The `domain` label has no frozen value set, a half-landed
  family (C without W) turns `metrics-registry` D2 red, and the witness needs the pinned gitleaks in
  the probe package. It belongs to G80-6 (Phase 14). The structural fence proves "once" without it.
- *Remove the `scan` stage key.* That breaks xtask soak parsing (outside the card's files) for no
  information gain. It is kept at ≈ 0 with a ponytail note.
- *Periodic full re-hash on a timer.* It closes only the (a)–(c) window above, at unbounded cost.
  Deploy-time read-only ownership is the control.

## Files touched beyond the card's allowed list

Forced by D-C, not chosen: `bins/gateway/src/bootstrap.rs` (scanner and the three
`HUMAUX_GATEWAY_GITLEAKS_*` keys removed; otherwise they stay required config for nothing),
`bins/gateway/Cargo.toml` (the scanner crate moves to `[dev-dependencies]`),
`xtask/src/e2e_onboard.rs` (it set the three keys, which the gateway now refuses as unknown),
`crates/retrieval-provider/src/contract.rs` (one doc sentence), `crates/domain/src/error.rs`
(`Called-by` header gains `retrieval-worker::rpc`, which `dep-map --check` computes from the new
`ErrorCode` match) and `bins/gateway/tests/read_decoupling.rs` (its now-unused scanner helper).

## Tests

Each named with the fault that turns it red (all run red → green on 2026-10-01):

| Test | Fault ⇒ red |
|---|---|
| `local-secret-scan` unit `seal_path_never_runs_contribution_privacy_rules` (fixture-less: the five shapes are `Forbidden` on `scan()` and `DependencyUnavailable`, never `Forbidden`, on both seals) | `privacy_rejection` added to `scan_secrets_only`; gitleaks skipped on the seal (`Ok`) |
| unit `seal_card_refuses_secret_material_before_any_scan`, `seal_receipt_names_the_seal_rule_set` | — (pins §7.5 B and D-B) |
| `local-secret-scan/tests/seal_rules.rs` lane(b) `seal_query_and_seal_card_accept_date_email_phone_long_number_and_uuid` (classifier = the seal identity recomputed from the public receipt fields, ≠ the contribution identity) | privacy rules on the seal |
| `seal_rules.rs` lane(b) `seal_query_and_seal_card_refuse_a_gitleaks_finding` (fake `ghp_` vector) | gitleaks skipped on the seal |
| `seal_rules.rs` lane(b) `scan_outcome_still_rejects_email_and_phone_as_deterministic_privacy` | — (contribution path pinned) |
| `seal_rules.rs` `swapped_binary_by_content_is_detected` (same length, mtime restored, marker proves B never ran), `swapped_binary_by_path_is_detected` (rename), `touched_binary_with_identical_bytes_still_scans` — fake shell scanners, run in the default lane | cached stamp accepted without comparing (content and path tests red) |
| `gateway/tests/query_embedding_rpc.rs` `worker_refuses_a_credential_query_as_scan_rejected_and_the_gateway_client_surfaces_it` (`SCAN_REJECTED`, replayed on retry, 0 provider calls) | worker maps `Forbidden` to `SCANNER_UNAVAILABLE`; gitleaks skipped |
| `query_embedding_rpc.rs` `worker_embeds_queries_with_date_email_phone_number_and_uuid` | privacy rules on the seal |
| `query_embedding_rpc.rs` `worker_scanner_outage_is_scanner_unavailable_not_scan_rejected` | — (pins the split) |
| `gateway/tests/mcp_gateway.rs` lane(a:request_guard) `recall_answers_identifier_bearing_queries_and_refuses_a_credential_query` (real PG + Qdrant + in-process worker + pinned gitleaks: five identifier queries → 200, own memory ranked first; `ghp_` query → HTTP 403 `FORBIDDEN`) | privacy rules on the seal (5 FORBIDDEN); gitleaks skipped (200); gateway `SCAN_REJECTED` arm removed (200 + tool error `DEPENDENCY_UNAVAILABLE`) |
| `adapters/tests/projection_worker.rs` `cards_with_date_email_phone_number_and_uuid_project_done` | privacy rules on `seal_card` (FAILED) |
| `projection_worker.rs` `a_card_with_a_real_gitleaks_finding_settles_failed_secret_scan_rejected` | gitleaks skipped (DONE) |
| gate `gateway_no_scanner_dep` (`cargo tree -p humaux-gateway -e normal --depth 1` has no scanner crate) + no scanner parameter on `SemanticRecallRuntime::new` | a gateway seal re-added (the dependency back in `[dependencies]` ⇒ gate exit 1) |
| gate `worker_single_query_seal` (`grep -c '\.seal_query(' bins/retrieval-worker/src/rpc.rs` `== 1`; review P1, stands in for the §80.2 D5 count until G80-6 wires `egress_chars_total`) | a second `seal_query` call in the worker RPC (count 2 ⇒ gate exit 1) |
| `docs/ops/rehearse.sh` step pst item 7 (live MiniMax + DashScope): five identifier memories DONE, returned by a recall whose query carries the identifier, a `ghp_` memory terminal and not projected, a `ghp_` query `FORBIDDEN`; soak `recall.stage.scan` p50 < 5 ms | — (live witness) |

Unchanged and green unmodified: `crates/adapters/tests/contribution_scan.rs`, the in-crate
`deterministic_privacy_rejection_is_typed_and_legacy_scan_is_forbidden`, private-worker `--tests`,
`fault_main`, retrieval-provider contract tests.

## Measurements

Release rehearsal, the card-30 harness and parameters (`c30b_rehearse.sh measure`,
`REHEARSE_PROFILE=release SOAK_SECS=1800 SOAK_SESSIONS=1 SOAK_THINK_MS=14000 SOAK_DRAIN=300
SOAK_CHAOS_SECS=400`), 2026-10-01 06:09–06:52, evidence `card30b_rehearsal_evidence/measure/`.
Unit ms, soak `latency[]` p50 / p95.

| Bucket | card 30 (n = 351) | card 30b (n = 368) | Δ p50 |
|---|---|---|---|
| `recall` (over the wire) | 934.6 / 2430.7 | **486.6 / 604.6** | −448.0 |
| `recall.stage_sum` | 892.9 / 2279.3 | 462.5 / 584.7 | −430.4 |
| `recall.stage.scan` | 247.5 / 577.1 | **0.0 / 0.0** | −247.5 |
| `recall.stage.embed` (worker seal + provider + ledger) | 604.2 / 1490.1 | 435.5 / 564.2 | −168.7 |
| provider query embedding (ledger ⋈ `ops.retrieval_embedding_rpc_calls`) | 245.5 / 506.8 (n = 342) | 176.0 / 326.6 (n = 362) | |
| `recall.stage.hydrate` | 15.2 / 76.6 | 17.9 / 24.8 | |
| `recall.stage.qdrant` | 4.8 / 23.1 | 5.3 / 8.6 | |
| `recall.stage.route` | 2.7 / 51.5 | 2.0 / 2.6 | |
| `recall.stage.assemble` | 2.2 / 7.5 | 2.2 / 3.4 | |
| `memory.get` | 34.5 / 271.5 | 25.5 / 34.2 | |
| `memory.enumerate` | 92.5 / 595.8 | 74.6 / 113.7 | |
| `remember` | 86.8 / 380.2 | 48.2 / 55.9 | |

Reading: the gateway seal (247.5 ms p50: two ≈ 60 ms hashes plus one gitleaks spawn) is gone
from `scan`; `embed` dropped by 168.7 ms, consistent with the worker seal's two hashes (≈ 120 ms)
disappearing and the remaining spawn running under `spawn_blocking`. The p95 collapse
(2430.7 → 604.6) is not attributed by this run; the likely cause is that no scan blocks a Tokio
worker any more, so concurrent requests no longer queue behind one (inference, not measured). Stage coverage 462.5 / 486.6 = 95 % (gate ≥ 90 %).
`recall` failed 1 of 368 (`query_embedding_unavailable reason=TRANSPORT` in a retrieval-worker chaos
restart window; the soak's 1 % bound holds). The one `query_scan_rejected` line in `gateway.log` is
the rehearsal's own credential query.

Rehearsal pst item 7 in that run: 5/5 identifier tickets DONE, `secret_scan_rejected=0`; 5/5
identifier queries returned their memory, 0 FORBIDDEN; all five distilled memories kept their
identifier (`identifier_in_memory=5`); the credential query answered `FORBIDDEN`. The credential
put's distilled memory read "contains the credential used by the release bot" — live MiniMax dropped
the token — so that card was clean and projected `DONE`; the first version of the assertion
demanded a terminal ticket and was red for that reason alone (`83 passed, 1 failed`). It now grades
"no live projected card carries the token" (0 of 165 live points in that tenant) and the terminal
verdict only when the distilled card does carry it; the deterministic card-side witness is
`projection_worker::a_card_with_a_real_gitleaks_finding_settles_failed_secret_scan_rejected`.
