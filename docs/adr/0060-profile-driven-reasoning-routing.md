# ADR-0060 — Profile-driven reasoning routing: R3 for PRIVATE_DISTILL_* and PRIVATE_CONSOLIDATE

- Status: Accepted (card 33b, 2026-10-03, on HEAD `a4990e0`). Implemented in four serial slices:
  **slice S1: database runtime pieces, locator capabilities, ledger route + one-transaction disclosure,
  processing-run profile, fair-share claim, worker-observed health definer (migration 0206)**; **slice S2: adapter
  seam (reasoners take a provider per admitted route), route-vs-instance check, request shape from the admitted
  profile, route fields in the operator lines**; **slice S3: the worker builds providers from routes (two worker
  reads, migration 0207), the process-level provider keys are removed, worker-observed health renewal,
  two-provider live gate**; **slice S4: operator doors and CLI (migration 0208), rehearsal onboarding through the
  doors with two providers in one deployed worker, docs**; **review pass 2: two applied definer bodies corrected
  forward (migration 0209)**. Decisions D-A … D-N are recorded below per slice.
- Spec: Baseline §11.1, §11.2, §11.2.2 (R3; "R4" in this ADR always means **multi-profile routing (§11.2.2 R4)**),
  §11.2.3, §11.2.4, §11.2.5, §11.5, §19.1, §67.2, §72.3; design `card_33b_design.md` with its main-line rulings
  (2026-10-02: E1–E7 approved, E2 = USER, E3 with the liveness amendment, D-F fair share) and the research
  addendum (amendments 1–6).
- Supersedes in part: ADR-0058 D-B (tenant order of the distill claim), migration 0166's "platform-paid" reading
  of the private hops.

## Context

The route schema (0128) and the admission resolver (0130) exist, and distill / consolidation carry
`binding_version` into their sealed context, but the provider a job is sent to came from one process-level
descriptor, the private ledger rows carried no route, and the ledger row and the disclosure of a private call were
reserved in two transactions. Every tenant of a deployment therefore used one LLM, and nothing in the database
proved which route, account or payer a private call used.

## Decisions (slice S1)

### D-A  The locator carries the admitted Profile@version's capabilities
`control.reasoning_profile_capabilities(uuid,bigint)` (owner SECURITY DEFINER, `search_path=pg_catalog`, filtered by
the `humaux.tenant_id` GUC, EXECUTE `role_private_worker` only) is called by the one admission wrapper
(`adapters::reasoning_route_admission`) in the resolver's transaction, and fills
`ReasoningAdmissionLocator.capabilities` (closed set; unknown value ⇒ `InvalidLocator`). Profile@version is
immutable (0128), so the second read equals a read in the resolver snapshot. Rejected: widening the 0130 resolver's
RETURNS TABLE (four SQL callers and architecture-check parse it); the catalog's capabilities (an upper bound, not
the declaration); process configuration.

### D-F  Fair-share distill claim
`ops.claim_derived_work_v2` picks the tenant with READY work holding the fewest provider slots, then the least
recently served (0206; was least recently served only, ADR-0058 D-B). Work-conserving; with N busy tenants none
holds more than ceil(4/N) of the four slots, so a hanging provider holds only its share. The four slots and the one
`PRIVATE_REASONING` arbiter stay the single §67.2 bound. Rejected: reserving the last slot (idles 25 % of capacity);
per-provider budget rows (needs a definition of "provider" and a claim that knows the route before admission — R4).

### D-I  The private ledger row equals the admitted route (ruling E2: the payer is the user's account)
The snapshot CHECK gains a private arm: the 14 route columns are all NULL (rows reserved before 0206) or all present
with `billing_responsibility = 'USER'` and no cost estimate (§11.5). The BEFORE INSERT validator requires the
present form for every new private row — no role exemption, the owner included — and requires the row to equal one
current exact admission on tenant, binding, domain, policy, profile, account, provider, model, revision, endpoint,
egress id, credential and billing ids. The recorded health ids must belong to the same identity and have admitted
the route at `admitted_at`, but are not compared with the resolver's latest: a newer attestation does not refuse
the row (§11.2.5). The adapter has one private entry point,
`model_call_ledger::reserve_private_call_with_disclosure(pool, purpose, locator, …)`; the route is a required
argument, never optional. 0166's header called the private hops platform-paid: that described one process-level
key serving every tenant; with per-route credentials the account of the admitted credential pays (§11.2.4).

### D-K  Processing runs name their Profile@version
`private.processing_runs.profile_id / profile_version` (both or neither; FK to the profile). The distill read leg
fills them from the admitted locator; earlier rows stay NULL ("route not recorded").

### D-N  One reserve transaction for a private call
The ledger row and its `USER_REASONING` disclosure (whose `model_call_id` names the row) are reserved in one
transaction. The disclosure validator's purpose list covers the three private purposes (processor id = the row's
egress id, else 23514), and a deferred constraint trigger refuses, at COMMIT, a private ledger row without its
matching disclosure. A refusal rolls both back, so no `ops.begin_call` and no provider call follow. ADR-0058 L13
(reservation and `begin_call` are two transactions) is unchanged.

### Research amendments 1 and 2 (schema half)
`control.reasoning_profiles.request_extras` (jsonb object, at most 4096 bytes, never an adapter-owned key —
`humaux_adapters::byok::ADAPTER_OWNED_REQUEST_KEYS`, pinned to the CHECK by contract tests) holds a profile's
vendor request fields. `JSON_OBJECT` joins the §11.2 capability closed set (three CHECKs + Rust
`ReasoningCapability::ALL`). The adapter behaviour (merge extras, three output channels) lands in S2/S3.

### Ruling E3  Worker-observed route health (definer half)
`control.observe_reasoning_route_health(model_call_id, credential_rejected, valid_for_seconds)` (owner SECURITY
DEFINER, EXECUTE `role_private_worker` only) derives every identity column from one finalized routed call of the
session tenant and from the observations that call was admitted under: after a SUCCEEDED call, when the admitted
validity has less than half of `valid_for_seconds` left, one HEALTHY provider row and one HEALTHY/VALID account row
(`source_kind = 'WORKER_OBSERVED'`); after a rejected credential, one account row with credential verdict
`INVALID`, so the next admission is refused at once. A call admitted under an observation that is no longer the
latest, or older than `valid_for_seconds`, appends nothing. The worker calls it from S3 with the required key
`HUMAUX_PRIVATE_WORKER_HEALTH_RENEW_SECS`.

### D-J  (boot-check definer half)
`control.reasoning_credential_accounts(uuid[])` returns `(credential_ref, processor_id, external_account_ref_hash)`
for bound references across tenants — no tenant, secret or `openbao_ref` — EXECUTE `role_private_worker` only. It
visits each tenant under that tenant's GUC (the bindings are FORCE-RLS per tenant) and restores the caller's value.

## Decisions (slice S2)

### D-B  The seam: one provider instance per admitted route
`reasoning_route_admission::ProviderFor` is a type alias over `Fn(&ReasoningAdmissionLocator) ->
Result<Arc<dyn UserReasoningProvider>, &'static str>` — no trait, no registry. The distill, consolidation and
contribution reasoners hold `&ProviderFor` instead of one provider and ask it only after the resolver admitted the
locator (D-G); the error is a static NOT_READY class (ADR-0058 D-H). Distill carries the instance in
`DistillAdmission` / `PreparedDistillCall`; contribution re-obtains it per stage from the stage's own locator.
Until S3, `humaux-private-worker`'s `main.rs` passes a TEMPORARY closure over its one boot provider that keeps the
singular deny-only checks (`EGRESS_PROCESSOR_NOT_ALLOWED` unless the route's recipient and region are the
deployment's); S3 replaces it with the per-Profile@version instances.

### D-C  The instance must equal the route
`provider_matches_admission(provider, locator)` refuses an instance whose provider id, model, revision, endpoint or
capability SET differ from the admitted route (`ROUTE_PROVIDER_MISMATCH` — a seam or cache bug) and a route whose
Profile does not declare `STRUCTURED_OUTPUT` (`PROFILE_LACKS_STRUCTURED_OUTPUT`); it runs after admission in all
three reasoners. `ContributionReasonerConfig.{allowed_egress_processor_id, region}` are deleted: the deny-only
recipient / region lists belong to the one source of instances, which can refuse a route but never select one
(§11.2.5). Rejected: dropping the instance check because production builds from the route (it is the only guard
against a seam or cache bug); keeping the lists in the reasoner config (two places checking one thing).

### D-D and research amendments 1–4 (adapter half)  The request follows the admitted Profile
The distill output channel is chosen from the admitted instance's declared capabilities: `TOOL_CALLS` → one
function, no `tool_choice`; else `JSON_OBJECT` → `response_format: {"type":"json_object"}`; else plain content
(the prompt-contract hash tags the channel, so Content and Tool hashes are unchanged and JSON_OBJECT hashes apart).
`ReasoningProviderDescriptor.request_extras` is merged into every structured body after the adapter's own fields;
an adapter-owned key is never written; the adapter no longer writes `reasoning_split` (REASONING_SPLIT selects
nothing). `<think>` blocks are stripped on both content channels and a `reasoning_content` field is never read; a
tool-call reply is read from its call whether `content` is absent or `""`; a reply without `usage` leaves every
token count unknown (NULL), never fails. Contribution and consolidation keep the content channel (their prompt
contracts are unchanged). Until S3 the boot descriptor carries no extras (no process key is added for them; S3's
route-built descriptors take them from the Profile).

### D-E  Consolidation names its route through the registration row
`RpcState.providers: Box<ProviderFor>`; the consolidation reasoner admits the registered binding, then obtains its
instance. The RPC wire is unchanged (call id only).

### D-M  Operator lines name the route
`provider_failure_line`, the distill per-job line and a new per-call RPC finish line carry
`provider=… model=… profile=<id>@<v> binding=<id>@<v>` (`ReasoningAdmissionLocator::route_fields`) — never
`endpoint_ref`, a key, or provider/user text; `route=-` when no route was admitted.

## Decisions (slice S3)

### D-B (production)  `RouteProviders`: one instance per admitted Profile@version
`bins/private-worker/src/route_providers.rs` is the one production source of provider instances (architecture-check
T31: one `RouteProviders::new(` in `main.rs`, one `ReasoningProviderDescriptor {` literal under `bins/*/src`). For an
admitted locator it checks, in order and before any DNS lookup or client build: the credential reference is in the
key map (`CREDENTIAL_NOT_MAPPED`; this closes ADR-0059 L7 for consolidation and contribution too, and the card-33
pre-check on the distill dispatcher is deleted), the route's egress recipient is listed
(`EGRESS_PROCESSOR_NOT_ALLOWED`), the endpoint's host — parsed by the §11.4 parser, `ssrf::https_host` — equals or
is a subdomain of a host that recipient may dial (`EGRESS_HOST_NOT_ALLOWED`, D-L), the region is listed
(`REGION_NOT_ALLOWED`). Then it returns the cached instance of `(tenant, profile_id, profile_version)` or builds one
through `OpenAiCompatibleProvider::with_egress_transport` from the route's provider, model, revision, endpoint,
capabilities and request extras, with the one resolver of the process; a build failure returns its own class and is
not cached. Profile@version is immutable, so the cache needs no eviction (L4); admission precedes every lookup, so a
disabled profile, a switched binding or stale health never reaches the cache.

### Research amendment 1 (worker half) and ruling E3 (c): two worker reads (migration 0207)
0206 added `control.reasoning_profiles.request_extras` but no reader the private worker may call, so a route-built
instance could not carry its profile's vendor fields. 0207 adds `control.reasoning_profile_request_extras(uuid,
bigint)` (same shape and fence as the capabilities definer); the locator carries `request_extras` and
`provider_matches_admission` compares them too. 0207 also adds `control.reasoning_route_health_state(uuid,bigint)`:
when the resolver admits no row, two booleans say whether the latest observations of the route behind that current
Binding@version are stale or valid-but-denied, so the job is NOT_READY `ROUTE_HEALTH_STALE` or `ROUTE_HEALTH_DENIED`
instead of the generic refusal (distinct from `CREDENTIAL_NOT_MAPPED` and from an unbound domain). Both are
EXECUTE `role_private_worker` only. The doors of D-H therefore take migration 0208.

### D-C (env)  The worker's environment names no provider
Removed and refused at boot when set (E7): `HUMAUX_PRIVATE_WORKER_{PROVIDER_ID, MODEL_ID, MODEL_REVISION, CHAT_URL,
CAPABILITIES}`, the singular `_EGRESS_PROCESSOR_ID` / `_REGION`, and card 33's `_KEY_ENV`. Required, explicitly empty
accepted: `HUMAUX_PRIVATE_WORKER_CREDENTIALS`, `_EGRESS_RECIPIENTS = <uuid>=<host>[|<host>…][,…]` (bare lowercase
hosts) and `_REGIONS`. `e2e-seed` prints the recipient (`<processor-id>=<endpoint host>`) and region lines and no
provider line.

### D-J (boot)  One secret serves one vendor account
At boot the worker groups its map entries by the sha256 of the key value (so two variable names holding one key are
one group), reads the vendor identity behind each reference through `control.reasoning_credential_accounts`, and
refuses to start — naming references and variable names, never a value — when a group or a single reference spans
more than one `(processor_id, external_account_ref_hash)`, or when a reference unknown to the database shares its
key. One line per entry is logged (`account_hash=<8 hex|unregistered>`). `e2e-seed --account-ref <text>` declares
`sha256(text)` as the account hash of every lane it seeds, so lanes mapped to one key name one account.

### Ruling E3 (b) (worker half)  Traffic keeps a route's health alive
`HUMAUX_PRIVATE_WORKER_HEALTH_RENEW_SECS` is required (no default). After a SUCCEEDED distill or consolidation call
the worker asks `control.observe_reasoning_route_health` for a renewal valid that long (appended only when the
admitted observation has less than half of it left); after a provider 401 it records the account's credential
INVALID, so the next admission of that route is refused `ROUTE_HEALTH_DENIED` at once. The observation is best
effort: a failure is logged with a static class and changes no settle. Contribution calls observe the same way
(review pass 2): the R4 runner renews after an answered A or B call and records INVALID on a 401, which 0131 leaves
RESERVED for reconciliation (0209 accepts that row for a rejection, CONTRIBUTION_DEIDENTIFY only). One helper,
`humaux_private_worker::observe_route_health`, is the call shape for all three paths.

## Decisions (slice S4)

### D-H  Operator doors: register, bind (the R2 projection), attest health, profile state (migration 0208)
Five owner SECURITY DEFINER functions, `search_path=pg_catalog`, EXECUTE `role_maintenance` only (PUBLIC and the
seven runtime roles revoked; rls-check `ADR-0060 reasoning route doors`), each asserting `p_tenant` equals the
installed `humaux.tenant_id` (42501 otherwise), refusing with 55000 and a reason token, rejecting a malformed request
with 22023. The 0128 triggers still run on every INSERT, so a door can only name a refusal earlier, never write a
row the schema would refuse.
1. `control.register_reasoning_profile` — catalog row (created with the declared capabilities when absent; refused
   `capabilities_exceed_catalog` when the declared set is wider than an existing row, `model_retired` when RETIRED),
   vendor account (reused on tenant, owner, processor and the sha256 of `--account-ref`), endpoint (reused only with
   equal region / tier / egress recipient, else `endpoint_identity_conflict`; `endpoint_not_https`), credential
   reference (minted, `openbao_ref = env-map:HUMAUX_PRIVATE_WORKER_CREDENTIALS`, when none is named; a named one must
   be bound to the account, `credential_not_bound`), Profile@version with `request_extras` (an adapter-owned key is
   refused `request_extras_invalid` — the door maps the 0206 CHECK's violation, so the key list stays in one place).
   An enabled identical profile answers `existing`; `--successor-of` registers the next version of a profile.
2. `control.bind_reasoning_domain` — only `PRIVATE_DISTILL_TEXT` / `PRIVATE_CONSOLIDATE` (`purpose_not_bindable`;
   CONTRIBUTION_DEIDENTIFY keeps its 0129 bootstrap), domain ACTIVE and owned by the profile's owner
   (`owner_mismatch`), profile enabled and declaring STRUCTURED_OUTPUT. One PINNED Policy (DRAFT → SHADOW → SERVING,
   owner = domain owner), exactly one Candidate, and either a new Binding@1 or the close of the current interval
   plus the successor Binding@version+1 with the same `binding_id`. A current binding already serving exactly this
   Profile@version answers `existing`. Old rows stay, so history keeps naming the route it ran under.
3. `control.attest_reasoning_route_health` — one HEALTHY provider observation (the profile's exact provider
   identity) and one HEALTHY / VALID account observation, `source_kind = OPERATOR_ATTEST`, valid for an explicit
   number of seconds (> 0, required flag, no default; ruling E3 (a)).
4. `control.set_reasoning_profile_enabled` — the one mutable profile column; the no-restart stop of one route.
5. `control.reasoning_route_status` (read-only) — one row per ACTIVE domain × bindable purpose: Binding@version,
   Profile@version, provider / model / revision, endpoint id, region, recipient, credential reference,
   capabilities, the latest provider / account / credential verdicts, `valid_until` (the earlier of the two) and a
   class `UNBOUND` / `MISSING` / `STALE` / `DENIED` / `ADMISSIBLE` (ruling E3 (c); also answers L12's "unbound
   domains" view). role_maintenance holds no SELECT on the route tables, so this is its only view of them.

`adapters::reasoning_route_onboarding` runs each door in one READ COMMITTED transaction under the tenant GUC with
its §77 SUCCESS row (risk tag `reasoning_route`); a refusal rolls back and writes its DENIED row
(`provisioning::audit_denied`, reused). Errors reuse `ProvisioningError` (no new error enum).
`humaux-maintenance reasoning register | bind | attest-health | profile-state | status` carry the §77 flags and
print one JSON receipt; exit 0 created/existing, 3 refused, 2 usage, 1 infrastructure. `register` takes
`--account-ref` as text and sends only its sha256, never a key, and prints the `<credential_ref>=<ENV_NAME>` map
entry the operator adds. `--request-extras` is a required flag (`{}` for none), so no vendor field is implied.

Rejected: one door doing register + bind + attest (three authorities: catalog / account, routing, observation; a
rebind must not mint credentials); a legacy bootstrap from `user_reasoning_profiles` like 0129 (it has no
production writer); catalog rows only by migration (every model change would need a schema release); `bind`
attesting implicitly (a binding is not an observation); a SELECT grant on the route tables for the status view (a
matrix change for one read; the definer returns exactly the operator's view).

### D-G  Admission precedes every lookup; a provider outage is RETRY → DEAD, not WAITING_KEY
A domain with no current Binding is NOT_READY `NO_DISTILL_BINDING` and parks WAITING_KEY after the ADR-0058 D-H
backoff; a disabled profile / candidate / account / endpoint, a retired model, a SHADOW policy or more than one
candidate is NOT_READY `reasoning route not admitted`; stale or denied health is `ROUTE_HEALTH_STALE` /
`ROUTE_HEALTH_DENIED`. None of them reaches `providers`, the ledger or a provider. There is no process default to
fall back to and no PLATFORM_PUBLIC path. **Correction of card scope 6:** a PINNED binding whose provider is down
does NOT park — a timeout or 5xx is `RetryWait`, the job settles RETRY with the capped backoff and counts an
attempt, and at `max_attempts` it is DEAD with its outbox FAILED (ADR-0058 D-F); only a 401 parks WAITING_KEY (and,
ruling E3 (b), records the credential INVALID). Recovery once the provider is back: `humaux-maintenance jobs
requeue-dead --tenant <id> --error-class <class>` (0197). Fair share (D-F) keeps the other providers' tenants
running meanwhile. Fallback across models is R4.

### D-L  A second provider has its own egress recipient
Each `control.provider_endpoints` row carries its own `egress_processor_id`; a second vendor's endpoints get their
own uuid (one per vendor / region recipient, reused by every tenant's endpoint for it), so
`ops.data_disclosures.processor_id` and the ledger's `egress_processor_id` tell the vendors apart. The deployment
binds each recipient uuid to the hosts it may dial (`HUMAUX_PRIVATE_WORKER_EGRESS_RECIPIENTS`, D-C), checked before
any DNS lookup or build (D-B), so an endpoint cannot claim one vendor's recipient while dialing another's host. The
register door takes `--egress-processor-id` from the operator and can see only the scheme; the worker is the binding
point. `deletion_capability` stays process-level Unknown (L7).

### Rulings recorded
- **E1** allowed-files extension approved. **E2 = USER**: a private distill / consolidate call is paid by the
  account of the credential its admitted route names (§11.2.4 authority `(tenant_id, credential_ref,
  provider_account_id)`, §11.5); 0166's "platform-paid" header described the single process key; `actual_cost`
  stays NULL when unknown (§19.1); PLATFORM payer exists only for the dormant PLATFORM_PUBLIC reasoning.
- **E3** approved with the liveness amendment: (a) explicit `--valid-for-secs`; (b) worker renewal from traffic and
  INVALID on a 401 (`control.observe_reasoning_route_health`, 0206; worker half S3); (c) `ROUTE_HEALTH_STALE` and
  `reasoning status`; (d) the no-traffic expiry is a known limit (below).
- **E5** catalog label `caps-<sorted capabilities joined by .>` for a wider set (never on the wire). **E6**
  contribution uses the per-route instance. **E7** removed and singular keys are refused at boot.
- **Research amendments** 1 per-profile `request_extras` (adapter-owned keys refused by CHECK and door); 2 three
  output channels TOOL_CALLS > JSON_OBJECT > content; 3 reasoning text in content tolerated, `reasoning_content`
  ignored; 4 `usage` optional; 5 the second provider of the live gate and the rehearsal comes from the
  `HUMAUX_LIVE_P2_*` environment, never from the repo; 6 rate limits are account-scoped on at least one vendor
  (below).

## Review pass 2 (migration 0209)

### Migration numbering
0206 = runtime (S1), 0207 = worker reads (S3), 0208 = operator doors (S4), 0209 = this pass. The card named
0206 + `0207_reasoning_route_doors`; S3 needed two worker reads 0206 did not provide and migrations apply in number
order, so the reads took 0207, the doors 0208, and a correction to an applied body takes the next free number (§46).

### 0206 header lock note (correction recorded here; the file stays byte-identical)
`cargo xtask migrate` records an FNV-1a checksum of the whole file, comments included, and refuses a changed
applied file (drift); no documented command rewrites a recorded checksum, and a hand edit of
`ops.schema_migrations` is the SEC-2 forgery `deploy-check` exists to catch. So the 0206 header is corrected here,
not in place. It says "ACCESS EXCLUSIVE on … ops.data_disclosures (trigger only)". In fact 0206 takes:
ACCESS EXCLUSIVE on `control.user_reasoning_profiles` and `control.processor_models` (CHECK re-created),
`control.reasoning_profiles` (CHECK re-created; `ADD COLUMN request_extras` with a constant default, a catalog-only
change, plus its CHECK), `ops.model_call_ledger` (CHECK; SHARE ROW EXCLUSIVE for the deferred constraint trigger)
and `private.processing_runs` (two columns and the FK); SHARE ROW EXCLUSIVE on `control.reasoning_profiles` for
that FK. `ops.data_disclosures` gets no lock: 0206 only replaces its validator function. Size the migrate window
from this list.

### E3 (b) correction: a 401 after a newer renewal still records INVALID
`control.observe_reasoning_route_health` (0206) skipped a rejection whenever any newer account row of the identity
existed, so a seat that renewed just before the key was revoked let one more admission and one more call through.
0209: renewals keep the guard; a rejection is skipped only by a newer INVALID row. Trade-off accepted: a call
admitted before an operator's rotation and attestation that then meets the old key's 401 denies the route until
the next attestation (fail-closed). Test `rejection_after_a_newer_renewal_still_denies_the_next_admission`.

### E3 (c) correction: a disabled route is "not admitted", never stale
`control.reasoning_route_health_state` (0207) ignored administrative state, so a disabled profile whose
attestation had also expired parked `ROUTE_HEALTH_STALE`, and re-attesting did not reopen it. 0209 reads the route
with the 0130 resolver's administrative conditions (policy SERVING and in effect, candidate / profile / account /
endpoint enabled, account and model ACTIVE); a disabled or retired route returns no row and the job carries the
generic `reasoning route not admitted`. Test `disabled_route_with_stale_health_is_not_reported_stale`.

## Tests (each red under its named fault; logs `c33b_s{1..4}_red_*.log` in the card's TW)

| Test | Fault that turns it red |
|---|---|
| T1 `profile_capabilities_definer_is_tenant_scoped` | GUC predicate dropped and NO FORCE RLS → B's caps returned |
| T2 `locator_carries_profile_not_catalog_capabilities` | definer selects the catalog's capabilities |
| T3 `private_reservation_row_equals_admitted_route` | `credential_ref` not bound in the INSERT → 23514 |
| T4 `private_insert_refused_unless_it_equals_a_current_admission` | private branch removed from the validator |
| T5 `private_route_columns_all_or_none` | all-NULL shortcut in the private branch |
| T6 `newer_attestation_does_not_refuse_reservation` | health-id equality with the latest observation |
| T7 `processing_run_profile_pair_and_fk` | `processing_runs_profile_pair` dropped |
| T8 0206 / 0207 / 0208 manifest postchecks | one GRANT revoked (or one extra GRANT) → postcheck false |
| T9 `provider_matches_admission_compares_instance_to_route` | capability-set comparison dropped |
| T10 `provider_failure_line_names_route…` | `profile=` dropped from the line |
| T11 `t11_consolidation_uses_the_provider_of_its_admitted_route` | RPC memoises the first instance |
| T13 `route_cache_builds_once_per_profile_version` | cache keyed by (processor, model) |
| T14 `route_descriptor_takes_capabilities_from_the_profile` | descriptor built with `ReasoningCapability::ALL` |
| T15 / T17 unmapped reference refused before any build / ledger row | map check removed from `provider_for` |
| T16 `credential_map_resolves_each_ref_to_its_own_key` | `resolve` ignores its reference |
| T18 `two_profiles_two_providers_one_dispatch` | reasoner keeps the first instance |
| T19 `binding_switch_takes_effect_on_next_job_without_restart` | first binding version remembered |
| T20 `unbound_domain_parks_waiting_key_others_unaffected` | NO_DISTILL_BINDING settled DEAD |
| T21 `disabled_profile_stops_being_used` | locator cached per binding |
| T22 `removed_provider_env_is_refused_at_boot` | one name dropped from the refusal list |
| T23 recipient / region list parsing | unset list treated as empty |
| T24 `distill_two_provider_live` (live, two vendors) | `provider_for` returns the first-built instance |
| T25 `register_then_bind_projects_one_of_each` | bind without its `existing` check |
| T26 `rebind_closes_old_interval_and_inserts_successor_version` | rebind mints a new `binding_id` |
| T27 `bind_refuses_owner_mismatch_and_unbindable_purpose` | purpose check dropped |
| T28 `register_refuses_capabilities_wider_than_catalog` | subset check dropped (exit 1); extras refusal unmapped (exit 1) |
| T29 `attest_health_makes_a_bound_profile_admissible` | attest writes the provider observation only |
| T30 rls-check route-door / definer EXECUTE sets | route-door check skips its functions; GRANT to a runtime role |
| T31 `route_provider_sources_are_single` | a second descriptor literal in `main.rs` |
| T32 `c33b_rehearse.sh` (deployed binary, providers_distinct=2) | tenant C bound to the first provider → 1 |
| T33 `claim_prefers_tenant_holding_fewest_slots` | 0199 least-recently-served-only ORDER BY |
| T34 `slow_provider_tenant_cannot_take_all_slots` | same as T33 → the slow tenant holds 4 slots |
| T35 empty credential map accepted, zero-profile boot parks | card 33's empty-map refusal kept |
| T36 `shared_secret_requires_one_vendor_account` | secret groups by variable name only |
| T37 `credential_accounts_definer_returns_vendor_identity_only` | EXECUTE granted to role_gateway |
| T38 `endpoint_host_must_belong_to_its_egress_recipient` | recipient checked by uuid only |
| T39 `private_reserve_is_one_transaction_with_matching_disclosure` | deferred trigger dropped; 0130 CONTRIBUTION-only list |
| E3 renewal / INVALID / rate limit | no renewal; no negative row; rate-limit clause removed |
| Amendments: extras, JSON_OBJECT, `<think>` strip, tool reply with `content: ""`, `usage` optional | adapter writes `reasoning_split`; JSON_OBJECT falls through; strip on one channel; `content` read first; `usage` required |
| `e2e_seed` `seeding_two_tenants_yields_two_independently_admitted_distill_routes` (extras clause) + `request_extras_flag_takes_a_json_object_only` | seeded Profile stores `{}` instead of `--request-extras` (`1 of 1 profile(s) lack --request-extras`); so `cargo xtask e2e-onboard`'s MiniMax lane, which declares `REASONING_SPLIT`, sends `{"reasoning_split":true}` as rehearse.sh's registered profile does (amendment 1) |
| `distill_poison_live` (M6, live) fixture | a tenant's second profile of one vendor inserts a second account row for the same `--account-ref` → 23505 on `provider_accounts_tenant_id_owner_user_id_processor_id_exte_key`; the fixture reuses account and endpoint as `register_reasoning_profile` does (D-J) |

## Consequences and known limits

- **L1** Four slots stay shared by every provider; fair share bounds a slow provider's tenant to its share while
  others have READY work; five or more tenants on hanging providers can still hold all four. Upgrade: budget rows
  per provider account, then worker-written negative health so a down route parks.
- **L8** Ledger and disclosure are one transaction with DB-checked equality; ADR-0058 L13 remains.
- **L11** `processing_runs.profile_*` stay writable by the worker's table-level UPDATE. Upgrade: an immutability
  trigger.
- **E3 (d)** A route with no traffic for longer than its validity parks until an operator re-attests; a prober
  that sends no tenant data is a later card (card 38).
- **Credential-accounts scan** One indexed probe per tenant row (about 125 ms at 10k tenants, boot only). Upgrade:
  an owner-read policy on the two tables.
- **request_extras bound** 4096 bytes; raise by forward migration when a vendor needs more.
- **L4** The instance cache never evicts (one idle client per admitted Profile@version). **L5** The §11.4 DNS check
  of a cache miss runs on the seat's task; a failure is not cached. **L6** One client per Profile@version, not per
  distinct endpoint.
- **L15** The key map is read at boot: a credential registered later needs a map entry and a restart. A binding
  switch between already-mapped credentials needs none (T19, T24). **L16** The value behind a variable name is
  checked at boot only.
- **Rate limits** Two profiles on one vendor account share that account's provider-side limit (research
  amendment 6, measured account + model scope on at least one vendor); nothing separates them. Upgrade: a token
  bucket per provider account (L2), input for R4 and card 38.
- **L2** No per-provider RPM/TPM node for private reasoning (§72.3); a 429 backs off one job only.
- **L3 / E3 (d) no-traffic expiry** Health is operator-attested with an explicit validity and renewed by traffic; a
  route idle for longer than its validity parks `ROUTE_HEALTH_STALE` (visible in `reasoning status`, no spend)
  until an operator re-attests. Upgrade: a prober that sends no tenant data (card 38, with the provider circuit
  breaker).
- **L7** `deletion_capability` is process-level Unknown for every recipient. Upgrade: an endpoint column or the
  Processor Registry.
- **L9** The HTTP timeout is process-level; the slowest provider sets it. Upgrade: a per-profile timeout in
  `custom_endpoint_policy`.
- **L10** `custom_endpoint_policy` is not wired; the provider error-code table is a generic "present and nonzero"
  rule. Upgrade: a per-profile error map when a bound vendor needs one.
- **L12** A freshly onboarded tenant's domains park until bound; `reasoning status` lists them `UNBOUND`.
- **L13** Only OpenAI-compatible chat endpoints. Upgrade: a second `UserReasoningProvider` built in
  `RouteProviders` by a declared endpoint kind, never by vendor name.
- **L14** The credential map is env (card 54 → OpenBao).
- **e2e-onboard and ADR-0058 L23** `recall_all_5` needs all five live Distill calls DONE; a reply refused twice
  (`FAILED_OUTPUT_SCHEMA`, about 1 per 100 Evidence on the rehearsal model) leaves one memory unrecalled until
  `jobs requeue-dead`. Measured 2026-10-03: one `confidence_invalid` DEAD in 10 calls on runs whose profile sent
  no `reasoning_split` (the seed gap above), 0 in 55 calls before card 33b. Upgrade: card 35's automatic
  re-drive of a `FAILED_OUTPUT_SCHEMA` death.
- **T26 scope** The rebind test proves the closed Binding@1 and its Policy / Candidate rows unchanged; that a ledger
  row reserved under v1 keeps naming v1 rests on the ledger guard trigger (route columns immutable after INSERT,
  F6) and T4, not on a second reservation in T26.

## What R4 (§11.2.2 multi-profile routing) needs later

Already in place: the provider instance is a function of one Profile@version (`RouteProviders`), so N candidates are
N cache entries, not a new construction path; one ledger row and one disclosure per actual attempt, each naming its
own Profile@version / Binding@version (D-I, D-N), so a fallback attempt is a second row, never a rewrite; admission is
a definer the worker cannot bypass; health is append-only observations keyed by exact identity.
R4 adds: (a) a gated new `strategy` value relaxing the PINNED / `fallback_class = NONE` CHECKs and the resolver's
`count(*) = 1`; (b) a resolver variant returning the ordered legal candidate set after the hard filter — same tenant,
same owner / trust domain, same payer, valid credential binding, region allowed by tenant policy — and only then a
ranking; (c) fallback only inside the same legal fallback universe (never PLATFORM_PUBLIC, never another user's key,
never another payer); (d) per-attempt reservation; (e) a health writer beyond operator attestation and traffic
renewal (probe or worker-reported negative observations) so health-based selection has data; (f) per-provider
budget domains (L1, L2); (g) its own research pass and owner gate (automatic fallback across models is a separate
card).

## Addendum (2026-10-05, card 35 final verification) — T34's sampler counted the wrong population; the claim is already serialized

- **Observation.** T34 `slow_provider_tenant_cannot_take_all_slots` went red once in card 35's final verification (`c33b-t34 samples=58 max_slots_b=4 done_a=500`). An external 100 ms observer on a second run showed: while tenant A had READY `DERIVED_DISTILL` work, B held at most 2 of 4 slots in every sample; after A's 500 distill jobs were DONE, B took all 4 — correct, work-conserving D-F behaviour — and the sampler kept folding those samples in.
- **Root cause (test side).** The sampler's "A still has work" guard counted `ops.jobs` rows with `status IN ('PENDING','PROCESSING')` across **all job types**. The 0164 trigger `derived_consolidate_work_enqueue` queues one `DERIVED_CONSOLIDATE` job per accepted Evidence that nothing in T34 claims, so the guard never dropped to 0. Fixed in the test: the guard now requires `job_type = 'DERIVED_DISTILL'`. The assertion `max_slots_b <= 2` is unchanged.
- **Hypothesis disproved.** The main line first suspected a claim race (two overlapping `ops.claim_derived_work_v2` calls, the second skipping the fewest-held tenant's `FOR UPDATE … SKIP LOCKED` row and falling through to the over-share tenant). A two-session experiment on a throwaway database refuted it: the single `ops.provider_arbiters` row taken `FOR UPDATE` by 0190 (ADR-0058 D-B) already serializes every claim — session 2 waited on `transactionid` until session 1 committed, then received the fewest-held tenant's job (final slots B, B, A, A). No migration or claim change was made; D-F holds as written.
- **Lesson.** A fairness witness must define the contended window by the exact READY predicate of the work it measures, never by an untyped job count.
