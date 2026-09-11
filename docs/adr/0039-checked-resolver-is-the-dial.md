# ADR-0039 — The checked resolver *is* the dial (closing the egress DNS-rebinding window)

- Status: accepted
- Date: 2026-09-10
- Revised: 2026-09-11 after review — the first round left the gap open on the **only**
  production caller (P0, D3), the gate could not observe it (P1, D5 判据5–8), and this
  document misdescribed its own diff on the DashScope fixture (P1, see Latency).
- Card: 17
- Spec: §11.4 (Custom / OpenAI-compatible Endpoint Security — `DNS/IP validation` +
  `private/reserved IP policy`), §83.4 (G80-3 outbound network choke point, 判据1/判据3),
  §78.1 (named, overridable knobs — no literals), §7.3/§7.4 (egress permit + disclosure ledger,
  untouched by this card), §79 (observe real network behaviour, do not infer it)
- Supersedes nothing. Corrects the residual gap ADR-0003's second round left open on Layer 1A
  (it had closed it only for Layer 1B). Composes with ADR-0003 (Layer 0/1A/1B split).

## Context

The deployment report's first reviewer-P1 debt, and the only security one: **the egress check
and the dial disagreed.**

- §11.4's SSRF choke point (`adapters::byok::ssrf::validate_custom_endpoint`) resolved the
  custom endpoint's hostname through an **injected** `DnsResolver` and rejected any answer in a
  private/reserved/loopback/link-local range.
- The transport that then sent the request (`adapters::byok::EgressHttpTransport` →
  `infra_egress::raw::RawHttpPost`) held a `reqwest::Client` built by
  `humaux_infra_network::http::build_client` — i.e. **`reqwest`'s default `GaiResolver`, the
  system DNS**.

Two lookups, two answers, nothing structurally connecting them. That is the OWASP SSRF Cheat
Sheet's DNS-rebinding / TOCTOU pinning bypass verbatim: the first answer is a public address
that passes the check, the second answer — the one the connector actually dials — is
`169.254.169.254`. And this is the code path that carries **user provider API keys** to MiniMax
and DashScope.

`ssrf.rs` had recorded the hole honestly (a `ponytail:` comment on `ValidatedEndpoint` naming
the exact upgrade path), which is how it stayed visible long enough to be scheduled. Layer 1B
(`infra-cell::transport`) had already been fixed the right way in ADR-0003's second round —
`build_client_with_resolver` exists precisely for this — and Layer 1A had simply never been
moved onto it.

The sibling path had the same shape with a different owner: `infra_egress::http::
HttpExternalCall` (the PlatformManaged/DashScope transport) also built through `build_client`,
so it too performed an unobserved second name resolution between "the address this process
resolved" and "the address it connected to".

## Decision

### D1 — Delete the resolver-less constructor; do not deprecate it

`humaux_infra_network::http::build_client` is **gone**. `build_client_with_resolver` is the
whole public surface of the Layer 0 choke point.

This is the load-bearing decision: a guard added in one caller leaves every sibling caller open
and is one merge away from being reintroduced. Removing the function makes "an outbound client
that dials with the system resolver" a thing that **cannot be spelled** — the compiler is the
gate, and the source scan (D5) only has to keep it deleted.

### D2 — One Layer 1A construction point, carrying the caller's own policy

`crates/infra-egress/src/resolver.rs` is new and is the only place in Layer 1A that builds a
client:

```rust
pub trait CheckedDnsResolve: Send + Sync {
    fn resolve_checked(&self, host: &str) -> Result<Vec<IpAddr>, String>;
}
pub fn build_pinned_client(config, resolver: Arc<dyn CheckedDnsResolve>, ttl) -> reqwest::Client
```

`PinnedResolver` wraps the caller's `CheckedDnsResolve` and is installed as the client's **only**
`reqwest::dns::Resolve`. There is one lookup, not two; the addresses the caller's policy admitted
are the addresses the connector receives.

The module carries **no address policy of its own**. Which addresses are allowed is a Layer 1
semantic that already has an owner in each domain — §11.4's `is_forbidden_ip` lives in
`adapters::byok::ssrf`, Cell-CIDR membership lives in `infra-cell` — and duplicating either table
here is how two copies drift apart. The trait's signature is `std`-only so `adapters` can
implement it without ever naming an `infra-network` type (G80-3's
`g80_3_infra_network_dependents_check` pins that manifest set to `{infra-egress, infra-cell}`).

### D3 — BYOK: the same `DnsResolver` and the same `is_forbidden_ip` run on the dial

`adapters::byok::SsrfCheckedResolver` implements `CheckedDnsResolve` by calling the injected
`ssrf::DnsResolver` and then applying `ssrf::is_forbidden_ip` to **every** returned address —
literally the same function `validate_custom_endpoint` applies, with the same "any forbidden
address rejects the whole answer" rule (not "clean if any one is fine").

`EgressHttpTransport` first got the `new` / `with_resolver` pair `infra-cell::transport`
established: `new(timeout)` hardwired `ssrf::SystemDnsResolver` and so "needed no caller churn".
**That was wrong, and the card-17 review caught it on the only production path.** The pair leaves
"which resolver checks" and "which resolver dials" as two unrelated arguments, and the sole
production caller supplied them from two different sources: `bins/private-worker` handed the
operator's `HUMAUX_PRIVATE_WORKER_DNS_PINS` resolver to `OpenAiCompatibleProvider::new` (the
check) and let `EgressHttpTransport::new` pick system DNS for the dial. The window this ADR
exists to close was therefore still open on the one path that carries user provider API keys —
and on precisely the node type the pins exist for, the dial would additionally have been refused
outright by `is_forbidden_ip` (this machine's system DNS answers `api.minimaxi.com` with
`198.18.0.42`, RFC 2544 reserved; the true address is `47.79.117.67`). Nothing caught it: 判据1–4
observe *where* a client is built, never *which resolver a caller injects*, and both live
witnesses ran behind an egress proxy, where `reqwest` never consults the resolver at all.

Revised decision:

- `EgressHttpTransport::new` is **deleted**. A constructor that picks the dial resolver for you
  is a constructor that can disagree with the check; `with_resolver` is the only one left.
- `OpenAiCompatibleProvider::with_egress_transport(descriptor, base_url, request_timeout,
  decryptor, policy, resolver: Arc<dyn ssrf::DnsResolver>)` is the **only** production entry
  point. It takes the resolver **once** and derives both legs from that one value — the dial via
  `EgressHttpTransport::with_resolver`, the §11.4 check via `Self::new(…, resolver.as_ref())`.
  A mismatch is no longer expressible at a call site.
- 判据5–8 (D5) pin it, so the next caller cannot re-open the gap by hand.

A consequence worth stating plainly: **the transport's resolver is now the authority.** Whatever
a construction-time check concluded, a request cannot land on an address the transport's own
policy refuses. That cuts both ways, and the review's second consequence follows from it: the
three live fixtures each carried a `PinnedPublicResolver` returning a hard-coded
`93.184.216.34` for *every* host — harmless while it only fed the check, and a silent redirect
of the live outbound call once it fed the dial. All three are replaced by the production
mechanism, `ssrf::PinnedDnsResolver::parse(HUMAUX_MINIMAX_DNS_PINS)` (`host=ip[|ip],…`, same
format as `HUMAUX_PRIVATE_WORKER_DNS_PINS`); absent or empty means every host falls through to
system DNS, which is the CI / non-fake-IP default. On this machine the live suites therefore run
with `HUMAUX_MINIMAX_DNS_PINS=api.minimaxi.com=47.79.117.67`.

### D4 — A refusal is a refusal, not "some network error"

`PinnedResolver` raises a typed `EgressResolutionRefused` as the source of `reqwest`'s connect
error; `RawHttpPost` recovers it from the error chain (the same `downcast` walk
`infra-cell::transport` uses) and returns a distinct `RawSendError::EgressRefused { host, reason }`,
which `byok.rs` maps to `Transport("egress policy refused <host>: <reason>")`.

Without this, a successfully blocked rebinding attack is indistinguishable from a flaky network,
and nobody would ever learn it happened. The variant is deliberately not folded into
`RawSendError::Network`.

### D5 — The repo-level assertion, following the `no_handwritten_filter_scan` precedent

`xtask architecture-check` gains
`ADR-0039 / §11.4 (outbound client dials only through the checked resolver)`
(`checked_resolver_dial_problems`, a pure comparator over an already-scanned file set so faults
can be injected as fixtures):

1. **判据1** — `build_client(` must not appear as a call anywhere. (`build_client_with_resolver(`
   does not contain that needle: the `_with` is in the way, so the two never alias.)
2. **判据2** — `build_client_with_resolver(` call sites ⊆
   `{crates/infra-network/src/http.rs, crates/infra-egress/src/resolver.rs,
   crates/infra-cell/src/transport.rs}`.
3. **判据3** (living sentinels, §53.3 规则3's shape) — `infra-network/src/http.rs` still defines
   `build_client_with_resolver`, does **not** re-export `pub fn build_client(`, and both Layer 1
   resolver modules still call it. Without these, deleting the mechanism reads as pass.
4. **判据4** — `.dns_resolver(` may only be named in the Layer 0 construction point.

判据1–4 pin *where* an outbound client is constructed. They cannot see *which resolver a Layer 1
caller injects*, which is why the production mismatch above was green on every gate. 判据5–8,
added by the card-17 review, close that:

5. **判据5** — `EgressHttpTransport::new(` must not appear anywhere: the resolver-hardwiring
   constructor stays deleted (the compiler enforces it too; this keeps a re-added definition
   from going unnoticed).
6. **判据6** — `EgressHttpTransport::with_resolver(` call sites ⊆
   `{crates/adapters/src/byok.rs, crates/adapters/tests/byok_egress_rebinding.rs}` — the derived
   constructor, plus the rebinding acceptance test that drives the dial leg on purpose.
7. **判据7** — no file under `bins/<x>/src/` may call `OpenAiCompatibleProvider::new(`.
   Production goes through `with_egress_transport`, which takes the resolver once. This is the
   judgement that would have failed on `bins/private-worker/src/main.rs:253`. Integration tests
   under `bins/<x>/tests/` are unaffected — that is how a fake transport gets injected.
8. **判据8** (living sentinel) — `crates/adapters/src/byok.rs` still defines
   `pub fn with_egress_transport` and still builds its transport with
   `EgressHttpTransport::with_resolver(`. Delete the derived constructor and 判据5/6/7 would all
   pass vacuously.

Comment lines are filtered (`line_is_comment_at`), and `xtask/src/architecture_check.rs` excludes
itself — it contains every needle as a string literal, the same `this_file` exclusion
`crates/projection/tests/no_handwritten_filter_scan.rs` uses.

### D6 — Pin for a TTL, then re-check; do not pin forever

`DEFAULT_PIN_TTL = 60s` (§78.1: a named knob, not a literal buried in a constructor). Inside the
window a request costs **zero** name lookups (the speed criterion: pinning must not add a lookup
per request). After it, the name is resolved again **and re-judged** — a pin is not "trust the
first answer forever": a host that rebinds to a forbidden address is refused at the next
re-resolution, and a legitimate DNS change is picked up within 60s.

## Ceilings (deliberate, with upgrade paths)

- **An egress proxy defeats the pin.** With `HTTP_PROXY`/`HTTPS_PROXY` set and the destination
  outside `NO_PROXY`, `reqwest` connects to the proxy and issues `CONNECT <host>` — the
  destination is resolved **at the proxy** and no client-side resolver is consulted. Layer 1A
  keeps `trust_env_proxy: true` (an org's egress proxy is a legitimate path for traffic that is
  by definition already leaving Humaux infrastructure), so this is a deployment-shape residue,
  not a hole this layer can close. It is recorded in `resolver.rs`'s module doc, in
  `ClientConfig::trust_env_proxy`'s doc (which already said it), and in Baseline §11.4. Upgrade
  path: an operator who wants the pin to hold end-to-end runs without an env proxy, or terminates
  policy at the proxy itself.
- **`HttpExternalCall` pins but applies no address policy** (`SystemCheckedResolve`). Its
  `endpoint` is operator-configured, not user-supplied SSRF input, and this crate's own loopback
  test fixtures are legitimate destinations. What it gains from ADR-0039 is the pin. Upgrade path:
  pass a policy-carrying `CheckedDnsResolve` if a platform endpoint ever becomes tenant-supplied.
- **`PinnedResolver`'s cache is an unbounded `HashMap`** keyed by hostname. A client instance
  targets a single-digit number of hosts (one endpoint per provider instance). Upgrade path: a
  capacity-bounded LRU if a client is ever shared across thousands of hostnames.
- **IP-literal authorities never reach any resolver.** `reqwest`'s connector skips a custom
  `dns_resolver` when the URL authority is an IP literal — the address in the URL *is* the
  address dialed, so there is no second lookup to disagree with, and §11.4's literal-IP branch
  (`validate_custom_endpoint`'s `HostIpForbidden`) already judges that case at check time.
  `infra-cell` had to canonicalize URL-parser-folded IP forms because its registry hosts can be
  literals; a BYOK `base_url` goes through the same `ssrf` parser, so the same judgment applies.

## Negative controls / fault injection (red → green, actually run)

| Fault | Gate that must go red | Result |
| --- | --- | --- |
| `crates/infra-egress/src/raw.rs` calls `build_client(` (real edit, real file) | the **compiler**, before any gate | `error[E0425]: cannot find function \`build_client\` in this scope` — D1's point: the regression is unspellable |
| the realistic regression: `pub fn build_client` re-added to Layer 0 **and** called from `raw.rs` (real edit to both real files, then reverted) | `cargo xtask architecture-check` | `exit 1`, three named problems (判据1 ×2 + 判据3) — see Evidence |
| planted fixture: a caller calls `build_client(` | `adr_0039_fault_system_dns_dial_reintroduced_in_a_caller_is_red_and_named` | red |
| planted fixture: a fourth crate calls `build_client_with_resolver(` | `adr_0039_fault_fourth_call_site_is_red_and_named` | red |
| planted fixture: `pub fn build_client(` re-added to Layer 0 | `adr_0039_fault_build_client_reexported_is_red` | red |
| planted fixture: a Layer 1 resolver module stops calling the constructor | `adr_0039_fault_layer1_resolver_module_stops_calling_is_red` | red |
| planted fixture: `.dns_resolver(` outside Layer 0 | `adr_0039_fault_dns_resolver_installed_outside_layer0_is_red` | red |
| a rustdoc comment naming `build_client(` | `adr_0039_comments_naming_build_client_are_not_offenders` | green (negative control: the gate is not a blind grep) |
| planted fixture: a bin calls `EgressHttpTransport::new(` | `adr_0039_fault_hardwired_transport_constructor_is_red_and_named` | red (判据5) |
| planted fixture: a bin picks the dial resolver itself via `EgressHttpTransport::with_resolver(` | `adr_0039_fault_transport_resolver_chosen_outside_the_derived_constructor_is_red` | red (判据6) |
| planted fixture: **the actual P0** — a bin calls `OpenAiCompatibleProvider::new(` with a hand-supplied check resolver | `adr_0039_fault_bin_hand_pairs_check_resolver_and_transport_is_red` | red (判据7) |
| planted fixture: `with_egress_transport` deleted from `byok.rs` | `adr_0039_fault_derived_constructor_deleted_is_red` | red (判据8 living sentinel) |
| a `bins/<x>/tests/` file calling `OpenAiCompatibleProvider::new(` | `adr_0039_tests_may_still_call_the_generic_constructor` | green (negative control: 判据7 is about production bins) |
| clean fixture | `adr_0039_clean_fixture_has_no_problems` | green (positive control) |

Behavioural (real sockets, §79 — not an error-code side channel):

- `a_rebound_second_answer_is_refused_before_any_connection` (infra-egress): two real listeners on
  the **same port**, `127.0.0.1` and `[::1]` (the port in the URL overrides the resolved port, so
  a wrong dial is only observable by *address*). Lookup #1 returns the allowed address, lookup #2
  refuses; the refusal is recovered as a typed `EgressResolutionRefused`, and **neither** listener
  ever accepts a connection.
- `the_checked_address_is_the_one_dialed` (infra-egress): the positive control that keeps the
  above from passing merely because the client cannot connect to anything — an accepting resolver
  really connects, to the resolved address, and the other address sees nothing.
- `a_rebound_endpoint_is_refused_before_the_connection_and_never_dialed`
  (`crates/adapters/tests/byok_egress_rebinding.rs`): the same simulation through the real BYOK
  stack — `validate_custom_endpoint` accepts on lookup #1, `EgressHttpTransport::send` fails on
  lookup #2, and the loopback listener the rebound answer pointed at records zero connections.
  **This one is environment-aware, and says so out loud.** The BYOK client is
  `trust_env_proxy: true` (D-Ceilings), so on a machine with an egress proxy configured (this
  development machine: `HTTPS_PROXY=127.0.0.1:7897`, `NO_PROXY=localhost,127.0.0.1,::1,.local`)
  `reqwest` dials the proxy and never consults the resolver at all. The test detects that,
  prints `PARTIAL … missing object: 无代理的出网环境 …` naming what it could not assert and
  where the unconditional version lives, and **still** asserts the rebound address saw zero
  connections. It does not silently pass, and it does not claim a refusal it did not observe.
  The unconditional typed-refusal assertion is the `infra-egress` test above
  (`trust_env_proxy: false`, no ambient proxy in play).
- `the_dial_time_resolver_applies_the_same_policy_as_the_construction_time_check`: check and dial
  are asserted to agree on five address sets, including the "one public + one loopback" case where
  "clean if any one is fine" would be wrong.

## Latency (speed is an acceptance goal — card 24 turns these into the baseline)

See "Evidence" below for the raw command output.

- **Pin-hit request path**, `the_checked_answer_is_reused_for_the_ttl_not_re_resolved_per_request`,
  loopback, n = 30, ms: **p50 0.155, p95 0.670** (a second run of the same test: p50 0.170,
  p95 0.237 — the p95 is a single sample and moves with scheduler noise), with **1** name lookup
  for 30 requests (asserted, not merely measured — the test's resolver refuses every lookup after
  the first, so a re-resolution inside the TTL would fail the test, not merely slow it).
- **MiniMax hop** (`bins/private-worker` `d1_live_distill_writes_memories_and_projection_resolves_ticket`,
  live, through the dial-pinned client): **passed**, 11.06 s for the whole four-leg test
  (`memories=1`, `disclosures=["SUCCESS"]`, `projection(done=1, highwater=1)`, `ticket=(DONE)`).
  `crates/adapters/tests/minimax_live_smoke.rs` (3 tests, incl. the real M3 call): **passed**,
  4.76 s. n = 1 per run: a 30-sample end-to-end chat benchmark is model-latency- and
  token-cost-dominated and would measure MiniMax, not this change — the number this change can
  move is the lookup count above, which is now zero per request inside the TTL.
- **DashScope embedding hop** (`crates/retrieval-provider` `dashscope_live_smoke`): **passed**,
  1.42 s (a second run: 1.46 s), a real `text-embedding-v4` call through
  `HttpExternalCall`'s pinned client, with the authorize/reserve/finalize sequence completed.
  The first round of this ADR said this suite "could not be run" and that fixing it "belongs to a
  separate card" — that was wrong about its own diff: the working tree already carried the fix,
  and the review was right to call it out. Recorded properly now, because the fixture had **two**
  defects and both are this card's:
  1. `migrations/0117_retrieval_provider_budget.sql:265` guards
     `ops.reserve_retrieval_provider_budget` on `session_user`, so the old fixture's `postgres`
     superuser + `options=-c role=role_retrieval_worker` (`SET ROLE` changes `current_user`, never
     `session_user`) was refused with `42501` before a single byte left the process. Fixed by
     connecting as the real LOGIN role via `HUMAUX_RETRIEVAL_WORKER_PG_DSN`, which already exists
     for exactly this.
  2. Past that, the same function raises `P0003` unless **all four** canonical admission tiers
     (GLOBAL, REGION, TENANT, TENANT+PURPOSE) have exactly one ACTIVE limit row. The fixture
     seeded none. Fixed by seeding the same four rows `xtask e2e-seed` provisions.
  Negative control for the first fix, run: with `HUMAUX_RETRIEVAL_WORKER_PG_DSN` unset and
  `HUMAUX_REQUIRE_DB=1`, the suite fails loudly with
  `isolation setup failed: HUMAUX_RETRIEVAL_WORKER_PG_DSN (role_retrieval_worker LOGIN DSN) is
  not set` rather than skipping — the silent skip is what hid the defect for so long.
  Run env: `set -a; source .env.local; set +a` (for `DASHSCOPE_API_KEY`) plus
  `HUMAUX_REQUIRE_DASHSCOPE=1`, `HUMAUX_REQUIRE_DB=1` and the DSNs.
- **MiniMax hop, un-proxied (the witness the first round did not have).** The review's P0 note is
  exact: the first round's live runs all went through `HTTPS_PROXY=127.0.0.1:7897`, where
  `reqwest` never consults the client's resolver, so they could not have observed a wrong dial
  resolver. Both MiniMax suites were therefore re-run with every proxy variable unset, so the
  pinned checked resolver really is the thing that picks the address:
  - `crates/adapters/tests/minimax_live_smoke.rs` (3 tests incl. the real M3 call): **passed**,
    2.75 s; the live test alone 3.22 s, `reasoning_tokens=Some(20)`.
  - `bins/private-worker` `d1_live_distill_writes_memories_and_projection_resolves_ticket`:
    **passed**, 11.10 s — `memories=1`, `disclosures=["SUCCESS"]`,
    `projection(done=1, highwater=1)`, `ticket=(DONE)`. First run, no rerun needed (the known
    `origin_authority_ceiling` flake did not fire).
  - **Negative control, run:** the same live test with `HUMAUX_MINIMAX_DNS_PINS` unset and no
    proxy fails at construction with
    `EndpointRejected(ResolvedIpForbidden { host: "api.minimaxi.com", ip: 198.18.0.42 })`.
    That is the proof the resolver is load-bearing on this path: this node's system DNS is
    proxy-hijacked into RFC 2544's `198.18.0.0/15`, §11.4 refuses it, and the pin
    (`api.minimaxi.com=47.79.117.67`, the address a DoH lookup returns) is what makes both the
    check and the dial land on the real host.

Directional claim, and the honest bound on it: this change cannot add a lookup per request (the
n = 30 assertion above), and removes lookups relative to `GaiResolver` whenever a connection is
re-established inside the TTL window. The live wall-clocks above are model- and RTT-dominated at
n = 1 per run and are recorded as evidence that the hop works end to end through the pinned
client, not as a latency baseline — a 30-sample chat benchmark would measure MiniMax, not this
change. The number this change actually moves is the lookup count, which is zero per request
inside the TTL.

## Consequences

- Two public signatures changed in a way callers can see: `RawHttpPost::new` now takes
  `Arc<dyn CheckedDnsResolve>` (its only caller is `EgressHttpTransport`), and
  `EgressHttpTransport::new` is **gone** — every caller moves to
  `OpenAiCompatibleProvider::with_egress_transport`. Four call sites edited:
  `bins/private-worker/src/main.rs` (the P0), `bins/private-worker/tests/distill_hop_e2e.rs`,
  `bins/consolidation-worker/tests/consolidation_hop_e2e.rs`,
  `crates/adapters/tests/minimax_live_smoke.rs`. The earlier claim in this ADR that these
  "needed no edit" was the defect, not a saving.
- `bins/private-worker`'s resolver is now an `Arc<dyn ssrf::DnsResolver>` built by
  `PinnedDnsResolver::parse(HUMAUX_PRIVATE_WORKER_DNS_PINS)` unconditionally — `PinnedDnsResolver`
  already falls through to `SystemDnsResolver` for unpinned hosts, so an empty/absent spec is
  byte-for-byte the old default and the `match` it replaces is gone.
- `RawSendError` gains `EgressRefused`; the one `match` over it (`byok.rs`) is updated. §52's
  18 frozen `ErrorCode` variants are untouched — this is a transport-internal enum.
- No new table, no new grant, no migration, no new MCP operation, no new output branch: §6.2.2 /
  rls_check `MATRIX`, `SUPPORTED_OPERATION_KEYS` and the `memory.output.schema.json` `oneOf`
  pins are all unchanged by construction.
