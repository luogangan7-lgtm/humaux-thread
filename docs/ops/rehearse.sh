#!/bin/zsh
# 部署点演练驱动（本机四进程；真 DashScope + 真 MiniMax）。密钥只 source 进对应子 shell；任何输出不含 bearer/key。
# Verifies one deployment on this host, step by step: seed and onboarding, the resident processes and
# their probes, the observability bundle (Prometheus / Alertmanager / collector from the pinned
# binaries, ADR-0061 D-K), traffic with named assertions, metrics_scrape / admin_probes / alert_drill,
# and the soak when SOAK_SECS is set. The resident maintenance daemon (`humaux-maintenance --serve`, card 35) runs
# only against this run's own throwaway database humaux_thread_c35_rh_<pid> (ruling E8), never against $DB.
# Every process is started and signalled only through its pidfile.
# Exit: 0 = REHEARSAL VERDICT with 0 failed; 1 = an assertion failed; 2 = usage or missing configuration.
set -u
S=${HUMAUX_REHEARSE_WORK:-${TMPDIR:-/tmp}/humaux-rehearsal}   # work dir: pidfiles, helpers, evidence (override with HUMAUX_REHEARSE_WORK)
EV=$S/e2e_evidence; SOCK=/tmp/hq-e2e; mkdir -p $EV $SOCK
# Card 30 (ADR-0055 D-E): one build profile for every binary this script builds, spawns, probes
# or writes into a chaos hook. `release` is what the recall stage timing is measured on; the
# default stays `debug` so every earlier gate keeps its shape.
REHEARSE_PROFILE=${REHEARSE_PROFILE:-debug}
case $REHEARSE_PROFILE in debug|release) ;; *) echo "REHEARSE_PROFILE must be debug or release"; exit 2;; esac
BIN_DIR="${CARGO_TARGET_DIR:-target}/$REHEARSE_PROFILE"
R=/Volumes/data/humaux-thread; cd $R
PG=127.0.0.1:54329; DB=${HUMAUX_REHEARSE_DB:-humaux_thread_dev}; MYUID=$(id -u)
export HUMAUX_TEST_PG_DSN="postgres://postgres:${HUMAUX_DEV_PG_SUPERUSER_PASSWORD:?}@$PG/$DB" HUMAUX_MAINTENANCE_PG_DSN="postgres://role_maintenance:${HUMAUX_ROLE_PASSWORD_MAINTENANCE:?}@$PG/$DB"
GITLEAKS_BIN=/private/tmp/gitleaks-8.30.1/gitleaks; GITLEAKS_SHA=ba52fb1bfabbcde42f032afad3d6e0b19dff8ed105229a16e7caa338bbc0e84f; GITLEAKS_VER=8.30.1
# 统一的流族（card 21）：三个字面量删了。DOMAIN/PKIND/PVER 现在由 e2e-seed 从
# `domain::ticket_family::TicketFamily` 打印出来（见 step seed 之后的赋值），retrieval worker
# 自己从 `RetrievalFamily::PrivateMemoryV1` 推导、不再读三个 env。以前这一行、consolidate_repo.rs
# 的三个字面量、worker 的三个 env 是同一个值的三份手工副本：写错一个没有任何运行期信号，
# worker 只会永远轮询一条没人写的流。
# DashScope（§19 冻结：text-embedding-v4 / 1024；revision 先例 2026-08）
EMB_MODEL=text-embedding-v4; EMB_DIM=1024; EMB_REV=2026-08; EMB_VER=text-embedding-v4@2026-08; EMB_REGION=cn-beijing; EMB_MAX_TOK=8192
COLLECTION=humaux_private_memory_v1_e2e
CELL_ID=$(uuidgen | tr 'A-Z' 'a-z'); PEPPER_HEX=$(openssl rand -hex 32)
# ADR-0059 D-G: one per-run token MAC key, shared by every gateway (re)start of this run; never printed.
TOKEN_HMAC_HEX=$(openssl rand -hex 32)
# MiniMax lane (tenants A and B): the values `humaux-maintenance reasoning register` declares (ADR-0060 D-H).
MM_URL=https://api.minimaxi.com/v1/chat/completions; MM_PROVIDER=minimax; MM_MODEL=MiniMax-M3; MM_REV=caps-REASONING_SPLIT.STRUCTURED_OUTPUT.TEXT.TOOL_CALLS; MM_REGION=cn-shanghai; MM_TIER=standard
# ADR-0058 R10: the rehearsal profile's distill channel, chosen by the live A/B probe
# `distill_channel_ab_live` (rule: TOOL_CALLS only if the tool channel's DEAD count and first-reply
# malformed rate are not higher than the content channel's). 2026-10-03, n=100 each: tool dead=0
# malformed=1, content dead=1 malformed=6 -> TOOL_CALLS. One definition for every worker below.
# ADR-0060 D-A / D-B / E5: the registered profile declares this set (reasoning register --capabilities)
# and the worker builds each call's provider from its admitted route — no worker env names a provider,
# model, endpoint or capability (D-C). The catalog row for a tool-capable profile is a new revision
# label (MM_REV above; the 2026-08 row is frozen at TEXT,STRUCTURED_OUTPUT; never on the wire).
PW_CAPABILITIES=TEXT,STRUCTURED_OUTPUT,TOOL_CALLS,REASONING_SPLIT
# ADR-0060 ruling E3: traffic renews a route's health once less than half of this is left (the
# operator attestation below covers the whole run; renewal keeps a busy route admissible beyond it).
PW_HEALTH_RENEW_SECS=1800
EGRESS_PROC=$(uuidgen | tr 'A-Z' 'a-z')
step() { echo "### STEP $1 $(date +%T)" | tee -a $EV/rehearsal.log; }
doh_ips() { curl -s "https://dns.alidns.com/resolve?name=$1&type=A" | python3 -c "import sys,json; d=json.load(sys.stdin); print('|'.join(a['data'] for a in d.get('Answer',[]) if a.get('type')==1))"; }
MM_HOST=$(print -r -- "$MM_URL" | sed -E 's#https://([^/]+)/.*#\1#')
MM_PINS="$MM_HOST=$(doh_ips $MM_HOST)"
echo "dns pins: $MM_PINS" | tee -a $EV/rehearsal.log
# ADR-0060 (card 33b, research amendment 5): tenant C reasons on a second provider taken from the
# environment — the repo names no vendor. The nine names below (and the key variable the map will name)
# are required before anything is seeded; a missing one is exit 2, never a single-provider rehearsal.
# HUMAUX_LIVE_P2_DNS_PINS is optional (absent = no pin). HUMAUX_LIVE_P2_HOSTS is `|`-separated, the
# separator HUMAUX_PRIVATE_WORKER_EGRESS_RECIPIENTS uses inside one recipient (live_provider.rs agrees).
for v in HUMAUX_LIVE_P2_BASE_URL HUMAUX_LIVE_P2_HOSTS HUMAUX_LIVE_P2_MODEL HUMAUX_LIVE_P2_CAPABILITIES \
         HUMAUX_LIVE_P2_REQUEST_EXTRAS HUMAUX_LIVE_P2_KEY_ENV HUMAUX_LIVE_P2_PROVIDER_ID \
         HUMAUX_LIVE_P2_REGION HUMAUX_LIVE_P2_EGRESS_PROCESSOR_ID; do
  [ -n "${(P)v:-}" ] || { echo "second provider: $v is not set (ADR-0060 research amendment 5)" | tee -a $EV/rehearsal.log; exit 2; }
done
case "$HUMAUX_LIVE_P2_HOSTS" in *,*) echo "second provider: HUMAUX_LIVE_P2_HOSTS is |-separated, not comma-separated" | tee -a $EV/rehearsal.log; exit 2;; esac
[ -n "${(P)HUMAUX_LIVE_P2_KEY_ENV:-}" ] || { echo "second provider: the variable named by HUMAUX_LIVE_P2_KEY_ENV is not set" | tee -a $EV/rehearsal.log; exit 2; }
# Ruling E5: the second provider's catalog label is its sorted capability set (never on the wire).
P2_REV=caps-$(print -r -- "$HUMAUX_LIVE_P2_CAPABILITIES" | tr ',' '\n' | sort | paste -sd. -); P2_TIER=standard
PW_DNS_PINS="$MM_PINS${HUMAUX_LIVE_P2_DNS_PINS:+,$HUMAUX_LIVE_P2_DNS_PINS}"

# ---------- process ownership (HARD RULE — after the 2026-09-09 incident) ----------
# Every process this script starts records its PID in $S/<name>.pid, and NOTHING here ever
# selects a process any other way. `pkill -f` / `pgrep -f` match "whose argv contains this
# string" and `lsof -ti :PORT` matches "who holds this port" — neither means "the process I
# started", and on 2026-09-09 a port-kill in this very rehearsal killed the user's WeChat.
# `own_signal` refuses any PID whose `ps -o comm=` basename is not the binary it expects.
# Written to a file rather than defined as a shell function because the soak's chaos hooks are
# run by `sh -c` out of xtask and cannot see this shell's functions.
cat > $S/own_signal.sh <<'OWNEOF'
#!/bin/sh
# own_signal <pidfile> <expected binary basename> <signal> [wait_secs]
# Kills ONLY a PID the caller recorded at spawn, and only after ps confirms the binary name.
# Never kill by port or by command-line pattern: those select by coincidence, not by ownership.
own_signal() {
  [ -f "$1" ] || { echo "own_signal: no pidfile $1 — refusing to guess a PID"; return 1; }
  p=$(cat "$1")
  case "$p" in ''|*[!0-9]*) echo "own_signal: bad pid '$p' in $1"; return 1;; esac
  if ! kill -0 "$p" 2>/dev/null; then echo "own_signal: pid $p ($2) already gone"; return 0; fi
  comm=$(ps -o comm= -p "$p" 2>/dev/null | sed 's#.*/##')
  [ "$comm" = "$2" ] || { echo "own_signal: pid $p is '$comm', not '$2' — refusing"; return 1; }
  kill -"$3" "$p" || return 1
  i=0
  while kill -0 "$p" 2>/dev/null && [ $i -lt "${4:-30}" ]; do sleep 1; i=$((i+1)); done
  echo "own_signal: SIG$3 -> $2 pid $p, gone after ${i}s"
  return 0
}
OWNEOF
chmod +x $S/own_signal.sh
. $S/own_signal.sh
own_pid() { print -r -- "$2" > $S/$1.pid; }   # $1=name $2=pid, at spawn time

# ---------- helpers ----------
PGQ() { docker exec humaux-thread-pg psql -U postgres -d $DB -Atc "$1"; }
mcp() { # $1=tool $2=arguments-json ; prints http_code + body (body may contain memory ids only)
  local id=$RANDOM
  local body="{\"jsonrpc\":\"2.0\",\"id\":$id,\"method\":\"tools/call\",\"params\":{\"name\":\"$1\",\"arguments\":$2,\"_meta\":{\"io.modelcontextprotocol/protocolVersion\":\"2026-07-28\",\"io.modelcontextprotocol/clientInfo\":{\"name\":\"rehearsal\",\"version\":\"1\"},\"io.modelcontextprotocol/clientCapabilities\":{}}}}"
  local gw=${GW_URL:-http://127.0.0.1:8080}   # ADR-0061 D-K: a scratch gateway sets GW_URL
  curl -s -w '\nHTTP %{http_code}\n' -X POST "$gw/mcp" \
    -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' -H 'MCP-Protocol-Version: 2026-07-28' -H 'Mcp-Method: tools/call' -H "Mcp-Name: $1" \
    -H "Origin: $gw" -H "Authorization: Bearer $BEARER" --data "$body"
}

# Same request, a different principal's bearer. zsh `local` is dynamically scoped, so the
# assignment is visible to `mcp` and reverts on return — no global to forget to restore.
mcp_as() { local BEARER=$1; shift; mcp "$@"; }
# §33.10 confirm gate: a destructive op answers `confirmation_required` + `confirm_token` on the
# first call and executes on the second. One definition, so every gated op in this script goes
# through the same two legs instead of each step hand-rolling them.
gated_as() { # $1=bearer $2=tool $3=arguments-json (no confirm_token)
  local b=$1 tool=$2 args=$3
  local first=$(mcp_as "$b" "$tool" "$args")
  local tok=$(print -r -- "$first" | head -1 | python3 -c "import sys,json
try: print(json.load(sys.stdin)['result']['structuredContent'].get('confirm_token',''))
except Exception: print('')" 2>/dev/null)
  if [ -z "$tok" ]; then print -r -- "$first"; return 0; fi
  local args2=$(print -r -- "$args" | python3 -c "import sys,json
a=json.load(sys.stdin); a['confirm_token']=sys.argv[1]; print(json.dumps(a))" "$tok")
  mcp_as "$b" "$tool" "$args2"
}
# structuredContent field by dotted path; empty string when absent, so a missing field is a
# visible assertion failure rather than a shell error.
sc() { python3 -c "import sys,json
try: v=json.load(sys.stdin)['result'].get('structuredContent',{})
except Exception: v=None
for k in sys.argv[1].split('.'):
    v = v.get(k) if isinstance(v,dict) else None
print('' if v is None else (json.dumps(v) if isinstance(v,(dict,list)) else v))" "$1" 2>/dev/null; }
# Card 31 (ADR-0057 D-A): after a drain, A2 must close in memory points on lane A's workspace —
# recall's pipeline.projection reads current=true, visible == points_settled (L), nothing in flight
# (F) or unsettled (Q), and no PROJECTION_INVISIBLE_LOSS. Each check logs every number it graded.
# $2 (optional) grades a recall response already on disk instead of sending a new one.
typeset -gA A2
a2_check() { # $1=label [$2=recall json]
  local f=${2:-$EV/a2_$1.json}
  [ -n "${2:-}" ] || mcp recall "{\"query\":\"which language do we prefer for backend services?\",\"workspace_id\":\"$WS\",\"mode\":\"semantic\"}" > $f 2>&1
  local line=$(head -1 $f | python3 -c "
import sys,json
try:
    sc=json.load(sys.stdin)['result']['structuredContent']; p=sc['pipeline']['projection']; dg=sc['completeness'].get('degradations',[])
    ok=p['current'] is True and p['visible']==p['points_settled'] and p['points_in_flight']==0 and p['points_unsettled']==0 and 'PROJECTION_INVISIBLE_LOSS' not in dg
    print('%d visible=%s L=%s F=%s Q=%s U=%s done=%s current=%s degradations=%s' % (ok, p['visible'], p['points_settled'], p['points_in_flight'], p['points_unsettled'], p['points_expected'], p['done'], p['current'], ','.join(dg) or '-'))
except Exception as e: print('0 unparsed(%s)' % type(e).__name__)")
  A2[$1]=${line%% *}; A2[${1}_n]=${line#* }
  echo "a2 $1: ${A2[${1}_n]} closed=${A2[$1]}" | tee -a $EV/rehearsal.log
}

# ---------- 0. build ----------
step build
BUILD_FLAGS=(); [ "$REHEARSE_PROFILE" = release ] && BUILD_FLAGS=(--release)
# humaux-admin: step observability renders the Watchdog's git_sha from `q deploy.binary`, and step
# admin_probes runs the §4.4 catalog (ADR-0061 D-J / D-K).
cargo build $BUILD_FLAGS -p humaux-gateway -p humaux-retrieval-worker -p humaux-consolidation-worker -p humaux-private-worker -p humaux-maintenance -p humaux-admin -p xtask 2>&1 | tail -2 | tee -a $EV/rehearsal.log
echo "build profile: $REHEARSE_PROFILE ($BIN_DIR)" | tee -a $EV/rehearsal.log

# ---------- 1. seed (stdout kept in a variable only) ----------
# ADR-0060 D-H (card 33b): `--no-lane` — the seed writes tenants, users, domains and keys only; every
# reasoning route comes from the operator doors in step onboard_routes below.
step seed
SEED_OUT=$(cargo run -q -p xtask -- e2e-seed --pepper-hex $PEPPER_HEX --scopes memory:write,context:read --limit 1000 --workspaces 2 \
  --no-lane --processor-id $EGRESS_PROC \
  --collection $COLLECTION --dimension $EMB_DIM --embedding-provider dashscope --embedding-region $EMB_REGION 2>$EV/seed.stderr)
BEARER=$(print -r -- "$SEED_OUT" | sed -n 's/^Authorization: Bearer //p' | head -1)
val() { print -r -- "$SEED_OUT" | grep -iE "^[[:space:]]*$1[[:space:]]*[:=]" | head -1 | sed -E 's/^[^:=]*[:=][[:space:]]*//' | tr -d ' '; }
TENANT=$(val tenant_id); USERID=$(val user_id); WS=$(val workspace_id); RDOM=$(val reasoning_domain_id); APIKEY_ID=$(val api_key_id)
eval "$(print -r -- "$SEED_OUT" | grep -E '^export HUMAUX_(CONSOLIDATION_WORKER|PRIVATE_WORKER|RETRIEVAL_WORKER|GATEWAY)_')"
# card 21: the §15.1 ticket family comes from the binary, not from this file. e2e-seed prints it
# out of `domain::ticket_family::TicketFamily`, the one closed set that owns the triple.
DOMAIN=${HUMAUX_GATEWAY_REMEMBER_DOMAIN:-}; PKIND=${HUMAUX_GATEWAY_REMEMBER_PROJECTION_KIND:-}; PVER=${HUMAUX_GATEWAY_REMEMBER_PROJECTION_VERSION:-}
[ -z "$DOMAIN" -o -z "$PKIND" -o -z "$PVER" ] && { echo "seed: ticket family not emitted (HUMAUX_GATEWAY_REMEMBER_{DOMAIN,PROJECTION_KIND,PROJECTION_VERSION})" | tee -a $EV/rehearsal.log; exit 2; }
[ -z "${HUMAUX_RETRIEVAL_WORKER_EGRESS_PROCESSOR_ID:-}" ] && { echo "seed: retrieval worker egress identity not emitted" | tee -a $EV/rehearsal.log; exit 2; }
echo "ticket family (derived): $DOMAIN/$PKIND/$PVER" | tee -a $EV/rehearsal.log
[ -z "$BEARER" ] && { echo "seed: no bearer parsed" | tee -a $EV/rehearsal.log; exit 2; }
print -r -- "$SEED_OUT" | grep -vE 'Bearer|bearer_|export' | tee -a $EV/seed_ids.txt >/dev/null   # ids only, no secret
echo "seed ok tenant=$TENANT ws=$WS domain=$RDOM" | tee -a $EV/rehearsal.log

# ---------- 1b. seed tenant B (card 24 acceptance gate: TWO tenants on ONE gateway) ----------
# e2e-seed provisions ONE tenant per invocation, so it runs twice with the SAME pepper; one
# gateway process serves N (tenant, workspace) pairs per request since cards 10/11/13. This used
# to live inside the `if SOAK_SECS` block, which meant the 5/5 rehearsal the delivery claim rests
# on was single-tenant and could not witness cross-tenant isolation at all.
# --processor-id is the DEPLOYMENT's egress processor (§7.3), not a per-tenant value: both
# tenants get $EGRESS_PROC or tenant B's distill defers every row forever, silently (card 16 P0).
step seed_b
SEED_OUT_B=$(cargo run -q -p xtask -- e2e-seed --pepper-hex $PEPPER_HEX --scopes memory:write,context:read --limit 1000 --workspaces 2 \
  --no-lane --processor-id $EGRESS_PROC \
  --collection $COLLECTION --dimension $EMB_DIM --embedding-provider dashscope --embedding-region $EMB_REGION 2>$EV/seed_b.stderr)
valb() { print -r -- "$SEED_OUT_B" | grep -iE "^[[:space:]]*$1[[:space:]]*[:=]" | head -1 | sed -E 's/^[^:=]*[:=][[:space:]]*//' | tr -d ' '; }
export BEARER_A="$BEARER"
export BEARER_B=$(print -r -- "$SEED_OUT_B" | sed -n 's/^Authorization: Bearer //p' | head -1)
TENANT_B=$(valb tenant_id); WS_B=$(valb workspace_id); USERID_B=$(valb user_id); RDOM_B=$(valb reasoning_domain_id)
print -r -- "$SEED_OUT_B" | grep -vE 'Bearer|bearer_|export' | tee -a $EV/seed_ids.txt >/dev/null
[ -z "$BEARER_B" ] && { echo "seed_b: no bearer for tenant B" | tee -a $EV/rehearsal.log; exit 2; }
[ "$TENANT_B" = "$TENANT" ] && { echo "seed_b: tenant B is tenant A — isolation cannot be witnessed" | tee -a $EV/rehearsal.log; exit 2; }
echo "tenants: A=$TENANT/$WS  B=$TENANT_B/$WS_B" | tee -a $EV/rehearsal.log

# ---------- 1c. seed tenant C + second workspaces (card 27: 3 tenants × 2 workspaces) ----------
# ADR-0052's gate needs >= 3 tenants × 2 workspaces behind ONE tenant-free --serve process.
# `--workspaces 2` (above, and here) gives each tenant a second workspace with its own membership
# and workspace-bound key; `bearer_2:` lines are secrets and never reach seed_ids.txt.
step seed_c
# Card 32 (ADR-0058 M8 twin): tenant C also gets a second user who owns a second reasoning domain
# with its own admitted lane and key (`bearer_d2:`, a secret) — one tenant, two domains.
SEED_OUT_C=$(cargo run -q -p xtask -- e2e-seed --pepper-hex $PEPPER_HEX --scopes memory:write,context:read --limit 1000 --workspaces 2 --second-domain \
  --no-lane --processor-id $EGRESS_PROC \
  --collection $COLLECTION --dimension $EMB_DIM --embedding-provider dashscope --embedding-region $EMB_REGION 2>$EV/seed_c.stderr)
print -r -- "$SEED_OUT_C" | grep -vE 'Bearer|bearer_|export' | tee -a $EV/seed_ids.txt >/dev/null
seedval() { print -r -- "$1" | grep -iE "^[[:space:]]*$2[[:space:]]*[:=]" | head -1 | sed -E 's/^[^:=]*[:=][[:space:]]*//' | tr -d ' '; }
TENANT_C=$(seedval "$SEED_OUT_C" tenant_id); WS_C=$(seedval "$SEED_OUT_C" workspace_id)
USERID_C=$(seedval "$SEED_OUT_C" user_id); RDOM_C=$(seedval "$SEED_OUT_C" reasoning_domain_id); USERID_C2=$(seedval "$SEED_OUT_C" second_user_id)
export BEARER_C=$(print -r -- "$SEED_OUT_C" | sed -n 's/^Authorization: Bearer //p' | head -1)
WS_A2=$(seedval "$SEED_OUT" workspace_id_2); WS_B2=$(seedval "$SEED_OUT_B" workspace_id_2); WS_C2=$(seedval "$SEED_OUT_C" workspace_id_2)
export BEARER_A2=$(seedval "$SEED_OUT" bearer_2) BEARER_B2=$(seedval "$SEED_OUT_B" bearer_2) BEARER_C2=$(seedval "$SEED_OUT_C" bearer_2)
export BEARER_C_D2=$(seedval "$SEED_OUT_C" bearer_d2); RDOM_C2=$(seedval "$SEED_OUT_C" second_reasoning_domain_id)
for v in TENANT_C WS_C BEARER_C WS_A2 WS_B2 WS_C2 BEARER_A2 BEARER_B2 BEARER_C2 BEARER_C_D2 RDOM_C2 USERID USERID_B USERID_C USERID_C2 RDOM RDOM_B RDOM_C; do
  [ -z "${(P)v}" ] && { echo "seed_c: $v not parsed from e2e-seed --workspaces 2" | tee -a $EV/rehearsal.log; exit 2; }
done
SEEDED="'$TENANT','$TENANT_B','$TENANT_C'"
echo "tenants: C=$TENANT_C/$WS_C  second workspaces: A2=$WS_A2 B2=$WS_B2 C2=$WS_C2" | tee -a $EV/rehearsal.log

# ---------- 1d. reasoning routes through the operator doors (ADR-0060 D-H, card 33b) ----------
# Every domain is routed the way an operator does it: `humaux-maintenance reasoning register`
# (Profile@version + a credential reference) → `bind` (both derived purposes, the R2 projection) →
# `attest-health` (OPERATOR_ATTEST, valid for the whole run, ruling E3 (a)). Tenants A and B reason
# on the first provider and declare ONE vendor account (same --account-ref), so their references may
# share MINIMAX_API_KEY (ADR-0060 D-J); tenant C — both domains; its checks are model-independent —
# reasons on the second provider. The worker's credential map, recipient (uuid=hosts) and region lists
# are composed from the receipts; the workers start only after this step (L15). Receipts hold ids
# and variable NAMES only.
step onboard_routes
MAINT=("$BIN_DIR"/humaux-maintenance)
RADMIN=(--actor rehearsal --reason "card 33b rehearsal onboarding" --ticket C33B-REHEARSAL --step-up-auth rehearsal-local)
ATTEST_SECS=$(( ${SOAK_SECS:-0} + ${SOAK_DRAIN:-150} + 5400 ))
MM_ROUTE=(--provider-id $MM_PROVIDER --provider-model-id $MM_MODEL --model-revision $MM_REV
  --capabilities $PW_CAPABILITIES --account-ref rehearsal-minimax --request-extras '{"reasoning_split":true}'
  --endpoint-ref $MM_URL --region $MM_REGION --service-tier $MM_TIER --egress-processor-id $EGRESS_PROC)
P2_ROUTE=(--provider-id $HUMAUX_LIVE_P2_PROVIDER_ID --provider-model-id $HUMAUX_LIVE_P2_MODEL --model-revision $P2_REV
  --capabilities $HUMAUX_LIVE_P2_CAPABILITIES --account-ref rehearsal-p2 --request-extras "$HUMAUX_LIVE_P2_REQUEST_EXTRAS"
  --endpoint-ref $HUMAUX_LIVE_P2_BASE_URL --region $HUMAUX_LIVE_P2_REGION --service-tier $P2_TIER
  --egress-processor-id $HUMAUX_LIVE_P2_EGRESS_PROCESSOR_ID)
rcpt() { python3 -c "import sys,json
try: print(json.loads(sys.stdin.read().strip().splitlines()[-1]).get(sys.argv[1],''))
except Exception: print('')" "$1"; }
CRED_MAP=""; ROUTE_PROFILES=()
onboard_route() { # $1=route array name $2=key variable NAME $3=tenant $4=owner user $5=reasoning domain
  local out prof ver ref purpose
  out=$("${MAINT[@]}" reasoning register --tenant $3 --owner-user $4 "${(@P)1}" "${RADMIN[@]}" 2>>$EV/onboard.stderr)
  print -r -- "$out" >> $EV/onboard.log
  prof=$(print -r -- "$out" | rcpt profile_id); ver=$(print -r -- "$out" | rcpt profile_version); ref=$(print -r -- "$out" | rcpt credential_ref)
  [ -n "$prof" -a -n "$ver" -a -n "$ref" ] || { echo "onboard_routes: register refused for tenant $3 ($(print -r -- "$out" | tail -1))" | tee -a $EV/rehearsal.log; exit 2; }
  for purpose in PRIVATE_DISTILL_TEXT PRIVATE_CONSOLIDATE; do
    "${MAINT[@]}" reasoning bind --tenant $3 --domain $5 --purpose $purpose --profile $prof --profile-version $ver "${RADMIN[@]}" >> $EV/onboard.log 2>>$EV/onboard.stderr \
      || { echo "onboard_routes: bind $purpose refused for tenant $3 domain $5" | tee -a $EV/rehearsal.log; exit 2; }
  done
  "${MAINT[@]}" reasoning attest-health --tenant $3 --profile $prof --profile-version $ver --valid-for-secs $ATTEST_SECS "${RADMIN[@]}" >> $EV/onboard.log 2>>$EV/onboard.stderr \
    || { echo "onboard_routes: attest-health refused for tenant $3" | tee -a $EV/rehearsal.log; exit 2; }
  ROUTE_PROFILES+=("$3 $prof $ver")
  CRED_MAP="${CRED_MAP:+$CRED_MAP,}$ref=$2"
  echo "onboard_routes: tenant=$3 domain=$5 profile=$prof@$ver credential_ref=$ref -> $2 (valid ${ATTEST_SECS}s)" | tee -a $EV/rehearsal.log
}
onboard_route MM_ROUTE MINIMAX_API_KEY $TENANT $USERID $RDOM
onboard_route MM_ROUTE MINIMAX_API_KEY $TENANT_B $USERID_B $RDOM_B
onboard_route P2_ROUTE $HUMAUX_LIVE_P2_KEY_ENV $TENANT_C $USERID_C $RDOM_C
onboard_route P2_ROUTE $HUMAUX_LIVE_P2_KEY_ENV $TENANT_C $USERID_C2 $RDOM_C2
# ADR-0059 D-I / ADR-0060 D-C, D-J, D-L: the worker serves a reference only if its map names it, dials
# a recipient only at the hosts listed for it, and only in a listed region (deny-only, never selects).
export HUMAUX_PRIVATE_WORKER_CREDENTIALS="$CRED_MAP"
# parse_recipients / parse_regions refuse a repeated entry, so a shared recipient or region is listed once.
if [ "$HUMAUX_LIVE_P2_EGRESS_PROCESSOR_ID" = "$EGRESS_PROC" ]; then
  export HUMAUX_PRIVATE_WORKER_EGRESS_RECIPIENTS="$EGRESS_PROC=$MM_HOST|$HUMAUX_LIVE_P2_HOSTS"
else
  export HUMAUX_PRIVATE_WORKER_EGRESS_RECIPIENTS="$EGRESS_PROC=$MM_HOST,$HUMAUX_LIVE_P2_EGRESS_PROCESSOR_ID=$HUMAUX_LIVE_P2_HOSTS"
fi
if [ "$HUMAUX_LIVE_P2_REGION" = "$MM_REGION" ]; then
  export HUMAUX_PRIVATE_WORKER_REGIONS="$MM_REGION"
else
  export HUMAUX_PRIVATE_WORKER_REGIONS="$MM_REGION,$HUMAUX_LIVE_P2_REGION"
fi
echo "credential map: ${#ROUTE_PROFILES} refs; recipients: ${#${(s:,:)HUMAUX_PRIVATE_WORKER_EGRESS_RECIPIENTS}}; regions: $HUMAUX_PRIVATE_WORKER_REGIONS" | tee -a $EV/rehearsal.log
"${MAINT[@]}" reasoning status --tenant $TENANT_C > $EV/route_status_c.json 2>>$EV/onboard.stderr
echo "route status C: $(python3 -c "import sys,json; print(' '.join(r['purpose']+'='+r['health'] for r in json.load(sys.stdin)['routes']))" < $EV/route_status_c.json 2>/dev/null)" | tee -a $EV/rehearsal.log

# ---------- 2. processes ----------
step processes
rm -f $SOCK/*.sock
# ADR-0061 D-B: one loopback ops port per resident mode (job:mode -> port), never per binary — two
# modes of one binary run at once from one env. Every launch line sets its own mode's key from here,
# and the Prometheus targets are written from it; the alert drill's scratch gateway is not in it.
# Defined before the first launch: every worker launch line and chaos heredoc below reads it.
typeset -A OPS_PORTS
OPS_PORTS=(humaux-gateway:serve 19101 humaux-retrieval-worker:serve-rpc 19102 humaux-retrieval-worker:serve 19103
  humaux-private-worker:serve-rpc 19104 humaux-private-worker:distill-serve 19105 humaux-consolidation-worker:serve 19106
  humaux-maintenance:health-serve 19107 humaux-maintenance:serve 19108)
# The per-mode keys, one line each, reused by every launch of that mode (first spawn, kill -9 recovery,
# the soak's resident spawn and its chaos restart).
OPS_RW_RPC="HUMAUX_RETRIEVAL_WORKER_SERVE_RPC_METRICS_ADDR=127.0.0.1:${OPS_PORTS[humaux-retrieval-worker:serve-rpc]}"
OPS_RW_SERVE="HUMAUX_RETRIEVAL_WORKER_SERVE_METRICS_ADDR=127.0.0.1:${OPS_PORTS[humaux-retrieval-worker:serve]}"
OPS_PW_RPC="HUMAUX_PRIVATE_WORKER_SERVE_RPC_METRICS_ADDR=127.0.0.1:${OPS_PORTS[humaux-private-worker:serve-rpc]}"
OPS_PW_DISTILL="HUMAUX_PRIVATE_WORKER_DISTILL_SERVE_METRICS_ADDR=127.0.0.1:${OPS_PORTS[humaux-private-worker:distill-serve]}"
OPS_CW_SERVE="HUMAUX_CONSOLIDATION_WORKER_SERVE_METRICS_ADDR=127.0.0.1:${OPS_PORTS[humaux-consolidation-worker:serve]}"
# ONE definition per resident process, used by the first spawn AND by the kill -9 recovery in
# step kill9_rotation. A chaos step that restarts a differently-configured process grades a
# deployment nobody ran (the same argument $DS_ENV/$CW_ENV already make for the soak).
start_pw() {
( export PRIVATE_WORKER_PG_DSN="postgres://role_private_worker:${HUMAUX_ROLE_PASSWORD_PRIVATE_WORKER:?}@$PG/$DB" \
    HUMAUX_PRIVATE_WORKER_RPC_SOCKET_PATH=$SOCK/inference.sock HUMAUX_PRIVATE_WORKER_CONSOLIDATION_UID=$MYUID \
    HUMAUX_PRIVATE_WORKER_CREDENTIALS="$HUMAUX_PRIVATE_WORKER_CREDENTIALS" HUMAUX_PRIVATE_WORKER_HEALTH_RENEW_SECS=$PW_HEALTH_RENEW_SECS HUMAUX_PRIVATE_WORKER_HTTP_TIMEOUT_SECS=120 HUMAUX_PRIVATE_WORKER_PERMIT_TTL_SECS=60 \
    HUMAUX_PRIVATE_WORKER_DNS_PINS="$PW_DNS_PINS" $OPS_PW_RPC
  set -a; source /Volumes/data/viral-skill-eval/.env; set +a
  exec "$BIN_DIR"/humaux-private-worker --serve-rpc >> $EV/private-worker.log 2>&1 ) &
own_pid pw $!
}
start_pw; PW_PID=$(cat $S/pw.pid)
start_rw() {
( export HUMAUX_RETRIEVAL_WORKER_PG_DSN="postgres://role_retrieval_worker:${HUMAUX_ROLE_PASSWORD_RETRIEVAL_WORKER:?}@$PG/$DB" \
    HUMAUX_RETRIEVAL_WORKER_RPC_SOCKET_PATH=$SOCK/retrieval.sock HUMAUX_RETRIEVAL_WORKER_GATEWAY_UID=$MYUID \
    HUMAUX_RETRIEVAL_WORKER_EMBEDDING_PROVIDER=dashscope HUMAUX_RETRIEVAL_WORKER_EMBEDDING_MODEL=$EMB_MODEL HUMAUX_RETRIEVAL_WORKER_MODEL_REVISION=$EMB_REV \
    HUMAUX_RETRIEVAL_WORKER_DIMENSION=$EMB_DIM HUMAUX_RETRIEVAL_WORKER_EMBEDDING_VERSION=$EMB_VER HUMAUX_RETRIEVAL_WORKER_REGION=$EMB_REGION HUMAUX_RETRIEVAL_WORKER_MAX_INPUT_TOKENS=$EMB_MAX_TOK \
    HUMAUX_RETRIEVAL_WORKER_QDRANT_HOST=127.0.0.1 HUMAUX_RETRIEVAL_WORKER_QDRANT_PORT=6333 HUMAUX_RETRIEVAL_WORKER_QDRANT_CIDR=127.0.0.1/32 HUMAUX_RETRIEVAL_WORKER_QDRANT_TLS=false \
    HUMAUX_RETRIEVAL_WORKER_CELL_ID=$CELL_ID HUMAUX_RETRIEVAL_WORKER_CALLER=retrieval-worker \
    HUMAUX_RETRIEVAL_WORKER_GITLEAKS_BIN=$GITLEAKS_BIN HUMAUX_RETRIEVAL_WORKER_GITLEAKS_SHA256=$GITLEAKS_SHA HUMAUX_RETRIEVAL_WORKER_GITLEAKS_VERSION=$GITLEAKS_VER \
    $OPS_RW_RPC
  set -a; source $R/.env.local; set +a
  exec "$BIN_DIR"/humaux-retrieval-worker --serve-rpc >> $EV/retrieval-worker.log 2>&1 ) &
own_pid rw $!
}
start_rw; RW_PID=$(cat $S/rw.pid)
# card 27 / ADR-0052: the fifth resident process — ONE tenant-free projection runner for every
# placed tenant. It reads no tenant, workspace or collection (the claim hands each ticket its
# placement); the seven pass keys are its only projection configuration. $1 (optional) overrides
# the Qdrant port: step projection_serve_multi_tenant points it at a closed loopback port to
# simulate a Qdrant outage without touching the shared container.
RP_PASS_ENV="HUMAUX_RETRIEVAL_WORKER_BATCH=16 HUMAUX_RETRIEVAL_WORKER_PER_TENANT_CAP=8 HUMAUX_RETRIEVAL_WORKER_LEASE_SECS=60 \
HUMAUX_RETRIEVAL_WORKER_POLL_INTERVAL_SECS=1 HUMAUX_RETRIEVAL_WORKER_MAX_ATTEMPTS=6 \
HUMAUX_RETRIEVAL_WORKER_BACKOFF_BASE_SECS=30 HUMAUX_RETRIEVAL_WORKER_BACKOFF_MAX_SECS=300"
start_rp() {
( export HUMAUX_RETRIEVAL_WORKER_PG_DSN="postgres://role_retrieval_worker:${HUMAUX_ROLE_PASSWORD_RETRIEVAL_WORKER:?}@$PG/$DB" \
    HUMAUX_RETRIEVAL_WORKER_EMBEDDING_PROVIDER=dashscope HUMAUX_RETRIEVAL_WORKER_EMBEDDING_MODEL=$EMB_MODEL HUMAUX_RETRIEVAL_WORKER_MODEL_REVISION=$EMB_REV \
    HUMAUX_RETRIEVAL_WORKER_DIMENSION=$EMB_DIM HUMAUX_RETRIEVAL_WORKER_EMBEDDING_VERSION=$EMB_VER HUMAUX_RETRIEVAL_WORKER_REGION=$EMB_REGION HUMAUX_RETRIEVAL_WORKER_MAX_INPUT_TOKENS=$EMB_MAX_TOK \
    HUMAUX_RETRIEVAL_WORKER_QDRANT_HOST=127.0.0.1 HUMAUX_RETRIEVAL_WORKER_QDRANT_PORT=${1:-6333} HUMAUX_RETRIEVAL_WORKER_QDRANT_CIDR=127.0.0.1/32 HUMAUX_RETRIEVAL_WORKER_QDRANT_TLS=false \
    HUMAUX_RETRIEVAL_WORKER_CELL_ID=$CELL_ID HUMAUX_RETRIEVAL_WORKER_CALLER=retrieval-worker \
    HUMAUX_RETRIEVAL_WORKER_GITLEAKS_BIN=$GITLEAKS_BIN HUMAUX_RETRIEVAL_WORKER_GITLEAKS_SHA256=$GITLEAKS_SHA HUMAUX_RETRIEVAL_WORKER_GITLEAKS_VERSION=$GITLEAKS_VER \
    $OPS_RW_SERVE
  eval "export $RP_PASS_ENV"
  set -a; source $R/.env.local; set +a
  exec "$BIN_DIR"/humaux-retrieval-worker --serve >> $EV/projection-runner.log 2>&1 ) &
own_pid rp $!
}
start_rp; RP_PID=$(cat $S/rp.pid)
# Card 31 (ADR-0057 D-F): the §78 ProjectionLag threshold has no default — boot-fatal when absent.
# One value, read by start_gw AND by step stall_lag's window, so the assertion grades the deployment.
LAG_SECS=20
# Arguments (all optional; the main gateway passes none): $1 = bind host:port, $2 = ops port,
# $3 = pidfile name, $4 = log. Only step alert_drill passes them, for its scratch gateway G'
# (ADR-0061 D-K): the same configuration on its own listener, origin and ops port.
start_gw() {
local gw_bind=${1:-127.0.0.1:8080} gw_ops=${2:-${OPS_PORTS[humaux-gateway:serve]}} gw_name=${3:-gw} gw_log=${4:-$EV/gateway.log}
( export HUMAUX_GATEWAY_PG_DSN="postgres://role_gateway:${HUMAUX_ROLE_PASSWORD_GATEWAY:?}@$PG/$DB" HUMAUX_GATEWAY_BIND_ADDR=$gw_bind \
    HUMAUX_GATEWAY_CREDENTIAL_PEPPER_HEX=$PEPPER_HEX HUMAUX_GATEWAY_TOKEN_HMAC_KEY=$TOKEN_HMAC_HEX HUMAUX_GATEWAY_ALLOWED_HOSTS=$gw_bind HUMAUX_GATEWAY_ALLOWED_ORIGINS=http://$gw_bind \
    HUMAUX_GATEWAY_MAX_REQUEST_BODY_BYTES=1048576 HUMAUX_GATEWAY_TRUSTED_PROXY_CIDRS= HUMAUX_GATEWAY_MAX_FORWARDED_HOPS=1 HUMAUX_GATEWAY_GLOBAL_DENYLIST= HUMAUX_GATEWAY_GLOBAL_EMERGENCY_ALLOWLIST= \
    HUMAUX_GATEWAY_RESERVATION_TTL_SECONDS=30 HUMAUX_GATEWAY_HANDLER_TIMEOUT_SECONDS=20 HUMAUX_GATEWAY_FINALIZE_TIMEOUT_SECONDS=5 HUMAUX_GATEWAY_REPLAY_TTL_SECONDS=60 \
    HUMAUX_GATEWAY_CONFIRM_TOKEN_TTL_SECONDS=300 HUMAUX_GATEWAY_UNDO_WINDOW_SECONDS=86400 HUMAUX_GATEWAY_MOOD_HALF_LIFE_SECONDS=21600 HUMAUX_GATEWAY_PROJECTION_LAG_SECONDS=$LAG_SECS HUMAUX_GATEWAY_ENUMERATION_TTL_SECONDS=900 HUMAUX_GATEWAY_ENUMERATION_MANIFEST_CAP=1000 \
    HUMAUX_GATEWAY_REMEMBER_SCOPE_KIND=workspace \
    HUMAUX_GATEWAY_REMEMBER_DOMAIN=$DOMAIN HUMAUX_GATEWAY_REMEMBER_PROJECTION_KIND=$PKIND HUMAUX_GATEWAY_REMEMBER_PROJECTION_VERSION=$PVER \
    HUMAUX_GATEWAY_REMEMBER_REASONING_DOMAIN_ID=$RDOM HUMAUX_GATEWAY_REMEMBER_TOKEN_TTL_SECONDS=60 HUMAUX_GATEWAY_REMEMBER_DATA_CLASS=INTERNAL \
    HUMAUX_GATEWAY_REMEMBER_VISIBILITY_CLASS=WORKSPACE_SHARED HUMAUX_GATEWAY_REMEMBER_EVENT_KIND=USER_MESSAGE \
    HUMAUX_GATEWAY_CONTEXT_TOTAL_TOKENS=2048 HUMAUX_GATEWAY_CONTEXT_MANDATORY_TOKENS=1024 \
    HUMAUX_GATEWAY_RETRIEVAL_RPC_SOCKET_PATH=$SOCK/retrieval.sock HUMAUX_GATEWAY_RETRIEVAL_RPC_PERMIT_TTL_SECONDS=60 \
    HUMAUX_GATEWAY_EMBEDDING_DIMENSION=$EMB_DIM HUMAUX_GATEWAY_EMBEDDING_VERSION=$EMB_VER \
    HUMAUX_GATEWAY_QDRANT_HOST=127.0.0.1 HUMAUX_GATEWAY_QDRANT_PORT=6333 HUMAUX_GATEWAY_QDRANT_CIDR=127.0.0.1/32 HUMAUX_GATEWAY_QDRANT_TLS=false \
    HUMAUX_GATEWAY_CELL_ID=$CELL_ID HUMAUX_GATEWAY_CALLER_ID=gateway \
    HUMAUX_GATEWAY_METRICS_ADDR=127.0.0.1:$gw_ops HUMAUX_GATEWAY_READINESS_REFRESH_SECONDS=2 \
    HUMAUX_GATEWAY_RATE_PREAUTH_IP_CAPACITY=100 HUMAUX_GATEWAY_RATE_PREAUTH_IP_REFILL_PER_SECOND=100 HUMAUX_GATEWAY_RATE_CREDENTIAL_CAPACITY=100 HUMAUX_GATEWAY_RATE_CREDENTIAL_REFILL_PER_SECOND=100 HUMAUX_GATEWAY_RATE_USER_CAPACITY=100 HUMAUX_GATEWAY_RATE_USER_REFILL_PER_SECOND=100 HUMAUX_GATEWAY_RATE_TENANT_CAPACITY=100 HUMAUX_GATEWAY_RATE_TENANT_REFILL_PER_SECOND=100 HUMAUX_GATEWAY_RATE_OPERATION_CAPACITY=100 HUMAUX_GATEWAY_RATE_OPERATION_REFILL_PER_SECOND=100
  exec "$BIN_DIR"/humaux-gateway >> $gw_log 2>&1 ) &
own_pid $gw_name $!
}
start_gw; GW_PID=$(cat $S/gw.pid)
# ADR-0061 D-D / E7: the one resident sampler of the §41.2 SQL-derived health gauges
# (`ops.health_snapshot()` as role_maintenance), on its own mode's ops key. No other process
# exports those gauges, so exactly one instance runs.
start_mh() {
( export HUMAUX_MAINTENANCE_PG_DSN="postgres://role_maintenance:${HUMAUX_ROLE_PASSWORD_MAINTENANCE:?}@$PG/$DB" \
    HUMAUX_MAINTENANCE_HEALTH_SERVE_METRICS_ADDR=127.0.0.1:${OPS_PORTS[humaux-maintenance:health-serve]} HUMAUX_MAINTENANCE_HEALTH_SAMPLE_SECONDS=5
  exec "$BIN_DIR"/humaux-maintenance health serve >> $EV/maintenance-health.log 2>&1 ) &
own_pid mh $!
}
start_mh; MH_PID=$(cat $S/mh.pid)
# ADR-0062 D-T / ruling E8 (card 35): the resident maintenance daemon `humaux-maintenance --serve` (the eighth ops
# pair) works ONLY on this run's own throwaway database, never on $DB: on the shared dev database LOST, REISSUE and
# REDRIVE would act on real tenants, and the confirm door's unconsumed branch ignores any retention. The database
# is created through the owner psql path, migrated with `xtask migrate --dsn`, seeded with eight tenants of
# purgeable work plus one orphan ticket each, and dropped WITH (FORCE) on every exit path (md_teardown; obs_stop
# calls it once the observability trap replaces this one).
MD_DB=humaux_thread_c35_rh_$$
MDQ() { docker exec humaux-thread-pg psql -U postgres -d $MD_DB -Atc "$1"; }
md_teardown() {
  [ -f $S/md.pid ] && own_signal $S/md.pid humaux-maintenance TERM 30
  rm -f $S/md.pid
  docker exec humaux-thread-pg psql -U postgres -d postgres -qc "DROP DATABASE IF EXISTS $MD_DB WITH (FORCE)" >/dev/null 2>&1
  return 0
}
rm -f $S/md.pid
trap md_teardown EXIT
# Rows per tenant per purge table: enough that purging outlasts the soak's second chaos round (the daemon's hook
# runs second), so its kill -9 lands mid-purge; 6 without a soak. LIMIT 2 x 3 tenants per run every 3 s.
MD_K=$(( ${SOAK_SECS:-0} > 0 ? ${SOAK_SECS:-0} / 8 : 6 ))
# ONE definition, used by the first spawn, the kill -9 restart below and the soak's chaos hook. Retentions 0 so
# every seeded row is purgeable at once; LOST_AFTER = LAG_SECS + 1 (the boot relation, ADR-0057 D-F); the budget
# window is the distill worker's own value ($DS_ENV); the password stays a reference until the subshell runs.
MD_ENV="export HUMAUX_MAINTENANCE_PG_DSN=\"postgres://role_maintenance:\${HUMAUX_ROLE_PASSWORD_MAINTENANCE:?}@$PG/$MD_DB\" \
HUMAUX_MAINTENANCE_SERVE_METRICS_ADDR=127.0.0.1:${OPS_PORTS[humaux-maintenance:serve]} \
HUMAUX_MAINTENANCE_SERVE_CYCLE_SECONDS=1 HUMAUX_MAINTENANCE_SERVE_TENANTS_PER_RUN=3 \
HUMAUX_MAINTENANCE_SERVE_LOST_EVERY_SECONDS=3 HUMAUX_MAINTENANCE_SERVE_LOST_LIMIT=2 \
HUMAUX_MAINTENANCE_SERVE_QUOTA_RESERVATIONS_EVERY_SECONDS=3 HUMAUX_MAINTENANCE_SERVE_QUOTA_RESERVATIONS_LIMIT=2 \
HUMAUX_MAINTENANCE_SERVE_PROVIDER_BUDGETS_EVERY_SECONDS=3 HUMAUX_MAINTENANCE_SERVE_PROVIDER_BUDGETS_LIMIT=2 \
HUMAUX_MAINTENANCE_SERVE_CONFIRM_TOKENS_EVERY_SECONDS=3 HUMAUX_MAINTENANCE_SERVE_CONFIRM_TOKENS_LIMIT=2 \
HUMAUX_MAINTENANCE_SERVE_SNAPSHOTS_EVERY_SECONDS=3 HUMAUX_MAINTENANCE_SERVE_SNAPSHOTS_LIMIT=2 \
HUMAUX_MAINTENANCE_SERVE_RATE_BUCKETS_EVERY_SECONDS=3 HUMAUX_MAINTENANCE_SERVE_RATE_BUCKETS_LIMIT=2 \
HUMAUX_MAINTENANCE_SERVE_JOBS_EVERY_SECONDS=3 HUMAUX_MAINTENANCE_SERVE_JOBS_LIMIT=2 \
HUMAUX_MAINTENANCE_SERVE_REISSUE_EVERY_SECONDS=3 HUMAUX_MAINTENANCE_SERVE_REISSUE_LIMIT=2 \
HUMAUX_MAINTENANCE_SERVE_REDRIVE_EVERY_SECONDS=3 HUMAUX_MAINTENANCE_SERVE_REDRIVE_LIMIT=2 \
HUMAUX_MAINTENANCE_SERVE_LOST_AFTER_SECONDS=$((LAG_SECS + 1)) HUMAUX_GATEWAY_PROJECTION_LAG_SECONDS=$LAG_SECS \
HUMAUX_MAINTENANCE_SERVE_CONFIRM_TOKENS_CONSUMED_RETENTION_SECONDS=0 HUMAUX_MAINTENANCE_SERVE_RATE_BUCKETS_IDLE_SECONDS=0 \
HUMAUX_MAINTENANCE_SERVE_JOBS_DONE_RETENTION_SECONDS=0 HUMAUX_MAINTENANCE_SERVE_JOBS_DEAD_RETENTION_SECONDS=0 \
HUMAUX_PRIVATE_WORKER_DISTILL_BUDGET_WINDOW_SECS=60 \
HUMAUX_MAINTENANCE_SERVE_REISSUE_COOLDOWN_SECONDS=2 HUMAUX_MAINTENANCE_SERVE_REDRIVE_COOLDOWN_SECONDS=2"
# (MD_COOLDOWN in step maintenance_drain is REISSUE_COOLDOWN_SECONDS above.)
# ADR-0062 E8: the database the daemon writes, read through the daemon's own DSN — $MD_ENV evaluated in a subshell
# of this shell exactly as start_md does, so a DSN left out of $MD_ENV reads the inherited one (line 21, $DB).
md_daemon_db() {
  ( eval "$MD_ENV"
    python3 -c "
import os, psycopg2
c = psycopg2.connect(os.environ['HUMAUX_MAINTENANCE_PG_DSN']); cur = c.cursor(); cur.execute('select current_database()'); print(cur.fetchone()[0])" ) 2>/dev/null
}
# Refuses to spawn the daemon unless that database is this run's throwaway one: the check runs before any write.
start_md() {
  MD_SPAWN_DB=$(md_daemon_db)
  if [ "$MD_SPAWN_DB" != "$MD_DB" ] || [ "$MD_SPAWN_DB" = "$DB" ]; then
    echo "REFUSED humaux-maintenance --serve: its DSN reads database '$MD_SPAWN_DB', not the throwaway $MD_DB" | tee -a $EV/rehearsal.log
    MD_SPAWN_REFUSED=1
    return 1
  fi
( eval "$MD_ENV"
  exec "$BIN_DIR"/humaux-maintenance --serve >> $EV/maintenance-serve.log 2>&1 ) &
own_pid md $!
}
MD_SPAWN_REFUSED=0
MD_SETUP_OK=0
if docker exec humaux-thread-pg psql -U postgres -d postgres -qc "CREATE DATABASE $MD_DB" >> $EV/rehearsal.log 2>&1 \
   && "$BIN_DIR"/xtask migrate --dsn "postgres://postgres:${HUMAUX_DEV_PG_SUPERUSER_PASSWORD:?}@$PG/$MD_DB" > $EV/maintenance-migrate.log 2>&1 \
   && docker exec -i humaux-thread-pg psql -U postgres -d $MD_DB -v ON_ERROR_STOP=1 -v k=$MD_K -1 -q >> $EV/maintenance-seed.log 2>&1 <<'MDSQL'
SELECT set_config('c35.k', :'k', false);
-- Eight tenants, each: k expired confirm tokens, k expired snapshots (3 items each), k idle full rate buckets,
-- k old DONE/DEAD jobs (no calls, no links, not distill), and one orphan ISSUED ticket whose Evidence carries an
-- indexable memory (the reissue door's input; ADR-0062 D-L / D-N). Owner SQL, one transaction.
DO $do$
DECLARE
  k int := current_setting('c35.k')::int;
  t uuid; u uuid; w uuid; s uuid; d uuid; e uuid; m uuid; sc uuid;
BEGIN
  FOR i IN 1..8 LOOP
    INSERT INTO control.tenants (name) VALUES ('c35 rehearsal maintenance ' || i) RETURNING tenant_id INTO t;
    INSERT INTO control.users (user_id) VALUES (gen_random_uuid()) RETURNING user_id INTO u;
    INSERT INTO control.workspaces (tenant_id, name) VALUES (t, 'c35 rh') RETURNING workspace_id INTO w;
    INSERT INTO control.confirm_tokens (tenant_id, user_id, operation, target_id, nonce_sha256, issued_at,
        expires_at, workspace_id)
      SELECT t, u, 'c35.rh', gen_random_uuid(), sha256(convert_to(gen_random_uuid()::text, 'UTF8')),
        now() - interval '1 day', now() - interval '1 minute', w FROM generate_series(1, k);
    FOR j IN 1..k LOOP
      INSERT INTO ops.selection_snapshots (tenant_id, query_fingerprint, expires_at)
        VALUES (t, 'c35-rh', now() - interval '1 minute') RETURNING selection_snapshot_id INTO s;
      INSERT INTO ops.selection_snapshot_items (selection_snapshot_id, tenant_id, item_id, ordinal)
        SELECT s, t, gen_random_uuid(), g - 1 FROM generate_series(1, 3) g;
    END LOOP;
    INSERT INTO control.rate_buckets (tenant_id, subject_kind, subject_id, operation, bucket_key, capacity, tokens,
        refill_per_second, updated_at)
      SELECT t, 'tenant', 'c35-rh-' || g, 'mcp.read', 'default', 5, 5, 1, now() - interval '2 hours'
      FROM generate_series(1, k) g;
    INSERT INTO ops.jobs (tenant_id, job_type, status, idempotency_key, created_at)
      SELECT t, 'c35.rehearsal', CASE WHEN g % 2 = 0 THEN 'DONE' ELSE 'DEAD' END, gen_random_uuid()::text,
        now() - interval '2 hours' FROM generate_series(1, k) g;
    sc := gen_random_uuid();
    INSERT INTO projection.stream_checkpoints (tenant_id, scope_kind, scope_id, domain, projection_kind,
        projection_version, issued_highwater) VALUES (t, 'workspace', sc, 'code', 'retrieval_card', 'v1', 1);
    INSERT INTO projection.stream_log (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version,
        stream_seq, commit_seq, state, issued_at)
      VALUES (t, 'workspace', sc, 'code', 'retrieval_card', 'v1', 1, i, 'ISSUED', now() - interval '1 hour');
    INSERT INTO control.private_reasoning_domains (tenant_id, name) VALUES (t, 'c35 rh')
      RETURNING reasoning_domain_id INTO d;
    INSERT INTO private.evidence_objects (tenant_id, evidence_kind, payload_sha256, data_class, origin_class,
        visibility_class, reasoning_domain_id)
      VALUES (t, 'EVENT', sha256(convert_to('c35-rh-' || i, 'UTF8')), 'INTERNAL', 'DirectUserInput',
        'TENANT_SHARED', d) RETURNING evidence_id INTO e;
    INSERT INTO private.events (event_id, event_kind, payload) VALUES (e, 'USER_MESSAGE', '{}');
    -- DONE: the Evidence was distilled; only then is an ISSUED ticket an orphan (a PENDING / PROCESSING carrier is
    -- held back by the claim on purpose and sweep_lost leaves it, ADR-0062 D-L).
    INSERT INTO ops.outbox (tenant_id, commit_seq, stream_seq, event_type, evidence_id, status)
      VALUES (t, i, 1, 'EVIDENCE_ACCEPTED', e, 'DONE');
    INSERT INTO private.memory_records (tenant_id, memory_type, content, visibility_class, authority_class,
        confidence, status, asserted_at)
      VALUES (t, 'NOTE', '{"title":"c35 rehearsal"}', 'TENANT_SHARED', 'PrivateKnowledge', 0.9, 'active', now())
      RETURNING memory_id INTO m;
    INSERT INTO private.memory_evidence (memory_id, evidence_id, role, ordinal) VALUES (m, e, 'PRIMARY', 0);
  END LOOP;
  -- The orphans took commit_seq 1..8 by hand; the reissue door draws from the sequence.
  PERFORM setval('ops.commit_seq_seq', 100);
END $do$;
MDSQL
then
  MD_SETUP_OK=1
fi
# Seeded counts per purge door, read once before the daemon starts (the receipts-balance baseline).
MD_SEEDED=$(MDQ "select (select count(*) from control.confirm_tokens where operation='c35.rh')||' '||(select count(*) from ops.selection_snapshots where query_fingerprint='c35-rh')||' '||(select count(*) from control.rate_buckets where subject_id like 'c35-rh-%')||' '||(select count(*) from ops.jobs where job_type='c35.rehearsal')")
echo "maintenance daemon db: $MD_DB setup_ok=$MD_SETUP_OK k=$MD_K seeded(confirm snapshots buckets jobs)=$MD_SEEDED orphans=$(MDQ "select count(*) from projection.stream_log where state='ISSUED'") migrate: $(tail -1 $EV/maintenance-migrate.log)" | tee -a $EV/rehearsal.log
print -r -- $MD_DB > $S/md.db   # for the TW twin's safety net: the one database name this run may drop
for i in $(seq 1 30); do code=$(curl -s -o /dev/null -w '%{http_code}' -X POST http://127.0.0.1:8080/mcp -H 'Origin: http://127.0.0.1:8080' --data '{}' 2>/dev/null); [ "$code" != "000" ] && break; sleep 1; done
echo "gateway http=$code pw=$PW_PID rw=$RW_PID rp=$RP_PID gw=$GW_PID mh=$MH_PID" | tee -a $EV/rehearsal.log
ls -la $SOCK | tee -a $EV/rehearsal.log

# ---------- 2b. readiness gate (card 15 / ADR-0037; docs/ops/supervision.md §1) ----------
# No traffic is sent until every process answers its OWN probe. A rehearsal that starts writing
# while a dependency is still coming up measures the race, not the system.
step readyz
# Each probe uses exactly the keys ADR-0037 says that probe needs — nothing more, and no tenant
# id for the two derived workers (ADR-0036 / card 14 env contract).
rw_readyz() { ( export HUMAUX_RETRIEVAL_WORKER_PG_DSN="postgres://role_retrieval_worker:${HUMAUX_ROLE_PASSWORD_RETRIEVAL_WORKER:?}@$PG/$DB" \
    HUMAUX_RETRIEVAL_WORKER_QDRANT_HOST=127.0.0.1 HUMAUX_RETRIEVAL_WORKER_QDRANT_PORT=6333 \
    HUMAUX_RETRIEVAL_WORKER_QDRANT_CIDR=127.0.0.1/32 HUMAUX_RETRIEVAL_WORKER_QDRANT_TLS=false \
    HUMAUX_RETRIEVAL_WORKER_CELL_ID=$CELL_ID HUMAUX_RETRIEVAL_WORKER_CALLER=retrieval-worker
  exec "$BIN_DIR"/humaux-retrieval-worker --readyz ) }
pw_readyz() { ( export PRIVATE_WORKER_PG_DSN="postgres://role_private_worker:${HUMAUX_ROLE_PASSWORD_PRIVATE_WORKER:?}@$PG/$DB"
  exec "$BIN_DIR"/humaux-private-worker --readyz ) }
cw_readyz() { ( export CONSOLIDATION_WORKER_PG_DSN="postgres://role_consolidation_worker:${HUMAUX_ROLE_PASSWORD_CONSOLIDATION_WORKER:?}@$PG/$DB" \
    HUMAUX_CONSOLIDATION_WORKER_RPC_SOCKET_PATH=$SOCK/inference.sock
  exec "$BIN_DIR"/humaux-consolidation-worker --readyz ) }
gw_readyz() { curl -fsS -o /dev/null http://127.0.0.1:8080/readyz; }
READY_OK=0; READY_BAD=0
wait_ready() { # $1=label $2=probe function name; polls until exit 0 or 90s
  local label=$1 probe=$2 i=0 out=
  while [ $i -lt 90 ]; do
    out=$($probe 2>&1) && { echo "READY $label after ${i}s: $out" | tee -a $EV/rehearsal.log; READY_OK=$((READY_OK+1)); return 0; }
    sleep 1; i=$((i+1))
  done
  # §4.4 坑5: a probe that could not reach its object NAMES the object; never report it as green.
  echo "NOT READY $label after ${i}s: $out" | tee -a $EV/rehearsal.log; READY_BAD=$((READY_BAD+1)); return 1
}
wait_ready private-worker pw_readyz
wait_ready retrieval-worker rw_readyz
# The projection runner's own readiness: its process is alive (a --serve that died on config is
# not ready) AND the dependencies it needs answer the same one-shot probe (ADR-0037).
rp_readyz() { kill -0 "$(cat $S/rp.pid)" 2>/dev/null || { echo "projection runner pid $(cat $S/rp.pid) is not running"; return 1; }; rw_readyz; }
wait_ready projection-runner rp_readyz
wait_ready consolidation-worker cw_readyz   # dials the private worker's UDS from the side that uses it
wait_ready gateway gw_readyz
# ADR-0061 D-D: health serve answers 200 only while its latest sample succeeded and is fresh.
mh_readyz() { curl -fsS -o /dev/null http://127.0.0.1:${OPS_PORTS[humaux-maintenance:health-serve]}/metrics; }
wait_ready maintenance-health-serve mh_readyz
# ADR-0062 D-A: the daemon answers 200 only after a finished cycle with no failed call.
md_readyz() { curl -fsS -o /dev/null http://127.0.0.1:${OPS_PORTS[humaux-maintenance:serve]}/metrics; }
# Started here, not with the others: the kill -9 below must land while the doors still have seeded rows.
start_md; MD_PID=$(cat $S/md.pid)
wait_ready maintenance-serve md_readyz
# ADR-0062 D-E / D-T: kill -9 while the doors are purging (the first receipt is in, rows are left), then the
# same daemon again. Every purge is one statement with its receipt, so nothing can half-apply; the receipts
# balance in step maintenance_drain grades it.
MD_LEFT_SQL="select (select count(*) from control.confirm_tokens where operation='c35.rh')+(select count(*) from ops.selection_snapshots where query_fingerprint='c35-rh')+(select count(*) from control.rate_buckets where subject_id like 'c35-rh-%')+(select count(*) from ops.jobs where job_type='c35.rehearsal')"
MD_K9_T0=$(date +%s); MD_K9_RECEIPTS=0
while [ $(( $(date +%s) - MD_K9_T0 )) -le 60 ]; do
  MD_K9_RECEIPTS=$(MDQ "select count(*) from ops.maintenance_receipts"); [ "${MD_K9_RECEIPTS:-0}" -gt 0 ] 2>/dev/null && break; sleep 0.2
done
own_signal $S/md.pid humaux-maintenance 9 15 | tee -a $EV/rehearsal.log
MD_K9_LEFT=$(MDQ "$MD_LEFT_SQL")
start_md
wait_ready maintenance-serve-after-kill9 md_readyz
echo "maintenance daemon: kill -9 after ${MD_K9_RECEIPTS:-0} receipt(s) with ${MD_K9_LEFT:-?} seeded row(s) left; restarted pid $(cat $S/md.pid)" | tee -a $EV/rehearsal.log

# ---------- 2c. observability: Prometheus, Alertmanager, collector (ADR-0061 D-G / D-K) ----------
# The pinned host binaries (deploy/prometheus/pinned-tool.sh: sha256 + version, never PATH; exit 2
# = a pin variable unset) run the repo's own configs, rendered only where the deployer contract in
# prometheus.yml / alertmanager.yml says: the targets file (exactly the eight OPS_PORTS entries),
# the rule paths, the Alertmanager address, the git sha, the webhook URL files. Every listener is
# loopback and owned through its pidfile; the readiness waits below are part of the gate above.
step observability
PT=(sh $R/deploy/prometheus/pinned-tool.sh)
PROM_PORT=19190; AM_PORT=19193; SINK_PORT=19194; OBS=$S/prom
obs_stop() { # every observability process this script started, each through its own pidfile
  local p
  for p in "promd prometheus" "gwd humaux-gateway" "prom prometheus" "am alertmanager" "otel otelcol" "sink $(cat $S/sink.comm 2>/dev/null)"; do
    set -- ${=p}; [ -f $S/$1.pid ] && own_signal $S/$1.pid "$2" TERM 30
  done
  md_teardown   # card 35: the daemon and its throwaway database go on every exit path too
  return 0
}
# A previous run's pidfiles are dropped, never acted on: a PID recorded then may name someone else's
# process now (the sink's `Python` comm is not distinctive). A leftover listener makes the start
# below fail on its port, loudly. Every exit path of THIS run stops its own processes (trap).
rm -f $S/promd.pid $S/gwd.pid $S/prom.pid $S/am.pid $S/otel.pid $S/sink.pid $S/sink.comm
trap obs_stop EXIT
rm -rf $OBS; mkdir -p $OBS/targets $OBS/data $OBS/am-data
DEPLOY_SHA=$("$BIN_DIR"/humaux-admin q deploy.binary 2>>$EV/rehearsal.log | python3 -c "import sys,json; print(json.load(sys.stdin)['detail']['git_sha'])" 2>/dev/null)
echo "observability: deployed git_sha (humaux-admin q deploy.binary) = ${DEPLOY_SHA:-NONE}" | tee -a $EV/rehearsal.log
python3 - $OBS/targets/humaux.json ${(kv)OPS_PORTS} <<'PYEOF'
import json, sys
kv = sys.argv[2:]
t = [{"targets": ["127.0.0.1:%s" % kv[i + 1]], "labels": dict(zip(("job", "mode"), kv[i].split(":", 1)))}
     for i in range(0, len(kv), 2)]
json.dump(sorted(t, key=lambda e: (e["labels"]["job"], e["labels"]["mode"])), open(sys.argv[1], "w"), indent=1)
PYEOF
sed -e "s#__HUMAUX_GIT_SHA__#${DEPLOY_SHA:-__HUMAUX_GIT_SHA__}#" -e "s#\"127.0.0.1:9093\"#\"127.0.0.1:$AM_PORT\"#" \
    -e "s#\"targets/\\*.json\"#\"$OBS/targets/*.json\"#" -e "s#^  - \\([a-z]*\\.rules\\.yml\\)\$#  - $R/deploy/prometheus/\\1#" \
    $R/deploy/prometheus/prometheus.yml > $OBS/prometheus.yml
print -r -- "http://127.0.0.1:$SINK_PORT/log" > $OBS/log_sink.url
print -r -- "http://127.0.0.1:$SINK_PORT/watchdog" > $OBS/watchdog.url
sed -e "s#__HUMAUX_ALERTMANAGER_LOG_SINK_URL_FILE__#$OBS/log_sink.url#" -e "s#__HUMAUX_ALERTMANAGER_WATCHDOG_URL_FILE__#$OBS/watchdog.url#" \
    $R/deploy/prometheus/alertmanager.yml > $OBS/alertmanager.yml
OBS_CFG=$( { $PT promtool check config $OBS/prometheus.yml && $PT amtool check-config $OBS/alertmanager.yml; } >> $EV/observability.log 2>&1; echo $?)
echo "observability: targets=$(python3 -c "import json,sys; print(len(json.load(open(sys.argv[1]))))" $OBS/targets/humaux.json) rendered configs check=$OBS_CFG placeholders_left=$(cat $OBS/prometheus.yml $OBS/alertmanager.yml | grep -v '^ *#' | grep -c '__HUMAUX')" | tee -a $EV/rehearsal.log
# The webhook receiver: every POST body becomes one compact JSON line {"path", "body"} in the evidence.
cat > $S/alert_sink.py <<'PYEOF'
#!/usr/bin/env python3
# alert_sink.py <port> <jsonl>: the rehearsal's Alertmanager webhook receiver (ADR-0061 D-K), loopback only.
import json, sys
from http.server import BaseHTTPRequestHandler, HTTPServer
port, out = int(sys.argv[1]), sys.argv[2]
class Sink(BaseHTTPRequestHandler):
    def do_POST(self):
        raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        try:
            body = json.loads(raw)
        except ValueError:
            body = raw.decode("utf-8", "replace")
        with open(out, "a") as f:
            f.write(json.dumps({"path": self.path, "body": body}, separators=(",", ":")) + "\n")
        self.send_response(200)
        self.end_headers()
    def log_message(self, *args):
        pass
HTTPServer(("127.0.0.1", port), Sink).serve_forever()
PYEOF
: > $EV/alert_receipts.jsonl
( exec python3 $S/alert_sink.py $SINK_PORT $EV/alert_receipts.jsonl >> $EV/alert_sink.log 2>&1 ) &
own_pid sink $!; sleep 1; ps -o comm= -p "$(cat $S/sink.pid)" | sed 's#.*/##' > $S/sink.comm   # own_signal's expected name, read at spawn
( exec "${PT[@]}" alertmanager --config.file=$OBS/alertmanager.yml --storage.path=$OBS/am-data \
    --web.listen-address=127.0.0.1:$AM_PORT --cluster.listen-address= >> $EV/alertmanager.log 2>&1 ) &
own_pid am $!
( exec "${PT[@]}" prometheus --config.file=$OBS/prometheus.yml --storage.tsdb.path=$OBS/data \
    --storage.tsdb.retention.time=30d --web.listen-address=127.0.0.1:$PROM_PORT >> $EV/prometheus.log 2>&1 ) &
own_pid prom $!
( exec "${PT[@]}" otelcol --config=$R/deploy/prometheus/otel-collector.yml >> $EV/otelcol.log 2>&1 ) &
own_pid otel $!
am_ready() { curl -fsS http://127.0.0.1:$AM_PORT/-/ready; }
prom_ready() { curl -fsS http://127.0.0.1:$PROM_PORT/-/ready; }
otel_ready() { curl -fsS -o /dev/null http://127.0.0.1:8888/metrics; }
# §42.1: the Watchdog reaches its own receiver carrying the deployed git sha (external_labels).
WD_OK=0
watchdog_receipt() {
  python3 - $EV/alert_receipts.jsonl "${DEPLOY_SHA:-}" <<'PYEOF'
import json, sys
want = sys.argv[2]
for line in open(sys.argv[1]):
    r = json.loads(line)
    body = r["body"] if isinstance(r["body"], dict) else {}
    for a in body.get("alerts", []):
        if a.get("labels", {}).get("alertname") == "Watchdog" and r["path"] == "/watchdog":
            sha = a["labels"].get("git_sha")
            print("watchdog receipt git_sha=%s status=%s" % (sha, a.get("status")))
            sys.exit(0 if want and sha == want and "__HUMAUX" not in sha else 1)
print("no Watchdog receipt yet")
sys.exit(1)
PYEOF
}
wait_ready alertmanager am_ready
wait_ready prometheus prom_ready
wait_ready otel-collector otel_ready
wait_ready watchdog-receipt watchdog_receipt && WD_OK=1
READY_BAD_BEFORE_TRAFFIC=$READY_BAD   # frozen here; step kill9_rotation reuses wait_ready
echo "readiness: $READY_OK ready, $READY_BAD not ready" | tee -a $EV/rehearsal.log
[ $READY_BAD -gt 0 ] && echo "readiness gate failed — traffic below is measuring a race" | tee -a $EV/rehearsal.log

# ---------- 3. remember ×2 ----------
step remember
mcp remember "{\"operation\":\"put\",\"content\":\"New backend services must expose a health endpoint before any traffic is routed to them.\",\"idempotency_key\":\"$(uuidgen | tr A-Z a-z)\",\"workspace_id\":\"$WS\"}" | tee $EV/remember_1.json | tail -1
mcp remember "{\"operation\":\"put\",\"content\":\"Rust is the preferred language for backend services because of memory safety and predictable performance.\",\"idempotency_key\":\"$(uuidgen | tr A-Z a-z)\",\"workspace_id\":\"$WS\"}" | tee $EV/remember_2.json | tail -1
PGQ "select 'evidence='||count(*) from private.evidence_objects where tenant_id='$TENANT'" | tee -a $EV/rehearsal.log
PGQ "select 'memory_records='||count(*) from private.memory_records where tenant_id='$TENANT'" | tee -a $EV/rehearsal.log
# Tenant B, same gateway process, same request path, a corpus that does NOT overlap A's — an
# isolation assertion over two empty or two identical corpora witnesses nothing.
mcp_as "$BEARER_B" remember "{\"operation\":\"put\",\"content\":\"Tenant B ships its inventory service on a weekly train every Thursday.\",\"idempotency_key\":\"$(uuidgen | tr A-Z a-z)\",\"workspace_id\":\"$WS_B\"}" | tee $EV/remember_b_1.json | tail -1
mcp_as "$BEARER_B" remember "{\"operation\":\"put\",\"content\":\"Tenant B keeps warehouse stock counts reconciled nightly against the ledger.\",\"idempotency_key\":\"$(uuidgen | tr A-Z a-z)\",\"workspace_id\":\"$WS_B\"}" | tee $EV/remember_b_2.json | tail -1
PGQ "select 'evidence_b='||count(*) from private.evidence_objects where tenant_id='$TENANT_B'" | tee -a $EV/rehearsal.log

# ---------- 3b. distill (evidence → memories, real MiniMax; private worker owns it) ----------
step distill
bounded() { # $1=seconds, rest=command ; runs in background and kills after the bound
  local secs=$1; shift; "$@" & local pid=$!; local i=0
  while kill -0 $pid 2>/dev/null && [ $i -lt $secs ]; do sleep 1; i=$((i+1)); done
  if kill -0 $pid 2>/dev/null; then echo "bounded: killing after ${secs}s" ; kill $pid; fi; wait $pid 2>/dev/null; return $?
}
# One definition, called by this step AND by `drain_all` after every later write. ADR-0036: the
# distill dispatch is tenant-free, so one pass serves every tenant (ADR-0058: four seats, lease 30,
# hard deadline 300 = 2 x (HTTP 120 + lease 30)). One pass does NOT settle a job whose call hit a
# provider transient: ADR-0058 D-F settles it RETRY (PENDING, attempt counted, class RETRY_WAIT,
# backoff 30/60 s). The chain run of 2026-10-02 lost tenant B's whole corpus that way (two fast
# RETRY_WAITs in the first burst, 9 assertions red), so further passes run while a seeded tenant
# has such a retry due within 70 s — at most 3, never for a NOT_READY, parked or DEAD job.
distill_once() {
  local pass due
  for pass in 1 2 3 4; do
    distill_pass
    [ $pass = 4 ] && break
    due=$(PGQ "select ceil(greatest(0, extract(epoch from min(next_retry_at) - now()))) from ops.jobs where job_type='DERIVED_DISTILL' and tenant_id in ($SEEDED) and status='PENDING' and attempt > 0 and last_error_class='RETRY_WAIT' and next_retry_at < now() + interval '70 seconds'")
    [ -z "$due" ] && break
    echo "distill: a seeded RETRY_WAIT job is due in ${due}s — pass $((pass + 1))"
    sleep $due
  done
}
distill_pass() {
( export PRIVATE_WORKER_PG_DSN="postgres://role_private_worker:${HUMAUX_ROLE_PASSWORD_PRIVATE_WORKER:?}@$PG/$DB" \
    HUMAUX_PRIVATE_WORKER_CREDENTIALS="$HUMAUX_PRIVATE_WORKER_CREDENTIALS" HUMAUX_PRIVATE_WORKER_HEALTH_RENEW_SECS=$PW_HEALTH_RENEW_SECS HUMAUX_PRIVATE_WORKER_HTTP_TIMEOUT_SECS=120 HUMAUX_PRIVATE_WORKER_PERMIT_TTL_SECS=60 \
    HUMAUX_PRIVATE_WORKER_DNS_PINS="$PW_DNS_PINS" HUMAUX_PRIVATE_WORKER_RPC_SOCKET_PATH=$SOCK/inference-distill.sock HUMAUX_PRIVATE_WORKER_CONSOLIDATION_UID=$MYUID \
    HUMAUX_PRIVATE_WORKER_CANDIDATE_TTL_SECONDS=86400 \
    HUMAUX_PRIVATE_WORKER_DISTILL_LEASE_SECS=30 HUMAUX_PRIVATE_WORKER_DISTILL_IN_FLIGHT=4 HUMAUX_PRIVATE_WORKER_DISTILL_HARD_DEADLINE_SECS=300 HUMAUX_PRIVATE_WORKER_DISTILL_NOT_READY_PARK_SECS=600 HUMAUX_PRIVATE_WORKER_DISTILL_MAX_ATTEMPTS=5 HUMAUX_PRIVATE_WORKER_DISTILL_BUDGET_WINDOW_SECS=60 HUMAUX_PRIVATE_WORKER_DISTILL_BUDGET_MAX_CALLS=120
  set -a; source /Volumes/data/viral-skill-eval/.env; set +a
  exec "$BIN_DIR"/humaux-private-worker --distill-once ) 2>&1 | tee -a $EV/distill.log | tail -3
}
distill_once
PGQ "select 'memory_records='||(select count(*) from private.memory_records where tenant_id='$TENANT')||' memory_evidence='||(select count(*) from private.memory_evidence me join private.memory_records m on m.memory_id=me.memory_id where m.tenant_id='$TENANT')||' processing_runs='||(select count(*) from private.processing_runs where tenant_id='$TENANT' and completed_at is not null)||' outbox_done='||(select count(*) from ops.outbox where tenant_id='$TENANT' and status='DONE')" | tee -a $EV/rehearsal.log
PGQ "select 'memory: '||authority_class||' '||visibility_class||' '||left(content::text,90) from private.memory_records where tenant_id='$TENANT' order by created_at" | tee -a $EV/rehearsal.log

# ---------- 3c. SIGTERM mid-load: no lease may be stranded (card 15 §3 / card 16 ADR-0038 D5) ----------
# The resident distill worker observes the signal only BETWEEN passes, so a pass always settles
# every job it claimed. The witness is SQL, not the exit code: after the process is gone, no
# ops.jobs row may still be PROCESSING with a live lease.
step sigterm_mid_load
( export PRIVATE_WORKER_PG_DSN="postgres://role_private_worker:${HUMAUX_ROLE_PASSWORD_PRIVATE_WORKER:?}@$PG/$DB" \
    HUMAUX_PRIVATE_WORKER_CREDENTIALS="$HUMAUX_PRIVATE_WORKER_CREDENTIALS" HUMAUX_PRIVATE_WORKER_HEALTH_RENEW_SECS=$PW_HEALTH_RENEW_SECS HUMAUX_PRIVATE_WORKER_HTTP_TIMEOUT_SECS=120 HUMAUX_PRIVATE_WORKER_PERMIT_TTL_SECS=60 \
    HUMAUX_PRIVATE_WORKER_DNS_PINS="$PW_DNS_PINS" HUMAUX_PRIVATE_WORKER_RPC_SOCKET_PATH=$SOCK/inference-drain.sock HUMAUX_PRIVATE_WORKER_CONSOLIDATION_UID=$MYUID \
    HUMAUX_PRIVATE_WORKER_CANDIDATE_TTL_SECONDS=86400 \
    HUMAUX_PRIVATE_WORKER_DISTILL_LEASE_SECS=30 HUMAUX_PRIVATE_WORKER_DISTILL_IN_FLIGHT=4 HUMAUX_PRIVATE_WORKER_DISTILL_HARD_DEADLINE_SECS=300 HUMAUX_PRIVATE_WORKER_DISTILL_NOT_READY_PARK_SECS=600 HUMAUX_PRIVATE_WORKER_DISTILL_MAX_ATTEMPTS=5 HUMAUX_PRIVATE_WORKER_DISTILL_BUDGET_WINDOW_SECS=60 HUMAUX_PRIVATE_WORKER_DISTILL_BUDGET_MAX_CALLS=120 \
    HUMAUX_PRIVATE_WORKER_DISTILL_POLL_INTERVAL_SECS=3 $OPS_PW_DISTILL
  set -a; source /Volumes/data/viral-skill-eval/.env; set +a
  exec "$BIN_DIR"/humaux-private-worker --distill-serve > $EV/distill-serve.log 2>&1 ) &
DS_PID=$!
mcp remember "{\"operation\":\"put\",\"content\":\"Deploys are frozen on the last working day of each quarter.\",\"idempotency_key\":\"$(uuidgen | tr A-Z a-z)\",\"workspace_id\":\"$WS\"}" >/dev/null 2>&1
sleep 5
kill -TERM $DS_PID 2>/dev/null
for i in $(seq 1 180); do kill -0 $DS_PID 2>/dev/null || break; sleep 1; done
kill -9 $DS_PID 2>/dev/null; wait $DS_PID 2>/dev/null
STRANDED_LEASES=$(PGQ "select count(*) from ops.jobs where tenant_id='$TENANT' and status='PROCESSING' and lease_expires_at is not null and lease_expires_at > now()")
echo "sigterm drain: exited after ${i}s, ops.jobs PROCESSING with a live lease = $STRANDED_LEASES" | tee -a $EV/rehearsal.log
# A worker that died on a missing config key would make the lease witness vacuous: it never
# claimed anything, so "no stranded lease" would be true for the wrong reason.
grep -q "missing required configuration" $EV/distill-serve.log && echo "sigterm drain: WITNESS VACUOUS — the distill worker never started" | tee -a $EV/rehearsal.log
tail -n 2 $EV/distill-serve.log | tee -a $EV/rehearsal.log

# ---------- 4. consolidation (second hop, real MiniMax) ----------
step consolidation
( export CONSOLIDATION_WORKER_PG_DSN="postgres://role_consolidation_worker:${HUMAUX_ROLE_PASSWORD_CONSOLIDATION_WORKER:?}@$PG/$DB" \
    HUMAUX_CONSOLIDATION_WORKER_RPC_SOCKET_PATH=$SOCK/inference.sock HUMAUX_CONSOLIDATION_WORKER_CALL_TTL_SECS=120 HUMAUX_CONSOLIDATION_WORKER_DIAL_TIMEOUT_SECS=10 \
    HUMAUX_CONSOLIDATION_WORKER_MAX_INPUTS=50 HUMAUX_CONSOLIDATION_WORKER_LEASE_SECS=120 HUMAUX_CONSOLIDATION_WORKER_BATCH=8 HUMAUX_CONSOLIDATION_WORKER_MAX_ATTEMPTS=5
  exec "$BIN_DIR"/humaux-consolidation-worker --run-once ) > $EV/consolidation.log 2>&1 &
CW_PID=$!; for i in $(seq 1 180); do kill -0 $CW_PID 2>/dev/null || break; grep -q "published\|Published\|rollup" $EV/consolidation.log 2>/dev/null && sleep 3 && break; sleep 1; done; kill $CW_PID 2>/dev/null; tail -n 3 $EV/consolidation.log
PGQ "select 'rollups='||count(*) from private.memory_rollups where tenant_id='$TENANT'" | tee -a $EV/rehearsal.log
PGQ "select 'derived_jobs: '||coalesce(string_agg(job_type||'/'||status||'='||n, ', ' order by job_type, status),'none') from (select job_type, status, count(*) n from ops.jobs where tenant_id='$TENANT' and left(job_type,8)='DERIVED_' group by 1,2) t" | tee -a $EV/rehearsal.log
PGQ "select 'rpc='||outcome||' run_bound='||(consolidation_run_id is not null) from ops.private_inference_rpc_calls where tenant_id='$TENANT' order by registered_at desc limit 1" | tee -a $EV/rehearsal.log

# ---------- 5. projection (third hop, real DashScope) ----------
step projection
# Card 27 / ADR-0052: nothing here projects any more — the resident `--serve` runner (step
# processes) claims every placed tenant's tickets. This only WAITS until no ticket of the seeded
# tenants is left that the runner could claim (ISSUED, distill closed), bounded.
wait_projection_idle() { # $1=timeout secs
  local i=0 n=
  while [ $i -lt ${1:-120} ]; do
    n=$(PGQ "select count(*) from projection.stream_log s where s.tenant_id in ($SEEDED) and s.state='ISSUED' and s.scope_kind='workspace' and not exists (select 1 from ops.outbox o where o.tenant_id=s.tenant_id and o.commit_seq=s.commit_seq and o.event_type='EVIDENCE_ACCEPTED' and o.status in ('PENDING','PROCESSING'))")
    [ "$n" = "0" ] && { echo "projection idle after ${i}s" | tee -a $EV/rehearsal.log; return 0; }
    sleep 1; i=$((i+1))
  done
  echo "projection NOT idle after ${i}s: $n claimable ISSUED tickets in the seeded tenants" | tee -a $EV/rehearsal.log; return 1
}
wait_projection_idle 180
PGQ "select 'tickets: '||string_agg(stream_seq||':'||state||'/'||coalesce(error_class,'-'), ', ' order by stream_seq) from projection.stream_log where tenant_id='$TENANT'" | tee -a $EV/rehearsal.log
PGQ "select 'ledger: '||coalesce(string_agg(purpose||'/'||status||'/'||coalesce(error_class,'-'), ', '),'none') from ops.model_call_ledger where tenant_id='$TENANT'" | tee -a $EV/rehearsal.log
PGQ "select 'checkpoint issued_highwater='||coalesce(max(issued_highwater)::text,'none') from projection.stream_checkpoints where tenant_id='$TENANT'" | tee -a $EV/rehearsal.log
PGQ "select 'points='||count(*) from projection.private_memory_points where tenant_id='$TENANT'" | tee -a $EV/rehearsal.log

# ---------- 5b. §16.2 serving switch (ops action; recall reads only the serving version) ----------
step serve_switch
# card 20 (§15.2.1, migration 0167): retire this version's exhausted FAILED tickets first. A
# settled FAILED row is both an open_gaps row (§16.3 criterion ②) and a permanent pin on the
# §15.4 prefix, so without this a single live-model flake in the first ticket window makes the
# family unpromotable for the whole run. The two classes named are the ones the projection worker
# writes for a terminally-failed distill (`adapters::projection_worker::terminal_for_missing_memory`);
# a class that matches nothing retires nothing and says so.
serve_switch_tenant() { # $1=tenant $2=workspace
  cargo run -q -p xtask -- projection-serve --tenant $1 --workspace $2 --domain $DOMAIN --projection-kind $PKIND --version $PVER --retire-failed distill_failed,no_visible_memory_record 2>&1 | tee -a $EV/rehearsal.log
}
serve_switch_tenant $TENANT $WS
serve_switch_tenant $TENANT_B $WS_B
# Every later write in this rehearsal goes through ONE drain, so no step has to remember which
# of the four hops its own write needs. Both tenants, every time.
# Rehearsal4 run 2 (2026-09-26): the distill a drain lands creates a DERIVED_CONSOLIDATE job;
# step 4's consolidation pass ran before it existed, so it stayed PENDING and
# `derived_jobs_not_done` counted it. A drain is not a drain until the second hop ran too.
# `--run-once` is one bounded pass then exit (ADR-0036), so this waits for exit, no kill.
consolidate_once() {
( export CONSOLIDATION_WORKER_PG_DSN="postgres://role_consolidation_worker:${HUMAUX_ROLE_PASSWORD_CONSOLIDATION_WORKER:?}@$PG/$DB" \
    HUMAUX_CONSOLIDATION_WORKER_RPC_SOCKET_PATH=$SOCK/inference.sock HUMAUX_CONSOLIDATION_WORKER_CALL_TTL_SECS=120 HUMAUX_CONSOLIDATION_WORKER_DIAL_TIMEOUT_SECS=10 \
    HUMAUX_CONSOLIDATION_WORKER_MAX_INPUTS=50 HUMAUX_CONSOLIDATION_WORKER_LEASE_SECS=120 HUMAUX_CONSOLIDATION_WORKER_BATCH=8 HUMAUX_CONSOLIDATION_WORKER_MAX_ATTEMPTS=5
  exec "$BIN_DIR"/humaux-consolidation-worker --run-once ) >> $EV/consolidation.log 2>&1
}
drain_all() {
  distill_once
  consolidate_once
  wait_projection_idle 180
  serve_switch_tenant $TENANT $WS
  serve_switch_tenant $TENANT_B $WS_B
}
PGQ "select 'serving='||serving||' projected='||coalesce(projection_highwater::text,'-') from projection.stream_checkpoints where tenant_id='$TENANT'" | tee -a $EV/rehearsal.log
# A tenant whose FIRST switch is refused on OpenGaps can never be promoted (a FAILED ticket is
# terminal, so the §15.4 prefix never clears it) and every recall for it answers
# no_serving_projection for the rest of the run. That is a live-model flake landing in the first
# ticket window (card 24 D1), not something a 10-minute soak can recover from — so fail here,
# at 3 minutes, instead of discovering it in the assertion table at 13.
# Card 29: scoped to lane A's workspace — since card 28 the seed activates EVERY seeded workspace
# (WS and WS_A2 are both serving), so a tenant-wide count is 2 on a healthy run.
if [ "$(PGQ "select count(*) from projection.stream_checkpoints where tenant_id='$TENANT' and scope_id='$WS' and serving")" != "1" ]; then
  echo "serve_switch: tenant A has no serving projection (first switch refused; see soak-projection reject reasons). This run cannot measure lane A - re-seed." | tee -a $EV/rehearsal.log
  kill $GW_PID $RW_PID $PW_PID 2>/dev/null
  exit 2
fi

# ---------- 6. recall (fourth hop) ----------
step recall
mcp recall "{\"query\":\"which language do we prefer for backend services?\",\"workspace_id\":\"$WS\",\"mode\":\"semantic\"}" | tee $EV/recall.json | tail -1
mcp recall "{\"query\":\"what must a new service expose before it gets traffic?\",\"workspace_id\":\"$WS\",\"mode\":\"semantic\"}" | tee $EV/recall_2.json | tail -1
echo "recall hits: rust=$(grep -c 'Rust' $EV/recall.json) health=$(grep -c 'health endpoint' $EV/recall_2.json) code1=$(grep -oE 'DEPENDENCY_UNAVAILABLE|"isError":true' $EV/recall.json | head -1)" | tee -a $EV/rehearsal.log
head -c 500 $EV/recall.json | tee -a $EV/rehearsal.log; echo | tee -a $EV/rehearsal.log


# ============================================================================
# card 24 ACCEPTANCE GATE — the six witnesses the rehearsal itself must carry.
# Before this pass the 5/5 rehearsal was single-tenant with no subject, no lifecycle, no
# completeness class and no kill -9, so "the delivery claim is an assembly of per-card gates"
# (the card's own Why section) was still true of the artefact meant to replace them.
# Everything here runs BEFORE the RYW probe on purpose: the probe deliberately leaves one
# un-drained write, and the drain assertions exclude exactly that one Evidence.
# ============================================================================

# ---------- 6a1. cross-tenant isolation (acceptance item 1) ----------
step cross_tenant
XT_POINTS_A=$(PGQ "select count(*) from projection.private_memory_points where tenant_id='$TENANT'")
XT_POINTS_B=$(PGQ "select count(*) from projection.private_memory_points where tenant_id='$TENANT_B'")
echo "cross-tenant corpus: A points=$XT_POINTS_A  B points=$XT_POINTS_B" | tee -a $EV/rehearsal.log
# B asks A's question on the same gateway process. A hit on A's sentence would be a cross-tenant read.
mcp_as "$BEARER_B" recall "{\"query\":\"which language do we prefer for backend services?\",\"workspace_id\":\"$WS_B\",\"mode\":\"semantic\"}" > $EV/recall_b_cross.json 2>&1
XT_B_SEES_A=$(head -1 $EV/recall_b_cross.json | python3 -c "
import sys,json
try: items=json.load(sys.stdin)['result'].get('structuredContent',{}).get('items',[])
except Exception: items=[]
print(sum(1 for i in items if 'Rust is the preferred language' in json.dumps(i, ensure_ascii=False)))")
# …and the mirror leg: A must not see B's sentence either.
mcp recall "{\"query\":\"when does the inventory service ship?\",\"workspace_id\":\"$WS\",\"mode\":\"semantic\"}" > $EV/recall_a_cross.json 2>&1
XT_A_SEES_B=$(head -1 $EV/recall_a_cross.json | python3 -c "
import sys,json
try: items=json.load(sys.stdin)['result'].get('structuredContent',{}).get('items',[])
except Exception: items=[]
print(sum(1 for i in items if 'inventory service' in json.dumps(i, ensure_ascii=False)))")
# Every memory_id B can enumerate must be B's own row in PG — the enumeration is the widest
# read B has, so a foreign id here is a cross-tenant row the recall lane could also serve.
mcp_as "$BEARER_B" memory "{\"action\":\"enumerate\",\"workspace_id\":\"$WS_B\",\"limit\":100}" > $EV/enumerate_b.json 2>&1
XT_B_ENUM_FOREIGN=0; XT_B_ENUM_N=0
for mid in $(head -1 $EV/enumerate_b.json | python3 -c "
import sys,json,re
try: d=json.load(sys.stdin)['result'].get('structuredContent',{})
except Exception: d={}
out=[]
def walk(v):
    if isinstance(v,dict):
        if isinstance(v.get('memory_id'),str): out.append(v['memory_id'])
        for x in v.values(): walk(x)
    elif isinstance(v,list):
        for x in v: walk(x)
walk(d)
print('\n'.join(sorted(set(out))))"); do
  XT_B_ENUM_N=$((XT_B_ENUM_N+1))
  n=$(PGQ "select count(*) from private.memory_records where memory_id='$mid' and tenant_id<>'$TENANT_B'")
  XT_B_ENUM_FOREIGN=$((XT_B_ENUM_FOREIGN + n))
done
echo "cross-tenant: B_sees_A=$XT_B_SEES_A A_sees_B=$XT_A_SEES_B B_enumerated=$XT_B_ENUM_N foreign=$XT_B_ENUM_FOREIGN" | tee -a $EV/rehearsal.log

# ---------- 6a1b. subject-scoped recall (acceptance item 2) ----------
step subject_scope
SUBJ=$(mcp memory "{\"action\":\"subject_register\",\"kind\":\"PERSON\",\"display_name\":\"Ada Rehearsal\",\"workspace_id\":\"$WS\"}" | head -1 | sc subject_id)
echo "subject registered in tenant A: ${SUBJ:-<none>}" | tee -a $EV/rehearsal.log
if [ -n "$SUBJ" ]; then
  mcp remember "{\"operation\":\"put\",\"content\":\"Ada owns the billing on-call rotation for the payments platform team.\",\"idempotency_key\":\"$(uuidgen | tr A-Z a-z)\",\"workspace_id\":\"$WS\",\"subject_ids\":[\"$SUBJ\"]}" | tee $EV/remember_subject.json | tail -1
  drain_all
  # authorized: tenant A's own principal, narrowing to that subject
  mcp recall "{\"query\":\"who owns the billing on-call rotation?\",\"workspace_id\":\"$WS\",\"mode\":\"semantic\",\"subject_ids\":[\"$SUBJ\"]}" > $EV/recall_subject_a.json 2>&1
  SUBJ_A_N=0; SUBJ_A_UNLINKED=0
  for mid in $(head -1 $EV/recall_subject_a.json | python3 -c "
import sys,json
try: d=json.load(sys.stdin)['result'].get('structuredContent',{})
except Exception: d={}
out=[]
def walk(v):
    if isinstance(v,dict):
        if isinstance(v.get('memory_id'),str): out.append(v['memory_id'])
        for x in v.values(): walk(x)
    elif isinstance(v,list):
        for x in v: walk(x)
walk(d.get('items',[]))
print('\n'.join(sorted(set(out))))"); do
    SUBJ_A_N=$((SUBJ_A_N+1))
    n=$(PGQ "select count(*) from private.memory_records m where m.memory_id='$mid' and not exists (select 1 from private.memory_subjects ms where ms.tenant_id='$TENANT' and ms.memory_id=m.memory_id and ms.subject_id='$SUBJ')")
    SUBJ_A_UNLINKED=$((SUBJ_A_UNLINKED + n))
  done
  # unauthorized: a principal with no right to that subject asks for it by id. Migration 0155's
  # header states the delivered ACL verbatim — "Subject visibility today = same tenant" — so the
  # unauthorized principal this deployment can actually produce is a caller outside the subject's
  # tenant. A per-subject ACL below tenant does not exist yet; this leg is the boundary that
  # does, and it is named as such rather than dressed up as a per-user ACL witness.
  mcp_as "$BEARER_B" recall "{\"query\":\"who owns the billing on-call rotation?\",\"workspace_id\":\"$WS_B\",\"mode\":\"semantic\",\"subject_ids\":[\"$SUBJ\"]}" > $EV/recall_subject_b.json 2>&1
  SUBJ_B_N=$(head -1 $EV/recall_subject_b.json | python3 -c "
import sys,json
try: items=json.load(sys.stdin)['result'].get('structuredContent',{}).get('items',[])
except Exception: items=[]
print(len(items))")
  echo "subject recall: authorized items=$SUBJ_A_N unlinked_in_result=$SUBJ_A_UNLINKED  unauthorized items=$SUBJ_B_N" | tee -a $EV/rehearsal.log
else
  SUBJ_A_N=-1; SUBJ_A_UNLINKED=-1; SUBJ_B_N=-1
  echo "subject_scope: memory.subject_register returned no subject_id — the leg cannot run" | tee -a $EV/rehearsal.log
fi

# ---------- 6a1c. supersede then restore (acceptance item 3) ----------
step lifecycle
LC_PAIR=($(PGQ "select memory_id from private.memory_records where tenant_id='$TENANT' and superseded_at is null order by created_at limit 2"))
LC_TARGET=${LC_PAIR[1]:-}; LC_REPL=${LC_PAIR[2]:-}
echo "lifecycle target=${LC_TARGET:-<none>} replacement=${LC_REPL:-<none>}" | tee -a $EV/rehearsal.log
if [ -n "$LC_TARGET" ] && [ -n "$LC_REPL" ]; then
  # §33.10 confirm gate: leg 1 answers confirmation_required, leg 2 executes. gated_as does both.
  gated_as "$BEARER" memory "{\"action\":\"supersede\",\"memory_id\":\"$LC_TARGET\",\"replacement_memory_id\":\"$LC_REPL\"}" > $EV/supersede.json 2>&1
  LC_SUPERSEDE_EVENTS=$(PGQ "select count(*) from ops.memory_lifecycle_events where tenant_id='$TENANT' and op='SUPERSEDE' and memory_id='$LC_TARGET'")
  drain_all
  a2_check supersede
  LC_RECALL_WHILE_SUPERSEDED=$(mcp memory "{\"action\":\"get\",\"memory_id\":\"$LC_TARGET\",\"workspace_id\":\"$WS\"}" | head -1 | sc superseded_at)
  gated_as "$BEARER" memory "{\"action\":\"restore\",\"memory_id\":\"$LC_TARGET\"}" > $EV/restore.json 2>&1
  LC_RESTORE_EVENTS=$(PGQ "select count(*) from ops.memory_lifecycle_events r join ops.memory_lifecycle_events s on s.event_id=r.undoes_event_id where r.tenant_id='$TENANT' and r.op='RESTORE' and s.op='SUPERSEDE' and s.memory_id='$LC_TARGET'")
  drain_all
  # …and the restore is visible in RECALL, not only in the log: the row is servable again.
  LC_BODY=$(PGQ "select left(content::text,60) from private.memory_records where memory_id='$LC_TARGET'")
  mcp recall "{\"query\":\"which language do we prefer for backend services?\",\"workspace_id\":\"$WS\",\"mode\":\"semantic\"}" > $EV/recall_after_restore.json 2>&1
  LC_RECALL_AFTER_RESTORE=$(head -1 $EV/recall_after_restore.json | python3 -c "
import sys,json
try: d=json.load(sys.stdin)['result'].get('structuredContent',{})
except Exception: d={}
print(json.dumps(d, ensure_ascii=False).count(sys.argv[1]))" "$LC_TARGET")
  a2_check restore $EV/recall_after_restore.json
  echo "lifecycle: supersede_events=$LC_SUPERSEDE_EVENTS restore_undoes=$LC_RESTORE_EVENTS superseded_at_while_gone='$LC_RECALL_WHILE_SUPERSEDED' recall_hits_after_restore=$LC_RECALL_AFTER_RESTORE" | tee -a $EV/rehearsal.log
else
  LC_SUPERSEDE_EVENTS=-1; LC_RESTORE_EVENTS=-1; LC_RECALL_AFTER_RESTORE=-1
  echo "lifecycle: fewer than two live memories in tenant A — the leg cannot run" | tee -a $EV/rehearsal.log
fi

# ---------- 6a1c. archive: recall excludes, memory.get still answers (card 4 / ADR-0024, folded debt) ----------
# Card 24 review P1: the card-4 recall-exclusion witness is in-scope folded debt. Archive the
# restored memory, prove recall.search no longer returns it while memory.get still answers it
# with the archived flag, unarchive, prove recall serves it again, and drain so the ryw probe's
# "everything before my write is projected" precondition still holds.
step archive_exclusion
AR_TARGET=${LC_TARGET:-}
AR_RECALL_WHILE_ARCHIVED=""; AR_GET_ARCHIVED=""; AR_RECALL_AFTER_UNARCHIVE=""
if [ -n "$AR_TARGET" ]; then
  gated_as "$BEARER" memory "{\"action\":\"archive\",\"memory_id\":\"$AR_TARGET\"}" > $EV/archive.json 2>&1
  mcp recall "{\"query\":\"which language do we prefer for backend services?\",\"workspace_id\":\"$WS\",\"mode\":\"semantic\"}" > $EV/recall_while_archived.json 2>&1
  AR_RECALL_WHILE_ARCHIVED=$(head -1 $EV/recall_while_archived.json | python3 -c "
import sys,json
try: d=json.load(sys.stdin)['result'].get('structuredContent',{})
except Exception: d={}
print(json.dumps(d, ensure_ascii=False).count(sys.argv[1]))" "$AR_TARGET")
  mcp memory "{\"action\":\"get\",\"memory_id\":\"$AR_TARGET\",\"workspace_id\":\"$WS\"}" > $EV/get_while_archived.json 2>&1
  AR_GET_ARCHIVED=$(head -1 $EV/get_while_archived.json | python3 -c "
import sys,json
try: d=json.load(sys.stdin)['result'].get('structuredContent',{})
except Exception: d={}
s=json.dumps(d)
print('true' if ('\"archived\": true' in s or d.get('archived_at')) else 'false')")
  drain_all
  a2_check archive
  gated_as "$BEARER" memory "{\"action\":\"unarchive\",\"memory_id\":\"$AR_TARGET\"}" > $EV/unarchive.json 2>&1
  # Card 30 (ADR-0055 D-B): the Qdrant prefilter drops `archived == true` points, so an unarchived
  # memory serves again once its MEMORY_LIFECYCLE ticket has re-projected the point with
  # `archived=false` — projection lag, no longer instant through the PG gate alone. Poll (bounded
  # at 60 s) and report the lag instead of asserting on the first recall after the write.
  AR_T0=$(date +%s); AR_RECALL_AFTER_UNARCHIVE=0
  while [ "$AR_RECALL_AFTER_UNARCHIVE" = 0 ] && [ $(( $(date +%s) - AR_T0 )) -lt 60 ]; do
    mcp recall "{\"query\":\"which language do we prefer for backend services?\",\"workspace_id\":\"$WS\",\"mode\":\"semantic\"}" > $EV/recall_after_unarchive.json 2>&1
    AR_RECALL_AFTER_UNARCHIVE=$(head -1 $EV/recall_after_unarchive.json | python3 -c "
import sys,json
try: d=json.load(sys.stdin)['result'].get('structuredContent',{})
except Exception: d={}
print(json.dumps(d, ensure_ascii=False).count(sys.argv[1]))" "$AR_TARGET")
    [ "$AR_RECALL_AFTER_UNARCHIVE" = 0 ] && sleep 1
  done
  AR_UNARCHIVE_LAG_S=$(( $(date +%s) - AR_T0 ))
  drain_all
  a2_check unarchive
  echo "archive: recall_hits_while_archived=$AR_RECALL_WHILE_ARCHIVED get_reports_archived=$AR_GET_ARCHIVED recall_hits_after_unarchive=$AR_RECALL_AFTER_UNARCHIVE unarchive_to_served_s=$AR_UNARCHIVE_LAG_S (bounded 60)" | tee -a $EV/rehearsal.log
else
  echo "archive: no target (lifecycle step had no pair)" | tee -a $EV/rehearsal.log
fi

# ---------- 6a1c2. card 31: correct retires M1's point; undoing it keeps A2 closed (ADR-0057 D-B) ----------
# correct issues two lifecycle tickets (E2 projects M2, E1 retires M1); restore of the corrected
# memory issues M2's retire ticket before deactivating it. Both must leave the identity closed, and
# M1's point must be gone from the index — registry AND Qdrant, by point id.
step correct
CR_M1=${LC_TARGET:-}; CR_M2=""
if [ -n "$CR_M1" ]; then
  gated_as "$BEARER" memory "{\"action\":\"correct\",\"memory_id\":\"$CR_M1\",\"text\":\"card 31 correction: backend services are written in Rust, reviewed weekly.\"}" > $EV/correct.json 2>&1
  CR_M2=$(head -1 $EV/correct.json | sc memory_id)
  drain_all
  a2_check correct
  CR_ROWS=$(PGQ "select count(*) from projection.private_memory_points where memory_id='$CR_M1'")
  CR_LIVE=$(PGQ "select count(*) from projection.private_memory_points where memory_id='$CR_M1' and projection_live")
  CR_IDS=$(PGQ "select coalesce(string_agg('\"'||point_id||'\"', ','),'') from projection.private_memory_points where memory_id='$CR_M1'")
  CR_QD=$(curl -s -X POST "http://127.0.0.1:6333/collections/$COLLECTION/points/count" -H 'Content-Type: application/json' --data "{\"exact\":true,\"filter\":{\"must\":[{\"has_id\":[$CR_IDS]}]}}" | python3 -c "import sys,json; print(json.load(sys.stdin)['result']['count'])" 2>/dev/null)
  gated_as "$BEARER" memory "{\"action\":\"restore\",\"memory_id\":\"$CR_M1\"}" > $EV/correct_undo.json 2>&1
  drain_all
  a2_check correct_undo
  CR_M2_LIVE=$(PGQ "select count(*) from projection.private_memory_points where memory_id='${CR_M2:-00000000-0000-0000-0000-000000000000}' and projection_live")
  echo "correct: m1=$CR_M1 m2=${CR_M2:-<none>} m1_registry_rows=$CR_ROWS m1_live=$CR_LIVE m1_qdrant_points=${CR_QD:-?} m2_live_after_undo=$CR_M2_LIVE" | tee -a $EV/rehearsal.log
else
  echo "correct: no target (lifecycle step had no pair)" | tee -a $EV/rehearsal.log
fi

# ---------- 6a1d. completeness class on a closed ledger (acceptance item 4) ----------
# memory.enumerate is the route with an enumerable universe and a frozen denominator
# (ADR-0041 D-D); recall/memory.get answer semantic_bounded / cannot_establish BY CONSTRUCTION,
# so they are not the route this item can be asserted on.
step completeness
mcp memory "{\"action\":\"enumerate\",\"workspace_id\":\"$WS\",\"limit\":100}" > $EV/enumerate_a.json 2>&1
CMP_CLASS=$(head -1 $EV/enumerate_a.json | python3 -c "
import sys,json
try: d=json.load(sys.stdin)['result'].get('structuredContent',{})
except Exception: d={}
found=[]
def walk(v):
    if isinstance(v,dict):
        c=v.get('completeness')
        if isinstance(c,dict) and isinstance(c.get('class'),str): found.append(c['class'])
        for x in v.values(): walk(x)
    elif isinstance(v,list):
        for x in v: walk(x)
walk(d)
print(found[0] if found else '')")
echo "completeness: memory.enumerate class='${CMP_CLASS:-<absent>}'" | tee -a $EV/rehearsal.log

# ---------- 6a1d2. card 29 / ADR-0054: governance on BOTH seeded tenants, ONE gateway, no default pair ----------
# start_gw passes no (tenant, workspace): every governance / subject / affect write derives its
# pair per request. One leg per seeded tenant, each on its own bearer and workspace: supersede ->
# restore -> archive -> unarchive -> pin -> bind (REFERENCE_ONLY) -> annotate_affect. Fixture
# writes (superuser PGQ, no product path creates them — ADR-0044): one coord.tasks row per tenant,
# and one bindable memory per leg (ProjectConstraint + TenantAdmin evidence — distilled memories are
# PrivateKnowledge, which §10.1 never lets bind MANDATORY). The leg's tickets must land on its own
# (tenant, 'workspace', workspace) stream and on no other seeded stream.
step governance_both_tenants
typeset -gA GOV
PGQ "insert into coord.tasks(tenant_id,title) values ('$TENANT','card29 rehearsal task'),('$TENANT_B','card29 rehearsal task')" >/dev/null
governance_leg() { # $1=label $2=bearer $3=tenant $4=workspace $5=reasoning domain
  local L=$1 b=$2 t=$3 w=$4 rd=$5
  local pick=($(PGQ "select memory_id from private.memory_records where tenant_id='$t' and status='active' and visibility_workspace_id='$w' and memory_id <> '${LC_TARGET:-00000000-0000-0000-0000-000000000000}' order by created_at desc limit 2"))
  local tgt=${pick[1]:-} repl=${pick[2]:-}
  local task=$(PGQ "select task_id from coord.tasks where tenant_id='$t' and title='card29 rehearsal task' limit 1")
  local bindable=$(PGQ "with e as (insert into private.evidence_objects (tenant_id,evidence_kind,payload_sha256,data_class,origin_class,visibility_class,visibility_workspace_id,reasoning_domain_id) values ('$t','EVENT',sha256(gen_random_uuid()::text::bytea),'INTERNAL','TenantAdmin','WORKSPACE_SHARED','$w','$rd') returning evidence_id), m as (insert into private.memory_records (tenant_id,memory_type,content,visibility_class,visibility_workspace_id,authority_class,confidence,status,asserted_at) values ('$t','NOTE','{\"rehearsal\":\"card29 bind target\"}','WORKSPACE_SHARED','$w','ProjectConstraint',0.9,'active',clock_timestamp()) returning memory_id), l as (insert into private.memory_evidence (memory_id,evidence_id,role,grounding_mode) select m.memory_id, e.evidence_id, 'PRIMARY', 'SNAPSHOT' from m, e returning memory_id) select memory_id from l")
  local own0=$(PGQ "select count(*) from projection.stream_log where tenant_id='$t' and scope_kind='workspace' and scope_id='$w'")
  local foreign0=$(PGQ "select count(*) from projection.stream_log where tenant_id in ($SEEDED) and not (tenant_id='$t' and scope_kind='workspace' and scope_id='$w')")
  echo "governance $L: tenant=$t ws=$w target=${tgt:-<none>} replacement=${repl:-<none>} bindable=${bindable:-<none>} task=${task:-<none>}" | tee -a $EV/rehearsal.log
  if [ -n "$tgt" ] && [ -n "$repl" ] && [ -n "$bindable" ] && [ -n "$task" ]; then
    gated_as "$b" memory "{\"action\":\"supersede\",\"memory_id\":\"$tgt\",\"replacement_memory_id\":\"$repl\"}" > $EV/gov_${L}_supersede.json 2>&1
    gated_as "$b" memory "{\"action\":\"restore\",\"memory_id\":\"$tgt\"}" > $EV/gov_${L}_restore.json 2>&1
    gated_as "$b" memory "{\"action\":\"archive\",\"memory_id\":\"$tgt\"}" > $EV/gov_${L}_archive.json 2>&1
    gated_as "$b" memory "{\"action\":\"unarchive\",\"memory_id\":\"$tgt\"}" > $EV/gov_${L}_unarchive.json 2>&1
    gated_as "$b" memory "{\"action\":\"pin\",\"memory_id\":\"$tgt\"}" > $EV/gov_${L}_pin.json 2>&1
    gated_as "$b" memory "{\"action\":\"bind\",\"memory_id\":\"$bindable\",\"task_id\":\"$task\",\"purpose\":\"REFERENCE_ONLY\"}" > $EV/gov_${L}_bind.json 2>&1
    mcp_as "$b" memory "{\"action\":\"annotate_affect\",\"memory_id\":\"$tgt\",\"workspace_id\":\"$w\",\"affects\":[{\"kind\":\"EMOTION\",\"intensity\":5000,\"confidence\":9000}]}" > $EV/gov_${L}_annotate.json 2>&1
  fi
  for op in supersede restore archive unarchive pin bind annotate; do
    echo "governance $L $op: http=$(tail -1 $EV/gov_${L}_$op.json 2>/dev/null) isError=$(head -1 $EV/gov_${L}_$op.json 2>/dev/null | python3 -c "import sys,json
try: print(json.load(sys.stdin)['result'].get('isError', False))
except Exception: print('?')")" | tee -a $EV/rehearsal.log
  done
  GOV[${L}_target]=${tgt:-none}
  GOV[${L}_supersede]=$(PGQ "select count(*) from ops.memory_lifecycle_events where tenant_id='$t' and op='SUPERSEDE' and memory_id='${tgt:-00000000-0000-0000-0000-000000000000}'")
  GOV[${L}_restore]=$(PGQ "select count(*) from ops.memory_lifecycle_events r join ops.memory_lifecycle_events s on s.event_id=r.undoes_event_id where r.tenant_id='$t' and r.op='RESTORE' and s.op='SUPERSEDE' and s.memory_id='${tgt:-00000000-0000-0000-0000-000000000000}'")
  GOV[${L}_archive]=$(PGQ "select count(*) from ops.memory_lifecycle_events e where e.tenant_id='$t' and e.memory_id='${tgt:-00000000-0000-0000-0000-000000000000}' and (e.op='ARCHIVE' or (e.op='RESTORE' and exists (select 1 from ops.memory_lifecycle_events a where a.event_id=e.undoes_event_id and a.op='ARCHIVE')))")
  GOV[${L}_pin]=$(PGQ "select count(*) from private.context_bindings where tenant_id='$t' and memory_id='${tgt:-00000000-0000-0000-0000-000000000000}' and mode='PINNED' and scope_kind='WORKSPACE' and scope_id='$w' and revoked_at is null")
  GOV[${L}_bind]=$(PGQ "select count(*) from private.context_bindings where tenant_id='$t' and memory_id='${bindable:-00000000-0000-0000-0000-000000000000}' and mode='MANDATORY' and scope_kind='TASK' and scope_id='${task:-00000000-0000-0000-0000-000000000000}' and revoked_at is null")
  GOV[${L}_affects]=$(PGQ "select count(*) from private.memory_affects where tenant_id='$t' and memory_id='${tgt:-00000000-0000-0000-0000-000000000000}'")
  GOV[${L}_own]=$(( $(PGQ "select count(*) from projection.stream_log where tenant_id='$t' and scope_kind='workspace' and scope_id='$w'") - own0 ))
  GOV[${L}_foreign]=$(( $(PGQ "select count(*) from projection.stream_log where tenant_id in ($SEEDED) and not (tenant_id='$t' and scope_kind='workspace' and scope_id='$w')") - foreign0 ))
  echo "governance $L: supersede=${GOV[${L}_supersede]} restore=${GOV[${L}_restore]} archive+unarchive=${GOV[${L}_archive]} pin=${GOV[${L}_pin]} bind=${GOV[${L}_bind]} affects=${GOV[${L}_affects]} own_tickets=${GOV[${L}_own]} foreign_tickets=${GOV[${L}_foreign]}" | tee -a $EV/rehearsal.log
}
governance_leg A "$BEARER" "$TENANT" "$WS" "$RDOM"
governance_leg B "$BEARER_B" "$TENANT_B" "$WS_B" "$RDOM_B"
GOV[tenants]=$(PGQ "select count(distinct tenant_id) from ops.memory_lifecycle_events where op='SUPERSEDE' and tenant_id in ('$TENANT','$TENANT_B') and memory_id in ('${GOV[A_target]/none/00000000-0000-0000-0000-000000000000}','${GOV[B_target]/none/00000000-0000-0000-0000-000000000000}')")
# Live replay witness (ADR-0054 D-C): tenant A's user mints `pin` in WS with $BEARER and presents
# the token with $BEARER_A2 (WS_A2, same user, second workspace) — one indistinguishable CONFLICT.
GOV_REPLAY_TARGET=${GOV[A_target]}
GOV_MINT=$(mcp_as "$BEARER" memory "{\"action\":\"pin\",\"memory_id\":\"$GOV_REPLAY_TARGET\"}")
GOV_TOKEN=$(print -r -- "$GOV_MINT" | head -1 | sc confirm_token)
GOV[replay]=$(mcp_as "$BEARER_A2" memory "{\"action\":\"pin\",\"memory_id\":\"$GOV_REPLAY_TARGET\",\"confirm_token\":\"$GOV_TOKEN\"}" | head -1 | sc code)
echo "governance: tenants_exercised=${GOV[tenants]} cross_workspace_replay=${GOV[replay]:-<none>}" | tee -a $EV/rehearsal.log
drain_all

# ---------- 6a1d3. card 31: the PINNED lane under §25.4.B(6) (ADR-0057 D-G, ruling C) ----------
# The PINNED floor is ProjectConstraint and project_active_constraints_v1 claims every such row, so
# a pin at the floor is delivered through Mandatory and counted in pinned_excluded; a below-floor
# pin (leg A's distilled target) is counted there and not returned (v2 authorization path). Fixture:
# one ProjectConstraint memory, the governance leg's shape (superuser insert, ADR-0044).
step pinned_lane
PN_AT=$(PGQ "with e as (insert into private.evidence_objects (tenant_id,evidence_kind,payload_sha256,data_class,origin_class,visibility_class,visibility_workspace_id,reasoning_domain_id) values ('$TENANT','EVENT',sha256(gen_random_uuid()::text::bytea),'INTERNAL','TenantAdmin','WORKSPACE_SHARED','$WS','$RDOM') returning evidence_id), m as (insert into private.memory_records (tenant_id,memory_type,content,visibility_class,visibility_workspace_id,authority_class,confidence,status,asserted_at) values ('$TENANT','NOTE','{\"rehearsal\":\"card31 pinned at the floor\"}','WORKSPACE_SHARED','$WS','ProjectConstraint',0.9,'active',clock_timestamp()) returning memory_id), l as (insert into private.memory_evidence (memory_id,evidence_id,role,grounding_mode) select m.memory_id, e.evidence_id, 'PRIMARY', 'SNAPSHOT' from m, e returning memory_id) select memory_id from l")
PN_BELOW=${GOV[A_target]:-none}
PN_BELOW_AUTH=$(PGQ "select authority_class from private.memory_records where memory_id='${PN_BELOW/none/00000000-0000-0000-0000-000000000000}'")
gated_as "$BEARER" memory "{\"action\":\"pin\",\"memory_id\":\"$PN_AT\"}" > $EV/pin_at_floor.json 2>&1
mcp context "{\"workspace_id\":\"$WS\"}" > $EV/context_pinned.json 2>&1
PN=$(head -1 $EV/context_pinned.json | python3 -c "
import sys,json
try:
    sc=json.load(sys.stdin)['result']['structuredContent']; h=sc['handoff']; c=h['counts']
    ids=lambda xs:{x['memory_id'] for x in xs}
    items={i['memory_id'] for i in sc['content']['items']}
    m,pn=ids(h['mandatory']),ids(h['pinned'])
    f=lambda x:'%d|%d|%d' % (x in m, x in items, x in pn)
    print(f(sys.argv[1]), f(sys.argv[2]), '%d|%d|%d' % (c['pinned_expected'], c['pinned_returned'], c['pinned_excluded']))
except Exception as e: print('unparsed unparsed unparsed(%s)' % type(e).__name__)" "$PN_AT" "$PN_BELOW")
PN_AT_V=${PN%% *}; PN_REST=${PN#* }; PN_BELOW_V=${PN_REST%% *}; PN_COUNTS=${PN_REST#* }
# Both pins must exist as live PINNED bindings, or "not returned" would be true for the wrong reason.
PN_BINDINGS=$(PGQ "select count(*) from private.context_bindings where tenant_id='$TENANT' and mode='PINNED' and revoked_at is null and memory_id in ('${PN_AT:-00000000-0000-0000-0000-000000000000}','${PN_BELOW/none/00000000-0000-0000-0000-000000000000}')")
echo "pinned lane: pinned_bindings=$PN_BINDINGS at_floor=$PN_AT (mandatory|items|pinned)=$PN_AT_V below_floor=$PN_BELOW authority=$PN_BELOW_AUTH (mandatory|items|pinned)=$PN_BELOW_V counts(expected|returned|excluded)=$PN_COUNTS" | tee -a $EV/rehearsal.log

# ---------- 6a1e. kill -9 every resident worker and recover (acceptance item 6) ----------
# SIGTERM drains, which is the case that is safe by construction; kill -9 is the case the leases
# exist to survive. Each worker is signalled ONLY through its own pidfile via own_signal, which
# refuses any PID whose `ps -o comm=` basename is not the expected binary (2026-09-09 incident).
step kill9_rotation
READY_BAD_BEFORE_CHAOS=$READY_BAD
own_signal $S/gw.pid humaux-gateway 9 15; start_gw; wait_ready gateway-after-kill9 gw_readyz
own_signal $S/rw.pid humaux-retrieval-worker 9 15; start_rw; wait_ready retrieval-worker-after-kill9 rw_readyz
own_signal $S/rp.pid humaux-retrieval-worker 9 15; start_rp; wait_ready projection-runner-after-kill9 rp_readyz
own_signal $S/pw.pid humaux-private-worker 9 15; start_pw
# The private worker's own --readyz is a one-shot PG check and would be green with the resident
# process dead. cw_readyz dials its UDS from the side that uses it, so it grades the listener.
wait_ready private-worker-after-kill9 cw_readyz
K9_RECOVERY_BAD=$((READY_BAD - READY_BAD_BEFORE_CHAOS))
# End-to-end proof the restarted processes are the ones serving: a recall crosses the gateway
# AND the retrieval worker's UDS. A probe that only touches PG cannot say this.
mcp recall "{\"query\":\"what must a new service expose before it gets traffic?\",\"workspace_id\":\"$WS\",\"mode\":\"semantic\"}" > $EV/recall_after_kill9.json 2>&1
K9_RECALL_ERROR=$(head -1 $EV/recall_after_kill9.json | python3 -c "
import sys,json
try: print('1' if json.load(sys.stdin)['result'].get('isError') else '0')
except Exception: print('1')")
drain_all
K9_STRANDED=$(PGQ "select count(*) from ops.jobs where tenant_id in ('$TENANT','$TENANT_B') and status='PROCESSING' and lease_expires_at is not null and lease_expires_at > now()")
# …and the projection runner's own leases (ADR-0052): after the drain none may still be live.
K9_STRANDED=$((K9_STRANDED + $(PGQ "select count(*) from projection.stream_log where tenant_id in ($SEEDED) and lease_owner is not null and lease_expires_at > now() and state='ISSUED'")))
# Exactly-once after a mid-flight kill: a re-run hop may NOT index the same memory twice into the
# same live projection slot. (Different versions/scopes are different slots by design, so the
# grouping is the slot, not the memory.)
K9_DUP_POINTS=$(PGQ "select count(*) from (select memory_id from projection.private_memory_points where tenant_id in ('$TENANT','$TENANT_B') and projection_live and retired_at is null group by memory_id, scope_id, projection_version, embedding_version having count(*) > 1) t")
echo "kill9 rotation: recovery_failures=$K9_RECOVERY_BAD recall_isError=$K9_RECALL_ERROR stranded_leases=$K9_STRANDED duplicate_live_points=$K9_DUP_POINTS" | tee -a $EV/rehearsal.log
# recall_after_kill9 ran before the drain and may legitimately read in-flight; this one may not.
a2_check kill9_drained

# ---------- 6a1f. card 31: a stalled projection runner surfaces as PROJECTION_LAG (ADR-0057 D-E) ----------
# SIGSTOP/SIGCONT go ONLY to the runner PID this script spawned, through own_signal (which checks
# the binary name); never a container, never a foreign PID. The write is distilled concurrently so
# its ticket is claimable and only the stopped runner holds it; the ticket state is read before the
# resume to prove that. Lag is the age of the oldest pending ticket, strictly beyond LAG_SECS.
step stall_lag
# An interrupted run must not leave the runner stopped: a SIGSTOPped process ignores the teardown's TERM.
# (obs_stop: the observability trap set in step observability stays armed through this one.)
trap "own_signal $S/rp.pid humaux-retrieval-worker CONT 0; obs_stop" EXIT
own_signal $S/rp.pid humaux-retrieval-worker STOP 0
ST_T0=$(date +%s)
mcp remember "{\"operation\":\"put\",\"content\":\"card 31 stall probe: the projection runner is paused while this write waits.\",\"idempotency_key\":\"stall-probe-$RANDOM\",\"workspace_id\":\"$WS\"}" > $EV/stall_put.json 2>&1
ST_EV=$(head -1 $EV/stall_put.json | sc evidence_id)
distill_once > /dev/null 2>&1 &
ST_DPID=$!
lag_probe() { mcp recall "{\"query\":\"which language do we prefer for backend services?\",\"workspace_id\":\"$WS\",\"mode\":\"semantic\"}" | head -1 | python3 -c "
import sys,json
try:
    sc=json.load(sys.stdin)['result']['structuredContent']; c=sc['completeness']
    print('%d %s %s %s' % ('PROJECTION_LAG' in c.get('degradations',[]), sc['pipeline']['projection']['current'], c.get('class'), c.get('reason')))
except Exception as e: print('x unparsed %s -' % type(e).__name__)"; }
ST_SECS=-1; ST_SEEN=""
while [ $(( $(date +%s) - ST_T0 )) -le $((LAG_SECS + 30)) ]; do
  ST_SEEN=$(lag_probe)
  [ "${ST_SEEN%% *}" = 1 ] && { ST_SECS=$(( $(date +%s) - ST_T0 )); break; }
  sleep 2
done
wait $ST_DPID 2>/dev/null
ST_STATE=$(PGQ "select s.state||'/lease='||coalesce(s.lease_owner,'none') from ops.outbox o join projection.stream_log s on s.tenant_id=o.tenant_id and s.commit_seq=o.commit_seq where o.evidence_id='${ST_EV:-00000000-0000-0000-0000-000000000000}' and o.event_type='EVIDENCE_ACCEPTED'")
echo "stall: runner stopped, lag seen after ${ST_SECS}s (threshold ${LAG_SECS}s) probe='$ST_SEEN' ticket_while_stopped=$ST_STATE" | tee -a $EV/rehearsal.log
own_signal $S/rp.pid humaux-retrieval-worker CONT 0
trap obs_stop EXIT
ST_C0=$(date +%s); ST_CLEAR_SECS=-1; ST_AFTER=""
while [ $(( $(date +%s) - ST_C0 )) -le 60 ]; do
  ST_AFTER=$(lag_probe)
  [ "${ST_AFTER%% *}" = 0 ] && [ "$(print -r -- "$ST_AFTER" | cut -d' ' -f2)" = True ] && { ST_CLEAR_SECS=$(( $(date +%s) - ST_C0 )); break; }
  sleep 2
done
echo "stall: runner resumed, lag cleared after ${ST_CLEAR_SECS}s (bounded 60) probe='$ST_AFTER'" | tee -a $EV/rehearsal.log
drain_all

# ---------- 6a2. card 17 RYW probe: the soak replay's shape, with and without `limit` ----------
# §55.1 reserves candidate depth to the registered profile. Card 16's post-drain replay sent
# "limit": <n>, which the gateway refuses BEFORE the embedding step and (until card 17) refused
# silently. Both legs are sent here against the real deployment so the report carries the two
# response codes and the two new operator lines side by side.
step ryw_probe
GWLOG_BEFORE=$(wc -l < $EV/gateway.log)
PUT=$(mcp remember "{\"operation\":\"put\",\"content\":\"card 17 ryw probe: a backend service must publish a readiness probe before traffic reaches it.\",\"idempotency_key\":\"ryw-probe-$RANDOM\",\"workspace_id\":\"$WS\"}")
RYW_TOKEN=$(print -r -- "$PUT" | head -1 | python3 -c "import sys,json;d=json.load(sys.stdin);print(d['result']['structuredContent']['consistency_token'])")
RYW_EV=$(print -r -- "$PUT" | head -1 | python3 -c "import sys,json;d=json.load(sys.stdin);print(d['result']['structuredContent']['evidence_id'])")
RYW_SEQ=$(PGQ "select stream_seq from ops.outbox where evidence_id='$RYW_EV' and stream_seq is not null limit 1")
RYW_HW=$(PGQ "select coalesce(max(projection_highwater),0) from projection.stream_checkpoints where tenant_id='$TENANT' and serving")
echo "ryw probe: token seq=$RYW_SEQ serving_highwater=$RYW_HW (expected overlay range $((RYW_HW+1))..$RYW_SEQ)" | tee -a $EV/rehearsal.log
# leg A — the shape card 16 sent: token + a caller-chosen `limit`
mcp recall "{\"query\":\"readiness probe before traffic\",\"workspace_id\":\"$WS\",\"mode\":\"semantic\",\"consistency_token\":\"$RYW_TOKEN\",\"limit\":17}" > $EV/ryw_with_limit.json 2>&1
echo "leg A (token + limit=17): http=$(tail -1 $EV/ryw_with_limit.json) code=$(head -1 $EV/ryw_with_limit.json | grep -oE '\"code\":\"[A-Z_]+\"' | head -1)" | tee -a $EV/rehearsal.log
# leg B — the shape card 17 sends: token, no `limit`
mcp recall "{\"query\":\"readiness probe before traffic\",\"workspace_id\":\"$WS\",\"mode\":\"semantic\",\"consistency_token\":\"$RYW_TOKEN\"}" > $EV/ryw_no_limit.json 2>&1
echo "leg B (token, no limit): http=$(tail -1 $EV/ryw_no_limit.json)" | tee -a $EV/rehearsal.log
head -1 $EV/ryw_no_limit.json | python3 -c "
import sys,json
d=json.load(sys.stdin); sc=d['result'].get('structuredContent',{})
items=sc.get('items',[])
seqs=sorted(i['stream_seq'] for i in items if 'stream_seq' in i)
print('leg B: isError=%s items=%d overlay_seqs=%s top_k=%s' % (
  d['result'].get('isError'), len(items), seqs,
  sc.get('provenance',{}).get('profile',{}).get('top_k')))
" | tee -a $EV/rehearsal.log
echo "--- gateway operator lines this probe produced ---" | tee -a $EV/rehearsal.log
tail -n +$((GWLOG_BEFORE+1)) $EV/gateway.log | tee -a $EV/rehearsal.log

# ---------- 6a3. card 30 planner lane substitution (ADR-0055 D-C) ----------
# Everyday queries the §20 planner classes as STATE/TEMPORAL/ASSOCIATION/LITERAL/DIRECT_GET used
# to be refused INVALID_INPUT (`query_not_semantic`). With no `mode`, dense answers each one and
# says so (`LANE_SUBSTITUTED` + `provenance.planner_class`); an explicit undelivered `mode` is the
# one refusal left, and it is the schema-documented DEPENDENCY_UNAVAILABLE.
step lane_substitution
LS_OK=0; LS_N=0
# A fixed UUID keeps this leg reproducible. Since ADR-0056 a random one is no longer refused
# (the seal runs no phone-like rule); step pst item 7 recalls with a random UUID on purpose.
LS_UUID=a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5
for q in "目前项目进度" "客户张三最近的情绪怎么样" "和支付相关的决定" 'the "frozen contract" decision' "$LS_UUID"; do
  LS_ARGS=$(python3 -c "import sys,json; print(json.dumps({'query':sys.argv[1],'workspace_id':sys.argv[2]}, ensure_ascii=False))" "$q" "$WS")
  LS_OUT=$(mcp recall "$LS_ARGS")
  LS_N=$((LS_N+1))
  LS_V=$(print -r -- "$LS_OUT" | head -1 | python3 -c "
import sys,json
try:
    d=json.load(sys.stdin)['result']; sc=d.get('structuredContent',{})
    pc=sc.get('provenance',{}).get('planner_class')
    ok=(not d.get('isError')) and 'LANE_SUBSTITUTED' in sc.get('completeness',{}).get('degradations',[]) and pc not in (None,'SEMANTIC')
    print('%d %s' % (ok, pc))
except Exception: print('0 unparsed')")
  echo "lane substitution: planner_class=${LS_V#* } answered=${LS_V%% *} $(print -r -- "$LS_OUT" | tail -1)" | tee -a $EV/rehearsal.log
  [ "${LS_V%% *}" = 1 ] && LS_OK=$((LS_OK+1))
done
mcp recall "{\"query\":\"readiness probe before traffic\",\"workspace_id\":\"$WS\",\"mode\":\"literal\"}" > $EV/recall_mode_literal.json 2>&1
LIT_CODE=$(head -1 $EV/recall_mode_literal.json | grep -oE '"code":"[A-Z_]+"' | head -1 | sed -E 's/.*:"([A-Z_]+)"/\1/')
echo "explicit mode literal: code=$LIT_CODE $(tail -1 $EV/recall_mode_literal.json)" | tee -a $EV/rehearsal.log

# ---------- 6b. assertions (ADR-0036 / card 14 witness) ----------
step assertions
A_OK=0; A_BAD=0
assert_eq() { # $1=label $2=actual $3=expected
  if [ "$2" = "$3" ]; then echo "ASSERTION PASS $1: $2" | tee -a $EV/rehearsal.log; A_OK=$((A_OK+1));
  else echo "ASSERTION FAIL $1: got '$2' want '$3'" | tee -a $EV/rehearsal.log; A_BAD=$((A_BAD+1)); fi
}
assert_gt() { # $1=label $2=actual $3=floor
  if [ "$2" -gt "$3" ] 2>/dev/null; then echo "ASSERTION PASS $1: $2 > $3" | tee -a $EV/rehearsal.log; A_OK=$((A_OK+1));
  else echo "ASSERTION FAIL $1: got '$2' want > $3" | tee -a $EV/rehearsal.log; A_BAD=$((A_BAD+1)); fi
}
# ADR-0036: neither derived worker may be handed a tenant/domain through the environment.
assert_eq "no_tenant_env_in_seed_exports" \
  "$(print -r -- "$SEED_OUT" | grep -cE '^export HUMAUX_(CONSOLIDATION_WORKER|PRIVATE_WORKER_DISTILL)_(TENANT_ID|WORKSPACE_ID|REASONING_DOMAIN_ID|BINDING_ID|BINDING_VERSION)=')" 0
assert_eq "no_tenant_env_in_worker_blocks" \
  "$(sed -n '/3b. distill/,/5. projection/p' $0 | grep -cE 'HUMAUX_(CONSOLIDATION_WORKER|PRIVATE_WORKER_DISTILL)_(TENANT_ID|REASONING_DOMAIN_ID|BINDING_ID|BINDING_VERSION)=')" 0
# The two derived hops claimed and settled this tenant's work cross-tenant.
# card 30 (ADR-0055 D-C): dense answers every planner class; only an explicit undelivered mode refuses.
assert_eq "recall_everyday_queries_answered_with_lane_substituted" "$LS_OK/$LS_N" "5/5"
assert_eq "recall_explicit_literal_mode_refused_dependency_unavailable" "$LIT_CODE" "DEPENDENCY_UNAVAILABLE"
assert_gt "derived_jobs_total" "$(PGQ "select count(*) from ops.jobs where tenant_id='$TENANT' and left(job_type,8)='DERIVED_'")" 0
# The RYW probe (step 6a2) deliberately writes a memory and does NOT drain it — an un-projected
# write is the entire point of a read-your-writes overlay probe. Both drain assertions therefore
# exclude that one Evidence, and a third assertion pins that it is the ONLY thing left undrained,
# so the exclusion cannot quietly swallow a second stranded row. (Card 21 fix pass: before this,
# these two were red on every run that reached them — card 16's final rehearsal log carries the
# same `projection_tickets_unresolved` failure — because they were written before the probe
# existed and were never re-scoped. An assertion that is always red proves nothing.)
assert_eq "derived_jobs_not_done" "$(PGQ "select count(*) from ops.jobs where tenant_id='$TENANT' and left(job_type,8)='DERIVED_' and status<>'DONE' and coalesce(payload->>'evidence_id','') <> '${RYW_EV:-none}'")" 0
assert_eq "the_only_undrained_write_is_the_ryw_probes_own" "$(PGQ "select count(*) from ops.outbox where tenant_id='$TENANT' and status<>'DONE' and evidence_id<>'${RYW_EV:-00000000-0000-0000-0000-000000000000}'")" 0
assert_gt "memory_records" "$(PGQ "select count(*) from private.memory_records where tenant_id='$TENANT'")" 0
assert_eq "projection_tickets_unresolved" "$(PGQ "select count(*) from projection.stream_log where tenant_id='$TENANT' and state not in ('DONE','SKIPPED_BY_POLICY') and stream_seq <> ${RYW_SEQ:-0}")" 0
# ---- card 21: derived-value hygiene, asserted on the live deployment ----
# §7.4: a disclosure row names the processor that received the private data. The retrieval
# worker used to build its embedding provider with ProcessorId(Uuid::nil()), so EVERY row it
# wrote said all-zeros. Asserted over the whole table for this tenant, not one row.
assert_gt "disclosure_rows_written" "$(PGQ "select count(*) from ops.data_disclosures where tenant_id='$TENANT'")" 0
assert_eq "disclosure_processor_never_nil" "$(PGQ "select count(*) from ops.data_disclosures where tenant_id='$TENANT' and processor_id='00000000-0000-0000-0000-000000000000'")" 0
assert_eq "disclosure_processor_is_the_seeded_identity" "$(PGQ "select count(*) from ops.data_disclosures where tenant_id='$TENANT' and processor_id<>'$EGRESS_PROC'")" 0
# Card 21 fix pass (reviewer P1): §15.4 + migration 0171 — the checkpoint this worker advanced
# names THIS worker. Before 0171 `projection.stream_checkpoints` had no column that could carry
# the answer, so the card's "a checkpoint written by one worker is attributed to it" was claimed
# but asserted nowhere. `advance_prefix` writes `projection_processor_id` in the SAME statement
# as `projection_highwater`, so a row whose watermark moved must name the mover.
assert_gt "checkpoints_advanced" "$(PGQ "select count(*) from projection.stream_checkpoints where tenant_id='$TENANT' and projection_highwater > 0")" 0
assert_eq "checkpoint_attributed_to_the_worker_that_advanced_it" "$(PGQ "select count(*) from projection.stream_checkpoints where tenant_id='$TENANT' and projection_highwater > 0 and projection_processor_id is distinct from '$EGRESS_PROC'")" 0
# §16.1.1: the fingerprint must be recomputable from the persisted row alone — the axis the run
# row records has to be the axis the hash consumed. (The byte-for-byte recomputation itself is
# `distill_hop_e2e::assert_fingerprint_recomputes`; this is its live counterpart.)
assert_gt "processing_runs_written" "$(PGQ "select count(*) from private.processing_runs where tenant_id='$TENANT'")" 0
assert_eq "source_hash_axis_is_the_stored_evidence_anchor" "$(PGQ "select count(*) from private.processing_runs r join private.evidence_objects e on e.evidence_id=r.evidence_id and e.tenant_id=r.tenant_id where r.tenant_id='$TENANT' and not (e.payload_sha256 = any(r.evidence_payload_sha256))")" 0
# §15.1/§78.1: one family, derived, not three hand-aligned copies. Every ticket this run issued
# carries the triple the seed emitted, and this script no longer configures the worker with it.
assert_gt "tickets_issued" "$(PGQ "select count(*) from projection.stream_log where tenant_id='$TENANT'")" 0
assert_eq "ticket_family_agrees_across_processes" "$(PGQ "select count(*) from projection.stream_log where tenant_id='$TENANT' and (domain,projection_kind,projection_version) is distinct from ('$DOMAIN','$PKIND','$PVER')")" 0
assert_eq "no_ticket_family_env_for_the_retrieval_worker" "$(grep -cE 'HUMAUX_RETRIEVAL_WORKER_(DOMAIN|PROJECTION_KIND|PROJECTION_VERSION)=' $0)" 0
# The flag is a hard error in the CLI now; this catches a re-introduction here. The pattern
# requires a following space or `$` so this assertion line cannot match itself (a self-matching
# scan is a gate that can never be green).
assert_eq "no_visible_shadow_flag_left_in_this_script" "$(grep -cE -- '--visible-shadow[ $]' $0)" 0
# Card 15 / ADR-0037: every process answered its own probe before any traffic was sent.
assert_eq "all_processes_ready_before_traffic" "$READY_BAD_BEFORE_TRAFFIC" 0
# Card 16 / ADR-0038 D5: a SIGTERM between passes may never strand a lease.
assert_eq "no_stranded_lease_after_sigterm" "$STRANDED_LEASES" 0
# ---- card 24 acceptance gate: the six witnesses (each one a named assertion, not prose) ----
# (1) two tenants isolated, on a corpus where BOTH sides are non-empty.
assert_gt "tenant_a_has_a_corpus" "$XT_POINTS_A" 0
assert_gt "tenant_b_has_a_corpus" "$XT_POINTS_B" 0
assert_eq "tenant_b_cannot_recall_tenant_a" "$XT_B_SEES_A" 0
assert_eq "tenant_a_cannot_recall_tenant_b" "$XT_A_SEES_B" 0
assert_eq "tenant_b_enumerates_no_foreign_memory" "$XT_B_ENUM_FOREIGN" 0
# (2) subject-scoped recall: only that subject's memories for the authorized caller, zero for the
#     unauthorized one (0155: subject visibility today = same tenant — see the step's comment).
assert_gt "subject_recall_returns_the_subjects_memories" "$SUBJ_A_N" 0
assert_eq "subject_recall_returns_nothing_unlinked" "$SUBJ_A_UNLINKED" 0
assert_eq "subject_recall_is_zero_for_an_unauthorized_caller" "$SUBJ_B_N" 0
# (3) supersede then restore, visible in the lifecycle log AND in recall.
assert_eq "supersede_is_in_the_lifecycle_log" "$LC_SUPERSEDE_EVENTS" 1
assert_eq "restore_undoes_that_supersede_event" "$LC_RESTORE_EVENTS" 1
assert_gt "restored_memory_is_servable_again" "$LC_RECALL_AFTER_RESTORE" 0
# (3b) card 4 / ADR-0024: archive excludes from recall, keeps memory.get, unarchive restores.
assert_eq "archived_row_is_excluded_from_recall" "$AR_RECALL_WHILE_ARCHIVED" 0
assert_eq "archived_row_still_answers_memory_get_with_the_flag" "$AR_GET_ARCHIVED" "true"
assert_gt "unarchive_makes_the_row_servable_again" "$AR_RECALL_AFTER_UNARCHIVE" 0
# (4) completeness class on a closed ledger is not cannot_establish.
assert_eq "enumerate_completeness_is_exact" "$CMP_CLASS" "exact"
# (6) every resident worker kill -9'd and recovered, exactly once.
assert_eq "every_worker_recovered_after_kill9" "$K9_RECOVERY_BAD" 0
assert_eq "recall_serves_again_after_kill9" "$K9_RECALL_ERROR" 0
assert_eq "no_stranded_lease_after_kill9" "$K9_STRANDED" 0
assert_eq "exactly_once_no_duplicate_live_points_after_kill9" "$K9_DUP_POINTS" 0
# Card 31 (ADR-0057): A2 closes in points after every lifecycle transition, numbers in the label.
for l in supersede restore archive unarchive correct correct_undo kill9_drained; do
  assert_eq "a2_after_${l}(${A2[${l}_n]:-not run})" "${A2[$l]:-}" 1
done
assert_eq "correct_retired_m1_point(n=${CR_ROWS:-0} registry rows: live|qdrant points)" "${CR_LIVE:-x}|${CR_QD:-x}|$([ "${CR_ROWS:-0}" -gt 0 ] && echo projected || echo never_projected)" "0|0|projected"
assert_eq "pinned_constraint_delivered_by_context_assemble(mandatory|items|pinned_lane, counts=$PN_COUNTS)" "$PN_AT_V|bindings=$PN_BINDINGS" "1|1|0|bindings=2"
assert_eq "pinned_below_floor_named_excluded(authority=$PN_BELOW_AUTH mandatory|items|pinned_lane, counts=$PN_COUNTS)" "$PN_BELOW_V|$(print -r -- "$PN_COUNTS" | awk -F'|' '{print ($1==$3 && $2==0)?"all_excluded":"not_all_excluded"}')|$([ -n "$PN_BELOW_AUTH" ] && [ "$PN_BELOW_AUTH" != ProjectConstraint ] && echo below_floor || echo not_below_floor)" "0|0|0|all_excluded|below_floor"
assert_eq "stall_yields_projection_lag_within_threshold(threshold=${LAG_SECS}s observed=${ST_SECS}s ticket_while_stopped=$ST_STATE)" "$([ "$ST_SECS" -ge "$LAG_SECS" ] && [ "$ST_SECS" -le $((LAG_SECS + 10)) ] && echo 1 || echo 0)" 1
assert_eq "stall_ticket_held_by_the_stopped_runner(n=1)" "${ST_STATE%%/*}" "ISSUED"
assert_eq "resume_clears_projection_lag(cleared_after=${ST_CLEAR_SECS}s bounded 60)" "$([ "$ST_CLEAR_SECS" -ge 0 ] && echo 1 || echo 0)" 1
# (7) card 29 / ADR-0054: governance ops on BOTH seeded tenants through one gateway that was started
#     without a default write pair; each leg's tickets on its own stream only; a token minted in one
#     workspace is refused in another. The boot witness greps this script's own gateway block; the
#     pattern cannot match this line (after REMEMBER_ comes a parenthesis here, not the key name).
assert_eq "gateway_boots_without_a_default_write_pair" "$(grep -cE 'HUMAUX_GATEWAY_REMEMBER_(TENANT|WORKSPACE)_ID=' $0)" 0
for L in A B; do
  assert_eq "gov_${L}_supersede_in_lifecycle_log" "${GOV[${L}_supersede]}" 1
  assert_eq "gov_${L}_restore_undoes_supersede" "${GOV[${L}_restore]}" 1
  assert_eq "gov_${L}_archive_and_unarchive_events" "${GOV[${L}_archive]}" 2
  assert_eq "gov_${L}_pin_binding_row" "${GOV[${L}_pin]}" 1
  assert_eq "gov_${L}_bind_binding_row" "${GOV[${L}_bind]}" 1
  assert_gt "gov_${L}_affect_rows" "${GOV[${L}_affects]}" 0
  assert_gt "gov_${L}_tickets_on_own_stream" "${GOV[${L}_own]}" 0
  assert_eq "gov_${L}_tickets_on_foreign_streams" "${GOV[${L}_foreign]}" 0
done
assert_eq "governance_tenants_exercised" "${GOV[tenants]}" 2
assert_eq "cross_workspace_token_replay_is_conflict" "${GOV[replay]}" "CONFLICT"
echo "ASSERTIONS $A_OK passed, $A_BAD failed" | tee -a $EV/rehearsal.log

# ============================================================================
# card 27 / ADR-0052 — step projection_serve_multi_tenant
# ONE tenant-free `humaux-retrieval-worker --serve` (start_rp, no tenant env) projects 3 tenants
# × 2 workspaces on live MiniMax distill + live DashScope + real Qdrant. Every assertion prints
# its n. Gate counts are scoped to the three seeded tenants ($SEEDED); dev leftovers in other
# tenants are counted once, up front, and never enter a gate. Runs after the card-24 table so its
# load cannot move those assertions. FAULT=revoke_claim: REVOKE the claim's EXECUTE before the
# timed load (a trap re-GRANTs on any exit) — projection_lag_within_120s must go red and the
# REHEARSAL VERDICT must fail; the rest of the run is skipped.
# ============================================================================
step projection_serve_multi_tenant
zmodload zsh/datetime
PST=$EV/pst; mkdir -p $PST
CLAIM_FN="projection.claim_issued_tickets(text,text,text,text,text,double precision,bigint,bigint)"
FAMS=("$TENANT $WS BEARER_A" "$TENANT $WS_A2 BEARER_A2" "$TENANT_B $WS_B BEARER_B" "$TENANT_B $WS_B2 BEARER_B2" "$TENANT_C $WS_C BEARER_C" "$TENANT_C $WS_C2 BEARER_C2")
echo "pst: rp config (one process, no tenant env): $RP_PASS_ENV" | tr -d '\\' | tr -s ' ' | tee -a $EV/rehearsal.log
echo "pst: dev leftovers outside the seeded tenants: $(PGQ "select count(*) from projection.stream_log where state='ISSUED' and tenant_id not in ($SEEDED)") ISSUED (never counted below)" | tee -a $EV/rehearsal.log
assert_eq "no_tenant_env_for_the_projection_runner" \
  "$(sed -n '/^start_rp() {/,/^}/p' $0 | grep -cE 'HUMAUX_RETRIEVAL_WORKER_(TENANT_ID|SCOPE_ID|SCOPE_KIND|QDRANT_COLLECTION)=')" 0
# The resident distiller for this step (the gate's deployment runs distill resident; the earlier
# steps drive it one pass at a time). Stopped at the end of the step; the soak starts its own.
( export PRIVATE_WORKER_PG_DSN="postgres://role_private_worker:${HUMAUX_ROLE_PASSWORD_PRIVATE_WORKER:?}@$PG/$DB" \
    HUMAUX_PRIVATE_WORKER_CREDENTIALS="$HUMAUX_PRIVATE_WORKER_CREDENTIALS" HUMAUX_PRIVATE_WORKER_HEALTH_RENEW_SECS=$PW_HEALTH_RENEW_SECS HUMAUX_PRIVATE_WORKER_HTTP_TIMEOUT_SECS=120 HUMAUX_PRIVATE_WORKER_PERMIT_TTL_SECS=60 \
    HUMAUX_PRIVATE_WORKER_DNS_PINS="$PW_DNS_PINS" HUMAUX_PRIVATE_WORKER_RPC_SOCKET_PATH=$SOCK/inference-pst.sock HUMAUX_PRIVATE_WORKER_CONSOLIDATION_UID=$MYUID \
    HUMAUX_PRIVATE_WORKER_CANDIDATE_TTL_SECONDS=86400 \
    HUMAUX_PRIVATE_WORKER_DISTILL_LEASE_SECS=30 HUMAUX_PRIVATE_WORKER_DISTILL_IN_FLIGHT=4 HUMAUX_PRIVATE_WORKER_DISTILL_HARD_DEADLINE_SECS=300 HUMAUX_PRIVATE_WORKER_DISTILL_NOT_READY_PARK_SECS=600 HUMAUX_PRIVATE_WORKER_DISTILL_MAX_ATTEMPTS=5 HUMAUX_PRIVATE_WORKER_DISTILL_BUDGET_WINDOW_SECS=60 HUMAUX_PRIVATE_WORKER_DISTILL_BUDGET_MAX_CALLS=120 \
    HUMAUX_PRIVATE_WORKER_DISTILL_POLL_INTERVAL_SECS=1 $OPS_PW_DISTILL
  set -a; source /Volumes/data/viral-skill-eval/.env; set +a
  exec "$BIN_DIR"/humaux-private-worker --distill-serve >> $EV/pst-distill.log 2>&1 ) &
own_pid ds $!
# …and the resident consolidation worker: every distilled load memory enqueues a
# DERIVED_CONSOLIDATE job (0164), and a step that left ~100 of them behind would sit, FIFO, in
# front of the next rehearsal's own jobs (run 2026-09-29 #2: derived_jobs_not_done red for exactly
# that reason). The step drains what it created before it ends.
( export CONSOLIDATION_WORKER_PG_DSN="postgres://role_consolidation_worker:${HUMAUX_ROLE_PASSWORD_CONSOLIDATION_WORKER:?}@$PG/$DB" \
    HUMAUX_CONSOLIDATION_WORKER_RPC_SOCKET_PATH=$SOCK/inference.sock HUMAUX_CONSOLIDATION_WORKER_CALL_TTL_SECS=120 HUMAUX_CONSOLIDATION_WORKER_DIAL_TIMEOUT_SECS=10 \
    HUMAUX_CONSOLIDATION_WORKER_MAX_INPUTS=50 HUMAUX_CONSOLIDATION_WORKER_LEASE_SECS=120 HUMAUX_CONSOLIDATION_WORKER_BATCH=8 HUMAUX_CONSOLIDATION_WORKER_MAX_ATTEMPTS=5 \
    HUMAUX_CONSOLIDATION_WORKER_POLL_INTERVAL_SECS=1 $OPS_CW_SERVE
  exec "$BIN_DIR"/humaux-consolidation-worker --serve >> $EV/pst-consolidation.log 2>&1 ) &
own_pid cw $!
pst_put() { # $1=bearer var $2=workspace $3=content $4=tsv — one remember.put, recorded with its put time
  local t0=$EPOCHREALTIME ev=
  ev=$(mcp_as "${(P)1}" remember "{\"operation\":\"put\",\"content\":\"$3\",\"idempotency_key\":\"$(uuidgen | tr A-Z a-z)\",\"workspace_id\":\"$2\"}" | head -1 | python3 -c "import sys,json
try: print(json.load(sys.stdin)['result']['structuredContent']['evidence_id'])
except Exception: print('')")
  if [ -n "$ev" ]; then print -r -- "$ev	$1	$2	$t0	$3" >> $4; else echo "pst: put refused ($1 $2)" | tee -a $EV/rehearsal.log; fi
}
ev_list() { awk -F'\t' '{printf "%s'"'"'%s'"'"'", (NR>1?",":""), $1}' $1; }
# settled/total tickets of the Evidence in $1
# A ticket is settled once the runner has nothing left to do with it: DONE, SKIPPED_BY_POLICY, or FAILED
# `distill_failed` — the §15.2 settlement of a DEAD distill (ADR-0058 R11: fail-closed after one re-ask,
# bounded by `distill_dead` below, retired by the operator). Counting that FAILED row as unsettled made
# `projection_lag_within_120s` and `crash_tickets_all_done` grade the model's reply a second time: card-33
# chain re-run 2026-10-03 11:09 went red on exactly one FAILED_OUTPUT_SCHEMA death in 60 puts.
tickets_settled() { PGQ "select count(*) filter (where s.state in ('DONE','SKIPPED_BY_POLICY') or (s.state='FAILED' and s.error_class='distill_failed'))||'/'||count(*) from ops.outbox o join projection.stream_log s on s.tenant_id=o.tenant_id and s.commit_seq=o.commit_seq where o.event_type='EVIDENCE_ACCEPTED' and o.evidence_id in ($(ev_list $1))"; }
wait_settled() { # $1=tsv $2=deadline secs; echoes seconds waited, 0 status when every ticket settled OK
  local i=0 st= want=$(wc -l < $1 | tr -d ' ')
  while [ $i -lt $2 ]; do
    st=$(tickets_settled $1)
    [ "$st" = "$want/$want" ] && { echo $i; return 0; }
    sleep 1; i=$((i+1))
  done
  echo "$i ($st)"; return 1
}
# Contents carry no uuid: gitleaks' generic-key rule flags one inside a distilled memory, and that
# card then fails `secret_scan_rejected` (run 2026-09-29 #3 — the load would grade the scanner).
NOUNS=(billing search ledger inventory payroll gateway audit catalog shipping pricing)
DAYS=(Monday Tuesday Wednesday Thursday Friday Saturday Sunday Monday Tuesday Wednesday)

# ---- 1. warm-up + first activation (an operator act until card 28, logged as such) ----
: > $PST/warm.tsv
f=0; for fam in $FAMS; do f=$((f+1)); set -- ${=fam}
  pst_put $3 $2 "Card27 warm-up family $f: the ${NOUNS[$f]} team owns the on-call rota for its own service." $PST/warm.tsv
done
W=$(wait_settled $PST/warm.tsv 600); echo "pst warm-up: $(wc -l < $PST/warm.tsv | tr -d ' ') tickets settled after ${W}s" | tee -a $EV/rehearsal.log
for fam in $FAMS; do set -- ${=fam}
  echo "pst: first activation of ($1, $2) — operator act until card 28 (ADR-0017)" | tee -a $EV/rehearsal.log
  cargo run -q -p xtask -- projection-serve --tenant $1 --workspace $2 --domain $DOMAIN --projection-kind $PKIND --version $PVER --retire-failed distill_failed,no_visible_memory_record 2>&1 | tail -1 | tee -a $EV/rehearsal.log
done

# ---- 2. timed load: 10 remember.put per family = 60 ----
if [ "${FAULT:-}" = "revoke_claim" ]; then
  trap "docker exec humaux-thread-pg psql -U postgres -d $DB -qc \"GRANT EXECUTE ON FUNCTION $CLAIM_FN TO role_retrieval_worker\"; obs_stop" EXIT
  PGQ "REVOKE EXECUTE ON FUNCTION $CLAIM_FN FROM role_retrieval_worker" >/dev/null
  echo "pst FAULT=revoke_claim: EXECUTE on the claim revoked from role_retrieval_worker (trap re-grants)" | tee -a $EV/rehearsal.log
fi
: > $PST/load.tsv
LOAD_T0=$EPOCHREALTIME
f=0; for fam in $FAMS; do f=$((f+1)); set -- ${=fam}
  for k in $(seq 1 10); do
    pst_put $3 $2 "Card27 family $f note $k: the ${NOUNS[$k]} service of team $f listens on port $((7000 + 100*f + k)) and ships every ${DAYS[$k]}." $PST/load.tsv
  done
done
LOAD_T1=$EPOCHREALTIME
N_LOAD=$(wc -l < $PST/load.tsv | tr -d ' ')
echo "pst load: $N_LOAD puts over 6 families in $(printf '%.1f' $((LOAD_T1 - LOAD_T0)))s" | tee -a $EV/rehearsal.log
# recall poller, concurrent with the drain: NO consistency_token, until every distilled memory is seen
cat > $S/pst_visible.py <<'PYEOF'
import json, os, subprocess, sys, time, urllib.request, uuid
from concurrent.futures import ThreadPoolExecutor
tsv, db, deadline = sys.argv[1], sys.argv[2], time.time() + float(sys.argv[3])
rows = [l.rstrip("\n").split("\t") for l in open(tsv) if l.strip()]
def pg(sql):
    return subprocess.run(["docker","exec","humaux-thread-pg","psql","-U","postgres","-d",db,"-Atc",sql],
                          capture_output=True, text=True).stdout.split()
def recall(bearer, ws, query):
    body = {"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"recall","arguments":{"query":query,"workspace_id":ws,"mode":"semantic"},
            "_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientInfo":{"name":"rehearsal","version":"1"},"io.modelcontextprotocol/clientCapabilities":{}}}}
    gw = os.environ.get("GW_URL", "http://127.0.0.1:8080")  # ADR-0061 D-K: a scratch gateway sets GW_URL
    req = urllib.request.Request(gw + "/mcp", data=json.dumps(body).encode(), method="POST", headers={
        "Content-Type":"application/json","Accept":"application/json, text/event-stream","MCP-Protocol-Version":"2026-07-28",
        "Mcp-Method":"tools/call","Mcp-Name":"recall","Origin":gw,"Authorization":"Bearer "+os.environ[bearer]})
    try:
        d = json.load(urllib.request.urlopen(req, timeout=30))
        return {i.get("memory_id") for i in d["result"].get("structuredContent",{}).get("items",[])}
    except Exception as e:
        print("recall error:", type(e).__name__, file=sys.stderr)
        return set()
seen = {}          # memory_id -> seconds from its put
mem_of = {}        # evidence -> [memory_id]
no_memory = set()
query_of = {}      # memory_id -> its key claim
errors = 0
while time.time() < deadline:
    evs = ",".join("'%s'" % r[0] for r in rows)
    # the query for a memory is its own distilled key claim: recall is top_k (5), and near-twin
    # memories of one family ("ships every Monday") would otherwise crowd a put's text out of it
    for line in subprocess.run(["docker","exec","humaux-thread-pg","psql","-U","postgres","-d",db,"-AtF","\t","-c",
            f"select me.evidence_id, me.memory_id, coalesce(m.content->>'key_claim', m.content->>'title', m.content::text) from private.memory_evidence me join private.memory_records m on m.memory_id=me.memory_id where m.status='active' and me.evidence_id in ({evs})"],
            capture_output=True, text=True).stdout.splitlines():
        e, m, claim = line.split("\t", 2); mem_of.setdefault(e, set()).add(m); query_of[m] = claim
    for e in pg(f"select o.evidence_id from ops.outbox o join projection.stream_log s on s.tenant_id=o.tenant_id and s.commit_seq=o.commit_seq where o.event_type='EVIDENCE_ACCEPTED' and o.evidence_id in ({evs}) and s.state='SKIPPED_BY_POLICY'"):
        no_memory.add(e)
    pending = [(r, m) for r in rows for m in mem_of.get(r[0], ()) if m not in seen]
    if not pending and all(r[0] in mem_of or r[0] in no_memory for r in rows):
        break
    def probe(item):
        r, m = item
        if m in recall(r[1], r[2], query_of.get(m, r[4])):
            return m, time.time() - float(r[3])
        return None
    with ThreadPoolExecutor(8) as pool:
        for hit in pool.map(probe, pending):
            if hit: seen[hit[0]] = hit[1]
    time.sleep(1)
lat = sorted(seen.values())
pct = lambda q: round(lat[min(len(lat)-1, int(q*len(lat)))], 1) if lat else None
total = sum(len(v) for v in mem_of.values())
print(json.dumps({"memories": total, "visible": len(seen), "missing": total - len(seen),
                  "evidence_without_memory": len(no_memory), "n": len(lat), "p50_s": pct(0.5), "p95_s": pct(0.95),
                  "max_s": round(lat[-1],1) if lat else None}))
PYEOF
python3 $S/pst_visible.py $PST/load.tsv $DB $([ "${FAULT:-}" = "revoke_claim" ] && echo 150 || echo 900) > $PST/visible.json 2>$PST/visible.stderr &
VIS_PID=$!
DRAIN=$(wait_settled $PST/load.tsv $([ "${FAULT:-}" = "revoke_claim" ] && echo 150 || echo 900)); DRAIN_RC=$?
wait $VIS_PID
# per ticket: put → DONE, and distill-complete → DONE (the §42 projection lag the runner owns)
LOAD_STATS=$(PGQ "select coalesce(string_agg(o.evidence_id||'|'||extract(epoch from s.settled_at)||'|'||extract(epoch from coalesce((select min(m.created_at) from private.memory_evidence me join private.memory_records m on m.memory_id=me.memory_id where me.evidence_id=o.evidence_id), o.processed_at)), ' '),'') from ops.outbox o join projection.stream_log s on s.tenant_id=o.tenant_id and s.commit_seq=o.commit_seq where o.event_type='EVIDENCE_ACCEPTED' and s.settled_at is not null and o.evidence_id in ($(ev_list $PST/load.tsv))")
LAG=$(python3 - "$PST/load.tsv" "$LOAD_STATS" <<'PYEOF'
import sys, json
put = {l.split("\t")[0]: float(l.split("\t")[3]) for l in open(sys.argv[1]) if l.strip()}
p2d, lag = [], []
for rec in sys.argv[2].split():
    e, done, ready = rec.split("|"); done, ready = float(done), float(ready)
    p2d.append(done - put[e]); lag.append(done - ready)
def s(v):
    v = sorted(v)
    return {"n": len(v), "p50": round(v[len(v)//2],1), "p95": round(v[min(len(v)-1,int(0.95*len(v)))],1), "max": round(v[-1],1)} if v else {"n": 0}
print(json.dumps({"put_to_done_s": s(p2d), "distill_done_to_ticket_done_s": s(lag)}))
PYEOF
)
echo "pst drain: waited ${DRAIN}s for $N_LOAD tickets; $LAG" | tee -a $EV/rehearsal.log
echo "pst recall (no consistency_token): $(cat $PST/visible.json)" | tee -a $EV/rehearsal.log
LAG_MAX=$(print -r -- "$LAG" | python3 -c "import sys,json; d=json.load(sys.stdin)['distill_done_to_ticket_done_s']; print(d.get('max', 9999))")
SETTLED=$(tickets_settled $PST/load.tsv)
# The assertion grades the runner's own share (distill done → ticket DONE, the §42 projection
# lag) and is named for exactly that. The card's literal gate "all tickets DONE within 120 s" of
# the puts is NOT graded here: put→DONE is bounded by live MiniMax distill (one resident
# distiller), which card 27 does not change. It is printed as GATE-LITERAL, MET or NOT MET, and
# never folded into the verdict under another name — the main line decides it (review 2026-09-29).
assert_eq "projection_lag_within_120s(n=$N_LOAD settled=$SETTLED lag_max_s=$LAG_MAX)" \
  "$([ "$SETTLED" = "$N_LOAD/$N_LOAD" ] && python3 -c "print(1 if float('$LAG_MAX') <= 120 else 0)" || echo 0)" 1
PUT_DONE=$(print -r -- "$LAG" | python3 -c "import sys,json; d=json.load(sys.stdin)['put_to_done_s']; print('n=%s p95=%s max=%s' % (d.get('n'), d.get('p95'), d.get('max')))")
PUT_MAX=$(print -r -- "$LAG" | python3 -c "import sys,json; print(json.load(sys.stdin)['put_to_done_s'].get('max', 9999))")
echo "GATE-LITERAL all_tickets_done_within_120s_of_put: $([ "$SETTLED" = "$N_LOAD/$N_LOAD" ] && python3 -c "print('MET' if float('$PUT_MAX') <= 120 else 'NOT MET')" || echo 'NOT MET') (put->DONE $PUT_DONE settled=$SETTLED; main-line decision, not in the verdict)" | tee -a $EV/rehearsal.log
if [ "${FAULT:-}" = "revoke_claim" ]; then
  own_signal $S/ds.pid humaux-private-worker TERM 90; own_signal $S/cw.pid humaux-consolidation-worker TERM 90
  echo "pst FAULT=revoke_claim: stopping here (steps 3-6 and the soak need a working claim)" | tee -a $EV/rehearsal.log
  own_signal $S/gw.pid humaux-gateway TERM 30; own_signal $S/rw.pid humaux-retrieval-worker TERM 30
  own_signal $S/rp.pid humaux-retrieval-worker TERM 90; own_signal $S/pw.pid humaux-private-worker TERM 30
  own_signal $S/mh.pid humaux-maintenance TERM 30; obs_stop
  echo "REHEARSAL VERDICT: $A_OK passed, $A_BAD failed" | tee -a $EV/rehearsal.log
  [ "${A_BAD:-1}" -eq 0 ] || exit 1
  exit 0
fi
VIS=$(cat $PST/visible.json)
assert_eq "recall_without_token_returns_every_memory(n=$(print -r -- "$VIS" | python3 -c "import sys,json; print(json.load(sys.stdin)['memories'])"))" \
  "$(print -r -- "$VIS" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['missing'] if d['memories'] > 0 else -1)")" 0

# ---- 2b. card 32 (ADR-0058 §7) twins of the live gate `distill_fairness_live` ----
# M2: tenant A queues 40 puts, then tenant B puts ONE row; the tenant-fair claim (least-recently-
# served tenant first, four slots) must finish B within 60 s of its put whatever A's queue. The
# queue depth at B's put is graded too, so a drained queue cannot make the assertion vacuous.
: > $PST/fair_a.tsv; : > $PST/fair_b.tsv
for k in $(seq 1 40); do
  pst_put BEARER_A $WS "Card32 fairness queue note $k: the ${NOUNS[$(( (k - 1) % 10 + 1 ))]} batch of shard $k closes at $(( k % 12 + 1 )) pm." $PST/fair_a.tsv
done
pst_put BEARER_B $WS_B "Card32 fairness single note: the tenant B billing export runs every Friday at noon." $PST/fair_b.tsv
FAIR_B=$(cut -f1 $PST/fair_b.tsv | head -1); FAIR_B_T0=$(cut -f4 $PST/fair_b.tsv | head -1)
FAIR_A_AT_PUT=$(PGQ "select count(*) from ops.outbox where evidence_id in ($(ev_list $PST/fair_a.tsv)) and status in ('PENDING','PROCESSING')")
for i in $(seq 1 180); do
  [ "$(PGQ "select status in ('DONE','FAILED') from ops.outbox where evidence_id='${FAIR_B:-00000000-0000-0000-0000-000000000000}'")" = t ] && break
  sleep 1
done
FAIR_B_LAT=$(PGQ "select coalesce(round((extract(epoch from processed_at) - ${FAIR_B_T0:-0})::numeric, 1)::text, '9999') from ops.outbox where evidence_id='${FAIR_B:-00000000-0000-0000-0000-000000000000}' and status = 'DONE'")
FAIR_A_AT_DONE=$(PGQ "select count(*) from ops.outbox a, ops.outbox b where b.evidence_id='${FAIR_B:-00000000-0000-0000-0000-000000000000}' and a.evidence_id in ($(ev_list $PST/fair_a.tsv)) and (a.processed_at is null or a.processed_at > b.processed_at)")
echo "pst fairness: tenant B single row put->DONE ${FAIR_B_LAT:-none}s; tenant A queue at B's put=$FAIR_A_AT_PUT at B's DONE=$FAIR_A_AT_DONE" | tee -a $EV/rehearsal.log
assert_gt "tenantA_has_queue_at_tenantB_put" "$FAIR_A_AT_PUT" 0
assert_eq "tenantB_single_row_done_within_60s_while_A_has_queue(latency_s=${FAIR_B_LAT:-none} a_queue_at_done=$FAIR_A_AT_DONE)" \
  "$(python3 -c "print(1 if float('${FAIR_B_LAT:-9999}' or 9999) <= 60 else 0)")" 1
# M8: tenant C's two reasoning domains (seed_c --second-domain): three puts per domain user, every
# Evidence distilled, none FAILED (P1-4: a job only ever takes its own Evidence, by its own domain).
: > $PST/twodom.tsv
for k in 1 2 3; do
  pst_put BEARER_C $WS_C "Card32 domain one note $k: the ${NOUNS[$k]} rollout of team C needs sign-off by $(( k + 8 )) am." $PST/twodom.tsv
  pst_put BEARER_C_D2 $WS_C "Card32 domain two note $k: the ${NOUNS[$(( k + 3 ))]} review of team C is booked on ${DAYS[$k]}." $PST/twodom.tsv
done
# M7: five puts that carry feelings. The gateway stamps every remember.put AuthenticatedAgent
# (bins/gateway/src/remember.rs, `origin_class`); ADR-0058 D-P (amended by the card-32 review) offers
# the affect menu to that origin, so this ingress must yield inferred (origin DISTILL) rows, an
# affect-filtered recall must return one of their memories, and no row may pass the 5000 bp ceiling.
# All three are in the verdict. Rehearsal profile: $PW_CAPABILITIES (ADR-0058 R10),
# §72.3 distill budget 120 calls per tenant per 60 s (ADR-0058 D-M / D-T).
: > $PST/affect.tsv
for t in "I am thrilled: the migration finished two days early and the whole team celebrated together." \
         "Honestly I feel anxious about tomorrow's launch; the load tests kept failing all week." \
         "I was furious when the vendor cancelled our contract without any warning yesterday." \
         "I feel deeply grateful to Maria for staying late to fix the billing outage with me." \
         "Losing the Hamburg customer left me sad and exhausted after months of work on that account."; do
  pst_put BEARER_B $WS_B "$t" $PST/affect.tsv
done
cat $PST/fair_a.tsv $PST/fair_b.tsv $PST/twodom.tsv $PST/affect.tsv > $PST/c32.tsv
C32_WAIT=$(wait_settled $PST/c32.tsv 600)
echo "pst card32 twins: $(wc -l < $PST/c32.tsv | tr -d ' ') tickets settled after ${C32_WAIT}s" | tee -a $EV/rehearsal.log
TWODOM=$(PGQ "select count(distinct e.reasoning_domain_id)||' '||count(*) filter (where o.status='DONE')||' '||count(*) filter (where o.status='FAILED') from ops.outbox o join private.evidence_objects e using (evidence_id) where o.evidence_id in ($(ev_list $PST/twodom.tsv))")
echo "pst two domains (tenant C, second domain $RDOM_C2): domains done failed = $TWODOM" | tee -a $EV/rehearsal.log
assert_eq "one_tenant_two_domains_all_distilled_none_failed(n=$(wc -l < $PST/twodom.tsv | tr -d ' '))" "$TWODOM" "2 6 0"
AFF_MEM=$(PGQ "select me.memory_id from private.memory_affects a join private.memory_evidence me on me.memory_id = a.memory_id where a.tenant_id='$TENANT_B' and a.origin='DISTILL' and me.evidence_id in ($(ev_list $PST/affect.tsv)) order by a.created_at limit 1")
AFF_ROWS=$(PGQ "select count(*)||' '||count(*) filter (where a.confidence_bp > 5000) from private.memory_affects a join private.memory_evidence me on me.memory_id = a.memory_id where a.tenant_id='$TENANT_B' and a.origin='DISTILL' and me.evidence_id in ($(ev_list $PST/affect.tsv))")
AFF_HIT=0
if [ -n "$AFF_MEM" ]; then
  AFF_Q=$(PGQ "select coalesce(content->>'key_claim', content->>'title', content::text) from private.memory_records where memory_id='$AFF_MEM'" | python3 -c "import sys,json; print(json.dumps(sys.stdin.read().strip()))")
  mcp_as "$BEARER_B" recall "{\"query\":$AFF_Q,\"workspace_id\":\"$WS_B\",\"mode\":\"semantic\",\"affect\":{\"kinds\":[\"EMOTION\"]}}" > $PST/affect_recall.json 2>&1
  AFF_HIT=$(head -1 $PST/affect_recall.json | python3 -c "
import sys,json
try: print(int('$AFF_MEM' in {i.get('memory_id') for i in json.load(sys.stdin)['result']['structuredContent'].get('items',[])}))
except Exception: print(0)")
fi
echo "pst inferred affect: rows over_ceiling = ${AFF_ROWS:-none}; memory ${AFF_MEM:-none}; recall(affect EMOTION) hit=$AFF_HIT" | tee -a $EV/rehearsal.log
AFF_ORIGINS=$(PGQ "select string_agg(distinct origin_class, ',') from private.evidence_objects where evidence_id in ($(ev_list $PST/affect.tsv))")
assert_gt "rehearsal_inferred_affect_rows(evidence_origin=$AFF_ORIGINS)" "${AFF_ROWS%% *}" 0
assert_eq "rehearsal_inferred_affect_row_recalled_by_affect_filter(memory=${AFF_MEM:-none})" "$AFF_HIT" 1
assert_eq "inferred_affect_within_ceiling" "${AFF_ROWS##* }" 0

# ---- 3. crash: SIGKILL the runner mid-batch, restart, converge ----
: > $PST/crash.tsv
CRASH_N=(3 2 3 2)   # 10 puts on tenants A + B, both workspaces each
for j in 1 2 3 4; do set -- ${=FAMS[$j]}
  for k in $(seq 1 ${CRASH_N[$j]}); do pst_put $3 $2 "Card27 crash note $k for family $j: the rollout of build $((100*j + k)) waits for a green canary." $PST/crash.tsv; done
done
i=0; LIVE=0
while [ $i -lt 3000 ]; do   # 0.2 s polls, bounded at 600 s
  LIVE=$(PGQ "select count(*) from projection.stream_log where tenant_id in ($SEEDED) and lease_owner is not null and lease_expires_at > now()")
  [ "$LIVE" -gt 0 ] && break; sleep 0.2; i=$((i+1))
done
echo "pst crash: $LIVE live leases held by rp pid $(cat $S/rp.pid) — SIGKILL" | tee -a $EV/rehearsal.log
own_signal $S/rp.pid humaux-retrieval-worker 9 15 | tee -a $EV/rehearsal.log
start_rp; wait_ready projection-runner-after-crash rp_readyz
C_WAIT=$(wait_settled $PST/crash.tsv 600)
echo "pst crash: all $(wc -l < $PST/crash.tsv | tr -d ' ') tickets settled ${C_WAIT}s after restart" | tee -a $EV/rehearsal.log
assert_eq "crash_killed_the_runner_mid_batch(n=$LIVE live leases)" "$([ "$LIVE" -gt 0 ] && echo 1 || echo 0)" 1
assert_eq "duplicate_live_points(n=3 tenants)" "$(PGQ "select count(*) from (select memory_id from projection.private_memory_points where tenant_id in ($SEEDED) and projection_live and retired_at is null group by memory_id, scope_id, projection_version, embedding_version having count(*) > 1) t")" 0
assert_eq "stranded_leases(n=$(PGQ "select count(*) from projection.stream_log where tenant_id in ($SEEDED)") tickets)" "$(PGQ "select count(*) from projection.stream_log where tenant_id in ($SEEDED) and lease_owner is not null and (state <> 'ISSUED' or lease_expires_at < now())")" 0
assert_eq "crash_tickets_all_done(n=$(wc -l < $PST/crash.tsv | tr -d ' '))" "$(tickets_settled $PST/crash.tsv | awk -F/ '{print ($1==$2)?1:0}')" 1

# ---- 4. Qdrant unreachable for 30 s (a closed loopback port; the container is never touched) ----
DEAD_PORT=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); p=s.getsockname()[1]; s.close(); print(p)")
own_signal $S/rp.pid humaux-retrieval-worker TERM 90 | tee -a $EV/rehearsal.log
start_rp $DEAD_PORT
echo "pst outage: rp restarted with Qdrant at 127.0.0.1:$DEAD_PORT (closed)" | tee -a $EV/rehearsal.log
: > $PST/outage.tsv
for j in 3 4; do set -- ${=FAMS[$j]}
  for k in 1 2; do pst_put $3 $2 "Card27 outage note $k for family $j: invoices are reconciled against the bank feed at $((k+1)) am." $PST/outage.tsv; done
done
N_OUT=$(wc -l < $PST/outage.tsv | tr -d ' ')
OUT_Q="from ops.outbox o join projection.stream_log s on s.tenant_id=o.tenant_id and s.commit_seq=o.commit_seq where o.event_type='EVIDENCE_ACCEPTED' and o.evidence_id in ($(ev_list $PST/outage.tsv))"
# Each ticket is graded at the moment its FIRST attempt has failed (the four distill at different
# times, so one global snapshot could catch an early one already on attempt 2 after its 30 s
# backoff): ISSUED, attempts 1, lease cleared, backing off, class qdrant_upsert_failed.
typeset -A OUT_SEEN; OUT_T0=; i=0
while [ $i -lt 600 ] && [ ${#OUT_SEEN} -lt $N_OUT ]; do
  # stop early once every ticket that still has a distill has been seen (a dead one never will be)
  OUT_DEAD_NOW=$(PGQ "select count(*) from ops.jobs j where j.job_type='DERIVED_DISTILL' and j.status='DEAD' and j.payload->>'evidence_id' in ($(ev_list $PST/outage.tsv))")
  [ $(( ${#OUT_SEEN} + ${OUT_DEAD_NOW:-0} )) -ge $N_OUT ] && break
  for seq in $(PGQ "select s.tenant_id||':'||s.stream_seq $OUT_Q and s.state='ISSUED' and s.attempts=1 and s.next_attempt_at > now() and s.lease_owner is null and s.error_class='qdrant_upsert_failed'"); do
    OUT_SEEN[$seq]=1; [ -z "$OUT_T0" ] && OUT_T0=$EPOCHREALTIME
  done
  sleep 1; i=$((i+1))
done
[ -z "$OUT_T0" ] && OUT_T0=$EPOCHREALTIME
# ADR-0058 R11: an Evidence whose distill died (DEAD FAILED_OUTPUT_SCHEMA after the one re-ask; MiniMax
# NUL-character replies, card-34b chain 2026-10-04) never reaches ISSUED/attempts=1 and is graded by
# `distill_dead` below, not here. The population of this witness is the outage tickets that still have a
# distill; it must keep at least two members or the step is red naming the dead count.
OUT_DEAD=$(PGQ "select count(*) from ops.jobs j where j.job_type='DERIVED_DISTILL' and j.status='DEAD' and j.payload->>'evidence_id' in ($(ev_list $PST/outage.tsv))")
N_OUT_LIVE=$(( N_OUT - ${OUT_DEAD:-0} ))
echo "pst outage: tickets=$N_OUT distill_dead=${OUT_DEAD:-0} graded=$N_OUT_LIVE seen_attempts_1=${#OUT_SEEN}" | tee -a $EV/rehearsal.log
assert_eq "outage_population_keeps_two_live_tickets(n=$N_OUT dead=${OUT_DEAD:-0})" "$([ "$N_OUT_LIVE" -ge 2 ] && echo 1 || echo 0)" 1
assert_eq "outage_tickets_retry_with_attempts_1(n=$N_OUT_LIVE of $N_OUT)" "${#OUT_SEEN}" "$N_OUT_LIVE"
# ADR-0058 R11: the two outage witnesses count tickets FAILED by the projection path (every class
# except `distill_failed`); a distill that died is graded under its own name by `distill_dead` below.
assert_eq "no_ticket_failed_during_outage(n=$(PGQ "select count(*) from projection.stream_log where tenant_id in ($SEEDED)") tickets)" "$(PGQ "select count(*) from projection.stream_log where tenant_id in ($SEEDED) and state='FAILED' and error_class is distinct from 'distill_failed'")" 0
sleep $(( 30 - (EPOCHREALTIME - OUT_T0) > 0 ? 30 - (EPOCHREALTIME - OUT_T0) : 0 ))
own_signal $S/rp.pid humaux-retrieval-worker TERM 90 | tee -a $EV/rehearsal.log
start_rp; wait_ready projection-runner-after-outage rp_readyz
O_WAIT=$(wait_settled $PST/outage.tsv 120); O_RC=$?
echo "pst outage: restored after $(printf '%.0f' $((EPOCHREALTIME - OUT_T0)))s; $N_OUT tickets settled ${O_WAIT}s after restore" | tee -a $EV/rehearsal.log
assert_eq "outage_tickets_done_after_restore(n=$N_OUT within 120s)" "$O_RC" 0

# ---- 5. permanent fault, last and on its own family (C / ws2) ----
own_signal $S/rp.pid humaux-retrieval-worker TERM 90 | tee -a $EV/rehearsal.log
: > $PST/perm.tsv
pst_put BEARER_C2 $WS_C2 "Card27 permanent fault: the archive bucket keeps snapshots for ninety days." $PST/perm.tsv
PERM_EV=$(cut -f1 $PST/perm.tsv)
i=0; while [ $i -lt 600 ]; do [ "$(PGQ "select status from ops.outbox where evidence_id='$PERM_EV' and event_type='EVIDENCE_ACCEPTED'")" = "DONE" ] && break; sleep 1; i=$((i+1)); done
PERM_SEQ=$(PGQ "select s.stream_seq from ops.outbox o join projection.stream_log s on s.tenant_id=o.tenant_id and s.commit_seq=o.commit_seq where o.evidence_id='$PERM_EV' and o.event_type='EVIDENCE_ACCEPTED'")
# --run-once claims across ALL tenants: it must find nothing claimable but this one ticket, or the
# 512-d fault would land on someone else's ticket. Checked, never assumed.
PERM_OTHERS=$(PGQ "select count(*) from projection.stream_log s join projection.tenant_placements p on p.tenant_id=s.tenant_id and p.projection_family='private_memory_v1' where s.state='ISSUED' and s.scope_kind='workspace' and s.domain='$DOMAIN' and s.projection_kind='$PKIND' and s.projection_version='$PVER' and (s.lease_expires_at is null or s.lease_expires_at < now()) and (s.next_attempt_at is null or s.next_attempt_at <= now()) and not exists (select 1 from ops.outbox o where o.tenant_id=s.tenant_id and o.commit_seq=s.commit_seq and o.event_type='EVIDENCE_ACCEPTED' and o.status in ('PENDING','PROCESSING')) and not (s.tenant_id='$TENANT_C' and s.scope_id='$WS_C2' and s.stream_seq=${PERM_SEQ:-0})")
PTS_BEFORE=$(curl -s -X POST "http://127.0.0.1:6333/collections/$COLLECTION/points/count" -H 'Content-Type: application/json' --data "{\"exact\":true,\"filter\":{\"must\":[{\"key\":\"tenant_id\",\"match\":{\"value\":\"$TENANT_C\"}},{\"key\":\"workspace_id\",\"match\":{\"value\":\"$WS_C2\"}}]}}" | python3 -c "import sys,json; print(json.load(sys.stdin)['result']['count'])")
if [ "$PERM_OTHERS" = "0" ]; then
  ( export HUMAUX_RETRIEVAL_WORKER_PG_DSN="postgres://role_retrieval_worker:${HUMAUX_ROLE_PASSWORD_RETRIEVAL_WORKER:?}@$PG/$DB" \
      HUMAUX_RETRIEVAL_WORKER_EMBEDDING_PROVIDER=dashscope HUMAUX_RETRIEVAL_WORKER_EMBEDDING_MODEL=$EMB_MODEL HUMAUX_RETRIEVAL_WORKER_MODEL_REVISION=$EMB_REV \
      HUMAUX_RETRIEVAL_WORKER_DIMENSION=512 HUMAUX_RETRIEVAL_WORKER_EMBEDDING_VERSION=$EMB_VER HUMAUX_RETRIEVAL_WORKER_REGION=$EMB_REGION HUMAUX_RETRIEVAL_WORKER_MAX_INPUT_TOKENS=$EMB_MAX_TOK \
      HUMAUX_RETRIEVAL_WORKER_QDRANT_HOST=127.0.0.1 HUMAUX_RETRIEVAL_WORKER_QDRANT_PORT=6333 HUMAUX_RETRIEVAL_WORKER_QDRANT_CIDR=127.0.0.1/32 HUMAUX_RETRIEVAL_WORKER_QDRANT_TLS=false \
      HUMAUX_RETRIEVAL_WORKER_CELL_ID=$CELL_ID HUMAUX_RETRIEVAL_WORKER_CALLER=retrieval-worker \
      HUMAUX_RETRIEVAL_WORKER_GITLEAKS_BIN=$GITLEAKS_BIN HUMAUX_RETRIEVAL_WORKER_GITLEAKS_SHA256=$GITLEAKS_SHA HUMAUX_RETRIEVAL_WORKER_GITLEAKS_VERSION=$GITLEAKS_VER
    eval "export $RP_PASS_ENV"; export HUMAUX_RETRIEVAL_WORKER_BATCH=1 HUMAUX_RETRIEVAL_WORKER_PER_TENANT_CAP=1
    set -a; source $R/.env.local; set +a
    exec "$BIN_DIR"/humaux-retrieval-worker --run-once ) 2>&1 | tee -a $EV/projection-runner.log | tail -2 | tee -a $EV/rehearsal.log
else
  echo "pst permanent fault NOT injected: $PERM_OTHERS other claimable ticket(s) exist and would take the fault" | tee -a $EV/rehearsal.log
fi
PERM_ROW=$(PGQ "select state||'|'||coalesce(error_class,'-') from projection.stream_log where tenant_id='$TENANT_C' and scope_id='$WS_C2' and stream_seq=${PERM_SEQ:-0}")
PTS_AFTER=$(curl -s -X POST "http://127.0.0.1:6333/collections/$COLLECTION/points/count" -H 'Content-Type: application/json' --data "{\"exact\":true,\"filter\":{\"must\":[{\"key\":\"tenant_id\",\"match\":{\"value\":\"$TENANT_C\"}},{\"key\":\"workspace_id\",\"match\":{\"value\":\"$WS_C2\"}}]}}" | python3 -c "import sys,json; print(json.load(sys.stdin)['result']['count'])")
PERM_REG=$(PGQ "select count(*) from projection.private_memory_points p join private.memory_evidence me on me.memory_id=p.memory_id where me.evidence_id='$PERM_EV' and p.retired_at is null")
echo "pst permanent: ticket seq=$PERM_SEQ row=$PERM_ROW others_claimable=$PERM_OTHERS qdrant_points(C/ws2) before=$PTS_BEFORE after=$PTS_AFTER live_registry_rows=$PERM_REG" | tee -a $EV/rehearsal.log
assert_eq "permanent_fault_ticket_failed_with_class(n=1)" "$PERM_ROW" "FAILED|qdrant_upsert_rejected"
assert_eq "permanent_fault_memory_has_no_point(n=1 memory: registry rows, new qdrant points)" "$PERM_REG|$((PTS_AFTER - PTS_BEFORE))" "0|0"
assert_eq "other_tickets_unaffected(n=$(PGQ "select count(*) from projection.stream_log where tenant_id in ($SEEDED)") tickets)" \
  "$(PGQ "select count(*) from projection.stream_log where tenant_id in ($SEEDED) and state not in ('DONE','SKIPPED_BY_POLICY','RETIRED_FAILED') and not (state='FAILED' and error_class='distill_failed') and not (tenant_id='$TENANT_C' and scope_id='$WS_C2' and stream_seq=${PERM_SEQ:-0})")" 0
cargo run -q -p xtask -- projection-serve --tenant $TENANT_C --workspace $WS_C2 --domain $DOMAIN --projection-kind $PKIND --version $PVER --retire-failed qdrant_upsert_rejected 2>&1 | tail -1 | tee -a $EV/rehearsal.log
start_rp; wait_ready projection-runner-after-permanent rp_readyz

# ---- 6. EXPLAIN: no Seq Scan on ops.outbox / ops.jobs / private.memory_evidence ----
cat > $S/explain_gate.py <<'PYEOF'
#!/usr/bin/env python3
# card 27 EXPLAIN gate: plans of the projection claim, the 0164 job claim (consolidation only since
# 0193; both via auto_explain, inside BEGIN..ROLLBACK), the retrieve.rs RYW overlay, the distill
# job's own outbox take and the v2 claim's tenant/job picks (card 32, EXPLAIN only).
# usage: explain_gate.py <db> <tenant> <workspace> <out_dir> [--no-projection-claim]
# prints one line per plan: "plan=<name> nodes=<n> seq_scan_hot=<k> <tables>" and a summary.
import json, re, subprocess, sys, uuid

db, tenant, ws, out = sys.argv[1:5]
with_proj = "--no-projection-claim" not in sys.argv
HOT = {("ops", "outbox"), ("ops", "jobs"), ("private", "memory_evidence")}
U = str(uuid.UUID(int=0))

overlay = f"""SELECT sl.stream_seq, sl.state, ob.evidence_id,
       array_agg(DISTINCT mr.memory_id) FILTER (WHERE mr.memory_id IS NOT NULL) AS memory_ids
FROM projection.stream_log sl
JOIN ops.outbox ob ON ob.tenant_id = sl.tenant_id AND ob.commit_seq = sl.commit_seq
JOIN private.evidence_objects evidence
  ON evidence.evidence_id = ob.evidence_id AND evidence.tenant_id = sl.tenant_id
LEFT JOIN private.memory_evidence me ON me.evidence_id = evidence.evidence_id
LEFT JOIN private.memory_records mr
  ON mr.memory_id = me.memory_id AND mr.tenant_id = sl.tenant_id
 AND mr.status = 'active'
 AND (mr.visibility_class = 'TENANT_SHARED'
   OR (mr.visibility_class = 'USER_PRIVATE' AND mr.visibility_user_id = '{U}')
   OR (mr.visibility_class = 'WORKSPACE_SHARED' AND mr.visibility_workspace_id = ANY(ARRAY['{ws}']::uuid[])))
WHERE sl.tenant_id = '{tenant}' AND sl.scope_kind = 'workspace' AND sl.scope_id = '{ws}'
  AND sl.domain = 'private_memory' AND sl.projection_kind = 'PRIVATE_MEMORY' AND sl.projection_version = 'v1'
  AND sl.stream_seq > 0 AND sl.stream_seq <= 1000000 AND sl.state <> 'TOMBSTONED'
  AND (evidence.visibility_class = 'TENANT_SHARED'
    OR (evidence.visibility_class = 'USER_PRIVATE' AND evidence.visibility_user_id = '{U}')
    OR (evidence.visibility_class = 'WORKSPACE_SHARED' AND evidence.visibility_workspace_id = ANY(ARRAY['{ws}']::uuid[])))
GROUP BY sl.stream_seq, sl.state, ob.evidence_id
ORDER BY sl.stream_seq"""

# ADR-0058 D-C: a distill job takes exactly its own Evidence's outbox row
# (distill_repo::take_outbox_row; the tenant-batch claim is gone).
distill = f"""UPDATE ops.outbox
SET status = 'PROCESSING', lease_owner = 'explain-probe', lease_expires_at = clock_timestamp() + make_interval(secs => 120)
WHERE tenant_id = '{tenant}' AND event_type = 'EVIDENCE_ACCEPTED' AND evidence_id = '00000000-0000-0000-0000-000000000000'
  AND status IN ('PENDING', 'PROCESSING')
RETURNING outbox_id, evidence_id, commit_seq, stream_seq"""

# ADR-0058 D-B: the v2 claim's two ops.jobs reads (0193 ops.claim_derived_work_v2), run as its owner.
tenant_pick = """SELECT t.tenant_id FROM ops.distill_tenant_scheduler t
WHERE EXISTS (SELECT 1 FROM ops.jobs j WHERE j.tenant_id = t.tenant_id AND j.job_type = 'DERIVED_DISTILL'
  AND j.status IN ('PENDING', 'RETRY_WAIT', 'WAITING_KEY') AND j.next_retry_at <= clock_timestamp())
ORDER BY t.last_served_turn, t.tenant_id LIMIT 1 FOR UPDATE OF t SKIP LOCKED"""
job_pick = f"""SELECT j.job_id FROM ops.jobs j
WHERE j.tenant_id = '{tenant}' AND j.job_type = 'DERIVED_DISTILL'
  AND j.status IN ('PENDING', 'RETRY_WAIT', 'WAITING_KEY') AND j.next_retry_at <= clock_timestamp()
ORDER BY j.created_at, j.job_id LIMIT 1 FOR UPDATE SKIP LOCKED"""

proj = ("SELECT count(*) FROM projection.claim_issued_tickets('private_memory','PRIVATE_MEMORY','v1',"
        "'private_memory_v1','explain-probe',1,1,1);\n") if with_proj else ""
script = f"""\\set ON_ERROR_STOP on
LOAD 'auto_explain';
SET auto_explain.log_min_duration = 0;
SET auto_explain.log_nested_statements = on;
SET auto_explain.log_level = notice;
SET auto_explain.log_format = json;
SET auto_explain.log_verbose = on;
SET client_min_messages = notice;
BEGIN;
SELECT '@@claim_derived_work';
SELECT count(*) FROM ops.claim_derived_work(ARRAY['DERIVED_CONSOLIDATE'], 'explain-probe', 1, 1);
SELECT '@@claim_issued_tickets';
{proj}ROLLBACK;
SET auto_explain.log_min_duration = -1;
BEGIN;
SET LOCAL ROLE role_gateway;
SELECT set_config('humaux.tenant_id', '{tenant}', true), set_config('humaux.user_id', '{U}', true);
EXPLAIN (FORMAT JSON, VERBOSE) {overlay};
ROLLBACK;
BEGIN;
SET LOCAL ROLE role_private_worker;
SELECT set_config('humaux.tenant_id', '{tenant}', true), set_config('humaux.user_id', '{U}', true);
EXPLAIN (FORMAT JSON, VERBOSE) {distill};
ROLLBACK;
BEGIN;
SET LOCAL ROLE role_migration_owner;
EXPLAIN (FORMAT JSON, VERBOSE) {tenant_pick};
EXPLAIN (FORMAT JSON, VERBOSE) {job_pick};
ROLLBACK;
"""
p = subprocess.run(["docker", "exec", "-i", "humaux-thread-pg", "psql", "-U", "postgres", "-d", db, "-At"],
                   input=script, capture_output=True, text=True)
if p.returncode != 0:
    print("explain_gate: psql failed:", p.stderr[-800:]); sys.exit(2)

def objs(text, opener):
    dec = json.JSONDecoder(); res = []
    for m in re.finditer(r"(?m)^" + re.escape(opener), text):
        try: res.append(dec.raw_decode(text, m.start())[0])
        except ValueError: pass
    return res

notices = objs(p.stderr, "{")          # auto_explain (nested statements of the two definers)
explains = objs(p.stdout, "[")         # EXPLAIN results: [ {Plan..} ]
plans = {}
for n in notices:
    q = n.get("Query Text", "")
    if "ops.jobs j" in q and "UPDATE ops.jobs" in q: plans["job_claim"] = n["Plan"]
    if "projection.stream_log s" in q and "UPDATE projection.stream_log" in q: plans["projection_claim"] = n["Plan"]
ex = [e[0]["Plan"] for e in explains if isinstance(e, list)]
if len(ex) >= 4: plans["overlay"], plans["distill_claim"], plans["distill_tenant_pick"], plans["distill_job_pick"] = ex[:4]

def walk(pl, acc):
    acc.append(pl)
    for c in pl.get("Plans", []): walk(c, acc)
    return acc

total_hot = 0
for name in ["projection_claim", "job_claim", "overlay", "distill_claim", "distill_tenant_pick", "distill_job_pick"]:
    if name not in plans:
        print(f"plan={name} MISSING"); total_hot += 0 if (name == "projection_claim" and not with_proj) else 1000; continue
    nodes = walk(plans[name], [])
    json.dump(plans[name], open(f"{out}/plan_{name}.json", "w"), indent=1)
    hot = [f"{x.get('Schema')}.{x.get('Relation Name')}" for x in nodes
           if x.get("Node Type") == "Seq Scan" and (x.get("Schema"), x.get("Relation Name")) in HOT]
    kinds = sorted({f"{x['Node Type']}:{x.get('Schema','')}.{x.get('Relation Name','')}" for x in nodes if x.get("Relation Name")})
    print(f"plan={name} nodes={len(nodes)} seq_scan_hot={len(hot)} {hot} scans={kinds}")
    total_hot += len(hot)
print(f"explain_gate: plans={len(plans)} seq_scan_on_outbox_jobs_memory_evidence={total_hot}")
PYEOF
docker exec humaux-thread-pg psql -U postgres -d $DB -qc "ANALYZE ops.outbox; ANALYZE ops.jobs; ANALYZE private.memory_evidence; ANALYZE projection.stream_log"
mkdir -p $PST/plans
python3 $S/explain_gate.py $DB $TENANT $WS $PST/plans 2>&1 | tee -a $EV/rehearsal.log
EXPLAIN_HOT=$(grep -oE 'seq_scan_on_outbox_jobs_memory_evidence=[0-9]+' $EV/rehearsal.log | tail -1 | cut -d= -f2)
EXPLAIN_N=$(grep -oE 'explain_gate: plans=[0-9]+' $EV/rehearsal.log | tail -1 | cut -d= -f2)
assert_eq "no_seq_scan_on_outbox_jobs_memory_evidence(n=${EXPLAIN_N:-0} plans)" "${EXPLAIN_N:-0}|${EXPLAIN_HOT:-x}" "6|0"
# A plan cannot pin an index (at dev data sizes the planner has another path for 4 of the 6 P1-15
# indexes — review 2026-09-29, fault f_delete_p1_15_index), so the catalog does: all seven exist,
# valid and ready. The exact definitions are pinned by crates/adapters/tests/hot_path_indexes.rs.
P1_15_IDX="'stream_log_issued_claim_idx','outbox_tenant_commit_seq_uidx','outbox_tenant_evidence_idx','outbox_evidence_claim_idx','memory_evidence_evidence_idx','jobs_claim_active_idx','memory_records_superseded_by_idx'"
assert_eq "p1_15_indexes_present_valid_ready(n=7 indexes)" \
  "$(PGQ "select count(*) from pg_index i join pg_class c on c.oid=i.indexrelid where c.relname in ($P1_15_IDX) and i.indisvalid and i.indisready")" 7


# ---- 7. card 30b (ADR-0056): identifier-bearing memories and queries; a credential stays refused ----
# Five memories on C/ws1 carry a date, an e-mail address, a phone number, a 12-digit order number and
# a random UUID — each one used to be refused by the contribution-privacy phone/e-mail rules on the
# seal path. Each must project DONE and be returned by a recall whose QUERY carries the same
# identifier (the put text itself). One put on C/ws2 carries the fake GitHub-shaped vector of
# crates/adapters/tests/contribution_scan.rs (never a live key); a recall query carrying it is
# FORBIDDEN, and no projected card may carry it. Live MiniMax decides whether the distilled memory
# keeps the token (measure run 2026-10-01: it wrote "contains the credential" and dropped it), so
# the ticket verdict is graded only when the card does carry it; the deterministic card-side witness
# is projection_worker::a_card_with_a_real_gitleaks_finding_settles_failed_secret_scan_rejected.
: > $PST/ident.tsv; : > $PST/cred.tsv
ID_UUID=$(uuidgen | tr A-Z a-z)
ID_TEXTS=(
  "Card30b date: the billing cutover of team seven is scheduled for 2026-10-15."
  "Card30b email: invoice questions for the shipping team go to billing-desk@example.test."
  "Card30b phone: the payroll on-call line of team seven is +1 (415) 555-0123."
  "Card30b order: purchase order 482913570264 covers the new warehouse scanners."
  "Card30b uuid: incident $ID_UUID was the catalog outage of last spring."
)
for t in $ID_TEXTS; do pst_put BEARER_C $WS_C "$t" $PST/ident.tsv; done
CRED_FAKE=ghp_RkqFzVpLwNyHtBvDgXsWuCePjMoTnAiSyEkl
pst_put BEARER_C2 $WS_C2 "Card30b credential: the release bot uses $CRED_FAKE to publish builds." $PST/cred.tsv
ID_W=$(wait_settled $PST/ident.tsv 600); echo "c30b identifiers: tickets settled after ${ID_W}s" | tee -a $EV/rehearsal.log
CRED_EV=$(cut -f1 $PST/cred.tsv)
CRED_ROW=
i=0; while [ $i -lt 600 ]; do
  CRED_ROW=$(PGQ "select s.state||'|'||coalesce(s.error_class,'-') from ops.outbox o join projection.stream_log s on s.tenant_id=o.tenant_id and s.commit_seq=o.commit_seq where o.event_type='EVIDENCE_ACCEPTED' and o.evidence_id='${CRED_EV:-00000000-0000-0000-0000-000000000000}'")
  case "$CRED_ROW" in FAILED*|SKIPPED_BY_POLICY*|DONE*) break;; esac
  sleep 2; i=$((i+2))
done
ID_STATES=$(PGQ "select count(*) filter (where s.state='DONE')||'/'||count(*)||' secret_scan_rejected='||count(*) filter (where s.error_class='secret_scan_rejected') from ops.outbox o join projection.stream_log s on s.tenant_id=o.tenant_id and s.commit_seq=o.commit_seq where o.event_type='EVIDENCE_ACCEPTED' and o.evidence_id in ($(ev_list $PST/ident.tsv))")
cat > $S/c30b_ident.py <<'PYEOF'
# card 30b: recall each identifier memory with its own put text (which carries the identifier) as
# the query; poll until returned (projection -> Qdrant lag), record any error code.
import json, os, subprocess, sys, time, urllib.request
tsv, db, deadline = sys.argv[1], sys.argv[2], time.time() + float(sys.argv[3])
rows = [l.rstrip("\n").split("\t") for l in open(tsv) if l.strip()]
def pg(sql):
    return subprocess.run(["docker","exec","humaux-thread-pg","psql","-U","postgres","-d",db,"-AtF","\t","-c",sql],
                          capture_output=True, text=True).stdout.splitlines()
def recall(bearer, ws, query):
    body = {"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"recall","arguments":{"query":query,"workspace_id":ws},
            "_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientInfo":{"name":"rehearsal","version":"1"},"io.modelcontextprotocol/clientCapabilities":{}}}}
    gw = os.environ.get("GW_URL", "http://127.0.0.1:8080")  # ADR-0061 D-K: a scratch gateway sets GW_URL
    req = urllib.request.Request(gw + "/mcp", data=json.dumps(body).encode(), method="POST", headers={
        "Content-Type":"application/json","Accept":"application/json, text/event-stream","MCP-Protocol-Version":"2026-07-28",
        "Mcp-Method":"tools/call","Mcp-Name":"recall","Origin":gw,"Authorization":"Bearer "+os.environ[bearer]})
    try:
        d = json.load(urllib.request.urlopen(req, timeout=30))
    except urllib.error.HTTPError as e:
        try: d = json.load(e)
        except Exception: return set(), "HTTP_%d" % e.code
    except Exception as e:
        return set(), type(e).__name__
    if "error" in d: return set(), (d["error"].get("data") or {}).get("code", "PROTOCOL_ERROR")
    sc = d["result"].get("structuredContent", {})
    if d["result"].get("isError"): return set(), sc.get("code", "TOOL_ERROR")
    return {i.get("memory_id") for i in sc.get("items", [])}, None
out = {"n": len(rows), "returned": 0, "forbidden": 0, "codes": [], "identifier_in_memory": 0}
marks = ["2026-10-15", "billing-desk@example.test", "555-0123", "482913570264", None]
for k, (ev, bearer, ws, _t0, text) in enumerate(rows):
    mark = marks[k] if k < len(marks) and marks[k] else text.split("incident ")[-1].split(" ")[0]
    found = [l.split("\t", 1) for l in pg(f"select me.memory_id, m.content::text from private.memory_evidence me join private.memory_records m on m.memory_id=me.memory_id where m.status='active' and me.evidence_id='{ev}'")]
    mems = {m for m, _ in found}
    # informational: the distiller may paraphrase the identifier away; the seal verdict is graded by the ticket
    out["identifier_in_memory"] += any(mark in c for _, c in found)
    code, hit = None, False
    while time.time() < deadline and not hit:
        got, code = recall(bearer, ws, text)
        if code == "FORBIDDEN": break
        hit = bool(mems & got)
        if not hit: time.sleep(2)
    out["returned"] += hit
    out["forbidden"] += code == "FORBIDDEN"
    out["codes"].append(code or "-")
print(json.dumps(out))
PYEOF
ID_RECALL=$(python3 $S/c30b_ident.py $PST/ident.tsv $DB 300 2>>$EV/rehearsal.log)
CRED_IN_CARD=$(PGQ "select count(*) from private.memory_evidence me join private.memory_records m on m.memory_id=me.memory_id where me.evidence_id='${CRED_EV:-00000000-0000-0000-0000-000000000000}' and position('$CRED_FAKE' in m.content::text) > 0")
CRED_LEAKED=$(PGQ "select count(*) from projection.private_memory_points p join private.memory_records m on m.memory_id=p.memory_id and m.tenant_id=p.tenant_id where p.tenant_id='$TENANT_C' and p.retired_at is null and position('$CRED_FAKE' in m.content::text) > 0")
CRED_Q=$(python3 -c "import sys,json; print(json.dumps({'query':'what does the release bot use '+sys.argv[1]+' for?','workspace_id':sys.argv[2]}))" "$CRED_FAKE" "$WS_C2")
CRED_CODE=$(mcp_as "$BEARER_C2" recall "$CRED_Q" | head -1 | python3 -c "import sys,json
try:
    d=json.load(sys.stdin); print((d.get('error',{}).get('data') or {}).get('code') or d.get('result',{}).get('structuredContent',{}).get('code','none'))
except Exception: print('unparsed')")
echo "c30b identifiers: tickets=$ID_STATES recall=$ID_RECALL | credential ticket=$CRED_ROW token_in_distilled_memory=$CRED_IN_CARD live_points_carrying_token=$CRED_LEAKED query_code=$CRED_CODE" | tee -a $EV/rehearsal.log
assert_eq "c30b_identifier_memories_project_done(n=5: date,email,phone,order,uuid)" "$ID_STATES" "5/5 secret_scan_rejected=0"
assert_eq "c30b_identifier_queries_return_their_memory_never_forbidden(n=5)" \
  "$(print -r -- "$ID_RECALL" | python3 -c "import sys,json
try: d=json.load(sys.stdin); print('%d/%d forbidden=%d' % (d['returned'], d['n'], d['forbidden']))
except Exception: print('unparsed')")" "5/5 forbidden=0"
CRED_VERDICT=ok
[ "${CRED_IN_CARD:-0}" -gt 0 ] && case "$CRED_ROW" in 'FAILED|secret_scan_rejected'|SKIPPED_BY_POLICY*) ;; *) CRED_VERDICT="carried-but-$CRED_ROW";; esac
assert_eq "c30b_no_projected_card_carries_the_credential(n=1 put: token_in_card=$CRED_IN_CARD ticket=$CRED_ROW)" "$CRED_VERDICT|$CRED_LEAKED" "ok|0"
assert_eq "c30b_credential_query_forbidden(n=1)" "$CRED_CODE" "FORBIDDEN"
# ADR-0056 D-E: the operator act for a secret_scan_rejected ticket (FAILED is terminal and pins the
# §15.4 prefix); the soak does not use C/ws2, this keeps the family promotable for the next run.
case "$CRED_ROW" in FAILED*) cargo run -q -p xtask -- projection-serve --tenant $TENANT_C --workspace $WS_C2 --domain $DOMAIN --projection-kind $PKIND --version $PVER --retire-failed secret_scan_rejected,distill_failed,no_visible_memory_record 2>&1 | tail -1 | tee -a $EV/rehearsal.log;; esac

# ---- claim latency, one sample per pass ----
CLAIM=$(grep -oE 'projection pass claimed=[0-9]+ .* claim_ms=[0-9]+' $EV/projection-runner.log | python3 -c "
import sys
v=sorted(int(l.rsplit('claim_ms=',1)[1]) for l in sys.stdin)
print('n=%d p50=%sms p95=%sms max=%sms' % (len(v), v[len(v)//2] if v else '-', v[min(len(v)-1,int(0.95*len(v)))] if v else '-', v[-1] if v else '-'))")
echo "pst claim_issued_tickets wall time per pass: $CLAIM" | tee -a $EV/rehearsal.log
# drain the derived work this step created (see the cw start above), bounded
i=0; while [ $i -lt 600 ]; do
  PST_BACKLOG=$(PGQ "select count(*) from ops.jobs where tenant_id in ($SEEDED) and left(job_type,8)='DERIVED_' and status in ('PENDING','RETRY_WAIT','PROCESSING')")
  [ "$PST_BACKLOG" = "0" ] && break; sleep 2; i=$((i+2))
done
echo "pst derived backlog of the seeded tenants after ${i}s: $PST_BACKLOG" | tee -a $EV/rehearsal.log
assert_eq "pst_leaves_no_derived_backlog(n=$(PGQ "select count(*) from ops.jobs where tenant_id in ($SEEDED) and left(job_type,8)='DERIVED_'") jobs)" "$PST_BACKLOG" 0
# ---------- 6b2. metrics_scrape (ADR-0061 D-H / D-K) ----------
# Here, after the traffic and while all eight resident modes run (the resident distiller and the
# consolidation worker live only inside this step and the soak): every ops endpoint is scraped and
# graded against its own binary's `--metrics-families` and §41.2, and the production prometheus.yml's
# scrape path is proven by `up` being EXACTLY the eight OPS_PORTS pairs at 1 — never "every result".
step metrics_scrape
mkdir -p $EV/metrics; MS_ARGS=(); MS_BAD=0; ST_BAD=0
for k in ${(ko)OPS_PORTS}; do
  MS_F=$EV/metrics/${k/:/-}.prom
  curl -fsS -o $MS_F http://127.0.0.1:${OPS_PORTS[$k]}/metrics \
    || { echo "metrics_scrape: $k /metrics did not answer 200 on ${OPS_PORTS[$k]}" | tee -a $EV/rehearsal.log; MS_BAD=$((MS_BAD+1)); }
  # ADR-0062 D-S: `humaux-maintenance --serve` is graded against `--serve --metrics-families` (its own families).
  MS_P=${${k%%:*}#humaux-}; [ "$k" = humaux-maintenance:serve ] && MS_P=maintenance-serve
  MS_ARGS+=(--exposition "$MS_P=$MS_F")
  curl -fsS http://127.0.0.1:${OPS_PORTS[$k]}/status 2>/dev/null > $EV/metrics/${k/:/-}.status.json
  python3 -c "import sys,json; sys.exit(0 if json.load(open(sys.argv[1])).get('process') else 1)" $EV/metrics/${k/:/-}.status.json 2>/dev/null \
    || { echo "metrics_scrape: $k /status is not JSON naming its process" | tee -a $EV/rehearsal.log; ST_BAD=$((ST_BAD+1)); }
done
cargo run -q -p xtask -- metrics-registry "${MS_ARGS[@]}" > $EV/metrics/metrics_registry_exposition.log 2>&1; MR_RC=$?
tail -3 $EV/metrics/metrics_registry_exposition.log | tee -a $EV/rehearsal.log
python3 - "http://127.0.0.1:$PROM_PORT" 30 ${(k)OPS_PORTS} > $EV/metrics/up.txt <<'PYEOF'
# Poll up{job=~"humaux-.*"} every 1 s for at most 2 x scrape_interval until the (job, mode) pairs at 1
# EQUAL the expected set; an empty or short result at the bound is a failure naming the missing pairs.
import json, sys, time, urllib.parse, urllib.request
base, bound, want = sys.argv[1], float(sys.argv[2]), {tuple(k.split(":", 1)) for k in sys.argv[3:]}
url = base + "/api/v1/query?" + urllib.parse.urlencode({"query": 'up{job=~"humaux-.*"}'})
t0, got, zero = time.time(), set(), set()
while True:
    try:
        res = json.load(urllib.request.urlopen(url, timeout=5))["data"]["result"]
    except Exception:
        res = []
    pairs = [((s["metric"].get("job"), s["metric"].get("mode")), s["value"][1]) for s in res]
    got = {p for p, v in pairs if v == "1"}
    zero = {p for p, v in pairs if v != "1"}
    if got == want or time.time() - t0 >= bound:
        break
    time.sleep(1)
print("up n=%d after %.0fs: %s" % (len(got), time.time() - t0, " ".join("%s/%s" % p for p in sorted(got, key=str))))
if got != want:
    print("up mismatch: missing %s; at 0 %s; extra %s" % (sorted(want - got, key=str), sorted(zero, key=str), sorted(got - want, key=str)))
sys.exit(0 if got == want else 1)
PYEOF
UP_RC=$?
tee -a $EV/rehearsal.log < $EV/metrics/up.txt
DG_SUM=$(cat $EV/metrics/*.prom | awk '/^degrade_total\{/ {s += $NF} END {printf "%d", s}')
echo "metrics_scrape: degrade_total summed over the eight scrapes = $DG_SUM" | tee -a $EV/rehearsal.log
# card 34b (§41.2, §42 no-output stage): this step's resident distiller distilled real Evidence, so its three
# private-plane counters are above 0. The §42 injection (a parser stub ⇒ runs up, outputs flat) is proven by the
# promtool test of DistillNoOutput, not here: a stub parser in the deployed binary is a code change (ADR-0061).
for PW_F in private_distill_runs_total private_distill_outputs_total private_reasoning_usage_total; do
  PW_V=$(awk -v f=$PW_F '$1 == f {printf "%d", $2}' $EV/metrics/humaux-private-worker-distill-serve.prom 2>/dev/null)
  echo "metrics_scrape: humaux-private-worker distill-serve $PW_F = ${PW_V:-absent}" | tee -a $EV/rehearsal.log
  assert_gt "metrics_scrape_private_worker_${PW_F}_after_distill" "${PW_V:-absent}" 0
done
assert_eq "metrics_scrape_every_ops_endpoint_answers(n=${#OPS_PORTS})" "$MS_BAD" 0
assert_eq "metrics_scrape_status_is_json_naming_its_process(n=${#OPS_PORTS})" "$ST_BAD" 0
assert_eq "metrics_scrape_matches_metrics_families_and_41_2(n=${#OPS_PORTS} scrapes)" "$MR_RC" 0
assert_eq "prometheus_up_is_exactly_the_eight_ops_pairs" "$UP_RC" 0
assert_eq "watchdog_receipt_carries_the_deployed_git_sha(sha=${DEPLOY_SHA:-none})" "$WD_OK" 1

# ---------- 6b3. admin_probes: the §4.4 catalog over role_admin (ADR-0061 D-J) ----------
# 8 Readings with the five-key envelope and 3 refusals that name their missing object (ruling E2).
# cell.resources probes the same two sockets the gateway (retrieval.sock) and the private worker (inference.sock) are
# given, so its value is 3 of 3 here (ruling B1).
step admin_probes
mkdir -p $EV/admin_probes
openssl req -x509 -newkey rsa:2048 -nodes -keyout $S/tls_probe.key -out $S/tls_probe.pem -days 30 -subj /CN=humaux-rehearsal >/dev/null 2>&1
AP_ADDRS=; for k in ${(ko)OPS_PORTS}; do AP_ADDRS+="${AP_ADDRS:+,}${k/:/-}=127.0.0.1:${OPS_PORTS[$k]}"; done
admin_q() { ( export HUMAUX_ADMIN_PG_DSN="postgres://role_admin:${HUMAUX_ROLE_PASSWORD_ADMIN:?}@$PG/$DB" \
    HUMAUX_ADMIN_OPS_ADDRS=$AP_ADDRS HUMAUX_ADMIN_TLS_CERT_PATHS=$S/tls_probe.pem \
    HUMAUX_CELL_ID=$CELL_ID HUMAUX_CELL_CALLER_ID=admin HUMAUX_QDRANT_HOST=127.0.0.1 HUMAUX_QDRANT_PORT=6333 \
    HUMAUX_QDRANT_CIDR=127.0.0.1/32 HUMAUX_QDRANT_TLS=false \
    HUMAUX_ADMIN_RETRIEVAL_RPC_SOCKET_PATH=$SOCK/retrieval.sock HUMAUX_ADMIN_PRIVATE_INFERENCE_RPC_SOCKET_PATH=$SOCK/inference.sock
  exec "$BIN_DIR"/humaux-admin q $1 ) > $EV/admin_probes/$1.json 2> $EV/admin_probes/$1.err; }
apv() { python3 -c "import sys,json
try: d=json.load(open(sys.argv[1]))
except Exception: print(''); raise SystemExit
print(d.get(sys.argv[2], ''))" $EV/admin_probes/$1.json $2; }
AP_READ=0
for p in stream.watermark outbox.backlog jobs.stuck degrade.counters flags.effective deploy.binary tls.expiry cell.resources; do
  admin_q $p; AP_RC=$?
  AP_ENV=$(python3 -c "import sys,json
try: d=json.load(open(sys.argv[1]))
except Exception: print(0); raise SystemExit
print(int({'value','scanned_n','scope_hash','checked_at','probe_version'} <= set(d) and isinstance(d['scanned_n'],int) and d['scanned_n'] > 0))" $EV/admin_probes/$p.json)
  echo "admin_probes: $p rc=$AP_RC envelope=$AP_ENV value=$(apv $p value) scanned_n=$(apv $p scanned_n) version=$(apv $p probe_version)$([ $AP_RC -ne 0 ] && echo " err=$(head -c 300 $EV/admin_probes/$p.err | tr '\n' ' ')")" | tee -a $EV/rehearsal.log
  [ $AP_RC -eq 0 ] && [ "$AP_ENV" = 1 ] && AP_READ=$((AP_READ+1))
done
AP_REFUSED=0
for p in public.corroborated:public.claims.corroboration public.consensus_ready:public.claims.contributor_set parse.poison:limit_hit; do
  admin_q ${p%%:*}; AP_RC=$?
  AP_NAMED=$(cat $EV/admin_probes/${p%%:*}.json $EV/admin_probes/${p%%:*}.err | grep -cF -- "${p#*:}")
  echo "admin_probes: ${p%%:*} rc=$AP_RC names ${p#*:}: $AP_NAMED" | tee -a $EV/rehearsal.log
  [ $AP_RC -ne 0 ] && [ "$AP_NAMED" -gt 0 ] && AP_REFUSED=$((AP_REFUSED+1))
done
assert_eq "admin_probes_eight_readings_with_the_five_key_envelope" "$AP_READ" 8
assert_eq "admin_probes_three_refusals_name_their_missing_object" "$AP_REFUSED" 3
assert_eq "admin_jobs_stuck_is_a_count(value=$(apv jobs.stuck value))" "$(apv jobs.stuck value | grep -cE '^[0-9]+$')" 1
assert_eq "admin_degrade_counters_equals_the_scraped_degrade_total_sum" "$(apv degrade.counters value)" "$DG_SUM"

own_signal $S/ds.pid humaux-private-worker TERM 90 | tee -a $EV/rehearsal.log
own_signal $S/cw.pid humaux-consolidation-worker TERM 90 | tee -a $EV/rehearsal.log
# ADR-0060 (card 33b, T32): one deployed worker served two providers, each tenant on its own route.
# Counted from the SUCCEEDED private-reasoning ledger rows the worker wrote for the seeded tenants.
PRIV_PURPOSES="'PRIVATE_DISTILL_TEXT','PRIVATE_CONSOLIDATE'"
C33B_PROV=$(PGQ "select count(distinct provider||'/'||model) from ops.model_call_ledger where tenant_id in ($SEEDED) and purpose in ($PRIV_PURPOSES) and status='SUCCEEDED'")
C33B_C_OFF=$(PGQ "select count(*) from ops.model_call_ledger where tenant_id='$TENANT_C' and purpose in ($PRIV_PURPOSES) and status='SUCCEEDED' and provider<>'$HUMAUX_LIVE_P2_PROVIDER_ID'")
C33B_AB_OFF=$(PGQ "select count(*) from ops.model_call_ledger where tenant_id in ('$TENANT','$TENANT_B') and purpose in ($PRIV_PURPOSES) and status='SUCCEEDED' and provider<>'$MM_PROVIDER'")
C33B_N=$(PGQ "select count(*) from ops.model_call_ledger where tenant_id in ($SEEDED) and purpose in ($PRIV_PURPOSES) and status='SUCCEEDED'")
assert_eq "c33b_providers_distinct(n=$C33B_N succeeded rows)" "$C33B_PROV" 2
assert_eq "c33b_tenant_c_only_on_its_route" "$C33B_C_OFF" 0
assert_eq "c33b_tenants_a_b_only_on_their_route" "$C33B_AB_OFF" 0
echo "ASSERTIONS (incl. projection_serve_multi_tenant) $A_OK passed, $A_BAD failed" | tee -a $EV/rehearsal.log

# ---------- 6c. soak (card 16 / ADR-0038) — only when SOAK_SECS is set ----------
# Two tenants (e2e-seed provisions ONE per invocation, so it runs twice with the SAME pepper —
# one gateway serves N (tenant, workspace) pairs per request since cards 10/11/13), concurrent
# sessions on both, a chaos hook that kill -9s and restarts each resident worker in turn — the
# tenant-free projection runner (`--serve`, ADR-0052) included. There is no projection loop any
# more: the runner that serves the whole rehearsal serves the soak.
if [ "${SOAK_SECS:-0}" -gt 0 ]; then
step soak
# --processor-id is the recipient the deployment's HUMAUX_PRIVATE_WORKER_EGRESS_RECIPIENTS lists
# (ADR-0060 D-C / D-L, exported by the first seed with its host). Minting a fresh uuid here gave
# tenant B a route no running worker could admit, so its distill deferred every row forever,
# silently (card 16 P0). Both tenants: $EGRESS_PROC.
# Tenant B is provisioned in step seed_b on the main path now — the soak reuses that pair
# instead of minting a third tenant nothing else in this run ever asserts on.
echo "soak tenants: A=$TENANT/$WS  B=$TENANT_B/$WS_B" | tee -a $EV/rehearsal.log

# Self-contained hook scripts: the chaos/probe commands are run by `sh -c` out of xtask, so they
# cannot see this shell's functions. No API key is written into any of them — the retrieval
# worker's restart sources $R/.env.local exactly as step 2 does.
RW_ENV="export HUMAUX_RETRIEVAL_WORKER_PG_DSN=\"postgres://role_retrieval_worker:\${HUMAUX_ROLE_PASSWORD_RETRIEVAL_WORKER:?}@$PG/$DB\" \
HUMAUX_RETRIEVAL_WORKER_QDRANT_HOST=127.0.0.1 HUMAUX_RETRIEVAL_WORKER_QDRANT_PORT=6333 \
HUMAUX_RETRIEVAL_WORKER_QDRANT_CIDR=127.0.0.1/32 HUMAUX_RETRIEVAL_WORKER_QDRANT_TLS=false \
HUMAUX_RETRIEVAL_WORKER_CELL_ID=$CELL_ID \
HUMAUX_RETRIEVAL_WORKER_CALLER=retrieval-worker HUMAUX_RETRIEVAL_WORKER_RPC_SOCKET_PATH=$SOCK/retrieval.sock \
HUMAUX_RETRIEVAL_WORKER_GATEWAY_UID=$MYUID HUMAUX_RETRIEVAL_WORKER_EMBEDDING_PROVIDER=dashscope \
HUMAUX_RETRIEVAL_WORKER_EMBEDDING_MODEL=$EMB_MODEL HUMAUX_RETRIEVAL_WORKER_MODEL_REVISION=$EMB_REV \
HUMAUX_RETRIEVAL_WORKER_DIMENSION=$EMB_DIM HUMAUX_RETRIEVAL_WORKER_EMBEDDING_VERSION=$EMB_VER \
HUMAUX_RETRIEVAL_WORKER_REGION=$EMB_REGION HUMAUX_RETRIEVAL_WORKER_MAX_INPUT_TOKENS=$EMB_MAX_TOK \
HUMAUX_RETRIEVAL_WORKER_GITLEAKS_BIN=$GITLEAKS_BIN \
HUMAUX_RETRIEVAL_WORKER_GITLEAKS_SHA256=$GITLEAKS_SHA HUMAUX_RETRIEVAL_WORKER_GITLEAKS_VERSION=$GITLEAKS_VER \
$OPS_RW_RPC $OPS_RW_SERVE"

# One definition per worker, used by BOTH the resident spawn below and its chaos restart, so a
# restarted process is the same process — a chaos hook that starts a differently-configured
# worker grades a deployment nobody ran.
DS_ENV="export PRIVATE_WORKER_PG_DSN=\"postgres://role_private_worker:\${HUMAUX_ROLE_PASSWORD_PRIVATE_WORKER:?}@$PG/$DB\" \
HUMAUX_PRIVATE_WORKER_CREDENTIALS='$HUMAUX_PRIVATE_WORKER_CREDENTIALS' HUMAUX_PRIVATE_WORKER_HEALTH_RENEW_SECS=$PW_HEALTH_RENEW_SECS HUMAUX_PRIVATE_WORKER_HTTP_TIMEOUT_SECS=120 HUMAUX_PRIVATE_WORKER_PERMIT_TTL_SECS=60 \
HUMAUX_PRIVATE_WORKER_DNS_PINS='$PW_DNS_PINS' HUMAUX_PRIVATE_WORKER_RPC_SOCKET_PATH=$SOCK/inference-soak.sock \
HUMAUX_PRIVATE_WORKER_CONSOLIDATION_UID=$MYUID HUMAUX_PRIVATE_WORKER_CANDIDATE_TTL_SECONDS=86400 \
HUMAUX_PRIVATE_WORKER_DISTILL_LEASE_SECS=30 HUMAUX_PRIVATE_WORKER_DISTILL_IN_FLIGHT=4 \
HUMAUX_PRIVATE_WORKER_DISTILL_HARD_DEADLINE_SECS=300 HUMAUX_PRIVATE_WORKER_DISTILL_NOT_READY_PARK_SECS=600 \
HUMAUX_PRIVATE_WORKER_DISTILL_MAX_ATTEMPTS=5 HUMAUX_PRIVATE_WORKER_DISTILL_BUDGET_WINDOW_SECS=60 HUMAUX_PRIVATE_WORKER_DISTILL_BUDGET_MAX_CALLS=120 \
HUMAUX_PRIVATE_WORKER_DISTILL_POLL_INTERVAL_SECS=3 $OPS_PW_DISTILL"
CW_ENV="export CONSOLIDATION_WORKER_PG_DSN=\"postgres://role_consolidation_worker:\${HUMAUX_ROLE_PASSWORD_CONSOLIDATION_WORKER:?}@$PG/$DB\" \
HUMAUX_CONSOLIDATION_WORKER_RPC_SOCKET_PATH=$SOCK/inference.sock HUMAUX_CONSOLIDATION_WORKER_CALL_TTL_SECS=120 \
HUMAUX_CONSOLIDATION_WORKER_DIAL_TIMEOUT_SECS=10 HUMAUX_CONSOLIDATION_WORKER_MAX_INPUTS=50 \
HUMAUX_CONSOLIDATION_WORKER_LEASE_SECS=120 HUMAUX_CONSOLIDATION_WORKER_BATCH=8 \
HUMAUX_CONSOLIDATION_WORKER_MAX_ATTEMPTS=5 HUMAUX_CONSOLIDATION_WORKER_POLL_INTERVAL_SECS=3 $OPS_CW_SERVE"

cat > $S/soak_probe_rw.sh <<EOF
#!/bin/sh
cd $R || exit 1
$RW_ENV
exec "$BIN_DIR"/humaux-retrieval-worker --readyz
EOF
cat > $S/soak_probe_pw.sh <<EOF
#!/bin/sh
cd $R || exit 1
PRIVATE_WORKER_PG_DSN="postgres://role_private_worker:\${HUMAUX_ROLE_PASSWORD_PRIVATE_WORKER:?}@$PG/$DB"
export PRIVATE_WORKER_PG_DSN
exec "$BIN_DIR"/humaux-private-worker --readyz
EOF
# kill -9 each resident worker in turn and bring it back: the exact case the leases exist to
# survive. SIGTERM would drain, which is the case that is already safe by construction.
#
# Each hook kills ONLY the PID this script recorded at spawn, via own_signal, which refuses any
# PID whose `ps -o comm=` basename is not the expected binary. The previous version used
# `pkill -9 -f 'humaux-retrieval-worker --serve-rpc'` — a pattern match against every process on
# the host, the same class of action as the port-kill that killed the user's WeChat on
# 2026-09-09 — and backgrounded the restart without capturing `$!`, so the process it created
# could only ever be reaped by another pattern-kill. Every restart below writes the new PID back
# into the pidfile, so the next chaos round and the teardown both reach the live process.
#
# The gateway is deliberately NOT in the rotation: its `/readyz` is the only probe bound to a
# live process (the worker probes are one-shot binaries checking PG/Qdrant), so killing it would
# make `probes_green` grade the harness's own outage window instead of the deployment. The
# private worker's `--serve-rpc` listener is out for a different reason: it holds no `ops.jobs`
# lease, and it is the UDS server the consolidation worker dials, so a restart races §11.8's
# bind rather than the lease path this chaos step exists to exercise. Both are named here rather
# than silently omitted.
cat > $S/soak_chaos_rw.sh <<EOF
#!/bin/sh
cd $R || exit 1
. $S/own_signal.sh
own_signal $S/rw.pid humaux-retrieval-worker 9 || exit 1
(
$RW_ENV
set -a; . $R/.env.local; set +a
exec "$BIN_DIR"/humaux-retrieval-worker --serve-rpc >> $EV/retrieval-worker.log 2>&1
) &
echo \$! > $S/rw.pid
sleep 3
exit 0
EOF
# The distill worker holds the DERIVED_DISTILL ops.jobs lease — kill -9 mid-pass is the case
# `no_live_lease_after_drain` exists to grade, and it had never been exercised.
cat > $S/soak_chaos_ds.sh <<EOF
#!/bin/sh
cd $R || exit 1
. $S/own_signal.sh
own_signal $S/ds.pid humaux-private-worker 9 || exit 1
(
$DS_ENV
set -a; . /Volumes/data/viral-skill-eval/.env; set +a
exec "$BIN_DIR"/humaux-private-worker --distill-serve >> $EV/soak-distill.log 2>&1
) &
echo \$! > $S/ds.pid
sleep 3
exit 0
EOF
# …and the consolidation worker holds the DERIVED_CONSOLIDATION lease.
cat > $S/soak_chaos_cw.sh <<EOF
#!/bin/sh
cd $R || exit 1
. $S/own_signal.sh
own_signal $S/cw.pid humaux-consolidation-worker 9 || exit 1
(
$CW_ENV
exec "$BIN_DIR"/humaux-consolidation-worker --serve >> $EV/soak-consolidation.log 2>&1
) &
echo \$! > $S/cw.pid
sleep 3
exit 0
EOF
# …and the projection runner (ADR-0052) holds projection.stream_log ticket leases — kill -9
# mid-batch is what `no_live_ticket_lease_after_drain` grades. Same $RW_ENV + $RP_PASS_ENV as
# start_rp: the restarted runner is the runner the rehearsal ran.
cat > $S/soak_chaos_rp.sh <<EOF
#!/bin/sh
cd $R || exit 1
. $S/own_signal.sh
own_signal $S/rp.pid humaux-retrieval-worker 9 || exit 1
(
$RW_ENV
export $RP_PASS_ENV
set -a; . $R/.env.local; set +a
exec "$BIN_DIR"/humaux-retrieval-worker --serve >> $EV/projection-runner.log 2>&1
) &
echo \$! > $S/rp.pid
sleep 3
exit 0
EOF
# …and the resident maintenance daemon (ADR-0062 D-T): kill -9 while its doors purge the throwaway database; the
# receipts balance in step maintenance_drain proves no purge half-applied. Same $MD_ENV as start_md.
cat > $S/soak_chaos_md.sh <<EOF
#!/bin/sh
cd $R || exit 1
. $S/own_signal.sh
own_signal $S/md.pid humaux-maintenance 9 || exit 1
(
$MD_ENV
exec "$BIN_DIR"/humaux-maintenance --serve >> $EV/maintenance-serve.log 2>&1
) &
echo \$! > $S/md.pid
sleep 3
exit 0
EOF
chmod +x $S/soak_probe_rw.sh $S/soak_probe_pw.sh \
         $S/soak_chaos_rw.sh $S/soak_chaos_ds.sh $S/soak_chaos_cw.sh $S/soak_chaos_rp.sh $S/soak_chaos_md.sh

# macOS XProtect assesses each freshly linked binary on FIRST exec (~98 s, strictly serial).
# Warm every binary the soak launches or probes BEFORE the timed window; never widen a
# production timeout to absorb this (docs/ops/soak.md §5).
# Card 30 / ADR-0060 ruling E3 (a): a soak sized for n >= 300 recalls (docs/ops/soak.md "Sizing a
# latency measurement") can outlive the onboarding attestation; every distill after that would park
# ROUTE_HEALTH_STALE and the settlement verdicts would grade the attestation's clock, not the soak.
# The operator re-attests each onboarded Profile@version once, before the timed window, for the
# soak's own span — through the same door, never an owner INSERT (append-only, nothing rewritten).
HEALTH_SECS=$(( SOAK_SECS + ${SOAK_DRAIN:-150} + 900 ))
for rp in "${ROUTE_PROFILES[@]}"; do set -- ${=rp}
  "${MAINT[@]}" reasoning attest-health --tenant $1 --profile $2 --profile-version $3 --valid-for-secs $HEALTH_SECS "${RADMIN[@]}" >> $EV/onboard.log 2>>$EV/onboard.stderr \
    || echo "soak: attest-health refused for tenant $1 profile $2@$3" | tee -a $EV/rehearsal.log
done
echo "soak health re-observation: valid for ${HEALTH_SECS}s" | tee -a $EV/rehearsal.log

step soak_warmup
W0=$(date +%s)
for b in humaux-gateway humaux-retrieval-worker humaux-private-worker humaux-consolidation-worker xtask; do
  env -i "$BIN_DIR"/$b --help >/dev/null 2>&1
done
echo "soak warm-up: $(( $(date +%s) - W0 ))s for 5 binaries (XProtect first-exec assessment)" | tee -a $EV/rehearsal.log

# ADR-0058 R11 — the harness is the operator during the soak. A DEAD distill (job line
# `outcome=DEAD`) settles its Evidence's ticket FAILED `distill_failed`, which pins that stream's
# §15.4 prefix for the rest of the run; the runbook's answer to a DEAD job is the audited
# `--retire-failed distill_failed` (re-driving it with `jobs requeue-dead` needs the cause fixed
# first, which a soak cannot do). Chain run 2 (2026-10-03): tenant B's ws stream was pinned at seq 20
# by a DEAD from the pst step BEFORE the soak (no retirement ran after it), so `projection_promoted`
# saw its prefix never move. So: retire once before the timed window, then every
# SOAK_RETIRE_SECS while the soak runs. Only `distill_failed` — any other FAILED class is a
# projection-path failure and stays for `projection_promoted` to grade; the distill death itself is
# graded by `distill_dead`.
soak_retire_distill_failed() {
  local fam
  for fam in "$TENANT $WS" "$TENANT_B $WS_B" "$TENANT_C $WS_C"; do set -- ${=fam}
    "$BIN_DIR"/xtask projection-serve --tenant $1 --workspace $2 --domain $DOMAIN --projection-kind $PKIND --version $PVER --retire-failed distill_failed 2>&1 \
      | grep -E '^projection-serve: (retired|retire-failed)' | sed "s|^|soak operator $1/$2: |" >> $EV/soak-operator.log
  done
}
: > $EV/soak-operator.log
soak_retire_distill_failed
rm -f $S/soak_retire.stop
( while [ ! -f $S/soak_retire.stop ]; do
    i=0; while [ $i -lt ${SOAK_RETIRE_SECS:-60} ] && [ ! -f $S/soak_retire.stop ]; do sleep 1; i=$((i+1)); done
    [ -f $S/soak_retire.stop ] || soak_retire_distill_failed
  done ) &
SOAK_RETIRE_PID=$!
echo "soak operator: pre-soak retirement $(grep -c ': projection-serve: retired' $EV/soak-operator.log) retired line(s); every ${SOAK_RETIRE_SECS:-60}s during the soak (log soak-operator.log)" | tee -a $EV/rehearsal.log

# resident derived workers for the soak window (both tenant-free, ADR-0036).
# Same $DS_ENV / $CW_ENV the chaos restarts use — one definition, no drift.
( eval "$DS_ENV"
  set -a; source /Volumes/data/viral-skill-eval/.env; set +a
  exec "$BIN_DIR"/humaux-private-worker --distill-serve > $EV/soak-distill.log 2>&1 ) &
SOAK_DS_PID=$!; own_pid ds $SOAK_DS_PID
( eval "$CW_ENV"
  exec "$BIN_DIR"/humaux-consolidation-worker --serve > $EV/soak-consolidation.log 2>&1 ) &
SOAK_CW_PID=$!; own_pid cw $SOAK_CW_PID

# The runner's chaos hook runs FIRST: a SOAK_SECS=120 run with a 90 s chaos period fires exactly one
# hook, and the card-27 gate is the runner's kill -9 (the retrieval worker's restart window fails a
# handful of recalls, which op_failure_rate counts at 1%; longer soaks rotate through all four).
# ADR-0061 D-F / E15: the gateway is graded on /livez. Its /readyz is now dependency-truthful, so
# the chaos kill of the retrieval worker it depends on correctly makes it 503 for that window; the
# old /readyz was only the accepting flag, which /livez carries at the same strength.
cargo run -q -p xtask -- soak \
  --gateway-url http://127.0.0.1:8080/mcp \
  --tenant "$TENANT:$WS:BEARER_A" --tenant "$TENANT_B:$WS_B:BEARER_B" --tenant "$TENANT_C:$WS_C:BEARER_C" \
  --sessions-per-tenant ${SOAK_SESSIONS:-2} \
  --duration-secs $SOAK_SECS --drain-secs ${SOAK_DRAIN:-150} --think-ms ${SOAK_THINK_MS:-500} \
  --probe-every-secs 15 \
  --probe-cmd "curl -fsS -o /dev/null http://127.0.0.1:8080/livez" \
  --probe-cmd "$S/soak_probe_rw.sh" \
  --probe-cmd "$S/soak_probe_pw.sh" \
  --watch-pidfile gw=$S/gw.pid --watch-pidfile rw=$S/rw.pid --watch-pidfile pw=$S/pw.pid \
  --watch-pidfile ds=$S/ds.pid --watch-pidfile cw=$S/cw.pid --watch-pidfile rp=$S/rp.pid --watch-pidfile md=$S/md.pid \
  --chaos-every-secs ${SOAK_CHAOS_SECS:-90} --chaos-grace-secs ${SOAK_CHAOS_GRACE:-60} \
  --chaos-cmd "$S/soak_chaos_rp.sh" --chaos-cmd "$S/soak_chaos_md.sh" --chaos-cmd "$S/soak_chaos_rw.sh" --chaos-cmd "$S/soak_chaos_ds.sh" --chaos-cmd "$S/soak_chaos_cw.sh" \
  --lease-secs 120 --max-rss-mib ${SOAK_MAX_RSS_MIB:-2048} --max-db-connections ${SOAK_MAX_CONNS:-120} \
  --max-op-failure-rate ${SOAK_MAX_OP_FAIL:-0.01} \
  --report $EV/soak-report.json 2>&1 | tee -a $EV/rehearsal.log
SOAK_RC=${pipestatus[1]}   # zsh: the tee at the end of the pipe is NOT the verdict
touch $S/soak_retire.stop; wait $SOAK_RETIRE_PID 2>/dev/null
echo "soak operator: $(grep -c ': projection-serve: retired' $EV/soak-operator.log) retired line(s) in total: $(grep ': projection-serve: retired' $EV/soak-operator.log | tr '\n' ';')" | tee -a $EV/rehearsal.log
# …the two resident workers go through the pidfiles: a chaos step may have restarted them, so
# $SOAK_DS_PID / $SOAK_CW_PID can be stale, and a stale PID is exactly what must never be killed.
own_signal $S/ds.pid humaux-private-worker TERM 30
own_signal $S/cw.pid humaux-consolidation-worker TERM 30
echo "soak exit=$SOAK_RC report=$EV/soak-report.json" | tee -a $EV/rehearsal.log
# (5) the soak report meets its stated thresholds, with n and units — asserted here rather than
# read by hand out of the JSON afterwards, and folded into the SAME verdict as everything above.
assert_eq "soak_exit_code" "$SOAK_RC" 0
SOAK_SUMMARY=$(python3 -c "
import json,sys
try: d=json.load(open(sys.argv[1]))
except Exception: print('unreadable'); raise SystemExit
lat=d.get('latency',[])
print(' | '.join('%s n=%s p50=%sms p95=%sms failed=%s' % (
  o.get('operation'), o.get('n'), o.get('p50'), o.get('p95'), o.get('failed_calls'))
  for o in lat) or 'no latency rows')
" $EV/soak-report.json)
echo "soak thresholds: $SOAK_SUMMARY" | tee -a $EV/rehearsal.log
# card 30 (ADR-0055 D-E): `provenance.stage_ms` partitions recall's search(); its per-recall sum
# must carry >= 90% of the end-to-end recall p50 the soak measured over the wire, or the gap sits
# outside search() (guard/HTTP) and the stage table cannot attribute it.
STAGE_COVER=$(python3 -c "
import json,sys
try: d=json.load(open(sys.argv[1]))
except Exception: print('0 unreadable'); raise SystemExit
lat={o.get('operation'): o for o in d.get('latency',[])}
r=lat.get('recall',{}); s=lat.get('recall.stage_sum',{})
ok=bool(r.get('p50')) and bool(s.get('n')) and s.get('p50') is not None and s['p50'] >= 0.9*r['p50']
print('%d recall_p50=%sms(n=%s) stage_sum_p50=%sms(n=%s)' % (ok, r.get('p50'), r.get('n'), s.get('p50'), s.get('n')))
print(' | '.join('%s p50=%s p95=%s' % (k, o.get('p50'), o.get('p95')) for k,o in sorted(lat.items()) if k.startswith('recall.stage')), file=sys.stderr)
" $EV/soak-report.json 2>>$EV/rehearsal.log)
echo "recall stage coverage: $STAGE_COVER" | tee -a $EV/rehearsal.log
assert_eq "recall_stage_sum_covers_90pct_of_recall_p50" "${STAGE_COVER%% *}" 1
# card 30b (ADR-0056 D-C): the gateway no longer seals, so its `scan` lap is trusted_query() only.
SCAN_P50=$(python3 -c "
import json,sys
try: print({o.get('operation'): o for o in json.load(open(sys.argv[1])).get('latency',[])}.get('recall.stage.scan',{}).get('p50','none'))
except Exception: print('unreadable')" $EV/soak-report.json)
assert_eq "recall_stage_scan_p50_below_5ms(p50=${SCAN_P50}ms)" "$(python3 -c "import sys; print(1 if float(sys.argv[1]) < 5 else 0)" "$SCAN_P50" 2>/dev/null || echo 0)" 1
assert_eq "soak_report_carries_latency_rows_with_n" \
  "$(python3 -c "
import json,sys
try: d=json.load(open(sys.argv[1]))
except Exception: print(1); raise SystemExit
lat=d.get('latency',[])
print(0 if lat and all(o.get('n') for o in lat) else 1)" $EV/soak-report.json)" 0
SOAK_RAN=1
fi
# A rehearsal that skipped the soak must SAY so in its own verdict; silence is how "no soak ran
# in this pass" ended up only in the report's §8.2 instead of in the evidence file.
[ "${SOAK_RAN:-0}" = "1" ] || echo "NOTE: SOAK_SECS unset — acceptance item (5) NOT witnessed by this run" | tee -a $EV/rehearsal.log

# ---------- 6c2. maintenance_drain: the resident daemon's work on its throwaway database (ADR-0062 D-T, E8) ----------
# After every kill -9 (the explicit one in step readyz, and the soak's rotation when it ran): the seeded work is
# gone and, per purge door, seeded − remaining = Σ ops.maintenance_receipts.affected (snapshots counted in
# snapshots) — a multi-statement or receipt-less purge interrupted by kill -9 breaks the equality; every seeded
# orphan is LOST with exactly one reissue; the restarted daemon finished a cycle and answers 200.
step maintenance_drain
MD_D0=$(date +%s); MD_LEFT=-1
while [ $(( $(date +%s) - MD_D0 )) -le $(( MD_K * 8 / 2 + 120 )) ]; do
  MD_LEFT=$(MDQ "$MD_LEFT_SQL"); [ "$MD_LEFT" = 0 ] && break; sleep 3
done
MD_REMAINING=$(MDQ "select (select count(*) from control.confirm_tokens where operation='c35.rh')||' '||(select count(*) from ops.selection_snapshots where query_fingerprint='c35-rh')||' '||(select count(*) from control.rate_buckets where subject_id like 'c35-rh-%')||' '||(select count(*) from ops.jobs where job_type='c35.rehearsal')")
MD_RECEIPTS=$(MDQ "select coalesce(sum(affected) filter (where task='confirm_tokens'),0)||' '||coalesce(sum(affected) filter (where task='selection_snapshots'),0)||' '||coalesce(sum(affected) filter (where task='rate_buckets'),0)||' '||coalesce(sum(affected) filter (where task='terminal_jobs'),0) from ops.maintenance_receipts")
MD_BAL_BAD=0; MD_BAL_LINE=""; MD_TASKS=(confirm_tokens selection_snapshots rate_buckets terminal_jobs)
MD_SA=(${=MD_SEEDED}); MD_RA=(${=MD_REMAINING}); MD_AA=(${=MD_RECEIPTS})
for MD_I in 1 2 3 4; do
  MD_T=${MD_TASKS[$MD_I]}; MD_S=${MD_SA[$MD_I]:-}; MD_R=${MD_RA[$MD_I]:-}; MD_A=${MD_AA[$MD_I]:-}
  MD_OK=0; [ -n "$MD_S" ] && [ -n "$MD_R" ] && [ -n "$MD_A" ] && [ $(( MD_S - MD_R )) -eq "$MD_A" ] 2>/dev/null && [ "$MD_S" -gt 0 ] && MD_OK=1
  [ $MD_OK = 1 ] || MD_BAL_BAD=$((MD_BAL_BAD + 1))
  echo "maintenance receipts balance $MD_T: seeded=$MD_S remaining=$MD_R receipts_affected=$MD_A ok=$MD_OK" | tee -a $EV/rehearsal.log
  MD_BAL_LINE+="${MD_BAL_LINE:+,}$MD_T=$MD_S-$MD_R/$MD_A"
done
MD_ORPHANS=$(MDQ "select count(*) from projection.stream_log s where s.stream_seq = 1 and s.commit_seq between 1 and 8 and s.state = 'LOST' and (select count(*) from projection.ticket_reissues r where r.tenant_id = s.tenant_id and r.source_commit_seq = s.commit_seq) = 1")
MD_STATUS=$(curl -fsS http://127.0.0.1:${OPS_PORTS[humaux-maintenance:serve]}/status 2>/dev/null | python3 -c "
import sys,json
try: d=json.load(sys.stdin); print('%d %s %s' % (d.get('cycles',0), d['readiness']['state'], ','.join('%s:%s/%s' % (t['task'], t['affected'], t['failed']) for t in d.get('last_cycle') or [])))
except Exception as e: print('0 unparsed(%s) -' % type(e).__name__)")
MD_CODE=$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:${OPS_PORTS[humaux-maintenance:serve]}/metrics)
MD_CURRENT=$(md_daemon_db)
# ADR-0062 D-N (0223): a LOST ticket's reissue waits the cool-down from its sweep (lost_at), and one memory is
# reissued at most once per cool-down window. With no runner here every reissued ticket goes LOST again, so the
# chain repeats for the whole soak; each link must keep both spacings.
MD_COOLDOWN=2
MD_REISSUE_EARLY=$(MDQ "select count(*) from projection.ticket_reissues r join projection.stream_log s on s.tenant_id = r.tenant_id and s.commit_seq = r.source_commit_seq where r.source_state = 'LOST' and (s.lost_at is null or r.reissued_at < s.lost_at + interval '$MD_COOLDOWN seconds')")
MD_REISSUE_DENSE=$(MDQ "select count(*) from (select r.reissued_at - lag(r.reissued_at) over (partition by r.tenant_id, o.evidence_id order by r.reissued_at) as gap from projection.ticket_reissues r join ops.outbox o on o.tenant_id = r.tenant_id and o.commit_seq = r.reissued_commit_seq) g where g.gap < interval '$MD_COOLDOWN seconds'")
MD_REISSUES=$(MDQ "select count(*) from projection.ticket_reissues")
echo "maintenance_drain: left=$MD_LEFT after $(( $(date +%s) - MD_D0 ))s; orphans LOST+reissued once=$MD_ORPHANS/8; status(cycles state last_cycle)=$MD_STATUS /metrics=$MD_CODE daemon_db=$MD_CURRENT" | tee -a $EV/rehearsal.log
assert_eq "maintenance_daemon_db_is_throwaway(db=$MD_CURRENT, shared=$DB, spawn_refused=$MD_SPAWN_REFUSED)" "$([ "$MD_CURRENT" = "$MD_DB" ] && [ "$MD_CURRENT" != "$DB" ] && [ "$MD_SPAWN_REFUSED" = 0 ] && echo 1 || echo 0)" 1
assert_eq "maintenance_reissue_waits_cooldown_after_lost(reissues=$MD_REISSUES, dense=${MD_REISSUE_DENSE:-?})" "${MD_REISSUE_EARLY:-x}:${MD_REISSUE_DENSE:-x}" "0:0"
assert_eq "maintenance_daemon_setup_migrated_and_seeded(k=$MD_K)" "$MD_SETUP_OK" 1
assert_gt "maintenance_kill9_landed_mid_purge(receipts_before=${MD_K9_RECEIPTS:-0})" "${MD_K9_LEFT:-0}" 0
assert_eq "maintenance_soak_receipts_balance($MD_BAL_LINE)" "$MD_BAL_BAD" 0
assert_eq "maintenance_seeded_work_drained" "$MD_LEFT" 0
assert_eq "maintenance_every_orphan_lost_and_reissued_once" "$MD_ORPHANS" 8
assert_eq "maintenance_daemon_cycled_and_ready_after_restart($MD_STATUS)" "$([ "${MD_STATUS%% *}" -ge 1 ] 2>/dev/null && [ "$MD_CODE" = 200 ] && echo 1 || echo 0)" 1

# ADR-0058 R11: every DEAD distill of the run, graded under its own name. Classes are the job's
# `last_error_class` plus, for a refused reply, the parser/tool-shape reason the worker printed
# (`distill evidence=<id> failed: InvalidInput (<reason>)`).
DD_N=$(PGQ "select count(*) from ops.outbox where tenant_id in ($SEEDED) and event_type='EVIDENCE_ACCEPTED'")
# `dead` is the database's own count(*): a PGQ that fails (docker exec, psql, fork EAGAIN) leaves it
# empty and the assertion red. The row query below only labels the classes.
DD_K=$(PGQ "select count(*) from ops.jobs where tenant_id in ($SEEDED) and job_type='DERIVED_DISTILL' and status='DEAD'")
DD_ROWS=$(PGQ "select coalesce(payload->>'evidence_id','-')||' '||coalesce(last_error_class,'-') from ops.jobs where tenant_id in ($SEEDED) and job_type='DERIVED_DISTILL' and status='DEAD' order by created_at")
DD_CLASSES=$(print -r -- "$DD_ROWS" | while read dd_ev dd_cls; do
    [ -n "$dd_ev" ] || continue
    dd_r=$(cat $EV/*.log 2>/dev/null | grep -oE "distill evidence=$dd_ev failed: InvalidInput \([a-z_]+\)" | tail -1 | sed -E 's/.*\(([a-z_]+)\)/\1/')
    print -r -- "$dd_cls${dd_r:+/$dd_r}"
  done | sort | uniq -c | awk '{printf "%s%s:%s", (NR>1?",":""), $2, $1}')
# Bound, not zero: a live model's reply can be refused twice for one Evidence (measured: 0-2 per rehearsal,
# ADR-0058 R11 amendment). The run is red above 1 % of its Evidence — the soak's own op-failure bound — and on
# any count the database did not answer. Every death is still printed with its class.
DD_MAX=$(( ${DD_N:-0} / 100 )) 2>/dev/null || DD_MAX=0
case "$DD_N$DD_K" in ''|*[!0-9]*) DD_OK=0 ;; *) [ -n "$DD_N" ] && [ -n "$DD_K" ] && [ "$DD_K" -le "$DD_MAX" ] && DD_OK=1 || DD_OK=0 ;; esac
assert_eq "distill_dead(n=$DD_N, dead=$DD_K, at_most=$DD_MAX = 1% of n, classes=${DD_CLASSES:--})" "$DD_OK" 1

# ---------- 6d. alert_drill: a synthetic INV-1 reaches the alert route (ADR-0061 D-K) ----------
# A real scratch gateway G' and a real LaneSubstituted degrade; NO database grant or row is changed.
# The INV-1' shape (denominator absent) is made at the drill Prometheus P''s ingestion: P' drops
# humaux_retrieval_requests_total from G' with a metric_relabel rule, exactly as if it were never
# exported. P' loads the two production rule files unchanged and sends to the same Alertmanager;
# its alerts carry `prometheus: drill` so they never merge with the main Prometheus's. G'/P' ports
# are local to this step: they are not in OPS_PORTS, so the main `up` set never sees them.
step alert_drill
AD_T0=$(date +%s)
GWD_PORT=18081; GWD_OPS=19111; PROMD_PORT=19191; PD=$S/prom-drill; PDB=http://127.0.0.1:$PROMD_PORT
rm -rf $PD; mkdir -p $PD/data
start_gw 127.0.0.1:$GWD_PORT $GWD_OPS gwd $EV/gateway-drill.log
gwd_readyz() { curl -fsS -o /dev/null http://127.0.0.1:$GWD_PORT/readyz; }
render_drill() { # $1 = 1 with the relabel drop, 0 without
  cat > $PD/prometheus.yml <<YEOF
global:
  scrape_interval: 5s
  evaluation_interval: 5s
  external_labels:
    git_sha: "${DEPLOY_SHA:-__HUMAUX_GIT_SHA__}"
    prometheus: drill
rule_files:
  - $R/deploy/prometheus/invariants.rules.yml
  - $R/deploy/prometheus/alerts.rules.yml
alerting:
  alertmanagers:
    - static_configs:
        - targets: ["127.0.0.1:$AM_PORT"]
scrape_configs:
  - job_name: humaux-gateway-drill
    static_configs:
      - targets: ["127.0.0.1:$GWD_OPS"]
YEOF
  [ "$1" = 1 ] && cat >> $PD/prometheus.yml <<'YEOF'
    metric_relabel_configs:
      - source_labels: [__name__]
        regex: humaux_retrieval_requests_total
        action: drop
YEOF
  $PT promtool check config $PD/prometheus.yml >> $EV/observability.log 2>&1
}
pq() { # $1 = PromQL against P'; prints the first sample's value, empty when the result is empty
  curl -fsS -G "$PDB/api/v1/query" --data-urlencode "query=$1" 2>/dev/null | python3 -c "import sys,json
try: r=json.load(sys.stdin)['data']['result']; print(r[0]['value'][1] if r else '')
except Exception: print('')"; }
inv1_state() { curl -fsS $PDB/api/v1/alerts 2>/dev/null | python3 -c "import sys,json
try: a=json.load(sys.stdin)['data']['alerts']
except Exception: a=[]
s=[x['state'] for x in a if x['labels'].get('alertname')=='INV-1']
print(s[0] if s else 'inactive')"; }
receipt() { # $1 = alertname $2 = status; 0 once the sink holds that alert from P' in that status
  python3 - $EV/alert_receipts.jsonl $1 $2 <<'PYEOF'
import json, sys
path, name, status = sys.argv[1:4]
for line in open(path):
    body = json.loads(line)["body"]
    for a in (body.get("alerts", []) if isinstance(body, dict) else []):
        l = a.get("labels", {})
        if l.get("alertname") == name and l.get("prometheus") == "drill" and a.get("status") == status:
            sys.exit(0)
sys.exit(1)
PYEOF
}
poll() { # $1 = bound secs, rest = a command; prints the seconds waited; status 0 once the command succeeds
  local bound=$1 i=0; shift
  while [ $i -le $bound ]; do "$@" >/dev/null 2>&1 && { echo $i; return 0; }; sleep 1; i=$((i+1)); done
  echo $i; return 1
}
deg_at() { [ "$(pq 'sum(degrade_total{job="humaux-gateway-drill",code="LaneSubstituted"})')" = "$1" ]; }
deg_ge1() { [ "$(pq 'sum(degrade_total{job="humaux-gateway-drill",code="LaneSubstituted"})' | cut -d. -f1)" -ge 1 ] 2>/dev/null; }
req_seen() { [ -n "$(pq 'sum(humaux_retrieval_requests_total{job="humaux-gateway-drill"})')" ]; }
req_above() { [ "$(pq 'sum(humaux_retrieval_requests_total{job="humaux-gateway-drill"})' | cut -d. -f1)" -gt "$1" ] 2>/dev/null; }
inv1_is() { [ "$(inv1_state)" = "$1" ]; }
AD_PRE_OK=0; AD_DROP_OK=0; AD_LS=0; AD_FIRE_OK=0; AD_RCPT_OK=0; AD_CLEAR_OK=0; AD_RES_OK=0
if wait_ready drill-gateway gwd_readyz; then
  render_drill 1
  ( exec "${PT[@]}" prometheus --config.file=$PD/prometheus.yml --storage.tsdb.path=$PD/data \
      --web.listen-address=127.0.0.1:$PROMD_PORT >> $EV/prometheus-drill.log 2>&1 ) &
  own_pid promd $!
  promd_ready() { curl -fsS $PDB/-/ready; }
  wait_ready drill-prometheus promd_ready
  # 3. one sample of G''s degrade at 0 first, so rate() sees the increment.
  W=$(poll 15 deg_at 0) && AD_PRE_OK=1
  [ -z "$(pq 'count(humaux_retrieval_requests_total)')" ] && AD_DROP_OK=1
  echo "alert_drill: pre-sample degrade_total{code=LaneSubstituted}=0 in P' after ${W}s ok=$AD_PRE_OK; denominator absent in P' ok=$AD_DROP_OK" | tee -a $EV/rehearsal.log
  # 4. an abstaining recall: the lane_substitution step's queries, in order, until one degrades.
  for q in "目前项目进度" "客户张三最近的情绪怎么样" "和支付相关的决定" 'the "frozen contract" decision' "$LS_UUID"; do
    AD_ARGS=$(python3 -c "import sys,json; print(json.dumps({'query':sys.argv[1],'workspace_id':sys.argv[2]}, ensure_ascii=False))" "$q" "$WS")
    AD_OUT=$(GW_URL=http://127.0.0.1:$GWD_PORT mcp recall "$AD_ARGS")
    print -r -- "$AD_OUT" | head -1 | grep -q 'LANE_SUBSTITUTED' && { AD_LS=1; break; }
  done
  [ $AD_LS = 1 ] || echo "alert_drill: no recall to G' carried LANE_SUBSTITUTED (step lane_substitution's queries)" | tee -a $EV/rehearsal.log
  # 5. INV-1 fires in P' within scrape 5 s + eval 5 s + 2 s of the increment being visible there.
  W1=$(poll 10 deg_ge1); W2=$(poll 12 inv1_is firing) && AD_FIRE_OK=1
  echo "alert_drill: degrade visible in P' after ${W1}s; INV-1 $(inv1_state) in P' ${W2}s later" | tee -a $EV/rehearsal.log
  # 6. the route delivers it: group_wait 10 s + 10 s.
  W3=$(poll 20 receipt INV-1 firing) && AD_RCPT_OK=1
  echo "alert_drill: INV-1 firing receipt in alert_receipts.jsonl after ${W3}s ok=$AD_RCPT_OK" | tee -a $EV/rehearsal.log
  # 7. clear: the denominator comes back (config reload on SIGHUP, no lifecycle API), one recall
  #    moves it, INV-1 goes inactive within 2 evals and a resolved notification arrives.
  render_drill 0; own_signal $S/promd.pid prometheus HUP 0 >/dev/null
  W4=$(poll 20 req_seen); REQ0=$(pq 'sum(humaux_retrieval_requests_total{job="humaux-gateway-drill"})' | cut -d. -f1)
  GW_URL=http://127.0.0.1:$GWD_PORT mcp recall "{\"query\":\"which language do we prefer for backend services?\",\"workspace_id\":\"$WS\",\"mode\":\"semantic\"}" > $EV/alert_drill_clear_recall.json 2>&1
  W5=$(poll 10 req_above ${REQ0:-0}); W6=$(poll 12 inv1_is inactive) && AD_CLEAR_OK=1
  echo "alert_drill: denominator back in P' after ${W4}s (sum=${REQ0:-none}), moved after ${W5}s; INV-1 $(inv1_state) ${W6}s later" | tee -a $EV/rehearsal.log
  W7=$(poll 75 receipt INV-1 resolved) && AD_RES_OK=1
  echo "alert_drill: INV-1 resolved receipt after ${W7}s ok=$AD_RES_OK" | tee -a $EV/rehearsal.log
fi
# 8. G' and P' through their own pidfiles (obs_stop does the same on any other exit path).
[ -f $S/promd.pid ] && own_signal $S/promd.pid prometheus TERM 30 >/dev/null
own_signal $S/gwd.pid humaux-gateway TERM 30 >/dev/null
echo "alert_drill: elapsed $(( $(date +%s) - AD_T0 ))s" | tee -a $EV/rehearsal.log
assert_eq "alert_drill_presample_at_zero_and_denominator_dropped" "$AD_PRE_OK$AD_DROP_OK" 11
assert_eq "alert_drill_abstaining_recall_on_the_scratch_gateway" "$AD_LS" 1
assert_eq "alert_drill_inv1_fires_in_the_drill_prometheus" "$AD_FIRE_OK" 1
assert_eq "alert_drill_inv1_firing_reaches_the_alert_route" "$AD_RCPT_OK" 1
assert_eq "alert_drill_inv1_inactive_after_the_denominator_returns" "$AD_CLEAR_OK" 1
assert_eq "alert_drill_inv1_resolved_reaches_the_alert_route" "$AD_RES_OK" 1

# ---------- 7. stop ----------
step stop
# Through the pidfiles, not the variables: the soak's chaos hook restarts the retrieval worker,
# so $RW_PID is stale by here. own_signal confirms `ps -o comm=` before signalling anything.
own_signal $S/gw.pid humaux-gateway TERM 30
own_signal $S/rw.pid humaux-retrieval-worker TERM 30
own_signal $S/rp.pid humaux-retrieval-worker TERM 90
own_signal $S/pw.pid humaux-private-worker TERM 30
own_signal $S/mh.pid humaux-maintenance TERM 30
obs_stop; trap - EXIT
sleep 1
echo "done; tenant kept for inspection: $TENANT (teardown: cargo run -q -p xtask -- e2e-seed --teardown $TENANT)" | tee -a $EV/rehearsal.log
# Card 21 fix pass: the assertion table IS the verdict. Without this the script exited 0 whatever
# the assertions said, so "the rehearsal ran" and "the rehearsal passed" were indistinguishable
# from the outside — which is exactly how a table of unrun assertions got reported as delivered.
echo "REHEARSAL VERDICT: $A_OK passed, $A_BAD failed" | tee -a $EV/rehearsal.log
[ "${A_BAD:-1}" -eq 0 ] || exit 1
