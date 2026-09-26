#!/bin/zsh
# 部署点演练驱动（本机四进程；真 DashScope + 真 MiniMax）。密钥只 source 进对应子 shell；任何输出不含 bearer/key。
set -u
S=${HUMAUX_REHEARSE_WORK:-${TMPDIR:-/tmp}/humaux-rehearsal}   # work dir: pidfiles, helpers, evidence (override with HUMAUX_REHEARSE_WORK)
EV=$S/e2e_evidence; SOCK=/tmp/hq-e2e; mkdir -p $EV $SOCK
R=/Volumes/data/humaux-thread; cd $R
PG=127.0.0.1:54329; DB=${HUMAUX_REHEARSE_DB:-humaux_thread_dev}; MYUID=$(id -u)
export HUMAUX_TEST_PG_DSN="postgres://postgres:devlocal@$PG/$DB" HUMAUX_MAINTENANCE_PG_DSN="postgres://role_maintenance:devlocal_role_maintenance@$PG/$DB"
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
# MiniMax lane（值须与种子一致；provider_id/model 由 seed flags 决定）
MM_URL=https://api.minimaxi.com/v1/chat/completions; MM_PROVIDER=minimax; MM_MODEL=MiniMax-M3; MM_REV=2026-08; MM_REGION=cn-shanghai; MM_TIER=standard
EGRESS_PROC=$(uuidgen | tr 'A-Z' 'a-z')
step() { echo "### STEP $1 $(date +%T)" | tee -a $EV/rehearsal.log; }
doh_ips() { curl -s "https://dns.alidns.com/resolve?name=$1&type=A" | python3 -c "import sys,json; d=json.load(sys.stdin); print('|'.join(a['data'] for a in d.get('Answer',[]) if a.get('type')==1))"; }
MM_HOST=$(print -r -- "$MM_URL" | sed -E 's#https://([^/]+)/.*#\1#')
MM_PINS="$MM_HOST=$(doh_ips $MM_HOST)"
echo "dns pins: $MM_PINS" | tee -a $EV/rehearsal.log

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
  curl -s -w '\nHTTP %{http_code}\n' -X POST "http://127.0.0.1:8080/mcp" \
    -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' -H 'MCP-Protocol-Version: 2026-07-28' -H 'Mcp-Method: tools/call' -H "Mcp-Name: $1" \
    -H 'Origin: http://127.0.0.1:8080' -H "Authorization: Bearer $BEARER" --data "$body"
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

# ---------- 0. build ----------
step build
cargo build -p humaux-gateway -p humaux-retrieval-worker -p humaux-consolidation-worker -p humaux-private-worker -p xtask 2>&1 | tail -2 | tee -a $EV/rehearsal.log

# ---------- 1. seed (stdout kept in a variable only) ----------
step seed
SEED_OUT=$(cargo run -q -p xtask -- e2e-seed --pepper-hex $PEPPER_HEX --scopes memory:write,context:read --limit 1000 \
  --processor-id $EGRESS_PROC --region $MM_REGION --service-tier $MM_TIER --endpoint-ref $MM_URL \
  --provider-id $MM_PROVIDER --provider-model-id $MM_MODEL --model-revision $MM_REV \
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
print -r -- "$SEED_OUT" | grep -vE 'Bearer|export' | tee -a $EV/seed_ids.txt >/dev/null   # ids only, no secret
echo "seed ok tenant=$TENANT ws=$WS domain=$RDOM" | tee -a $EV/rehearsal.log

# ---------- 1b. seed tenant B (card 24 acceptance gate: TWO tenants on ONE gateway) ----------
# e2e-seed provisions ONE tenant per invocation, so it runs twice with the SAME pepper; one
# gateway process serves N (tenant, workspace) pairs per request since cards 10/11/13. This used
# to live inside the `if SOAK_SECS` block, which meant the 5/5 rehearsal the delivery claim rests
# on was single-tenant and could not witness cross-tenant isolation at all.
# --processor-id is the DEPLOYMENT's egress processor (§7.3), not a per-tenant value: both
# tenants get $EGRESS_PROC or tenant B's distill defers every row forever, silently (card 16 P0).
step seed_b
SEED_OUT_B=$(cargo run -q -p xtask -- e2e-seed --pepper-hex $PEPPER_HEX --scopes memory:write,context:read --limit 1000 \
  --processor-id $EGRESS_PROC --region $MM_REGION --service-tier $MM_TIER --endpoint-ref $MM_URL \
  --provider-id $MM_PROVIDER --provider-model-id $MM_MODEL --model-revision $MM_REV \
  --collection $COLLECTION --dimension $EMB_DIM --embedding-provider dashscope --embedding-region $EMB_REGION 2>$EV/seed_b.stderr)
valb() { print -r -- "$SEED_OUT_B" | grep -iE "^[[:space:]]*$1[[:space:]]*[:=]" | head -1 | sed -E 's/^[^:=]*[:=][[:space:]]*//' | tr -d ' '; }
export BEARER_A="$BEARER"
export BEARER_B=$(print -r -- "$SEED_OUT_B" | sed -n 's/^Authorization: Bearer //p' | head -1)
TENANT_B=$(valb tenant_id); WS_B=$(valb workspace_id); USERID_B=$(valb user_id)
print -r -- "$SEED_OUT_B" | grep -vE 'Bearer|export' | tee -a $EV/seed_ids.txt >/dev/null
[ -z "$BEARER_B" ] && { echo "seed_b: no bearer for tenant B" | tee -a $EV/rehearsal.log; exit 2; }
[ "$TENANT_B" = "$TENANT" ] && { echo "seed_b: tenant B is tenant A — isolation cannot be witnessed" | tee -a $EV/rehearsal.log; exit 2; }
echo "tenants: A=$TENANT/$WS  B=$TENANT_B/$WS_B" | tee -a $EV/rehearsal.log

# ---------- 2. processes ----------
step processes
rm -f $SOCK/*.sock
# ONE definition per resident process, used by the first spawn AND by the kill -9 recovery in
# step kill9_rotation. A chaos step that restarts a differently-configured process grades a
# deployment nobody ran (the same argument $DS_ENV/$CW_ENV already make for the soak).
start_pw() {
( export PRIVATE_WORKER_PG_DSN="postgres://role_private_worker:devlocal_role_private_worker@$PG/$DB" \
    HUMAUX_PRIVATE_WORKER_RPC_SOCKET_PATH=$SOCK/inference.sock HUMAUX_PRIVATE_WORKER_CONSOLIDATION_UID=$MYUID \
    HUMAUX_PRIVATE_WORKER_KEY_ENV=MINIMAX_API_KEY HUMAUX_PRIVATE_WORKER_HTTP_TIMEOUT_SECS=120 HUMAUX_PRIVATE_WORKER_PERMIT_TTL_SECS=60 \
    HUMAUX_PRIVATE_WORKER_DNS_PINS="$MM_PINS"
  set -a; source /Volumes/data/viral-skill-eval/.env; set +a
  exec "${CARGO_TARGET_DIR:-target}"/debug/humaux-private-worker --serve-rpc >> $EV/private-worker.log 2>&1 ) &
own_pid pw $!
}
start_pw; PW_PID=$(cat $S/pw.pid)
start_rw() {
( export HUMAUX_RETRIEVAL_WORKER_PG_DSN="postgres://role_retrieval_worker:devlocal_role_retrieval_worker@$PG/$DB" \
    HUMAUX_RETRIEVAL_WORKER_RPC_SOCKET_PATH=$SOCK/retrieval.sock HUMAUX_RETRIEVAL_WORKER_GATEWAY_UID=$MYUID \
    HUMAUX_RETRIEVAL_WORKER_EMBEDDING_PROVIDER=dashscope HUMAUX_RETRIEVAL_WORKER_EMBEDDING_MODEL=$EMB_MODEL HUMAUX_RETRIEVAL_WORKER_MODEL_REVISION=$EMB_REV \
    HUMAUX_RETRIEVAL_WORKER_DIMENSION=$EMB_DIM HUMAUX_RETRIEVAL_WORKER_EMBEDDING_VERSION=$EMB_VER HUMAUX_RETRIEVAL_WORKER_REGION=$EMB_REGION HUMAUX_RETRIEVAL_WORKER_MAX_INPUT_TOKENS=$EMB_MAX_TOK \
    HUMAUX_RETRIEVAL_WORKER_QDRANT_HOST=127.0.0.1 HUMAUX_RETRIEVAL_WORKER_QDRANT_PORT=6333 HUMAUX_RETRIEVAL_WORKER_QDRANT_CIDR=127.0.0.1/32 HUMAUX_RETRIEVAL_WORKER_QDRANT_TLS=false HUMAUX_RETRIEVAL_WORKER_QDRANT_COLLECTION=$COLLECTION \
    HUMAUX_RETRIEVAL_WORKER_CELL_ID=$CELL_ID HUMAUX_RETRIEVAL_WORKER_CALLER=retrieval-worker \
    HUMAUX_RETRIEVAL_WORKER_TENANT_ID=$TENANT HUMAUX_RETRIEVAL_WORKER_SCOPE_KIND=workspace HUMAUX_RETRIEVAL_WORKER_SCOPE_ID=$WS HUMAUX_RETRIEVAL_WORKER_BATCH=50 \
    HUMAUX_RETRIEVAL_WORKER_GITLEAKS_BIN=$GITLEAKS_BIN HUMAUX_RETRIEVAL_WORKER_GITLEAKS_SHA256=$GITLEAKS_SHA HUMAUX_RETRIEVAL_WORKER_GITLEAKS_VERSION=$GITLEAKS_VER
  set -a; source $R/.env.local; set +a
  exec "${CARGO_TARGET_DIR:-target}"/debug/humaux-retrieval-worker --serve-rpc >> $EV/retrieval-worker.log 2>&1 ) &
own_pid rw $!
}
start_rw; RW_PID=$(cat $S/rw.pid)
start_gw() {
( export HUMAUX_GATEWAY_PG_DSN="postgres://role_gateway:devlocal_role_gateway@$PG/$DB" HUMAUX_GATEWAY_BIND_ADDR=127.0.0.1:8080 \
    HUMAUX_GATEWAY_CREDENTIAL_PEPPER_HEX=$PEPPER_HEX HUMAUX_GATEWAY_ALLOWED_HOSTS=127.0.0.1:8080 HUMAUX_GATEWAY_ALLOWED_ORIGINS=http://127.0.0.1:8080 \
    HUMAUX_GATEWAY_MAX_REQUEST_BODY_BYTES=1048576 HUMAUX_GATEWAY_TRUSTED_PROXY_CIDRS= HUMAUX_GATEWAY_MAX_FORWARDED_HOPS=1 HUMAUX_GATEWAY_GLOBAL_DENYLIST= HUMAUX_GATEWAY_GLOBAL_EMERGENCY_ALLOWLIST= \
    HUMAUX_GATEWAY_RESERVATION_TTL_SECONDS=30 HUMAUX_GATEWAY_HANDLER_TIMEOUT_SECONDS=20 HUMAUX_GATEWAY_FINALIZE_TIMEOUT_SECONDS=5 HUMAUX_GATEWAY_REPLAY_TTL_SECONDS=60 \
    HUMAUX_GATEWAY_CONFIRM_TOKEN_TTL_SECONDS=300 HUMAUX_GATEWAY_UNDO_WINDOW_SECONDS=86400 HUMAUX_GATEWAY_MOOD_HALF_LIFE_SECONDS=21600 \
    HUMAUX_GATEWAY_REMEMBER_TENANT_ID=$TENANT HUMAUX_GATEWAY_REMEMBER_WORKSPACE_ID=$WS HUMAUX_GATEWAY_REMEMBER_SCOPE_KIND=workspace \
    HUMAUX_GATEWAY_REMEMBER_DOMAIN=$DOMAIN HUMAUX_GATEWAY_REMEMBER_PROJECTION_KIND=$PKIND HUMAUX_GATEWAY_REMEMBER_PROJECTION_VERSION=$PVER \
    HUMAUX_GATEWAY_REMEMBER_REASONING_DOMAIN_ID=$RDOM HUMAUX_GATEWAY_REMEMBER_TOKEN_TTL_SECONDS=60 HUMAUX_GATEWAY_REMEMBER_DATA_CLASS=INTERNAL \
    HUMAUX_GATEWAY_REMEMBER_VISIBILITY_CLASS=WORKSPACE_SHARED HUMAUX_GATEWAY_REMEMBER_EVENT_KIND=USER_MESSAGE \
    HUMAUX_GATEWAY_CONTEXT_TOTAL_TOKENS=2048 HUMAUX_GATEWAY_CONTEXT_MANDATORY_TOKENS=1024 \
    HUMAUX_GATEWAY_RETRIEVAL_RPC_SOCKET_PATH=$SOCK/retrieval.sock HUMAUX_GATEWAY_RETRIEVAL_RPC_PERMIT_TTL_SECONDS=60 \
    HUMAUX_GATEWAY_EMBEDDING_DIMENSION=$EMB_DIM HUMAUX_GATEWAY_EMBEDDING_VERSION=$EMB_VER \
    HUMAUX_GATEWAY_QDRANT_HOST=127.0.0.1 HUMAUX_GATEWAY_QDRANT_PORT=6333 HUMAUX_GATEWAY_QDRANT_CIDR=127.0.0.1/32 HUMAUX_GATEWAY_QDRANT_TLS=false \
    HUMAUX_GATEWAY_CELL_ID=$CELL_ID HUMAUX_GATEWAY_CALLER_ID=gateway \
    HUMAUX_GATEWAY_RATE_PREAUTH_IP_CAPACITY=100 HUMAUX_GATEWAY_RATE_PREAUTH_IP_REFILL_PER_SECOND=100 HUMAUX_GATEWAY_RATE_CREDENTIAL_CAPACITY=100 HUMAUX_GATEWAY_RATE_CREDENTIAL_REFILL_PER_SECOND=100 HUMAUX_GATEWAY_RATE_USER_CAPACITY=100 HUMAUX_GATEWAY_RATE_USER_REFILL_PER_SECOND=100 HUMAUX_GATEWAY_RATE_TENANT_CAPACITY=100 HUMAUX_GATEWAY_RATE_TENANT_REFILL_PER_SECOND=100 HUMAUX_GATEWAY_RATE_OPERATION_CAPACITY=100 HUMAUX_GATEWAY_RATE_OPERATION_REFILL_PER_SECOND=100 \
    HUMAUX_GATEWAY_GITLEAKS_BIN=$GITLEAKS_BIN HUMAUX_GATEWAY_GITLEAKS_SHA256=$GITLEAKS_SHA HUMAUX_GATEWAY_GITLEAKS_VERSION=$GITLEAKS_VER
  exec "${CARGO_TARGET_DIR:-target}"/debug/humaux-gateway >> $EV/gateway.log 2>&1 ) &
own_pid gw $!
}
start_gw; GW_PID=$(cat $S/gw.pid)
for i in $(seq 1 30); do code=$(curl -s -o /dev/null -w '%{http_code}' -X POST http://127.0.0.1:8080/mcp -H 'Origin: http://127.0.0.1:8080' --data '{}' 2>/dev/null); [ "$code" != "000" ] && break; sleep 1; done
echo "gateway http=$code pw=$PW_PID rw=$RW_PID gw=$GW_PID" | tee -a $EV/rehearsal.log
ls -la $SOCK | tee -a $EV/rehearsal.log

# ---------- 2b. readiness gate (card 15 / ADR-0037; docs/ops/supervision.md §1) ----------
# No traffic is sent until every process answers its OWN probe. A rehearsal that starts writing
# while a dependency is still coming up measures the race, not the system.
step readyz
# Each probe uses exactly the keys ADR-0037 says that probe needs — nothing more, and no tenant
# id for the two derived workers (ADR-0036 / card 14 env contract).
rw_readyz() { ( export HUMAUX_RETRIEVAL_WORKER_PG_DSN="postgres://role_retrieval_worker:devlocal_role_retrieval_worker@$PG/$DB" \
    HUMAUX_RETRIEVAL_WORKER_QDRANT_HOST=127.0.0.1 HUMAUX_RETRIEVAL_WORKER_QDRANT_PORT=6333 \
    HUMAUX_RETRIEVAL_WORKER_QDRANT_CIDR=127.0.0.1/32 HUMAUX_RETRIEVAL_WORKER_QDRANT_TLS=false \
    HUMAUX_RETRIEVAL_WORKER_CELL_ID=$CELL_ID HUMAUX_RETRIEVAL_WORKER_CALLER=retrieval-worker
  exec "${CARGO_TARGET_DIR:-target}"/debug/humaux-retrieval-worker --readyz ) }
pw_readyz() { ( export PRIVATE_WORKER_PG_DSN="postgres://role_private_worker:devlocal_role_private_worker@$PG/$DB"
  exec "${CARGO_TARGET_DIR:-target}"/debug/humaux-private-worker --readyz ) }
cw_readyz() { ( export CONSOLIDATION_WORKER_PG_DSN="postgres://role_consolidation_worker:devlocal_role_consolidation_worker@$PG/$DB" \
    HUMAUX_CONSOLIDATION_WORKER_RPC_SOCKET_PATH=$SOCK/inference.sock
  exec "${CARGO_TARGET_DIR:-target}"/debug/humaux-consolidation-worker --readyz ) }
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
wait_ready consolidation-worker cw_readyz   # dials the private worker's UDS from the side that uses it
wait_ready gateway gw_readyz
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
# distill dispatch is tenant-free, so ONE pass settles both tenants' work.
distill_once() {
( export PRIVATE_WORKER_PG_DSN="postgres://role_private_worker:devlocal_role_private_worker@$PG/$DB" \
    HUMAUX_PRIVATE_WORKER_KEY_ENV=MINIMAX_API_KEY HUMAUX_PRIVATE_WORKER_HTTP_TIMEOUT_SECS=120 HUMAUX_PRIVATE_WORKER_PERMIT_TTL_SECS=60 \
    HUMAUX_PRIVATE_WORKER_DNS_PINS="$MM_PINS" HUMAUX_PRIVATE_WORKER_RPC_SOCKET_PATH=$SOCK/inference-distill.sock HUMAUX_PRIVATE_WORKER_CONSOLIDATION_UID=$MYUID \
    HUMAUX_PRIVATE_WORKER_CANDIDATE_TTL_SECONDS=86400 \
    HUMAUX_PRIVATE_WORKER_DISTILL_BATCH=50 HUMAUX_PRIVATE_WORKER_DISTILL_LEASE_SECS=120 HUMAUX_PRIVATE_WORKER_DISTILL_JOB_BATCH=8 HUMAUX_PRIVATE_WORKER_DISTILL_MAX_ATTEMPTS=5
  set -a; source /Volumes/data/viral-skill-eval/.env; set +a
  exec "${CARGO_TARGET_DIR:-target}"/debug/humaux-private-worker --distill-once ) 2>&1 | tee -a $EV/distill.log | tail -3
}
distill_once
PGQ "select 'memory_records='||(select count(*) from private.memory_records where tenant_id='$TENANT')||' memory_evidence='||(select count(*) from private.memory_evidence me join private.memory_records m on m.memory_id=me.memory_id where m.tenant_id='$TENANT')||' processing_runs='||(select count(*) from private.processing_runs where tenant_id='$TENANT' and completed_at is not null)||' outbox_done='||(select count(*) from ops.outbox where tenant_id='$TENANT' and status='DONE')" | tee -a $EV/rehearsal.log
PGQ "select 'memory: '||authority_class||' '||visibility_class||' '||left(content::text,90) from private.memory_records where tenant_id='$TENANT' order by created_at" | tee -a $EV/rehearsal.log

# ---------- 3c. SIGTERM mid-load: no lease may be stranded (card 15 §3 / card 16 ADR-0038 D5) ----------
# The resident distill worker observes the signal only BETWEEN passes, so a pass always settles
# every job it claimed. The witness is SQL, not the exit code: after the process is gone, no
# ops.jobs row may still be PROCESSING with a live lease.
step sigterm_mid_load
( export PRIVATE_WORKER_PG_DSN="postgres://role_private_worker:devlocal_role_private_worker@$PG/$DB" \
    HUMAUX_PRIVATE_WORKER_KEY_ENV=MINIMAX_API_KEY HUMAUX_PRIVATE_WORKER_HTTP_TIMEOUT_SECS=120 HUMAUX_PRIVATE_WORKER_PERMIT_TTL_SECS=60 \
    HUMAUX_PRIVATE_WORKER_DNS_PINS="$MM_PINS" HUMAUX_PRIVATE_WORKER_RPC_SOCKET_PATH=$SOCK/inference-drain.sock HUMAUX_PRIVATE_WORKER_CONSOLIDATION_UID=$MYUID \
    HUMAUX_PRIVATE_WORKER_CANDIDATE_TTL_SECONDS=86400 \
    HUMAUX_PRIVATE_WORKER_DISTILL_BATCH=50 HUMAUX_PRIVATE_WORKER_DISTILL_LEASE_SECS=120 HUMAUX_PRIVATE_WORKER_DISTILL_JOB_BATCH=8 HUMAUX_PRIVATE_WORKER_DISTILL_MAX_ATTEMPTS=5 \
    HUMAUX_PRIVATE_WORKER_DISTILL_POLL_INTERVAL_SECS=3
  set -a; source /Volumes/data/viral-skill-eval/.env; set +a
  exec "${CARGO_TARGET_DIR:-target}"/debug/humaux-private-worker --distill-serve > $EV/distill-serve.log 2>&1 ) &
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
( export CONSOLIDATION_WORKER_PG_DSN="postgres://role_consolidation_worker:devlocal_role_consolidation_worker@$PG/$DB" \
    HUMAUX_CONSOLIDATION_WORKER_RPC_SOCKET_PATH=$SOCK/inference.sock HUMAUX_CONSOLIDATION_WORKER_CALL_TTL_SECS=120 HUMAUX_CONSOLIDATION_WORKER_DIAL_TIMEOUT_SECS=10 \
    HUMAUX_CONSOLIDATION_WORKER_MAX_INPUTS=50 HUMAUX_CONSOLIDATION_WORKER_LEASE_SECS=120 HUMAUX_CONSOLIDATION_WORKER_BATCH=8 HUMAUX_CONSOLIDATION_WORKER_MAX_ATTEMPTS=5
  exec "${CARGO_TARGET_DIR:-target}"/debug/humaux-consolidation-worker --run-once ) > $EV/consolidation.log 2>&1 &
CW_PID=$!; for i in $(seq 1 180); do kill -0 $CW_PID 2>/dev/null || break; grep -q "published\|Published\|rollup" $EV/consolidation.log 2>/dev/null && sleep 3 && break; sleep 1; done; kill $CW_PID 2>/dev/null; tail -n 3 $EV/consolidation.log
PGQ "select 'rollups='||count(*) from private.memory_rollups where tenant_id='$TENANT'" | tee -a $EV/rehearsal.log
PGQ "select 'derived_jobs: '||coalesce(string_agg(job_type||'/'||status||'='||n, ', ' order by job_type, status),'none') from (select job_type, status, count(*) n from ops.jobs where tenant_id='$TENANT' and left(job_type,8)='DERIVED_' group by 1,2) t" | tee -a $EV/rehearsal.log
PGQ "select 'rpc='||outcome||' run_bound='||(consolidation_run_id is not null) from ops.private_inference_rpc_calls where tenant_id='$TENANT' order by registered_at desc limit 1" | tee -a $EV/rehearsal.log

# ---------- 5. projection (third hop, real DashScope) ----------
step projection
# The projection worker IS tenant-bound (§17.1 / ADR-0036): one pass per (tenant, workspace).
project_tenant() { # $1=tenant $2=workspace
( export HUMAUX_RETRIEVAL_WORKER_PG_DSN="postgres://role_retrieval_worker:devlocal_role_retrieval_worker@$PG/$DB" \
    HUMAUX_RETRIEVAL_WORKER_EMBEDDING_PROVIDER=dashscope HUMAUX_RETRIEVAL_WORKER_EMBEDDING_MODEL=$EMB_MODEL HUMAUX_RETRIEVAL_WORKER_MODEL_REVISION=$EMB_REV \
    HUMAUX_RETRIEVAL_WORKER_DIMENSION=$EMB_DIM HUMAUX_RETRIEVAL_WORKER_EMBEDDING_VERSION=$EMB_VER HUMAUX_RETRIEVAL_WORKER_REGION=$EMB_REGION HUMAUX_RETRIEVAL_WORKER_MAX_INPUT_TOKENS=$EMB_MAX_TOK \
    HUMAUX_RETRIEVAL_WORKER_QDRANT_HOST=127.0.0.1 HUMAUX_RETRIEVAL_WORKER_QDRANT_PORT=6333 HUMAUX_RETRIEVAL_WORKER_QDRANT_CIDR=127.0.0.1/32 HUMAUX_RETRIEVAL_WORKER_QDRANT_TLS=false HUMAUX_RETRIEVAL_WORKER_QDRANT_COLLECTION=$COLLECTION \
    HUMAUX_RETRIEVAL_WORKER_CELL_ID=$CELL_ID HUMAUX_RETRIEVAL_WORKER_CALLER=retrieval-worker \
    HUMAUX_RETRIEVAL_WORKER_TENANT_ID=$1 HUMAUX_RETRIEVAL_WORKER_SCOPE_KIND=workspace HUMAUX_RETRIEVAL_WORKER_SCOPE_ID=$2 HUMAUX_RETRIEVAL_WORKER_BATCH=50 \
    HUMAUX_RETRIEVAL_WORKER_GITLEAKS_BIN=$GITLEAKS_BIN HUMAUX_RETRIEVAL_WORKER_GITLEAKS_SHA256=$GITLEAKS_SHA HUMAUX_RETRIEVAL_WORKER_GITLEAKS_VERSION=$GITLEAKS_VER
  set -a; source $R/.env.local; set +a
  exec "${CARGO_TARGET_DIR:-target}"/debug/humaux-retrieval-worker --run-once ) 2>&1 | tee -a $EV/projection.log | tail -3
}
project_tenant $TENANT $WS
project_tenant $TENANT_B $WS_B
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
( export CONSOLIDATION_WORKER_PG_DSN="postgres://role_consolidation_worker:devlocal_role_consolidation_worker@$PG/$DB" \
    HUMAUX_CONSOLIDATION_WORKER_RPC_SOCKET_PATH=$SOCK/inference.sock HUMAUX_CONSOLIDATION_WORKER_CALL_TTL_SECS=120 HUMAUX_CONSOLIDATION_WORKER_DIAL_TIMEOUT_SECS=10 \
    HUMAUX_CONSOLIDATION_WORKER_MAX_INPUTS=50 HUMAUX_CONSOLIDATION_WORKER_LEASE_SECS=120 HUMAUX_CONSOLIDATION_WORKER_BATCH=8 HUMAUX_CONSOLIDATION_WORKER_MAX_ATTEMPTS=5
  exec "${CARGO_TARGET_DIR:-target}"/debug/humaux-consolidation-worker --run-once ) >> $EV/consolidation.log 2>&1
}
drain_all() {
  distill_once
  consolidate_once
  project_tenant $TENANT $WS
  project_tenant $TENANT_B $WS_B
  serve_switch_tenant $TENANT $WS
  serve_switch_tenant $TENANT_B $WS_B
}
PGQ "select 'serving='||serving||' projected='||coalesce(projection_highwater::text,'-') from projection.stream_checkpoints where tenant_id='$TENANT'" | tee -a $EV/rehearsal.log
# A tenant whose FIRST switch is refused on OpenGaps can never be promoted (a FAILED ticket is
# terminal, so the §15.4 prefix never clears it) and every recall for it answers
# no_serving_projection for the rest of the run. That is a live-model flake landing in the first
# ticket window (card 24 D1), not something a 10-minute soak can recover from — so fail here,
# at 3 minutes, instead of discovering it in the assertion table at 13.
if [ "$(PGQ "select count(*) from projection.stream_checkpoints where tenant_id='$TENANT' and serving")" != "1" ]; then
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
  gated_as "$BEARER" memory "{\"action\":\"unarchive\",\"memory_id\":\"$AR_TARGET\"}" > $EV/unarchive.json 2>&1
  mcp recall "{\"query\":\"which language do we prefer for backend services?\",\"workspace_id\":\"$WS\",\"mode\":\"semantic\"}" > $EV/recall_after_unarchive.json 2>&1
  AR_RECALL_AFTER_UNARCHIVE=$(head -1 $EV/recall_after_unarchive.json | python3 -c "
import sys,json
try: d=json.load(sys.stdin)['result'].get('structuredContent',{})
except Exception: d={}
print(json.dumps(d, ensure_ascii=False).count(sys.argv[1]))" "$AR_TARGET")
  drain_all
  echo "archive: recall_hits_while_archived=$AR_RECALL_WHILE_ARCHIVED get_reports_archived=$AR_GET_ARCHIVED recall_hits_after_unarchive=$AR_RECALL_AFTER_UNARCHIVE" | tee -a $EV/rehearsal.log
else
  echo "archive: no target (lifecycle step had no pair)" | tee -a $EV/rehearsal.log
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

# ---------- 6a1e. kill -9 every resident worker and recover (acceptance item 6) ----------
# SIGTERM drains, which is the case that is safe by construction; kill -9 is the case the leases
# exist to survive. Each worker is signalled ONLY through its own pidfile via own_signal, which
# refuses any PID whose `ps -o comm=` basename is not the expected binary (2026-09-09 incident).
step kill9_rotation
READY_BAD_BEFORE_CHAOS=$READY_BAD
own_signal $S/gw.pid humaux-gateway 9 15; start_gw; wait_ready gateway-after-kill9 gw_readyz
own_signal $S/rw.pid humaux-retrieval-worker 9 15; start_rw; wait_ready retrieval-worker-after-kill9 rw_readyz
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
# Exactly-once after a mid-flight kill: a re-run hop may NOT index the same memory twice into the
# same live projection slot. (Different versions/scopes are different slots by design, so the
# grouping is the slot, not the memory.)
K9_DUP_POINTS=$(PGQ "select count(*) from (select memory_id from projection.private_memory_points where tenant_id in ('$TENANT','$TENANT_B') and projection_live and retired_at is null group by memory_id, scope_id, projection_version, embedding_version having count(*) > 1) t")
echo "kill9 rotation: recovery_failures=$K9_RECOVERY_BAD recall_isError=$K9_RECALL_ERROR stranded_leases=$K9_STRANDED duplicate_live_points=$K9_DUP_POINTS" | tee -a $EV/rehearsal.log

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
echo "ASSERTIONS $A_OK passed, $A_BAD failed" | tee -a $EV/rehearsal.log

# ---------- 6c. soak (card 16 / ADR-0038) — only when SOAK_SECS is set ----------
# Two tenants (e2e-seed provisions ONE per invocation, so it runs twice with the SAME pepper —
# one gateway serves N (tenant, workspace) pairs per request since cards 10/11/13), concurrent
# sessions on both, a chaos hook that kill -9s and restarts the retrieval worker, and one
# projection loop per tenant (the projection worker IS tenant-bound; only the two DERIVED
# workers are tenant-free, ADR-0036).
if [ "${SOAK_SECS:-0}" -gt 0 ]; then
step soak
# --processor-id is the DEPLOYMENT's egress processor (§7.3), not a per-tenant value: the private
# worker holds one HUMAUX_PRIVATE_WORKER_EGRESS_PROCESSOR_ID and provider_matches_admission
# compares the two. Minting a fresh uuid here gave tenant B a route no running worker could admit,
# so its distill deferred every row forever, silently (card 16 P0). Both tenants: $EGRESS_PROC.
# Tenant B is provisioned in step seed_b on the main path now — the soak reuses that pair
# instead of minting a third tenant nothing else in this run ever asserts on.
echo "soak tenants: A=$TENANT/$WS  B=$TENANT_B/$WS_B" | tee -a $EV/rehearsal.log

# Self-contained hook scripts: the chaos/probe commands are run by `sh -c` out of xtask, so they
# cannot see this shell's functions. No API key is written into any of them — the retrieval
# worker's restart sources $R/.env.local exactly as step 2 does.
RW_ENV="export HUMAUX_RETRIEVAL_WORKER_PG_DSN='postgres://role_retrieval_worker:devlocal_role_retrieval_worker@$PG/$DB' \
HUMAUX_RETRIEVAL_WORKER_QDRANT_HOST=127.0.0.1 HUMAUX_RETRIEVAL_WORKER_QDRANT_PORT=6333 \
HUMAUX_RETRIEVAL_WORKER_QDRANT_CIDR=127.0.0.1/32 HUMAUX_RETRIEVAL_WORKER_QDRANT_TLS=false \
HUMAUX_RETRIEVAL_WORKER_QDRANT_COLLECTION=$COLLECTION HUMAUX_RETRIEVAL_WORKER_CELL_ID=$CELL_ID \
HUMAUX_RETRIEVAL_WORKER_CALLER=retrieval-worker HUMAUX_RETRIEVAL_WORKER_RPC_SOCKET_PATH=$SOCK/retrieval.sock \
HUMAUX_RETRIEVAL_WORKER_GATEWAY_UID=$MYUID HUMAUX_RETRIEVAL_WORKER_EMBEDDING_PROVIDER=dashscope \
HUMAUX_RETRIEVAL_WORKER_EMBEDDING_MODEL=$EMB_MODEL HUMAUX_RETRIEVAL_WORKER_MODEL_REVISION=$EMB_REV \
HUMAUX_RETRIEVAL_WORKER_DIMENSION=$EMB_DIM HUMAUX_RETRIEVAL_WORKER_EMBEDDING_VERSION=$EMB_VER \
HUMAUX_RETRIEVAL_WORKER_REGION=$EMB_REGION HUMAUX_RETRIEVAL_WORKER_MAX_INPUT_TOKENS=$EMB_MAX_TOK \
HUMAUX_RETRIEVAL_WORKER_SCOPE_KIND=workspace \
HUMAUX_RETRIEVAL_WORKER_BATCH=50 HUMAUX_RETRIEVAL_WORKER_GITLEAKS_BIN=$GITLEAKS_BIN \
HUMAUX_RETRIEVAL_WORKER_GITLEAKS_SHA256=$GITLEAKS_SHA HUMAUX_RETRIEVAL_WORKER_GITLEAKS_VERSION=$GITLEAKS_VER"

# One definition per worker, used by BOTH the resident spawn below and its chaos restart, so a
# restarted process is the same process — a chaos hook that starts a differently-configured
# worker grades a deployment nobody ran.
DS_ENV="export PRIVATE_WORKER_PG_DSN='postgres://role_private_worker:devlocal_role_private_worker@$PG/$DB' \
HUMAUX_PRIVATE_WORKER_KEY_ENV=MINIMAX_API_KEY HUMAUX_PRIVATE_WORKER_HTTP_TIMEOUT_SECS=120 HUMAUX_PRIVATE_WORKER_PERMIT_TTL_SECS=60 \
HUMAUX_PRIVATE_WORKER_DNS_PINS='$MM_PINS' HUMAUX_PRIVATE_WORKER_RPC_SOCKET_PATH=$SOCK/inference-soak.sock \
HUMAUX_PRIVATE_WORKER_CONSOLIDATION_UID=$MYUID HUMAUX_PRIVATE_WORKER_CANDIDATE_TTL_SECONDS=86400 \
HUMAUX_PRIVATE_WORKER_DISTILL_BATCH=50 HUMAUX_PRIVATE_WORKER_DISTILL_LEASE_SECS=120 \
HUMAUX_PRIVATE_WORKER_DISTILL_JOB_BATCH=8 HUMAUX_PRIVATE_WORKER_DISTILL_MAX_ATTEMPTS=5 \
HUMAUX_PRIVATE_WORKER_DISTILL_POLL_INTERVAL_SECS=3"
CW_ENV="export CONSOLIDATION_WORKER_PG_DSN='postgres://role_consolidation_worker:devlocal_role_consolidation_worker@$PG/$DB' \
HUMAUX_CONSOLIDATION_WORKER_RPC_SOCKET_PATH=$SOCK/inference.sock HUMAUX_CONSOLIDATION_WORKER_CALL_TTL_SECS=120 \
HUMAUX_CONSOLIDATION_WORKER_DIAL_TIMEOUT_SECS=10 HUMAUX_CONSOLIDATION_WORKER_MAX_INPUTS=50 \
HUMAUX_CONSOLIDATION_WORKER_LEASE_SECS=120 HUMAUX_CONSOLIDATION_WORKER_BATCH=8 \
HUMAUX_CONSOLIDATION_WORKER_MAX_ATTEMPTS=5 HUMAUX_CONSOLIDATION_WORKER_POLL_INTERVAL_SECS=3"

cat > $S/soak_probe_rw.sh <<EOF
#!/bin/sh
cd $R || exit 1
$RW_ENV
exec "${CARGO_TARGET_DIR:-target}"/debug/humaux-retrieval-worker --readyz
EOF
cat > $S/soak_probe_pw.sh <<EOF
#!/bin/sh
cd $R || exit 1
PRIVATE_WORKER_PG_DSN='postgres://role_private_worker:devlocal_role_private_worker@$PG/$DB'
export PRIVATE_WORKER_PG_DSN
exec "${CARGO_TARGET_DIR:-target}"/debug/humaux-private-worker --readyz
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
exec "${CARGO_TARGET_DIR:-target}"/debug/humaux-retrieval-worker --serve-rpc >> $EV/retrieval-worker.log 2>&1
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
exec "${CARGO_TARGET_DIR:-target}"/debug/humaux-private-worker --distill-serve >> $EV/soak-distill.log 2>&1
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
exec "${CARGO_TARGET_DIR:-target}"/debug/humaux-consolidation-worker --serve >> $EV/soak-consolidation.log 2>&1
) &
echo \$! > $S/cw.pid
sleep 3
exit 0
EOF
# one projection pass per tenant, then the §16.2 serving switch for that tenant
cat > $S/soak_project.sh <<EOF
#!/bin/sh
cd $R || exit 1
HUMAUX_MAINTENANCE_PG_DSN='postgres://role_maintenance:devlocal_role_maintenance@$PG/$DB'
export HUMAUX_MAINTENANCE_PG_DSN
while [ -f $S/soak_project.on ]; do
  for t_w in "$TENANT|$WS" "$TENANT_B|$WS_B"; do
    t=\$(echo "\$t_w" | cut -d'|' -f1); w=\$(echo "\$t_w" | cut -d'|' -f2)
    (
$RW_ENV
    export HUMAUX_RETRIEVAL_WORKER_TENANT_ID=\$t HUMAUX_RETRIEVAL_WORKER_SCOPE_ID=\$w
    set -a; . $R/.env.local; set +a
    "${CARGO_TARGET_DIR:-target}"/debug/humaux-retrieval-worker --run-once >> $EV/soak-projection.log 2>&1
    ) 
    n=\$(docker exec humaux-thread-pg psql -U postgres -d $DB -Atc "select count(*) from projection.private_memory_points where tenant_id='\$t'")
    # card 20: retire this version's exhausted FAILED tickets (§15.2.1) so the §15.4 prefix
    # projection_promoted grades can move, and offer the version to §16.3 only when it is not
    # already this family's serving row — projection-serve short-circuits that case itself, so the
    # loop no longer re-offers v1 to itself every 5 s (VisibleSameVersionDeclared, soak25/26/27).
    "${CARGO_TARGET_DIR:-target}"/debug/xtask projection-serve --tenant \$t --workspace \$w --domain $DOMAIN \
      --projection-kind $PKIND --version $PVER \
      --retire-failed distill_failed,no_visible_memory_record \
      >> $EV/soak-projection.log 2>&1
  done
  sleep 5
done
EOF
chmod +x $S/soak_probe_rw.sh $S/soak_probe_pw.sh $S/soak_project.sh \
         $S/soak_chaos_rw.sh $S/soak_chaos_ds.sh $S/soak_chaos_cw.sh

# macOS XProtect assesses each freshly linked binary on FIRST exec (~98 s, strictly serial).
# Warm every binary the soak launches or probes BEFORE the timed window; never widen a
# production timeout to absorb this (docs/ops/soak.md §5).
step soak_warmup
W0=$(date +%s)
for b in humaux-gateway humaux-retrieval-worker humaux-private-worker humaux-consolidation-worker xtask; do
  env -i "${CARGO_TARGET_DIR:-target}"/debug/$b --help >/dev/null 2>&1
done
echo "soak warm-up: $(( $(date +%s) - W0 ))s for 5 binaries (XProtect first-exec assessment)" | tee -a $EV/rehearsal.log

# resident derived workers for the soak window (both tenant-free, ADR-0036).
# Same $DS_ENV / $CW_ENV the chaos restarts use — one definition, no drift.
( eval "$DS_ENV"
  set -a; source /Volumes/data/viral-skill-eval/.env; set +a
  exec "${CARGO_TARGET_DIR:-target}"/debug/humaux-private-worker --distill-serve > $EV/soak-distill.log 2>&1 ) &
SOAK_DS_PID=$!; own_pid ds $SOAK_DS_PID
( eval "$CW_ENV"
  exec "${CARGO_TARGET_DIR:-target}"/debug/humaux-consolidation-worker --serve > $EV/soak-consolidation.log 2>&1 ) &
SOAK_CW_PID=$!; own_pid cw $SOAK_CW_PID
touch $S/soak_project.on; $S/soak_project.sh & SOAK_PROJ_PID=$!

cargo run -q -p xtask -- soak \
  --gateway-url http://127.0.0.1:8080/mcp \
  --tenant "$TENANT:$WS:BEARER_A" --tenant "$TENANT_B:$WS_B:BEARER_B" \
  --sessions-per-tenant ${SOAK_SESSIONS:-2} \
  --duration-secs $SOAK_SECS --drain-secs ${SOAK_DRAIN:-150} --think-ms ${SOAK_THINK_MS:-500} \
  --probe-every-secs 15 \
  --probe-cmd "curl -fsS -o /dev/null http://127.0.0.1:8080/readyz" \
  --probe-cmd "$S/soak_probe_rw.sh" \
  --probe-cmd "$S/soak_probe_pw.sh" \
  --chaos-every-secs ${SOAK_CHAOS_SECS:-90} \
  --chaos-cmd "$S/soak_chaos_rw.sh" --chaos-cmd "$S/soak_chaos_ds.sh" --chaos-cmd "$S/soak_chaos_cw.sh" \
  --lease-secs 120 --max-rss-mib ${SOAK_MAX_RSS_MIB:-2048} --max-db-connections ${SOAK_MAX_CONNS:-120} \
  --report $EV/soak-report.json 2>&1 | tee -a $EV/rehearsal.log
SOAK_RC=${pipestatus[1]}   # zsh: the tee at the end of the pipe is NOT the verdict
rm -f $S/soak_project.on
kill $SOAK_PROJ_PID 2>/dev/null              # the projection loop is this script's own zsh child
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

# ---------- 7. stop ----------
step stop
# Through the pidfiles, not the variables: the soak's chaos hook restarts the retrieval worker,
# so $RW_PID is stale by here. own_signal confirms `ps -o comm=` before signalling anything.
own_signal $S/gw.pid humaux-gateway TERM 30
own_signal $S/rw.pid humaux-retrieval-worker TERM 30
own_signal $S/pw.pid humaux-private-worker TERM 30
sleep 1
echo "done; tenant kept for inspection: $TENANT (teardown: cargo run -q -p xtask -- e2e-seed --teardown $TENANT)" | tee -a $EV/rehearsal.log
# Card 21 fix pass: the assertion table IS the verdict. Without this the script exited 0 whatever
# the assertions said, so "the rehearsal ran" and "the rehearsal passed" were indistinguishable
# from the outside — which is exactly how a table of unrun assertions got reported as delivered.
echo "REHEARSAL VERDICT: $A_OK passed, $A_BAD failed" | tee -a $EV/rehearsal.log
[ "${A_BAD:-1}" -eq 0 ] || exit 1
