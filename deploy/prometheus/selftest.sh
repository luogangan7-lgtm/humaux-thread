#!/bin/sh
# selftest.sh — the `promtool_selftest` gate (ADR-0061 D-E, D-G, D-I): proves the gate scripts in
# this directory can go red for the right reason. Every fixture is derived at run time from the real
# files by one substitution whose application is proven with `cmp`, so fixtures cannot drift.
#   T-E1 promtool pin unset        → mutations.sh exits 2 and prints no `red`
#   T-E2 a row matching nothing    → mutations.sh exits 1, "did not apply"
#   T-E3 a row deleting a `)`      → mutations.sh exits 1, "breaks syntax", no `red`
#   T-E4 a wrong rule_files path   → mutations.sh exits 1, "baseline red in copy"
#   T-E5 bad compose fixtures      → check-compose.sh exits 1 naming each defect (and 0 on the real file),
#                                    including a collector listener off loopback or left to its default
#                                    and an alertmanager.yml cluster listener
#   T-E6 sha256 one digit off      → pinned-tool.sh exits 1, "sha256 mismatch"
#   T-E7 one test block deleted    → test-rules.sh exits 1 naming the under-covered alert (card fault)
#
# Exit codes: 0 = every case behaved as listed; 1 = a case did not (named); 2 = not_applicable
#             (promtool pin, a version pin or docker missing — named by the script that found it).
set -u
here=$(cd "$(dirname "$0")" && pwd)

sh "$here/pinned-tool.sh" promtool --version >/dev/null || exit $?
tmp=$(mktemp -d "${TMPDIR:-/tmp}/c34s.XXXXXX") || exit 1
trap 'rm -rf "$tmp"' EXIT INT TERM
fail=0

# expect <case> <want-rc> <got-rc> <output-file> <must-contain> [must-not-contain-regex]
expect() {
  ok=1
  [ "$3" -eq "$2" ] || ok=0
  grep -qF -- "$5" "$4" || ok=0
  if [ $# -ge 6 ] && grep -qE -- "$6" "$4"; then ok=0; fi
  if [ "$ok" -eq 1 ]; then
    echo "$1 ok (exit $3, '$5')"
  else
    echo "$1 FAIL (exit $3, want $2 with '$5')"; sed 's/^/    /' "$4" | tail -5
    fail=1
  fi
}

# derive <src> <dst> <sed-expr>: one substitution, proven applied.
derive() {
  sed -e "$3" "$1" >"$2"
  if cmp -s "$1" "$2"; then
    echo "selftest: fixture '$3' did not apply to $1"; exit 1
  fi
}

HUMAUX_TEST_PROMTOOL_BIN= sh "$here/mutations.sh" >"$tmp/e1" 2>&1
expect T-E1 2 $? "$tmp/e1" "not_applicable" 'mutation=.* red'

echo 'noop|invariants.rules.yml|token_absent_from_every_rule|x|INV-1' >"$tmp/rows2"
sh "$here/mutations.sh" "$here" "$tmp/rows2" >"$tmp/e2" 2>&1
expect T-E2 1 $? "$tmp/e2" "did not apply" 'mutation=.* red'

echo 'paren|invariants.rules.yml|absent_over_time(humaux_retrieval_requests_total[5m]))|absent_over_time(humaux_retrieval_requests_total[5m])|INV-1' >"$tmp/rows3"
sh "$here/mutations.sh" "$here" "$tmp/rows3" >"$tmp/e3" 2>&1
expect T-E3 1 $? "$tmp/e3" "breaks syntax" 'mutation=.* red'

mkdir "$tmp/e4src" && cp -R "$here/." "$tmp/e4src/"
derive "$here/tests/alerts.test.yml" "$tmp/e4src/tests/alerts.test.yml" 's#\.\./alerts\.rules\.yml#../missing.rules.yml#'
sh "$here/mutations.sh" "$tmp/e4src" >"$tmp/e4" 2>&1
expect T-E4 1 $? "$tmp/e4" "baseline red in copy" 'mutation=.* red'

compose="$here/../compose/observability.yml"
pv="v$HUMAUX_TEST_PROMETHEUS_VERSION"
sh "$here/check-compose.sh" "$compose" >"$tmp/e5ok" 2>&1
expect "T-E5 real file" 0 $? "$tmp/e5ok" "check-compose: pass"
derive "$compose" "$tmp/tag.yml" "s#prom/prometheus:$pv@sha256:[0-9a-f]*#prom/prometheus:$pv#"
sh "$here/check-compose.sh" "$tmp/tag.yml" >"$tmp/e5a" 2>&1
expect "T-E5 tag-only image" 1 $? "$tmp/e5a" "prometheus: image is not name:tag@sha256:<digest>"
derive "$compose" "$tmp/any.yml" 's#--web.listen-address=127.0.0.1:9090#--web.listen-address=0.0.0.0:9090#'
sh "$here/check-compose.sh" "$tmp/any.yml" >"$tmp/e5b" 2>&1
expect "T-E5 0.0.0.0" 1 $? "$tmp/e5b" "0.0.0.0 in rendered config"
derive "$compose" "$tmp/rw.yml" 's#- --storage.tsdb.retention.time=30d#&\
      - --web.enable-remote-write-receiver#'
sh "$here/check-compose.sh" "$tmp/rw.yml" >"$tmp/e5c" 2>&1
expect "T-E5 remote-write receiver" 1 $? "$tmp/e5c" "forbidden flag --web.enable-remote-write-receiver"
derive "$compose" "$tmp/gossip.yml" '/- --cluster.listen-address=$/d'
sh "$here/check-compose.sh" "$tmp/gossip.yml" >"$tmp/e5d" 2>&1
expect "T-E5 cluster listener" 1 $? "$tmp/e5d" "no empty --cluster.listen-address="
derive "$compose" "$tmp/ver.yml" "s#prom/prometheus:$pv@#prom/prometheus:v0.0.0@#"
sh "$here/check-compose.sh" "$tmp/ver.yml" >"$tmp/e5e" 2>&1
expect "T-E5 version != pin" 1 $? "$tmp/e5e" "prometheus: tag v0.0.0 != pinned $pv"
derive "$here/otel-collector.yml" "$tmp/otel-any.yml" 's#endpoint: 127.0.0.1:4317#endpoint: 0.0.0.0:4317#'
sh "$here/check-compose.sh" "$compose" "$tmp/otel-any.yml" >"$tmp/e5f" 2>&1
expect "T-E5 collector receiver 0.0.0.0" 1 $? "$tmp/e5f" "endpoint 0.0.0.0:4317 is not 127.0.0.1"
derive "$here/otel-collector.yml" "$tmp/otel-tel.yml" 's#host: 127.0.0.1#host: 0.0.0.0#'
sh "$here/check-compose.sh" "$compose" "$tmp/otel-tel.yml" >"$tmp/e5g" 2>&1
expect "T-E5 collector telemetry 0.0.0.0" 1 $? "$tmp/e5g" "host 0.0.0.0 is not 127.0.0.1"
derive "$here/otel-collector.yml" "$tmp/otel-default.yml" '/endpoint: 127.0.0.1:4318/d'
sh "$here/check-compose.sh" "$compose" "$tmp/otel-default.yml" >"$tmp/e5h" 2>&1
expect "T-E5 collector default endpoint" 1 $? "$tmp/e5h" "2 listener lines"
derive "$here/alertmanager.yml" "$tmp/am-cluster.yml" 's#^  group_by: \[alertname\]#&\
  cluster_listen_address: 127.0.0.1:9094#'
sh "$here/check-compose.sh" "$compose" "$here/otel-collector.yml" "$tmp/am-cluster.yml" >"$tmp/e5i" 2>&1
expect "T-E5 alertmanager cluster listener" 1 $? "$tmp/e5i" "listener or cluster setting: cluster_listen_address"

sha=$HUMAUX_TEST_PROMTOOL_SHA256
if [ "$(printf %.1s "$sha")" = 0 ]; then digit=1; else digit=0; fi
HUMAUX_TEST_PROMTOOL_SHA256="$digit${sha#?}" sh "$here/pinned-tool.sh" promtool --version >"$tmp/e6" 2>&1
expect T-E6 1 $? "$tmp/e6" "sha256 mismatch for promtool"

mkdir "$tmp/e7src" && cp -R "$here/." "$tmp/e7src/"
derive "$here/tests/alerts.test.yml" "$tmp/e7src/tests/alerts.test.yml" '/# dead silent/,/exp_alerts: \[\]/d'
sh "$tmp/e7src/test-rules.sh" >"$tmp/e7" 2>&1
expect T-E7 1 $? "$tmp/e7" "alert QueueDeadLetterIncrease has 1 alertname checks"

[ "$fail" -eq 0 ] || { echo "selftest: fail"; exit 1; }
echo "selftest: pass — T-E1..T-E7"
