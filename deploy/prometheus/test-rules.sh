#!/bin/sh
# test-rules.sh — the `promtool_test_rules` gate (ADR-0061 D-E).
#
# 1. Runs the pinned `promtool test rules` over every tests/*.yml (rule_files are relative to tests/).
# 2. Coverage: every `- alert: X` in *.rules.yml must appear as `alertname: X` at least twice across
#    tests/*.yml (a firing and a silent check). promtool alone stays green when a test block is
#    deleted; this grep is what turns that deletion red (card 34 fault "delete one rule test").
#
# Exit codes: 0 = all tests pass and every alert is covered; 1 = a test failed or an alert is
#             under-covered (named); 2 = not_applicable (promtool pin unset, from pinned-tool.sh).
set -u
here=$(cd "$(dirname "$0")" && pwd)

sh "$here/pinned-tool.sh" promtool test rules "$here"/tests/*.yml
rc=$?
[ "$rc" -eq 0 ] || exit "$rc"

fail=0
for a in $(sed -n 's/^[[:space:]]*- alert:[[:space:]]*\([^[:space:]]*\).*$/\1/p' "$here"/*.rules.yml); do
  n=$(cat "$here"/tests/*.yml | grep -cE "^[[:space:]]*alertname:[[:space:]]*${a}[[:space:]]*$")
  if [ "$n" -lt 2 ]; then
    echo "test-rules: fail — alert $a has $n alertname checks in tests/*.yml, need >= 2 (firing + silent)"
    fail=1
  fi
done
[ "$fail" -eq 0 ] || exit 1
echo "test-rules: pass — every alert in *.rules.yml has >= 2 checks"
