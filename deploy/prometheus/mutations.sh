#!/bin/sh
# mutations.sh [src-dir [rows-file]] — the §80.1 red→green record for the loaded rules (ADR-0061 D-E).
#
# Each row substitutes exactly one token in one rule file of a fresh copy of src-dir (default: this
# directory) and requires the unit tests to catch it. A row counts as red ONLY when:
#   (a) the token occurs exactly once and the substituted file differs (`cmp`), else "did not apply";
#   (b) the pinned `promtool check rules` accepts the mutated file, else "breaks syntax" (a broken
#       mutation is not a red: promtool exits 1 for a load error and for a failed test alike, W6);
#   (c) the pinned `promtool test rules` exits 1 AND prints `alertname: <expected>, time:` (W6).
# Before any row: the promtool pin is checked, and the unmutated tree must pass test-rules.sh in the
# same copy layout ("baseline red in copy" otherwise), so a path or layout error is never read as red.
# Rows: `id|rule-file|from|to|expected-alertname`; rows-file overrides the built-in 33 (selftest.sh);
# every `absent(...)` branch of CoreMetricAbsent, HealthGaugesAbsent, MaintenanceCountersAbsent and PartitionHorizonAbsent
# has its own row.
#
# Exit codes: 0 = every row printed `mutation=<id> red`; 1 = a row did not apply, broke syntax or
#             was NOT-DETECTED, or the baseline failed; 2 = not_applicable (promtool pin unset);
#             1 also for a pin mismatch (from pinned-tool.sh).
set -u
here=$(cd "$(dirname "$0")" && pwd)
src=$(cd "${1:-$here}" && pwd) || exit 1
rows=${2:-}

sh "$src/pinned-tool.sh" promtool --version >/dev/null
rc=$?
if [ "$rc" -ne 0 ]; then
  echo "mutations: promtool pin not usable (exit $rc); no mutation tried"
  exit "$rc"
fi

tmp=$(mktemp -d "${TMPDIR:-/tmp}/c34m.XXXXXX") || exit 1
trap 'rm -rf "$tmp"' EXIT INT TERM

mkdir "$tmp/base" && cp -R "$src/." "$tmp/base/"
if ! sh "$tmp/base/test-rules.sh" >"$tmp/base.log" 2>&1; then
  cat "$tmp/base.log"
  echo "mutations: fail — baseline red in copy"
  exit 1
fi

if [ -z "$rows" ]; then
  rows="$tmp/rows"
  cat >"$rows" <<'ROWS'
inv1|invariants.rules.yml|sum(rate(degrade_total[5m])) > 0|sum(rate(degrade_total[5m])) > 1|INV-1
inv1s|invariants.rules.yml|absent_over_time(humaux_retrieval_requests_total[5m])|absent_over_time(queries_total[5m])|INV-1
inv1p|invariants.rules.yml|absent_over_time(humaux_retrieval_requests_total[5m])|absent(vector(1))|INV-1
inv2|invariants.rules.yml|sum(rate(data_disclosures_finalized_total[5m])) == 0|sum(rate(data_disclosures_finalized_total[5m])) > 0|INV-2
inv2m|invariants.rules.yml|degrade_total{code=~"Egress.*"}|degrade_total{code=~".*"}|INV-2
inv2p|invariants.rules.yml|absent_over_time(data_disclosures_finalized_total[5m])|absent(vector(1))|INV-2
inv3|invariants.rules.yml|{age_bucket="gt_60s"}|{age_bucket="le_60s"}|INV-3
inv4|invariants.rules.yml|> 0.4|> 0.6|INV-4
core|alerts.rules.yml|absent(degrade_total)|absent(vector(1))|CoreMetricAbsent
core_rr|alerts.rules.yml|absent(humaux_retrieval_requests_total)|absent(vector(1))|CoreMetricAbsent
core_mcp|alerts.rules.yml|absent(humaux_mcp_requests_total)|absent(vector(1))|CoreMetricAbsent
lag|alerts.rules.yml|for: 10m|for: 0m|ProjectionLagExceedsSLO
dead|alerts.rules.yml|delta(jobs_dead[15m]) > 0|delta(jobs_dead[15m]) > 2|QueueDeadLetterIncrease
backup|alerts.rules.yml|93600|187200|BackupFailure
health|alerts.rules.yml|absent(data_disclosures_reserved_unfinalized)|absent(vector(1))|HealthGaugesAbsent
health_dead|alerts.rules.yml|absent(jobs_dead)|absent(vector(1))|HealthGaugesAbsent
health_lag|alerts.rules.yml|absent(projection_lag_events)|absent(vector(1))|HealthGaugesAbsent
watchdog|alerts.rules.yml|vector(1)|absent(vector(1))|Watchdog
distill_and|alerts.rules.yml|> 0 and increase(private_distill_outputs_total[1h]) == 0|> 0|DistillNoOutput
distill_flat|alerts.rules.yml|private_distill_outputs_total[1h]) == 0|private_distill_outputs_total[1h]) > 0|DistillNoOutput
maint_label|alerts.rules.yml|maintenance_task_runs_total{outcome="failed"}|maintenance_task_runs_total|MaintenanceTaskFailing
maint_cmp|alerts.rules.yml|{outcome="failed"}[1h]) > 0|{outcome="failed"}[1h]) < 0|MaintenanceTaskFailing
maint_absent|alerts.rules.yml|absent(maintenance_task_runs_total)|absent(vector(1))|MaintenanceCountersAbsent
horizon_le1|alerts.rules.yml|(partition_horizon_months) <= 1|(partition_horizon_months) < 1|PartitionHorizonShort
horizon_min|alerts.rules.yml|min by (table) (partition_horizon_months) <= 1|partition_horizon_months <= 1|PartitionHorizonShort
horizon_le0|alerts.rules.yml|(partition_horizon_months) <= 0|(partition_horizon_months) < 0|PartitionHorizonExhausted
horizon_absent|alerts.rules.yml|absent(partition_horizon_months)|absent(vector(1))|PartitionHorizonAbsent
not_offsite|alerts.rules.yml|label_replace(vector(0),|label_replace(vector(0) < 0,|BackupNotOffsite
drill|alerts.rules.yml|691200|1382400|RestoreDrillFailure
wal_failing|alerts.rules.yml|wal_archive_failing > 0|wal_archive_failing > 1|WalArchiveFailing
budget_low|alerts.rules.yml|backup_budget_headroom_bytes < 0|backup_budget_headroom_bytes > 0|BackupBudgetLow
disk_free_low|alerts.rules.yml|8053063680|0|DiskFreeLow
admission|alerts.rules.yml|increase(admission_rejected_total[5m]) > 0|increase(admission_rejected_total[5m]) < 0|AdmissionRejected
ROWS
fi
# ponytail: `dead` uses `> 2`, not `> 1`: measured, delta() extrapolates a 0→1 step over [15m] at 1m
# samples to 1.05, so `> 1` still fires on one new DEAD job and is not a behaviour change.

fail=0; red=0; total=0
while IFS='|' read -r id file from to alert; do
  [ -n "$id" ] || continue
  total=$((total + 1))
  d="$tmp/m.$id"
  mkdir "$d" && cp -R "$src/." "$d/"
  f="$d/$file"
  n=$(grep -oF -- "$from" "$f" | wc -l | tr -d ' ')
  if [ "$n" -ne 1 ]; then
    if [ "$n" -eq 0 ]; then
      echo "mutation=$id fail — mutation $id did not apply ('$from' not found in $file)"
    else
      echo "mutation=$id fail — '$from' occurs $n times in $file, need exactly 1"
    fi
    fail=1; continue
  fi
  re=$(printf '%s' "$from" | sed -e 's/[]\/$*.^[]/\\&/g')
  rep=$(printf '%s' "$to" | sed -e 's/[\/&]/\\&/g')
  sed -e "s/$re/$rep/" "$src/$file" >"$f"
  if cmp -s "$src/$file" "$f"; then
    echo "mutation=$id fail — mutation $id did not apply (file unchanged)"
    fail=1; continue
  fi
  if ! sh "$src/pinned-tool.sh" promtool check rules "$f" >"$d.check" 2>&1; then
    cat "$d.check"
    echo "mutation=$id fail — mutation $id breaks syntax"
    fail=1; continue
  fi
  sh "$src/pinned-tool.sh" promtool test rules "$d"/tests/*.yml >"$d.out" 2>&1
  rc=$?
  if [ "$rc" -eq 1 ] && grep -qF "alertname: $alert, time:" "$d.out"; then
    echo "mutation=$id red ($alert: $(grep -F "alertname: $alert, time:" "$d.out" | head -1 | sed 's/^ *//'))"
    red=$((red + 1))
  else
    echo "mutation=$id NOT-DETECTED (promtool test rules exit $rc, no 'alertname: $alert, time:' line)"
    fail=1
  fi
done <"$rows"

echo "mutations: red=$red/$total"
[ "$fail" -eq 0 ] && [ "$total" -gt 0 ] || exit 1
