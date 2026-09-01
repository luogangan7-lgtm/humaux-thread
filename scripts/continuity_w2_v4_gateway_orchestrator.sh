#!/bin/sh
# Phase 9 Continuity W2 v4.2 Stage G test/evidence owner.
set -eu
umask 077
LC_ALL=C
export LC_ALL

test_name='mcp_application::tests::trusted_multi_workspace_omission_resolves_exact_project_parent_not_first'
frozen_sha='da206cbbc266ca8760599300ce78a0d09410c27ade55e8a0ac3509d6f233484c'
authority_sha='64c138228902d3839174bc499200289bb9d0e1f7eae8443f00b3453fa80b0e4c'
adjudication_sha='315eaf6ab2d7144a20020e7a37c0c52792ef027ec24743499a27d9e38c809762'
review_sha='04279fdf7e08d88ffa64467b87a22ae56299c65a1e5e903065791a83f11965a1'

sha256() { shasum -a 256 "$1" | awk '{print $1}'; }
file_size() { wc -c <"$1" | tr -d '[:space:]'; }

qualify_one_pass_log() {
    log=$1
    [ "$(grep -Fxc 'running 1 test' "$log" || true)" = 1 ] &&
        [ "$(grep -Fxc "test $test_name ... ok" "$log" || true)" = 1 ] &&
        [ "$(grep -Ec '^test [^ ]+ \.\.\. (ok|FAILED)$' "$log" || true)" = 1 ] &&
        [ "$(grep -Ec '^test result: ok\. 1 passed; 0 failed; 0 ignored; 0 measured; [0-9]+ filtered out; finished in [0-9.]+s$' "$log" || true)" = 1 ] &&
        ! grep -Eq 'panicked at|^test result: FAILED\.|^error: test failed' "$log"
}

qualify_one_expected_failure_log() {
    log=$1
    [ "$(grep -Fxc 'running 1 test' "$log" || true)" = 1 ] &&
        [ "$(grep -Fxc "test $test_name ... FAILED" "$log" || true)" = 1 ] &&
        [ "$(grep -Ec '^test [^ ]+ \.\.\. (ok|FAILED)$' "$log" || true)" = 1 ] &&
        [ "$(grep -Fxc 'W2_EXPECTED_SAME_KEY_429:v1' "$log" || true)" = 1 ] &&
        [ "$(grep -Fc 'W2_EXPECTED_SAME_KEY_429_PANIC:v1' "$log" || true)" = 1 ] &&
        [ "$(grep -Fc 'panicked at' "$log" || true)" = 1 ] &&
        [ "$(grep -Ec '^test result: FAILED\. 0 passed; 1 failed; 0 ignored; 0 measured; [0-9]+ filtered out; finished in [0-9.]+s$' "$log" || true)" = 1 ]
}

manifest_syntax_check() {
    awk -F '\t' '
        BEGIN { schema = 0; bad = 0 }
        $1 == "schema" {
            if (NF != 3 || $2 != "continuity-w2-v4.2-gateway-orchestrator" || $3 != "1" || schema++) bad = 1
            next
        }
        $1 == "binding" {
            if (NF != 3 || $2 !~ /^[a-z0-9_]+$/ || seen_binding[$2]++) bad = 1
            next
        }
        $1 == "artifact" {
            if (NF != 4 || $2 !~ /^[A-Za-z0-9][A-Za-z0-9._-]*$/ || seen_path[$2]++ ||
                $3 !~ /^[0-9]+$/ || length($4) != 64 || $4 !~ /^[0-9a-f]+$/) bad = 1
            next
        }
        { bad = 1 }
        END { exit(bad || schema != 1) }
    ' "$1"
}

verify_manifest_bindings() {
    manifest=$1 expected=$2
    actual=$(awk -F '\t' '$1 == "binding" { print $2 "\t" $3 }' "$manifest")
    [ "$actual" = "$expected" ]
}

verify_manifest_inventory() {
    manifest=$1 dir=$2 expected=$3
    manifest_syntax_check "$manifest" || return 1
    listed=$(awk -F '\t' '$1 == "artifact" { print $2 }' "$manifest" | sort)
    wanted=$(printf '%s\n' "$expected" | sort)
    [ "$listed" = "$wanted" ] || return 1
    actual=''
    for path in "$dir"/*; do
        [ -e "$path" ] || continue
        name=${path##*/}
        [ "$name" = manifest.txt ] && continue
        [ -f "$path" ] && [ ! -L "$path" ] || return 1
        if [ -n "$actual" ]; then
            actual="$actual
$name"
        else
            actual=$name
        fi
    done
    [ "$actual" = "$wanted" ] || return 1
    tab=$(printf '\t')
    awk -F '\t' '$1 == "artifact" { print $2 "\t" $3 "\t" $4 }' "$manifest" |
        while IFS="$tab" read -r name recorded_size recorded_sha; do
            path=$dir/$name
            [ -f "$path" ] && [ ! -L "$path" ] || exit 1
            [ "$(file_size "$path")" = "$recorded_size" ] || exit 1
            [ "$(sha256 "$path")" = "$recorded_sha" ] || exit 1
        done
}

canonical_audit_line() {
    audit_line_file=$1
    audit_tab=$(printf '\t')
    grep -Eq "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}${audit_tab}[0-9a-f]{64}$" "$audit_line_file"
}

qualify_audit_transition() {
    baseline=$1 post=$2 delta=$3 lost=$4
    comm -13 "$baseline" "$post" >"$delta" || return 1
    comm -23 "$baseline" "$post" >"$lost" || return 1
    baseline_lines=$(wc -l <"$baseline" | tr -d '[:space:]')
    post_lines=$(wc -l <"$post" | tr -d '[:space:]')
    [ "$(wc -l <"$delta" | tr -d '[:space:]')" = 1 ] &&
        [ "$(file_size "$lost")" = 0 ] &&
        [ "$post_lines" -eq "$((baseline_lines + 1))" ] &&
        canonical_audit_line "$delta"
}

relation_created=0
run_uuid=''
artifact_dir=''
pid_a=''
pid_b=''
same_a=''
same_b=''

stop_children() {
    for child in "$pid_a" "$pid_b" "$same_a" "$same_b"; do
        [ -n "$child" ] || continue
        if kill -0 "$child" 2>/dev/null; then
            kill -TERM "$child" 2>/dev/null || true
        fi
        wait "$child" 2>/dev/null || true
    done
    pid_a=''; pid_b=''; same_a=''; same_b=''
}

cleanup_relation() {
    cleanup_reason=$1
    cleanup_expected=$2
    cleanup_tmp=$artifact_dir/cleanup.tsv.tmp
    cleanup_final=$artifact_dir/cleanup.tsv
    cleanup_failed=0
    : >"$cleanup_tmp" || return 1
    printf 'reason\t%s\n' "$cleanup_reason" >>"$cleanup_tmp"
    if [ "$relation_created" -eq 1 ]; then
        if [ -n "$run_uuid" ]; then
            if delete_out=$(db_run "DELETE FROM control.w2_test_request_intervals WHERE run_uuid=:'run_uuid'::uuid"); then
                printf '%s\n' "$delete_out" | grep -Eq '^DELETE [0-9]+$' || cleanup_failed=1
                if [ "$cleanup_expected" != any ] && [ "$delete_out" != "DELETE $cleanup_expected" ]; then
                    cleanup_failed=1
                fi
            else
                delete_out='DELETE_FAILED'
                cleanup_failed=1
            fi
        else
            delete_out='DELETE_NOT_APPLICABLE_NO_RUN_UUID'
        fi
        printf 'delete\t%s\n' "$delete_out" >>"$cleanup_tmp"
        if drop_out=$(db 'DROP TABLE control.w2_test_request_intervals'); then
            [ "$drop_out" = 'DROP TABLE' ] || cleanup_failed=1
        else
            drop_out='DROP_FAILED'
            cleanup_failed=1
        fi
        printf 'drop\t%s\n' "$drop_out" >>"$cleanup_tmp"
        if absent_out=$(db "SELECT to_regclass('control.w2_test_request_intervals') IS NULL"); then
            [ "$absent_out" = t ] || cleanup_failed=1
        else
            absent_out='ABSENCE_CHECK_FAILED'
            cleanup_failed=1
        fi
        printf 'absent\t%s\n' "$absent_out" >>"$cleanup_tmp"
    else
        printf 'delete\tNOT_CREATED\ndrop\tNOT_CREATED\nabsent\tNOT_CREATED\n' >>"$cleanup_tmp"
    fi
    if [ "$cleanup_failed" -eq 0 ]; then
        printf 'result\tCLEANUP_OK\n' >>"$cleanup_tmp"
        mv "$cleanup_tmp" "$cleanup_final" || return 1
        relation_created=0
        return 0
    fi
    printf 'result\tCLEANUP_FAILED\n' >>"$cleanup_tmp" || true
    mv "$cleanup_tmp" "$artifact_dir/cleanup-failed.tsv" 2>/dev/null || true
    return 1
}

on_exit() {
    primary_status=$1
    trap - EXIT HUP INT TERM
    stop_children
    cleanup_status=0
    if [ "$relation_created" -eq 1 ]; then
        cleanup_relation failure any || cleanup_status=$?
    fi
    if [ "$cleanup_status" -ne 0 ]; then
        echo 'CLEANUP_FAILED' >&2
        exit 70
    fi
    exit "$primary_status"
}

on_signal() {
    signal_status=$1
    trap - HUP INT TERM
    exit "$signal_status"
}

self_test() {
    self_tmp=$(mktemp -d /Volumes/data/.humaux-w2-gateway-self-test.XXXXXX)
    trap 'rm -rf "$self_tmp"' EXIT
    cat >"$self_tmp/pass.log" <<EOF
running 1 test
test $test_name ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 117 filtered out; finished in 0.01s
EOF
    qualify_one_pass_log "$self_tmp/pass.log"
    sed 's/running 1 test/running 0 tests/' "$self_tmp/pass.log" >"$self_tmp/zero.log"
    ! qualify_one_pass_log "$self_tmp/zero.log"
    { cat "$self_tmp/pass.log"; printf 'test unrelated ... ok\n'; } >"$self_tmp/extra.log"
    ! qualify_one_pass_log "$self_tmp/extra.log"
    { cat "$self_tmp/pass.log"; printf "thread 'unexpected' panicked at source.rs:1:1:\n"; } >"$self_tmp/panic.log"
    ! qualify_one_pass_log "$self_tmp/panic.log"
    cat >"$self_tmp/fail.log" <<EOF
running 1 test
W2_EXPECTED_SAME_KEY_429:v1
thread '$test_name' panicked at source.rs:1:1:
W2_EXPECTED_SAME_KEY_429_PANIC:v1
test $test_name ... FAILED
test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 117 filtered out; finished in 0.01s
EOF
    qualify_one_expected_failure_log "$self_tmp/fail.log"
    { cat "$self_tmp/fail.log"; printf 'W2_EXPECTED_SAME_KEY_429:v1\n'; } >"$self_tmp/double.log"
    ! qualify_one_expected_failure_log "$self_tmp/double.log"

    mkdir "$self_tmp/inventory"
    printf 'alpha\n' >"$self_tmp/inventory/a.log"
    : >"$self_tmp/inventory/b.set"
    a_size=$(file_size "$self_tmp/inventory/a.log")
    a_sha=$(sha256 "$self_tmp/inventory/a.log")
    b_size=$(file_size "$self_tmp/inventory/b.set")
    b_sha=$(sha256 "$self_tmp/inventory/b.set")
    cat >"$self_tmp/inventory/manifest.txt" <<EOF
schema	continuity-w2-v4.2-gateway-orchestrator	1
binding	fixture	self-test
artifact	a.log	$a_size	$a_sha
artifact	b.set	$b_size	$b_sha
EOF
    verify_manifest_bindings "$self_tmp/inventory/manifest.txt" "fixture	self-test"
    verify_manifest_inventory "$self_tmp/inventory/manifest.txt" "$self_tmp/inventory" 'a.log
b.set'
    printf 'mutation' >>"$self_tmp/inventory/a.log"
    ! verify_manifest_inventory "$self_tmp/inventory/manifest.txt" "$self_tmp/inventory" 'a.log
b.set'
    printf 'alpha\n' >"$self_tmp/inventory/a.log"
    cp "$self_tmp/inventory/manifest.txt" "$self_tmp/valid-manifest"
    printf 'artifact\ta.log\t%s\t%s\n' "$a_size" "$a_sha" >>"$self_tmp/inventory/manifest.txt"
    ! manifest_syntax_check "$self_tmp/inventory/manifest.txt"
    mv "$self_tmp/valid-manifest" "$self_tmp/inventory/manifest.txt"
    sed '/binding.fixture/d' "$self_tmp/inventory/manifest.txt" >"$self_tmp/missing-binding-manifest"
    ! verify_manifest_bindings "$self_tmp/missing-binding-manifest" "fixture	self-test"
    sed '/artifact.b.set/d' "$self_tmp/inventory/manifest.txt" >"$self_tmp/omitted-manifest"
    ! verify_manifest_inventory "$self_tmp/omitted-manifest" "$self_tmp/inventory" 'a.log
b.set'
    sed 's/artifact.a\.log/artifact...\/a.log/' "$self_tmp/inventory/manifest.txt" >"$self_tmp/malformed-path-manifest"
    ! manifest_syntax_check "$self_tmp/malformed-path-manifest"
    ln -s a.log "$self_tmp/inventory/z-link"
    ! verify_manifest_inventory "$self_tmp/inventory/manifest.txt" "$self_tmp/inventory" 'a.log
b.set'
    rm "$self_tmp/inventory/z-link"
    mkdir "$self_tmp/prepublish"
    printf 'schema\tcontinuity-w2-v4.2-gateway-orchestrator\t1\n' >"$self_tmp/prepublish/manifest.tmp"
    [ ! -e "$self_tmp/prepublish/manifest.txt" ]
    ! grep -Fq 'QUALIFIED_SUCCESS' "$self_tmp/prepublish/manifest.tmp"

    row_a='00000000-0000-0000-0000-000000000001'
    row_b='00000000-0000-0000-0000-000000000002'
    row_c='00000000-0000-0000-0000-000000000003'
    hash_a='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
    hash_b='bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'
    hash_c='cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc'
    printf '%s\t%s\n' "$row_a" "$hash_a" >"$self_tmp/baseline.set"
    { cat "$self_tmp/baseline.set"; printf '%s\t%s\n' "$row_b" "$hash_b"; } >"$self_tmp/post.set"
    qualify_audit_transition "$self_tmp/baseline.set" "$self_tmp/post.set" "$self_tmp/delta.set" "$self_tmp/lost.set"
    printf '%s\t%s\n' "$row_b" "$hash_c" >"$self_tmp/wrong-readback.set"
    [ "$(cat "$self_tmp/wrong-readback.set")" != "$(cat "$self_tmp/delta.set")" ]
    printf '%s\t%s\n' "$row_a" "$hash_b" >"$self_tmp/changed.set"
    ! qualify_audit_transition "$self_tmp/baseline.set" "$self_tmp/changed.set" "$self_tmp/changed-delta.set" "$self_tmp/changed-lost.set"
    : >"$self_tmp/lost-post.set"
    ! qualify_audit_transition "$self_tmp/baseline.set" "$self_tmp/lost-post.set" "$self_tmp/lost-delta.set" "$self_tmp/lost-lost.set"
    { cat "$self_tmp/post.set"; printf '%s\t%s\n' "$row_c" "$hash_c"; } >"$self_tmp/two-post.set"
    ! qualify_audit_transition "$self_tmp/baseline.set" "$self_tmp/two-post.set" "$self_tmp/two-delta.set" "$self_tmp/two-lost.set"

    cleanup_fixture=$self_tmp/cleanup-good
    mkdir "$cleanup_fixture"
    if (
        artifact_dir=$cleanup_fixture
        relation_created=1
        run_uuid='018f0000-0000-7000-8000-000000000001'
        db_run() { printf 'DELETE 2\n'; }
        db() { case "$1" in DROP*) printf 'DROP TABLE\n' ;; *) printf 't\n' ;; esac; }
        trap 'on_exit $?' EXIT
        exit 23
    ); then cleanup_rc=0; else cleanup_rc=$?; fi
    [ "$cleanup_rc" -eq 23 ]
    grep -Fqx 'result	CLEANUP_OK' "$cleanup_fixture/cleanup.tsv"

    cleanup_fixture=$self_tmp/cleanup-signal
    mkdir "$cleanup_fixture"
    if (
        artifact_dir=$cleanup_fixture
        relation_created=1
        run_uuid='018f0000-0000-7000-8000-000000000001'
        db_run() { printf 'DELETE 2\n'; }
        db() { case "$1" in DROP*) printf 'DROP TABLE\n' ;; *) printf 't\n' ;; esac; }
        trap 'on_exit $?' EXIT
        on_signal 143
    ); then cleanup_rc=0; else cleanup_rc=$?; fi
    [ "$cleanup_rc" -eq 143 ]
    grep -Fqx 'result	CLEANUP_OK' "$cleanup_fixture/cleanup.tsv"

    cleanup_fixture=$self_tmp/cleanup-bad-drop
    mkdir "$cleanup_fixture"
    if (
        artifact_dir=$cleanup_fixture
        relation_created=1
        run_uuid='018f0000-0000-7000-8000-000000000001'
        db_run() { printf 'DELETE 2\n'; }
        db() { case "$1" in DROP*) return 1 ;; *) printf 't\n' ;; esac; }
        trap 'on_exit $?' EXIT
        exit 23
    ) 2>/dev/null; then cleanup_rc=0; else cleanup_rc=$?; fi
    [ "$cleanup_rc" -eq 70 ]
    grep -Fqx 'result	CLEANUP_FAILED' "$cleanup_fixture/cleanup-failed.tsv"

    cleanup_fixture=$self_tmp/cleanup-bad-delete
    mkdir "$cleanup_fixture"
    if (
        artifact_dir=$cleanup_fixture
        relation_created=1
        run_uuid='018f0000-0000-7000-8000-000000000001'
        db_run() { return 1; }
        db() { case "$1" in DROP*) printf 'DROP TABLE\n' ;; *) printf 't\n' ;; esac; }
        trap 'on_exit $?' EXIT
        exit 23
    ) 2>/dev/null; then cleanup_rc=0; else cleanup_rc=$?; fi
    [ "$cleanup_rc" -eq 70 ]
    grep -Fqx 'result	CLEANUP_FAILED' "$cleanup_fixture/cleanup-failed.tsv"

    rm -rf "$self_tmp"
    trap - EXIT
    echo 'SELF_TEST_OK'
}

case ${1-} in
    --self-test) [ "$#" -eq 1 ] || exit 64; self_test; exit 0 ;;
    '') ;;
    *) echo 'usage: continuity_w2_v4_gateway_orchestrator.sh [--self-test]' >&2; exit 64 ;;
esac

repo_root=$(CDPATH= cd "$(dirname "$0")/.." && pwd)
cd "$repo_root"
source_file='bins/gateway/src/mcp_application.rs'
frozen_file='bins/gateway/tests/continuity_get.rs'

require_nonempty() {
    [ -n "$1" ] || { echo "missing required environment variable: $2" >&2; exit 64; }
}
require_nonempty "${HUMAUX_TEST_PG_DSN-}" HUMAUX_TEST_PG_DSN
require_nonempty "${HUMAUX_CONTINUITY_W2_GATEWAY_TEST_BINARY-}" HUMAUX_CONTINUITY_W2_GATEWAY_TEST_BINARY
require_nonempty "${HUMAUX_CONTINUITY_W2_GATEWAY_BINARY_SHA256-}" HUMAUX_CONTINUITY_W2_GATEWAY_BINARY_SHA256
require_nonempty "${HUMAUX_CONTINUITY_W2_GATEWAY_SOURCE_SHA256-}" HUMAUX_CONTINUITY_W2_GATEWAY_SOURCE_SHA256
require_nonempty "${HUMAUX_CONTINUITY_W2_ARTIFACT_DIR-}" HUMAUX_CONTINUITY_W2_ARTIFACT_DIR

PSQL=${PSQL:-psql}
binary=$HUMAUX_CONTINUITY_W2_GATEWAY_TEST_BINARY
artifact_dir=$HUMAUX_CONTINUITY_W2_ARTIFACT_DIR
tab_character=$(printf '\t')
newline_character='
'
case "$binary$artifact_dir" in
    *"$tab_character"*|*"$newline_character"*) echo 'binary and artifact paths must not contain TAB or newline' >&2; exit 64 ;;
esac
command -v "$PSQL" >/dev/null 2>&1 || { echo 'psql is unavailable' >&2; exit 64; }
command -v shasum >/dev/null 2>&1 || { echo 'shasum is unavailable' >&2; exit 64; }
[ -x "$binary" ] || { echo 'gateway test binary is not executable' >&2; exit 64; }
[ ! -e "$artifact_dir" ] || { echo 'artifact directory must not already exist' >&2; exit 64; }

source_sha=$(sha256 "$source_file")
binary_sha=$(sha256 "$binary")
frozen_actual=$(sha256 "$frozen_file")
[ "$source_sha" = "$HUMAUX_CONTINUITY_W2_GATEWAY_SOURCE_SHA256" ] || { echo 'gateway source hash binding mismatch' >&2; exit 65; }
[ "$binary_sha" = "$HUMAUX_CONTINUITY_W2_GATEWAY_BINARY_SHA256" ] || { echo 'gateway binary hash binding mismatch' >&2; exit 65; }
[ "$frozen_actual" = "$frozen_sha" ] || { echo 'frozen witness hash mismatch' >&2; exit 65; }

mkdir "$artifact_dir"
db() { "$PSQL" -X -v ON_ERROR_STOP=1 -At -d "$HUMAUX_TEST_PG_DSN" -c "$1"; }
db_run() { "$PSQL" -X -v ON_ERROR_STOP=1 -At -d "$HUMAUX_TEST_PG_DSN" -v run_uuid="$run_uuid" -c "$1"; }
db_audit() { "$PSQL" -X -v ON_ERROR_STOP=1 -At -d "$HUMAUX_TEST_PG_DSN" -v audit_id="$1" -c "$2"; }

trap 'on_exit $?' EXIT
trap 'on_signal 129' HUP
trap 'on_signal 130' INT
trap 'on_signal 143' TERM

version_num=$(db 'SHOW server_version_num')
case "$version_num" in 18[0-9][0-9][0-9][0-9]) ;; *) echo "fresh PostgreSQL 18 required, got $version_num" >&2; exit 65;; esac
[ "$(db "SELECT to_regclass('control.w2_test_request_intervals') IS NULL")" = t ] || { echo 'barrier relation already exists' >&2; exit 65; }

db "CREATE TABLE control.w2_test_request_intervals (
      run_uuid uuid NOT NULL,
      side text NOT NULL CHECK (side IN ('A','B')),
      ready_at timestamptz NOT NULL,
      request_start_at timestamptz,
      request_end_at timestamptz,
      PRIMARY KEY (run_uuid, side),
      CHECK (request_end_at IS NULL OR
             (request_start_at IS NOT NULL AND request_end_at > request_start_at))
    );
    GRANT SELECT, INSERT, UPDATE ON control.w2_test_request_intervals TO role_gateway;" >/dev/null
relation_created=1
run_uuid=$(db 'SELECT uuidv7()::text')
printf '%s\n' "$run_uuid" | grep -Eq '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' || { echo 'database did not produce canonical run UUID' >&2; exit 65; }

audit_rows() {
    db "SELECT lower(audit_event_id::text) || E'\\t' ||
      encode(digest(convert_to(jsonb_build_array(
        audit_event_id, to_char(occurred_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'),
        tenant_id, actor_type, actor_id, action, resource_type, resource_id, result,
        request_id, trace_id, client_ip::text, user_agent_hash, to_jsonb(risk_tags),
        before_fingerprint, after_fingerprint, metadata::jsonb
      )::text,'UTF8'),'sha256'),'hex')
      FROM control.audit_events ORDER BY lower(audit_event_id::text) COLLATE \"C\""
}

audit_row_json() {
    db_audit "$1" "SELECT jsonb_build_object(
      'audit_event_id', lower(audit_event_id::text),
      'row_sha256', encode(digest(convert_to(jsonb_build_array(
        audit_event_id, to_char(occurred_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'),
        tenant_id, actor_type, actor_id, action, resource_type, resource_id, result,
        request_id, trace_id, client_ip::text, user_agent_hash, to_jsonb(risk_tags),
        before_fingerprint, after_fingerprint, metadata::jsonb
      )::text,'UTF8'),'sha256'),'hex'),
      'row', jsonb_build_array(
        audit_event_id, to_char(occurred_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'),
        tenant_id, actor_type, actor_id, action, resource_type, resource_id, result,
        request_id, trace_id, client_ip::text, user_agent_hash, to_jsonb(risk_tags),
        before_fingerprint, after_fingerprint, metadata::jsonb
      )
    )::text FROM control.audit_events WHERE audit_event_id=:'audit_id'::uuid"
}

audit_row_line() {
    db_audit "$1" "SELECT lower(audit_event_id::text) || E'\\t' ||
      encode(digest(convert_to(jsonb_build_array(
        audit_event_id, to_char(occurred_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'),
        tenant_id, actor_type, actor_id, action, resource_type, resource_id, result,
        request_id, trace_id, client_ip::text, user_agent_hash, to_jsonb(risk_tags),
        before_fingerprint, after_fingerprint, metadata::jsonb
      )::text,'UTF8'),'sha256'),'hex')
      FROM control.audit_events WHERE audit_event_id=:'audit_id'::uuid"
}

audit_rows >"$artifact_dir/audit_baseline.set"
baseline_count=$(wc -l <"$artifact_dir/audit_baseline.set" | tr -d '[:space:]')
baseline_sha=$(sha256 "$artifact_dir/audit_baseline.set")
trigger_before=$(db "SELECT count(*)=1 AND bool_and(tgenabled='O') FROM pg_trigger WHERE tgrelid='control.audit_events'::regclass AND tgname='audit_events_reject_mutation' AND NOT tgisinternal")
[ "$trigger_before" = t ] || { echo 'audit mutation trigger is not uniquely enabled' >&2; exit 65; }

run_barrier_child() {
    side=$1
    (
        unset HUMAUX_CONTINUITY_DIRECT_PREAUTH_SAME_KEY RUST_BACKTRACE
        export HUMAUX_REQUIRE_DB=1
        export HUMAUX_CONTINUITY_W2_BARRIER_RUN_ID=$run_uuid
        export HUMAUX_CONTINUITY_W2_BARRIER_SIDE=$side
        "$binary" --exact "$test_name" --nocapture --test-threads=1
    )
}
run_barrier_child A >"$artifact_dir/barrier-a.log" 2>&1 & pid_a=$!
run_barrier_child B >"$artifact_dir/barrier-b.log" 2>&1 & pid_b=$!
status_a=0; wait "$pid_a" || status_a=$?; pid_a=''
status_b=0; wait "$pid_b" || status_b=$?; pid_b=''
[ "$status_a" -eq 0 ] && [ "$status_b" -eq 0 ] || { echo "barrier child status A=$status_a B=$status_b" >&2; exit 1; }
qualify_one_pass_log "$artifact_dir/barrier-a.log" || { echo 'barrier A is not one exact passing test' >&2; exit 1; }
qualify_one_pass_log "$artifact_dir/barrier-b.log" || { echo 'barrier B is not one exact passing test' >&2; exit 1; }

overlap=$(db_run "SELECT count(*) = 2
  AND array_agg(side ORDER BY side) = ARRAY['A','B']::text[]
  AND count(request_start_at) = 2 AND count(request_end_at) = 2
  AND bool_and(request_end_at > request_start_at)
  AND max(request_start_at) < min(request_end_at)
  FROM control.w2_test_request_intervals WHERE run_uuid=:'run_uuid'::uuid")
[ "$overlap" = t ] || { echo 'request interval overlap proof failed' >&2; exit 1; }
db_run "SELECT run_uuid,side,ready_at,request_start_at,request_end_at
    FROM control.w2_test_request_intervals WHERE run_uuid=:'run_uuid'::uuid ORDER BY side" >"$artifact_dir/barrier-readback.tsv"
[ "$(wc -l <"$artifact_dir/barrier-readback.tsv" | tr -d '[:space:]')" = 2 ] || { echo 'barrier readback is not exactly two rows' >&2; exit 1; }

pair_start=$(db 'SELECT clock_timestamp()::text')
run_same_key_child() {
    (
        unset HUMAUX_CONTINUITY_W2_BARRIER_RUN_ID HUMAUX_CONTINUITY_W2_BARRIER_SIDE RUST_BACKTRACE
        export HUMAUX_REQUIRE_DB=1 HUMAUX_CONTINUITY_DIRECT_PREAUTH_SAME_KEY=1
        "$binary" --exact "$test_name" --nocapture --test-threads=1
    )
}
run_same_key_child >"$artifact_dir/same-key-a.log" 2>&1 & same_a=$!
run_same_key_child >"$artifact_dir/same-key-b.log" 2>&1 & same_b=$!
same_status_a=0; wait "$same_a" || same_status_a=$?; same_a=''
same_status_b=0; wait "$same_b" || same_status_b=$?; same_b=''
pair_end=$(db 'SELECT clock_timestamp()::text')
printf 'owner_start\t%s\nowner_end\t%s\n' "$pair_start" "$pair_end" >"$artifact_dir/strict-pair-window.tsv"
case "$same_status_a:$same_status_b" in
    0:101) pass_log=$artifact_dir/same-key-a.log; fail_log=$artifact_dir/same-key-b.log ;;
    101:0) pass_log=$artifact_dir/same-key-b.log; fail_log=$artifact_dir/same-key-a.log ;;
    *) echo "strict same-key requires exactly 0/101, got $same_status_a/$same_status_b" >&2; exit 1;;
esac
qualify_one_pass_log "$pass_log" || { echo 'same-key 200 child is not one exact passing test' >&2; exit 1; }
qualify_one_expected_failure_log "$fail_log" || { echo 'same-key 429 child is not the one fixed expected failure' >&2; exit 1; }

audit_rows >"$artifact_dir/audit_post.set"
post_count=$(wc -l <"$artifact_dir/audit_post.set" | tr -d '[:space:]')
post_sha=$(sha256 "$artifact_dir/audit_post.set")
qualify_audit_transition \
    "$artifact_dir/audit_baseline.set" "$artifact_dir/audit_post.set" \
    "$artifact_dir/audit_delta.set" "$artifact_dir/audit_lost.set" || {
        echo 'audit full row/hash set union/equality failed' >&2
        exit 1
    }
delta_line=$(sed -n '1p' "$artifact_dir/audit_delta.set")
canonical_audit_line "$artifact_dir/audit_delta.set" || { echo 'audit delta framing is not canonical' >&2; exit 1; }
audit_id=${delta_line%%	*}
expected_row_sha=${delta_line#*	}
audit_row_json "$audit_id" >"$artifact_dir/expected_audit_delta.json"
audit_row_line "$audit_id" >"$artifact_dir/expected_audit_delta.readback.tsv"
[ "$(cat "$artifact_dir/expected_audit_delta.readback.tsv")" = "$delta_line" ] || { echo 'registered audit row digest readback mismatch' >&2; exit 1; }
grep -Fq "\"row_sha256\": \"$expected_row_sha\"" "$artifact_dir/expected_audit_delta.json" || { echo 'expected audit JSON digest mismatch' >&2; exit 1; }

audit_valid=$("$PSQL" -X -v ON_ERROR_STOP=1 -At -d "$HUMAUX_TEST_PG_DSN" \
    -v audit_id="$audit_id" -v pair_start="$pair_start" -v pair_end="$pair_end" -c \
    "SELECT count(*)=1 AND bool_and(
      tenant_id='00000000-0000-0000-0000-000000000000'::uuid
      AND action='MCP_REQUEST_DENIED' AND resource_id='protocol' AND result='RATE_LIMITED'
      AND client_ip::text='127.0.0.1'
      AND occurred_at BETWEEN :'pair_start'::timestamptz AND :'pair_end'::timestamptz)
     FROM control.audit_events WHERE audit_event_id=:'audit_id'::uuid")
[ "$audit_valid" = t ] || { echo 'registered audit delta does not match v4.2' >&2; exit 1; }
trigger_after=$(db "SELECT count(*)=1 AND bool_and(tgenabled='O') FROM pg_trigger WHERE tgrelid='control.audit_events'::regclass AND tgname='audit_events_reject_mutation' AND NOT tgisinternal")
[ "$trigger_after" = t ] || { echo 'audit mutation trigger changed' >&2; exit 1; }

bucket_where="tenant_id='00000000-0000-0000-0000-000000000000'::uuid AND subject_kind='ip' AND subject_id='127.0.0.1' AND operation='mcp' AND bucket_key='preauth'"
before=$(db "SELECT count(*) FROM control.rate_buckets WHERE $bucket_where")
[ "$before" = 1 ] || { echo "preauth before must be 1, got $before" >&2; exit 1; }
deleted=$(db "DELETE FROM control.rate_buckets WHERE $bucket_where")
[ "$deleted" = 'DELETE 1' ] || { echo "preauth delete must affect 1, got $deleted" >&2; exit 1; }
after=$(db "SELECT count(*) FROM control.rate_buckets WHERE $bucket_where")
[ "$after" = 0 ] || { echo "preauth after must be 0, got $after" >&2; exit 1; }

cleanup_relation qualified 2 || { echo 'qualified cleanup failed' >&2; exit 70; }
completed_run_uuid=$run_uuid
script_sha=$(sha256 "$0")
expected_delta_sha=$(sha256 "$artifact_dir/expected_audit_delta.json")
expected_artifacts='audit_baseline.set
audit_delta.set
audit_lost.set
audit_post.set
barrier-a.log
barrier-b.log
barrier-readback.tsv
cleanup.tsv
expected_audit_delta.json
expected_audit_delta.readback.tsv
same-key-a.log
same-key-b.log
strict-pair-window.tsv'

expected_bindings=$(cat <<EOF
authority_sha256	$authority_sha
adjudication_sha256	$adjudication_sha
independent_review_sha256	$review_sha
test_name	$test_name
source_sha256	$source_sha
frozen_sha256	$frozen_actual
binary_sha256	$binary_sha
script_sha256	$script_sha
run_uuid	$completed_run_uuid
postgres_server_version_num	$version_num
barrier_overlap	$overlap
barrier_statuses	$status_a,$status_b
same_key_statuses	$same_status_a,$same_status_b
audit_baseline_count	$baseline_count
audit_post_count	$post_count
audit_baseline_set_sha256	$baseline_sha
audit_post_set_sha256	$post_sha
expected_audit_delta_sha256	$expected_delta_sha
expected_row_sha256	$expected_row_sha
audit_trigger_before	$trigger_before
audit_trigger_after	$trigger_after
preauth_before	$before
preauth_deleted	$deleted
preauth_after	$after
env_binding	HUMAUX_TEST_PG_DSN,HUMAUX_REQUIRE_DB
EOF
)

manifest_tmp=$artifact_dir/manifest.tmp
manifest_final=$artifact_dir/manifest.txt
[ ! -e "$manifest_tmp" ] && [ ! -e "$manifest_final" ] || { echo 'manifest path already exists' >&2; exit 1; }
{
    printf 'schema\tcontinuity-w2-v4.2-gateway-orchestrator\t1\n'
    printf '%s\n' "$expected_bindings" | while IFS= read -r binding; do
        printf 'binding\t%s\n' "$binding"
    done
    printf '%s\n' "$expected_artifacts" | while IFS= read -r name; do
        path=$artifact_dir/$name
        printf 'artifact\t%s\t%s\t%s\n' "$name" "$(file_size "$path")" "$(sha256 "$path")"
    done
} >"$manifest_tmp"
mv "$manifest_tmp" "$manifest_final"

verify_manifest_inventory "$manifest_final" "$artifact_dir" "$expected_artifacts" || { echo 'final manifest inventory/readback failed' >&2; exit 1; }
verify_manifest_bindings "$manifest_final" "$expected_bindings" || { echo 'final manifest binding readback failed' >&2; exit 1; }
manifest_size=$(file_size "$manifest_final")
manifest_sha=$(sha256 "$manifest_final")
[ "$(file_size "$manifest_final")" = "$manifest_size" ] || { echo 'final manifest size readback changed' >&2; exit 1; }
[ "$(sha256 "$manifest_final")" = "$manifest_sha" ] || { echo 'final manifest hash readback changed' >&2; exit 1; }

trap - EXIT HUP INT TERM
printf 'manifest_path=%s\nmanifest_size=%s\nmanifest_sha256=%s\nQUALIFIED_SUCCESS=STAGE_G_V4_2\n' \
    "$manifest_final" "$manifest_size" "$manifest_sha"
