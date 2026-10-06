//! `maintenance::tests::spike` — the twelve kept spikes of ADR-0064 section 10.6, run once at the start of card 37
//!   S4 (`-- --ignored --nocapture`). Each measures one fact the pgBackRest / PostgreSQL 18 / Docker documentation
//!   leaves open, on the pinned image, and prints it as `MEASURE sp<n> <key>=<value> <unit> n=<n> pgbackrest=<version>`
//!   (pasted into ADR-0064; gate c37_spikes_recorded). Each asserts the fact a card-37 decision rests on, so a pin
//!   or upgrade that changes it turns the spike red.
//! Depends-on: crates=[postgres]; services=[subprocess(sh), PostgreSQL(role_maintenance)]; env=[];
//!   modules=[maintenance::tests::support::containers, maintenance::tests::support::throwaway]
//! Called-by: [cargo-test]
//! Invariants: [ids are exactly 1, 2, 3, 4, 5, 9, 10, 11, 12, 13, 14, 16 (6, 7, 8, 15 stay reserved for card 37d);
//!   every Docker resource is a humaux-c37-sp<n>-<pid>-<n> project or one-shot removed by its guard; the shared
//!   containers are never touched; no secret is printed]
//! Spec: ADR-0064 section 10.6; ADR-0064 D-H; ADR-0064 D-J; ADR-0064 D-V

#[path = "support/throwaway.rs"]
#[allow(dead_code)]
mod throwaway;

#[path = "support/containers.rs"]
#[allow(dead_code)]
mod containers;

use containers::{Source, both, root};

fn measure(
    n: u32,
    key: &str,
    value: impl std::fmt::Display,
    unit: &str,
    runs: u32,
    version: &str,
    note: &str,
) {
    println!("MEASURE sp{n} {key}={value} {unit} n={runs} pgbackrest={version} ({note})");
}

/// A source with one full set of ~`mib` MiB.
fn with_one_full(test: &str, purpose: &str, mib: u32) -> Option<(Source, String)> {
    let source = Source::start(test, purpose, "")?;
    source.stanza_create();
    source.seed("w", mib);
    let out = source.pgbackrest(&[
        "backup",
        "--type=full",
        "--no-expire-auto",
        "--log-level-console=warn",
    ]);
    assert!(out.status.success(), "backup: {}", both(&out));
    let label = source.labels().pop().expect("one set");
    Some((source, label))
}

/// SP-1: `verify --set` on a set with one byte flipped in a bundle: its exit code, what it reports, bytes read.
#[test]
#[ignore = "lane(c) ADR-0064 10.6 spike, run once and pasted into the ADR as its MEASURE line; never in the chain or the lane: run once with HUMAUX_REQUIRE_DOCKER=1 -- --ignored --nocapture"]
fn sp1_verify_on_one_corrupt_file() {
    let Some((source, label)) = with_one_full("sp1_verify_on_one_corrupt_file", "sp1", 8) else {
        return;
    };
    let bundle = source
        .repo()
        .join(format!("backup/humaux/{label}/bundle/1"));
    let mut bytes = std::fs::read(&bundle).expect("bundle 1");
    bytes[1000] ^= 0xff;
    std::fs::write(&bundle, &bytes).expect("flip one byte");
    let out = source.pgbackrest(&[
        "verify",
        &format!("--set={label}"),
        "--output=text",
        "--verbose",
        "--log-level-console=warn",
    ]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    let set_bytes = source.info()["backup"][0]["info"]["repository"]["size"]
        .as_i64()
        .unwrap_or(-1);
    let code = out.status.code().unwrap_or(-1);
    measure(
        1,
        "verify_exit_on_corrupt_file",
        code,
        "exit_code",
        1,
        &source.pgbackrest_version(),
        &format!(
            "text output reports status: error={}; checksum invalid: 1={}; bytes_read=whole set={set_bytes} B",
            stdout.lines().any(|l| l.trim() == "status: error"),
            stdout.contains("checksum invalid: 1")
        ),
    );
    assert!(
        stdout.lines().any(|l| l.trim() == "status: error"),
        "the corruption is reported: {stdout}"
    );
}

/// SP-2: `archive-push` of an already archived segment name, same content vs other content (posix).
#[test]
#[ignore = "lane(c) ADR-0064 10.6 spike, run once and pasted into the ADR as its MEASURE line; never in the chain or the lane: run once with HUMAUX_REQUIRE_DOCKER=1 -- --ignored --nocapture"]
fn sp2_archive_push_same_name() {
    let Some(source) = Source::start("sp2_archive_push_same_name", "sp2", "") else {
        return;
    };
    source.stanza_create();
    let codes = source.sh(
        "set -u; G=/var/lib/postgresql/18/docker; S=$(psql -qAt -c 'select pg_walfile_name(pg_current_wal_lsn())'); \
         psql -qAt -c 'create table t(i int); insert into t values (1); select pg_switch_wal()' >/dev/null; sleep 2; \
         d=$(mktemp -d); mkdir -p $d/pg_wal/archive_status $d/global; cp $G/global/pg_control $d/global/; \
         cp $G/pg_wal/$S $d/pg_wal/; P=\"pgbackrest --stanza=humaux --pg1-path=$d --log-level-console=off archive-push\"; \
         $P $d/pg_wal/$S; a=$?; printf 'x' | dd of=$d/pg_wal/$S bs=1 seek=100000 conv=notrunc 2>/dev/null; \
         $P $d/pg_wal/$S; b=$?; rm -rf $d; echo $a $b",
    );
    let v = source.pgbackrest_version();
    measure(
        2,
        "archive_push_same_name_same_content_exit",
        codes.split(' ').next().unwrap_or("?"),
        "exit_code",
        1,
        &v,
        &format!(
            "other content exit={}",
            codes.split(' ').nth(1).unwrap_or("?")
        ),
    );
    assert!(
        codes.starts_with("0 ") && !codes.ends_with(" 0"),
        "same=0, other!=0: {codes}"
    );
}

/// SP-3: the bundle + cipher layout on posix, and `repo-get` of a set's manifest (decrypted inside the container).
#[test]
#[ignore = "lane(c) ADR-0064 10.6 spike, run once and pasted into the ADR as its MEASURE line; never in the chain or the lane: run once with HUMAUX_REQUIRE_DOCKER=1 -- --ignored --nocapture"]
fn sp3_bundle_cipher_layout_and_manifest_repo_get() {
    let Some((source, label)) =
        with_one_full("sp3_bundle_cipher_layout_and_manifest_repo_get", "sp3", 8)
    else {
        return;
    };
    let bundles = std::fs::read_dir(source.repo().join(format!("backup/humaux/{label}/bundle")))
        .map(|d| d.count())
        .unwrap_or(0);
    let on_disk = std::fs::read(
        source
            .repo()
            .join(format!("backup/humaux/{label}/backup.manifest")),
    )
    .expect("manifest");
    let plaintext_on_disk = String::from_utf8_lossy(&on_disk).contains("[backrest]");
    let got = source.sh(&format!(
        "pgbackrest --stanza=humaux --log-level-console=warn repo-get backup/humaux/{label}/backup.manifest | head -1"
    ));
    measure(
        3,
        "bundle_files_in_one_full",
        bundles,
        "files",
        1,
        &source.pgbackrest_version(),
        &format!(
            "manifest plaintext on disk={plaintext_on_disk}; repo-get backup/humaux/<label>/backup.manifest first line={got}"
        ),
    );
    assert!(
        bundles > 0 && !plaintext_on_disk && got == "[backrest]",
        "bundled, encrypted, readable through repo-get"
    );
}

/// SP-4: the pinned build: version, every option of pgbackrest.conf accepted, zst used; 2.59.3 in trixie-pgdg.
#[test]
#[ignore = "lane(c) ADR-0064 10.6 spike, run once and pasted into the ADR as its MEASURE line; never in the chain or the lane: run once with HUMAUX_REQUIRE_DOCKER=1 -- --ignored --nocapture"]
fn sp4_pinned_build_options_and_zst() {
    let Some((source, label)) = with_one_full("sp4_pinned_build_options_and_zst", "sp4", 4) else {
        return;
    };
    let check = source.pgbackrest(&["check"]);
    let log = both(&check);
    let zst = source.sh(&format!(
        "ls /var/lib/pgbackrest/repo/backup/humaux/{label}/pg_data | grep -c 'zst$' || true"
    ));
    let dpkg = source.sh("dpkg-query -W -f '${Version}' pgbackrest");
    let v = source.pgbackrest_version();
    measure(
        4,
        "pgbackrest_package",
        &dpkg,
        "version",
        1,
        &v,
        &format!(
            "check exit={}; conf options reported invalid={}; zst files in the set's pg_data={zst}; \
                  2.59.3-1.pgdg13+1 installed from trixie-pgdg, so O2 pins it",
            check.status.code().unwrap_or(-1),
            log.matches("invalid option").count()
        ),
    );
    assert!(
        check.status.success() && !log.contains("invalid option") && dpkg == "2.59.3-1.pgdg13+1"
    );
}

/// SP-5: restore and `archive-get` from a READ-ONLY posix bind of the repository (the drill's mount).
#[test]
#[ignore = "lane(c) ADR-0064 10.6 spike, run once and pasted into the ADR as its MEASURE line; never in the chain or the lane: run once with HUMAUX_REQUIRE_DOCKER=1 -- --ignored --nocapture"]
fn sp5_restore_from_a_read_only_bind() {
    let Some((mut source, _label)) = with_one_full("sp5_restore_from_a_read_only_bind", "sp5", 8)
    else {
        return;
    };
    let rows = source.psql("select count(*) from w");
    let conf = format!(
        "{}:/etc/pgbackrest/pgbackrest.conf:ro",
        root().join("deploy/pgbackrest/pgbackrest.conf").display()
    );
    let repo = format!("{}:/var/lib/pgbackrest:ro", source.repo_dir.display());
    let one = source.oneshot(
        "restore",
        &[
            "-v",
            &conf,
            "-v",
            &repo,
            "--entrypoint",
            "sleep",
            containers::IMAGE,
            "infinity",
        ],
    );
    let run =
        |script: &str| containers::docker(&["exec", "-u", "postgres", &one, "sh", "-c", script]);
    let probe =
        run("touch /var/lib/pgbackrest/repo/.drill-probe 2>&1; echo rc=$?").unwrap_or_default();
    let restored = run(
        "D=/var/lib/postgresql/18/docker; mkdir -p $D && chmod 700 $D && \
         pgbackrest --stanza=humaux --log-level-console=off --archive-mode=off --type=immediate --target-action=promote restore && \
         pg_ctl -D $D -o '-c listen_addresses= -c archive_mode=off' -w -t 60 start >/tmp/pg.log 2>&1 && \
         psql -qAt -c 'select count(*) from w'; grep -c 'archive-get command end: completed successfully' /tmp/pg.log",
    );
    let restored = restored.unwrap_or_else(|e| e);
    measure(
        5,
        "restore_from_read_only_bind_rows",
        restored.lines().next().unwrap_or("?"),
        "rows",
        1,
        &source.pgbackrest_version(),
        &format!(
            "source rows={rows}; archive-get completions={}; write probe={}",
            restored.lines().nth(1).unwrap_or("?"),
            probe.lines().last().unwrap_or("?")
        ),
    );
    assert!(
        probe.contains("Read-only file system") && restored.lines().next() == Some(rows.as_str())
    );
}

/// SP-9: repository bytes of one forced-switch WAL segment under zst + aes-256-cbc (10 switches with one row each).
#[test]
#[ignore = "lane(c) ADR-0064 10.6 spike, run once and pasted into the ADR as its MEASURE line; never in the chain or the lane: run once with HUMAUX_REQUIRE_DOCKER=1 -- --ignored --nocapture"]
fn sp9_bytes_per_forced_switch_segment() {
    let Some(source) = Source::start("sp9_bytes_per_forced_switch_segment", "sp9", "") else {
        return;
    };
    source.stanza_create();
    // Warm-up: archive everything initdb and the table left, so the ten measured segments are forced switches only.
    source.psql("create table t(i int); insert into t values (-1); select pg_switch_wal()");
    let settled = (0..40).any(|_| {
        std::thread::sleep(std::time::Duration::from_millis(250));
        source.psql("select count(*) = 0 from pg_ls_archive_statusdir() where name like '%.ready'")
            == "t"
    });
    assert!(settled, "warm-up segments archived");
    let a = "find /var/lib/pgbackrest/repo/archive -type f -name '0*.zst' -exec stat -c %s {} + | awk '{s+=$1;n++} END {print n+0, s+0}'";
    let before = source.sh(a);
    for i in 0..10 {
        source.psql(&format!(
            "insert into t values ({i}); select pg_switch_wal()"
        ));
    }
    let drained = (0..40).any(|_| {
        std::thread::sleep(std::time::Duration::from_millis(250));
        source.psql("select count(*) = 0 from pg_ls_archive_statusdir() where name like '%.ready'")
            == "t"
    });
    assert!(drained, "the ten segments archived");
    let after = source.sh(a);
    let nums = |s: &str| -> (i64, i64) {
        let mut it = s.split_whitespace().map(|x| x.parse().unwrap_or(0));
        (it.next().unwrap_or(0), it.next().unwrap_or(0))
    };
    let ((n0, b0), (n1, b1)) = (nums(&before), nums(&after));
    let per = (b1 - b0) / (n1 - n0).max(1);
    measure(
        9,
        "repo_bytes_per_forced_switch_segment",
        per,
        "bytes",
        10,
        &source.pgbackrest_version(),
        &format!(
            "segments archived={}; 24 switches/day at archive_timeout=3600 s ≈ {} bytes/day",
            n1 - n0,
            per * 24
        ),
    );
    assert_eq!(n1 - n0, 10, "ten segments archived");
    assert!(
        per > 0 && per < 1 << 20,
        "a forced near-empty segment compresses far below 16 MiB: {per}"
    );
}

/// SP-10: `archive-push-queue-max` in sync mode: past the cap the segment is dropped and reported archived, and
/// `pg_wal` stops growing (archive dir unwritable, queue max 64 MiB, max_wal_size 64 MB, a write burst).
#[test]
#[ignore = "lane(c) ADR-0064 10.6 spike, run once and pasted into the ADR as its MEASURE line; never in the chain or the lane: run once with HUMAUX_REQUIRE_DOCKER=1 -- --ignored --nocapture"]
fn sp10_queue_max_drops_and_caps_pg_wal() {
    let extra = "services:\n  pg:\n    environment:\n      PGBACKREST_ARCHIVE_PUSH_QUEUE_MAX: 64MiB\n    command: [postgres, -c, max_wal_size=64MB, -c, min_wal_size=32MB, -c, wal_level=replica, -c, archive_mode=on, -c, archive_timeout=3600s, -c, \"archive_command=pgbackrest --stanza=humaux archive-push %p\", -c, \"listen_addresses=\"]\n";
    let Some(source) = Source::start("sp10_queue_max_drops_and_caps_pg_wal", "sp10", extra) else {
        return;
    };
    source.stanza_create();
    source.psql("select pg_switch_wal()");
    std::thread::sleep(std::time::Duration::from_secs(2));
    source.sh("chmod 0500 /var/lib/pgbackrest/repo/archive/humaux/18-1/0000000100000000");
    let started = source.psql("select pg_postmaster_start_time()");
    let mut max_wal = 0_i64;
    for _ in 0..16 {
        source.seed("burst", 12);
        source.psql("select pg_switch_wal()");
        let w: i64 = source
            .psql("select sum(size) from pg_ls_waldir()")
            .parse()
            .unwrap_or(0);
        max_wal = max_wal.max(w);
    }
    let dropped = source
        .logs()
        .matches("because archive queue exceeded")
        .count();
    let still = source.psql("select pg_postmaster_start_time()") == started;
    source.sh("chmod 0750 /var/lib/pgbackrest/repo/archive/humaux/18-1/0000000100000000");
    measure(
        10,
        "pg_wal_max_bytes",
        max_wal,
        "bytes",
        1,
        &source.pgbackrest_version(),
        &format!(
            "queue max 64MiB, max_wal_size 64MB, 16 rounds of ~12 MiB; dropped segments={dropped} (pgBackRest \
                  acknowledges a dropped segment, so PostgreSQL counts it archived); postmaster unchanged={still}"
        ),
    );
    assert!(
        dropped > 0 && still,
        "segments dropped and PostgreSQL kept running"
    );
    assert!(
        max_wal <= (64 + 64 + 3 * 16) << 20,
        "pg_wal stays near queue max + max_wal_size: {max_wal}"
    );
}

/// SP-11: the `info --output=json` fields: a set's repository size, and repo1's cipher.
#[test]
#[ignore = "lane(c) ADR-0064 10.6 spike, run once and pasted into the ADR as its MEASURE line; never in the chain or the lane: run once with HUMAUX_REQUIRE_DOCKER=1 -- --ignored --nocapture"]
fn sp11_info_json_set_size_and_cipher() {
    let Some((source, _label)) = with_one_full("sp11_info_json_set_size_and_cipher", "sp11", 8)
    else {
        return;
    };
    let info = source.info();
    let size = &info["backup"][0]["info"]["repository"]["size"];
    let cipher = &info["repo"][0]["cipher"];
    let du = source.sh("du -sb /var/lib/pgbackrest/repo/backup/humaux/2* | cut -f1");
    measure(
        11,
        "info_backup_info_repository_size",
        size,
        "bytes",
        1,
        &source.pgbackrest_version(),
        &format!(
            "field backup[].info.repository.size; set directory du -sb={du}; field repo[].cipher={cipher}"
        ),
    );
    assert!(size.as_i64().is_some_and(|s| s > 0) && cipher == "aes-256-cbc");
}

/// SP-12: `pg_stat_archiver` is readable by role_maintenance on PG18 (no grant needed by the DR_EVIDENCE latch).
#[test]
#[ignore = "lane(c) ADR-0064 10.6 spike, run once and pasted into the ADR as its MEASURE line; never in the chain or the lane: run once -- --ignored --nocapture"]
fn sp12_pg_stat_archiver_readable_by_role_maintenance() {
    let Some(db) = throwaway::empty(
        "sp12_pg_stat_archiver_readable_by_role_maintenance",
        "c37_sp12",
    ) else {
        return;
    };
    // dep: PostgreSQL(role_maintenance) — pg_stat_archiver, the cluster-wide view the DR_EVIDENCE latch reads
    let mut c = postgres::Client::connect(&db.maintenance_dsn, postgres::NoTls)
        .expect("connect as role_maintenance");
    let row = c
        .query_one("SELECT current_user::text, archived_count, failed_count, last_failed_time IS NULL FROM pg_stat_archiver", &[])
        .expect("pg_stat_archiver as role_maintenance");
    let who: String = row.get(0);
    let version: String = c
        .query_one("SHOW server_version", &[])
        .expect("version")
        .get(0);
    measure(
        12,
        "pg_stat_archiver_select_as_role_maintenance",
        "ok",
        "result",
        1,
        "n/a",
        &format!(
            "current_user={who}; server_version={version}; archived_count={}",
            row.get::<_, i64>(1)
        ),
    );
    assert_eq!(who, "role_maintenance");
}

/// SP-13: uid / gid of `postgres` in the pinned image (runbook go-live step 1 chowns the repository mount to it).
#[test]
#[ignore = "lane(c) ADR-0064 10.6 spike, run once and pasted into the ADR as its MEASURE line; never in the chain or the lane: run once with HUMAUX_REQUIRE_DOCKER=1 -- --ignored --nocapture"]
fn sp13_postgres_uid_gid() {
    if !containers::docker_ready("sp13_postgres_uid_gid") {
        return;
    }
    let ids = containers::docker(&[
        "run",
        "--rm",
        "--memory",
        "64m",
        containers::IMAGE,
        "sh",
        "-c",
        "echo $(id -u postgres):$(id -g postgres); pgbackrest version",
    ])
    .expect("id postgres");
    let mut lines = ids.lines();
    let uid_gid = lines.next().unwrap_or("?").to_owned();
    let v = lines
        .next()
        .unwrap_or("?")
        .trim_start_matches("pgBackRest ")
        .to_owned();
    measure(
        13,
        "postgres_uid_gid",
        &uid_gid,
        "uid:gid",
        1,
        &v,
        "docker run --rm <pinned image> id postgres",
    );
    assert_eq!(uid_gid, "999:999");
}

/// SP-14: a posix repository on a Docker Desktop host bind (virtiofs ownership) through stanza-create, backup and
/// verify.
#[test]
#[ignore = "lane(c) ADR-0064 10.6 spike, run once and pasted into the ADR as its MEASURE line; never in the chain or the lane: run once with HUMAUX_REQUIRE_DOCKER=1 -- --ignored --nocapture"]
fn sp14_posix_repo_on_a_docker_desktop_bind() {
    let Some((source, label)) =
        with_one_full("sp14_posix_repo_on_a_docker_desktop_bind", "sp14", 4)
    else {
        return;
    };
    let out = source.pgbackrest(&[
        "verify",
        &format!("--set={label}"),
        "--output=text",
        "--verbose",
        "--log-level-console=warn",
    ]);
    let ok = String::from_utf8_lossy(&out.stdout)
        .lines()
        .any(|l| l.trim() == "status: ok");
    let inside = source.sh("stat -c '%U:%G %a' /var/lib/pgbackrest/repo");
    measure(
        14,
        "posix_repo_on_host_bind_stanza_backup_verify",
        if ok { "ok" } else { "failed" },
        "result",
        1,
        &source.pgbackrest_version(),
        &format!(
            "repo/ owner:group mode inside pg={inside}; host dir under $TMPDIR, no external volume needed"
        ),
    );
    assert!(ok, "verify on the host bind: {}", both(&out));
}

/// SP-16: the pinned amtool `config routes test` accepts alertmanager.yml with its `url_file` placeholders.
#[test]
#[ignore = "lane(c) ADR-0064 10.6 spike, run once and pasted into the ADR as its MEASURE line; never in the chain or the lane: run once -- --ignored --nocapture"]
fn sp16_amtool_routes_test_with_url_file_placeholders() {
    // dep: subprocess(sh) — deploy/prometheus/pinned-tool.sh, the pinned amtool (ADR-0061 D-I)
    let out = std::process::Command::new("sh")
        .current_dir(root())
        .args([
            "deploy/prometheus/pinned-tool.sh",
            "amtool",
            "config",
            "routes",
            "test",
            "--config.file=deploy/prometheus/alertmanager.yml",
            "alertname=BackupNotOffsite",
        ])
        .output()
        .expect("pinned amtool");
    let receiver = String::from_utf8_lossy(&out.stdout).trim().to_owned();
    measure(
        16,
        "amtool_routes_test_exit",
        out.status.code().unwrap_or(-1),
        "exit_code",
        1,
        "n/a",
        &format!("placeholders accepted as-is, no temp copy needed; receiver today={receiver}"),
    );
    assert!(out.status.success(), "{}", both(&out));
}
