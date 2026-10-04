//! `retrieval-worker::tests::rpc_readyz` — T-W5 (ADR-0061 D-F): the worker's `GET /internal/v1/retrieval/readyz`, the
//!   gateway's `retrieval_rpc` readiness round trip, served by the real `rpc::router` on a temporary UDS against a
//!   throwaway database: 200 while `role_retrieval_worker` can connect, 503 naming it once it cannot, 403 for a
//!   wrong peer uid.
//! Depends-on: crates=[axum, humaux-adapters, humaux-local-secret-scan, humaux-retrieval-provider, humaux-testkit,
//!   postgres, tokio, uuid]; services=[PostgreSQL(owner), PostgreSQL(role_retrieval_worker), UDS(serve),
//!   UDS(retrieval-worker)]; env=[HUMAUX_RETRIEVAL_WORKER_PG_DSN, HUMAUX_TEST_GITLEAKS_BIN,
//!   HUMAUX_TEST_GITLEAKS_SHA256, HUMAUX_TEST_GITLEAKS_VERSION, HUMAUX_TEST_PG_DSN]; modules=[adapters::postgres,
//!   humaux-local-secret-scan, humaux-testkit, retrieval-provider::adapters, retrieval-provider::contract,
//!   retrieval-worker::rpc]
//! Called-by: [cargo-test]
//! Invariants: [CONNECT is revoked only on the test's own throwaway database humaux_thread_c34_rw_<pid>, dropped by the
//!   fixture's Drop even on panic; the socket is the test's own under /tmp; a missing env is a failure under
//!   HUMAUX_REQUIRE_DB, a named SKIP otherwise (§79.2)]
//! Spec: Baseline §6.2.3; §79.2; ADR-0012; ADR-0061 D-F

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::sync::Arc;
use std::time::Duration;

use humaux_adapters::postgres::RetrievalWorkerDbPool;
use humaux_local_secret_scan::{LocalSecretScanner, LocalSecretScannerConfig};
use humaux_retrieval_provider::adapters::TestDoubleProvider;
use humaux_retrieval_provider::contract::{
    CalibrationProfileId, EmbeddingModelDescriptor, ModelId, RerankModelDescriptor,
    RerankScoreSemantics,
};
use humaux_retrieval_worker::rpc::{PeerIdentity, RpcState, router};
use humaux_testkit::{ExternalDep, skip_or_fail};
use postgres::{Client, NoTls};
use uuid::Uuid;

const READYZ: &str = "/internal/v1/retrieval/readyz";

fn env_or_skip(test: &str, name: &str) -> Option<String> {
    match std::env::var(name).ok().filter(|v| !v.is_empty()) {
        Some(v) => Some(v),
        None => {
            skip_or_fail(
                test,
                &format!("missing object: {name}"),
                ExternalDep::Postgres,
            );
            None
        }
    }
}

/// `postgres://user:pass@host:port/<db>?query` with the database replaced.
fn with_db(dsn: &str, db: &str) -> String {
    let (head, tail) = dsn.split_at(dsn.rfind('/').expect("dsn has a database path") + 1);
    let query = tail.find('?').map_or("", |i| &tail[i..]);
    format!("{head}{db}{query}")
}

/// `/tmp` directly: a macOS temp dir plus a name overflows `sockaddr_un`'s path limit.
fn socket_path(tag: &str) -> String {
    format!(
        "/tmp/hq-c34rw-{tag}-{}.sock",
        &Uuid::now_v7().simple().to_string()[20..]
    )
}

/// This process's uid, read as the peer credential of a self-connected socket pair.
fn own_uid() -> u32 {
    let path = socket_path("uid");
    // dep: UDS(serve) — a probe socket to read the peer uid
    let listener = tokio::net::UnixListener::bind(&path).expect("bind uid probe");
    let client = std::thread::spawn({
        let path = path.clone();
        // dep: UDS(retrieval-worker) — dial the probe socket
        move || UnixStream::connect(path).expect("dial uid probe")
    });
    let uid = tokio::runtime::Handle::current().block_on(async {
        let (conn, _) = listener.accept().await.expect("accept uid probe");
        conn.peer_cred().expect("peer credential").uid()
    });
    drop(client.join());
    let _ = std::fs::remove_file(&path);
    uid
}

/// A throwaway empty database, dropped WITH (FORCE) on every exit path.
struct ScratchDb {
    owner_dsn: String,
    name: String,
}

impl ScratchDb {
    fn create(owner_dsn: &str) -> Self {
        let name = format!("humaux_thread_c34_rw_{}", std::process::id());
        for sql in [
            format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"),
            format!("CREATE DATABASE {name}"),
        ] {
            Self::exec(owner_dsn, &sql);
        }
        Self {
            owner_dsn: owner_dsn.to_owned(),
            name,
        }
    }

    fn admin_of(owner_dsn: &str) -> Client {
        // dep: PostgreSQL(owner) — cluster-level statements on the scratch database
        Client::connect(&with_db(owner_dsn, "postgres"), NoTls).expect("owner connects")
    }

    /// Runs `sql` as the owner; the sync client must not block an async worker thread.
    fn exec(owner_dsn: &str, sql: &str) {
        tokio::task::block_in_place(|| {
            Self::admin_of(owner_dsn)
                .batch_execute(sql)
                .unwrap_or_else(|e| panic!("{sql}: {e}"));
        });
    }
}

impl Drop for ScratchDb {
    fn drop(&mut self) {
        let drop_db = format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", self.name);
        tokio::task::block_in_place(|| {
            // dep: PostgreSQL(owner) — drop this test's throwaway database
            match Client::connect(&with_db(&self.owner_dsn, "postgres"), NoTls) {
                Ok(mut admin) => {
                    if let Err(e) = admin.batch_execute(&drop_db) {
                        eprintln!("rpc_readyz cleanup: {drop_db} failed: {e}");
                    }
                }
                Err(e) => eprintln!("rpc_readyz cleanup: connect failed: {e}"),
            }
        });
    }
}

fn scanner(test: &str) -> Option<LocalSecretScanner> {
    let bin = env_or_skip(test, "HUMAUX_TEST_GITLEAKS_BIN")?;
    let version = env_or_skip(test, "HUMAUX_TEST_GITLEAKS_VERSION")?;
    let sha = env_or_skip(test, "HUMAUX_TEST_GITLEAKS_SHA256")?;
    Some(
        LocalSecretScanner::new(LocalSecretScannerConfig {
            executable: bin.into(),
            expected_version: version,
            expected_executable_sha256: sha,
            timeout: Duration::from_secs(5),
            max_payload_bytes: 64 * 1024,
            finding_exit_code: 1,
        })
        .expect("valid scanner config"),
    )
}

fn provider() -> TestDoubleProvider {
    TestDoubleProvider::new(
        EmbeddingModelDescriptor {
            model_id: ModelId("test-embedding-model".to_owned()),
            model_revision: "v1".to_owned(),
            dimension_options: vec![4],
            max_input_tokens: 1_000,
            batch_supported: true,
            dense_supported: true,
            sparse_supported: false,
        },
        RerankModelDescriptor {
            model_id: ModelId("unused-rerank-model".to_owned()),
            model_revision: "v1".to_owned(),
            max_documents: 10,
            max_input_tokens: 1_000,
            score_semantics: RerankScoreSemantics::RawLogit,
            calibration_profile: CalibrationProfileId("unused".to_owned()),
        },
    )
}

/// Serves the real `rpc::router` on a fresh socket; returns its path.
async fn serve(state: RpcState) -> String {
    let path = socket_path("rpc");
    // dep: UDS(serve) — the real retrieval-worker RPC router on the test's own socket
    let listener = tokio::net::UnixListener::bind(&path).expect("bind worker rpc socket");
    let app = router(Arc::new(state));
    tokio::spawn(async move {
        let _ = axum::serve(
            listener,
            app.into_make_service_with_connect_info::<PeerIdentity>(),
        )
        .await;
    });
    path
}

/// One `GET` over the socket, blocking, off the runtime's workers.
async fn get(path: &str) -> (u16, String) {
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || {
        // dep: UDS(retrieval-worker) — the gateway's side of the readiness round trip
        let mut stream = UnixStream::connect(&path).expect("dial worker rpc socket");
        stream
            .set_read_timeout(Some(Duration::from_secs(30)))
            .expect("read timeout");
        write!(
            stream,
            "GET {READYZ} HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        )
        .expect("write request");
        let mut raw = String::new();
        stream.read_to_string(&mut raw).expect("read response");
        let code = raw
            .split_whitespace()
            .nth(1)
            .and_then(|c| c.parse().ok())
            .expect("status code");
        let body = raw.split_once("\r\n\r\n").map_or("", |(_, b)| b).to_owned();
        (code, body)
    })
    .await
    .expect("request thread")
}

/// T-W5: 200 while `role_retrieval_worker` connects; after CONNECT is revoked and its sessions are
/// terminated on the scratch database, 503 naming `PostgreSQL as role_retrieval_worker`; a peer with
/// the wrong uid gets 403 from the existing layer. Fault: answer 200 without connecting ⇒ red.
#[tokio::test(flavor = "multi_thread")]
async fn readyz_answers_from_a_fresh_role_connection_and_names_it_when_refused() {
    let test = "readyz_answers_from_a_fresh_role_connection_and_names_it_when_refused";
    let Some(owner) = env_or_skip(test, "HUMAUX_TEST_PG_DSN") else {
        return;
    };
    let Some(worker_dsn) = env_or_skip(test, "HUMAUX_RETRIEVAL_WORKER_PG_DSN") else {
        return;
    };
    let Some(scanner) = scanner(test) else {
        return;
    };
    let scanner = Arc::new(scanner);
    let db = ScratchDb::create(&owner);
    let dsn = with_db(&worker_dsn, &db.name);
    let uid = tokio::task::block_in_place(own_uid);
    let state = |expected_gateway_uid: u32, calls| RpcState {
        expected_gateway_uid,
        calls,
        pg_dsn: dsn.clone(),
        scanner: Arc::clone(&scanner),
        embedder: Arc::new(provider()),
        dimension: 4,
        provider_id: "test-provider".to_owned(),
    };
    // dep: PostgreSQL(role_retrieval_worker) — the RPC state's own pool, as --serve-rpc opens it
    let calls = RetrievalWorkerDbPool::connect(&dsn)
        .await
        .expect("role_retrieval_worker connects to the scratch database");
    let path = serve(state(uid, calls)).await;
    let (code, body) = get(&path).await;
    assert_eq!(code, 200, "{body}");

    ScratchDb::exec(
        &owner,
        &format!(
            "REVOKE CONNECT ON DATABASE {0} FROM PUBLIC; REVOKE CONNECT ON DATABASE {0} FROM role_retrieval_worker;
             SELECT pg_terminate_backend(pid) FROM pg_stat_activity
              WHERE datname = '{0}' AND usename = 'role_retrieval_worker'",
            db.name
        ),
    );
    let (code, body) = get(&path).await;
    assert_eq!(code, 503, "{body}");
    assert!(
        body.contains("missing object: PostgreSQL as role_retrieval_worker"),
        "{body}"
    );
    let _ = std::fs::remove_file(&path);

    // Restore CONNECT so the wrong-uid server can build its pool.
    ScratchDb::exec(
        &owner,
        &format!("GRANT CONNECT ON DATABASE {} TO PUBLIC", db.name),
    );
    // dep: PostgreSQL(role_retrieval_worker) — the wrong-uid server's own pool
    let calls = RetrievalWorkerDbPool::connect(&dsn)
        .await
        .expect("role_retrieval_worker connects again");
    let wrong = serve(state(uid.wrapping_add(1), calls)).await;
    let (code, body) = get(&wrong).await;
    assert_eq!(
        code, 403,
        "a wrong peer uid is refused before the route: {body}"
    );
    let _ = std::fs::remove_file(&wrong);
}
