//! `adapters::tests::support::scratch_qdrant` — one test-owned Qdrant container `humaux-c37-qdrant-<pid>-<n>` on a
//!   loopback port, created and removed by its guard (ADR-0064 S3: the rebuild tests delete and recreate
//!   collections, so they never run against the shared dev Qdrant).
//! Depends-on: crates=[]; services=[subprocess(docker), HTTP(loopback)]; env=[]; modules=[]
//! Called-by: [adapters::tests::rebuild, maintenance::tests::drill, maintenance::tests::measure,
//!   maintenance::tests::rebuild_cli]
//! Invariants: [the image is the pinned qdrant/qdrant:v1.19.0 arm64 digest; the container is capped (512 MiB, one
//!   CPU), publishes 6333 on 127.0.0.1 only and carries the label humaux.c37=scratch; the guard exists before
//!   `docker run`, so its Drop removes the container and its anonymous volumes (`rm -f -v`) on every path,
//!   panic included, and prints a failed removal; below 1 GiB of free VM memory nothing starts and the reason is
//!   `blocked: free=<n> MiB` (ADR-0064 D-P, ruling E7)]
//! Spec: Baseline §79.2; ADR-0063; ADR-0064 D-P

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// The pinned Qdrant of the card (linux/arm64 manifest).
const IMAGE: &str =
    "qdrant/qdrant:v1.19.0@sha256:139bbec1a1e6c0f04c978c96b5359568e72e60ae6abc9db0ab4b7643a8cd957f";
/// The container cap, and what must stay free besides it (D-P: 1 GiB floor).
const MEM_LIMIT_MIB: u64 = 512;
const FLOOR_MIB: u64 = 1024;

static NEXT: AtomicUsize = AtomicUsize::new(0);

/// A running scratch Qdrant; dropping it removes the container.
pub struct ScratchQdrant {
    pub name: String,
    pub port: u16,
}

impl Drop for ScratchQdrant {
    fn drop(&mut self) {
        // dep: subprocess(docker) — remove this test's container and its anonymous volumes
        match Command::new("docker")
            .args(["rm", "-f", "-v", &self.name])
            .output()
        {
            Ok(out) if out.status.success() => {}
            Ok(out) => {
                let stderr = String::from_utf8_lossy(&out.stderr);
                if !stderr.contains("No such container") {
                    eprintln!(
                        "scratch_qdrant cleanup: docker rm -f -v {} failed: {stderr}",
                        self.name
                    );
                }
            }
            Err(e) => eprintln!("scratch_qdrant cleanup: docker rm -f -v {}: {e}", self.name),
        }
    }
}

fn docker(args: &[&str]) -> Result<String, String> {
    // dep: subprocess(docker) — one docker CLI call
    let out = Command::new("docker")
        .args(args)
        .output()
        .map_err(|e| format!("docker {}: {e}", args.join(" ")))?;
    if !out.status.success() {
        return Err(format!(
            "docker {}: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_owned())
}

/// `"123.4MiB / 7.7GiB"` → MiB of the first figure.
fn mib(usage: &str) -> u64 {
    let figure = usage.split('/').next().unwrap_or("").trim();
    let (number, unit) = figure.split_at(
        figure
            .find(|c: char| c.is_ascii_alphabetic())
            .unwrap_or(figure.len()),
    );
    let n: f64 = number.trim().parse().unwrap_or(0.0);
    let scale = match unit {
        "GiB" | "GB" => 1024.0,
        "KiB" | "kB" => 1.0 / 1024.0,
        "B" => 1.0 / (1024.0 * 1024.0),
        _ => 1.0,
    };
    (n * scale) as u64
}

/// The Docker VM's memory minus what its running containers use, in MiB.
fn vm_free_mib() -> Result<u64, String> {
    let total: u64 = docker(&["info", "--format", "{{.MemTotal}}"])?
        .parse()
        .map_err(|e| format!("docker info MemTotal: {e}"))?;
    let used: u64 = docker(&["stats", "--no-stream", "--format", "{{.MemUsage}}"])?
        .lines()
        .map(mib)
        .sum();
    Ok((total / (1024 * 1024)).saturating_sub(used))
}

fn ready(port: u16) -> bool {
    // dep: HTTP(loopback) — the scratch container's readiness probe
    let Ok(mut stream) = TcpStream::connect(("127.0.0.1", port)) else {
        return false;
    };
    let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
    if stream
        .write_all(b"GET /readyz HTTP/1.0\r\nHost: 127.0.0.1\r\n\r\n")
        .is_err()
    {
        return false;
    }
    let mut response = String::new();
    let _ = stream.read_to_string(&mut response);
    response.starts_with("HTTP/1.0 200") || response.starts_with("HTTP/1.1 200")
}

impl ScratchQdrant {
    /// Starts `humaux-c37-<purpose>-<pid>-<n>` and waits (≤ 60 s) until it answers `/readyz`.
    pub fn start(purpose: &str) -> Result<Self, String> {
        let free = vm_free_mib()?;
        if free < FLOOR_MIB + MEM_LIMIT_MIB {
            return Err(format!("blocked: free={free} MiB"));
        }
        let mut guard = Self {
            name: format!(
                "humaux-c37-{purpose}-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::SeqCst)
            ),
            port: 0,
        };
        let mem = format!("{MEM_LIMIT_MIB}m");
        docker(&[
            "run",
            "-d",
            "--name",
            &guard.name,
            "--label",
            "humaux.c37=scratch",
            "--memory",
            &mem,
            "--cpus",
            "1",
            "-p",
            "127.0.0.1::6333",
            IMAGE,
        ])?;
        let mapped = docker(&["port", &guard.name, "6333/tcp"])?;
        guard.port = mapped
            .lines()
            .next()
            .and_then(|line| line.rsplit(':').next())
            .and_then(|p| p.parse().ok())
            .ok_or_else(|| format!("docker port {}: {mapped}", guard.name))?;
        let started = Instant::now();
        while !ready(guard.port) {
            if started.elapsed() > Duration::from_secs(60) {
                return Err(format!("{} not ready within 60 s", guard.name));
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        Ok(guard)
    }
}
