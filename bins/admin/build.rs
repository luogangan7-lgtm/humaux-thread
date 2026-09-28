//! `admin::build` — Burns the revision this binary was built from into `HUMAUX_BUILD_GIT_SHA`, which is the ONE input
//!   `humaux-admin q deploy.binary` (§4.4) has and cannot obtain at runtime — reading it from the process environment
//!   would make "set a variable, claim to be another build" true, which is §4.4 坑4 itself.
//! Depends-on: crates=[]; services=[subprocess(git)]; env=[HUMAUX_BUILD_GIT_SHA]; modules=[]
//! Called-by: [cargo-build]
//! Invariants: [a build without a resolvable git SHA still succeeds with a placeholder value, it never fails the build]
//! Spec: Baseline §4.4
//!
//! Before this script existed nothing in the repository ever set that variable, so the probe
//! that Baseline §4.4 and docs/ops/supervision.md both list as *live* answered `missing object`
//! in every build the workspace produced. An explicit `HUMAUX_BUILD_GIT_SHA` in the environment
//! still wins (release builds set it per the runbook); this is the fallback that makes an
//! ordinary `cargo build` honest too.
//!
//! Not a repository (a vendored tarball, a source package) ⇒ nothing is emitted and the probe
//! keeps refusing to answer. That is the correct outcome: a build that cannot name its revision
//! must say so rather than print a plausible one.
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=HUMAUX_BUILD_GIT_SHA");
    println!("cargo:rerun-if-env-changed=HUMAUX_BUILD_TIME");
    if std::env::var("HUMAUX_BUILD_GIT_SHA").is_ok_and(|v| !v.trim().is_empty()) {
        return; // rustc already sees it; do not shadow an explicit release value.
    }
    let Some(sha) = git(&["rev-parse", "HEAD"]) else {
        return;
    };
    // Rebuild when the checkout moves. `HEAD` covers checkouts/rebases; the file the symbolic
    // ref points at covers new commits on the current branch.
    // ponytail: two paths, not a full ref-log watch — a packed-refs-only branch update can leave
    // the sha one build stale in a dev tree; release builds pass HUMAUX_BUILD_GIT_SHA explicitly
    // and are unaffected. Widen to `git rev-parse --git-path` over every ref if that ever bites.
    if let Some(git_dir) = git(&["rev-parse", "--absolute-git-dir"]) {
        println!("cargo:rerun-if-changed={git_dir}/HEAD");
        if let Some(reference) = git(&["symbolic-ref", "--quiet", "HEAD"]) {
            println!("cargo:rerun-if-changed={git_dir}/{reference}");
        }
    }
    println!("cargo:rustc-env=HUMAUX_BUILD_GIT_SHA={sha}");
}

fn git(args: &[&str]) -> Option<String> {
    // dep: subprocess(git) — runs `git rev-parse` to read the build SHA
    let output = Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8(output.stdout).ok()?.trim().to_owned();
    (!value.is_empty()).then_some(value)
}
