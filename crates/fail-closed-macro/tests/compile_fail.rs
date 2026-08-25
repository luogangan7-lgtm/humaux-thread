//! Trybuild-style compile-fail check for `#[fail_closed]` (§53.6: 缺 threat 参数编译不过).
//! No `trybuild` dependency exists in this crate, and adding one only for this single check
//! would be exactly the one-implementation dependency ponytail forbids when
//! `Command::new("cargo")` already does the job — see
//! `xtask/src/architecture_check.rs::dependency_rule_check` for the identical pattern
//! already in this repo. This drives a real `cargo build` against a throwaway two-crate
//! workspace and reads its exit status + stderr, same as trybuild does under the hood.
//!
//! Task rule 3 (tempdir fixture copies only, real repo files never written): the macro
//! crate's own source is *copied* into the tempdir rather than path-dependency-linked to
//! the real crate directory, so the throwaway workspace never nests inside this repo's real
//! Cargo workspace and nothing under the real repo is ever opened for writing.

use std::path::PathBuf;
use std::process::Command;

fn macro_src() -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/lib.rs");
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()))
}

fn tempdir(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "fail-closed-macro-compiletest-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

/// Builds a throwaway 2-crate workspace (`macro/` = a copy of this crate's real source,
/// `fixture/` = one fn with `#[fail_closed(<attr>)]`) and runs `cargo build` against it.
/// Returns `(build succeeded, stderr)`.
fn try_build(name: &str, attr: &str) -> (bool, String) {
    let root = tempdir(name);
    std::fs::create_dir_all(root.join("macro/src")).unwrap();
    std::fs::create_dir_all(root.join("fixture/src")).unwrap();

    std::fs::write(
        root.join("Cargo.toml"),
        "[workspace]\nresolver = \"3\"\nmembers = [\"macro\", \"fixture\"]\n",
    )
    .unwrap();
    std::fs::write(
        root.join("macro/Cargo.toml"),
        "[package]\nname = \"humaux-fail-closed-macro\"\nversion = \"0.0.0\"\nedition = \"2024\"\n\n\
         [lib]\nproc-macro = true\n",
    )
    .unwrap();
    std::fs::write(root.join("macro/src/lib.rs"), macro_src()).unwrap();
    std::fs::write(
        root.join("fixture/Cargo.toml"),
        "[package]\nname = \"fixture\"\nversion = \"0.0.0\"\nedition = \"2024\"\n\n\
         [dependencies]\nhumaux-fail-closed-macro = { path = \"../macro\" }\n",
    )
    .unwrap();
    std::fs::write(
        root.join("fixture/src/lib.rs"),
        format!(
            "use humaux_fail_closed_macro::fail_closed;\n\n#[fail_closed({attr})]\npub fn f() {{}}\n"
        ),
    )
    .unwrap();

    let output = Command::new("cargo")
        .args(["build", "--quiet"])
        .current_dir(&root)
        .output()
        .expect("failed to spawn cargo build");
    std::fs::remove_dir_all(&root).ok();
    (
        output.status.success(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

/// 注错: `#[fail_closed]` with no `threat` argument must fail to compile, and the error must
/// actually mention `threat` — not merely fail for some unrelated reason.
#[test]
fn missing_threat_fails_to_compile() {
    let (ok, stderr) = try_build("missing-threat", "");
    assert!(
        !ok,
        "expected compile failure, got success. stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("threat"),
        "compile error must mention `threat`, got:\n{stderr}"
    );
}

/// 红转绿 positive control: the identical fixture shape with `threat = "..."` present must
/// compile clean — proves the failure above is about the missing argument, not some other
/// defect in the fixture scaffolding.
#[test]
fn present_threat_compiles() {
    let (ok, stderr) = try_build("present-threat", "threat = \"example threat\"");
    assert!(
        ok,
        "expected successful compile, got failure. stderr:\n{stderr}"
    );
}
