//! `testkit::reaped` — kill-on-drop ownership of a subprocess a test spawns.
//! Depends-on: crates=[]; services=[subprocess(kill), subprocess(sleep)]; env=[]; modules=[]
//! Called-by: [tests]
//! Invariants: [a subprocess still running when its owner is dropped — at scope end or while a failed assertion
//!   unwinds — is killed and waited; an orphan inherits the test's stdout, which under a gate is the pipe the gate
//!   captures, so a red run would hang its gate instead of reporting red (ADR-0063 "Chain stall")]
//! Spec: §79.2; ADR-0063 ("Chain stall, 2026-10-05")

/// A subprocess that never outlives its owner. Declare it after the database fixture it works against: locals
/// drop in reverse order, so the worker is gone before the fixture purges its tenants.
pub struct Reaped(std::process::Child);

impl Drop for Reaped {
    fn drop(&mut self) {
        if matches!(self.0.try_wait(), Ok(None)) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

impl std::ops::Deref for Reaped {
    type Target = std::process::Child;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for Reaped {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// `Command::spawn` returning a [`Reaped`]. The e2e test files that spawn a worker under test construct their
/// subprocesses here and nowhere else (gate `c36_children_are_reaped`).
pub trait SpawnReaped {
    /// Spawns the command; panics with `what` when the spawn itself fails.
    fn spawn_reaped(&mut self, what: &str) -> Reaped;
}

impl SpawnReaped for std::process::Command {
    fn spawn_reaped(&mut self, what: &str) -> Reaped {
        Reaped(self.spawn().expect(what))
    }
}

#[cfg(test)]
mod tests {
    use super::SpawnReaped;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[test]
    fn reaped_child_dies_with_its_unwinding_owner() {
        let pid = AtomicU32::new(0);
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            // dep: subprocess(sleep) — a long-lived stand-in for a worker under test
            let child = std::process::Command::new("sleep")
                .arg("300")
                .spawn_reaped("spawn sleep");
            pid.store(child.id(), Ordering::SeqCst);
            // resume_unwind: unwinds like a failed assertion without printing a panic message
            std::panic::resume_unwind(Box::new(()));
        }));
        assert!(outcome.is_err());
        let pid = pid.load(Ordering::SeqCst);
        assert_ne!(pid, 0, "the subprocess was never spawned");
        // dep: subprocess(kill) — `kill -0` probes whether the pid still exists
        let alive = std::process::Command::new("kill")
            .arg("-0")
            .arg(pid.to_string())
            .stderr(std::process::Stdio::null())
            .status()
            .expect("probe the subprocess")
            .success();
        assert!(!alive, "the subprocess outlived its unwinding owner");
    }
}
