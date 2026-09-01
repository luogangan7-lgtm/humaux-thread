//! Canonical §1.14 registry parsing and pure runtime status derivation.
//!
//! Bootstrap columns are retained for static rendering only. They are never inputs to
//! [`derive_status`]; a cached database status is not evidence of an executed mechanism.

use serde::Serialize;

/// §1.14 G4 freshness limit, in microseconds (the database timestamp resolution).
pub const OBSERVATION_MAX_AGE_MICROS: i64 = 90 * 24 * 60 * 60 * 1_000_000;

/// Closed activation-kind vocabulary from the canonical registry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum ActivationKind {
    /// No runtime mechanism exists for this chapter.
    NoMechanism,
    /// Every fresh observation needs an executed E2E witness.
    Always,
    /// The live denominator must reach the registry threshold before activation.
    DenominatorGated,
}

/// A static registry row. This is a parsed view, not another registry authority.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct MechanismSpec {
    /// Chapter number, also the first component of [`Self::id`].
    pub ch: u32,
    /// Canonical mechanism label.
    pub mechanism: String,
    /// Frozen activation semantics.
    pub activation_kind: ActivationKind,
    /// Comparable live threshold; absent for `NO_MECHANISM`.
    pub min_denominator: Option<i64>,
    /// Canonical named probe, SQL query or metric expression.
    pub probe: String,
    /// Historical migration evidence, never runtime truth.
    pub bootstrap_value: String,
    /// Historical migration date, never runtime freshness.
    pub bootstrap_measured_at: String,
    /// Canonical explanatory note.
    pub note: String,
}

impl MechanismSpec {
    /// Join key, using the existing §50.1 `ch:mechanism` reference format.
    pub fn id(&self) -> String {
        format!("{}:{}", self.ch, self.mechanism)
    }
}

/// Extract the canonical fence bodies. Callers can report duplicate/missing fences.
pub fn extract_fences(text: &str) -> Vec<Vec<&str>> {
    let mut fences = Vec::new();
    let mut lines = text.lines();
    while let Some(line) = lines.next() {
        if line.trim() == "```mechanism-registry" {
            let mut body = Vec::new();
            for inner in lines.by_ref() {
                if inner.trim() == "```" {
                    break;
                }
                body.push(inner);
            }
            fences.push(body);
        }
    }
    fences
}

/// Parse the sole eight-column fence. Unknown kinds, duplicate IDs and malformed
/// thresholds fail loudly instead of producing empty or default runtime mechanisms.
pub fn parse_registry(text: &str) -> Result<Vec<MechanismSpec>, String> {
    let fences = extract_fences(text);
    if fences.len() != 1 {
        return Err(format!(
            "expected one mechanism-registry fence, found {}",
            fences.len()
        ));
    }
    let mut ids = std::collections::BTreeSet::new();
    let mut specs = Vec::new();
    for line in fences[0]
        .iter()
        .map(|line| line.trim())
        .filter(|line| !line.is_empty())
    {
        let fields: Vec<_> = line.split('|').map(str::trim).collect();
        if fields.len() != 8 {
            return Err("mechanism registry row must have eight columns".into());
        }
        let ch: u32 = fields[0].parse().map_err(|_| "invalid mechanism chapter")?;
        let activation_kind = match fields[2] {
            "NO_MECHANISM" => ActivationKind::NoMechanism,
            "ALWAYS" => ActivationKind::Always,
            "DENOMINATOR_GATED" => ActivationKind::DenominatorGated,
            _ => return Err(format!("chapter {ch}: unknown activation kind")),
        };
        let min_denominator = if activation_kind == ActivationKind::NoMechanism {
            if fields[1] != "-" || fields[3] != "-" || fields[4] != "-" {
                return Err(format!(
                    "chapter {ch}: NO_MECHANISM must not declare a probe"
                ));
            }
            None
        } else {
            let value: i64 = fields[3]
                .parse()
                .map_err(|_| format!("chapter {ch}: invalid denominator"))?;
            if value < 0
                || fields[1].is_empty()
                || fields[1] == "-"
                || !["metric:", "sql:", "admin:"]
                    .iter()
                    .any(|prefix| fields[4].starts_with(prefix))
            {
                return Err(format!("chapter {ch}: invalid mechanism/probe/denominator"));
            }
            Some(value)
        };
        let spec = MechanismSpec {
            ch,
            mechanism: fields[1].into(),
            activation_kind,
            min_denominator,
            probe: fields[4].into(),
            bootstrap_value: fields[5].into(),
            bootstrap_measured_at: fields[6].into(),
            note: fields[7].into(),
        };
        if !ids.insert(spec.ch) {
            return Err(format!("duplicate mechanism chapter {ch}"));
        }
        specs.push(spec);
    }
    if specs.is_empty() {
        return Err("empty mechanism registry".into());
    }
    Ok(specs)
}

/// Closed persisted/derived runtime status vocabulary (§1.14.1).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum MechanismStatus {
    /// A fresh, nonempty, scoped observation has a valid E2E witness.
    Active,
    /// A fresh, nonempty scan measured a denominator below its threshold.
    NotApplicableYet,
    /// Fresh observation or trustworthy execution evidence is missing.
    Stale,
}

impl MechanismStatus {
    /// Canonical database/CLI spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "ACTIVE",
            Self::NotApplicableYet => "NOT_APPLICABLE_YET",
            Self::Stale => "STALE",
        }
    }

    /// Reject unknown database statuses; do not silently replace them with a default.
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "ACTIVE" => Ok(Self::Active),
            "NOT_APPLICABLE_YET" => Ok(Self::NotApplicableYet),
            "STALE" => Ok(Self::Stale),
            _ => Err("unknown mechanism status".into()),
        }
    }
}

/// Explicit deployment/cell scope. UUID parsing belongs to the I/O boundary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ObservationTarget {
    /// Deployment UUID in canonical textual form.
    pub deployment_id: String,
    /// Cell UUID in canonical textual form.
    pub cell_id: String,
}

/// Parse the shared admin/xtask scope flags without accepting ignored or duplicate
/// options. UUID validity is checked by the database boundary before querying.
pub fn parse_target_args(args: &[String]) -> Result<ObservationTarget, String> {
    if args.len() != 4 {
        return Err("expected --deployment UUID --cell UUID".into());
    }
    let mut deployment_id = None;
    let mut cell_id = None;
    for pair in args.chunks_exact(2) {
        match pair[0].as_str() {
            "--deployment" if deployment_id.is_none() => deployment_id = Some(pair[1].clone()),
            "--cell" if cell_id.is_none() => cell_id = Some(pair[1].clone()),
            _ => return Err("unknown or duplicate mechanism scope argument".into()),
        }
    }
    Ok(ObservationTarget {
        deployment_id: deployment_id.ok_or("missing --deployment")?,
        cell_id: cell_id.ok_or("missing --cell")?,
    })
}

/// One actual database observation, including its immutable provenance.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct MechanismObservation {
    /// Database observation UUID.
    pub observation_id: String,
    /// Exact measured target.
    pub target: ObservationTarget,
    /// Canonical `ch:mechanism` join key.
    pub mechanism_id: String,
    /// Actual scalar reading.
    pub value: i64,
    /// Actual scanned population; null means unknown, never zero-filled.
    pub scanned_n: Option<i64>,
    /// UTC Unix microseconds from the measuring process.
    pub measured_at_micros: i64,
    /// Persisted cache, deliberately ignored by [`derive_status`].
    pub recorded_status: MechanismStatus,
    /// Versioned probe name; comparisons across versions are invalid.
    pub probe_version: String,
    /// Exact build identity, not a mutable branch name.
    pub binary_build: String,
    /// SHA-256 of the normalized scan domain; legacy rows can lack it.
    pub scope_hash: Option<String>,
}

/// Immutable link created by the controlled runner after a real before/after execution.
/// A pair of adjacent observations without this link is not an E2E witness.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct MechanismE2eEvidence {
    /// Completed run UUID.
    pub run_id: String,
    /// Target explicitly bound by the run record.
    pub target: ObservationTarget,
    /// Mechanism explicitly bound by the run record.
    pub mechanism_id: String,
    /// First actual reading.
    pub before: MechanismObservation,
    /// Must identify the selected latest observation, not an older successful run.
    pub after_observation_id: String,
    /// Exact scan-domain hash bound by the receipt itself, not only its endpoints.
    pub scope_hash: String,
    /// Probe version bound by the run record.
    pub probe_version: String,
    /// Binary identity bound by the run record.
    pub binary_build: String,
    /// Start of the controlled execution interval, in UTC Unix microseconds.
    pub started_at_micros: i64,
    /// Completion of the controlled execution interval, in UTC Unix microseconds.
    pub completed_at_micros: i64,
}

/// Recomputed status with an explicit reason suitable for gate diagnostics.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct DerivedMechanismStatus {
    /// Absent only for `NO_MECHANISM`.
    pub status: Option<MechanismStatus>,
    /// Machine-readable explanation; not a new runtime error classification.
    pub reason: &'static str,
}

fn stale(reason: &'static str) -> DerivedMechanismStatus {
    DerivedMechanismStatus {
        status: Some(MechanismStatus::Stale),
        reason,
    }
}

fn valid_hash(hash: Option<&str>) -> bool {
    hash.and_then(|s| s.strip_prefix("sha256:"))
        .is_some_and(|s| {
            s.len() == 64
                && s.bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        })
}

fn fresh(obs: &MechanismObservation, now: i64) -> bool {
    obs.scanned_n.is_some_and(|n| n > 0)
        && obs.value >= 0
        && obs.measured_at_micros <= now
        && now.saturating_sub(obs.measured_at_micros) <= OBSERVATION_MAX_AGE_MICROS
        && !obs.probe_version.is_empty()
        && !obs.binary_build.is_empty()
}

/// Derive §1.14 runtime status from the requested scope and live evidence only.
/// Stored `ACTIVE`, bootstrap data, unlinked deltas and cross-scope pairs cannot activate.
pub fn derive_status(
    spec: &MechanismSpec,
    target: &ObservationTarget,
    observation: Option<&MechanismObservation>,
    e2e: Option<&MechanismE2eEvidence>,
    now_micros: i64,
) -> DerivedMechanismStatus {
    if spec.activation_kind == ActivationKind::NoMechanism {
        return DerivedMechanismStatus {
            status: None,
            reason: "no_mechanism",
        };
    }
    let Some(after) = observation else {
        return stale("no_data");
    };
    if &after.target != target || after.mechanism_id != spec.id() {
        return stale("wrong_target");
    }
    if !fresh(after, now_micros) {
        return stale("invalid_or_stale_observation");
    }
    let Some(minimum) = spec.min_denominator else {
        return stale("invalid_spec");
    };
    if spec.activation_kind == ActivationKind::DenominatorGated && after.value < minimum {
        return DerivedMechanismStatus {
            status: Some(MechanismStatus::NotApplicableYet),
            reason: "below_denominator",
        };
    }
    if !valid_hash(after.scope_hash.as_deref()) {
        return stale("missing_e2e_scope");
    }
    let Some(run) = e2e else {
        return stale("no_e2e_evidence");
    };
    let before = &run.before;
    if run.run_id.is_empty()
        || &run.target != target
        || run.mechanism_id != spec.id()
        || &before.target != target
        || before.mechanism_id != spec.id()
        || run.after_observation_id != after.observation_id
        || before.observation_id == after.observation_id
        || run.probe_version != after.probe_version
        || before.probe_version != after.probe_version
        || run.binary_build != after.binary_build
        || before.binary_build != after.binary_build
        || !valid_hash(Some(&run.scope_hash))
        || after.scope_hash.as_deref() != Some(run.scope_hash.as_str())
        || before.scope_hash != after.scope_hash
        || !fresh(before, now_micros)
        || run.started_at_micros > before.measured_at_micros
        || before.measured_at_micros >= after.measured_at_micros
        || after.measured_at_micros > run.completed_at_micros
        || run.completed_at_micros > now_micros
    {
        return stale("invalid_e2e_binding");
    }
    if after.value <= before.value {
        return stale("no_positive_e2e_delta");
    }
    DerivedMechanismStatus {
        status: Some(MechanismStatus::Active),
        reason: "observed_e2e_delta",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const REGISTRY: &str = "```mechanism-registry\n1 | - | NO_MECHANISM | - | - | - | - | -\n12 | consensus | DENOMINATOR_GATED | 1 | admin:public.consensus_ready | 999 | 2026-08-24 | test\n22 | ledger | ALWAYS | 1 | metric:counter | - | - | test\n```";

    fn fixture() -> (MechanismSpec, MechanismObservation, MechanismE2eEvidence) {
        let spec = parse_registry(REGISTRY).unwrap().remove(1);
        let target = ObservationTarget {
            deployment_id: "deployment-a".into(),
            cell_id: "cell-a".into(),
        };
        let after = MechanismObservation {
            observation_id: "after".into(),
            target: target.clone(),
            mechanism_id: spec.id(),
            value: 1,
            scanned_n: Some(4),
            measured_at_micros: 30,
            recorded_status: MechanismStatus::Active,
            probe_version: "probe@1".into(),
            binary_build: "build-a".into(),
            scope_hash: Some(format!("sha256:{}", "a".repeat(64))),
        };
        let mut before = after.clone();
        before.observation_id = "before".into();
        before.value = 0;
        before.measured_at_micros = 20;
        let run = MechanismE2eEvidence {
            run_id: "run".into(),
            target,
            mechanism_id: spec.id(),
            before,
            after_observation_id: after.observation_id.clone(),
            probe_version: after.probe_version.clone(),
            scope_hash: after.scope_hash.clone().unwrap(),
            binary_build: after.binary_build.clone(),
            started_at_micros: 10,
            completed_at_micros: 40,
        };
        (spec, after, run)
    }

    #[test]
    fn live_denominator_not_bootstrap_and_no_fabricated_active() {
        let (spec, mut after, run) = fixture();
        assert_eq!(
            derive_status(&spec, &after.target, Some(&after), Some(&run), 50).status,
            Some(MechanismStatus::Active)
        );
        assert_eq!(
            derive_status(&spec, &after.target, Some(&after), None, 50).reason,
            "no_e2e_evidence"
        );
        after.value = 0;
        assert_eq!(
            derive_status(&spec, &after.target, Some(&after), None, 50).status,
            Some(MechanismStatus::NotApplicableYet)
        );
        assert_eq!(
            derive_status(&spec, &after.target, None, None, 50).reason,
            "no_data"
        );
    }

    #[test]
    fn empty_old_future_unknown_scope_observations_fail_closed() {
        let (spec, after, run) = fixture();
        for fault in 0..5 {
            let mut bad = after.clone();
            match fault {
                0 => bad.scanned_n = Some(0),
                1 => bad.scanned_n = None,
                2 => bad.scope_hash = None,
                3 => bad.measured_at_micros = 51,
                _ => bad.measured_at_micros = 49 - OBSERVATION_MAX_AGE_MICROS,
            }
            assert_eq!(
                derive_status(&spec, &bad.target, Some(&bad), Some(&run), 50).status,
                Some(MechanismStatus::Stale)
            );
        }
        let mut legacy = after.clone();
        legacy.value = 0;
        legacy.scope_hash = None;
        assert_eq!(
            derive_status(&spec, &legacy.target, Some(&legacy), None, 50).status,
            Some(MechanismStatus::NotApplicableYet)
        );
    }

    #[test]
    fn e2e_links_bind_scope_identity_time_and_positive_delta() {
        let (spec, after, run) = fixture();
        for fault in 0..10 {
            let mut bad = run.clone();
            match fault {
                0 => bad.target.cell_id = "other".into(),
                1 => bad.before.mechanism_id = "other".into(),
                2 => bad.after_observation_id = "older".into(),
                3 => bad.before.probe_version = "probe@2".into(),
                4 => bad.before.binary_build = "other".into(),
                5 => bad.before.scope_hash = Some(format!("sha256:{}", "b".repeat(64))),
                6 => bad.before.measured_at_micros = after.measured_at_micros,
                7 => bad.completed_at_micros = 51,
                8 => bad.before.value = after.value,
                _ => bad.scope_hash = format!("sha256:{}", "b".repeat(64)),
            }
            assert_eq!(
                derive_status(&spec, &after.target, Some(&after), Some(&bad), 50).status,
                Some(MechanismStatus::Stale)
            );
        }
        let mut other = after.target.clone();
        other.cell_id = "other".into();
        assert_eq!(
            derive_status(&spec, &other, Some(&after), Some(&run), 50).reason,
            "wrong_target"
        );
    }

    #[test]
    fn parser_rejects_second_authority_and_invalid_rows() {
        assert!(parse_registry(&format!("{REGISTRY}\n{REGISTRY}")).is_err());
        assert!(parse_registry(&REGISTRY.replace("DENOMINATOR_GATED", "MANUAL")).is_err());
        assert!(parse_registry(&REGISTRY.replace("| 1 | admin:", "| -1 | admin:")).is_err());
        assert_eq!(parse_registry(REGISTRY).unwrap().len(), 3);
        for value in ["ACTIVE", "NOT_APPLICABLE_YET", "STALE"] {
            assert_eq!(MechanismStatus::parse(value).unwrap().as_str(), value);
        }
    }
}
