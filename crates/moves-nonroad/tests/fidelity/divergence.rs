//! The divergence-comparison engine and its report.
//!
//! [`compare_runs`] pairs a reference capture against the Rust
//! port's output — both expressed as [`ReferenceRecord`]s — and
//! produces a [`DivergenceReport`]: every value outside tolerance,
//! every structural mismatch (a record one side produced and the
//! other did not, or value vectors of unequal length), and the
//! per-phase tallies.
//!
//! The report is the artifact handed to Task 116 (`mo-490cm`,
//! NONROAD numerical-divergence triage): [`DivergenceReport::to_json`]
//! serialises it for a CI artifact, and the [`std::fmt::Display`]
//! impl renders the human-readable form a triager reads.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use super::reference::{Phase, RecordKey, ReferenceRecord};
use super::tolerance::{classify, compare, is_known, Quantity};

/// One scalar value that fell outside its tolerance rule.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct Divergence {
    /// Which record (phase, context, label) the value belongs to.
    pub key: RecordKey,
    /// Index of the value within the record's value vector.
    pub index: usize,
    /// The reference (gfortran NONROAD) value.
    pub expected: f64,
    /// The actual (`moves-nonroad` port) value.
    pub actual: f64,
    /// `|expected - actual|`.
    pub abs_diff: f64,
    /// Relative difference (see [`super::tolerance::compare`]).
    pub rel_diff: f64,
    /// The quantity class whose rule was applied.
    pub quantity: Quantity,
    /// `true` when either operand is `NaN`/infinite.
    pub non_finite: bool,
}

impl fmt::Display for Divergence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}[{}]  expected {:.9e}  actual {:.9e}  abs {:.3e}  rel {:.3e}  ({})",
            self.key,
            self.index,
            self.expected,
            self.actual,
            self.abs_diff,
            self.rel_diff,
            self.quantity.rule(),
        )?;
        if self.non_finite {
            f.write_str("  [NON-FINITE]")?;
        }
        Ok(())
    }
}

/// A record whose reference and actual value vectors had different
/// lengths — a structural divergence the value-wise diff cannot
/// express.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct CountMismatch {
    /// The record whose lengths disagreed.
    pub key: RecordKey,
    /// Value count in the reference capture.
    pub reference_len: usize,
    /// Value count in the port's output.
    pub actual_len: usize,
}

/// Per-phase tally line within a [`DivergenceReport`].
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize)]
pub struct PhaseTally {
    /// The phase this line summarises.
    pub phase: Phase,
    /// Scalar values compared for the phase.
    pub values_compared: usize,
    /// Of those, how many fell outside tolerance.
    pub divergences: usize,
}

/// The position of `phase` in [`Phase::all`] order — the index into
/// the per-phase tally arrays built by [`compare_runs`].
fn phase_index(phase: Phase) -> usize {
    match phase {
        Phase::Getpop => 0,
        Phase::Agedist => 1,
        Phase::Grwfac => 2,
        Phase::Clcems => 3,
    }
}

/// The result of diffing one fixture's reference capture against the
/// Rust port's output.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct DivergenceReport {
    /// The fixture this report covers (e.g. `nr-construction-state`).
    pub fixture: String,
    /// Total scalar values compared across all matched records.
    pub values_compared: usize,
    /// Count of comparisons where an operand was `NaN`/infinite
    /// (whether or not the pair matched).
    pub non_finite: usize,
    /// Every value that fell outside its tolerance rule.
    pub divergences: Vec<Divergence>,
    /// Records present in the reference but absent from the port's
    /// output — the port skipped a computation the reference made.
    pub missing_from_actual: Vec<RecordKey>,
    /// Records present in the port's output but absent from the
    /// reference — the port emitted something the reference did not.
    pub missing_from_reference: Vec<RecordKey>,
    /// Matched records whose value vectors had unequal lengths.
    pub count_mismatches: Vec<CountMismatch>,
    /// Matched records carrying a label the tolerance table does not
    /// classify — a signal the `dbgemit` instrumentation changed.
    pub unknown_labels: Vec<RecordKey>,
    /// Count of duplicate `(phase, context, label)` keys collapsed
    /// while indexing either side (each side should be unique).
    pub duplicate_keys: usize,
    /// Per-phase value/divergence tallies, in [`Phase::all`] order.
    pub phase_tallies: Vec<PhaseTally>,
}

impl DivergenceReport {
    /// Values that satisfied their tolerance rule.
    pub fn values_within_tolerance(&self) -> usize {
        self.values_compared - self.divergences.len()
    }

    /// `true` when nothing diverged: no out-of-tolerance values, no
    /// missing records on either side, no count mismatches.
    ///
    /// Unknown labels and tolerated non-finite values are surfaced
    /// as warnings but do not, by themselves, fail the report — they
    /// are flagged for a human to examine.
    pub fn passed(&self) -> bool {
        self.divergences.is_empty()
            && self.missing_from_actual.is_empty()
            && self.missing_from_reference.is_empty()
            && self.count_mismatches.is_empty()
    }

    /// Per-phase value/divergence tallies, in [`Phase::all`] order.
    pub fn phase_breakdown(&self) -> &[PhaseTally] {
        &self.phase_tallies
    }

    /// A single-line summary suitable for a test log header.
    pub fn summary(&self) -> String {
        format!(
            "fixture `{}`: {} value(s) compared, {} divergence(s), {} non-finite, {} structural \
             issue(s) — {}",
            self.fixture,
            self.values_compared,
            self.divergences.len(),
            self.non_finite,
            self.missing_from_actual.len()
                + self.missing_from_reference.len()
                + self.count_mismatches.len(),
            if self.passed() {
                "PASS"
            } else {
                "DIVERGENCES FOUND"
            },
        )
    }

    /// Serialise the report as pretty-printed JSON for the Task 116
    /// triage handoff and CI artifact upload.
    pub fn to_json(&self) -> String {
        // The report is plain data (strings, numbers, enums) — serde
        // cannot fail to serialise it; fall back rather than panic.
        serde_json::to_string_pretty(self)
            .unwrap_or_else(|e| format!("{{\"serialization_error\":\"{e}\"}}"))
    }
}

impl fmt::Display for DivergenceReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "NONROAD fidelity report — fixture `{}`", self.fixture)?;
        writeln!(f, "  values compared:         {}", self.values_compared)?;
        writeln!(
            f,
            "  within tolerance:        {}",
            self.values_within_tolerance()
        )?;
        writeln!(f, "  divergences:             {}", self.divergences.len())?;
        writeln!(f, "  non-finite values:       {}", self.non_finite)?;
        writeln!(
            f,
            "  records missing (port):  {}",
            self.missing_from_actual.len()
        )?;
        writeln!(
            f,
            "  records missing (ref):   {}",
            self.missing_from_reference.len()
        )?;
        writeln!(
            f,
            "  count mismatches:        {}",
            self.count_mismatches.len()
        )?;
        writeln!(
            f,
            "  unknown labels:          {}",
            self.unknown_labels.len()
        )?;
        if self.duplicate_keys > 0 {
            writeln!(f, "  duplicate keys:          {}", self.duplicate_keys)?;
        }
        writeln!(
            f,
            "  VERDICT: {}",
            if self.passed() {
                "PASS"
            } else {
                "DIVERGENCES FOUND"
            }
        )?;

        // Cap the inline divergence listing; the full set is in the
        // JSON form. A triage run with thousands of divergences must
        // not flood the test log.
        const MAX_SHOWN: usize = 50;
        if !self.divergences.is_empty() {
            writeln!(f, "\n  divergences ({}):", self.divergences.len())?;
            for d in self.divergences.iter().take(MAX_SHOWN) {
                writeln!(f, "    {d}")?;
            }
            if self.divergences.len() > MAX_SHOWN {
                writeln!(
                    f,
                    "    ... {} more (see JSON report)",
                    self.divergences.len() - MAX_SHOWN
                )?;
            }
        }
        for cm in &self.count_mismatches {
            writeln!(
                f,
                "  count mismatch: {} — reference {} value(s), port {}",
                cm.key, cm.reference_len, cm.actual_len
            )?;
        }
        for key in self.missing_from_actual.iter().take(MAX_SHOWN) {
            writeln!(f, "  missing from port output: {key}")?;
        }
        for key in self.missing_from_reference.iter().take(MAX_SHOWN) {
            writeln!(f, "  missing from reference:   {key}")?;
        }
        for key in &self.unknown_labels {
            writeln!(f, "  unclassified label (update tolerance table): {key}")?;
        }
        Ok(())
    }
}

/// Index records by their pairing key. On a duplicate key the first
/// record wins and the duplicate count is incremented — each side of
/// a real capture should have one record per `(phase, context,
/// label)`.
fn index_by_key(records: &[ReferenceRecord]) -> (BTreeMap<RecordKey, &ReferenceRecord>, usize) {
    let mut map = BTreeMap::new();
    let mut duplicates = 0;
    for record in records {
        if map.insert(record.key(), record).is_some() {
            duplicates += 1;
        }
    }
    (map, duplicates)
}

/// Diff a reference capture against the Rust port's output.
///
/// Records are paired by [`ReferenceRecord::key`] — phase, canonical
/// context, label. Matched records have their value vectors compared
/// element-wise under the [`super::tolerance`] policy; unmatched
/// records on either side, and length disagreements, are recorded as
/// structural divergences.
pub fn compare_runs(
    fixture: impl Into<String>,
    reference: &[ReferenceRecord],
    actual: &[ReferenceRecord],
) -> DivergenceReport {
    let (ref_map, ref_dups) = index_by_key(reference);
    let (act_map, act_dups) = index_by_key(actual);

    let mut report = DivergenceReport {
        fixture: fixture.into(),
        values_compared: 0,
        non_finite: 0,
        divergences: Vec::new(),
        missing_from_actual: Vec::new(),
        missing_from_reference: Vec::new(),
        count_mismatches: Vec::new(),
        unknown_labels: Vec::new(),
        duplicate_keys: ref_dups + act_dups,
        phase_tallies: Vec::new(),
    };

    // Per-phase scalar-value counters, indexed by `phase_index`.
    let mut compared_by_phase = [0usize; 4];

    // Deterministic iteration over the union of both key sets.
    let keys: BTreeSet<&RecordKey> = ref_map.keys().chain(act_map.keys()).collect();

    for key in keys {
        match (ref_map.get(key), act_map.get(key)) {
            (Some(ref_rec), Some(act_rec)) => {
                if !is_known(key.phase, &key.label) {
                    report.unknown_labels.push((*key).clone());
                }
                let quantity = classify(key.phase, &key.label);

                if ref_rec.values.len() != act_rec.values.len() {
                    report.count_mismatches.push(CountMismatch {
                        key: (*key).clone(),
                        reference_len: ref_rec.values.len(),
                        actual_len: act_rec.values.len(),
                    });
                }

                // Compare the common prefix value-by-value. A length
                // disagreement is already recorded above; comparing
                // the overlap still surfaces value divergences.
                let common = ref_rec.values.len().min(act_rec.values.len());
                for index in 0..common {
                    let expected = ref_rec.values[index];
                    let actual_v = act_rec.values[index];
                    let cmp = compare(expected, actual_v, quantity);
                    report.values_compared += 1;
                    compared_by_phase[phase_index(key.phase)] += 1;
                    if cmp.non_finite {
                        report.non_finite += 1;
                    }
                    if !cmp.within_tolerance {
                        report.divergences.push(Divergence {
                            key: (*key).clone(),
                            index,
                            expected,
                            actual: actual_v,
                            abs_diff: cmp.abs_diff,
                            rel_diff: cmp.rel_diff,
                            quantity,
                            non_finite: cmp.non_finite,
                        });
                    }
                }
            }
            (Some(_), None) => report.missing_from_actual.push((*key).clone()),
            (None, Some(_)) => report.missing_from_reference.push((*key).clone()),
            (None, None) => unreachable!("key came from the union of both maps"),
        }
    }

    report.phase_tallies = Phase::all()
        .into_iter()
        .map(|phase| PhaseTally {
            phase,
            values_compared: compared_by_phase[phase_index(phase)],
            divergences: report
                .divergences
                .iter()
                .filter(|d| d.key.phase == phase)
                .count(),
        })
        .collect();

    report
}

#[cfg(test)]
mod tests {
    use super::super::reference::Context;
    use super::*;

    fn rec(phase: Phase, ctx: &str, label: &str, values: &[f64]) -> ReferenceRecord {
        ReferenceRecord::new(phase, Context::parse(ctx), label, values.to_vec())
    }

    #[test]
    fn identical_runs_have_no_divergences() {
        let recs = vec![
            rec(Phase::Agedist, "call=1,fips=26000", "baspop", &[1000.0]),
            rec(
                Phase::Agedist,
                "call=1,fips=26000",
                "mdyrfrc",
                &[0.1, 0.5, 0.4],
            ),
        ];
        let report = compare_runs("identical", &recs, &recs.clone());
        assert!(report.passed());
        assert_eq!(report.values_compared, 4);
        assert_eq!(report.values_within_tolerance(), 4);
        assert!(report.divergences.is_empty());
        assert!(report.summary().contains("PASS"));
    }

    #[test]
    fn a_perturbed_value_is_one_divergence() {
        let reference = vec![rec(Phase::Clcems, "call=1", "emsday", &[1.0, 2.0, 3.0])];
        // Middle value perturbed beyond 1e-9 relative.
        let actual = vec![rec(
            Phase::Clcems,
            "call=1",
            "emsday",
            &[1.0, 2.0 + 1e-6, 3.0],
        )];
        let report = compare_runs("perturbed", &reference, &actual);
        assert!(!report.passed());
        assert_eq!(report.divergences.len(), 1);
        let d = &report.divergences[0];
        assert_eq!(d.index, 1);
        assert_eq!(d.expected, 2.0);
        assert!((d.abs_diff - 1e-6).abs() < 1e-15);
        assert_eq!(d.quantity, Quantity::Energy);
    }

    #[test]
    fn tiny_perturbation_within_tolerance_is_no_divergence() {
        let reference = vec![rec(Phase::Clcems, "call=1", "emsday", &[1.0e6])];
        let actual = vec![rec(Phase::Clcems, "call=1", "emsday", &[1.0e6 + 1.0e-4])];
        let report = compare_runs("tiny", &reference, &actual);
        assert!(report.passed());
    }

    #[test]
    fn missing_records_are_reported_on_both_sides() {
        let reference = vec![
            rec(Phase::Getpop, "call=1", "popeqp", &[5.0]),
            rec(Phase::Getpop, "call=2", "popeqp", &[6.0]),
        ];
        let actual = vec![
            rec(Phase::Getpop, "call=1", "popeqp", &[5.0]),
            rec(Phase::Getpop, "call=3", "popeqp", &[7.0]),
        ];
        let report = compare_runs("missing", &reference, &actual);
        assert!(!report.passed());
        assert_eq!(report.missing_from_actual.len(), 1);
        assert_eq!(report.missing_from_actual[0].context, "call=2");
        assert_eq!(report.missing_from_reference.len(), 1);
        assert_eq!(report.missing_from_reference[0].context, "call=3");
    }

    #[test]
    fn count_mismatch_is_recorded_and_prefix_still_compared() {
        let reference = vec![rec(Phase::Agedist, "call=1", "mdyrfrc", &[0.1, 0.2, 0.3])];
        let actual = vec![rec(Phase::Agedist, "call=1", "mdyrfrc", &[0.1, 0.9])];
        let report = compare_runs("count", &reference, &actual);
        assert!(!report.passed());
        assert_eq!(report.count_mismatches.len(), 1);
        assert_eq!(report.count_mismatches[0].reference_len, 3);
        assert_eq!(report.count_mismatches[0].actual_len, 2);
        // The overlapping prefix is still diffed: index 1 (0.2 vs 0.9).
        assert_eq!(report.values_compared, 2);
        assert_eq!(report.divergences.len(), 1);
        assert_eq!(report.divergences[0].index, 1);
    }

    #[test]
    fn unknown_label_is_flagged_but_does_not_fail_alone() {
        let recs = vec![rec(Phase::Clcems, "call=1", "mystery_var", &[1.0])];
        let report = compare_runs("unknown", &recs, &recs.clone());
        assert!(
            report.passed(),
            "a tolerated unknown label is a warning, not a failure"
        );
        assert_eq!(report.unknown_labels.len(), 1);
    }

    #[test]
    fn non_finite_values_are_counted() {
        let reference = vec![rec(Phase::Clcems, "call=1", "emsday", &[f64::NAN, 1.0])];
        let actual = vec![rec(Phase::Clcems, "call=1", "emsday", &[f64::NAN, 1.0])];
        let report = compare_runs("nonfinite", &reference, &actual);
        // Both NaN → tolerated match, but flagged.
        assert!(report.passed());
        assert_eq!(report.non_finite, 1);
    }

    #[test]
    fn duplicate_keys_are_counted() {
        let recs = vec![
            rec(Phase::Getpop, "call=1", "popeqp", &[1.0]),
            rec(Phase::Getpop, "call=1", "popeqp", &[2.0]),
        ];
        let report = compare_runs("dup", &recs, &recs.clone());
        assert_eq!(report.duplicate_keys, 2); // one per side
    }

    #[test]
    fn report_serialises_to_json() {
        let reference = vec![rec(Phase::Clcems, "call=1", "emsday", &[1.0])];
        let actual = vec![rec(Phase::Clcems, "call=1", "emsday", &[2.0])];
        let report = compare_runs("json", &reference, &actual);
        let json = report.to_json();
        assert!(json.contains("\"fixture\": \"json\""));
        assert!(json.contains("\"divergences\""));
        // Round-trips back to an equal value through serde_json.
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["fixture"], "json");
        assert_eq!(parsed["divergences"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn display_renders_verdict_and_divergences() {
        let reference = vec![rec(Phase::Clcems, "call=1", "emsday", &[1.0])];
        let actual = vec![rec(Phase::Clcems, "call=1", "emsday", &[5.0])];
        let report = compare_runs("display", &reference, &actual);
        let text = format!("{report}");
        assert!(text.contains("DIVERGENCES FOUND"));
        assert!(text.contains("emsday"));
    }

    #[test]
    fn phase_breakdown_tallies_values_per_phase() {
        let recs = vec![
            rec(Phase::Agedist, "call=1", "baspop", &[1.0]),
            rec(Phase::Agedist, "call=1", "mdyrfrc", &[0.1, 0.9]),
            rec(Phase::Clcems, "call=1", "emsday", &[5.0]),
        ];
        let report = compare_runs("breakdown", &recs, &recs.clone());
        let tallies = report.phase_breakdown();
        assert_eq!(tallies.len(), 4);
        let agedist = tallies.iter().find(|t| t.phase == Phase::Agedist).unwrap();
        assert_eq!(agedist.values_compared, 3); // baspop(1) + mdyrfrc(2)
        assert_eq!(agedist.divergences, 0);
        let clcems = tallies.iter().find(|t| t.phase == Phase::Clcems).unwrap();
        assert_eq!(clcems.values_compared, 1);
        let getpop = tallies.iter().find(|t| t.phase == Phase::Getpop).unwrap();
        assert_eq!(getpop.values_compared, 0);
    }
}
