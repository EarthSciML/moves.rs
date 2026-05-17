//! The numerical-fidelity tolerance policy for Task 115.
//!
//! The bead fixes three comparison rules for diffing the Rust port
//! against the gfortran NONROAD reference:
//!
//! | Class            | Rule                       | Constant                       |
//! |------------------|----------------------------|--------------------------------|
//! | Energy quantity  | `1e-9` **relative**        | [`ENERGY_RELATIVE_TOLERANCE`]   |
//! | Count / index    | `1e-12` **absolute**       | [`COUNT_ABSOLUTE_TOLERANCE`]    |
//! | SCC/eqp/year key | **exact** match            | (no constant — bitwise `==`)    |
//!
//! [`classify`] maps each emitted `dbgemit` variable to its class;
//! [`compare`] applies the matching rule to one expected/actual pair.
//!
//! # A note on `real*4` precision
//!
//! NONROAD's reals are Fortran `real*4` (`f32`), whose machine
//! epsilon is ≈`1.19e-7` — *larger* than the `1e-9` relative
//! tolerance above. A `1e-9` relative bound is therefore tighter
//! than a single `f32` ULP: any energy quantity that is not
//! bit-identical between the port and the reference is reported.
//!
//! That strictness is deliberate. Task 115 is a *surface-everything*
//! characterization gate — it is supposed to expose every divergence
//! (`EXP`/`LOG`/`POW` differences, summation order, accumulated
//! rounding) so that Task 116 (`mo-490cm`, numerical-divergence
//! triage) can examine each one and, where a divergence is a
//! tolerable artifact, widen the budget for that specific
//! pollutant/equipment class. The constants below are the knobs
//! Task 116 turns.

use super::reference::Phase;

/// Relative tolerance for energy quantities (bead: `1e-9` relative).
pub const ENERGY_RELATIVE_TOLERANCE: f64 = 1e-9;

/// Absolute tolerance for counts and indices (bead: `1e-12`
/// absolute).
pub const COUNT_ABSOLUTE_TOLERANCE: f64 = 1e-12;

/// The comparison class a quantity falls into.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize)]
pub enum Quantity {
    /// A continuous physical or derived quantity — emissions,
    /// populations, growth factors, fractions. Compared with a
    /// relative tolerance ([`ENERGY_RELATIVE_TOLERANCE`]).
    Energy,
    /// An integer count or index — e.g. a record's year. Compared
    /// with an absolute tolerance ([`COUNT_ABSOLUTE_TOLERANCE`]),
    /// which for the small integers involved is effectively exact.
    Count,
    /// A key that identifies a record rather than measuring it —
    /// SCC, equipment code, year key. Compared bit-exactly.
    Key,
}

impl Quantity {
    /// A short human description of the rule, for divergence reports.
    pub fn rule(self) -> &'static str {
        match self {
            Quantity::Energy => "1e-9 relative",
            Quantity::Count => "1e-12 absolute",
            Quantity::Key => "exact",
        }
    }
}

/// The `dbgemit` value labels and their quantity class — the single
/// source of truth for [`classify`].
///
/// The label set is closed: it is exactly what the four
/// instrumentation patches in `characterization/nonroad-build/`
/// emit. A label that is *not* in this table is a signal that the
/// instrumentation changed and the table needs updating; [`classify`]
/// falls back to [`Quantity::Energy`] for the unknown label and
/// [`is_known`] lets the harness report the gap.
const CLASSIFIED_LABELS: &[(Phase, &str, Quantity)] = &[
    // getpop.f — per-SCC population apportionment.
    (Phase::Getpop, "popeqp", Quantity::Energy),
    (Phase::Getpop, "avghpc", Quantity::Energy),
    (Phase::Getpop, "usehrs", Quantity::Energy),
    (Phase::Getpop, "ipopyr", Quantity::Count),
    // agedist.f — age-distribution growth.
    (Phase::Agedist, "mdyrfrc", Quantity::Energy),
    (Phase::Agedist, "baspop", Quantity::Energy),
    // grwfac.f — growth-factor application.
    (Phase::Grwfac, "factor", Quantity::Energy),
    (Phase::Grwfac, "baseyearind", Quantity::Energy),
    (Phase::Grwfac, "growthyearind", Quantity::Energy),
    // clcems.f — exhaust-emissions calculation.
    (Phase::Clcems, "emsday", Quantity::Energy),
    (Phase::Clcems, "emsbmy", Quantity::Energy),
    (Phase::Clcems, "pop", Quantity::Energy),
    (Phase::Clcems, "mfrac", Quantity::Energy),
    (Phase::Clcems, "afac", Quantity::Energy),
    (Phase::Clcems, "dage", Quantity::Energy),
];

/// Classify one emitted variable into its [`Quantity`] class.
///
/// Unknown `(phase, label)` pairs fall back to [`Quantity::Energy`]
/// — the relative rule surfaces divergences rather than hiding them.
/// Pair this with [`is_known`] to detect labels the table does not
/// yet cover.
pub fn classify(phase: Phase, label: &str) -> Quantity {
    let label = label.trim();
    CLASSIFIED_LABELS
        .iter()
        .find(|(p, l, _)| *p == phase && *l == label)
        .map(|(_, _, q)| *q)
        .unwrap_or(Quantity::Energy)
}

/// Whether `(phase, label)` is a known `dbgemit` label. A `false`
/// here means [`classify`] used its fallback class.
pub fn is_known(phase: Phase, label: &str) -> bool {
    let label = label.trim();
    CLASSIFIED_LABELS
        .iter()
        .any(|(p, l, _)| *p == phase && *l == label)
}

/// The full classification table, for harness coverage checks.
pub fn classified_labels() -> &'static [(Phase, &'static str, Quantity)] {
    CLASSIFIED_LABELS
}

/// The outcome of comparing one expected/actual value pair.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize)]
pub struct Comparison {
    /// The quantity class the rule was drawn from.
    pub quantity: Quantity,
    /// `|expected - actual|`. `NaN`/`inf` when an operand is
    /// non-finite.
    pub abs_diff: f64,
    /// `abs_diff / max(|expected|, |actual|)`, or `0.0` when both
    /// operands are zero. `NaN`/`inf` when an operand is non-finite.
    pub rel_diff: f64,
    /// `true` when the pair satisfies its class's tolerance rule.
    pub within_tolerance: bool,
    /// `true` when either operand is `NaN` or infinite. A non-finite
    /// pair is always worth a human's attention even when the
    /// verdict is `within_tolerance` (e.g. both `NaN`): NONROAD's
    /// `real*4` arithmetic can silently produce non-finite values
    /// and the harness must not let them pass unremarked.
    pub non_finite: bool,
}

/// Compare an expected (reference) value against an actual (Rust
/// port) value under the rule for `quantity`.
///
/// Non-finite handling: two `NaN`s, or two identical infinities,
/// count as a tolerated match (the port faithfully reproduced the
/// reference) but are flagged via [`Comparison::non_finite`]. Any
/// other non-finite combination fails the comparison.
pub fn compare(expected: f64, actual: f64, quantity: Quantity) -> Comparison {
    let non_finite = !expected.is_finite() || !actual.is_finite();

    if non_finite {
        let matched = (expected.is_nan() && actual.is_nan())
            || (expected.is_infinite()
                && actual.is_infinite()
                && expected.is_sign_positive() == actual.is_sign_positive());
        return Comparison {
            quantity,
            abs_diff: (expected - actual).abs(),
            rel_diff: f64::NAN,
            within_tolerance: matched,
            non_finite: true,
        };
    }

    let abs_diff = (expected - actual).abs();
    let scale = expected.abs().max(actual.abs());
    let rel_diff = if scale == 0.0 { 0.0 } else { abs_diff / scale };

    let within_tolerance = match quantity {
        Quantity::Energy => rel_diff <= ENERGY_RELATIVE_TOLERANCE,
        Quantity::Count => abs_diff <= COUNT_ABSOLUTE_TOLERANCE,
        Quantity::Key => expected == actual,
    };

    Comparison {
        quantity,
        abs_diff,
        rel_diff,
        within_tolerance,
        non_finite: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_known_labels() {
        assert_eq!(classify(Phase::Clcems, "emsday"), Quantity::Energy);
        assert_eq!(classify(Phase::Agedist, "mdyrfrc"), Quantity::Energy);
        assert_eq!(classify(Phase::Getpop, "ipopyr"), Quantity::Count);
        assert_eq!(classify(Phase::Grwfac, "factor"), Quantity::Energy);
        assert!(is_known(Phase::Getpop, "popeqp"));
        assert!(is_known(Phase::Getpop, " ipopyr ")); // trimmed
    }

    #[test]
    fn unknown_label_falls_back_to_energy() {
        assert_eq!(classify(Phase::Clcems, "brand_new_var"), Quantity::Energy);
        assert!(!is_known(Phase::Clcems, "brand_new_var"));
        // A label real for one phase is unknown for another.
        assert!(!is_known(Phase::Agedist, "emsday"));
    }

    #[test]
    fn classification_table_is_non_empty_and_covers_all_phases() {
        let table = classified_labels();
        assert!(!table.is_empty());
        for phase in Phase::all() {
            assert!(
                table.iter().any(|(p, _, _)| *p == phase),
                "phase {phase} has no classified labels"
            );
        }
    }

    #[test]
    fn energy_relative_tolerance_boundary() {
        // Within: relative diff just under 1e-9.
        let c = compare(1.0, 1.0 + 9e-10, Quantity::Energy);
        assert!(c.within_tolerance);
        assert!(c.rel_diff < ENERGY_RELATIVE_TOLERANCE);
        // Outside: relative diff above 1e-9.
        let c = compare(1.0, 1.0 + 5e-9, Quantity::Energy);
        assert!(!c.within_tolerance);
        // abs_diff is ≈ 5e-9, give or take f64 representation error.
        assert!((4.9e-9..5.1e-9).contains(&c.abs_diff));
    }

    #[test]
    fn energy_uses_relative_not_absolute_scale() {
        // A large emissions value: 1e-9 relative is a big absolute slack.
        let big = compare(1.0e9, 1.0e9 + 0.5, Quantity::Energy);
        assert!(big.within_tolerance, "0.5 of 1e9 is within 1e-9 relative");
        // The same absolute gap on a small value fails.
        let small = compare(1.0e-3, 1.0e-3 + 0.5, Quantity::Energy);
        assert!(!small.within_tolerance);
    }

    #[test]
    fn energy_both_zero_is_a_match() {
        let c = compare(0.0, 0.0, Quantity::Energy);
        assert!(c.within_tolerance);
        assert_eq!(c.rel_diff, 0.0);
    }

    #[test]
    fn energy_zero_versus_nonzero_diverges() {
        let c = compare(0.0, 1e-6, Quantity::Energy);
        assert!(!c.within_tolerance);
        assert_eq!(c.rel_diff, 1.0);
    }

    #[test]
    fn count_uses_absolute_tolerance() {
        assert!(compare(2021.0, 2021.0 + 1e-13, Quantity::Count).within_tolerance);
        assert!(!compare(2021.0, 2022.0, Quantity::Count).within_tolerance);
        // 1e-9 relative would pass year 2021 vs 2021.000002; absolute
        // 1e-12 does not — counts must be (effectively) exact.
        assert!(!compare(2021.0, 2021.000002, Quantity::Count).within_tolerance);
    }

    #[test]
    fn key_requires_exact_match() {
        assert!(compare(2270002000.0, 2270002000.0, Quantity::Key).within_tolerance);
        assert!(!compare(2270002000.0, 2270002001.0, Quantity::Key).within_tolerance);
    }

    #[test]
    fn matching_non_finite_values_are_tolerated_but_flagged() {
        let nan = compare(f64::NAN, f64::NAN, Quantity::Energy);
        assert!(nan.within_tolerance);
        assert!(nan.non_finite);

        let inf = compare(f64::INFINITY, f64::INFINITY, Quantity::Energy);
        assert!(inf.within_tolerance);
        assert!(inf.non_finite);
    }

    #[test]
    fn mismatched_non_finite_values_fail() {
        assert!(!compare(f64::NAN, 1.0, Quantity::Energy).within_tolerance);
        assert!(!compare(1.0, f64::INFINITY, Quantity::Energy).within_tolerance);
        assert!(!compare(f64::INFINITY, f64::NEG_INFINITY, Quantity::Energy).within_tolerance);
        assert!(compare(f64::NAN, 1.0, Quantity::Energy).non_finite);
    }

    #[test]
    fn quantity_rule_strings_are_distinct() {
        assert_eq!(Quantity::Energy.rule(), "1e-9 relative");
        assert_eq!(Quantity::Count.rule(), "1e-12 absolute");
        assert_eq!(Quantity::Key.rule(), "exact");
    }
}
