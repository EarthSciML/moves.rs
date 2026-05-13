//! Per-cell filter validation.
//!
//! Mirrors `gov.epa.otaq.moves.master.framework.importers.ImporterManager`
//! `FILTER_*` constants and the `doesInclude(filterType, value)` dispatcher.
//!
//! The Java side resolves filters against a runspec-derived value set
//! (e.g. `FILTER_COUNTY` rejects rows whose `countyID` is not selected
//! in the RunSpec). When a runspec is supplied, this crate enforces
//! the same membership check; without one, membership filters degrade
//! to a no-op so the importer can be used standalone for offline
//! template-validation.
//!
//! ## Behavior parity
//!
//! Java's `ImporterManager.doesInclude` returns `true` for membership
//! filters (`Integer`/`String` data type) when the `TreeSet` is null or
//! empty — it skips the row only when there's an explicit allow-list
//! and the value falls outside it. We follow the same rule.
//!
//! Java's range filters (`zeroToOneFraction`, `zeroTo100Percentage`,
//! `ModelYear`, `ModelYearRange`, `GreaterThanZeroInt`) are enforced
//! unconditionally. `NonNegativeFloat`, `NonNegativeFloatDefault1`,
//! and `GreaterThanZeroFloat` accept *any* finite f64 — the comment in
//! the Java source explicitly notes "accept all floating point values
//! coming into system" because the underlying SQL type already rules
//! out non-numerics.

use std::collections::BTreeSet;

/// One [`Filter`] is one column-level constraint as declared by the
/// importer's `dataTableDescriptor` array on the Java side.
///
/// `None` is the empty-string sentinel Java uses for "no filter,
/// passthrough". Any [`Filter`] variant maps to a single Java
/// `FILTER_*` constant; we only carry the variants we actually use in
/// the project-only importers — mostly membership filters (sourceType,
/// county, zone, road type, hour-day, op-mode, polProcessID) plus the
/// numeric ranges (`NonNegativeFloat`, `zeroToOneFraction`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Filter {
    /// `ImporterManager.FILTER_COUNTY` — `countyID` must be in the runspec set.
    County,
    /// `ImporterManager.FILTER_ZONE` — `zoneID` must be in the runspec set.
    Zone,
    /// `ImporterManager.FILTER_ROAD_TYPE` — `roadTypeID` must be in the runspec set.
    RoadType,
    /// `ImporterManager.FILTER_SOURCE` — `sourceTypeID` must be in the runspec set.
    SourceType,
    /// `ImporterManager.FILTER_HOURDAY` — `hourDayID` must be in the runspec set.
    HourDay,
    /// `ImporterManager.FILTER_OPMODEID` — `opModeID` must be in the runspec set.
    OpMode,
    /// `ImporterManager.FILTER_POLPROCESSID` — `polProcessID` must be in the runspec set.
    PolProcess,
    /// `ImporterManager.FILTER_NON_NEGATIVE` — accept any finite f64.
    /// Java's `doesInclude` is a no-op here; we additionally reject
    /// NaN/inf because the column's SQL type would reject them too.
    NonNegativeFloat,
    /// `ImporterManager.FILTER_0_TO_1_FRACTION` — `0.0 ≤ value ≤ 1.0`.
    ZeroToOneFraction,
}

/// Outcome of a single-cell filter check.
///
/// `Pass` mirrors Java's `doesInclude → true` (no warning emitted).
/// `Filtered` mirrors `doesInclude → false`: Java keeps writing the
/// row but emits a `WARNING: <column> <value> is not used.` line and
/// (typically) tags the importer as NOT_READY further down. We propagate
/// the warning to the caller verbatim so it can choose to log,
/// hard-fail, or accumulate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterOutcome {
    Pass,
    Filtered { reason: String },
}

/// Runspec-derived membership sets that integer-typed filters check
/// against. Every field is `Option<BTreeSet<i64>>`: `None` means "no
/// allow-list configured for this dimension" — Java's null TreeSet —
/// and the filter degrades to a passthrough.
///
/// Standalone callers (template-validation, characterization fixtures)
/// can construct `RunSpecFilter::default()` and skip membership
/// checks entirely; runtime callers (the wired-in importer) populate
/// the sets from `RunSpec.geographicSelections`, `runSpec.sourceTypes`,
/// etc.
#[derive(Debug, Clone, Default)]
pub struct RunSpecFilter {
    pub county_ids: Option<BTreeSet<i64>>,
    pub zone_ids: Option<BTreeSet<i64>>,
    pub road_type_ids: Option<BTreeSet<i64>>,
    pub source_type_ids: Option<BTreeSet<i64>>,
    pub hour_day_ids: Option<BTreeSet<i64>>,
    pub op_mode_ids: Option<BTreeSet<i64>>,
    pub pol_process_ids: Option<BTreeSet<i64>>,
}

impl RunSpecFilter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Builder helper used by tests and small drivers.
    pub fn with_counties<I: IntoIterator<Item = i64>>(mut self, ids: I) -> Self {
        self.county_ids = Some(ids.into_iter().collect());
        self
    }

    pub fn with_zones<I: IntoIterator<Item = i64>>(mut self, ids: I) -> Self {
        self.zone_ids = Some(ids.into_iter().collect());
        self
    }

    pub fn with_road_types<I: IntoIterator<Item = i64>>(mut self, ids: I) -> Self {
        self.road_type_ids = Some(ids.into_iter().collect());
        self
    }

    pub fn with_source_types<I: IntoIterator<Item = i64>>(mut self, ids: I) -> Self {
        self.source_type_ids = Some(ids.into_iter().collect());
        self
    }

    pub fn with_hour_days<I: IntoIterator<Item = i64>>(mut self, ids: I) -> Self {
        self.hour_day_ids = Some(ids.into_iter().collect());
        self
    }

    pub fn with_op_modes<I: IntoIterator<Item = i64>>(mut self, ids: I) -> Self {
        self.op_mode_ids = Some(ids.into_iter().collect());
        self
    }

    pub fn with_pol_processes<I: IntoIterator<Item = i64>>(mut self, ids: I) -> Self {
        self.pol_process_ids = Some(ids.into_iter().collect());
        self
    }
}

/// Cell value as observed after CSV parsing — already coerced to the
/// column's declared type. Membership filters look at the integer
/// payload; numeric range filters look at the float payload. Strings
/// don't appear in any project-only column with a [`Filter`] attached,
/// so we leave that variant out.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CellValue {
    Int(i64),
    Float(f64),
}

impl CellValue {
    fn as_i64(self) -> Option<i64> {
        match self {
            CellValue::Int(v) => Some(v),
            CellValue::Float(v) => {
                let i = v as i64;
                if (i as f64 - v).abs() < 1e-9 {
                    Some(i)
                } else {
                    None
                }
            }
        }
    }

    fn as_f64(self) -> f64 {
        match self {
            CellValue::Int(v) => v as f64,
            CellValue::Float(v) => v,
        }
    }
}

impl Filter {
    /// Check a cell against this filter using the runspec-derived
    /// allow-lists (where applicable).
    pub fn check(&self, value: CellValue, runspec: &RunSpecFilter) -> FilterOutcome {
        match self {
            Filter::County => check_membership("countyID", value, runspec.county_ids.as_ref()),
            Filter::Zone => check_membership("zoneID", value, runspec.zone_ids.as_ref()),
            Filter::RoadType => {
                check_membership("roadTypeID", value, runspec.road_type_ids.as_ref())
            }
            Filter::SourceType => {
                check_membership("sourceTypeID", value, runspec.source_type_ids.as_ref())
            }
            Filter::HourDay => check_membership("hourDayID", value, runspec.hour_day_ids.as_ref()),
            Filter::OpMode => check_membership("opModeID", value, runspec.op_mode_ids.as_ref()),
            Filter::PolProcess => {
                check_membership("polProcessID", value, runspec.pol_process_ids.as_ref())
            }
            Filter::NonNegativeFloat => {
                let v = value.as_f64();
                if v.is_nan() || v.is_infinite() {
                    FilterOutcome::Filtered {
                        reason: format!("{v} is not a finite number"),
                    }
                } else {
                    FilterOutcome::Pass
                }
            }
            Filter::ZeroToOneFraction => {
                let v = value.as_f64();
                if !(0.0..=1.0).contains(&v) {
                    FilterOutcome::Filtered {
                        reason: format!("{v} is not a number from 0 to 1, inclusive"),
                    }
                } else {
                    FilterOutcome::Pass
                }
            }
        }
    }
}

fn check_membership(label: &str, value: CellValue, set: Option<&BTreeSet<i64>>) -> FilterOutcome {
    let Some(set) = set else {
        return FilterOutcome::Pass;
    };
    if set.is_empty() {
        return FilterOutcome::Pass;
    }
    let Some(id) = value.as_i64() else {
        return FilterOutcome::Filtered {
            reason: format!("{label} value is not an integer"),
        };
    };
    if set.contains(&id) {
        FilterOutcome::Pass
    } else {
        FilterOutcome::Filtered {
            reason: format!("{label} {id} is not used."),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn membership_passthrough_when_set_unconfigured() {
        let runspec = RunSpecFilter::default();
        assert_eq!(
            Filter::County.check(CellValue::Int(99), &runspec),
            FilterOutcome::Pass
        );
    }

    #[test]
    fn membership_passthrough_when_set_empty() {
        let runspec = RunSpecFilter::default().with_counties(std::iter::empty());
        assert_eq!(
            Filter::County.check(CellValue::Int(99), &runspec),
            FilterOutcome::Pass
        );
    }

    #[test]
    fn membership_filter_rejects_outside_allow_list() {
        let runspec = RunSpecFilter::default().with_counties([26161]);
        match Filter::County.check(CellValue::Int(6037), &runspec) {
            FilterOutcome::Filtered { reason } => assert!(reason.contains("6037")),
            other => panic!("expected filtered, got {other:?}"),
        }
    }

    #[test]
    fn membership_filter_accepts_inside_allow_list() {
        let runspec = RunSpecFilter::default().with_counties([26161, 6037]);
        assert_eq!(
            Filter::County.check(CellValue::Int(26161), &runspec),
            FilterOutcome::Pass
        );
    }

    #[test]
    fn non_negative_float_accepts_any_finite() {
        let runspec = RunSpecFilter::default();
        for v in [0.0_f64, 1.5, -3.0, 1e9] {
            assert_eq!(
                Filter::NonNegativeFloat.check(CellValue::Float(v), &runspec),
                FilterOutcome::Pass,
                "value {v} should pass FILTER_NON_NEGATIVE per Java parity"
            );
        }
    }

    #[test]
    fn non_negative_float_rejects_nan_and_inf() {
        let runspec = RunSpecFilter::default();
        for v in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(matches!(
                Filter::NonNegativeFloat.check(CellValue::Float(v), &runspec),
                FilterOutcome::Filtered { .. }
            ));
        }
    }

    #[test]
    fn zero_to_one_fraction_bounds() {
        let runspec = RunSpecFilter::default();
        assert_eq!(
            Filter::ZeroToOneFraction.check(CellValue::Float(0.0), &runspec),
            FilterOutcome::Pass
        );
        assert_eq!(
            Filter::ZeroToOneFraction.check(CellValue::Float(1.0), &runspec),
            FilterOutcome::Pass
        );
        assert!(matches!(
            Filter::ZeroToOneFraction.check(CellValue::Float(-0.01), &runspec),
            FilterOutcome::Filtered { .. }
        ));
        assert!(matches!(
            Filter::ZeroToOneFraction.check(CellValue::Float(1.01), &runspec),
            FilterOutcome::Filtered { .. }
        ));
    }

    #[test]
    fn membership_accepts_float_with_integer_value() {
        let runspec = RunSpecFilter::default().with_counties([42]);
        assert_eq!(
            Filter::County.check(CellValue::Float(42.0), &runspec),
            FilterOutcome::Pass
        );
        assert!(matches!(
            Filter::County.check(CellValue::Float(42.5), &runspec),
            FilterOutcome::Filtered { .. }
        ));
    }
}
