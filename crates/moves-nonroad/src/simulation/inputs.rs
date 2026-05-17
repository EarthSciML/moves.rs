//! Pre-loaded input bundle for [`run_simulation`](super::run_simulation)
//! — the in-memory replacement for NONROAD's input *files*.
//!
//! In the Java↔Fortran bridge this task replaces, MOVES wrote ~30
//! fixed-width input files (`.POP`, `.ALO`, `.GRW`, …) to a worker
//! scratch directory and `nonroad.exe` read them back. The Rust
//! orchestrator instead parses its source data once, in memory, into a
//! [`NonroadInputs`] value — no scratch files, no re-parsing.
//!
//! # What this type carries
//!
//! [`NonroadInputs`] holds the two things the **driver loop** needs:
//!
//! - the population records, pre-grouped by SCC into [`SccGroup`]s —
//!   one group is what `getpop` returns per outer-loop pass
//!   (`nonroad.f` label `111`);
//! - the [`RunRegions`] selection tables the inner record loop filters
//!   against ([`plan_scc_group`](crate::driver::plan_scc_group)).
//!
//! The deeper per-record reference data — emission-factor, technology,
//! activity, growth, and allocation tables — is consumed not by the
//! driver loop but by the geography routines, behind the
//! [`GeographyExecutor`](super::GeographyExecutor) seam. A production
//! `GeographyExecutor` owns that data; keeping it out of
//! [`NonroadInputs`] keeps the driver-loop contract small and lets the
//! executor evolve independently.

use crate::driver::{DriverRecord, RunRegions};

/// One SCC group's worth of population records, in file order.
///
/// The Fortran `getpop` routine returns exactly this: all population
/// records sharing one Source Classification Code, ordered as they
/// appeared in the `.POP` input. `nonroad.f`'s outer loop processes
/// the groups one at a time; [`run_simulation`](super::run_simulation)
/// iterates [`NonroadInputs::scc_groups`] in the same way.
#[derive(Debug, Clone, PartialEq)]
pub struct SccGroup {
    /// The 10-character SCC shared by every record in [`records`](Self::records).
    pub scc: String,
    /// The group's population records, in `.POP`-file order. Growth
    /// pairs (a base record immediately followed by its projection
    /// record) must stay adjacent — the inner loop's lookahead
    /// ([`growth_pair`](crate::driver::growth_pair)) depends on it.
    pub records: Vec<DriverRecord>,
}

impl SccGroup {
    /// Bundle `records` under their shared `scc`.
    pub fn new(scc: impl Into<String>, records: Vec<DriverRecord>) -> Self {
        Self {
            scc: scc.into(),
            records,
        }
    }

    /// Number of population records in the group.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// `true` when the group carries no records. A no-op group: the
    /// driver still classifies its fuel but the record loop is empty.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

/// The complete pre-loaded input bundle handed to
/// [`run_simulation`](super::run_simulation).
///
/// See the module docs for the rationale behind carrying only the
/// driver-loop inputs (SCC groups + region selection) and leaving the
/// reference tables to the [`GeographyExecutor`](super::GeographyExecutor).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct NonroadInputs {
    /// Population records grouped by SCC — one [`SccGroup`] per outer-
    /// loop pass. Order is preserved into the output; sort upstream if
    /// a particular SCC order is wanted.
    pub scc_groups: Vec<SccGroup>,
    /// The run's state / county selection and the subcounty region
    /// list — Fortran `statcd`/`fipcod`/`reglst`. The inner record
    /// loop filters every record against these.
    pub regions: RunRegions,
}

impl NonroadInputs {
    /// Create an empty input bundle — no SCC groups, no region
    /// selection. A [`run_simulation`](super::run_simulation) over this
    /// produces an empty [`NonroadOutputs`](super::NonroadOutputs) with
    /// a successful completion message.
    pub fn new() -> Self {
        Self::default()
    }

    /// Append an [`SccGroup`] built from `scc` and `records`.
    ///
    /// Returns `&mut Self` so groups can be chained onto a freshly
    /// constructed bundle.
    pub fn push_group(&mut self, scc: impl Into<String>, records: Vec<DriverRecord>) -> &mut Self {
        self.scc_groups.push(SccGroup::new(scc, records));
        self
    }

    /// Total population records across every SCC group.
    pub fn record_count(&self) -> usize {
        self.scc_groups.iter().map(SccGroup::len).sum()
    }

    /// Number of SCC groups in the bundle.
    pub fn group_count(&self) -> usize {
        self.scc_groups.len()
    }

    /// `true` when the bundle has no SCC groups at all.
    pub fn is_empty(&self) -> bool {
        self.scc_groups.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(region: &str, hp: f32, pop: f32, year: i32) -> DriverRecord {
        DriverRecord {
            region_code: region.to_string(),
            hp_avg: hp,
            population: pop,
            pop_year: year,
        }
    }

    #[test]
    fn scc_group_reports_length_and_emptiness() {
        let group = SccGroup::new("2270001010", vec![rec("06037", 25.0, 100.0, 2020)]);
        assert_eq!(group.scc, "2270001010");
        assert_eq!(group.len(), 1);
        assert!(!group.is_empty());

        let empty = SccGroup::new("2265001010", Vec::new());
        assert!(empty.is_empty());
        assert_eq!(empty.len(), 0);
    }

    #[test]
    fn new_inputs_are_empty() {
        let inputs = NonroadInputs::new();
        assert!(inputs.is_empty());
        assert_eq!(inputs.group_count(), 0);
        assert_eq!(inputs.record_count(), 0);
    }

    #[test]
    fn push_group_accumulates_and_chains() {
        let mut inputs = NonroadInputs::new();
        inputs
            .push_group("2270001010", vec![rec("06037", 25.0, 100.0, 2020)])
            .push_group(
                "2265001010",
                vec![
                    rec("06037", 10.0, 50.0, 2020),
                    rec("06038", 10.0, 60.0, 2020),
                ],
            );
        assert_eq!(inputs.group_count(), 2);
        assert_eq!(inputs.record_count(), 3);
        assert!(!inputs.is_empty());
        assert_eq!(inputs.scc_groups[0].scc, "2270001010");
        assert_eq!(inputs.scc_groups[1].records.len(), 2);
    }

    #[test]
    fn record_count_sums_across_groups() {
        let inputs = NonroadInputs {
            scc_groups: vec![
                SccGroup::new("a", vec![rec("06037", 1.0, 1.0, 2020)]),
                SccGroup::new("b", Vec::new()),
                SccGroup::new(
                    "c",
                    vec![rec("06038", 1.0, 1.0, 2020), rec("06039", 1.0, 1.0, 2020)],
                ),
            ],
            regions: RunRegions::default(),
        };
        assert_eq!(inputs.record_count(), 3);
        assert_eq!(inputs.group_count(), 3);
    }
}
