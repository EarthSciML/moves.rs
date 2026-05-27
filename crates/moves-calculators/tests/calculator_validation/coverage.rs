//! The fixture × calculator coverage matrix.
//!
//! "Run all fixtures through the Rust calculators" (Task 73) is a
//! cross-product: for each onroad fixture, which calculators does a
//! MOVES run of that fixture exercise? A calculator is exercised by a
//! fixture iff their `(pollutant_id, process_id)` sets overlap — the
//! fixture's `<pollutantprocessassociations>` against the calculator's
//! `registrations()`.
//!
//! The matrix is what the canonical-capture diff iterates once the
//! gate activates: every exercised `(fixture, calculator)` cell is one
//! `(produced table, canonical table)` pair to validate.
//!
//! # Chained-only calculators
//!
//! Some calculators — those with empty `registrations()` — are
//! "chained-only": they are invoked by their chain parent rather than
//! from a master-loop subscription. Such calculators appear as
//! [`CoverageKind::ChainedOnly`] in every coverage cell; they are
//! exercised whenever their parent is exercised, but the harness
//! records that via the parent's cell, not their own.

use std::collections::BTreeSet;

use moves_framework::Calculator;

use super::calculators::registered_ppa_ids;
use super::fixtures::OnroadFixture;

/// How a calculator relates to a fixture.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoverageKind {
    /// The fixture and calculator share at least one (pollutant, process) pair.
    Exercised {
        /// The shared (pollutant_id, process_id) pairs, sorted.
        shared_pairs: Vec<(u32, u32)>,
    },
    /// The fixture does not exercise any of the calculator's registered
    /// (pollutant, process) pairs.
    NotExercised,
    /// The calculator has no registrations — it is chained-only and
    /// runs when its chain parent fires.
    ChainedOnly,
}

impl CoverageKind {
    /// `true` when the calculator is exercised by this fixture
    /// (directly or via chain parent).
    pub fn is_exercised_or_chained(&self) -> bool {
        matches!(
            self,
            CoverageKind::Exercised { .. } | CoverageKind::ChainedOnly
        )
    }
}

/// One cell of the coverage matrix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoverageCell {
    pub kind: CoverageKind,
}

/// The fixture × calculator coverage matrix.
///
/// `cells[f][c]` is the [`CoverageCell`] for `fixture_names[f]` and
/// `calculator_names[c]`.
#[derive(Debug, Clone)]
pub struct CoverageMatrix {
    fixture_names: Vec<String>,
    calculator_names: Vec<String>,
    cells: Vec<Vec<CoverageCell>>,
}

#[allow(dead_code)]
impl CoverageMatrix {
    /// Build the matrix from the fixture and calculator catalogues.
    pub fn build(fixtures: &[OnroadFixture], calculators: &[Box<dyn Calculator>]) -> Self {
        let calc_ppa_sets: Vec<BTreeSet<(u32, u32)>> = calculators
            .iter()
            .map(|c| registered_ppa_ids(c.as_ref()))
            .collect();

        let cells: Vec<Vec<CoverageCell>> = fixtures
            .iter()
            .map(|fixture| {
                let fixture_ppas: BTreeSet<(u32, u32)> = fixture.ppa_ids.iter().copied().collect();
                calc_ppa_sets
                    .iter()
                    .map(|calc_ppas| {
                        let kind = if calc_ppas.is_empty() {
                            CoverageKind::ChainedOnly
                        } else {
                            let shared: Vec<(u32, u32)> =
                                calc_ppas.intersection(&fixture_ppas).copied().collect();
                            if shared.is_empty() {
                                CoverageKind::NotExercised
                            } else {
                                CoverageKind::Exercised {
                                    shared_pairs: shared,
                                }
                            }
                        };
                        CoverageCell { kind }
                    })
                    .collect()
            })
            .collect();

        CoverageMatrix {
            fixture_names: fixtures.iter().map(|f| f.name.clone()).collect(),
            calculator_names: calculators.iter().map(|c| c.name().to_string()).collect(),
            cells,
        }
    }

    /// The fixture names in matrix row order.
    pub fn fixture_names(&self) -> &[String] {
        &self.fixture_names
    }

    /// The calculator names in matrix column order.
    pub fn calculator_names(&self) -> &[String] {
        &self.calculator_names
    }

    /// The coverage cell for a given (fixture index, calculator index).
    pub fn cell(&self, fixture_idx: usize, calc_idx: usize) -> &CoverageCell {
        &self.cells[fixture_idx][calc_idx]
    }

    /// Iterator over all (fixture_name, calculator_name, cell) triples.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str, &CoverageCell)> {
        let fixture_names = self.fixture_names.as_slice();
        let calculator_names = self.calculator_names.as_slice();
        self.cells.iter().enumerate().flat_map(move |(fi, row)| {
            row.iter().enumerate().map(move |(ci, cell)| {
                (
                    fixture_names[fi].as_str(),
                    calculator_names[ci].as_str(),
                    cell,
                )
            })
        })
    }

    /// `true` when every fixture has at least one exercised or
    /// chained-only calculator.
    pub fn every_fixture_has_coverage(&self) -> bool {
        self.cells
            .iter()
            .all(|row| row.iter().any(|cell| cell.kind.is_exercised_or_chained()))
    }

    /// `true` when every calculator is exercised by at least one fixture,
    /// or is chained-only.
    pub fn every_calculator_has_coverage(&self) -> bool {
        (0..self.calculator_names.len()).all(|ci| {
            self.cells
                .iter()
                .any(|row| row[ci].kind.is_exercised_or_chained())
        })
    }

    /// Render a human-readable ASCII coverage matrix for `--nocapture` output.
    pub fn render(&self) -> String {
        let mut out = String::new();
        out.push_str("Calculator coverage matrix (E=Exercised, C=ChainedOnly, .=Not):\n");

        // Column headers (truncated calculator names).
        let header_len = 16;
        let col_width = 2;
        let row_label = " ".repeat(header_len);
        out.push_str(&row_label);
        for name in &self.calculator_names {
            let truncated = if name.len() > col_width {
                &name[name.len() - col_width..]
            } else {
                name
            };
            out.push_str(&format!("{truncated:>col_width$}"));
        }
        out.push('\n');

        for (fi, fixture_name) in self.fixture_names.iter().enumerate() {
            let label = if fixture_name.len() > header_len {
                &fixture_name[..header_len]
            } else {
                fixture_name
            };
            out.push_str(&format!("{label:<header_len$}"));
            for ci in 0..self.calculator_names.len() {
                let ch = match &self.cells[fi][ci].kind {
                    CoverageKind::Exercised { .. } => "E",
                    CoverageKind::ChainedOnly => "C",
                    CoverageKind::NotExercised => ".",
                };
                out.push_str(&format!("{ch:>col_width$}"));
            }
            out.push('\n');
        }

        // Summary row.
        let total_cells = self.fixture_names.len() * self.calculator_names.len();
        let exercised = self
            .cells
            .iter()
            .flat_map(|r| r.iter())
            .filter(|c| matches!(c.kind, CoverageKind::Exercised { .. }))
            .count();
        let chained = self
            .cells
            .iter()
            .flat_map(|r| r.iter())
            .filter(|c| matches!(c.kind, CoverageKind::ChainedOnly))
            .count();
        out.push_str(&format!(
            "\n{exercised}/{total_cells} cells exercised, \
             {chained} chained-only (same across all fixtures)\n"
        ));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_fixture(name: &str, ppa_ids: &[(u32, u32)]) -> OnroadFixture {
        use moves_runspec::ModelScale;
        let process_ids: Vec<u32> = ppa_ids.iter().map(|&(_, p)| p).collect();
        OnroadFixture {
            name: name.to_string(),
            path: std::path::PathBuf::from(format!("/tmp/{name}.xml")),
            is_onroad: true,
            scale: ModelScale::Inventory,
            domain: None,
            year: Some(2020),
            process_ids,
            ppa_ids: ppa_ids.to_vec(),
            description: None,
        }
    }

    #[derive(Debug)]
    struct StubCalculator {
        name: &'static str,
        regs: Vec<moves_data::PollutantProcessAssociation>,
    }

    impl Calculator for StubCalculator {
        fn name(&self) -> &'static str {
            self.name
        }
        fn subscriptions(&self) -> &[moves_framework::CalculatorSubscription] {
            &[]
        }
        fn registrations(&self) -> &[moves_data::PollutantProcessAssociation] {
            &self.regs
        }
        fn execute(
            &self,
            _ctx: &moves_framework::CalculatorContext,
        ) -> Result<moves_framework::CalculatorOutput, moves_framework::Error> {
            Ok(moves_framework::CalculatorOutput::empty())
        }
    }

    fn stub(name: &'static str, pairs: &[(u16, u16)]) -> Box<dyn Calculator> {
        let regs = pairs
            .iter()
            .map(|&(pol, proc)| moves_data::PollutantProcessAssociation {
                pollutant_id: moves_data::PollutantId(pol),
                process_id: moves_data::ProcessId(proc),
            })
            .collect();
        Box::new(StubCalculator { name, regs })
    }

    #[test]
    fn exercised_when_ppa_overlap() {
        let fixtures = vec![make_fixture("f1", &[(1, 1), (2, 1)])];
        let calcs: Vec<Box<dyn Calculator>> = vec![stub("CalcA", &[(1, 1)])];
        let matrix = CoverageMatrix::build(&fixtures, &calcs);
        assert!(
            matches!(matrix.cell(0, 0).kind, CoverageKind::Exercised { .. }),
            "should be exercised"
        );
    }

    #[test]
    fn not_exercised_when_no_ppa_overlap() {
        let fixtures = vec![make_fixture("f1", &[(1, 1)])];
        let calcs: Vec<Box<dyn Calculator>> = vec![stub("CalcA", &[(2, 2)])];
        let matrix = CoverageMatrix::build(&fixtures, &calcs);
        assert_eq!(matrix.cell(0, 0).kind, CoverageKind::NotExercised);
    }

    #[test]
    fn chained_only_when_no_registrations() {
        let fixtures = vec![make_fixture("f1", &[(1, 1)])];
        let calcs: Vec<Box<dyn Calculator>> = vec![stub("ChainedCalc", &[])];
        let matrix = CoverageMatrix::build(&fixtures, &calcs);
        assert_eq!(matrix.cell(0, 0).kind, CoverageKind::ChainedOnly);
    }

    #[test]
    fn exercised_shared_pairs_are_correct() {
        let fixtures = vec![make_fixture("f1", &[(1, 1), (2, 2), (3, 3)])];
        let calcs: Vec<Box<dyn Calculator>> = vec![stub("CalcA", &[(1, 1), (3, 3)])];
        let matrix = CoverageMatrix::build(&fixtures, &calcs);
        match &matrix.cell(0, 0).kind {
            CoverageKind::Exercised { shared_pairs } => {
                assert_eq!(shared_pairs, &[(1, 1), (3, 3)]);
            }
            other => panic!("expected Exercised, got {other:?}"),
        }
    }

    #[test]
    fn every_fixture_has_coverage_works() {
        let fixtures = vec![make_fixture("f1", &[(1, 1)]), make_fixture("f2", &[(2, 2)])];
        let calcs: Vec<Box<dyn Calculator>> =
            vec![stub("CalcA", &[(1, 1)]), stub("CalcB", &[(2, 2)])];
        let matrix = CoverageMatrix::build(&fixtures, &calcs);
        assert!(matrix.every_fixture_has_coverage());
    }

    #[test]
    fn render_produces_non_empty_string() {
        let fixtures = vec![make_fixture("f1", &[(1, 1)])];
        let calcs: Vec<Box<dyn Calculator>> = vec![stub("CalcA", &[(1, 1)])];
        let matrix = CoverageMatrix::build(&fixtures, &calcs);
        let rendered = matrix.render();
        assert!(
            rendered.contains('E'),
            "should contain 'E' for exercised cell"
        );
    }
}
