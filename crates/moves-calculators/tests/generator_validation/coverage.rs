//! The fixture × generator coverage matrix.
//!
//! "Run all fixtures through the Rust generators" (the work item)
//! is a cross-product: for each onroad fixture, which generators does
//! a MOVES run of that fixture exercise? A generator's master-loop
//! subscription fires for a run iff the run exercises one of the
//! generator's emission processes — so the matrix is the join of
//! each fixture's [`OnroadFixture::process_ids`] against each
//! generator's [`subscribed_process_ids`].
//!
//! The matrix is what the canonical-capture diff iterates once the
//! gate activates: every exercised `(fixture, generator)` cell is one
//! `(produced table, canonical table)` pair to validate.
//!
//! # A note on scale
//!
//! The join is purely process-based — the defensible signal that
//! comes straight from the RunSpec. Model *scale* refines it (the
//! `MesoscaleLookup…` generators only fire under the Rates scale,
//! for instance); the harness records each fixture's
//! [`OnroadFixture::scale`] so the activation wiring can apply that
//! refinement, but the matrix itself does not encode scheduler rules
//! it would only be guessing at.

use moves_framework::Generator;

use super::fixtures::OnroadFixture;
use super::generators::subscribed_process_ids;

/// One cell of the coverage matrix — whether a fixture exercises a
/// generator, and the emission processes they share.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoverageCell {
 /// `true` when the fixture and generator share at least one
 /// emission process.
    pub exercised: bool,
 /// The shared process IDs, ascending. Empty iff `!exercised`.
    pub shared_processes: Vec<u32>,
}

/// The fixture × generator coverage matrix.
///
/// `cells[f][g]` is the [`CoverageCell`] for `fixture_names[f]` and
/// `generator_names[g]`.
#[derive(Debug, Clone)]
pub struct CoverageMatrix {
    fixture_names: Vec<String>,
    generator_names: Vec<String>,
    cells: Vec<Vec<CoverageCell>>,
}

impl CoverageMatrix {
 /// Build the matrix from the fixture and generator catalogues.
    pub fn build(fixtures: &[OnroadFixture], generators: &[Box<dyn Generator>]) -> Self {
        let generator_processes: Vec<_> = generators
            .iter()
            .map(|g| subscribed_process_ids(g.as_ref()))
            .collect();

        let cells: Vec<Vec<CoverageCell>> = fixtures
            .iter()
            .map(|fixture| {
                generator_processes
                    .iter()
                    .map(|gen_processes| {
                        let shared: Vec<u32> = fixture
                            .process_ids
                            .iter()
                            .copied()
                            .filter(|p| gen_processes.contains(p))
                            .collect();
                        CoverageCell {
                            exercised: !shared.is_empty(),
                            shared_processes: shared,
                        }
                    })
                    .collect()
            })
            .collect();

        Self {
            fixture_names: fixtures.iter().map(|f| f.name.clone()).collect(),
            generator_names: generators.iter().map(|g| g.name().to_string()).collect(),
            cells,
        }
    }

 /// The fixture names, in row order.
    pub fn fixture_names(&self) -> &[String] {
        &self.fixture_names
    }

 /// The generator names, in column order.
    pub fn generator_names(&self) -> &[String] {
        &self.generator_names
    }

 /// The cell for a `(fixture index, generator index)` pair.
    pub fn cell(&self, fixture: usize, generator: usize) -> &CoverageCell {
        &self.cells[fixture][generator]
    }

 /// Total exercised `(fixture, generator)` pairs — the number of
 /// table diffs the activated gate performs.
    pub fn exercised_pair_count(&self) -> usize {
        self.cells.iter().flatten().filter(|c| c.exercised).count()
    }

 /// The generators a fixture exercises, by name, in column order.
    pub fn generators_for_fixture(&self, fixture: &str) -> Vec<&str> {
        let Some(f) = self.fixture_names.iter().position(|n| n == fixture) else {
            return Vec::new();
        };
        self.cells[f]
            .iter()
            .enumerate()
            .filter(|(_, cell)| cell.exercised)
            .map(|(g, _)| self.generator_names[g].as_str())
            .collect()
    }

 /// The fixtures that exercise a generator, by name, in row order.
    pub fn fixtures_for_generator(&self, generator: &str) -> Vec<&str> {
        let Some(g) = self.generator_names.iter().position(|n| n == generator) else {
            return Vec::new();
        };
        self.cells
            .iter()
            .enumerate()
            .filter(|(_, row)| row[g].exercised)
            .map(|(f, _)| self.fixture_names[f].as_str())
            .collect()
    }

 /// Render the matrix as a Markdown table — one row per fixture,
 /// listing the processes it exercises and the generators that
 /// fire. Printed by the harness status banner under
 /// `cargo test -- --nocapture`.
    pub fn render_markdown(&self) -> String {
        let mut out = String::from(
            "| Fixture | Processes | Generators exercised |\n\
             |---------|-----------|----------------------|\n",
        );
        for (f, fixture) in self.fixture_names.iter().enumerate() {
            let mut processes: Vec<u32> = self.cells[f]
                .iter()
                .flat_map(|c| c.shared_processes.iter().copied())
                .collect();
            processes.sort_unstable();
            processes.dedup();
            let process_list = processes
                .iter()
                .map(u32::to_string)
                .collect::<Vec<_>>()
                .join(", ");
            let generators = self.generators_for_fixture(fixture).join(", ");
            out.push_str(&format!(
                "| `{fixture}` | {process_list} | {generators} |\n"
            ));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::super::fixtures::load_all_fixtures;
    use super::super::generators::all_generators;
    use super::*;

    fn matrix() -> CoverageMatrix {
        let fixtures = load_all_fixtures().expect("onroad fixtures must load");
        CoverageMatrix::build(&fixtures, &all_generators())
    }

    #[test]
    fn matrix_is_23_by_17() {
        let m = matrix();
        assert_eq!(m.fixture_names().len(), 23);
        assert_eq!(m.generator_names().len(), 17);
        assert!(m.cells.iter().all(|row| row.len() == 17));
    }

    #[test]
    fn every_fixture_exercises_at_least_one_generator() {
 // A fixture that fired no generator would be untestable by this
 // gate — and almost certainly a bug in the process join.
        let m = matrix();
        for fixture in m.fixture_names() {
            let gens = m.generators_for_fixture(fixture);
            assert!(
                !gens.is_empty(),
                "fixture `{fixture}` exercises no generator"
            );
        }
    }

    #[test]
    fn source_type_physics_is_exercised_by_no_fixture() {
 // It has no subscriptions — a helper, not master-loop scheduled.
        let m = matrix();
        assert!(
            m.fixtures_for_generator("SourceTypePhysics").is_empty(),
            "SourceTypePhysics should not be process-scheduled"
        );
    }

    #[test]
    fn shared_processes_are_consistent_with_the_exercised_flag() {
        let m = matrix();
        for f in 0..m.fixture_names().len() {
            for g in 0..m.generator_names().len() {
                let cell = m.cell(f, g);
                assert_eq!(cell.exercised, !cell.shared_processes.is_empty());
            }
        }
    }

    #[test]
    fn render_markdown_lists_every_fixture() {
        let m = matrix();
        let md = m.render_markdown();
        for fixture in m.fixture_names() {
            assert!(
                md.contains(fixture),
                "rendered matrix omits fixture `{fixture}`"
            );
        }
    }
}
