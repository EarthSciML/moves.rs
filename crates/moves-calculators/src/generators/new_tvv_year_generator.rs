//! `NewTvvYearGenerator` — promotes year-specific TVV coefficient tables to
//! canonical scratch names once per calendar year.
//!
//! Ports the `NewTVVYear` section of
//! `database/MultidayTankVaporVentingCalculator.sql`. That section creates
//! `stmyTVVCoeffs{year}` and `stmyTVVEquations{year}` in the execution
//! database, then caches them under the unsuffixed names `stmyTVVCoeffs` /
//! `stmyTVVEquations` for the downstream Processing section.
//!
//! The snapshot pipeline captures the year-suffixed execution-DB tables
//! (e.g. `stmyTVVCoeffs2020`). This generator promotes them to the canonical
//! names in the scratch namespace so the multiday-TVV calculator can find
//! them via its scratch-first lookup in `execute`.

use moves_calculator_info::{Granularity, Priority};
use moves_data::ProcessId;
use moves_framework::{
    CalculatorContext, CalculatorOutput, CalculatorSubscription, DataFrameStore, Error, Generator,
};

const NAME: &str = "NewTvvYearGenerator";

static INPUT_TABLES: &[&str] = &[];

static OUTPUT_TABLES: &[&str] = &["stmyTVVCoeffs", "stmyTVVEquations"];

#[derive(Debug)]
pub struct NewTvvYearGenerator {
    subscriptions: Vec<CalculatorSubscription>,
}

impl NewTvvYearGenerator {
    pub const NAME: &'static str = NAME;

    #[must_use]
    pub fn new() -> Self {
        let priority =
            Priority::parse("GENERATOR").expect("\"GENERATOR\" is a canonical MasterLoopPriority");
        Self {
            subscriptions: vec![CalculatorSubscription::new(
                ProcessId(12),
                Granularity::Year,
                priority,
            )],
        }
    }
}

impl Default for NewTvvYearGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl Generator for NewTvvYearGenerator {
    fn name(&self) -> &'static str {
        NAME
    }

    fn subscriptions(&self) -> &[CalculatorSubscription] {
        &self.subscriptions
    }

    fn input_tables(&self) -> &[&'static str] {
        INPUT_TABLES
    }

    fn output_tables(&self) -> &[&'static str] {
        OUTPUT_TABLES
    }

    fn execute(&self, ctx: &mut CalculatorContext) -> Result<CalculatorOutput, Error> {
        let year = ctx
            .position()
            .time
            .year
            .ok_or_else(|| Error::Polars("no year in iteration position".into()))?;

        for base in ["stmyTVVCoeffs", "stmyTVVEquations"] {
            let year_name = format!("{base}{year}");
            if let Some(arc_df) = ctx.tables().get(&year_name) {
                let df_owned = (*arc_df).clone();
                ctx.scratch_mut().insert(base, df_owned);
            }
        }

        Ok(CalculatorOutput::empty())
    }
}

pub fn factory() -> Box<dyn Generator> {
    Box::new(NewTvvYearGenerator::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use moves_framework::execution::execution_db::{
        ExecutionLocation, ExecutionTime, IterationPosition,
    };
    use moves_framework::{CalculatorContext, DataFrameStore, InMemoryStore};
    use polars::prelude::*;

    fn make_df(col: &str) -> DataFrame {
        let s = Series::new(col.into(), [1i32]);
        DataFrame::new(1, vec![s.into()]).unwrap()
    }

    fn position_year_2020() -> IterationPosition {
        IterationPosition {
            iteration: 0,
            process_id: Some(ProcessId(12)),
            location: ExecutionLocation::link(26, 26_161, 90, 5001),
            time: ExecutionTime::year(2020),
        }
    }

    #[test]
    fn generator_metadata() {
        let gen = NewTvvYearGenerator::new();
        assert_eq!(gen.name(), NAME);
        assert_eq!(gen.output_tables(), OUTPUT_TABLES);
        assert_eq!(gen.subscriptions().len(), 1);
        assert_eq!(gen.subscriptions()[0].process_id, ProcessId(12));
    }

    #[test]
    fn factory_builds_generator() {
        let gen = factory();
        assert_eq!(gen.name(), NAME);
    }

    #[test]
    fn execute_copies_year_suffixed_tables_to_scratch() {
        let mut store = InMemoryStore::new();
        store.insert("stmyTVVCoeffs2020", make_df("sourceTypeID"));
        store.insert("stmyTVVEquations2020", make_df("tvvEquation"));

        let mut ctx = CalculatorContext::with_position_and_tables(position_year_2020(), store);
        NewTvvYearGenerator::new()
            .execute(&mut ctx)
            .expect("execute ok");

        assert!(
            ctx.scratch().store.contains("stmyTVVCoeffs"),
            "stmyTVVCoeffs must be in scratch after execute"
        );
        assert!(
            ctx.scratch().store.contains("stmyTVVEquations"),
            "stmyTVVEquations must be in scratch after execute"
        );
    }

    #[test]
    fn execute_is_noop_when_year_tables_absent() {
        let mut ctx =
            CalculatorContext::with_position_and_tables(position_year_2020(), InMemoryStore::new());
        NewTvvYearGenerator::new()
            .execute(&mut ctx)
            .expect("execute ok even with missing tables");

        assert!(
            !ctx.scratch().store.contains("stmyTVVCoeffs"),
            "stmyTVVCoeffs must not appear in scratch when source table is absent"
        );
    }
}
