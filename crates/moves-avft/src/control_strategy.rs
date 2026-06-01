//! AVFT internal control strategy — ports
//! `gov.epa.otaq.moves.master.implementation.ghg.internalcontrolstrategies.avft.AVFTStrategy`.
//!
//! # Role
//!
//! The AVFT control strategy takes a completed, gap-filled, projected AVFT
//! table and replaces the `AVFT` entry in the execution database so that
//! downstream emission calculators see the user-specified fleet-composition
//! fractions instead of the model defaults.
//!
//! It runs entirely in [`pre_run`](AvftControlStrategy::pre_run) — a
//! single call before the master loop begins. No per-iteration subscription
//! is needed because AVFT fractions are global (not location- or
//! time-varying at the granularity MOVES iterates).
//!
//! # Constructors
//!
//! * [`AvftControlStrategy::from_tool_inputs`] — builds the completed table
//! from raw user AVFT + defaults + spec by running the gap-fill and
//! projection logic (crate `moves-avft::tool`,) at construction
//! time. Use this when the completed table is not cached on disk.
//! * [`AvftControlStrategy::from_completed`] — accepts an already-built
//! table (e.g. loaded from a Parquet file via
//! [`crate::parquet_io::read_parquet`]). Use this when the AVFT CLI has
//! been run separately and its Parquet output is available.
//!
//! # Data plane
//!
//! [`pre_run`](AvftControlStrategy::pre_run) serialises the completed table
//! via [`TableRow`] and inserts it into the slow-tier
//! [`moves_framework::InMemoryStore`] under `"AVFT"`. All downstream
//! calculators that read from `ctx.tables()` will see the user-specified
//! fractions for the entire run.

use moves_framework::{
    DataFrameStore, InMemoryStore, InternalControlStrategy, IntoDataFrame, TableRow,
};
use polars::prelude::{DataFrame, DataType, NamedFrom, PolarsResult, Schema, Series};

use crate::error::Result;
use crate::model::{AvftRecord, AvftTable};
use crate::spec::ToolSpec;
use crate::tool::{run as run_tool, ToolInputs};

impl TableRow for AvftRecord {
    fn table_name() -> &'static str {
        "AVFT"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("modelYearID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("engTechID".into(), DataType::Int32),
            ("fuelEngFraction".into(), DataType::Float64),
        ])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "modelYearID".into(),
                    rows.iter().map(|r| r.model_year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelTypeID".into(),
                    rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "engTechID".into(),
                    rows.iter().map(|r| r.eng_tech_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelEngFraction".into(),
                    rows.iter()
                        .map(|r| r.fuel_eng_fraction)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| moves_framework::Error::RowExtraction {
                    table: "AVFT".into(),
                    row: 0,
                    column: col.into(),
                    message: e.to_string(),
                })?
                .i32()
                .map_err(|e| moves_framework::Error::RowExtraction {
                    table: "AVFT".into(),
                    row: 0,
                    column: col.into(),
                    message: e.to_string(),
                })
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| moves_framework::Error::RowExtraction {
                    table: "AVFT".into(),
                    row: 0,
                    column: col.into(),
                    message: e.to_string(),
                })?
                .f64()
                .map_err(|e| moves_framework::Error::RowExtraction {
                    table: "AVFT".into(),
                    row: 0,
                    column: col.into(),
                    message: e.to_string(),
                })
        };
        let src_type = get_i32("sourceTypeID")?;
        let model_year = get_i32("modelYearID")?;
        let fuel_type = get_i32("fuelTypeID")?;
        let eng_tech = get_i32("engTechID")?;
        let fraction = get_f64("fuelEngFraction")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| moves_framework::Error::RowExtraction {
                    table: "AVFT".into(),
                    row: i,
                    column: col.into(),
                    message: "null value".into(),
                };
                Ok(AvftRecord::new(
                    src_type.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    model_year.get(i).ok_or_else(|| null("modelYearID"))?,
                    fuel_type.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    eng_tech.get(i).ok_or_else(|| null("engTechID"))?,
                    fraction.get(i).ok_or_else(|| null("fuelEngFraction"))?,
                ))
            })
            .collect()
    }
}

/// AVFT (Alternative Vehicle Fuel Technology) internal control strategy.
///
/// See the [module docs](self) for the full description.
#[derive(Debug)]
pub struct AvftControlStrategy {
    completed: AvftTable,
}

impl AvftControlStrategy {
 /// Build by running the AVFT Tool on raw user inputs.
 ///
 /// Runs the gap-fill and projection steps defined in `spec` over
 /// `input` (the user-authored AVFT table) and `default` (the
 /// model-default AVFT). The resulting completed table is stored and
 /// applied in [`pre_run`](Self::pre_run).
 ///
 /// `known_fractions` is consulted only when at least one source type
 /// uses [`crate::spec::ProjectionMethod::KnownFractions`]; pass
 /// `&AvftTable::new()` if no source type needs it.
 ///
 /// # Errors
 ///
 /// Returns [`crate::error::Error::ToolSpec`] if `spec` fails validation, or
 /// [`crate::error::Error::ToolFailure`] if the tool cannot produce a valid output
 /// for any enabled source type.
    pub fn from_tool_inputs(
        spec: &ToolSpec,
        input: &AvftTable,
        default: &AvftTable,
        known_fractions: &AvftTable,
    ) -> Result<Self> {
        let inputs = ToolInputs {
            spec,
            input,
            default,
            known_fractions,
        };
        let report = run_tool(&inputs)?;
        Ok(Self {
            completed: report.output,
        })
    }

 /// Build from an already-completed AVFT table.
 ///
 /// Useful when the AVFT Tool has been run offline and its Parquet
 /// output loaded via [`crate::parquet_io::read_parquet`], or in tests
 /// that construct the table directly.
    pub fn from_completed(table: AvftTable) -> Self {
        Self { completed: table }
    }

 /// The completed AVFT table that will be applied in
 /// [`pre_run`](Self::pre_run).
    pub fn completed_table(&self) -> &AvftTable {
        &self.completed
    }
}

impl InternalControlStrategy for AvftControlStrategy {
    fn name(&self) -> &'static str {
        "AvftControlStrategy"
    }

    fn modified_tables(&self) -> &[&'static str] {
        &["AVFT"]
    }

    fn pre_run(
        &self,
        tables: &mut InMemoryStore,
    ) -> std::result::Result<(), moves_framework::Error> {
        let rows = self.completed.to_vec();
        let df = rows
            .into_dataframe()
            .map_err(|e| moves_framework::Error::Polars(e.to_string()))?;
        tables.insert("AVFT", df);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::AvftRecord;
    use crate::spec::{GapFillingMethod, MethodEntry, ProjectionMethod, ToolSpec};
    use moves_framework::{DataFrameStore, InMemoryStore};

    fn small_table(records: &[(i32, i32, i32, i32, f64)]) -> AvftTable {
        records
            .iter()
            .map(|&(st, my, fuel, eng, frac)| AvftRecord::new(st, my, fuel, eng, frac))
            .collect()
    }

    fn minimal_spec(source_type_id: i32) -> ToolSpec {
        ToolSpec {
            last_complete_model_year: 2020,
            analysis_year: 2020,
            methods: vec![MethodEntry {
                source_type_id,
                enabled: true,
                gap_filling: GapFillingMethod::Automatic,
                projection: ProjectionMethod::Constant,
            }],
        }
    }

    #[test]
    fn from_completed_preserves_table() {
        let t = small_table(&[(11, 2020, 1, 1, 0.7), (11, 2020, 2, 1, 0.3)]);
        let strategy = AvftControlStrategy::from_completed(t.clone());
        assert_eq!(
            strategy.completed_table().to_vec(),
            t.to_vec(),
            "from_completed must preserve the input table unchanged"
        );
    }

    #[test]
    fn name_is_stable() {
        let strategy = AvftControlStrategy::from_completed(AvftTable::new());
        assert_eq!(strategy.name(), "AvftControlStrategy");
    }

    #[test]
    fn modified_tables_contains_avft() {
        let strategy = AvftControlStrategy::from_completed(AvftTable::new());
        assert!(
            strategy.modified_tables().contains(&"AVFT"),
            "strategy must declare AVFT in modified_tables"
        );
    }

    #[test]
    fn pre_run_writes_avft_table_into_store() {
        let table = small_table(&[(11, 2020, 1, 1, 0.6), (11, 2020, 2, 1, 0.4)]);
        let strategy = AvftControlStrategy::from_completed(table);
        let mut store = InMemoryStore::new();
        strategy.pre_run(&mut store).expect("pre_run must not fail");
        let df = store
            .get("AVFT")
            .expect("AVFT must be present after pre_run");
        assert_eq!(df.height(), 2, "AVFT must have 2 rows");
    }

    #[test]
    fn pre_run_succeeds_with_empty_table() {
        let strategy = AvftControlStrategy::from_completed(AvftTable::new());
        let mut store = InMemoryStore::new();
        strategy
            .pre_run(&mut store)
            .expect("pre_run must not fail with empty table");
        let df = store
            .get("AVFT")
            .expect("AVFT must be present even when empty");
        assert_eq!(df.height(), 0);
    }

    #[test]
    fn from_tool_inputs_produces_complete_table() {
 // Single source type, single model year, defaults fully cover the gap.
        let default_t = small_table(&[(11, 2020, 1, 1, 0.8), (11, 2020, 2, 1, 0.2)]);
        let user_t = small_table(&[]);
        let known = AvftTable::new();
        let spec = minimal_spec(11);

        let strategy = AvftControlStrategy::from_tool_inputs(&spec, &user_t, &default_t, &known)
            .expect("tool must succeed with valid inputs");

        let rows = strategy.completed_table().to_vec();
        assert!(
            !rows.is_empty(),
            "completed table must have rows for source type 11"
        );
        assert_eq!(rows[0].source_type_id, 11);
    }

    #[test]
    fn strategy_is_trait_object_safe() {
        let strategy: Box<dyn InternalControlStrategy> =
            Box::new(AvftControlStrategy::from_completed(AvftTable::new()));
        assert_eq!(strategy.name(), "AvftControlStrategy");
    }
}
