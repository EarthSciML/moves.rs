//! OnRoadRetrofit internal control strategy — ports
//! `gov.epa.otaq.moves.master.implementation.ghg.internalcontrolstrategies.onroadretrofit.OnRoadRetrofit`.
//!
//! # Role
//!
//! The OnRoadRetrofit control strategy applies emission-reduction adjustments
//! to account for retrofit programs: a fraction of the fleet has been
//! equipped with emission-control devices, reducing emissions by a specified
//! effectiveness factor.
//!
//! For each `(sourceType, modelYear, pollutant, process)` combination, the
//! combined emission adjustment factor is:
//!
//! ```text
//! factor = ∏ over active programs p of (1 - p.fraction * p.effectiveness)
//! ```
//!
//! where "active" means `p.retrofit_year_id ≤ analysis_year` and the model
//! year falls within `[p.start_model_year, p.end_model_year]`.
//!
//! # Lifecycle
//!
//! The strategy runs entirely in [`pre_run`](OnRoadRetrofitStrategy::pre_run).
//! Per-iteration subscriptions are not needed because the retrofit programs
//! are indexed by model year and retrofit year — they do not vary across
//! counties or months within a single MOVES run.
//!
//! # Mechanism (post-output)
//!
//! Canonical OnRoadRetrofit is a *post-output* transform, not an input-table
//! mutation: `CompiledLine.buildSQL` issues `UPDATE MOVESWorkerOutput SET
//! emissionQuant = emissionQuant * (retrofitFactor + nonRetrofitFactor)` keyed
//! by `(pollutant, process, fuel, source, yearID, modelYearID)`. The port
//! mirrors this in [`apply_to_output`](OnRoadRetrofitStrategy::apply_to_output),
//! which the engine calls on the aggregated emission records after the streaming
//! aggregator is drained; the per-record factor comes from
//! [`RetrofitTable::combined_factor`](crate::model::RetrofitTable::combined_factor).
//! [`pre_run`](OnRoadRetrofitStrategy::pre_run) loads + compiles the programs
//! from the `onRoadRetrofit` execution table (an empty table is a clean no-op).
//!
//! Known gap: canonical also keys on `fuelTypeID` (retrofit is diesel-only);
//! [`RetrofitRecord`](crate::model::RetrofitRecord) does not yet carry it, so
//! matching is on `(source, modelYear, pollutant, process, year)` only. And
//! end-to-end numerical fidelity is unverified (no applied-retrofit canonical
//! capture exists in-repo), though the factor + scaling match the Java formula.

use std::sync::Mutex;

use moves_data::output_schema::EmissionRecord;
use moves_framework::{DataFrameStore, InMemoryStore, InternalControlStrategy};
use polars::prelude::DataFrame;

use crate::model::{RetrofitRecord, RetrofitTable};

/// Execution-DB table the retrofit programs are read from in `pre_run`.
const ONROAD_RETROFIT_TABLE: &str = "onroadretrofit";

/// OnRoadRetrofit internal control strategy.
///
/// See the [module docs](self) for the full description.
///
/// The compiled programs are held behind a [`Mutex`] because the trait methods
/// take `&self` (and the strategy is shared as `Arc<dyn ...>`): `pre_run` loads
/// them from the execution DB and [`apply_to_output`](Self::apply_to_output)
/// reads them. Both run single-threaded in the engine.
#[derive(Debug, Default)]
pub struct OnRoadRetrofitStrategy {
    programs: Mutex<RetrofitTable>,
}

impl OnRoadRetrofitStrategy {
    /// Build from a [`RetrofitTable`] already loaded from user input.
    ///
    /// In the live run path the table is loaded from the `onRoadRetrofit`
    /// execution-DB table in [`pre_run`](InternalControlStrategy::pre_run)
    /// instead; pass [`RetrofitTable::new`] there.
    pub fn new(programs: RetrofitTable) -> Self {
        Self {
            programs: Mutex::new(programs),
        }
    }

    /// Number of compiled retrofit program rows currently held.
    pub fn program_count(&self) -> usize {
        self.programs
            .lock()
            .expect("retrofit programs mutex poisoned")
            .len()
    }
}

impl InternalControlStrategy for OnRoadRetrofitStrategy {
    fn name(&self) -> &'static str {
        "OnRoadRetrofitStrategy"
    }

    fn pre_run(
        &self,
        tables: &mut InMemoryStore,
    ) -> std::result::Result<(), moves_framework::Error> {
        // Canonical `OnRoadRetrofitStrategy.loadFromDB()` reads the
        // `onRoadRetrofit` execution table. If the run supplies that table, load
        // and compile it; otherwise keep whatever was passed to `new` (empty in
        // the live path → a clean no-op, mirroring the `dbLines.size() <= 0`
        // early-return in `subscribeToMe`). The actual emission scaling is a
        // post-output transform applied in `apply_to_output`, not an input-table
        // mutation, so nothing is written back to `tables` here.
        if let Some(df) = tables.get(ONROAD_RETROFIT_TABLE) {
            let loaded = retrofit_table_from_dataframe(&df);
            *self
                .programs
                .lock()
                .expect("retrofit programs mutex poisoned") = loaded;
        }
        Ok(())
    }

    fn apply_to_output(&self, records: &mut [EmissionRecord]) {
        let programs = self
            .programs
            .lock()
            .expect("retrofit programs mutex poisoned");
        if programs.is_empty() {
            return;
        }
        // Canonical `CompiledLine.buildSQL`:
        //   emissionQuant = emissionQuant * (retrofitFactor + nonRetrofitFactor)
        // keyed by (pollutant, process, fuel, source, yearID, modelYearID). Here
        // each output record carries its own yearID/modelYearID, so the bucket
        // factor is computed per record (see `RetrofitTable::combined_factor`).
        for r in records.iter_mut() {
            let (Some(src), Some(my), Some(pol), Some(proc), Some(year)) = (
                r.source_type_id,
                r.model_year_id,
                r.pollutant_id,
                r.process_id,
                r.year_id,
            ) else {
                continue;
            };
            let (Ok(pol), Ok(proc)) = (u16::try_from(pol), u16::try_from(proc)) else {
                continue;
            };
            let factor =
                programs.combined_factor(i32::from(src), i32::from(my), pol, proc, i32::from(year));
            if (factor - 1.0).abs() <= f64::EPSILON {
                continue;
            }
            if let Some(q) = r.emission_quant.as_mut() {
                *q *= factor;
            }
            if let Some(rate) = r.emission_rate.as_mut() {
                *rate *= factor;
            }
        }
    }
}

/// Parse the `onRoadRetrofit` execution table into a [`RetrofitTable`].
///
/// Columns (canonical schema): `pollutantID`, `processID`, `fuelTypeID`,
/// `sourceTypeID`, `retrofitYearID`, `beginModelYearID`, `endModelYearID`
/// (int64) and `cumFractionRetrofit`, `retrofitEffectiveFraction` (float64).
/// `fuelTypeID` is not carried by [`RetrofitRecord`] (canonical retrofit is
/// diesel-only); a row whose key columns are missing/non-integer is skipped.
/// Returns an empty table if a required column is absent.
fn retrofit_table_from_dataframe(df: &DataFrame) -> RetrofitTable {
    let i64_col = |name: &str| df.column(name).ok().and_then(|c| c.i64().ok().cloned());
    let f64_col = |name: &str| df.column(name).ok().and_then(|c| c.f64().ok().cloned());

    let (Some(pol), Some(proc), Some(src), Some(ry), Some(bmy), Some(emy)) = (
        i64_col("pollutantID"),
        i64_col("processID"),
        i64_col("sourceTypeID"),
        i64_col("retrofitYearID"),
        i64_col("beginModelYearID"),
        i64_col("endModelYearID"),
    ) else {
        return RetrofitTable::new();
    };
    let (Some(cum), Some(eff)) = (
        f64_col("cumFractionRetrofit"),
        f64_col("retrofitEffectiveFraction"),
    ) else {
        return RetrofitTable::new();
    };

    let mut table = RetrofitTable::new();
    for i in 0..df.height() {
        let (Some(p), Some(pr), Some(s), Some(y), Some(b), Some(e), Some(c), Some(ef)) = (
            pol.get(i),
            proc.get(i),
            src.get(i),
            ry.get(i),
            bmy.get(i),
            emy.get(i),
            cum.get(i),
            eff.get(i),
        ) else {
            continue;
        };
        let (Ok(p), Ok(pr)) = (u16::try_from(p), u16::try_from(pr)) else {
            continue;
        };
        table.insert(RetrofitRecord::new(
            s as i32, b as i32, e as i32, y as i32, p, pr, c, ef,
        ));
    }
    table
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::RetrofitRecord;
    use moves_framework::InMemoryStore;

    #[allow(clippy::too_many_arguments)]
    fn make_record(
        source_type: i32,
        start_my: i32,
        end_my: i32,
        retrofit_year: i32,
        pollutant: u16,
        process: u16,
        fraction: f64,
        effectiveness: f64,
    ) -> RetrofitRecord {
        RetrofitRecord::new(
            source_type,
            start_my,
            end_my,
            retrofit_year,
            pollutant,
            process,
            fraction,
            effectiveness,
        )
    }

    #[test]
    fn name_is_stable() {
        let s = OnRoadRetrofitStrategy::new(RetrofitTable::new());
        assert_eq!(s.name(), "OnRoadRetrofitStrategy");
    }

    #[allow(clippy::too_many_arguments)]
    fn emission_record(
        src: i16,
        my: i16,
        pol: i16,
        proc: i16,
        year: i16,
        quant: f64,
    ) -> EmissionRecord {
        EmissionRecord {
            moves_run_id: 1,
            iteration_id: None,
            year_id: Some(year),
            month_id: None,
            day_id: None,
            hour_id: None,
            state_id: None,
            county_id: None,
            zone_id: None,
            link_id: None,
            pollutant_id: Some(pol),
            process_id: Some(proc),
            source_type_id: Some(src),
            reg_class_id: None,
            fuel_type_id: None,
            fuel_sub_type_id: None,
            model_year_id: Some(my),
            road_type_id: None,
            scc: None,
            eng_tech_id: None,
            sector_id: None,
            hp_id: None,
            emission_quant: Some(quant),
            emission_rate: None,
            run_hash: String::new(),
        }
    }

    #[test]
    fn modified_tables_is_empty_retrofit_is_post_output() {
        // Retrofit scales the finalized output in `apply_to_output`; it does not
        // mutate any input table, so it declares none (the engine reloads
        // modified_tables after pre_run).
        let s = OnRoadRetrofitStrategy::new(RetrofitTable::new());
        assert!(s.modified_tables().is_empty());
    }

    #[test]
    fn pre_run_succeeds_with_empty_context() {
        let s = OnRoadRetrofitStrategy::new(RetrofitTable::new());
        let mut store = InMemoryStore::new();
        s.pre_run(&mut store).expect("pre_run must not fail");
    }

    #[test]
    fn apply_to_output_scales_matching_records_by_combined_factor() {
        // One program: source 11, MY 2005-2015, retrofitYear 2020, NOx (3),
        // running (1), 50% retrofit × 80% effective → factor 0.60.
        let programs: RetrofitTable = [make_record(11, 2005, 2015, 2020, 3, 1, 0.5, 0.8)]
            .into_iter()
            .collect();
        let s = OnRoadRetrofitStrategy::new(programs);
        let mut records = vec![
            // matches: source 11, MY 2010, NOx running, year 2025 ≥ 2020.
            emission_record(11, 2010, 3, 1, 2025, 100.0),
            // wrong source type → untouched.
            emission_record(21, 2010, 3, 1, 2025, 100.0),
            // year before retrofitYear → untouched.
            emission_record(11, 2010, 3, 1, 2019, 100.0),
        ];
        s.apply_to_output(&mut records);
        assert!((records[0].emission_quant.unwrap() - 60.0).abs() < 1e-9);
        assert!((records[1].emission_quant.unwrap() - 100.0).abs() < 1e-9);
        assert!((records[2].emission_quant.unwrap() - 100.0).abs() < 1e-9);
    }

    #[test]
    fn apply_to_output_is_a_no_op_when_no_programs() {
        let s = OnRoadRetrofitStrategy::new(RetrofitTable::new());
        let mut records = vec![emission_record(11, 2010, 3, 1, 2025, 100.0)];
        s.apply_to_output(&mut records);
        assert!((records[0].emission_quant.unwrap() - 100.0).abs() < 1e-9);
    }

    #[test]
    fn programs_accessor_returns_inserted_table() {
        let programs: RetrofitTable = [make_record(21, 2000, 2010, 2018, 3, 1, 0.4, 0.7)]
            .into_iter()
            .collect();
        let s = OnRoadRetrofitStrategy::new(programs);
        assert_eq!(s.program_count(), 1);
    }

    #[test]
    fn no_subscriptions() {
        let s = OnRoadRetrofitStrategy::new(RetrofitTable::new());
        assert!(s.subscriptions().is_empty());
    }

    #[test]
    fn strategy_is_trait_object_safe() {
        let strategy: Box<dyn InternalControlStrategy> =
            Box::new(OnRoadRetrofitStrategy::new(RetrofitTable::new()));
        assert_eq!(strategy.name(), "OnRoadRetrofitStrategy");
    }
}
