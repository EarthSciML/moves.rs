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
//! # Data-plane status
//!
//! The computed adjustment factors would normally be written into the
//! `EmissionRateAdjustment` table in the execution database so the Base Rate
//! Calculator scales rates by them (canonical Java multiplies
//! `emissionQuant`/`emissionRate` by `retrofitFactor + nonRetrofitFactor`).
//! That write is **not yet ported**: it needs the run's analysis years and the
//! `EmissionRateAdjustment` key columns (`polProcessID`, `regClassID`,
//! `fuelTypeID`) that [`RetrofitRecord`](crate::model::RetrofitRecord) does not
//! carry. Until it is ported, [`pre_run`](OnRoadRetrofitStrategy::pre_run)
//! returns [`moves_framework::Error::NotImplemented`] when any programs are
//! present rather than silently returning success and dropping the entire
//! retrofit effect. An empty program table remains a clean no-op, matching the
//! canonical `subscribeToMe` early-return when no `onRoadRetrofit` rows exist.

use moves_framework::{InMemoryStore, InternalControlStrategy};

use crate::model::RetrofitTable;

/// OnRoadRetrofit internal control strategy.
///
/// See the [module docs](self) for the full description.
#[derive(Debug)]
pub struct OnRoadRetrofitStrategy {
    programs: RetrofitTable,
}

impl OnRoadRetrofitStrategy {
 /// Build from a [`RetrofitTable`] already loaded from user input.
    pub fn new(programs: RetrofitTable) -> Self {
        Self { programs }
    }

 /// The retrofit programs this strategy will apply.
    pub fn programs(&self) -> &RetrofitTable {
        &self.programs
    }
}

impl InternalControlStrategy for OnRoadRetrofitStrategy {
    fn name(&self) -> &'static str {
        "OnRoadRetrofitStrategy"
    }

    fn modified_tables(&self) -> &[&'static str] {
        &["emissionRateAdjustment"]
    }

    fn pre_run(
        &self,
        _tables: &mut InMemoryStore,
    ) -> std::result::Result<(), moves_framework::Error> {
 // The canonical `OnRoadRetrofitStrategy.subscribeToMe` early-returns
 // when there are no `onRoadRetrofit` rows (`dbLines.size() <= 0`); with
 // no programs there is genuinely nothing to apply, so an empty table is
 // a clean no-op.
        if self.programs.is_empty() {
            return Ok(());
        }

 // When programs ARE present the canonical code multiplies emission
 // rates/quantities by the combined retrofit factor (Java
 // `CompiledLine.buildSQL`: `emissionQuant=emissionQuant*(retrofitFactor
 // +nonRetrofitFactor)`, applied per pollutant/process/fuel/source/year/
 // modelYear). The data-plane write of that adjustment is not yet ported:
 // it requires the run's analysis years and the model-year / regClass /
 // fuelType universe (none of which reach `pre_run`), plus the
 // `EmissionRateAdjustment` key columns (`polProcessID`, `regClassID`,
 // `fuelTypeID`) that `RetrofitRecord` does not carry. Returning `Ok(())`
 // here would silently drop the entire retrofit effect while the engine
 // reports success, so surface the unported live path explicitly instead.
        Err(moves_framework::Error::NotImplemented)
    }
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

    #[test]
    fn modified_tables_contains_emission_rate_adjustment() {
        let s = OnRoadRetrofitStrategy::new(RetrofitTable::new());
        assert!(
            s.modified_tables().contains(&"emissionRateAdjustment"),
            "strategy must declare emissionRateAdjustment in modified_tables"
        );
    }

    #[test]
    fn pre_run_succeeds_with_empty_context() {
        let s = OnRoadRetrofitStrategy::new(RetrofitTable::new());
        let mut store = InMemoryStore::new();
        s.pre_run(&mut store).expect("pre_run must not fail");
    }

    #[test]
    fn pre_run_errors_with_populated_programs_until_data_plane_ported() {
        // The retrofit adjustment write-out is not yet ported (it needs the
        // run's analysis years and the EmissionRateAdjustment key columns that
        // `RetrofitRecord` does not carry). Until then, applying real retrofit
        // programs must fail loudly rather than silently drop the reduction.
        let programs: RetrofitTable = [make_record(11, 2005, 2015, 2020, 98, 1, 0.5, 0.8)]
            .into_iter()
            .collect();
        let s = OnRoadRetrofitStrategy::new(programs);
        let mut store = InMemoryStore::new();
        let err = s
            .pre_run(&mut store)
            .expect_err("pre_run must surface the unported retrofit data plane");
        assert!(matches!(err, moves_framework::Error::NotImplemented));
    }

    #[test]
    fn programs_accessor_returns_inserted_table() {
        let programs: RetrofitTable = [make_record(21, 2000, 2010, 2018, 3, 1, 0.4, 0.7)]
            .into_iter()
            .collect();
        let s = OnRoadRetrofitStrategy::new(programs);
        assert_eq!(s.programs().len(), 1);
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
