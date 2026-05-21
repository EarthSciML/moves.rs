//! The Phase 3 calculator catalogue (Tasks 45–72 + 78 closing checkpoint).
//!
//! Tasks 45–72 port the MOVES onroad hot-path emission calculators (37
//! implementations). Task 78 adds `DummyCalculator` (the no-op placeholder
//! from `CalculatorInfo.txt`) to satisfy the "every module is represented"
//! completeness criterion, bringing the total to **38**:
//!
//! | Task | Calculator(s) |
//! |------|---------------|
//! | 45 | `BaseRateCalculator` |
//! | 46 | `CriteriaRunningCalculator` |
//! | 47 | `CriteriaStartCalculator` |
//! | 48 | `HcSpeciationCalculator` |
//! | 49 | `NrHcSpeciationCalculator` |
//! | 50 | `AirToxicsCalculator` |
//! | 51 | `AirToxicsDistanceCalculator` |
//! | 52 | `NrAirToxicsCalculator` |
//! | 53 | `PmTotalExhaustCalculator`, `BasicRunningPmEmissionCalculator` |
//! | 54 | `BasicStartPmEmissionCalculator` |
//! | 55 | `PM10EmissionCalculator`, `PM10BrakeTireCalculator` |
//! | 56 | `BasicBrakeWearPmEmissionCalculator`, `BasicTireWearPmEmissionCalculator` |
//! | 57 | `SulfatePMCalculator` |
//! | 58 | `EvaporativePermeationCalculator` |
//! | 59 | `TankVaporVentingCalculator` |
//! | 60 | `MultidayTankVaporVentingCalculator` |
//! | 61 | `LiquidLeakingCalculator` |
//! | 62 | `RefuelingLossCalculator` |
//! | 63 | `CrankcaseEmissionCalculatorNonPM`, `CrankcaseEmissionCalculatorPM` |
//! | 64 | `CO2AERunningStartExtendedIdleCalculator` |
//! | 65 | `Ch4N2oRunningStartCalculator` |
//! | 66 | `Nh3RunningCalculator`, `Nh3StartCalculator` |
//! | 67 | `SO2Calculator` |
//! | 68 | `NOCalculator`, `NO2Calculator` |
//! | 69 | `WellToPumpProcessor`, `Co2AtmosphericWtpCalculator`, `Ch4N2oWtpCalculator`, `Co2EquivalentWtpCalculator` |
//! | 70 | `TogSpeciationCalculator` |
//! | 71 | `ActivityCalculator` |
//! | 72 | `DistanceCalculator` |
//! | 78 | `DummyCalculator` |
//!
//! [`all_calculators`] instantiates all 38 as boxed trait objects.
//! The harness reads `name()` and `registrations()` straight off those
//! objects, so the catalogue is the live trait impls — there is no
//! hand-copied table to drift.

use std::collections::BTreeSet;

use moves_framework::Calculator;

use moves_calculators::calculators::{
    activitycalculator::ActivityCalculator,
    airtoxics::AirToxicsCalculator,
    airtoxicsdistance::AirToxicsDistanceCalculator,
    baseratecalculator::BaseRateCalculator,
    basicbraketirepm::{BasicBrakeWearPmEmissionCalculator, BasicTireWearPmEmissionCalculator},
    basicstartpm::BasicStartPmEmissionCalculator,
    ch4n2o_running_start::Ch4N2oRunningStartCalculator,
    co2ae_running_start_extended_idle::CO2AERunningStartExtendedIdleCalculator,
    crankcase_emission::{CrankcaseEmissionCalculatorNonPM, CrankcaseEmissionCalculatorPM},
    criteria_running_calculator::CriteriaRunningCalculator,
    criteria_start_calculator::CriteriaStartCalculator,
    distance_calculator::DistanceCalculator,
    dummy::DummyCalculator,
    evaporative_permeation_calculator::EvaporativePermeationCalculator,
    hcspeciation::HcSpeciationCalculator,
    liquid_leaking_calculator::LiquidLeakingCalculator,
    multiday_tank_vapor_venting_calculator::MultidayTankVaporVentingCalculator,
    nh3::{running::Nh3RunningCalculator, start::Nh3StartCalculator},
    nitrogen_oxide::{NOCalculator, NO2Calculator},
    nrairtoxics::NrAirToxicsCalculator,
    nrhcspeciation::NrHcSpeciationCalculator,
    pm10::{PM10BrakeTireCalculator, PM10EmissionCalculator},
    pmexhaust::{running::BasicRunningPmEmissionCalculator, total::PmTotalExhaustCalculator},
    refueling_loss_calculator::RefuelingLossCalculator,
    so2_calculator::SO2Calculator,
    sulfate_pm_calculator::SulfatePMCalculator,
    tank_vapor_venting_calculator::TankVaporVentingCalculator,
    togspeciation::TogSpeciationCalculator,
    welltopump::{
        ch4n2o::Ch4N2oWtpCalculator,
        co2_atmospheric::Co2AtmosphericWtpCalculator,
        co2_equivalent::Co2EquivalentWtpCalculator,
        total_energy::WellToPumpProcessor,
    },
};

/// The number of calculators the Phase 3 plan lands (Tasks 45–72 + 78).
pub const CALCULATOR_COUNT: usize = 38;

/// Construct every Phase 3 hot-path calculator as a boxed trait object.
///
/// The list is the harness's source of truth for "the calculators"
/// — it is the real `Calculator` implementations from the
/// `moves-calculators` crate, not a description of them.
pub fn all_calculators() -> Vec<Box<dyn Calculator>> {
    vec![
        // Task 45
        Box::new(BaseRateCalculator),
        // Task 46
        Box::new(CriteriaRunningCalculator::new()),
        // Task 47
        Box::new(CriteriaStartCalculator::new()),
        // Task 48
        Box::new(HcSpeciationCalculator::new()),
        // Task 49
        Box::new(NrHcSpeciationCalculator::new()),
        // Task 50
        Box::new(AirToxicsCalculator::new()),
        // Task 51
        Box::new(AirToxicsDistanceCalculator::new()),
        // Task 52
        Box::new(NrAirToxicsCalculator::new()),
        // Task 53
        Box::new(PmTotalExhaustCalculator::new()),
        Box::new(BasicRunningPmEmissionCalculator::new()),
        // Task 54
        Box::new(BasicStartPmEmissionCalculator::new()),
        // Task 55
        Box::new(PM10EmissionCalculator::new()),
        Box::new(PM10BrakeTireCalculator::new()),
        // Task 56
        Box::new(BasicBrakeWearPmEmissionCalculator::new()),
        Box::new(BasicTireWearPmEmissionCalculator::new()),
        // Task 57
        Box::new(SulfatePMCalculator),
        // Task 58
        Box::new(EvaporativePermeationCalculator::new()),
        // Task 59
        Box::new(TankVaporVentingCalculator::new()),
        // Task 60
        Box::new(MultidayTankVaporVentingCalculator::new()),
        // Task 61
        Box::new(LiquidLeakingCalculator::new()),
        // Task 62
        Box::new(RefuelingLossCalculator),
        // Task 63
        Box::new(CrankcaseEmissionCalculatorNonPM),
        Box::new(CrankcaseEmissionCalculatorPM),
        // Task 64
        Box::new(CO2AERunningStartExtendedIdleCalculator),
        // Task 65
        Box::new(Ch4N2oRunningStartCalculator::new()),
        // Task 66
        Box::new(Nh3RunningCalculator::new()),
        Box::new(Nh3StartCalculator::new()),
        // Task 67
        Box::new(SO2Calculator),
        // Task 68
        Box::new(NOCalculator::new()),
        Box::new(NO2Calculator::new()),
        // Task 69
        Box::new(WellToPumpProcessor),
        Box::new(Co2AtmosphericWtpCalculator),
        Box::new(Ch4N2oWtpCalculator),
        Box::new(Co2EquivalentWtpCalculator),
        // Task 70
        Box::new(TogSpeciationCalculator),
        // Task 71
        Box::new(ActivityCalculator),
        // Task 72
        Box::new(DistanceCalculator::new()),
        // Task 78 — DummyCalculator (no-op completeness entry)
        Box::new(DummyCalculator),
    ]
}

/// The distinct (pollutant_id, process_id) pairs a calculator registers.
///
/// The master loop fires a calculator for a run iff the run exercises
/// one of these (pollutant, process) pairs; [`super::coverage`] joins
/// this set against each fixture's
/// [`super::fixtures::OnroadFixture::ppa_ids`].
///
/// A calculator with no registrations (chained-only — it runs when
/// its chain parent fires, not from a master-loop subscription) yields
/// an empty set. Such calculators are noted in the coverage matrix as
/// `ChainedOnly`.
pub fn registered_ppa_ids(calculator: &dyn Calculator) -> BTreeSet<(u32, u32)> {
    calculator
        .registrations()
        .iter()
        .map(|reg| (u32::from(reg.pollutant_id.0), u32::from(reg.process_id.0)))
        .collect()
}

/// The sorted, deduplicated names of all Phase 3 calculators.
pub fn sorted_calculator_names() -> Vec<String> {
    let mut names: Vec<String> = all_calculators()
        .iter()
        .map(|c| c.name().to_string())
        .collect();
    names.sort_unstable();
    names
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_38_calculators_instantiate() {
        let calcs = all_calculators();
        assert_eq!(
            calcs.len(),
            CALCULATOR_COUNT,
            "expected {CALCULATOR_COUNT} Phase 3 calculators, got {}",
            calcs.len()
        );
    }

    #[test]
    fn all_calculator_names_are_unique() {
        let names = sorted_calculator_names();
        let mut deduped = names.clone();
        deduped.dedup();
        assert_eq!(
            deduped.len(),
            names.len(),
            "duplicate calculator name(s): {names:?}"
        );
    }

    #[test]
    fn all_calculators_can_be_held_as_trait_objects() {
        let calcs: Vec<Box<dyn Calculator>> = all_calculators();
        assert_eq!(calcs.len(), CALCULATOR_COUNT);
        for calc in &calcs {
            assert!(!calc.name().is_empty(), "calculator name must not be empty");
        }
    }

    #[test]
    fn registered_ppa_ids_are_pairs_of_u32() {
        for calc in all_calculators() {
            let ppa_ids = registered_ppa_ids(calc.as_ref());
            // All returned pairs must have positive IDs — MOVES IDs are 1-based.
            for &(pollutant_id, process_id) in &ppa_ids {
                assert!(pollutant_id > 0, "{}: zero pollutant_id", calc.name());
                assert!(process_id > 0, "{}: zero process_id", calc.name());
            }
        }
    }
}
