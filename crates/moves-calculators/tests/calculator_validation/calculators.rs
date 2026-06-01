//! The calculator catalogue.
//!
//! port the MOVES onroad hot-path emission calculators (37
//! implementations). adds `DummyCalculator` (the no-op placeholder
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
    nitrogen_oxide::{NO2Calculator, NOCalculator},
    nonroad_emission::NonroadEmissionCalculator,
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
        ch4n2o::Ch4N2oWtpCalculator, co2_atmospheric::Co2AtmosphericWtpCalculator,
        co2_equivalent::Co2EquivalentWtpCalculator, total_energy::WellToPumpProcessor,
    },
};

/// The number of calculators the plan lands.
pub const CALCULATOR_COUNT: usize = 39;

/// Construct every hot-path calculator as a boxed trait object.
///
/// The list is the harness's source of truth for "the calculators"
/// it is the real `Calculator` implementations from the
/// `moves-calculators` crate, not a description of them.
pub fn all_calculators() -> Vec<Box<dyn Calculator>> {
    vec![
 //
        Box::new(BaseRateCalculator::default()),
 //
        Box::new(CriteriaRunningCalculator::new()),
 //
        Box::new(CriteriaStartCalculator::new()),
 //
        Box::new(HcSpeciationCalculator::new()),
 //
        Box::new(NrHcSpeciationCalculator::new()),
 //
        Box::new(AirToxicsCalculator::new()),
 //
        Box::new(AirToxicsDistanceCalculator::new()),
 //
        Box::new(NrAirToxicsCalculator::new()),
 //
        Box::new(PmTotalExhaustCalculator::new()),
        Box::new(BasicRunningPmEmissionCalculator::new()),
 //
        Box::new(BasicStartPmEmissionCalculator::new()),
 //
        Box::new(PM10EmissionCalculator::new()),
        Box::new(PM10BrakeTireCalculator::new()),
 //
        Box::new(BasicBrakeWearPmEmissionCalculator::new()),
        Box::new(BasicTireWearPmEmissionCalculator::new()),
 //
        Box::new(SulfatePMCalculator),
 //
        Box::new(EvaporativePermeationCalculator::new()),
 //
        Box::new(TankVaporVentingCalculator::new()),
 //
        Box::new(MultidayTankVaporVentingCalculator::new()),
 //
        Box::new(LiquidLeakingCalculator::new()),
 //
        Box::new(RefuelingLossCalculator),
 //
        Box::new(CrankcaseEmissionCalculatorNonPM),
        Box::new(CrankcaseEmissionCalculatorPM),
 //
        Box::new(CO2AERunningStartExtendedIdleCalculator),
 //
        Box::new(Ch4N2oRunningStartCalculator::new()),
 //
        Box::new(Nh3RunningCalculator::new()),
        Box::new(Nh3StartCalculator::new()),
 //
        Box::new(SO2Calculator),
 //
        Box::new(NOCalculator::new()),
        Box::new(NO2Calculator::new()),
 //
        Box::new(WellToPumpProcessor),
        Box::new(Co2AtmosphericWtpCalculator),
        Box::new(Ch4N2oWtpCalculator),
        Box::new(Co2EquivalentWtpCalculator),
 //
        Box::new(TogSpeciationCalculator),
 //
        Box::new(ActivityCalculator),
 //
        Box::new(DistanceCalculator::new()),
 // — DummyCalculator (no-op completeness entry)
        Box::new(DummyCalculator),
 // () — NonroadEmissionCalculator adapter
        Box::new(NonroadEmissionCalculator::new()),
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

/// The sorted, deduplicated names of all calculators.
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
            "expected {CALCULATOR_COUNT} calculators, got {}",
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
