//! `moves-calculators` — onroad emission calculators and generators ported
//! from Java and Go.
//!
//! Hosts the ~70 calculator implementations under
//! `gov/epa/otaq/moves/master/implementation/ghg/` and related packages,
//! plus the generators that run ahead of them in the master loop. Each
//! module declares the `(pollutant, process)` pairs it produces and the
//! granularity at which it subscribes to the master loop; `moves-framework`
//! drives them according to the chain reconstructed in Phase 1
//! (Task 10, `moves-calculator-info`).
//!
//! See `moves-rust-migration-plan.md`:
//!
//! * Phase 3 — Tasks 29–43 cover the generators, Tasks 45–88 the calculators.
//!
//! # Phase 3 status
//!
//! The crate is filled in module by module by the Phase 3 implementation
//! tasks, grouped into two areas: the [`generators`] module hosts the
//! generator ports (Tasks 29–43) and the [`calculators`] module hosts the
//! calculator ports (Tasks 45–88). Each port adds its module under the
//! relevant area and registers it with a single `pub mod` line in that
//! area's `mod.rs`, never in this file — so the crate root stays a stable,
//! merge-conflict-free area list as Phase 3 grows.

pub mod calculators;
pub mod error;
pub mod generators;
pub(crate) mod wiring;

pub use error::{Error, Result};

/// Register every wired calculator and generator factory with `registry`.
///
/// Call this after constructing a [`moves_framework::CalculatorRegistry`] from
/// the DAG to make all ported implementations available for execution.
///
/// # Errors
///
/// Returns [`moves_framework::Error::UnknownModule`] if a calculator's DAG name
/// does not appear in the registry's DAG (i.e., the DAG and the crate are out
/// of sync).
pub fn register_all(
    registry: &mut moves_framework::CalculatorRegistry,
) -> std::result::Result<(), moves_framework::Error> {
    use calculators::activitycalculator;
    use calculators::airtoxics;
    use calculators::airtoxicsdistance;
    use calculators::baseratecalculator;
    use calculators::basicbraketirepm;
    use calculators::basicstartpm;
    use calculators::ch4n2o_running_start;
    use calculators::co2ae_running_start_extended_idle;
    use calculators::crankcase_emission;
    use calculators::criteria_running_calculator;
    use calculators::criteria_start_calculator;
    use calculators::distance_calculator;
    use calculators::evaporative_permeation_calculator;
    use calculators::hcspeciation;
    use calculators::liquid_leaking_calculator;
    use calculators::nh3;
    use calculators::nitrogen_oxide;
    use calculators::nrairtoxics;
    use calculators::nrhcspeciation;
    use calculators::pm10;
    use calculators::pmexhaust;
    use calculators::refueling_loss_calculator;
    use calculators::so2_calculator;
    use calculators::sulfate_pm_calculator;
    use calculators::tank_vapor_venting_calculator;
    use calculators::togspeciation;
    use calculators::welltopump;
    use generators::avg_speed_op_mode_distribution;
    use generators::baserategenerator;
    use generators::evap_op_mode_distribution;
    use generators::fueleffectsgenerator;
    use generators::meteorology;
    use generators::operating_mode_distribution;
    use generators::source_bin_distribution_generator;
    use generators::sourcetypephysics;
    use generators::start_operating_mode_distribution;
    use generators::tank_fuel_generator;
    use generators::tank_temperature_generator;
    use generators::totalactivitygenerator;

    registry.register_calculator(
        activitycalculator::ActivityCalculator::NAME,
        activitycalculator::factory,
    )?;
    registry.register_calculator(airtoxics::AirToxicsCalculator::NAME, airtoxics::factory)?;
    registry.register_calculator(
        airtoxicsdistance::AirToxicsDistanceCalculator::NAME,
        airtoxicsdistance::factory,
    )?;
    registry.register_calculator(
        baseratecalculator::BaseRateCalculator::NAME,
        baseratecalculator::factory,
    )?;
    registry.register_calculator(
        crankcase_emission::CrankcaseEmissionCalculatorNonPM::NAME,
        crankcase_emission::nonpm_factory,
    )?;
    // multiday_tank_vapor_venting_calculator is intentionally not registered:
    // it has no DAG entry — "MultidayTankVaporVentingCalculator" is not in
    // calculator-dag.json. The live TankVaporVentingCalculator DAG entry and
    // its (THC × process 12) registration belong to tank_vapor_venting_calculator
    // (already registered above). The multiday module is the algorithm body a
    // future runtime would dispatch to via USE_MULTIDAY_DIURNALS.
    registry.register_calculator(
        nitrogen_oxide::NOCalculator::NAME,
        nitrogen_oxide::no_factory,
    )?;
    registry.register_calculator(
        nitrogen_oxide::NO2Calculator::NAME,
        nitrogen_oxide::no2_factory,
    )?;
    registry.register_calculator(
        hcspeciation::HcSpeciationCalculator::NAME,
        hcspeciation::factory,
    )?;
    registry.register_calculator(
        nrhcspeciation::NrHcSpeciationCalculator::NAME,
        nrhcspeciation::factory,
    )?;
    registry.register_calculator(
        ch4n2o_running_start::Ch4N2oRunningStartCalculator::NAME,
        ch4n2o_running_start::factory,
    )?;
    registry.register_calculator(
        co2ae_running_start_extended_idle::CO2AERunningStartExtendedIdleCalculator::NAME,
        co2ae_running_start_extended_idle::factory,
    )?;
    registry.register_calculator(
        criteria_running_calculator::CriteriaRunningCalculator::NAME,
        criteria_running_calculator::factory,
    )?;
    registry.register_calculator(
        criteria_start_calculator::CriteriaStartCalculator::NAME,
        criteria_start_calculator::factory,
    )?;
    registry.register_calculator(
        distance_calculator::DistanceCalculator::NAME,
        distance_calculator::factory,
    )?;
    registry.register_calculator(
        evaporative_permeation_calculator::EvaporativePermeationCalculator::NAME,
        evaporative_permeation_calculator::factory,
    )?;
    registry.register_calculator(
        liquid_leaking_calculator::LiquidLeakingCalculator::NAME,
        liquid_leaking_calculator::factory,
    )?;
    registry.register_calculator(
        nh3::running::Nh3RunningCalculator::NAME,
        nh3::running::factory,
    )?;
    registry.register_calculator(nh3::start::Nh3StartCalculator::NAME, nh3::start::factory)?;
    registry.register_calculator(
        refueling_loss_calculator::RefuelingLossCalculator::NAME,
        refueling_loss_calculator::factory,
    )?;
    registry.register_calculator(so2_calculator::SO2Calculator::NAME, so2_calculator::factory)?;
    registry.register_calculator(
        sulfate_pm_calculator::SulfatePMCalculator::NAME,
        sulfate_pm_calculator::factory,
    )?;
    registry.register_calculator(
        tank_vapor_venting_calculator::TankVaporVentingCalculator::NAME,
        tank_vapor_venting_calculator::factory,
    )?;
    registry.register_calculator(
        togspeciation::TogSpeciationCalculator::NAME,
        togspeciation::factory,
    )?;
    registry.register_calculator(
        nrairtoxics::NrAirToxicsCalculator::NAME,
        nrairtoxics::factory,
    )?;
    registry.register_calculator(
        basicbraketirepm::BasicBrakeWearPmEmissionCalculator::NAME,
        basicbraketirepm::brakewear_factory,
    )?;
    registry.register_calculator(
        basicbraketirepm::BasicTireWearPmEmissionCalculator::NAME,
        basicbraketirepm::tirewear_factory,
    )?;
    registry.register_calculator(
        basicstartpm::BasicStartPmEmissionCalculator::NAME,
        basicstartpm::factory,
    )?;
    registry.register_calculator(pm10::PM10EmissionCalculator::NAME, pm10::emission_factory)?;
    registry.register_calculator(
        pm10::PM10BrakeTireCalculator::NAME,
        pm10::brake_tire_factory,
    )?;
    registry.register_calculator(
        pmexhaust::running::BasicRunningPmEmissionCalculator::NAME,
        pmexhaust::running::factory,
    )?;
    registry.register_calculator(
        pmexhaust::total::PmTotalExhaustCalculator::NAME,
        pmexhaust::total::factory,
    )?;
    registry.register_calculator(
        welltopump::ch4n2o::Ch4N2oWtpCalculator::NAME,
        welltopump::ch4n2o::factory,
    )?;
    registry.register_calculator(
        welltopump::co2_atmospheric::Co2AtmosphericWtpCalculator::NAME,
        welltopump::co2_atmospheric::factory,
    )?;
    registry.register_calculator(
        welltopump::co2_equivalent::Co2EquivalentWtpCalculator::NAME,
        welltopump::co2_equivalent::factory,
    )?;
    registry.register_calculator(
        welltopump::total_energy::WellToPumpProcessor::NAME,
        welltopump::total_energy::factory,
    )?;

    registry.register_generator("BaseRateGenerator", baserategenerator::factory)?;
    registry.register_generator(
        avg_speed_op_mode_distribution::AverageSpeedOperatingModeDistributionGenerator::NAME,
        avg_speed_op_mode_distribution::factory,
    )?;
    registry.register_generator(
        evap_op_mode_distribution::EvaporativeEmissionsOperatingModeDistributionGenerator::NAME,
        evap_op_mode_distribution::factory,
    )?;
    registry.register_generator("MeteorologyGenerator", meteorology::factory)?;
    registry.register_generator(
        operating_mode_distribution::OperatingModeDistributionGenerator::NAME,
        operating_mode_distribution::factory,
    )?;
    registry.register_generator(
        source_bin_distribution_generator::SourceBinDistributionGenerator::NAME,
        source_bin_distribution_generator::factory,
    )?;
    registry.register_generator(
        "StartOperatingModeDistributionGenerator",
        start_operating_mode_distribution::factory,
    )?;
    registry.register_generator(
        totalactivitygenerator::TotalActivityGenerator::NAME,
        totalactivitygenerator::factory,
    )?;
    registry.register_generator("FuelEffectsGenerator", fueleffectsgenerator::factory)?;
    registry.register_generator(
        sourcetypephysics::SourceTypePhysics::NAME,
        sourcetypephysics::factory,
    )?;
    registry.register_generator(
        "TankTemperatureGenerator",
        tank_temperature_generator::factory,
    )?;
    registry.register_generator("TankFuelGenerator", tank_fuel_generator::factory)?;

    Ok(())
}

/// Register every wired control strategy factory with `registry`.
///
/// Call this after constructing a [`moves_framework::ControlStrategyRegistry`] to
/// make all ported strategy implementations available for execution. Parallels
/// [`register_all`] for [`moves_framework::CalculatorRegistry`].
///
/// Additional strategies (OnRoadRetrofitStrategy, RateOfProgressControlStrategy,
/// etc.) will be added here as each strategy wiring bead lands (see mo-4wci, mo-e7ug).
pub fn register_strategies(registry: &mut moves_framework::ControlStrategyRegistry) {
    registry.register(|| Box::new(moves_fuel_control::FuelControlStrategy::new()));
    // AVFT uses polars+parquet and is not available on wasm32 (mio linkage issue).
    #[cfg(not(target_arch = "wasm32"))]
    registry.register(|| {
        Box::new(moves_avft::AvftControlStrategy::from_completed(
            moves_avft::AvftTable::new(),
        ))
    });
}
