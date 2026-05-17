//! The Phase 3 generator catalogue (Tasks 29–43).
//!
//! Tasks 29–43 of the migration plan port the MOVES onroad
//! generators. They land **16** `Generator` implementations — one per
//! task, except Task 35 (`MesoscaleLookup…`) which ports a paired
//! operating-mode + total-activity generator.
//!
//! [`all_generators`] instantiates all 16 as boxed trait objects.
//! The harness reads `name()`, `subscriptions()`, and
//! `output_tables()` straight off those objects, so the catalogue is
//! the live trait impls — there is no hand-copied table to drift.

use std::collections::BTreeSet;

use moves_framework::Generator;

use moves_calculators::generators::{
    avg_speed_op_mode_distribution::AverageSpeedOperatingModeDistributionGenerator,
    baserategenerator::BaseRateGenerator,
    evap_op_mode_distribution::EvaporativeEmissionsOperatingModeDistributionGenerator,
    fueleffectsgenerator::FuelEffectsGenerator,
    link_op_mode_distribution::LinkOperatingModeDistributionGenerator,
    mesoscale_lookup::op_mode_distribution::MesoscaleLookupOperatingModeDistributionGenerator,
    mesoscale_lookup::total_activity::MesoscaleLookupTotalActivityGenerator,
    meteorology::MeteorologyGenerator,
    operating_mode_distribution::OperatingModeDistributionGenerator,
    rates_op_mode_distribution::RatesOperatingModeDistributionGenerator,
    source_bin_distribution_generator::SourceBinDistributionGenerator,
    sourcetypephysics::SourceTypePhysics,
    start_operating_mode_distribution::StartOperatingModeDistributionGenerator,
    tank_fuel_generator::TankFuelGenerator, tank_temperature_generator::TankTemperatureGenerator,
    totalactivitygenerator::TotalActivityGenerator,
};

/// The number of generators the Phase 3 plan lands (Tasks 29–43,
/// counting the paired Task 35 generators separately).
pub const GENERATOR_COUNT: usize = 16;

/// Construct every Phase 3 generator as a boxed trait object.
///
/// The list is the harness's source of truth for "the generators"
/// — it is the real `Generator` implementations from the
/// `moves-calculators` crate, not a description of them.
pub fn all_generators() -> Vec<Box<dyn Generator>> {
    vec![
        Box::new(AverageSpeedOperatingModeDistributionGenerator::new()),
        Box::new(BaseRateGenerator),
        Box::new(EvaporativeEmissionsOperatingModeDistributionGenerator::new()),
        Box::new(FuelEffectsGenerator),
        Box::new(LinkOperatingModeDistributionGenerator::new()),
        Box::new(MesoscaleLookupOperatingModeDistributionGenerator::new()),
        Box::new(MesoscaleLookupTotalActivityGenerator::new()),
        Box::new(MeteorologyGenerator),
        Box::new(OperatingModeDistributionGenerator::new()),
        Box::new(RatesOperatingModeDistributionGenerator::new()),
        Box::new(SourceBinDistributionGenerator),
        Box::new(SourceTypePhysics::new()),
        Box::new(StartOperatingModeDistributionGenerator),
        Box::new(TankFuelGenerator::new()),
        Box::new(TankTemperatureGenerator::new()),
        Box::new(TotalActivityGenerator),
    ]
}

/// The distinct emission-process IDs a generator subscribes to.
///
/// The master loop fires a generator for a run iff the run exercises
/// one of these processes; [`super::coverage`] joins this set against
/// each fixture's [`super::fixtures::OnroadFixture::process_ids`].
///
/// `MOVES` `ProcessId` is a `u16`; it is widened to `u32` here so the
/// set joins directly against the RunSpec-derived fixture processes.
/// A generator with no subscriptions (e.g. `SourceTypePhysics`, a
/// helper invoked by other generators rather than master-loop
/// scheduled) yields an empty set.
pub fn subscribed_process_ids(generator: &dyn Generator) -> BTreeSet<u32> {
    generator
        .subscriptions()
        .iter()
        .map(|sub| u32::from(sub.process_id.0))
        .collect()
}

/// The generator names, sorted — for the catalogue-stability test.
pub fn sorted_generator_names() -> Vec<String> {
    let mut names: Vec<String> = all_generators()
        .iter()
        .map(|g| g.name().to_string())
        .collect();
    names.sort();
    names
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalogue_has_16_generators() {
        assert_eq!(all_generators().len(), GENERATOR_COUNT);
    }

    #[test]
    fn generator_names_are_unique_and_non_empty() {
        let names = sorted_generator_names();
        for name in &names {
            assert!(!name.is_empty(), "a generator returned an empty name()");
        }
        let mut deduped = names.clone();
        deduped.dedup();
        assert_eq!(
            deduped.len(),
            names.len(),
            "two generators share a name(): {names:?}"
        );
    }

    #[test]
    fn every_generator_subscribes_or_is_a_known_helper() {
        // All generators are master-loop scheduled except SourceTypePhysics,
        // a helper that other generators invoke directly. Any *other*
        // generator with no subscriptions would never fire — a bug worth
        // catching here.
        for generator in all_generators() {
            let subs = subscribed_process_ids(generator.as_ref());
            if subs.is_empty() {
                assert_eq!(
                    generator.name(),
                    "SourceTypePhysics",
                    "generator `{}` has no subscriptions but is not the known helper",
                    generator.name()
                );
            }
        }
    }
}
