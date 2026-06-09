//! Activity Calculator —.
//!
//! Pure-Rust port of
//! `gov/epa/otaq/moves/master/implementation/ghg/ActivityCalculator.java`
//! and the `database/ActivityCalculator.sql` script it drives (964 lines of
//! SQL). The calculator captures *activity* — not emissions — for the
//! `MOVESWorkerActivityOutput` table when the RunSpec asks for activity
//! output: source hours, hours operating / parked, hotelling and extended
//! idle, engine starts, and vehicle population.
//!
//! # What the Java did, and what this port keeps
//!
//! `ActivityCalculator.java` is a thin shell: per master-loop iteration it
//! decides which of eight named *activity types* the RunSpec needs, then
//! `readAndHandleScriptedCalculations` runs the matching sections of
//! `ActivityCalculator.sql` against the MariaDB execution database. All of
//! the arithmetic lives in the SQL.
//!
//! The port keeps that arithmetic — the script's `Processing` section, plus
//! the `createSourceTypeFuelFraction` step — and replaces the database I/O
//! with plain values: an [`ActivityInputs`] in, a `Vec<`[`ActivityRow`]`>`
//! out. The script's first two sections (`Create Remote Tables`,
//! `Extract Data`) are `CREATE TABLE` / `TRUNCATE` / `SELECT … INTO OUTFILE`
//! scaffolding for distributing extracted data to a worker — pure database
//! mechanics with no algorithmic content — and have no analogue here; the
//! filters they apply (`WHERE yearID = …`, `zoneID = …`) are recorded on the
//! [`inputs`] structs instead.
//!
//! # The eight activity types
//!
//! [`ActivityType`] mirrors the Java `ActivityInfo` table. Each maps to one
//! `Processing` section and one `activityTypeID` on the output row:
//!
//! | Activity | `activityTypeID` | Base table | Placed at |
//! |---------------------|------------------|------------------|-----------|
//! | `SourceHours` | 2 | `SourceHours` | link |
//! | `ExtendedIdleHours` | 3 | `hotellingHours` | zone |
//! | `SHO` | 4 | `SHO` | link |
//! | `SHP` | 5 | `SHP` | zone |
//! | `Population` | 6 | several | zone |
//! | `Starts` | 7 | `Starts` | zone |
//! | `hotellingHours` | 13 / 14 / 15 | `hotellingHours` | zone |
//! | `ONI` | 4 | `SHO` | link |
//!
//! # `WithRegClassID` only
//!
//! Every `Processing` section has a `WithRegClassID` and a `NoRegClassID`
//! variant. `ActivityCalculator.java` force-enables the former
//! unconditionally — the `outputEmissionsBreakdownSelection.regClassID`
//! conditional that once chose between them is commented out in the
//! 2017-09-29 source. `NoRegClassID` is therefore dead in current MOVES, the
//! validation fixtures only ever exercise `WithRegClassID`, and this port
//! implements `WithRegClassID` alone. The canonical SQL retains `NoRegClassID`
//! as reference.
//!
//! # Module map
//!
//! | Module | Ports |
//! |--------|-------|
//! | [`inputs`] | the extracted input tables and the iteration context |
//! | [`model`] | the output and intermediate row types |
//! | [`fuelfraction`] | `createSourceTypeFuelFraction` and the source-bin join |
//! | [`hours`] | `SourceHours`, `SHO`, `ONI`, `SHP`, `Starts` |
//! | [`hotelling`] | `ExtendedIdleHours`, `hotellingHours` |
//! | [`population`] | `Population` (Non-Project and Project domains) |
//!
//! [`ActivityCalculator::run`] is the numerical entry point: it builds
//! `sourceTypeFuelFraction` once, then runs each [`ActivityConfig`]-enabled
//! section.
//!
//! # Data-plane status
//!
//! [`ActivityCalculator::run`] is fully exercised by this crate's tests. The
//! [`Calculator`] trait's [`execute`](Calculator::execute) method is a shell:
//! the [`CalculatorContext`] it receives exposes only the placeholder
//! `ExecutionTables` / `ScratchNamespace`, which have no row storage yet.
//! (`DataFrameStore`) lands that storage; `execute` will then
//! materialise an [`ActivityInputs`] from the context, call
//! [`run`](ActivityCalculator::run), and write the rows back. Until then
//! `execute` returns an empty [`CalculatorOutput`].
//!
//! [`subscriptions`](Calculator::subscriptions) and
//! [`registrations`](Calculator::registrations) are empty, matching the
//! `ActivityCalculator` entry in
//! `characterization/calculator-chains/calculator-dag.json`
//! (`subscribes_directly: false`, `registrations_count: 0`). The Java builds
//! its subscription set dynamically inside `subscribeToMe` from the RunSpec
//! output flags, so `CalculatorInfo.txt` — and the DAG reconstructed from it
//! records no static `Subscribe` directive; and the constructor comments
//! that the calculator "doesn't determine pollutants in any way, so it does
//! not register itself". The [`ActivityType`] metadata carries the per-type
//! process set the dynamic subscription draws on.

pub mod fuelfraction;
pub mod hotelling;
pub mod hours;
pub mod inputs;
pub mod model;
pub mod population;
mod rowbuild;

use moves_calculator_info::{Granularity, Priority};
use moves_data::{EmissionProcess, PollutantProcessAssociation};
use moves_framework::{
    Calculator, CalculatorContext, CalculatorOutput, CalculatorSubscription, DataFrameStoreTyped,
    Error,
};
use std::sync::OnceLock;

pub use fuelfraction::FuelFractionMode;
pub use inputs::{ActivityInputs, IterationContext};
pub use model::ActivityRow;

/// Stable module name in the calculator-chain DAG.
const CALCULATOR_NAME: &str = "ActivityCalculator";

/// Processes [`ActivityCalculator::subscribeToMe`] signs up for — the same
/// set as `TotalActivityGenerator`, since activity data (SHO, Starts, etc.)
/// is available for every process that generator covers.
const SUBSCRIBED_PROCESSES: [&str; 10] = [
    "Running Exhaust",
    "Start Exhaust",
    "Extended Idle Exhaust",
    "Auxiliary Power Exhaust",
    "Evap Permeation",
    "Evap Fuel Vapor Venting",
    "Evap Fuel Leaks",
    "Evap Non-Fuel Vapors",
    "Brakewear",
    "Tirewear",
];

fn build_subscriptions() -> Vec<CalculatorSubscription> {
    let priority = Priority::parse("EMISSION_CALCULATOR")
        .expect("\"EMISSION_CALCULATOR\" is a valid MasterLoop priority");
    SUBSCRIBED_PROCESSES
        .iter()
        .filter_map(|&name| {
            let process = EmissionProcess::find_by_name(name)?;
            Some(CalculatorSubscription::new(
                process.id,
                Granularity::Year,
                priority,
            ))
        })
        .collect()
}

/// Whether the activity-type key is formed at link or zone resolution — the
/// Java `ActivityInfo.locationLevel`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocationLevel {
    /// Keyed per road link.
    Link,
    /// Keyed per zone.
    Zone,
}

/// One of the eight activity types `ActivityCalculator` handles — the Rust
/// analogue of a Java `ActivityInfo` row.
///
/// The metadata methods ([`process_ids`](Self::process_ids),
/// [`off_network_only`](Self::off_network_only), …) reproduce the
/// `ActivityInfo` fields the Java `subscribeToMe` / `doesProcessContext` use
/// to decide, per master-loop iteration, whether the type is computed. The
/// pure [`ActivityCalculator::run`] does not re-derive that gating — the
/// caller passes the already-decided set through [`ActivityConfig`] — but the
/// metadata is kept for the wiring that will.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivityType {
    /// Source hours (`activityTypeID` 2).
    SourceHours,
    /// Extended idle hours (`activityTypeID` 3).
    ExtendedIdleHours,
    /// Hotelling hours (`activityTypeID` 13 / 14 / 15).
    HotellingHours,
    /// Source hours operating (`activityTypeID` 4).
    Sho,
    /// Source hours parked (`activityTypeID` 5).
    Shp,
    /// Vehicle population (`activityTypeID` 6).
    Population,
    /// Engine starts (`activityTypeID` 7).
    Starts,
    /// Off-network idle (`activityTypeID` 4) — the `SHO` table processed for
    /// off-network links.
    Oni,
}

impl ActivityType {
    /// The eight activity types, in the order the Java `activities` array
    /// declares them.
    #[must_use]
    pub const fn all() -> [ActivityType; 8] {
        [
            ActivityType::SourceHours,
            ActivityType::ExtendedIdleHours,
            ActivityType::HotellingHours,
            ActivityType::Sho,
            ActivityType::Shp,
            ActivityType::Population,
            ActivityType::Starts,
            ActivityType::Oni,
        ]
    }

    /// The `ActivityInfo.sectionName` — the `ActivityCalculator.sql` section
    /// this type enables.
    #[must_use]
    pub const fn section_name(self) -> &'static str {
        match self {
            ActivityType::SourceHours => "SourceHours",
            ActivityType::ExtendedIdleHours => "ExtendedIdleHours",
            ActivityType::HotellingHours => "hotellingHours",
            ActivityType::Sho => "SHO",
            ActivityType::Shp => "SHP",
            ActivityType::Population => "Population",
            ActivityType::Starts => "Starts",
            ActivityType::Oni => "ONI",
        }
    }

    /// The emission processes that generate this activity — the
    /// `ActivityInfo.processIDs` the Java subscribes the calculator to.
    #[must_use]
    pub const fn process_ids(self) -> &'static [i32] {
        match self {
            ActivityType::SourceHours => &[11, 12, 13],
            ActivityType::ExtendedIdleHours => &[90],
            ActivityType::HotellingHours => &[91],
            ActivityType::Sho => &[1, 9, 10, 11, 12, 13],
            ActivityType::Shp => &[11, 12, 13],
            ActivityType::Population => &[1, 2, 9, 10, 11, 12, 13, 90, 91],
            ActivityType::Starts => &[2],
            ActivityType::Oni => &[1],
        }
    }

    /// `ActivityInfo.offNetworkOnly` — `true` when the type is computed only
    /// for the off-network road type (`roadTypeID` 1).
    #[must_use]
    pub const fn off_network_only(self) -> bool {
        matches!(
            self,
            ActivityType::ExtendedIdleHours
                | ActivityType::HotellingHours
                | ActivityType::Shp
                | ActivityType::Starts
                | ActivityType::Oni
        )
    }

    /// `ActivityInfo.onNetworkOnly` — `true` when the type is computed only
    /// for non-off-network road types. Only `SHO` sets this.
    #[must_use]
    pub const fn on_network_only(self) -> bool {
        matches!(self, ActivityType::Sho)
    }

    /// `ActivityInfo.locationLevel` — the resolution of the dedup key the
    /// Java forms for the type.
    #[must_use]
    pub const fn location_level(self) -> LocationLevel {
        match self {
            ActivityType::SourceHours | ActivityType::Sho | ActivityType::Oni => {
                LocationLevel::Link
            }
            ActivityType::ExtendedIdleHours
            | ActivityType::HotellingHours
            | ActivityType::Shp
            | ActivityType::Population
            | ActivityType::Starts => LocationLevel::Zone,
        }
    }
}

/// The model domain — the Java `RunSpec.domain`. It selects which
/// `Population` script variant runs; the other seven activity types are
/// domain-independent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Domain {
    /// A County / Nation / default-scale run — `Population` is allocated from
    /// `sourceTypeAgePopulation`.
    #[default]
    NonProject,
    /// A Project-scale run — `Population` is allocated from `offNetworkLink`
    /// and `linkSourceTypeHour`.
    Project,
}

/// What [`ActivityCalculator::run`] should compute for one master-loop
/// iteration.
///
/// The Java decides this from the RunSpec — `enableActivity` gates each type
/// on an output flag (`outputSHO`, `outputStarts`, …), `RunSpec.domain`
/// picks the [`Domain`], and `CompilationFlags.USE_FUELUSAGEFRACTION` picks
/// the [`FuelFractionMode`]. The pure `run` takes the already-resolved
/// decision so it stays free of RunSpec plumbing.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ActivityConfig {
    /// The activity types to compute, in order. A type listed twice is
    /// computed twice; the caller (the Java master loop, later) is
    /// responsible for the per-iteration deduplication.
    pub enabled: Vec<ActivityType>,
    /// The model domain — selects the `Population` variant.
    pub domain: Domain,
    /// Which `createSourceTypeFuelFraction` variant to run.
    pub fuel_fraction_mode: FuelFractionMode,
}

impl ActivityConfig {
    /// A config that computes every [`ActivityType`] with the default
    /// [`Domain`] and [`FuelFractionMode`].
    #[must_use]
    pub fn all_activities() -> Self {
        Self {
            enabled: ActivityType::all().to_vec(),
            domain: Domain::default(),
            fuel_fraction_mode: FuelFractionMode::default(),
        }
    }
}

/// Default-DB / scratch tables [`ActivityCalculator::run`] reads, named as
/// `ActivityCalculator.sql` references them. (`InputDataManager`)
/// uses these for lazy loading; exact default-DB casing is reconciled when
/// the data plane lands.
static INPUT_TABLES: &[&str] = &[
    "HourDay",
    "link",
    "sourceUseType",
    "runSpecSourceType",
    "runSpecSourceFuelType",
    "SourceHours",
    "SHO",
    "SHP",
    "Starts",
    "hotellingHours",
    "hotellingActivityDistribution",
    "RegClassSourceTypeFraction",
    "sampleVehiclePopulation",
    "fuelUsageFraction",
    "sourceTypeModelYear",
    "roadTypeDistribution",
    "zoneRoadType",
    "sourceTypeAgePopulation",
    "offNetworkLink",
    "linkSourceTypeHour",
    "sourceTypeAgeDistribution",
];

/// The Activity Calculator.
///
/// A zero-sized value type: the calculator owns no per-run state, exactly as
/// the [`Calculator`] trait contract requires. All run-varying input flows
/// through [`ActivityInputs`].
#[derive(Debug, Clone, Copy, Default)]
pub struct ActivityCalculator;

impl ActivityCalculator {
    /// Stable module name — matches the `ActivityCalculator` entry in the
    /// calculator-chain DAG.
    pub const NAME: &'static str = CALCULATOR_NAME;

    /// Compute the activity rows for one master-loop iteration.
    ///
    /// Ports the `Processing` section of `ActivityCalculator.sql`: build the
    /// `sourceTypeFuelFraction` source-bin split once, then run each activity
    /// section [`config`](ActivityConfig::enabled) enables and concatenate
    /// the rows. `SHO` and `ONI` share one section body (the SQL is
    /// identical); `Population` dispatches on [`ActivityConfig::domain`].
    #[must_use]
    pub fn run(&self, inputs: &ActivityInputs, config: &ActivityConfig) -> Vec<ActivityRow> {
        // createSourceTypeFuelFraction — built once, shared by every section.
        let source_type_fuel_fraction =
            fuelfraction::create_source_type_fuel_fraction(inputs, config.fuel_fraction_mode);
        let fuel = fuelfraction::FuelFractionIndex::new(&source_type_fuel_fraction);
        let reg = fuelfraction::RegClassIndex::new(&inputs.reg_class_source_type_fraction);

        let mut out = Vec::new();
        for &activity in &config.enabled {
            let rows = match activity {
                ActivityType::SourceHours => hours::source_hours(inputs, &fuel, &reg),
                // ONI's SQL is byte-identical to SHO's — both process `SHO`.
                ActivityType::Sho | ActivityType::Oni => hours::sho(inputs, &fuel, &reg),
                ActivityType::Shp => hours::shp(inputs, &fuel, &reg),
                ActivityType::Starts => hours::starts(inputs, &fuel, &reg),
                ActivityType::ExtendedIdleHours => hotelling::extended_idle_hours(inputs, &reg),
                ActivityType::HotellingHours => hotelling::hotelling_hours(inputs, &reg),
                ActivityType::Population => match config.domain {
                    Domain::NonProject => population::population_non_project(inputs, &fuel, &reg),
                    Domain::Project => population::population_project(inputs, &fuel, &reg),
                },
            };
            out.extend(rows);
        }
        out
    }
}

/// Construct the calculator as a boxed trait object — matches the engine's
/// calculator-factory signature so the calculator registry can register it.
#[must_use]
pub fn factory() -> Box<dyn Calculator> {
    Box::new(ActivityCalculator)
}

impl Calculator for ActivityCalculator {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    /// Subscribes to the same nine processes as [`TotalActivityGenerator`],
    /// at `EMISSION_CALCULATOR` priority so the calculator runs after
    /// generators have written their scratch tables.
    ///
    /// [`TotalActivityGenerator`]: crate::generators::totalactivitygenerator
    fn subscriptions(&self) -> &[CalculatorSubscription] {
        static SUBSCRIPTIONS: OnceLock<Vec<CalculatorSubscription>> = OnceLock::new();
        SUBSCRIPTIONS.get_or_init(build_subscriptions).as_slice()
    }

    /// Empty: the calculator emits activity, not emissions, and registers no
    /// `(pollutant, process)` pair.
    fn registrations(&self) -> &[PollutantProcessAssociation] {
        &[]
    }

    fn input_tables(&self) -> &[&'static str] {
        INPUT_TABLES
    }

    fn execute(&self, ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        let tables = ctx.tables();
        let pos = ctx.position();
        let context = inputs::IterationContext {
            year: pos.time.year.map(|y| y as i32).unwrap_or(0),
            state_id: pos.location.state_id.map(|s| s as i32).unwrap_or(0),
            county_id: pos.location.county_id.map(|c| c as i32).unwrap_or(0),
            zone_id: pos.location.zone_id.map(|z| z as i32).unwrap_or(0),
            link_id: pos.location.link_id.map(|l| l as i32).unwrap_or(0),
            road_type_id: 0,
            fuel_year_id: pos.time.year.map(|y| y as i32).unwrap_or(0),
        };
        let activity_inputs = inputs::ActivityInputs {
            context,
            source_hours: tables.iter_typed("SourceHours")?,
            sho: tables.iter_typed("SHO")?,
            shp: tables.iter_typed("SHP")?,
            starts: tables.iter_typed("Starts")?,
            hotelling_hours: tables.iter_typed("hotellingHours")?,
            hour_day: tables.iter_typed("HourDay")?,
            link: tables.iter_typed("link")?,
            reg_class_source_type_fraction: tables.iter_typed("RegClassSourceTypeFraction")?,
            hotelling_activity_distribution: tables.iter_typed("hotellingActivityDistribution")?,
            sample_vehicle_population: tables.iter_typed("sampleVehiclePopulation")?,
            fuel_usage_fraction: tables.iter_typed("fuelUsageFraction")?,
            source_type_model_year: tables.iter_typed("sourceTypeModelYear")?,
            run_spec_source_fuel_type: tables.iter_typed("runSpecSourceFuelType")?,
            source_use_type: tables.iter_typed("sourceUseType")?,
            road_type_distribution: tables.iter_typed("roadTypeDistribution")?,
            zone_road_type: tables.iter_typed("zoneRoadType")?,
            source_type_age_population: tables.iter_typed("sourceTypeAgePopulation")?,
            run_spec_source_type: tables.iter_typed("runSpecSourceType")?,
            off_network_link: tables.iter_typed("offNetworkLink")?,
            link_source_type_hour: tables.iter_typed("linkSourceTypeHour")?,
            source_type_age_distribution: tables.iter_typed("sourceTypeAgeDistribution")?,
        };
        let config = ActivityConfig::all_activities();
        let rows = self.run(&activity_inputs, &config);
        crate::wiring::emit_rows(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::inputs::{
        HourDayRow, LinkRow, RegClassSourceTypeFractionRow, RunSpecSourceFuelTypeRow,
        SampleVehiclePopulationRow, SourceHoursRow, SourceTypeModelYearRow, StartsRow,
    };
    use super::*;
    use moves_framework::TableRow;

    fn context() -> IterationContext {
        IterationContext {
            year: 2020,
            state_id: 26,
            county_id: 26161,
            zone_id: 261610,
            link_id: 900,
            road_type_id: 4,
            fuel_year_id: 2020,
        }
    }

    /// Inputs covering a single source bin: source type 21, model year 2015
    /// (age 5 of 2020), one fuel type, one regulatory class — every weight
    /// is 1, so `activity` equals the base table value.
    fn whole_bin_inputs() -> ActivityInputs {
        ActivityInputs {
            context: context(),
            hour_day: vec![HourDayRow {
                hour_day_id: 85,
                day_id: 5,
                hour_id: 8,
            }],
            link: vec![LinkRow {
                link_id: 900,
                zone_id: 261610,
                road_type_id: 4,
                link_volume: 0.0,
            }],
            source_hours: vec![SourceHoursRow {
                hour_day_id: 85,
                month_id: 7,
                year_id: 2020,
                age_id: 5,
                link_id: 900,
                source_type_id: 21,
                source_hours: 100.0,
            }],
            starts: vec![StartsRow {
                hour_day_id: 85,
                month_id: 7,
                year_id: 2020,
                age_id: 5,
                zone_id: 261610,
                source_type_id: 21,
                starts: 12.0,
            }],
            reg_class_source_type_fraction: vec![RegClassSourceTypeFractionRow {
                source_type_id: 21,
                fuel_type_id: 1,
                model_year_id: 2015,
                reg_class_id: 30,
                reg_class_fraction: 1.0,
            }],
            sample_vehicle_population: vec![SampleVehiclePopulationRow {
                source_type_model_year_id: 9,
                fuel_type_id: 1,
                stmy_fraction: 1.0,
            }],
            source_type_model_year: vec![SourceTypeModelYearRow {
                source_type_model_year_id: 9,
                source_type_id: 21,
                model_year_id: 2015,
            }],
            run_spec_source_fuel_type: vec![RunSpecSourceFuelTypeRow {
                source_type_id: 21,
                fuel_type_id: 1,
            }],
            ..ActivityInputs::default()
        }
    }

    #[test]
    fn name_matches_dag_module() {
        assert_eq!(ActivityCalculator.name(), "ActivityCalculator");
    }

    #[test]
    fn subscribes_to_activity_processes_and_has_no_emission_registrations() {
        let calc = ActivityCalculator;
        let subs = calc.subscriptions();
        // "Evap Non-Fuel Vapors" does not resolve to a valid process; nine of
        // ten SUBSCRIBED_PROCESSES entries produce a subscription.
        assert_eq!(subs.len(), 9, "expected 9 activity-process subscriptions");
        assert!(
            subs.iter().all(|s| s.granularity == Granularity::Year),
            "all subscriptions must be YEAR granularity"
        );
        assert!(calc.registrations().is_empty());
        assert!(calc.upstream().is_empty());
    }

    #[test]
    fn input_tables_cover_every_section() {
        let calc = ActivityCalculator;
        for table in [
            "SourceHours",
            "SHO",
            "SHP",
            "Starts",
            "hotellingHours",
            "sampleVehiclePopulation",
            "offNetworkLink",
        ] {
            assert!(calc.input_tables().contains(&table), "missing {table}");
        }
    }

    #[test]
    fn execute_returns_nonempty_dataframe_for_minimal_inputs() {
        use inputs::{
            FuelUsageFractionRow, HotellingActivityDistributionRow, HotellingHoursRow, HourDayRow,
            LinkRow, LinkSourceTypeHourRow, OffNetworkLinkRow, RegClassSourceTypeFractionRow,
            RoadTypeDistributionRow, RunSpecSourceFuelTypeRow, RunSpecSourceTypeRow,
            SampleVehiclePopulationRow, ShoRow, ShpRow, SourceHoursRow,
            SourceTypeAgeDistributionRow, SourceTypeAgePopulationRow, SourceTypeModelYearRow,
            SourceUseTypeRow, StartsRow, ZoneRoadTypeRow,
        };
        use moves_framework::execution::execution_db::{
            ExecutionLocation, ExecutionTime, IterationPosition,
        };
        use moves_framework::{DataFrameStore, InMemoryStore};
        let wb = whole_bin_inputs();
        let mut store = InMemoryStore::new();
        store.insert(
            "SourceHours",
            SourceHoursRow::into_dataframe(wb.source_hours.clone()).unwrap(),
        );
        store.insert("SHO", ShoRow::into_dataframe(wb.sho.clone()).unwrap());
        store.insert("SHP", ShpRow::into_dataframe(wb.shp.clone()).unwrap());
        store.insert(
            "Starts",
            StartsRow::into_dataframe(wb.starts.clone()).unwrap(),
        );
        store.insert(
            "hotellingHours",
            HotellingHoursRow::into_dataframe(wb.hotelling_hours.clone()).unwrap(),
        );
        store.insert(
            "HourDay",
            HourDayRow::into_dataframe(wb.hour_day.clone()).unwrap(),
        );
        store.insert("link", LinkRow::into_dataframe(wb.link.clone()).unwrap());
        store.insert(
            "RegClassSourceTypeFraction",
            RegClassSourceTypeFractionRow::into_dataframe(
                wb.reg_class_source_type_fraction.clone(),
            )
            .unwrap(),
        );
        store.insert(
            "hotellingActivityDistribution",
            HotellingActivityDistributionRow::into_dataframe(
                wb.hotelling_activity_distribution.clone(),
            )
            .unwrap(),
        );
        store.insert(
            "sampleVehiclePopulation",
            SampleVehiclePopulationRow::into_dataframe(wb.sample_vehicle_population.clone())
                .unwrap(),
        );
        store.insert(
            "fuelUsageFraction",
            FuelUsageFractionRow::into_dataframe(wb.fuel_usage_fraction.clone()).unwrap(),
        );
        store.insert(
            "sourceTypeModelYear",
            SourceTypeModelYearRow::into_dataframe(wb.source_type_model_year.clone()).unwrap(),
        );
        store.insert(
            "runSpecSourceFuelType",
            RunSpecSourceFuelTypeRow::into_dataframe(wb.run_spec_source_fuel_type.clone()).unwrap(),
        );
        store.insert(
            "sourceUseType",
            SourceUseTypeRow::into_dataframe(wb.source_use_type.clone()).unwrap(),
        );
        store.insert(
            "roadTypeDistribution",
            RoadTypeDistributionRow::into_dataframe(wb.road_type_distribution.clone()).unwrap(),
        );
        store.insert(
            "zoneRoadType",
            ZoneRoadTypeRow::into_dataframe(wb.zone_road_type.clone()).unwrap(),
        );
        store.insert(
            "sourceTypeAgePopulation",
            SourceTypeAgePopulationRow::into_dataframe(wb.source_type_age_population.clone())
                .unwrap(),
        );
        store.insert(
            "runSpecSourceType",
            RunSpecSourceTypeRow::into_dataframe(wb.run_spec_source_type.clone()).unwrap(),
        );
        store.insert(
            "offNetworkLink",
            OffNetworkLinkRow::into_dataframe(wb.off_network_link.clone()).unwrap(),
        );
        store.insert(
            "linkSourceTypeHour",
            LinkSourceTypeHourRow::into_dataframe(wb.link_source_type_hour.clone()).unwrap(),
        );
        store.insert(
            "sourceTypeAgeDistribution",
            SourceTypeAgeDistributionRow::into_dataframe(wb.source_type_age_distribution.clone())
                .unwrap(),
        );

        let position = IterationPosition {
            iteration: 0,
            process_id: None,
            location: ExecutionLocation::link(26, 26_161, 90, 5001),
            time: ExecutionTime::year(2020),
        };
        let ctx = CalculatorContext::with_position_and_tables(position, store);
        let calc = ActivityCalculator;
        let out = calc.execute(&ctx).expect("execute ok");
        assert!(out.dataframe().is_some(), "expected non-empty DataFrame");
        assert!(
            out.dataframe().unwrap().height() > 0,
            "expected at least one row"
        );
    }

    #[test]
    fn calculator_can_be_held_as_a_trait_object() {
        let calc = factory();
        assert_eq!(calc.name(), "ActivityCalculator");
    }

    #[test]
    fn run_with_no_enabled_sections_produces_nothing() {
        let out = ActivityCalculator.run(&whole_bin_inputs(), &ActivityConfig::default());
        assert!(out.is_empty());
    }

    #[test]
    fn run_computes_each_enabled_section() {
        let inputs = whole_bin_inputs();
        let config = ActivityConfig {
            enabled: vec![ActivityType::SourceHours, ActivityType::Starts],
            ..ActivityConfig::default()
        };
        let out = ActivityCalculator.run(&inputs, &config);
        // One whole-bin source-hours row and one whole-bin starts row.
        assert_eq!(out.len(), 2);
        let source_hours = out.iter().find(|r| r.activity_type_id == 2).unwrap();
        assert!((source_hours.activity - 100.0).abs() < 1e-9);
        let starts = out.iter().find(|r| r.activity_type_id == 7).unwrap();
        assert!((starts.activity - 12.0).abs() < 1e-9);
    }

    #[test]
    fn run_processes_sho_and_oni_through_the_same_section() {
        use super::inputs::ShoRow;
        let mut inputs = whole_bin_inputs();
        inputs.sho = vec![ShoRow {
            hour_day_id: 85,
            month_id: 7,
            year_id: 2020,
            age_id: 5,
            link_id: 900,
            source_type_id: 21,
            sho: 50.0,
        }];
        let sho_only = ActivityCalculator.run(
            &inputs,
            &ActivityConfig {
                enabled: vec![ActivityType::Sho],
                ..ActivityConfig::default()
            },
        );
        let oni_only = ActivityCalculator.run(
            &inputs,
            &ActivityConfig {
                enabled: vec![ActivityType::Oni],
                ..ActivityConfig::default()
            },
        );
        assert_eq!(sho_only, oni_only);
        assert_eq!(sho_only.len(), 1);
        assert_eq!(sho_only[0].activity_type_id, 4);
    }

    #[test]
    fn activity_type_metadata_matches_the_java_table() {
        assert_eq!(ActivityType::all().len(), 8);
        assert_eq!(ActivityType::SourceHours.section_name(), "SourceHours");
        assert_eq!(ActivityType::Sho.process_ids(), &[1, 9, 10, 11, 12, 13]);
        assert_eq!(ActivityType::Oni.process_ids(), &[1]);
        // SHO is the only on-network-only type; ONI is off-network-only.
        assert!(ActivityType::Sho.on_network_only());
        assert!(!ActivityType::Sho.off_network_only());
        assert!(ActivityType::Oni.off_network_only());
        assert_eq!(
            ActivityType::SourceHours.location_level(),
            LocationLevel::Link
        );
        assert_eq!(
            ActivityType::Population.location_level(),
            LocationLevel::Zone
        );
    }
}
