//! Total Activity Generator — Phase 3 Task 36.
//!
//! Pure-Rust port of
//! `gov/epa/otaq/moves/master/implementation/ghg/TotalActivityGenerator.java`
//! (2,793 lines of Java + embedded SQL). This is **the single most important
//! generator**: every onroad emission is `rate × activity`, and this
//! generator computes the activity — total VMT, source hours operating,
//! engine starts, source hours parked, and hotelling hours — that every
//! running-, start-, evap- and extended-idle calculator multiplies its rate
//! against.
//!
//! # What the Java did, and what this port keeps
//!
//! The Java generator ran inside the master loop: once per calendar year it
//! grew the vehicle population and VMT forward from the nearest base year,
//! split them across road type / source type / age / hour, and converted
//! the result to an activity basis; once per zone it allocated that activity
//! onto individual road links. It did all of this through `INSERT … SELECT`
//! statements against a MariaDB execution database.
//!
//! The port keeps the **computation** — every growth recurrence, join,
//! weighting, and aggregation — and replaces the database I/O with plain
//! values: a [`TotalActivityInputs`] in, a [`TotalActivityOutput`] out. The
//! `CREATE TABLE` / `TRUNCATE` scaffolding has no algorithmic content and no
//! analogue here.
//!
//! # Module map
//!
//! | Module | Ports | Algorithm steps |
//! |--------|-------|-----------------|
//! | [`inputs`] | the default-DB / RunSpec input tables | — |
//! | [`model`] | the working and output tables | — |
//! | [`population`] | `determineBaseYear`, `calculateBaseYearPopulation`, `growPopulationToAnalysisYear` | 110-139 |
//! | [`travel`] | `calculateFractionOfTravelUsingHPMS`, `growVMTToAnalysisYear` | 140-159 |
//! | [`vmt`] | `allocateVMTByRoadTypeSourceAge`, `calculateVMTByRoadwayHour` | 160-179 |
//! | [`activity`] | `convertVMTToTotalActivityBasis` | 180-189 |
//! | [`allocation`] | the pure kernels of `allocateTotalActivityBasis`, `calculateDistance` | 190-209 |
//!
//! [`TotalActivityGenerator::run`] chains steps 110-189 — the year/zone
//! activity computation — into a [`TotalActivityOutput`]. Steps 190-209 are
//! the *spatial allocation* of that activity onto links: their arithmetic is
//! ported as the standalone pure kernels in [`allocation`], but the master
//! loop's per-`(process, zone, link)` sequencing of those kernels — together
//! with the three external `database/Adjust*.sql` scripts the Java shells
//! out to — is orchestration that lands with the Task 50 `execute` wiring,
//! exactly as Task 29's `SourceBinDistributionGenerator` deferred its
//! per-callback dedup state.
//!
//! # Data-plane status
//!
//! [`TotalActivityGenerator::run`] is the numerical entry point and is fully
//! exercised by this crate's tests. The [`Generator`] trait's
//! [`execute`](Generator::execute) method is a shell: the
//! [`CalculatorContext`] it receives exposes only the Phase 2 placeholder
//! `ExecutionTables` / `ScratchNamespace`, which have no row storage yet.
//! Task 50 (`DataFrameStore`) lands that storage; `execute` will then
//! materialise a [`TotalActivityInputs`] from the context, call
//! [`run`](TotalActivityGenerator::run), and write the
//! [`TotalActivityOutput`] back into the scratch namespace. Until then
//! `execute` returns an empty [`CalculatorOutput`] and the metadata methods
//! carry the real wiring information the registry needs.

pub mod activity;
pub mod allocation;
pub mod inputs;
pub mod model;
pub mod population;
pub mod travel;
pub mod vmt;

use std::sync::OnceLock;

use moves_calculator_info::{Granularity, Priority};
use moves_data::EmissionProcess;
use moves_framework::{
    CalculatorContext, CalculatorOutput, CalculatorSubscription, Error, Generator,
};

pub use inputs::TotalActivityInputs;
pub use model::TotalActivityOutput;

/// Stable module name in the calculator-chain DAG.
const GENERATOR_NAME: &str = "TotalActivityGenerator";

/// The processes the Java `subscribeToMe` signs up for, paired with the
/// `MasterLoopPriority` it uses for each.
///
/// Running Exhaust subscribes at `GENERATOR-3` ("Run after BaseRateGenerator"
/// — the Java comment); every other process at plain `GENERATOR`.
/// `"Evap Non-Fuel Vapors"` has no row in the MOVES process table, so
/// [`EmissionProcess::find_by_name`] drops it — the exact behaviour of the
/// Java `if (process != null)` guard. The nine that resolve match the
/// `TotalActivityGenerator` subscription set in
/// `characterization/calculator-chains/calculator-dag.json`.
///
/// **Fidelity note.** That DAG, reconstructed from `CalculatorInfo.txt`,
/// records Running Exhaust at plain `GENERATOR`; the Java `subscribeToMe` —
/// the runtime source of truth — overrides it to `GENERATOR-3` so the
/// generator runs after `BaseRateGenerator` (`GENERATOR-2`). This port
/// follows the Java. Task 44's generator-integration validation reconciles
/// the metadata.
const SUBSCRIBED_PROCESSES: [(&str, &str); 10] = [
    ("Running Exhaust", "GENERATOR-3"),
    ("Start Exhaust", "GENERATOR"),
    ("Extended Idle Exhaust", "GENERATOR"),
    ("Auxiliary Power Exhaust", "GENERATOR"),
    ("Evap Permeation", "GENERATOR"),
    ("Evap Fuel Vapor Venting", "GENERATOR"),
    ("Evap Fuel Leaks", "GENERATOR"),
    ("Evap Non-Fuel Vapors", "GENERATOR"),
    ("Brakewear", "GENERATOR"),
    ("Tirewear", "GENERATOR"),
];

/// Default-DB and RunSpec tables [`TotalActivityGenerator::run`] reads.
/// Names match the casing used in the MOVES default database.
static INPUT_TABLES: &[&str] = &[
    "Year",
    "SourceTypeYear",
    "SourceTypeAgeDistribution",
    "SourceTypeAge",
    "SourceUseType",
    "HPMSVTypeYear",
    "RunSpecSourceType",
    "SourceTypeYearVMT",
    "RoadType",
    "RoadTypeDistribution",
    "SourceTypeDayVMT",
    "HPMSVTypeDay",
    "MonthVMTFraction",
    "DayVMTFraction",
    "HourVMTFraction",
    "HourDay",
    "DayOfAnyWeek",
    "MonthOfAnyYear",
    "SourceTypeHour",
    "RunSpecDay",
    "AvgSpeedBin",
    "AvgSpeedDistribution",
    "HourOfAnyDay",
    "ZoneRoadType",
    "hotellingCalendarYear",
    "hotellingHoursPerDay",
    "SampleVehicleDay",
    "SampleVehicleTrip",
    "StartsPerVehicle",
];

/// Scratch tables the generator writes for downstream calculators — the
/// year/zone activity tables [`run`](TotalActivityGenerator::run) produces.
/// Their per-link spatial allocation (`SHO`, `SHP`, `SourceHours`,
/// `hotellingHours`) is sequenced by the master loop from the
/// [`allocation`] kernels once the Task 50 data plane lands.
static OUTPUT_TABLES: &[&str] = &[
    "SourceTypeAgePopulation",
    "SourceTypeAgeDistribution",
    "TravelFraction",
    "AnalysisYearVMT",
    "AnnualVMTByAgeRoadway",
    "VMTByAgeRoadwayHour",
    "vmtByMYRoadHourFraction",
    "SHOByAgeRoadwayHour",
    "VMTByAgeRoadwayDay",
    "IdleHoursByAgeHour",
    "StartsByAgeHour",
    "SHPByAgeHour",
];

/// Resolve [`SUBSCRIBED_PROCESSES`] into the generator's subscription set:
/// every resolvable process, at `YEAR` granularity, with its declared
/// priority.
fn build_subscriptions() -> Vec<CalculatorSubscription> {
    SUBSCRIBED_PROCESSES
        .iter()
        .filter_map(|&(name, priority)| {
            let process = EmissionProcess::find_by_name(name)?;
            let priority =
                Priority::parse(priority).expect("SUBSCRIBED_PROCESSES priorities are well-formed");
            Some(CalculatorSubscription::new(
                process.id,
                Granularity::Year,
                priority,
            ))
        })
        .collect()
}

/// The Total Activity Generator.
///
/// A zero-sized value type: the generator owns no per-run state, exactly as
/// the [`Generator`] trait contract requires. All run-varying input flows
/// through [`TotalActivityInputs`].
#[derive(Debug, Clone, Copy, Default)]
pub struct TotalActivityGenerator;

impl TotalActivityGenerator {
    /// Stable module name — matches the `TotalActivityGenerator` entry in
    /// the Phase 1 calculator-chain DAG.
    pub const NAME: &'static str = GENERATOR_NAME;

    /// Compute the year/zone activity tables — algorithm steps 110-189.
    ///
    /// Ports the year- and zone-scoped body of `executeLoop`: determine the
    /// base year, grow the vehicle population and HPMS-typed VMT forward to
    /// the analysis year, split VMT across road type / source type / age /
    /// hour, and convert it to a total-activity basis (`SHO`, hotelling
    /// hours, starts, `SHP`).
    ///
    /// When no base year is at or below [`TotalActivityInputs::analysis_year`]
    /// the Java logs the failure and abandons the year; this port returns an
    /// empty [`TotalActivityOutput`] in that case.
    #[must_use]
    pub fn run(&self, inputs: &TotalActivityInputs) -> TotalActivityOutput {
        let analysis_year = inputs.analysis_year;

        // Steps 110-139 — population.
        let Some(base_year) = population::determine_base_year(&inputs.year, analysis_year) else {
            return TotalActivityOutput::default();
        };
        let base_population = population::calculate_base_year_population(
            &inputs.source_type_year,
            &inputs.source_type_age_distribution,
            base_year,
        );
        let grown = population::grow_population_to_analysis_year(
            &base_population,
            &inputs.source_type_year,
            &inputs.source_type_age,
            &inputs.source_type_age_distribution,
            base_year,
            analysis_year,
        );

        // Steps 140-159 — HPMS travel fraction and VMT growth.
        let vmt_by_source_type =
            !inputs.source_type_day_vmt.is_empty() || !inputs.source_type_year_vmt.is_empty();
        let travel = travel::calculate_fraction_of_travel_using_hpms(
            &grown.population,
            &inputs.source_use_type,
            &inputs.source_type_age,
            analysis_year,
            vmt_by_source_type,
        );
        let analysis_year_vmt = travel::grow_vmt_to_analysis_year(
            &inputs.hpms_v_type_year,
            &inputs.run_spec_source_type,
            &inputs.source_use_type,
            base_year,
            analysis_year,
        );

        // Steps 160-179 — VMT allocation by road type, source, age, hour.
        let annual_vmt = vmt::allocate_vmt_by_road_type_source_age(
            &travel.travel_fraction,
            &inputs.road_type,
            &inputs.road_type_distribution,
            &analysis_year_vmt,
            &inputs.source_use_type,
            &inputs.source_type_year_vmt,
            analysis_year,
        );
        let from_annual = vmt::hourly_vmt_from_annual(
            &annual_vmt,
            &inputs.month_vmt_fraction,
            &inputs.day_vmt_fraction,
            &inputs.hour_vmt_fraction,
            &inputs.hour_day,
            &inputs.month_of_any_year,
        );
        let daily_tables = vmt::DailyVmtJoinTables {
            road_type_distribution: &inputs.road_type_distribution,
            hour_day: &inputs.hour_day,
            hour_vmt_fraction: &inputs.hour_vmt_fraction,
            travel_fraction: &travel.travel_fraction,
            day_of_any_week: &inputs.day_of_any_week,
        };
        let from_source_type_day = vmt::hourly_vmt_from_source_type_day(
            &inputs.source_type_day_vmt,
            &daily_tables,
            analysis_year,
        );
        let from_hpms_day = vmt::hourly_vmt_from_hpms_day(
            &inputs.hpms_v_type_day,
            &inputs.source_use_type,
            &daily_tables,
            analysis_year,
        );
        let vmt_by_age_roadway_hour =
            vmt::combine_hourly_vmt(from_annual, from_source_type_day, from_hpms_day);
        let vmt_by_my_road_hour_fraction =
            vmt::vmt_by_my_road_hour_fraction(&vmt_by_age_roadway_hour);

        // Steps 180-189 — conversion to total-activity basis.
        let source_type_hour_2 = activity::source_type_hour_expanded(
            &inputs.source_type_hour,
            &inputs.hour_day,
            &inputs.run_spec_day,
        );
        let average_speed = activity::average_speed(
            &inputs.road_type,
            &inputs.run_spec_source_type,
            &inputs.run_spec_day,
            &inputs.hour_of_any_day,
            &inputs.avg_speed_bin,
            &inputs.avg_speed_distribution,
            &inputs.hour_day,
        );
        let sho_by_age_roadway_hour =
            activity::sho_by_age_roadway_hour(&vmt_by_age_roadway_hour, &average_speed);
        let vmt_by_age_roadway_day = activity::vmt_by_age_roadway_day(
            &vmt_by_age_roadway_hour,
            &inputs.zone_road_type,
            &inputs.hotelling_calendar_year,
            inputs.zone_id,
            inputs.has_hotelling_hours_per_day_input,
        );
        let idle_hours_by_age_hour =
            activity::idle_hours_by_age_hour(&vmt_by_age_roadway_day, &source_type_hour_2);
        let starts_per_sample_vehicle = activity::starts_per_sample_vehicle(
            &inputs.sample_vehicle_day,
            &inputs.sample_vehicle_trip,
            &inputs.hour_day,
            &inputs.day_of_any_week,
        );
        let new_starts_per_vehicle = activity::starts_per_vehicle(
            &inputs.sample_vehicle_day,
            &starts_per_sample_vehicle,
            &inputs.starts_per_vehicle,
        );
        // StartsByAgeHour joins the full StartsPerVehicle table — the rows
        // already present plus the ones just computed.
        let mut starts_per_vehicle_full = inputs.starts_per_vehicle.clone();
        starts_per_vehicle_full.extend(new_starts_per_vehicle);
        let starts_by_age_hour =
            activity::starts_by_age_hour(&grown.population, &starts_per_vehicle_full);
        let shp_by_age_hour = activity::shp_by_age_hour(
            &sho_by_age_roadway_hour,
            &grown.population,
            &inputs.day_of_any_week,
        );

        TotalActivityOutput {
            source_type_age_population: grown.population,
            source_type_age_distribution_additions: grown.age_distribution_additions,
            travel_fraction: travel.travel_fraction,
            analysis_year_vmt,
            annual_vmt_by_age_roadway: annual_vmt,
            vmt_by_age_roadway_hour,
            vmt_by_my_road_hour_fraction,
            sho_by_age_roadway_hour,
            vmt_by_age_roadway_day,
            idle_hours_by_age_hour,
            starts_by_age_hour,
            shp_by_age_hour,
        }
    }
}

/// Construct the generator as a boxed trait object — matches the engine's
/// generator-factory signature so the calculator registry can register it.
#[must_use]
pub fn factory() -> Box<dyn Generator> {
    Box::new(TotalActivityGenerator)
}

impl Generator for TotalActivityGenerator {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn subscriptions(&self) -> &[CalculatorSubscription] {
        static SUBSCRIPTIONS: OnceLock<Vec<CalculatorSubscription>> = OnceLock::new();
        SUBSCRIPTIONS.get_or_init(build_subscriptions).as_slice()
    }

    fn input_tables(&self) -> &[&'static str] {
        INPUT_TABLES
    }

    fn output_tables(&self) -> &[&'static str] {
        OUTPUT_TABLES
    }

    /// Phase 2 skeleton: returns an empty [`CalculatorOutput`].
    ///
    /// [`CalculatorContext`] cannot yet surface the input tables or accept
    /// the activity-table output — its row storage lands with the Task 50
    /// `DataFrameStore`. The computation itself is ported and tested in
    /// [`TotalActivityGenerator::run`]; see the [module documentation](self).
    fn execute(&self, _ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        Ok(CalculatorOutput::empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use moves_data::ProcessId;

    /// Build a minimal one-source-type, one-base-year input that exercises
    /// the population → travel → VMT → activity chain end to end.
    fn minimal_inputs() -> TotalActivityInputs {
        use inputs::{
            AvgSpeedBinRow, AvgSpeedDistributionRow, DayOfAnyWeekRow, DayVmtFractionRow,
            HourDayRow, HourOfAnyDayRow, HourVmtFractionRow, MonthOfAnyYearRow,
            MonthVmtFractionRow, RoadTypeDistributionRow, RoadTypeRow, RunSpecDayRow,
            RunSpecSourceTypeRow, SourceTypeAgeDistributionRow, SourceTypeAgeRow,
            SourceTypeYearRow, SourceTypeYearVmtRow, SourceUseTypeRow, YearRow,
        };

        TotalActivityInputs {
            analysis_year: 2020,
            zone_id: 100,
            has_hotelling_hours_per_day_input: false,
            year: vec![YearRow {
                year_id: 2020,
                is_base_year: true,
            }],
            source_type_year: vec![SourceTypeYearRow {
                year_id: 2020,
                source_type_id: 21,
                source_type_population: 1000.0,
                migration_rate: 1.0,
                sales_growth_factor: 1.0,
            }],
            source_type_age_distribution: vec![
                SourceTypeAgeDistributionRow {
                    source_type_id: 21,
                    year_id: 2020,
                    age_id: 0,
                    age_fraction: 0.6,
                },
                SourceTypeAgeDistributionRow {
                    source_type_id: 21,
                    year_id: 2020,
                    age_id: 1,
                    age_fraction: 0.4,
                },
            ],
            source_type_age: vec![
                SourceTypeAgeRow {
                    source_type_id: 21,
                    age_id: 0,
                    survival_rate: 1.0,
                    relative_mar: 1.0,
                },
                SourceTypeAgeRow {
                    source_type_id: 21,
                    age_id: 1,
                    survival_rate: 1.0,
                    relative_mar: 1.0,
                },
            ],
            source_use_type: vec![SourceUseTypeRow {
                source_type_id: 21,
                hpms_v_type_id: 10,
            }],
            hpms_v_type_year: vec![],
            run_spec_source_type: vec![RunSpecSourceTypeRow { source_type_id: 21 }],
            // VMT supplied by source type.
            source_type_year_vmt: vec![SourceTypeYearVmtRow {
                year_id: 2020,
                source_type_id: 21,
                vmt: 8400.0,
            }],
            road_type: vec![RoadTypeRow { road_type_id: 2 }],
            road_type_distribution: vec![RoadTypeDistributionRow {
                source_type_id: 21,
                road_type_id: 2,
                road_type_vmt_fraction: 1.0,
            }],
            source_type_day_vmt: vec![],
            hpms_v_type_day: vec![],
            month_vmt_fraction: vec![MonthVmtFractionRow {
                source_type_id: 21,
                month_id: 1,
                month_vmt_fraction: 1.0,
            }],
            day_vmt_fraction: vec![DayVmtFractionRow {
                source_type_id: 21,
                month_id: 1,
                road_type_id: 2,
                day_id: 5,
                day_vmt_fraction: 1.0,
            }],
            hour_vmt_fraction: vec![HourVmtFractionRow {
                source_type_id: 21,
                road_type_id: 2,
                day_id: 5,
                hour_id: 8,
                hour_vmt_fraction: 1.0,
            }],
            hour_day: vec![HourDayRow {
                hour_day_id: 85,
                hour_id: 8,
                day_id: 5,
            }],
            day_of_any_week: vec![DayOfAnyWeekRow {
                day_id: 5,
                no_of_real_days: 1.0,
            }],
            month_of_any_year: vec![MonthOfAnyYearRow {
                month_id: 1,
                no_of_days: 7,
            }],
            source_type_hour: vec![],
            run_spec_day: vec![RunSpecDayRow { day_id: 5 }],
            avg_speed_bin: vec![AvgSpeedBinRow {
                avg_speed_bin_id: 1,
                avg_bin_speed: 60.0,
            }],
            avg_speed_distribution: vec![AvgSpeedDistributionRow {
                road_type_id: 2,
                source_type_id: 21,
                hour_day_id: 85,
                avg_speed_bin_id: 1,
                avg_speed_fraction: 1.0,
            }],
            hour_of_any_day: vec![HourOfAnyDayRow { hour_id: 8 }],
            zone_road_type: vec![],
            hotelling_calendar_year: vec![],
            sample_vehicle_day: vec![],
            sample_vehicle_trip: vec![],
            starts_per_vehicle: vec![],
        }
    }

    #[test]
    fn name_matches_dag_module() {
        assert_eq!(TotalActivityGenerator.name(), "TotalActivityGenerator");
    }

    #[test]
    fn subscribes_to_nine_year_granularity_processes() {
        let gen = TotalActivityGenerator;
        let subs = gen.subscriptions();
        // Ten processes are listed; "Evap Non-Fuel Vapors" does not resolve.
        assert_eq!(subs.len(), 9);
        assert!(subs.iter().all(|s| s.granularity == Granularity::Year));
    }

    #[test]
    fn running_exhaust_subscribes_after_baserategenerator() {
        let gen = TotalActivityGenerator;
        // Running Exhaust is processID 1; the Java subscribes it at
        // GENERATOR-3 so it runs after BaseRateGenerator (GENERATOR-2).
        let running = gen
            .subscriptions()
            .iter()
            .find(|s| s.process_id == ProcessId(1))
            .expect("Running Exhaust subscription present");
        assert_eq!(running.priority.display(), "GENERATOR-3");
        // Start Exhaust (processID 2) stays at plain GENERATOR.
        let start = gen
            .subscriptions()
            .iter()
            .find(|s| s.process_id == ProcessId(2))
            .expect("Start Exhaust subscription present");
        assert_eq!(start.priority.display(), "GENERATOR");
    }

    #[test]
    fn output_tables_are_declared() {
        let gen = TotalActivityGenerator;
        assert!(gen.output_tables().contains(&"SHOByAgeRoadwayHour"));
        assert!(gen.output_tables().contains(&"StartsByAgeHour"));
        assert!(gen.input_tables().contains(&"SourceTypeYear"));
    }

    #[test]
    fn execute_is_a_shell_until_the_data_plane_lands() {
        let gen = TotalActivityGenerator;
        let ctx = CalculatorContext::new();
        assert!(gen.execute(&ctx).is_ok());
    }

    #[test]
    fn run_without_a_base_year_yields_empty_output() {
        let mut inputs = minimal_inputs();
        // No base year at or below the analysis year.
        inputs.year = vec![inputs::YearRow {
            year_id: 2030,
            is_base_year: true,
        }];
        let out = TotalActivityGenerator.run(&inputs);
        assert_eq!(out, TotalActivityOutput::default());
    }

    #[test]
    fn run_produces_the_activity_chain() {
        let out = TotalActivityGenerator.run(&minimal_inputs());

        // Population: 1000 vehicles split 60/40 across two ages.
        assert_eq!(out.source_type_age_population.len(), 2);
        let age0 = out
            .source_type_age_population
            .iter()
            .find(|r| r.age_id == 0)
            .unwrap();
        assert!((age0.population - 600.0).abs() < 1e-9);

        // VMT flows all the way to the single hour cell:
        // 8400 annual VMT, all on road 2, month/day/hour fractions all 1,
        // 7-day month -> weeksPerMonth 1 -> 8400 hourly VMT.
        assert_eq!(out.vmt_by_age_roadway_hour.len(), 2); // one row per age
        let total_vmt: f64 = out.vmt_by_age_roadway_hour.iter().map(|r| r.vmt).sum();
        assert!((total_vmt - 8400.0).abs() < 1e-6);

        // SHO = VMT / averageSpeed; averageSpeed = 60.
        let total_sho: f64 = out.sho_by_age_roadway_hour.iter().map(|r| r.sho).sum();
        assert!((total_sho - 8400.0 / 60.0).abs() < 1e-6);

        // The analysis-year age distribution was rebuilt.
        assert_eq!(out.source_type_age_distribution_additions.len(), 0);
    }
}
