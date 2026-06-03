//! ProjectTAG — Total Activity Generator for Project-domain runs.
//!
//! Ports `gov.epa.otaq.moves.master.implementation.ghg.ProjectTAG`.
//!
//! # What ProjectTAG does
//!
//! Project-domain MOVES runs supply link-level traffic data directly:
//! `link.linkVolume`, `link.linkLength`, `link.linkAvgSpeed`, and per-link
//! source-type hour fractions from `linkSourceTypeHour`. Canonical MOVES
//! county/national-domain runs derive all of this from zone-level aggregate
//! data; project-domain runs skip that derivation and go straight to the
//! link.
//!
//! `ProjectTAG` is the analogue of `TotalActivityGenerator` for that mode. It
//! computes:
//!
//! | Output table | When produced |
//! |---|---|
//! | `SHO` (Source Hours Operating) | Running Exhaust, Brakewear, Tirewear, and evap processes on non-off-network links |
//! | `SHP` (Source Hours Parked) | Evap processes on off-network (road type 1) links |
//! | `SourceHours` | Evap processes — like SHO on on-road links, like SHP on off-network |
//! | `Starts` | Start Exhaust process |
//! | `HotellingHours` | Extended Idle Exhaust and Auxiliary Power Exhaust processes |
//!
//! # Granularity and process subscriptions
//!
//! `ProjectTAG` subscribes at `YEAR` granularity with `GENERATOR` priority.
//! Unlike `TotalActivityGenerator`, which subscribes to all nine processes, the
//! Java subscription is conditional on each process being in the RunSpec. The
//! Rust port subscribes unconditionally (the subscription set is built at
//! instantiation time) and defers to `executes_for_process` at runtime.
//!
//! Java's `GENERATOR` priority means this fires *before* emission calculators
//! but *after* `LinkOperatingModeDistributionGenerator` (which uses
//! `GENERATOR+1`). The produced `SHO` rows are consumed by
//! `BaseRateCalculator` and other emission calculators.
//!
//! # Data-plane status
//!
//! The computational kernels ([`ShoRow`], [`ShpRow`], [`StartsRow`],
//! [`HotellingRow`], [`SourceHoursRow`], and [`ProjectTagInputs`]) are fully
//! implemented and unit-tested. The [`Generator::execute`] implementation will
//! shell out to them once the project-domain input tables are available in the
//! execution context (`link`, `linkSourceTypeHour`, `offNetworkLink`, `avft`,
//! and the `sourceTypeAgeDistribution` tables supplied by
//! `TotalActivityGenerator`) — which requires `TableRow` implementations and
//! schema-registry entries for those row types that do not yet exist.
//!
//! Until that wiring lands, `execute` returns [`Error::NotImplemented`] rather
//! than an empty `CalculatorOutput`. Unlike `TotalActivityGenerator::execute`
//! — which actually reads its inputs, runs the kernels, and writes its scratch
//! tables — silently returning an empty output here would diverge from the
//! Java `ProjectTAG.executeLoop`, which always runs
//! `allocateTotalActivityBasis` to populate `SHO`/`SHP`/`Starts`/
//! `hotellingHours`/`sourceHours`. Producing nothing would make a
//! project-domain run yield zero (or grossly truncated) emissions with no
//! error, so this generator fails loudly instead.

use std::collections::HashSet;

use moves_calculator_info::{Granularity, Priority};
use moves_data::ProcessId;
use moves_framework::{
    CalculatorContext, CalculatorOutput, CalculatorSubscription, DataFrameStore,
    DataFrameStoreTyped, Error, Generator, InMemoryStore, ModelScale, TableRow,
};
use polars::prelude::{DataFrame, DataType, NamedFrom, PolarsResult, Schema, Series};

// ── Process ids ──────────────────────────────────────────────────────────────

const RUNNING_EXHAUST: ProcessId = ProcessId(1);
const START_EXHAUST: ProcessId = ProcessId(2);
const BRAKEWEAR: ProcessId = ProcessId(9);
const TIREWEAR: ProcessId = ProcessId(10);
const EVAP_PERMEATION: ProcessId = ProcessId(11);
const EVAP_FUEL_VAPOR_VENTING: ProcessId = ProcessId(12);
const EVAP_FUEL_LEAKS: ProcessId = ProcessId(13);
const EXTENDED_IDLE_EXHAUST: ProcessId = ProcessId(90);
const AUXILIARY_POWER_EXHAUST: ProcessId = ProcessId(91);

/// All processes `ProjectTAG` subscribes to (matches Java's `subscribeToMe`).
const SUBSCRIBED_PROCESSES: &[ProcessId] = &[
    RUNNING_EXHAUST,
    START_EXHAUST,
    EXTENDED_IDLE_EXHAUST,
    AUXILIARY_POWER_EXHAUST,
    EVAP_PERMEATION,
    EVAP_FUEL_VAPOR_VENTING,
    EVAP_FUEL_LEAKS,
    BRAKEWEAR,
    TIREWEAR,
];

// ── Input types ───────────────────────────────────────────────────────────────

/// One `link` row — the project-domain per-link traffic data.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LinkRow {
    /// `linkID`.
    pub link_id: i32,
    /// `linkVolume` — vehicle count on the link per hour.
    pub link_volume: f64,
    /// `linkLength` — link length in miles.
    pub link_length: f64,
    /// `linkAvgSpeed` — average speed in miles per hour. `0` or negative
    /// means the link is effectively idle (no movement).
    pub link_avg_speed: f64,
    /// `roadTypeID` — 1 = off-network, 2–5 = on-road types.
    pub road_type_id: i32,
    /// `zoneID` — the zone the link belongs to.
    pub zone_id: i32,
}

impl LinkRow {
    /// Whether this is an off-network (parked / idling) link.
    #[must_use]
    pub fn is_off_network(&self) -> bool {
        self.road_type_id == 1
    }
}

/// One `linkSourceTypeHour` row — source-type fraction for a specific link.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LinkSourceTypeHourRow {
    /// `linkID`.
    pub link_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `sourceTypeHourFraction` — fraction of the link's volume that is
    /// this source type.
    pub source_type_hour_fraction: f64,
}

/// One `sourceTypeAgeDistribution` row.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceTypeAgeDistributionRow {
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `yearID`.
    pub year_id: i32,
    /// `ageID`.
    pub age_id: i32,
    /// `ageFraction`.
    pub age_fraction: f64,
}

/// One `hourDay` row.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HourDayRow {
    /// `hourDayID`.
    pub hour_day_id: i32,
    /// `dayID`.
    pub day_id: i32,
}

/// One `runSpecHourDay` row — which `hourDayID` values are in the run.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RunSpecHourDayRow {
    /// `hourDayID`.
    pub hour_day_id: i32,
}

/// One `runSpecMonth` row.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RunSpecMonthRow {
    /// `monthID`.
    pub month_id: i32,
}

/// One `dayOfAnyWeek` row.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DayOfAnyWeekRow {
    /// `dayID`.
    pub day_id: i32,
    /// `noOfRealDays` — number of real calendar days this day type covers.
    pub no_of_real_days: f64,
}

/// One `offNetworkLink` row — vehicle population data for off-network idling.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OffNetworkLinkRow {
    /// `zoneID`.
    pub zone_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `vehiclePopulation` — total vehicle count in the zone for this source type.
    pub vehicle_population: f64,
    /// `parkedVehicleFraction` — fraction of population that is parked.
    pub parked_vehicle_fraction: f64,
    /// `startFraction` — fraction of population that starts in this hour.
    pub start_fraction: f64,
    /// `extendedIdleFraction` — fraction of population that is extended-idling.
    pub extended_idle_fraction: f64,
}

/// One `avft` row — alternative vehicle fuel type distribution.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AvftRow {
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `modelYearID`.
    pub model_year_id: i32,
    /// `fuelTypeID`.
    pub fuel_type_id: i32,
    /// `fuelEngFraction` — fraction of this source type / model year that
    /// uses this fuel type.
    pub fuel_eng_fraction: f64,
}

/// All input tables `ProjectTAG` consumes for one `(link, year)` computation.
#[derive(Debug, Clone)]
pub struct ProjectTagInputs {
    /// All links in the project-domain run.
    pub links: Vec<LinkRow>,
    /// Source-type hour fractions by link.
    pub link_source_type_hours: Vec<LinkSourceTypeHourRow>,
    /// Age distribution for the target year.
    pub source_type_age_distributions: Vec<SourceTypeAgeDistributionRow>,
    /// Hour/day dimension table.
    pub hour_days: Vec<HourDayRow>,
    /// Run's selected hour/day combinations.
    pub run_spec_hour_days: Vec<RunSpecHourDayRow>,
    /// Run's selected months.
    pub run_spec_months: Vec<RunSpecMonthRow>,
    /// Day-of-week dimension with real-day counts.
    pub days_of_any_week: Vec<DayOfAnyWeekRow>,
    /// Off-network link populations (for starts, SHP, hotelling).
    pub off_network_links: Vec<OffNetworkLinkRow>,
    /// Alternative vehicle fuel type fractions (for hotelling hours).
    pub avft: Vec<AvftRow>,
    /// Calendar year being computed.
    pub year_id: i32,
    /// Whether the run is Rates-scale (affects starts computation).
    pub is_rates: bool,
}

// ── Output types ──────────────────────────────────────────────────────────────

/// One `SHO` output row.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ShoRow {
    pub hour_day_id: i32,
    pub month_id: i32,
    pub year_id: i32,
    pub age_id: i32,
    pub link_id: i32,
    pub source_type_id: i32,
    /// Source hours operating.
    pub sho: f64,
    /// Distance travelled (miles).
    pub distance: f64,
}

/// One `SHP` output row.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ShpRow {
    pub hour_day_id: i32,
    pub month_id: i32,
    pub year_id: i32,
    pub age_id: i32,
    pub zone_id: i32,
    pub source_type_id: i32,
    /// Source hours parked.
    pub shp: f64,
}

/// One `Starts` output row.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StartsRow {
    pub hour_day_id: i32,
    pub month_id: i32,
    pub year_id: i32,
    pub age_id: i32,
    pub zone_id: i32,
    pub source_type_id: i32,
    /// Number of starts.
    pub starts: f64,
}

/// One `hotellingHours` output row.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HotellingRow {
    pub hour_day_id: i32,
    pub month_id: i32,
    pub year_id: i32,
    pub age_id: i32,
    pub zone_id: i32,
    pub source_type_id: i32,
    pub fuel_type_id: i32,
    /// Hotelling hours.
    pub hotelling_hours: f64,
}

/// One `sourceHours` output row.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceHoursRow {
    pub hour_day_id: i32,
    pub month_id: i32,
    pub year_id: i32,
    pub age_id: i32,
    pub link_id: i32,
    pub source_type_id: i32,
    /// Source hours.
    pub source_hours: f64,
}

/// All activity tables `ProjectTAG` produces for a project-domain run.
#[derive(Debug, Clone, Default)]
pub struct ProjectTagOutput {
    pub sho: Vec<ShoRow>,
    pub shp: Vec<ShpRow>,
    pub starts: Vec<StartsRow>,
    pub hotelling_hours: Vec<HotellingRow>,
    pub source_hours: Vec<SourceHoursRow>,
}

// ── Computation ───────────────────────────────────────────────────────────────

/// Build `SHO` rows for one on-road link.
///
/// Ports the `INSERT IGNORE INTO SHO ...` SQL from `ProjectTAG.allocateTotalActivityBasis`.
/// The formula:
/// - If `linkAvgSpeed > 0`: `SHO = linkVolume × sourceTypeHourFraction × noOfRealDays × ageFraction × min(linkLength/linkAvgSpeed, 1.0)`
/// - Otherwise: `SHO = linkVolume × sourceTypeHourFraction × noOfRealDays × ageFraction`
///
/// Distance is `SHO × linkAvgSpeed` when speed > 0, else `0`.
#[must_use]
pub fn compute_sho(
    link: &LinkRow,
    inputs: &ProjectTagInputs,
    seen_keys: &mut HashSet<(i32, i32, i32, i32, i32)>,
) -> Vec<ShoRow> {
    let mut rows = Vec::new();
    let lsth: Vec<_> = inputs
        .link_source_type_hours
        .iter()
        .filter(|r| r.link_id == link.link_id)
        .collect();
    let valid_hd: HashSet<i32> = inputs
        .run_spec_hour_days
        .iter()
        .map(|r| r.hour_day_id)
        .collect();
    for stad in inputs
        .source_type_age_distributions
        .iter()
        .filter(|r| r.year_id == inputs.year_id)
    {
        for lsth_row in lsth
            .iter()
            .filter(|r| r.source_type_id == stad.source_type_id)
        {
            for hd in inputs
                .hour_days
                .iter()
                .filter(|r| valid_hd.contains(&r.hour_day_id))
            {
                let day_opt = inputs
                    .days_of_any_week
                    .iter()
                    .find(|d| d.day_id == hd.day_id);
                let Some(dow) = day_opt else { continue };
                for month in &inputs.run_spec_months {
                    let key = (
                        hd.hour_day_id,
                        month.month_id,
                        stad.age_id,
                        lsth_row.source_type_id,
                        link.link_id,
                    );
                    if !seen_keys.insert(key) {
                        continue;
                    }
                    let base = link.link_volume
                        * lsth_row.source_type_hour_fraction
                        * dow.no_of_real_days
                        * stad.age_fraction;
                    let (sho, distance) = if link.link_avg_speed > 0.0 {
                        let travel_time = (link.link_length / link.link_avg_speed).min(1.0);
                        let s = base * travel_time;
                        (s, s * link.link_avg_speed)
                    } else {
                        (base, 0.0)
                    };
                    rows.push(ShoRow {
                        hour_day_id: hd.hour_day_id,
                        month_id: month.month_id,
                        year_id: inputs.year_id,
                        age_id: stad.age_id,
                        link_id: link.link_id,
                        source_type_id: lsth_row.source_type_id,
                        sho,
                        distance,
                    });
                }
            }
        }
    }
    rows
}

/// Build `SHP` rows for one zone (from its off-network link).
///
/// Ports the `INSERT IGNORE INTO SHP ...` SQL from `ProjectTAG.allocateTotalActivityBasis`.
/// Formula: `SHP = vehiclePopulation × parkedVehicleFraction × ageFraction × noOfRealDays`.
#[must_use]
pub fn compute_shp(
    zone_id: i32,
    inputs: &ProjectTagInputs,
    seen_keys: &mut HashSet<(i32, i32, i32, i32, i32)>,
) -> Vec<ShpRow> {
    let mut rows = Vec::new();
    let valid_hd: HashSet<i32> = inputs
        .run_spec_hour_days
        .iter()
        .map(|r| r.hour_day_id)
        .collect();
    let onl_rows: Vec<_> = inputs
        .off_network_links
        .iter()
        .filter(|r| r.zone_id == zone_id)
        .collect();
    for onl in &onl_rows {
        for stad in inputs
            .source_type_age_distributions
            .iter()
            .filter(|r| r.year_id == inputs.year_id && r.source_type_id == onl.source_type_id)
        {
            for hd in inputs
                .hour_days
                .iter()
                .filter(|r| valid_hd.contains(&r.hour_day_id))
            {
                let day_opt = inputs
                    .days_of_any_week
                    .iter()
                    .find(|d| d.day_id == hd.day_id);
                let Some(dow) = day_opt else { continue };
                for month in &inputs.run_spec_months {
                    let key = (
                        hd.hour_day_id,
                        month.month_id,
                        stad.age_id,
                        onl.source_type_id,
                        zone_id,
                    );
                    if !seen_keys.insert(key) {
                        continue;
                    }
                    let shp = onl.vehicle_population
                        * onl.parked_vehicle_fraction
                        * stad.age_fraction
                        * dow.no_of_real_days;
                    rows.push(ShpRow {
                        hour_day_id: hd.hour_day_id,
                        month_id: month.month_id,
                        year_id: inputs.year_id,
                        age_id: stad.age_id,
                        zone_id,
                        source_type_id: onl.source_type_id,
                        shp,
                    });
                }
            }
        }
    }
    rows
}

/// Build `Starts` rows for one zone.
///
/// Ports the `INSERT IGNORE INTO starts ...` SQL from `ProjectTAG.allocateTotalActivityBasis`.
/// In inventory mode: `starts = vehiclePopulation × startFraction × ageFraction × noOfRealDays`.
/// In rates mode: `starts = vehiclePopulation × ageFraction × noOfRealDays` (startFraction not
/// multiplied, matching the Java comment about project-domain rates output).
#[must_use]
pub fn compute_starts(
    zone_id: i32,
    inputs: &ProjectTagInputs,
    seen_keys: &mut HashSet<(i32, i32, i32, i32, i32)>,
) -> Vec<StartsRow> {
    let mut rows = Vec::new();
    let valid_hd: HashSet<i32> = inputs
        .run_spec_hour_days
        .iter()
        .map(|r| r.hour_day_id)
        .collect();
    let onl_rows: Vec<_> = inputs
        .off_network_links
        .iter()
        .filter(|r| r.zone_id == zone_id)
        .collect();
    for onl in &onl_rows {
        for stad in inputs
            .source_type_age_distributions
            .iter()
            .filter(|r| r.year_id == inputs.year_id && r.source_type_id == onl.source_type_id)
        {
            for hd in inputs
                .hour_days
                .iter()
                .filter(|r| valid_hd.contains(&r.hour_day_id))
            {
                let day_opt = inputs
                    .days_of_any_week
                    .iter()
                    .find(|d| d.day_id == hd.day_id);
                let Some(dow) = day_opt else { continue };
                for month in &inputs.run_spec_months {
                    let key = (
                        hd.hour_day_id,
                        month.month_id,
                        stad.age_id,
                        onl.source_type_id,
                        zone_id,
                    );
                    if !seen_keys.insert(key) {
                        continue;
                    }
                    let base = onl.vehicle_population * stad.age_fraction * dow.no_of_real_days;
                    let starts = if inputs.is_rates {
                        base
                    } else {
                        base * onl.start_fraction
                    };
                    rows.push(StartsRow {
                        hour_day_id: hd.hour_day_id,
                        month_id: month.month_id,
                        year_id: inputs.year_id,
                        age_id: stad.age_id,
                        zone_id,
                        source_type_id: onl.source_type_id,
                        starts,
                    });
                }
            }
        }
    }
    rows
}

/// Build `hotellingHours` rows for one zone.
///
/// Ports the `INSERT IGNORE INTO hotellingHours ...` SQL.
/// Formula (per fuel type): `hotellingHours = vehiclePopulation × extendedIdleFraction × ageFraction × noOfRealDays × fuelEngFraction`.
/// Only source type 62 (combination long-haul trucks) contributes to hotelling, matching the
/// Java `and onl.sourceTypeID=62` filter.
#[must_use]
pub fn compute_hotelling(
    zone_id: i32,
    inputs: &ProjectTagInputs,
    seen_keys: &mut HashSet<(i32, i32, i32, i32, i32, i32)>,
) -> Vec<HotellingRow> {
    let mut rows = Vec::new();
    let valid_hd: HashSet<i32> = inputs
        .run_spec_hour_days
        .iter()
        .map(|r| r.hour_day_id)
        .collect();
    // Java: `and onl.sourceTypeID=62`
    let onl_rows: Vec<_> = inputs
        .off_network_links
        .iter()
        .filter(|r| r.zone_id == zone_id && r.source_type_id == 62)
        .collect();
    for onl in &onl_rows {
        for stad in inputs
            .source_type_age_distributions
            .iter()
            .filter(|r| r.year_id == inputs.year_id && r.source_type_id == onl.source_type_id)
        {
            let model_year_id = inputs.year_id - stad.age_id;
            let avft_rows: Vec<_> = inputs
                .avft
                .iter()
                .filter(|r| {
                    r.source_type_id == onl.source_type_id && r.model_year_id == model_year_id
                })
                .collect();
            for avft in &avft_rows {
                for hd in inputs
                    .hour_days
                    .iter()
                    .filter(|r| valid_hd.contains(&r.hour_day_id))
                {
                    let day_opt = inputs
                        .days_of_any_week
                        .iter()
                        .find(|d| d.day_id == hd.day_id);
                    let Some(dow) = day_opt else { continue };
                    for month in &inputs.run_spec_months {
                        let key = (
                            hd.hour_day_id,
                            month.month_id,
                            stad.age_id,
                            onl.source_type_id,
                            zone_id,
                            avft.fuel_type_id,
                        );
                        if !seen_keys.insert(key) {
                            continue;
                        }
                        let hotelling_hours = onl.vehicle_population
                            * onl.extended_idle_fraction
                            * stad.age_fraction
                            * dow.no_of_real_days
                            * avft.fuel_eng_fraction;
                        rows.push(HotellingRow {
                            hour_day_id: hd.hour_day_id,
                            month_id: month.month_id,
                            year_id: inputs.year_id,
                            age_id: stad.age_id,
                            zone_id,
                            source_type_id: onl.source_type_id,
                            fuel_type_id: avft.fuel_type_id,
                            hotelling_hours,
                        });
                    }
                }
            }
        }
    }
    rows
}

/// Build `sourceHours` rows for one link.
///
/// For off-network links (road type 1), sourceHours mirrors SHP.
/// For on-road links, sourceHours mirrors SHO (without the `min(length/speed,1)` cap).
#[must_use]
pub fn compute_source_hours(
    link: &LinkRow,
    inputs: &ProjectTagInputs,
    seen_keys: &mut HashSet<(i32, i32, i32, i32, i32)>,
) -> Vec<SourceHoursRow> {
    let mut rows = Vec::new();
    let valid_hd: HashSet<i32> = inputs
        .run_spec_hour_days
        .iter()
        .map(|r| r.hour_day_id)
        .collect();
    if link.is_off_network() {
        let onl_rows: Vec<_> = inputs
            .off_network_links
            .iter()
            .filter(|r| r.zone_id == link.zone_id)
            .collect();
        for onl in &onl_rows {
            for stad in inputs
                .source_type_age_distributions
                .iter()
                .filter(|r| r.year_id == inputs.year_id && r.source_type_id == onl.source_type_id)
            {
                for hd in inputs
                    .hour_days
                    .iter()
                    .filter(|r| valid_hd.contains(&r.hour_day_id))
                {
                    let day_opt = inputs
                        .days_of_any_week
                        .iter()
                        .find(|d| d.day_id == hd.day_id);
                    let Some(dow) = day_opt else { continue };
                    for month in &inputs.run_spec_months {
                        let key = (
                            hd.hour_day_id,
                            month.month_id,
                            stad.age_id,
                            onl.source_type_id,
                            link.link_id,
                        );
                        if !seen_keys.insert(key) {
                            continue;
                        }
                        let source_hours = onl.vehicle_population
                            * onl.parked_vehicle_fraction
                            * stad.age_fraction
                            * dow.no_of_real_days;
                        rows.push(SourceHoursRow {
                            hour_day_id: hd.hour_day_id,
                            month_id: month.month_id,
                            year_id: inputs.year_id,
                            age_id: stad.age_id,
                            link_id: link.link_id,
                            source_type_id: onl.source_type_id,
                            source_hours,
                        });
                    }
                }
            }
        }
    } else {
        let lsth: Vec<_> = inputs
            .link_source_type_hours
            .iter()
            .filter(|r| r.link_id == link.link_id)
            .collect();
        for stad in inputs
            .source_type_age_distributions
            .iter()
            .filter(|r| r.year_id == inputs.year_id)
        {
            for lsth_row in lsth
                .iter()
                .filter(|r| r.source_type_id == stad.source_type_id)
            {
                for hd in inputs
                    .hour_days
                    .iter()
                    .filter(|r| valid_hd.contains(&r.hour_day_id))
                {
                    let day_opt = inputs
                        .days_of_any_week
                        .iter()
                        .find(|d| d.day_id == hd.day_id);
                    let Some(dow) = day_opt else { continue };
                    for month in &inputs.run_spec_months {
                        let key = (
                            hd.hour_day_id,
                            month.month_id,
                            stad.age_id,
                            lsth_row.source_type_id,
                            link.link_id,
                        );
                        if !seen_keys.insert(key) {
                            continue;
                        }
                        let source_hours = link.link_volume
                            * lsth_row.source_type_hour_fraction
                            * dow.no_of_real_days
                            * stad.age_fraction;
                        rows.push(SourceHoursRow {
                            hour_day_id: hd.hour_day_id,
                            month_id: month.month_id,
                            year_id: inputs.year_id,
                            age_id: stad.age_id,
                            link_id: link.link_id,
                            source_type_id: lsth_row.source_type_id,
                            source_hours,
                        });
                    }
                }
            }
        }
    }
    rows
}

/// Run the full `ProjectTAG` activity computation for a process context.
///
/// Mirrors `ProjectTAG.allocateTotalActivityBasis`: decides which tables to
/// populate based on the current process and link road type, then delegates
/// to the per-table kernels.
///
/// The Java uses `hasGenerated` to skip work when the same link/zone appears
/// in multiple process iterations. This function uses the `seen_*` sets for
/// the same deduplication.
#[must_use]
pub fn allocate_total_activity_basis(
    process_id: ProcessId,
    inputs: &ProjectTagInputs,
    sho_seen: &mut HashSet<(i32, i32, i32, i32, i32)>,
    shp_seen: &mut HashSet<(i32, i32, i32, i32, i32)>,
    starts_seen: &mut HashSet<(i32, i32, i32, i32, i32)>,
    hotelling_seen: &mut HashSet<(i32, i32, i32, i32, i32, i32)>,
    sh_seen: &mut HashSet<(i32, i32, i32, i32, i32)>,
) -> ProjectTagOutput {
    let mut out = ProjectTagOutput::default();
    let is_evap = matches!(
        process_id,
        EVAP_PERMEATION | EVAP_FUEL_VAPOR_VENTING | EVAP_FUEL_LEAKS
    );
    let is_sho_producer = matches!(process_id, RUNNING_EXHAUST | BRAKEWEAR | TIREWEAR);
    let is_start = process_id == START_EXHAUST;
    let is_hotelling = matches!(process_id, EXTENDED_IDLE_EXHAUST | AUXILIARY_POWER_EXHAUST);

    for link in &inputs.links {
        if is_evap {
            out.source_hours
                .extend(compute_source_hours(link, inputs, sh_seen));
            if link.is_off_network() {
                out.shp.extend(compute_shp(link.zone_id, inputs, shp_seen));
            } else {
                out.sho.extend(compute_sho(link, inputs, sho_seen));
            }
        }
        if is_sho_producer {
            out.sho.extend(compute_sho(link, inputs, sho_seen));
        }
        if is_start && link.is_off_network() {
            out.starts
                .extend(compute_starts(link.zone_id, inputs, starts_seen));
        }
        if is_hotelling && link.is_off_network() {
            out.hotelling_hours
                .extend(compute_hotelling(link.zone_id, inputs, hotelling_seen));
        }
    }
    out
}

// ── TableRow implementations ──────────────────────────────────────────────────

// ── Input types ───────────────────────────────────────────────────────────────

impl TableRow for LinkRow {
    fn table_name() -> &'static str {
        "link"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("linkID".into(), DataType::Int32),
            ("linkVolume".into(), DataType::Float64),
            ("linkLength".into(), DataType::Float64),
            ("linkAvgSpeed".into(), DataType::Float64),
            ("roadTypeID".into(), DataType::Int32),
            ("zoneID".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new("linkID".into(), rows.iter().map(|r| r.link_id).collect::<Vec<i32>>()).into(),
                Series::new("linkVolume".into(), rows.iter().map(|r| r.link_volume).collect::<Vec<f64>>()).into(),
                Series::new("linkLength".into(), rows.iter().map(|r| r.link_length).collect::<Vec<f64>>()).into(),
                Series::new("linkAvgSpeed".into(), rows.iter().map(|r| r.link_avg_speed).collect::<Vec<f64>>()).into(),
                Series::new("roadTypeID".into(), rows.iter().map(|r| r.road_type_id).collect::<Vec<i32>>()).into(),
                Series::new("zoneID".into(), rows.iter().map(|r| r.zone_id).collect::<Vec<i32>>()).into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "link";
        macro_rules! col_i32 {
            ($col:expr) => {
                df.column($col)
                    .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: $col.into(), message: e.to_string() })?
                    .i32()
                    .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: $col.into(), message: e.to_string() })?
            };
        }
        macro_rules! col_f64 {
            ($col:expr) => {
                df.column($col)
                    .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: $col.into(), message: e.to_string() })?
                    .f64()
                    .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: $col.into(), message: e.to_string() })?
            };
        }
        let link_id = col_i32!("linkID");
        let link_volume = col_f64!("linkVolume");
        let link_length = col_f64!("linkLength");
        let link_avg_speed = col_f64!("linkAvgSpeed");
        let road_type_id = col_i32!("roadTypeID");
        let zone_id = col_i32!("zoneID");
        (0..df.height())
            .map(|i| {
                let null = |col: &str| Error::RowExtraction { table: T.into(), row: i, column: col.into(), message: "null value".into() };
                Ok(LinkRow {
                    link_id: link_id.get(i).ok_or_else(|| null("linkID"))?,
                    link_volume: link_volume.get(i).ok_or_else(|| null("linkVolume"))?,
                    link_length: link_length.get(i).ok_or_else(|| null("linkLength"))?,
                    link_avg_speed: link_avg_speed.get(i).ok_or_else(|| null("linkAvgSpeed"))?,
                    road_type_id: road_type_id.get(i).ok_or_else(|| null("roadTypeID"))?,
                    zone_id: zone_id.get(i).ok_or_else(|| null("zoneID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for LinkSourceTypeHourRow {
    fn table_name() -> &'static str {
        "linkSourceTypeHour"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("linkID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("sourceTypeHourFraction".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new("linkID".into(), rows.iter().map(|r| r.link_id).collect::<Vec<i32>>()).into(),
                Series::new("sourceTypeID".into(), rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>()).into(),
                Series::new("sourceTypeHourFraction".into(), rows.iter().map(|r| r.source_type_hour_fraction).collect::<Vec<f64>>()).into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "linkSourceTypeHour";
        macro_rules! col_i32 {
            ($col:expr) => {
                df.column($col)
                    .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: $col.into(), message: e.to_string() })?
                    .i32()
                    .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: $col.into(), message: e.to_string() })?
            };
        }
        macro_rules! col_f64 {
            ($col:expr) => {
                df.column($col)
                    .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: $col.into(), message: e.to_string() })?
                    .f64()
                    .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: $col.into(), message: e.to_string() })?
            };
        }
        let link_id = col_i32!("linkID");
        let source_type_id = col_i32!("sourceTypeID");
        let frac = col_f64!("sourceTypeHourFraction");
        (0..df.height())
            .map(|i| {
                let null = |col: &str| Error::RowExtraction { table: T.into(), row: i, column: col.into(), message: "null value".into() };
                Ok(LinkSourceTypeHourRow {
                    link_id: link_id.get(i).ok_or_else(|| null("linkID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    source_type_hour_fraction: frac.get(i).ok_or_else(|| null("sourceTypeHourFraction"))?,
                })
            })
            .collect()
    }
}

impl TableRow for SourceTypeAgeDistributionRow {
    fn table_name() -> &'static str {
        "sourceTypeAgeDistribution"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("yearID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
            ("ageFraction".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new("sourceTypeID".into(), rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>()).into(),
                Series::new("yearID".into(), rows.iter().map(|r| r.year_id).collect::<Vec<i32>>()).into(),
                Series::new("ageID".into(), rows.iter().map(|r| r.age_id).collect::<Vec<i32>>()).into(),
                Series::new("ageFraction".into(), rows.iter().map(|r| r.age_fraction).collect::<Vec<f64>>()).into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "sourceTypeAgeDistribution";
        macro_rules! col_i32 {
            ($col:expr) => {
                df.column($col)
                    .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: $col.into(), message: e.to_string() })?
                    .i32()
                    .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: $col.into(), message: e.to_string() })?
            };
        }
        macro_rules! col_f64 {
            ($col:expr) => {
                df.column($col)
                    .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: $col.into(), message: e.to_string() })?
                    .f64()
                    .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: $col.into(), message: e.to_string() })?
            };
        }
        let source_type_id = col_i32!("sourceTypeID");
        let year_id = col_i32!("yearID");
        let age_id = col_i32!("ageID");
        let age_fraction = col_f64!("ageFraction");
        (0..df.height())
            .map(|i| {
                let null = |col: &str| Error::RowExtraction { table: T.into(), row: i, column: col.into(), message: "null value".into() };
                Ok(SourceTypeAgeDistributionRow {
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    year_id: year_id.get(i).ok_or_else(|| null("yearID"))?,
                    age_id: age_id.get(i).ok_or_else(|| null("ageID"))?,
                    age_fraction: age_fraction.get(i).ok_or_else(|| null("ageFraction"))?,
                })
            })
            .collect()
    }
}

impl TableRow for HourDayRow {
    fn table_name() -> &'static str {
        "hourDay"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("hourDayID".into(), DataType::Int32),
            ("dayID".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new("hourDayID".into(), rows.iter().map(|r| r.hour_day_id).collect::<Vec<i32>>()).into(),
                Series::new("dayID".into(), rows.iter().map(|r| r.day_id).collect::<Vec<i32>>()).into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "hourDay";
        macro_rules! col_i32 {
            ($col:expr) => {
                df.column($col)
                    .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: $col.into(), message: e.to_string() })?
                    .i32()
                    .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: $col.into(), message: e.to_string() })?
            };
        }
        let hour_day_id = col_i32!("hourDayID");
        let day_id = col_i32!("dayID");
        (0..df.height())
            .map(|i| {
                let null = |col: &str| Error::RowExtraction { table: T.into(), row: i, column: col.into(), message: "null value".into() };
                Ok(HourDayRow {
                    hour_day_id: hour_day_id.get(i).ok_or_else(|| null("hourDayID"))?,
                    day_id: day_id.get(i).ok_or_else(|| null("dayID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for RunSpecHourDayRow {
    fn table_name() -> &'static str {
        "runSpecHourDay"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([("hourDayID".into(), DataType::Int32)])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![Series::new("hourDayID".into(), rows.iter().map(|r| r.hour_day_id).collect::<Vec<i32>>()).into()],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "runSpecHourDay";
        let col = df.column("hourDayID")
            .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: "hourDayID".into(), message: e.to_string() })?
            .i32()
            .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: "hourDayID".into(), message: e.to_string() })?;
        (0..df.height())
            .map(|i| {
                let null = |c: &str| Error::RowExtraction { table: T.into(), row: i, column: c.into(), message: "null value".into() };
                Ok(RunSpecHourDayRow { hour_day_id: col.get(i).ok_or_else(|| null("hourDayID"))? })
            })
            .collect()
    }
}

impl TableRow for RunSpecMonthRow {
    fn table_name() -> &'static str {
        "RunSpecMonth"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([("monthID".into(), DataType::Int32)])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![Series::new("monthID".into(), rows.iter().map(|r| r.month_id).collect::<Vec<i32>>()).into()],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "RunSpecMonth";
        let col = df.column("monthID")
            .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: "monthID".into(), message: e.to_string() })?
            .i32()
            .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: "monthID".into(), message: e.to_string() })?;
        (0..df.height())
            .map(|i| {
                let null = |c: &str| Error::RowExtraction { table: T.into(), row: i, column: c.into(), message: "null value".into() };
                Ok(RunSpecMonthRow { month_id: col.get(i).ok_or_else(|| null("monthID"))? })
            })
            .collect()
    }
}

impl TableRow for DayOfAnyWeekRow {
    fn table_name() -> &'static str {
        "DayOfAnyWeek"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("dayID".into(), DataType::Int32),
            ("noOfRealDays".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new("dayID".into(), rows.iter().map(|r| r.day_id).collect::<Vec<i32>>()).into(),
                Series::new("noOfRealDays".into(), rows.iter().map(|r| r.no_of_real_days).collect::<Vec<f64>>()).into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "DayOfAnyWeek";
        macro_rules! col_i32 {
            ($col:expr) => {
                df.column($col)
                    .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: $col.into(), message: e.to_string() })?
                    .i32()
                    .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: $col.into(), message: e.to_string() })?
            };
        }
        macro_rules! col_f64 {
            ($col:expr) => {
                df.column($col)
                    .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: $col.into(), message: e.to_string() })?
                    .f64()
                    .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: $col.into(), message: e.to_string() })?
            };
        }
        let day_id = col_i32!("dayID");
        let no_of_real_days = col_f64!("noOfRealDays");
        (0..df.height())
            .map(|i| {
                let null = |col: &str| Error::RowExtraction { table: T.into(), row: i, column: col.into(), message: "null value".into() };
                Ok(DayOfAnyWeekRow {
                    day_id: day_id.get(i).ok_or_else(|| null("dayID"))?,
                    no_of_real_days: no_of_real_days.get(i).ok_or_else(|| null("noOfRealDays"))?,
                })
            })
            .collect()
    }
}

impl TableRow for OffNetworkLinkRow {
    fn table_name() -> &'static str {
        "offNetworkLink"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("zoneID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("vehiclePopulation".into(), DataType::Float64),
            ("parkedVehicleFraction".into(), DataType::Float64),
            ("startFraction".into(), DataType::Float64),
            ("extendedIdleFraction".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new("zoneID".into(), rows.iter().map(|r| r.zone_id).collect::<Vec<i32>>()).into(),
                Series::new("sourceTypeID".into(), rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>()).into(),
                Series::new("vehiclePopulation".into(), rows.iter().map(|r| r.vehicle_population).collect::<Vec<f64>>()).into(),
                Series::new("parkedVehicleFraction".into(), rows.iter().map(|r| r.parked_vehicle_fraction).collect::<Vec<f64>>()).into(),
                Series::new("startFraction".into(), rows.iter().map(|r| r.start_fraction).collect::<Vec<f64>>()).into(),
                Series::new("extendedIdleFraction".into(), rows.iter().map(|r| r.extended_idle_fraction).collect::<Vec<f64>>()).into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "offNetworkLink";
        macro_rules! col_i32 {
            ($col:expr) => {
                df.column($col)
                    .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: $col.into(), message: e.to_string() })?
                    .i32()
                    .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: $col.into(), message: e.to_string() })?
            };
        }
        macro_rules! col_f64 {
            ($col:expr) => {
                df.column($col)
                    .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: $col.into(), message: e.to_string() })?
                    .f64()
                    .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: $col.into(), message: e.to_string() })?
            };
        }
        let zone_id = col_i32!("zoneID");
        let source_type_id = col_i32!("sourceTypeID");
        let vehicle_population = col_f64!("vehiclePopulation");
        let parked_vehicle_fraction = col_f64!("parkedVehicleFraction");
        let start_fraction = col_f64!("startFraction");
        let extended_idle_fraction = col_f64!("extendedIdleFraction");
        (0..df.height())
            .map(|i| {
                let null = |col: &str| Error::RowExtraction { table: T.into(), row: i, column: col.into(), message: "null value".into() };
                Ok(OffNetworkLinkRow {
                    zone_id: zone_id.get(i).ok_or_else(|| null("zoneID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    vehicle_population: vehicle_population.get(i).ok_or_else(|| null("vehiclePopulation"))?,
                    parked_vehicle_fraction: parked_vehicle_fraction.get(i).ok_or_else(|| null("parkedVehicleFraction"))?,
                    start_fraction: start_fraction.get(i).ok_or_else(|| null("startFraction"))?,
                    extended_idle_fraction: extended_idle_fraction.get(i).ok_or_else(|| null("extendedIdleFraction"))?,
                })
            })
            .collect()
    }
}

impl TableRow for AvftRow {
    fn table_name() -> &'static str {
        "avft"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("modelYearID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("fuelEngFraction".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new("sourceTypeID".into(), rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>()).into(),
                Series::new("modelYearID".into(), rows.iter().map(|r| r.model_year_id).collect::<Vec<i32>>()).into(),
                Series::new("fuelTypeID".into(), rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>()).into(),
                Series::new("fuelEngFraction".into(), rows.iter().map(|r| r.fuel_eng_fraction).collect::<Vec<f64>>()).into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "avft";
        macro_rules! col_i32 {
            ($col:expr) => {
                df.column($col)
                    .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: $col.into(), message: e.to_string() })?
                    .i32()
                    .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: $col.into(), message: e.to_string() })?
            };
        }
        macro_rules! col_f64 {
            ($col:expr) => {
                df.column($col)
                    .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: $col.into(), message: e.to_string() })?
                    .f64()
                    .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: $col.into(), message: e.to_string() })?
            };
        }
        let source_type_id = col_i32!("sourceTypeID");
        let model_year_id = col_i32!("modelYearID");
        let fuel_type_id = col_i32!("fuelTypeID");
        let fuel_eng_fraction = col_f64!("fuelEngFraction");
        (0..df.height())
            .map(|i| {
                let null = |col: &str| Error::RowExtraction { table: T.into(), row: i, column: col.into(), message: "null value".into() };
                Ok(AvftRow {
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    model_year_id: model_year_id.get(i).ok_or_else(|| null("modelYearID"))?,
                    fuel_type_id: fuel_type_id.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    fuel_eng_fraction: fuel_eng_fraction.get(i).ok_or_else(|| null("fuelEngFraction"))?,
                })
            })
            .collect()
    }
}

// ── Output types ──────────────────────────────────────────────────────────────

impl TableRow for ShoRow {
    fn table_name() -> &'static str {
        "SHO"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("hourDayID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("yearID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
            ("linkID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("SHO".into(), DataType::Float64),
            ("distance".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new("hourDayID".into(), rows.iter().map(|r| r.hour_day_id).collect::<Vec<i32>>()).into(),
                Series::new("monthID".into(), rows.iter().map(|r| r.month_id).collect::<Vec<i32>>()).into(),
                Series::new("yearID".into(), rows.iter().map(|r| r.year_id).collect::<Vec<i32>>()).into(),
                Series::new("ageID".into(), rows.iter().map(|r| r.age_id).collect::<Vec<i32>>()).into(),
                Series::new("linkID".into(), rows.iter().map(|r| r.link_id).collect::<Vec<i32>>()).into(),
                Series::new("sourceTypeID".into(), rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>()).into(),
                Series::new("SHO".into(), rows.iter().map(|r| r.sho).collect::<Vec<f64>>()).into(),
                Series::new("distance".into(), rows.iter().map(|r| r.distance).collect::<Vec<f64>>()).into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "SHO";
        macro_rules! col_i32 {
            ($col:expr) => {
                df.column($col)
                    .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: $col.into(), message: e.to_string() })?
                    .i32()
                    .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: $col.into(), message: e.to_string() })?
            };
        }
        macro_rules! col_f64 {
            ($col:expr) => {
                df.column($col)
                    .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: $col.into(), message: e.to_string() })?
                    .f64()
                    .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: $col.into(), message: e.to_string() })?
            };
        }
        let hour_day_id = col_i32!("hourDayID");
        let month_id = col_i32!("monthID");
        let year_id = col_i32!("yearID");
        let age_id = col_i32!("ageID");
        let link_id = col_i32!("linkID");
        let source_type_id = col_i32!("sourceTypeID");
        let sho = col_f64!("SHO");
        let distance = col_f64!("distance");
        (0..df.height())
            .map(|i| {
                let null = |col: &str| Error::RowExtraction { table: T.into(), row: i, column: col.into(), message: "null value".into() };
                Ok(ShoRow {
                    hour_day_id: hour_day_id.get(i).ok_or_else(|| null("hourDayID"))?,
                    month_id: month_id.get(i).ok_or_else(|| null("monthID"))?,
                    year_id: year_id.get(i).ok_or_else(|| null("yearID"))?,
                    age_id: age_id.get(i).ok_or_else(|| null("ageID"))?,
                    link_id: link_id.get(i).ok_or_else(|| null("linkID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    sho: sho.get(i).ok_or_else(|| null("SHO"))?,
                    distance: distance.get(i).ok_or_else(|| null("distance"))?,
                })
            })
            .collect()
    }
}

impl TableRow for ShpRow {
    fn table_name() -> &'static str {
        "SHP"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("hourDayID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("yearID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
            ("zoneID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("SHP".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new("hourDayID".into(), rows.iter().map(|r| r.hour_day_id).collect::<Vec<i32>>()).into(),
                Series::new("monthID".into(), rows.iter().map(|r| r.month_id).collect::<Vec<i32>>()).into(),
                Series::new("yearID".into(), rows.iter().map(|r| r.year_id).collect::<Vec<i32>>()).into(),
                Series::new("ageID".into(), rows.iter().map(|r| r.age_id).collect::<Vec<i32>>()).into(),
                Series::new("zoneID".into(), rows.iter().map(|r| r.zone_id).collect::<Vec<i32>>()).into(),
                Series::new("sourceTypeID".into(), rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>()).into(),
                Series::new("SHP".into(), rows.iter().map(|r| r.shp).collect::<Vec<f64>>()).into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "SHP";
        macro_rules! col_i32 {
            ($col:expr) => {
                df.column($col)
                    .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: $col.into(), message: e.to_string() })?
                    .i32()
                    .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: $col.into(), message: e.to_string() })?
            };
        }
        macro_rules! col_f64 {
            ($col:expr) => {
                df.column($col)
                    .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: $col.into(), message: e.to_string() })?
                    .f64()
                    .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: $col.into(), message: e.to_string() })?
            };
        }
        let hour_day_id = col_i32!("hourDayID");
        let month_id = col_i32!("monthID");
        let year_id = col_i32!("yearID");
        let age_id = col_i32!("ageID");
        let zone_id = col_i32!("zoneID");
        let source_type_id = col_i32!("sourceTypeID");
        let shp = col_f64!("SHP");
        (0..df.height())
            .map(|i| {
                let null = |col: &str| Error::RowExtraction { table: T.into(), row: i, column: col.into(), message: "null value".into() };
                Ok(ShpRow {
                    hour_day_id: hour_day_id.get(i).ok_or_else(|| null("hourDayID"))?,
                    month_id: month_id.get(i).ok_or_else(|| null("monthID"))?,
                    year_id: year_id.get(i).ok_or_else(|| null("yearID"))?,
                    age_id: age_id.get(i).ok_or_else(|| null("ageID"))?,
                    zone_id: zone_id.get(i).ok_or_else(|| null("zoneID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    shp: shp.get(i).ok_or_else(|| null("SHP"))?,
                })
            })
            .collect()
    }
}

impl TableRow for StartsRow {
    fn table_name() -> &'static str {
        "Starts"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("hourDayID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("yearID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
            ("zoneID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("starts".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new("hourDayID".into(), rows.iter().map(|r| r.hour_day_id).collect::<Vec<i32>>()).into(),
                Series::new("monthID".into(), rows.iter().map(|r| r.month_id).collect::<Vec<i32>>()).into(),
                Series::new("yearID".into(), rows.iter().map(|r| r.year_id).collect::<Vec<i32>>()).into(),
                Series::new("ageID".into(), rows.iter().map(|r| r.age_id).collect::<Vec<i32>>()).into(),
                Series::new("zoneID".into(), rows.iter().map(|r| r.zone_id).collect::<Vec<i32>>()).into(),
                Series::new("sourceTypeID".into(), rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>()).into(),
                Series::new("starts".into(), rows.iter().map(|r| r.starts).collect::<Vec<f64>>()).into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "Starts";
        macro_rules! col_i32 {
            ($col:expr) => {
                df.column($col)
                    .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: $col.into(), message: e.to_string() })?
                    .i32()
                    .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: $col.into(), message: e.to_string() })?
            };
        }
        macro_rules! col_f64 {
            ($col:expr) => {
                df.column($col)
                    .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: $col.into(), message: e.to_string() })?
                    .f64()
                    .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: $col.into(), message: e.to_string() })?
            };
        }
        let hour_day_id = col_i32!("hourDayID");
        let month_id = col_i32!("monthID");
        let year_id = col_i32!("yearID");
        let age_id = col_i32!("ageID");
        let zone_id = col_i32!("zoneID");
        let source_type_id = col_i32!("sourceTypeID");
        let starts = col_f64!("starts");
        (0..df.height())
            .map(|i| {
                let null = |col: &str| Error::RowExtraction { table: T.into(), row: i, column: col.into(), message: "null value".into() };
                Ok(StartsRow {
                    hour_day_id: hour_day_id.get(i).ok_or_else(|| null("hourDayID"))?,
                    month_id: month_id.get(i).ok_or_else(|| null("monthID"))?,
                    year_id: year_id.get(i).ok_or_else(|| null("yearID"))?,
                    age_id: age_id.get(i).ok_or_else(|| null("ageID"))?,
                    zone_id: zone_id.get(i).ok_or_else(|| null("zoneID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    starts: starts.get(i).ok_or_else(|| null("starts"))?,
                })
            })
            .collect()
    }
}

impl TableRow for HotellingRow {
    fn table_name() -> &'static str {
        "hotellingHours"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("hourDayID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("yearID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
            ("zoneID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("hotellingHours".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new("hourDayID".into(), rows.iter().map(|r| r.hour_day_id).collect::<Vec<i32>>()).into(),
                Series::new("monthID".into(), rows.iter().map(|r| r.month_id).collect::<Vec<i32>>()).into(),
                Series::new("yearID".into(), rows.iter().map(|r| r.year_id).collect::<Vec<i32>>()).into(),
                Series::new("ageID".into(), rows.iter().map(|r| r.age_id).collect::<Vec<i32>>()).into(),
                Series::new("zoneID".into(), rows.iter().map(|r| r.zone_id).collect::<Vec<i32>>()).into(),
                Series::new("sourceTypeID".into(), rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>()).into(),
                Series::new("fuelTypeID".into(), rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>()).into(),
                Series::new("hotellingHours".into(), rows.iter().map(|r| r.hotelling_hours).collect::<Vec<f64>>()).into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "hotellingHours";
        macro_rules! col_i32 {
            ($col:expr) => {
                df.column($col)
                    .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: $col.into(), message: e.to_string() })?
                    .i32()
                    .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: $col.into(), message: e.to_string() })?
            };
        }
        macro_rules! col_f64 {
            ($col:expr) => {
                df.column($col)
                    .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: $col.into(), message: e.to_string() })?
                    .f64()
                    .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: $col.into(), message: e.to_string() })?
            };
        }
        let hour_day_id = col_i32!("hourDayID");
        let month_id = col_i32!("monthID");
        let year_id = col_i32!("yearID");
        let age_id = col_i32!("ageID");
        let zone_id = col_i32!("zoneID");
        let source_type_id = col_i32!("sourceTypeID");
        let fuel_type_id = col_i32!("fuelTypeID");
        let hotelling_hours = col_f64!("hotellingHours");
        (0..df.height())
            .map(|i| {
                let null = |col: &str| Error::RowExtraction { table: T.into(), row: i, column: col.into(), message: "null value".into() };
                Ok(HotellingRow {
                    hour_day_id: hour_day_id.get(i).ok_or_else(|| null("hourDayID"))?,
                    month_id: month_id.get(i).ok_or_else(|| null("monthID"))?,
                    year_id: year_id.get(i).ok_or_else(|| null("yearID"))?,
                    age_id: age_id.get(i).ok_or_else(|| null("ageID"))?,
                    zone_id: zone_id.get(i).ok_or_else(|| null("zoneID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    fuel_type_id: fuel_type_id.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    hotelling_hours: hotelling_hours.get(i).ok_or_else(|| null("hotellingHours"))?,
                })
            })
            .collect()
    }
}

impl TableRow for SourceHoursRow {
    fn table_name() -> &'static str {
        "sourceHours"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("hourDayID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("yearID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
            ("linkID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("sourceHours".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new("hourDayID".into(), rows.iter().map(|r| r.hour_day_id).collect::<Vec<i32>>()).into(),
                Series::new("monthID".into(), rows.iter().map(|r| r.month_id).collect::<Vec<i32>>()).into(),
                Series::new("yearID".into(), rows.iter().map(|r| r.year_id).collect::<Vec<i32>>()).into(),
                Series::new("ageID".into(), rows.iter().map(|r| r.age_id).collect::<Vec<i32>>()).into(),
                Series::new("linkID".into(), rows.iter().map(|r| r.link_id).collect::<Vec<i32>>()).into(),
                Series::new("sourceTypeID".into(), rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>()).into(),
                Series::new("sourceHours".into(), rows.iter().map(|r| r.source_hours).collect::<Vec<f64>>()).into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "sourceHours";
        macro_rules! col_i32 {
            ($col:expr) => {
                df.column($col)
                    .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: $col.into(), message: e.to_string() })?
                    .i32()
                    .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: $col.into(), message: e.to_string() })?
            };
        }
        macro_rules! col_f64 {
            ($col:expr) => {
                df.column($col)
                    .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: $col.into(), message: e.to_string() })?
                    .f64()
                    .map_err(|e| Error::RowExtraction { table: T.into(), row: 0, column: $col.into(), message: e.to_string() })?
            };
        }
        let hour_day_id = col_i32!("hourDayID");
        let month_id = col_i32!("monthID");
        let year_id = col_i32!("yearID");
        let age_id = col_i32!("ageID");
        let link_id = col_i32!("linkID");
        let source_type_id = col_i32!("sourceTypeID");
        let source_hours = col_f64!("sourceHours");
        (0..df.height())
            .map(|i| {
                let null = |col: &str| Error::RowExtraction { table: T.into(), row: i, column: col.into(), message: "null value".into() };
                Ok(SourceHoursRow {
                    hour_day_id: hour_day_id.get(i).ok_or_else(|| null("hourDayID"))?,
                    month_id: month_id.get(i).ok_or_else(|| null("monthID"))?,
                    year_id: year_id.get(i).ok_or_else(|| null("yearID"))?,
                    age_id: age_id.get(i).ok_or_else(|| null("ageID"))?,
                    link_id: link_id.get(i).ok_or_else(|| null("linkID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    source_hours: source_hours.get(i).ok_or_else(|| null("sourceHours"))?,
                })
            })
            .collect()
    }
}

// ── Seen-set helpers (cross-process dedup from existing scratch) ────────────────

fn sho_keys_from_scratch(store: &InMemoryStore) -> HashSet<(i32, i32, i32, i32, i32)> {
    store
        .iter_typed_or_empty::<ShoRow>("SHO")
        .unwrap_or_default()
        .iter()
        .map(|r| (r.hour_day_id, r.month_id, r.age_id, r.source_type_id, r.link_id))
        .collect()
}

fn shp_keys_from_scratch(store: &InMemoryStore) -> HashSet<(i32, i32, i32, i32, i32)> {
    store
        .iter_typed_or_empty::<ShpRow>("SHP")
        .unwrap_or_default()
        .iter()
        .map(|r| (r.hour_day_id, r.month_id, r.age_id, r.source_type_id, r.zone_id))
        .collect()
}

fn starts_keys_from_scratch(store: &InMemoryStore) -> HashSet<(i32, i32, i32, i32, i32)> {
    store
        .iter_typed_or_empty::<StartsRow>("Starts")
        .unwrap_or_default()
        .iter()
        .map(|r| (r.hour_day_id, r.month_id, r.age_id, r.source_type_id, r.zone_id))
        .collect()
}

fn hotelling_keys_from_scratch(store: &InMemoryStore) -> HashSet<(i32, i32, i32, i32, i32, i32)> {
    store
        .iter_typed_or_empty::<HotellingRow>("hotellingHours")
        .unwrap_or_default()
        .iter()
        .map(|r| (r.hour_day_id, r.month_id, r.age_id, r.source_type_id, r.zone_id, r.fuel_type_id))
        .collect()
}

fn sh_keys_from_scratch(store: &InMemoryStore) -> HashSet<(i32, i32, i32, i32, i32)> {
    store
        .iter_typed_or_empty::<SourceHoursRow>("sourceHours")
        .unwrap_or_default()
        .iter()
        .map(|r| (r.hour_day_id, r.month_id, r.age_id, r.source_type_id, r.link_id))
        .collect()
}

// ── Generator struct ──────────────────────────────────────────────────────────

/// Project-domain total activity generator.
///
/// Subscribes to all nine onroad processes at `YEAR` granularity with
/// `GENERATOR` priority and populates the `SHO`, `SHP`, `Starts`,
/// `hotellingHours`, and `sourceHours` scratch tables from project-level
/// link data.
#[derive(Debug, Clone)]
pub struct ProjectTAG {
    subscriptions: Vec<CalculatorSubscription>,
}

impl ProjectTAG {
    /// Chain-DAG name — matches the Java class name.
    pub const NAME: &'static str = "ProjectTAG";

    /// Construct the generator with subscriptions for all nine project-domain
    /// processes at `YEAR` granularity.
    #[must_use]
    pub fn new() -> Self {
        let priority =
            Priority::parse("GENERATOR").expect("\"GENERATOR\" is a valid MasterLoop priority");
        let subscriptions = SUBSCRIBED_PROCESSES
            .iter()
            .map(|&p| CalculatorSubscription::new(p, Granularity::Year, priority))
            .collect();
        Self { subscriptions }
    }
}

impl Default for ProjectTAG {
    fn default() -> Self {
        Self::new()
    }
}

impl Generator for ProjectTAG {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn subscriptions(&self) -> &[CalculatorSubscription] {
        &self.subscriptions
    }

    fn input_tables(&self) -> &[&'static str] {
        &[
            "link",
            "linkSourceTypeHour",
            "sourceTypeAgeDistribution",
            "hourDay",
            "runSpecHourDay",
            "RunSpecMonth",
            "DayOfAnyWeek",
            "offNetworkLink",
            "avft",
        ]
    }

    fn output_tables(&self) -> &[&'static str] {
        &["SHO", "SHP", "Starts", "hotellingHours", "sourceHours"]
    }

    fn execute(&self, ctx: &mut CalculatorContext) -> Result<CalculatorOutput, Error> {
        // ProjectTAG is a PROJECT-DOMAIN generator. For Default/County runs,
        // `linkSourceTypeHour` and `offNetworkLink` are absent (row count 0),
        // so we contribute nothing — exactly what the asserted onroad fixtures depend on.
        let project_rows = |t: &str| ctx.tables().get(t).map_or(0, |df| df.height());
        let is_project_domain =
            project_rows("linkSourceTypeHour") > 0 || project_rows("offNetworkLink") > 0;
        if !is_project_domain {
            return Ok(CalculatorOutput::empty());
        }

        let year_id = ctx.position().time.year.map(|y| y as i32).unwrap_or(0);
        let is_rates = matches!(ctx.model_scale(), Some(ModelScale::Rates));
        let process_id = ctx.position().process_id.unwrap_or(ProcessId(0));

        // Read project-domain inputs from the slow tier.
        let inputs = {
            let tables = ctx.tables();
            ProjectTagInputs {
                links: tables.iter_typed("link")?,
                link_source_type_hours: tables.iter_typed("linkSourceTypeHour")?,
                source_type_age_distributions: tables
                    .iter_typed_or_empty("sourceTypeAgeDistribution")?,
                hour_days: tables.iter_typed_or_empty("hourDay")?,
                run_spec_hour_days: tables.iter_typed_or_empty("runSpecHourDay")?,
                run_spec_months: tables.iter_typed_or_empty("RunSpecMonth")?,
                days_of_any_week: tables.iter_typed_or_empty("DayOfAnyWeek")?,
                off_network_links: tables.iter_typed_or_empty("offNetworkLink")?,
                avft: tables.iter_typed_or_empty("avft")?,
                year_id,
                is_rates,
            }
        };

        // Rebuild seen sets from any rows already written to scratch by a prior
        // process invocation (mirrors Java INSERT IGNORE + hasGenerated semantics).
        let (mut sho_seen, mut shp_seen, mut starts_seen, mut hotelling_seen, mut sh_seen) = {
            let store = &ctx.scratch().store;
            (
                sho_keys_from_scratch(store),
                shp_keys_from_scratch(store),
                starts_keys_from_scratch(store),
                hotelling_keys_from_scratch(store),
                sh_keys_from_scratch(store),
            )
        };

        let new_out = allocate_total_activity_basis(
            process_id,
            &inputs,
            &mut sho_seen,
            &mut shp_seen,
            &mut starts_seen,
            &mut hotelling_seen,
            &mut sh_seen,
        );

        // Append new rows to existing scratch tables (read + extend + write).
        macro_rules! append_scratch {
            ($new_rows:expr, $name:literal, $RowType:ty) => {{
                if !$new_rows.is_empty() {
                    let scratch = ctx.scratch_mut();
                    let mut existing: Vec<$RowType> =
                        scratch.store.iter_typed_or_empty($name)?;
                    existing.extend($new_rows);
                    let df = <$RowType as TableRow>::into_dataframe(existing)
                        .map_err(|e| Error::Polars(e.to_string()))?;
                    scratch.store.insert($name, df);
                }
            }};
        }

        append_scratch!(new_out.sho, "SHO", ShoRow);
        append_scratch!(new_out.shp, "SHP", ShpRow);
        append_scratch!(new_out.starts, "Starts", StartsRow);
        append_scratch!(new_out.hotelling_hours, "hotellingHours", HotellingRow);
        append_scratch!(new_out.source_hours, "sourceHours", SourceHoursRow);

        Ok(CalculatorOutput::empty())
    }
}

/// Factory function for `CalculatorRegistry::register_generator`.
pub fn factory() -> Box<dyn Generator> {
    Box::new(ProjectTAG::new())
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use moves_framework::Generator;

    // --- Helper builders ---

    fn link(link_id: i32, volume: f64, length: f64, avg_speed: f64, road_type_id: i32) -> LinkRow {
        LinkRow {
            link_id,
            link_volume: volume,
            link_length: length,
            link_avg_speed: avg_speed,
            road_type_id,
            zone_id: 1,
        }
    }

    fn lsth(link_id: i32, source_type_id: i32, frac: f64) -> LinkSourceTypeHourRow {
        LinkSourceTypeHourRow {
            link_id,
            source_type_id,
            source_type_hour_fraction: frac,
        }
    }

    fn stad(
        source_type_id: i32,
        year_id: i32,
        age_id: i32,
        frac: f64,
    ) -> SourceTypeAgeDistributionRow {
        SourceTypeAgeDistributionRow {
            source_type_id,
            year_id,
            age_id,
            age_fraction: frac,
        }
    }

    fn hourday(hour_day_id: i32, day_id: i32) -> HourDayRow {
        HourDayRow {
            hour_day_id,
            day_id,
        }
    }

    fn rshd(hour_day_id: i32) -> RunSpecHourDayRow {
        RunSpecHourDayRow { hour_day_id }
    }

    fn rsm(month_id: i32) -> RunSpecMonthRow {
        RunSpecMonthRow { month_id }
    }

    fn dow(day_id: i32, real_days: f64) -> DayOfAnyWeekRow {
        DayOfAnyWeekRow {
            day_id,
            no_of_real_days: real_days,
        }
    }

    fn onl(
        zone_id: i32,
        source_type_id: i32,
        pop: f64,
        parked: f64,
        start: f64,
        ext_idle: f64,
    ) -> OffNetworkLinkRow {
        OffNetworkLinkRow {
            zone_id,
            source_type_id,
            vehicle_population: pop,
            parked_vehicle_fraction: parked,
            start_fraction: start,
            extended_idle_fraction: ext_idle,
        }
    }

    fn avft_row(source_type_id: i32, model_year_id: i32, fuel_type_id: i32, frac: f64) -> AvftRow {
        AvftRow {
            source_type_id,
            model_year_id,
            fuel_type_id,
            fuel_eng_fraction: frac,
        }
    }

    fn minimal_inputs() -> ProjectTagInputs {
        ProjectTagInputs {
            links: vec![link(100, 500.0, 1.5, 30.0, 2)],
            link_source_type_hours: vec![lsth(100, 21, 1.0)],
            source_type_age_distributions: vec![stad(21, 2020, 0, 1.0)],
            hour_days: vec![hourday(52, 5)],
            run_spec_hour_days: vec![rshd(52)],
            run_spec_months: vec![rsm(7)],
            days_of_any_week: vec![dow(5, 2.0)],
            off_network_links: vec![],
            avft: vec![],
            year_id: 2020,
            is_rates: false,
        }
    }

    // --- ProjectTAG struct ---

    #[test]
    fn project_tag_has_correct_name() {
        assert_eq!(ProjectTAG::new().name(), "ProjectTAG");
    }

    #[test]
    fn project_tag_subscribes_to_all_nine_processes_at_year_granularity() {
        let gen = ProjectTAG::new();
        let subs = gen.subscriptions();
        assert_eq!(subs.len(), SUBSCRIBED_PROCESSES.len());
        for sub in subs {
            assert_eq!(sub.granularity, Granularity::Year);
        }
        // All nine expected processes present.
        let pids: Vec<ProcessId> = subs.iter().map(|s| s.process_id).collect();
        for &expected in SUBSCRIBED_PROCESSES {
            assert!(
                pids.contains(&expected),
                "{expected:?} missing from subscriptions"
            );
        }
    }

    #[test]
    fn factory_produces_a_box() {
        let g = factory();
        assert_eq!(g.name(), "ProjectTAG");
    }

    #[test]
    fn execute_is_noop_for_non_project_run() {
        // For a non-project (Default/County) run the project-domain input tables
        // are absent, ProjectTAG is not part of canonical's master loop, and the
        // activity basis is produced by `TotalActivityGenerator`. ProjectTAG must
        // therefore contribute nothing (empty output) — the behaviour every
        // asserted onroad fixture depends on. Erroring here would break
        // previously-correct non-project runs.
        let gen = ProjectTAG::new();
        let mut ctx = CalculatorContext::new();
        let out = gen.execute(&mut ctx).expect("non-project run is a no-op");
        assert!(out.dataframe().is_none(), "expected empty output");
    }

    // Builds a complete project-domain InMemoryStore with one on-road link (road type 2)
    // and one off-network link (road type 1, zone 1) plus all supporting dimension tables.
    fn project_domain_store(with_off_network: bool) -> InMemoryStore {
        let mut store = InMemoryStore::new();

        // link table — one on-road link (linkID=100, road type 2) and optionally
        // one off-network link (linkID=200, road type 1) in zone 1.
        let mut links = vec![LinkRow {
            link_id: 100,
            link_volume: 500.0,
            link_length: 1.5,
            link_avg_speed: 30.0,
            road_type_id: 2,
            zone_id: 1,
        }];
        if with_off_network {
            links.push(LinkRow {
                link_id: 200,
                link_volume: 0.0,
                link_length: 0.0,
                link_avg_speed: 0.0,
                road_type_id: 1,
                zone_id: 1,
            });
        }
        store.insert("link", LinkRow::into_dataframe(links).unwrap());

        // linkSourceTypeHour — marks this as a project-domain run
        store.insert(
            "linkSourceTypeHour",
            LinkSourceTypeHourRow::into_dataframe(vec![LinkSourceTypeHourRow {
                link_id: 100,
                source_type_id: 21,
                source_type_hour_fraction: 1.0,
            }])
            .unwrap(),
        );

        // sourceTypeAgeDistribution
        store.insert(
            "sourceTypeAgeDistribution",
            SourceTypeAgeDistributionRow::into_dataframe(vec![SourceTypeAgeDistributionRow {
                source_type_id: 21,
                year_id: 2020,
                age_id: 0,
                age_fraction: 1.0,
            }])
            .unwrap(),
        );

        // hourDay
        store.insert(
            "hourDay",
            HourDayRow::into_dataframe(vec![HourDayRow {
                hour_day_id: 52,
                day_id: 5,
            }])
            .unwrap(),
        );

        // runSpecHourDay
        store.insert(
            "runSpecHourDay",
            RunSpecHourDayRow::into_dataframe(vec![RunSpecHourDayRow { hour_day_id: 52 }])
                .unwrap(),
        );

        // RunSpecMonth
        store.insert(
            "RunSpecMonth",
            RunSpecMonthRow::into_dataframe(vec![RunSpecMonthRow { month_id: 7 }]).unwrap(),
        );

        // DayOfAnyWeek
        store.insert(
            "DayOfAnyWeek",
            DayOfAnyWeekRow::into_dataframe(vec![DayOfAnyWeekRow {
                day_id: 5,
                no_of_real_days: 2.0,
            }])
            .unwrap(),
        );

        if with_off_network {
            // offNetworkLink — source type 21 for starts/SHP, source type 62 for hotelling
            store.insert(
                "offNetworkLink",
                OffNetworkLinkRow::into_dataframe(vec![
                    OffNetworkLinkRow {
                        zone_id: 1,
                        source_type_id: 21,
                        vehicle_population: 1000.0,
                        parked_vehicle_fraction: 0.8,
                        start_fraction: 0.5,
                        extended_idle_fraction: 0.0,
                    },
                    OffNetworkLinkRow {
                        zone_id: 1,
                        source_type_id: 62,
                        vehicle_population: 500.0,
                        parked_vehicle_fraction: 0.0,
                        start_fraction: 0.0,
                        extended_idle_fraction: 0.3,
                    },
                ])
                .unwrap(),
            );

            // sourceTypeAgeDistribution — add source type 62 entry for hotelling
            store.insert(
                "sourceTypeAgeDistribution",
                SourceTypeAgeDistributionRow::into_dataframe(vec![
                    SourceTypeAgeDistributionRow {
                        source_type_id: 21,
                        year_id: 2020,
                        age_id: 0,
                        age_fraction: 1.0,
                    },
                    SourceTypeAgeDistributionRow {
                        source_type_id: 62,
                        year_id: 2020,
                        age_id: 0,
                        age_fraction: 1.0,
                    },
                ])
                .unwrap(),
            );

            // avft — fuel type fractions for source type 62, model year 2020
            store.insert(
                "avft",
                AvftRow::into_dataframe(vec![AvftRow {
                    source_type_id: 62,
                    model_year_id: 2020,
                    fuel_type_id: 2,
                    fuel_eng_fraction: 0.9,
                }])
                .unwrap(),
            );
        }

        store
    }

    fn run_execute_with_process(
        store: InMemoryStore,
        process_id: ProcessId,
        year: u16,
    ) -> CalculatorContext {
        use moves_framework::{ExecutionTime, IterationPosition};
        let pos = IterationPosition {
            iteration: 0,
            process_id: Some(process_id),
            location: Default::default(),
            time: ExecutionTime::year(year),
        };
        let gen = ProjectTAG::new();
        let mut ctx = CalculatorContext::with_position_and_tables(pos, store);
        gen.execute(&mut ctx).expect("project-domain execute should succeed");
        ctx
    }

    #[test]
    fn execute_project_domain_running_exhaust_populates_sho() {
        // Running Exhaust + on-road link → SHO rows in scratch.
        // SHO = linkVolume * hourFrac * noOfRealDays * ageFrac * min(length/speed, 1)
        //     = 500 * 1.0 * 2 * 1.0 * min(1.5/30, 1.0) = 500 * 2 * 0.05 = 50
        let ctx = run_execute_with_process(project_domain_store(false), RUNNING_EXHAUST, 2020);
        let sho: Vec<ShoRow> = ctx.scratch().store.iter_typed("SHO").unwrap();
        assert_eq!(sho.len(), 1, "expected 1 SHO row");
        assert!((sho[0].sho - 50.0).abs() < 1e-9, "SHO value mismatch: {}", sho[0].sho);
        assert!((sho[0].distance - 1500.0).abs() < 1e-9);
        // sourceHours not produced by Running Exhaust
        assert!(!ctx.scratch().store.contains("sourceHours"));
    }

    #[test]
    fn execute_project_domain_evap_on_road_populates_sho_and_source_hours() {
        // Evap Permeation + on-road link → SHO + sourceHours.
        let ctx = run_execute_with_process(project_domain_store(false), EVAP_PERMEATION, 2020);
        assert!(ctx.scratch().store.contains("SHO"), "SHO missing");
        assert!(ctx.scratch().store.contains("sourceHours"), "sourceHours missing");
        assert!(!ctx.scratch().store.contains("SHP"), "SHP should not appear for on-road evap");
    }

    #[test]
    fn execute_project_domain_evap_off_network_populates_shp_and_source_hours() {
        // Evap Permeation with off-network link → SHP + sourceHours (no SHO for off-network link).
        let ctx = run_execute_with_process(project_domain_store(true), EVAP_PERMEATION, 2020);
        assert!(ctx.scratch().store.contains("SHP"), "SHP missing");
        assert!(ctx.scratch().store.contains("sourceHours"), "sourceHours missing");
    }

    #[test]
    fn execute_project_domain_start_exhaust_populates_starts() {
        // Start Exhaust + off-network link → Starts rows.
        let ctx = run_execute_with_process(project_domain_store(true), START_EXHAUST, 2020);
        let starts: Vec<StartsRow> = ctx.scratch().store.iter_typed("Starts").unwrap();
        assert!(!starts.is_empty(), "expected Starts rows");
        // starts = vehiclePopulation * startFraction * ageFraction * noOfRealDays
        //        = 1000 * 0.5 * 1.0 * 2 = 1000
        assert!((starts[0].starts - 1000.0).abs() < 1e-9);
    }

    #[test]
    fn execute_project_domain_extended_idle_populates_hotelling() {
        // Extended Idle + off-network link + source type 62 → hotellingHours rows.
        let ctx =
            run_execute_with_process(project_domain_store(true), EXTENDED_IDLE_EXHAUST, 2020);
        let hotelling: Vec<HotellingRow> =
            ctx.scratch().store.iter_typed("hotellingHours").unwrap();
        assert!(!hotelling.is_empty(), "expected hotellingHours rows");
        // hotellingHours = 500 * 0.3 * 1.0 * 2 * 0.9 = 270
        assert!((hotelling[0].hotelling_hours - 270.0).abs() < 1e-9);
    }

    #[test]
    fn execute_project_domain_cross_process_dedup_prevents_duplicate_sho() {
        // Running Exhaust followed by Evap Permeation: both would produce SHO for the same
        // on-road link. The dedup logic (seen-set rebuilt from scratch) must prevent duplicates.
        let store = project_domain_store(false);
        use moves_framework::{ExecutionTime, IterationPosition};

        let gen = ProjectTAG::new();

        // First call: Running Exhaust.
        let pos1 = IterationPosition {
            iteration: 0,
            process_id: Some(RUNNING_EXHAUST),
            location: Default::default(),
            time: ExecutionTime::year(2020),
        };
        let mut ctx = CalculatorContext::with_position_and_tables(pos1, store);
        gen.execute(&mut ctx).unwrap();
        let sho_after_first: Vec<ShoRow> = ctx.scratch().store.iter_typed("SHO").unwrap();
        assert_eq!(sho_after_first.len(), 1, "first call should produce 1 SHO row");

        // Second call: Evap Permeation (also produces SHO for on-road links).
        let pos2 = IterationPosition {
            iteration: 1,
            process_id: Some(EVAP_PERMEATION),
            location: Default::default(),
            time: ExecutionTime::year(2020),
        };
        ctx.set_position(pos2);
        gen.execute(&mut ctx).unwrap();
        let sho_after_second: Vec<ShoRow> = ctx.scratch().store.iter_typed("SHO").unwrap();
        // The seen set rebuilt from scratch prevents the Evap Permeation call from
        // adding duplicate SHO rows for the same (hourDayID, monthID, ageID, sourceTypeID, linkID).
        assert_eq!(
            sho_after_second.len(),
            1,
            "cross-process dedup should prevent duplicate SHO rows (got {})",
            sho_after_second.len()
        );
    }

    #[test]
    fn execute_non_project_run_remains_noop() {
        // Regression: after wiring, non-project runs (no linkSourceTypeHour / offNetworkLink)
        // must still produce empty output and write nothing to scratch.
        let gen = ProjectTAG::new();
        let mut ctx = CalculatorContext::new();
        let out = gen.execute(&mut ctx).expect("non-project run must succeed");
        assert!(out.dataframe().is_none());
        assert!(!ctx.scratch().store.contains("SHO"));
    }

    #[test]
    fn link_is_off_network_when_road_type_is_1() {
        assert!(link(1, 0.0, 0.0, 0.0, 1).is_off_network());
        assert!(!link(1, 0.0, 0.0, 0.0, 2).is_off_network());
    }

    // --- compute_sho ---

    #[test]
    fn sho_with_positive_speed_uses_travel_time() {
        // linkVolume=500, hourFrac=1.0, noOfRealDays=2, ageFrac=1.0, length=1.5, speed=30
        // base = 500 * 1.0 * 2 * 1.0 = 1000
        // travel_time = min(1.5/30, 1.0) = 0.05
        // SHO = 1000 * 0.05 = 50
        // distance = 50 * 30 = 1500
        let inputs = minimal_inputs();
        let mut seen = HashSet::new();
        let rows = compute_sho(&inputs.links[0], &inputs, &mut seen);
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert!((row.sho - 50.0).abs() < 1e-9, "SHO = {}", row.sho);
        assert!(
            (row.distance - 1500.0).abs() < 1e-9,
            "distance = {}",
            row.distance
        );
    }

    #[test]
    fn sho_clamps_travel_time_at_one_for_short_slow_links() {
        // length=100, speed=30: length/speed = 3.33 > 1.0 → capped at 1.0
        let mut inputs = minimal_inputs();
        inputs.links[0].link_length = 100.0;
        let mut seen = HashSet::new();
        let rows = compute_sho(&inputs.links[0], &inputs, &mut seen);
        assert_eq!(rows.len(), 1);
        // base = 500 * 1.0 * 2 * 1.0 = 1000; travel_time = 1.0 (capped)
        assert!((rows[0].sho - 1000.0).abs() < 1e-9);
    }

    #[test]
    fn sho_with_zero_speed_uses_volume_directly_and_zero_distance() {
        let mut inputs = minimal_inputs();
        inputs.links[0].link_avg_speed = 0.0;
        let mut seen = HashSet::new();
        let rows = compute_sho(&inputs.links[0], &inputs, &mut seen);
        assert_eq!(rows.len(), 1);
        // base = 500 * 1.0 * 2 * 1.0 = 1000
        assert!((rows[0].sho - 1000.0).abs() < 1e-9);
        assert!((rows[0].distance - 0.0).abs() < 1e-9);
    }

    #[test]
    fn sho_dedup_by_seen_set() {
        let inputs = minimal_inputs();
        let mut seen = HashSet::new();
        let first = compute_sho(&inputs.links[0], &inputs, &mut seen);
        let second = compute_sho(&inputs.links[0], &inputs, &mut seen);
        // Second call skips all rows (already in seen).
        assert_eq!(first.len(), 1);
        assert_eq!(second.len(), 0);
    }

    #[test]
    fn sho_skips_links_not_in_lsth() {
        let mut inputs = minimal_inputs();
        // lsth points at link 99, but our link is 100.
        inputs.link_source_type_hours = vec![lsth(99, 21, 1.0)];
        let mut seen = HashSet::new();
        let rows = compute_sho(&inputs.links[0], &inputs, &mut seen);
        assert!(rows.is_empty());
    }

    // --- compute_shp ---

    #[test]
    fn shp_formula_correct() {
        // vehiclePopulation=1000, parkedVehicleFraction=0.8, ageFraction=1.0, noOfRealDays=2
        // SHP = 1000 * 0.8 * 1.0 * 2 = 1600
        let mut inputs = minimal_inputs();
        inputs.off_network_links = vec![onl(1, 21, 1000.0, 0.8, 0.1, 0.05)];
        let mut seen = HashSet::new();
        let rows = compute_shp(1, &inputs, &mut seen);
        assert_eq!(rows.len(), 1);
        assert!((rows[0].shp - 1600.0).abs() < 1e-9, "SHP = {}", rows[0].shp);
    }

    #[test]
    fn shp_empty_when_no_offnetwork_for_zone() {
        let mut inputs = minimal_inputs();
        inputs.off_network_links = vec![onl(99, 21, 1000.0, 0.8, 0.1, 0.05)]; // zone 99, not 1
        let mut seen = HashSet::new();
        let rows = compute_shp(1, &inputs, &mut seen);
        assert!(rows.is_empty());
    }

    // --- compute_starts ---

    #[test]
    fn starts_inventory_mode_multiplies_start_fraction() {
        // vehiclePopulation=1000, startFraction=0.5, ageFraction=1.0, noOfRealDays=2
        // starts = 1000 * 0.5 * 1.0 * 2 = 1000
        let mut inputs = minimal_inputs();
        inputs.off_network_links = vec![onl(1, 21, 1000.0, 0.8, 0.5, 0.05)];
        inputs.is_rates = false;
        let mut seen = HashSet::new();
        let rows = compute_starts(1, &inputs, &mut seen);
        assert_eq!(rows.len(), 1);
        assert!((rows[0].starts - 1000.0).abs() < 1e-9);
    }

    #[test]
    fn starts_rates_mode_omits_start_fraction() {
        // In rates mode, startFraction is NOT multiplied.
        // starts = 1000 * 1.0 * 2 = 2000
        let mut inputs = minimal_inputs();
        inputs.off_network_links = vec![onl(1, 21, 1000.0, 0.8, 0.5, 0.05)];
        inputs.is_rates = true;
        let mut seen = HashSet::new();
        let rows = compute_starts(1, &inputs, &mut seen);
        assert_eq!(rows.len(), 1);
        assert!((rows[0].starts - 2000.0).abs() < 1e-9);
    }

    // --- compute_hotelling ---

    #[test]
    fn hotelling_only_for_source_type_62() {
        // Source type 62 (long-haul combo trucks) contributes; others don't.
        let mut inputs = minimal_inputs();
        // Both source type 21 and 62 present; only 62 should appear.
        inputs.off_network_links = vec![
            onl(1, 21, 1000.0, 0.8, 0.1, 0.05),
            onl(1, 62, 500.0, 0.8, 0.1, 0.3),
        ];
        inputs.source_type_age_distributions = vec![stad(21, 2020, 0, 1.0), stad(62, 2020, 0, 1.0)];
        inputs.avft = vec![avft_row(62, 2020, 2, 0.9)];
        let mut seen = HashSet::new();
        let rows = compute_hotelling(1, &inputs, &mut seen);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].source_type_id, 62);
        // hotellingHours = 500 * 0.3 * 1.0 * 2 * 0.9 = 270
        assert!((rows[0].hotelling_hours - 270.0).abs() < 1e-9);
    }

    #[test]
    fn hotelling_joins_avft_by_model_year() {
        // model_year_id = year_id - age_id = 2020 - 0 = 2020
        let mut inputs = minimal_inputs();
        inputs.off_network_links = vec![onl(1, 62, 100.0, 0.0, 0.0, 1.0)];
        inputs.source_type_age_distributions = vec![stad(62, 2020, 0, 1.0)];
        // avft for wrong model year — no rows produced.
        inputs.avft = vec![avft_row(62, 1999, 2, 1.0)];
        let mut seen = HashSet::new();
        let rows = compute_hotelling(1, &inputs, &mut seen);
        assert!(rows.is_empty(), "wrong model year should produce no rows");
    }

    // --- allocate_total_activity_basis ---

    #[test]
    fn running_exhaust_produces_sho_only() {
        let inputs = minimal_inputs();
        let mut sho_seen = HashSet::new();
        let mut shp_seen = HashSet::new();
        let mut starts_seen = HashSet::new();
        let mut hotelling_seen = HashSet::new();
        let mut sh_seen = HashSet::new();
        let out = allocate_total_activity_basis(
            RUNNING_EXHAUST,
            &inputs,
            &mut sho_seen,
            &mut shp_seen,
            &mut starts_seen,
            &mut hotelling_seen,
            &mut sh_seen,
        );
        assert!(!out.sho.is_empty());
        assert!(out.shp.is_empty());
        assert!(out.starts.is_empty());
        assert!(out.hotelling_hours.is_empty());
        assert!(out.source_hours.is_empty());
    }

    #[test]
    fn evap_permeation_produces_sho_and_source_hours() {
        let inputs = minimal_inputs();
        let mut sho_seen = HashSet::new();
        let mut shp_seen = HashSet::new();
        let mut starts_seen = HashSet::new();
        let mut hotelling_seen = HashSet::new();
        let mut sh_seen = HashSet::new();
        let out = allocate_total_activity_basis(
            EVAP_PERMEATION,
            &inputs,
            &mut sho_seen,
            &mut shp_seen,
            &mut starts_seen,
            &mut hotelling_seen,
            &mut sh_seen,
        );
        // On-road link: SHO + SourceHours; no SHP (that's for off-network).
        assert!(!out.sho.is_empty());
        assert!(!out.source_hours.is_empty());
        assert!(out.shp.is_empty());
    }

    #[test]
    fn start_exhaust_on_offnetwork_link_produces_starts() {
        let mut inputs = minimal_inputs();
        inputs.links = vec![link(200, 0.0, 0.0, 0.0, 1)]; // off-network
        inputs.links[0].zone_id = 1;
        inputs.off_network_links = vec![onl(1, 21, 1000.0, 0.8, 0.5, 0.05)];
        let mut sho_seen = HashSet::new();
        let mut shp_seen = HashSet::new();
        let mut starts_seen = HashSet::new();
        let mut hotelling_seen = HashSet::new();
        let mut sh_seen = HashSet::new();
        let out = allocate_total_activity_basis(
            START_EXHAUST,
            &inputs,
            &mut sho_seen,
            &mut shp_seen,
            &mut starts_seen,
            &mut hotelling_seen,
            &mut sh_seen,
        );
        assert!(!out.starts.is_empty());
        assert!(out.sho.is_empty());
    }

    #[test]
    fn extended_idle_on_offnetwork_link_produces_hotelling() {
        let mut inputs = minimal_inputs();
        inputs.links = vec![link(200, 0.0, 0.0, 0.0, 1)];
        inputs.links[0].zone_id = 1;
        inputs.off_network_links = vec![onl(1, 62, 500.0, 0.8, 0.1, 0.3)];
        inputs.source_type_age_distributions = vec![stad(62, 2020, 0, 1.0)];
        inputs.avft = vec![avft_row(62, 2020, 2, 1.0)];
        let mut sho_seen = HashSet::new();
        let mut shp_seen = HashSet::new();
        let mut starts_seen = HashSet::new();
        let mut hotelling_seen = HashSet::new();
        let mut sh_seen = HashSet::new();
        let out = allocate_total_activity_basis(
            EXTENDED_IDLE_EXHAUST,
            &inputs,
            &mut sho_seen,
            &mut shp_seen,
            &mut starts_seen,
            &mut hotelling_seen,
            &mut sh_seen,
        );
        assert!(!out.hotelling_hours.is_empty());
        assert!(out.sho.is_empty());
    }
}
