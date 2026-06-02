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
    CalculatorContext, CalculatorOutput, CalculatorSubscription, DataFrameStore, Error, Generator,
};

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

    fn execute(&self, ctx: &mut CalculatorContext) -> Result<CalculatorOutput, Error> {
 // ProjectTAG is a PROJECT-DOMAIN generator. Canonical MOVES only
 // instantiates and subscribes it when the run domain is Project, where
 // `ProjectTAG.executeLoop` calls `allocateTotalActivityBasis` to write the
 // SHO/SHP/Starts/HotellingHours/SourceHours scratch tables
 // (ProjectTAG.java:142, :255). For every non-project (Default/County) run
 // it is absent from the master loop entirely, and the activity basis is
 // produced by `TotalActivityGenerator` instead — so contributing nothing
 // here is the correct, canonical-faithful result (and is exactly what the
 // asserted onroad fixtures depend on; ProjectTAG previously no-op'd for
 // them).
 //
 // The project-domain kernels (`allocate_total_activity_basis` and friends)
 // are fully ported above, but wiring them needs the project-domain input
 // tables — `linkSourceTypeHour`, `offNetworkLink`, `link`, `avft` — projected
 // from the execution context, which still lacks `TableRow`/schema-registry
 // entries for those row types. So when those tables ARE present (a genuine
 // project-domain run) we cannot reproduce the Java output: fail loudly with
 // `NotImplemented` rather than emit bogus zeros. `linkSourceTypeHour` /
 // `offNetworkLink` are project-domain-only user inputs; the execution store
 // carries them as empty schema tables for every non-project run, so a
 // *non-empty* one (row count > 0) is the signal that this is a real project
 // run with activity that must be allocated.
        let project_rows =
            |t: &str| ctx.tables().get(t).map_or(0, |df| df.height());
        let is_project_domain =
            project_rows("linkSourceTypeHour") > 0 || project_rows("offNetworkLink") > 0;
        if is_project_domain {
            return Err(Error::NotImplemented);
        }
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

    #[test]
    fn execute_errors_for_project_domain_run_until_inputs_are_wired() {
 // When the project-domain input tables ARE present (a genuine Project run),
 // the Java `ProjectTAG.executeLoop` runs `allocateTotalActivityBasis` to
 // populate the activity scratch tables. The kernels are ported above but
 // cannot yet be invoked (the project-domain input row types lack TableRow /
 // schema-registry entries), so `execute` must fail loudly rather than
 // silently produce zero activity (which would yield zero project-domain
 // emissions). When wiring lands, replace this with an assertion that the
 // SHO/SHP/Starts/HotellingHours/SourceHours scratch tables are populated.
        use moves_framework::InMemoryStore;
        let mut store = InMemoryStore::new();
 // A *non-empty* project-domain table marks a genuine project run (the
 // execution store carries these as empty schema tables for non-project
 // runs, so presence alone is not the signal — row count is).
        let project_links =
            polars::prelude::df!("linkID" => &[1i32]).expect("build 1-row linkSourceTypeHour");
        store.insert("linkSourceTypeHour", project_links);
        let gen = ProjectTAG::new();
        let mut ctx = CalculatorContext::with_tables(store);
        let err = gen.execute(&mut ctx).unwrap_err();
        assert!(matches!(err, Error::NotImplemented), "got {err:?}");
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
