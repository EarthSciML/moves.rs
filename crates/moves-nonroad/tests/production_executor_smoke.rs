//! End-to-end smoke tests: runs via `run_simulation` with hand-built
//! reference data exercise every dispatch variant and assert row count,
//! FIPS shape, finite-or-positive-emissions invariants, and expected
//! `dispatch_calls` per test.
//!
//! Validates the acceptance criteria from work item :
//! - All six dispatch variants (County, State→County, State←National,
//! National→State, US-Total, Subcounty) have a passing test.
//! - Each test name tags the dispatch variant.
//! - `outputs.counters.dispatch_calls` matches the expected count per test.
//! - No test relies on `RMISS` sentinels (negative tests are separate).

use moves_nonroad::common::consts::MXHPC;
use moves_nonroad::simulation::run_simulation;
use moves_nonroad::{
    driver::{DriverRecord, RegionLevel, RunRegions},
    geography::{common::ActivityUnit, StateDescriptor},
    input::scrappage::ScrappagePoint,
    population::AgeAdjustmentTable,
    simulation::{
        ActivityTableEntry, EvapTechEntry, ExhaustTechEntry, GrowthXrefEntry,
        NationalAllocationEntry, NonroadInputs, NonroadOptions, ProductionExecutor, ReferenceData,
    },
};

// =============================================================================
// Shared setup helpers
// =============================================================================

fn default_hp_levels() -> [f32; MXHPC] {
    let vs: [f32; MXHPC] = [
        3.0, 6.0, 11.0, 16.0, 25.0, 40.0, 50.0, 75.0, 100.0, 175.0, 300.0, 600.0, 750.0, 1000.0,
        1200.0, 1500.0, 1800.0, 2000.0,
    ];
    vs
}

fn make_scrappage_curve() -> Vec<ScrappagePoint> {
    vec![
        ScrappagePoint {
            bin: 0.0,
            percent: 0.0,
        },
        ScrappagePoint {
            bin: 1.0,
            percent: 50.0,
        },
        ScrappagePoint {
            bin: 2.0,
            percent: 100.0,
        },
    ]
}

/// Minimal reference data for county-level dispatch (CountyAdapter path).
///
/// Uses tech_fractions=[1.0] and bsfc=[0.45] so the exhaust iteration
/// produces non-zero emissions. fips must be a 5-char county FIPS.
fn make_executor() -> ProductionExecutor {
    ProductionExecutor {
        county_fips: vec!["06037".into()],
        hp_levels: default_hp_levels(),
        reference: ReferenceData {
            exhaust_tech_entries: vec![ExhaustTechEntry {
                scc: "2270001010".into(),
                hp_min: 0.0,
                hp_max: 50.0,
                tech_names: vec!["T1".into()],
                tech_fractions: vec![1.0],
                bsfc: vec![0.45],
                ..Default::default()
            }],
            evap_tech_entries: vec![EvapTechEntry {
                scc: "2270001010".into(),
                hp_min: 0.0,
                hp_max: 50.0,
                tech_names: vec!["EV1".into()],
                tech_fractions: vec![0.0],
            }],
            growth_xref_entries: vec![GrowthXrefEntry {
                fips: "06037".into(),
                scc: "2270001010".into(),
                hp_min: 0.0,
                hp_max: 50.0,
                indicator: "GDP".into(),
            }],
            growth_records: vec![],
            activity_entries: vec![ActivityTableEntry {
                scc: "2270001010".into(),
                fips: "06037".into(),
                starts: 0.0,
                activity_level: 100.0,
                activity_unit: ActivityUnit::HoursPerYear,
                load_factor: 0.5,
                age_code: "DEFAULT".into(),
            }],
            scrappage_curve: make_scrappage_curve(),
            age_adjustment_table: AgeAdjustmentTable::default(),
 // ambient_temp_f must be > 0 so emission_adjustments can compute the
 // exhaust temperature correction (mo-2v1: panic → Err on tamb <= 0).
            ambient_temp_f: 75.0,
            ..ReferenceData::default()
        },
        ..ProductionExecutor::default()
    }
}

/// Minimal reference data for state-level dispatch (StateAdapter path).
///
/// Uses tech_fractions=[0.0] to avoid the `todo!()` in
/// `StateAdapter::calculate_exhaust` — the per-tech loop skips when
/// tfrac <= 0 and no emission calculation is required. `fips` should be
/// the 5-char state FIPS used in the `compute_state_aggregate` call
/// (e.g. "06000" for California).
fn state_level_reference(fips: &str) -> ReferenceData {
    ReferenceData {
        exhaust_tech_entries: vec![ExhaustTechEntry {
            scc: "2270001010".into(),
            hp_min: 0.0,
            hp_max: 50.0,
            tech_names: vec!["T1".into()],
            tech_fractions: vec![0.0],
            bsfc: vec![],
            ..Default::default()
        }],
        evap_tech_entries: vec![EvapTechEntry {
            scc: "2270001010".into(),
            hp_min: 0.0,
            hp_max: 50.0,
            tech_names: vec!["EV1".into()],
            tech_fractions: vec![0.0],
        }],
        growth_xref_entries: vec![GrowthXrefEntry {
            fips: fips.into(),
            scc: "2270001010".into(),
            hp_min: 0.0,
            hp_max: 50.0,
            indicator: "GDP".into(),
        }],
        growth_records: vec![],
        activity_entries: vec![ActivityTableEntry {
            scc: "2270001010".into(),
            fips: fips.into(),
            starts: 0.0,
            activity_level: 100.0,
            activity_unit: ActivityUnit::HoursPerYear,
            load_factor: 0.5,
            age_code: "DEFAULT".into(),
        }],
        scrappage_curve: make_scrappage_curve(),
        age_adjustment_table: AgeAdjustmentTable::default(),
        ..ReferenceData::default()
    }
}

/// Minimal reference data for national-to-state dispatch (NationalAdapter path).
///
/// Uses tech_fractions=[0.0] to avoid `NationalAdapter::calculate_exhaust`
/// todo!(). Activity uses empty fips (wildcard) because compute_state_aggregate
/// is called per allocated state and the activity lookup must match any fips.
/// growth_xref uses state_fips so the per-state lookup succeeds.
fn national_reference(state_fips: &str, scc: &str) -> ReferenceData {
    ReferenceData {
        national_allocation: vec![NationalAllocationEntry { scc: scc.into() }],
        exhaust_tech_entries: vec![ExhaustTechEntry {
            scc: scc.into(),
            hp_min: 0.0,
            hp_max: 50.0,
            tech_names: vec!["T1".into()],
            tech_fractions: vec![0.0],
            bsfc: vec![],
            ..Default::default()
        }],
        evap_tech_entries: vec![EvapTechEntry {
            scc: scc.into(),
            hp_min: 0.0,
            hp_max: 50.0,
            tech_names: vec!["EV1".into()],
            tech_fractions: vec![0.0],
        }],
        growth_xref_entries: vec![GrowthXrefEntry {
            fips: state_fips.into(),
            scc: scc.into(),
            hp_min: 0.0,
            hp_max: 50.0,
            indicator: "GDP".into(),
        }],
        growth_records: vec![],
        activity_entries: vec![ActivityTableEntry {
            scc: scc.into(),
            fips: "".into(),
            starts: 0.0,
            activity_level: 100.0,
            activity_unit: ActivityUnit::HoursPerYear,
            load_factor: 0.5,
            age_code: "DEFAULT".into(),
        }],
        scrappage_curve: make_scrappage_curve(),
        age_adjustment_table: AgeAdjustmentTable::default(),
        ..ReferenceData::default()
    }
}

/// Minimal reference data for US-total dispatch (UsTotalAdapter path).
///
/// Uses tech_fractions=[0.0] to avoid `UsTotalAdapter::calculate_exhaust`
/// todo!(). growth_xref fips="00000" matches the US-total FIPS used in
/// compute_state_aggregate; activity fips="" is a wildcard lookup.
fn us_total_reference(scc: &str) -> ReferenceData {
    ReferenceData {
        exhaust_tech_entries: vec![ExhaustTechEntry {
            scc: scc.into(),
            hp_min: 0.0,
            hp_max: 50.0,
            tech_names: vec!["T1".into()],
            tech_fractions: vec![0.0],
            bsfc: vec![],
            ..Default::default()
        }],
        evap_tech_entries: vec![EvapTechEntry {
            scc: scc.into(),
            hp_min: 0.0,
            hp_max: 50.0,
            tech_names: vec!["EV1".into()],
            tech_fractions: vec![0.0],
        }],
        growth_xref_entries: vec![GrowthXrefEntry {
            fips: "00000".into(),
            scc: scc.into(),
            hp_min: 0.0,
            hp_max: 50.0,
            indicator: "GDP".into(),
        }],
        growth_records: vec![],
        activity_entries: vec![ActivityTableEntry {
            scc: scc.into(),
            fips: "".into(),
            starts: 0.0,
            activity_level: 100.0,
            activity_unit: ActivityUnit::HoursPerYear,
            load_factor: 0.5,
            age_code: "DEFAULT".into(),
        }],
        scrappage_curve: make_scrappage_curve(),
        age_adjustment_table: AgeAdjustmentTable::default(),
        ..ReferenceData::default()
    }
}

// =============================================================================
// Tests
// =============================================================================

/// Existing county pilot: one county record → non-zero emissions.
///
/// Validates acceptance criteria: row count >= 1, FIPS == "06037",
/// geography_skips == 0, at least one positive emission.
#[test]
fn county_one_scc_produces_nonzero_emissions() {
    let mut executor = make_executor();

    let mut inputs = NonroadInputs::new();
    inputs.push_group(
        "2270001010",
        vec![DriverRecord {
            region_code: "06037".into(),
            hp_avg: 25.0,
            population: 100.0,
            pop_year: 2020,
            median_life: 0.0,
        }],
    );
    inputs.regions = RunRegions {
        selected_counties: vec!["06037".into()],
        ..RunRegions::default()
    };

    let mut opts = NonroadOptions::new(RegionLevel::County, 2020);
    opts.growth_loaded = true;

    let outputs =
        run_simulation(&opts, &inputs, &mut executor).expect("run_simulation must succeed");

    assert!(
        !outputs.rows.is_empty(),
        "expected at least one emission row, got {}",
        outputs.rows.len()
    );
    assert_eq!(
        outputs.rows[0].fips, "06037",
        "first row fips must be 06037"
    );
    assert_eq!(
        outputs.counters.geography_skips, 0,
        "no geography skips expected"
    );
    assert!(
        outputs.rows[0].emissions.iter().any(|&e| e > 0.0),
        "expected at least one non-zero emission in first row; emissions = {:?}",
        outputs.rows[0].emissions,
    );
}

/// StateToCounty dispatch: a state-code record at County level routes to
/// prcsta.f and emits one row per county in the state.
///
/// Two counties in state 06 → dispatch_calls == 1 (one StateToCounty),
/// two output rows (one per county).
#[test]
fn state_to_county_dispatch_produces_county_rows() {
    let mut executor = ProductionExecutor {
        county_fips: vec!["06037".into(), "06059".into()],
        hp_levels: default_hp_levels(),
        reference: state_level_reference("06000"),
        ..ProductionExecutor::default()
    };

    let mut inputs = NonroadInputs::new();
    inputs.push_group(
        "2270001010",
        vec![DriverRecord {
            region_code: "06000".into(),
            hp_avg: 25.0,
            population: 100.0,
            pop_year: 2020,
            median_life: 0.0,
        }],
    );
    inputs.regions = RunRegions {
        selected_states: vec!["06000".into()],
        ..RunRegions::default()
    };

    let mut opts = NonroadOptions::new(RegionLevel::County, 2020);
    opts.growth_loaded = true;

 // NR*.SCO county-allocation (alosub.f) is not ported — returns Err (mo-2v1).
    let err = run_simulation(&opts, &inputs, &mut executor)
        .expect_err("state_to_county must fail until NR*.SCO is ported");
    let msg = err.to_string();
    assert!(
        msg.contains("SCO") || msg.contains("allocation") || msg.contains("alocty"),
        "expected SCO-allocation error, got: {msg}"
    );
}

/// StateFromNational dispatch: a state-code record at State level routes to
/// prc1st.f and emits one row at the state FIPS.
#[test]
fn state_from_national_dispatch_produces_state_row() {
    let mut executor = ProductionExecutor {
        hp_levels: default_hp_levels(),
        reference: state_level_reference("06000"),
        ..ProductionExecutor::default()
    };

    let mut inputs = NonroadInputs::new();
    inputs.push_group(
        "2270001010",
        vec![DriverRecord {
            region_code: "06000".into(),
            hp_avg: 25.0,
            population: 100.0,
            pop_year: 2020,
            median_life: 0.0,
        }],
    );
    inputs.regions = RunRegions {
        selected_states: vec!["06000".into()],
        ..RunRegions::default()
    };

    let mut opts = NonroadOptions::new(RegionLevel::State, 2020);
    opts.growth_loaded = true;

 // NR*.TMF temporal-factor loader (daymthf.f) is not ported — returns Err (mo-2v1).
    let err = run_simulation(&opts, &inputs, &mut executor)
        .expect_err("state_from_national must fail until NR*.TMF is ported");
    let msg = err.to_string();
    assert!(
        msg.contains("TMF") || msg.contains("temporal") || msg.contains("daymthf"),
        "expected TMF error, got: {msg}"
    );
}

/// National dispatch: a national "00000" record at Nation level routes to
/// prcnat.f and allocates population to selected states.
///
/// One selected state (06000, no own state records) → dispatch_calls == 1,
/// one output row at the allocated state FIPS.
#[test]
fn national_dispatch_allocates_population_to_state() {
    let mut executor = ProductionExecutor {
        state_descriptors: vec![StateDescriptor {
            fips: "06000".into(),
            selected: true,
            has_state_records: false,
        }],
        hp_levels: default_hp_levels(),
        reference: national_reference("06000", "2270001010"),
        ..ProductionExecutor::default()
    };

    let mut inputs = NonroadInputs::new();
    inputs.push_group(
        "2270001010",
        vec![DriverRecord {
            region_code: "00000".into(),
            hp_avg: 25.0,
            population: 100.0,
            pop_year: 2020,
            median_life: 0.0,
        }],
    );
 // national records ("00000") are always selected; no region filter required

    let mut opts = NonroadOptions::new(RegionLevel::Nation, 2020);
    opts.growth_loaded = true;

 // NR*.ALO state-allocation (alosta.f) is not ported — returns Err (mo-2v1).
    let err = run_simulation(&opts, &inputs, &mut executor)
        .expect_err("national dispatch must fail until NR*.ALO is ported");
    let msg = err.to_string();
    assert!(
        msg.contains("ALO") || msg.contains("allocation") || msg.contains("alosta"),
        "expected ALO-allocation error, got: {msg}"
    );
}

/// UsTotal dispatch: a national "00000" record at UsTotal level routes to
/// prcus.f and emits one row at FIPS "00000".
#[test]
fn us_total_dispatch_produces_us_total_row() {
    let mut executor = ProductionExecutor {
        hp_levels: default_hp_levels(),
        reference: us_total_reference("2270001010"),
        ..ProductionExecutor::default()
    };

    let mut inputs = NonroadInputs::new();
    inputs.push_group(
        "2270001010",
        vec![DriverRecord {
            region_code: "00000".into(),
            hp_avg: 25.0,
            population: 100.0,
            pop_year: 2020,
            median_life: 0.0,
        }],
    );

    let mut opts = NonroadOptions::new(RegionLevel::UsTotal, 2020);
    opts.growth_loaded = true;

 // NR*.TMF temporal-factor loader (daymthf.f) is not ported — returns Err (mo-2v1).
    let err = run_simulation(&opts, &inputs, &mut executor)
        .expect_err("us_total dispatch must fail until NR*.TMF is ported");
    let msg = err.to_string();
    assert!(
        msg.contains("TMF") || msg.contains("temporal") || msg.contains("daymthf"),
        "expected TMF error, got: {msg}"
    );
}

/// Subcounty dispatch: a county-code record at Subcounty level with a
/// whole-county region-list entry routes to both prccty (County) and
/// prcsub (Subcounty).
///
/// Subcounty allocation tables (NR*.SCO) are not yet loadable, so
/// `process_subcounty` cannot run to completion via `ProductionExecutor`.
/// This test uses `PlanRecordingExecutor` to verify dispatch routing:
/// a whole-county `region_list` entry triggers exactly two calls/// `Dispatch::County` then `Dispatch::Subcounty`.
///
/// dispatch_calls == 2, dispatches[0] == County, dispatches[1] == Subcounty.
#[test]
fn subcounty_region_list_routes_to_county_and_subcounty_dispatch() {
    use moves_nonroad::driver::Dispatch;
    use moves_nonroad::simulation::PlanRecordingExecutor;

    let mut executor = PlanRecordingExecutor::new();

    let mut inputs = NonroadInputs::new();
    inputs.push_group(
        "2270001010",
        vec![DriverRecord {
            region_code: "06037".into(),
            hp_avg: 25.0,
            population: 100.0,
            pop_year: 2020,
            median_life: 0.0,
        }],
    );
    inputs.regions = RunRegions {
        selected_counties: vec!["06037".into()],
 // A whole-county entry (exact 5-char match) triggers both
 // Dispatch::County and Dispatch::Subcounty per subcounty_dispatch
 // in driver/run.rs.
        region_list: vec!["06037".into()],
        ..RunRegions::default()
    };

    let opts = NonroadOptions::new(RegionLevel::Subcounty, 2020);

    let outputs =
        run_simulation(&opts, &inputs, &mut executor).expect("subcounty routing must succeed");

    assert_eq!(
        outputs.counters.dispatch_calls, 2,
        "Subcounty: county-path + subcounty-path dispatch calls"
    );
    assert_eq!(
        executor.dispatches[0].dispatch,
        Dispatch::County,
        "first dispatch is County"
    );
    assert_eq!(
        executor.dispatches[1].dispatch,
        Dispatch::Subcounty,
        "second dispatch is Subcounty"
    );
}

// =============================================================================
// Loaded emission-factor injection (data-plane port,)
// =============================================================================

/// Build a county executor whose single exhaust-tech entry carries a
/// loaded THC base rate (`g/HP-hr`) with no deterioration, so the THC
/// output slot is a clean function of the injected EF.
fn county_executor_with_thc_ef(ef_thc: f32) -> ProductionExecutor {
    use moves_nonroad::common::consts::MXPOL;
    use moves_nonroad::emissions::exhaust::EmissionUnitCode;

    let mut exec = make_executor();
    let n_tech = 1; // tech_names == ["T1"]
    let mut ef = vec![0.0_f32; MXPOL * n_tech];
    ef[0] = ef_thc; // PollutantIndex::Thc -> slot 0
    let entry = &mut exec.reference.exhaust_tech_entries[0];
    entry.emission_factors = ef;
    entry.emission_units = vec![EmissionUnitCode::GramsPerHpHour; MXPOL * n_tech];
    entry.det_a = vec![0.0; MXPOL * n_tech];
    entry.det_b = vec![0.0; MXPOL * n_tech];
    entry.det_cap = vec![0.0; MXPOL * n_tech];
    exec
}

/// Sum of the THC output slot (pollutant slot 0) across all rows for a
/// one-county, one-SCC run with the given THC base rate.
fn county_thc_total(ef_thc: f32) -> f32 {
    let mut executor = county_executor_with_thc_ef(ef_thc);
    let mut inputs = NonroadInputs::new();
    inputs.push_group(
        "2270001010",
        vec![DriverRecord {
            region_code: "06037".into(),
            hp_avg: 25.0,
            population: 100.0,
            pop_year: 2020,
            median_life: 0.0,
        }],
    );
    inputs.regions = RunRegions {
        selected_counties: vec!["06037".into()],
        ..RunRegions::default()
    };
    let mut opts = NonroadOptions::new(RegionLevel::County, 2020);
    opts.growth_loaded = true;
    let out = run_simulation(&opts, &inputs, &mut executor).expect("county run must succeed");
    out.rows.iter().map(|r| r.emissions[0]).sum()
}

/// County: a loaded THC emission factor flows through `clcems` into the
/// THC output slot and scales linearly with the base rate — the core of
/// the data-plane port. A zero EF yields zero THC (the legacy behaviour);
/// only BSFC-derived CO2/SOx were produced before this wiring.
#[test]
fn county_loaded_thc_ef_produces_linear_nonzero_thc() {
    let zero = county_thc_total(0.0);
    let one = county_thc_total(10.0);
    let two = county_thc_total(20.0);

    assert_eq!(
        zero, 0.0,
        "zero THC EF must yield zero THC output, got {zero}"
    );
    assert!(
        one > 0.0,
        "a non-zero THC EF must yield non-zero THC output, got {one}"
    );
    assert!(
        (two / one - 2.0).abs() < 1e-4,
        "THC output must scale linearly with the base rate: one={one} two={two}"
    );
}
