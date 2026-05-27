//! End-to-end smoke test: county-level run with hand-built reference data
//! produces non-zero emissions without loading any real input files.
//!
//! Validates the four acceptance criteria from bead mo-peoe4:
//! - `outputs.rows.len() >= 1`
//! - `outputs.rows[0].fips == "06037"`
//! - `outputs.counters.geography_skips == 0`
//! - `outputs.rows[0].emissions.iter().any(|&e| e > 0.0)`

use moves_nonroad::{
    driver::{DriverRecord, RegionLevel, RunRegions},
    geography::common::ActivityUnit,
    input::scrappage::ScrappagePoint,
    population::AgeAdjustmentTable,
    simulation::{
        ActivityTableEntry, EvapTechEntry, ExhaustTechEntry, GrowthXrefEntry, NonroadInputs,
        NonroadOptions, ProductionExecutor, ReferenceData,
    },
};
use moves_nonroad::simulation::run_simulation;
use moves_nonroad::common::consts::MXHPC;

fn default_hp_levels() -> [f32; MXHPC] {
    let vs: [f32; MXHPC] = [
        3.0, 6.0, 11.0, 16.0, 25.0, 40.0, 50.0, 75.0, 100.0, 175.0, 300.0, 600.0, 750.0,
        1000.0, 1200.0, 1500.0, 1800.0, 2000.0,
    ];
    vs
}

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
            // Bins are multiples of median life: 0x, 1x, 2x.
            scrappage_curve: vec![
                ScrappagePoint { bin: 0.0, percent: 0.0 },
                ScrappagePoint { bin: 1.0, percent: 50.0 },
                ScrappagePoint { bin: 2.0, percent: 100.0 },
            ],
            age_adjustment_table: AgeAdjustmentTable::default(),
            ..ReferenceData::default()
        },
        ..ProductionExecutor::default()
    }
}

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
        }],
    );
    inputs.regions = RunRegions {
        selected_counties: vec!["06037".into()],
        ..RunRegions::default()
    };

    let mut opts = NonroadOptions::new(RegionLevel::County, 2020);
    opts.growth_loaded = true;

    let outputs = run_simulation(&opts, &inputs, &mut executor)
        .expect("run_simulation must succeed");

    assert!(
        outputs.rows.len() >= 1,
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
