//! NONROAD numerical-fidelity validation harness — Task 115
//! (`mo-065ko`).
//!
//! This integration test is the entry point of the fidelity gate.
//! The harness machinery lives in the [`fidelity`] module tree
//! (`tests/fidelity/`); this file wires it together and runs it.
//!
//! The tests below, in order:
//!
//! 1. pin the ten Phase 0 NONROAD fixtures;
//! 2. confirm the tolerance table classifies every `dbgemit` label;
//! 3. route live `age_distribution` output through the divergence
//!    engine — the harness machinery exercised on real port output;
//! 4. confirm the engine catches a perturbed port value;
//! 5. route live `growth_factor` output through the engine;
//! 6. load and validate a gfortran reference corpus when one is
//!    supplied via `NONROAD_FIDELITY_REFERENCE`;
//! 7. print a harness-status banner.
//!
//! See `tests/fidelity/mod.rs` for what runs today versus what is
//! gated behind Task 117 (`run_simulation` wiring) and the
//! Apptainer-built gfortran reference.

mod fidelity;

use std::io::BufReader;

use fidelity::adapter;
use fidelity::divergence::compare_runs;
use fidelity::fixtures;
use fidelity::reference::{parse_reference, Context, Phase};
use fidelity::tolerance;

use moves_nonroad::common::consts::MXAGYR;
use moves_nonroad::population::{
    age_distribution, growth_factor, AgeDistributionResult, GrowthFactor, GrowthIndicatorRecord,
};

/// Run `age_distribution` for a fixed, well-understood case: 100
/// head of equipment, one year forward, 10 % growth, zero
/// scrappage. This mirrors the `forward_growth_single_year_zero_\
/// scrappage` unit test inside `population::agedist`, so the
/// youngest-age fraction is known to grow to ≈ 0.10.
fn known_agedist() -> AgeDistributionResult {
    let mut mdyrfrc = vec![0.0f32; MXAGYR];
    mdyrfrc[0] = 0.5;
    mdyrfrc[1] = 0.5;
    let scrap = vec![0.0f32; MXAGYR];
    age_distribution(100.0, &mdyrfrc, 2020, 2021, &scrap, |_, _| {
        Ok(GrowthFactor {
            factor: 0.10,
            base_indicator: 1.0,
            growth_indicator: 1.10,
            warning: None,
        })
    })
    .expect("age_distribution must succeed for a well-formed input")
}

#[test]
fn all_ten_nonroad_fixtures_are_present_and_valid() {
    let fixtures =
        fixtures::load_all_fixtures().expect("the ten Phase 0 NONROAD fixtures must be readable");
    assert_eq!(fixtures.len(), 10, "expected ten nr-*.xml fixtures");

    for fixture in &fixtures {
        assert!(
            fixture.is_nonroad,
            "{} is not a NONROAD runspec",
            fixture.name
        );

        let level = fixture
            .geography_level
            .as_deref()
            .unwrap_or_else(|| panic!("{} has no <geographicselection>", fixture.name));
        assert!(
            matches!(level, "NATION" | "STATE" | "COUNTY"),
            "{}: unexpected geography level {level:?}",
            fixture.name
        );

        let year = fixture
            .year
            .unwrap_or_else(|| panic!("{} has no <timespan> year", fixture.name));
        assert!(
            (1990..=2060).contains(&year),
            "{}: implausible calendar year {year}",
            fixture.name
        );
    }
}

#[test]
fn tolerance_table_covers_the_dbgemit_label_set() {
    // Every value label the four dbgemit instrumentation patches
    // emit (characterization/nonroad-build/README.md) must be
    // classified by the tolerance policy.
    let dbgemit_labels: &[(Phase, &[&str])] = &[
        (Phase::Getpop, &["popeqp", "avghpc", "usehrs", "ipopyr"]),
        (Phase::Agedist, &["mdyrfrc", "baspop"]),
        (Phase::Grwfac, &["factor", "baseyearind", "growthyearind"]),
        (
            Phase::Clcems,
            &["emsday", "emsbmy", "pop", "mfrac", "afac", "dage"],
        ),
    ];
    for (phase, labels) in dbgemit_labels {
        for label in *labels {
            assert!(
                tolerance::is_known(*phase, label),
                "tolerance table is missing {phase} label {label:?}"
            );
        }
    }

    // The bead's three rules, spot-checked: a year index is a count
    // (absolute), an emissions value is an energy quantity (relative).
    assert_eq!(
        tolerance::classify(Phase::Getpop, "ipopyr"),
        tolerance::Quantity::Count
    );
    assert_eq!(
        tolerance::classify(Phase::Clcems, "emsday"),
        tolerance::Quantity::Energy
    );
}

#[test]
fn harness_composes_with_live_agedist_output() {
    let result = known_agedist();
    let ctx = Context::parse("call=1,fips=26000,year=2021");
    let port = adapter::agedist_records(&ctx, &result);

    // The adapter must produce dbgemit-shaped records: `mdyrfrc`
    // carries MXAGYR values, `baspop` carries one.
    let mdyrfrc = port
        .iter()
        .find(|r| r.label == "mdyrfrc")
        .expect("agedist adapter must emit an mdyrfrc record");
    assert_eq!(mdyrfrc.values.len(), MXAGYR);
    let baspop = port
        .iter()
        .find(|r| r.label == "baspop")
        .expect("agedist adapter must emit a baspop record");
    assert_eq!(baspop.values.len(), 1);

    // Plumbing check: live port output routed through the
    // divergence engine against itself shows zero divergences. This
    // exercises adapter → compare_runs on genuine `moves-nonroad`
    // output. It is *not* a fidelity check against gfortran — that
    // needs the reference corpus (see `reference_corpus_*`).
    let report = compare_runs("self-check::agedist", &port, &port);
    assert!(report.passed(), "self-comparison must pass:\n{report}");
    assert_eq!(report.values_compared, MXAGYR + 1);

    // Sanity anchor: the known case grows the youngest-age fraction
    // to ≈ 0.10 and leaves the base population at 100. Loose f32
    // tolerance — this is a smoke check on the port, not the gate.
    assert!((result.mdyrfrc[0] - 0.10).abs() < 1e-5);
    assert!((result.base_population - 100.0).abs() < 1e-5);
}

#[test]
fn divergence_engine_catches_a_perturbed_port_value() {
    let result = known_agedist();
    let ctx = Context::parse("call=1,fips=26000,year=2021");
    let port = adapter::agedist_records(&ctx, &result);

    // Perturb a single mdyrfrc value well beyond the 1e-9 relative
    // budget; the engine must report exactly that one divergence.
    let mut perturbed = port.clone();
    let mdyrfrc = perturbed
        .iter_mut()
        .find(|r| r.label == "mdyrfrc")
        .expect("perturbed set still has an mdyrfrc record");
    mdyrfrc.values[1] += 1e-3;

    let report = compare_runs("self-check::perturbed", &port, &perturbed);
    assert!(!report.passed(), "a perturbed value must fail the report");
    assert_eq!(report.divergences.len(), 1);
    assert_eq!(report.divergences[0].index, 1);
    assert_eq!(report.divergences[0].key.label, "mdyrfrc");

    // The JSON form is the artifact handed to Task 116 triage.
    let json = report.to_json();
    assert!(json.contains("self-check::perturbed"));
    assert!(json.contains("\"divergences\""));
}

#[test]
fn harness_composes_with_live_grwfac_output() {
    // A two-year national growth-indicator series — enough for
    // `grwfac` to compute a slope.
    let records = [
        GrowthIndicatorRecord {
            indicator: "POP".to_string(),
            fips: "00000".to_string(),
            subregion: String::new(),
            year: 2018,
            value: 1000.0,
        },
        GrowthIndicatorRecord {
            indicator: "POP".to_string(),
            fips: "00000".to_string(),
            subregion: String::new(),
            year: 2021,
            value: 1150.0,
        },
    ];
    let refs: Vec<&GrowthIndicatorRecord> = records.iter().collect();
    let gf = growth_factor(&refs, 2018, 2021, "26161")
        .expect("growth_factor must succeed for a two-year national series");

    let ctx = Context::parse("call=1,fips=26161,year=2021");
    let port = adapter::grwfac_records(&ctx, &gf);
    assert_eq!(port.len(), 3, "grwfac adapter emits factor/base/growth");

    let report = compare_runs("self-check::grwfac", &port, &port);
    assert!(report.passed(), "self-comparison must pass:\n{report}");
    assert_eq!(report.values_compared, 3);
}

#[test]
fn reference_corpus_validates_when_present() {
    let Some(dir) = fidelity::reference_dir() else {
        eprintln!(
            "NONROAD fidelity gate: DORMANT. Set {} to a directory of \
             captured gfortran `dbgemit` baselines (one <fixture>.tsv per \
             Phase 0 NONROAD fixture) to activate reference validation. \
             See characterization/nonroad-fidelity/README.md.",
            fidelity::REFERENCE_DIR_ENV
        );
        return;
    };

    let fixtures = fixtures::load_all_fixtures().expect("fixtures must load");
    let mut found = 0;
    for fixture in &fixtures {
        let path = dir.join(fixture.reference_filename());
        if !path.exists() {
            eprintln!("  {}: no baseline at {}", fixture.name, path.display());
            continue;
        }
        found += 1;

        let file = std::fs::File::open(&path)
            .unwrap_or_else(|e| panic!("cannot open {}: {e}", path.display()));
        let records = parse_reference(BufReader::new(file))
            .unwrap_or_else(|e| panic!("{} is not a valid dbgemit capture: {e}", path.display()));
        assert!(
            !records.is_empty(),
            "{} parsed to zero records",
            path.display()
        );

        for phase in Phase::all() {
            let n = records.iter().filter(|r| r.phase == phase).count();
            eprintln!("  {} · {phase}: {n} record(s)", fixture.name);
        }
    }

    assert!(
        found > 0,
        "{} is set ({}) but holds no <fixture>.tsv baseline",
        fidelity::REFERENCE_DIR_ENV,
        dir.display()
    );
}

#[test]
fn fidelity_harness_status() {
    // An always-on status line, visible under `cargo test -- --nocapture`.
    eprintln!("── NONROAD numerical-fidelity harness · Task 115 (mo-065ko) ──");
    eprintln!("  fixtures registered:  {}", fixtures::FIXTURE_NAMES.len());
    eprintln!(
        "  tolerance budget:     energy {:e} relative · count {:e} absolute · key exact",
        tolerance::ENERGY_RELATIVE_TOLERANCE,
        tolerance::COUNT_ABSOLUTE_TOLERANCE,
    );
    match fidelity::reference_dir() {
        Some(dir) => eprintln!("  reference corpus:     {}", dir.display()),
        None => eprintln!(
            "  reference corpus:     (none — gate dormant; set {})",
            fidelity::REFERENCE_DIR_ENV
        ),
    }
}
