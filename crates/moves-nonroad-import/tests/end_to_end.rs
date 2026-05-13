//! End-to-end import: CSV templates on disk → Parquet tree + manifest.
//!
//! Each test wires the four named importers (population, age
//! distribution, retrofit, monthly throttle) through the orchestrator
//! using small but realistic fixtures. Reading the Parquet back via
//! `parquet::arrow::arrow_reader` proves both the schema mapping and
//! the byte-level encoding survive a round trip.

use std::path::PathBuf;

use bytes::Bytes;
use moves_nonroad_import::{import, read_manifest, ImportOptions, MANIFEST_FILENAME};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

const STAMP: &str = "2026-05-12T00:00:00Z";

fn write_fixture(dir: &std::path::Path, file: &str, body: &str) {
    std::fs::write(dir.join(file), body).unwrap();
}

fn write_all_fixtures(dir: &std::path::Path) {
    write_fixture(
        dir,
        "nrbaseyearequippopulation.csv",
        "sourceTypeID,stateID,population,NRBaseYearID\n\
         1,26,1500.5,2020\n\
         2,26,750.0,2020\n\
         1,27,2000.0,2020\n",
    );
    // Age distribution = NREngTechFraction; per-(sourceTypeID, modelYearID,
    // processID) sum to 1.0 across the engTechID axis.
    write_fixture(
        dir,
        "nrengtechfraction.csv",
        "sourceTypeID,modelYearID,processID,engTechID,NREngTechFraction\n\
         1,2020,1,10,0.6\n\
         1,2020,1,20,0.4\n\
         1,2020,2,10,1.0\n\
         2,2020,1,10,0.5\n\
         2,2020,1,20,0.5\n",
    );
    write_fixture(
        dir,
        "nrretrofitfactors.csv",
        "retrofitStartYear,retrofitEndYear,StartModelYear,EndModelYear,SCC,engTechID,hpMin,hpMax,pollutantID,retrofitID,annualFractionRetrofit,retrofitEffectiveFraction\n\
         2020,2030,2010,2020,2270002000,10,50,300,3,1,0.25,0.85\n\
         2021,2031,2011,2021,2270002000,10,50,300,3,2,0.10,0.90\n",
    );
    // Monthly throttle: per-(NREquipTypeID, stateID) sum to 1.0 across months.
    let mut monthly = String::from("NREquipTypeID,stateID,monthID,monthFraction\n");
    for &state in &[26u16, 27] {
        for month in 1..=12u16 {
            monthly.push_str(&format!("1,{state},{month},0.083333\n"));
        }
        // Last month bumped slightly so the per-state sum is exactly 1.0.
        monthly.push_str(&format!("2,{state},12,0.5\n"));
        for month in 1..=11u16 {
            monthly.push_str(&format!("2,{state},{month},0.0454545\n"));
        }
    }
    // The 12 × 0.083333 = 0.999996 sum is within the 1e-3 tolerance.
    write_fixture(dir, "nrmonthallocation.csv", &monthly);
}

fn read_back(path: &PathBuf) -> arrow::record_batch::RecordBatch {
    let bytes = std::fs::read(path).unwrap();
    let bytes = Bytes::from(bytes);
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .unwrap()
        .build()
        .unwrap();
    reader.next().unwrap().unwrap()
}

#[test]
fn imports_all_four_named_tables() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input");
    let output = dir.path().join("output");
    std::fs::create_dir_all(&input).unwrap();
    write_all_fixtures(&input);

    let mut opts = ImportOptions::new(&input, &output);
    opts.generated_at_utc = Some(STAMP.into());
    let (manifest, report) = import(&opts).unwrap();

    assert_eq!(report.tables_written.len(), 4);
    assert!(report.tables_skipped.is_empty());
    assert_eq!(report.total_rows, 3 + 5 + 2 + 48);

    // Manifest written + finalised in alphabetical order.
    let manifest_path = output.join(MANIFEST_FILENAME);
    let from_disk = read_manifest(&manifest_path).unwrap();
    assert_eq!(from_disk, manifest);
    let names: Vec<&str> = manifest.tables.iter().map(|t| t.name.as_str()).collect();
    assert_eq!(
        names,
        vec![
            "nrbaseyearequippopulation",
            "nrengtechfraction",
            "nrmonthallocation",
            "nrretrofitfactors",
        ]
    );

    // Each named Parquet file exists, has a non-zero size, and the sha
    // recorded in the manifest matches the bytes on disk.
    for entry in &manifest.tables {
        let path = output.join(&entry.path);
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(bytes.len() as u64, entry.bytes);
        assert_eq!(
            moves_nonroad_import::sha256_hex(&bytes),
            entry.sha256,
            "sha mismatch for {}",
            entry.name
        );
    }

    // Round-trip the population Parquet: we should see the three rows
    // we wrote in the order they appeared.
    let batch = read_back(&output.join("nrbaseyearequippopulation.parquet"));
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.num_columns(), 4);
    let id = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(id.values(), &[1, 2, 1]);
    let pop = batch
        .column(2)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    assert_eq!(pop.values(), &[1500.5, 750.0, 2000.0]);
}

#[test]
fn import_is_byte_deterministic() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input");
    let output_a = dir.path().join("a");
    let output_b = dir.path().join("b");
    std::fs::create_dir_all(&input).unwrap();
    write_all_fixtures(&input);

    let mut opts_a = ImportOptions::new(&input, &output_a);
    opts_a.generated_at_utc = Some(STAMP.into());
    let mut opts_b = ImportOptions::new(&input, &output_b);
    opts_b.generated_at_utc = Some(STAMP.into());

    let (m_a, _) = import(&opts_a).unwrap();
    let (m_b, _) = import(&opts_b).unwrap();
    assert_eq!(m_a, m_b);

    for entry in &m_a.tables {
        let bytes_a = std::fs::read(output_a.join(&entry.path)).unwrap();
        let bytes_b = std::fs::read(output_b.join(&entry.path)).unwrap();
        assert_eq!(bytes_a, bytes_b, "{} differs across runs", entry.name);
    }
    assert_eq!(
        std::fs::read(output_a.join(MANIFEST_FILENAME)).unwrap(),
        std::fs::read(output_b.join(MANIFEST_FILENAME)).unwrap(),
    );
}

#[test]
fn missing_csv_is_skipped_by_default_and_required_on_demand() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input");
    let output = dir.path().join("output");
    std::fs::create_dir_all(&input).unwrap();
    // Only the population fixture is provided.
    write_fixture(
        &input,
        "nrbaseyearequippopulation.csv",
        "sourceTypeID,stateID,population,NRBaseYearID\n1,26,100.0,2020\n",
    );

    let mut opts = ImportOptions::new(&input, &output);
    opts.generated_at_utc = Some(STAMP.into());
    let (_, report) = import(&opts).unwrap();
    assert_eq!(
        report.tables_written,
        vec!["nrbaseyearequippopulation".to_string()]
    );
    assert_eq!(report.tables_skipped.len(), 3);

    let mut opts_strict = opts.clone();
    opts_strict.require_all_tables = true;
    let err = import(&opts_strict).unwrap_err();
    matches!(err, moves_nonroad_import::Error::Io { .. });
}

#[test]
fn fraction_sum_violation_reports_offending_group() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input");
    let output = dir.path().join("output");
    std::fs::create_dir_all(&input).unwrap();
    // Only this importer; two months sum to 0.5 instead of 1.0.
    write_fixture(
        &input,
        "nrmonthallocation.csv",
        "NREquipTypeID,stateID,monthID,monthFraction\n\
         1,26,1,0.25\n\
         1,26,2,0.25\n",
    );
    let mut opts = ImportOptions::new(&input, &output);
    opts.tables = vec!["nrmonthallocation".into()];
    opts.generated_at_utc = Some(STAMP.into());
    let err = import(&opts).unwrap_err();
    match err {
        moves_nonroad_import::Error::AllocationSum {
            actual,
            expected,
            tolerance,
            ..
        } => {
            assert!((actual - 0.5).abs() < 1e-9);
            assert_eq!(expected, 1.0);
            assert!(tolerance > 0.0);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn engtech_fraction_round_trips_six_columns() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input");
    let output = dir.path().join("output");
    std::fs::create_dir_all(&input).unwrap();
    write_fixture(
        &input,
        "nrengtechfraction.csv",
        "sourceTypeID,modelYearID,processID,engTechID,NREngTechFraction\n\
         1,2020,1,10,1.0\n",
    );
    let mut opts = ImportOptions::new(&input, &output);
    opts.tables = vec!["nrengtechfraction".into()];
    opts.generated_at_utc = Some(STAMP.into());
    import(&opts).unwrap();
    let batch = read_back(&output.join("nrengtechfraction.parquet"));
    assert_eq!(batch.num_columns(), 5);
    assert_eq!(batch.num_rows(), 1);
    let frac = batch
        .column(4)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    assert_eq!(frac.value(0), 1.0);
}

#[test]
fn retrofit_nullable_fractions_pass_through() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input");
    let output = dir.path().join("output");
    std::fs::create_dir_all(&input).unwrap();
    // Both annualFractionRetrofit and retrofitEffectiveFraction empty.
    write_fixture(
        &input,
        "nrretrofitfactors.csv",
        "retrofitStartYear,retrofitEndYear,StartModelYear,EndModelYear,SCC,engTechID,hpMin,hpMax,pollutantID,retrofitID,annualFractionRetrofit,retrofitEffectiveFraction\n\
         2020,2030,2010,2020,2270002000,10,50,300,3,1,,\n",
    );
    let mut opts = ImportOptions::new(&input, &output);
    opts.tables = vec!["nrretrofitfactors".into()];
    opts.generated_at_utc = Some(STAMP.into());
    import(&opts).unwrap();
    let batch = read_back(&output.join("nrretrofitfactors.parquet"));
    assert_eq!(batch.num_rows(), 1);
    assert!(batch.column(10).is_null(0));
    assert!(batch.column(11).is_null(0));
}
