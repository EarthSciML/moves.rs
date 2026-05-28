//! `compare-canonical` — compare a canonical-MOVES snapshot against a moves.rs output directory.
//!
//! Reads the `db__movesoutput__movesoutput` table from a canonical fixture snapshot
//! and the `MOVESOutput/` partitioned Parquet tree from a moves.rs run.  Groups
//! by `pollutantID`, sums `emissionQuant`, and emits a per-pollutant comparison
//! table in either Markdown or JSON.
//!
//! # Usage
//!
//! ```sh
//! compare-canonical \
//!     --canonical  characterization/snapshots/sample-runspec \
//!     --moves-rs   /tmp/audit/sample-runspec/moves-rs-output \
//!     --fixture    sample-runspec \
//!     [--canonical-wall 42.0] \
//!     [--moves-rs-wall  0.5]  \
//!     [--format text|json]
//! ```
//!
//! Exit codes: 0 = success, 2 = error.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use arrow::array::{Array, Float64Array, Int16Array};
use clap::{Parser, ValueEnum};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::Serialize;

use moves_snapshot::Snapshot;

// ── CLI ──────────────────────────────────────────────────────────────────────

#[derive(Debug, Parser)]
#[command(
    name = "compare-canonical",
    about = "Compare a canonical-MOVES snapshot against a moves.rs output directory.",
    version
)]
struct Args {
    /// Canonical fixture snapshot directory (from `characterization/snapshots/<fixture>`).
    #[arg(long, value_name = "DIR")]
    canonical: PathBuf,

    /// moves.rs output directory (produced by `moves run --output <dir>`).
    #[arg(long, value_name = "DIR")]
    moves_rs: PathBuf,

    /// Fixture name used in the report header.
    #[arg(long, value_name = "NAME")]
    fixture: String,

    /// Canonical-MOVES wall-clock time in seconds (omit if not available).
    #[arg(long, value_name = "SECS")]
    canonical_wall: Option<f64>,

    /// moves.rs wall-clock time in seconds.
    #[arg(long, value_name = "SECS")]
    moves_rs_wall: Option<f64>,

    /// moves.rs peak RSS in MiB (from `/usr/bin/time -v`; omit if not available).
    #[arg(long, value_name = "MIB")]
    moves_rs_peak_mb: Option<f64>,

    /// Canonical-MOVES peak RSS in MiB (from `/usr/bin/time -v`; omit if not available).
    #[arg(long, value_name = "MIB")]
    canonical_peak_mb: Option<f64>,

    /// Output format.
    #[arg(long, value_enum, default_value_t = Format::Text)]
    format: Format,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Format {
    Text,
    Json,
}

// ── Data structures ───────────────────────────────────────────────────────────

/// Per-pollutant comparison row.
#[derive(Debug, Serialize)]
struct PollutantRow {
    pollutant_id: i64,
    name: String,
    canonical_emission_quant: f64,
    moves_rs_emission_quant: f64,
    delta: f64,
    pct_diff: f64,
}

/// Full per-fixture comparison result.
#[derive(Debug, Serialize)]
struct FixtureResult {
    fixture: String,
    canonical_wall_secs: Option<f64>,
    moves_rs_wall_secs: Option<f64>,
    speedup: Option<f64>,
    canonical_peak_mb: Option<f64>,
    moves_rs_peak_mb: Option<f64>,
    pollutant_count: usize,
    max_abs_delta: f64,
    max_pct_diff: f64,
    rows: Vec<PollutantRow>,
}

// ── Entry point ───────────────────────────────────────────────────────────────

fn main() -> ExitCode {
    let args = Args::parse();
    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::from(2)
        }
    }
}

fn run(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    let canonical = read_canonical(&args.canonical)?;
    let moves_rs = read_moves_rs(&args.moves_rs)?;

    let result = build_result(args, canonical, moves_rs);

    let stdout = io::stdout();
    let mut out = stdout.lock();

    match args.format {
        Format::Json => {
            let mut bytes = serde_json::to_vec_pretty(&result)?;
            bytes.push(b'\n');
            out.write_all(&bytes)?;
        }
        Format::Text => render_text(&mut out, &result)?,
    }
    Ok(())
}

// ── Reading canonical snapshot ────────────────────────────────────────────────

/// `pollutantID → emissionQuant sum` from the canonical snapshot.
fn read_canonical(dir: &Path) -> Result<BTreeMap<i64, f64>, Box<dyn std::error::Error>> {
    let mut sums: BTreeMap<i64, f64> = BTreeMap::new();

    if !dir.exists() {
        return Ok(sums);
    }

    let snap = match Snapshot::load(dir) {
        Ok(s) => s,
        Err(e) => {
            eprintln!(
                "warning: could not load canonical snapshot at {}: {e}",
                dir.display()
            );
            return Ok(sums);
        }
    };

    // The output DB name varies per fixture (e.g. `db__out_expand_counties`,
    // `db__junittestoutput`). Find the table by its suffix rather than its
    // full name.  Primary suffix first; fall back to activity-output tables
    // for fixtures that only produce `movesactivityoutput`.
    let table_name = snap
        .table_names()
        .find(|n| n.ends_with("__movesoutput"))
        .map(str::to_string)
        .or_else(|| {
            snap.table_names()
                .find(|n| n.ends_with("__movesactivityoutput"))
                .map(str::to_string)
        });
    let table = match table_name.as_deref().and_then(|name| snap.table(name)) {
        Some(t) => t,
        None => return Ok(sums),
    };

    let pid_idx = match table.column_index("pollutantID") {
        Some(i) => i,
        None => return Ok(sums),
    };
    let eq_idx = match table.column_index("emissionQuant") {
        Some(i) => i,
        None => return Ok(sums),
    };

    let cols = table.columns();
    let pid_col = &cols[pid_idx];
    let eq_col = &cols[eq_idx];

    use moves_snapshot::NormalizedColumn;
    for row in 0..table.row_count() {
        let pid = match pid_col {
            NormalizedColumn::Int64(v) => match v[row] {
                Some(n) => n,
                None => continue,
            },
            _ => continue,
        };
        let eq_str = match eq_col {
            NormalizedColumn::Float64String(v) => match &v[row] {
                Some(s) => s.clone(),
                None => continue,
            },
            _ => continue,
        };
        let eq: f64 = match eq_str.parse() {
            Ok(v) => v,
            Err(_) => continue,
        };
        if eq.is_finite() {
            *sums.entry(pid).or_insert(0.0) += eq;
        }
    }

    Ok(sums)
}

// ── Reading moves.rs output ───────────────────────────────────────────────────

/// `pollutantID → emissionQuant sum` from the moves.rs MOVESOutput Parquet tree.
fn read_moves_rs(output_dir: &Path) -> Result<BTreeMap<i64, f64>, Box<dyn std::error::Error>> {
    let mut sums: BTreeMap<i64, f64> = BTreeMap::new();

    let moves_output_dir = output_dir.join("MOVESOutput");
    if !moves_output_dir.exists() {
        return Ok(sums);
    }

    let parquet_files = collect_parquet_files(&moves_output_dir);
    for path in &parquet_files {
        accumulate_parquet_file(path, &mut sums)?;
    }

    Ok(sums)
}

fn collect_parquet_files(dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                files.extend(collect_parquet_files(&path));
            } else if path.extension().is_some_and(|e| e == "parquet") {
                files.push(path);
            }
        }
    }
    files.sort();
    files
}

fn accumulate_parquet_file(
    path: &Path,
    sums: &mut BTreeMap<i64, f64>,
) -> Result<(), Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;

    for batch in reader {
        let batch = batch?;
        let schema = batch.schema();

        let pid_idx = match schema.index_of("pollutantID") {
            Ok(i) => i,
            Err(_) => continue,
        };
        let eq_idx = match schema.index_of("emissionQuant") {
            Ok(i) => i,
            Err(_) => continue,
        };

        let pid_arr = batch.column(pid_idx);
        let eq_arr = batch.column(eq_idx);

        let pids = pid_arr
            .as_any()
            .downcast_ref::<Int16Array>()
            .ok_or("pollutantID column is not Int16")?;
        let eqs = eq_arr
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or("emissionQuant column is not Float64")?;

        for i in 0..batch.num_rows() {
            if pids.is_null(i) || eqs.is_null(i) {
                continue;
            }
            let pid = pids.value(i) as i64;
            let eq = eqs.value(i);
            if eq.is_finite() {
                *sums.entry(pid).or_insert(0.0) += eq;
            }
        }
    }

    Ok(())
}

// ── Building the result ───────────────────────────────────────────────────────

fn build_result(
    args: &Args,
    canonical: BTreeMap<i64, f64>,
    moves_rs: BTreeMap<i64, f64>,
) -> FixtureResult {
    // Union of all pollutant IDs from both sources.
    let mut all_ids: std::collections::BTreeSet<i64> = BTreeSet::new();
    all_ids.extend(canonical.keys().copied());
    all_ids.extend(moves_rs.keys().copied());

    let eps: f64 = 1e-30;
    let mut rows = Vec::new();
    let mut max_abs_delta: f64 = 0.0;
    let mut max_pct_diff: f64 = 0.0;

    for pid in all_ids {
        let can = canonical.get(&pid).copied().unwrap_or(0.0);
        let mrs = moves_rs.get(&pid).copied().unwrap_or(0.0);
        let delta = mrs - can;
        let pct_diff = delta / can.abs().max(eps);

        if delta.abs() > max_abs_delta {
            max_abs_delta = delta.abs();
        }
        if pct_diff.abs() > max_pct_diff.abs() {
            max_pct_diff = pct_diff;
        }

        rows.push(PollutantRow {
            pollutant_id: pid,
            name: pollutant_name(pid),
            canonical_emission_quant: can,
            moves_rs_emission_quant: mrs,
            delta,
            pct_diff,
        });
    }

    let speedup = match (args.canonical_wall, args.moves_rs_wall) {
        (Some(c), Some(m)) if m > 0.0 => Some(c / m),
        _ => None,
    };

    FixtureResult {
        fixture: args.fixture.clone(),
        canonical_wall_secs: args.canonical_wall,
        moves_rs_wall_secs: args.moves_rs_wall,
        speedup,
        canonical_peak_mb: args.canonical_peak_mb,
        moves_rs_peak_mb: args.moves_rs_peak_mb,
        pollutant_count: rows.len(),
        max_abs_delta,
        max_pct_diff,
        rows,
    }
}

use std::collections::BTreeSet;

// ── Markdown renderer ─────────────────────────────────────────────────────────

fn render_text(out: &mut impl Write, r: &FixtureResult) -> Result<(), Box<dyn std::error::Error>> {
    writeln!(out, "### {}", r.fixture)?;
    writeln!(out)?;

    let can_wall = r
        .canonical_wall_secs
        .map(|s| format!("{s:.1}"))
        .unwrap_or_else(|| "N/A".into());
    let mrs_wall = r
        .moves_rs_wall_secs
        .map(|s| format!("{s:.1}"))
        .unwrap_or_else(|| "N/A".into());
    let speedup = r
        .speedup
        .map(|s| format!("{s:.1}×"))
        .unwrap_or_else(|| "N/A".into());
    let can_peak = r
        .canonical_peak_mb
        .map(|m| format!("{m:.1} MiB"))
        .unwrap_or_else(|| "N/A".into());
    let mrs_peak = r
        .moves_rs_peak_mb
        .map(|m| format!("{m:.1} MiB"))
        .unwrap_or_else(|| "N/A".into());

    writeln!(
        out,
        "Canonical wall: {can_wall} s | moves.rs wall: {mrs_wall} s | Speedup: {speedup}"
    )?;
    writeln!(
        out,
        "Canonical peak: {can_peak} | moves.rs peak: {mrs_peak}"
    )?;
    writeln!(out)?;

    writeln!(
        out,
        "| pollutantID | name | canonical sum | moves.rs sum | delta | pct diff |"
    )?;
    writeln!(out, "|---|---|---|---|---|---|")?;

    if r.rows.is_empty() {
        writeln!(
            out,
            "| — | *(no emission data in either source)* | — | — | — | — |"
        )?;
    } else {
        for row in &r.rows {
            writeln!(
                out,
                "| {} | {} | {:.6e} | {:.6e} | {:.6e} | {:.1}% |",
                row.pollutant_id,
                row.name,
                row.canonical_emission_quant,
                row.moves_rs_emission_quant,
                row.delta,
                row.pct_diff * 100.0,
            )?;
        }
    }

    writeln!(out)?;
    Ok(())
}

// ── Pollutant name lookup ─────────────────────────────────────────────────────

fn pollutant_name(id: i64) -> String {
    let name = match id {
        1 => "Total Gaseous Hydrocarbons",
        2 => "Carbon Monoxide (CO)",
        3 => "Oxides of Nitrogen (NOx)",
        5 => "Methane (CH4)",
        6 => "Nitrous Oxide (N2O)",
        20 => "Benzene",
        21 => "Ethanol",
        22 => "MTBE",
        23 => "Naphthalene particle",
        24 => "1,3-Butadiene",
        25 => "Formaldehyde",
        26 => "Acetaldehyde",
        27 => "Acrolein",
        28 => "50% Naphthalene particle / 50% Naphthalene vapor",
        30 => "Naphthalene vapor",
        31 => "PM2.5 Primary (Filt) Direct",
        32 => "PM10 Primary (Filt) Direct",
        33 => "Elemental Carbon",
        34 => "Organic Carbon",
        35 => "Sulfate Particulate",
        40 => "PM2.5 Primary (Filt) Direct - Brakewear",
        41 => "PM2.5 Primary (Filt) Direct - Tirewear",
        42 => "PM10 Primary (Filt) Direct - Brakewear",
        43 => "PM10 Primary (Filt) Direct - Tirewear",
        44 => "Ammonia (NH3)",
        51 => "Styrene",
        52 => "Hexane",
        53 => "Butadiene",
        54 => "MTBE",
        58 => "Acetaldehyde",
        60 => "Sulfur Dioxide (SO2)",
        62 => "N2O",
        63 => "CH4",
        66 => "CO2 Equivalent",
        67 => "Total Energy Consumption",
        68 => "Atmospheric CO2",
        69 => "CO2",
        86 => "PM2.5 Primary (Filt) Direct - Crankcase",
        87 => "PM10 Primary (Filt) Direct - Crankcase",
        88 => "Organic Carbon - Crankcase",
        89 => "Elemental Carbon - Crankcase",
        91 => "Non-Methane Hydrocarbons",
        93 => "Total Organic Gases",
        98 => "Non-methane organic gases (NMOG)",
        100 => "MSAT Unspeciated HC",
        101 => "Distance Traveled",
        102 => "Source Hours",
        110 => "Total Gaseous Hydrocarbons - Crankcase",
        115 => "Non-Methane Hydrocarbons - Crankcase",
        116 => "Volatile Organic Compounds",
        119 => "Non-Methane Organic Gases",
        121 => "Total Organic Gases - Crankcase",
        168 => "PM2.5 Primary (Filt) Direct - Extended Idle",
        169 => "PM10 Primary (Filt) Direct - Extended Idle",
        _ => "",
    };
    if name.is_empty() {
        format!("Pollutant {id}")
    } else {
        name.to_string()
    }
}
