//! `compare-canonical` — compare a canonical-MOVES snapshot against a moves.rs output directory.
//!
//! Reads the `db__movesoutput__movesoutput` table from a canonical fixture snapshot
//! and the `MOVESOutput/` partitioned Parquet tree from a moves.rs run. Groups
//! by `pollutantID`, sums `emissionQuant`, and emits a per-pollutant comparison
//! table in either Markdown or JSON.
//!
//! # Usage
//!
//! ```sh
//! compare-canonical \
//! --canonical characterization/snapshots/sample-runspec \
//! --moves-rs /tmp/audit/sample-runspec/moves-rs-output \
//! --fixture sample-runspec \
//! [--canonical-wall 42.0] \
//! [--moves-rs-wall 0.5] \
//! [--format text|json]
//! ```
//!
//! Exit codes: 0 = success, 2 = error.

use std::io::{self, Write};
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, ValueEnum};
use serde::Serialize;

use moves_snapshot::{
    compare_pollutant_sums, pollutant_sums_from_output_dir, pollutant_sums_from_snapshot, Snapshot,
};

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
    canonical_row_count: usize,
    moves_rs_row_count: usize,
    row_count_ratio: f64,
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
    let canonical = if args.canonical.exists() {
        match Snapshot::load(&args.canonical) {
            Ok(s) => pollutant_sums_from_snapshot(&s),
            Err(e) => {
                eprintln!(
                    "warning: could not load canonical snapshot at {}: {e}",
                    args.canonical.display()
                );
                Default::default()
            }
        }
    } else {
        Default::default()
    };
    let moves_rs = pollutant_sums_from_output_dir(&args.moves_rs)?;

    let result = build_result(args, &canonical, &moves_rs);

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

// ── Building the result ───────────────────────────────────────────────────────

fn build_result(
    args: &Args,
    canonical: &moves_snapshot::PollutantSums,
    moves_rs: &moves_snapshot::PollutantSums,
) -> FixtureResult {
    let eps: f64 = 1e-30;
    let cmp = compare_pollutant_sums(canonical, moves_rs, eps);

    let canonical_row_count = canonical.row_count;
    let moves_rs_row_count = moves_rs.row_count;

    let rows: Vec<PollutantRow> = cmp
        .rows
        .iter()
        .map(|r| PollutantRow {
            pollutant_id: r.pollutant_id,
            name: pollutant_name(r.pollutant_id),
            canonical_emission_quant: r.canonical,
            moves_rs_emission_quant: r.port,
            delta: r.delta,
            pct_diff: r.rel_diff,
        })
        .collect();
    let max_abs_delta = cmp.max_abs_delta;
    let max_pct_diff = cmp.max_rel_diff;

    let speedup = match (args.canonical_wall, args.moves_rs_wall) {
        (Some(c), Some(m)) if m > 0.0 => Some(c / m),
        _ => None,
    };

    let row_count_ratio = if canonical_row_count > 0 {
        moves_rs_row_count as f64 / canonical_row_count as f64
    } else {
        0.0
    };

    FixtureResult {
        fixture: args.fixture.clone(),
        canonical_wall_secs: args.canonical_wall,
        moves_rs_wall_secs: args.moves_rs_wall,
        speedup,
        canonical_peak_mb: args.canonical_peak_mb,
        moves_rs_peak_mb: args.moves_rs_peak_mb,
        canonical_row_count,
        moves_rs_row_count,
        row_count_ratio,
        pollutant_count: rows.len(),
        max_abs_delta,
        max_pct_diff,
        rows,
    }
}

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
    writeln!(
        out,
        "Canonical rows: {} | moves.rs rows: {} | Row ratio: {:.2}",
        r.canonical_row_count, r.moves_rs_row_count, r.row_count_ratio,
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
