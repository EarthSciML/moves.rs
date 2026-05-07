//! `moves-fixture-capture` CLI.
//!
//! Reads a captures directory laid out by `run-fixture.sh`, plus the SIF
//! lockfile and the original RunSpec, and writes a deterministic
//! `moves-snapshot::Snapshot` to the output directory.
//!
//! Invocation pattern (from `run-fixture.sh`):
//!
//! ```sh
//! moves-fixture-capture \
//!   --captures-dir /scratch/.../captures \
//!   --runspec /opt/moves/testdata/SampleRunSpec.xml \
//!   --sif-lockfile characterization/fixture-image.lock \
//!   --output-dir characterization/snapshots/samplerunspec
//! ```
//!
//! The output directory is overwritten on each invocation (the snapshot
//! crate handles atomic-ish replacement of `manifest.json` and the
//! `tables/` subdirectory). Provenance is written as `provenance.json`
//! alongside the manifest.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;

use moves_fixture_capture::{
    build_snapshot, provenance, write_provenance, BuildOptions, Provenance, RunSpec,
};

#[derive(Debug, Parser)]
#[command(
    name = "moves-fixture-capture",
    about = "Convert a MOVES fixture-run captures directory into a deterministic snapshot.",
    version
)]
struct Args {
    /// Captures directory laid out by `run-fixture.sh`.
    #[arg(long, value_name = "DIR")]
    captures_dir: PathBuf,

    /// RunSpec XML the run executed against. Used for provenance and to
    /// determine which database is the read-only default DB (excluded from
    /// the snapshot).
    #[arg(long, value_name = "PATH")]
    runspec: PathBuf,

    /// `fixture-image.lock` from `characterization/`. Provides the SIF
    /// SHA256 written into provenance.
    #[arg(long, value_name = "PATH")]
    sif_lockfile: PathBuf,

    /// Output snapshot directory. Will be created if absent. Existing
    /// `manifest.json` and `tables/` are replaced atomically; other files
    /// (such as a stale `provenance.json`) are overwritten in place.
    #[arg(long, value_name = "DIR")]
    output_dir: PathBuf,

    /// Override the fixture name written into provenance. Defaults to the
    /// filename-derived value from the RunSpec.
    #[arg(long, value_name = "NAME")]
    fixture_name: Option<String>,

    /// Additional database directories to exclude (case-insensitive). The
    /// RunSpec's scale-input DB is always excluded; this flag lets you
    /// drop more (e.g. `mysql`, `sys` if the dumper accidentally dumped
    /// them). Repeatable.
    #[arg(long = "exclude-db", value_name = "DB")]
    exclude_db: Vec<String>,
}

fn main() -> ExitCode {
    match run() {
        Ok(_) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {err}");
            ExitCode::from(1)
        }
    }
}

fn run() -> moves_fixture_capture::Result<()> {
    let args = Args::parse();

    let runspec = RunSpec::from_file(&args.runspec)?;
    let runspec_bytes =
        std::fs::read(&args.runspec).map_err(|source| moves_fixture_capture::Error::Io {
            path: args.runspec.clone(),
            source,
        })?;
    let runspec_sha256 = provenance::sha256_hex(&runspec_bytes);

    // Build options: always exclude the scale-input DB (it's read-only
    // during a run, and pinned by the SIF SHA already). Layer in any
    // user-specified extras.
    let mut opts = BuildOptions::default();
    if let Some(default_db) = runspec.scale_input_database.as_deref() {
        opts = opts.excluding_db(default_db);
    }
    for db in &args.exclude_db {
        opts = opts.excluding_db(db.clone());
    }

    let snapshot = build_snapshot(&args.captures_dir, &opts)?;

    // Materialize the snapshot before computing the aggregate hash for the
    // provenance — the hash and the on-disk manifest are guaranteed to
    // agree because the snapshot crate hashes the same bytes.
    snapshot.write(&args.output_dir)?;
    let aggregate_sha = snapshot.aggregate_hash()?;

    let sif_sha = match provenance::read_sif_sha_from_lockfile(&args.sif_lockfile)? {
        Some(s) => s,
        None => "PENDING_FIRST_BUILD".to_string(),
    };

    let fixture_name = args
        .fixture_name
        .clone()
        .unwrap_or_else(|| runspec.fixture_name.clone());

    let prov = Provenance::new(
        fixture_name,
        sif_sha,
        args.sif_lockfile.to_string_lossy().into_owned(),
        args.runspec.to_string_lossy().into_owned(),
        runspec_sha256,
        aggregate_sha,
        runspec.output_database.clone(),
        runspec.scale_input_database.clone(),
    );
    write_provenance(&args.output_dir, &prov)?;

    eprintln!(
        "[moves-fixture-capture] wrote snapshot to {}",
        args.output_dir.display()
    );
    eprintln!("  tables: {}", snapshot.len());
    eprintln!("  aggregate_sha256: {}", prov.snapshot_aggregate_sha256);
    eprintln!("  sif_sha256:       {}", prov.sif_sha256);
    Ok(())
}
