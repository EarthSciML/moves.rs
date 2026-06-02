//! Native CLI entry point for `moves-nonroad`.
//!
//!The library's in-process entry point//! [`moves_nonroad::run_simulation`] — is wired up by the
//! integration step ([`moves_nonroad::simulation`]). This binary is
//! the native-only thin wrapper described in `ARCHITECTURE.md` § 7:
//! it will read a NONROAD options file and its inputs from disk, call
//! `run_simulation`, and write the legacy text output format for
//! parity testing against the Windows-compiled reference.
//!
//! The disk-side orchestration — parsing the `.opt` file into a
//! [`NonroadOptions`](moves_nonroad::NonroadOptions) and the input
//! files into a [`NonroadInputs`](moves_nonroad::NonroadInputs), plus
//! the production `GeographyExecutor` that evaluates the geography
//! routines — is the remaining wrapper work. Until it lands, this
//! binary only confirms the in-process entry point is live; it does
//! NOT impersonate a completed run. Because it reads no disk inputs
//! and computes no emissions, it prints a "not implemented"
//! diagnostic and exits with a non-zero status rather than the
//! reference's "Successful completion" banner.
//!
//! The library (see [`moves_nonroad`]) is the WASM-compatible
//! surface; this binary is a native-only thin wrapper.

use std::process::ExitCode;

use moves_nonroad::driver::RegionLevel;
use moves_nonroad::simulation::PlanRecordingExecutor;
use moves_nonroad::{run_simulation, NonroadInputs, NonroadOptions};

fn main() -> ExitCode {
    // The disk-side CLI orchestration that the reference `nonroad.exe`
    // performs — parse the `.opt` file named on the command line into a
    // `NonroadOptions`, load its fixed-width input files into a
    // `NonroadInputs`, and drive the production `GeographyExecutor` — is
    // not wired up here yet (ARCHITECTURE.md § 7). The reference reads
    // those inputs from `argv`, errors if the options file is missing
    // (`nonroad.f:92` -> 7002) or if no population data matches the
    // requested regions/equipment (`nonroad.f:146` -> 7000), and only
    // prints "Successful completion" (`nonroad.f:349`) after actually
    // processing records.
    //
    // Until that path lands, this binary must NOT impersonate a
    // successful production run: hard-coding empty options/inputs and a
    // non-evaluating `PlanRecordingExecutor` would compute zero
    // emissions while printing a success banner, masking that nothing
    // was read or computed. We still exercise the in-process entry point
    // end to end (so a regression in `run_simulation` is caught), but we
    // exit with a non-zero status and a clear "not yet implemented"
    // diagnostic rather than a success banner.
    let options = NonroadOptions::new(RegionLevel::County, 2020);
    let inputs = NonroadInputs::new();
    let mut executor = PlanRecordingExecutor::new();

    match run_simulation(&options, &inputs, &mut executor) {
        Ok(outputs) => {
            eprintln!(
                "moves-nonroad: run_simulation entry point is wired up \
                 ({} SCC groups planned, {} dispatch calls, {} output rows on \
                 the empty smoke run).",
                outputs.counters.scc_groups_planned,
                outputs.counters.dispatch_calls,
                outputs.row_count(),
            );
        }
        Err(err) => {
            eprintln!("moves-nonroad: run_simulation entry-point check failed: {err}");
            return ExitCode::FAILURE;
        }
    }

    // No disk inputs were read and no production executor was run, so
    // this is not a completed NONROAD run. Surface that explicitly
    // instead of printing the reference's "Successful completion"
    // banner with a zero exit code.
    eprintln!(
        "moves-nonroad: ERROR: the disk-reading CLI wrapper is not implemented \
         yet. The command line was ignored; no options file was parsed, no \
         input files were loaded, and the production GeographyExecutor was not \
         run, so no emissions were computed. Use the in-process \
         `moves_nonroad::run_simulation` entry point until the wrapper lands \
         (crates/moves-nonroad/ARCHITECTURE.md § 7)."
    );
    ExitCode::FAILURE
}
