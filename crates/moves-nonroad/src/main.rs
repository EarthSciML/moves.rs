//! Native CLI entry point for `moves-nonroad`.
//!
//! Phase 5. The library's in-process entry point —
//! [`moves_nonroad::run_simulation`] — is wired up by the Task 117
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
//! binary runs an *empty* simulation: enough to confirm the entry
//! point is live and to print the completion banner.
//!
//! The library (see [`moves_nonroad`]) is the WASM-compatible
//! surface; this binary is a native-only thin wrapper.

use moves_nonroad::driver::RegionLevel;
use moves_nonroad::simulation::PlanRecordingExecutor;
use moves_nonroad::{run_simulation, NonroadInputs, NonroadOptions};

fn main() {
    // An empty run: valid options, no SCC groups. This always
    // succeeds — it exercises the Task 117 entry point end to end and
    // produces the "successful completion" banner.
    let options = NonroadOptions::new(RegionLevel::County, 2020);
    let inputs = NonroadInputs::new();
    let mut executor = PlanRecordingExecutor::new();

    match run_simulation(&options, &inputs, &mut executor) {
        Ok(outputs) => {
            eprintln!("moves-nonroad: run_simulation entry point is wired up (Task 117).");
            eprintln!("{}", outputs.completion_message);
            eprintln!(
                "Empty run summary: {} SCC groups planned, {} dispatch calls, \
                 {} output rows.",
                outputs.counters.scc_groups_planned,
                outputs.counters.dispatch_calls,
                outputs.row_count(),
            );
            eprintln!(
                "The disk-reading CLI wrapper (option-file parsing and the \
                 production GeographyExecutor) is the remaining work — see \
                 crates/moves-nonroad/ARCHITECTURE.md § 7."
            );
        }
        Err(err) => {
            // Unreachable for an empty run, but reported rather than
            // panicked so a future non-empty wiring fails cleanly.
            eprintln!("moves-nonroad: run_simulation failed: {err}");
        }
    }
}
