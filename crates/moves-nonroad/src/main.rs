//! Native CLI entry point for `moves-nonroad`.
//!
//! Phase 5 skeleton — the binary currently prints a banner and exits.
//! The `nonroad.exe`-equivalent invocation it will host is wired up
//! by the Task 117 integration step, once the Task 114 output writers
//! and the geography callback context are in place. Task 113 landed
//! the [`moves_nonroad::driver`] loop logic that invocation builds on.
//!
//! The library (see [`moves_nonroad`]) is the WASM-compatible
//! surface; this binary is a native-only thin wrapper.

fn main() {
    eprintln!(
        "moves-nonroad: Phase 5 in progress. The driver loop logic is \
         ported (Task 113), but the end-to-end run is not yet wired up \
         — that is the Task 117 integration step. See \
         crates/moves-nonroad/ARCHITECTURE.md for the porting roadmap."
    );
}
