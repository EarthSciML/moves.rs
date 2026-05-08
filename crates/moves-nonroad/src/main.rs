//! Native CLI entry point for `moves-nonroad`.
//!
//! Phase 5 skeleton — the binary currently prints a banner and exits.
//! Task 113 wires this up to [`moves_nonroad::driver`] for the
//! `nonroad.exe`-equivalent invocation used by Task 115's
//! characterization tests.
//!
//! The library (see [`moves_nonroad`]) is the WASM-compatible
//! surface; this binary is a native-only thin wrapper.

fn main() {
    eprintln!(
        "moves-nonroad: Phase 5 skeleton (Task 91). The driver loop \
         is not yet wired up; see crates/moves-nonroad/ARCHITECTURE.md \
         for the porting roadmap."
    );
}
