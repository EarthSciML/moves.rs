//! NONROAD2008a Rust port — batch emissions calculator for nonroad equipment.
//!
//! Wraps the seven functional clusters of the 29k-line Fortran codebase into
//! organized Rust modules. WASM-compatible — no `std::process`, no platform-
//! specific calls, no Fortran FFI in the runtime path.
//!
//! ## Modules
//!
//! | Module | Fortran equivalent | Purpose |
//! |--------|-------------------|---------|
//! | `main` | `nonroad.f`, driver loop | Entry point, SCC×geography×year iteration |
//! | `geography` | `prccty.f`, `prcsta.f`, et al. | Process routines (6 → 1 parameterized) |
//! | `population` | `getpop.f`, `getgrw.f`, et al. | Population, growth, age distribution |
//! | `emissions` | `clcems.f`, `clcevems.f`, et al. | Exhaust, evaporative, retrofit calculation |
//! | `allocation` | `alocty.f`, `alosta.f`, `alosub.f` | Spatial apportionment |
//! | `input` | ~30 `rd*.f` parsers | All input file parsers |
//! | `output` | ~50 `wrt*.f`, `fnd*.f`, string utils | Output writers + utility helpers |
//! | `common` | 11 `.inc` files, 65 COMMON blocks | Shared state, error type, constants |

pub mod allocation;
pub mod common;
pub mod emissions;
pub mod geography;
pub mod input;
pub mod main_loop;
pub mod output;
pub mod population;

/// Run a NONROAD simulation from parsed options.
///
/// The top-level entry point called by the MOVES orchestrator (Phase 2) or
/// the standalone CLI binary. WASM-safe: no subprocess, no FFI.
pub fn run_simulation(_opts: &NonroadOptions) -> Result<NonroadOutputs, common::NonroadError> {
    // TODO(Task 113): Implement the main driver loop.
    Err(common::NonroadError::Other("not yet implemented".into()))
}

/// Configuration parsed from the `.opt` input file and CLI flags.
pub struct NonroadOptions {
    /// Path to the options file (`--opt` equivalent).
    pub opt_path: Option<String>,
    /// Override output directory.
    pub output_dir: Option<String>,
}

/// Aggregated simulation outputs, ready for serialization or Parquet export.
pub struct NonroadOutputs {
    /// Emission records keyed by SCC × geography × year × pollutant.
    pub records: Vec<EmissionRecord>,
}

/// One emission result row.
pub struct EmissionRecord {
    pub scc: String,
    pub county: Option<String>,
    pub state: Option<String>,
    pub year: i32,
    pub month: i32,
    pub day: i32,
    pub pollutant_id: i32,
    pub process_id: i32,
    pub emission_rate: f64,
}
