//! Calculator-chain reconstruction for the MOVES → Rust port (Phase 1 Task 10).
//!
//! Parses three families of input:
//!
//! 1. **`CalculatorInfo.txt`** — the runtime log MOVES writes via
//!    `InterconnectionTracker` when `CompilationFlags.GENERATE_CALCULATOR_INFO_DOCUMENTATION`
//!    is set. Three directive shapes, tab-separated:
//!
//!    | Prefix         | Meaning                                                          |
//!    |----------------|------------------------------------------------------------------|
//!    | `Registration` | A calculator advertised that it produces `(pollutant, process)`. |
//!    | `Subscribe`    | A module hooked into the MasterLoop at `(granularity, priority)`.|
//!    | `Chain`        | The first module's output depends on the second module's output. |
//!
//! 2. **`MasterLoopGranularity.java` / `MasterLoopPriority.java`** — the enum
//!    constants the runtime log textualises. Encoded into the crate as
//!    [`Granularity`] and [`Priority`] so consumers downstream don't need to
//!    re-derive the numeric ordering.
//!
//! 3. **Calculator `.java` files** (optional) — used to fill in granularities
//!    for direct-subscriber calculators that didn't fire during the run that
//!    produced `CalculatorInfo.txt`. Two patterns are recognised:
//!
//!    * Direct `targetLoop.subscribe(this, process, MasterLoopGranularity.X,
//!      MasterLoopPriority.Y)` calls inside `subscribeToMe()`.
//!    * `extends GenericCalculatorBase` subclasses whose constructor passes
//!      `MasterLoopGranularity.X, N` (priority adjustment) to `super(...)`.
//!
//! The output ([`CalculatorDag`]) is a single self-contained JSON document
//! that Phase 2 Task 19 (`CalculatorRegistry`) consumes to wire up the
//! Rust calculator graph. Two runs against the same inputs produce a
//! byte-identical file.
//!
//! ## Architecture, in one paragraph
//!
//! Calculators in MOVES fall into two camps. **Direct subscribers** (e.g.
//! `BaseRateCalculator`, `NonroadEmissionCalculator`) hook themselves into
//! the MasterLoop at a declared granularity. **Chained calculators** (e.g.
//! `HCSpeciationCalculator`, `AirToxicsCalculator`) never subscribe directly;
//! instead they attach themselves as downstream consumers of one or more
//! upstream calculators via `chainCalculator(this)`. When the upstream fires
//! at MasterLoop time, the chained calculator fires inside the same
//! execution loop. The DAG this crate builds captures both the registration
//! map (which calculator handles which `(process, pollutant)` pair) and the
//! chain topology (who triggers whom). For every `(process, pollutant)` the
//! suite covers, the JSON lists the topologically ordered chain of
//! calculators that fire, annotated with each step's role (`subscriber`,
//! `chained`, `chained-via`).

mod chain;
mod directives;
mod error;
mod java;
mod loop_meta;
mod output;

pub use chain::{
    build_dag, CalculatorDag, ChainEdge, ChainRole, ChainStep, ChainTemplate, DagCounts,
    ExecutionChain, ModuleEntry, ModuleKind, RegistrationEntry, Source, SubscriptionEntry,
};
pub use directives::{
    parse_calculator_info, parse_calculator_info_str, CalculatorInfo, ChainDirective,
    DirectiveLocation, RegistrationDirective, SubscribeDirective,
};
pub use error::{Error, Result};
pub use java::{parse_java_subscriptions, scan_source_dir, JavaSubscription, SubscribeStyle};
pub use loop_meta::{Granularity, Priority};
pub use output::{write_dag_json, DAG_FILE, DAG_VERSION};
