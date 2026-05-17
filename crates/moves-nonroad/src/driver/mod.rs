//! Top-level driver loop.
//!
//! Cluster 1 of the seven functional groups (see
//! `ARCHITECTURE.md` § 2.1). Owns the SCC × geography × year
//! iteration that ties parsing, calculation, and writing together.
//!
//! # Fortran source files this module ports
//!
//! | File | Lines | Role | Submodule |
//! |---|---|---|---|
//! | `nonroad.f`  | 397 | Main entry point; orchestration | [`run`] |
//! | `dayloop.f`  | 126 | Day-of-year loop | [`dayloop`] |
//! | `daymthf.f`  | 194 | Month → day fractioning | [`daymthf`] |
//! | `dispit.f`   |  50 | Iteration progress display | [`progress`] |
//! | `mspinit.f`  |  41 | Progress-spinner advance | [`progress`] |
//! | `spinit.f`   |  38 | Progress-spinner init (dead) | [`progress`] |
//! | `scrptime.f` | 212 | Scrappage-time accounting | [`mod@scrptime`] |
//!
//! # Naming note
//!
//! The migration plan (`moves-rust-migration-plan.md`, Task 91)
//! refers to this cluster as `moves-nonroad::main`. The actual
//! module is named `driver` to avoid collision with the `fn main()`
//! function in `src/main.rs`. See `ARCHITECTURE.md` § 2.1 for the
//! discussion.
//!
//! # Scope of this port (Task 113)
//!
//! `nonroad.f` is the program's outermost orchestration: it reads the
//! options file, loads every input, drives the SCC × geography × year
//! iteration, and writes output. Two of those responsibilities are
//! owned by *later* tasks:
//!
//! - the output writers (`wrthdr`, `wrtsum`, `wrtsi`, `wrtams`,
//!   `clsnon`) are **Task 114**, which depends on this task;
//! - assembling the fully-populated callback context that the six
//!   geography routines need is the **Task 117** integration step.
//!
//! So this module ports the driver loop the way the `geography`
//! module ported its routines (`ARCHITECTURE.md` § 2.2): the
//! *decision logic* and *control flow* are ported as pure, tested
//! functions that return structured data, and the I/O / context
//! wiring is left as a documented boundary for the consuming task.
//! Concretely:
//!
//! - [`dayloop`], [`daymthf`], [`mod@scrptime`], and [`progress`] are the
//!   six self-contained helper routines — fully ported and tested.
//! - [`run`] ports `nonroad.f`'s record loop: SCC-prefix fuel
//!   classification, the growth-record-pair lookahead, region-shape
//!   classification, and the region-level dispatch decision. The loop
//!   is exposed as a *planner* ([`run::plan_scc_group`]) that produces
//!   the ordered sequence of geography-routine dispatch decisions; the
//!   executor that runs each decision against the geography routines
//!   and the writers is the Task 117 integration layer.

pub mod dayloop;
pub mod daymthf;
pub mod progress;
pub mod run;
pub mod scrptime;

pub use dayloop::{day_loop, DayLoopPeriod, DayRange, Season};
pub use daymthf::{day_month_factors, DayMonthFactors};
pub use progress::{Progress, Spinner};
pub use run::{
    classify_region, completion_message, dispatch_for, fuel_for_scc, growth_pair, plan_scc_group,
    Dispatch, DriverRecord, DriverStep, RegionLevel, RegionShape, RunRegions, SccGroupPlan,
    StepOutcome, PROGRAM_NAME, VERSION,
};
pub use scrptime::scrptime;
