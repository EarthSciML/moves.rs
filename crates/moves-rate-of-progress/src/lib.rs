//! `moves-rate-of-progress` — Rate-of-Progress internal control strategy
//! (Phase 6 Task 121).
//!
//! Ports `gov.epa.otaq.moves.master.implementation.internalcontrolstrategies.rateofprogress.RateOfProgressStrategy`
//! from the Java MOVES source (~1k lines).
//!
//! # Purpose
//!
//! The Rate-of-Progress (ROP) control strategy applies emission-reduction
//! percentages by pollutant, source type, regulatory class, and model year.
//! It is used to model the effect of regulations that mandate specific
//! percentage reductions in pollutant output from specific vehicle classes —
//! for example, the Clean Air Act Title I Rate of Progress requirements.
//!
//! # Data model
//!
//! The core type is [`RopTable`], a keyed collection of [`RopRecord`] values
//! where each record specifies:
//!
//! | Field | Type | Description |
//! |-------|------|-------------|
//! | `pollutantID` | `i32` | Pollutant being regulated |
//! | `sourceTypeID` | `i32` | Vehicle source type |
//! | `regClassID` | `i32` | Regulatory class |
//! | `modelYearID` | `i32` | Model year this reduction applies to |
//! | `reductionFraction` | `f64` | Fraction of emissions to remove (0–1) |
//!
//! The downstream emission scaling factor for any matching row is
//! `1.0 - reductionFraction`.
//!
//! # I/O
//!
//! [`csv_io::read_csv`] / [`csv_io::write_csv`] read and write the table in
//! canonical CSV format. The column order in input files is flexible; output
//! always uses the canonical order listed in [`csv_io::COLUMNS`].
//!
//! # Control strategy
//!
//! [`RateOfProgressControlStrategy`] implements
//! [`moves_framework::InternalControlStrategy`]. Register it with the engine's
//! [`moves_framework::ControlStrategyRegistry`]:
//!
//! ```ignore
//! let table = csv_io::read_csv("rop.csv")?.table;
//! registry.register(|| Box::new(RateOfProgressControlStrategy::new(table.clone())));
//! ```
//!
//! See `moves-rust-migration-plan.md` Task 121.

pub mod control_strategy;
pub mod csv_io;
pub mod error;
pub mod model;

pub use control_strategy::RateOfProgressControlStrategy;
pub use error::{Error, Result};
pub use model::{ModelYearId, PollutantId, RegClassId, RopKey, RopRecord, RopTable, SourceTypeId};
