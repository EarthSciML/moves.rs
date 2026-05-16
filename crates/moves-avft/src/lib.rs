//! `moves-avft` — Alternative Vehicle Fuel Technology input importer and
//! AVFT Tool equivalent (Phase 4 Task 86).
//!
//! Two responsibilities, both ported from the Java MOVES master:
//!
//! 1. **AVFT input importer** — read a user-authored CSV in the legacy
//!    `avft` table shape (`sourceTypeID, modelYearID, fuelTypeID,
//!    engTechID, fuelEngFraction`) and validate it against the rules
//!    that `database/AVFTImporter.sql` enforces in canonical MOVES.
//! 2. **AVFT Tool equivalent** — combine a (typically sparse) user AVFT
//!    table with the model-supplied default AVFT, run a gap-filling
//!    method (defaults / preserve-inputs / zeros / automatic) and a
//!    projection method (constant / national / proportional /
//!    known-fractions) per source type, and write the result as a
//!    canonical-schema AVFT Parquet file.
//!
//! ## Surface formats
//!
//! | Surface       | Format        | Role                                       |
//! |---------------|---------------|--------------------------------------------|
//! | User AVFT     | CSV           | Sparse user-authored input                 |
//! | Default AVFT  | CSV           | Per-source-type defaults from default DB   |
//! | Tool spec     | TOML          | Per-source-type method selection           |
//! | Known frac.   | CSV           | Optional known-fractions for projection    |
//! | Output AVFT   | CSV + Parquet | Completed, gap-filled, projected table     |
//!
//! The Java original uses XLSX for the user files and an XML
//! [`AVFTToolSpec`] document; the Rust port favors CSV + TOML to match
//! the Phase 4 plan ("CLI taking TOML/CSV input"). Default AVFT data is
//! supplied as a CSV path rather than read from the converted default
//! DB directly: this keeps the crate decoupled from `moves-data-default`
//! and means upstream tooling (the Phase 2 `InputDataManager`, the CDB
//! importer in Task 83) is free to derive the defaults whichever way it
//! likes.
//!
//! ## Library entry points
//!
//! * [`import::read_csv`] / [`import::validate`] — Java
//!   `AVFTImporter` + `database/AVFTImporter.sql`.
//! * [`tool::run`] — Java `AVFTTool` (gap-filling + projection).
//! * [`csv_io::write_csv`] / [`parquet_io::write_parquet`] — output the
//!   completed AVFT table.
//!
//! ## CLI
//!
//! The `moves-avft` binary wraps the library with two subcommands:
//!
//! ```text
//! moves-avft validate --input avft.csv
//! moves-avft tool --spec spec.toml \
//!                 --input avft.csv \
//!                 --default-avft default.csv \
//!                 --output-csv out.csv \
//!                 --output-parquet out.parquet
//! ```
//!
//! See `moves-rust-migration-plan.md` Task 86. The accompanying control
//! strategy (Task 120) consumes the Parquet output emitted here.
//!
//! [`AVFTToolSpec`]: https://github.com/USEPA/EPA_MOVES_Model/blob/master/gov/epa/otaq/moves/master/gui/avfttool/AVFTToolSpec.java

pub mod csv_io;
pub mod error;
pub mod import;
pub mod model;
pub mod parquet_io;
pub mod spec;
pub mod tool;

pub use error::{Error, Result};
pub use model::{AvftRecord, AvftTable, EngTechId, FuelTypeId, ModelYearId, SourceTypeId};
pub use spec::{
    GapFillingMethod, MethodEntry, ProjectionMethod, ToolSpec, GAP_FILLING_AUTOMATIC,
    GAP_FILLING_DEFAULTS_PRESERVE_INPUTS, GAP_FILLING_DEFAULTS_RENORMALIZE_INPUTS,
    PROJECTION_CONSTANT, PROJECTION_KNOWN, PROJECTION_NATIONAL, PROJECTION_PROPORTIONAL,
};
