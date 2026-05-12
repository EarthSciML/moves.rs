//! SQL macro expander and section-marker preprocessor for MOVES SQL scripts
//! (Phase 2 Task 22).
//!
//! Ports two pieces of canonical-MOVES Java that work as a pipeline:
//!
//! 1. [`expander::MacroExpander`] — port of
//!    `gov/epa/otaq/moves/master/framework/SQLMacroExpander.java` (318 Java
//!    lines, EPA MOVES 25dc6c83). Stores named value sets keyed by macro
//!    names of the form `##macro.PREFIXcolumn##` and `##macro.csv.COLUMN##`,
//!    and expands each SQL line into the cartesian product of all matching
//!    value-set rows.
//!
//! 2. [`sections::process_sections`] — port of the section-marker pass
//!    inside `gov/epa/otaq/moves/master/framework/BundleUtilities.java`
//!    `readAndHandleScriptedCalculations`. Filters SQL lines based on
//!    `-- Section <name>` / `-- End Section <name>` markers and a caller-
//!    provided set of enabled section names, with optional `##context.*##`
//!    textual replacements applied along the way.
//!
//! # Not on the runtime path
//!
//! The migration plan deliberately keeps this code out of the calculator
//! runtime — the Rust calculators do not run macro-templated SQL; they
//! consume the same input data via Polars expressions. This crate exists
//! for two reasons:
//!
//! * **Calculator porting**. The
//!   [`moves-sql-expand`](../moves_sql_expand/index.html) binary takes a
//!   raw MOVES SQL script and a configuration of value sets / enabled
//!   sections / context replacements, and prints what the SQL looks like
//!   after MOVES would have macro-expanded and section-filtered it. Phase
//!   3 calculator authors use this output as the "canonical reference" for
//!   the queries they are reimplementing in Polars.
//! * **Section semantics**. The section-marker preprocessor encodes
//!   RunSpec-conditional behaviour that the Rust calculators must replicate
//!   (e.g. `Process2`, `WithRegClassID`, `Inventory` vs. `Rates`). Tests in
//!   `moves-calculators` can import [`sections::process_sections`] to
//!   verify the calculator's branching matches what canonical MOVES would
//!   produce for the same RunSpec.
//!
//! # Workflow
//!
//! ```no_run
//! use moves_sql_macros::{expander::MacroExpander, sections::process_sections};
//!
//! let mut expander = MacroExpander::new();
//! expander.add_csv_data(
//!     "select sourceTypeID from RunSpecSourceType",
//!     "sourceTypeID",
//!     &["21", "31"],
//!     5000,
//!     false,
//!     false,
//!     Some("0"),
//! );
//! expander.compile();
//!
//! let raw: Vec<String> = std::fs::read_to_string("BaseRateCalculator.sql")?
//!     .lines()
//!     .map(|s| s.to_string())
//!     .collect();
//!
//! // Stage 1: macro-expand each line.
//! let mut macro_expanded = Vec::new();
//! for line in &raw {
//!     expander.expand_and_add(line, &mut macro_expanded);
//! }
//!
//! // Stage 2: section-filter the macro-expanded lines.
//! let out = process_sections(
//!     &macro_expanded,
//!     &["WithRegClassID", "Process2", "Inventory"],
//!     &[(
//!         "##context.year##".to_string(),
//!         "2030".to_string(),
//!     )],
//! );
//!
//! for line in &out.lines {
//!     println!("{line}");
//! }
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

pub mod config;
pub mod error;
pub mod expander;
pub mod sections;

pub use config::{CsvSet, DataSet, ExpandConfig};
pub use error::{Error, Result};
pub use expander::{do_replacements, escape_sql, MacroExpander};
pub use sections::{process_sections, SectionProcessOutput};
