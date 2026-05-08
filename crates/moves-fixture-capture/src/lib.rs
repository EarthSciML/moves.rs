//! Fixture-run capture: convert a directory of MOVES intermediate-state
//! captures into a deterministic [`moves_snapshot::Snapshot`].
//!
//! The shell wrapper (`characterization/apptainer/run-fixture.sh`) executes
//! a patched MOVES inside the Apptainer SIF, dumps every non-system MariaDB
//! database to TSV, and copies `MOVESTemporary/` and `WorkerFolder/` out of
//! the SIF's bind-mounted scratch. The result is a `captures-dir` laid out as
//!
//! ```text
//! <captures-dir>/
//!   databases/<db-name>/<table>.tsv          ← rows (mariadb -B -N -r format)
//!   databases/<db-name>/<table>.schema.tsv   ← name<TAB>data_type<TAB>column_key per column
//!   moves-temporary/<file>                   ← contents of MOVESTemporary/
//!   worker-folder/<workerN>/<file>           ← contents of WorkerFolder/WorkerTempXX/
//! ```
//!
//! [`build_snapshot`] walks this directory deterministically and produces a
//! [`moves_snapshot::Snapshot`] in which every captured table is one entry.
//! Determinism guarantees:
//! * Directory walks are sorted lexicographically.
//! * MariaDB dumps are decoded via the `mariadb -B -N -r` raw escape set.
//! * Float values inherit the snapshot crate's fixed-decimal canonicalization.
//! * Worker `.tbl` / `.csv` files are imported as all-utf8 tables (literal
//!   bytes) since they ship without a schema sidecar — type-aware
//!   normalization comes from the database dumps.

pub mod capture;
pub mod error;
pub mod provenance;
pub mod runspec;
pub mod tabular;
pub mod trace;
pub mod tree;

pub use capture::{build_snapshot, BuildOptions};
pub use error::{Error, Result};
pub use provenance::{write_provenance, Provenance};
pub use runspec::RunSpec;
pub use trace::{
    build_execution_trace, write_execution_trace, ExecutionTrace, GoCalculator, JavaClass, SqlFile,
    TraceInputs, TraceSources, WorkerBundle,
};
