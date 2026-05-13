//! Table descriptors for the user-input Nonroad importers.
//!
//! Each sub-module owns one table: column schema, validation rules, and
//! the conventional CSV filename the user is expected to provide.
//!
//! The four importers materialised here are the ones called out in the
//! Phase 4 Task 85 description ("population, age distribution, retrofit,
//! monthly throttle"). Adding a sibling importer is a matter of writing
//! one more module, registering it in [`all`], and providing a fixture.

pub mod age_distribution;
pub mod monthly_allocation;
pub mod population;
pub mod retrofit;

use crate::schema::TableSchema;

/// One importer entry: the table schema plus the relative CSV filename
/// the importer reads from. Filenames mirror the lower-case table name
/// with a `.csv` suffix so the user can predict the layout of an import
/// directory without consulting the documentation.
#[derive(Debug, Clone, Copy)]
pub struct ImporterEntry {
    pub schema: &'static TableSchema,
    pub csv_filename: &'static str,
    /// Output Parquet filename (relative to the manifest's root).
    pub parquet_filename: &'static str,
}

/// All built-in importers, in a stable order. Iteration order matters
/// for the manifest's `tables` array — it is independently sorted by
/// [`Manifest::finalize`](crate::manifest::Manifest::finalize), but
/// stable here makes the convert pipeline's intermediate state
/// inspectable.
pub fn all() -> &'static [ImporterEntry] {
    ALL
}

static ALL: &[ImporterEntry] = &[
    ImporterEntry {
        schema: &population::SCHEMA,
        csv_filename: "nrbaseyearequippopulation.csv",
        parquet_filename: "nrbaseyearequippopulation.parquet",
    },
    ImporterEntry {
        schema: &age_distribution::SCHEMA,
        csv_filename: "nrengtechfraction.csv",
        parquet_filename: "nrengtechfraction.parquet",
    },
    ImporterEntry {
        schema: &retrofit::SCHEMA,
        csv_filename: "nrretrofitfactors.csv",
        parquet_filename: "nrretrofitfactors.parquet",
    },
    ImporterEntry {
        schema: &monthly_allocation::SCHEMA,
        csv_filename: "nrmonthallocation.csv",
        parquet_filename: "nrmonthallocation.parquet",
    },
];

/// Look up an importer by lower-cased table name. Used by
/// [`crate::importer::ImportOptions`] to filter the set of tables that
/// the caller wants processed.
pub fn find(name: &str) -> Option<&'static ImporterEntry> {
    all()
        .iter()
        .find(|e| e.schema.name.eq_ignore_ascii_case(name))
}
