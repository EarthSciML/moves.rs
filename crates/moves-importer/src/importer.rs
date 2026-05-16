//! The [`Importer`] trait — one impl per MOVES importer class.
//!
//! Concrete implementations live in sibling crates
//! (`moves-importer-county`, `moves-importer-project`,
//! `moves-importer-nonroad`). Each impl is a zero-sized type that
//! returns static descriptors and optionally overrides
//! [`Importer::validate_imported`] for cross-row checks.

use crate::descriptor::TableDescriptor;
use crate::validator::{ImportedTable, ValidationContext, ValidationMessage};

/// One MOVES importer (e.g., `SourceTypePopulation`, `ZoneRoadType`).
///
/// Trait methods return `&'static` data so impls can be const unit
/// types — see `moves-importer-county` for examples.
pub trait Importer: Send + Sync {
    /// Human-readable name from the Java `super(...)` call, e.g.
    /// `"Source Type Population"`. Used in error messages.
    fn name(&self) -> &'static str;

    /// XML node type, e.g. `"sourcetypepopulation"`. Matches the second
    /// argument to the `super(...)` call in the Java importer.
    /// Reserved for the XML-RunSpec format (Task 12) which references
    /// importers by this id.
    fn xml_node_type(&self) -> &'static str;

    /// One or more tables this importer reads.
    fn tables(&self) -> &'static [TableDescriptor];

    /// Cross-row validation hook. Default impl returns no messages.
    ///
    /// Importers that have allocation-factor sum invariants
    /// (`ZoneRoadType.SHOAllocFactor` must sum to 1 per `roadTypeID`,
    /// per `database/ZoneRoadTypeImporter.sql`) or coverage rules
    /// (`SourceTypeYear` rows must cover every (year, sourceTypeID)
    /// pair from the RunSpec) override this and emit
    /// [`ValidationMessage`]s.
    ///
    /// `tables` is parallel to [`Importer::tables`]: `tables[i]`
    /// carries the imported rows for `self.tables()[i]`. The slice's
    /// order is the importer's declaration order so impls can index
    /// without searching.
    fn validate_imported(
        &self,
        _tables: &[ImportedTable<'_>],
        _ctx: &ValidationContext<'_>,
    ) -> Vec<ValidationMessage> {
        Vec::new()
    }
}
