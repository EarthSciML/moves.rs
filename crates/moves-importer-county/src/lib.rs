//! `moves-importer-county` — County-scale (CDB) input-database importers
//! (Phase 4 Task 83).
//!
//! Each importer here ports one Java class from
//! `gov/epa/otaq/moves/master/implementation/importers/`. The
//! per-importer modules are thin wrappers around a
//! [`TableDescriptor`](moves_importer::TableDescriptor) plus an
//! optional [`Importer::validate_imported`](moves_importer::Importer::validate_imported)
//! override that ports the cross-row checks from the matching
//! `database/<XYZ>Importer.sql` script.
//!
//! ## Implementation status
//!
//! | Java importer | Rust module | Cross-row check |
//! |---|---|---|
//! | `SourceTypePopulationImporter` | [`source_type_population`] | year range + null-population (per `SourceTypePopulationImporter.sql`) |
//! | `ZoneRoadTypeImporter` | [`zone_road_type`] | `SHOAllocFactor` sums to 1 per `roadTypeID` (per `ZoneRoadTypeImporter.sql`) |
//! | `AgeDistributionImporter` | [`age_distribution`] | `ageFraction` sums to 1 per (sourceTypeID, yearID) |
//! | `ZoneImporter` | [`zone`] | allocation factors sum to 1 per `countyID`; embeds `zoneRoadType` |
//!
//! The remaining ~20 CDB importers (`AverageSpeedDistribution`,
//! `FuelSupply`, `FuelFormulation`, `IMCoverage`, `Hotelling`, etc.)
//! follow the same pattern and will be added in follow-up tasks under
//! mo-t8eg's tracking. See `moves-rust-migration-plan.md` Task 83 for
//! the full list, and the Java sources for the validation SQL each
//! needs ported.
//!
//! ## Validation strategy
//!
//! Per-column constraints flow through the framework's
//! [`validate_table`](moves_importer::validate_table) call:
//! null checks, numeric ranges, foreign-key membership against the
//! MOVES default DB. Each Rust importer here adds only the
//! cross-row invariants its SQL script imposes — typically allocation
//! sums or required-tuple coverage.

pub mod age_distribution;
pub mod source_type_population;
pub mod zone;
pub mod zone_road_type;

pub use age_distribution::AgeDistributionImporter;
pub use source_type_population::SourceTypePopulationImporter;
pub use zone::ZoneImporter;
pub use zone_road_type::ZoneRoadTypeImporter;

/// Every county-scale importer this crate ships.
///
/// Use this in the importer CLI as the default work list:
/// ```ignore
/// for importer in moves_importer_county::ALL {
///     run(importer, &user_inputs)?;
/// }
/// ```
pub const ALL: &[&dyn moves_importer::Importer] = &[
    &SourceTypePopulationImporter,
    &ZoneRoadTypeImporter,
    &AgeDistributionImporter,
    &ZoneImporter,
];
