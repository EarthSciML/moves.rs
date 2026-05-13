//! Per-table importers — one module per project-only table.
//!
//! Each module exposes a thin wrapper around [`crate::csv_reader::read_csv`]
//! that pins the [`TableSchema`](crate::schema::TableSchema) and (where
//! the Java `getProjectDataStatus` does so) wires in the relevant
//! [`crate::validate`] calls. Wiring lives here so `lib.rs` can stay
//! focused on the public orchestration API.

pub mod drive_schedule_second_link;
pub mod link;
pub mod link_source_type_hour;
pub mod off_network_link;
pub mod op_mode_distribution;
