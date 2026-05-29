//! `moves-source-control` — SourceMaintenance, SourceManufacturing, and
//! SourceUsage internal control strategies (Phase 6, mo-7hng).
//!
//! Ports three classes from
//! `gov.epa.otaq.moves.master.implementation.general`
//! (MOVES5.0.1 commit 25dc6c83):
//!
//! * `SourceMaintenanceControlStrategy`
//! * `SourceManufacturingControlStrategy`
//! * `SourceUsageControlStrategy`
//!
//! # What these do
//!
//! All three canonical Java implementations are no-op stubs — the lifecycle
//! methods (`subscribeToMe`, `executeLoop`, `cleanDataLoop`) have empty bodies,
//! and the accompanying JUnit test classes each declare only a `testNothing()`
//! method. The docstrings describe the *intended* scope ("modifies ELDB and
//! EERDB based on user parameters") but the logic was never implemented upstream.
//!
//! These Rust ports faithfully match that behavior: each strategy registers
//! under its canonical Java class name but leaves all input tables and emission
//! rates unchanged (`pct_diff = 0` vs canonical).
//!
//! They exist so that run configurations referencing any of these class names
//! are recognized by the framework without error.
//!
//! # Usage
//!
//! ```ignore
//! use moves_source_control::{
//!     SourceMaintenanceControlStrategy,
//!     SourceManufacturingControlStrategy,
//!     SourceUsageControlStrategy,
//! };
//!
//! registry.register(|| Box::new(SourceMaintenanceControlStrategy::new()));
//! registry.register(|| Box::new(SourceManufacturingControlStrategy::new()));
//! registry.register(|| Box::new(SourceUsageControlStrategy::new()));
//! ```

use moves_framework::InternalControlStrategy;

/// SourceMaintenanceControlStrategy — no-op internal control strategy.
///
/// Ports `gov.epa.otaq.moves.master.implementation.general.SourceMaintenanceControlStrategy`.
///
/// The canonical Java source intends to modify ELDB and EERDB (execution
/// location and emission-rate databases) to adjust source maintenance
/// schedules, but all lifecycle hooks have empty bodies. This Rust port
/// matches that behavior exactly (`pct_diff = 0`).
#[derive(Debug, Default, Clone, Copy)]
pub struct SourceMaintenanceControlStrategy;

impl SourceMaintenanceControlStrategy {
    /// Construct a new `SourceMaintenanceControlStrategy`.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl InternalControlStrategy for SourceMaintenanceControlStrategy {
    fn name(&self) -> &'static str {
        "SourceMaintenanceControlStrategy"
    }
}

/// SourceManufacturingControlStrategy — no-op internal control strategy.
///
/// Ports `gov.epa.otaq.moves.master.implementation.general.SourceManufacturingControlStrategy`.
///
/// The canonical Java source intends to modify ELDB and EERDB (execution
/// location and emission-rate databases) to adjust source manufacturing
/// rates, but all lifecycle hooks have empty bodies. This Rust port
/// matches that behavior exactly (`pct_diff = 0`).
#[derive(Debug, Default, Clone, Copy)]
pub struct SourceManufacturingControlStrategy;

impl SourceManufacturingControlStrategy {
    /// Construct a new `SourceManufacturingControlStrategy`.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl InternalControlStrategy for SourceManufacturingControlStrategy {
    fn name(&self) -> &'static str {
        "SourceManufacturingControlStrategy"
    }
}

/// SourceUsageControlStrategy — no-op internal control strategy.
///
/// Ports `gov.epa.otaq.moves.master.implementation.general.SourceUsageControlStrategy`.
///
/// The canonical Java source intends to modify ELDB (the execution location
/// database) to adjust source usage patterns, but all lifecycle hooks have
/// empty bodies. This Rust port matches that behavior exactly (`pct_diff = 0`).
#[derive(Debug, Default, Clone, Copy)]
pub struct SourceUsageControlStrategy;

impl SourceUsageControlStrategy {
    /// Construct a new `SourceUsageControlStrategy`.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl InternalControlStrategy for SourceUsageControlStrategy {
    fn name(&self) -> &'static str {
        "SourceUsageControlStrategy"
    }
}

#[cfg(test)]
mod tests {
    use moves_framework::{DataFrameStore, InMemoryStore};

    use super::*;

    // --- SourceMaintenanceControlStrategy ---

    #[test]
    fn maintenance_name_is_stable() {
        assert_eq!(
            SourceMaintenanceControlStrategy::new().name(),
            "SourceMaintenanceControlStrategy"
        );
    }

    #[test]
    fn maintenance_subscriptions_is_empty() {
        assert!(SourceMaintenanceControlStrategy::new()
            .subscriptions()
            .is_empty());
    }

    #[test]
    fn maintenance_modified_tables_is_empty() {
        assert!(
            SourceMaintenanceControlStrategy::new()
                .modified_tables()
                .is_empty(),
            "no-op strategy must not declare modified tables"
        );
    }

    #[test]
    fn maintenance_pre_run_does_not_modify_store() {
        let s = SourceMaintenanceControlStrategy::new();
        let mut store = InMemoryStore::new();
        s.pre_run(&mut store).expect("pre_run must not fail");
        assert!(
            store.names().is_empty(),
            "no-op strategy must not insert any tables (pct_diff = 0 vs canonical)"
        );
    }

    #[test]
    fn maintenance_is_trait_object_safe() {
        let s: Box<dyn InternalControlStrategy> = Box::new(SourceMaintenanceControlStrategy::new());
        assert_eq!(s.name(), "SourceMaintenanceControlStrategy");
    }

    // --- SourceManufacturingControlStrategy ---

    #[test]
    fn manufacturing_name_is_stable() {
        assert_eq!(
            SourceManufacturingControlStrategy::new().name(),
            "SourceManufacturingControlStrategy"
        );
    }

    #[test]
    fn manufacturing_subscriptions_is_empty() {
        assert!(SourceManufacturingControlStrategy::new()
            .subscriptions()
            .is_empty());
    }

    #[test]
    fn manufacturing_modified_tables_is_empty() {
        assert!(
            SourceManufacturingControlStrategy::new()
                .modified_tables()
                .is_empty(),
            "no-op strategy must not declare modified tables"
        );
    }

    #[test]
    fn manufacturing_pre_run_does_not_modify_store() {
        let s = SourceManufacturingControlStrategy::new();
        let mut store = InMemoryStore::new();
        s.pre_run(&mut store).expect("pre_run must not fail");
        assert!(
            store.names().is_empty(),
            "no-op strategy must not insert any tables (pct_diff = 0 vs canonical)"
        );
    }

    #[test]
    fn manufacturing_is_trait_object_safe() {
        let s: Box<dyn InternalControlStrategy> =
            Box::new(SourceManufacturingControlStrategy::new());
        assert_eq!(s.name(), "SourceManufacturingControlStrategy");
    }

    // --- SourceUsageControlStrategy ---

    #[test]
    fn usage_name_is_stable() {
        assert_eq!(
            SourceUsageControlStrategy::new().name(),
            "SourceUsageControlStrategy"
        );
    }

    #[test]
    fn usage_subscriptions_is_empty() {
        assert!(SourceUsageControlStrategy::new().subscriptions().is_empty());
    }

    #[test]
    fn usage_modified_tables_is_empty() {
        assert!(
            SourceUsageControlStrategy::new()
                .modified_tables()
                .is_empty(),
            "no-op strategy must not declare modified tables"
        );
    }

    #[test]
    fn usage_pre_run_does_not_modify_store() {
        let s = SourceUsageControlStrategy::new();
        let mut store = InMemoryStore::new();
        s.pre_run(&mut store).expect("pre_run must not fail");
        assert!(
            store.names().is_empty(),
            "no-op strategy must not insert any tables (pct_diff = 0 vs canonical)"
        );
    }

    #[test]
    fn usage_is_trait_object_safe() {
        let s: Box<dyn InternalControlStrategy> = Box::new(SourceUsageControlStrategy::new());
        assert_eq!(s.name(), "SourceUsageControlStrategy");
    }

    // --- cross-strategy ---

    #[test]
    fn all_three_names_are_distinct() {
        let names = [
            SourceMaintenanceControlStrategy::new().name(),
            SourceManufacturingControlStrategy::new().name(),
            SourceUsageControlStrategy::new().name(),
        ];
        assert_eq!(names[0], "SourceMaintenanceControlStrategy");
        assert_eq!(names[1], "SourceManufacturingControlStrategy");
        assert_eq!(names[2], "SourceUsageControlStrategy");
    }

    #[test]
    fn all_three_are_independent_instances() {
        let strategies: Vec<Box<dyn InternalControlStrategy>> = vec![
            Box::new(SourceMaintenanceControlStrategy::new()),
            Box::new(SourceManufacturingControlStrategy::new()),
            Box::new(SourceUsageControlStrategy::new()),
        ];
        assert_eq!(strategies.len(), 3);
        assert_eq!(strategies[0].name(), "SourceMaintenanceControlStrategy");
        assert_eq!(strategies[1].name(), "SourceManufacturingControlStrategy");
        assert_eq!(strategies[2].name(), "SourceUsageControlStrategy");
    }
}
