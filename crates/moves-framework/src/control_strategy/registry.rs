//! `ControlStrategyRegistry` — register and instantiate control strategies.
//!
//! Mirrors the structure of [`crate::calculator::CalculatorRegistry`] but
//! for [`InternalControlStrategy`] implementations. The registry is simpler:
//! there is no DAG, no RunSpec filtering, and no topological sort — control
//! strategies are unconditional and run in registration order.
//!
//! # Usage
//!
//! Each control-strategy crate registers its factory at startup:
//!
//! ```ignore
//! let mut registry = ControlStrategyRegistry::new();
//! registry.register(|| Box::new(AvftControlStrategy::new()));
//! registry.register(|| Box::new(RateOfProgressControlStrategy::new()));
//! ```
//!
//! The engine calls [`instantiate_all`](ControlStrategyRegistry::instantiate_all)
//! once per run to obtain the concrete instances it will drive through the
//! lifecycle hooks.

use crate::control_strategy::traits::InternalControlStrategy;

/// Factory function that produces a fresh [`InternalControlStrategy`] instance.
pub type ControlStrategyFactory = fn() -> Box<dyn InternalControlStrategy>;

/// Registry of control-strategy factories.
///
/// The engine queries this at run start to instantiate every registered strategy
/// and drive it through `pre_run → execute (per-iteration) → post_run`.
#[derive(Debug, Default)]
pub struct ControlStrategyRegistry {
    factories: Vec<ControlStrategyFactory>,
}

impl ControlStrategyRegistry {
    /// Create an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a factory. Strategies are instantiated in registration order.
    pub fn register(&mut self, factory: ControlStrategyFactory) {
        self.factories.push(factory);
    }

    /// Instantiate every registered strategy, in registration order.
    #[must_use]
    pub fn instantiate_all(&self) -> Vec<Box<dyn InternalControlStrategy>> {
        self.factories.iter().map(|f| f()).collect()
    }

    /// Number of registered factories.
    #[must_use]
    pub fn len(&self) -> usize {
        self.factories.len()
    }

    /// True iff no factories have been registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.factories.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::calculator::CalculatorContext;

    #[derive(Debug)]
    struct AlphaStrategy;
    impl InternalControlStrategy for AlphaStrategy {
        fn name(&self) -> &'static str {
            "AlphaStrategy"
        }
    }

    #[derive(Debug)]
    struct BetaStrategy;
    impl InternalControlStrategy for BetaStrategy {
        fn name(&self) -> &'static str {
            "BetaStrategy"
        }
    }

    #[test]
    fn empty_registry() {
        let r = ControlStrategyRegistry::new();
        assert!(r.is_empty());
        assert_eq!(r.len(), 0);
        assert!(r.instantiate_all().is_empty());
    }

    #[test]
    fn register_and_instantiate_preserves_order() {
        let mut r = ControlStrategyRegistry::new();
        r.register(|| Box::new(AlphaStrategy));
        r.register(|| Box::new(BetaStrategy));
        assert_eq!(r.len(), 2);
        let strategies = r.instantiate_all();
        assert_eq!(strategies.len(), 2);
        assert_eq!(strategies[0].name(), "AlphaStrategy");
        assert_eq!(strategies[1].name(), "BetaStrategy");
    }

    #[test]
    fn instantiate_all_produces_independent_instances() {
        let mut r = ControlStrategyRegistry::new();
        r.register(|| Box::new(AlphaStrategy));
        let a = r.instantiate_all();
        let b = r.instantiate_all();
        // Both lists have the same names — each call produced a fresh instance.
        assert_eq!(a[0].name(), b[0].name());
    }

    #[test]
    fn strategies_are_callable_after_instantiation() {
        let mut r = ControlStrategyRegistry::new();
        r.register(|| Box::new(AlphaStrategy));
        let strategies = r.instantiate_all();
        let ctx = CalculatorContext::new();
        strategies[0].pre_run(&ctx).expect("pre_run ok");
        strategies[0].post_run(&ctx).expect("post_run ok");
    }
}
