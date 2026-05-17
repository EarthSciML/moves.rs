//! `CalculatorRegistry` — wire the calculator-chain DAG to factory functions
//! and RunSpec-driven filtering.
//!
//! Ports `gov.epa.otaq.moves.master.framework.MOVESInstantiator` (1.9k lines
//! in Java). The Rust split:
//!
//! * **DAG knowledge** — supplied by Phase 1
//!   [`moves_calculator_info::CalculatorDag`]. The registry borrows it
//!   instead of re-parsing `CalculatorInfo.txt` at startup.
//! * **Factory lookup** — registrations of `name → fn() -> Box<dyn …>`
//!   pairs. Phase 3 calculators call [`CalculatorRegistry::register_calculator`]
//!   / [`CalculatorRegistry::register_generator`] to wire themselves in.
//! * **RunSpec filtering** — given the `(pollutant, process)` pairs the
//!   RunSpec selects, [`CalculatorRegistry::modules_for_runspec`] returns
//!   the union of:
//!   1. calculators registered for any selected `(pollutant, process)` pair,
//!   2. chain-template roots driving those calculators plus every step in
//!      those templates,
//!   3. direct subscribers with no registrations (generators) whose
//!      subscription targets a selected process — `process_id` `0` from
//!      the Java-source fallback is treated as "matches any process,"
//!   4. transitive `depends_on` upstream from any of the above.
//! * **Topological order** — [`CalculatorRegistry::topological_order`]
//!   returns a Kahn ordering over the chain-DAG restricted to the input
//!   set, with name as a deterministic tie-breaker. Upstream producers
//!   come before downstream consumers.
//!
//! # Phase 2 status
//!
//! The registry compiles against the Phase 1 DAG and the Phase 2 trait
//! definitions in [`crate::calculator`]. Phase 3 fills the factory table as
//! individual calculators land. Until then the registry can be constructed
//! from a DAG, can filter for a RunSpec, and can topologically order the
//! result — exercising every code path except `instantiate_*`, which simply
//! returns `None` for unregistered names.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use moves_calculator_info::{
    CalculatorDag, ChainTemplate, ExecutionChain, ModuleEntry, SubscriptionEntry,
};
use moves_data::{PollutantId, ProcessId};

use crate::calculator::{Calculator, Generator};
use crate::error::{Error, Result};

/// Factory function that instantiates a fresh `Calculator`.
pub type CalculatorFactory = fn() -> Box<dyn Calculator>;

/// Factory function that instantiates a fresh `Generator`.
pub type GeneratorFactory = fn() -> Box<dyn Generator>;

/// Either-kind module factory. The DAG distinguishes calculators from
/// generators by [`moves_calculator_info::ModuleKind`]; the registry mirrors
/// that split in the factory map so that callers asking for a calculator
/// don't accidentally instantiate a generator (or vice versa).
#[derive(Debug, Clone, Copy)]
pub enum ModuleFactory {
    Calculator(CalculatorFactory),
    Generator(GeneratorFactory),
}

/// The runtime registry. Owns the DAG (passed in) and a table of
/// name → factory bindings.
///
/// Construction:
///
/// * [`CalculatorRegistry::new`] — wrap a DAG already in memory.
/// * [`CalculatorRegistry::load_from_json`] — read the Phase 1 artifact
///   (`calculator-dag.json`) and wrap it.
///
/// Mutation: [`register_calculator`](Self::register_calculator) and
/// [`register_generator`](Self::register_generator) add factory bindings.
/// Phase 3 calculators typically do this once at startup (or inside a
/// helper that the engine wiring calls).
#[derive(Debug)]
pub struct CalculatorRegistry {
    dag: CalculatorDag,
    factories: BTreeMap<String, ModuleFactory>,
    /// Reverse index: `module_name → index into dag.modules`. Built once at
    /// construction so [`module`](Self::module) is O(log n).
    module_index: BTreeMap<String, usize>,
    /// Reverse index: `root_name → index into dag.chain_templates`.
    template_index: BTreeMap<String, usize>,
}

impl CalculatorRegistry {
    /// Wrap an in-memory [`CalculatorDag`] in a registry with no factory
    /// bindings yet.
    #[must_use]
    pub fn new(dag: CalculatorDag) -> Self {
        let module_index = dag
            .modules
            .iter()
            .enumerate()
            .map(|(i, m)| (m.name.clone(), i))
            .collect();
        let template_index = dag
            .chain_templates
            .iter()
            .enumerate()
            .map(|(i, t)| (t.root.clone(), i))
            .collect();
        Self {
            dag,
            factories: BTreeMap::new(),
            module_index,
            template_index,
        }
    }

    /// Load a [`CalculatorDag`] from a JSON file written by the Phase 1
    /// `moves-chain-reconstruct` binary.
    ///
    /// Returns [`Error::DagLoad`] for IO or JSON errors.
    pub fn load_from_json(path: &Path) -> Result<Self> {
        let bytes = fs::read(path).map_err(|source| Error::DagLoad {
            path: path.to_path_buf(),
            message: source.to_string(),
        })?;
        let dag: CalculatorDag =
            serde_json::from_slice(&bytes).map_err(|source| Error::DagLoad {
                path: path.to_path_buf(),
                message: source.to_string(),
            })?;
        Ok(Self::new(dag))
    }

    /// Register a calculator factory under the given DAG name. The name
    /// must match a [`ModuleEntry::name`] in the DAG; otherwise the binding
    /// is unreachable.
    ///
    /// Returns [`Error::UnknownModule`] if `name` is not in the DAG. A duplicate
    /// binding overwrites the previous one — Phase 3 should never register
    /// the same name twice, so this is treated as the caller's bug to surface
    /// loudly via an assertion when needed rather than as a typed error.
    pub fn register_calculator(&mut self, name: &str, factory: CalculatorFactory) -> Result<()> {
        if !self.module_index.contains_key(name) {
            return Err(Error::UnknownModule(name.to_string()));
        }
        self.factories
            .insert(name.to_string(), ModuleFactory::Calculator(factory));
        Ok(())
    }

    /// Register a generator factory under the given DAG name.
    pub fn register_generator(&mut self, name: &str, factory: GeneratorFactory) -> Result<()> {
        if !self.module_index.contains_key(name) {
            return Err(Error::UnknownModule(name.to_string()));
        }
        self.factories
            .insert(name.to_string(), ModuleFactory::Generator(factory));
        Ok(())
    }

    /// Borrow the DAG the registry was constructed with.
    #[must_use]
    pub fn dag(&self) -> &CalculatorDag {
        &self.dag
    }

    /// Look up a module entry by name. `None` if absent.
    #[must_use]
    pub fn module(&self, name: &str) -> Option<&ModuleEntry> {
        let idx = *self.module_index.get(name)?;
        Some(&self.dag.modules[idx])
    }

    /// Look up the chain template rooted at the named subscriber. `None`
    /// if the module isn't a chain-template root (e.g. it isn't a direct
    /// subscriber, or it's a subscriber with no registrations that drives
    /// no chain).
    #[must_use]
    pub fn chain_template(&self, root: &str) -> Option<&ChainTemplate> {
        let idx = *self.template_index.get(root)?;
        Some(&self.dag.chain_templates[idx])
    }

    /// Iterate the execution chains keyed by `(process_id, pollutant_id)`.
    /// Useful for diagnostic dumps and Phase-2 testing.
    pub fn execution_chains(&self) -> impl Iterator<Item = &ExecutionChain> {
        self.dag.execution_chains.iter()
    }

    /// Names of every module the registry has a factory for, in DAG order.
    pub fn registered_names(&self) -> impl Iterator<Item = &str> {
        self.factories.keys().map(String::as_str)
    }

    /// True iff a factory has been registered under `name`.
    #[must_use]
    pub fn has_factory(&self, name: &str) -> bool {
        self.factories.contains_key(name)
    }

    /// Instantiate the calculator registered under `name`. `None` if no
    /// factory is registered, or if a generator factory was registered
    /// under the name (caller should use [`instantiate_generator`](Self::instantiate_generator)).
    #[must_use]
    pub fn instantiate_calculator(&self, name: &str) -> Option<Box<dyn Calculator>> {
        match self.factories.get(name)? {
            ModuleFactory::Calculator(f) => Some(f()),
            ModuleFactory::Generator(_) => None,
        }
    }

    /// Instantiate the generator registered under `name`. `None` if no
    /// factory is registered, or if a calculator factory was registered
    /// under the name.
    #[must_use]
    pub fn instantiate_generator(&self, name: &str) -> Option<Box<dyn Generator>> {
        match self.factories.get(name)? {
            ModuleFactory::Generator(f) => Some(f()),
            ModuleFactory::Calculator(_) => None,
        }
    }

    /// Compute the set of modules that participate in a run with the given
    /// `(pollutant, process)` selections. See module docs for the algorithm.
    /// Returned names are sorted lexicographically for determinism.
    #[must_use]
    pub fn modules_for_runspec(&self, selections: &[(PollutantId, ProcessId)]) -> Vec<String> {
        let pair_set: BTreeSet<(u32, u32)> = selections
            .iter()
            .map(|(p, q)| (u32::from(p.0), u32::from(q.0)))
            .collect();
        let process_set: BTreeSet<u32> = selections.iter().map(|(_, q)| u32::from(q.0)).collect();

        let mut keep: BTreeSet<String> = BTreeSet::new();

        // (a) Calculators registered for any selected (pollutant, process)
        //     plus the roots driving them and every step in those roots'
        //     chain templates.
        for ec in &self.dag.execution_chains {
            if !pair_set.contains(&(ec.pollutant_id, ec.process_id)) {
                continue;
            }
            for c in &ec.registered_calculators {
                keep.insert(c.clone());
            }
            for root in &ec.roots {
                if let Some(template) = self.chain_template(root) {
                    for step in &template.steps {
                        keep.insert(step.module.clone());
                    }
                } else {
                    // Root listed in execution_chain but no template — fall
                    // back to keeping just the root name.
                    keep.insert(root.clone());
                }
            }
        }

        // (b) Direct subscribers with no registrations (generators and
        //     similar) whose subscription targets a selected process.
        //     process_id == 0 from JavaSource fallback means "we don't know
        //     which process" — we conservatively include the module if any
        //     process is selected, matching the Java fallback semantics.
        for m in &self.dag.modules {
            if !m.subscribes_directly || m.registrations_count > 0 {
                continue;
            }
            for sub in &m.subscriptions {
                if subscription_matches(sub, &process_set) {
                    keep.insert(m.name.clone());
                    break;
                }
            }
        }

        // (c) Transitive upstream closure through `depends_on`.
        let mut frontier: Vec<String> = keep.iter().cloned().collect();
        while let Some(n) = frontier.pop() {
            if let Some(m) = self.module(&n) {
                for d in &m.depends_on {
                    if keep.insert(d.clone()) {
                        frontier.push(d.clone());
                    }
                }
            }
        }

        keep.into_iter().collect()
    }

    /// Topologically order the given module names so that every module in
    /// the result appears after every other module it `depends_on`. Names
    /// not present in the DAG are returned in lexicographic order at the
    /// end (no dependency edges to honour).
    ///
    /// Returns [`Error::CyclicChain`] if the chain DAG restricted to
    /// `names` contains a cycle (which would indicate the input data was
    /// malformed; the calculator chain in MOVES is acyclic by construction).
    pub fn topological_order(&self, names: &[&str]) -> Result<Vec<String>> {
        let kept: BTreeSet<String> = names.iter().map(|s| (*s).to_string()).collect();

        // Build the dependency edges and in-degree counts limited to `kept`.
        // `incoming[n] = number of upstream producers of n that are also in
        // `kept`. We emit a module once every upstream producer has been
        // emitted. Sort the work queue lexicographically so tie-breaks are
        // deterministic.
        let mut incoming: BTreeMap<String, usize> = BTreeMap::new();
        for n in &kept {
            let upstream_in_set = self
                .module(n)
                .map(|m| m.depends_on.iter().filter(|u| kept.contains(*u)).count())
                .unwrap_or(0);
            incoming.insert(n.clone(), upstream_in_set);
        }

        let mut ready: BTreeSet<String> = incoming
            .iter()
            .filter_map(|(n, &c)| if c == 0 { Some(n.clone()) } else { None })
            .collect();

        let mut out: Vec<String> = Vec::with_capacity(kept.len());
        while let Some(n) = ready.iter().next().cloned() {
            ready.remove(&n);
            out.push(n.clone());
            // Visit downstream consumers (those who list `n` in their
            // `depends_on`). The DAG records this as `dependents` on the
            // upstream module. Decrement their in-degree.
            if let Some(m) = self.module(&n) {
                for d in &m.dependents {
                    if !kept.contains(d) {
                        continue;
                    }
                    if let Some(c) = incoming.get_mut(d) {
                        *c -= 1;
                        if *c == 0 {
                            ready.insert(d.clone());
                        }
                    }
                }
            }
        }

        if out.len() != kept.len() {
            // Some node still has positive in-degree → cycle exists.
            let unresolved: Vec<String> = incoming
                .iter()
                .filter(|(_, &c)| c > 0)
                .map(|(n, _)| n.clone())
                .collect();
            return Err(Error::CyclicChain { unresolved });
        }
        Ok(out)
    }

    /// Convenience: filter + topo-sort in one call. Returns the modules
    /// relevant to `selections`, in execution-safe order.
    pub fn execution_order_for_runspec(
        &self,
        selections: &[(PollutantId, ProcessId)],
    ) -> Result<Vec<String>> {
        let names = self.modules_for_runspec(selections);
        let refs: Vec<&str> = names.iter().map(String::as_str).collect();
        self.topological_order(&refs)
    }

    /// Verify every registered factory's name is in the DAG. The
    /// `register_*` methods already enforce this on insertion, but this
    /// method is useful when factories were registered by code paths that
    /// bypass type-checking (e.g. via a builder consuming external data).
    pub fn validate_factories(&self) -> Result<()> {
        for name in self.factories.keys() {
            if !self.module_index.contains_key(name) {
                return Err(Error::UnknownModule(name.clone()));
            }
        }
        Ok(())
    }
}

fn subscription_matches(sub: &SubscriptionEntry, process_set: &BTreeSet<u32>) -> bool {
    // Sentinel from the Java-source fallback: process unknown, treat as
    // "matches if any process is in the RunSpec." For runtime-log subscriptions
    // process_id is always >= 1.
    if sub.process_id == 0 {
        return !process_set.is_empty();
    }
    process_set.contains(&sub.process_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use moves_calculator_info::{
        build_dag, parse_calculator_info_str, CalculatorInfo, Granularity, Priority,
    };
    use moves_data::{PollutantId, ProcessId};
    use std::path::Path;

    fn parse(text: &str) -> CalculatorInfo {
        parse_calculator_info_str(text, Path::new("test")).unwrap()
    }

    /// Tiny minimal DAG: one direct-subscriber calculator with one
    /// registration.
    fn single_calc_dag() -> CalculatorDag {
        let info = parse(
            "Registration\tCO\t2\tRunning Exhaust\t1\tBaseRateCalculator\n\
             Subscribe\tBaseRateCalculator\tRunning Exhaust\t1\tMONTH\tEMISSION_CALCULATOR\n",
        );
        build_dag(&info, &[]).unwrap()
    }

    /// DAG with a chained downstream calc: BaseRateCalculator (subscriber)
    /// drives HCSpeciationCalculator (chained). Both register for (CO, 1).
    fn chained_calc_dag() -> CalculatorDag {
        let info = parse(
            "Registration\tCO\t2\tRunning Exhaust\t1\tBaseRateCalculator\n\
             Registration\tCO\t2\tRunning Exhaust\t1\tHCSpeciationCalculator\n\
             Subscribe\tBaseRateCalculator\tRunning Exhaust\t1\tMONTH\tEMISSION_CALCULATOR\n\
             Chain\tHCSpeciationCalculator\tBaseRateCalculator\n",
        );
        build_dag(&info, &[]).unwrap()
    }

    #[derive(Debug, Default)]
    struct StubCalculator(&'static str);
    impl Calculator for StubCalculator {
        fn name(&self) -> &'static str {
            self.0
        }
        fn subscriptions(&self) -> &[crate::calculator::CalculatorSubscription] {
            &[]
        }
        fn registrations(&self) -> &[moves_data::PollutantProcessAssociation] {
            &[]
        }
        fn execute(
            &self,
            _ctx: &crate::calculator::CalculatorContext,
        ) -> std::result::Result<crate::calculator::CalculatorOutput, Error> {
            Ok(crate::calculator::CalculatorOutput::empty())
        }
    }

    fn base_rate_calc() -> Box<dyn Calculator> {
        Box::new(StubCalculator("BaseRateCalculator"))
    }

    fn hc_speciation_calc() -> Box<dyn Calculator> {
        Box::new(StubCalculator("HCSpeciationCalculator"))
    }

    #[derive(Debug, Default)]
    struct StubGenerator(&'static str);
    impl Generator for StubGenerator {
        fn name(&self) -> &'static str {
            self.0
        }
        fn subscriptions(&self) -> &[crate::calculator::CalculatorSubscription] {
            &[]
        }
        fn execute(
            &self,
            _ctx: &crate::calculator::CalculatorContext,
        ) -> std::result::Result<crate::calculator::CalculatorOutput, Error> {
            Ok(crate::calculator::CalculatorOutput::empty())
        }
    }

    fn average_speed_gen() -> Box<dyn Generator> {
        Box::new(StubGenerator("AverageSpeedGenerator"))
    }

    #[test]
    fn new_indexes_modules_and_templates() {
        let reg = CalculatorRegistry::new(single_calc_dag());
        assert!(reg.module("BaseRateCalculator").is_some());
        assert!(reg.module("Nonexistent").is_none());
        // Single subscriber drives a one-step chain template.
        let template = reg.chain_template("BaseRateCalculator").expect("template");
        assert_eq!(template.root, "BaseRateCalculator");
        assert_eq!(template.steps.len(), 1);
        assert_eq!(template.granularity, Some(Granularity::Month));
    }

    #[test]
    fn register_calculator_requires_module_in_dag() {
        let mut reg = CalculatorRegistry::new(single_calc_dag());
        assert!(reg
            .register_calculator("BaseRateCalculator", base_rate_calc)
            .is_ok());
        let err = reg
            .register_calculator("MissingFromDag", base_rate_calc)
            .expect_err("should reject unknown name");
        match err {
            Error::UnknownModule(name) => assert_eq!(name, "MissingFromDag"),
            other => panic!("expected UnknownModule, got {other:?}"),
        }
    }

    #[test]
    fn register_generator_requires_module_in_dag() {
        let mut reg = CalculatorRegistry::new(single_calc_dag());
        // BaseRateCalculator is in the DAG but is a Calculator — registering
        // as a generator is *allowed* at the DAG level (the registry doesn't
        // verify ModuleKind because the DAG infers it from name heuristics
        // which Phase 3 may override). Instead, instantiate_generator
        // returns Some only when the factory was registered as a generator.
        assert!(reg
            .register_generator("BaseRateCalculator", average_speed_gen)
            .is_ok());
        assert!(reg.has_factory("BaseRateCalculator"));
        assert!(reg.instantiate_generator("BaseRateCalculator").is_some());
        assert!(reg.instantiate_calculator("BaseRateCalculator").is_none());
    }

    #[test]
    fn instantiate_calculator_returns_none_when_unregistered() {
        let reg = CalculatorRegistry::new(single_calc_dag());
        assert!(reg.instantiate_calculator("BaseRateCalculator").is_none());
    }

    #[test]
    fn instantiate_calculator_and_generator_keep_kinds_separate() {
        let mut reg = CalculatorRegistry::new(chained_calc_dag());
        reg.register_calculator("BaseRateCalculator", base_rate_calc)
            .unwrap();
        reg.register_generator("HCSpeciationCalculator", average_speed_gen)
            .unwrap();
        // BaseRateCalculator is a calculator — instantiate_generator returns
        // None even though has_factory returns true.
        assert!(reg.has_factory("BaseRateCalculator"));
        assert!(reg.instantiate_calculator("BaseRateCalculator").is_some());
        assert!(reg.instantiate_generator("BaseRateCalculator").is_none());
        // And vice versa for the generator binding.
        assert!(reg
            .instantiate_generator("HCSpeciationCalculator")
            .is_some());
        assert!(reg
            .instantiate_calculator("HCSpeciationCalculator")
            .is_none());
    }

    #[test]
    fn modules_for_runspec_picks_calculators_registered_for_pair() {
        let reg = CalculatorRegistry::new(single_calc_dag());
        let selections = vec![(PollutantId(2), ProcessId(1))]; // CO, Running Exhaust
        let modules = reg.modules_for_runspec(&selections);
        assert_eq!(modules, vec!["BaseRateCalculator"]);
    }

    #[test]
    fn modules_for_runspec_empty_when_no_pairs_match() {
        let reg = CalculatorRegistry::new(single_calc_dag());
        let selections = vec![(PollutantId(99), ProcessId(99))];
        assert!(reg.modules_for_runspec(&selections).is_empty());
    }

    #[test]
    fn modules_for_runspec_includes_chain_template_steps() {
        let reg = CalculatorRegistry::new(chained_calc_dag());
        let selections = vec![(PollutantId(2), ProcessId(1))];
        let modules = reg.modules_for_runspec(&selections);
        assert_eq!(
            modules,
            vec!["BaseRateCalculator", "HCSpeciationCalculator"]
        );
    }

    #[test]
    fn modules_for_runspec_includes_transitive_depends_on() {
        // Chain: Generator → Root → Leaf. Root registers for (CO, 1); Leaf
        // is chained from Root. Generator is upstream of Root (a chain edge
        // Generator → Root means Root depends_on Generator).
        //
        // Selecting (CO, 1) should pull in Generator transitively even though
        // it has no registration for (CO, 1) itself.
        let info = parse(
            "Registration\tCO\t2\tRunning Exhaust\t1\tRoot\n\
             Subscribe\tRoot\tRunning Exhaust\t1\tMONTH\tEMISSION_CALCULATOR\n\
             Subscribe\tUpstreamGen\tRunning Exhaust\t1\tPROCESS\tGENERATOR\n\
             Chain\tRoot\tUpstreamGen\n\
             Chain\tLeaf\tRoot\n",
        );
        let dag = build_dag(&info, &[]).unwrap();
        let reg = CalculatorRegistry::new(dag);
        let selections = vec![(PollutantId(2), ProcessId(1))];
        let modules = reg.modules_for_runspec(&selections);
        // Root + Leaf (from chain template) + UpstreamGen (transitive
        // depends_on, since Root depends_on UpstreamGen). Alphabetical.
        assert_eq!(modules, vec!["Leaf", "Root", "UpstreamGen"]);
    }

    #[test]
    fn modules_for_runspec_includes_generator_subscribed_to_process() {
        // Standalone generator subscribes to process 1, no registrations,
        // no chain edges. A RunSpec selecting any (pollutant, 1) pair
        // should pull it in.
        let info = parse(
            "Registration\tCO\t2\tRunning Exhaust\t1\tBaseRateCalculator\n\
             Subscribe\tBaseRateCalculator\tRunning Exhaust\t1\tMONTH\tEMISSION_CALCULATOR\n\
             Subscribe\tAverageSpeedGenerator\tRunning Exhaust\t1\tPROCESS\tGENERATOR\n",
        );
        let dag = build_dag(&info, &[]).unwrap();
        let reg = CalculatorRegistry::new(dag);
        let selections = vec![(PollutantId(2), ProcessId(1))];
        let modules = reg.modules_for_runspec(&selections);
        assert_eq!(modules, vec!["AverageSpeedGenerator", "BaseRateCalculator"]);
    }

    #[test]
    fn modules_for_runspec_skips_generators_when_process_not_selected() {
        let info = parse(
            "Registration\tCO\t2\tRunning Exhaust\t1\tBaseRateCalculator\n\
             Subscribe\tBaseRateCalculator\tRunning Exhaust\t1\tMONTH\tEMISSION_CALCULATOR\n\
             Subscribe\tStartGen\tStart Exhaust\t2\tPROCESS\tGENERATOR\n",
        );
        let dag = build_dag(&info, &[]).unwrap();
        let reg = CalculatorRegistry::new(dag);
        // RunSpec selects (CO, Running Exhaust=1) only; StartGen subscribes
        // to process 2 and isn't chain-linked to BaseRateCalculator.
        let selections = vec![(PollutantId(2), ProcessId(1))];
        let modules = reg.modules_for_runspec(&selections);
        assert_eq!(modules, vec!["BaseRateCalculator"]);
    }

    #[test]
    fn topological_order_returns_upstream_before_downstream() {
        // Generator → Root → Leaf
        let info = parse(
            "Registration\tCO\t2\tRunning Exhaust\t1\tRoot\n\
             Subscribe\tRoot\tRunning Exhaust\t1\tMONTH\tEMISSION_CALCULATOR\n\
             Subscribe\tUpstreamGen\tRunning Exhaust\t1\tPROCESS\tGENERATOR\n\
             Chain\tRoot\tUpstreamGen\n\
             Chain\tLeaf\tRoot\n",
        );
        let dag = build_dag(&info, &[]).unwrap();
        let reg = CalculatorRegistry::new(dag);
        let names = vec!["Leaf", "Root", "UpstreamGen"];
        let order = reg.topological_order(&names).unwrap();
        assert_eq!(order, vec!["UpstreamGen", "Root", "Leaf"]);
    }

    #[test]
    fn topological_order_independent_modules_in_lexicographic_order() {
        // Three direct subscribers with no chain edges between them.
        let info = parse(
            "Subscribe\tApple\tRunning Exhaust\t1\tMONTH\tEMISSION_CALCULATOR\n\
             Subscribe\tBanana\tRunning Exhaust\t1\tMONTH\tEMISSION_CALCULATOR\n\
             Subscribe\tCherry\tRunning Exhaust\t1\tMONTH\tEMISSION_CALCULATOR\n",
        );
        let dag = build_dag(&info, &[]).unwrap();
        let reg = CalculatorRegistry::new(dag);
        let names = vec!["Banana", "Apple", "Cherry"];
        let order = reg.topological_order(&names).unwrap();
        assert_eq!(order, vec!["Apple", "Banana", "Cherry"]);
    }

    #[test]
    fn topological_order_with_names_not_in_dag_appends_them() {
        // Unknown names have no edges; they should still appear in the
        // output. We place them lexicographically among the rest.
        let reg = CalculatorRegistry::new(single_calc_dag());
        let names = vec!["UnknownA", "BaseRateCalculator", "UnknownB"];
        let order = reg.topological_order(&names).unwrap();
        assert_eq!(order, vec!["BaseRateCalculator", "UnknownA", "UnknownB"]);
    }

    #[test]
    fn execution_order_for_runspec_combines_filter_and_topo_sort() {
        let info = parse(
            "Registration\tCO\t2\tRunning Exhaust\t1\tRoot\n\
             Subscribe\tRoot\tRunning Exhaust\t1\tMONTH\tEMISSION_CALCULATOR\n\
             Subscribe\tUpstreamGen\tRunning Exhaust\t1\tPROCESS\tGENERATOR\n\
             Chain\tRoot\tUpstreamGen\n\
             Chain\tLeaf\tRoot\n",
        );
        let dag = build_dag(&info, &[]).unwrap();
        let reg = CalculatorRegistry::new(dag);
        let order = reg
            .execution_order_for_runspec(&[(PollutantId(2), ProcessId(1))])
            .unwrap();
        assert_eq!(order, vec!["UpstreamGen", "Root", "Leaf"]);
    }

    #[test]
    fn load_from_json_round_trips_through_real_fixture() {
        // Locate the calculator-chains fixture relative to the workspace
        // root so the test passes regardless of which worktree it runs in.
        let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
        let fixture = manifest
            .parent()
            .and_then(Path::parent)
            .map(|root| root.join("characterization/calculator-chains/calculator-dag.json"))
            .expect("workspace root reachable from manifest dir");
        let reg = CalculatorRegistry::load_from_json(&fixture).unwrap_or_else(|e| {
            panic!("loading calculator-dag.json from {fixture:?} failed: {e:?}")
        });
        let dag = reg.dag();
        assert!(
            dag.modules.len() >= 60,
            "expect ~63 modules; got {}",
            dag.modules.len()
        );
        assert!(reg.module("BaseRateCalculator").is_some());
        // A characterization-level smoke check: filter on (CO, Running Exhaust)
        // and confirm the resulting set is non-trivial and is topologically
        // orderable (no cycles).
        let order = reg
            .execution_order_for_runspec(&[(PollutantId(2), ProcessId(1))])
            .unwrap();
        assert!(order.contains(&"BaseRateCalculator".to_string()));
        // BaseRateCalculator has no upstream chain edges, so any chained
        // calculator it produces should sort AFTER it.
        let base_pos = order
            .iter()
            .position(|n| n == "BaseRateCalculator")
            .unwrap();
        if let Some(hc_pos) = order.iter().position(|n| n == "HCSpeciationCalculator") {
            assert!(
                base_pos < hc_pos,
                "BaseRateCalculator must precede HCSpeciationCalculator"
            );
        }
    }

    #[test]
    fn load_from_json_returns_error_for_missing_file() {
        let err =
            CalculatorRegistry::load_from_json(Path::new("/nonexistent/dag.json")).unwrap_err();
        match err {
            Error::DagLoad { .. } => {}
            other => panic!("expected DagLoad error, got {other:?}"),
        }
    }

    #[test]
    fn load_from_json_returns_error_for_malformed_json() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), "{ not valid json }").unwrap();
        let err = CalculatorRegistry::load_from_json(tmp.path()).unwrap_err();
        match err {
            Error::DagLoad { .. } => {}
            other => panic!("expected DagLoad error, got {other:?}"),
        }
    }

    #[test]
    fn validate_factories_accepts_empty_or_in_dag() {
        let mut reg = CalculatorRegistry::new(single_calc_dag());
        reg.validate_factories().expect("empty registry is valid");
        reg.register_calculator("BaseRateCalculator", base_rate_calc)
            .unwrap();
        reg.validate_factories().expect("registered name in dag");
    }

    #[test]
    fn registered_names_returns_factories_in_alphabetical_order() {
        let mut reg = CalculatorRegistry::new(chained_calc_dag());
        reg.register_calculator("HCSpeciationCalculator", hc_speciation_calc)
            .unwrap();
        reg.register_calculator("BaseRateCalculator", base_rate_calc)
            .unwrap();
        let names: Vec<&str> = reg.registered_names().collect();
        assert_eq!(names, vec!["BaseRateCalculator", "HCSpeciationCalculator"]);
    }

    #[test]
    fn process_id_zero_subscription_is_included_when_any_process_selected() {
        // Modules whose Java-source fallback yielded process_id 0 should be
        // pulled in conservatively: as long as the RunSpec selects at least
        // one process, the unknown-process generator is included.
        use moves_calculator_info::{JavaSubscription, SubscribeStyle};
        let info = parse("Registration\tCO\t2\tRunning Exhaust\t1\tBaseRateCalculator\n");
        let java = vec![JavaSubscription {
            calculator: "UnknownProcessGen".into(),
            java_path: std::path::PathBuf::from("UnknownProcessGen.java"),
            style: SubscribeStyle::GenericBase,
            granularity: Some(Granularity::Process),
            priority: Some(Priority::parse("GENERATOR").unwrap()),
            process_expr: String::new(),
        }];
        let dag = build_dag(&info, &java).unwrap();
        let reg = CalculatorRegistry::new(dag);
        let modules = reg.modules_for_runspec(&[(PollutantId(2), ProcessId(1))]);
        // UnknownProcessGen has no registrations and process_id 0; under
        // the "matches any process" rule it should be included.
        assert!(modules.contains(&"UnknownProcessGen".to_string()));
    }
}
