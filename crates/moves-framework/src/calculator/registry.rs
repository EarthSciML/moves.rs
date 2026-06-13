//! `CalculatorRegistry` — wire the calculator-chain DAG to factory functions
//! and RunSpec-driven filtering.
//!
//! Ports `gov.epa.otaq.moves.master.framework.MOVESInstantiator` (1.9k lines
//! in Java). The Rust split:
//!
//! * **DAG knowledge** — supplied by
//! [`moves_calculator_info::CalculatorDag`]. The registry borrows it
//! instead of re-parsing `CalculatorInfo.txt` at startup.
//! * **Factory lookup** — registrations of `name → fn() -> Box<dyn …>`
//! pairs. calculators call [`CalculatorRegistry::register_calculator`]
//! / [`CalculatorRegistry::register_generator`] to wire themselves in.
//! * **RunSpec filtering** — given the `(pollutant, process)` pairs the
//! RunSpec selects, [`CalculatorRegistry::modules_for_runspec`] returns
//! the union of:
//! 1. calculators registered for any selected `(pollutant, process)` pair,
//! 2. chain-template roots driving those calculators plus every step in
//! those templates,
//! 3. direct subscribers with no registrations (generators) whose
//! subscription targets a selected process — `process_id` `0` from
//! the Java-source fallback is treated as "matches any process,"
//! 4. transitive `depends_on` upstream from any of the above.
//! * **Topological order** — [`CalculatorRegistry::topological_order`]
//! returns a Kahn ordering over the chain-DAG restricted to the input
//! set, with name as a deterministic tie-breaker. Upstream producers
//! come before downstream consumers.
//!
//! # status
//!
//! The registry compiles against the DAG and the trait
//! definitions in [`crate::calculator`]. fills the factory table as
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
/// * [`CalculatorRegistry::load_from_json`] — read the artifact
/// (`calculator-dag.json`) and wrap it.
///
/// Mutation: [`register_calculator`](Self::register_calculator) and
/// [`register_generator`](Self::register_generator) add factory bindings.
/// calculators typically do this once at startup (or inside a
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
    /// Slow-store table names (lowercased) required by all registered
    /// calculators and generators — union of each module's `input_tables()`.
    /// Populated at registration time so callers can filter snapshot loads.
    module_input_tables: BTreeSet<String>,
    /// Per-module input tables (lowercased), captured at registration. Used by
    /// [`chunk_chains`](crate::execution::executor::chunk_chains) to connect a
    /// consumer to the producers of the scratch tables it reads.
    module_inputs: BTreeMap<String, BTreeSet<String>>,
    /// Per-module output (scratch) tables (lowercased), captured at
    /// registration. The producer side of the table-dependency edges.
    module_outputs: BTreeMap<String, BTreeSet<String>>,
    /// Union of every registered calculator's
    /// [`replaced_pollutants`](crate::Calculator::replaced_pollutants) —
    /// pollutants a chained calculator consumes and replaces (e.g. SulfatePM's
    /// EC 112 / NonECPM 118). Producers drop their zero-valued rows for these
    /// before the output aggregator; the canonical-snapshot gate uses this set
    /// to assert canonical never emits a zero row for a replaced pollutant.
    replaced_pollutants: BTreeSet<i32>,
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
            module_input_tables: BTreeSet::new(),
            module_inputs: BTreeMap::new(),
            module_outputs: BTreeMap::new(),
            replaced_pollutants: BTreeSet::new(),
        }
    }

    /// Load a [`CalculatorDag`] from a JSON file written by the
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
    /// binding overwrites the previous one — should never register
    /// the same name twice, so this is treated as the caller's bug to surface
    /// loudly via an assertion when needed rather than as a typed error.
    ///
    /// The factory is called once at registration to read `input_tables()` so
    /// [`required_input_tables`](Self::required_input_tables) can answer without
    /// instantiating again later.
    pub fn register_calculator(&mut self, name: &str, factory: CalculatorFactory) -> Result<()> {
        if !self.module_index.contains_key(name) {
            return Err(Error::UnknownModule(name.to_string()));
        }
        let instance = factory();
        let inputs: BTreeSet<String> = instance
            .input_tables()
            .iter()
            .map(|t| t.to_ascii_lowercase())
            .collect();
        self.replaced_pollutants
            .extend(instance.replaced_pollutants().iter().copied());
        self.module_input_tables.extend(inputs.iter().cloned());
        self.module_inputs.insert(name.to_string(), inputs);
        // Calculators emit emission/activity output, not scratch tables, so
        // they have no producer-side table edges.
        self.module_outputs
            .insert(name.to_string(), BTreeSet::new());
        self.factories
            .insert(name.to_string(), ModuleFactory::Calculator(factory));
        Ok(())
    }

    /// Register a generator factory under the given DAG name.
    ///
    /// Like [`register_calculator`](Self::register_calculator), the factory is
    /// called once to harvest `input_tables()` for
    /// [`required_input_tables`](Self::required_input_tables).
    pub fn register_generator(&mut self, name: &str, factory: GeneratorFactory) -> Result<()> {
        if !self.module_index.contains_key(name) {
            return Err(Error::UnknownModule(name.to_string()));
        }
        let instance = factory();
        let inputs: BTreeSet<String> = instance
            .input_tables()
            .iter()
            .map(|t| t.to_ascii_lowercase())
            .collect();
        let outputs: BTreeSet<String> = instance
            .output_tables()
            .iter()
            .map(|t| t.to_ascii_lowercase())
            .collect();
        self.module_input_tables.extend(inputs.iter().cloned());
        self.module_inputs.insert(name.to_string(), inputs);
        self.module_outputs.insert(name.to_string(), outputs);
        self.factories
            .insert(name.to_string(), ModuleFactory::Generator(factory));
        Ok(())
    }

    /// The set of slow-store table names (lowercased) that at least one
    /// registered calculator or generator declares in its `input_tables()`.
    ///
    /// Use this to filter a snapshot load so only tables that any registered
    /// module actually needs are materialised in memory — tables consumed
    /// exclusively by unregistered (not-yet-ported) calculators are skipped,
    /// reducing peak RSS.
    ///
    /// Returns an empty set when no factories have been registered (e.g.
    /// when running without a snapshot).
    #[must_use]
    pub fn required_input_tables(&self) -> BTreeSet<String> {
        self.module_input_tables.clone()
    }

    /// Union of every registered calculator's
    /// [`replaced_pollutants`](crate::Calculator::replaced_pollutants) — the
    /// pollutants a chained calculator consumes and replaces (e.g. SulfatePM's
    /// EC 112 / NonECPM 118), whose zero-valued producer rows the engine drops.
    ///
    /// Exposed so the canonical-snapshot regression gate can assert canonical
    /// never emits a zero row for one of these (the premise that makes the drop
    /// safe). Empty when no factories are registered.
    #[must_use]
    pub fn replaced_pollutants(&self) -> &BTreeSet<i32> {
        &self.replaced_pollutants
    }

    /// The (lowercased) scratch input tables a registered module reads, or
    /// `None` if the module has no registered factory. The consumer side of
    /// the producer→consumer table-dependency edges used by chunking.
    #[must_use]
    pub fn module_input_tables_of(&self, name: &str) -> Option<&BTreeSet<String>> {
        self.module_inputs.get(name)
    }

    /// The (lowercased) scratch output tables a registered module produces, or
    /// `None` if the module has no registered factory. Calculators produce no
    /// scratch tables, so their set is empty; generators list their
    /// [`Generator::output_tables`].
    #[must_use]
    pub fn module_output_tables_of(&self, name: &str) -> Option<&BTreeSet<String>> {
        self.module_outputs.get(name)
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
        // plus the roots driving them and every step in those roots'
        // chain templates.
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
        // similar) whose subscription targets a selected process.
        // process_id == 0 from JavaSource fallback means "we don't know
        // which process" — we conservatively include the module if any
        // process is selected, matching the Java fallback semantics.
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

    /// The set of NONROAD-only module names: every module the MOVES NONROAD
    /// model owns, which must **not** run for a RunSpec that does not select
    /// the NONROAD model.
    ///
    /// The NONROAD emission processes (1, 15, 18–21, 30–32) share the
    /// process-ID namespace with onroad, so the bare `(pollutant, process)`
    /// filter in [`modules_for_runspec`](Self::modules_for_runspec) pulls
    /// `NonroadEmissionCalculator`
    /// (and its NONROAD-only chained downstream — `NRHCSpeciationCalculator`,
    /// `NRAirToxicsCalculator`) into the plan for an onroad-only run. Canonical
    /// MOVES gates these on the model selection (`Models.evaluateModels`);
    /// [`execution_order_for_models`](Self::execution_order_for_models) drops
    /// this set when NONROAD is not selected.
    ///
    /// The set is computed from the DAG, not hard-coded: a module is in it iff
    /// its Java source lives under the `.../master/nonroad/` package (only
    /// `NonroadEmissionCalculator` does), together with the transitive
    /// `chained_downstream` closure of every such module (the NR speciation /
    /// air-toxics calculators, whose only upstream is the nonroad calculator).
    #[must_use]
    pub fn nonroad_only_modules(&self) -> BTreeSet<String> {
        let mut set: BTreeSet<String> = BTreeSet::new();
        for m in &self.dag.modules {
            let is_nonroad_pkg = m
                .java_path
                .as_deref()
                .is_some_and(|p| p.replace('\\', "/").contains("/nonroad/"));
            if is_nonroad_pkg {
                set.insert(m.name.clone());
                for d in &m.chained_downstream {
                    set.insert(d.clone());
                }
            }
        }
        set
    }

    /// The `OpModeDistribution`-producing generators that must **not** run for
    /// the given domain/scale.
    ///
    /// Three generators — `OperatingModeDistributionGenerator`,
    /// `LinkOperatingModeDistributionGenerator` and
    /// `MesoscaleLookupOperatingModeDistributionGenerator` — all produce the
    /// `OpModeDistribution` execution table and all subscribe to Running Exhaust
    /// (process 1) + Brakewear (process 9). Canonical MOVES instantiates exactly
    /// **one** of them, chosen by domain/scale
    /// (`MOVESInstantiator.instantiate`, the `M1` swap):
    ///
    /// * Project domain                → `LinkOperatingModeDistributionGenerator`
    /// * Mesoscale-Lookup (Rates) scale → `MesoscaleLookupOperatingModeDistributionGenerator`
    /// * otherwise (Inventory)          → `OperatingModeDistributionGenerator`
    ///
    /// The bare `(pollutant, process)` filter in
    /// [`modules_for_runspec`](Self::modules_for_runspec) cannot make this choice
    /// (all three share processes 1+9), so it over-selects all three. They then
    /// collide on the single `OpModeDistribution` scratch name — the no-`linkID`
    /// Mesoscale variant clobbering the link-keyed one, which panics downstream
    /// with `OpModeDistribution.linkID not found`. This returns the two variants
    /// to drop so only the canonical producer remains, mirroring
    /// [`nonroad_only_modules`](Self::nonroad_only_modules) on the model axis.
    #[must_use]
    pub fn domain_scale_excluded_omd_modules(
        &self,
        is_project: bool,
        is_mesoscale: bool,
    ) -> BTreeSet<String> {
        const STANDARD: &str = "OperatingModeDistributionGenerator";
        const LINK: &str = "LinkOperatingModeDistributionGenerator";
        const MESOSCALE: &str = "MesoscaleLookupOperatingModeDistributionGenerator";
        // Project takes precedence over scale, matching the order of the
        // `isProjectDomain` / `isMesoscaleLookup` blocks in MOVESInstantiator.
        let keep = if is_project {
            LINK
        } else if is_mesoscale {
            MESOSCALE
        } else {
            STANDARD
        };
        [STANDARD, LINK, MESOSCALE]
            .into_iter()
            .filter(|&n| n != keep)
            .map(String::from)
            .collect()
    }

    /// The selected modules to drop under canonical `MOVESInstantiator`
    /// `DO_RATES_FIRST` (the released-MOVES default, `CompilationFlags
    /// .DO_RATES_FIRST = true`): every selected **emission calculator** that is
    /// not on the rates-first keep-list. Canonical clears `neededClassNames` and
    /// re-adds only `BaseRateCalculator` plus a chained-calculator whitelist;
    /// the legacy inventory calculators (Basic*PM, Criteria*, NH3*, CH4N2O*,
    /// …) are never instantiated. Mirroring that here prevents the default-DB
    /// path — which carries both the rates and inventory execution tables —
    /// from running both pipelines and double-counting.
    ///
    /// Only **calculators** are considered: generators are left untouched (the
    /// port's BaseRate pipeline still consumes the inventory OperatingMode
    /// distribution generator's output; the RatesOMD swap is modeled separately
    /// by [`domain_scale_excluded_omd_modules`](Self::domain_scale_excluded_omd_modules)).
    pub fn rates_first_excluded_calculators(&self, selected: &[String]) -> BTreeSet<String> {
        // BaseRateCalculator + the MOVESInstantiator DO_RATES_FIRST whitelist
        // (chained calculators), mapped to the port's calculator names. This is
        // a faithful 1:1 mirror of the canonical `whiteList[]` in
        // `MOVESInstantiator.generateExecutionGraph` (the `neededClasses` array
        // adds `BaseRateCalculator`; the `whiteList` adds the rest). Keep this
        // list in lockstep with that array — notably it INCLUDES
        // `EvaporativePermeationCalculator` and `NonroadEmissionCalculator` (so
        // the evap-permeation and NONROAD pipelines survive rates-first), and it
        // EXCLUDES `AirToxicsDistanceCalculator` (canonical does not whitelist
        // it, so the per-distance air-toxics calculator is dropped under
        // rates-first).
        const KEEP: &[&str] = &[
            "BaseRateCalculator",
            "NonroadEmissionCalculator",
            "ActivityCalculator",
            "AirToxicsCalculator",
            "DistanceCalculator",
            "CO2AERunningStartExtendedIdleCalculator",
            "CrankcaseEmissionCalculatorNonPM",
            "EvaporativePermeationCalculator",
            "HCSpeciationCalculator",
            "LiquidLeakingCalculator",
            "NOCalculator",
            "NO2Calculator",
            "PM10BrakeTireCalculator",
            "PM10EmissionCalculator",
            "RefuelingLossCalculator",
            "SO2Calculator",
            "SulfatePMCalculator",
            "TankVaporVentingCalculator",
            "TOGSpeciationCalculator",
        ];
        selected
            .iter()
            .filter(|n| {
                !KEEP.contains(&n.as_str())
                    // A calculator (not a generator): generators are kept so the
                    // BaseRate pipeline's inputs are still produced.
                    && self.instantiate_calculator(n).is_some()
            })
            .cloned()
            .collect()
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

    /// Like [`execution_order_for_runspec`](Self::execution_order_for_runspec)
    /// but gated on the RunSpec's model selection.
    ///
    /// When `nonroad` is `false`, every module in
    /// [`nonroad_only_modules`](Self::nonroad_only_modules) is dropped from the
    /// plan before topological ordering — so an onroad-only RunSpec never runs
    /// `NonroadEmissionCalculator` (which would otherwise emit a fixed block of
    /// NONROAD-coded `MOVESOutput` rows against the `nr*` execution-DB tables
    /// present in every snapshot, regardless of the onroad RunSpec). This
    /// mirrors canonical MOVES, where the NONROAD calculator chain only
    /// subscribes when the NONROAD model is selected.
    ///
    /// `onroad` is accepted for symmetry and forward use; it does not currently
    /// drop any modules (the onroad calculators live in the shared `ghg`
    /// package and are filtered by `(pollutant, process)` alone).
    pub fn execution_order_for_models(
        &self,
        selections: &[(PollutantId, ProcessId)],
        onroad: bool,
        nonroad: bool,
    ) -> Result<Vec<String>> {
        let _ = onroad;
        let mut names = self.modules_for_runspec(selections);
        if !nonroad {
            let drop = self.nonroad_only_modules();
            names.retain(|n| !drop.contains(n));
        }
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
        build_dag, parse_calculator_info_str, CalculatorInfo, Granularity, JavaSubscription,
        Priority, SubscribeStyle,
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
            _ctx: &mut crate::calculator::CalculatorContext,
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
        // which may override). Instead, instantiate_generator
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

    /// DAG where an onroad calculator and a NONROAD calculator both register
    /// for the same shared `(pollutant, process)` pair, and the NONROAD
    /// calculator drives a chained NONROAD downstream. The NONROAD calculator's
    /// Java source lives under `.../master/nonroad/`, which is how
    /// `nonroad_only_modules` discriminates it from the shared `ghg` package.
    fn shared_process_onroad_nonroad_dag() -> CalculatorDag {
        let info = parse(
            "Registration\tCO\t2\tRunning Exhaust\t1\tBaseRateCalculator\n\
             Registration\tCO\t2\tRunning Exhaust\t1\tNonroadEmissionCalculator\n\
             Registration\tCO\t2\tRunning Exhaust\t1\tNRHCSpeciationCalculator\n\
             Subscribe\tBaseRateCalculator\tRunning Exhaust\t1\tMONTH\tEMISSION_CALCULATOR\n\
             Subscribe\tNonroadEmissionCalculator\tRunning Exhaust\t1\tDAY\tEMISSION_CALCULATOR\n\
             Chain\tNRHCSpeciationCalculator\tNonroadEmissionCalculator\n",
        );
        // The source-dir scan supplies the Java package path; only the nonroad
        // calculator lives under `.../master/nonroad/`.
        let java = vec![
            JavaSubscription {
                calculator: "BaseRateCalculator".to_string(),
                java_path: std::path::PathBuf::from(
                    "gov/epa/otaq/moves/master/implementation/ghg/BaseRateCalculator.java",
                ),
                style: SubscribeStyle::Explicit,
                granularity: Some(Granularity::Month),
                priority: Some(Priority::parse("EMISSION_CALCULATOR").unwrap()),
                process_expr: "process".to_string(),
            },
            JavaSubscription {
                calculator: "NonroadEmissionCalculator".to_string(),
                java_path: std::path::PathBuf::from(
                    "gov/epa/otaq/moves/master/nonroad/NonroadEmissionCalculator.java",
                ),
                style: SubscribeStyle::Explicit,
                granularity: Some(Granularity::Day),
                priority: Some(Priority::parse("EMISSION_CALCULATOR").unwrap()),
                process_expr: "process".to_string(),
            },
        ];
        build_dag(&info, &java).unwrap()
    }

    #[test]
    fn nonroad_only_modules_finds_calc_and_chained_downstream() {
        let reg = CalculatorRegistry::new(shared_process_onroad_nonroad_dag());
        let nr = reg.nonroad_only_modules();
        assert!(
            nr.contains("NonroadEmissionCalculator"),
            "the /nonroad/ packaged calculator must be in the set"
        );
        assert!(
            nr.contains("NRHCSpeciationCalculator"),
            "its chained NONROAD downstream must be in the set"
        );
        assert!(
            !nr.contains("BaseRateCalculator"),
            "the shared-package onroad calculator must not be in the set"
        );
    }

    #[test]
    fn execution_order_for_models_drops_nonroad_when_not_selected() {
        let reg = CalculatorRegistry::new(shared_process_onroad_nonroad_dag());
        let selections = vec![(PollutantId(2), ProcessId(1))];

        // ONROAD only: NONROAD calculator chain must be excluded even though it
        // registered for the selected (pollutant, process) pair.
        let onroad = reg
            .execution_order_for_models(&selections, true, false)
            .unwrap();
        assert!(onroad.contains(&"BaseRateCalculator".to_string()));
        assert!(
            !onroad.contains(&"NonroadEmissionCalculator".to_string()),
            "onroad-only plan must not include NonroadEmissionCalculator"
        );
        assert!(
            !onroad.contains(&"NRHCSpeciationCalculator".to_string()),
            "onroad-only plan must not include the NONROAD chained downstream"
        );

        // NONROAD selected: the NONROAD chain is included.
        let with_nr = reg
            .execution_order_for_models(&selections, true, true)
            .unwrap();
        assert!(with_nr.contains(&"NonroadEmissionCalculator".to_string()));
        assert!(with_nr.contains(&"NRHCSpeciationCalculator".to_string()));
        assert!(with_nr.contains(&"BaseRateCalculator".to_string()));
    }

    #[test]
    fn domain_scale_excluded_omd_modules_keeps_exactly_one_producer() {
        // The method returns the OMD producers to DROP; the swap rule itself is
        // DAG-independent, so any registry works.
        let reg = CalculatorRegistry::new(single_calc_dag());
        const STANDARD: &str = "OperatingModeDistributionGenerator";
        const LINK: &str = "LinkOperatingModeDistributionGenerator";
        const MESOSCALE: &str = "MesoscaleLookupOperatingModeDistributionGenerator";

        // Default Inventory: keep STANDARD, drop LINK + MESOSCALE.
        let inv = reg.domain_scale_excluded_omd_modules(false, false);
        assert!(!inv.contains(STANDARD), "Inventory keeps the standard OMDG");
        assert!(inv.contains(LINK) && inv.contains(MESOSCALE));

        // Project domain: keep LINK, drop STANDARD + MESOSCALE.
        let proj = reg.domain_scale_excluded_omd_modules(true, false);
        assert!(!proj.contains(LINK), "Project keeps the link OMDG");
        assert!(proj.contains(STANDARD) && proj.contains(MESOSCALE));

        // Mesoscale-Lookup (Rates): keep MESOSCALE, drop STANDARD + LINK.
        let meso = reg.domain_scale_excluded_omd_modules(false, true);
        assert!(!meso.contains(MESOSCALE), "Mesoscale keeps the mesoscale OMDG");
        assert!(meso.contains(STANDARD) && meso.contains(LINK));

        // Project takes precedence over scale (matches MOVESInstantiator order).
        let both = reg.domain_scale_excluded_omd_modules(true, true);
        assert!(!both.contains(LINK), "Project wins over Mesoscale");
        assert!(both.contains(STANDARD) && both.contains(MESOSCALE));
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
    fn real_dag_over_selects_all_three_omd_producers_and_swap_keeps_one() {
        // Against the real calculator DAG: the three OpModeDistribution
        // producers all carry the `process_id == 0` JavaSource fallback, so the
        // `(pollutant, process)` filter pulls in ALL THREE for any selection —
        // they then collide on the single `OpModeDistribution` scratch table.
        // The domain/scale swap must leave exactly the canonical one.
        let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
        let fixture = manifest
            .parent()
            .and_then(Path::parent)
            .map(|root| root.join("characterization/calculator-chains/calculator-dag.json"))
            .expect("workspace root reachable from manifest dir");
        let reg = CalculatorRegistry::load_from_json(&fixture)
            .unwrap_or_else(|e| panic!("loading calculator-dag.json failed: {e:?}"));

        const STANDARD: &str = "OperatingModeDistributionGenerator";
        const LINK: &str = "LinkOperatingModeDistributionGenerator";
        const MESOSCALE: &str = "MesoscaleLookupOperatingModeDistributionGenerator";

        // Running Exhaust (1) + Brakewear (9) — the onroad-inventory demo's set.
        let selections = vec![(PollutantId(2), ProcessId(1)), (PollutantId(2), ProcessId(9))];
        let planned = reg.modules_for_runspec(&selections);
        for n in [STANDARD, LINK, MESOSCALE] {
            assert!(
                planned.contains(&n.to_string()),
                "the raw (pollutant, process) filter should over-select {n}"
            );
        }

        // Inventory/Default swap: drop LINK + MESOSCALE, keep STANDARD.
        let drop = reg.domain_scale_excluded_omd_modules(false, false);
        let kept: Vec<&String> = planned
            .iter()
            .filter(|n| !drop.contains(*n))
            .filter(|n| [STANDARD, LINK, MESOSCALE].contains(&n.as_str()))
            .collect();
        assert_eq!(
            kept,
            vec![&STANDARD.to_string()],
            "exactly one OMD producer survives the Inventory/Default swap"
        );
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

    // ---- required_input_tables tests ----

    // Stubs that declare named input tables so required_input_tables can be tested.
    #[derive(Debug, Default)]
    struct CalcWithTables;
    static CALC_TABLES: &[&str] = &["emissionRate", "SomeTable"];
    impl Calculator for CalcWithTables {
        fn name(&self) -> &'static str {
            "BaseRateCalculator"
        }
        fn subscriptions(&self) -> &[crate::calculator::CalculatorSubscription] {
            &[]
        }
        fn registrations(&self) -> &[moves_data::PollutantProcessAssociation] {
            &[]
        }
        fn input_tables(&self) -> &[&'static str] {
            CALC_TABLES
        }
        fn execute(
            &self,
            _ctx: &crate::calculator::CalculatorContext,
        ) -> std::result::Result<crate::calculator::CalculatorOutput, Error> {
            Ok(crate::calculator::CalculatorOutput::empty())
        }
    }
    fn calc_with_tables() -> Box<dyn Calculator> {
        Box::new(CalcWithTables)
    }

    #[derive(Debug, Default)]
    struct GenWithTables;
    static GEN_TABLES: &[&str] = &["GenTable", "SharedTable"];
    impl Generator for GenWithTables {
        fn name(&self) -> &'static str {
            "HCSpeciationCalculator"
        }
        fn subscriptions(&self) -> &[crate::calculator::CalculatorSubscription] {
            &[]
        }
        fn input_tables(&self) -> &[&'static str] {
            GEN_TABLES
        }
        fn execute(
            &self,
            _ctx: &mut crate::calculator::CalculatorContext,
        ) -> std::result::Result<crate::calculator::CalculatorOutput, Error> {
            Ok(crate::calculator::CalculatorOutput::empty())
        }
    }
    fn gen_with_tables() -> Box<dyn Generator> {
        Box::new(GenWithTables)
    }

    #[test]
    fn required_input_tables_empty_when_nothing_registered() {
        let reg = CalculatorRegistry::new(single_calc_dag());
        assert!(
            reg.required_input_tables().is_empty(),
            "no factories → no required tables"
        );
    }

    #[test]
    fn required_input_tables_union_of_registered_modules() {
        let mut reg = CalculatorRegistry::new(chained_calc_dag());
        reg.register_calculator("BaseRateCalculator", calc_with_tables)
            .unwrap();
        reg.register_generator("HCSpeciationCalculator", gen_with_tables)
            .unwrap();
        let tables = reg.required_input_tables();
        // CalcWithTables: "emissionRate" → "emissionrate", "SomeTable" → "sometable"
        // GenWithTables: "GenTable" → "gentable", "SharedTable" → "sharedtable"
        assert!(
            tables.contains("emissionrate"),
            "calc tables must be present"
        );
        assert!(tables.contains("sometable"), "calc tables must be present");
        assert!(tables.contains("gentable"), "gen tables must be present");
        assert!(tables.contains("sharedtable"), "gen tables must be present");
        assert_eq!(tables.len(), 4, "union has exactly 4 distinct names");
    }

    #[test]
    fn required_input_tables_names_are_lowercased() {
        let mut reg = CalculatorRegistry::new(single_calc_dag());
        reg.register_calculator("BaseRateCalculator", calc_with_tables)
            .unwrap();
        let tables = reg.required_input_tables();
        // CalcWithTables returns "emissionRate" and "SomeTable"; both must be stored lowercase.
        assert!(
            tables.contains("emissionrate"),
            "mixed-case name must be lowercased"
        );
        assert!(
            !tables.contains("emissionRate"),
            "original-case name must not appear"
        );
    }

    #[test]
    fn required_input_tables_adding_new_calculator_auto_includes_its_tables() {
        let mut reg = CalculatorRegistry::new(chained_calc_dag());
        // Start with an empty set.
        assert!(reg.required_input_tables().is_empty());
        // Register one calculator — its tables should appear.
        reg.register_calculator("BaseRateCalculator", calc_with_tables)
            .unwrap();
        let after_first = reg.required_input_tables();
        assert!(
            after_first.contains("emissionrate"),
            "tables from newly registered calculator must appear"
        );
        // Register another — its tables should be added too.
        reg.register_generator("HCSpeciationCalculator", gen_with_tables)
            .unwrap();
        let after_second = reg.required_input_tables();
        assert!(after_second.contains("emissionrate"));
        assert!(after_second.contains("gentable"));
    }
}
