//! DAG construction from parsed directives + optional Java subscriptions.
//!
//! The output ([`CalculatorDag`]) is the artifact Phase 2 Task 19
//! (`CalculatorRegistry`) consumes. Three views are baked in so consumers
//! don't have to reconstruct them:
//!
//! 1. **Modules table** — one entry per calculator/generator, with kind,
//!    subscription style, declared granularity + priority, registration
//!    fanout, and chain dependencies in both directions.
//! 2. **Chain edges** — the raw `Chain` directives as-parsed, plus a per-
//!    module transitive `chained_downstream` closure so consumers can ask
//!    "what fires when X fires?" without recomputing.
//! 3. **Execution chains per (process, pollutant)** — for every
//!    `(process_id, pollutant_id)` that any calculator registers for, the
//!    list of registered calculators. Phase 2 can then walk the chain
//!    closure from each root subscriber to determine which downstream
//!    calculators fire alongside.
//!
//! Determinism: every list in the output is in a stable order
//! (lexicographic on names, numeric on ids). Two runs over the same
//! `CalculatorInfo.txt` produce a byte-identical JSON document.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::directives::{CalculatorInfo, ChainDirective};
use crate::error::{Error, Result};
use crate::java::{JavaSubscription, SubscribeStyle};
use crate::loop_meta::{Granularity, Priority};

/// Top-level container. Mirror of the JSON document this crate produces.
///
/// The chain structure for a given root subscriber is the same regardless
/// of which `(process, pollutant)` triggered it — Phase 2's
/// `CalculatorRegistry` instantiates that chain once and replays it. To
/// keep the JSON compact, the full ordered step list is factored into
/// [`ChainTemplate`] entries (keyed by root) and the per-key
/// [`ExecutionChain`] just lists the roots that apply.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CalculatorDag {
    pub schema: String,
    pub source: Source,
    pub counts: DagCounts,
    pub modules: Vec<ModuleEntry>,
    pub chains: Vec<ChainEdge>,
    pub registrations: Vec<RegistrationEntry>,
    pub chain_templates: Vec<ChainTemplate>,
    pub execution_chains: Vec<ExecutionChain>,
    pub global_execution_order: Vec<GlobalExecutionEntry>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Source {
    /// SHA-256 of the parsed `CalculatorInfo.txt`. Lowercase hex.
    pub calculator_info_sha256: String,
    /// Number of Java source files the optional source-dir scan visited.
    /// Zero if the scan was skipped.
    pub java_files_scanned: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DagCounts {
    pub registrations: usize,
    pub subscriptions: usize,
    pub chains: usize,
    pub modules: usize,
    pub direct_subscribers: usize,
    pub chained_only_modules: usize,
    pub unique_process_pollutant_pairs: usize,
}

/// Whether a module is a calculator (produces an emission output) or a
/// generator (produces upstream activity / fuel-effect / opmode data).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModuleKind {
    Calculator,
    Generator,
    Unknown,
}

impl ModuleKind {
    fn from_name(name: &str) -> Self {
        if name.ends_with("Generator") {
            ModuleKind::Generator
        } else if name.ends_with("Calculator") || name.contains("Calculator") {
            ModuleKind::Calculator
        } else {
            ModuleKind::Unknown
        }
    }
}

/// Where a piece of subscription info came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubscriptionSource {
    /// Runtime log `Subscribe` directive — captured during a real MOVES run.
    CalculatorInfo,
    /// Recovered from a `.java` source file under the optional source-dir.
    JavaSource,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModuleEntry {
    pub name: String,
    pub kind: ModuleKind,
    /// True if any source records the module subscribing directly to the
    /// MasterLoop. False if it only participates via chaining or never
    /// hooks the loop at all.
    pub subscribes_directly: bool,
    /// Subscription records, oldest-known-first. Each entry pins the
    /// `(process, granularity, priority)` triple along with the source it
    /// was learned from.
    pub subscriptions: Vec<SubscriptionEntry>,
    /// Total number of `(process, pollutant)` pairs this module registered
    /// to produce. The full list is in the top-level `registrations`
    /// array; this is the cheap-to-read summary.
    pub registrations_count: usize,
    /// Modules whose output this module consumes (upstream).
    pub depends_on: Vec<String>,
    /// Modules that consume this module's output (downstream).
    pub dependents: Vec<String>,
    /// Transitive closure of `dependents`. When this module fires, every
    /// module in this list (in topological order) also fires.
    pub chained_downstream: Vec<String>,
    /// Java source path, if the optional source-dir scan saw it.
    pub java_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionEntry {
    pub process_id: u32,
    pub process_name: String,
    pub granularity: Granularity,
    pub priority: Priority,
    /// Total integer priority (Java's `MasterLoopPriority` value).
    pub priority_value: i32,
    /// Numeric granularity value matching Java's
    /// `MasterLoopGranularity.granularityValue`.
    pub granularity_value: i32,
    /// Execution-order index — smaller fires earlier. Combines granularity
    /// (coarsest first) and priority (highest first) into a single key
    /// downstream sort routines can use. See [`Granularity::execution_index`].
    pub execution_index: ExecutionIndex,
    pub source: SubscriptionSource,
}

/// `(granularity_index, -priority)` pair: lexicographic compare gives the
/// MasterLoop's iteration order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ExecutionIndex {
    pub granularity_index: i32,
    pub neg_priority: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChainEdge {
    pub output: String,
    pub input: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegistrationEntry {
    pub process_id: u32,
    pub process_name: String,
    pub pollutant_id: u32,
    pub pollutant_name: String,
    pub calculator: String,
}

/// Per-`(process, pollutant)` view: who registered to produce it, and which
/// chain template(s) fire to do the work. Sorted by `(process_id,
/// pollutant_id)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionChain {
    pub process_id: u32,
    pub process_name: String,
    pub pollutant_id: u32,
    pub pollutant_name: String,
    /// Calculators that registered to produce this `(process, pollutant)`,
    /// sorted alphabetically.
    pub registered_calculators: Vec<String>,
    /// Root subscribers whose chain templates fire for this key. Sorted
    /// in MasterLoop firing order: coarsest granularity first, then
    /// highest priority. Each name is also a key in
    /// [`CalculatorDag::chain_templates`].
    pub roots: Vec<String>,
}

/// One-per-root-subscriber chain template. The `steps` field is the full
/// topological order of modules that fire when the root fires; chained
/// calculators inherit the root's (granularity, priority).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChainTemplate {
    /// The direct-subscriber that drives this chain. Unique within the
    /// `chain_templates` array.
    pub root: String,
    /// The (granularity, priority) the root subscribes at. `None` for
    /// roots whose subscription metadata we don't have (e.g. Java-source
    /// scan saw the class but didn't find a usable subscribe call).
    pub granularity: Option<Granularity>,
    pub priority: Option<Priority>,
    /// Topological order, starting at `root`. Each step is a module that
    /// fires when the root fires.
    pub steps: Vec<ChainStep>,
}

/// One module-firing event inside a [`ChainTemplate`]. Granularity and
/// priority are not repeated per step — they're on the parent template.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChainStep {
    pub module: String,
    pub role: ChainRole,
    /// For chained steps, the parent in the chain tree (the module that
    /// directly triggers this step). `None` for the root.
    pub triggered_by: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChainRole {
    Subscriber,
    Chained,
}

/// One row of the `global_execution_order` table. Sorted by
/// `execution_index` ascending — i.e. MasterLoop firing order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GlobalExecutionEntry {
    pub module: String,
    pub process_id: u32,
    pub process_name: String,
    pub granularity: Granularity,
    pub priority: Priority,
    pub execution_index: ExecutionIndex,
}

/// Build a complete DAG from parsed `CalculatorInfo.txt` plus an optional
/// fallback set of Java-source subscriptions. The Java set is consulted only
/// for modules that the runtime log doesn't mention — runtime data wins on
/// conflict because it reflects RunSpec-derived gating the static scanner
/// can't see.
pub fn build_dag(
    info: &CalculatorInfo,
    java_subscriptions: &[JavaSubscription],
) -> Result<CalculatorDag> {
    let module_set = collect_module_set(info, java_subscriptions);
    let chains_in: BTreeMap<String, BTreeSet<String>> = group_chains_inputs(&info.chains);
    let chains_out: BTreeMap<String, BTreeSet<String>> = group_chains_outputs(&info.chains);

    // 1. Modules table.
    let mut modules: Vec<ModuleEntry> = Vec::new();
    for name in &module_set {
        let subscriptions = build_subscription_entries(name, info, java_subscriptions);
        let subscribes_directly = !subscriptions.is_empty();
        let registrations_count = info
            .registrations
            .iter()
            .filter(|r| &r.calculator == name)
            .count();
        let depends_on: Vec<String> = chains_out
            .get(name)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default();
        let dependents: Vec<String> = chains_in
            .get(name)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default();
        let java_path = java_subscriptions
            .iter()
            .find(|s| &s.calculator == name && !s.java_path.as_os_str().is_empty())
            .map(|s| s.java_path.to_string_lossy().into_owned());
        let kind = ModuleKind::from_name(name);
        modules.push(ModuleEntry {
            name: name.clone(),
            kind,
            subscribes_directly,
            subscriptions,
            registrations_count,
            depends_on,
            dependents,
            chained_downstream: Vec::new(), // filled below
            java_path,
        });
    }

    // 2. Transitive downstream closure.
    fill_chained_downstream(&mut modules, &chains_in);

    // 3. Chain edges (verbatim copy of directives, deduplicated).
    let mut chain_edges: BTreeSet<(String, String)> = BTreeSet::new();
    for c in &info.chains {
        chain_edges.insert((c.output.clone(), c.input.clone()));
    }
    let chains: Vec<ChainEdge> = chain_edges
        .into_iter()
        .map(|(output, input)| ChainEdge { output, input })
        .collect();

    // 4. Registrations sorted on (process_id, pollutant_id, calculator).
    // BTreeMap keyed by the sort tuple deduplicates raw directives that
    // recorded the same (process, pollutant, calculator) more than once.
    let mut registration_map: BTreeMap<(u32, u32, String), RegistrationEntry> = BTreeMap::new();
    for r in &info.registrations {
        registration_map
            .entry((r.process_id, r.pollutant_id, r.calculator.clone()))
            .or_insert(RegistrationEntry {
                process_id: r.process_id,
                process_name: r.process_name.clone(),
                pollutant_id: r.pollutant_id,
                pollutant_name: r.pollutant_name.clone(),
                calculator: r.calculator.clone(),
            });
    }
    let registrations: Vec<RegistrationEntry> = registration_map.into_values().collect();

    // 5. Per-(process, pollutant) execution chains, plus the deduplicated
    //    chain templates they reference.
    let module_index: BTreeMap<String, &ModuleEntry> =
        modules.iter().map(|m| (m.name.clone(), m)).collect();
    let (chain_templates, execution_chains) =
        build_execution_chains(&registrations, &module_index, &chains_out, &chains_in, info)?;

    // 6. Global execution order over all subscribers.
    let global_execution_order = build_global_execution_order(&modules);

    // 7. Counts.
    let direct_subscribers = modules.iter().filter(|m| m.subscribes_directly).count();
    let chained_only_modules = modules
        .iter()
        .filter(|m| {
            !m.subscribes_directly
                && (chains_out.contains_key(&m.name) || chains_in.contains_key(&m.name))
        })
        .count();
    let unique_process_pollutant_pairs = execution_chains.len();
    let counts = DagCounts {
        registrations: info.registrations.len(),
        subscriptions: info.subscribes.len(),
        chains: info.chains.len(),
        modules: modules.len(),
        direct_subscribers,
        chained_only_modules,
        unique_process_pollutant_pairs,
    };

    let java_files_scanned = java_subscriptions
        .iter()
        .map(|s| s.java_path.clone())
        .collect::<BTreeSet<_>>()
        .len();
    Ok(CalculatorDag {
        schema: crate::output::DAG_VERSION.to_string(),
        source: Source {
            calculator_info_sha256: info.source_sha256.clone(),
            java_files_scanned,
        },
        counts,
        modules,
        chains,
        registrations,
        chain_templates,
        execution_chains,
        global_execution_order,
    })
}

/// Union of module names seen across registrations, subscribes, chains,
/// and (optionally) the Java scan.
fn collect_module_set(
    info: &CalculatorInfo,
    java_subscriptions: &[JavaSubscription],
) -> BTreeSet<String> {
    let mut set: BTreeSet<String> = BTreeSet::new();
    for r in &info.registrations {
        set.insert(r.calculator.clone());
    }
    for s in &info.subscribes {
        set.insert(s.module.clone());
    }
    for c in &info.chains {
        set.insert(c.output.clone());
        set.insert(c.input.clone());
    }
    for j in java_subscriptions {
        set.insert(j.calculator.clone());
    }
    set
}

/// `Chain output input` — the *output* depends on the *input*. So
/// `chains_out[output] = { inputs }` lists upstream producers.
fn group_chains_outputs(chains: &[ChainDirective]) -> BTreeMap<String, BTreeSet<String>> {
    let mut map: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for c in chains {
        map.entry(c.output.clone())
            .or_default()
            .insert(c.input.clone());
    }
    map
}

/// `chains_in[input] = { outputs }` lists downstream consumers.
fn group_chains_inputs(chains: &[ChainDirective]) -> BTreeMap<String, BTreeSet<String>> {
    let mut map: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for c in chains {
        map.entry(c.input.clone())
            .or_default()
            .insert(c.output.clone());
    }
    map
}

fn build_subscription_entries(
    module: &str,
    info: &CalculatorInfo,
    java_subscriptions: &[JavaSubscription],
) -> Vec<SubscriptionEntry> {
    let mut seen: BTreeSet<(u32, Granularity, Priority)> = BTreeSet::new();
    let mut out: Vec<SubscriptionEntry> = Vec::new();
    for s in &info.subscribes {
        if s.module != module {
            continue;
        }
        let key = (s.process_id, s.granularity, s.priority);
        if seen.insert(key) {
            out.push(subscription_entry(
                s.process_id,
                &s.process_name,
                s.granularity,
                s.priority,
                SubscriptionSource::CalculatorInfo,
            ));
        }
    }
    // Java-source fallback: only consulted when the runtime log didn't
    // mention this module.
    if out.is_empty() {
        for j in java_subscriptions
            .iter()
            .filter(|j| j.calculator == module && j.style != SubscribeStyle::ChainedOnly)
        {
            if let (Some(g), Some(p)) = (j.granularity, j.priority) {
                // For GenericBase style the constructor declares ONE
                // granularity that covers every process the calc handles;
                // we can't know the per-process breakdown without a deeper
                // scan, so we emit a single entry with process_id=0 as a
                // sentinel for "all registered processes."
                let (pid, pname): (u32, &str) = match j.style {
                    SubscribeStyle::GenericBase => (0, ""),
                    _ => (0, j.process_expr.as_str()),
                };
                let key = (pid, g, p);
                if seen.insert(key) {
                    out.push(subscription_entry(
                        pid,
                        pname,
                        g,
                        p,
                        SubscriptionSource::JavaSource,
                    ));
                }
            }
        }
    }
    out.sort_by(|a, b| {
        a.execution_index
            .cmp(&b.execution_index)
            .then_with(|| a.process_id.cmp(&b.process_id))
            .then_with(|| a.granularity.as_str().cmp(b.granularity.as_str()))
            .then_with(|| a.priority.value().cmp(&b.priority.value()))
    });
    out
}

fn subscription_entry(
    process_id: u32,
    process_name: &str,
    granularity: Granularity,
    priority: Priority,
    source: SubscriptionSource,
) -> SubscriptionEntry {
    SubscriptionEntry {
        process_id,
        process_name: process_name.to_string(),
        granularity,
        priority,
        priority_value: priority.value(),
        granularity_value: granularity.granularity_value(),
        execution_index: ExecutionIndex {
            granularity_index: granularity.execution_index(),
            neg_priority: -priority.value(),
        },
        source,
    }
}

fn fill_chained_downstream(
    modules: &mut [ModuleEntry],
    chains_in: &BTreeMap<String, BTreeSet<String>>,
) {
    for module in modules.iter_mut() {
        let closure = transitive_closure(&module.name, chains_in);
        module.chained_downstream = closure;
    }
}

fn transitive_closure(start: &str, chains_in: &BTreeMap<String, BTreeSet<String>>) -> Vec<String> {
    let mut visited: BTreeSet<String> = BTreeSet::new();
    let mut queue: Vec<String> = chains_in
        .get(start)
        .map(|s| s.iter().cloned().collect())
        .unwrap_or_default();
    while let Some(n) = queue.pop() {
        if !visited.insert(n.clone()) {
            continue;
        }
        if let Some(next) = chains_in.get(&n) {
            for m in next {
                if !visited.contains(m) {
                    queue.push(m.clone());
                }
            }
        }
    }
    let mut out: Vec<String> = visited.into_iter().collect();
    out.sort();
    out
}

#[allow(clippy::too_many_arguments)]
fn build_execution_chains(
    registrations: &[RegistrationEntry],
    module_index: &BTreeMap<String, &ModuleEntry>,
    chains_out: &BTreeMap<String, BTreeSet<String>>,
    chains_in: &BTreeMap<String, BTreeSet<String>>,
    _info: &CalculatorInfo,
) -> Result<(Vec<ChainTemplate>, Vec<ExecutionChain>)> {
    // Phase A: for every registered calculator, walk up to its root
    // subscriber(s). Phase B: rebuild every distinct root's chain steps
    // once (cached). Phase C: emit one ExecutionChain per (process,
    // pollutant), referencing roots by name.

    // Pre-compute root chains in a deterministic order.
    let mut templates: BTreeMap<String, ChainTemplate> = BTreeMap::new();
    let mut roots_for_registered: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut all_registered: BTreeSet<String> = BTreeSet::new();
    for r in registrations {
        all_registered.insert(r.calculator.clone());
    }
    for reg in &all_registered {
        let module = module_index
            .get(reg)
            .ok_or_else(|| Error::DagBuild(format!("missing module entry: {reg}")))?;
        let roots: Vec<String> = if module.subscribes_directly {
            vec![reg.clone()]
        } else {
            find_subscriber_roots(reg, module_index, chains_out)
        };
        roots_for_registered.insert(reg.clone(), roots.clone());
        for root in roots {
            if templates.contains_key(&root) {
                continue;
            }
            let template = build_chain_template(&root, module_index, chains_in)?;
            templates.insert(root, template);
        }
    }

    // Group registrations by (process_id, pollutant_id).
    let mut grouped: BTreeMap<(u32, u32), (String, String, BTreeSet<String>)> = BTreeMap::new();
    for r in registrations {
        let entry = grouped.entry((r.process_id, r.pollutant_id)).or_insert((
            r.process_name.clone(),
            r.pollutant_name.clone(),
            BTreeSet::new(),
        ));
        entry.2.insert(r.calculator.clone());
    }

    let mut out: Vec<ExecutionChain> = Vec::new();
    for ((process_id, pollutant_id), (process_name, pollutant_name, regs)) in grouped {
        let registered: Vec<String> = regs.iter().cloned().collect();
        // Collect roots across all registered calcs (deduplicated).
        let mut roots_set: BTreeSet<String> = BTreeSet::new();
        for reg in &registered {
            if let Some(rs) = roots_for_registered.get(reg) {
                for r in rs {
                    roots_set.insert(r.clone());
                }
            }
        }
        // Order roots by MasterLoop firing order, then by name as a stable
        // tiebreaker.
        let mut roots: Vec<String> = roots_set.into_iter().collect();
        roots.sort_by(|a, b| {
            let ia = template_execution_index(templates.get(a));
            let ib = template_execution_index(templates.get(b));
            ia.cmp(&ib).then_with(|| a.cmp(b))
        });
        out.push(ExecutionChain {
            process_id,
            process_name,
            pollutant_id,
            pollutant_name,
            registered_calculators: registered,
            roots,
        });
    }
    out.sort_by(|a, b| {
        a.process_id
            .cmp(&b.process_id)
            .then_with(|| a.pollutant_id.cmp(&b.pollutant_id))
    });

    let chain_templates: Vec<ChainTemplate> = templates.into_values().collect();
    Ok((chain_templates, out))
}

fn template_execution_index(template: Option<&ChainTemplate>) -> ExecutionIndex {
    match template.and_then(|t| t.granularity.zip(t.priority)) {
        Some((g, p)) => ExecutionIndex {
            granularity_index: g.execution_index(),
            neg_priority: -p.value(),
        },
        // Unknown subscription metadata sorts last.
        None => ExecutionIndex {
            granularity_index: i32::MAX,
            neg_priority: i32::MAX,
        },
    }
}

/// Walk up `chains_out` from `start` to find every direct-subscriber root
/// reachable by following the (output -> input) edges. Returns at least
/// one root if the chain DAG terminates in a direct subscriber; the empty
/// list means the registered calculator has no path to any subscriber
/// (which would indicate a broken DAG or a calculator that registers
/// without ever running — Phase 2 should flag these).
fn find_subscriber_roots(
    start: &str,
    module_index: &BTreeMap<String, &ModuleEntry>,
    chains_out: &BTreeMap<String, BTreeSet<String>>,
) -> Vec<String> {
    let mut roots: BTreeSet<String> = BTreeSet::new();
    let mut visited: BTreeSet<String> = BTreeSet::new();
    let mut queue: Vec<String> = vec![start.to_string()];
    while let Some(n) = queue.pop() {
        if !visited.insert(n.clone()) {
            continue;
        }
        if let Some(m) = module_index.get(&n) {
            if m.subscribes_directly && n != start {
                roots.insert(n);
                continue;
            }
        }
        if let Some(inputs) = chains_out.get(&n) {
            for inp in inputs {
                if !visited.contains(inp) {
                    queue.push(inp.clone());
                }
            }
        }
    }
    roots.into_iter().collect()
}

fn build_chain_template(
    root: &str,
    module_index: &BTreeMap<String, &ModuleEntry>,
    chains_in: &BTreeMap<String, BTreeSet<String>>,
) -> Result<ChainTemplate> {
    let root_module = module_index
        .get(root)
        .ok_or_else(|| Error::DagBuild(format!("missing root module: {root}")))?;
    let root_sub = root_module.subscriptions.first();
    let granularity = root_sub.map(|s| s.granularity);
    let priority = root_sub.map(|s| s.priority);
    let mut steps: Vec<ChainStep> = vec![ChainStep {
        module: root.to_string(),
        role: ChainRole::Subscriber,
        triggered_by: None,
    }];

    // BFS to enumerate the downstream chain rooted at `root`, in topo
    // order (parent fires before child). Parents are recorded so the
    // output captures who triggers whom.
    let mut visited: BTreeSet<String> = BTreeSet::new();
    visited.insert(root.to_string());
    let mut frontier: Vec<String> = vec![root.to_string()];
    while !frontier.is_empty() {
        let mut next_frontier: BTreeSet<String> = BTreeSet::new();
        for parent in &frontier {
            if let Some(children) = chains_in.get(parent) {
                for child in children {
                    if visited.insert(child.clone()) {
                        steps.push(ChainStep {
                            module: child.clone(),
                            role: ChainRole::Chained,
                            triggered_by: Some(parent.clone()),
                        });
                        next_frontier.insert(child.clone());
                    }
                }
            }
        }
        frontier = next_frontier.into_iter().collect();
    }
    Ok(ChainTemplate {
        root: root.to_string(),
        granularity,
        priority,
        steps,
    })
}

fn build_global_execution_order(modules: &[ModuleEntry]) -> Vec<GlobalExecutionEntry> {
    let mut out: Vec<GlobalExecutionEntry> = Vec::new();
    for m in modules {
        for s in &m.subscriptions {
            if s.source == SubscriptionSource::CalculatorInfo {
                out.push(GlobalExecutionEntry {
                    module: m.name.clone(),
                    process_id: s.process_id,
                    process_name: s.process_name.clone(),
                    granularity: s.granularity,
                    priority: s.priority,
                    execution_index: s.execution_index,
                });
            }
        }
    }
    out.sort_by(|a, b| {
        a.execution_index
            .cmp(&b.execution_index)
            .then_with(|| a.module.cmp(&b.module))
            .then_with(|| a.process_id.cmp(&b.process_id))
    });
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::directives::parse_calculator_info_str;
    use std::path::Path;

    fn parse(text: &str) -> CalculatorInfo {
        parse_calculator_info_str(text, Path::new("test")).unwrap()
    }

    #[test]
    fn empty_input_yields_empty_dag() {
        let info = CalculatorInfo::empty();
        let dag = build_dag(&info, &[]).unwrap();
        assert!(dag.modules.is_empty());
        assert!(dag.chains.is_empty());
        assert!(dag.registrations.is_empty());
        assert_eq!(dag.counts.registrations, 0);
    }

    #[test]
    fn single_registration_creates_module() {
        let info = parse(
            "Registration\tCO\t2\tRunning Exhaust\t1\tBaseRateCalculator\n\
             Subscribe\tBaseRateCalculator\tRunning Exhaust\t1\tMONTH\tEMISSION_CALCULATOR\n",
        );
        let dag = build_dag(&info, &[]).unwrap();
        assert_eq!(dag.modules.len(), 1);
        let m = &dag.modules[0];
        assert_eq!(m.name, "BaseRateCalculator");
        assert!(m.subscribes_directly);
        assert_eq!(m.subscriptions.len(), 1);
        assert_eq!(m.subscriptions[0].granularity, Granularity::Month);
        assert_eq!(m.registrations_count, 1);
    }

    #[test]
    fn chain_directives_populate_depends_on_and_dependents() {
        let info = parse(
            "Subscribe\tBaseRateCalculator\tRunning Exhaust\t1\tMONTH\tEMISSION_CALCULATOR\n\
             Chain\tHCSpeciationCalculator\tBaseRateCalculator\n",
        );
        let dag = build_dag(&info, &[]).unwrap();
        let by_name: BTreeMap<_, _> = dag.modules.iter().map(|m| (m.name.as_str(), m)).collect();
        let base = by_name["BaseRateCalculator"];
        let hc = by_name["HCSpeciationCalculator"];
        assert_eq!(hc.depends_on, vec!["BaseRateCalculator"]);
        assert_eq!(base.dependents, vec!["HCSpeciationCalculator"]);
        assert_eq!(base.chained_downstream, vec!["HCSpeciationCalculator"]);
        // HCSpeciation has no direct subscriber line — so it's chained-only.
        assert!(!hc.subscribes_directly);
    }

    #[test]
    fn transitive_chained_downstream() {
        // Chain A → B → C; A subscribes; B and C do not.
        let info = parse(
            "Subscribe\tA\tRunning Exhaust\t1\tMONTH\tEMISSION_CALCULATOR\n\
             Chain\tB\tA\n\
             Chain\tC\tB\n",
        );
        let dag = build_dag(&info, &[]).unwrap();
        let by_name: BTreeMap<_, _> = dag.modules.iter().map(|m| (m.name.as_str(), m)).collect();
        assert_eq!(by_name["A"].chained_downstream, vec!["B", "C"]);
        assert_eq!(by_name["B"].chained_downstream, vec!["C"]);
        assert!(by_name["C"].chained_downstream.is_empty());
    }

    #[test]
    fn execution_chain_walks_from_root_subscriber() {
        // Two registrations for (1, 2): a direct subscriber (Root) and a
        // chained child (Leaf) reachable via Root.
        let info = parse(
            "Registration\tCO\t2\tRunning Exhaust\t1\tRoot\n\
             Registration\tCO\t2\tRunning Exhaust\t1\tLeaf\n\
             Subscribe\tRoot\tRunning Exhaust\t1\tMONTH\tEMISSION_CALCULATOR\n\
             Chain\tLeaf\tRoot\n",
        );
        let dag = build_dag(&info, &[]).unwrap();
        assert_eq!(dag.execution_chains.len(), 1);
        let ec = &dag.execution_chains[0];
        assert_eq!(ec.process_id, 1);
        assert_eq!(ec.pollutant_id, 2);
        assert_eq!(ec.registered_calculators, vec!["Leaf", "Root"]);
        // Both registered calcs resolve to Root, so a single root drives
        // this (process, pollutant).
        assert_eq!(ec.roots, vec!["Root"]);
        // The matching template captures Root → Leaf in topological order.
        assert_eq!(dag.chain_templates.len(), 1);
        let template = &dag.chain_templates[0];
        assert_eq!(template.root, "Root");
        assert_eq!(template.granularity, Some(Granularity::Month));
        assert_eq!(template.steps.len(), 2);
        assert_eq!(template.steps[0].module, "Root");
        assert_eq!(template.steps[0].role, ChainRole::Subscriber);
        assert_eq!(template.steps[1].module, "Leaf");
        assert_eq!(template.steps[1].role, ChainRole::Chained);
        assert_eq!(template.steps[1].triggered_by.as_deref(), Some("Root"));
    }

    #[test]
    fn execution_chain_orders_roots_by_firing_order() {
        // Two registrations for (1, 2): two direct subscribers at
        // different granularities. Coarser (MONTH) should fire first.
        let info = parse(
            "Registration\tCO\t2\tRunning Exhaust\t1\tMonthRoot\n\
             Registration\tCO\t2\tRunning Exhaust\t1\tDayRoot\n\
             Subscribe\tMonthRoot\tRunning Exhaust\t1\tMONTH\tEMISSION_CALCULATOR\n\
             Subscribe\tDayRoot\tRunning Exhaust\t1\tDAY\tEMISSION_CALCULATOR\n",
        );
        let dag = build_dag(&info, &[]).unwrap();
        let ec = &dag.execution_chains[0];
        assert_eq!(ec.roots, vec!["MonthRoot", "DayRoot"]);
    }

    #[test]
    fn global_execution_order_is_coarse_first_then_high_priority() {
        let info = parse(
            "Subscribe\tDayCalc\tRunning Exhaust\t1\tDAY\tEMISSION_CALCULATOR\n\
             Subscribe\tMonthCalc\tRunning Exhaust\t1\tMONTH\tEMISSION_CALCULATOR\n\
             Subscribe\tGenLow\tRunning Exhaust\t1\tMONTH\tGENERATOR-1\n\
             Subscribe\tGenHi\tRunning Exhaust\t1\tMONTH\tGENERATOR\n",
        );
        let dag = build_dag(&info, &[]).unwrap();
        let order: Vec<&str> = dag
            .global_execution_order
            .iter()
            .map(|e| e.module.as_str())
            .collect();
        // Inside MONTH (coarser than DAY): GenHi(=100) > GenLow(=99) > MonthCalc(=10)
        // DAY (finer) fires last.
        assert_eq!(order, vec!["GenHi", "GenLow", "MonthCalc", "DayCalc"]);
    }

    #[test]
    fn duplicate_registrations_are_deduplicated() {
        let info = parse(
            "Registration\tCO\t2\tRunning Exhaust\t1\tBaseRateCalculator\n\
             Registration\tCO\t2\tRunning Exhaust\t1\tBaseRateCalculator\n\
             Subscribe\tBaseRateCalculator\tRunning Exhaust\t1\tMONTH\tEMISSION_CALCULATOR\n",
        );
        let dag = build_dag(&info, &[]).unwrap();
        // Two raw directives → one canonical registration entry.
        assert_eq!(dag.registrations.len(), 1);
        // The module entry still counts every original raw directive, since
        // that's how Phase 2's RunSpec-driven instantiation will see them
        // (every duplicate pre-image came from a distinct call site).
        assert_eq!(dag.modules[0].registrations_count, 2);
    }

    #[test]
    fn java_subscription_fills_in_missing_module() {
        // BaseCalc has no Subscribe in CalculatorInfo (didn't fire) but
        // Java says it's a GenericCalculatorBase subclass at MONTH/+1.
        let info = parse("Registration\tCO\t2\tRunning Exhaust\t1\tBaseCalc\n");
        let java = vec![JavaSubscription {
            calculator: "BaseCalc".into(),
            java_path: std::path::PathBuf::from("BaseCalc.java"),
            style: SubscribeStyle::GenericBase,
            granularity: Some(Granularity::Month),
            priority: Some(Priority {
                base: crate::loop_meta::PriorityBase::EmissionCalculator,
                offset: 1,
            }),
            process_expr: String::new(),
        }];
        let dag = build_dag(&info, &java).unwrap();
        assert_eq!(dag.modules.len(), 1);
        let m = &dag.modules[0];
        assert!(m.subscribes_directly);
        assert_eq!(m.subscriptions[0].source, SubscriptionSource::JavaSource);
        assert_eq!(
            m.subscriptions[0].priority.display(),
            "EMISSION_CALCULATOR+1"
        );
        assert_eq!(m.java_path.as_deref(), Some("BaseCalc.java"));
    }

    #[test]
    fn calculator_info_subscription_wins_over_java_on_conflict() {
        let info = parse(
            "Subscribe\tBaseRateCalculator\tRunning Exhaust\t1\tMONTH\tEMISSION_CALCULATOR\n",
        );
        let java = vec![JavaSubscription {
            calculator: "BaseRateCalculator".into(),
            java_path: std::path::PathBuf::from("BaseRateCalculator.java"),
            style: SubscribeStyle::Explicit,
            granularity: Some(Granularity::Year), // intentionally different
            priority: Some(Priority {
                base: crate::loop_meta::PriorityBase::EmissionCalculator,
                offset: 0,
            }),
            process_expr: "process".into(),
        }];
        let dag = build_dag(&info, &java).unwrap();
        assert_eq!(dag.modules[0].subscriptions.len(), 1);
        let sub = &dag.modules[0].subscriptions[0];
        assert_eq!(sub.source, SubscriptionSource::CalculatorInfo);
        assert_eq!(sub.granularity, Granularity::Month); // runtime wins
    }

    #[test]
    fn registrations_are_sorted_by_process_then_pollutant_then_calculator() {
        let info = parse(
            "Registration\tCO\t2\tRunning Exhaust\t1\tZCalc\n\
             Registration\tNO\t3\tRunning Exhaust\t1\tACalc\n\
             Registration\tCO\t2\tStart Exhaust\t2\tACalc\n",
        );
        let dag = build_dag(&info, &[]).unwrap();
        let keys: Vec<(u32, u32, &str)> = dag
            .registrations
            .iter()
            .map(|r| (r.process_id, r.pollutant_id, r.calculator.as_str()))
            .collect();
        assert_eq!(
            keys,
            vec![(1, 2, "ZCalc"), (1, 3, "ACalc"), (2, 2, "ACalc")]
        );
    }

    #[test]
    fn registered_calculator_not_chained_to_any_subscriber_has_no_chains() {
        // Orphan: registers for (1, 2), no subscribe entry, no chain.
        let info = parse("Registration\tCO\t2\tRunning Exhaust\t1\tOrphan\n");
        let dag = build_dag(&info, &[]).unwrap();
        assert_eq!(dag.execution_chains.len(), 1);
        let ec = &dag.execution_chains[0];
        assert_eq!(ec.registered_calculators, vec!["Orphan"]);
        // Orphan doesn't subscribe and has no upstream → no roots.
        assert!(ec.roots.is_empty());
        assert!(dag.chain_templates.is_empty());
    }
}
