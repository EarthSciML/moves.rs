//! Bounded-concurrency executor — Task 27's parallelism layer.
//!
//! The migration plan's concurrency model (see `moves-rust-migration-plan.md`,
//! "Concurrency and memory model" plus Task 27) replaces MOVES's
//! filesystem-mediated master/worker split with in-process data parallelism
//! over the calculator-graph DAG. Memory pressure in that model is dominated
//! by how many `DataFrame` intermediates are live at once, which scales
//! linearly with the parallel-task width. The lever is therefore *bounded
//! parallelism*: a configurable cap on how many independent calculator
//! chains run concurrently.
//!
//! This module is the lever. It provides three pieces the plan calls out
//! explicitly:
//!
//! * [`chunk_chains`] — the chunking logic. It splits a set of calculator
//!   modules into [`Chunk`]s: the weakly-connected components of the
//!   dependency DAG. Two modules land in the same chunk iff a chain of
//!   `depends_on` edges connects them, so distinct chunks share no data
//!   and can run in any interleaving. Within a chunk the modules are
//!   topologically ordered — a chunk runs *sequentially* inside itself.
//! * [`Semaphore`] — a counting semaphore (`Mutex` + `Condvar`). The
//!   executor holds one permit per unit of allowed parallelism and a chunk
//!   acquires a permit for the whole span in which it allocates and holds
//!   its working set. With `N` permits, at most `N` chunk working sets are
//!   ever live, which is what bounds peak memory.
//! * [`BoundedExecutor`] — owns a [`rayon::ThreadPool`] sized by
//!   `--max-parallel-chunks` and dispatches chunks onto it, each gated by
//!   the semaphore. `N` chunks run concurrently; the rest queue.
//!
//! # Why both a pool *and* a semaphore
//!
//! The `rayon::ThreadPool` is sized to the parallelism limit, so the pool
//! alone already caps how many chunks *execute* at once. The [`Semaphore`]
//! is not redundant: it is acquired immediately before a chunk allocates
//! its working set and released only after that set is dropped, so the
//! "at most `N` working sets resident" guarantee is tied to the data
//! lifecycle rather than to pool-thread scheduling internals. It also keeps
//! the bound correct if a caller later hands work to a larger shared pool.
//! Both the pool and the semaphore are sized to the same limit.
//!
//! # Memory model, restated
//!
//! Peak memory ≈ `limit × max(chunk working set)`. Doubling
//! `--max-parallel-chunks` roughly doubles peak RSS; halving it roughly
//! halves it. The regression tests at the bottom of this module pin that
//! relationship deterministically — a barrier proves exactly `limit`
//! chunks are co-resident, so the co-resident working-set count tracks the
//! limit exactly. The `tests/memory_pressure.rs` integration test confirms
//! it empirically, reading `VmHWM` from `/proc/self/status` in an isolated
//! process where the measurement cannot be perturbed by other tests.
//!
//! # Phase 2 status
//!
//! The executor is data-plane agnostic: [`BoundedExecutor::execute`] takes
//! an arbitrary per-chunk closure. Task 27's [`crate::MOVESEngine`] uses it
//! to run one [`crate::MasterLoop`] per chunk; Phase 3 calculators allocate
//! their real `DataFrame` working sets inside that closure once Task 50's
//! data plane lands. Nothing here changes when they do.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Condvar, Mutex};

use crate::calculator::CalculatorRegistry;
use crate::error::{Error, Result};

// ---------------------------------------------------------------------------
// Semaphore — the explicit memory-bound gate.
// ---------------------------------------------------------------------------

/// A counting semaphore: a permit pool of fixed size that blocks
/// [`acquire`](Self::acquire) when empty and refills on permit drop.
///
/// Rust's standard library has no semaphore (and `tokio`'s is async-only —
/// the migration plan reserves `tokio` for I/O boundaries), so the
/// executor carries this small `Mutex` + `Condvar` implementation. It is
/// the gate that makes "at most `N` chunk working sets resident" a
/// guarantee: a chunk holds a permit for exactly the span in which its
/// working set is allocated.
///
/// Permits are returned by dropping the [`SemaphorePermit`] guard, so a
/// chunk that panics still releases its permit during unwinding.
#[derive(Debug)]
pub struct Semaphore {
    /// Permits currently available. `0` means [`acquire`](Self::acquire)
    /// blocks until a [`SemaphorePermit`] is dropped.
    available: Mutex<usize>,
    /// Signalled by [`release`](Self::release) whenever a permit returns.
    refilled: Condvar,
}

impl Semaphore {
    /// Construct a semaphore holding `permits` permits.
    ///
    /// `permits == 0` produces a semaphore that blocks every
    /// [`acquire`](Self::acquire) forever; callers size it to a positive
    /// parallelism limit ([`BoundedExecutor`] resolves `0` to a sensible
    /// default before reaching here).
    #[must_use]
    pub fn new(permits: usize) -> Self {
        Self {
            available: Mutex::new(permits),
            refilled: Condvar::new(),
        }
    }

    /// Take one permit, blocking the calling thread until one is free.
    ///
    /// The returned [`SemaphorePermit`] holds the permit; dropping it
    /// returns the permit and wakes one waiter.
    #[must_use = "the permit is released as soon as it is dropped"]
    pub fn acquire(&self) -> SemaphorePermit<'_> {
        let mut available = self.available.lock().expect("semaphore mutex poisoned");
        // `while`, not `if`: `Condvar::wait` is allowed spurious wakeups.
        while *available == 0 {
            available = self
                .refilled
                .wait(available)
                .expect("semaphore mutex poisoned");
        }
        *available -= 1;
        SemaphorePermit { semaphore: self }
    }

    /// Return one permit to the pool. Private — callers release by dropping
    /// the [`SemaphorePermit`] guard.
    fn release(&self) {
        let mut available = self.available.lock().expect("semaphore mutex poisoned");
        *available += 1;
        // One returned permit can satisfy exactly one waiter.
        self.refilled.notify_one();
    }
}

/// RAII guard for one [`Semaphore`] permit. The permit is returned to the
/// semaphore when the guard is dropped — including during panic unwinding,
/// so a panicking chunk never leaks its slot.
#[derive(Debug)]
pub struct SemaphorePermit<'a> {
    semaphore: &'a Semaphore,
}

impl Drop for SemaphorePermit<'_> {
    fn drop(&mut self) {
        self.semaphore.release();
    }
}

// ---------------------------------------------------------------------------
// Chunk — one independent calculator chain.
// ---------------------------------------------------------------------------

/// One independent calculator chain: a weakly-connected component of the
/// calculator-graph DAG, with its modules in topological order.
///
/// "Independent" is the load-bearing word — [`chunk_chains`] guarantees
/// that no `depends_on` edge crosses a chunk boundary, so two chunks share
/// no upstream producer and consume no common scratch table. They can run
/// concurrently in any interleaving. *Within* a chunk the modules are
/// ordered so every module follows the modules it depends on; a chunk runs
/// sequentially inside itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chunk {
    /// Module names, topologically ordered (upstream producers first).
    modules: Vec<String>,
}

impl Chunk {
    /// Wrap a topologically-ordered module list. Private — chunks are
    /// produced by [`chunk_chains`], which is responsible for the
    /// independence and ordering invariants.
    fn new(modules: Vec<String>) -> Self {
        Self { modules }
    }

    /// The chunk's modules, in execution (topological) order.
    #[must_use]
    pub fn modules(&self) -> &[String] {
        &self.modules
    }

    /// Number of modules in the chunk.
    #[must_use]
    pub fn len(&self) -> usize {
        self.modules.len()
    }

    /// Whether the chunk has no modules. A well-formed chunk always has at
    /// least one; the method exists so Clippy's `len`-without-`is_empty`
    /// lint stays quiet and for symmetry with the standard collections.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.modules.is_empty()
    }
}

/// Split `names` into independent [`Chunk`]s — the chunking logic of
/// Task 27's bounded-concurrency executor.
///
/// Two modules share a chunk iff a path of `depends_on` edges (in either
/// direction) connects them within `names`; that is, chunks are the
/// weakly-connected components of the dependency subgraph induced on
/// `names`. Modules absent from the registry's DAG have no edges and
/// become singleton chunks.
///
/// Each chunk's modules are returned in topological order via
/// [`CalculatorRegistry::topological_order`], and the chunk list itself is
/// ordered by each chunk's lexicographically-smallest module name, so the
/// result is fully deterministic for a given input set regardless of the
/// order `names` arrives in.
///
/// # Errors
///
/// Returns [`Error::CyclicChain`] if the dependency subgraph induced on a
/// chunk contains a cycle. The MOVES calculator chain is acyclic by
/// construction, so this signals malformed input data.
pub fn chunk_chains(registry: &CalculatorRegistry, names: &[&str]) -> Result<Vec<Chunk>> {
    // BTreeSet → deterministic, de-duplicated iteration order.
    let members: BTreeSet<&str> = names.iter().copied().collect();
    if members.is_empty() {
        return Ok(Vec::new());
    }

    // Undirected adjacency restricted to `members`. Every directed
    // `depends_on` edge is recorded once from the dependent's side; adding
    // it symmetrically captures the whole undirected graph without also
    // walking `dependents`.
    let mut adjacency: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    for &name in &members {
        let Some(module) = registry.module(name) else {
            continue;
        };
        for dependency in &module.depends_on {
            if let Some(&other) = members.get(dependency.as_str()) {
                adjacency.entry(name).or_default().insert(other);
                adjacency.entry(other).or_default().insert(name);
            }
        }
    }

    // Connected components via BFS. Iterating `members` in sorted order
    // and starting each component at the first not-yet-seen member means
    // components are discovered — and therefore returned — ordered by
    // their smallest module name.
    let mut visited: BTreeSet<&str> = BTreeSet::new();
    let mut chunks: Vec<Chunk> = Vec::new();
    for &start in &members {
        if !visited.insert(start) {
            continue;
        }
        let mut component: Vec<&str> = Vec::new();
        let mut frontier: VecDeque<&str> = VecDeque::from([start]);
        while let Some(current) = frontier.pop_front() {
            component.push(current);
            if let Some(neighbours) = adjacency.get(current) {
                for &neighbour in neighbours {
                    if visited.insert(neighbour) {
                        frontier.push_back(neighbour);
                    }
                }
            }
        }
        // Order the component so producers precede consumers.
        let ordered = registry.topological_order(&component)?;
        chunks.push(Chunk::new(ordered));
    }
    Ok(chunks)
}

// ---------------------------------------------------------------------------
// BoundedExecutor — the rayon pool + semaphore.
// ---------------------------------------------------------------------------

/// Resolve a requested parallelism limit to a concrete positive thread
/// count. `0` means "let the runtime choose" and maps to the number of
/// available hardware threads (falling back to `1` if that probe fails).
fn resolve_parallelism(requested: usize) -> usize {
    if requested > 0 {
        requested
    } else {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    }
}

/// Runs independent [`Chunk`]s concurrently up to a fixed parallelism
/// limit.
///
/// Owns a [`rayon::ThreadPool`] sized by `--max-parallel-chunks` and a
/// [`Semaphore`] of matching permit count. [`execute`](Self::execute)
/// dispatches every chunk onto the pool; each chunk acquires a permit
/// before running its body and releases it after, so at most `limit`
/// chunks — and at most `limit` chunk working sets — are ever live.
///
/// The executor is reusable: a single [`BoundedExecutor`] can serve many
/// [`execute`](Self::execute) calls, each with its own chunk set.
#[derive(Debug)]
pub struct BoundedExecutor {
    /// Worker pool. Sized to [`limit`](Self::limit) threads.
    pool: rayon::ThreadPool,
    /// Resolved parallelism limit — the pool's thread count and the
    /// per-`execute` semaphore's permit count.
    limit: usize,
}

impl BoundedExecutor {
    /// Build an executor capped at `max_parallel_chunks` concurrent chunks.
    ///
    /// `max_parallel_chunks == 0` selects the host's available parallelism
    /// (number of hardware threads). Any positive value is used verbatim,
    /// so callers tuning for a memory-constrained environment can force a
    /// low limit regardless of core count.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ThreadPool`] if `rayon` cannot build the pool —
    /// for example when the OS refuses to spawn the worker threads.
    pub fn new(max_parallel_chunks: usize) -> Result<Self> {
        let limit = resolve_parallelism(max_parallel_chunks);
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(limit)
            .thread_name(|index| format!("moves-chunk-{index}"))
            .build()
            .map_err(|err| Error::ThreadPool(err.to_string()))?;
        Ok(Self { pool, limit })
    }

    /// The resolved parallelism limit — the maximum number of chunks (and
    /// chunk working sets) that run concurrently.
    #[must_use]
    pub fn limit(&self) -> usize {
        self.limit
    }

    /// Run every chunk in `chunks` through `run_chunk`, with at most
    /// [`limit`](Self::limit) running concurrently.
    ///
    /// Each chunk is dispatched onto the pool; the chunk body acquires a
    /// [`Semaphore`] permit, runs `run_chunk`, then releases the permit on
    /// return (or on panic, via the guard's `Drop`). The call blocks until
    /// every chunk has finished.
    ///
    /// # Errors
    ///
    /// If one or more chunk bodies return [`Err`], the first error (in an
    /// unspecified order — chunks run concurrently) is returned after all
    /// chunks complete. The executor never cancels in-flight chunks: a
    /// `rayon` scope runs every spawned task to completion.
    pub fn execute<F>(&self, chunks: &[Chunk], run_chunk: F) -> Result<()>
    where
        F: Fn(&Chunk) -> Result<()> + Sync,
    {
        if chunks.is_empty() {
            return Ok(());
        }

        let permits = Semaphore::new(self.limit);
        let errors: Mutex<Vec<Error>> = Mutex::new(Vec::new());

        self.pool.scope(|scope| {
            for chunk in chunks {
                let permits = &permits;
                let errors = &errors;
                let run_chunk = &run_chunk;
                scope.spawn(move |_| {
                    // Hold a permit for the whole body: the working set the
                    // chunk allocates inside `run_chunk` stays bounded by
                    // `limit` concurrent copies.
                    let _permit = permits.acquire();
                    if let Err(err) = run_chunk(chunk) {
                        errors
                            .lock()
                            .expect("executor error-collection mutex poisoned")
                            .push(err);
                    }
                });
            }
        });

        match errors
            .into_inner()
            .expect("executor error-collection mutex poisoned")
            .into_iter()
            .next()
        {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use moves_calculator_info::{build_dag, parse_calculator_info_str, CalculatorInfo};
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};

    // ---- Semaphore ---------------------------------------------------------

    #[test]
    fn semaphore_acquire_decrements_and_permit_drop_restores() {
        let sem = Semaphore::new(2);
        let a = sem.acquire();
        let b = sem.acquire();
        // Both permits taken; the count is exhausted.
        assert_eq!(*sem.available.lock().unwrap(), 0);
        drop(a);
        assert_eq!(*sem.available.lock().unwrap(), 1);
        drop(b);
        assert_eq!(*sem.available.lock().unwrap(), 2);
    }

    #[test]
    fn semaphore_blocks_until_a_permit_is_released() {
        // One permit, two threads contending. The second `acquire` must
        // block until the first permit's guard drops.
        let sem = Arc::new(Semaphore::new(1));
        let held = sem.acquire();

        let progressed = Arc::new(AtomicUsize::new(0));
        let waiter = {
            let sem = Arc::clone(&sem);
            let progressed = Arc::clone(&progressed);
            std::thread::spawn(move || {
                let _permit = sem.acquire();
                progressed.store(1, Ordering::SeqCst);
            })
        };

        // Give the waiter time to reach (and block on) `acquire`. It cannot
        // have progressed: the only permit is still held here.
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert_eq!(progressed.load(Ordering::SeqCst), 0);

        drop(held);
        waiter.join().unwrap();
        assert_eq!(progressed.load(Ordering::SeqCst), 1);
    }

    // ---- chunk_chains ------------------------------------------------------

    fn parse(text: &str) -> CalculatorInfo {
        parse_calculator_info_str(text, Path::new("test")).unwrap()
    }

    fn registry(text: &str) -> CalculatorRegistry {
        CalculatorRegistry::new(build_dag(&parse(text), &[]).unwrap())
    }

    /// Sorted module list of a chunk — order-insensitive content check.
    fn members(chunk: &Chunk) -> Vec<String> {
        let mut m = chunk.modules().to_vec();
        m.sort();
        m
    }

    #[test]
    fn chunk_chains_empty_input_yields_no_chunks() {
        let reg = registry("Registration\tCO\t2\tRunning Exhaust\t1\tBaseRateCalculator\n");
        assert!(chunk_chains(&reg, &[]).unwrap().is_empty());
    }

    #[test]
    fn chunk_chains_unconnected_modules_each_form_a_singleton() {
        // Three direct subscribers, no chain edges between them.
        let reg = registry(
            "Subscribe\tApple\tRunning Exhaust\t1\tMONTH\tEMISSION_CALCULATOR\n\
             Subscribe\tBanana\tRunning Exhaust\t1\tMONTH\tEMISSION_CALCULATOR\n\
             Subscribe\tCherry\tRunning Exhaust\t1\tMONTH\tEMISSION_CALCULATOR\n",
        );
        let chunks = chunk_chains(&reg, &["Cherry", "Apple", "Banana"]).unwrap();
        assert_eq!(chunks.len(), 3);
        // Chunk order is by smallest member name, independent of input order.
        assert_eq!(chunks[0].modules(), ["Apple"]);
        assert_eq!(chunks[1].modules(), ["Banana"]);
        assert_eq!(chunks[2].modules(), ["Cherry"]);
    }

    #[test]
    fn chunk_chains_groups_a_dependency_path_into_one_chunk() {
        // UpstreamGen → Root → Leaf is a single connected component.
        let reg = registry(
            "Registration\tCO\t2\tRunning Exhaust\t1\tRoot\n\
             Subscribe\tRoot\tRunning Exhaust\t1\tMONTH\tEMISSION_CALCULATOR\n\
             Subscribe\tUpstreamGen\tRunning Exhaust\t1\tPROCESS\tGENERATOR\n\
             Chain\tRoot\tUpstreamGen\n\
             Chain\tLeaf\tRoot\n",
        );
        let chunks = chunk_chains(&reg, &["Leaf", "Root", "UpstreamGen"]).unwrap();
        assert_eq!(chunks.len(), 1);
        // Topologically ordered: producers before consumers.
        assert_eq!(chunks[0].modules(), ["UpstreamGen", "Root", "Leaf"]);
    }

    #[test]
    fn chunk_chains_separates_independent_components() {
        // Two disjoint chains: (GenA→RootA) and (GenB→RootB).
        let reg = registry(
            "Registration\tCO\t2\tRunning Exhaust\t1\tRootA\n\
             Registration\tNOx\t3\tRunning Exhaust\t1\tRootB\n\
             Subscribe\tRootA\tRunning Exhaust\t1\tMONTH\tEMISSION_CALCULATOR\n\
             Subscribe\tRootB\tRunning Exhaust\t1\tMONTH\tEMISSION_CALCULATOR\n\
             Subscribe\tGenA\tRunning Exhaust\t1\tPROCESS\tGENERATOR\n\
             Subscribe\tGenB\tRunning Exhaust\t1\tPROCESS\tGENERATOR\n\
             Chain\tRootA\tGenA\n\
             Chain\tRootB\tGenB\n",
        );
        let chunks = chunk_chains(&reg, &["RootA", "RootB", "GenA", "GenB"]).unwrap();
        assert_eq!(chunks.len(), 2);
        // Component reachable from "GenA" sorts first (smallest member).
        assert_eq!(members(&chunks[0]), ["GenA", "RootA"]);
        assert_eq!(members(&chunks[1]), ["GenB", "RootB"]);
        // Each chunk is internally topo-ordered.
        assert_eq!(chunks[0].modules(), ["GenA", "RootA"]);
        assert_eq!(chunks[1].modules(), ["GenB", "RootB"]);
    }

    #[test]
    fn chunk_chains_is_deterministic_under_input_reordering() {
        let reg = registry(
            "Registration\tCO\t2\tRunning Exhaust\t1\tRootA\n\
             Registration\tNOx\t3\tRunning Exhaust\t1\tRootB\n\
             Subscribe\tRootA\tRunning Exhaust\t1\tMONTH\tEMISSION_CALCULATOR\n\
             Subscribe\tRootB\tRunning Exhaust\t1\tMONTH\tEMISSION_CALCULATOR\n\
             Subscribe\tGenA\tRunning Exhaust\t1\tPROCESS\tGENERATOR\n\
             Chain\tRootA\tGenA\n",
        );
        let one = chunk_chains(&reg, &["GenA", "RootA", "RootB"]).unwrap();
        let two = chunk_chains(&reg, &["RootB", "GenA", "RootA"]).unwrap();
        let three = chunk_chains(&reg, &["RootA", "RootB", "GenA"]).unwrap();
        assert_eq!(one, two);
        assert_eq!(two, three);
    }

    #[test]
    fn chunk_chains_treats_unknown_module_as_singleton() {
        let reg = registry("Registration\tCO\t2\tRunning Exhaust\t1\tBaseRateCalculator\n");
        let chunks = chunk_chains(&reg, &["BaseRateCalculator", "NotInDag"]).unwrap();
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].modules(), ["BaseRateCalculator"]);
        assert_eq!(chunks[1].modules(), ["NotInDag"]);
    }

    // ---- BoundedExecutor — wiring -----------------------------------------

    /// Build `count` singleton chunks named `m0..m{count}`.
    fn synthetic_chunks(count: usize) -> Vec<Chunk> {
        (0..count)
            .map(|i| Chunk::new(vec![format!("m{i}")]))
            .collect()
    }

    #[test]
    fn executor_resolves_zero_limit_to_available_parallelism() {
        let exec = BoundedExecutor::new(0).unwrap();
        let expected = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        assert_eq!(exec.limit(), expected);
        assert!(exec.limit() >= 1);
    }

    #[test]
    fn executor_uses_an_explicit_positive_limit_verbatim() {
        assert_eq!(BoundedExecutor::new(3).unwrap().limit(), 3);
    }

    #[test]
    fn executor_runs_every_chunk_exactly_once() {
        let exec = BoundedExecutor::new(4).unwrap();
        let chunks = synthetic_chunks(50);
        let runs = AtomicUsize::new(0);
        exec.execute(&chunks, |_chunk| {
            runs.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
        assert_eq!(runs.load(Ordering::SeqCst), 50);
    }

    #[test]
    fn executor_empty_chunk_set_is_a_no_op() {
        let exec = BoundedExecutor::new(2).unwrap();
        exec.execute(&[], |_chunk| -> Result<()> {
            panic!("closure must not run for an empty chunk set");
        })
        .unwrap();
    }

    #[test]
    fn executor_surfaces_a_chunk_error() {
        let exec = BoundedExecutor::new(4).unwrap();
        let chunks = synthetic_chunks(20);
        let err = exec
            .execute(&chunks, |chunk| {
                if chunk.modules() == ["m7"] {
                    Err(Error::ThreadPool("synthetic failure".into()))
                } else {
                    Ok(())
                }
            })
            .unwrap_err();
        match err {
            Error::ThreadPool(msg) => assert_eq!(msg, "synthetic failure"),
            other => panic!("expected the chunk's ThreadPool error, got {other:?}"),
        }
    }

    // ---- BoundedExecutor — memory-pressure regression ---------------------
    //
    // Peak memory ≈ limit × max(chunk working set). These tests pin that
    // relationship: the bounded-concurrency cap is what keeps peak RSS
    // proportional to `--max-parallel-chunks`.

    /// Drive `4 * limit` chunks through an executor of the given limit and
    /// return the greatest number of chunks observed running concurrently.
    ///
    /// A `Barrier` of `limit` parties makes the result deterministic: the
    /// pool has exactly `limit` threads, so `limit` chunk bodies reach the
    /// barrier together, observe a concurrency of `limit`, and only then
    /// release — no sleeps, no timing assumptions.
    fn observed_peak_concurrency(limit: usize) -> usize {
        let exec = BoundedExecutor::new(limit).unwrap();
        let chunks = synthetic_chunks(4 * limit);
        let active = AtomicUsize::new(0);
        let peak = AtomicUsize::new(0);
        let barrier = Barrier::new(limit);
        exec.execute(&chunks, |_chunk| {
            let now = active.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(now, Ordering::SeqCst);
            barrier.wait();
            active.fetch_sub(1, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
        // `active` returns to zero once every chunk has finished.
        assert_eq!(active.load(Ordering::SeqCst), 0);
        peak.load(Ordering::SeqCst)
    }

    #[test]
    fn executor_caps_concurrency_at_the_limit() {
        // The observed peak equals the limit exactly: the barrier proves
        // `limit` is reached, the pool + semaphore prove it is never
        // exceeded.
        for limit in [1, 2, 3, 5] {
            assert_eq!(
                observed_peak_concurrency(limit),
                limit,
                "executor with limit {limit} must run exactly {limit} chunks at once"
            );
        }
    }

    #[test]
    fn doubling_the_limit_doubles_peak_concurrency() {
        // The core memory-pressure invariant: peak co-residency scales
        // linearly with `--max-parallel-chunks`, so peak RSS does too.
        assert_eq!(observed_peak_concurrency(2), 2);
        assert_eq!(observed_peak_concurrency(4), 4);
        assert_eq!(observed_peak_concurrency(8), 8);
    }

    // The empirical peak-RSS counterpart of these tests — actually reading
    // `VmHWM` while chunks hold real buffers — lives in the
    // `tests/memory_pressure.rs` integration test. It runs in its own
    // process so the process-global RSS high-water mark is not perturbed by
    // the other tests in this binary.
}
