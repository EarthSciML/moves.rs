//! Memory-pressure regression for Task 27's bounded-concurrency executor.
//!
//! The migration plan requires that "doubling the parallelism limit roughly
//! doubles peak RSS as expected". The unit tests in `executor.rs` pin the
//! *mechanism* deterministically — a barrier proves exactly `limit` chunks
//! are co-resident. This integration test confirms the *consequence*
//! empirically: it makes chunks hold real, paged-in buffers and reads the
//! process peak RSS (`VmHWM`) back from `/proc/self/status`.
//!
//! `VmHWM` is a process-global, monotonic high-water mark, so a reading is
//! only meaningful if nothing else in the process allocates around it. That
//! is why this lives in its own integration-test file: `cargo test` runs it
//! as a dedicated process containing this single test, so the measurement
//! cannot be perturbed by the ~230 other `moves-framework` unit tests.
//!
//! Linux-only — `/proc/self/status` is the RSS source. The file compiles to
//! an empty test binary elsewhere.
#![cfg(target_os = "linux")]

use std::path::Path;
use std::sync::Barrier;

use moves_calculator_info::{build_dag, parse_calculator_info_str};
use moves_framework::{chunk_chains, BoundedExecutor, CalculatorRegistry, Chunk};

/// Per-chunk working-set size. Large enough that `limit` co-resident copies
/// dwarf allocator and OS noise, small enough that the test's peak stays
/// well under 100 MiB.
const BUFFER_BYTES: usize = 8 * 1024 * 1024;
/// Page stride for forcing residency.
const PAGE: usize = 4096;
/// Number of independent calculator modules — one chunk each. A multiple of
/// every `limit` used below, so the `limit`-party barrier always fills.
const MODULE_COUNT: usize = 32;

/// A registry of `MODULE_COUNT` independent direct subscribers — no `Chain`
/// edges, so `chunk_chains` yields one singleton chunk per module.
fn synthetic_registry() -> CalculatorRegistry {
    let mut text = String::new();
    for i in 0..MODULE_COUNT {
        text.push_str(&format!(
            "Subscribe\tmod{i:02}\tRunning Exhaust\t1\tMONTH\tEMISSION_CALCULATOR\n"
        ));
    }
    let info = parse_calculator_info_str(&text, Path::new("memory_pressure")).unwrap();
    CalculatorRegistry::new(build_dag(&info, &[]).unwrap())
}

/// Peak resident-set size in KiB (`VmHWM` from `/proc/self/status`), or
/// `None` if the field is unavailable.
fn peak_rss_kib() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status.lines().find_map(|line| {
        line.strip_prefix("VmHWM:")?
            .split_whitespace()
            .next()?
            .parse()
            .ok()
    })
}

/// Run every chunk through an executor capped at `limit`, each chunk
/// holding a paged-in [`BUFFER_BYTES`] buffer across a `limit`-party
/// barrier — so exactly `limit` buffers are provably co-resident — and
/// return the process peak RSS in KiB afterwards.
fn peak_rss_holding_buffers(chunks: &[Chunk], limit: usize) -> u64 {
    let executor = BoundedExecutor::new(limit).unwrap();
    let barrier = Barrier::new(limit);
    executor
        .execute(chunks, |_chunk| {
            let mut buffer = vec![0u8; BUFFER_BYTES];
            // Touch every page so the buffer is genuinely resident.
            let mut page = 0;
            while page < BUFFER_BYTES {
                buffer[page] = 1;
                page += PAGE;
            }
            // Hold the buffer across the rendezvous: at the barrier, `limit`
            // buffers are simultaneously resident.
            std::hint::black_box(&buffer);
            barrier.wait();
            std::hint::black_box(&buffer);
            Ok(())
        })
        .unwrap();
    peak_rss_kib().expect("VmHWM is readable once /proc/self/status is")
}

#[test]
fn peak_rss_grows_with_the_parallelism_limit() {
    // Skip rather than fail if /proc is unavailable (restricted sandbox).
    if peak_rss_kib().is_none() {
        return;
    }

    let registry = synthetic_registry();
    let names: Vec<String> = (0..MODULE_COUNT).map(|i| format!("mod{i:02}")).collect();
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let chunks = chunk_chains(&registry, &refs).unwrap();
    assert_eq!(
        chunks.len(),
        MODULE_COUNT,
        "independent modules must each form their own chunk"
    );

    // `VmHWM` is monotonic, so measure the low limit first. limit 1 holds
    // one 8 MiB buffer at a time; limit 8 holds eight — a true peak-RSS
    // delta near 56 MiB. The 24 MiB threshold absorbs allocator / OS slack.
    let baseline = peak_rss_holding_buffers(&chunks, 1);
    let wide = peak_rss_holding_buffers(&chunks, 8);
    let growth_kib = wide.saturating_sub(baseline);

    assert!(
        growth_kib >= 24 * 1024,
        "raising --max-parallel-chunks from 1 to 8 should grow peak RSS by \
         tens of MiB; saw {growth_kib} KiB (limit 1: {baseline} KiB, \
         limit 8: {wide} KiB)"
    );
}
