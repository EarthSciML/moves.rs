//! Phase 1 Task 9: aggregate fixture execution traces into a coverage map.
//!
//! Phase 0 produces one [`ExecutionTrace`] per fixture as a sidecar to the
//! snapshot. This module rolls those per-fixture traces up into a
//! suite-wide [`CoverageMap`]: for each Java class, SQL macro file, and Go
//! calculator, how many fixtures invoke it, how many worker bundles
//! reference it, and what fraction of the suite-wide statement workload it
//! accounts for. The output drives Phase 3 calculator-port ordering —
//! hot-path items first.
//!
//! ## Inputs
//!
//! [`build_coverage_map`] consumes a slice of [`ExecutionTrace`] values, one
//! per fixture. The convenience function [`read_traces_dir`] walks a
//! `characterization/snapshots/` directory and loads `execution-trace.json`
//! from each subdirectory that has one. Subdirectories without a trace
//! file are silently skipped (the same robustness contract as
//! [`build_execution_trace`](crate::trace::build_execution_trace)).
//!
//! ## Weighting
//!
//! The coverage map needs to answer "fraction of total execution time" but
//! [`ExecutionTrace`] does not carry timings — Phase 0 instrumentation
//! captures *which* code ran, not *for how long*. The map uses
//! [`WorkerBundle::statement_count`](crate::trace::WorkerBundle::statement_count)
//! as a proxy:
//!
//! * The **suite-wide weight** is the sum of `statement_count` across all
//!   worker bundles of all fixtures.
//! * An item's **statement weight** is the sum of `statement_count` over
//!   every bundle that referenced it. A Java class loaded only via the JVM
//!   class-load log (i.e. not seen in any bundle's `worker.sql`) gets
//!   `statement_weight = 0` — the fixture-fraction column still captures
//!   that it was reached.
//!
//! `statement_count` is a coarse proxy; the resulting ranking is meant for
//! migration-ordering heuristics, not precise time accounting.
//!
//! ## Ranking
//!
//! Each item carries a `score = (fixture_fraction + statement_weight_fraction) / 2`,
//! both terms in `[0, 1]`. Items are sorted within each category by
//! descending score with name as a deterministic tiebreaker. The
//! `hot_paths` slice flattens the three categories together for a single
//! ranked migration list.
//!
//! ## Determinism
//!
//! [`build_coverage_map`] is a pure function of the input traces. Every
//! collection in [`CoverageMap`] is sorted (the per-item field lists by
//! lexicographic key, the rankings by descending score with a name
//! tiebreaker). [`write_coverage_map`] serializes via `serde_json` and
//! appends a trailing newline; two runs with the same inputs produce a
//! byte-identical file.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::trace::{ExecutionTrace, WorkerBundle};

/// Coverage-map format version. Bumped on incompatible schema changes so
/// downstream consumers can fail fast on unfamiliar shapes.
pub const COVERAGE_VERSION: &str = "moves-coverage/v1";

/// Default coverage-map filename inside `characterization/coverage/`.
pub const COVERAGE_FILE: &str = "coverage-map.json";

/// Filename the loader looks for inside each snapshot subdirectory.
pub const TRACE_FILE: &str = "execution-trace.json";

/// Cap for the cross-category `hot_paths` list. Top-N items by score
/// across all three item kinds. Phase 3 will work down this list in order.
pub const HOT_PATHS_LIMIT: usize = 50;

/// Suite-wide coverage rollup. Sibling structure of [`ExecutionTrace`],
/// but per-suite rather than per-fixture.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CoverageMap {
    /// Schema version. Mirrors `ExecutionTrace::trace_version`'s role.
    pub coverage_version: String,

    /// Number of fixtures aggregated into this map (i.e. number of
    /// `execution-trace.json` files read).
    pub total_fixtures: usize,

    /// Suite-wide statement-count sum across every worker bundle of every
    /// fixture. Denominator for `statement_weight_fraction`.
    pub total_statement_weight: usize,

    /// One entry per source trace, sorted alphabetically by `fixture_name`.
    /// Lets consumers correlate suite-level aggregates back to the
    /// per-fixture inputs without re-reading the traces.
    pub fixtures: Vec<FixtureSummary>,

    /// Java class coverage, sorted by descending `score` with `name` as
    /// the deterministic tiebreaker.
    pub java_classes: Vec<JavaClassCoverage>,

    /// SQL macro/template file coverage, sorted by descending `score`
    /// with `id` as the tiebreaker.
    pub sql_files: Vec<ItemCoverage>,

    /// Go calculator coverage, sorted by descending `score` with `id` as
    /// the tiebreaker.
    pub go_calculators: Vec<ItemCoverage>,

    /// Flat, cross-category migration-ordering hint.
    pub hot_paths: Vec<HotPath>,
}

/// Per-fixture summary derived from a single [`ExecutionTrace`]. Carries
/// just enough of the trace to identify it back to its snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FixtureSummary {
    /// Mirrors `ExecutionTrace::fixture_name`.
    pub fixture_name: String,
    /// Mirrors `ExecutionTrace::sif_sha256`.
    pub sif_sha256: String,
    /// Mirrors `ExecutionTrace::runspec_sha256`.
    pub runspec_sha256: String,
    /// Mirrors `ExecutionTrace::trace_version` — surfaced in case the
    /// suite has a mix of trace schema versions during a transition.
    pub trace_version: String,
    /// `ExecutionTrace::java_classes.len()`.
    pub java_class_count: usize,
    /// `ExecutionTrace::sql_files.len()`.
    pub sql_file_count: usize,
    /// `ExecutionTrace::go_calculators.len()`.
    pub go_calculator_count: usize,
    /// `ExecutionTrace::worker_bundles.len()`.
    pub worker_bundle_count: usize,
    /// Sum of `worker_bundles[].statement_count`.
    pub statement_count: usize,
}

/// Per-Java-class coverage. Carries the trace's `kind` tag in addition to
/// the generic [`ItemCoverage`] columns.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JavaClassCoverage {
    /// Fully qualified Java class name (e.g.
    /// `gov.epa.otaq.moves.master.calculator.BaseRateCalculator`).
    pub name: String,
    /// Hierarchy tag from the underlying [`JavaClass::kind`](crate::trace::JavaClass::kind).
    /// One of `calculator`, `generator`, `framework`, `worker`, `master`,
    /// `common`, `utils`, `other`.
    pub kind: String,
    /// Number of distinct fixtures whose trace contains this class.
    pub fixture_count: usize,
    /// `fixture_count / total_fixtures`. `0.0` when there are no
    /// fixtures.
    pub fixture_fraction: f64,
    /// Fixture names that contain this class, sorted alphabetically.
    pub fixtures: Vec<String>,
    /// Number of `(fixture, worker_bundle)` pairs that referenced this
    /// class in their `worker.sql`. Class-load-log-only references count
    /// 0 here.
    pub bundle_count: usize,
    /// Sum of `worker_bundles[].statement_count` for the bundles in
    /// `bundle_count`. Proxy for time spent in code paths that reference
    /// this class.
    pub statement_weight: usize,
    /// `statement_weight / total_statement_weight`. `0.0` when the suite
    /// has no statements (e.g. empty suite).
    pub statement_weight_fraction: f64,
    /// `(fixture_fraction + statement_weight_fraction) / 2`. Used for the
    /// ranking; high score = high migration priority.
    pub score: f64,
}

/// Coverage row for an SQL macro file or a Go calculator. The two share
/// the same shape; only the `id` semantics differ (path vs. name).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ItemCoverage {
    /// Identifier: SQL path (`database/CalculatorSQL/...`) for SQL files,
    /// calculator name for Go calculators.
    pub id: String,
    /// Number of distinct fixtures that referenced this item.
    pub fixture_count: usize,
    /// `fixture_count / total_fixtures`. `0.0` for empty suites.
    pub fixture_fraction: f64,
    /// Fixture names that referenced this item, sorted alphabetically.
    pub fixtures: Vec<String>,
    /// Number of `(fixture, worker_bundle)` pairs that referenced this
    /// item.
    pub bundle_count: usize,
    /// Sum of `worker_bundles[].statement_count` for the bundles in
    /// `bundle_count`.
    pub statement_weight: usize,
    /// `statement_weight / total_statement_weight`. `0.0` for empty
    /// suites.
    pub statement_weight_fraction: f64,
    /// `(fixture_fraction + statement_weight_fraction) / 2`.
    pub score: f64,
}

/// Cross-category hot-path entry. The list is the union of the three
/// per-category rankings, re-sorted by `score`, capped at
/// [`HOT_PATHS_LIMIT`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HotPath {
    /// Item category. One of `java_class`, `sql_file`, `go_calculator`.
    pub kind: String,
    /// Identifier — matches the per-category row's `name`/`id` column.
    pub id: String,
    /// Number of distinct fixtures that referenced this item.
    pub fixture_count: usize,
    /// Statement-count weight (see [`ItemCoverage::statement_weight`]).
    pub statement_weight: usize,
    /// Same score as the per-category row.
    pub score: f64,
}

/// Build a suite-wide coverage map from per-fixture traces.
///
/// `traces` may be in any order; the function sorts everything it emits.
/// An empty slice yields a valid, all-zero map.
pub fn build_coverage_map(traces: &[ExecutionTrace]) -> CoverageMap {
    let total_fixtures = traces.len();
    let total_statement_weight: usize = traces
        .iter()
        .flat_map(|t| t.worker_bundles.iter().map(|b| b.statement_count))
        .sum();

    let java_classes = aggregate_java_classes(traces, total_fixtures, total_statement_weight);
    let sql_files = aggregate_items(
        traces,
        |t| t.sql_files.iter().map(|s| s.path.clone()).collect(),
        |b| &b.sql_files,
        total_fixtures,
        total_statement_weight,
    );
    let go_calculators = aggregate_items(
        traces,
        |t| t.go_calculators.iter().map(|g| g.name.clone()).collect(),
        |b| &b.go_calculators,
        total_fixtures,
        total_statement_weight,
    );

    let hot_paths = build_hot_paths(&java_classes, &sql_files, &go_calculators, HOT_PATHS_LIMIT);

    let mut fixtures: Vec<FixtureSummary> = traces
        .iter()
        .map(|t| FixtureSummary {
            fixture_name: t.fixture_name.clone(),
            sif_sha256: t.sif_sha256.clone(),
            runspec_sha256: t.runspec_sha256.clone(),
            trace_version: t.trace_version.clone(),
            java_class_count: t.java_classes.len(),
            sql_file_count: t.sql_files.len(),
            go_calculator_count: t.go_calculators.len(),
            worker_bundle_count: t.worker_bundles.len(),
            statement_count: t.worker_bundles.iter().map(|b| b.statement_count).sum(),
        })
        .collect();
    fixtures.sort_by(|a, b| a.fixture_name.cmp(&b.fixture_name));

    CoverageMap {
        coverage_version: COVERAGE_VERSION.to_string(),
        total_fixtures,
        total_statement_weight,
        fixtures,
        java_classes,
        sql_files,
        go_calculators,
        hot_paths,
    }
}

/// Load every `execution-trace.json` under `snapshots_root` (one per
/// snapshot subdirectory). Subdirectories without a trace file are
/// skipped; non-directory entries at the root are ignored. The walk is
/// sorted lexicographically by subdirectory name so the resulting trace
/// order is stable.
pub fn read_traces_dir(snapshots_root: &Path) -> Result<Vec<ExecutionTrace>> {
    if !snapshots_root.exists() {
        return Err(Error::Io {
            path: snapshots_root.to_path_buf(),
            source: std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "snapshots root does not exist",
            ),
        });
    }
    let mut subdirs: Vec<PathBuf> = std::fs::read_dir(snapshots_root)
        .map_err(|source| Error::Io {
            path: snapshots_root.to_path_buf(),
            source,
        })?
        .filter_map(|r| r.ok())
        .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
        .map(|e| e.path())
        .collect();
    subdirs.sort();

    let mut out = Vec::with_capacity(subdirs.len());
    for dir in subdirs {
        let trace_path = dir.join(TRACE_FILE);
        if !trace_path.is_file() {
            continue;
        }
        let bytes = std::fs::read(&trace_path).map_err(|source| Error::Io {
            path: trace_path.clone(),
            source,
        })?;
        let trace: ExecutionTrace =
            serde_json::from_slice(&bytes).map_err(|source| Error::Json {
                path: trace_path.clone(),
                source,
            })?;
        out.push(trace);
    }
    Ok(out)
}

/// Write `map` to `<dir>/coverage-map.json` pretty-printed with a
/// trailing newline. The output directory is created if it doesn't
/// exist. Byte-deterministic for fixed inputs.
pub fn write_coverage_map(dir: &Path, map: &CoverageMap) -> Result<PathBuf> {
    std::fs::create_dir_all(dir).map_err(|source| Error::Io {
        path: dir.to_path_buf(),
        source,
    })?;
    let path = dir.join(COVERAGE_FILE);
    let mut bytes = serde_json::to_vec_pretty(map).map_err(|source| Error::Json {
        path: path.clone(),
        source,
    })?;
    bytes.push(b'\n');
    std::fs::write(&path, &bytes).map_err(|source| Error::Io {
        path: path.clone(),
        source,
    })?;
    Ok(path)
}

fn aggregate_java_classes(
    traces: &[ExecutionTrace],
    total_fixtures: usize,
    total_statement_weight: usize,
) -> Vec<JavaClassCoverage> {
    let mut kinds: BTreeMap<String, String> = BTreeMap::new();
    let mut fixtures_by_name: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut bundle_count_by_name: BTreeMap<String, usize> = BTreeMap::new();
    let mut weight_by_name: BTreeMap<String, usize> = BTreeMap::new();

    for trace in traces {
        for cls in &trace.java_classes {
            kinds
                .entry(cls.name.clone())
                .or_insert_with(|| cls.kind.clone());
            fixtures_by_name
                .entry(cls.name.clone())
                .or_default()
                .insert(trace.fixture_name.clone());
        }
        for bundle in &trace.worker_bundles {
            for cls in &bundle.java_classes {
                *bundle_count_by_name.entry(cls.clone()).or_default() += 1;
                *weight_by_name.entry(cls.clone()).or_default() += bundle.statement_count;
            }
        }
    }

    let mut out: Vec<JavaClassCoverage> = kinds
        .into_iter()
        .map(|(name, kind)| {
            let fixtures_set = fixtures_by_name.remove(&name).unwrap_or_default();
            let fixture_count = fixtures_set.len();
            let fixtures: Vec<String> = fixtures_set.into_iter().collect();
            let bundle_count = bundle_count_by_name.remove(&name).unwrap_or(0);
            let statement_weight = weight_by_name.remove(&name).unwrap_or(0);
            let fixture_fraction = frac(fixture_count, total_fixtures);
            let statement_weight_fraction = frac(statement_weight, total_statement_weight);
            let score = (fixture_fraction + statement_weight_fraction) / 2.0;
            JavaClassCoverage {
                name,
                kind,
                fixture_count,
                fixture_fraction,
                fixtures,
                bundle_count,
                statement_weight,
                statement_weight_fraction,
                score,
            }
        })
        .collect();

    out.sort_by(|a, b| {
        score_cmp(b.score, a.score) // descending
            .then_with(|| a.name.cmp(&b.name))
    });
    out
}

fn aggregate_items<F, G>(
    traces: &[ExecutionTrace],
    fixture_ids: F,
    bundle_ids: G,
    total_fixtures: usize,
    total_statement_weight: usize,
) -> Vec<ItemCoverage>
where
    F: Fn(&ExecutionTrace) -> Vec<String>,
    G: Fn(&WorkerBundle) -> &Vec<String>,
{
    let mut fixtures_by_id: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut bundle_count_by_id: BTreeMap<String, usize> = BTreeMap::new();
    let mut weight_by_id: BTreeMap<String, usize> = BTreeMap::new();

    for trace in traces {
        for id in fixture_ids(trace) {
            fixtures_by_id
                .entry(id)
                .or_default()
                .insert(trace.fixture_name.clone());
        }
        for bundle in &trace.worker_bundles {
            for id in bundle_ids(bundle) {
                *bundle_count_by_id.entry(id.clone()).or_default() += 1;
                *weight_by_id.entry(id.clone()).or_default() += bundle.statement_count;
            }
        }
    }

    let mut out: Vec<ItemCoverage> = fixtures_by_id
        .into_iter()
        .map(|(id, fixtures_set)| {
            let fixture_count = fixtures_set.len();
            let fixtures: Vec<String> = fixtures_set.into_iter().collect();
            let bundle_count = bundle_count_by_id.remove(&id).unwrap_or(0);
            let statement_weight = weight_by_id.remove(&id).unwrap_or(0);
            let fixture_fraction = frac(fixture_count, total_fixtures);
            let statement_weight_fraction = frac(statement_weight, total_statement_weight);
            let score = (fixture_fraction + statement_weight_fraction) / 2.0;
            ItemCoverage {
                id,
                fixture_count,
                fixture_fraction,
                fixtures,
                bundle_count,
                statement_weight,
                statement_weight_fraction,
                score,
            }
        })
        .collect();

    out.sort_by(|a, b| score_cmp(b.score, a.score).then_with(|| a.id.cmp(&b.id)));
    out
}

fn build_hot_paths(
    java: &[JavaClassCoverage],
    sql: &[ItemCoverage],
    go: &[ItemCoverage],
    limit: usize,
) -> Vec<HotPath> {
    let mut combined: Vec<HotPath> = java
        .iter()
        .map(|c| HotPath {
            kind: "java_class".to_string(),
            id: c.name.clone(),
            fixture_count: c.fixture_count,
            statement_weight: c.statement_weight,
            score: c.score,
        })
        .chain(sql.iter().map(|c| HotPath {
            kind: "sql_file".to_string(),
            id: c.id.clone(),
            fixture_count: c.fixture_count,
            statement_weight: c.statement_weight,
            score: c.score,
        }))
        .chain(go.iter().map(|c| HotPath {
            kind: "go_calculator".to_string(),
            id: c.id.clone(),
            fixture_count: c.fixture_count,
            statement_weight: c.statement_weight,
            score: c.score,
        }))
        .collect();

    combined.sort_by(|a, b| {
        score_cmp(b.score, a.score)
            .then_with(|| a.kind.cmp(&b.kind))
            .then_with(|| a.id.cmp(&b.id))
    });
    combined.truncate(limit);
    combined
}

/// Safe `numer / denom` returning `0.0` when `denom == 0` so an empty
/// suite doesn't produce NaN fractions in the JSON output.
fn frac(numer: usize, denom: usize) -> f64 {
    if denom == 0 {
        0.0
    } else {
        numer as f64 / denom as f64
    }
}

/// Total order on f64 scores. Scores in this module are always finite
/// (non-NaN, non-infinite) by construction — they're produced by `frac`
/// from finite integers. `partial_cmp` is therefore always `Some`, and
/// we fall back to `Equal` only as a belt-and-suspenders for any future
/// surprise.
fn score_cmp(a: f64, b: f64) -> std::cmp::Ordering {
    a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trace::{
        ExecutionTrace, GoCalculator, JavaClass, SqlFile, TraceSources, WorkerBundle,
    };
    use tempfile::tempdir;

    const TRACE_VERSION: &str = "moves-fixture-capture/v1";

    /// `(id, java_classes, sql_files, go_calculators, statement_count)`.
    type BundleSpec<'a> = (&'a str, &'a [&'a str], &'a [&'a str], &'a [&'a str], usize);

    fn trace(
        fixture: &str,
        classes: &[(&str, &str)],
        sqls: &[(&str, &[&str])],
        gos: &[(&str, &[&str])],
        bundles: &[BundleSpec<'_>],
    ) -> ExecutionTrace {
        ExecutionTrace {
            trace_version: TRACE_VERSION.to_string(),
            fixture_name: fixture.to_string(),
            sif_sha256: format!("sif-{fixture}"),
            runspec_sha256: format!("rs-{fixture}"),
            java_classes: classes
                .iter()
                .map(|(n, k)| JavaClass {
                    name: (*n).to_string(),
                    kind: (*k).to_string(),
                })
                .collect(),
            sql_files: sqls
                .iter()
                .map(|(p, by)| SqlFile {
                    path: (*p).to_string(),
                    consumed_by: by.iter().map(|s| (*s).to_string()).collect(),
                })
                .collect(),
            go_calculators: gos
                .iter()
                .map(|(n, by)| GoCalculator {
                    name: (*n).to_string(),
                    invoked_in: by.iter().map(|s| (*s).to_string()).collect(),
                })
                .collect(),
            worker_bundles: bundles
                .iter()
                .map(|(id, classes, sqls, gos, sc)| WorkerBundle {
                    id: (*id).to_string(),
                    java_classes: classes.iter().map(|s| (*s).to_string()).collect(),
                    sql_files: sqls.iter().map(|s| (*s).to_string()).collect(),
                    go_calculators: gos.iter().map(|s| (*s).to_string()).collect(),
                    statement_count: *sc,
                })
                .collect(),
            sources: TraceSources {
                worker_sql_files: bundles.len(),
                class_load_log_files: 0,
            },
        }
    }

    #[test]
    fn empty_input_yields_empty_but_valid_map() {
        let map = build_coverage_map(&[]);
        assert_eq!(map.coverage_version, COVERAGE_VERSION);
        assert_eq!(map.total_fixtures, 0);
        assert_eq!(map.total_statement_weight, 0);
        assert!(map.fixtures.is_empty());
        assert!(map.java_classes.is_empty());
        assert!(map.sql_files.is_empty());
        assert!(map.go_calculators.is_empty());
        assert!(map.hot_paths.is_empty());
    }

    #[test]
    fn single_trace_yields_full_fixture_fraction() {
        let t = trace(
            "f1",
            &[(
                "gov.epa.otaq.moves.master.calculator.BaseRateCalculator",
                "calculator",
            )],
            &[(
                "database/CalculatorSQL/BaseRateCalculator.sql",
                &["WorkerTemp00"],
            )],
            &[("BaseRateCalculator", &["WorkerTemp00"])],
            &[(
                "WorkerTemp00",
                &["gov.epa.otaq.moves.master.calculator.BaseRateCalculator"],
                &["database/CalculatorSQL/BaseRateCalculator.sql"],
                &["BaseRateCalculator"],
                10,
            )],
        );
        let map = build_coverage_map(&[t]);
        assert_eq!(map.total_fixtures, 1);
        assert_eq!(map.total_statement_weight, 10);
        assert_eq!(map.fixtures.len(), 1);
        assert_eq!(map.fixtures[0].fixture_name, "f1");
        assert_eq!(map.fixtures[0].statement_count, 10);

        assert_eq!(map.java_classes.len(), 1);
        let jc = &map.java_classes[0];
        assert_eq!(jc.kind, "calculator");
        assert_eq!(jc.fixture_count, 1);
        assert_eq!(jc.fixture_fraction, 1.0);
        assert_eq!(jc.bundle_count, 1);
        assert_eq!(jc.statement_weight, 10);
        assert_eq!(jc.statement_weight_fraction, 1.0);
        assert_eq!(jc.score, 1.0);

        assert_eq!(map.sql_files.len(), 1);
        assert_eq!(
            map.sql_files[0].id,
            "database/CalculatorSQL/BaseRateCalculator.sql"
        );
        assert_eq!(map.sql_files[0].score, 1.0);

        assert_eq!(map.go_calculators.len(), 1);
        assert_eq!(map.go_calculators[0].id, "BaseRateCalculator");

        // Hot paths span all three categories.
        assert_eq!(map.hot_paths.len(), 3);
        assert!(map.hot_paths.iter().any(|h| h.kind == "java_class"));
        assert!(map.hot_paths.iter().any(|h| h.kind == "sql_file"));
        assert!(map.hot_paths.iter().any(|h| h.kind == "go_calculator"));
    }

    #[test]
    fn aggregates_fixture_membership_across_traces() {
        // Two fixtures: one references BaseRate, the other references BaseRate + Criteria.
        let t1 = trace(
            "f1",
            &[(
                "gov.epa.otaq.moves.master.calculator.BaseRateCalculator",
                "calculator",
            )],
            &[],
            &[],
            &[(
                "WorkerTemp00",
                &["gov.epa.otaq.moves.master.calculator.BaseRateCalculator"],
                &[],
                &[],
                100,
            )],
        );
        let t2 = trace(
            "f2",
            &[
                (
                    "gov.epa.otaq.moves.master.calculator.BaseRateCalculator",
                    "calculator",
                ),
                (
                    "gov.epa.otaq.moves.master.calculator.CriteriaRunningCalculator",
                    "calculator",
                ),
            ],
            &[],
            &[],
            &[(
                "WorkerTemp00",
                &[
                    "gov.epa.otaq.moves.master.calculator.BaseRateCalculator",
                    "gov.epa.otaq.moves.master.calculator.CriteriaRunningCalculator",
                ],
                &[],
                &[],
                50,
            )],
        );
        let map = build_coverage_map(&[t1, t2]);
        assert_eq!(map.total_fixtures, 2);
        assert_eq!(map.total_statement_weight, 150);

        let base = map
            .java_classes
            .iter()
            .find(|c| c.name.ends_with("BaseRateCalculator"))
            .unwrap();
        assert_eq!(base.fixture_count, 2);
        assert_eq!(base.fixture_fraction, 1.0);
        assert_eq!(base.statement_weight, 150);
        assert_eq!(base.bundle_count, 2);
        // 1.0 (everywhere) + 1.0 (all weight) / 2 = 1.0
        assert_eq!(base.score, 1.0);

        let criteria = map
            .java_classes
            .iter()
            .find(|c| c.name.ends_with("CriteriaRunningCalculator"))
            .unwrap();
        assert_eq!(criteria.fixture_count, 1);
        assert_eq!(criteria.fixture_fraction, 0.5);
        assert_eq!(criteria.statement_weight, 50);
        assert!((criteria.statement_weight_fraction - 50.0 / 150.0).abs() < 1e-12);
        // Sorted by descending score; BaseRate must precede Criteria.
        assert_eq!(map.java_classes[0].name, base.name);
    }

    #[test]
    fn class_load_only_class_gets_zero_weight_but_nonzero_coverage() {
        // The Java class is in the top-level java_classes list (came from
        // a class-load log) but not in any bundle's worker.sql refs.
        let t = trace(
            "f1",
            &[("gov.epa.otaq.moves.utils.FileUtil", "utils")],
            &[],
            &[],
            &[("WorkerTemp00", &[], &[], &[], 25)],
        );
        let map = build_coverage_map(&[t]);
        let fu = map
            .java_classes
            .iter()
            .find(|c| c.name == "gov.epa.otaq.moves.utils.FileUtil")
            .unwrap();
        assert_eq!(fu.fixture_count, 1);
        assert_eq!(fu.bundle_count, 0);
        assert_eq!(fu.statement_weight, 0);
        assert_eq!(fu.statement_weight_fraction, 0.0);
        assert!((fu.fixture_fraction - 1.0).abs() < 1e-12);
        assert!((fu.score - 0.5).abs() < 1e-12);
    }

    #[test]
    fn sorted_by_score_descending_with_id_tiebreak() {
        // Two SQL files with identical coverage but different paths must
        // come back sorted by path alphabetically when scores tie.
        let t = trace(
            "f1",
            &[],
            &[
                ("database/aa.sql", &["WorkerTemp00"]),
                ("database/zz.sql", &["WorkerTemp00"]),
            ],
            &[],
            &[(
                "WorkerTemp00",
                &[],
                &["database/aa.sql", "database/zz.sql"],
                &[],
                10,
            )],
        );
        let map = build_coverage_map(&[t]);
        assert_eq!(map.sql_files.len(), 2);
        assert_eq!(map.sql_files[0].id, "database/aa.sql");
        assert_eq!(map.sql_files[1].id, "database/zz.sql");
    }

    #[test]
    fn hot_paths_truncated_to_limit() {
        // Build a single trace with > HOT_PATHS_LIMIT distinct SQL files.
        let n = HOT_PATHS_LIMIT + 10;
        let sql_ids: Vec<String> = (0..n).map(|i| format!("database/f{i:04}.sql")).collect();
        let sql_pairs: Vec<(&str, &[&str])> = sql_ids
            .iter()
            .map(|p| (p.as_str(), &["WorkerTemp00"] as &[&str]))
            .collect();
        let bundle_sqls: Vec<&str> = sql_ids.iter().map(String::as_str).collect();
        let bundle = (
            "WorkerTemp00",
            &[] as &[&str],
            bundle_sqls.as_slice(),
            &[] as &[&str],
            1usize,
        );
        let t = trace("f1", &[], &sql_pairs, &[], &[bundle]);
        let map = build_coverage_map(&[t]);
        assert_eq!(map.hot_paths.len(), HOT_PATHS_LIMIT);
    }

    #[test]
    fn write_and_read_round_trips() {
        let t = trace(
            "f1",
            &[(
                "gov.epa.otaq.moves.master.calculator.BaseRateCalculator",
                "calculator",
            )],
            &[],
            &[],
            &[(
                "WorkerTemp00",
                &["gov.epa.otaq.moves.master.calculator.BaseRateCalculator"],
                &[],
                &[],
                5,
            )],
        );
        let map = build_coverage_map(&[t]);

        let dir = tempdir().unwrap();
        let path = write_coverage_map(dir.path(), &map).unwrap();
        let bytes = std::fs::read(&path).unwrap();
        assert!(bytes.ends_with(b"\n"));
        let parsed: CoverageMap = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed, map);
    }

    #[test]
    fn write_coverage_map_is_byte_deterministic() {
        let t = trace(
            "f1",
            &[(
                "gov.epa.otaq.moves.master.calculator.BaseRateCalculator",
                "calculator",
            )],
            &[],
            &[],
            &[(
                "WorkerTemp00",
                &["gov.epa.otaq.moves.master.calculator.BaseRateCalculator"],
                &[],
                &[],
                5,
            )],
        );
        let map = build_coverage_map(&[t]);

        let dir = tempdir().unwrap();
        let p1 = write_coverage_map(dir.path(), &map).unwrap();
        let bytes1 = std::fs::read(&p1).unwrap();
        let p2 = write_coverage_map(dir.path(), &map).unwrap();
        let bytes2 = std::fs::read(&p2).unwrap();
        assert_eq!(bytes1, bytes2);
    }

    #[test]
    fn read_traces_dir_skips_subdirs_without_trace_file() {
        let dir = tempdir().unwrap();
        let t = trace(
            "a",
            &[("gov.epa.otaq.moves.utils.FileUtil", "utils")],
            &[],
            &[],
            &[],
        );

        // Subdir with trace.
        std::fs::create_dir_all(dir.path().join("a")).unwrap();
        let json = serde_json::to_vec_pretty(&t).unwrap();
        std::fs::write(dir.path().join("a").join(TRACE_FILE), json).unwrap();

        // Subdir without trace — must be skipped.
        std::fs::create_dir_all(dir.path().join("b")).unwrap();

        // Non-directory at root — must be ignored.
        std::fs::write(dir.path().join("README.md"), b"hi").unwrap();

        let got = read_traces_dir(dir.path()).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].fixture_name, "a");
    }

    #[test]
    fn read_traces_dir_returns_error_when_root_missing() {
        let dir = tempdir().unwrap();
        let missing = dir.path().join("nope");
        let err = read_traces_dir(&missing).unwrap_err();
        match err {
            Error::Io { .. } => {}
            other => panic!("expected Io error, got {other:?}"),
        }
    }

    #[test]
    fn read_traces_dir_walks_in_sorted_order() {
        let dir = tempdir().unwrap();
        for name in ["zz", "aa", "mm"] {
            let t = trace(name, &[], &[], &[], &[]);
            std::fs::create_dir_all(dir.path().join(name)).unwrap();
            let json = serde_json::to_vec_pretty(&t).unwrap();
            std::fs::write(dir.path().join(name).join(TRACE_FILE), json).unwrap();
        }
        let got = read_traces_dir(dir.path()).unwrap();
        let names: Vec<&str> = got.iter().map(|t| t.fixture_name.as_str()).collect();
        assert_eq!(names, vec!["aa", "mm", "zz"]);
    }

    #[test]
    fn hot_paths_rank_high_coverage_over_low_coverage() {
        // Three classes: one ubiquitous + heavy, one ubiquitous + light, one rare.
        let t1 = trace(
            "f1",
            &[
                (
                    "gov.epa.otaq.moves.master.calculator.BaseRateCalculator",
                    "calculator",
                ),
                (
                    "gov.epa.otaq.moves.master.calculator.LightHelper",
                    "calculator",
                ),
            ],
            &[],
            &[],
            &[(
                "WorkerTemp00",
                &[
                    "gov.epa.otaq.moves.master.calculator.BaseRateCalculator",
                    "gov.epa.otaq.moves.master.calculator.LightHelper",
                ],
                &[],
                &[],
                100,
            )],
        );
        let t2 = trace(
            "f2",
            &[
                (
                    "gov.epa.otaq.moves.master.calculator.BaseRateCalculator",
                    "calculator",
                ),
                (
                    "gov.epa.otaq.moves.master.calculator.LightHelper",
                    "calculator",
                ),
                (
                    "gov.epa.otaq.moves.master.calculator.RareEdgeCase",
                    "calculator",
                ),
            ],
            &[],
            &[],
            &[
                (
                    "WorkerTemp00",
                    &[
                        "gov.epa.otaq.moves.master.calculator.BaseRateCalculator",
                        "gov.epa.otaq.moves.master.calculator.LightHelper",
                    ],
                    &[],
                    &[],
                    1000,
                ),
                (
                    "WorkerTemp01",
                    &["gov.epa.otaq.moves.master.calculator.RareEdgeCase"],
                    &[],
                    &[],
                    10,
                ),
            ],
        );
        let map = build_coverage_map(&[t1, t2]);
        // Top hot path must be BaseRateCalculator (in every fixture, all heavy bundles).
        let top = &map.hot_paths[0];
        assert_eq!(top.kind, "java_class");
        assert!(top.id.ends_with("BaseRateCalculator"));
        // LightHelper appears in every fixture too but with less weight than BaseRate?
        // Actually LightHelper has identical bundle membership to BaseRate here so
        // it should tie with BaseRate on score; the deterministic tiebreaker is the id.
        // BaseRateCalculator < LightHelper alphabetically, so BaseRate comes first.
        assert_eq!(
            map.hot_paths[1].id,
            "gov.epa.otaq.moves.master.calculator.LightHelper"
        );
        // Rare edge case should be later: only 1 fixture (0.5 fraction) and only 10
        // statements (10/1110 ≈ 0.009 fraction) — score ≈ 0.255.
        let rare_pos = map
            .hot_paths
            .iter()
            .position(|h| h.id.ends_with("RareEdgeCase"))
            .expect("RareEdgeCase must be in hot paths");
        assert!(rare_pos > 1);
    }

    #[test]
    fn frac_handles_zero_denominator() {
        assert_eq!(frac(0, 0), 0.0);
        assert_eq!(frac(5, 0), 0.0);
        assert_eq!(frac(0, 5), 0.0);
        assert_eq!(frac(3, 6), 0.5);
    }
}
