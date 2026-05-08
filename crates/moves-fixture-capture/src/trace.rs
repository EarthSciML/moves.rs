//! Per-fixture execution trace.
//!
//! Phase 0 Task 8 (bead `mo-d7or`) deliverable. The trace is the bridge
//! between Phase 0 (fixture-snapshot regression baseline) and Phase 1
//! (coverage analysis + migration ordering, Task 9). Where the snapshot
//! answers "what numbers did MOVES produce?", the trace answers "which
//! pieces of MOVES were exercised producing them?":
//!
//! * **Java classes** in the `gov.epa.otaq.moves.*` package space that
//!   were loaded (and therefore at least referenced) during the run,
//!   tagged by hierarchy (`calculator`, `generator`, `framework`,
//!   `worker`, `master`, `common`, `utils`, `other`).
//! * **SQL files** referenced by `worker.sql` bundles (the macro-expanded
//!   files routed through `SQLMacroExpander`).
//! * **Go calculator** invocations, observed via filename markers in the
//!   worker temp directories.
//! * **Per-bundle breakdown** linking each `WorkerTempXX/` to the classes,
//!   SQL files, and Go calculators it touched.
//!
//! ## Inputs
//!
//! The trace is assembled post-hoc from the same captures directory that
//! [`build_snapshot`](crate::build_snapshot) reads:
//!
//! ```text
//! <captures-dir>/
//!   moves-temporary/
//!     instrumentation/
//!       class-load-*.log         ← JVM `-Xlog:class+load=info` output
//!     ...
//!   worker-folder/
//!     WorkerTemp00/
//!       worker.sql               ← parsed for SQL-file refs + class names
//!       ...                      ← walked for Go-calc filename markers
//!     WorkerTemp01/
//!       ...
//! ```
//!
//! The `instrumentation/` subdirectory is created by
//! `characterization/apptainer/run-fixture.sh`, which sets
//! `JAVA_TOOL_OPTIONS=-Xlog:class+load=info:file=...` so every JVM
//! launched during the run (ant + the forked MOVES JVM) deposits its
//! class-load events there. Files outside `gov.epa.otaq.moves.*` are
//! filtered out by [`parse_class_load_log`].
//!
//! ## Determinism
//!
//! Every aggregate field in [`ExecutionTrace`] is sorted lexicographically.
//! The walk over `worker-folder/` is sorted (via [`crate::tree::walk_files`])
//! and the parser does not depend on iteration order. The same captures
//! directory always produces a byte-identical `execution-trace.json`.
//!
//! ## Stability
//!
//! The parsers tolerate missing inputs. A captures directory with no
//! `worker-folder/` and no `instrumentation/` yields a valid (empty)
//! trace rather than an error. Phase 0 fixtures that don't fork worker
//! bundles (e.g. some chain-coverage fixtures that finish entirely
//! master-side) will produce a trace whose Java-class set comes only
//! from the JVM class-load log.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::tree::walk_files;

const TRACE_VERSION: &str = "moves-fixture-capture/v1";
const TRACE_FILE: &str = "execution-trace.json";
const MOVES_TEMPORARY_SUBDIR: &str = "moves-temporary";
const WORKER_FOLDER_SUBDIR: &str = "worker-folder";
const INSTRUMENTATION_SUBDIR: &str = "instrumentation";
const WORKER_SQL_FILE: &str = "worker.sql";

/// Class FQN prefix the trace cares about. Other classes (JDK, ant,
/// third-party libraries) are filtered out — not because they aren't
/// loaded but because Phase 1 coverage analysis only reasons about MOVES
/// code.
const MOVES_PACKAGE_PREFIX: &str = "gov.epa.otaq.moves.";

/// Per-fixture execution trace.
///
/// Sibling of [`crate::Provenance`]: written next to the snapshot's
/// `manifest.json` as `execution-trace.json`. Phase 1 (coverage analysis,
/// Task 9) consumes this directly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionTrace {
    /// Sidecar format version. Bumped on incompatible schema changes so
    /// downstream consumers can fail fast on unfamiliar shapes.
    pub trace_version: String,

    /// Fixture identifier. Mirrors `Provenance::fixture_name` so a trace
    /// can be associated back to its snapshot without reading provenance.
    pub fixture_name: String,

    /// SHA256 of the SIF the run executed against. Mirrors
    /// `Provenance::sif_sha256` for the same reason.
    pub sif_sha256: String,

    /// SHA256 of the RunSpec the trace describes.
    pub runspec_sha256: String,

    /// MOVES Java classes touched by the run, sorted alphabetically by
    /// `name`. Sourced from JVM class-load logs and `worker.sql` files.
    pub java_classes: Vec<JavaClass>,

    /// SQL macro/template files referenced by worker bundles, sorted
    /// alphabetically by `path`. The `consumed_by` list names every
    /// worker-bundle id that referenced the file.
    pub sql_files: Vec<SqlFile>,

    /// Go-calculator invocations observed via worker-temp filename
    /// markers (e.g. `*.go.input`, `*.go.output`, `calcgo*`). Sorted
    /// alphabetically by `name`.
    pub go_calculators: Vec<GoCalculator>,

    /// Per-worker-bundle breakdown, sorted alphabetically by `id`.
    pub worker_bundles: Vec<WorkerBundle>,

    /// Inventory of source artifacts the trace was assembled from.
    pub sources: TraceSources,
}

/// One MOVES Java class observed during the run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JavaClass {
    /// Fully qualified class name, e.g.
    /// `gov.epa.otaq.moves.master.framework.Generator`.
    pub name: String,
    /// Hierarchy hint derived from the package path. One of:
    /// `calculator`, `generator`, `framework`, `worker`, `master`,
    /// `common`, `utils`, `other`. Not load-bearing — the FQN is the
    /// canonical identity.
    pub kind: String,
}

/// One SQL file referenced by a worker bundle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlFile {
    /// Path as it appeared in `worker.sql`. Typically MOVES-relative
    /// (e.g. `database/CalculatorSQL/CriteriaRunningCalculator.sql`),
    /// but the parser stores whatever the bundle wrote — Phase 1 can
    /// canonicalize if needed.
    pub path: String,
    /// Worker-bundle ids that consumed this file, sorted alphabetically.
    pub consumed_by: Vec<String>,
}

/// Go-calculator invocation derived from worker temp filename markers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoCalculator {
    /// Calculator name (the basename stem before the `.go.*` suffix).
    pub name: String,
    /// Worker-bundle ids that invoked it, sorted alphabetically.
    pub invoked_in: Vec<String>,
}

/// Per-worker-bundle execution detail.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerBundle {
    /// Worker bundle id (the directory name under `worker-folder/`,
    /// typically `WorkerTemp00`, `WorkerTemp01`, ...).
    pub id: String,
    /// MOVES Java classes referenced in this bundle's `worker.sql`,
    /// sorted alphabetically.
    pub java_classes: Vec<String>,
    /// SQL files referenced in this bundle's `worker.sql`, sorted
    /// alphabetically.
    pub sql_files: Vec<String>,
    /// Go calculator names invoked (filename markers in this bundle's
    /// directory), sorted alphabetically.
    pub go_calculators: Vec<String>,
    /// Best-effort statement count for `worker.sql`: non-empty,
    /// non-comment lines. Useful for Phase 1 to weight bundles.
    pub statement_count: usize,
}

/// Inventory of source files the trace builder consumed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceSources {
    /// Number of `worker.sql` files parsed.
    pub worker_sql_files: usize,
    /// Number of JVM class-load log files parsed.
    pub class_load_log_files: usize,
}

/// Inputs required to build an [`ExecutionTrace`]. The captures dir
/// contents are walked; the SHA fields are mirrored verbatim.
pub struct TraceInputs<'a> {
    /// Captures root, same path passed to [`crate::build_snapshot`].
    pub captures_dir: &'a Path,
    /// Filename-derived fixture identifier (matches provenance).
    pub fixture_name: &'a str,
    /// SHA256 of the SIF the run executed against.
    pub sif_sha256: &'a str,
    /// SHA256 of the RunSpec.
    pub runspec_sha256: &'a str,
}

/// Build an [`ExecutionTrace`] from a captures directory.
pub fn build_execution_trace(inputs: &TraceInputs<'_>) -> Result<ExecutionTrace> {
    let mut classes_global: BTreeSet<String> = BTreeSet::new();
    let mut sql_to_bundles: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut go_to_bundles: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut bundles: BTreeMap<String, WorkerBundle> = BTreeMap::new();

    let mut worker_sql_files = 0usize;
    let mut class_load_log_files = 0usize;

    // 1. Worker bundles.
    let worker_root = inputs.captures_dir.join(WORKER_FOLDER_SUBDIR);
    if worker_root.exists() {
        let bundle_dirs = list_immediate_subdirs(&worker_root)?;
        for (bundle_id, bundle_path) in bundle_dirs {
            let mut bundle_classes: BTreeSet<String> = BTreeSet::new();
            let mut bundle_sql: BTreeSet<String> = BTreeSet::new();
            let mut bundle_go: BTreeSet<String> = BTreeSet::new();
            let mut statement_count = 0usize;

            // worker.sql is the primary signal.
            let worker_sql = bundle_path.join(WORKER_SQL_FILE);
            if worker_sql.is_file() {
                worker_sql_files += 1;
                let bytes = std::fs::read(&worker_sql).map_err(|source| Error::Io {
                    path: worker_sql.clone(),
                    source,
                })?;
                let text = std::str::from_utf8(&bytes).map_err(|source| Error::Parse {
                    path: worker_sql.clone(),
                    line: 0,
                    message: format!("worker.sql is not utf-8: {source}"),
                })?;
                let parsed = parse_worker_sql(text);
                bundle_classes.extend(parsed.classes);
                bundle_sql.extend(parsed.sql_files);
                bundle_go.extend(parsed.go_calculators);
                statement_count = parsed.statement_count;
            }

            // Filename markers in the bundle dir surface Go calculators
            // that worker.sql may not have referenced explicitly (e.g.
            // when MOVES drops a *.go.input file as part of dispatching
            // a Go calculator binary).
            let bundle_files = walk_files(&bundle_path)?;
            for entry in &bundle_files {
                if let Some(go_name) = go_calculator_from_filename(&entry.relative) {
                    bundle_go.insert(go_name);
                }
            }

            // Roll into globals.
            for class in &bundle_classes {
                classes_global.insert(class.clone());
            }
            for sql in &bundle_sql {
                sql_to_bundles
                    .entry(sql.clone())
                    .or_default()
                    .insert(bundle_id.clone());
            }
            for go in &bundle_go {
                go_to_bundles
                    .entry(go.clone())
                    .or_default()
                    .insert(bundle_id.clone());
            }

            bundles.insert(
                bundle_id.clone(),
                WorkerBundle {
                    id: bundle_id,
                    java_classes: bundle_classes.into_iter().collect(),
                    sql_files: bundle_sql.into_iter().collect(),
                    go_calculators: bundle_go.into_iter().collect(),
                    statement_count,
                },
            );
        }
    }

    // 2. JVM class-load logs from MOVESTemporary/instrumentation/.
    let instrumentation_root = inputs
        .captures_dir
        .join(MOVES_TEMPORARY_SUBDIR)
        .join(INSTRUMENTATION_SUBDIR);
    if instrumentation_root.exists() {
        let log_files = walk_files(&instrumentation_root)?;
        for entry in &log_files {
            // Only consider files that look like JVM logs. The exact
            // filename pattern depends on the host (`%p` substitutes the
            // PID, so `class-load-12345.log` is typical) — accept any
            // .log under the directory.
            let lower = entry.relative.to_ascii_lowercase();
            if !lower.ends_with(".log") {
                continue;
            }
            class_load_log_files += 1;
            let bytes = std::fs::read(&entry.absolute).map_err(|source| Error::Io {
                path: entry.absolute.clone(),
                source,
            })?;
            let text = match std::str::from_utf8(&bytes) {
                Ok(t) => t,
                Err(source) => {
                    return Err(Error::Parse {
                        path: entry.absolute.clone(),
                        line: 0,
                        message: format!("class-load log is not utf-8: {source}"),
                    });
                }
            };
            for class in parse_class_load_log(text) {
                classes_global.insert(class);
            }
        }
    }

    // 3. Materialize aggregates.
    let java_classes: Vec<JavaClass> = classes_global
        .into_iter()
        .map(|name| {
            let kind = classify_java_class(&name).to_string();
            JavaClass { name, kind }
        })
        .collect();

    let sql_files: Vec<SqlFile> = sql_to_bundles
        .into_iter()
        .map(|(path, consumed_by)| SqlFile {
            path,
            consumed_by: consumed_by.into_iter().collect(),
        })
        .collect();

    let go_calculators: Vec<GoCalculator> = go_to_bundles
        .into_iter()
        .map(|(name, invoked_in)| GoCalculator {
            name,
            invoked_in: invoked_in.into_iter().collect(),
        })
        .collect();

    let worker_bundles: Vec<WorkerBundle> = bundles.into_values().collect();

    Ok(ExecutionTrace {
        trace_version: TRACE_VERSION.to_string(),
        fixture_name: inputs.fixture_name.to_string(),
        sif_sha256: inputs.sif_sha256.to_string(),
        runspec_sha256: inputs.runspec_sha256.to_string(),
        java_classes,
        sql_files,
        go_calculators,
        worker_bundles,
        sources: TraceSources {
            worker_sql_files,
            class_load_log_files,
        },
    })
}

/// Write `trace` to `<snapshot_dir>/execution-trace.json` deterministically.
/// Pretty-printed with a trailing newline so the file is byte-stable across
/// re-runs of the same inputs (matches the snapshot crate's manifest and
/// the provenance sidecar).
pub fn write_execution_trace(snapshot_dir: &Path, trace: &ExecutionTrace) -> Result<PathBuf> {
    let path = snapshot_dir.join(TRACE_FILE);
    let mut bytes = serde_json::to_vec_pretty(trace).map_err(|source| Error::Json {
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

/// Parsed contents of a single `worker.sql` file.
struct WorkerSqlParse {
    classes: BTreeSet<String>,
    sql_files: BTreeSet<String>,
    go_calculators: BTreeSet<String>,
    statement_count: usize,
}

/// Scan a `worker.sql` text for MOVES-class references, SQL-file
/// references, Go calculator references, and a best-effort statement
/// count.
///
/// `worker.sql` is generated by MOVES master and shipped to a worker.
/// Its exact format varies by calculator chain, but two stable patterns
/// hold across MOVES versions:
///
/// * Java classes appear as fully qualified names embedded in
///   section-marker comments and macro-expansion preambles. We match
///   any token starting with `gov.epa.otaq.moves.`.
/// * SQL files appear as paths ending in `.sql` (case-insensitive),
///   either in section markers or in `--` line comments referencing the
///   source macro file.
///
/// Go calculator references in `worker.sql` are less standardized. We
/// look for `calcgo` substrings and `*.go.*` filename references; the
/// per-bundle filename walk supplements this.
fn parse_worker_sql(text: &str) -> WorkerSqlParse {
    let mut classes: BTreeSet<String> = BTreeSet::new();
    let mut sql_files: BTreeSet<String> = BTreeSet::new();
    let mut go_calculators: BTreeSet<String> = BTreeSet::new();
    let mut statement_count = 0usize;

    for raw_line in text.lines() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }
        if !line.starts_with("--") && !line.starts_with("/*") && !line.starts_with("#") {
            statement_count += 1;
        }
        for class in extract_moves_classes(line) {
            classes.insert(class);
        }
        for sql in extract_sql_paths(line) {
            sql_files.insert(sql);
        }
        for go in extract_go_calculator_refs(line) {
            go_calculators.insert(go);
        }
    }

    WorkerSqlParse {
        classes,
        sql_files,
        go_calculators,
        statement_count,
    }
}

/// Find every `gov.epa.otaq.moves.<...>` FQN in `line`. Tokens are
/// delimited by whitespace, common punctuation, and string quotes.
fn extract_moves_classes(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let bytes = line.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i..].starts_with(MOVES_PACKAGE_PREFIX.as_bytes()) {
            let start = i;
            let mut j = i + MOVES_PACKAGE_PREFIX.len();
            while j < bytes.len() && is_class_token_byte(bytes[j]) {
                j += 1;
            }
            // Trim a trailing dot, which would mean we ran into a
            // sentence-ending period rather than a real package
            // continuation.
            let mut end = j;
            while end > start && bytes[end - 1] == b'.' {
                end -= 1;
            }
            // A bare "gov.epa.otaq.moves." prefix with nothing after is
            // not a class — require at least one identifier segment past
            // the prefix.
            if end > start + MOVES_PACKAGE_PREFIX.len() {
                let token = &line[start..end];
                // Drop trailing single dot or non-identifier remnants
                // that may sneak in from the byte loop's view of the
                // string. (Defensive — `is_class_token_byte` already
                // filters most.)
                if looks_like_java_class_fqn(token) {
                    out.push(token.to_string());
                }
            }
            i = j;
        } else {
            i += 1;
        }
    }
    out
}

fn is_class_token_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'$' || b == b'.'
}

fn looks_like_java_class_fqn(s: &str) -> bool {
    if !s.starts_with(MOVES_PACKAGE_PREFIX) {
        return false;
    }
    let tail = &s[MOVES_PACKAGE_PREFIX.len()..];
    if tail.is_empty() {
        return false;
    }
    // Must contain at least one segment whose first char is alphabetic
    // (avoids matching e.g. `gov.epa.otaq.moves.123`).
    tail.split('.').all(|seg| {
        !seg.is_empty()
            && seg
                .chars()
                .next()
                .is_some_and(|c| c.is_alphabetic() || c == '_')
    })
}

/// Find tokens ending in `.sql` (case-insensitive). A token is any
/// non-whitespace run; we strip surrounding punctuation that's clearly
/// not part of a path (`'`, `"`, `(`, `)`, `,`, `;`).
fn extract_sql_paths(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    for raw in line.split(|c: char| c.is_whitespace()) {
        let token =
            raw.trim_matches(|c: char| matches!(c, '\'' | '"' | '(' | ')' | ',' | ';' | '<' | '>'));
        if token.len() < 5 {
            continue;
        }
        // Case-insensitive `.sql` suffix.
        let lower = token.to_ascii_lowercase();
        if !lower.ends_with(".sql") {
            continue;
        }
        // Reject obvious non-paths: must contain at least one `/`,
        // `\`, or look like a bare filename (no whitespace, no `=`).
        if token.contains('=') {
            continue;
        }
        out.push(token.to_string());
    }
    out
}

/// Find Go-calculator references in a single line. Matches:
/// * the literal substring `calcgo` (the MOVES Go binary stem)
/// * filenames ending in `.go.input` / `.go.output` / `.go.txt`
fn extract_go_calculator_refs(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let lower = line.to_ascii_lowercase();
    if lower.contains("calcgo") {
        out.push("calcgo".to_string());
    }
    for raw in line.split(|c: char| c.is_whitespace()) {
        let token =
            raw.trim_matches(|c: char| matches!(c, '\'' | '"' | '(' | ')' | ',' | ';' | '<' | '>'));
        if let Some(name) = go_calculator_from_filename(token) {
            out.push(name);
        }
    }
    out
}

/// Map a worker-folder filename to a Go calculator name, if any.
/// Recognized markers: `*.go.input`, `*.go.output`, `*.go.txt`. Returns
/// the leading stem (e.g. `BaseRateCalculator.go.input` →
/// `BaseRateCalculator`).
fn go_calculator_from_filename(rel_path: &str) -> Option<String> {
    let basename = rel_path.rsplit('/').next().unwrap_or(rel_path);
    let lower = basename.to_ascii_lowercase();
    for suffix in [".go.input", ".go.output", ".go.txt"] {
        if let Some(stem) = lower.strip_suffix(suffix) {
            if !stem.is_empty() {
                // Preserve original-case stem from the basename, not the
                // lowercased one, so the calculator name matches the MOVES
                // class capitalization.
                let stem_len = stem.len();
                return Some(basename[..stem_len].to_string());
            }
        }
    }
    None
}

/// Parse a JVM `-Xlog:class+load=info:file=...` log file and return the
/// MOVES classes loaded.
///
/// Two log shapes are supported:
///
/// * Java 11+ unified logging:
///   `[0.123s][info][class,load] gov.epa.otaq.moves.master.framework.Generator source: ...`
/// * Legacy `-verbose:class`: `[Loaded gov.epa.otaq.moves.master.framework.Generator from ...]`
///
/// We don't try to be cute about parsing — the `gov.epa.otaq.moves.`
/// prefix is unambiguous and never appears as a substring of unrelated
/// log content.
pub fn parse_class_load_log(text: &str) -> Vec<String> {
    let mut out: BTreeSet<String> = BTreeSet::new();
    for line in text.lines() {
        for class in extract_moves_classes(line) {
            out.insert(class);
        }
    }
    out.into_iter().collect()
}

/// Tag a MOVES Java class with a coarse hierarchy. The kind comes from
/// the segment immediately following the package prefix:
/// `gov.epa.otaq.moves.<kind>.<rest>`.
fn classify_java_class(fqn: &str) -> &'static str {
    let Some(tail) = fqn.strip_prefix(MOVES_PACKAGE_PREFIX) else {
        return "other";
    };
    // First segment after the prefix.
    let first = tail.split('.').next().unwrap_or("");
    match first {
        "master" => classify_master_subpackage(tail),
        "worker" => classify_worker_subpackage(tail),
        "common" => "common",
        "utils" => "utils",
        _ => "other",
    }
}

fn classify_master_subpackage(tail_after_prefix: &str) -> &'static str {
    // `master.<sub>...`
    let mut iter = tail_after_prefix.splitn(3, '.');
    let _ = iter.next(); // "master"
    let sub = iter.next().unwrap_or("");
    match sub {
        "calculator" => "calculator",
        "generator" => "generator",
        "framework" => "framework",
        _ => "master",
    }
}

fn classify_worker_subpackage(tail_after_prefix: &str) -> &'static str {
    let mut iter = tail_after_prefix.splitn(3, '.');
    let _ = iter.next(); // "worker"
    let sub = iter.next().unwrap_or("");
    match sub {
        "calculator" => "calculator",
        "framework" => "framework",
        _ => "worker",
    }
}

/// List the immediate subdirectories of `dir` and sort by name.
fn list_immediate_subdirs(dir: &Path) -> Result<Vec<(String, PathBuf)>> {
    let mut out: Vec<(String, PathBuf)> = std::fs::read_dir(dir)
        .map_err(|source| Error::Io {
            path: dir.to_path_buf(),
            source,
        })?
        .filter_map(|r| r.ok())
        .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
        .map(|e| (e.file_name().to_string_lossy().into_owned(), e.path()))
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    fn write(root: &Path, rel: &str, body: &str) {
        let path = root.join(rel);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, body).unwrap();
    }

    #[test]
    fn classify_kinds_by_package_segment() {
        assert_eq!(
            classify_java_class("gov.epa.otaq.moves.master.calculator.CriteriaRunningCalculator"),
            "calculator"
        );
        assert_eq!(
            classify_java_class(
                "gov.epa.otaq.moves.master.generator.SourceBinDistributionGenerator"
            ),
            "generator"
        );
        assert_eq!(
            classify_java_class("gov.epa.otaq.moves.master.framework.Generator"),
            "framework"
        );
        assert_eq!(
            classify_java_class("gov.epa.otaq.moves.master.runspec.RunSpec"),
            "master"
        );
        assert_eq!(
            classify_java_class("gov.epa.otaq.moves.worker.calculator.CalculatorImpl"),
            "calculator"
        );
        assert_eq!(
            classify_java_class("gov.epa.otaq.moves.worker.framework.RemoteEmissionsCalculator"),
            "framework"
        );
        assert_eq!(
            classify_java_class("gov.epa.otaq.moves.worker.bundles.WorkerBundle"),
            "worker"
        );
        assert_eq!(
            classify_java_class("gov.epa.otaq.moves.utils.FileUtil"),
            "utils"
        );
        assert_eq!(
            classify_java_class("gov.epa.otaq.moves.common.Models"),
            "common"
        );
        assert_eq!(classify_java_class("gov.epa.otaq.moves.unknown.X"), "other");
        assert_eq!(classify_java_class("java.lang.Object"), "other");
    }

    #[test]
    fn extract_moves_classes_from_mixed_text() {
        let line =
            "-- Macro: gov.epa.otaq.moves.master.calculator.CriteriaRunningCalculator with gov.epa.otaq.moves.utils.FileUtil";
        let mut got = extract_moves_classes(line);
        got.sort();
        assert_eq!(
            got,
            vec![
                "gov.epa.otaq.moves.master.calculator.CriteriaRunningCalculator",
                "gov.epa.otaq.moves.utils.FileUtil",
            ]
        );
    }

    #[test]
    fn extract_moves_classes_handles_inner_class_dollar() {
        let line =
            "[0.5s][info][class,load] gov.epa.otaq.moves.master.framework.MasterLoop$Iter source: ...";
        let got = extract_moves_classes(line);
        assert_eq!(
            got,
            vec!["gov.epa.otaq.moves.master.framework.MasterLoop$Iter"]
        );
    }

    #[test]
    fn extract_moves_classes_strips_trailing_punctuation() {
        // A sentence-ending period after an FQN must not be folded in.
        let line = "Used gov.epa.otaq.moves.utils.FileUtil.";
        let got = extract_moves_classes(line);
        assert_eq!(got, vec!["gov.epa.otaq.moves.utils.FileUtil"]);
    }

    #[test]
    fn extract_moves_classes_rejects_bare_prefix() {
        let line = "gov.epa.otaq.moves. is not a class";
        let got = extract_moves_classes(line);
        assert!(got.is_empty(), "got {got:?}");
    }

    #[test]
    fn extract_sql_paths_finds_paths_in_comments() {
        let line = "-- @@@ source: database/CalculatorSQL/CriteriaRunningCalculator.sql";
        let got = extract_sql_paths(line);
        assert_eq!(
            got,
            vec!["database/CalculatorSQL/CriteriaRunningCalculator.sql"]
        );
    }

    #[test]
    fn extract_sql_paths_handles_quotes_and_punctuation() {
        let line = "EXEC 'database/foo/bar.sql';";
        let got = extract_sql_paths(line);
        assert_eq!(got, vec!["database/foo/bar.sql"]);
    }

    #[test]
    fn extract_sql_paths_skips_assignment_tokens() {
        // `key=value.sql` shouldn't be reported — too speculative.
        let line = "key=value.sql other/path/real.sql";
        let got = extract_sql_paths(line);
        assert_eq!(got, vec!["other/path/real.sql"]);
    }

    #[test]
    fn go_calculator_filename_extracts_stem() {
        assert_eq!(
            go_calculator_from_filename("BaseRateCalculator.go.input"),
            Some("BaseRateCalculator".to_string())
        );
        assert_eq!(
            go_calculator_from_filename("WorkerTemp00/RatesCalc.go.output"),
            Some("RatesCalc".to_string())
        );
        assert_eq!(
            go_calculator_from_filename("BasicRunningPMEmissionCalculator.go.txt"),
            Some("BasicRunningPMEmissionCalculator".to_string())
        );
        // Non-matching files return None.
        assert_eq!(go_calculator_from_filename("worker.sql"), None);
        assert_eq!(go_calculator_from_filename("Output.tbl"), None);
    }

    #[test]
    fn parse_worker_sql_extracts_classes_and_sql_files_and_counts_statements() {
        let text =
            "-- @@@ Calculator: gov.epa.otaq.moves.master.calculator.CriteriaRunningCalculator
-- @@@ source: database/CalculatorSQL/CriteriaRunningCalculator.sql

CREATE TABLE if not exists Output (a INT);
INSERT INTO Output VALUES (1);
INSERT INTO Output VALUES (2);

-- @@@ Sub-macro: gov.epa.otaq.moves.utils.FileUtil
-- @@@ helper: database/macros/aggregate.sql
SELECT * FROM Output;
";
        let parsed = parse_worker_sql(text);
        let classes: Vec<&str> = parsed.classes.iter().map(String::as_str).collect();
        assert_eq!(
            classes,
            vec![
                "gov.epa.otaq.moves.master.calculator.CriteriaRunningCalculator",
                "gov.epa.otaq.moves.utils.FileUtil",
            ]
        );
        let sql: Vec<&str> = parsed.sql_files.iter().map(String::as_str).collect();
        assert_eq!(
            sql,
            vec![
                "database/CalculatorSQL/CriteriaRunningCalculator.sql",
                "database/macros/aggregate.sql",
            ]
        );
        // Four non-comment, non-blank lines: `CREATE TABLE`, two
        // `INSERT`s, and `SELECT *`.
        assert_eq!(parsed.statement_count, 4);
    }

    #[test]
    fn parse_worker_sql_picks_up_calcgo_marker() {
        let text = "-- worker invokes calcgo for ratesopmodedist\n";
        let parsed = parse_worker_sql(text);
        assert!(parsed.go_calculators.contains("calcgo"));
    }

    #[test]
    fn parse_class_load_log_supports_unified_logging_format() {
        let text = "[0.001s][info][class,load] java.lang.Object source: shared objects file
[0.123s][info][class,load] gov.epa.otaq.moves.master.framework.Generator source: file:/opt/moves/build/...
[0.124s][info][class,load] gov.epa.otaq.moves.master.calculator.CriteriaRunningCalculator
";
        let got = parse_class_load_log(text);
        assert_eq!(
            got,
            vec![
                "gov.epa.otaq.moves.master.calculator.CriteriaRunningCalculator",
                "gov.epa.otaq.moves.master.framework.Generator",
            ]
        );
    }

    #[test]
    fn parse_class_load_log_supports_legacy_verbose_format() {
        let text = "[Loaded java.lang.String from /jdk/...]
[Loaded gov.epa.otaq.moves.utils.FileUtil from file:/opt/moves/build/...]
";
        let got = parse_class_load_log(text);
        assert_eq!(got, vec!["gov.epa.otaq.moves.utils.FileUtil"]);
    }

    #[test]
    fn build_trace_walks_worker_and_instrumentation() {
        let dir = tempdir().unwrap();
        write(
            dir.path(),
            "worker-folder/WorkerTemp00/worker.sql",
            "-- gov.epa.otaq.moves.master.calculator.CriteriaRunningCalculator\n\
             -- src: database/CalculatorSQL/CriteriaRunningCalculator.sql\n\
             SELECT 1;\n",
        );
        write(
            dir.path(),
            "worker-folder/WorkerTemp00/BaseRateCalculator.go.input",
            "go calc input data\n",
        );
        write(
            dir.path(),
            "worker-folder/WorkerTemp01/worker.sql",
            "-- gov.epa.otaq.moves.master.generator.SourceBinDistributionGenerator\n",
        );
        write(
            dir.path(),
            "moves-temporary/instrumentation/class-load-101.log",
            "[0.1s][info][class,load] gov.epa.otaq.moves.master.framework.Generator\n",
        );

        let inputs = TraceInputs {
            captures_dir: dir.path(),
            fixture_name: "samplerunspec",
            sif_sha256: "abc",
            runspec_sha256: "def",
        };
        let trace = build_execution_trace(&inputs).unwrap();
        assert_eq!(trace.fixture_name, "samplerunspec");
        assert_eq!(trace.sif_sha256, "abc");
        assert_eq!(trace.runspec_sha256, "def");

        let class_names: Vec<&str> = trace.java_classes.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            class_names,
            vec![
                "gov.epa.otaq.moves.master.calculator.CriteriaRunningCalculator",
                "gov.epa.otaq.moves.master.framework.Generator",
                "gov.epa.otaq.moves.master.generator.SourceBinDistributionGenerator",
            ]
        );

        // The single calculator SQL file should attribute to WorkerTemp00.
        assert_eq!(trace.sql_files.len(), 1);
        assert_eq!(
            trace.sql_files[0].path,
            "database/CalculatorSQL/CriteriaRunningCalculator.sql"
        );
        assert_eq!(trace.sql_files[0].consumed_by, vec!["WorkerTemp00"]);

        // Go calculator BaseRateCalculator detected via the *.go.input file.
        assert_eq!(trace.go_calculators.len(), 1);
        assert_eq!(trace.go_calculators[0].name, "BaseRateCalculator");
        assert_eq!(trace.go_calculators[0].invoked_in, vec!["WorkerTemp00"]);

        // Per-bundle detail.
        assert_eq!(trace.worker_bundles.len(), 2);
        assert_eq!(trace.worker_bundles[0].id, "WorkerTemp00");
        assert_eq!(
            trace.worker_bundles[0].java_classes,
            vec!["gov.epa.otaq.moves.master.calculator.CriteriaRunningCalculator"]
        );
        assert_eq!(trace.worker_bundles[0].statement_count, 1);

        // Source inventory.
        assert_eq!(trace.sources.worker_sql_files, 2);
        assert_eq!(trace.sources.class_load_log_files, 1);
    }

    #[test]
    fn build_trace_tolerates_empty_captures() {
        let dir = tempdir().unwrap();
        let inputs = TraceInputs {
            captures_dir: dir.path(),
            fixture_name: "f",
            sif_sha256: "s",
            runspec_sha256: "r",
        };
        let trace = build_execution_trace(&inputs).unwrap();
        assert!(trace.java_classes.is_empty());
        assert!(trace.sql_files.is_empty());
        assert!(trace.go_calculators.is_empty());
        assert!(trace.worker_bundles.is_empty());
        assert_eq!(trace.sources.worker_sql_files, 0);
        assert_eq!(trace.sources.class_load_log_files, 0);
    }

    #[test]
    fn write_execution_trace_is_byte_deterministic() {
        let dir = tempdir().unwrap();
        let trace = ExecutionTrace {
            trace_version: TRACE_VERSION.to_string(),
            fixture_name: "f".into(),
            sif_sha256: "s".into(),
            runspec_sha256: "r".into(),
            java_classes: vec![JavaClass {
                name: "gov.epa.otaq.moves.utils.FileUtil".into(),
                kind: "utils".into(),
            }],
            sql_files: vec![],
            go_calculators: vec![],
            worker_bundles: vec![],
            sources: TraceSources {
                worker_sql_files: 0,
                class_load_log_files: 0,
            },
        };
        let p1 = write_execution_trace(dir.path(), &trace).unwrap();
        let bytes1 = fs::read(&p1).unwrap();
        let p2 = write_execution_trace(dir.path(), &trace).unwrap();
        let bytes2 = fs::read(&p2).unwrap();
        assert_eq!(bytes1, bytes2);
        assert!(bytes1.ends_with(b"\n"));
    }

    #[test]
    fn write_execution_trace_round_trips() {
        let dir = tempdir().unwrap();
        let trace = ExecutionTrace {
            trace_version: TRACE_VERSION.to_string(),
            fixture_name: "f".into(),
            sif_sha256: "s".into(),
            runspec_sha256: "r".into(),
            java_classes: vec![JavaClass {
                name: "gov.epa.otaq.moves.master.calculator.X".into(),
                kind: "calculator".into(),
            }],
            sql_files: vec![SqlFile {
                path: "database/x.sql".into(),
                consumed_by: vec!["WorkerTemp00".into()],
            }],
            go_calculators: vec![GoCalculator {
                name: "Y".into(),
                invoked_in: vec!["WorkerTemp00".into()],
            }],
            worker_bundles: vec![WorkerBundle {
                id: "WorkerTemp00".into(),
                java_classes: vec!["gov.epa.otaq.moves.master.calculator.X".into()],
                sql_files: vec!["database/x.sql".into()],
                go_calculators: vec!["Y".into()],
                statement_count: 7,
            }],
            sources: TraceSources {
                worker_sql_files: 1,
                class_load_log_files: 0,
            },
        };
        write_execution_trace(dir.path(), &trace).unwrap();
        let bytes = fs::read(dir.path().join(TRACE_FILE)).unwrap();
        let parsed: ExecutionTrace = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed, trace);
    }

    #[test]
    fn build_trace_aggregates_consumers_across_bundles() {
        let dir = tempdir().unwrap();
        // Two bundles consume the same SQL file.
        write(
            dir.path(),
            "worker-folder/WorkerTemp00/worker.sql",
            "-- src: database/shared.sql\n",
        );
        write(
            dir.path(),
            "worker-folder/WorkerTemp01/worker.sql",
            "-- src: database/shared.sql\n",
        );
        let inputs = TraceInputs {
            captures_dir: dir.path(),
            fixture_name: "f",
            sif_sha256: "s",
            runspec_sha256: "r",
        };
        let trace = build_execution_trace(&inputs).unwrap();
        assert_eq!(trace.sql_files.len(), 1);
        assert_eq!(
            trace.sql_files[0].consumed_by,
            vec!["WorkerTemp00", "WorkerTemp01"]
        );
    }

    #[test]
    fn build_trace_skips_non_log_files_in_instrumentation() {
        let dir = tempdir().unwrap();
        write(
            dir.path(),
            "moves-temporary/instrumentation/class-load.log",
            "[Loaded gov.epa.otaq.moves.utils.FileUtil from ...]\n",
        );
        // A non-.log file should be ignored.
        write(
            dir.path(),
            "moves-temporary/instrumentation/notes.txt",
            "gov.epa.otaq.moves.NOPE\n",
        );
        let inputs = TraceInputs {
            captures_dir: dir.path(),
            fixture_name: "f",
            sif_sha256: "s",
            runspec_sha256: "r",
        };
        let trace = build_execution_trace(&inputs).unwrap();
        // Only FileUtil from the .log; NOPE is skipped.
        let names: Vec<&str> = trace.java_classes.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["gov.epa.otaq.moves.utils.FileUtil"]);
        assert_eq!(trace.sources.class_load_log_files, 1);
    }
}
