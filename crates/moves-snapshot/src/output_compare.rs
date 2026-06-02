//! `MOVESOutput` comparison helpers — canonical snapshot vs. moves.rs run.
//!
//! The byte/cell-level [`diff_snapshots`](crate::diff_snapshots) contract is too
//! strict for comparing the Rust port's emission output against a canonical
//! MOVES snapshot: even when the *physics* matches to floating-point precision,
//! the two outputs disagree on metadata/labeling columns that do not affect the
//! emitted mass — notably `iterationID` (the port leaves it NULL), `roadTypeID`
//! (the port emits `0` where canonical emits the link's road type), and the
//! `SCC` string (whose road-type subfield therefore differs). Canonical also
//! carries `emissionQuantMean`/`emissionQuantSigma` (always NULL with
//! uncertainty off) where the port carries `emissionRate`/`runHash`.
//!
//! The established audit gate (`characterization/audit/regression_gate.sh`)
//! therefore compares **per-pollutant `emissionQuant` sums**: group by
//! `pollutantID`, sum `emissionQuant`, and require the two totals to agree
//! within a relative tolerance. That abstracts over row ordering, granularity,
//! and the labeling differences above while still catching real divergences in
//! the emitted mass. This module provides that comparison as a reusable
//! function so both the `compare-canonical` binary and the full-suite
//! regression gate share one implementation.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io;
use std::path::Path;

use arrow::array::{Array, Float64Array, Int16Array};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::snapshot::Snapshot;
use crate::table::NormalizedColumn;

/// `pollutantID → Σ emissionQuant` plus the total `MOVESOutput` row count.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PollutantSums {
 /// Sum of `emissionQuant` for each `pollutantID` present in the table.
    pub sums: BTreeMap<i64, f64>,
 /// Total number of `MOVESOutput` rows the sums were accumulated from.
    pub row_count: usize,
}

/// One pollutant's canonical-vs-port comparison.
#[derive(Debug, Clone, PartialEq)]
pub struct PollutantRow {
    pub pollutant_id: i64,
    pub canonical: f64,
    pub port: f64,
 /// `port - canonical`.
    pub delta: f64,
 /// `delta / max(|canonical|, abs_floor)` — signed relative difference.
    pub rel_diff: f64,
}

/// Result of [`compare_pollutant_sums`].
#[derive(Debug, Clone, PartialEq)]
pub struct PollutantComparison {
    pub rows: Vec<PollutantRow>,
    pub canonical_row_count: usize,
    pub port_row_count: usize,
 /// Largest `|delta|` across all pollutants.
    pub max_abs_delta: f64,
 /// Largest `|rel_diff|` across all pollutants (signed value carried through).
    pub max_rel_diff: f64,
}

impl PollutantComparison {
 /// `true` when every pollutant's `|rel_diff| <= rel_tol`.
    pub fn within(&self, rel_tol: f64) -> bool {
        self.max_rel_diff.abs() <= rel_tol
    }
}

/// Find the `MOVESOutput` (or, failing that, `MOVESActivityOutput`) table in a
/// canonical snapshot. The output database name varies per fixture
/// (`db__out_<fixture>__movesoutput`, `db__junittestoutput__movesoutput`, …) so
/// the table is located by suffix.
fn find_output_table_name(snap: &Snapshot) -> Option<String> {
    snap.table_names()
        .find(|n| n.ends_with("__movesoutput"))
        .map(str::to_string)
        .or_else(|| {
            snap.table_names()
                .find(|n| n.ends_with("__movesactivityoutput"))
                .map(str::to_string)
        })
}

/// Accumulate `pollutantID → Σ emissionQuant` from a canonical snapshot's
/// `MOVESOutput` table. Returns empty sums (row count `0`) when the snapshot has
/// no output table or it lacks the expected columns.
pub fn pollutant_sums_from_snapshot(snap: &Snapshot) -> PollutantSums {
    let mut out = PollutantSums::default();

    let table = match find_output_table_name(snap).and_then(|name| snap.table(&name).cloned()) {
        Some(t) => t,
        None => return out,
    };
    out.row_count = table.row_count();

    let (pid_idx, eq_idx) = match (
        table.column_index("pollutantID"),
        table.column_index("emissionQuant"),
    ) {
        (Some(p), Some(e)) => (p, e),
        _ => return out,
    };

    let cols = table.columns();
    let pid_col = &cols[pid_idx];
    let eq_col = &cols[eq_idx];

    for row in 0..table.row_count() {
        let pid = match pid_col {
            NormalizedColumn::Int64(v) => match v[row] {
                Some(n) => n,
                None => continue,
            },
            _ => continue,
        };
        let eq = match eq_col {
            NormalizedColumn::Float64String(v) => match &v[row] {
                Some(s) => match s.parse::<f64>() {
                    Ok(v) if v.is_finite() => v,
                    _ => continue,
                },
                None => continue,
            },
            _ => continue,
        };
 *out.sums.entry(pid).or_insert(0.0) += eq;
    }

    out
}

/// For each pollutant in `pollutants`, count the canonical `MOVESOutput` rows
/// whose `emissionQuant` is exactly zero.
///
/// The engine drops an upstream producer's zero-valued row for any pollutant a
/// chained calculator consumes and replaces (e.g. SulfatePM's EC 112 / NonECPM
/// 118), because canonical's `delete from MOVESWorkerOutput` removed it and the
/// additive chained delta cannot. That is exact only while canonical itself
/// never emits a zero row for such a pollutant. The canonical-snapshot gate
/// calls this to assert the premise holds for every captured snapshot — a
/// non-empty result means the drop heuristic is invalid for that fixture and
/// must be revisited, rather than silently producing a row-count divergence.
#[must_use]
pub fn zero_valued_replaced_rows(
    snap: &Snapshot,
    pollutants: &BTreeSet<i64>,
) -> BTreeMap<i64, usize> {
    let mut counts: BTreeMap<i64, usize> = BTreeMap::new();
    if pollutants.is_empty() {
        return counts;
    }
    let table = match find_output_table_name(snap).and_then(|name| snap.table(&name).cloned()) {
        Some(t) => t,
        None => return counts,
    };
    let (pid_idx, eq_idx) = match (
        table.column_index("pollutantID"),
        table.column_index("emissionQuant"),
    ) {
        (Some(p), Some(e)) => (p, e),
        _ => return counts,
    };
    let cols = table.columns();
    let pid_col = &cols[pid_idx];
    let eq_col = &cols[eq_idx];
    for row in 0..table.row_count() {
        let pid = match pid_col {
            NormalizedColumn::Int64(v) => match v[row] {
                Some(n) => n,
                None => continue,
            },
            _ => continue,
        };
        if !pollutants.contains(&pid) {
            continue;
        }
        // Mirror `pollutant_sums_from_snapshot`'s view of the same column: a NULL
        // or unparseable/non-finite `emissionQuant` is skipped there, so it must
        // not be counted as a zero-valued row here either, or the premise guard
        // and the emission-sum comparison would disagree on which rows count. A
        // row is a genuine zero only when it parses to a finite `0.0`.
        let eq = match eq_col {
            NormalizedColumn::Float64String(v) => match &v[row] {
                Some(s) => match s.parse::<f64>() {
                    Ok(v) if v.is_finite() => v,
                    _ => continue,
                },
                None => continue,
            },
            _ => continue,
        };
        if eq == 0.0 {
            *counts.entry(pid).or_insert(0) += 1;
        }
    }
    counts
}

/// Accumulate `pollutantID → Σ emissionQuant` from a moves.rs run's
/// `MOVESOutput/` partitioned Parquet tree (`<output_dir>/MOVESOutput/**/*.parquet`).
/// Returns empty sums when the directory does not exist.
pub fn pollutant_sums_from_output_dir(output_dir: &Path) -> io::Result<PollutantSums> {
    let mut out = PollutantSums::default();
    let moves_output_dir = output_dir.join("MOVESOutput");
    if !moves_output_dir.exists() {
        return Ok(out);
    }
    let mut files = Vec::new();
    collect_parquet_files(&moves_output_dir, &mut files)?;
    files.sort();
    for path in &files {
        out.row_count += accumulate_output_file(path, &mut out.sums)?;
    }
    Ok(out)
}

fn collect_parquet_files(dir: &Path, files: &mut Vec<std::path::PathBuf>) -> io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            collect_parquet_files(&path, files)?;
        } else if path.extension().is_some_and(|e| e == "parquet") {
            files.push(path);
        }
    }
    Ok(())
}

fn accumulate_output_file(path: &Path, sums: &mut BTreeMap<i64, f64>) -> io::Result<usize> {
    let file = File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
        .build()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let mut total_rows = 0;
    for batch in reader {
        let batch = batch.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        total_rows += batch.num_rows();
        let schema = batch.schema();
        let (pid_idx, eq_idx) = match (
            schema.index_of("pollutantID"),
            schema.index_of("emissionQuant"),
        ) {
            (Ok(p), Ok(e)) => (p, e),
            _ => continue,
        };
        let pids = batch.column(pid_idx).as_any().downcast_ref::<Int16Array>();
        let eqs = batch.column(eq_idx).as_any().downcast_ref::<Float64Array>();
        let (pids, eqs) = match (pids, eqs) {
            (Some(p), Some(e)) => (p, e),
            _ => continue,
        };
        for i in 0..batch.num_rows() {
            if pids.is_null(i) || eqs.is_null(i) {
                continue;
            }
            let eq = eqs.value(i);
            if eq.is_finite() {
 *sums.entry(pids.value(i) as i64).or_insert(0.0) += eq;
            }
        }
    }
    Ok(total_rows)
}

/// Compare canonical and port per-pollutant `emissionQuant` sums.
///
/// `abs_floor` guards the relative-difference denominator so a pollutant that
/// is exactly `0` in canonical does not produce an infinite `rel_diff`; pass a
/// small positive value (e.g. `1e-30`).
pub fn compare_pollutant_sums(
    canonical: &PollutantSums,
    port: &PollutantSums,
    abs_floor: f64,
) -> PollutantComparison {
    let ids: BTreeSet<i64> = canonical
        .sums
        .keys()
        .chain(port.sums.keys())
        .copied()
        .collect();

    let mut rows = Vec::with_capacity(ids.len());
    let mut max_abs_delta = 0.0_f64;
    let mut max_rel_diff = 0.0_f64;

    for pid in ids {
        let can = canonical.sums.get(&pid).copied().unwrap_or(0.0);
        let prt = port.sums.get(&pid).copied().unwrap_or(0.0);
        let delta = prt - can;
        let rel_diff = delta / can.abs().max(abs_floor);
        if delta.abs() > max_abs_delta {
            max_abs_delta = delta.abs();
        }
        if rel_diff.abs() > max_rel_diff.abs() {
            max_rel_diff = rel_diff;
        }
        rows.push(PollutantRow {
            pollutant_id: pid,
            canonical: can,
            port: prt,
            delta,
            rel_diff,
        });
    }

    PollutantComparison {
        rows,
        canonical_row_count: canonical.row_count,
        port_row_count: port.row_count,
        max_abs_delta,
        max_rel_diff,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::ColumnKind;
    use crate::table::{TableBuilder, Value};
    use crate::Snapshot;

    fn snap_with_output(rows: &[(i64, &str)]) -> Snapshot {
        let mut b = TableBuilder::new(
            "db__out_demo__movesoutput",
            [
                ("pollutantID".to_string(), ColumnKind::Int64),
                ("emissionQuant".to_string(), ColumnKind::Float64),
            ],
        )
        .unwrap();
        for (pid, eq) in rows {
            b.push_row([Value::Int64(*pid), Value::Float64(eq.parse().unwrap())])
                .unwrap();
        }
        let mut s = Snapshot::new();
        s.add_table(b.build().unwrap()).unwrap();
        s
    }

    #[test]
    fn snapshot_sums_group_by_pollutant() {
        let s = snap_with_output(&[(1, "10.0"), (1, "5.5"), (2, "3.0")]);
        let sums = pollutant_sums_from_snapshot(&s);
        assert_eq!(sums.row_count, 3);
        assert_eq!(sums.sums.get(&1).copied().unwrap(), 15.5);
        assert_eq!(sums.sums.get(&2).copied().unwrap(), 3.0);
    }

    #[test]
    fn empty_snapshot_yields_no_sums() {
        let sums = pollutant_sums_from_snapshot(&Snapshot::new());
        assert_eq!(sums, PollutantSums::default());
    }

    #[test]
    fn zero_valued_replaced_rows_counts_only_replaced_zeros() {
        // pollutant 112 has one zero row; 118 has none; 119 (not replaced) has
        // a zero row that must be ignored.
        let s = snap_with_output(&[
            (112, "5.0"),
            (112, "0.0"),
            (118, "3.0"),
            (119, "0.0"),
        ]);
        let replaced: BTreeSet<i64> = [112, 118].into_iter().collect();
        let zeros = zero_valued_replaced_rows(&s, &replaced);
        assert_eq!(zeros.get(&112).copied(), Some(1));
        assert!(!zeros.contains_key(&118), "118 has no zero row");
        assert!(!zeros.contains_key(&119), "119 is not a replaced pollutant");
    }

    #[test]
    fn zero_valued_replaced_rows_empty_set_short_circuits() {
        let s = snap_with_output(&[(112, "0.0")]);
        assert!(zero_valued_replaced_rows(&s, &BTreeSet::new()).is_empty());
    }

    #[test]
    fn missing_output_dir_yields_empty() {
        let dir = tempfile::tempdir().unwrap();
        let sums = pollutant_sums_from_output_dir(dir.path()).unwrap();
        assert_eq!(sums.row_count, 0);
        assert!(sums.sums.is_empty());
    }

    #[test]
    fn comparison_flags_within_and_beyond_tolerance() {
        let can = PollutantSums {
            sums: BTreeMap::from([(1, 100.0), (2, 50.0)]),
            row_count: 2,
        };
 // pollutant 1 within 1e-4, pollutant 2 off by 20%.
        let port = PollutantSums {
            sums: BTreeMap::from([(1, 100.005), (2, 60.0)]),
            row_count: 2,
        };
        let cmp = compare_pollutant_sums(&can, &port, 1e-30);
        assert!(!cmp.within(1e-4));
        assert!((cmp.max_rel_diff - 0.2).abs() < 1e-9);

        let port_close = PollutantSums {
            sums: BTreeMap::from([(1, 100.000001), (2, 50.000001)]),
            row_count: 2,
        };
        let cmp2 = compare_pollutant_sums(&can, &port_close, 1e-30);
        assert!(cmp2.within(1e-4));
    }

    #[test]
    fn zero_canonical_does_not_divide_by_zero() {
        let can = PollutantSums {
            sums: BTreeMap::from([(100, 0.0)]),
            row_count: 1,
        };
        let port = PollutantSums {
            sums: BTreeMap::from([(100, 5.0)]),
            row_count: 1,
        };
        let cmp = compare_pollutant_sums(&can, &port, 1e-30);
        assert!(cmp.max_rel_diff.is_finite());
        assert!(cmp.max_rel_diff > 0.0);
    }
}
