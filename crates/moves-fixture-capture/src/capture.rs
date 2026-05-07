//! High-level orchestrator: walk a captures directory and build a
//! [`moves_snapshot::Snapshot`] from it.
//!
//! Captures-directory layout (produced by `run-fixture.sh`):
//!
//! ```text
//! <captures-dir>/
//!   databases/<db-name>/<table>.tsv          ← rows (mariadb -B -N -e SELECT ...)
//!   databases/<db-name>/<table>.schema.tsv   ← column metadata sidecar
//!   moves-temporary/<file>                   ← MOVESTemporary contents
//!   worker-folder/<workerN>/<file>           ← WorkerFolder/WorkerTempXX/ contents
//! ```
//!
//! Tables are named in the snapshot using `<source>__<segment>__<segment>`,
//! lowercased. Examples:
//!
//! | Source path                                       | Snapshot table name             |
//! |---------------------------------------------------|---------------------------------|
//! | `databases/movesoutput/movesactivityoutput.tsv`   | `db__movesoutput__movesactivityoutput` |
//! | `moves-temporary/SourceTypeYearVMT_2020.tbl`      | `moves_temporary__sourcetypeyearvmt_2020_tbl` |
//! | `worker-folder/WorkerTemp00/Output.tbl`           | `worker_folder__workertemp00__output_tbl` |
//!
//! The walk is deterministic (lexicographic), tables are inserted into a
//! `BTreeMap` keyed by the snapshot table name, and the snapshot crate's
//! own `write` is byte-deterministic — together giving the bead's "same
//! inputs → byte-identical snapshot" guarantee.

use std::path::{Path, PathBuf};

use moves_snapshot::Snapshot;

use crate::error::{Error, Result};
use crate::tabular::{
    join_table_name, parse_mariadb_table, parse_schema_tsv, parse_worker_tbl, read_file,
    sanitize_segment,
};
use crate::tree::walk_files;

const DATABASES_SUBDIR: &str = "databases";
const MOVES_TEMPORARY_SUBDIR: &str = "moves-temporary";
const WORKER_FOLDER_SUBDIR: &str = "worker-folder";

/// Configuration for [`build_snapshot`].
#[derive(Debug, Clone, Default)]
pub struct BuildOptions {
    /// If set, database directories whose name matches one of these
    /// (case-insensitively) are skipped. Use to exclude the default DB,
    /// which is unchanged by a run and would just bloat the snapshot.
    pub exclude_databases: Vec<String>,
}

impl BuildOptions {
    pub fn excluding_db<S: Into<String>>(mut self, db: S) -> Self {
        self.exclude_databases.push(db.into());
        self
    }

    fn is_excluded(&self, db: &str) -> bool {
        self.exclude_databases
            .iter()
            .any(|e| e.eq_ignore_ascii_case(db))
    }
}

/// Build a deterministic [`Snapshot`] from the captures directory at `root`.
///
/// Missing top-level subdirectories (`databases/`, `moves-temporary/`,
/// `worker-folder/`) are tolerated — a fixture that produces no worker
/// bundles still yields a valid snapshot covering its database state.
pub fn build_snapshot(root: &Path, opts: &BuildOptions) -> Result<Snapshot> {
    let mut snapshot = Snapshot::new();

    add_databases(root, opts, &mut snapshot)?;
    add_moves_temporary(root, &mut snapshot)?;
    add_worker_folder(root, &mut snapshot)?;

    Ok(snapshot)
}

fn add_databases(root: &Path, opts: &BuildOptions, snapshot: &mut Snapshot) -> Result<()> {
    let dbs_dir = root.join(DATABASES_SUBDIR);
    if !dbs_dir.exists() {
        return Ok(());
    }

    let mut db_dirs: Vec<(String, PathBuf)> = std::fs::read_dir(&dbs_dir)
        .map_err(|source| Error::Io {
            path: dbs_dir.clone(),
            source,
        })?
        .filter_map(|r| r.ok())
        .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
        .map(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            (name, e.path())
        })
        .collect();
    db_dirs.sort_by(|a, b| a.0.cmp(&b.0));

    for (db_name, db_path) in db_dirs {
        if opts.is_excluded(&db_name) {
            continue;
        }
        add_one_database(&db_name, &db_path, snapshot)?;
    }
    Ok(())
}

fn add_one_database(db_name: &str, db_path: &Path, snapshot: &mut Snapshot) -> Result<()> {
    // Group .tsv with .schema.tsv. We list once and dispatch.
    let entries = walk_files(db_path)?;
    let mut tables: std::collections::BTreeMap<String, (Option<PathBuf>, Option<PathBuf>)> =
        std::collections::BTreeMap::new();

    for entry in entries {
        let rel = &entry.relative;
        let abs = entry.absolute;

        if let Some(stem) = rel.strip_suffix(".schema.tsv") {
            tables.entry(stem.to_string()).or_default().1 = Some(abs);
        } else if let Some(stem) = rel.strip_suffix(".tsv") {
            tables.entry(stem.to_string()).or_default().0 = Some(abs);
        }
        // Other extensions (e.g. .sql from mariadb-dump) are ignored — the
        // snapshot format stores tabular data only.
    }

    for (stem, (data_path, schema_path)) in tables {
        let table_name = join_table_name(["db", db_name, &stem]);
        let (data_path, schema_path) = match (data_path, schema_path) {
            (Some(d), Some(s)) => (d, s),
            (Some(d), None) => {
                return Err(Error::CapturesMissing {
                    path: d,
                    subdir: format!("{stem}.schema.tsv"),
                });
            }
            (None, Some(s)) => {
                return Err(Error::CapturesMissing {
                    path: s,
                    subdir: format!("{stem}.tsv"),
                });
            }
            (None, None) => unreachable!(),
        };

        let schema_bytes = read_file(&schema_path)?;
        let schema_hints = parse_schema_tsv(&schema_path, &schema_bytes)?;
        let data_bytes = read_file(&data_path)?;
        let table = parse_mariadb_table(&data_path, &table_name, &schema_hints, &data_bytes)?;
        snapshot.add_table(table).map_err(|source| match source {
            moves_snapshot::Error::DuplicateTable { table } => Error::DuplicateTableName {
                name: table,
                path: data_path.clone(),
            },
            other => Error::Snapshot(other),
        })?;
    }
    Ok(())
}

fn add_moves_temporary(root: &Path, snapshot: &mut Snapshot) -> Result<()> {
    let dir = root.join(MOVES_TEMPORARY_SUBDIR);
    add_tabular_tree(&dir, "moves_temporary", snapshot)
}

fn add_worker_folder(root: &Path, snapshot: &mut Snapshot) -> Result<()> {
    let dir = root.join(WORKER_FOLDER_SUBDIR);
    add_tabular_tree(&dir, "worker_folder", snapshot)
}

fn add_tabular_tree(dir: &Path, prefix: &str, snapshot: &mut Snapshot) -> Result<()> {
    let entries = walk_files(dir)?;
    for entry in entries {
        // Only `.tbl` and `.csv` files are imported as snapshot tables.
        // Other artifacts (logs, sql dumps, etc.) live in the source tree
        // for forensic reading but don't enter the snapshot — they aren't
        // tabular and the snapshot format wants tables.
        let rel = &entry.relative;
        let lower = rel.to_ascii_lowercase();
        if !(lower.ends_with(".tbl") || lower.ends_with(".csv")) {
            continue;
        }

        let mut segments: Vec<String> = vec![prefix.to_string()];
        for part in rel.split('/') {
            segments.push(sanitize_segment(part));
        }
        let table_name = join_table_name(segments);

        let bytes = read_file(&entry.absolute)?;
        let table = parse_worker_tbl(&entry.absolute, &table_name, &bytes)?;
        snapshot.add_table(table).map_err(|source| match source {
            moves_snapshot::Error::DuplicateTable { table } => Error::DuplicateTableName {
                name: table,
                path: entry.absolute.clone(),
            },
            other => Error::Snapshot(other),
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    fn write_capture(root: &Path, rel: &str, body: &[u8]) {
        let path = root.join(rel);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, body).unwrap();
    }

    fn populate_canonical_capture(root: &Path) {
        // One database with two tables.
        write_capture(
            root,
            "databases/movesoutput/movesactivityoutput.schema.tsv",
            b"yearid\tint\tPRI\nmonthid\tint\tPRI\nactivity\tdouble\t\n",
        );
        write_capture(
            root,
            "databases/movesoutput/movesactivityoutput.tsv",
            b"2020\t1\t100.5\n2020\t2\t150.25\n2020\t3\tNULL\n",
        );
        write_capture(
            root,
            "databases/movesoutput/movesoutput.schema.tsv",
            b"id\tint\tPRI\nrate\tdecimal\t\n",
        );
        write_capture(
            root,
            "databases/movesoutput/movesoutput.tsv",
            b"1\t0.001\n2\t0.002\n",
        );
        // The default DB — should be excluded.
        write_capture(
            root,
            "databases/movesdb20241112/sourceusetype.schema.tsv",
            b"sourcetypeid\tint\tPRI\nname\tvarchar\t\n",
        );
        write_capture(
            root,
            "databases/movesdb20241112/sourceusetype.tsv",
            b"21\tpassengercar\n",
        );
        // MOVESTemporary contents.
        write_capture(
            root,
            "moves-temporary/SourceTypeYearVMT_2020.tbl",
            b"sourcetypeid\tyear\tvmt\n21\t2020\t1.5e9\n",
        );
        // WorkerFolder bundle (a typical worker output file).
        write_capture(
            root,
            "worker-folder/WorkerTemp00/Output.tbl",
            b"a\tb\n1\t2\n",
        );
        write_capture(
            root,
            "worker-folder/WorkerTemp01/Output.tbl",
            b"a\tb\n3\t4\n",
        );
        // A non-tabular file in worker folder — should be ignored.
        write_capture(root, "worker-folder/WorkerTemp00/log.txt", b"some log\n");
    }

    #[test]
    fn end_to_end_capture_to_snapshot() {
        let dir = tempdir().unwrap();
        populate_canonical_capture(dir.path());

        let opts = BuildOptions::default().excluding_db("movesdb20241112");
        let snapshot = build_snapshot(dir.path(), &opts).unwrap();

        let names: Vec<&str> = snapshot.table_names().collect();
        assert_eq!(
            names,
            vec![
                "db__movesoutput__movesactivityoutput",
                "db__movesoutput__movesoutput",
                "moves_temporary__sourcetypeyearvmt_2020_tbl",
                "worker_folder__workertemp00__output_tbl",
                "worker_folder__workertemp01__output_tbl",
            ],
            "tables must appear in lexicographic order and exclude the default DB"
        );

        let activity = snapshot
            .table("db__movesoutput__movesactivityoutput")
            .unwrap();
        assert_eq!(activity.row_count(), 3);
        assert_eq!(activity.natural_key(), &["yearid", "monthid"]);

        // Default DB excluded — no `db__movesdb20241112__sourceusetype` table.
        assert!(snapshot
            .table("db__movesdb20241112__sourceusetype")
            .is_none());
    }

    #[test]
    fn deterministic_writes_byte_identical_snapshot() {
        let captures = tempdir().unwrap();
        populate_canonical_capture(captures.path());

        let opts = BuildOptions::default().excluding_db("movesdb20241112");
        let snap = build_snapshot(captures.path(), &opts).unwrap();

        let snap_dir1 = tempdir().unwrap();
        let snap_dir2 = tempdir().unwrap();
        snap.write(snap_dir1.path()).unwrap();
        snap.write(snap_dir2.path()).unwrap();

        // Compare every file across the two snapshot dirs.
        let files1 = walk_files(snap_dir1.path()).unwrap();
        let files2 = walk_files(snap_dir2.path()).unwrap();
        let rels1: Vec<&str> = files1.iter().map(|e| e.relative.as_str()).collect();
        let rels2: Vec<&str> = files2.iter().map(|e| e.relative.as_str()).collect();
        assert_eq!(rels1, rels2);
        for (a, b) in files1.iter().zip(files2.iter()) {
            let ba = fs::read(&a.absolute).unwrap();
            let bb = fs::read(&b.absolute).unwrap();
            assert_eq!(ba, bb, "differs at {}", a.relative);
        }
    }

    #[test]
    fn deterministic_across_independent_capture_dirs() {
        // Two captures dirs with byte-identical content but built in
        // different temp directories must produce byte-identical snapshots.
        let cap1 = tempdir().unwrap();
        let cap2 = tempdir().unwrap();
        populate_canonical_capture(cap1.path());
        populate_canonical_capture(cap2.path());

        let opts = BuildOptions::default().excluding_db("movesdb20241112");
        let s1 = build_snapshot(cap1.path(), &opts).unwrap();
        let s2 = build_snapshot(cap2.path(), &opts).unwrap();

        let snap_dir1 = tempdir().unwrap();
        let snap_dir2 = tempdir().unwrap();
        s1.write(snap_dir1.path()).unwrap();
        s2.write(snap_dir2.path()).unwrap();

        let files1 = walk_files(snap_dir1.path()).unwrap();
        for e in &files1 {
            let a = fs::read(&e.absolute).unwrap();
            let b = fs::read(snap_dir2.path().join(&e.relative)).unwrap();
            assert_eq!(a, b, "snapshot bytes differ at {}", e.relative);
        }

        let h1 = s1.aggregate_hash().unwrap();
        let h2 = s2.aggregate_hash().unwrap();
        assert_eq!(h1, h2, "aggregate hashes must match");
    }

    #[test]
    fn missing_subdirs_yield_partial_snapshot() {
        let dir = tempdir().unwrap();
        // Only databases — no MOVESTemporary, no WorkerFolder.
        write_capture(
            dir.path(),
            "databases/movesoutput/t.schema.tsv",
            b"id\tint\tPRI\n",
        );
        write_capture(dir.path(), "databases/movesoutput/t.tsv", b"1\n");
        let snap = build_snapshot(dir.path(), &BuildOptions::default()).unwrap();
        let names: Vec<&str> = snap.table_names().collect();
        assert_eq!(names, vec!["db__movesoutput__t"]);
    }

    #[test]
    fn empty_captures_dir_yields_empty_snapshot() {
        let dir = tempdir().unwrap();
        let snap = build_snapshot(dir.path(), &BuildOptions::default()).unwrap();
        assert!(snap.is_empty());
    }

    #[test]
    fn missing_schema_sidecar_is_an_error() {
        let dir = tempdir().unwrap();
        write_capture(dir.path(), "databases/o/t.tsv", b"1\n");
        let err = build_snapshot(dir.path(), &BuildOptions::default()).unwrap_err();
        assert!(matches!(err, Error::CapturesMissing { .. }));
    }

    #[test]
    fn excluded_db_case_insensitive() {
        let dir = tempdir().unwrap();
        write_capture(
            dir.path(),
            "databases/MoVeSdB20241112/t.schema.tsv",
            b"a\tint\t\n",
        );
        write_capture(dir.path(), "databases/MoVeSdB20241112/t.tsv", b"1\n");
        let opts = BuildOptions::default().excluding_db("movesdb20241112");
        let snap = build_snapshot(dir.path(), &opts).unwrap();
        assert!(snap.is_empty());
    }
}
