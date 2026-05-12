//! Deterministic JSON serialization for [`CalculatorDag`].

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::chain::CalculatorDag;
use crate::error::{Error, Result};

/// Output schema version. Bump on any incompatible JSON shape change.
pub const DAG_VERSION: &str = "moves-calculator-dag/v1";

/// Default filename written to the output directory.
pub const DAG_FILE: &str = "calculator-dag.json";

/// Write `dag` to `<dir>/<DAG_FILE>` as pretty-printed JSON with a single
/// trailing newline. The output directory is created if absent. Returns
/// the full output path.
///
/// Determinism: `dag` must already have every nested vector in its final
/// sort order — this function just serializes. Two calls with the same
/// `dag` value produce a byte-identical file.
pub fn write_dag_json(dir: &Path, dag: &CalculatorDag) -> Result<PathBuf> {
    fs::create_dir_all(dir).map_err(|e| Error::Io {
        path: dir.to_path_buf(),
        source: e,
    })?;
    let path = dir.join(DAG_FILE);
    let mut bytes = serde_json::to_vec_pretty(dag).map_err(|e| Error::Json {
        path: path.clone(),
        source: e,
    })?;
    bytes.push(b'\n');
    let mut f = fs::File::create(&path).map_err(|e| Error::Io {
        path: path.clone(),
        source: e,
    })?;
    f.write_all(&bytes).map_err(|e| Error::Io {
        path: path.clone(),
        source: e,
    })?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chain::build_dag;
    use crate::directives::CalculatorInfo;
    use tempfile::tempdir;

    #[test]
    fn writes_pretty_json_with_trailing_newline() {
        let dir = tempdir().unwrap();
        let dag = build_dag(&CalculatorInfo::empty(), &[]).unwrap();
        let path = write_dag_json(dir.path(), &dag).unwrap();
        let bytes = fs::read(&path).unwrap();
        assert!(bytes.ends_with(b"\n"));
        let text = std::str::from_utf8(&bytes).unwrap();
        assert!(text.starts_with('{'));
        // Pretty-printed: contains a newline before "schema":
        assert!(text.contains("\n  \"schema\":"));
    }

    #[test]
    fn byte_identical_across_re_runs() {
        let dir = tempdir().unwrap();
        let dag = build_dag(&CalculatorInfo::empty(), &[]).unwrap();
        let p1 = write_dag_json(dir.path(), &dag).unwrap();
        let a = fs::read(&p1).unwrap();
        // Overwrite and re-read.
        let p2 = write_dag_json(dir.path(), &dag).unwrap();
        let b = fs::read(&p2).unwrap();
        assert_eq!(a, b);
    }
}
