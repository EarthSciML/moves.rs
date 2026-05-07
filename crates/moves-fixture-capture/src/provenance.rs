//! Snapshot provenance sidecar.
//!
//! Captures the inputs that uniquely identify a fixture run:
//! * the SIF SHA256 (from `characterization/fixture-image.lock`)
//! * the RunSpec content hash and its filename-derived fixture name
//! * the moves-fixture-capture crate version
//!
//! Written as `provenance.json` alongside the snapshot's `manifest.json`.
//! Intentionally a small, stable JSON shape: downstream tooling reads it to
//! decide "did this snapshot come from the SIF I expect?"

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{Error, Result};

const PROVENANCE_VERSION: &str = "moves-fixture-capture/v1";
const PROVENANCE_FILE: &str = "provenance.json";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Provenance {
    /// Sidecar format version, bumped on incompatible changes.
    pub provenance_version: String,
    /// Filename-derived fixture identifier (e.g. `samplerunspec`).
    pub fixture_name: String,
    /// SHA256 of the SIF the run executed against.
    pub sif_sha256: String,
    /// File path the SIF lockfile recorded (relative to repo root). Captured
    /// for human-readability; the SHA is the canonical identity.
    pub sif_path: String,
    /// Original RunSpec path on the host. Captured for traceability.
    pub runspec_path: String,
    /// SHA256 of the RunSpec file's bytes — the input that, together with
    /// `sif_sha256`, defines the snapshot's identity.
    pub runspec_sha256: String,
    /// Snapshot's aggregate hash from the moves-snapshot manifest. Stored
    /// here for cross-reference; the manifest is the source of truth.
    pub snapshot_aggregate_sha256: String,
    /// Output and (optional) scale-input database names, in case downstream
    /// tools need them without re-parsing the RunSpec.
    pub output_database: String,
    pub scale_input_database: Option<String>,
}

impl Provenance {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        fixture_name: String,
        sif_sha256: String,
        sif_path: String,
        runspec_path: String,
        runspec_sha256: String,
        snapshot_aggregate_sha256: String,
        output_database: String,
        scale_input_database: Option<String>,
    ) -> Self {
        Self {
            provenance_version: PROVENANCE_VERSION.to_string(),
            fixture_name,
            sif_sha256,
            sif_path,
            runspec_path,
            runspec_sha256,
            snapshot_aggregate_sha256,
            output_database,
            scale_input_database,
        }
    }
}

/// Compute a content hash over `bytes` for use as `runspec_sha256`.
pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    let mut out = String::with_capacity(digest.len() * 2);
    for b in digest {
        out.push_str(&format!("{:02x}", b));
    }
    out
}

/// Read the `sif_sha256 = "..."` value from a lockfile in the
/// `key = "value"` format used by `canonical-image.lock` /
/// `fixture-image.lock`. Lines starting with `#` are comments and ignored.
///
/// Returns `Ok(None)` if the file lists `sif_sha256 = "PENDING_FIRST_BUILD"`
/// — that signals "the SIF hasn't been built yet on this host," which is a
/// recoverable state on a developer machine running fixture infra tests
/// without an actual SIF.
pub fn read_sif_sha_from_lockfile(path: &Path) -> Result<Option<String>> {
    let bytes = std::fs::read(path).map_err(|source| Error::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let text = std::str::from_utf8(&bytes).map_err(|source| Error::Parse {
        path: path.to_path_buf(),
        line: 0,
        message: format!("lockfile is not utf-8: {source}"),
    })?;

    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('#') || trimmed.is_empty() {
            continue;
        }
        // Match `sif_sha256 = "..."` or `sif_sha256= "..."` or `sif_sha256="..."`.
        let Some(rest) = trimmed.strip_prefix("sif_sha256") else {
            continue;
        };
        let rest = rest.trim_start();
        let Some(rest) = rest.strip_prefix('=') else {
            continue;
        };
        let rest = rest.trim_start();
        let Some(rest) = rest.strip_prefix('"') else {
            continue;
        };
        let value = rest.trim_end_matches('"').trim_end();
        if value == "PENDING_FIRST_BUILD" {
            return Ok(None);
        }
        return Ok(Some(value.to_string()));
    }
    Ok(None)
}

/// Write `provenance` to `<snapshot_dir>/provenance.json` deterministically.
/// JSON is pretty-printed with a trailing newline so the file is byte-stable
/// across writes — same as the snapshot crate's manifest.
pub fn write_provenance(snapshot_dir: &Path, provenance: &Provenance) -> Result<PathBuf> {
    let path = snapshot_dir.join(PROVENANCE_FILE);
    let mut bytes = serde_json::to_vec_pretty(provenance).map_err(|source| Error::Json {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tempfile::tempdir;

    #[test]
    fn sha256_hex_is_lowercase_hex() {
        let h = sha256_hex(b"abc");
        assert_eq!(h.len(), 64);
        assert!(h
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
        assert_eq!(
            h,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn lockfile_extracts_sha() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("fixture-image.lock");
        std::fs::write(
            &path,
            r#"# header comment
sif_path           = "characterization/apptainer/moves-fixture.sif"
sif_sha256         = "deadbeef00000000000000000000000000000000000000000000000000000000"
sif_bytes          = 1024
"#,
        )
        .unwrap();
        let result = read_sif_sha_from_lockfile(&path).unwrap();
        assert_eq!(
            result.as_deref(),
            Some("deadbeef00000000000000000000000000000000000000000000000000000000")
        );
    }

    #[test]
    fn lockfile_pending_returns_none() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("fixture-image.lock");
        std::fs::write(
            &path,
            r#"sif_sha256 = "PENDING_FIRST_BUILD"
"#,
        )
        .unwrap();
        assert_eq!(read_sif_sha_from_lockfile(&path).unwrap(), None);
    }

    #[test]
    fn lockfile_missing_field_returns_none() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("x.lock");
        std::fs::write(&path, "# nothing here\n").unwrap();
        assert_eq!(read_sif_sha_from_lockfile(&path).unwrap(), None);
    }

    #[test]
    fn write_provenance_is_deterministic() {
        let dir = tempdir().unwrap();
        let prov = Provenance::new(
            "samplerunspec".into(),
            "abc".into(),
            "characterization/apptainer/moves-fixture.sif".into(),
            "/tmp/SampleRunSpec.xml".into(),
            "def".into(),
            "0123".into(),
            "movesoutput".into(),
            Some("movesdb20241112".into()),
        );
        let p1 = write_provenance(dir.path(), &prov).unwrap();
        let bytes1 = std::fs::read(&p1).unwrap();
        let p2 = write_provenance(dir.path(), &prov).unwrap();
        let bytes2 = std::fs::read(&p2).unwrap();
        assert_eq!(bytes1, bytes2);
        assert!(bytes1.ends_with(b"\n"));
    }

    #[test]
    fn write_provenance_round_trips() {
        let dir = tempdir().unwrap();
        let prov = Provenance::new(
            "fixture".into(),
            "a".into(),
            "p".into(),
            "/r.xml".into(),
            "h".into(),
            "agg".into(),
            "out".into(),
            None,
        );
        write_provenance(dir.path(), &prov).unwrap();
        let bytes = std::fs::read(dir.path().join(PROVENANCE_FILE)).unwrap();
        let parsed: Provenance = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed, prov);
    }

    #[test]
    fn lockfile_reports_io_error_with_path() {
        let path = PathBuf::from("/definitely/not/here.lock");
        let err = read_sif_sha_from_lockfile(&path).unwrap_err();
        match err {
            Error::Io { path: p, .. } => assert_eq!(p, path),
            other => panic!("unexpected: {other:?}"),
        }
    }
}
