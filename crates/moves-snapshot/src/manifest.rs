//! Manifest and per-table metadata serialization structures.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::format::{ColumnSpec, FORMAT_VERSION, FLOAT_DECIMALS};

/// Top-level manifest written as `manifest.json`. Lists every table in
/// lexicographic order with its content hash, plus an aggregate hash that
/// covers the whole snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    pub format_version: String,
    /// Sorted lexicographically by `name` so the JSON itself is deterministic.
    pub tables: Vec<ManifestEntry>,
    /// Hash over the table list — see `compute_aggregate_hash`.
    pub aggregate_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub name: String,
    pub row_count: u64,
    pub content_sha256: String,
    pub metadata_sha256: String,
}

/// Per-table sidecar written as `tables/<name>.meta.json`. Captures schema,
/// row count, content hash, and the natural-key columns used to sort rows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableMetadata {
    pub format_version: String,
    pub name: String,
    pub schema: Vec<ColumnSpec>,
    pub natural_key: Vec<String>,
    pub row_count: u64,
    pub float_decimals: u32,
    pub content_sha256: String,
}

impl TableMetadata {
    pub fn new(
        name: String,
        schema: Vec<ColumnSpec>,
        natural_key: Vec<String>,
        row_count: u64,
        content_sha256: String,
    ) -> Self {
        Self {
            format_version: FORMAT_VERSION.to_string(),
            name,
            schema,
            natural_key,
            row_count,
            float_decimals: FLOAT_DECIMALS,
            content_sha256,
        }
    }
}

/// Compute the aggregate snapshot hash from the (already lexicographically
/// sorted) per-table entries.
///
/// Stable as long as the per-table content/metadata hashes are stable, so the
/// aggregate hash is a content-address for the whole snapshot.
pub fn compute_aggregate_hash(entries: &[ManifestEntry]) -> String {
    let mut hasher = Sha256::new();
    for entry in entries {
        hasher.update(entry.name.as_bytes());
        hasher.update(b"\n");
        hasher.update(entry.content_sha256.as_bytes());
        hasher.update(b"\n");
        hasher.update(entry.metadata_sha256.as_bytes());
        hasher.update(b"\n");
        hasher.update(entry.row_count.to_le_bytes());
        hasher.update(b"\n");
    }
    hex_lower(&hasher.finalize())
}

/// SHA256 the bytes and return a lowercase hex string.
pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex_lower(&hasher.finalize())
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{:02x}", b));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aggregate_hash_is_stable() {
        let entries = vec![ManifestEntry {
            name: "a".into(),
            row_count: 3,
            content_sha256: "00".into(),
            metadata_sha256: "11".into(),
        }];
        let h1 = compute_aggregate_hash(&entries);
        let h2 = compute_aggregate_hash(&entries);
        assert_eq!(h1, h2);
        assert_eq!(h1.len(), 64);
    }

    #[test]
    fn aggregate_hash_changes_with_input() {
        let mut entries = vec![ManifestEntry {
            name: "a".into(),
            row_count: 3,
            content_sha256: "00".into(),
            metadata_sha256: "11".into(),
        }];
        let h1 = compute_aggregate_hash(&entries);
        entries[0].content_sha256 = "01".into();
        let h2 = compute_aggregate_hash(&entries);
        assert_ne!(h1, h2);
    }

    #[test]
    fn sha256_hex_known_vector() {
        // SHA-256("abc") = ba7816bf...
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
