//! Deterministic directory walker.
//!
//! Standard library `read_dir` returns entries in OS-dependent order — on
//! ext4 it tends to be insertion order, on tmpfs it's hash-bucketed, and a
//! tmpfs-backed scratch under `/dev/shm` would silently produce non-stable
//! snapshots. To pin determinism we collect each level's entries and sort
//! them lexicographically before recursing.

use std::path::{Path, PathBuf};

use crate::error::{Error, Result};

/// One file discovered by [`walk_files`]. Paths are absolute; the
/// `relative` field holds the path relative to the root the walk started at,
/// using `/` as the separator regardless of OS — so it can be used as a
/// stable identity for the file across hosts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub absolute: PathBuf,
    pub relative: String,
}

/// Recursively walk `root`, returning every regular file in lexicographic
/// order of `relative` path.
///
/// Symlinks are not followed — the walker only emits regular files. Hidden
/// files (`.` prefix) are included; the caller filters if needed.
pub fn walk_files(root: &Path) -> Result<Vec<Entry>> {
    if !root.exists() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    walk_dir(root, root, &mut out)?;
    out.sort_by(|a, b| a.relative.cmp(&b.relative));
    Ok(out)
}

fn walk_dir(root: &Path, dir: &Path, out: &mut Vec<Entry>) -> Result<()> {
    let mut children: Vec<(PathBuf, std::fs::FileType)> = Vec::new();
    let entries = std::fs::read_dir(dir).map_err(|source| Error::Io {
        path: dir.to_path_buf(),
        source,
    })?;
    for entry in entries {
        let entry = entry.map_err(|source| Error::Io {
            path: dir.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        let file_type = entry.file_type().map_err(|source| Error::Io {
            path: path.clone(),
            source,
        })?;
        children.push((path, file_type));
    }
    children.sort_by(|a, b| a.0.cmp(&b.0));

    for (path, ft) in children {
        if ft.is_dir() {
            walk_dir(root, &path, out)?;
        } else if ft.is_file() {
            let relative = relative_string(root, &path);
            out.push(Entry {
                absolute: path,
                relative,
            });
        }
        // Skip symlinks and other types.
    }
    Ok(())
}

fn relative_string(root: &Path, path: &Path) -> String {
    let stripped = path.strip_prefix(root).unwrap_or(path);
    let mut parts: Vec<String> = Vec::new();
    for component in stripped.components() {
        if let Some(s) = component.as_os_str().to_str() {
            parts.push(s.to_string());
        }
    }
    parts.join("/")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn walks_in_lexicographic_order() {
        let dir = tempdir().unwrap();
        // Create files in non-sorted order.
        fs::write(dir.path().join("zeta.txt"), b"").unwrap();
        fs::write(dir.path().join("alpha.txt"), b"").unwrap();
        fs::create_dir_all(dir.path().join("sub/child")).unwrap();
        fs::write(dir.path().join("sub/child/leaf.txt"), b"").unwrap();
        fs::write(dir.path().join("sub/middle.txt"), b"").unwrap();
        fs::write(dir.path().join("beta.txt"), b"").unwrap();

        let out = walk_files(dir.path()).unwrap();
        let rels: Vec<&str> = out.iter().map(|e| e.relative.as_str()).collect();
        assert_eq!(
            rels,
            vec![
                "alpha.txt",
                "beta.txt",
                "sub/child/leaf.txt",
                "sub/middle.txt",
                "zeta.txt"
            ]
        );
    }

    #[test]
    fn missing_root_yields_empty_vec() {
        let out = walk_files(Path::new("/definitely/not/here")).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn skips_symlinks() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("real.txt"), b"x").unwrap();
        // Best-effort symlink: skip on platforms that disallow it.
        #[cfg(unix)]
        std::os::unix::fs::symlink(dir.path().join("real.txt"), dir.path().join("link.txt"))
            .unwrap();

        let out = walk_files(dir.path()).unwrap();
        let rels: Vec<&str> = out.iter().map(|e| e.relative.as_str()).collect();
        assert_eq!(rels, vec!["real.txt"]);
    }

    #[test]
    fn relative_paths_use_forward_slash() {
        let dir = tempdir().unwrap();
        fs::create_dir_all(dir.path().join("a/b/c")).unwrap();
        fs::write(dir.path().join("a/b/c/file.txt"), b"").unwrap();

        let out = walk_files(dir.path()).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].relative, "a/b/c/file.txt");
    }
}
