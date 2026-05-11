//! BSFC (brake-specific fuel consumption) dispatcher (`rdbsfc.f`).
//!
//! Task 97 introduced the dispatcher stub; Task 96 wires it to
//! the [`super::emfc`] parser. `rdbsfc.f` opens the BSFC file and
//! delegates to `rdemfc` with the `BSFC` pseudo-pollutant and the
//! `IORBSF` file-unit, which enables the lenient "no tech-type
//! column" path documented in [`super::emfc::Variant::Bsfc`].
//!
//! # Fortran source
//!
//! Ports `rdbsfc.f` (109 lines).

use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

use crate::{Error, Result};

pub use super::emfc::EmissionFactorRecord;

/// Where to find the BSFC bundle.
#[derive(Debug, Clone)]
pub struct BsfcSource {
    /// Path on disk.
    pub path: PathBuf,
}

/// Open `source.path` and parse it as a BSFC file
/// (`.EMF`-format with the BSFC pseudo-pollutant).
pub fn load(source: &BsfcSource) -> Result<Vec<EmissionFactorRecord>> {
    let file = File::open(&source.path).map_err(|e| Error::Io {
        path: source.path.clone(),
        source: e,
    })?;
    super::emfc::read_bsfc(BufReader::new(file))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn loads_empty_bsfc_packet() {
        // BSFC file with no data lines (header only); parser
        // returns no records but does not error.
        let mut f = NamedTempFile::new().unwrap();
        // Header with units at col 35, "BSFC" at col 45, no
        // tech-type columns → zero records.
        let header = format!(
            "{:5}{:<10}{:<5}{:<5}{:<5}{:<10}{:<10}",
            "", "2270001000", "", " 25.0", " 50.0", "G/HP-HR", "BSFC"
        );
        // Format: cols 1-5 blank, 6-15 SCC, 16-20 blank, 21-25
        // hp_min, 26-30 hp_max, 31-34 blank, 35-44 units, 45-54
        // pollutant. Build explicitly.
        let mut line = String::new();
        line.push_str("     "); // 1-5
        line.push_str("2270001000"); // 6-15
        line.push_str("     "); // 16-20
        line.push_str(" 25.0"); // 21-25
        line.push_str(" 50.0"); // 26-30
        line.push_str("    "); // 31-34
        line.push_str("G/HP-HR   "); // 35-44
        line.push_str("BSFC      "); // 45-54
        let _ = header;
        writeln!(f, "/EMSFAC/").unwrap();
        writeln!(f, "{line}").unwrap();
        writeln!(f, "/END/").unwrap();
        f.flush().unwrap();

        let records = load(&BsfcSource {
            path: f.path().to_path_buf(),
        })
        .unwrap();
        assert!(records.is_empty());
    }
}
