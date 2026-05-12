//! Run-files options parser (`opnnon.f`).
//!
//! Task 99. Reads the high-level file-registration packets at the
//! head of a NONROAD options (`.opt`) file:
//!
//! - `/RUNFILES/` — labelled file paths for each required and
//!   optional input/output file,
//! - `/POP FILES/` — list of population file paths,
//! - `/MODELYEAR OUT/` (optional) — exhaust / evap by-model-year
//!   output paths,
//! - `/SI REPORT/` (optional) — auxiliary SI report output path,
//! - `/DAILY FILES/` (optional) — daily temperature/RVP file path.
//!
//! In the Fortran source `opnnon.f` parses these *and* opens the
//! corresponding I/O units. In the Rust port we only collect the
//! filenames: each downstream parser already accepts a generic
//! [`std::io::BufRead`], so the orchestrator (Task 113) is free to
//! open them — or stub them in tests — as it sees fit.
//!
//! # Required entries
//!
//! Per `opnnon.f` lines 220–308, the following labels must appear in
//! `/RUNFILES/`: `MESSAGE`, `ALLOC XREF`, `ACTIVITY`, `EXH TECHNOLOGY`,
//! `EVP TECHNOLOGY`, `SEASONALITY`, `REGIONS`, `OUTPUT DATA`. Missing
//! any of them is a fatal parse error. The Fortran source additionally
//! checks file existence; that's deferred to [`RunFiles::check_exists`].
//!
//! # Format
//!
//! Each packet contains label-value records with the label in the
//! first 19 columns and the value beginning at column 21. Records can
//! also use `key: value` form (consistent with the other Task 97
//! parsers).
//!
//! ```text
//! /RUNFILES/
//! MESSAGE            : run.msg
//! OUTPUT DATA        : run.out
//! ALLOC XREF         : allocate.xref
//! ACTIVITY           : activity.dat
//! EXH TECHNOLOGY     : exhtech.dat
//! EVP TECHNOLOGY     : evptech.dat
//! SEASONALITY        : season.dat
//! REGIONS            : regions.dat
//! US COUNTIES FIPS   : fips.dat
//! /END/
//! /POP FILES/
//!                    : pop1.pop
//!                    : pop2.pop
//! /END/
//! ```
//!
//! # Fortran source
//!
//! Ports `opnnon.f` (633 lines).

use crate::{Error, Result};
use std::io::BufRead;
use std::path::{Path, PathBuf};

/// Collected file paths from the options-file header packets.
///
/// All `PathBuf` fields are interpreted relative to the working
/// directory the orchestrator runs in — the Fortran source has the
/// same behaviour. Use [`RunFiles::check_exists`] to validate that
/// the required inputs are reachable before kicking off the downstream
/// parsers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunFiles {
    /// Output message-log path (always required).
    pub message: PathBuf,
    /// Allocation cross-reference input path (required).
    pub alloc_xref: PathBuf,
    /// Activity-data input path (required).
    pub activity: PathBuf,
    /// Exhaust technology fractions input path (required).
    pub exhaust_tech: PathBuf,
    /// Evaporative technology fractions input path (required).
    pub evap_tech: PathBuf,
    /// Seasonality input path (required).
    pub seasonality: PathBuf,
    /// Regions definition input path (required).
    pub regions: PathBuf,
    /// Primary output-data path (required).
    pub output_data: PathBuf,
    /// EPS2 AMS workfile output path (optional).
    pub ams: Option<PathBuf>,
    /// US Counties FIPS lookup path (optional).
    pub fips: Option<PathBuf>,
    /// Retrofit input path (optional).
    pub retrofit: Option<PathBuf>,
    /// Population input files (at least one; `MXPFIL = 3265` in the
    /// Fortran source).
    pub population: Vec<PathBuf>,
    /// Exhaust by-model-year output path (optional).
    pub exhaust_bmy_out: Option<PathBuf>,
    /// Evaporative by-model-year output path (optional).
    pub evap_bmy_out: Option<PathBuf>,
    /// SI-report output path (optional).
    pub si_report: Option<PathBuf>,
    /// Daily-temperature/RVP input path (optional).
    pub daily_temp_rvp: Option<PathBuf>,
    /// Non-fatal warnings (unknown labels, etc.) collected during the
    /// parse. The Fortran source raises a fatal error on unknown
    /// labels (`7007`); the Rust port keeps the same default but
    /// callers that prefer a permissive parse can switch to
    /// [`RunFilesOptions::permissive`].
    pub warnings: Vec<String>,
}

/// Parser-time options.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunFilesOptions {
    /// When `true`, unknown labels in any packet are recorded as
    /// warnings instead of raised as fatal errors. The Fortran source
    /// is strict; this is provided as an opt-in escape hatch for
    /// adopted-but-extended packet vocabularies.
    pub permissive_labels: bool,
}

impl RunFilesOptions {
    /// Strict parse mirroring `opnnon.f`'s behaviour.
    pub const STRICT: Self = Self {
        permissive_labels: false,
    };

    /// Permissive parse — unknown labels become warnings.
    pub const fn permissive() -> Self {
        Self {
            permissive_labels: true,
        }
    }
}

impl Default for RunFilesOptions {
    fn default() -> Self {
        Self::STRICT
    }
}

/// Maximum number of population files supported by the Fortran source
/// (`MXPFIL` in `nonrdprm.inc`). Exceeding this is a fatal parse
/// error (Fortran error label `7009`).
pub const MAX_POPULATION_FILES: usize = 3265;

/// Parse the file-registration packets at the head of an `.opt` file.
///
/// This consumes the whole reader; it expects to encounter every
/// packet exactly once. Optional packets that are absent leave their
/// matching `Option` fields at `None`. Required entries that are
/// missing yield [`Error::Parse`].
pub fn read_runfiles<R: BufRead>(reader: R) -> Result<RunFiles> {
    read_runfiles_with(reader, RunFilesOptions::default())
}

/// Parse the file-registration packets with explicit options.
pub fn read_runfiles_with<R: BufRead>(reader: R, opts: RunFilesOptions) -> Result<RunFiles> {
    let path = PathBuf::from(".OPT");
    let mut builder = RunFilesBuilder::default();

    let mut packet: Option<Packet> = None;
    let mut line_num = 0;

    for line_result in reader.lines() {
        line_num += 1;
        let line = line_result.map_err(|e| Error::Io {
            path: path.clone(),
            source: e,
        })?;
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let upper = trimmed.to_ascii_uppercase();
        if upper.starts_with("/END/") {
            packet = None;
            continue;
        }
        if let Some(p) = Packet::recognize(&upper) {
            packet = Some(p);
            continue;
        }
        let Some(active) = packet else {
            continue;
        };
        let (label, value) = split_label_value(&line);
        match active {
            Packet::RunFiles => apply_runfiles(&mut builder, &label, value, line_num, &path, opts)?,
            Packet::PopFiles => apply_popfiles(&mut builder, value, line_num, &path)?,
            Packet::ModelYearOut => {
                apply_modelyear(&mut builder, &label, value, line_num, &path, opts)?
            }
            Packet::SiReport => apply_si(&mut builder, value),
            Packet::DailyFiles => apply_daily(&mut builder, value),
        }
    }

    builder.finish(&path, line_num)
}

#[derive(Debug, Default)]
struct RunFilesBuilder {
    message: Option<PathBuf>,
    alloc_xref: Option<PathBuf>,
    activity: Option<PathBuf>,
    exhaust_tech: Option<PathBuf>,
    evap_tech: Option<PathBuf>,
    seasonality: Option<PathBuf>,
    regions: Option<PathBuf>,
    output_data: Option<PathBuf>,
    ams: Option<PathBuf>,
    fips: Option<PathBuf>,
    retrofit: Option<PathBuf>,
    population: Vec<PathBuf>,
    exhaust_bmy_out: Option<PathBuf>,
    evap_bmy_out: Option<PathBuf>,
    si_report: Option<PathBuf>,
    daily_temp_rvp: Option<PathBuf>,
    warnings: Vec<String>,
}

impl RunFilesBuilder {
    fn finish(self, path: &Path, line_num: usize) -> Result<RunFiles> {
        let RunFilesBuilder {
            message,
            alloc_xref,
            activity,
            exhaust_tech,
            evap_tech,
            seasonality,
            regions,
            output_data,
            ams,
            fips,
            retrofit,
            population,
            exhaust_bmy_out,
            evap_bmy_out,
            si_report,
            daily_temp_rvp,
            warnings,
        } = self;
        let message = required(message, "MESSAGE", path, line_num)?;
        let alloc_xref = required(alloc_xref, "ALLOC XREF", path, line_num)?;
        let activity = required(activity, "ACTIVITY", path, line_num)?;
        let exhaust_tech = required(exhaust_tech, "EXH TECHNOLOGY", path, line_num)?;
        let evap_tech = required(evap_tech, "EVP TECHNOLOGY", path, line_num)?;
        let seasonality = required(seasonality, "SEASONALITY", path, line_num)?;
        let regions = required(regions, "REGIONS", path, line_num)?;
        let output_data = required(output_data, "OUTPUT DATA", path, line_num)?;
        if population.is_empty() {
            return Err(Error::Parse {
                file: path.to_path_buf(),
                line: line_num,
                message: "missing /POP FILES/ packet or no population entries listed".to_string(),
            });
        }
        Ok(RunFiles {
            message,
            alloc_xref,
            activity,
            exhaust_tech,
            evap_tech,
            seasonality,
            regions,
            output_data,
            ams,
            fips,
            retrofit,
            population,
            exhaust_bmy_out,
            evap_bmy_out,
            si_report,
            daily_temp_rvp,
            warnings,
        })
    }
}

#[derive(Clone, Copy, Debug)]
enum Packet {
    RunFiles,
    PopFiles,
    ModelYearOut,
    SiReport,
    DailyFiles,
}

impl Packet {
    fn recognize(upper: &str) -> Option<Self> {
        if upper.starts_with("/RUNFILES/") {
            Some(Self::RunFiles)
        } else if upper.starts_with("/POP FILES/") {
            Some(Self::PopFiles)
        } else if upper.starts_with("/MODELYEAR OUT/") {
            Some(Self::ModelYearOut)
        } else if upper.starts_with("/SI REPORT/") {
            Some(Self::SiReport)
        } else if upper.starts_with("/DAILY FILES/") {
            Some(Self::DailyFiles)
        } else {
            None
        }
    }
}

fn apply_runfiles(
    builder: &mut RunFilesBuilder,
    label: &str,
    value: String,
    line_num: usize,
    path: &Path,
    opts: RunFilesOptions,
) -> Result<()> {
    if value.is_empty() {
        return Ok(());
    }
    let v = PathBuf::from(value);
    let upper = label.to_ascii_uppercase();
    let upper = upper.trim();
    match upper {
        "MESSAGE" => builder.message = Some(v),
        "ALLOC XREF" => builder.alloc_xref = Some(v),
        "ACTIVITY" => builder.activity = Some(v),
        "EXH TECHNOLOGY" => builder.exhaust_tech = Some(v),
        "EVP TECHNOLOGY" => builder.evap_tech = Some(v),
        "SEASONALITY" => builder.seasonality = Some(v),
        "REGIONS" => builder.regions = Some(v),
        "OUTPUT DATA" => builder.output_data = Some(v),
        "EPS2 AMS" => builder.ams = Some(v),
        "US COUNTIES FIPS" => builder.fips = Some(v),
        "RETROFIT" => builder.retrofit = Some(v),
        other => return unknown_label("/RUNFILES/", other, line_num, path, opts, builder),
    }
    Ok(())
}

fn apply_popfiles(
    builder: &mut RunFilesBuilder,
    value: String,
    line_num: usize,
    path: &Path,
) -> Result<()> {
    if value.is_empty() {
        return Ok(());
    }
    if builder.population.len() >= MAX_POPULATION_FILES {
        return Err(Error::Parse {
            file: path.to_path_buf(),
            line: line_num,
            message: format!(
                "/POP FILES/ packet exceeds Fortran limit MXPFIL={}",
                MAX_POPULATION_FILES
            ),
        });
    }
    builder.population.push(PathBuf::from(value));
    Ok(())
}

fn apply_modelyear(
    builder: &mut RunFilesBuilder,
    label: &str,
    value: String,
    line_num: usize,
    path: &Path,
    opts: RunFilesOptions,
) -> Result<()> {
    if value.is_empty() {
        return Ok(());
    }
    let v = PathBuf::from(value);
    let upper = label.to_ascii_uppercase();
    let upper = upper.trim();
    match upper {
        "EXHAUST BMY OUT" => builder.exhaust_bmy_out = Some(v),
        "EVAP BMY OUT" => builder.evap_bmy_out = Some(v),
        other => return unknown_label("/MODELYEAR OUT/", other, line_num, path, opts, builder),
    }
    Ok(())
}

fn apply_si(builder: &mut RunFilesBuilder, value: String) {
    if !value.is_empty() {
        builder.si_report = Some(PathBuf::from(value));
    }
}

fn apply_daily(builder: &mut RunFilesBuilder, value: String) {
    if !value.is_empty() {
        builder.daily_temp_rvp = Some(PathBuf::from(value));
    }
}

fn unknown_label(
    packet: &str,
    label: &str,
    line_num: usize,
    path: &Path,
    opts: RunFilesOptions,
    builder: &mut RunFilesBuilder,
) -> Result<()> {
    let msg = format!("unknown file identifier in {packet} packet: {:?}", label);
    if opts.permissive_labels {
        builder.warnings.push(msg);
        Ok(())
    } else {
        Err(Error::Parse {
            file: path.to_path_buf(),
            line: line_num,
            message: msg,
        })
    }
}

fn required(value: Option<PathBuf>, label: &str, path: &Path, line_num: usize) -> Result<PathBuf> {
    value.ok_or_else(|| Error::Parse {
        file: path.to_path_buf(),
        line: line_num,
        message: format!("missing required entry {} in /RUNFILES/ packet", label),
    })
}

fn split_label_value(line: &str) -> (String, String) {
    if let Some(idx) = line.find(':') {
        (
            line[..idx].trim().to_string(),
            line[idx + 1..].trim().to_string(),
        )
    } else if line.len() > 20 {
        (line[..20].trim().to_string(), line[20..].trim().to_string())
    } else {
        (line.trim().to_string(), String::new())
    }
}

impl RunFiles {
    /// Verify that every required input file exists on disk.
    ///
    /// Returns an [`Error::Config`] listing every missing path. The
    /// Fortran source performs the same check inline via `inquire`
    /// statements (`opnnon.f` lines 241–323). Output paths are not
    /// checked — the orchestrator creates them on open.
    pub fn check_exists(&self) -> Result<()> {
        let mut missing: Vec<&Path> = Vec::new();
        let required_inputs: [&Path; 7] = [
            self.alloc_xref.as_path(),
            self.activity.as_path(),
            self.exhaust_tech.as_path(),
            self.evap_tech.as_path(),
            self.seasonality.as_path(),
            self.regions.as_path(),
            // FIPS file required only if specified — included as
            // optional via `self.fips` below.
            self.population
                .first()
                .map(|p| p.as_path())
                .unwrap_or_else(|| Path::new("")),
        ];
        for input in required_inputs {
            if input.as_os_str().is_empty() {
                continue;
            }
            if !input.exists() {
                missing.push(input);
            }
        }
        for pop in &self.population {
            if !pop.exists() && !missing.contains(&pop.as_path()) {
                missing.push(pop.as_path());
            }
        }
        for opt in [self.fips.as_ref(), self.retrofit.as_ref()]
            .into_iter()
            .flatten()
        {
            if !opt.exists() {
                missing.push(opt.as_path());
            }
        }
        if missing.is_empty() {
            Ok(())
        } else {
            Err(Error::Config(format!(
                "input files not found: {}",
                missing
                    .iter()
                    .map(|p| p.display().to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    fn full_packet() -> &'static str {
        "\
/RUNFILES/
MESSAGE            : run.msg
OUTPUT DATA        : run.out
ALLOC XREF         : allocate.xref
ACTIVITY           : activity.dat
EXH TECHNOLOGY     : exhtech.dat
EVP TECHNOLOGY     : evptech.dat
SEASONALITY        : season.dat
REGIONS            : regions.dat
US COUNTIES FIPS   : fips.dat
/END/
/POP FILES/
                   : pop1.pop
                   : pop2.pop
/END/
"
    }

    #[test]
    fn parses_minimal_packet() {
        let cfg = read_runfiles(full_packet().as_bytes()).unwrap();
        assert_eq!(cfg.message, PathBuf::from("run.msg"));
        assert_eq!(cfg.output_data, PathBuf::from("run.out"));
        assert_eq!(cfg.alloc_xref, PathBuf::from("allocate.xref"));
        assert_eq!(cfg.fips.as_deref(), Some(Path::new("fips.dat")));
        assert_eq!(cfg.population.len(), 2);
        assert_eq!(cfg.population[0], PathBuf::from("pop1.pop"));
        assert!(cfg.exhaust_bmy_out.is_none());
        assert!(cfg.daily_temp_rvp.is_none());
        assert!(cfg.warnings.is_empty());
    }

    #[test]
    fn rejects_missing_required_entry() {
        let bad = full_packet().replace("MESSAGE            : run.msg\n", "");
        let err = read_runfiles(bad.as_bytes()).unwrap_err();
        match err {
            Error::Parse { message, .. } => assert!(message.contains("MESSAGE")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn rejects_missing_pop_files_packet() {
        let bad = full_packet().replace(
            "/POP FILES/\n                   : pop1.pop\n                   : pop2.pop\n/END/\n",
            "",
        );
        let err = read_runfiles(bad.as_bytes()).unwrap_err();
        match err {
            Error::Parse { message, .. } => assert!(message.contains("POP FILES")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn parses_optional_packets() {
        let input = format!(
            "{}\
/MODELYEAR OUT/
EXHAUST BMY OUT    : exh.bmy
EVAP BMY OUT       : evp.bmy
/END/
/SI REPORT/
SI                 : si.out
/END/
/DAILY FILES/
DAILY              : daily.dat
/END/
",
            full_packet()
        );
        let cfg = read_runfiles(input.as_bytes()).unwrap();
        assert_eq!(cfg.exhaust_bmy_out.as_deref(), Some(Path::new("exh.bmy")));
        assert_eq!(cfg.evap_bmy_out.as_deref(), Some(Path::new("evp.bmy")));
        assert_eq!(cfg.si_report.as_deref(), Some(Path::new("si.out")));
        assert_eq!(cfg.daily_temp_rvp.as_deref(), Some(Path::new("daily.dat")));
    }

    #[test]
    fn unknown_label_is_fatal_by_default() {
        let bad = full_packet().replace("US COUNTIES FIPS", "MYSTERY LABEL   ");
        let err = read_runfiles(bad.as_bytes()).unwrap_err();
        match err {
            Error::Parse { message, .. } => assert!(message.contains("unknown")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn unknown_label_in_permissive_mode_is_a_warning() {
        let bad = full_packet().replace("US COUNTIES FIPS", "MYSTERY LABEL   ");
        let cfg = read_runfiles_with(bad.as_bytes(), RunFilesOptions::permissive()).unwrap();
        assert_eq!(cfg.warnings.len(), 1);
        assert!(cfg.warnings[0].to_ascii_uppercase().contains("MYSTERY"));
        assert!(cfg.fips.is_none());
    }

    #[test]
    fn check_exists_flags_missing_inputs() {
        let dir = TempDir::new().unwrap();
        // Create a subset of expected files; leave the rest missing.
        let exists = ["allocate.xref", "exhtech.dat", "pop1.pop"];
        for name in exists {
            let mut f = std::fs::File::create(dir.path().join(name)).unwrap();
            writeln!(f, "stub").unwrap();
        }
        let cfg = RunFiles {
            message: dir.path().join("run.msg"),
            alloc_xref: dir.path().join("allocate.xref"),
            activity: dir.path().join("activity.dat"),
            exhaust_tech: dir.path().join("exhtech.dat"),
            evap_tech: dir.path().join("evptech.dat"),
            seasonality: dir.path().join("season.dat"),
            regions: dir.path().join("regions.dat"),
            output_data: dir.path().join("run.out"),
            ams: None,
            fips: Some(dir.path().join("fips.dat")),
            retrofit: None,
            population: vec![dir.path().join("pop1.pop")],
            exhaust_bmy_out: None,
            evap_bmy_out: None,
            si_report: None,
            daily_temp_rvp: None,
            warnings: Vec::new(),
        };
        let err = cfg.check_exists().unwrap_err();
        match err {
            Error::Config(msg) => {
                assert!(msg.contains("activity.dat"));
                assert!(msg.contains("evptech.dat"));
                assert!(msg.contains("season.dat"));
                assert!(msg.contains("regions.dat"));
                assert!(msg.contains("fips.dat"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn pop_files_limit_enforced() {
        let mut input = String::from(
            "\
/RUNFILES/
MESSAGE            : run.msg
OUTPUT DATA        : run.out
ALLOC XREF         : allocate.xref
ACTIVITY           : activity.dat
EXH TECHNOLOGY     : exhtech.dat
EVP TECHNOLOGY     : evptech.dat
SEASONALITY        : season.dat
REGIONS            : regions.dat
/END/
/POP FILES/
",
        );
        for i in 0..=MAX_POPULATION_FILES {
            input.push_str(&format!("                   : pop{i}.pop\n"));
        }
        input.push_str("/END/\n");
        let err = read_runfiles(input.as_bytes()).unwrap_err();
        match err {
            Error::Parse { message, .. } => assert!(message.contains("MXPFIL")),
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
