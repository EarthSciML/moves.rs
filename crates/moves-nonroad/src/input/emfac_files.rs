//! Emission-factor file-list parser (`opnefc.f`).
//!
//! Task 99. Reads the `/EMFAC FILES/` and `/DETERIORATE FILES/`
//! packets from a NONROAD options file. Each packet lists labelled
//! file paths, one per pollutant / fuel-property quantity. The
//! parser collects the paths; opening them is the orchestrator's job.
//!
//! # Pollutant labels
//!
//! The Fortran source (`opnefc.f` lines 80–104) installs one key per
//! pollutant index ([`PollutantIndex`]):
//!
//! | Label              | Pollutant index |
//! |--------------------|-----------------|
//! | `BSFC`             | (BSFC factor)   |
//! | `THC EXHAUST`      | IDXTHC          |
//! | `NOX EXHAUST`      | IDXNOX          |
//! | `CO EXHAUST`       | IDXCO           |
//! | `PM EXHAUST`       | IDXPM           |
//! | `CRANKCASE`        | IDXCRA          |
//! | `DIURNAL`          | IDXDIU          |
//! | `DISPLACEMENT`     | IDXDIS          |
//! | `SPILLAGE`         | IDXSPL          |
//! | `HOT SOAKS`        | IDXSOK          |
//! | `TANK PERM`        | IDXTKP          |
//! | `NON-RM HOSE PERM` | IDXHOS          |
//! | `RM FILL NECK PERM`| IDXNCK          |
//! | `RM SUPPLY/RETURN` | IDXSR           |
//! | `RM VENT PERM`     | IDXVNT          |
//! | `RUNINGLOSS`       | IDXRLS          |
//! | `THC STARTS`       | IDSTHC          |
//! | `NOX STARTS`       | IDSNOX          |
//! | `CO STARTS`        | IDSCO           |
//! | `PM STARTS`        | IDSPM           |
//! | `SO2 EXHAUST`      | IDXSOX          |
//! | `CO2 EXHAUST`      | IDXCO2          |
//! | `SO2 STARTS`       | IDSSOX          |
//! | `CO2 STARTS`       | IDSCO2          |
//!
//! # Behaviour matching `opnefc.f`
//!
//! - `BSFC` is required; absence is a fatal parse error
//!   (`opnefc.f` label `7005`).
//! - `SO2 EXHAUST` / `CO2 EXHAUST` / `SO2 STARTS` / `CO2 STARTS`
//!   entries are accepted but downgraded to a warning — those species
//!   are computed from BSFC at runtime, so the supplied file is
//!   ignored (`opnefc.f` lines 141–149).
//! - `DISPLACEMENT` is similarly downgraded — refueling vapor
//!   displacement is computed from temperature/RVP at runtime.
//! - Missing factor entries (except the ones the Fortran source
//!   skips: `IDXCO2`, `IDXSOX`, `IDXSOK`, `IDXDIS`, `IDXRLS`) produce
//!   a warning. The simulation can proceed; the affected pollutant
//!   stays at its default missing-data value.
//! - `/DETERIORATE FILES/` is optional. When absent, all
//!   deterioration factors default to 1.0; the parser records a
//!   warning.
//!
//! # Fortran source
//!
//! Ports `opnefc.f` (437 lines).

use crate::{Error, Result};
use std::collections::HashMap;
use std::io::BufRead;
use std::path::{Path, PathBuf};

/// Pollutant index, matching the Fortran `IDX*` / `IDS*` parameters
/// declared in `nonrdprm.inc`. Preserves the Fortran 1-based numbering
/// so that ports of routines indexed by these constants line up.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PollutantIndex {
    /// Total HC exhaust (`IDXTHC = 1`).
    ThcExhaust = 1,
    /// CO exhaust (`IDXCO = 2`).
    CoExhaust = 2,
    /// NOx exhaust (`IDXNOX = 3`).
    NoxExhaust = 3,
    /// CO2 exhaust (`IDXCO2 = 4`) — computed from BSFC; file ignored.
    Co2Exhaust = 4,
    /// SO2 exhaust (`IDXSOX = 5`) — computed from BSFC; file ignored.
    SoxExhaust = 5,
    /// PM exhaust (`IDXPM = 6`).
    PmExhaust = 6,
    /// Crankcase (`IDXCRA = 7`).
    Crankcase = 7,
    /// Diurnal evap (`IDXDIU = 8`).
    Diurnal = 8,
    /// Tank permeation evap (`IDXTKP = 9`).
    TankPerm = 9,
    /// Non-rec-marine hose permeation evap (`IDXHOS = 10`).
    HosePerm = 10,
    /// Rec-marine fill-neck hose permeation (`IDXNCK = 11`).
    NeckPerm = 11,
    /// Rec-marine supply/return hose permeation (`IDXSR = 12`).
    SupplyReturnPerm = 12,
    /// Rec-marine vent hose permeation (`IDXVNT = 13`).
    VentPerm = 13,
    /// Hot soak (`IDXSOK = 14`).
    HotSoak = 14,
    /// Refueling displacement (`IDXDIS = 15`) — computed; file ignored.
    Displacement = 15,
    /// Spillage (`IDXSPL = 16`).
    Spillage = 16,
    /// Running loss (`IDXRLS = 17`).
    RunningLoss = 17,
    /// Start THC (`IDSTHC = 18`).
    ThcStarts = 18,
    /// Start CO (`IDSCO = 19`).
    CoStarts = 19,
    /// Start NOx (`IDSNOX = 20`).
    NoxStarts = 20,
    /// Start CO2 (`IDSCO2 = 21`) — computed; file ignored.
    Co2Starts = 21,
    /// Start SOx (`IDSSOX = 22`) — computed; file ignored.
    SoxStarts = 22,
    /// Start PM (`IDSPM = 23`).
    PmStarts = 23,
}

impl PollutantIndex {
    /// All declared variants.
    pub const ALL: &'static [Self] = &[
        Self::ThcExhaust,
        Self::CoExhaust,
        Self::NoxExhaust,
        Self::Co2Exhaust,
        Self::SoxExhaust,
        Self::PmExhaust,
        Self::Crankcase,
        Self::Diurnal,
        Self::TankPerm,
        Self::HosePerm,
        Self::NeckPerm,
        Self::SupplyReturnPerm,
        Self::VentPerm,
        Self::HotSoak,
        Self::Displacement,
        Self::Spillage,
        Self::RunningLoss,
        Self::ThcStarts,
        Self::CoStarts,
        Self::NoxStarts,
        Self::Co2Starts,
        Self::SoxStarts,
        Self::PmStarts,
    ];

    /// Map a label string (already trimmed and upper-cased) to its
    /// pollutant index. Returns `None` for unknown labels.
    pub fn from_label(label: &str) -> Option<Self> {
        Some(match label {
            "THC EXHAUST" => Self::ThcExhaust,
            "NOX EXHAUST" => Self::NoxExhaust,
            "CO EXHAUST" => Self::CoExhaust,
            "PM EXHAUST" => Self::PmExhaust,
            "CRANKCASE" => Self::Crankcase,
            "DIURNAL" => Self::Diurnal,
            "DISPLACEMENT" => Self::Displacement,
            "SPILLAGE" => Self::Spillage,
            "HOT SOAKS" => Self::HotSoak,
            "TANK PERM" => Self::TankPerm,
            "NON-RM HOSE PERM" => Self::HosePerm,
            "RM FILL NECK PERM" => Self::NeckPerm,
            "RM SUPPLY/RETURN" => Self::SupplyReturnPerm,
            "RM VENT PERM" => Self::VentPerm,
            "RUNINGLOSS" => Self::RunningLoss,
            "THC STARTS" => Self::ThcStarts,
            "NOX STARTS" => Self::NoxStarts,
            "CO STARTS" => Self::CoStarts,
            "PM STARTS" => Self::PmStarts,
            "SO2 EXHAUST" => Self::SoxExhaust,
            "CO2 EXHAUST" => Self::Co2Exhaust,
            "SO2 STARTS" => Self::SoxStarts,
            "CO2 STARTS" => Self::Co2Starts,
            _ => return None,
        })
    }

    /// Whether the Fortran code ignores files supplied for this
    /// pollutant (it's computed from other inputs).
    pub fn is_computed(self) -> bool {
        matches!(
            self,
            Self::Co2Exhaust
                | Self::SoxExhaust
                | Self::Co2Starts
                | Self::SoxStarts
                | Self::Displacement
        )
    }

    /// Whether the Fortran code skips the "missing factor file"
    /// warning for this pollutant (`opnefc.f` lines 184–189).
    fn skip_missing_emfac_warning(self) -> bool {
        matches!(
            self,
            Self::Co2Exhaust
                | Self::SoxExhaust
                | Self::HotSoak
                | Self::Displacement
                | Self::RunningLoss
        )
    }
}

/// Files declared by the `/EMFAC FILES/` and `/DETERIORATE FILES/`
/// packets.
#[derive(Debug, Clone)]
pub struct EmfacFiles {
    /// Required BSFC file path.
    pub bsfc: PathBuf,
    /// Per-pollutant emission-factor file paths.
    pub emission_factors: HashMap<PollutantIndex, PathBuf>,
    /// Per-pollutant deterioration-factor file paths.
    pub deterioration_factors: HashMap<PollutantIndex, PathBuf>,
    /// Non-fatal warnings produced during the parse.
    pub warnings: Vec<String>,
}

impl EmfacFiles {
    /// Look up the emission-factor file for a pollutant, if any.
    pub fn emission_factor(&self, pollutant: PollutantIndex) -> Option<&Path> {
        self.emission_factors.get(&pollutant).map(|p| p.as_path())
    }

    /// Look up the deterioration-factor file for a pollutant, if any.
    pub fn deterioration_factor(&self, pollutant: PollutantIndex) -> Option<&Path> {
        self.deterioration_factors
            .get(&pollutant)
            .map(|p| p.as_path())
    }
}

/// Parse the `/EMFAC FILES/` and (optional) `/DETERIORATE FILES/`
/// packets.
pub fn read_emfac_files<R: BufRead>(reader: R) -> Result<EmfacFiles> {
    let path = PathBuf::from(".OPT");
    let mut bsfc: Option<PathBuf> = None;
    let mut emission_factors: HashMap<PollutantIndex, PathBuf> = HashMap::new();
    let mut deterioration_factors: HashMap<PollutantIndex, PathBuf> = HashMap::new();
    let mut warnings: Vec<String> = Vec::new();
    let mut saw_emfac_packet = false;
    let mut saw_det_packet = false;

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
        if upper.starts_with("/EMFAC FILES/") {
            packet = Some(Packet::Emfac);
            saw_emfac_packet = true;
            continue;
        }
        if upper.starts_with("/DETERIORATE FILES/") {
            packet = Some(Packet::Deteriorate);
            saw_det_packet = true;
            continue;
        }
        if upper.starts_with('/') {
            // Some other packet header — bail out of any active packet.
            packet = None;
            continue;
        }
        let Some(active) = packet else {
            continue;
        };
        let (label, value) = split_label_value(&line);
        if value.is_empty() {
            continue;
        }
        let upper_label = label.to_ascii_uppercase();
        let upper_label = upper_label.trim();
        match active {
            Packet::Emfac => apply_emfac(
                upper_label,
                value,
                line_num,
                &path,
                &mut bsfc,
                &mut emission_factors,
                &mut warnings,
            )?,
            Packet::Deteriorate => apply_det(
                upper_label,
                value,
                line_num,
                &path,
                &mut deterioration_factors,
                &mut warnings,
            )?,
        }
    }

    if !saw_emfac_packet {
        return Err(Error::Parse {
            file: path,
            line: line_num,
            message: "missing required /EMFAC FILES/ packet".to_string(),
        });
    }
    let bsfc = bsfc.ok_or_else(|| Error::Parse {
        file: path.clone(),
        line: line_num,
        message: "missing BSFC entry in /EMFAC FILES/ packet".to_string(),
    })?;
    for pollutant in PollutantIndex::ALL {
        if matches!(
            pollutant,
            PollutantIndex::ThcStarts
                | PollutantIndex::CoStarts
                | PollutantIndex::NoxStarts
                | PollutantIndex::Co2Starts
                | PollutantIndex::SoxStarts
                | PollutantIndex::PmStarts
        ) {
            // Fortran `opnefc.f` iterates `1..=IDSTHC-1` only — start
            // emissions are not required and are not warned about.
            continue;
        }
        if pollutant.skip_missing_emfac_warning() {
            continue;
        }
        if !emission_factors.contains_key(pollutant) {
            if matches!(pollutant, PollutantIndex::Spillage) {
                warnings.push(
                    "no SPILLAGE file in /EMFAC FILES/ packet: \
                    spillage and vapor-displacement factors default to missing"
                        .to_string(),
                );
            } else {
                warnings.push(format!(
                    "no file in /EMFAC FILES/ packet for {:?}: factors default to missing",
                    pollutant
                ));
            }
        }
    }
    if !saw_det_packet {
        warnings.push(
            "missing /DETERIORATE FILES/ packet: deterioration factors default to 1.0".to_string(),
        );
    } else {
        for pollutant in &[
            PollutantIndex::ThcExhaust,
            PollutantIndex::CoExhaust,
            PollutantIndex::NoxExhaust,
            PollutantIndex::PmExhaust,
        ] {
            if !deterioration_factors.contains_key(pollutant) {
                warnings.push(format!(
                    "no file in /DETERIORATE FILES/ packet for {:?}: deterioration factor defaults to 1.0",
                    pollutant
                ));
            }
        }
    }

    Ok(EmfacFiles {
        bsfc,
        emission_factors,
        deterioration_factors,
        warnings,
    })
}

#[derive(Clone, Copy, Debug)]
enum Packet {
    Emfac,
    Deteriorate,
}

fn apply_emfac(
    label: &str,
    value: String,
    line_num: usize,
    path: &Path,
    bsfc: &mut Option<PathBuf>,
    emission_factors: &mut HashMap<PollutantIndex, PathBuf>,
    warnings: &mut Vec<String>,
) -> Result<()> {
    if label == "BSFC" {
        *bsfc = Some(PathBuf::from(value));
        return Ok(());
    }
    let Some(pollutant) = PollutantIndex::from_label(label) else {
        return Err(Error::Parse {
            file: path.to_path_buf(),
            line: line_num,
            message: format!(
                "unknown file identifier in /EMFAC FILES/ packet: {:?}",
                label
            ),
        });
    };
    if pollutant.is_computed() {
        warnings.push(format!(
            "emission factor file for {:?} ignored: derived from BSFC/temperature/RVP at runtime",
            pollutant
        ));
        return Ok(());
    }
    emission_factors.insert(pollutant, PathBuf::from(value));
    Ok(())
}

fn apply_det(
    label: &str,
    value: String,
    line_num: usize,
    path: &Path,
    deterioration_factors: &mut HashMap<PollutantIndex, PathBuf>,
    warnings: &mut Vec<String>,
) -> Result<()> {
    let Some(pollutant) = PollutantIndex::from_label(label) else {
        return Err(Error::Parse {
            file: path.to_path_buf(),
            line: line_num,
            message: format!(
                "unknown file identifier in /DETERIORATE FILES/ packet: {:?}",
                label
            ),
        });
    };
    if pollutant.is_computed() {
        warnings.push(format!(
            "deterioration factor file for {:?} ignored: pollutant is derived",
            pollutant
        ));
        return Ok(());
    }
    if matches!(pollutant, PollutantIndex::Spillage) {
        warnings.push(
            "deterioration factor file for SPILLAGE ignored: spillage uses tank-volume factors"
                .to_string(),
        );
        return Ok(());
    }
    deterioration_factors.insert(pollutant, PathBuf::from(value));
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    fn full_packet() -> &'static str {
        "\
/EMFAC FILES/
BSFC               : bsfc.dat
THC EXHAUST        : thc.exh
NOX EXHAUST        : nox.exh
CO EXHAUST         : co.exh
PM EXHAUST         : pm.exh
CRANKCASE          : crank.dat
DIURNAL            : diurnal.dat
SPILLAGE           : spill.dat
TANK PERM          : tank.dat
NON-RM HOSE PERM   : hose.dat
RM FILL NECK PERM  : neck.dat
RM SUPPLY/RETURN   : sr.dat
RM VENT PERM       : vent.dat
/END/
/DETERIORATE FILES/
THC EXHAUST        : thc.det
NOX EXHAUST        : nox.det
CO EXHAUST         : co.det
PM EXHAUST         : pm.det
/END/
"
    }

    #[test]
    fn parses_full_packet() {
        let cfg = read_emfac_files(full_packet().as_bytes()).unwrap();
        assert_eq!(cfg.bsfc, PathBuf::from("bsfc.dat"));
        assert_eq!(
            cfg.emission_factor(PollutantIndex::ThcExhaust),
            Some(Path::new("thc.exh"))
        );
        assert_eq!(
            cfg.deterioration_factor(PollutantIndex::PmExhaust),
            Some(Path::new("pm.det"))
        );
        assert!(cfg.warnings.is_empty(), "warnings: {:?}", cfg.warnings);
    }

    #[test]
    fn missing_bsfc_is_fatal() {
        let bad = full_packet().replace("BSFC               : bsfc.dat\n", "");
        let err = read_emfac_files(bad.as_bytes()).unwrap_err();
        match err {
            Error::Parse { message, .. } => assert!(message.contains("BSFC")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn missing_packet_is_fatal() {
        let err = read_emfac_files(b"" as &[u8]).unwrap_err();
        match err {
            Error::Parse { message, .. } => assert!(message.contains("/EMFAC FILES/")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn co2_and_so2_files_are_warned_and_ignored() {
        let input = "\
/EMFAC FILES/
BSFC               : bsfc.dat
THC EXHAUST        : thc.exh
NOX EXHAUST        : nox.exh
CO EXHAUST         : co.exh
PM EXHAUST         : pm.exh
CRANKCASE          : crank.dat
DIURNAL            : diurnal.dat
SPILLAGE           : spill.dat
TANK PERM          : tank.dat
NON-RM HOSE PERM   : hose.dat
RM FILL NECK PERM  : neck.dat
RM SUPPLY/RETURN   : sr.dat
RM VENT PERM       : vent.dat
SO2 EXHAUST        : so2.dat
CO2 EXHAUST        : co2.dat
/END/
/DETERIORATE FILES/
THC EXHAUST        : thc.det
NOX EXHAUST        : nox.det
CO EXHAUST         : co.det
PM EXHAUST         : pm.det
/END/
";
        let cfg = read_emfac_files(input.as_bytes()).unwrap();
        assert!(cfg.emission_factor(PollutantIndex::SoxExhaust).is_none());
        assert!(cfg.emission_factor(PollutantIndex::Co2Exhaust).is_none());
        assert!(
            cfg.warnings.iter().any(|w| w.contains("SoxExhaust")),
            "expected SOx warning, got {:?}",
            cfg.warnings
        );
        assert!(
            cfg.warnings.iter().any(|w| w.contains("Co2Exhaust")),
            "expected CO2 warning, got {:?}",
            cfg.warnings
        );
    }

    #[test]
    fn missing_pm_emfac_emits_warning() {
        let input = "\
/EMFAC FILES/
BSFC               : bsfc.dat
THC EXHAUST        : thc.exh
NOX EXHAUST        : nox.exh
CO EXHAUST         : co.exh
CRANKCASE          : crank.dat
DIURNAL            : diurnal.dat
SPILLAGE           : spill.dat
TANK PERM          : tank.dat
NON-RM HOSE PERM   : hose.dat
RM FILL NECK PERM  : neck.dat
RM SUPPLY/RETURN   : sr.dat
RM VENT PERM       : vent.dat
/END/
/DETERIORATE FILES/
THC EXHAUST        : thc.det
NOX EXHAUST        : nox.det
CO EXHAUST         : co.det
PM EXHAUST         : pm.det
/END/
";
        let cfg = read_emfac_files(input.as_bytes()).unwrap();
        assert!(
            cfg.warnings.iter().any(|w| w.contains("PmExhaust")),
            "expected PM warning, got {:?}",
            cfg.warnings
        );
    }

    #[test]
    fn missing_deterioration_packet_emits_warning() {
        let input = "\
/EMFAC FILES/
BSFC               : bsfc.dat
THC EXHAUST        : thc.exh
NOX EXHAUST        : nox.exh
CO EXHAUST         : co.exh
PM EXHAUST         : pm.exh
CRANKCASE          : crank.dat
DIURNAL            : diurnal.dat
SPILLAGE           : spill.dat
TANK PERM          : tank.dat
NON-RM HOSE PERM   : hose.dat
RM FILL NECK PERM  : neck.dat
RM SUPPLY/RETURN   : sr.dat
RM VENT PERM       : vent.dat
/END/
";
        let cfg = read_emfac_files(input.as_bytes()).unwrap();
        assert!(cfg.deterioration_factors.is_empty());
        assert!(
            cfg.warnings.iter().any(|w| w.contains("/DETERIORATE")),
            "expected deterioration-packet warning, got {:?}",
            cfg.warnings
        );
    }

    #[test]
    fn unknown_emfac_label_is_fatal() {
        let bad = full_packet().replace("THC EXHAUST        :", "MYSTERY            :");
        let err = read_emfac_files(bad.as_bytes()).unwrap_err();
        match err {
            Error::Parse { message, .. } => assert!(message.contains("unknown")),
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
