//! Options-file initialisation sequencer (`intnon.f`).
//!
//! Task 99. The Fortran `intnon.f` is a 307-line driver that walks
//! every `rd*.f` parser in a fixed order, stopping on the first
//! failure. In Rust the per-parser implementations already live in
//! [`crate::input`] (Tasks 94–98); this module supplies the
//! orchestration glue.
//!
//! # Scope of the Rust port
//!
//! The Fortran routine touches three categories of input:
//!
//! 1. **Options-file packets** — `/OPTIONS/`, `/PERIOD/`, `/REGION/`,
//!    `/SOURCE CATEGORY/`, `/EMFAC FILES/`, `/DETERIORATE FILES/`,
//!    `/STAGE II/`, `/PM BASE SULFUR/` — all parsed from the same
//!    options-file content.
//! 2. **Files declared in `/RUNFILES/`** — allocation, activity,
//!    technology, seasonality, regions, FIPS, retrofit, population.
//! 3. **Cross-cutting initialisation** — [`crate::init::intadj`]
//!    seeds the sulfur/RFG adjustment tables, [`crate::init::intams`]
//!    seeds the AMS output parameters.
//!
//! [`run_options_file_init`] implements (1) and (3) — everything that
//! is bounded by the options-file content. The per-file loaders
//! covered in (2) remain available through their respective modules
//! and the [`crate::input::efls`] / [`crate::input::bsfc`]
//! dispatchers; Task 113 ties (2) into the full driver loop.
//!
//! # Fortran source
//!
//! Ports `intnon.f` (307 lines).

use std::io::Cursor;

use crate::input::emfac_files::{read_emfac_files, EmfacFiles};
use crate::input::options::{read_options, OptionsConfig};
use crate::input::period::{read_period, PeriodConfig};
use crate::input::region::{read_region, RegionConfig};
use crate::input::source_cat::{read_source_category, SourceCategorySelection};
use crate::input::stage2::{read_stg2, Stage2Factor};
use crate::input::sulfur::{read_sulf, SulfurRecord};
use crate::Result;

use super::intadj::AdjustmentTables;
use super::intams::{initialize_ams_params, AmsParams};

/// State produced from parsing every packet held within the options
/// file plus the cross-cutting [`AdjustmentTables`] and [`AmsParams`]
/// initialisation.
#[derive(Debug, Clone)]
pub struct OptionsFileState {
    /// Parsed `/OPTIONS/` packet.
    pub options: OptionsConfig,
    /// Parsed `/PERIOD/` packet.
    pub period: PeriodConfig,
    /// Parsed `/REGION/` packet.
    pub region: RegionConfig,
    /// Parsed `/SOURCE CATEGORY/` packet (defaults to "all sources"
    /// when absent).
    pub source_category: SourceCategorySelection,
    /// Parsed `/EMFAC FILES/` + `/DETERIORATE FILES/` packets.
    pub emfac_files: EmfacFiles,
    /// Parsed `/STAGE II/` packet (passthrough if absent).
    pub stage2: Stage2Factor,
    /// Parsed `/PM BASE SULFUR/` packet (empty if absent).
    pub sulfur: Vec<SulfurRecord>,
    /// Sulfur and RFG-adjustment tables seeded from
    /// [`Self::options`].
    pub adjustments: AdjustmentTables,
    /// AMS output parameters seeded from [`Self::period`].
    pub ams: AmsParams,
    /// Non-fatal warnings collected across parsers + initialisers.
    pub warnings: Vec<String>,
}

/// Run the options-file initialisation sequence.
///
/// `options_text` is the full contents of the `.opt` file. Each
/// sub-parser scans it from the start for its specific packet —
/// mirroring how the Fortran source rewinds `IORUSR` between calls.
///
/// The function returns at the first fatal parse error encountered,
/// matching the Fortran `goto 9999` short-circuit behaviour.
pub fn run_options_file_init(options_text: &str) -> Result<OptionsFileState> {
    let mut warnings: Vec<String> = Vec::new();

    let options = read_options(Cursor::new(options_text))?;
    let period = read_period(Cursor::new(options_text))?;
    warnings.extend(period.warnings.iter().cloned());

    let region = read_region(Cursor::new(options_text))?;
    let source_category = read_source_category(Cursor::new(options_text))?;

    let emfac_files = read_emfac_files(Cursor::new(options_text))?;
    warnings.extend(emfac_files.warnings.iter().cloned());

    let stage2 = read_stg2(Cursor::new(options_text))?;
    let sulfur = read_sulf(Cursor::new(options_text))?;

    let adjustments = AdjustmentTables::from_options(&options);
    let ams = initialize_ams_params(&period);
    warnings.extend(ams.warnings.iter().cloned());

    Ok(OptionsFileState {
        options,
        period,
        region,
        source_category,
        emfac_files,
        stage2,
        sulfur,
        adjustments,
        ams,
        warnings,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::options::AltitudeFlag;
    use crate::input::period::{PeriodType, SummaryType};
    use crate::input::region::RegionLevel;

    fn sample_options_file() -> String {
        // A minimum-viable options file that exercises every required
        // packet in the order intnon.f walks them. Optional packets
        // are exercised in dedicated tests.
        "\
/OPTIONS/
Title 1            : Demo
Title 2            : Test
Fuel RVP           : 9.0
Oxygen Pct         : 2.7
Gas Sulfur         : 0.030
Diesel Sulfur      : 0.0015
CNG Sulfur         : 0.0001
Temp Min           : 60.0
Temp Max           : 84.0
Temp Mean          : 72.0
Altitude           : LOW
/END/
/PERIOD/
Period Type        : ANNUAL
Summary Type       : TOTAL
Episode Year       : 2025
Season             :
Month              :
Day                : WEEKDAY
Growth Year        : 2025
Technology Year    : 2025
/END/
/REGION/
Level              : COUNTY
Region             : 17031
/END/
/SOURCE CATEGORY/
Source             : 2270001000
/END/
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
        .to_string()
    }

    #[test]
    fn parses_minimal_options_file() {
        let text = sample_options_file();
        let state = run_options_file_init(&text).unwrap();
        // Options
        assert_eq!(state.options.altitude, AltitudeFlag::Low);
        // Period
        assert_eq!(state.period.period_type, PeriodType::Annual);
        assert_eq!(state.period.episode_year, 2025);
        // Region
        assert_eq!(state.region.level, RegionLevel::County);
        // Source category
        assert!(matches!(
            state.source_category,
            SourceCategorySelection::Selected(_)
        ));
        // Emfac files
        assert_eq!(state.emfac_files.bsfc.to_str().unwrap(), "bsfc.dat");
        // Stage2 / sulfur default to no-op
        assert!((state.stage2.retention_factor - 1.0).abs() < 1e-6);
        assert!(state.sulfur.is_empty());
        // Adjustments populated
        assert!((state.adjustments.sox_full[0] - 0.030).abs() < 1e-6);
        // AMS parameters populated
        assert_eq!(state.ams.report_type, 'B');
        assert_eq!(state.ams.reference_year, 25);
    }

    #[test]
    fn propagates_stage2_and_sulfur_when_present() {
        let mut text = sample_options_file();
        text.push_str("/STAGE II/                           25.0\n");
        text.push_str(
            "/PM BASE SULFUR/
BASE       0.0015 0.07
/END/
",
        );
        let state = run_options_file_init(&text).unwrap();
        assert!((state.stage2.reduction_pct - 25.0).abs() < 1e-6);
        assert!((state.stage2.retention_factor - 0.75).abs() < 1e-6);
        assert_eq!(state.sulfur.len(), 1);
    }

    #[test]
    fn typical_day_annual_collects_warning_from_intams() {
        let text = sample_options_file().replace("TOTAL", "TYPICAL DAY");
        let state = run_options_file_init(&text).unwrap();
        // intams emits a typical-day-mismatch warning for annual typical-day.
        assert_eq!(state.period.summary_type, SummaryType::TypicalDay);
        assert!(
            state.warnings.iter().any(|w| w.contains("typical day")),
            "warnings: {:?}",
            state.warnings
        );
    }

    #[test]
    fn missing_required_packet_short_circuits() {
        // Strip the /OPTIONS/ packet entirely.
        let text = "\
/PERIOD/
Period Type        : ANNUAL
Summary Type       : TOTAL
Episode Year       : 2025
Season             :
Month              :
Day                : WEEKDAY
Growth Year        : 2025
Technology Year    : 2025
/END/
/REGION/
Level              : COUNTY
Region             : 17031
/END/
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
        let err = run_options_file_init(text).unwrap_err();
        // /OPTIONS/ packet is the first parser invoked, so its
        // absence surfaces first.
        let msg = format!("{err}");
        assert!(
            msg.contains("/OPTIONS/") || msg.contains("OPTIONS"),
            "expected OPTIONS parse error, got {msg:?}"
        );
    }
}
