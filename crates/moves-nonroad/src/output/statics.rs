//! NONROAD `BLOCK DATA` static tables — `blknon.f` (Task 114).
//!
//! Fortran's `BLOCK DATA` program unit initialises named COMMON-block
//! variables at load time. `blknon.f` is NONROAD's: it seeds the
//! scratch filenames, the warning/`DATA`-counter arrays, the
//! month-to-season map, the per-pollutant name and code tables, the
//! horsepower-category boundaries, and the SI-report tech-type
//! tables.
//!
//! The Rust port replaces it with `const` data. Three groups of
//! `blknon.f` initialisers are *not* re-exported here because the
//! Rust type system supplies them for free or another module already
//! owns them:
//!
//! * **All-zero / all-false arrays** — `ndtfac` (`MXPOL*0`), `ldays`
//!   (`2*.FALSE.`), `lmonth` (`12*.FALSE.`), and the SI-report
//!   accumulators `popsi`/`actsi`/`fuelsi`/`emissi`. A Rust `Vec` or
//!   array gets these from `Default`; the SI accumulator's zero state
//!   is [`crate::output::si_report::SiReport::default`].
//! * **`hpclev`** — the 18 horsepower-category boundaries are already
//!   ported as [`crate::output::find::HPCLEV`] (Task 101); they are
//!   not duplicated here.
//! * **Scratch filenames** — [`SCRATCH_POP_FILE`], [`SCRATCH_IND_FILE`]
//!   and [`SCRATCH_GRW_FILE`] are kept as documentation of the legacy
//!   names, but the Rust port has no on-disk scratch files (the I/O
//!   policy of `ARCHITECTURE.md` § 4.3 passes in-memory buffers), so
//!   nothing reads them at runtime.

use crate::common::consts::MXPOL;

/// Legacy scratch-file name for the sorted population data —
/// `blknon.f` `data spopfl /'poptmp.txt'/`.
///
/// The Rust port keeps reference data in memory and never writes this
/// file; the constant documents the original name only.
pub const SCRATCH_POP_FILE: &str = "poptmp.txt";

/// Legacy scratch-file name for the spatial-indicator data —
/// `blknon.f` `data indfl /'indtmp.txt'/`.
pub const SCRATCH_IND_FILE: &str = "indtmp.txt";

/// Legacy scratch-file name for the growth-indicator data —
/// `blknon.f` `data grwfl /'grwtmp.txt'/`.
pub const SCRATCH_GRW_FILE: &str = "grwtmp.txt";

/// Season index for each month — `blknon.f` `data idseas /…/`.
///
/// Indexed by 0-based month (`January` = 0). The values are NONROAD's
/// 1-based season parameters (`IDXWTR = 1`, `IDXSPR = 2`,
/// `IDXSUM = 3`, `IDXFAL = 4`): December and January–February map to
/// winter, March–May to spring, June–August to summer, and
/// September–November to fall.
pub const MONTH_SEASON: [u8; 12] = [1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4, 1];

/// Model pollutant names — `blknon.f` `data polnam /…/`.
///
/// Indexed by 0-based pollutant slot
/// ([`crate::emissions::exhaust::PollutantIndex::slot`]). The Fortran
/// `polnam` is a `character*10` array; the trailing blanks of each
/// fixed-width entry are dropped here — a consumer that needs the
/// padded form applies [`crate::output::fortran_fmt::fortran_a`] with
/// width 10.
pub const POLLUTANT_NAMES: [&str; MXPOL] = [
    "Exh. THC",   // 0  IDXTHC
    "Exh. CO",    // 1  IDXCO
    "Exh. NOX",   // 2  IDXNOX
    "Exh. CO2",   // 3  IDXCO2
    "Exh. SO2",   // 4  IDXSOX
    "Exh. PM",    // 5  IDXPM
    "Crankcase",  // 6  IDXCRA
    "Diurnal",    // 7  IDXDIU
    "Tank Perm",  // 8  IDXTKP
    "Hose Perm",  // 9  IDXHOS
    "Neck Perm",  // 10 IDXNCK
    "S/R Perm",   // 11 IDXSR
    "Vent Perm",  // 12 IDXVNT
    "Hot Soak",   // 13 IDXSOK
    "Refueling",  // 14 IDXDIS
    "Spillage",   // 15 IDXSPL
    "RuningLoss", // 16 IDXRLS  (spelling preserved from NMRLS)
    "Start THC",  // 17 IDSTHC
    "Start CO",   // 18 IDSCO
    "Start NOX",  // 19 IDSNOX
    "Start CO2",  // 20 IDSCO2
    "Start SO2",  // 21 IDSSOX
    "Start PM",   // 22 IDSPM
];

/// AMS criteria-pollutant names — `blknon.f` `data amspol /…/`.
///
/// Indexed by 0-based pollutant slot. Every hydrocarbon-bearing
/// pollutant (crankcase, the evaporative modes, the rec-marine hose
/// modes) maps to the `THC` criteria pollutant, matching the Fortran
/// `AMSTHC` assignments. The Fortran `amspol` array is `character*4`.
pub const POLLUTANT_AMS_NAMES: [&str; MXPOL] = [
    "THC", // 0  IDXTHC
    "CO",  // 1  IDXCO
    "NOX", // 2  IDXNOX
    "CO2", // 3  IDXCO2
    "SO2", // 4  IDXSOX
    "PM",  // 5  IDXPM
    "THC", // 6  IDXCRA
    "THC", // 7  IDXDIU
    "THC", // 8  IDXTKP
    "THC", // 9  IDXHOS
    "THC", // 10 IDXNCK
    "THC", // 11 IDXSR
    "THC", // 12 IDXVNT
    "THC", // 13 IDXSOK
    "THC", // 14 IDXDIS
    "THC", // 15 IDXSPL
    "THC", // 16 IDXRLS
    "THC", // 17 IDSTHC
    "CO",  // 18 IDSCO
    "NOX", // 19 IDSNOX
    "CO2", // 20 IDSCO2
    "SO2", // 21 IDSSOX
    "PM",  // 22 IDSPM
];

/// SAROAD pollutant codes — `blknon.f` `data iscod /…/`.
///
/// Indexed by 0-based pollutant slot. The hydrocarbon-bearing
/// pollutants all carry the `ISCTHC` code. Slots `blknon.f` leaves
/// uninitialised (`IDXCO2`, the four permeation modes, `IDSCO2`) are
/// `0` here, mirroring the Fortran `BLOCK DATA` default and the
/// commented-out `iscod(IDXCO2)`/`iscod(IDSCO2)` lines.
pub const POLLUTANT_SAROAD: [i32; MXPOL] = [
    43101, // 0  IDXTHC = ISCTHC
    42101, // 1  IDXCO  = ISCCO
    42603, // 2  IDXNOX = ISCNOX
    0,     // 3  IDXCO2 (uninitialised in blknon.f)
    42401, // 4  IDXSOX = ISCSOX
    81102, // 5  IDXPM  = ISCPM
    43101, // 6  IDXCRA = ISCTHC
    43101, // 7  IDXDIU = ISCTHC
    0,     // 8  IDXTKP (uninitialised)
    0,     // 9  IDXHOS (uninitialised)
    0,     // 10 IDXNCK (uninitialised)
    0,     // 11 IDXSR  (uninitialised)
    0,     // 12 IDXVNT (uninitialised)
    43101, // 13 IDXSOK = ISCTHC
    43101, // 14 IDXDIS = ISCTHC
    43101, // 15 IDXSPL = ISCTHC
    43101, // 16 IDXRLS = ISCTHC
    43101, // 17 IDSTHC = ISCTHC
    42101, // 18 IDSCO  = ISCCO
    42603, // 19 IDSNOX = ISCNOX
    0,     // 20 IDSCO2 (uninitialised)
    42401, // 21 IDSSOX = ISCSOX
    81102, // 22 IDSPM  = ISCPM
];

/// Maximum input tech types tracked for the SI report — Fortran
/// `MXITCH = 42` in `nonrdeqp.inc`.
pub const MXITCH: usize = 42;

/// Maximum output tech-type bins for the SI report — Fortran
/// `MXOTCH = 14` in `nonrdeqp.inc`.
pub const MXOTCH: usize = 14;

/// Input tech-type codes mapped in the SI report — `blknon.f`
/// `data sitech /…/`.
///
/// The 42 codes group in threes; each group of three input codes maps
/// to one of the [`MXOTCH`] output bins via [`SI_INDEX`] (the
/// same-position entry).
pub const SI_TECH: [&str; MXITCH] = [
    "G2N1", "G2N11", "G2N12", "G4N1O", "G4N1O1", "G4N1O2", //
    "G4N1S", "G4N1S1", "G4N1S2", "G4N1SC", "G4N1SC1", "G4N1SC2", //
    "G2N2", "G2N21", "G2N22", "G4N2O", "G4N2O1", "G4N2O2", //
    "G4N2S", "G4N2S1", "G4N2S2", "G2H3", "G2H31", "G2H32", //
    "G2H3C", "G2H3C1", "G2H3C2", "G2H4", "G2H41", "G2H42", //
    "G2H4C", "G2H4C1", "G2H4C2", "G4H4", "G4H41", "G4H42", //
    "G2H5", "G2H51", "G2H52", "G2H5C", "G2H5C1", "G2H5C2", //
];

/// SI-report output bin for each [`SI_TECH`] code — `blknon.f`
/// `data indxsi /…/`.
///
/// 1-based bin index (`1..=`[`MXOTCH`]); position `i` is the bin for
/// `SI_TECH[i]`. Each consecutive group of three input codes shares a
/// bin.
pub const SI_INDEX: [u8; MXITCH] = [
    1, 1, 1, 2, 2, 2, //
    3, 3, 3, 4, 4, 4, //
    5, 5, 5, 6, 6, 6, //
    7, 7, 7, 8, 8, 8, //
    9, 9, 9, 10, 10, 10, //
    11, 11, 11, 12, 12, 12, //
    13, 13, 13, 14, 14, 14, //
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn si_tables_have_matching_lengths() {
        assert_eq!(SI_TECH.len(), MXITCH);
        assert_eq!(SI_INDEX.len(), MXITCH);
    }

    #[test]
    fn si_index_groups_threes_into_fourteen_bins() {
        // Each consecutive run of three input codes shares one bin,
        // and the bins run 1..=14.
        for (i, &bin) in SI_INDEX.iter().enumerate() {
            assert_eq!(bin as usize, i / 3 + 1);
        }
        assert_eq!(*SI_INDEX.iter().max().unwrap(), MXOTCH as u8);
    }

    #[test]
    fn month_season_maps_december_to_winter() {
        assert_eq!(MONTH_SEASON.len(), 12);
        assert_eq!(MONTH_SEASON[0], 1); // January → winter
        assert_eq!(MONTH_SEASON[11], 1); // December → winter
        assert_eq!(MONTH_SEASON[6], 3); // July → summer
    }

    #[test]
    fn pollutant_tables_cover_every_slot() {
        assert_eq!(POLLUTANT_NAMES.len(), MXPOL);
        assert_eq!(POLLUTANT_AMS_NAMES.len(), MXPOL);
        assert_eq!(POLLUTANT_SAROAD.len(), MXPOL);
        // The hydrocarbon family carries the THC criteria pollutant.
        assert_eq!(POLLUTANT_AMS_NAMES[6], "THC"); // crankcase
        assert_eq!(POLLUTANT_SAROAD[6], 43101); // crankcase = ISCTHC
                                                // CO2 is left uninitialised by blknon.f.
        assert_eq!(POLLUTANT_SAROAD[3], 0);
    }
}
