//! NONROAD original Fortran parameter dimensions (`nonrdprm.inc`).
//!
//! Per the array-size policy in `ARCHITECTURE.md` (§ 4.1), these are
//! **documentation only** in the Rust port: dynamic data structures
//! ([`Vec`], `HashMap`, `BTreeMap`, `ndarray`) replace the
//! fixed-size Fortran arrays. The constants are preserved here so
//! that:
//!
//! * code reviewing the port against the Fortran source can confirm
//!   the original dimensions;
//! * fixture data using the original ceilings round-trips through
//!   the port without surprise resizing;
//! * test suites can use them as sanity ceilings (e.g. assert that
//!   no fixture loads more than [`MXEMFC`] records, confirming the
//!   port has not silently exceeded the original capacity).
//!
//! Task 93 ports the rest of `nonrdprm.inc` (chemical constants,
//! conversion factors) into this module as additional `pub const`
//! items.

/// Maximum equipment categories per run.
///
/// Original Fortran parameter: `MXEQIP = 25` in `nonrdprm.inc`.
/// Documentation only — `Vec`-backed structures replace fixed arrays.
pub const MXEQIP: usize = 25;

/// Maximum pollutants per run.
///
/// Original Fortran parameter: `MXPOL = 23` in `nonrdprm.inc`.
pub const MXPOL: usize = 23;

/// State count used by FIPS-state arrays.
///
/// Original Fortran parameter: `NSTATE = 53` in `nonrdprm.inc`.
pub const NSTATE: usize = 53;

/// Maximum counties per run.
///
/// Original Fortran parameter: `NCNTY = 3400` in `nonrdprm.inc`.
pub const NCNTY: usize = 3400;

/// Maximum exhaust technology types.
///
/// Original Fortran parameter: `MXTECH = 15` in `nonrdprm.inc`.
pub const MXTECH: usize = 15;

/// Maximum evaporative technology types.
///
/// Original Fortran parameter: `MXEVTECH = 15` in `nonrdprm.inc`.
pub const MXEVTECH: usize = 15;

/// Maximum horsepower categories.
///
/// Original Fortran parameter: `MXHPC = 18` in `nonrdprm.inc`.
pub const MXHPC: usize = 18;

/// Maximum model-year ages tracked per equipment population.
///
/// Original Fortran parameter: `MXAGYR = 51` in `nonrdprm.inc`.
pub const MXAGYR: usize = 51;

/// Maximum days in a year.
///
/// Original Fortran parameter: `MXDAYS = 365` in `nonrdprm.inc`.
/// Leap-year handling is documented in Task 113.
pub const MXDAYS: usize = 365;

/// Maximum subcounty entries.
///
/// Original Fortran parameter: `MXSUBC = 300` in `nonrdprm.inc`.
pub const MXSUBC: usize = 300;

/// Maximum emission-factor table size.
///
/// Original Fortran parameter: `MXEMFC = 13000` in `nonrdprm.inc`.
pub const MXEMFC: usize = 13_000;

/// Maximum deterioration-factor table size.
///
/// Original Fortran parameter: `MXDTFC = 120` in `nonrdprm.inc`.
pub const MXDTFC: usize = 120;

/// Maximum population records loaded per run.
///
/// Original Fortran parameter: `MXPOP = 1000` in `nonrdprm.inc`.
pub const MXPOP: usize = 1_000;
