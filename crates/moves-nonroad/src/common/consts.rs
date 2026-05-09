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

// ============================================================================
// Chemical and conversion constants from nonrdprm.inc
// ============================================================================
// These are true compile-time invariants used in calculations, distinct from
// the array-dimension parameters above which are now documentation only.

/// Gas density (lbs/gal).
///
/// Original Fortran constant: `DENGAS = 6.237` in `nonrdprm.inc`.
/// Used in fuel-related emission calculations.
pub const DENGAS: f64 = 6.237;

/// Gallons per cubic foot.
///
/// Original Fortran constant: `GALPERCF = 7.481` in `nonrdprm.inc`.
/// Volume conversion factor.
pub const GALPERCF: f64 = 7.481;

/// Pounds per gallon (general).
///
/// Original Fortran constant: `LBSGAL = 8.337` in `nonrdprm.inc`.
/// Used for water and other liquid density conversions.
pub const LBSGAL: f64 = 8.337;

/// Pounds per metric ton.
///
/// Original Fortran constant: `LBSMT = 2204.6` in `nonrdprm.inc`.
/// Mass conversion factor.
pub const LBSMT: f64 = 2204.6;

/// Pounds per short ton.
///
/// Original Fortran constant: `LBSTON = 2000.0` in `nonrdprm.inc`.
/// Mass conversion factor.
pub const LBSTON: f64 = 2000.0;

/// Grams per pound.
///
/// Original Fortran constant: `GRMLB = 453.6` in `nonrdprm.inc`.
/// Mass conversion factor.
pub const GRMLB: f64 = 453.6;

/// Grams per grain.
///
/// Original Fortran constant: `GRMGRN = 0.0648` in `nonrdprm.inc`.
/// Mass conversion factor.
pub const GRMGRN: f64 = 0.0648;

/// Meters per mile.
///
/// Original Fortran constant: `MTRMI = 1609.0` in `nonrdprm.inc`.
/// Length conversion factor.
pub const MTRMI: f64 = 1609.0;

/// Feet per mile.
///
/// Original Fortran constant: `FTPMI = 5280.0` in `nonrdprm.inc`.
/// Length conversion factor.
pub const FTPMI: f64 = 5280.0;

/// Inches per foot.
///
/// Original Fortran constant: `INCHFT = 12.0` in `nonrdprm.inc`.
/// Length conversion factor.
pub const INCHFT: f64 = 12.0;

/// Hours per day.
///
/// Original Fortran constant: `HRSDAY = 24.0` in `nonrdprm.inc`.
/// Time conversion factor.
pub const HRSDAY: f64 = 24.0;

/// Minutes per hour.
///
/// Original Fortran constant: `MINHR = 60.0` in `nonrdprm.inc`.
/// Time conversion factor.
pub const MINHR: f64 = 60.0;

/// Seconds per minute.
///
/// Original Fortran constant: `SECMIN = 60.0` in `nonrdprm.inc`.
/// Time conversion factor.
pub const SECMIN: f64 = 60.0;

/// Days per month (average).
///
/// Original Fortran constant: `DAYSMO = 30.42` in `nonrdprm.inc`.
/// Time conversion factor for monthly calculations.
pub const DAYSMO: f64 = 30.42;

/// Months per year.
///
/// Original Fortran constant: `MTHSYR = 12.0` in `nonrdprm.inc`.
/// Time conversion factor.
pub const MTHSYR: f64 = 12.0;

/// Cubic centimeters per cubic foot.
///
/// Original Fortran constant: `CCPERCF = 28317.0` in `nonrdprm.inc`.
/// Volume conversion factor.
pub const CCPERCF: f64 = 28317.0;

/// Cubic centimeters per liter.
///
/// Original Fortran constant: `CCPERLTR = 1000.0` in `nonrdprm.inc`.
/// Volume conversion factor.
pub const CCPERLTR: f64 = 1000.0;

/// Liters per gallon.
///
/// Original Fortran constant: `LTRSGAL = 3.785` in `nonrdprm.inc`.
/// Volume conversion factor.
pub const LTRSGAL: f64 = 3.785;

/// Parts per million conversion.
///
/// Original Fortran constant: `PPMCON = 1.0E-6` in `nonrdprm.inc`.
/// Concentration conversion factor.
pub const PPMCON: f64 = 1.0e-6;

/// Standard temperature (Kelvin).
///
/// Original Fortran constant: `TKZERO = 273.15` in `nonrdprm.inc`.
/// Temperature offset for Celsius to Kelvin conversion.
pub const TKZERO: f64 = 273.15;

/// Standard pressure (atmospheres).
///
/// Original Fortran constant: `ATMSTD = 1.0` in `nonrdprm.inc`.
/// Reference pressure for gas law calculations.
pub const ATMSTD: f64 = 1.0;

/// Ideal gas constant (L·atm/(mol·K)).
///
/// Original Fortran constant: `RGAS = 0.08206` in `nonrdprm.inc`.
/// Used in vapor pressure and diurnal emission calculations.
pub const RGAS: f64 = 0.08206;
