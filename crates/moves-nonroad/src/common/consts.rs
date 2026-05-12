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

/// Maximum age-bin count for the `/AGE ADJUSTMENT/` curve.
///
/// Original Fortran parameter: `MXUSE = 51` in `nonrdact.inc`.
/// Used by [`crate::population::modyr`] when sizing the
/// `agebin`/`agepct` tables. Equal to [`MXAGYR`] but kept distinct
/// to mirror the Fortran source.
pub const MXUSE: usize = 51;

/// Maximum number of alternate age-vs-activity curves.
///
/// Original Fortran parameter: `MXAGE = 10` in `nonrdact.inc`.
pub const MXAGE: usize = 10;

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

/// Maximum scrappage-curve bins (per curve).
///
/// Original Fortran parameter: `MXSCRP = 197` in `nonrdeqp.inc`.
pub const MXSCRP: usize = 197;

/// Maximum retrofit records loaded per run.
///
/// Original Fortran parameter: `MXRTRFT = 500` in `nonrdrtrft.inc`.
pub const MXRTRFT: usize = 500;

/// Number of pollutants that are valid in retrofit records.
///
/// Original Fortran parameter: `NRTRFTPLLTNT = 4` in `nonrdrtrft.inc`.
/// The four allowed pollutants are HC, CO, NOx, PM.
pub const NRTRFTPLLTNT: usize = 4;

/// Index of total HC in pollutant arrays.
///
/// Original Fortran parameter: `IDXTHC = 1` in `nonrdprm.inc`.
/// Preserved as a 1-based Fortran index for cross-reference; Rust
/// callers offset to 0-based as needed.
pub const IDXTHC: usize = 1;

/// Index of CO in pollutant arrays.
///
/// Original Fortran parameter: `IDXCO = 2` in `nonrdprm.inc`.
pub const IDXCO: usize = 2;

/// Index of NOx in pollutant arrays.
///
/// Original Fortran parameter: `IDXNOX = 3` in `nonrdprm.inc`.
pub const IDXNOX: usize = 3;

/// Index of CO2 in pollutant arrays.
///
/// Original Fortran parameter: `IDXCO2 = 4` in `nonrdprm.inc`.
pub const IDXCO2: usize = 4;

/// Index of PM in pollutant arrays.
///
/// Original Fortran parameter: `IDXPM = 6` in `nonrdprm.inc`.
/// Note that the PM index skips `5` — that slot holds another
/// pollutant that does not participate in retrofit records.
pub const IDXPM: usize = 6;

/// Minimum non-zero growth indicator value.
///
/// Original Fortran constant: `MINGRWIND = 0.0001` in `nonrdprm.inc`.
/// Used by `grwfac` to avoid divide-by-zero when the base-year
/// indicator is exactly zero.
pub const MINGRWIND: f32 = 0.0001;

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

/// Compressed natural gas density (lbs/gal).
///
/// Original Fortran constant: `DENCNG = 0.0061` in `nonrdprm.inc`.
/// (12/18/01 fix: the source comment notes this was previously
/// `0.0517` lb/cu-ft and was converted to lb/gal.)
pub const DENCNG: f64 = 0.0061;

/// Liquefied petroleum gas density (lbs/gal).
///
/// Original Fortran constant: `DENLPG = 4.507` in `nonrdprm.inc`.
pub const DENLPG: f64 = 4.507;

/// Diesel density (lbs/gal).
///
/// Original Fortran constant: `DENDSL = 7.044` in `nonrdprm.inc`.
pub const DENDSL: f64 = 7.044;

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

// ============================================================================
// Sentinels, conversion factors, and per-fuel coefficients used by
// the Task 106 exhaust calculator (clcems / emfclc / emsadj / unitcf)
// and the Task 107 evaporative calculator (clcevems / evemfclc).
// ============================================================================

/// Real-valued "missing" sentinel.
///
/// Original Fortran constant: `RMISS = -9.0` in `nonrdprm.inc`. The
/// Fortran source uses this for "no emission factor found": the
/// exhaust calculator (`clcems.f`) propagates it through
/// `emsday`/`emsbmy` for fidelity, and the evaporative-factor code
/// (`evemfclc.f`) writes it into slots whose tech-type fraction is
/// non-zero but no record has yet been found — the evap calculator
/// (`clcevems.f`) then interprets `< 0` factor values as "data
/// missing" and switches to a zero-emission branch.
pub const RMISS: f32 = -9.0;

/// Minimum diurnal temperature (degrees Fahrenheit).
///
/// Original Fortran parameter: `DIUMIN = 40.0` in `nonrdprm.inc`. The
/// diurnal branch of `clcevems.f` clamps both `tmin` and `tmax` to
/// `DIUMIN` and bypasses the calculation entirely when `tmax <= DIUMIN`
/// (added 2005-08-17 to suppress the tiny negative-VapGen artefact
/// triggered by floating-point drift at 40 °F).
pub const DIUMIN: f32 = 40.0;

/// Grams-to-short-tons conversion factor.
///
/// Original Fortran constant: `CVTTON = 1.102311E-06` in `nonrdprm.inc`.
/// All emission outputs in `clcems.f` / `clcevems.f` / `clcrtrft.f`
/// scale from grams to short tons via this factor.
pub const CVTTON: f32 = 1.102311e-06;

/// Carbon mass fraction for gasoline.
///
/// Original Fortran constant: `CMFGAS = 0.87` in `nonrdprm.inc`.
/// Used by the CO2 branch of `clcems.f`.
pub const CMFGAS: f32 = 0.87;

/// Carbon mass fraction for CNG.
///
/// Original Fortran constant: `CMFCNG = 0.717` in `nonrdprm.inc`.
pub const CMFCNG: f32 = 0.717;

/// Carbon mass fraction for LPG.
///
/// Original Fortran constant: `CMFLPG = 0.817` in `nonrdprm.inc`.
pub const CMFLPG: f32 = 0.817;

/// Carbon mass fraction for diesel.
///
/// Original Fortran constant: `CMFDSL = 0.87` in `nonrdprm.inc`.
pub const CMFDSL: f32 = 0.87;

/// Baseline sulfur weight content for 2-stroke gasoline (fraction).
///
/// Original Fortran constant: `SWTGS2 = 0.0339` in `nonrdprm.inc`.
pub const SWTGS2: f32 = 0.0339;

/// Baseline sulfur weight content for 4-stroke gasoline (fraction).
///
/// Original Fortran constant: `SWTGS4 = 0.0339` in `nonrdprm.inc`.
pub const SWTGS4: f32 = 0.0339;

/// Baseline sulfur weight content for LPG (fraction).
///
/// Original Fortran constant: `SWTLPG = 0.008` in `nonrdprm.inc`.
pub const SWTLPG: f32 = 0.008;

/// Baseline sulfur weight content for CNG (fraction).
///
/// Original Fortran constant: `SWTCNG = 0.008` in `nonrdprm.inc`.
pub const SWTCNG: f32 = 0.008;

/// Baseline sulfur weight content for diesel (fraction).
///
/// Original Fortran constant: `SWTDSL = 0.33` in `nonrdprm.inc`.
pub const SWTDSL: f32 = 0.33;

/// Fraction of 2-stroke gasoline sulfur that becomes PM.
///
/// Original Fortran constant: `SFCGS2 = 0.03` in `nonrdprm.inc`.
pub const SFCGS2: f32 = 0.03;

/// Fraction of 4-stroke gasoline sulfur that becomes PM.
///
/// Original Fortran constant: `SFCGS4 = 0.03` in `nonrdprm.inc`.
pub const SFCGS4: f32 = 0.03;

/// Fraction of LPG sulfur that becomes PM.
///
/// Original Fortran constant: `SFCLPG = 0.03` in `nonrdprm.inc`.
pub const SFCLPG: f32 = 0.03;

/// Fraction of CNG sulfur that becomes PM.
///
/// Original Fortran constant: `SFCCNG = 0.03` in `nonrdprm.inc`.
pub const SFCCNG: f32 = 0.03;

/// Fraction of diesel sulfur that becomes PM.
///
/// Original Fortran constant: `SFCDSL = 0.02247` in `nonrdprm.inc`.
pub const SFCDSL: f32 = 0.02247;

/// Altitude correction factor for 2-stroke gasoline.
///
/// Original Fortran constant: `ALTGS2 = 1.0` in `nonrdprm.inc`. The
/// `1.0` value makes this a no-op in production, but the Rust port
/// keeps it as a named constant so a future data update only needs
/// to change one place.
pub const ALTGS2: f32 = 1.0;

/// Altitude correction factor for 4-stroke gasoline.
///
/// Original Fortran constant: `ALTGS4 = 1.0` in `nonrdprm.inc`.
pub const ALTGS4: f32 = 1.0;

/// Altitude correction factor for LPG.
///
/// Original Fortran constant: `ALTLPG = 1.0` in `nonrdprm.inc`.
pub const ALTLPG: f32 = 1.0;

/// Altitude correction factor for CNG.
///
/// Original Fortran constant: `ALTCNG = 1.0` in `nonrdprm.inc`.
pub const ALTCNG: f32 = 1.0;

/// Altitude correction factor for diesel.
///
/// Original Fortran constant: `ALTDSL = 1.0` in `nonrdprm.inc`.
pub const ALTDSL: f32 = 1.0;

/// Gasoline-pump spillage factor (grams per refueling event).
///
/// Original Fortran parameter: `PMPFAC = 3.6` in `nonrdefc.inc`. Used
/// by the spillage branch of `clcevems.f` (`IDXSPL`) when the refueling
/// mode is `PUMP` and a `SPILLAGE` EF file was supplied.
pub const PMPFAC: f32 = 3.6;

/// Portable-container spillage factor (grams per refueling event).
///
/// Original Fortran parameter: `CNTFAC = 17.0` in `nonrdefc.inc`. The
/// container counterpart to [`PMPFAC`]; used when refueling mode is
/// `CONTAINER`.
pub const CNTFAC: f32 = 17.0;
