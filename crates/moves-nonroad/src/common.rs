/// Unified error type for the NONROAD port.
///
/// All fallible functions return `Result<T, NonroadError>` instead of Fortran's
/// integer status codes. Functions that cannot fail remain infallible.
#[derive(Debug)]
pub enum NonroadError {
    Io(std::io::Error),
    Parse(String),
    NotFound(String),
    InvalidInput(String),
    NumericOverflow(String),
    /// Placeholder for unimplemented operations.
    Other(String),
}

impl std::fmt::Display for NonroadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NonroadError::Io(e) => write!(f, "I/O error: {e}"),
            NonroadError::Parse(msg) => write!(f, "parse error: {msg}"),
            NonroadError::NotFound(msg) => write!(f, "not found: {msg}"),
            NonroadError::InvalidInput(msg) => write!(f, "invalid input: {msg}"),
            NonroadError::NumericOverflow(msg) => write!(f, "numeric overflow: {msg}"),
            NonroadError::Other(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for NonroadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            NonroadError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for NonroadError {
    fn from(e: std::io::Error) -> Self {
        NonroadError::Io(e)
    }
}

/// Legacy maximum dimension constants (from nonrdprm.inc).
pub mod legacy_limits {
    pub const MXEQIP_LEGACY_MAX: usize = 25;
    pub const MXPOL_LEGACY_MAX: usize = 23;
    pub const NSTATE_LEGACY_MAX: usize = 53;
    pub const NCNTY_LEGACY_MAX: usize = 3_400;
    pub const MXTECH_LEGACY_MAX: usize = 15;
    pub const MXHPC_LEGACY_MAX: usize = 18;
    pub const MXAGYR_LEGACY_MAX: usize = 51;
    pub const MXDAYS_LEGACY_MAX: usize = 365;
    pub const MXSUBC_LEGACY_MAX: usize = 300;
    pub const MXEMFC_LEGACY_MAX: usize = 13_000;
    pub const MXDTFC_LEGACY_MAX: usize = 120;
    pub const MXPOP_LEGACY_MAX: usize = 1_000;
}

/// Top-level simulation context replacing COMMON-block global state.
///
/// Each `*State` struct replaces one group of COMMON blocks from the Fortran
/// include files. Passed explicitly between modules — no global mutable state.
pub struct NonroadContext {
    pub equipment: EquipmentState,
    pub pollutants: PollutantState,
    pub geography: GeographyState,
    pub population: PopulationState,
    pub emission_factors: EmissionFactorState,
    pub allocation: AllocationState,
    pub output: OutputState,
    pub temporal: TemporalState,
}

// State structs — each replaces one `.inc` file's COMMON blocks.
// Detailed field definitions will be filled in by Task 92.

pub struct EquipmentState;
pub struct PollutantState;
pub struct GeographyState;
pub struct PopulationState;
pub struct EmissionFactorState;
pub struct AllocationState;
pub struct OutputState;
pub struct TemporalState;

// Type aliases used across modules.

pub type EquipmentId = i32;
pub type PollutantId = i32;
pub type ProcessId = i32;
pub type SccKey = String;
pub type Year = i32;
pub type Month = i32;
pub type Day = i32;
pub type StateFips = String;
pub type CountyFips = String;
