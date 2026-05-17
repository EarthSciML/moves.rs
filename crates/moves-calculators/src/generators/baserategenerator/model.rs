//! Data structures for the Base Rate Generator port.
//!
//! Ports the struct declarations from
//! `generators/baserategenerator/baserategenerator.go`. Field-for-field
//! analogues; Go `int` becomes [`i32`] (every MOVES identifier fits
//! comfortably) and Go `float64` becomes [`f64`].
//!
//! Every key type derives [`Ord`] so the port can use deterministic
//! [`BTreeMap`](std::collections::BTreeMap) collections where the Go used
//! hash maps — Go map iteration is randomised, so the Go logic never relies
//! on map order; using ordered maps in the port keeps test output stable
//! without changing any computed value.

use crate::error::{Error, Result};

/// `false` in normal operation — the Go `ALWAYS_USE_ROMD_TABLE` constant.
///
/// When `true`, the drive-cycle fast path is disabled and a fully populated
/// `RatesOpModeDistribution` table must be supplied instead. The Go keeps
/// this as a compile-time toggle; the port mirrors it for fidelity.
pub const ALWAYS_USE_ROMD_TABLE: bool = false;

/// Flags and identifiers controlling the table-join logic.
///
/// Ports the Go `externalFlags` struct together with the `flags` global.
/// The booleans default to `false` (Go zero value); [`from_parameters`]
/// reads them from the worker's `-parameters=` CSV list.
///
/// [`from_parameters`]: ExternalFlags::from_parameters
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ExternalFlags {
    /// Keep the real `opModeID` in the output key rather than collapsing
    /// it to `0`. Go `keepOpModeID`.
    pub keep_op_mode_id: bool,
    /// Retain the average-speed bin in the output key. Go `useAvgSpeedBin`.
    pub use_avg_speed_bin: bool,
    /// Weight rate sums by `avgSpeedFraction`. Go `useAvgSpeedFraction`.
    pub use_avg_speed_fraction: bool,
    /// Weight operating-mode fractions by `sumSBD`. Go `useSumSBD`.
    pub use_sum_sbd: bool,
    /// Weight rate sums by `sumSBDRaw`. Go `useSumSBDRaw`.
    pub use_sum_sbd_raw: bool,
    /// Emission process being generated for. Go `processID`.
    pub process_id: i32,
    /// Calendar year being generated for. Go `yearID`.
    pub year_id: i32,
    /// Road type filter (`0` = no filter). Go `roadTypeID`.
    pub road_type_id: i32,
}

impl ExternalFlags {
    /// Parse the worker's `-parameters=` CSV list.
    ///
    /// Ports `readExternalFlags`. The list must hold at least 8 entries: a
    /// run of identifier tokens followed by exactly three trailing integers
    /// — `processID`, `yearID`, `roadTypeID`. Identifier tokens are matched
    /// across `params[.. len-3]`; unrecognised tokens are ignored, exactly
    /// as the Go `switch` (which has no `default` case) does.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Parameters`] if fewer than 8 entries are supplied or
    /// if a trailing identifier does not parse as an integer.
    pub fn from_parameters(params: &[&str]) -> Result<Self> {
        if params.len() < 8 {
            return Err(Error::Parameters(format!(
                "expected at least 8 CSV parameters, got {}",
                params.len()
            )));
        }
        let mut flags = ExternalFlags::default();
        for token in &params[..params.len() - 3] {
            match *token {
                "yOp" => flags.keep_op_mode_id = true,
                "nOp" => flags.keep_op_mode_id = false,
                "yASB" => flags.use_avg_speed_bin = true,
                "nASB" => flags.use_avg_speed_bin = false,
                "yASF" => flags.use_avg_speed_fraction = true,
                "nASF" => flags.use_avg_speed_fraction = false,
                "ySBD" => flags.use_sum_sbd = true,
                "nSBD" => flags.use_sum_sbd = false,
                "yRaw" => flags.use_sum_sbd_raw = true,
                "nRaw" => flags.use_sum_sbd_raw = false,
                // Unknown tokens are ignored — the Go switch has no default.
                _ => {}
            }
        }
        let n = params.len();
        flags.process_id = parse_trailing(params[n - 3], "processID")?;
        flags.year_id = parse_trailing(params[n - 2], "yearID")?;
        flags.road_type_id = parse_trailing(params[n - 1], "roadTypeID")?;
        Ok(flags)
    }
}

/// Parse one of the trailing integer parameters, naming it in any error.
fn parse_trailing(raw: &str, name: &str) -> Result<i32> {
    raw.trim()
        .parse::<i32>()
        .map_err(|e| Error::Parameters(format!("{name} = {raw:?}: {e}")))
}

/// One record from the `SourceUseTypePhysicsMapping` table.
///
/// Ports `SourceUseTypePhysicsMappingDetail`. `real_source_type_id` is the
/// source type traditionally used; `temp_source_type_id` is a temporary
/// source type carved out for a model-year-range / regulatory-class
/// combination; `op_mode_id_offset` shifts operating modes so the new modes
/// apply only to the temporary source type.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct SourceUseTypePhysicsMappingDetail {
    /// Real (traditional) source type id.
    pub real_source_type_id: i32,
    /// Temporary source type id, unique per record.
    pub temp_source_type_id: i32,
    /// Operating-mode id offset for the temporary source type.
    pub op_mode_id_offset: i32,
    /// Regulatory class id.
    pub reg_class_id: i32,
    /// First model year the mapping applies to.
    pub begin_model_year_id: i32,
    /// Last model year the mapping applies to.
    pub end_model_year_id: i32,
    /// Rolling-resistance term `A` of the road-load polynomial.
    pub rolling_term_a: f64,
    /// Rotating-mass term `B` of the road-load polynomial.
    pub rotating_term_b: f64,
    /// Aerodynamic-drag term `C` of the road-load polynomial.
    pub drag_term_c: f64,
    /// Source mass used in the VSP calculation.
    pub source_mass: f64,
    /// Fixed mass factor used in the VSP calculation.
    pub fixed_mass_factor: f64,
}

/// Unique key for a `RatesOpModeDistribution` record.
///
/// Ports `romdKey`. `begin_model_year_id`, `end_model_year_id` and
/// `reg_class_id` are `0` for records produced by the core
/// `RatesOpModeDistribution` path and carry real values only on the
/// drive-cycle path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct RomdKey {
    /// Source type id.
    pub source_type_id: i32,
    /// Pollutant/process id (`pollutantID * 100 + processID`).
    pub pol_process_id: i32,
    /// Road type id.
    pub road_type_id: i32,
    /// Hour/day id.
    pub hour_day_id: i32,
    /// Operating-mode id.
    pub op_mode_id: i32,
    /// Average-speed bin id.
    pub avg_speed_bin_id: i32,
    /// First model year, or `0` on the core path.
    pub begin_model_year_id: i32,
    /// Last model year, or `0` on the core path.
    pub end_model_year_id: i32,
    /// Regulatory class id, or `0` on the core path.
    pub reg_class_id: i32,
}

/// One `RatesOpModeDistribution` record handed to the base-rate aggregators.
///
/// Ports `romdBlock`.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct RomdBlock {
    /// Identifying key.
    pub key: RomdKey,
    /// Operating-mode fraction.
    pub op_mode_fraction: f64,
    /// Average bin speed.
    pub avg_bin_speed: f64,
    /// Average-speed-bin fraction.
    pub avg_speed_fraction: f64,
}

/// Lookup key into the `SBWeightedEmissionRate[ByAge]` tables.
///
/// Ports `sbWeightedEmissionRateByAgeKey`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct SbWeightedRateKey {
    /// Source type id.
    pub source_type_id: i32,
    /// Pollutant/process id.
    pub pol_process_id: i32,
    /// Operating-mode id.
    pub op_mode_id: i32,
}

/// One record from the `SBWeightedEmissionRate[ByAge]` tables.
///
/// Ports `sbWeightedEmissionRateByAgeDetail`. Records read from
/// `SBWeightedEmissionRate` (no age dimension) carry `age_group_id == 0`.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct SbWeightedRateDetail {
    /// Source type id.
    pub source_type_id: i32,
    /// Pollutant/process id.
    pub pol_process_id: i32,
    /// Operating-mode id.
    pub op_mode_id: i32,
    /// Model year id.
    pub model_year_id: i32,
    /// Fuel type id.
    pub fuel_type_id: i32,
    /// Age group id (`0` for `SBWeightedEmissionRate`).
    pub age_group_id: i32,
    /// Regulatory class id.
    pub reg_class_id: i32,
    /// Source-bin distribution sum.
    pub sum_sbd: f64,
    /// Raw source-bin distribution sum.
    pub sum_sbd_raw: f64,
    /// Mean base rate.
    pub mean_base_rate: f64,
    /// Mean base rate, I/M adjusted.
    pub mean_base_rate_im: f64,
    /// Mean base rate, air-conditioning adjusted.
    pub mean_base_rate_ac_adj: f64,
    /// Mean base rate, I/M and air-conditioning adjusted.
    pub mean_base_rate_im_ac_adj: f64,
}

/// One operating-mode definition (`OperatingMode` table).
///
/// Ports `operatingMode`. The Go reads each VSP/speed bound with both
/// `ifnull(col,0)` and `isnull(col)`; the port collapses that pair into an
/// [`Option`] — `None` means "this bound does not constrain the mode".
/// Only modes `1 < opModeID < 100` excluding `26` and `36` are loaded.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct OperatingMode {
    /// Operating-mode id.
    pub op_mode_id: i32,
    /// Lower VSP bound, or `None` if unconstrained.
    pub vsp_lower: Option<f64>,
    /// Upper VSP bound, or `None` if unconstrained.
    pub vsp_upper: Option<f64>,
    /// Lower speed bound, or `None` if unconstrained.
    pub speed_lower: Option<f64>,
    /// Upper speed bound, or `None` if unconstrained.
    pub speed_upper: Option<f64>,
}

/// Unique key for an `AvgSpeedDistribution` record.
///
/// Ports `avgSpeedDistributionKey`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct AvgSpeedDistributionKey {
    /// Source type id.
    pub source_type_id: i32,
    /// Road type id.
    pub road_type_id: i32,
    /// Hour/day id.
    pub hour_day_id: i32,
    /// Average-speed bin id.
    pub avg_speed_bin_id: i32,
}

/// Detail for an `AvgSpeedDistribution` record.
///
/// Ports `avgSpeedDistributionDetail`. `avg_bin_speed` is joined in from the
/// `avgSpeedBin` table.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct AvgSpeedDistributionDetail {
    /// Average-speed-bin fraction.
    pub avg_speed_fraction: f64,
    /// Average bin speed (joined from `avgSpeedBin`).
    pub avg_bin_speed: f64,
}

/// Unique key for a `DriveScheduleAssoc` record.
///
/// Ports `driveScheduleAssocKey`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct DriveScheduleAssocKey {
    /// Source type id.
    pub source_type_id: i32,
    /// Road type id.
    pub road_type_id: i32,
}

/// Grouping key for accumulated `BaseRate[ByAge]` output rows.
///
/// Ports `baseRateOutputKey`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct BaseRateOutputKey {
    /// Source type id.
    pub source_type_id: i32,
    /// Pollutant/process id.
    pub pol_process_id: i32,
    /// Road type id.
    pub road_type_id: i32,
    /// Hour/day id.
    pub hour_day_id: i32,
    /// Average-speed bin id.
    pub avg_speed_bin_id: i32,
    /// Model year id.
    pub model_year_id: i32,
    /// Fuel type id.
    pub fuel_type_id: i32,
    /// Age group id.
    pub age_group_id: i32,
    /// Regulatory class id.
    pub reg_class_id: i32,
    /// Operating-mode id.
    pub op_mode_id: i32,
}

/// One row of the `BaseRate` or `BaseRateByAge` table.
///
/// Ports `baseRateOutputRecord`. A `BaseRate` row carries `age_group_id == 0`
/// (the column is dropped on serialisation); a `BaseRateByAge` row carries
/// the real age group.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct BaseRateOutputRecord {
    /// Source type id.
    pub source_type_id: i32,
    /// Road type id.
    pub road_type_id: i32,
    /// Average-speed bin id.
    pub avg_speed_bin_id: i32,
    /// Hour/day id.
    pub hour_day_id: i32,
    /// Pollutant/process id.
    pub pol_process_id: i32,
    /// Pollutant id (`polProcessID / 100`).
    pub pollutant_id: i32,
    /// Process id (`polProcessID % 100`).
    pub process_id: i32,
    /// Model year id.
    pub model_year_id: i32,
    /// Fuel type id.
    pub fuel_type_id: i32,
    /// Age group id.
    pub age_group_id: i32,
    /// Regulatory class id.
    pub reg_class_id: i32,
    /// Operating-mode id.
    pub op_mode_id: i32,
    /// Mean base rate.
    pub mean_base_rate: f64,
    /// Mean base rate, I/M adjusted.
    pub mean_base_rate_im: f64,
    /// Mean base rate, air-conditioning adjusted.
    pub mean_base_rate_ac_adj: f64,
    /// Mean base rate, I/M and air-conditioning adjusted.
    pub mean_base_rate_im_ac_adj: f64,
    /// Distance-normalised emission rate.
    pub emission_rate: f64,
    /// Distance-normalised emission rate, I/M adjusted.
    pub emission_rate_im: f64,
    /// Distance-normalised emission rate, air-conditioning adjusted.
    pub emission_rate_ac_adj: f64,
    /// Distance-normalised emission rate, I/M and AC adjusted.
    pub emission_rate_im_ac_adj: f64,
    /// Operating-mode fraction (key-weighted accumulation).
    pub op_mode_fraction: f64,
    /// Operating-mode fraction used for rate normalisation.
    pub op_mode_fraction_rate: f64,
}

/// One row of the `DrivingIdleFraction` table produced by the drive-cycle
/// path. Used downstream for off-network idling (ONI).
///
/// Ports the line written by `idleFractionFiles.writeLine` in
/// `processDriveCycles`.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct DrivingIdleFractionRow {
    /// Hour/day id.
    pub hour_day_id: i32,
    /// Calendar year id.
    pub year_id: i32,
    /// Road type id.
    pub road_type_id: i32,
    /// Source type id.
    pub source_type_id: i32,
    /// Fraction of driving time spent idling.
    pub driving_idle_fraction: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_parameters_reads_flags_and_trailing_ids() {
        let flags = ExternalFlags::from_parameters(&[
            "yOp", "yASB", "nASF", "ySBD", "yRaw", "1", "2020", "5",
        ])
        .expect("parse ok");
        assert!(flags.keep_op_mode_id);
        assert!(flags.use_avg_speed_bin);
        assert!(!flags.use_avg_speed_fraction);
        assert!(flags.use_sum_sbd);
        assert!(flags.use_sum_sbd_raw);
        assert_eq!(flags.process_id, 1);
        assert_eq!(flags.year_id, 2020);
        assert_eq!(flags.road_type_id, 5);
    }

    #[test]
    fn from_parameters_ignores_unknown_tokens() {
        // The Go switch has no default case — unknown identifiers are skipped.
        let flags = ExternalFlags::from_parameters(&[
            "nOp", "mystery", "nASB", "nASF", "nSBD", "9", "2018", "0",
        ])
        .expect("parse ok");
        assert!(!flags.keep_op_mode_id);
        assert_eq!(flags.process_id, 9);
        assert_eq!(flags.road_type_id, 0);
    }

    #[test]
    fn from_parameters_rejects_short_list() {
        let err = ExternalFlags::from_parameters(&["nOp", "1", "2020"]).unwrap_err();
        assert!(matches!(err, Error::Parameters(_)));
    }

    #[test]
    fn from_parameters_rejects_non_integer_trailing() {
        let err = ExternalFlags::from_parameters(&[
            "nOp", "nASB", "nASF", "nSBD", "nRaw", "x", "2020", "0",
        ])
        .unwrap_err();
        assert!(matches!(err, Error::Parameters(_)));
    }

    #[test]
    fn defaults_are_all_false_and_zero() {
        let flags = ExternalFlags::default();
        assert!(!flags.keep_op_mode_id);
        assert!(!flags.use_avg_speed_bin);
        assert_eq!(flags.process_id, 0);
    }
}
