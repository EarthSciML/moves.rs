//! Data structures for the Base Rate Calculator port.
//!
//! Ports the struct declarations from `calc/baseratecalculator/baseratecalculator.go`
//! together with the subset of the shared Go `calc/mwo` package the calculator
//! touches. Field-for-field analogues; Go `int` becomes [`i32`] (every MOVES
//! identifier fits comfortably) and Go `float64` becomes [`f64`].
//!
//! # The `mwo` subset
//!
//! The Go worker shares a `mwo` (MOVES Worker Output) package across every
//! calculator: `MWOKey`, `MWOBaseRate`, `MWOEmission`, `FuelBlock`,
//! `MWOOpMode`, `FuelSupplyDetail`. Porting all of `mwo` is a separate
//! concern; this module ports only the fields the Base Rate Calculator
//! reads or writes — [`BlockKey`], [`BaseRate`], [`Emission`], [`FuelBlock`],
//! [`OpModeRates`], [`FuelSupplyDetail`].
//!
//! # Lookup keys
//!
//! Every lookup-table key type is all-integer and derives [`Ord`] so the port
//! can use deterministic [`BTreeMap`](std::collections::BTreeMap) collections
//! where the Go used hash maps. The Go `calculateAndAccumulate` runs across
//! several goroutines, so its accumulation order — and therefore its
//! floating-point sum order — is already non-deterministic; the sequential,
//! ordered-map port simply picks one stable order within that tolerance.

/// Run-level constants — the Go `mwo.Constants` (`MWOConstants`) global.
///
/// These identify the bundle the worker was handed: one state / county /
/// zone / link / year / month. Every base-rate row read in a run shares
/// them, so they are carried once here rather than per row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct RunConstants {
    /// State id.
    pub state_id: i32,
    /// County id.
    pub county_id: i32,
    /// Zone id.
    pub zone_id: i32,
    /// Link id.
    pub link_id: i32,
    /// Calendar year id.
    pub year_id: i32,
    /// Month id.
    pub month_id: i32,
}

/// The `BRC_*` worker flags plus the project-domain bit.
///
/// The Java `BaseRateCalculator.doExecute` derives a set of
/// `enabledSectionNames` from the runspec and passes them to the Go worker as
/// `BRC_<section>` external modules; the Go reads them through
/// `mwo.NeedsModule`. Only the flags that gate *computation* (not SQL-section
/// selection) are modelled here.
///
/// `evefficiency` is always enabled by the Java (`"always run evefficiency
/// section"`); the flag is still carried for fidelity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ModuleFlags {
    /// `configuration.Singleton.IsProject` — the run is a Project-domain run.
    pub is_project: bool,
    /// `BRC_EmissionRateAdjustment` — apply the `EmissionRateAdjustment`
    /// model-year factor.
    pub emission_rate_adjustment: bool,
    /// `BRC_evefficiency` — apply the EV battery/charging efficiency divisor.
    pub ev_efficiency: bool,
    /// `BRC_AggregateSMFR` — weight rates by the source/model-year/fuel/
    /// reg-class activity distribution.
    pub aggregate_smfr: bool,
    /// `BRC_AdjustExtendedIdleEmissionRate` — restrict the SMFR weighting
    /// activity to extended-idle hours.
    pub adjust_extended_idle_emission_rate: bool,
    /// `BRC_AdjustAPUEmissionRate` — restrict the SMFR weighting activity to
    /// APU / shorepower hours.
    pub adjust_apu_emission_rate: bool,
    /// `BRC_DiscardSourceTypeID` — collapse `sourceTypeID` to `0` when
    /// aggregating the SMFR activity distribution.
    pub discard_source_type_id: bool,
    /// `BRC_DiscardModelYearID` — collapse `modelYearID` to `0`.
    pub discard_model_year_id: bool,
    /// `BRC_DiscardFuelTypeID` — collapse `fuelTypeID` to `0`.
    pub discard_fuel_type_id: bool,
    /// `BRC_DiscardRegClassID` — collapse `regClassID` to `0`.
    pub discard_reg_class_id: bool,
    /// `BRC_AdjustEmissionRateOnly` — SMFR weighting touches only the
    /// emission rate (Starts, Extended Idle, Aux Power).
    pub adjust_emission_rate_only: bool,
    /// `BRC_AdjustMeanBaseRateAndEmissionRate` — SMFR weighting touches both
    /// the mean base rate and the emission rate.
    pub adjust_mean_base_rate_and_emission_rate: bool,
    /// `BRC_ApplyActivity` — convert rates to an inventory by multiplying by
    /// universal activity.
    pub apply_activity: bool,
}

/// One fuel-supply record — the Go `mwo.FuelSupplyDetail`.
///
/// The Go expands every base-rate file row into one [`BaseRate`] per fuel
/// formulation supplied to the row's `(county, year, month, fuelType)`; this
/// is the per-formulation detail that drives the expansion.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct FuelSupplyDetail {
    /// Fuel subtype id.
    pub fuel_sub_type_id: i32,
    /// Fuel formulation id.
    pub fuel_formulation_id: i32,
    /// Market share of this formulation within the fuel type.
    pub market_share: f64,
}

/// One per-fuel-formulation base rate — the Go `mwo.MWOBaseRate`.
///
/// `streamBaseRate[ByAge]` copies the eight rate fields straight from the
/// file row and the identifying fields from the matching [`FuelSupplyDetail`];
/// `calculate_and_accumulate` then mutates the eight rate fields in place.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct BaseRate {
    /// Fuel subtype id.
    pub fuel_sub_type_id: i32,
    /// Fuel formulation id.
    pub fuel_formulation_id: i32,
    /// Market share of this formulation.
    pub market_share: f64,
    /// Mean base rate.
    pub mean_base_rate: f64,
    /// Mean base rate, I/M adjusted.
    pub mean_base_rate_im: f64,
    /// Distance-normalised emission rate.
    pub emission_rate: f64,
    /// Distance-normalised emission rate, I/M adjusted.
    pub emission_rate_im: f64,
    /// Mean base rate, air-conditioning adjusted.
    pub mean_base_rate_ac_adj: f64,
    /// Mean base rate, I/M and air-conditioning adjusted.
    pub mean_base_rate_im_ac_adj: f64,
    /// Emission rate, air-conditioning adjusted.
    pub emission_rate_ac_adj: f64,
    /// Emission rate, I/M and air-conditioning adjusted.
    pub emission_rate_im_ac_adj: f64,
}

/// One aggregated emission record — the Go `mwo.MWOEmission`.
///
/// Produced by `aggregate_op_modes`, which sums the per-operating-mode
/// [`BaseRate`] list into one record per fuel formulation.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Emission {
    /// Fuel subtype id.
    pub fuel_sub_type_id: i32,
    /// Fuel formulation id.
    pub fuel_formulation_id: i32,
    /// Emission quantity (mean-base-rate weighted by market share).
    pub emission_quant: f64,
    /// Emission rate (emission-rate weighted by market share).
    pub emission_rate: f64,
}

/// Identifying key for a [`FuelBlock`] — the subset of the Go `mwo.MWOKey`
/// the Base Rate Calculator sets, reads, or accumulates on.
///
/// The Go `MWOKey` also carries `SCC`, `EngTechID`, `SectorID`, `HPID` (never
/// set by this calculator) and a derived `AgeGroupID`. `AgeGroupID` is a pure
/// function of `AgeID` — also in the key — so it cannot change accumulation
/// grouping and the calculator never reads it; the port omits it.
///
/// `process_id` (and the derived `pol_process_id`) are *mutable*: the
/// shorepower step rewrites `process_id` `91 → 93`. The block is accumulated
/// only after every adjustment, so the key reflects the final value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct BlockKey {
    /// Calendar year id.
    pub year_id: i32,
    /// Month id.
    pub month_id: i32,
    /// Day id (`hourDayID % 10`).
    pub day_id: i32,
    /// Hour id (`hourDayID / 10`).
    pub hour_id: i32,
    /// State id.
    pub state_id: i32,
    /// County id.
    pub county_id: i32,
    /// Zone id.
    pub zone_id: i32,
    /// Link id.
    pub link_id: i32,
    /// Road type id.
    pub road_type_id: i32,
    /// Source type id.
    pub source_type_id: i32,
    /// Regulatory class id.
    pub reg_class_id: i32,
    /// Fuel type id.
    pub fuel_type_id: i32,
    /// Model year id.
    pub model_year_id: i32,
    /// Average-speed bin id.
    pub avg_speed_bin_id: i32,
    /// Pollutant id.
    pub pollutant_id: i32,
    /// Process id (mutable — shorepower rewrites `91 → 93`).
    pub process_id: i32,
    /// Pollutant/process id (`pollutantID * 100 + processID`).
    pub pol_process_id: i32,
    /// Hour/day id (`hourID * 10 + dayID`).
    pub hour_day_id: i32,
    /// Age id (`yearID - modelYearID`).
    pub age_id: i32,
}

impl BlockKey {
    /// Recompute the derived ids — the Go `MWOKey.CalcIDs`.
    ///
    /// `pol_process_id` and `hour_day_id` are recomposed from their parts and
    /// `age_id` from `year_id - model_year_id`. Call after mutating
    /// `process_id` (the shorepower step) so the derived `pol_process_id`
    /// stays consistent.
    pub fn calc_ids(&mut self) {
        self.pol_process_id = self.pollutant_id * 100 + self.process_id;
        self.hour_day_id = self.hour_id * 10 + self.day_id;
        self.age_id = self.year_id - self.model_year_id;
    }
}

/// Operating-mode rates for a [`FuelBlock`] — the Go `mwo.MWOOpMode`.
///
/// `general_fraction` / `general_fraction_rate` are the operating-mode
/// fraction (`opModeFraction` / `opModeFractionRate`) read from the file row;
/// they weight the start-temperature adjustment.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct OpModeRates {
    /// Operating-mode id.
    pub op_mode_id: i32,
    /// Operating-mode fraction used for inventory weighting.
    pub general_fraction: f64,
    /// Operating-mode fraction used for rate weighting.
    pub general_fraction_rate: f64,
    /// Per-fuel-formulation base rates.
    pub base_rates: Vec<BaseRate>,
}

/// One unit of work — the Go `mwo.FuelBlock`.
///
/// Before `aggregate_op_modes` a block carries [`op_mode`](Self::op_mode)
/// (the per-operating-mode [`BaseRate`] list) and an empty
/// [`emissions`](Self::emissions); after, `op_mode` is cleared and
/// `emissions` holds one [`Emission`] per fuel formulation.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct FuelBlock {
    /// Identifying key.
    pub key: BlockKey,
    /// Operating-mode rates — `None` once `aggregate_op_modes` has run.
    pub op_mode: Option<OpModeRates>,
    /// Aggregated emissions — populated by `aggregate_op_modes`.
    pub emissions: Vec<Emission>,
}

/// Key for the extended-idle / APU / shorepower hourly-fraction tables.
///
/// The Go declares three identically-shaped key structs/// `ExtendedIdleFractionKey`, `APUFractionKey`, `ShorepowerFractionKey`,
/// each `{modelYearID, fuelTypeID}`. The port collapses them into one type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct ModelYearFuelKey {
    /// Model year id.
    pub model_year_id: i32,
    /// Fuel type id.
    pub fuel_type_id: i32,
}

/// Key for the `ZoneMonthHour` meteorology table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct ZoneMonthHourKey {
    /// Month id.
    pub month_id: i32,
    /// Zone id.
    pub zone_id: i32,
    /// Hour id.
    pub hour_id: i32,
}

/// Meteorology detail for a `ZoneMonthHour` cell.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct ZoneMonthHourDetail {
    /// Temperature (°F).
    pub temperature: f64,
    /// Relative humidity (%).
    pub rel_humidity: f64,
    /// Heat index (°F).
    pub heat_index: f64,
    /// Specific humidity (g H₂O per kg dry air).
    pub specific_humidity: f64,
    /// Water mole fraction.
    pub mol_water_fraction: f64,
}

/// Key into `PollutantProcessMappedModelYear`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct PollutantProcessMappedModelYearKey {
    /// Pollutant/process id.
    pub pol_process_id: i32,
    /// Model year id.
    pub model_year_id: i32,
}

/// Model-year-group mapping for a pollutant/process and model year.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PollutantProcessMappedModelYearDetail {
    /// Model year group id.
    pub model_year_group_id: i32,
    /// Fuel model year group id.
    pub fuel_my_group_id: i32,
    /// I/M model year group id.
    pub im_model_year_group_id: i32,
}

/// Key into `StartTempAdjustment`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct StartTempAdjustmentKey {
    /// Fuel type id.
    pub fuel_type_id: i32,
    /// Pollutant/process id.
    pub pol_process_id: i32,
    /// Model year group id.
    pub model_year_group_id: i32,
    /// Operating-mode id.
    pub op_mode_id: i32,
}

/// Start-temperature adjustment coefficients and equation form.
///
/// The Go reads `startTempEquationType` and sets `isLog` for `"LOG"` and
/// `isPoly` for `"POLY"`; the polynomial form is also the fallback when
/// neither flag is set, so `is_poly` and "neither" share one branch.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct StartTempAdjustmentDetail {
    /// Term `A`.
    pub term_a: f64,
    /// Term `B`.
    pub term_b: f64,
    /// Term `C`.
    pub term_c: f64,
    /// Logarithmic equation form (`startTempEquationType == "LOG"`).
    pub is_log: bool,
    /// Polynomial equation form (`startTempEquationType == "POLY"`).
    pub is_poly: bool,
}

/// County detail — the Go `CountyDetail`.
///
/// The Go struct comments list seven county columns but keeps only two:
/// `GPAFract` (the geographic-phase-in fraction blending normal and GPA fuel
/// effects) and `barometricPressure`.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct CountyDetail {
    /// Geographic-phase-in area fraction.
    pub gpa_fract: f64,
    /// Barometric pressure.
    pub barometric_pressure: f64,
}

/// Key into `GeneralFuelRatio`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct GeneralFuelRatioKey {
    /// Fuel formulation id.
    pub fuel_formulation_id: i32,
    /// Pollutant/process id.
    pub pol_process_id: i32,
    /// Source type id.
    pub source_type_id: i32,
}

/// One model-year / age range of a general fuel ratio.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct GeneralFuelRatioInnerDetail {
    /// First model year the ratio applies to.
    pub min_model_year_id: i32,
    /// Last model year the ratio applies to.
    pub max_model_year_id: i32,
    /// First age the ratio applies to.
    pub min_age_id: i32,
    /// Last age the ratio applies to.
    pub max_age_id: i32,
    /// Fuel effect ratio (normal area).
    pub fuel_effect_ratio: f64,
    /// Fuel effect ratio (geographic-phase-in area).
    pub fuel_effect_ratio_gpa: f64,
}

/// All general-fuel-ratio ranges for one key — the Go `GeneralFuelRatioDetail`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct GeneralFuelRatioDetail {
    /// Model-year / age ranges; the first whose bounds the row falls inside
    /// supplies the ratio.
    pub details: Vec<GeneralFuelRatioInnerDetail>,
}

/// Key into `CriteriaRatio` and `AltCriteriaRatio`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct CriteriaRatioKey {
    /// Fuel formulation id.
    pub fuel_formulation_id: i32,
    /// Pollutant/process id.
    pub pol_process_id: i32,
    /// Source type id.
    pub source_type_id: i32,
    /// Model year id.
    pub model_year_id: i32,
    /// Age id.
    pub age_id: i32,
}

/// Criteria-pollutant fuel-effect ratio detail.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct CriteriaRatioDetail {
    /// Ratio (normal area).
    pub ratio: f64,
    /// Ratio (geographic-phase-in area).
    pub ratio_gpa: f64,
    /// Ratio with no sulfur effect (read but unused by this calculator).
    pub ratio_no_sulfur: f64,
}

/// Key into `TemperatureAdjustment`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct TemperatureAdjustmentKey {
    /// Pollutant/process id.
    pub pol_process_id: i32,
    /// Fuel type id.
    pub fuel_type_id: i32,
    /// Regulatory class id (`0` is the wildcard).
    pub reg_class_id: i32,
    /// Model year id.
    pub model_year_id: i32,
}

/// Temperature-adjustment coefficients — the Go `TemperatureAdjustmentDetail`.
///
/// A zero-valued detail is the Go `defaultTemperatureAdjustment`, used for
/// every key not explicitly present in the table.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct TemperatureAdjustmentDetail {
    /// Term `A`.
    pub term_a: f64,
    /// Term `B`.
    pub term_b: f64,
    /// Term `C`.
    pub term_c: f64,
}

/// NOx humidity-adjustment detail, keyed by fuel type — the Go
/// `NOxHumidityAdjustDetail`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct NoxHumidityAdjustDetail {
    /// Equation name — `"CFR 86"`, `"CFR 1065"`, or other (→ no adjustment).
    pub humidity_nox_eq: String,
    /// Term `A`.
    pub humidity_term_a: f64,
    /// Term `B`.
    pub humidity_term_b: f64,
    /// Lower bound on the humidity input.
    pub humidity_low_bound: f64,
    /// Upper bound on the humidity input.
    pub humidity_up_bound: f64,
    /// Humidity units label (read but unused by the equations).
    pub humidity_units: String,
}

/// Key into `ZoneACFactor`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct ZoneAcFactorKey {
    /// Hour id.
    pub hour_id: i32,
    /// Source type id.
    pub source_type_id: i32,
    /// Model year id.
    pub model_year_id: i32,
}

/// Key into `IMFactor`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct ImFactorKey {
    /// Pollutant/process id.
    pub pol_process_id: i32,
    /// Inspection frequency.
    pub inspect_freq: i32,
    /// Test standards id.
    pub test_standards_id: i32,
    /// Source type id.
    pub source_type_id: i32,
    /// Fuel type id.
    pub fuel_type_id: i32,
    /// I/M model year group id.
    pub im_model_year_group_id: i32,
    /// Age group id.
    pub age_group_id: i32,
}

/// Key into `IMCoverage`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct ImCoverageKey {
    /// Pollutant/process id.
    pub pol_process_id: i32,
    /// Model year id.
    pub model_year_id: i32,
    /// Source type id.
    pub source_type_id: i32,
    /// Fuel type id.
    pub fuel_type_id: i32,
}

/// Key shared by `EmissionRateAdjustment` and `EVEfficiency`.
///
/// The Go declares two identically-shaped key structs/// `EmissionRateAdjustmentKey` and `EVEfficiencyKey`, each
/// `{polProcessID, sourceTypeID, regClassID, fuelTypeID, modelYearID}`. The
/// port collapses them into one type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct PolProcSourceRegFuelMyKey {
    /// Pollutant/process id.
    pub pol_process_id: i32,
    /// Source type id.
    pub source_type_id: i32,
    /// Regulatory class id.
    pub reg_class_id: i32,
    /// Fuel type id.
    pub fuel_type_id: i32,
    /// Model year id.
    pub model_year_id: i32,
}

/// EV battery / charging efficiency detail.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct EvEfficiencyDetail {
    /// Battery efficiency.
    pub battery_efficiency: f64,
    /// Charging efficiency.
    pub charging_efficiency: f64,
}

/// Key into `universalActivity`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct UniversalActivityKey {
    /// Hour/day id.
    pub hour_day_id: i32,
    /// Model year id.
    pub model_year_id: i32,
    /// Source type id.
    pub source_type_id: i32,
}

/// Key into the `activityWeight` accumulator built by `calculate_activity_weight`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct ActivityWeightKey {
    /// Hour/day id.
    pub hour_day_id: i32,
    /// Model year id.
    pub model_year_id: i32,
    /// Source type id.
    pub source_type_id: i32,
    /// Fuel type id.
    pub fuel_type_id: i32,
    /// Regulatory class id.
    pub reg_class_id: i32,
}

/// Activity-weight accumulator detail — the Go `activityWeightDetail`.
///
/// `smfr_fraction` weights the mean base rate; `smfr_rates_fraction` weights
/// the emission rate. They start equal and diverge under the extended-idle /
/// APU rate adjustments.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct ActivityWeightDetail {
    /// Activity fraction applied to the mean base rate.
    pub smfr_fraction: f64,
    /// Activity fraction applied to the emission rate.
    pub smfr_rates_fraction: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calc_ids_recomposes_derived_fields() {
        let mut key = BlockKey {
            pollutant_id: 3,
            process_id: 2,
            hour_id: 8,
            day_id: 5,
            year_id: 2020,
            model_year_id: 2015,
            ..BlockKey::default()
        };
        key.calc_ids();
        assert_eq!(key.pol_process_id, 302);
        assert_eq!(key.hour_day_id, 85);
        assert_eq!(key.age_id, 5);
    }

    #[test]
    fn calc_ids_tracks_a_mutated_process_id() {
        // The shorepower step rewrites process 91 -> 93; calc_ids must then
        // recompute pol_process_id from the new process.
        let mut key = BlockKey {
            pollutant_id: 91,
            process_id: 91,
            ..BlockKey::default()
        };
        key.calc_ids();
        assert_eq!(key.pol_process_id, 9191);
        key.process_id = 93;
        key.calc_ids();
        assert_eq!(key.pol_process_id, 9193);
    }

    #[test]
    fn age_id_can_be_negative_for_future_model_years() {
        // The Go `AgeID = YearID - ModelYearID` is an unclamped subtraction.
        let mut key = BlockKey {
            year_id: 2020,
            model_year_id: 2022,
            ..BlockKey::default()
        };
        key.calc_ids();
        assert_eq!(key.age_id, -2);
    }
}
