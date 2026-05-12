//! Exhaust, evaporative, and retrofit-emission calculation.
//!
//! Cluster 4 of the seven functional groups (see
//! `ARCHITECTURE.md` § 2.4).
//!
//! # Fortran source files this module ports
//!
//! | File | Lines | Role | Task |
//! |---|---|---|---|
//! | `clcems.f`    | 360 | Exhaust emissions                | 106 |
//! | `emfclc.f`    | 314 | Exhaust EF lookup                | 106 |
//! | `emsadj.f`    | 343 | Emissions adjustments            | 106 |
//! | `unitcf.f`    |  80 | Unit conversion factors          | 106 |
//! | `intadj.f`    | 141 | Integer-adjusted EF lookup       | 106 |
//! | `clcevems.f`  | 721 | Evaporative emissions            | 107 |
//! | `evemfclc.f`  | 370 | Evaporative EF lookup            | 107 |
//! | `clcrtrft.f`  | 309 | Retrofit emissions               | 108 |
//!
//! Plus the retrofit validators (`vldrtrftrecs.f`,
//! `vldrtrfthp.f`, `vldrtrftscc.f`, `vldrtrfttchtyp.f`).
//!
//! `clcevems.f` is the largest single file in NONROAD (721 lines);
//! `clcems.f` is the most numerically sensitive.
//!
//! # Submodules
//!
//! - [`exhaust`]: Task 106 — exhaust emissions calculator (`clcems`,
//!   `emfclc`, `emsadj`, `unitcf`).
//! - [`evaporative`]: Task 107 — evaporative emissions calculator
//!   (`clcevems`, `evemfclc`).
//! - [`retrofit`]: Task 108 — retrofit-emission reductions
//!   (`clcrtrft` and validators).
//!
//! The submodules' public surface is re-exported here so callers
//! can `use crate::emissions::*` without picking a submodule.

pub mod evaporative;
pub mod exhaust;
pub mod retrofit;

pub use evaporative::{
    calculate_evaporative_emissions, calculate_evaporative_factors, EthanolBlend,
    EvapEmissionsCalcContext, EvapEmissionsOutcome, EvapEmissionsWarning, EvapFactorsCalcContext,
    EvapFactorsForSpecies, EvapFactorsOutcome, FuelType, RefuelingContext,
};
pub use exhaust::{
    apply_deterioration, calculate_emission_adjustments, calculate_exhaust_emissions,
    compute_emission_factor_for_tech, unit_conversion_factor, AdjustmentInputs, DayRange,
    DeteriorationCoefficients, EmissionFactorContext, EmissionUnitCode, ExhaustCalcInputs,
    ExhaustCalcOutputs, FuelKind, PollutantIndex, RfgBinFactors, Season as AdjustSeason,
    SulfurAlternate,
};
pub use retrofit::{
    calculate_retrofit_reduction, RetrofitCalcContext, RetrofitCalcContextOwned,
    RetrofitCalcWarning, RetrofitReductionOutcome,
};
