//! Exhaust, evaporative, and retrofit-emission calculation.
//!
//! Cluster 4 of the seven functional groups (see
//! `ARCHITECTURE.md` § 2.4).
//!
//! # Fortran source files this module ports
//!
//! | File | Lines | Role |
//! |---|---|---|
//! | `clcems.f`    | 360 | Exhaust emissions (Task 106) |
//! | `emfclc.f`    | 314 | Exhaust EF lookup |
//! | `emsadj.f`    | 343 | Emissions adjustments |
//! | `unitcf.f`    |  80 | Unit conversion factors |
//! | `intadj.f`    | 141 | Integer-adjusted EF lookup |
//! | `clcevems.f`  | 721 | Evaporative emissions (Task 107) |
//! | `evemfclc.f`  | 370 | Evaporative EF lookup |
//! | `clcrtrft.f`  | 309 | Retrofit emissions (Task 108) |
//!
//! Plus the retrofit validators (`vldrtrftrecs.f`,
//! `vldrtrfthp.f`, `vldrtrftscc.f`, `vldrtrfttchtyp.f`).
//!
//! `clcevems.f` is the largest single file in NONROAD (721 lines);
//! `clcems.f` is the most numerically sensitive.
//!
//! # Status
//!
//! Task 107 (`clcevems.f` / `evemfclc.f`) is implemented in the
//! [`evaporative`] sub-module. Task 108 (`clcrtrft.f`) is implemented
//! in [`retrofit`]. Task 106 (exhaust) is still skeleton-only.

pub mod evaporative;
pub mod retrofit;

pub use evaporative::{
    calculate_evaporative_emissions, calculate_evaporative_factors, EthanolBlend,
    EvapEmissionsCalcContext, EvapEmissionsOutcome, EvapEmissionsWarning, EvapFactorsCalcContext,
    EvapFactorsForSpecies, EvapFactorsOutcome, FuelType, RefuelingContext,
};
pub use retrofit::{
    calculate_retrofit_reduction, RetrofitCalcContext, RetrofitCalcContextOwned,
    RetrofitCalcWarning, RetrofitReductionOutcome,
};
