//! Emission factor lookup and calculation.
//!
//! Exhaust (`clcems.f`, `emfclc.f`, `emsadj.f`, `unitcf.f`),
//! evaporative (`clcevems.f`, `evemfclc.f`),
//! and retrofit (`clcrtrft.f`) emissions.

pub mod exhaust;
pub mod evaporative;
pub mod retrofit;
