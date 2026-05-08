//! County, state-to-county, and subcounty allocation logic.
//!
//! Cluster 5 of the seven functional groups (see
//! `ARCHITECTURE.md` § 2.5). Smallest calculation cluster; the
//! three routines are similar in structure and may share helpers
//! in the Rust port.
//!
//! # Fortran source files this module ports
//!
//! | File | Lines | Role |
//! |---|---|---|
//! | `alocty.f` | 181 | County allocation |
//! | `alosta.f` | 176 | State-to-county allocation |
//! | `alosub.f` | 170 | Subcounty allocation |
//!
//! # Status
//!
//! Phase 5 skeleton — no code yet. Implementation lands in
//! Task 105.
