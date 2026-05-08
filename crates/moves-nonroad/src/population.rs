//! Population apportionment, growth-factor application, and
//! age-distribution / model-year fraction computation.
//!
//! Cluster 3 of the seven functional groups (see
//! `ARCHITECTURE.md` § 2.3).
//!
//! # Fortran source files this module ports
//!
//! | File | Lines | Role |
//! |---|---|---|
//! | `getpop.f`  | 285 | Population apportionment |
//! | `getgrw.f`  | 200 | Growth retrieval |
//! | `grwfac.f`  | 281 | Growth-factor application |
//! | `agedist.f` | 193 | Age-distribution computation |
//! | `modyr.f`   | 216 | Model-year fraction computation |
//! | `getscrp.f` | 107 | Scrappage retrieval |
//!
//! Plus the retrofit-population helpers (`cmprrtrft.f`,
//! `srtrtrft.f`, `swaprtrft.f`, `rtrftengovrlp.f`,
//! `initrtrft.f`).
//!
//! # Numerical-fidelity note
//!
//! `agedist.f` and `modyr.f` (Task 104) use iterative or
//! accumulating computations that are sensitive to evaluation
//! order. They are flagged in the migration plan's risk register
//! as the most likely source of numerical divergence between the
//! Rust port and the Windows-compiled Fortran reference. See
//! Tasks 115 (validation) and 116 (triage).
//!
//! # Status
//!
//! Phase 5 skeleton — no code yet. Implementation lands in
//! Tasks 103 (population/growth core) and 104 (age distribution).
