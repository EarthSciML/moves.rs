//! Geography processing — county, state, subcounty, US-total,
//! state-from-national, and national.
//!
//! Cluster 2 of the seven functional groups (see
//! `ARCHITECTURE.md` § 2.2). The bulk of the spatial-allocation
//! logic, currently spread across six near-duplicate "process"
//! routines that handle different geography levels.
//!
//! # Fortran source files this module ports
//!
//! | File | Lines | Role |
//! |---|---|---|
//! | `prccty.f` |   790 | County-level processing |
//! | `prcsta.f` | 1,034 | State-level processing |
//! | `prcsub.f` |   829 | Subcounty-level processing |
//! | `prcus.f`  |   775 | US-total processing |
//! | `prc1st.f` |   785 | State-from-national derivation |
//! | `prcnat.f` |   943 | National-level processing |
//!
//! # Status
//!
//! Phase 5 skeleton — no code yet. Tasks 109–111 port the routines
//! as separate functions for fidelity; Task 112 then refactors them
//! into a single parameterized routine, removing ~3,000 lines of
//! duplication. The refactor is gated on characterization-fixture
//! parity (Phase 0).
