//! Population apportionment, growth-factor application, and
//! age-distribution / model-year fraction computation.
//!
//! Cluster 3 of the seven functional groups (see
//! `ARCHITECTURE.md` § 2.3).
//!
//! # Fortran source files this module ports
//!
//! | File | Lines | Role | Task |
//! |---|---|---|---|
//! | `getpop.f`       | 285 | Population apportionment            | 103 |
//! | `getgrw.f`       | 200 | Growth retrieval                    | 103 |
//! | `grwfac.f`       | 281 | Growth-factor application           | 103 |
//! | `getscrp.f`      | 107 | Scrappage retrieval                 | 103 |
//! | `cmprrtrft.f`    | 153 | Retrofit-record comparison          | 103 |
//! | `srtrtrft.f`     | 116 | Retrofit-record quicksort           | 103 |
//! | `swaprtrft.f`    | 133 | Retrofit-record swap                | 103 |
//! | `rtrftengovrlp.f`| 110 | Retrofit engine-set overlap test    | 103 |
//! | `initrtrft.f`    |  82 | Retrofit-array initialization       | 103 |
//! | `agedist.f`      | 193 | Age-distribution computation        | 104 |
//! | `modyr.f`        | 216 | Model-year fraction computation     | 104 |
//!
//! # Scratch-file replacement
//!
//! The Fortran source uses sorted scratch files
//! (`popdir.txt`/`spopfl`, `grwdir.txt`/`grwfl`) to pass data between
//! the input-parsing stage and the lookup stage. Per the
//! architecture's array-size and I/O policies (`ARCHITECTURE.md`
//! §§ 4.1, 4.3), the Rust port replaces those scratch files with
//! in-memory [`Vec`] state owned by `NonroadContext`. The functions
//! in this module operate on those `Vec`s directly — they assume
//! the caller has loaded and sorted the records appropriately.
//!
//! # Numerical-fidelity note
//!
//! `agedist.f` and `modyr.f` (Task 104) use iterative or
//! accumulating computations that are sensitive to evaluation
//! order. They are flagged in the migration plan's risk register
//! as the most likely source of numerical divergence between the
//! Rust port and the Windows-compiled Fortran reference. See
//! Tasks 115 (validation) and 116 (triage).

pub mod agedist;
pub mod growth;
pub mod modyr;
pub mod pop;
pub mod retrofit;
pub mod scrappage;

pub use agedist::{age_distribution, AgeDistributionResult};
pub use growth::{
    growth_factor, select_for_indicator, GrowthFactor, GrowthFactorWarning, GrowthIndicatorRecord,
};
pub use modyr::{model_year, ActivityUnits, AgeAdjustmentTable, ModelYearOutput, ScrappageTime};
pub use pop::{select_for_scc, SelectedPopulation};
pub use retrofit::{
    compare_retrofits, engine_overlap, init_retrofit_state, sort_retrofits, swap_retrofits,
    Comparison, RetrofitPollutant, RetrofitRecord, RetrofitState, RTRFTSCC_ALL, RTRFTTECHTYPE_ALL,
};
pub use scrappage::{select_scrappage, AlternateCurves, ScrappageCurve};
