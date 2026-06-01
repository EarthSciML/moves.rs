//! `moves-control-strategy-validation` — validation gate.
//!
//! Exercises each control strategy with non-trivial fixture parameters and
//! validates that the Rust port's computation matches the canonical Java
//! formula. Cross-strategy tests confirm that the Java order-of-application
//! is preserved when multiple strategies are active simultaneously.
//!
//! # What is validated
//!
//! Each strategy is tested against a multi-row fixture whose expected output
//! is derived from the canonical Java formula (no Java runtime required):
//!
//! | Strategy | Formula | Reference |
//! |----------|---------|-----------|
//! | `RateOfProgressControlStrategy` | `scale = 1.0 − reductionFraction` | `RateOfProgressStrategy.java` |
//! | `OnRoadRetrofitStrategy` | `factor = ∏(1 − fraction × effectiveness)` | `OnRoadRetrofit.java` |
//! | `AvftControlStrategy` | gap-fill + project via `AVFTTool` | `AVFTTool.java` |
//! | `NonRoadRetrofitStrategy` | pass-through to `calculate_retrofit_reduction` | `clcrtrft.f` |
//!
//! # Cross-strategy order-of-application
//!
//! The Java engine applies strategies in registration order (all at priority
//! `INTERNAL_CONTROL_STRATEGY` = 1000). The canonical order is:
//!
//! 1. `AvftControlStrategy` — replaces the `AVFT` fleet-composition table
//! 2. `RateOfProgressControlStrategy` — scales per-(pollutant, sourceType, regClass, modelYear) rates
//! 3. `OnRoadRetrofitStrategy` — writes a multiplicative `emissionRateAdjustment` table
//! 4. `NonRoadRetrofitStrategy` — supplies per-SCC reduction records to the nonroad calculator
//!
//! Because each strategy touches independent tables (except that ROP and
//! OnRoadRetrofit both feed into the final emission rate), the combined
//! downstream emission factor for an on-road vehicle row is:
//!
//! ```text
//! emission_rate_final = base_rate × ROP_scale × OnRoadRetrofit_factor
//! ```
//!
//! See `tests/cross_strategy.rs` for the explicit test cases.
//!
//! # Fixtures
//!
//! CSV fixture files live in `tests/fixtures/` and are embedded via
//! `include_str!` so the tests are self-contained. The fixture values use
//! real MOVES pollutant IDs (VOC=1, CO=2, NOx=3, CO2eq=98) and source-type
//! IDs (passenger cars=11, light commercial trucks=21, heavy-duty diesel=52).
