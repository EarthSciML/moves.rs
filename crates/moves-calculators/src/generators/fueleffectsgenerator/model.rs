//! Data model for the Fuel Effects Generator port.
//!
//! Ports the nested helper classes of `FuelEffectsGenerator.java` that the
//! general-fuel-ratio path needs: [`FuelFormulation`] (the
//! `fuelFormulation`-table row), [`GeneralFuelRatioExpression`] (a
//! `generalFuelRatioExpression`-table row) and [`GeneralFuelRatioRow`] (a
//! `generalFuelRatio`-table row the generator emits), plus the
//! [`IntegerPair`] set helper used to deduplicate work.

use std::collections::BTreeSet;

use super::expression::VariableSource;

/// One row of the `fuelFormulation` table.
///
/// Ports the Java `FuelEffectsGenerator.FuelFormulation` inner class. The
/// property columns are `float` in both the MariaDB schema and the Java
/// class, so they are `f32` here; the evaluator promotes them to `f64`
/// when a `fuelEffectRatioExpression` references them.
///
/// `alt_rvp` is the alternate Reid vapor pressure: the Java generator adds
/// the `altRVP` column to `fuelFormulation` in `setup()` and the E85
/// THC adjustment (see [`super::generalfuelratio`]) rewrites `RVP` to
/// `altRVP`, so the column must be addressable by expressions.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct FuelFormulation {
    /// `fuelFormulationID` — primary key.
    pub fuel_formulation_id: i32,
    /// `fuelSubtypeID` — links to `fuelSubtype`/`fuelType`.
    pub fuel_subtype_id: i32,
    /// `RVP` — Reid vapor pressure.
    pub rvp: f32,
    /// `sulfurLevel` — sulfur content, ppm.
    pub sulfur_level: f32,
    /// `ETOHVolume` — ethanol volume percent.
    pub etoh_volume: f32,
    /// `MTBEVolume` — methyl tert-butyl ether volume percent.
    pub mtbe_volume: f32,
    /// `ETBEVolume` — ethyl tert-butyl ether volume percent.
    pub etbe_volume: f32,
    /// `TAMEVolume` — tert-amyl methyl ether volume percent.
    pub tame_volume: f32,
    /// `aromaticContent` — aromatics volume percent.
    pub aromatic_content: f32,
    /// `olefinContent` — olefins volume percent.
    pub olefin_content: f32,
    /// `benzeneContent` — benzene volume percent.
    pub benzene_content: f32,
    /// `e200` — percent evaporated at 200 °F.
    pub e200: f32,
    /// `e300` — percent evaporated at 300 °F.
    pub e300: f32,
    /// `volToWtPercentOxy` — volume-to-weight oxygen conversion factor.
    pub vol_to_wt_percent_oxy: f32,
    /// `BioDieselEsterVolume` — biodiesel ester volume percent.
    pub bio_diesel_ester_volume: f32,
    /// `CetaneIndex` — diesel cetane index.
    pub cetane_index: f32,
    /// `PAHContent` — polycyclic aromatic hydrocarbon content.
    pub pah_content: f32,
    /// `T50` — 50%-distillation temperature.
    pub t50: f32,
    /// `T90` — 90%-distillation temperature.
    pub t90: f32,
    /// `altRVP` — alternate RVP used by the E85 high-ethanol adjustment.
    pub alt_rvp: f32,
}

impl VariableSource for FuelFormulation {
    /// Resolve a `fuelFormulation` column by name, case-insensitively.
    ///
    /// MariaDB identifiers are case-insensitive, and `fuelEffectRatio`
    /// expressions reference these columns in whatever casing the data
    /// author used (`MTBEVolume`, `mtbevolume`, …), so the match folds
    /// case. An unknown name returns `None`, which the evaluator surfaces
    /// as an [`UnknownVariable`](super::expression::ExpressionError).
    fn variable(&self, name: &str) -> Option<f64> {
        let value = match name.to_ascii_lowercase().as_str() {
            "fuelformulationid" => f64::from(self.fuel_formulation_id),
            "fuelsubtypeid" => f64::from(self.fuel_subtype_id),
            "rvp" => f64::from(self.rvp),
            "sulfurlevel" => f64::from(self.sulfur_level),
            "etohvolume" => f64::from(self.etoh_volume),
            "mtbevolume" => f64::from(self.mtbe_volume),
            "etbevolume" => f64::from(self.etbe_volume),
            "tamevolume" => f64::from(self.tame_volume),
            "aromaticcontent" => f64::from(self.aromatic_content),
            "olefincontent" => f64::from(self.olefin_content),
            "benzenecontent" => f64::from(self.benzene_content),
            "e200" => f64::from(self.e200),
            "e300" => f64::from(self.e300),
            "voltowtpercentoxy" => f64::from(self.vol_to_wt_percent_oxy),
            "biodieselestervolume" => f64::from(self.bio_diesel_ester_volume),
            "cetaneindex" => f64::from(self.cetane_index),
            "pahcontent" => f64::from(self.pah_content),
            "t50" => f64::from(self.t50),
            "t90" => f64::from(self.t90),
            "altrvp" => f64::from(self.alt_rvp),
            _ => return None,
        };
        Some(value)
    }
}

/// One row of the `generalFuelRatioExpression` table.
///
/// Ports the Java `FuelEffectsGenerator.GeneralFuelRatioExpression` inner
/// class. `pollutant_id` and `process_id` are derived from `pol_process_id`
/// — the Java result-set constructor computes `polProcessID / 100` and
/// `polProcessID % 100`. Both Rust and Java truncate integer division
/// toward zero, so negative `pol_process_id` values (the
/// `testDoGeneralFuelRatio` fixture uses `-101`) decompose identically.
#[derive(Debug, Clone, PartialEq)]
pub struct GeneralFuelRatioExpression {
    /// `fuelTypeID` the expression applies to.
    pub fuel_type_id: i32,
    /// `polProcessID` — combined pollutant/process key.
    pub pol_process_id: i32,
    /// `pollutantID`, derived as `pol_process_id / 100`.
    pub pollutant_id: i32,
    /// `processID`, derived as `pol_process_id % 100`.
    pub process_id: i32,
    /// First model year the expression covers.
    pub min_model_year_id: i32,
    /// Last model year the expression covers.
    pub max_model_year_id: i32,
    /// First age the expression covers.
    pub min_age_id: i32,
    /// Last age the expression covers.
    pub max_age_id: i32,
    /// `sourceTypeID` the expression applies to (`0` = all).
    pub source_type_id: i32,
    /// SQL arithmetic for the standard fuel-effect ratio.
    pub fuel_effect_ratio_expression: String,
    /// SQL arithmetic for the geographic-phase-in (GPA) ratio.
    pub fuel_effect_ratio_gpa_expression: String,
    /// When set, restricts the expression to these `fuelSubtypeID`s — used
    /// by the derived E85 "Pseudo-THC" expressions, which target only the
    /// E70/E85 subtypes `51` and `52`.
    pub fuel_subtypes: Option<Vec<i32>>,
}

impl GeneralFuelRatioExpression {
    /// Build an expression row, deriving `pollutant_id`/`process_id` from
    /// `pol_process_id` exactly as the Java result-set constructor does.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        fuel_type_id: i32,
        pol_process_id: i32,
        min_model_year_id: i32,
        max_model_year_id: i32,
        min_age_id: i32,
        max_age_id: i32,
        source_type_id: i32,
        fuel_effect_ratio_expression: impl Into<String>,
        fuel_effect_ratio_gpa_expression: impl Into<String>,
    ) -> Self {
        Self {
            fuel_type_id,
            pol_process_id,
            pollutant_id: pol_process_id / 100,
            process_id: pol_process_id % 100,
            min_model_year_id,
            max_model_year_id,
            min_age_id,
            max_age_id,
            source_type_id,
            fuel_effect_ratio_expression: fuel_effect_ratio_expression.into(),
            fuel_effect_ratio_gpa_expression: fuel_effect_ratio_gpa_expression.into(),
            fuel_subtypes: None,
        }
    }
}

/// One row the generator inserts into the `generalFuelRatio` table.
///
/// Ports the column list of the `insert into GeneralFuelRatio (...)`
/// statement in `doGeneralFuelRatio`. `fuel_effect_ratio` and
/// `fuel_effect_ratio_gpa` are `double` in the schema — the result of
/// evaluating the two expression columns against one fuel formulation.
#[derive(Debug, Clone, PartialEq)]
pub struct GeneralFuelRatioRow {
    /// `fuelTypeID`.
    pub fuel_type_id: i32,
    /// `fuelFormulationID` the ratio was computed for.
    pub fuel_formulation_id: i32,
    /// `polProcessID`.
    pub pol_process_id: i32,
    /// `pollutantID`.
    pub pollutant_id: i32,
    /// `processID`.
    pub process_id: i32,
    /// First model year the row covers.
    pub min_model_year_id: i32,
    /// Last model year the row covers.
    pub max_model_year_id: i32,
    /// First age the row covers.
    pub min_age_id: i32,
    /// Last age the row covers.
    pub max_age_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// Evaluated standard fuel-effect ratio.
    pub fuel_effect_ratio: f64,
    /// Evaluated geographic-phase-in fuel-effect ratio.
    pub fuel_effect_ratio_gpa: f64,
}

/// An ordered pair of optionally-null integers.
///
/// Ports `FuelEffectsGenerator.IntegerPair`. The Java `compareTo` sorts a
/// `null` component before any non-null one and otherwise compares by
/// value — which is exactly the ordering Rust derives for
/// `(Option<i32>, Option<i32>)` (`None < Some(_)`). The derived `Ord` here
/// therefore reproduces the Java comparator without a hand-written `cmp`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IntegerPair {
    /// First component.
    pub a: Option<i32>,
    /// Second component.
    pub b: Option<i32>,
}

impl IntegerPair {
    /// Construct a pair from its two (possibly null) components.
    #[must_use]
    pub fn new(a: Option<i32>, b: Option<i32>) -> Self {
        Self { a, b }
    }
}

/// Test whether a set of [`IntegerPair`]s contains the pair `(a, b)`.
///
/// Ports the `static` `FuelEffectsGenerator.contains` helper.
#[must_use]
pub fn contains(pairs: &BTreeSet<IntegerPair>, a: Option<i32>, b: Option<i32>) -> bool {
    pairs.contains(&IntegerPair::new(a, b))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fuel_formulation_resolves_columns_case_insensitively() {
        let fuel = FuelFormulation {
            mtbe_volume: 10.0,
            rvp: 8.7,
            ..FuelFormulation::default()
        };
        assert_eq!(fuel.variable("MTBEVolume"), Some(10.0));
        assert_eq!(fuel.variable("mtbevolume"), Some(10.0));
        assert_eq!(fuel.variable("RVP"), Some(f64::from(8.7_f32)));
        assert_eq!(fuel.variable("altRVP"), Some(0.0));
        assert_eq!(fuel.variable("notAColumn"), None);
    }

    #[test]
    fn general_fuel_ratio_expression_derives_pol_and_process() {
        let exp = GeneralFuelRatioExpression::new(1, 201, 1960, 2060, 0, 30, 0, "1", "1");
        assert_eq!(exp.pollutant_id, 2);
        assert_eq!(exp.process_id, 1);

        // Negative polProcessID (the testDoGeneralFuelRatio fixture).
        let exp = GeneralFuelRatioExpression::new(1, -101, 1960, 2060, 0, 30, 0, "", "");
        assert_eq!(exp.pollutant_id, -1);
        assert_eq!(exp.process_id, -1);
    }

    #[test]
    fn integer_pair_orders_null_before_value() {
        // Java's compareTo: a null component sorts first.
        assert!(IntegerPair::new(None, Some(1)) < IntegerPair::new(Some(0), Some(0)));
        assert!(IntegerPair::new(Some(1), None) < IntegerPair::new(Some(1), Some(0)));
        assert!(IntegerPair::new(Some(1), Some(2)) < IntegerPair::new(Some(1), Some(3)));
    }

    #[test]
    fn contains_finds_present_pair() {
        let mut pairs = BTreeSet::new();
        pairs.insert(IntegerPair::new(Some(101), Some(201)));
        assert!(contains(&pairs, Some(101), Some(201)));
        assert!(!contains(&pairs, Some(101), Some(202)));
    }
}
