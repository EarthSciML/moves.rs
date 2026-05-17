//! The general-fuel-ratio compute core — a port of
//! `FuelEffectsGenerator.doGeneralFuelRatio`.
//!
//! This is the headline path called out in the migration plan: it
//! "apportions emission rates across the fuel-formulation distribution,
//! applying fuel adjustments via `generalFuelRatioExpression` table
//! entries". Each `generalFuelRatioExpression` row carries two SQL
//! arithmetic formulas; the generator evaluates them against every fuel
//! formulation of the matching fuel type and writes the results as
//! `generalFuelRatio` rows.
//!
//! # What the Java did, and what this port keeps
//!
//! The Java method ran entirely in MariaDB: it pasted each expression into
//! an `insert into GeneralFuelRatio … select (EXPRESSION) from
//! fuelFormulation` statement. The port keeps the **computation** — the
//! E85 "Pseudo-THC" expression derivation, the fuel-type / fuel-supply
//! intersection, the per-`(formulation, polProcessID)` deduplication, and
//! the expression evaluation (see [`super::expression`]) — and replaces the
//! SQL boundary with plain values: a [`GeneralFuelRatioInputs`] in, a
//! `Vec<GeneralFuelRatioRow>` out.
//!
//! Two Java steps drop out of this port:
//!
//! * `changeFuelFormulationNulls` (`ifnull(col, 0)` over every property
//!   column) is a no-op — a [`FuelFormulation`]'s `f32` fields are already
//!   concrete, never null.
//! * The `executeLoop` post-step that deletes `generalFuelRatio` rows whose
//!   ratio is exactly `1` belongs to the master-loop driver, not
//!   `doGeneralFuelRatio`, and `FuelEffectsGeneratorTest.testDoGeneralFuelRatio`
//!   calls `doGeneralFuelRatio` directly — so it is intentionally excluded.
//!
//! # Data-plane status
//!
//! [`do_general_fuel_ratio`] is the numerical entry point and is fully
//! exercised by the crate's tests. Wiring it into the master loop — reading
//! `generalFuelRatioExpression` / `fuelFormulation` / `fuelSupply` from the
//! execution context and writing `generalFuelRatio` back — waits on Task 50
//! (`DataFrameStore`); see [`super`] for the generator-trait shell.

use std::collections::{BTreeMap, BTreeSet};

use super::expression::{Expression, ExpressionError};
use super::model::{
    contains, FuelFormulation, GeneralFuelRatioExpression, GeneralFuelRatioRow, IntegerPair,
};

/// `pollutantID` of the derived E85 "Pseudo-THC" expressions.
const PSEUDO_THC_POLLUTANT_ID: i32 = 10001;
/// `fuelTypeID` for ethanol (E85) fuel.
const ETHANOL_FUEL_TYPE_ID: i32 = 5;
/// `pollutantID` for total hydrocarbons (THC).
const THC_POLLUTANT_ID: i32 = 1;
/// First model year the E85 Pseudo-THC adjustment applies to.
const E85_MIN_MODEL_YEAR: i32 = 2001;
/// `fuelSubtypeID`s the E85 Pseudo-THC expressions are restricted to
/// (E70 and E85).
const E85_FUEL_SUBTYPES: [i32; 2] = [51, 52];

/// Materialised inputs for [`do_general_fuel_ratio`].
///
/// Each field stands in for a query the Java method ran against the
/// execution database; Task 50 will populate them from the execution
/// context instead.
#[derive(Debug, Clone, Default)]
pub struct GeneralFuelRatioInputs {
    /// The `generalFuelRatioExpression` table.
    pub expressions: Vec<GeneralFuelRatioExpression>,
    /// Fuel formulations grouped by fuel type — the
    /// `getFuelFormulations(fuelTypeID)` result, carrying each
    /// formulation's column values so expressions can be evaluated.
    pub formulations_by_fuel_type: BTreeMap<i32, Vec<FuelFormulation>>,
    /// `fuelFormulationID`s present in the fuel supply, per fuel type —
    /// the `getFuelSupplyFormulations(fuelTypeID)` result.
    pub supplied_by_fuel_type: BTreeMap<i32, BTreeSet<i32>>,
    /// `(fuelFormulationID, polProcessID)` pairs already present in
    /// `generalFuelRatio` — the `getIntegerPairSet(...)` result. A
    /// formulation already ratioed for a `polProcessID` is skipped.
    pub already_ratioed: BTreeSet<IntegerPair>,
}

/// Derive the E85 "Pseudo-THC" expressions from a list of expressions.
///
/// Ports the `@step 050` block of `doGeneralFuelRatio`: for an ethanol
/// (fuel type 5) THC (pollutant 1) expression covering model years through
/// 2001-or-later for Running (process 1) or Start (process 2) exhaust,
/// MOVES adds a copy that
///
/// * targets the synthetic "Pseudo-THC" pollutant `10001`,
/// * is restricted to the E70/E85 fuel subtypes `51` and `52`,
/// * begins no earlier than model year 2001, and
/// * references `altRVP` wherever the original referenced `RVP`.
///
/// The returned expressions are the *new* ones; the caller appends them to
/// the original list. The derivation reads only the original expressions,
/// exactly as the Java loop does.
#[must_use]
pub fn derive_pseudo_thc_expressions(
    expressions: &[GeneralFuelRatioExpression],
) -> Vec<GeneralFuelRatioExpression> {
    let mut derived = Vec::new();
    for exp in expressions {
        let applies = exp.fuel_type_id == ETHANOL_FUEL_TYPE_ID
            && exp.pollutant_id == THC_POLLUTANT_ID
            && exp.max_model_year_id >= E85_MIN_MODEL_YEAR
            && (exp.process_id == 1 || exp.process_id == 2);
        if !applies {
            continue;
        }
        let mut n = exp.clone();
        n.pollutant_id = PSEUDO_THC_POLLUTANT_ID;
        // Java: n.polProcessID = n.pollutantID * 100 + n.processID.
        n.pol_process_id = PSEUDO_THC_POLLUTANT_ID * 100 + n.process_id;
        n.fuel_subtypes = Some(E85_FUEL_SUBTYPES.to_vec());
        n.min_model_year_id = exp.min_model_year_id.max(E85_MIN_MODEL_YEAR);
        // Java guards a re-scanning `StringUtilities.replace` with a
        // placeholder; Rust's `str::replace` never re-scans its own
        // output, so a direct case-sensitive replace is equivalent.
        n.fuel_effect_ratio_expression = exp.fuel_effect_ratio_expression.replace("RVP", "altRVP");
        n.fuel_effect_ratio_gpa_expression = exp
            .fuel_effect_ratio_gpa_expression
            .replace("RVP", "altRVP");
        derived.push(n);
    }
    derived
}

/// Compute the `generalFuelRatio` rows for a set of inputs.
///
/// Ports `FuelEffectsGenerator.doGeneralFuelRatio`. For each fuel type
/// referenced by an expression, the formulations that exist for that fuel
/// type **and** appear in the fuel supply are intersected; each matching
/// expression is then evaluated against every such formulation that has
/// not already been ratioed for the expression's `polProcessID`. An empty
/// expression string defaults to `"1"`, as in the Java.
///
/// Rows are produced in a deterministic order — fuel type ascending, then
/// expression-list order (originals before the derived Pseudo-THC ones),
/// then `fuelFormulationID` ascending. The Java `insert … select` has no
/// `ORDER BY`, so only the row *set* is defined; sorting here makes the
/// port reproducible.
///
/// # Errors
///
/// Returns an [`ExpressionError`] if any `fuelEffectRatioExpression` fails
/// to parse or references a column no [`FuelFormulation`] resolves.
pub fn do_general_fuel_ratio(
    inputs: &GeneralFuelRatioInputs,
) -> Result<Vec<GeneralFuelRatioRow>, ExpressionError> {
    // Original expressions plus the derived E85 Pseudo-THC ones.
    let mut expressions = inputs.expressions.clone();
    expressions.extend(derive_pseudo_thc_expressions(&inputs.expressions));

    // Distinct fuel types, ascending — Java reads them into a TreeSet.
    let fuel_type_ids: BTreeSet<i32> = inputs.expressions.iter().map(|e| e.fuel_type_id).collect();

    let mut rows = Vec::new();
    for &fuel_type_id in &fuel_type_ids {
        // fuelFormulationsToUse = getFuelFormulations ∩ getFuelSupplyFormulations.
        let Some(formulations) = inputs.formulations_by_fuel_type.get(&fuel_type_id) else {
            continue;
        };
        let supplied = inputs.supplied_by_fuel_type.get(&fuel_type_id);
        let mut to_use: Vec<&FuelFormulation> = formulations
            .iter()
            .filter(|f| supplied.is_some_and(|s| s.contains(&f.fuel_formulation_id)))
            .collect();
        // `getFuelFormulations` returns a `TreeSet<Integer>`: sorted and
        // unique. Sort by ID and drop duplicates to match.
        to_use.sort_by_key(|f| f.fuel_formulation_id);
        to_use.dedup_by_key(|f| f.fuel_formulation_id);
        if to_use.is_empty() {
            continue;
        }

        for exp in &expressions {
            if exp.fuel_type_id != fuel_type_id || exp.min_model_year_id > exp.max_model_year_id {
                continue;
            }

            // Formulations not yet ratioed for this polProcessID — the
            // `formulationIDsCSV` the Java builds before the INSERT.
            let candidates: Vec<&FuelFormulation> = to_use
                .iter()
                .copied()
                .filter(|f| {
                    !contains(
                        &inputs.already_ratioed,
                        Some(f.fuel_formulation_id),
                        Some(exp.pol_process_id),
                    )
                })
                .collect();
            if candidates.is_empty() {
                continue;
            }

            // An empty expression string defaults to "1".
            let ratio_text = non_empty_or_one(&exp.fuel_effect_ratio_expression);
            let gpa_text = non_empty_or_one(&exp.fuel_effect_ratio_gpa_expression);
            let ratio_expr = Expression::parse(ratio_text)?;
            let gpa_expr = Expression::parse(gpa_text)?;

            for fuel in candidates {
                // The Java SQL adds `and fuelSubtypeID in (fuelSubtypes)`
                // when the expression carries a subtype restriction.
                if let Some(subtypes) = &exp.fuel_subtypes {
                    if !subtypes.contains(&fuel.fuel_subtype_id) {
                        continue;
                    }
                }
                rows.push(GeneralFuelRatioRow {
                    fuel_type_id: exp.fuel_type_id,
                    fuel_formulation_id: fuel.fuel_formulation_id,
                    pol_process_id: exp.pol_process_id,
                    pollutant_id: exp.pollutant_id,
                    process_id: exp.process_id,
                    min_model_year_id: exp.min_model_year_id,
                    max_model_year_id: exp.max_model_year_id,
                    min_age_id: exp.min_age_id,
                    max_age_id: exp.max_age_id,
                    source_type_id: exp.source_type_id,
                    fuel_effect_ratio: ratio_expr.evaluate(fuel)?,
                    fuel_effect_ratio_gpa: gpa_expr.evaluate(fuel)?,
                });
            }
        }
    }
    Ok(rows)
}

/// Return `text`, or the literal `"1"` when `text` is empty — the Java
/// default for a blank `fuelEffectRatio`/`fuelEffectRatioGPA` expression.
fn non_empty_or_one(text: &str) -> &str {
    if text.is_empty() {
        "1"
    } else {
        text
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fuel formulation with the given ID, subtype and MTBE volume.
    fn fuel(id: i32, subtype: i32, mtbe: f32) -> FuelFormulation {
        FuelFormulation {
            fuel_formulation_id: id,
            fuel_subtype_id: subtype,
            mtbe_volume: mtbe,
            ..FuelFormulation::default()
        }
    }

    #[test]
    fn evaluates_one_expression_against_one_formulation() {
        // The shape of testDoGeneralFuelRatio: one expression, one fuel.
        let exp = GeneralFuelRatioExpression::new(
            1,
            -101,
            1960,
            2060,
            0,
            30,
            0,
            "MTBEVolume+7",
            "MTBEVolume*2",
        );
        let inputs = GeneralFuelRatioInputs {
            expressions: vec![exp],
            formulations_by_fuel_type: BTreeMap::from([(1, vec![fuel(100, 10, 10.0)])]),
            supplied_by_fuel_type: BTreeMap::from([(1, BTreeSet::from([100]))]),
            already_ratioed: BTreeSet::new(),
        };
        let rows = do_general_fuel_ratio(&inputs).expect("evaluates");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].fuel_effect_ratio, 17.0);
        assert_eq!(rows[0].fuel_effect_ratio_gpa, 20.0);
        assert_eq!(rows[0].pol_process_id, -101);
        assert_eq!(rows[0].fuel_formulation_id, 100);
    }

    #[test]
    fn empty_expression_defaults_to_ratio_one() {
        let exp = GeneralFuelRatioExpression::new(1, 201, 1960, 2060, 0, 30, 0, "", "");
        let inputs = GeneralFuelRatioInputs {
            expressions: vec![exp],
            formulations_by_fuel_type: BTreeMap::from([(1, vec![fuel(100, 10, 5.0)])]),
            supplied_by_fuel_type: BTreeMap::from([(1, BTreeSet::from([100]))]),
            already_ratioed: BTreeSet::new(),
        };
        let rows = do_general_fuel_ratio(&inputs).expect("evaluates");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].fuel_effect_ratio, 1.0);
        assert_eq!(rows[0].fuel_effect_ratio_gpa, 1.0);
    }

    #[test]
    fn formulation_not_in_supply_is_skipped() {
        let exp = GeneralFuelRatioExpression::new(1, 201, 1960, 2060, 0, 30, 0, "1", "1");
        let inputs = GeneralFuelRatioInputs {
            expressions: vec![exp],
            formulations_by_fuel_type: BTreeMap::from([(1, vec![fuel(100, 10, 5.0)])]),
            // Formulation 100 exists but is not supplied.
            supplied_by_fuel_type: BTreeMap::from([(1, BTreeSet::new())]),
            already_ratioed: BTreeSet::new(),
        };
        assert!(do_general_fuel_ratio(&inputs)
            .expect("evaluates")
            .is_empty());
    }

    #[test]
    fn already_ratioed_formulation_is_skipped() {
        let exp = GeneralFuelRatioExpression::new(1, 201, 1960, 2060, 0, 30, 0, "1", "1");
        let inputs = GeneralFuelRatioInputs {
            expressions: vec![exp],
            formulations_by_fuel_type: BTreeMap::from([(1, vec![fuel(100, 10, 5.0)])]),
            supplied_by_fuel_type: BTreeMap::from([(1, BTreeSet::from([100]))]),
            // (formulation 100, polProcessID 201) already ratioed.
            already_ratioed: BTreeSet::from([IntegerPair::new(Some(100), Some(201))]),
        };
        assert!(do_general_fuel_ratio(&inputs)
            .expect("evaluates")
            .is_empty());
    }

    #[test]
    fn min_model_year_after_max_is_skipped() {
        let exp = GeneralFuelRatioExpression::new(1, 201, 2060, 1960, 0, 30, 0, "1", "1");
        let inputs = GeneralFuelRatioInputs {
            expressions: vec![exp],
            formulations_by_fuel_type: BTreeMap::from([(1, vec![fuel(100, 10, 5.0)])]),
            supplied_by_fuel_type: BTreeMap::from([(1, BTreeSet::from([100]))]),
            already_ratioed: BTreeSet::new(),
        };
        assert!(do_general_fuel_ratio(&inputs)
            .expect("evaluates")
            .is_empty());
    }

    #[test]
    fn derives_pseudo_thc_expression_for_e85_thc() {
        // Ethanol (5), THC (pollutant 1), Running (process 1), through 2010.
        let exp = GeneralFuelRatioExpression::new(
            ETHANOL_FUEL_TYPE_ID,
            101, // pollutant 1, process 1
            1990,
            2010,
            0,
            30,
            0,
            "RVP*0.5",
            "RVP+1",
        );
        let derived = derive_pseudo_thc_expressions(&[exp]);
        assert_eq!(derived.len(), 1);
        let n = &derived[0];
        assert_eq!(n.pollutant_id, PSEUDO_THC_POLLUTANT_ID);
        assert_eq!(n.process_id, 1);
        assert_eq!(n.pol_process_id, PSEUDO_THC_POLLUTANT_ID * 100 + 1);
        assert_eq!(n.fuel_subtypes.as_deref(), Some(&[51, 52][..]));
        // minModelYearID is lifted to 2001.
        assert_eq!(n.min_model_year_id, 2001);
        // RVP is rewritten to altRVP in both expressions.
        assert_eq!(n.fuel_effect_ratio_expression, "altRVP*0.5");
        assert_eq!(n.fuel_effect_ratio_gpa_expression, "altRVP+1");
    }

    #[test]
    fn no_pseudo_thc_expression_for_non_ethanol() {
        // Gasoline (1) THC must not spawn a Pseudo-THC expression.
        let exp = GeneralFuelRatioExpression::new(1, 101, 1990, 2010, 0, 30, 0, "RVP*0.5", "RVP+1");
        assert!(derive_pseudo_thc_expressions(&[exp]).is_empty());
    }

    #[test]
    fn pseudo_thc_expression_filters_to_e85_subtypes() {
        // Ethanol THC expression — the derived Pseudo-THC row applies only
        // to fuel subtypes 51/52; a formulation with subtype 10 misses it.
        let exp = GeneralFuelRatioExpression::new(
            ETHANOL_FUEL_TYPE_ID,
            101,
            1990,
            2010,
            0,
            30,
            0,
            "1",
            "1",
        );
        let inputs = GeneralFuelRatioInputs {
            expressions: vec![exp],
            formulations_by_fuel_type: BTreeMap::from([(
                ETHANOL_FUEL_TYPE_ID,
                vec![fuel(200, 51, 0.0), fuel(201, 10, 0.0)],
            )]),
            supplied_by_fuel_type: BTreeMap::from([(
                ETHANOL_FUEL_TYPE_ID,
                BTreeSet::from([200, 201]),
            )]),
            already_ratioed: BTreeSet::new(),
        };
        let rows = do_general_fuel_ratio(&inputs).expect("evaluates");
        // Original expression: both formulations (no subtype filter) -> 2.
        // Derived Pseudo-THC expression: subtype 51 only -> 1. Total 3.
        assert_eq!(rows.len(), 3);
        let pseudo: Vec<_> = rows
            .iter()
            .filter(|r| r.pollutant_id == PSEUDO_THC_POLLUTANT_ID)
            .collect();
        assert_eq!(pseudo.len(), 1);
        assert_eq!(pseudo[0].fuel_formulation_id, 200);
    }
}
