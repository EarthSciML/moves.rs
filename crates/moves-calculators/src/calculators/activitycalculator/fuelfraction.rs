//! Source-bin fuel and regulatory-class weighting.
//!
//! Ports the `createSourceTypeFuelFraction` script section plus the
//! `sourceTypeFuelFraction` ⋈ `RegClassSourceTypeFraction` join that every
//! non-hotelling activity section runs to split a base activity quantity
//! across the source bin.
//!
//! `createSourceTypeFuelFraction` builds the `sourceTypeFuelFraction` table//! the share of a `(sourceType, modelYear)` population on each fuel type//! from the sample-vehicle fleet. The script offers two variants, selected by
//! the Java `CompilationFlags.USE_FUELUSAGEFRACTION` flag and modelled here
//! by [`FuelFractionMode`].
//!
//! # Fidelity
//!
//! `tempFuelFraction`, `tempTotal`, and `fuelFraction` are all `DOUBLE`
//! columns in the SQL, so `tempFuelFraction / tempTotal` is ordinary
//! floating-point division — no MariaDB integer-division rounding applies,
//! and the temp tables carry no `FLOAT` columns that would truncate an
//! intermediate to 32-bit. The port computes in [`f64`] throughout.

use std::collections::{HashMap, HashSet};

use super::inputs::ActivityInputs;
use super::model::SourceTypeFuelFractionRow;

/// Which `createSourceTypeFuelFraction` variant to run — the Java
/// `CompilationFlags.USE_FUELUSAGEFRACTION` switch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FuelFractionMode {
    /// `UseSampleVehiclePopulation` — `tempFuelFraction` is `sum(stmyFraction)`
    /// straight from `sampleVehiclePopulation`. The default, matching
    /// `USE_FUELUSAGEFRACTION = false`.
    #[default]
    SampleVehiclePopulation,
    /// `UseFuelUsageFraction` — `tempFuelFraction` reassigns each vehicle's
    /// nominal fuel to the fuel it actually burns via `fuelUsageFraction`,
    /// weighting by `usageFraction`.
    FuelUsageFraction,
}

/// Build the `sourceTypeFuelFraction` table — the fuel-type split of each
/// `(sourceType, modelYear)` population.
///
/// Ports the `createSourceTypeFuelFraction` section: build the per-fuel
/// numerator `sourceTypeFuelFractionTemp`, the per-`(sourceType, modelYear)`
/// denominator `sourceTypeFuelFractionTotal`, then divide. A fuel row
/// survives only when its `(sourceType, fuelType)` pair is in
/// `runSpecSourceFuelType`, and the fraction is `0` when the denominator is
/// not positive.
///
/// Output is sorted by `(sourceTypeID, modelYearID, fuelTypeID)` for a
/// deterministic result; the SQL leaves final-`SELECT` order unspecified.
#[must_use]
pub fn create_source_type_fuel_fraction(
    inputs: &ActivityInputs,
    mode: FuelFractionMode,
) -> Vec<SourceTypeFuelFractionRow> {
    // `sourceTypeFuelFractionTemp` — numerator, keyed (sourceTypeModelYearID,
    // fuelTypeID).
    let temp = build_temp(inputs, mode);

    // `sourceTypeFuelFractionTotal` — denominator, keyed sourceTypeModelYearID:
    // sum(stmyFraction) over all fuel types.
    let mut total: HashMap<i32, f64> = HashMap::new();
    for svp in &inputs.sample_vehicle_population {
        *total.entry(svp.source_type_model_year_id).or_insert(0.0) += svp.stmy_fraction;
    }

    // The `UPDATE sourceTypeFuelFractionTotal, sourceTypeModelYear` step:
    // resolve each surrogate key to its (sourceType, modelYear). Rows with no
    // match keep NULL ids and are dropped by the final join.
    let resolve: HashMap<i32, (i32, i32)> = inputs
        .source_type_model_year
        .iter()
        .map(|r| {
            (
                r.source_type_model_year_id,
                (r.source_type_id, r.model_year_id),
            )
        })
        .collect();

    // `runSpecSourceFuelType` gate on the final join.
    let run_spec: HashSet<(i32, i32)> = inputs
        .run_spec_source_fuel_type
        .iter()
        .map(|r| (r.source_type_id, r.fuel_type_id))
        .collect();

    let mut out = Vec::with_capacity(temp.len());
    for (&(stmy_id, fuel_type_id), &temp_fuel_fraction) in &temp {
        // INNER JOIN sourceTypeFuelFractionTotal t.
        let Some(&temp_total) = total.get(&stmy_id) else {
            continue;
        };
        // The UPDATE-supplied (sourceTypeID, modelYearID); NULL ids drop out.
        let Some(&(source_type_id, model_year_id)) = resolve.get(&stmy_id) else {
            continue;
        };
        // INNER JOIN runSpecSourceFuelType rs.
        if !run_spec.contains(&(source_type_id, fuel_type_id)) {
            continue;
        }
        let fuel_fraction = if temp_total > 0.0 {
            temp_fuel_fraction / temp_total
        } else {
            0.0
        };
        out.push(SourceTypeFuelFractionRow {
            source_type_id,
            model_year_id,
            fuel_type_id,
            fuel_fraction,
        });
    }

    out.sort_by(|a, b| {
        (a.source_type_id, a.model_year_id, a.fuel_type_id).cmp(&(
            b.source_type_id,
            b.model_year_id,
            b.fuel_type_id,
        ))
    });
    out
}

/// Build `sourceTypeFuelFractionTemp` — the per-`(sourceTypeModelYearID,
/// fuelTypeID)` numerator — for the selected [`FuelFractionMode`].
fn build_temp(inputs: &ActivityInputs, mode: FuelFractionMode) -> HashMap<(i32, i32), f64> {
    let mut temp: HashMap<(i32, i32), f64> = HashMap::new();
    match mode {
        FuelFractionMode::SampleVehiclePopulation => {
            // sum(stmyFraction) GROUP BY sourceTypeModelYearID, fuelTypeID.
            for svp in &inputs.sample_vehicle_population {
                *temp
                    .entry((svp.source_type_model_year_id, svp.fuel_type_id))
                    .or_insert(0.0) += svp.stmy_fraction;
            }
        }
        FuelFractionMode::FuelUsageFraction => {
            // fuelUsageFraction filtered to this county / fuel year, indexed
            // by the nominal (source-bin) fuel type.
            let mut usage: HashMap<i32, Vec<(i32, f64)>> = HashMap::new();
            for fuf in &inputs.fuel_usage_fraction {
                if fuf.county_id == inputs.context.county_id
                    && fuf.fuel_year_id == inputs.context.fuel_year_id
                    && fuf.model_year_group_id == 0
                {
                    usage
                        .entry(fuf.source_bin_fuel_type_id)
                        .or_default()
                        .push((fuf.fuel_supply_fuel_type_id, fuf.usage_fraction));
                }
            }
            // sum(stmyFraction * usageFraction) GROUP BY sourceTypeModelYearID,
            // fuelSupplyFuelTypeID.
            for svp in &inputs.sample_vehicle_population {
                let Some(supplies) = usage.get(&svp.fuel_type_id) else {
                    continue;
                };
                for &(supply_fuel_type_id, usage_fraction) in supplies {
                    *temp
                        .entry((svp.source_type_model_year_id, supply_fuel_type_id))
                        .or_insert(0.0) += svp.stmy_fraction * usage_fraction;
                }
            }
        }
    }
    temp
}

/// `sourceTypeFuelFraction` indexed for the `stff` join — the fuel-type rows
/// of each `(sourceType, modelYear)` bin.
#[derive(Debug, Default)]
pub struct FuelFractionIndex {
    rows: HashMap<(i32, i32), Vec<(i32, f64)>>,
}

impl FuelFractionIndex {
    /// Index `sourceTypeFuelFraction` rows by `(sourceTypeID, modelYearID)`.
    #[must_use]
    pub fn new(rows: &[SourceTypeFuelFractionRow]) -> Self {
        let mut map: HashMap<(i32, i32), Vec<(i32, f64)>> = HashMap::new();
        for r in rows {
            map.entry((r.source_type_id, r.model_year_id))
                .or_default()
                .push((r.fuel_type_id, r.fuel_fraction));
        }
        Self { rows: map }
    }

    /// The `(fuelTypeID, fuelFraction)` rows of a `(sourceType, modelYear)`
    /// bin — empty when the bin has no fuel-fraction row (the `stff` inner
    /// join then drops it).
    #[must_use]
    pub fn fractions(&self, source_type_id: i32, model_year_id: i32) -> &[(i32, f64)] {
        self.rows
            .get(&(source_type_id, model_year_id))
            .map_or(&[], Vec::as_slice)
    }
}

/// `RegClassSourceTypeFraction` indexed for the `stf` join — the
/// regulatory-class rows of each `(sourceType, fuelType, modelYear)` bin.
#[derive(Debug, Default)]
pub struct RegClassIndex {
    rows: HashMap<(i32, i32, i32), Vec<(i32, f64)>>,
}

impl RegClassIndex {
    /// Index `RegClassSourceTypeFraction` rows by `(sourceTypeID, fuelTypeID,
    /// modelYearID)`.
    #[must_use]
    pub fn new(rows: &[super::inputs::RegClassSourceTypeFractionRow]) -> Self {
        let mut map: HashMap<(i32, i32, i32), Vec<(i32, f64)>> = HashMap::new();
        for r in rows {
            map.entry((r.source_type_id, r.fuel_type_id, r.model_year_id))
                .or_default()
                .push((r.reg_class_id, r.reg_class_fraction));
        }
        Self { rows: map }
    }

    /// The `(regClassID, regClassFraction)` rows of a `(sourceType, fuelType,
    /// modelYear)` bin — empty when the bin has none (the `stf` inner join
    /// then drops it).
    #[must_use]
    pub fn reg_classes(
        &self,
        source_type_id: i32,
        fuel_type_id: i32,
        model_year_id: i32,
    ) -> &[(i32, f64)] {
        self.rows
            .get(&(source_type_id, fuel_type_id, model_year_id))
            .map_or(&[], Vec::as_slice)
    }
}

/// One leaf of the source-bin expansion: a `(fuelType, regClass)` pair and
/// the weight `fuelFraction * regClassFraction` applied to the base activity.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FuelRegClassWeight {
    /// `fuelTypeID`.
    pub fuel_type_id: i32,
    /// `regClassID`.
    pub reg_class_id: i32,
    /// `fuelFraction * regClassFraction`.
    pub weight: f64,
}

/// Expand a `(sourceType, modelYear)` bin into its `(fuelType, regClass,
/// weight)` leaves — the `stff` ⋈ `stf` join shared by `SourceHours`, `SHO`,
/// `ONI`, `SHP`, `Starts`, and both `Population` domains.
///
/// Iterates the `sourceTypeFuelFraction` rows of the bin, then for each the
/// `RegClassSourceTypeFraction` rows keyed by that fuel type — exactly the
/// nested inner joins of the SQL, in input order.
#[must_use]
pub fn fuel_reg_class_weights(
    fuel: &FuelFractionIndex,
    reg: &RegClassIndex,
    source_type_id: i32,
    model_year_id: i32,
) -> Vec<FuelRegClassWeight> {
    let mut out = Vec::new();
    for &(fuel_type_id, fuel_fraction) in fuel.fractions(source_type_id, model_year_id) {
        for &(reg_class_id, reg_class_fraction) in
            reg.reg_classes(source_type_id, fuel_type_id, model_year_id)
        {
            out.push(FuelRegClassWeight {
                fuel_type_id,
                reg_class_id,
                weight: fuel_fraction * reg_class_fraction,
            });
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::super::inputs::{
        RegClassSourceTypeFractionRow, RunSpecSourceFuelTypeRow, SampleVehiclePopulationRow,
        SourceTypeModelYearRow,
    };
    use super::*;

    /// Build inputs for one source-type/model-year sampled across two fuel
    /// types: stmyID 7 = (sourceType 21, modelYear 2018), fuel 1 share 0.75,
    /// fuel 2 share 0.25.
    fn two_fuel_inputs() -> ActivityInputs {
        ActivityInputs {
            sample_vehicle_population: vec![
                SampleVehiclePopulationRow {
                    source_type_model_year_id: 7,
                    fuel_type_id: 1,
                    stmy_fraction: 0.75,
                },
                SampleVehiclePopulationRow {
                    source_type_model_year_id: 7,
                    fuel_type_id: 2,
                    stmy_fraction: 0.25,
                },
            ],
            source_type_model_year: vec![SourceTypeModelYearRow {
                source_type_model_year_id: 7,
                source_type_id: 21,
                model_year_id: 2018,
            }],
            run_spec_source_fuel_type: vec![
                RunSpecSourceFuelTypeRow {
                    source_type_id: 21,
                    fuel_type_id: 1,
                },
                RunSpecSourceFuelTypeRow {
                    source_type_id: 21,
                    fuel_type_id: 2,
                },
            ],
            ..ActivityInputs::default()
        }
    }

    #[test]
    fn sample_vehicle_population_mode_normalises_to_one() {
        let out = create_source_type_fuel_fraction(
            &two_fuel_inputs(),
            FuelFractionMode::SampleVehiclePopulation,
        );
        assert_eq!(out.len(), 2);
        // 0.75 / (0.75 + 0.25) and 0.25 / 1.0.
        assert!((out[0].fuel_fraction - 0.75).abs() < 1e-12);
        assert_eq!(out[0].fuel_type_id, 1);
        assert!((out[1].fuel_fraction - 0.25).abs() < 1e-12);
        assert_eq!(out[1].fuel_type_id, 2);
        // Output carries the resolved (sourceType, modelYear).
        assert!(out
            .iter()
            .all(|r| r.source_type_id == 21 && r.model_year_id == 2018));
    }

    #[test]
    fn run_spec_gate_drops_unselected_fuel_pairs() {
        let mut inputs = two_fuel_inputs();
        // Drop fuel 2 from the RunSpec selection.
        inputs.run_spec_source_fuel_type.pop();
        let out =
            create_source_type_fuel_fraction(&inputs, FuelFractionMode::SampleVehiclePopulation);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].fuel_type_id, 1);
        // The denominator still includes the dropped fuel's sample share.
        assert!((out[0].fuel_fraction - 0.75).abs() < 1e-12);
    }

    #[test]
    fn unresolved_surrogate_key_drops_out() {
        let mut inputs = two_fuel_inputs();
        // Remove the sourceTypeModelYear row: the UPDATE leaves NULL ids.
        inputs.source_type_model_year.clear();
        let out =
            create_source_type_fuel_fraction(&inputs, FuelFractionMode::SampleVehiclePopulation);
        assert!(out.is_empty());
    }

    #[test]
    fn fuel_usage_fraction_mode_reassigns_fuel() {
        use super::super::inputs::{FuelUsageFractionRow, IterationContext};
        let mut inputs = two_fuel_inputs();
        inputs.context = IterationContext {
            county_id: 26161,
            fuel_year_id: 2018,
            ..IterationContext::default()
        };
        // All of nominal fuel 1 is actually burned as supply fuel 2.
        inputs.fuel_usage_fraction = vec![
            FuelUsageFractionRow {
                county_id: 26161,
                fuel_year_id: 2018,
                model_year_group_id: 0,
                source_bin_fuel_type_id: 1,
                fuel_supply_fuel_type_id: 2,
                usage_fraction: 1.0,
            },
            FuelUsageFractionRow {
                county_id: 26161,
                fuel_year_id: 2018,
                model_year_group_id: 0,
                source_bin_fuel_type_id: 2,
                fuel_supply_fuel_type_id: 2,
                usage_fraction: 1.0,
            },
        ];
        let out = create_source_type_fuel_fraction(&inputs, FuelFractionMode::FuelUsageFraction);
        // Both nominal fuels collapse onto supply fuel 2: one row, fraction 1.
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].fuel_type_id, 2);
        assert!((out[0].fuel_fraction - 1.0).abs() < 1e-12);
    }

    #[test]
    fn fuel_usage_fraction_filters_by_county_and_fuel_year() {
        use super::super::inputs::{FuelUsageFractionRow, IterationContext};
        let mut inputs = two_fuel_inputs();
        inputs.context = IterationContext {
            county_id: 26161,
            fuel_year_id: 2018,
            ..IterationContext::default()
        };
        // Wrong county / fuel year / model-year group — all filtered out.
        inputs.fuel_usage_fraction = vec![
            FuelUsageFractionRow {
                county_id: 99999,
                fuel_year_id: 2018,
                model_year_group_id: 0,
                source_bin_fuel_type_id: 1,
                fuel_supply_fuel_type_id: 2,
                usage_fraction: 1.0,
            },
            FuelUsageFractionRow {
                county_id: 26161,
                fuel_year_id: 2018,
                model_year_group_id: 17,
                source_bin_fuel_type_id: 2,
                fuel_supply_fuel_type_id: 2,
                usage_fraction: 1.0,
            },
        ];
        let out = create_source_type_fuel_fraction(&inputs, FuelFractionMode::FuelUsageFraction);
        assert!(out.is_empty());
    }

    #[test]
    fn expansion_multiplies_fuel_and_reg_class_fractions() {
        let fuel = FuelFractionIndex::new(&[SourceTypeFuelFractionRow {
            source_type_id: 21,
            model_year_id: 2018,
            fuel_type_id: 1,
            fuel_fraction: 0.8,
        }]);
        let reg = RegClassIndex::new(&[
            RegClassSourceTypeFractionRow {
                source_type_id: 21,
                fuel_type_id: 1,
                model_year_id: 2018,
                reg_class_id: 30,
                reg_class_fraction: 0.6,
            },
            RegClassSourceTypeFractionRow {
                source_type_id: 21,
                fuel_type_id: 1,
                model_year_id: 2018,
                reg_class_id: 40,
                reg_class_fraction: 0.4,
            },
        ]);
        let weights = fuel_reg_class_weights(&fuel, &reg, 21, 2018);
        assert_eq!(weights.len(), 2);
        assert!((weights[0].weight - 0.8 * 0.6).abs() < 1e-12);
        assert_eq!(weights[0].reg_class_id, 30);
        assert!((weights[1].weight - 0.8 * 0.4).abs() < 1e-12);
        assert_eq!(weights[1].reg_class_id, 40);
    }

    #[test]
    fn expansion_is_empty_when_a_join_side_is_missing() {
        let fuel = FuelFractionIndex::new(&[SourceTypeFuelFractionRow {
            source_type_id: 21,
            model_year_id: 2018,
            fuel_type_id: 1,
            fuel_fraction: 0.8,
        }]);
        // No RegClassSourceTypeFraction rows: the `stf` join yields nothing.
        let reg = RegClassIndex::new(&[]);
        assert!(fuel_reg_class_weights(&fuel, &reg, 21, 2018).is_empty());
        // Unknown bin: the `stff` join yields nothing.
        let reg2 = RegClassIndex::new(&[RegClassSourceTypeFractionRow {
            source_type_id: 21,
            fuel_type_id: 1,
            model_year_id: 2018,
            reg_class_id: 30,
            reg_class_fraction: 1.0,
        }]);
        assert!(fuel_reg_class_weights(&fuel, &reg2, 99, 2018).is_empty());
    }
}
