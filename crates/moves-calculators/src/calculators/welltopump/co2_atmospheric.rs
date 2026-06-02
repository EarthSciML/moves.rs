//! Port of `CO2AtmosphericWTPCalculator.java` and
//! `database/CO2AtmosphericWTPCalculator.sql` —.//!.
//!
//! `CO2AtmosphericWTPCalculator` computes **well-to-pump (upstream)
//! atmospheric CO2** — the CO2 released extracting, refining and distributing
//! the fuel a vehicle later burns. It scales the running, start and
//! extended-idle exhaust Total Energy Consumption by a fuel-specific GREET CO2
//! emission rate.
//!
//! # Superseded — empty registrations
//!
//! `CO2AtmosphericWTPCalculator` is not wired into the pinned MOVES runtime.
//! The Well-To-Pump process (id 99) has **no `Registration` directive at all**
//! in `CalculatorInfo.txt`, and
//! `characterization/calculator-chains/calculator-dag.json` records
//! `CO2AtmosphericWTPCalculator` with `registrations_count: 0`,
//! `subscriptions: []` and `depends_on: []`. The modern base-rate engine
//! (`BaseRateCalculator`, ) absorbed the per-pollutant
//! scripted-SQL calculators; the still lists this class as part
//! of, so the module ports its algorithm faithfully for reference and
//! cross-validation with [`Calculator::registrations`] returning an empty
//! slice. See [`super::common`] for the cluster's shared infrastructure.
//!
//! # Chained calculator
//!
//! `CO2AtmosphericWTPCalculator` is a *chained* calculator: its Java
//! `subscribeToMe` does not subscribe to the MasterLoop but chains the
//! calculator onto the ones producing Total Energy Consumption. The chain DAG
//! records `subscribes_directly: false`; the [`Calculator`] metadata mirrors
//! it — [`subscriptions`](Calculator::subscriptions) is empty, and
//! [`upstream`](Calculator::upstream) is empty too because the unwired process
//! leaves the DAG `depends_on` empty.
//!
//! # What it computes
//!
//! [`Co2AtmosphericWtpCalculator::calculate`] ports
//! `CO2AtmosphericWTPCalculator.sql`. For every running (1), start (2) or
//! extended-idle (90) exhaust Total Energy Consumption record
//! (`MOVESWorkerOutput`, pollutant 91):
//!
//! ```text
//! wellToPumpCO2 = Σ (pumpToWheelEnergy × sumCO2EmissionRate)
//! ```
//!
//! summed over the records of each output dimension cell. `sumCO2EmissionRate`
//! is the market-share-weighted GREET well-to-pump atmospheric-CO2 rate of the
//! cell's `(year, monthGroup, fuelType)` — the `GWTPCO2FactorByFuelType`
//! working table this port rebuilds in [`build_co2_factor_by_fuel_type`].
//!
//! Unlike [`super::total_energy`] and [`super::ch4n2o`], this calculator does
//! **not** interpolate the GREET rate between bracketing years: its SQL joins
//! `greetwelltopump` on `gwtp.yearID = ##context.year##` directly, so a fuel
//! subtype with no GREET row at the run year is dropped outright. The factor
//! is `Σ marketShare × emissionRate` over the run year's fuel supply.
//!
//! The SQL aggregate is `SUM(mwo.emissionQuant * gwtp.sumCO2EmissionRate)`//! the factor multiplies inside the sum, one row at a time (it is in fact
//! constant within a `GROUP BY` group, so the result equals
//! `Σ(emissionQuant) × rate` mathematically; the two differ only in `f64`
//! rounding, and the port reproduces the SQL's per-row form).
//!
//! The output row is stamped with atmospheric CO2 (pollutant 90) and the
//! well-to-pump process (99). Unlike the other WTP calculators, the SQL
//! `GROUP BY` **does** include `mwo.processID`, so each contributing source
//! process (running, start, extended-idle) produces its own output row — all
//! carrying process 99 but summed separately by source process.
//!
//! Every SQL join is an `INNER JOIN`, so a record that fails to resolve its
//! month group or CO2 factor is dropped; the port reproduces that with map
//! lookups that skip on a miss.
//!
//! # Scope of this port
//!
//! [`calculate`](Co2AtmosphericWtpCalculator::calculate) is the SQL
//! "Processing" section plus the market-share weighting of the "Extract Data"
//! section. Its [`WtpInputs`] argument is the set of tables the SQL extracts,
//! as plain row vectors; a future (`DataFrameStore`) wiring populates
//! it from the per-run filtered execution database.
//!
//! The Java `doExecute` gates the whole calculator on the RunSpec actually
//! requesting Atmospheric CO2 for Well-To-Pump; that is execution-gating,
//! reproduced by `calculate` returning no rows on empty input. `MOVESRunID`
//! and `SCC` are pass-through columns left to the wiring. The SQL
//! keys `GWTPCO2FactorByFuelType` by the literal context `countyID` and joins
//! it `gwtp.countyID = mwo.countyID`; a master-loop invocation is
//! single-county, so the join is trivially satisfied and the port carries
//! `countyID` straight from the energy row.
//!
//! One SQL artifact is **not** reproduced: the `GWTPCO2FactorByFuelType`
//! `INSERT … SELECT` ends in a `LIMIT 10` with no `ORDER BY`. That is an
//! arbitrary, non-deterministic truncation of the extracted factor table — an
//! extraction quirk, not algorithm — so the port builds the full factor table
//! and leaves any extraction-stage capping to the data-plane wiring.
//!
//! # Data plane
//!
//! [`Calculator::execute`] receives a [`CalculatorContext`] whose execution
//! tables and scratch namespace are placeholders until the
//! `DataFrameStore` lands (), so `execute` cannot yet
//! read `MOVESWorkerOutput` nor write the well-to-pump rows back. The numeric
//! algorithm is fully ported and unit-tested on
//! [`calculate`](Co2AtmosphericWtpCalculator::calculate); `execute` is a
//! documented shell returning an empty [`CalculatorOutput`].

use std::collections::{BTreeMap, HashMap, HashSet};

use moves_data::PollutantProcessAssociation;
use moves_framework::{
    Calculator, CalculatorContext, CalculatorOutput, CalculatorSubscription, DataFrameStoreTyped,
    Error,
};

use super::common::{
    month_group_index, FuelFormulationRow, FuelSubTypeRow, FuelSupplyRow, GreetWellToPumpRow,
    MonthGroupRow, WorkerOutputRow, WtpInputs, YearRow,
};

/// Stable module name — matches the Java class and the
/// `CO2AtmosphericWTPCalculator` entry in the chain DAG (`calculator-dag.json`).
const CALCULATOR_NAME: &str = "CO2AtmosphericWTPCalculator";

/// Total Energy Consumption — `Pollutant` id 91. The energy records the CO2
/// formula consumes are the `MOVESWorkerOutput` rows for this pollutant.
const TOTAL_ENERGY_POLLUTANT_ID: i32 = 91;

/// Atmospheric CO2 — `Pollutant` id 90. The pollutant this calculator
/// produces, and the GREET well-to-pump pollutant whose rate it weights.
const ATMOSPHERIC_CO2_POLLUTANT_ID: i32 = 90;

/// Well-To-Pump — `EmissionProcess` id 99. The process the output rows carry.
const WELL_TO_PUMP_PROCESS_ID: i32 = 99;

/// The source exhaust processes the SQL admits — running (1), start (2) and
/// extended-idle (90) exhaust. The `WHERE` clause keeps only `MOVESWorkerOutput`
/// energy records on one of these processes.
const SOURCE_PROCESS_IDS: [i32; 3] = [1, 2, 90];

/// The `GROUP BY` cell of `CO2AtmosphericWTPCalculator.sql`'s `MOVESOutputTemp1b`.
///
/// The SQL groups by the full output dimension **including** `mwo.processID`,
/// so the source process is part of the key even though every output row is
/// stamped with the well-to-pump process. Field order is the deterministic
/// output sort order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct GroupKey {
    year_id: i32,
    month_id: i32,
    day_id: i32,
    hour_id: i32,
    state_id: i32,
    county_id: i32,
    zone_id: i32,
    link_id: i32,
 /// The source exhaust process — a `GROUP BY` axis; the output row is
 /// stamped with the well-to-pump process regardless.
    source_process_id: i32,
    source_type_id: i32,
    fuel_type_id: i32,
    model_year_id: i32,
    road_type_id: i32,
}

/// Build `GWTPCO2FactorByFuelType` — the market-share-weighted GREET
/// well-to-pump atmospheric-CO2 rate, keyed `(yearID, monthGroupID,
/// fuelTypeID)`.
///
/// Ports the "Extract Data" computation of `CO2AtmosphericWTPCalculator.sql`.
/// For every `FuelSupply` row whose fuel year resolves to the run year and
/// whose formulation resolves a subtype with a GREET atmospheric-CO2 rate at
/// the run year:
///
/// ```text
/// GWTPCO2FactorByFuelType[year, monthGroup, fuelType]
/// += marketShare × greetRate(fuelSubType, atmosphericCO2, year)
/// ```
///
/// There is **no** year interpolation: the SQL joins `greetwelltopump` on
/// `gwtp.yearID = ##context.year##`, so only rates tabulated at the run year
/// participate. Every join is an `INNER JOIN`, reproduced by map lookups that
/// skip on a miss.
#[must_use]
pub fn build_co2_factor_by_fuel_type(inputs: &WtpInputs) -> HashMap<(i32, i32, i32), f64> {
 // GREET atmospheric-CO2 rate at the run year, keyed by fuel subtype.
    let co2_rate: HashMap<i32, f64> = inputs
        .greet
        .iter()
        .filter(|g| {
            g.pollutant_id == ATMOSPHERIC_CO2_POLLUTANT_ID && g.year_id == inputs.target_year
        })
        .map(|g| (g.fuel_sub_type_id, g.emission_rate))
        .collect();

    let formulation: HashMap<i32, &FuelFormulationRow> = inputs
        .fuel_formulation
        .iter()
        .map(|ff| (ff.fuel_formulation_id, ff))
        .collect();
    let sub_type: HashMap<i32, &FuelSubTypeRow> = inputs
        .fuel_sub_type
        .iter()
        .map(|fst| (fst.fuel_sub_type_id, fst))
        .collect();
 // Year resolves fuelYearID → yearID, keeping only the run-year rows.
    let target_fuel_years: HashSet<i32> = inputs
        .year
        .iter()
        .filter(|y| y.year_id == inputs.target_year)
        .map(|y| y.fuel_year_id)
        .collect();

    let mut weighted: HashMap<(i32, i32, i32), f64> = HashMap::new();
    for fs in &inputs.fuel_supply {
 // INNER JOIN Year ON Year.fuelYearID = FuelSupply.fuelYearID,
 // y.yearID = run year.
        if !target_fuel_years.contains(&fs.fuel_year_id) {
            continue;
        }
 // INNER JOIN FuelFormulation USING (fuelFormulationID).
        let Some(ff) = formulation.get(&fs.fuel_formulation_id) else {
            continue;
        };
 // INNER JOIN FuelSubtype USING (fuelSubtypeID).
        let Some(fst) = sub_type.get(&ff.fuel_sub_type_id) else {
            continue;
        };
 // INNER JOIN greetwelltopump ON fuelSubtypeID (and the run year).
        let Some(&rate) = co2_rate.get(&ff.fuel_sub_type_id) else {
            continue;
        };
 *weighted
            .entry((inputs.target_year, fs.month_group_id, fst.fuel_type_id))
            .or_default() += fs.market_share * rate;
    }
    weighted
}

/// The MOVES well-to-pump atmospheric-CO2 calculator.
///
/// A zero-sized value type owning no per-run state, as the [`Calculator`]
/// trait contract requires; all run-varying input flows through the
/// [`WtpInputs`] argument to [`calculate`](Self::calculate).
#[derive(Debug, Clone, Copy, Default)]
pub struct Co2AtmosphericWtpCalculator;

impl Co2AtmosphericWtpCalculator {
 /// Stable module name — matches the Java class and the chain-DAG entry.
    pub const NAME: &'static str = CALCULATOR_NAME;

 /// Compute the well-to-pump atmospheric-CO2 rows — the port of
 /// `CO2AtmosphericWTPCalculator.sql`.
 ///
 /// Returns no rows when the inputs carry no usable energy: an energy
 /// record contributes only if it is pollutant 91 on a running, start or
 /// extended-idle process, its month resolves a month group, and its
 /// `(year, monthGroup, fuelType)` resolves a CO2 factor — every SQL join
 /// is an `INNER JOIN`. The result is ordered by its `GROUP BY` cell for
 /// deterministic output; MOVES leaves `MOVESWorkerOutput` physically
 /// unordered.
    #[must_use]
    pub fn calculate(&self, inputs: &WtpInputs) -> Vec<WorkerOutputRow> {
        let factor_table = build_co2_factor_by_fuel_type(inputs);
        let month_group = month_group_index(&inputs.month_of_any_year);

 // emissionQuant = Σ (energy × sumCO2EmissionRate), grouped by the
 // output dimension including the source process.
        let mut groups: BTreeMap<GroupKey, f64> = BTreeMap::new();
        for energy in &inputs.worker_output {
 // mwo.pollutantID = 91.
            if energy.pollutant_id != TOTAL_ENERGY_POLLUTANT_ID {
                continue;
            }
 // mwo.processID = 1 OR 2 OR 90 — running, start, extended-idle.
            if !SOURCE_PROCESS_IDS.contains(&energy.process_id) {
                continue;
            }
 // INNER JOIN may ON may.monthID = mwo.monthID.
            let Some(&month_group_id) = month_group.get(&energy.month_id) else {
                continue;
            };
 // INNER JOIN gwtp ON yearID, monthGroupID, fuelTypeID (countyID is
 // the trivially-satisfied single-county join).
            let Some(&rate) =
                factor_table.get(&(energy.year_id, month_group_id, energy.fuel_type_id))
            else {
                continue;
            };
            let key = GroupKey {
                year_id: energy.year_id,
                month_id: energy.month_id,
                day_id: energy.day_id,
                hour_id: energy.hour_id,
                state_id: energy.state_id,
                county_id: energy.county_id,
                zone_id: energy.zone_id,
                link_id: energy.link_id,
                source_process_id: energy.process_id,
                source_type_id: energy.source_type_id,
                fuel_type_id: energy.fuel_type_id,
                model_year_id: energy.model_year_id,
                road_type_id: energy.road_type_id,
            };
 *groups.entry(key).or_insert(0.0) += energy.emission_quant * rate;
        }

        groups
            .into_iter()
            .map(|(key, emission_quant)| WorkerOutputRow {
                year_id: key.year_id,
                month_id: key.month_id,
                day_id: key.day_id,
                hour_id: key.hour_id,
                state_id: key.state_id,
                county_id: key.county_id,
                zone_id: key.zone_id,
                link_id: key.link_id,
                pollutant_id: ATMOSPHERIC_CO2_POLLUTANT_ID,
                process_id: WELL_TO_PUMP_PROCESS_ID,
                source_type_id: key.source_type_id,
                fuel_type_id: key.fuel_type_id,
                model_year_id: key.model_year_id,
                road_type_id: key.road_type_id,
                emission_quant,
            })
            .collect()
    }
}

/// `CO2AtmosphericWTPCalculator` is a chained calculator/// `subscribes_directly: false` in `calculator-dag.json` — so it declares no
/// MasterLoop subscription.
static NO_SUBSCRIPTIONS: &[CalculatorSubscription] = &[];

/// `CO2AtmosphericWTPCalculator` registers nothing: the Well-To-Pump process
/// has no `Registration` directive in `CalculatorInfo.txt` and the chain DAG
/// records `registrations_count: 0`. See the module-level supersession note.
static NO_REGISTRATIONS: &[PollutantProcessAssociation] = &[];

/// Default-DB tables the well-to-pump atmospheric-CO2 computation consumes.
static INPUT_TABLES: &[&str] = &[
    "FuelFormulation",
    "FuelSubtype",
    "FuelSupply",
    "GREETWellToPump",
    "MOVESWorkerOutput",
    "MonthOfAnyYear",
    "Year",
];

impl Calculator for Co2AtmosphericWtpCalculator {
    fn name(&self) -> &'static str {
        Self::NAME
    }

 /// `CO2AtmosphericWTPCalculator` is a chained calculator: it does not
 /// subscribe to the MasterLoop directly. `calculator-dag.json` records
 /// `subscribes_directly: false` and an empty `subscriptions` list.
    fn subscriptions(&self) -> &[CalculatorSubscription] {
        NO_SUBSCRIPTIONS
    }

 /// Empty — `CO2AtmosphericWTPCalculator` is superseded by
 /// `BaseRateCalculator` and registers no `(pollutant, process)` pairs; see
 /// the module-level note.
    fn registrations(&self) -> &[PollutantProcessAssociation] {
        NO_REGISTRATIONS
    }

    fn input_tables(&self) -> &[&'static str] {
        INPUT_TABLES
    }

    fn execute(&self, ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        let tables = ctx.tables();
        let year: Vec<YearRow> = tables.iter_typed("Year")?;
        // `CO2AtmosphericWTPCalculator.sql` substitutes the MasterLoop
        // `##context.year##` directly into `gwtp.yearID = ##context.year##`
        // and `y.yearID = ##context.year##` with no documented fallback. An
        // absent context year is a configuration error, not a defaultable
        // condition: substituting `0` (or an arbitrary `Year`-table row)
        // would silently weight emissions on the wrong GREET rate and fuel
        // supply, or hide a missing-year data gap as an empty result.
        // Propagate instead. `IterationPosition` has no dedicated error
        // variant; reuse `RowExtraction` (its documented "value was null
        // where a non-null value is required" case) keyed to a synthetic
        // table, matching the sibling WTP calculators.
        let pos = ctx.position();
        let target_year = pos
            .time
            .year
            .map(i32::from)
            .ok_or_else(|| Error::RowExtraction {
                table: "IterationPosition".into(),
                row: pos.iteration as usize,
                column: "year".into(),
                message: "required run-context year is unresolved (None)".into(),
            })?;
        let inputs = WtpInputs {
            greet: tables.iter_typed::<GreetWellToPumpRow>("GREETWellToPump")?,
            fuel_supply: tables.iter_typed::<FuelSupplyRow>("FuelSupply")?,
            fuel_formulation: tables.iter_typed::<FuelFormulationRow>("FuelFormulation")?,
            fuel_sub_type: tables.iter_typed::<FuelSubTypeRow>("FuelSubtype")?,
            year,
            month_of_any_year: tables.iter_typed::<MonthGroupRow>("MonthOfAnyYear")?,
            worker_output: tables.iter_typed::<WorkerOutputRow>("MOVESWorkerOutput")?,
            target_year,
        };
        let rows = self.calculate(&inputs);
        crate::wiring::emit_rows(rows)
    }
}

/// Construct the calculator as a boxed trait object — matches the engine's
/// calculator-factory signature so the registry can register it.
#[must_use]
pub fn factory() -> Box<dyn Calculator> {
    Box::new(Co2AtmosphericWtpCalculator)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::calculators::welltopump::common::{
        FuelFormulationRow, FuelSubTypeRow, FuelSupplyRow, GreetWellToPumpRow, MonthGroupRow,
        YearRow,
    };

 /// Build a one-formulation / one-energy-row input. The CO2 factor is
 /// `5.0 × 1.0 = 5.0` (GREET rate × market share) and the single
 /// running-exhaust energy record is `200.0`, so the one output row is
 /// `200.0 × 5.0 = 1000.0`.
    fn minimal_inputs() -> WtpInputs {
        WtpInputs {
            greet: vec![GreetWellToPumpRow {
                pollutant_id: 90,
                fuel_sub_type_id: 21,
                year_id: 2020,
                emission_rate: 5.0,
            }],
            fuel_supply: vec![FuelSupplyRow {
                fuel_year_id: 2020,
                month_group_id: 1,
                fuel_formulation_id: 100,
                market_share: 1.0,
            }],
            fuel_formulation: vec![FuelFormulationRow {
                fuel_formulation_id: 100,
                fuel_sub_type_id: 21,
            }],
            fuel_sub_type: vec![FuelSubTypeRow {
                fuel_sub_type_id: 21,
                fuel_type_id: 2,
            }],
            year: vec![YearRow {
                year_id: 2020,
                fuel_year_id: 2020,
            }],
            month_of_any_year: vec![MonthGroupRow {
                month_id: 1,
                month_group_id: 1,
            }],
            worker_output: vec![WorkerOutputRow {
                year_id: 2020,
                month_id: 1,
                day_id: 5,
                hour_id: 8,
                state_id: 26,
                county_id: 26_161,
                zone_id: 261_610,
                link_id: 5001,
                pollutant_id: 91,
                process_id: 1,
                source_type_id: 21,
                fuel_type_id: 2,
                model_year_id: 2018,
                road_type_id: 4,
                emission_quant: 200.0,
            }],
            target_year: 2020,
        }
    }

    fn assert_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 1e-6,
            "{actual} != expected {expected}",
        );
    }

    #[test]
    fn calculate_minimal_input_yields_one_row() {
        let rows = Co2AtmosphericWtpCalculator.calculate(&minimal_inputs());
        assert_eq!(rows.len(), 1);
        let r = rows[0];
        assert_eq!(r.county_id, 26_161);
        assert_eq!(r.fuel_type_id, 2);
        assert_eq!(r.model_year_id, 2018);
 // Pollutant relabelled to atmospheric CO2; process stamped 99.
        assert_eq!(r.pollutant_id, 90);
        assert_eq!(r.process_id, 99);
 // 200.0 × (5.0 × 1.0).
        assert_close(r.emission_quant, 1_000.0);
    }

    #[test]
    fn calculate_keeps_source_processes_separate() {
 // Two energy records, running (1) and start (2), same dimension cell:
 // the GROUP BY includes processID, so they yield two distinct output
 // rows — both stamped process 99.
        let mut inputs = minimal_inputs();
        inputs.worker_output.push(WorkerOutputRow {
            process_id: 2,
            emission_quant: 100.0,
            ..inputs.worker_output[0]
        });
        let rows = Co2AtmosphericWtpCalculator.calculate(&inputs);
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|r| r.process_id == 99));
 // 200.0 × 5.0 and 100.0 × 5.0 — summed separately by source process.
        let mut quants: Vec<f64> = rows.iter().map(|r| r.emission_quant).collect();
        quants.sort_by(f64::total_cmp);
        assert_close(quants[0], 500.0);
        assert_close(quants[1], 1_000.0);
    }

    #[test]
    fn calculate_admits_only_running_start_extended_idle() {
 // Process 4 (brake wear, say) is not a running/start/extended-idle
 // exhaust process — its energy is not a CO2 WTP input.
        let mut inputs = minimal_inputs();
        inputs.worker_output[0].process_id = 4;
        assert!(Co2AtmosphericWtpCalculator.calculate(&inputs).is_empty());

 // Extended idle (90) is admitted.
        let mut idle = minimal_inputs();
        idle.worker_output[0].process_id = 90;
        assert_eq!(Co2AtmosphericWtpCalculator.calculate(&idle).len(), 1);
    }

    #[test]
    fn calculate_ignores_non_energy_rows() {
        let mut inputs = minimal_inputs();
        inputs.worker_output[0].pollutant_id = 2;
        assert!(Co2AtmosphericWtpCalculator.calculate(&inputs).is_empty());
    }

    #[test]
    fn calculate_does_not_interpolate_the_greet_rate() {
 // The GREET rate is tabulated only at 2018 and 2022, not at the run
 // year 2020 — the SQL joins gwtp.yearID = run year, so the fuel
 // subtype is dropped (no interpolation, unlike the other WTP factors).
        let mut inputs = minimal_inputs();
        inputs.greet = vec![
            GreetWellToPumpRow {
                pollutant_id: 90,
                fuel_sub_type_id: 21,
                year_id: 2018,
                emission_rate: 5.0,
            },
            GreetWellToPumpRow {
                pollutant_id: 90,
                fuel_sub_type_id: 21,
                year_id: 2022,
                emission_rate: 9.0,
            },
        ];
        assert!(Co2AtmosphericWtpCalculator.calculate(&inputs).is_empty());
    }

    #[test]
    fn calculate_drops_energy_without_a_month_group() {
        let mut inputs = minimal_inputs();
        inputs.month_of_any_year.clear();
        assert!(Co2AtmosphericWtpCalculator.calculate(&inputs).is_empty());
    }

    #[test]
    fn calculate_drops_energy_without_a_factor() {
        let mut inputs = minimal_inputs();
        inputs.fuel_supply.clear();
        assert!(Co2AtmosphericWtpCalculator.calculate(&inputs).is_empty());
    }

    #[test]
    fn calculate_weights_factor_by_market_share() {
 // Two formulations of one fuel type, shares 0.25 / 0.75, GREET rates
 // 4.0 / 8.0 → factor 0.25×4 + 0.75×8 = 7.0; energy 200 → 1400.
        let mut inputs = minimal_inputs();
        inputs.greet = vec![
            GreetWellToPumpRow {
                pollutant_id: 90,
                fuel_sub_type_id: 21,
                year_id: 2020,
                emission_rate: 4.0,
            },
            GreetWellToPumpRow {
                pollutant_id: 90,
                fuel_sub_type_id: 22,
                year_id: 2020,
                emission_rate: 8.0,
            },
        ];
        inputs.fuel_supply = vec![
            FuelSupplyRow {
                fuel_year_id: 2020,
                month_group_id: 1,
                fuel_formulation_id: 100,
                market_share: 0.25,
            },
            FuelSupplyRow {
                fuel_year_id: 2020,
                month_group_id: 1,
                fuel_formulation_id: 101,
                market_share: 0.75,
            },
        ];
        inputs.fuel_formulation = vec![
            FuelFormulationRow {
                fuel_formulation_id: 100,
                fuel_sub_type_id: 21,
            },
            FuelFormulationRow {
                fuel_formulation_id: 101,
                fuel_sub_type_id: 22,
            },
        ];
        inputs.fuel_sub_type = vec![
            FuelSubTypeRow {
                fuel_sub_type_id: 21,
                fuel_type_id: 2,
            },
            FuelSubTypeRow {
                fuel_sub_type_id: 22,
                fuel_type_id: 2,
            },
        ];
        let rows = Co2AtmosphericWtpCalculator.calculate(&inputs);
        assert_eq!(rows.len(), 1);
        assert_close(rows[0].emission_quant, 1_400.0);
    }

    #[test]
    fn calculate_empty_input_yields_no_rows() {
        assert!(Co2AtmosphericWtpCalculator
            .calculate(&WtpInputs::default())
            .is_empty());
    }

    #[test]
    fn calculator_name_matches_dag_module() {
        assert_eq!(
            Co2AtmosphericWtpCalculator.name(),
            "CO2AtmosphericWTPCalculator"
        );
        assert_eq!(
            Co2AtmosphericWtpCalculator::NAME,
            "CO2AtmosphericWTPCalculator"
        );
    }

    #[test]
    fn calculator_is_chained_with_no_subscriptions() {
        assert!(Co2AtmosphericWtpCalculator.subscriptions().is_empty());
    }

    #[test]
    fn registrations_are_empty_because_the_process_is_unwired() {
        assert!(Co2AtmosphericWtpCalculator.registrations().is_empty());
    }

    #[test]
    fn upstream_is_empty() {
        assert!(Co2AtmosphericWtpCalculator.upstream().is_empty());
    }

    #[test]
    fn execute_returns_nonempty_dataframe_for_minimal_inputs() {
        use moves_framework::execution::execution_db::{ExecutionTime, IterationPosition};
        use moves_framework::{DataFrameStore, InMemoryStore, TableRow};
        let inputs = minimal_inputs();
        let mut store = InMemoryStore::new();
        store.insert(
            "GREETWellToPump",
            GreetWellToPumpRow::into_dataframe(inputs.greet).unwrap(),
        );
        store.insert(
            "FuelSupply",
            FuelSupplyRow::into_dataframe(inputs.fuel_supply).unwrap(),
        );
        store.insert(
            "FuelFormulation",
            FuelFormulationRow::into_dataframe(inputs.fuel_formulation).unwrap(),
        );
        store.insert(
            "FuelSubtype",
            FuelSubTypeRow::into_dataframe(inputs.fuel_sub_type).unwrap(),
        );
        store.insert("Year", YearRow::into_dataframe(inputs.year).unwrap());
        store.insert(
            "MonthOfAnyYear",
            MonthGroupRow::into_dataframe(inputs.month_of_any_year).unwrap(),
        );
        store.insert(
            "MOVESWorkerOutput",
            WorkerOutputRow::into_dataframe(inputs.worker_output).unwrap(),
        );
        // `execute` requires the MasterLoop `##context.year##`; supply it
        // through the position (the default `with_tables` context leaves it
        // `None`, which `execute` now rejects).
        let pos = IterationPosition {
            time: ExecutionTime::year(2020),
            ..IterationPosition::default()
        };
        let ctx = CalculatorContext::with_position_and_tables(pos, store);
        let out = Co2AtmosphericWtpCalculator
            .execute(&ctx)
            .expect("execute ok");
        let df = out.dataframe().expect("output should contain a DataFrame");
        assert!(df.height() > 0, "minimal inputs produce at least one row");
    }

    #[test]
    fn factory_builds_a_named_calculator() {
        assert_eq!(factory().name(), "CO2AtmosphericWTPCalculator");
    }

    #[test]
    fn calculator_is_object_safe() {
        let calc: Box<dyn Calculator> = Box::new(Co2AtmosphericWtpCalculator);
        assert_eq!(calc.name(), "CO2AtmosphericWTPCalculator");
    }
}
