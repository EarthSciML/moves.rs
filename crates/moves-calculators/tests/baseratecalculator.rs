//! Characterization tests for the Base Rate Calculator.
//!
//! Each test builds a small input scenario whose expected output is
//! hand-derived from the Go `baseratecalculator.go` algorithm, then drives
//! the whole `BaseRateCalculator::run` pipeline and asserts on the result.
//! Input values are chosen to be exactly representable in `f64` (sums and
//! products of small dyadic rationals), so the arithmetic assertions are
//! exact rather than tolerance-based.
//!
//! These tests pin the port against the Go reference as traced from source.
//! End-to-end validation against canonical-MOVES intermediate captures is a
//! separate downstream task (`moves-rust-.md`).

use moves_calculators::calculators::baseratecalculator::setup::{
    AgeCategoryRow, BaseRateCalculatorInputs, BaseRateRow, CriteriaRatioRow,
    EmissionRateAdjustmentRow, EvEfficiencyRow, FuelFormulationRow, FuelSupplyRow,
    GeneralFuelRatioRow, ImCoverageRow, ImFactorRow, ModelYearFuelFractionRow,
    PollutantProcessMappedModelYearRow, SmfrSbdSummaryRow, StartTempAdjustmentRow,
    TemperatureAdjustmentRow, UniversalActivityRow, ZoneMonthHourRow,
};
use moves_calculators::calculators::baseratecalculator::{
    BaseRateCalculator, ModuleFlags, RunConstants,
};

/// State 1 / county 1 / zone 1 / link 0 / year 2020 / month 7.
fn constants() -> RunConstants {
    RunConstants {
        state_id: 1,
        county_id: 1,
        zone_id: 1,
        link_id: 0,
        year_id: 2020,
        month_id: 7,
    }
}

/// One fuel-supply cell holding a single formulation at full market share.
fn fuel_supply_one() -> Vec<FuelSupplyRow> {
    vec![FuelSupplyRow {
        county_id: 1,
        year_id: 2020,
        month_id: 7,
        fuel_type_id: 1,
        fuel_sub_type_id: 10,
        fuel_formulation_id: 100,
        market_share: 1.0,
    }]
}

/// A base-rate row with the common fields filled. Source type 21, road type
/// 5, hour/day 85 (hour 8), model year 2018, fuel type 1, reg class 10,
/// operating mode 0. The I/M and AC-adjusted rate fields default to the
/// non-adjusted value / zero; tests override what they exercise.
fn base_rate_row(pollutant: i32, process: i32, mean: f64, rate: f64) -> BaseRateRow {
    BaseRateRow {
        source_type_id: 21,
        road_type_id: 5,
        avg_speed_bin_id: 8,
        hour_day_id: 85,
        pollutant_id: pollutant,
        process_id: process,
        model_year_id: 2018,
        fuel_type_id: 1,
        reg_class_id: 10,
        op_mode_id: 0,
        mean_base_rate: mean,
        mean_base_rate_im: mean,
        emission_rate: rate,
        emission_rate_im: rate,
        mean_base_rate_ac_adj: 0.0,
        mean_base_rate_im_ac_adj: 0.0,
        emission_rate_ac_adj: 0.0,
        emission_rate_im_ac_adj: 0.0,
        op_mode_fraction: 0.0,
        op_mode_fraction_rate: 0.0,
    }
}

#[test]
fn single_row_passes_through_unchanged_when_no_tables_apply() {
 // No lookup tables: every adjustment is skipped, aggregate_op_modes just
 // sums the one base rate by market share.
    let inputs = BaseRateCalculatorInputs {
        base_rate: vec![base_rate_row(2, 1, 4.0, 8.0)],
        fuel_supply: fuel_supply_one(),
        ..BaseRateCalculatorInputs::default()
    };
    let output = BaseRateCalculator::run(inputs, &constants(), &ModuleFlags::default());
    assert_eq!(output.blocks.len(), 1);
    let block = &output.blocks[0];
    assert_eq!(block.key.process_id, 1);
    assert_eq!(block.key.pol_process_id, 201);
    assert_eq!(block.key.age_id, 2); // 2020 - 2018
    assert!(block.op_mode.is_none()); // collapsed by aggregate_op_modes
    assert_eq!(block.emissions.len(), 1);
    assert_eq!(block.emissions[0].emission_quant, 4.0);
    assert_eq!(block.emissions[0].emission_rate, 8.0);
}

#[test]
fn age_based_pass_is_processed_like_the_non_age_pass() {
 // A row in `base_rate_by_age` flows through the same pipeline.
    let inputs = BaseRateCalculatorInputs {
        base_rate_by_age: vec![base_rate_row(2, 1, 4.0, 8.0)],
        fuel_supply: fuel_supply_one(),
        ..BaseRateCalculatorInputs::default()
    };
    let output = BaseRateCalculator::run(inputs, &constants(), &ModuleFlags::default());
    assert_eq!(output.rows().len(), 1);
    assert_eq!(output.rows()[0].emission_quant, 4.0);
}

#[test]
fn row_without_matching_fuel_supply_is_dropped() {
 // No fuel supply -> the row expands to zero base rates and is dropped.
    let inputs = BaseRateCalculatorInputs {
        base_rate: vec![base_rate_row(2, 1, 4.0, 8.0)],
        ..BaseRateCalculatorInputs::default()
    };
    let output = BaseRateCalculator::run(inputs, &constants(), &ModuleFlags::default());
    assert!(output.blocks.is_empty());
}

#[test]
fn two_fuel_formulations_expand_to_two_emissions() {
    let inputs = BaseRateCalculatorInputs {
        base_rate: vec![base_rate_row(2, 1, 4.0, 8.0)],
        fuel_supply: vec![
            FuelSupplyRow {
                county_id: 1,
                year_id: 2020,
                month_id: 7,
                fuel_type_id: 1,
                fuel_sub_type_id: 10,
                fuel_formulation_id: 100,
                market_share: 0.75,
            },
            FuelSupplyRow {
                county_id: 1,
                year_id: 2020,
                month_id: 7,
                fuel_type_id: 1,
                fuel_sub_type_id: 11,
                fuel_formulation_id: 101,
                market_share: 0.25,
            },
        ],
        ..BaseRateCalculatorInputs::default()
    };
    let output = BaseRateCalculator::run(inputs, &constants(), &ModuleFlags::default());
    let rows = output.rows();
    assert_eq!(rows.len(), 2);
 // emissions are ordered by fuel formulation id.
    assert_eq!(rows[0].fuel_formulation_id, 100);
    assert_eq!(rows[0].emission_quant, 3.0); // 4 * 0.75
    assert_eq!(rows[1].fuel_formulation_id, 101);
    assert_eq!(rows[1].emission_quant, 1.0); // 4 * 0.25
}

#[test]
fn rows_sharing_a_key_accumulate_before_aggregation() {
 // Two rows differing only in operating mode (not part of the block key)
 // accumulate into one block; aggregate_op_modes then sums both.
    let mut row_a = base_rate_row(2, 1, 4.0, 8.0);
    row_a.op_mode_id = 0;
    let mut row_b = base_rate_row(2, 1, 4.0, 8.0);
    row_b.op_mode_id = 1;
    let inputs = BaseRateCalculatorInputs {
        base_rate: vec![row_a, row_b],
        fuel_supply: fuel_supply_one(),
        ..BaseRateCalculatorInputs::default()
    };
    let output = BaseRateCalculator::run(inputs, &constants(), &ModuleFlags::default());
    assert_eq!(output.blocks.len(), 1);
    assert_eq!(output.blocks[0].emissions[0].emission_quant, 8.0); // 4 + 4
    assert_eq!(output.blocks[0].emissions[0].emission_rate, 16.0); // 8 + 8
}

#[test]
fn general_fuel_ratio_blends_normal_and_gpa_by_county_fraction() {
 // r = ratio + gpaFract*(ratioGPA - ratio) = 2 + 0.5*(4 - 2) = 3.
    let inputs = BaseRateCalculatorInputs {
        base_rate: vec![base_rate_row(2, 1, 4.0, 8.0)],
        fuel_supply: fuel_supply_one(),
        county: vec![
            moves_calculators::calculators::baseratecalculator::setup::CountyRow {
                county_id: 1,
                gpa_fract: 0.5,
                barometric_pressure: 0.0,
            },
        ],
        general_fuel_ratio: vec![GeneralFuelRatioRow {
            fuel_formulation_id: 100,
            pol_process_id: 201,
            source_type_id: 21,
            min_model_year_id: 2000,
            max_model_year_id: 2020,
            min_age_id: 0,
            max_age_id: 40,
            fuel_effect_ratio: 2.0,
            fuel_effect_ratio_gpa: 4.0,
        }],
        ..BaseRateCalculatorInputs::default()
    };
    let output = BaseRateCalculator::run(inputs, &constants(), &ModuleFlags::default());
    assert_eq!(output.rows()[0].emission_quant, 12.0); // 4 * 3
    assert_eq!(output.rows()[0].emission_rate, 24.0); // 8 * 3
}

#[test]
fn general_fuel_ratio_outside_its_year_range_does_not_apply() {
 // model year 2018 is outside [2000, 2010]; no scaling.
    let inputs = BaseRateCalculatorInputs {
        base_rate: vec![base_rate_row(2, 1, 4.0, 8.0)],
        fuel_supply: fuel_supply_one(),
        general_fuel_ratio: vec![GeneralFuelRatioRow {
            fuel_formulation_id: 100,
            pol_process_id: 201,
            source_type_id: 21,
            min_model_year_id: 2000,
            max_model_year_id: 2010,
            min_age_id: 0,
            max_age_id: 40,
            fuel_effect_ratio: 2.0,
            fuel_effect_ratio_gpa: 2.0,
        }],
        ..BaseRateCalculatorInputs::default()
    };
    let output = BaseRateCalculator::run(inputs, &constants(), &ModuleFlags::default());
    assert_eq!(output.rows()[0].emission_quant, 4.0); // unchanged
}

#[test]
fn criteria_ratio_scales_running_exhaust() {
    let inputs = BaseRateCalculatorInputs {
        base_rate: vec![base_rate_row(2, 1, 4.0, 8.0)],
        fuel_supply: fuel_supply_one(),
        criteria_ratio: vec![CriteriaRatioRow {
            fuel_formulation_id: 100,
            pol_process_id: 201,
            source_type_id: 21,
            model_year_id: 2018,
            age_id: 2,
            ratio: 1.25,
            ratio_gpa: 1.25,
            ratio_no_sulfur: 0.0,
        }],
        ..BaseRateCalculatorInputs::default()
    };
    let output = BaseRateCalculator::run(inputs, &constants(), &ModuleFlags::default());
    assert_eq!(output.rows()[0].emission_quant, 5.0); // 4 * 1.25
    assert_eq!(output.rows()[0].emission_rate, 10.0); // 8 * 1.25
}

#[test]
fn im_coverage_blends_the_im_and_non_im_rates() {
 // IM coverage = imFactor * (0.01 * complianceFactor) = 0.5 * 0.5 = 0.25.
 // meanBaseRate = (1 - 0.25)*4 + 0.25*8 = 5; emissionRate = 0.75*8 + 0.25*16 = 10.
    let mut row = base_rate_row(2, 1, 4.0, 8.0);
    row.mean_base_rate_im = 8.0;
    row.emission_rate_im = 16.0;
    let inputs = BaseRateCalculatorInputs {
        base_rate: vec![row],
        fuel_supply: fuel_supply_one(),
        pollutant_process_mapped_model_year: vec![PollutantProcessMappedModelYearRow {
            pol_process_id: 201,
            model_year_id: 2018,
            model_year_group_id: 0,
            fuel_my_group_id: 0,
            im_model_year_group_id: 7,
        }],
        age_category: vec![AgeCategoryRow {
            age_id: 2,
            age_group_id: 4,
        }],
        im_factor: vec![ImFactorRow {
            pol_process_id: 201,
            inspect_freq: 1,
            test_standards_id: 2,
            source_type_id: 21,
            fuel_type_id: 1,
            im_model_year_group_id: 7,
            age_group_id: 4,
            im_factor: 0.5,
        }],
        im_coverage: vec![ImCoverageRow {
            pol_process_id: 201,
            source_type_id: 21,
            fuel_type_id: 1,
            beg_model_year_id: 2018,
            end_model_year_id: 2018,
            inspect_freq: 1,
            test_standards_id: 2,
            compliance_factor: 50.0,
        }],
        ..BaseRateCalculatorInputs::default()
    };
    let output = BaseRateCalculator::run(inputs, &constants(), &ModuleFlags::default());
    assert_eq!(output.rows()[0].emission_quant, 5.0);
    assert_eq!(output.rows()[0].emission_rate, 10.0);
}

#[test]
fn air_conditioning_adds_the_ac_adjusted_rate() {
 // process 1 (not start exhaust): rate += acFactor * acAdj.
    let mut row = base_rate_row(2, 1, 4.0, 8.0);
    row.mean_base_rate_ac_adj = 2.0;
    row.emission_rate_ac_adj = 1.0;
    let inputs = BaseRateCalculatorInputs {
        base_rate: vec![row],
        fuel_supply: fuel_supply_one(),
        zone_ac_factor: vec![
            moves_calculators::calculators::baseratecalculator::setup::ZoneAcFactorRow {
                hour_id: 8,
                source_type_id: 21,
                model_year_id: 2018,
                ac_factor: 0.5,
            },
        ],
        ..BaseRateCalculatorInputs::default()
    };
    let output = BaseRateCalculator::run(inputs, &constants(), &ModuleFlags::default());
    assert_eq!(output.rows()[0].emission_quant, 5.0); // 4 + 0.5*2
    assert_eq!(output.rows()[0].emission_rate, 8.5); // 8 + 0.5*1
}

#[test]
fn extended_idle_scales_mean_rates_but_not_emission_rates() {
 // process 90: the four mean-base-rate fields scale by the opMode-200
 // fraction; emission rates are untouched.
    let inputs = BaseRateCalculatorInputs {
        base_rate: vec![base_rate_row(2, 90, 4.0, 8.0)],
        fuel_supply: fuel_supply_one(),
        extended_idle_emission_rate_fraction: vec![ModelYearFuelFractionRow {
            model_year_id: 2018,
            fuel_type_id: 1,
            hour_fraction_adjust: 0.5,
        }],
        ..BaseRateCalculatorInputs::default()
    };
    let output = BaseRateCalculator::run(inputs, &constants(), &ModuleFlags::default());
    assert_eq!(output.rows()[0].emission_quant, 2.0); // 4 * 0.5
    assert_eq!(output.rows()[0].emission_rate, 8.0); // emission rate untouched
}

#[test]
fn apu_scales_mean_rates_for_operating_mode_201() {
 // process 91, operating mode 201: the four mean-base-rate fields scale
 // by the opMode-201 fraction; the process is not retagged.
    let mut row = base_rate_row(91, 91, 4.0, 8.0);
    row.op_mode_id = 201;
    let inputs = BaseRateCalculatorInputs {
        base_rate: vec![row],
        fuel_supply: fuel_supply_one(),
        apu_emission_rate_fraction: vec![ModelYearFuelFractionRow {
            model_year_id: 2018,
            fuel_type_id: 1,
            hour_fraction_adjust: 0.5,
        }],
        ..BaseRateCalculatorInputs::default()
    };
    let output = BaseRateCalculator::run(inputs, &constants(), &ModuleFlags::default());
    assert_eq!(output.blocks.len(), 1);
    assert_eq!(output.blocks[0].key.process_id, 91); // not retagged
    assert_eq!(output.rows()[0].emission_quant, 2.0); // 4 * 0.5
    assert_eq!(output.rows()[0].emission_rate, 8.0); // emission rate untouched
}

#[test]
fn shorepower_retags_the_process_to_93() {
 // process 91, operating mode 203: process is retagged 91 -> 93, but
 // pol_process_id is deliberately not recomputed (stays 9191).
    let mut row = base_rate_row(91, 91, 4.0, 8.0);
    row.op_mode_id = 203;
    let inputs = BaseRateCalculatorInputs {
        base_rate: vec![row],
        fuel_supply: fuel_supply_one(),
        shorepower_emission_rate_fraction: vec![ModelYearFuelFractionRow {
            model_year_id: 2018,
            fuel_type_id: 1,
            hour_fraction_adjust: 0.5,
        }],
        ..BaseRateCalculatorInputs::default()
    };
    let output = BaseRateCalculator::run(inputs, &constants(), &ModuleFlags::default());
    assert_eq!(output.blocks.len(), 1);
    assert_eq!(output.blocks[0].key.process_id, 93);
    assert_eq!(output.blocks[0].key.pol_process_id, 9191); // stale, by design
    assert_eq!(output.rows()[0].emission_quant, 2.0); // 4 * 0.5
}

#[test]
fn emission_rate_adjustment_scales_mean_and_emission_rate() {
    let flags = ModuleFlags {
        emission_rate_adjustment: true,
        ..ModuleFlags::default()
    };
    let inputs = BaseRateCalculatorInputs {
        base_rate: vec![base_rate_row(2, 1, 4.0, 8.0)],
        fuel_supply: fuel_supply_one(),
        emission_rate_adjustment: vec![EmissionRateAdjustmentRow {
            pol_process_id: 201,
            source_type_id: 21,
            reg_class_id: 10,
            fuel_type_id: 1,
            begin_model_year_id: 2018,
            end_model_year_id: 2018,
            emission_rate_adjustment: 0.5,
        }],
        ..BaseRateCalculatorInputs::default()
    };
    let output = BaseRateCalculator::run(inputs, &constants(), &flags);
    assert_eq!(output.rows()[0].emission_quant, 2.0); // 4 * 0.5
    assert_eq!(output.rows()[0].emission_rate, 4.0); // 8 * 0.5
}

#[test]
fn ev_efficiency_divides_through_the_efficiency_product() {
 // divisor = battery * charging = 0.5 * 0.5 = 0.25.
 //
 // EVEfficiency only applies to electricity (fuelTypeID 9), and the raw
 // row is expanded over its [beginModelYearID, endModelYearID] range,
 // keeping only model years whose age (`year - modelYearID`) maps to the
 // row's ageGroupID via AgeCategory. Here year 2020, modelYear 2018 ->
 // ageID 2 -> ageGroupID 3, so the row covers the base rate's 2018 bin.
    let flags = ModuleFlags {
        ev_efficiency: true,
        ..ModuleFlags::default()
    };
    let mut ev_base = base_rate_row(2, 1, 4.0, 8.0);
    ev_base.fuel_type_id = 9;
    let inputs = BaseRateCalculatorInputs {
        base_rate: vec![ev_base],
        fuel_supply: vec![FuelSupplyRow {
            county_id: 1,
            year_id: 2020,
            month_id: 7,
            fuel_type_id: 9,
            fuel_sub_type_id: 90,
            fuel_formulation_id: 0,
            market_share: 1.0,
        }],
        age_category: vec![AgeCategoryRow {
            age_id: 2,
            age_group_id: 3,
        }],
        ev_efficiency: vec![EvEfficiencyRow {
            pol_process_id: 201,
            source_type_id: 21,
            reg_class_id: 10,
            age_group_id: 3,
            begin_model_year_id: 2018,
            end_model_year_id: 2018,
            battery_efficiency: 0.5,
            charging_efficiency: 0.5,
        }],
        ..BaseRateCalculatorInputs::default()
    };
    let output = BaseRateCalculator::run(inputs, &constants(), &flags);
    assert_eq!(output.rows()[0].emission_quant, 16.0); // 4 / 0.25
    assert_eq!(output.rows()[0].emission_rate, 32.0); // 8 / 0.25
}

#[test]
fn temperature_adjustment_applies_the_standard_quadratic_term() {
 // CO (pollutant 2), process 1: standard form
 // factor = 1 + (temp-75)*(A + (temp-75)*B). temp 77, A 0.5, B 0 -> 2.0.
    let inputs = BaseRateCalculatorInputs {
        base_rate: vec![base_rate_row(2, 1, 4.0, 8.0)],
        fuel_supply: fuel_supply_one(),
        fuel_types: vec![1],
        temperature_adjustment: vec![TemperatureAdjustmentRow {
            pol_process_id: 201,
            fuel_type_id: 1,
            reg_class_id: 10,
            min_model_year_id: 2018,
            max_model_year_id: 2018,
            term_a: 0.5,
            term_b: 0.0,
            term_c: Some(0.0),
        }],
        zone_month_hour: vec![ZoneMonthHourRow {
            month_id: 7,
            zone_id: 1,
            hour_id: 8,
            temperature: 77.0,
            rel_humidity: 0.0,
            heat_index: 0.0,
            specific_humidity: 0.0,
            mol_water_fraction: 0.0,
        }],
        ..BaseRateCalculatorInputs::default()
    };
    let output = BaseRateCalculator::run(inputs, &constants(), &ModuleFlags::default());
    assert_eq!(output.rows()[0].emission_quant, 8.0); // 4 * 2
    assert_eq!(output.rows()[0].emission_rate, 16.0); // 8 * 2
}

#[test]
fn start_temperature_adjustment_applies_the_polynomial_form() {
 // process 2, pollutant 1, polProcessID 102. POLY form:
 // rate = base + weight * d * (A + d*(B + d*C)), d = least(temp,75) - 75.
 // temp 71 -> d = -4; A 1, B 0, C 0; weight 0.5 -> base + 0.5*(-4)*1 = base - 2.
    let mut row = base_rate_row(1, 2, 4.0, 8.0);
    row.op_mode_fraction = 0.5;
    row.op_mode_fraction_rate = 0.5;
    let inputs = BaseRateCalculatorInputs {
        base_rate: vec![row],
        fuel_supply: fuel_supply_one(),
        fuel_types: vec![1],
        pollutant_process_mapped_model_year: vec![PollutantProcessMappedModelYearRow {
            pol_process_id: 102,
            model_year_id: 2018,
            model_year_group_id: 5,
            fuel_my_group_id: 0,
            im_model_year_group_id: 0,
        }],
        start_temp_adjustment: vec![StartTempAdjustmentRow {
            fuel_type_id: 1,
            pol_process_id: 102,
            model_year_group_id: 5,
            op_mode_id: 0,
            term_a: 1.0,
            term_b: 0.0,
            term_c: 0.0,
            equation_type: "POLY".to_string(),
        }],
        zone_month_hour: vec![ZoneMonthHourRow {
            month_id: 7,
            zone_id: 1,
            hour_id: 8,
            temperature: 71.0,
            rel_humidity: 0.0,
            heat_index: 0.0,
            specific_humidity: 0.0,
            mol_water_fraction: 0.0,
        }],
        ..BaseRateCalculatorInputs::default()
    };
    let output = BaseRateCalculator::run(inputs, &constants(), &ModuleFlags::default());
    assert_eq!(output.rows()[0].emission_quant, 2.0); // 4 - 2
    assert_eq!(output.rows()[0].emission_rate, 6.0); // 8 - 2
}

#[test]
fn e85_thc_emits_a_10000_offset_pollutant() {
 // E85 formulation (subtype 51), model year >= 2001, criteria + alt
 // criteria ratios present. The criteria step scales the base block by
 // 2.0; the E85 block then scales a copy by altRatio/ratio = 6/2 = 3.
    let cr_key = (100, 201, 21, 2018, 2);
    let inputs = BaseRateCalculatorInputs {
        base_rate: vec![base_rate_row(2, 1, 4.0, 8.0)],
        fuel_supply: fuel_supply_one(),
        fuel_formulations: vec![FuelFormulationRow {
            fuel_formulation_id: 100,
            fuel_sub_type_id: 51,
        }],
        criteria_ratio: vec![CriteriaRatioRow {
            fuel_formulation_id: cr_key.0,
            pol_process_id: cr_key.1,
            source_type_id: cr_key.2,
            model_year_id: cr_key.3,
            age_id: cr_key.4,
            ratio: 2.0,
            ratio_gpa: 2.0,
            ratio_no_sulfur: 0.0,
        }],
        alt_criteria_ratio: vec![CriteriaRatioRow {
            fuel_formulation_id: cr_key.0,
            pol_process_id: cr_key.1,
            source_type_id: cr_key.2,
            model_year_id: cr_key.3,
            age_id: cr_key.4,
            ratio: 6.0,
            ratio_gpa: 6.0,
            ratio_no_sulfur: 0.0,
        }],
        ..BaseRateCalculatorInputs::default()
    };
    let output = BaseRateCalculator::run(inputs, &constants(), &ModuleFlags::default());
    assert_eq!(output.blocks.len(), 2);
 // Block 0: the base pollutant 2, criteria-scaled by 2.
    assert_eq!(output.blocks[0].key.pollutant_id, 2);
    assert_eq!(output.blocks[0].emissions[0].emission_quant, 8.0); // 4 * 2
 // Block 1: pollutant 10002, scaled again by 3 (alt/criteria ratio).
    assert_eq!(output.blocks[1].key.pollutant_id, 10002);
    assert_eq!(output.blocks[1].key.pol_process_id, 1_000_201);
    assert_eq!(output.blocks[1].emissions[0].emission_quant, 24.0); // 8 * 3
    assert_eq!(output.blocks[1].emissions[0].emission_rate, 48.0); // 16 * 3
}

#[test]
fn apply_activity_converts_a_rate_into_an_inventory() {
 // BRC_ApplyActivity: emission quantity scales by universal activity.
    let flags = ModuleFlags {
        apply_activity: true,
        ..ModuleFlags::default()
    };
    let inputs = BaseRateCalculatorInputs {
        base_rate: vec![base_rate_row(2, 1, 4.0, 8.0)],
        fuel_supply: fuel_supply_one(),
        universal_activity: vec![UniversalActivityRow {
            hour_day_id: 85,
            model_year_id: 2018,
            source_type_id: 21,
            activity: 3.0,
        }],
        ..BaseRateCalculatorInputs::default()
    };
    let output = BaseRateCalculator::run(inputs, &constants(), &flags);
    assert_eq!(output.rows()[0].emission_quant, 12.0); // 4 * 3
    assert_eq!(output.rows()[0].emission_rate, 8.0); // emission rate untouched
}

#[test]
fn aggregate_smfr_weights_emissions_by_the_activity_distribution() {
 // Two reg classes contribute activity 1 and 3; with regClassID discarded
 // they share a total of 4, so reg class 10 normalises to 1/4 = 0.25.
    let flags = ModuleFlags {
        aggregate_smfr: true,
        adjust_mean_base_rate_and_emission_rate: true,
        discard_reg_class_id: true,
        ..ModuleFlags::default()
    };
    let inputs = BaseRateCalculatorInputs {
        base_rate: vec![base_rate_row(2, 1, 4.0, 8.0)], // reg class 10
        fuel_supply: fuel_supply_one(),
        universal_activity: vec![UniversalActivityRow {
            hour_day_id: 85,
            model_year_id: 2018,
            source_type_id: 21,
            activity: 1.0,
        }],
        smfr_sbd_summary: vec![
            SmfrSbdSummaryRow {
                source_type_id: 21,
                model_year_id: 2018,
                fuel_type_id: 1,
                reg_class_id: 10,
                sbd_total: 1.0,
            },
            SmfrSbdSummaryRow {
                source_type_id: 21,
                model_year_id: 2018,
                fuel_type_id: 1,
                reg_class_id: 20,
                sbd_total: 3.0,
            },
        ],
        ..BaseRateCalculatorInputs::default()
    };
    let output = BaseRateCalculator::run(inputs, &constants(), &flags);
    assert_eq!(output.rows()[0].emission_quant, 1.0); // 4 * 0.25
    assert_eq!(output.rows()[0].emission_rate, 2.0); // 8 * 0.25
}
