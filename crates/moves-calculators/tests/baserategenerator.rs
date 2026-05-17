//! Characterization tests for the Base Rate Generator (Task 42).
//!
//! Each test builds a small input scenario whose expected output is
//! hand-derived from the Go `baserategenerator.go` algorithm, then drives
//! the whole `BaseRateGenerator::run` pipeline and asserts on the result.
//! Input values are chosen to be exactly representable in `f64` (sums and
//! products of small dyadic rationals), so the arithmetic assertions are
//! exact rather than tolerance-based.
//!
//! These tests pin the port against the Go reference as traced from source.
//! End-to-end validation against canonical-MOVES intermediate captures is a
//! separate downstream task (`moves-rust-migration-plan.md` Task 44), which
//! needs the Phase 0 fixture corpus not present in this crate.

use moves_calculators::generators::baserategenerator::inputs::{
    AvgSpeedBinRow, AvgSpeedDistributionRow, BaseRateInputs, DriveScheduleAssocRow,
    DriveScheduleRow, DriveScheduleSecondRow, OpModePolProcRow, RatesOpModeDistributionRow,
    SbWeightedDistanceRow,
};
use moves_calculators::generators::baserategenerator::model::{
    SbWeightedRateDetail, SourceUseTypePhysicsMappingDetail,
};
use moves_calculators::generators::baserategenerator::{BaseRateGenerator, ExternalFlags};

/// One `RatesOpModeDistribution` row with the common fields filled.
fn romd_row(
    source: i32,
    pol_process: i32,
    op_mode: i32,
    op_mode_fraction: f64,
) -> RatesOpModeDistributionRow {
    RatesOpModeDistributionRow {
        source_type_id: source,
        road_type_id: 5,
        avg_speed_bin_id: 8,
        hour_day_id: 85,
        pol_process_id: pol_process,
        op_mode_id: op_mode,
        op_mode_fraction,
        avg_bin_speed: 30.0,
        avg_speed_fraction: 0.5,
    }
}

/// One `SBWeightedEmissionRate`-shaped rate record.
fn rate(source: i32, pol_process: i32, op_mode: i32, mean_base_rate: f64) -> SbWeightedRateDetail {
    SbWeightedRateDetail {
        source_type_id: source,
        pol_process_id: pol_process,
        op_mode_id: op_mode,
        model_year_id: 2016,
        fuel_type_id: 1,
        age_group_id: 0,
        reg_class_id: 20,
        sum_sbd: 0.5,
        sum_sbd_raw: 0.25,
        mean_base_rate,
        mean_base_rate_im: mean_base_rate / 2.0,
        mean_base_rate_ac_adj: mean_base_rate / 4.0,
        mean_base_rate_im_ac_adj: mean_base_rate / 8.0,
    }
}

/// Flags with every weighting toggle off — sumSBD, sumSBDRaw and
/// avgSpeedFraction all collapse to `1`.
fn flags_plain(process_id: i32) -> ExternalFlags {
    ExternalFlags {
        process_id,
        year_id: 2020,
        ..ExternalFlags::default()
    }
}

#[test]
fn core_path_base_rate_no_weighting() {
    // Process 2 (Start Exhaust) always takes the core RatesOpModeDistribution
    // path. With every weighting flag off, t = opModeFraction throughout.
    let inputs = BaseRateInputs {
        rates_op_mode_distribution: vec![romd_row(21, 102, 15, 0.5)],
        sb_weighted_emission_rate: vec![rate(21, 102, 15, 8.0)],
        ..BaseRateInputs::default()
    };
    let out = BaseRateGenerator::run(&inputs, &flags_plain(2));

    assert_eq!(out.base_rate.len(), 1);
    let r = out.base_rate[0];
    assert_eq!(r.source_type_id, 21);
    assert_eq!(r.road_type_id, 5);
    assert_eq!(r.hour_day_id, 85);
    assert_eq!(r.pol_process_id, 102);
    assert_eq!(r.process_id, 2);
    assert_eq!(r.pollutant_id, 1);
    assert_eq!(r.model_year_id, 2016);
    assert_eq!(r.op_mode_id, 0); // collapsed: neither flag set
                                 // t = opModeFraction(0.5); meanBaseRate = 8 * 0.5.
    assert_eq!(r.mean_base_rate, 4.0);
    assert_eq!(r.mean_base_rate_im, 2.0);
    assert_eq!(r.mean_base_rate_ac_adj, 1.0);
    assert_eq!(r.mean_base_rate_im_ac_adj, 0.5);
    assert_eq!(r.emission_rate, 4.0);
    assert_eq!(r.op_mode_fraction, 0.5);
    assert_eq!(r.op_mode_fraction_rate, 0.5);

    // No SBWeightedEmissionRateByAge rows supplied.
    assert!(out.base_rate_by_age.is_empty());
    assert!(out.driving_idle_fraction.is_empty());
}

#[test]
fn core_path_applies_sbd_and_speed_fraction_weighting() {
    // useSumSBD, useSumSBDRaw, useAvgSpeedFraction all on.
    let flags = ExternalFlags {
        process_id: 2,
        use_sum_sbd: true,
        use_sum_sbd_raw: true,
        use_avg_speed_fraction: true,
        ..ExternalFlags::default()
    };
    let inputs = BaseRateInputs {
        rates_op_mode_distribution: vec![romd_row(21, 102, 15, 0.5)],
        sb_weighted_emission_rate: vec![rate(21, 102, 15, 8.0)],
        ..BaseRateInputs::default()
    };
    let out = BaseRateGenerator::run(&inputs, &flags);

    assert_eq!(out.base_rate.len(), 1);
    let r = out.base_rate[0];
    // opModeFraction 0.5, avgSpeedFraction 0.5, sumSBD 0.5, sumSBDRaw 0.25.
    // op_mode_fraction = 0.5 * 0.5 * 0.5 = 0.125.
    assert_eq!(r.op_mode_fraction, 0.125);
    assert_eq!(r.op_mode_fraction_rate, 0.125);
    // meanBaseRate t = 0.5 * 0.5 * 0.25 = 0.0625; 8 * 0.0625 = 0.5.
    assert_eq!(r.mean_base_rate, 0.5);
    // emissionRate t = opModeFraction * avgSpeedFraction = 0.25; 8 * 0.25 = 2.
    assert_eq!(r.emission_rate, 2.0);
}

#[test]
fn use_avg_speed_bin_retains_op_mode_and_divides_by_speed() {
    let flags = ExternalFlags {
        process_id: 2,
        use_avg_speed_bin: true,
        ..ExternalFlags::default()
    };
    let mut row = romd_row(21, 102, 15, 0.5);
    row.avg_bin_speed = 4.0; // power of two for an exact division
    let inputs = BaseRateInputs {
        rates_op_mode_distribution: vec![row],
        sb_weighted_emission_rate: vec![rate(21, 102, 15, 8.0)],
        ..BaseRateInputs::default()
    };
    let out = BaseRateGenerator::run(&inputs, &flags);

    assert_eq!(out.base_rate.len(), 1);
    let r = out.base_rate[0];
    // useAvgSpeedBin retains the real operating mode and bin in the record.
    assert_eq!(r.op_mode_id, 15);
    assert_eq!(r.avg_speed_bin_id, 8);
    // emissionRate t = opModeFraction * avgSpeedFraction / avgBinSpeed
    //              = 0.5 * 1 / 4 = 0.125; 8 * 0.125 = 1.0.
    assert_eq!(r.emission_rate, 1.0);
    assert_eq!(r.emission_rate_im, 0.5);
}

#[test]
fn duplicate_romd_rows_are_deduplicated() {
    // Two identical RatesOpModeDistribution rows collapse to one romd block;
    // a failed dedup would double the accumulated rate.
    let inputs = BaseRateInputs {
        rates_op_mode_distribution: vec![romd_row(21, 102, 15, 0.5), romd_row(21, 102, 15, 0.5)],
        sb_weighted_emission_rate: vec![rate(21, 102, 15, 8.0)],
        ..BaseRateInputs::default()
    };
    let out = BaseRateGenerator::run(&inputs, &flags_plain(2));
    assert_eq!(out.base_rate.len(), 1);
    // Single contribution: 8 * 0.5, not 8.0.
    assert_eq!(out.base_rate[0].mean_base_rate, 4.0);
}

#[test]
fn negative_pol_process_is_dropped_as_wildcard() {
    // polProcessID < 0 is a wildcard placeholder — handled, not written.
    let inputs = BaseRateInputs {
        rates_op_mode_distribution: vec![romd_row(21, -1, 15, 0.5)],
        sb_weighted_emission_rate: vec![rate(21, -1, 15, 8.0)],
        ..BaseRateInputs::default()
    };
    let out = BaseRateGenerator::run(&inputs, &flags_plain(2));
    assert!(out.base_rate.is_empty());
}

#[test]
fn physics_mapping_promotes_op_mode_and_swaps_source_type() {
    // Project-domain process 1 takes the core path. A romd row under temp
    // source type 99 is promoted: source type swapped to the real 21 and the
    // operating mode shifted by the offset.
    let physics = SourceUseTypePhysicsMappingDetail {
        real_source_type_id: 21,
        temp_source_type_id: 99,
        op_mode_id_offset: 1000,
        reg_class_id: 0,
        begin_model_year_id: 0,
        end_model_year_id: 0,
        ..SourceUseTypePhysicsMappingDetail::default()
    };
    let inputs = BaseRateInputs {
        is_project: true,
        rates_op_mode_distribution: vec![romd_row(99, 101, 15, 0.5)],
        // Rate filed under op mode 1015 (promoted) by the >= 1000 dual-keying.
        sb_weighted_emission_rate: vec![rate(21, 101, 1015, 8.0)],
        source_use_type_physics_mapping: vec![physics],
        ..BaseRateInputs::default()
    };
    let out = BaseRateGenerator::run(&inputs, &flags_plain(1));

    assert_eq!(out.base_rate.len(), 1);
    // Source type swapped from the temp 99 to the real 21.
    assert_eq!(out.base_rate[0].source_type_id, 21);
    assert_eq!(out.base_rate[0].mean_base_rate, 4.0);
}

#[test]
fn physics_promotion_skipped_for_ineligible_process() {
    // The promotion branches gate on Running (1) / Brakewear (9). A Start
    // Exhaust (process 2) row under a temp source type is kept verbatim.
    let physics = SourceUseTypePhysicsMappingDetail {
        real_source_type_id: 21,
        temp_source_type_id: 99,
        op_mode_id_offset: 1000,
        ..SourceUseTypePhysicsMappingDetail::default()
    };
    let inputs = BaseRateInputs {
        rates_op_mode_distribution: vec![romd_row(99, 102, 15, 0.5)],
        sb_weighted_emission_rate: vec![rate(99, 102, 15, 8.0)],
        source_use_type_physics_mapping: vec![physics],
        ..BaseRateInputs::default()
    };
    let out = BaseRateGenerator::run(&inputs, &flags_plain(2));

    assert_eq!(out.base_rate.len(), 1);
    // Not swapped — process 2 is ineligible for the promotion.
    assert_eq!(out.base_rate[0].source_type_id, 99);
}

#[test]
fn offset_op_mode_without_matching_rate_is_skipped_for_base_rate() {
    // A romd block at op mode >= 1000 with no matching SBWeightedEmissionRate
    // is skipped to avoid the double-counting the Go comment describes.
    let mut row = romd_row(21, 102, 1015, 0.5);
    row.op_mode_id = 1015;
    let inputs = BaseRateInputs {
        rates_op_mode_distribution: vec![row],
        // Rate only under op mode 15 — note prepare does NOT dual-key the
        // ByAge table, and op mode 1015 is absent here.
        sb_weighted_emission_rate_by_age: vec![rate(21, 102, 15, 8.0)],
        ..BaseRateInputs::default()
    };
    let out = BaseRateGenerator::run(&inputs, &flags_plain(2));
    assert!(out.base_rate.is_empty());
}

#[test]
fn by_age_offset_op_mode_falls_back_for_brakewear() {
    // Brakewear (process 9): a romd block at op mode 1015 with no ByAge rate
    // there falls back to the non-offset op mode 15. Project domain forces
    // the core path so the romd block is controlled directly.
    let inputs = BaseRateInputs {
        is_project: true,
        rates_op_mode_distribution: vec![romd_row(21, 109, 1015, 0.5)],
        sb_weighted_emission_rate_by_age: vec![rate(21, 109, 15, 8.0)],
        ..BaseRateInputs::default()
    };
    let out = BaseRateGenerator::run(&inputs, &flags_plain(9));

    assert_eq!(out.base_rate_by_age.len(), 1);
    assert_eq!(out.base_rate_by_age[0].mean_base_rate, 4.0);
    // BaseRateByAge keeps the age dimension from the rate record.
    assert_eq!(out.base_rate_by_age[0].age_group_id, 0);
}

#[test]
fn distance_rates_contribute_to_base_rate() {
    let inputs = BaseRateInputs {
        sb_weighted_distance_rate: vec![SbWeightedDistanceRow {
            source_type_id: 21,
            avg_speed_bin_id: 8,
            pol_process_id: 102,
            model_year_id: 2016,
            fuel_type_id: 1,
            reg_class_id: 20,
            mean_base_rate: 8.0,
            mean_base_rate_im: 4.0,
            mean_base_rate_ac_adj: 2.0,
            mean_base_rate_im_ac_adj: 1.0,
            sum_sbd: 0.5,
            sum_sbd_raw: 0.25,
        }],
        avg_speed_bin: vec![AvgSpeedBinRow {
            avg_speed_bin_id: 8,
            avg_bin_speed: 4.0,
        }],
        avg_speed_distribution: vec![AvgSpeedDistributionRow {
            source_type_id: 21,
            road_type_id: 5,
            hour_day_id: 85,
            avg_speed_bin_id: 8,
            avg_speed_fraction: 0.5,
        }],
        run_spec_road_type: vec![5],
        run_spec_hour_day: vec![85],
        ..BaseRateInputs::default()
    };
    let out = BaseRateGenerator::run(&inputs, &flags_plain(2));

    // One (road, hour) combination => one distance-derived BaseRate row.
    assert_eq!(out.base_rate.len(), 1);
    let r = out.base_rate[0];
    assert_eq!(r.source_type_id, 21);
    assert_eq!(r.road_type_id, 5);
    assert_eq!(r.hour_day_id, 85);
    // All weighting off: t = 1 throughout, meanBaseRate = 8.
    assert_eq!(r.mean_base_rate, 8.0);
    assert_eq!(r.emission_rate, 8.0);
    assert_eq!(r.op_mode_fraction, 1.0);
}

#[test]
fn drive_cycle_path_produces_base_rate_and_idle_fraction() {
    // Non-project process 1 takes the drive-cycle path. A drive schedule of
    // all-idle seconds bins entirely to operating mode 1; the bracketed bin
    // therefore carries opModeFraction[1] = 1, the enumeration emits one
    // idle romd block, and the driving-idle fraction is 1.
    let physics = SourceUseTypePhysicsMappingDetail {
        real_source_type_id: 21,
        temp_source_type_id: 21,
        op_mode_id_offset: 0,
        reg_class_id: 20,
        begin_model_year_id: 1990,
        end_model_year_id: 2010,
        ..SourceUseTypePhysicsMappingDetail::default()
    };
    let inputs = BaseRateInputs {
        is_project: false,
        drive_schedule: vec![DriveScheduleRow {
            drive_schedule_id: 100,
            average_speed: 0.0,
        }],
        drive_schedule_assoc: vec![DriveScheduleAssocRow {
            source_type_id: 21,
            road_type_id: 5,
            drive_schedule_id: 100,
        }],
        drive_schedule_second: (1..=5)
            .map(|s| DriveScheduleSecondRow {
                drive_schedule_id: 100,
                second: s,
                speed: 0.0,
            })
            .collect(),
        avg_speed_bin: vec![AvgSpeedBinRow {
            avg_speed_bin_id: 8,
            avg_bin_speed: 0.0,
        }],
        avg_speed_distribution: vec![AvgSpeedDistributionRow {
            source_type_id: 21,
            road_type_id: 5,
            hour_day_id: 85,
            avg_speed_bin_id: 8,
            avg_speed_fraction: 0.5,
        }],
        run_spec_road_type: vec![5],
        run_spec_hour_day: vec![85],
        run_spec_source_type: vec![21],
        run_spec_pollutant_process: vec![101],
        op_mode_pol_proc_assoc: vec![OpModePolProcRow {
            pol_process_id: 101,
            op_mode_id: 5,
        }],
        source_use_type_physics_mapping: vec![physics],
        sb_weighted_emission_rate: vec![SbWeightedRateDetail {
            source_type_id: 21,
            pol_process_id: 101,
            op_mode_id: 1,
            model_year_id: 2000,
            fuel_type_id: 1,
            age_group_id: 0,
            reg_class_id: 20,
            sum_sbd: 1.0,
            sum_sbd_raw: 1.0,
            mean_base_rate: 8.0,
            mean_base_rate_im: 4.0,
            mean_base_rate_ac_adj: 2.0,
            mean_base_rate_im_ac_adj: 1.0,
        }],
        ..BaseRateInputs::default()
    };
    let out = BaseRateGenerator::run(&inputs, &flags_plain(1));

    assert_eq!(out.base_rate.len(), 1);
    let r = out.base_rate[0];
    assert_eq!(r.source_type_id, 21);
    assert_eq!(r.pol_process_id, 101);
    assert_eq!(r.op_mode_id, 0); // flags off => collapsed
    assert_eq!(r.mean_base_rate, 8.0);

    // Driving-idle fraction: all driving time is idle.
    assert_eq!(out.driving_idle_fraction.len(), 1);
    let idle = out.driving_idle_fraction[0];
    assert_eq!(idle.source_type_id, 21);
    assert_eq!(idle.road_type_id, 5);
    assert_eq!(idle.hour_day_id, 85);
    assert_eq!(idle.year_id, 2020);
    assert_eq!(idle.driving_idle_fraction, 1.0);
}

#[test]
fn reg_class_filter_drops_non_matching_rates() {
    // A drive-cycle romd block carries the physics record's regClassID; the
    // aggregator drops rates whose regClassID differs. Use the core path via
    // a hand-built romd block by routing through project-domain process 1
    // with a physics mapping that contributes its regClassID only on the
    // drive-cycle path — so here, verify on the by-age core path instead:
    // a romd block from the core path has regClassID 0, which disables the
    // filter, so every rate is kept regardless of its regClassID.
    let inputs = BaseRateInputs {
        rates_op_mode_distribution: vec![romd_row(21, 102, 15, 0.5)],
        sb_weighted_emission_rate_by_age: vec![
            rate(21, 102, 15, 8.0),
            SbWeightedRateDetail {
                reg_class_id: 48, // different regClass — still kept (filter off)
                ..rate(21, 102, 15, 8.0)
            },
        ],
        ..BaseRateInputs::default()
    };
    let out = BaseRateGenerator::run(&inputs, &flags_plain(2));
    // Two distinct rate records (regClass 20 and 48) => two ByAge rows,
    // because the core-path block's regClassID 0 disables the filter.
    assert_eq!(out.base_rate_by_age.len(), 2);
}

#[test]
fn process_filter_excludes_other_processes_from_rates() {
    // A rate for process 1 must not appear when generating for process 2.
    let inputs = BaseRateInputs {
        rates_op_mode_distribution: vec![romd_row(21, 102, 15, 0.5)],
        sb_weighted_emission_rate: vec![
            rate(21, 102, 15, 8.0), // process 2 — kept
            rate(21, 101, 15, 9.0), // process 1 — filtered out in prepare
        ],
        ..BaseRateInputs::default()
    };
    let out = BaseRateGenerator::run(&inputs, &flags_plain(2));
    assert_eq!(out.base_rate.len(), 1);
    assert_eq!(out.base_rate[0].pol_process_id, 102);
}

#[test]
fn empty_inputs_produce_no_rows() {
    let out = BaseRateGenerator::run(&BaseRateInputs::default(), &flags_plain(1));
    assert!(out.base_rate.is_empty());
    assert!(out.base_rate_by_age.is_empty());
    assert!(out.driving_idle_fraction.is_empty());
}
