//! Input tables and the table-preparation step for the Base Rate Generator.
//!
//! Ports the Go `setupTables` function and the thirteen `read*` helpers.
//! The Go reads rows out of the worker's MariaDB execution database; the
//! pure port instead takes a [`BaseRateInputs`] value holding those tables
//! as plain row vectors and a [`PreparedTables`] derived from it.
//!
//! [`BaseRateInputs`] is the data-plane contract: a future Task 50
//! (`DataFrameStore`) wiring populates it from the scratch / default-DB
//! `DataFrame`s. [`PreparedTables::from_inputs`] applies the per-table `WHERE`
//! filters, joins, sorts and keying that the Go SQL statements performed.
//!
//! The Go also reads `runSpecModelYear`; that table is loaded but never
//! consumed anywhere in `baserategenerator.go`, so the port omits it
//! entirely — loading it would have no observable effect.

use std::collections::{BTreeMap, BTreeSet};

use super::model::{
    AvgSpeedDistributionDetail, AvgSpeedDistributionKey, DriveScheduleAssocKey, ExternalFlags,
    OperatingMode, SbWeightedRateDetail, SbWeightedRateKey, SourceUseTypePhysicsMappingDetail,
};

/// One `avgSpeedBin` row: average bin speed keyed by bin id.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct AvgSpeedBinRow {
    /// Average-speed bin id.
    pub avg_speed_bin_id: i32,
    /// Average bin speed.
    pub avg_bin_speed: f64,
}

/// One `driveSchedule` row: average speed keyed by drive-schedule id.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct DriveScheduleRow {
    /// Drive-schedule id.
    pub drive_schedule_id: i32,
    /// Average speed of the schedule.
    pub average_speed: f64,
}

/// One `avgSpeedDistribution` row (before the `avgSpeedBin` join).
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct AvgSpeedDistributionRow {
    /// Source type id.
    pub source_type_id: i32,
    /// Road type id.
    pub road_type_id: i32,
    /// Hour/day id.
    pub hour_day_id: i32,
    /// Average-speed bin id.
    pub avg_speed_bin_id: i32,
    /// Average-speed-bin fraction.
    pub avg_speed_fraction: f64,
}

/// One `SBWeightedDistanceRate` row.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct SbWeightedDistanceRow {
    /// Source type id.
    pub source_type_id: i32,
    /// Average-speed bin id.
    pub avg_speed_bin_id: i32,
    /// Pollutant/process id.
    pub pol_process_id: i32,
    /// Model year id.
    pub model_year_id: i32,
    /// Fuel type id.
    pub fuel_type_id: i32,
    /// Regulatory class id.
    pub reg_class_id: i32,
    /// Mean base rate.
    pub mean_base_rate: f64,
    /// Mean base rate, I/M adjusted.
    pub mean_base_rate_im: f64,
    /// Mean base rate, air-conditioning adjusted.
    pub mean_base_rate_ac_adj: f64,
    /// Mean base rate, I/M and air-conditioning adjusted.
    pub mean_base_rate_im_ac_adj: f64,
    /// Source-bin distribution sum.
    pub sum_sbd: f64,
    /// Raw source-bin distribution sum.
    pub sum_sbd_raw: f64,
}

/// One `opModePolProcAssoc` row: which operating modes a pollutant/process
/// uses. Needed only to reproduce the `runSpecPollutantProcess` join.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct OpModePolProcRow {
    /// Pollutant/process id.
    pub pol_process_id: i32,
    /// Operating-mode id.
    pub op_mode_id: i32,
}

/// One `driveScheduleAssoc` row.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct DriveScheduleAssocRow {
    /// Source type id.
    pub source_type_id: i32,
    /// Road type id.
    pub road_type_id: i32,
    /// Drive-schedule id.
    pub drive_schedule_id: i32,
}

/// One `RatesOpModeDistribution` row.
///
/// The Go also selects `opModeFractionCV`; the port omits it because the Go
/// scans it into a variable and never reads it.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct RatesOpModeDistributionRow {
    /// Source type id.
    pub source_type_id: i32,
    /// Road type id.
    pub road_type_id: i32,
    /// Average-speed bin id.
    pub avg_speed_bin_id: i32,
    /// Hour/day id.
    pub hour_day_id: i32,
    /// Pollutant/process id.
    pub pol_process_id: i32,
    /// Operating-mode id.
    pub op_mode_id: i32,
    /// Operating-mode fraction.
    pub op_mode_fraction: f64,
    /// Average bin speed.
    pub avg_bin_speed: f64,
    /// Average-speed-bin fraction.
    pub avg_speed_fraction: f64,
}

/// One `driveScheduleSecond` row: a vehicle speed sample.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct DriveScheduleSecondRow {
    /// Drive-schedule id.
    pub drive_schedule_id: i32,
    /// Second within the schedule.
    pub second: i32,
    /// Vehicle speed at this second.
    pub speed: f64,
}

/// All execution-database tables the Base Rate Generator reads.
///
/// This is the data-plane contract: each field mirrors one MariaDB table
/// the Go worker queried. Tables are held as plain row vectors, unfiltered
/// and unjoined — [`PreparedTables::from_inputs`] applies the SQL `WHERE` /
/// `JOIN` / `ORDER BY` semantics.
#[derive(Debug, Clone, Default)]
pub struct BaseRateInputs {
    /// `avgSpeedBin` rows.
    pub avg_speed_bin: Vec<AvgSpeedBinRow>,
    /// `driveSchedule` rows.
    pub drive_schedule: Vec<DriveScheduleRow>,
    /// `avgSpeedDistribution` rows.
    pub avg_speed_distribution: Vec<AvgSpeedDistributionRow>,
    /// `sourceUseTypePhysicsMapping` rows.
    pub source_use_type_physics_mapping: Vec<SourceUseTypePhysicsMappingDetail>,
    /// `SBWeightedEmissionRateByAge` rows.
    pub sb_weighted_emission_rate_by_age: Vec<SbWeightedRateDetail>,
    /// `SBWeightedEmissionRate` rows (`age_group_id` ignored / `0`).
    pub sb_weighted_emission_rate: Vec<SbWeightedRateDetail>,
    /// `SBWeightedDistanceRate` rows.
    pub sb_weighted_distance_rate: Vec<SbWeightedDistanceRow>,
    /// `runSpecRoadType` rows (`roadTypeID`).
    pub run_spec_road_type: Vec<i32>,
    /// `runSpecHourDay` rows (`hourDayID`).
    pub run_spec_hour_day: Vec<i32>,
    /// `runSpecSourceType` rows (`sourceTypeID`).
    pub run_spec_source_type: Vec<i32>,
    /// `runSpecPollutantProcess` rows (`polProcessID`).
    pub run_spec_pollutant_process: Vec<i32>,
    /// `opModePolProcAssoc` rows — needed for the `runSpecPollutantProcess`
    /// join.
    pub op_mode_pol_proc_assoc: Vec<OpModePolProcRow>,
    /// `driveScheduleAssoc` rows.
    pub drive_schedule_assoc: Vec<DriveScheduleAssocRow>,
    /// `operatingMode` rows.
    pub operating_mode: Vec<OperatingMode>,
    /// `RatesOpModeDistribution` rows (core-path input).
    pub rates_op_mode_distribution: Vec<RatesOpModeDistributionRow>,
    /// `driveScheduleSecond` rows (drive-cycle-path input).
    pub drive_schedule_second: Vec<DriveScheduleSecondRow>,
    /// Whether the run is a Project-domain run
    /// (`configuration.Singleton.IsProject`).
    pub is_project: bool,
}

/// Tables prepared for the generator — the Go globals after `setupTables`.
///
/// Built by [`PreparedTables::from_inputs`], which reproduces every SQL
/// `WHERE`, `JOIN` and `ORDER BY` the Go `read*` helpers relied on. The
/// `runSpec*` lists are sorted ascending so the drive-cycle enumeration
/// order matches the `ORDER BY sourceTypeID, polProcessID, roadTypeID,
/// hourDayID, opModeID, avgSpeedBinID` the Go comments require.
#[derive(Debug, Clone, Default)]
pub struct PreparedTables {
    /// Average bin speed keyed by `avgSpeedBinID`.
    pub avg_speed_bin: BTreeMap<i32, f64>,
    /// Average schedule speed keyed by `driveScheduleID`.
    pub drive_schedule: BTreeMap<i32, f64>,
    /// Average-speed distribution keyed by source/road/hour/bin.
    pub avg_speed_distribution: BTreeMap<AvgSpeedDistributionKey, AvgSpeedDistributionDetail>,
    /// All physics-mapping records, ordered by real source type then
    /// begin model year.
    pub source_use_type_physics_mapping: Vec<SourceUseTypePhysicsMappingDetail>,
    /// Physics mapping keyed by `tempSourceTypeID` (unique).
    pub physics_by_temp_source_type: BTreeMap<i32, SourceUseTypePhysicsMappingDetail>,
    /// Physics mapping keyed by `realSourceTypeID` (last record wins, as in
    /// the Go map assignment).
    pub physics_by_real_source_type: BTreeMap<i32, SourceUseTypePhysicsMappingDetail>,
    /// `SBWeightedEmissionRateByAge` records keyed by source/process/opmode.
    pub sb_weighted_emission_rate_by_age: BTreeMap<SbWeightedRateKey, Vec<SbWeightedRateDetail>>,
    /// `SBWeightedEmissionRate` records keyed by source/process/opmode.
    pub sb_weighted_emission_rate: BTreeMap<SbWeightedRateKey, Vec<SbWeightedRateDetail>>,
    /// Run-spec road types excluding off-network (`roadTypeID == 1`).
    pub run_spec_road_type: Vec<i32>,
    /// Run-spec road types including off-network.
    pub run_spec_road_type_with_off_network: Vec<i32>,
    /// Run-spec hour/day ids.
    pub run_spec_hour_day: Vec<i32>,
    /// Run-spec source type ids.
    pub run_spec_source_type: Vec<i32>,
    /// Pollutant/process ids that need drive cycles for this process.
    pub run_spec_pol_process_id: Vec<i32>,
    /// Drive-schedule ids keyed by source/road type.
    pub drive_schedule_assoc: BTreeMap<DriveScheduleAssocKey, Vec<i32>>,
    /// Operating-mode definitions keyed by `opModeID`
    /// (`1 < opModeID < 100`, excluding `26` and `36`).
    pub operating_modes: BTreeMap<i32, OperatingMode>,
}

impl PreparedTables {
    /// Build the prepared tables from raw [`BaseRateInputs`], applying every
    /// filter/join/order the Go `read*` helpers and SQL statements applied.
    #[must_use]
    pub fn from_inputs(inputs: &BaseRateInputs, flags: &ExternalFlags) -> Self {
        let mut prepared = PreparedTables::default();

        // readAvgSpeedBin / readDriveSchedule — straight loads.
        for row in &inputs.avg_speed_bin {
            prepared
                .avg_speed_bin
                .insert(row.avg_speed_bin_id, row.avg_bin_speed);
        }
        for row in &inputs.drive_schedule {
            prepared
                .drive_schedule
                .insert(row.drive_schedule_id, row.average_speed);
        }

        // readAvgSpeedDistribution — inner join avgSpeedBin, optional
        // roadType filter.
        for row in &inputs.avg_speed_distribution {
            if flags.road_type_id > 0 && row.road_type_id != flags.road_type_id {
                continue;
            }
            let Some(&avg_bin_speed) = prepared.avg_speed_bin.get(&row.avg_speed_bin_id) else {
                continue; // inner join: drop rows without a matching bin
            };
            prepared.avg_speed_distribution.insert(
                AvgSpeedDistributionKey {
                    source_type_id: row.source_type_id,
                    road_type_id: row.road_type_id,
                    hour_day_id: row.hour_day_id,
                    avg_speed_bin_id: row.avg_speed_bin_id,
                },
                AvgSpeedDistributionDetail {
                    avg_speed_fraction: row.avg_speed_fraction,
                    avg_bin_speed,
                },
            );
        }

        // readSourceUseTypePhysicsMapping — select distinct, order by
        // realSourceTypeID, beginModelYearID.
        let mut physics: Vec<SourceUseTypePhysicsMappingDetail> = Vec::new();
        for &row in &inputs.source_use_type_physics_mapping {
            if !physics.contains(&row) {
                physics.push(row); // `select distinct` over all columns
            }
        }
        physics.sort_by(|a, b| {
            (a.real_source_type_id, a.begin_model_year_id)
                .cmp(&(b.real_source_type_id, b.begin_model_year_id))
        });
        for &row in &physics {
            prepared
                .physics_by_temp_source_type
                .insert(row.temp_source_type_id, row);
            // Last record for a real source type wins, mirroring the Go
            // `map[...] = d` overwrite under the begin-model-year ordering.
            prepared
                .physics_by_real_source_type
                .insert(row.real_source_type_id, row);
        }
        prepared.source_use_type_physics_mapping = physics;

        // readSBWeightedEmissionRateByAge — filter by process.
        for &row in &inputs.sb_weighted_emission_rate_by_age {
            if row.pol_process_id % 100 != flags.process_id {
                continue;
            }
            prepared
                .sb_weighted_emission_rate_by_age
                .entry(SbWeightedRateKey {
                    source_type_id: row.source_type_id,
                    pol_process_id: row.pol_process_id,
                    op_mode_id: row.op_mode_id,
                })
                .or_default()
                .push(row);
        }

        // readSBWeightedEmissionRate — filter by process, plus the
        // opModeID >= 1000 records are also filed under opModeID % 100.
        for &row in &inputs.sb_weighted_emission_rate {
            if row.pol_process_id % 100 != flags.process_id {
                continue;
            }
            let mut row = row;
            row.age_group_id = 0; // SBWeightedEmissionRate has no age dimension
            prepared
                .sb_weighted_emission_rate
                .entry(SbWeightedRateKey {
                    source_type_id: row.source_type_id,
                    pol_process_id: row.pol_process_id,
                    op_mode_id: row.op_mode_id,
                })
                .or_default()
                .push(row);
            if row.op_mode_id >= 1000 {
                prepared
                    .sb_weighted_emission_rate
                    .entry(SbWeightedRateKey {
                        source_type_id: row.source_type_id,
                        pol_process_id: row.pol_process_id,
                        op_mode_id: row.op_mode_id % 100,
                    })
                    .or_default()
                    .push(row);
            }
        }

        // readRunSpecRoadType — roadTypeID in (0,100), optional filter, then
        // split off off-network (roadTypeID == 1).
        let mut road_with_oni: BTreeSet<i32> = BTreeSet::new();
        for &road_type_id in &inputs.run_spec_road_type {
            if !(road_type_id > 0 && road_type_id < 100) {
                continue;
            }
            if flags.road_type_id > 0 && road_type_id != flags.road_type_id {
                continue;
            }
            road_with_oni.insert(road_type_id);
        }
        prepared.run_spec_road_type_with_off_network = road_with_oni.iter().copied().collect();
        prepared.run_spec_road_type = road_with_oni.iter().copied().filter(|&r| r != 1).collect();

        // readRunSpecHourDay / readRunSpecSourceType — sorted ascending so
        // the drive-cycle enumeration order is well defined.
        prepared.run_spec_hour_day = sorted_unique(&inputs.run_spec_hour_day);
        prepared.run_spec_source_type = sorted_unique(&inputs.run_spec_source_type);

        // readRunSpecPollutantProcess — inner join opModePolProcAssoc,
        // filter to driving-cycle operating modes for this process.
        let in_run_spec: BTreeSet<i32> =
            inputs.run_spec_pollutant_process.iter().copied().collect();
        let mut pol_proc: BTreeSet<i32> = BTreeSet::new();
        for assoc in &inputs.op_mode_pol_proc_assoc {
            if assoc.pol_process_id > 0
                && assoc.op_mode_id >= 0
                && assoc.op_mode_id < 100
                && assoc.pol_process_id % 100 == flags.process_id
                && in_run_spec.contains(&assoc.pol_process_id)
            {
                pol_proc.insert(assoc.pol_process_id);
            }
        }
        prepared.run_spec_pol_process_id = pol_proc.into_iter().collect();

        // readDriveScheduleAssoc — inner join runSpecRoadType (raw table),
        // optional roadType filter.
        let raw_road_types: BTreeSet<i32> = inputs.run_spec_road_type.iter().copied().collect();
        for row in &inputs.drive_schedule_assoc {
            if !raw_road_types.contains(&row.road_type_id) {
                continue;
            }
            if flags.road_type_id > 0 && row.road_type_id != flags.road_type_id {
                continue;
            }
            prepared
                .drive_schedule_assoc
                .entry(DriveScheduleAssocKey {
                    source_type_id: row.source_type_id,
                    road_type_id: row.road_type_id,
                })
                .or_default()
                .push(row.drive_schedule_id);
        }

        // readOperatingMode — 1 < opModeID < 100, excluding 26 and 36.
        for &mode in &inputs.operating_mode {
            if mode.op_mode_id > 1
                && mode.op_mode_id < 100
                && mode.op_mode_id != 26
                && mode.op_mode_id != 36
            {
                prepared.operating_modes.insert(mode.op_mode_id, mode);
            }
        }

        prepared
    }
}

/// Sort a list of ids ascending and drop duplicates.
fn sorted_unique(ids: &[i32]) -> Vec<i32> {
    let set: BTreeSet<i32> = ids.iter().copied().collect();
    set.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn flags() -> ExternalFlags {
        ExternalFlags {
            process_id: 1,
            ..ExternalFlags::default()
        }
    }

    #[test]
    fn avg_speed_distribution_inner_joins_bin() {
        let inputs = BaseRateInputs {
            avg_speed_bin: vec![AvgSpeedBinRow {
                avg_speed_bin_id: 10,
                avg_bin_speed: 22.5,
            }],
            avg_speed_distribution: vec![
                AvgSpeedDistributionRow {
                    source_type_id: 21,
                    road_type_id: 3,
                    hour_day_id: 85,
                    avg_speed_bin_id: 10,
                    avg_speed_fraction: 0.4,
                },
                // No matching bin — dropped by the inner join.
                AvgSpeedDistributionRow {
                    source_type_id: 21,
                    road_type_id: 3,
                    hour_day_id: 85,
                    avg_speed_bin_id: 99,
                    avg_speed_fraction: 0.6,
                },
            ],
            ..BaseRateInputs::default()
        };
        let prepared = PreparedTables::from_inputs(&inputs, &flags());
        assert_eq!(prepared.avg_speed_distribution.len(), 1);
        let detail = prepared
            .avg_speed_distribution
            .get(&AvgSpeedDistributionKey {
                source_type_id: 21,
                road_type_id: 3,
                hour_day_id: 85,
                avg_speed_bin_id: 10,
            })
            .copied()
            .unwrap();
        assert_eq!(detail.avg_speed_fraction, 0.4);
        assert_eq!(detail.avg_bin_speed, 22.5);
    }

    #[test]
    fn road_type_split_excludes_off_network() {
        let inputs = BaseRateInputs {
            run_spec_road_type: vec![5, 1, 3, 100, 0],
            ..BaseRateInputs::default()
        };
        let prepared = PreparedTables::from_inputs(&inputs, &flags());
        assert_eq!(prepared.run_spec_road_type, vec![3, 5]);
        assert_eq!(prepared.run_spec_road_type_with_off_network, vec![1, 3, 5]);
    }

    #[test]
    fn sb_weighted_emission_rate_files_offset_op_modes_twice() {
        let row = SbWeightedRateDetail {
            source_type_id: 21,
            pol_process_id: 101,
            op_mode_id: 1015,
            mean_base_rate: 3.0,
            ..SbWeightedRateDetail::default()
        };
        let inputs = BaseRateInputs {
            sb_weighted_emission_rate: vec![row],
            ..BaseRateInputs::default()
        };
        let prepared = PreparedTables::from_inputs(&inputs, &flags());
        assert!(prepared
            .sb_weighted_emission_rate
            .contains_key(&SbWeightedRateKey {
                source_type_id: 21,
                pol_process_id: 101,
                op_mode_id: 1015,
            }));
        assert!(prepared
            .sb_weighted_emission_rate
            .contains_key(&SbWeightedRateKey {
                source_type_id: 21,
                pol_process_id: 101,
                op_mode_id: 15,
            }));
    }

    #[test]
    fn pollutant_process_join_filters_by_process_and_op_mode() {
        let inputs = BaseRateInputs {
            run_spec_pollutant_process: vec![101, 201, 9909],
            op_mode_pol_proc_assoc: vec![
                OpModePolProcRow {
                    pol_process_id: 101,
                    op_mode_id: 0,
                },
                // process 1, but op mode >= 100 — excluded.
                OpModePolProcRow {
                    pol_process_id: 201,
                    op_mode_id: 300,
                },
                // process 1 and a driving op mode, but not in run spec.
                OpModePolProcRow {
                    pol_process_id: 301,
                    op_mode_id: 5,
                },
            ],
            ..BaseRateInputs::default()
        };
        let prepared = PreparedTables::from_inputs(&inputs, &flags());
        assert_eq!(prepared.run_spec_pol_process_id, vec![101]);
    }

    #[test]
    fn operating_mode_filter_drops_excluded_modes() {
        let inputs = BaseRateInputs {
            operating_mode: vec![
                OperatingMode {
                    op_mode_id: 1,
                    ..OperatingMode::default()
                },
                OperatingMode {
                    op_mode_id: 13,
                    ..OperatingMode::default()
                },
                OperatingMode {
                    op_mode_id: 26,
                    ..OperatingMode::default()
                },
                OperatingMode {
                    op_mode_id: 200,
                    ..OperatingMode::default()
                },
            ],
            ..BaseRateInputs::default()
        };
        let prepared = PreparedTables::from_inputs(&inputs, &flags());
        assert_eq!(prepared.operating_modes.len(), 1);
        assert!(prepared.operating_modes.contains_key(&13));
    }
}
