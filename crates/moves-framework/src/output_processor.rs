//! Parquet writer for the unified MOVES output schema (Phase 4 Task 89).
//!
//! Materialises the column declarations from
//! [`moves_data::output_schema`] into the on-disk layout described by
//! Task 89:
//!
//! ```text
//! <output-root>/
//!   MOVESRun.parquet
//!   MOVESOutput/
//!     yearID=2020/monthID=1/part.parquet
//!     yearID=2020/monthID=7/part.parquet
//!     ...
//!   MOVESActivityOutput/
//!     yearID=2020/monthID=1/part.parquet
//!     ...
//! ```
//!
//! [`OutputProcessor`] is the Phase 2 skeleton called out by the
//! migration plan ("wire into Phase 2 `OutputProcessor`"). Phase 3
//! calculators will deliver per-iteration outputs through it; until
//! [`CalculatorOutput`](crate::CalculatorOutput) widens to a real
//! `DataFrame` (Task 50), callers construct strongly-typed
//! [`moves_data::EmissionRecord`] / [`moves_data::ActivityRecord`]
//! values directly.
//!
//! # Determinism
//!
//! Writer settings are pinned to match the existing `moves-snapshot`
//! and `moves-default-db-convert` writers:
//!
//! * `UNCOMPRESSED`
//! * dictionary encoding disabled
//! * statistics disabled
//! * `PARQUET_1_0` writer version
//! * fixed `created_by` string
//!
//! With identical row contents this writer produces byte-identical
//! Parquet, which the upstream snapshot determinism contract depends on.
//! Float values flow through `Float64` columns unmodified — the
//! fixed-decimal stringification used by `moves-snapshot` is a
//! regression-test artefact, not part of the production output schema.
//!
//! # Partitioning
//!
//! Year/month partitioning uses Hive-style `<column>=<value>` directory
//! components, the same convention the input-side
//! `moves-default-db-convert` crate uses. `yearID`/`monthID` columns
//! remain in the row data so Polars / DuckDB / pandas readers see them
//! whether or not they leverage hive-aware globbing.
//!
//! Null partition values land in a `__NULL__` directory — mirrors
//! `moves_default_db_convert::partition::render_path`.
//!
//! # Atomicity
//!
//! Each parquet file is written to a `<path>.tmp` sibling and renamed
//! into place. A crash mid-write leaves either nothing or a fully valid
//! file; downstream readers never see a truncated parquet footer.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Float64Builder, Int16Builder, Int32Builder, RecordBatch, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use moves_data::output_schema::{
    ActivityRecord, EmissionRecord, MovesRunRecord, OutputColumn, OutputColumnType, OutputTable,
};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};

use crate::aggregation::AggregationPlan;
use crate::error::{Error, Result};
use crate::output_aggregate::{aggregate_activity, aggregate_emissions, TemporalScalingFactors};

/// Identifier stamped into the parquet footer's `created_by` field.
/// Hardcoded to keep parquet bytes byte-identical across builds.
pub const PARQUET_CREATED_BY: &str = "moves-framework/output_processor";

/// Partition directory used when a partition value is SQL `NULL`.
/// Matches `moves_default_db_convert::partition::sanitize_value`.
pub const NULL_PARTITION: &str = "__NULL__";

/// Hive-style `(yearID, monthID)` partition key. `None` represents a row
/// with a NULL value in the corresponding column.
type PartitionKey = (Option<i16>, Option<i16>);

/// Writes the three MOVES output tables to a per-run output directory.
///
/// Construct with [`OutputProcessor::new`]; this creates the root
/// directory and immediately writes `MOVESRun.parquet`. Subsequent
/// [`write_emissions`](Self::write_emissions) /
/// [`write_activity`](Self::write_activity) calls land partition files
/// under `MOVESOutput/` and `MOVESActivityOutput/`.
///
/// The processor is stateless between calls — it owns no buffered rows.
/// Phase 3 calculators can call the partition writers multiple times if
/// they produce one partition at a time (the typical case under the
/// hour/day/month/year MasterLoop iteration). Two writes to the same
/// `(yearID, monthID)` *overwrite* the partition file; the caller is
/// responsible for batching rows from one partition into a single call.
///
/// [`write_aggregated_emissions`](Self::write_aggregated_emissions) /
/// [`write_aggregated_activity`](Self::write_aggregated_activity) apply a
/// Task 25 [`AggregationPlan`] — group-by, `SUM`, temporal rescaling —
/// before writing; that roll-up step is the Task 26 port of
/// `OutputProcessor.java`.
///
/// **Phase 2 skeleton.** The strongly-typed row API will be widened to
/// accept Polars `DataFrame`s once Task 50 lands; calculator wiring (Task
/// 27 `MOVESEngine`) is a follow-up.
#[derive(Debug, Clone)]
pub struct OutputProcessor {
    output_root: PathBuf,
}

impl OutputProcessor {
    /// Create a new output processor rooted at `output_root` and write
    /// the single [`MovesRunRecord`] to `MOVESRun.parquet`.
    ///
    /// `output_root` is created if it does not exist. `MOVESRun.parquet`
    /// is overwritten if present, so re-running a fixture against the
    /// same output directory replaces the previous run's metadata.
    pub fn new(output_root: impl Into<PathBuf>, run: &MovesRunRecord) -> Result<Self> {
        let output_root = output_root.into();
        std::fs::create_dir_all(&output_root).map_err(|source| Error::Io {
            path: output_root.clone(),
            source,
        })?;
        let proc = Self { output_root };
        proc.write_run_metadata(run)?;
        Ok(proc)
    }

    /// Root directory the processor writes into.
    #[must_use]
    pub fn output_root(&self) -> &Path {
        &self.output_root
    }

    /// Relative-from-root path the processor writes for a `(table,
    /// year, month)` triple. `year`/`month` are ignored for
    /// [`OutputTable::Run`].
    ///
    /// Visible so callers (tests, future merge logic) can predict the
    /// layout without re-deriving the rules.
    #[must_use]
    pub fn partition_path(table: OutputTable, year: Option<i16>, month: Option<i16>) -> PathBuf {
        match table {
            OutputTable::Run => PathBuf::from(format!("{}.parquet", table.name())),
            OutputTable::Emissions | OutputTable::Activity => {
                let mut p = PathBuf::from(table.name());
                p.push(format!("yearID={}", partition_segment(year)));
                p.push(format!("monthID={}", partition_segment(month)));
                p.push("part.parquet");
                p
            }
        }
    }

    /// Write a batch of [`EmissionRecord`]s, grouped by
    /// `(yearID, monthID)` and written one parquet file per partition.
    ///
    /// Rows in `records` may be in any order; the writer groups them
    /// internally. Each output file's row order matches the order in
    /// which rows appear in the input slice for that partition — i.e.,
    /// a stable in-partition sort is up to the caller. (Determinism
    /// guarantees still hold for any fixed input ordering.)
    pub fn write_emissions(&self, records: &[EmissionRecord]) -> Result<Vec<PathBuf>> {
        let groups = group_by_year_month(records, |r| (r.year_id, r.month_id));
        let mut written = Vec::with_capacity(groups.len());
        for ((year, month), rows) in groups {
            let path =
                self.output_root
                    .join(Self::partition_path(OutputTable::Emissions, year, month));
            let columns = OutputTable::Emissions.columns();
            let batch = emission_record_batch(columns, &rows)?;
            write_record_batch_atomic(&path, columns, &batch)?;
            written.push(path);
        }
        Ok(written)
    }

    /// Write a batch of [`ActivityRecord`]s.
    ///
    /// See [`write_emissions`](Self::write_emissions) for grouping and
    /// ordering semantics.
    pub fn write_activity(&self, records: &[ActivityRecord]) -> Result<Vec<PathBuf>> {
        let groups = group_by_year_month(records, |r| (r.year_id, r.month_id));
        let mut written = Vec::with_capacity(groups.len());
        for ((year, month), rows) in groups {
            let path =
                self.output_root
                    .join(Self::partition_path(OutputTable::Activity, year, month));
            let columns = OutputTable::Activity.columns();
            let batch = activity_record_batch(columns, &rows)?;
            write_record_batch_atomic(&path, columns, &batch)?;
            written.push(path);
        }
        Ok(written)
    }

    /// Aggregate `records` with an emission [`AggregationPlan`] and write
    /// the rolled-up partitions.
    ///
    /// This is the Task 26 entry point: it composes
    /// [`aggregate_emissions`] — the group-by + `SUM` roll-up ported from
    /// `OutputProcessor.java` — with [`write_emissions`](Self::write_emissions),
    /// the Task 89 Parquet writer. `factors` supplies the per-row temporal
    /// rescaling; pass [`UnitScaling`](crate::UnitScaling) when the plan
    /// carries no temporal scaling.
    ///
    /// Returns the partition file paths written, exactly as
    /// [`write_emissions`](Self::write_emissions) does.
    ///
    /// # Errors
    ///
    /// Surfaces [`Error::AggregationPlanMismatch`] from
    /// [`aggregate_emissions`] and any I/O / Parquet error from the writer.
    pub fn write_aggregated_emissions(
        &self,
        plan: &AggregationPlan,
        records: &[EmissionRecord],
        factors: &impl TemporalScalingFactors,
    ) -> Result<Vec<PathBuf>> {
        let aggregated = aggregate_emissions(plan, records, factors)?;
        self.write_emissions(&aggregated)
    }

    /// Aggregate `records` with an activity [`AggregationPlan`] and write
    /// the rolled-up partitions.
    ///
    /// The activity-table counterpart of
    /// [`write_aggregated_emissions`](Self::write_aggregated_emissions); see
    /// it for the composition and `factors` semantics.
    ///
    /// # Errors
    ///
    /// Surfaces [`Error::AggregationPlanMismatch`] from
    /// [`aggregate_activity`] and any I/O / Parquet error from the writer.
    pub fn write_aggregated_activity(
        &self,
        plan: &AggregationPlan,
        records: &[ActivityRecord],
        factors: &impl TemporalScalingFactors,
    ) -> Result<Vec<PathBuf>> {
        let aggregated = aggregate_activity(plan, records, factors)?;
        self.write_activity(&aggregated)
    }

    fn write_run_metadata(&self, run: &MovesRunRecord) -> Result<()> {
        let path = self
            .output_root
            .join(Self::partition_path(OutputTable::Run, None, None));
        let columns = OutputTable::Run.columns();
        let batch = moves_run_record_batch(columns, std::slice::from_ref(run))?;
        write_record_batch_atomic(&path, columns, &batch)
    }
}

fn partition_segment(value: Option<i16>) -> String {
    match value {
        Some(v) => v.to_string(),
        None => NULL_PARTITION.to_string(),
    }
}

fn group_by_year_month<R, F>(rows: &[R], key: F) -> Vec<(PartitionKey, Vec<&R>)>
where
    F: Fn(&R) -> PartitionKey,
{
    // BTreeMap to keep an ordering that's stable across runs even if the
    // input ordering changes. `Option<i16>` orders Nones first, which is
    // exactly the partition order we want on disk.
    let mut buckets: BTreeMap<PartitionKey, Vec<&R>> = BTreeMap::new();
    for row in rows {
        buckets.entry(key(row)).or_default().push(row);
    }
    buckets.into_iter().collect()
}

fn arrow_schema(columns: &[OutputColumn]) -> SchemaRef {
    let fields: Vec<Field> = columns
        .iter()
        .map(|c| Field::new(c.name, arrow_dtype(c.kind), c.nullable))
        .collect();
    Arc::new(ArrowSchema::new(fields))
}

fn arrow_dtype(kind: OutputColumnType) -> DataType {
    match kind {
        OutputColumnType::Smallint => DataType::Int16,
        OutputColumnType::Int => DataType::Int32,
        OutputColumnType::Float => DataType::Float64,
        OutputColumnType::Text | OutputColumnType::DateTime => DataType::Utf8,
    }
}

fn moves_run_record_batch(
    columns: &[OutputColumn],
    rows: &[MovesRunRecord],
) -> Result<RecordBatch> {
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
    for col in columns {
        let array: ArrayRef = match col.name {
            "MOVESRunID" => i16_array(rows.iter().map(|r| Some(r.moves_run_id))),
            "outputTimePeriod" => utf8_array(rows.iter().map(|r| r.output_time_period.as_deref())),
            "timeUnits" => utf8_array(rows.iter().map(|r| r.time_units.as_deref())),
            "distanceUnits" => utf8_array(rows.iter().map(|r| r.distance_units.as_deref())),
            "massUnits" => utf8_array(rows.iter().map(|r| r.mass_units.as_deref())),
            "energyUnits" => utf8_array(rows.iter().map(|r| r.energy_units.as_deref())),
            "runSpecFileName" => utf8_array(rows.iter().map(|r| r.run_spec_file_name.as_deref())),
            "runSpecDescription" => {
                utf8_array(rows.iter().map(|r| r.run_spec_description.as_deref()))
            }
            "runSpecFileDateTime" => {
                utf8_array(rows.iter().map(|r| r.run_spec_file_date_time.as_deref()))
            }
            "runDateTime" => utf8_array(rows.iter().map(|r| r.run_date_time.as_deref())),
            "scale" => utf8_array(rows.iter().map(|r| r.scale.as_deref())),
            "minutesDuration" => f64_array(rows.iter().map(|r| r.minutes_duration)),
            "defaultDatabaseUsed" => {
                utf8_array(rows.iter().map(|r| r.default_database_used.as_deref()))
            }
            "masterVersion" => utf8_array(rows.iter().map(|r| r.master_version.as_deref())),
            "masterComputerID" => utf8_array(rows.iter().map(|r| r.master_computer_id.as_deref())),
            "masterIDNumber" => utf8_array(rows.iter().map(|r| r.master_id_number.as_deref())),
            "domain" => utf8_array(rows.iter().map(|r| r.domain.as_deref())),
            "domainCountyID" => i32_array(rows.iter().map(|r| r.domain_county_id)),
            "domainCountyName" => utf8_array(rows.iter().map(|r| r.domain_county_name.as_deref())),
            "domainDatabaseServer" => {
                utf8_array(rows.iter().map(|r| r.domain_database_server.as_deref()))
            }
            "domainDatabaseName" => {
                utf8_array(rows.iter().map(|r| r.domain_database_name.as_deref()))
            }
            "expectedDONEFiles" => i32_array(rows.iter().map(|r| r.expected_done_files)),
            "retrievedDONEFiles" => i32_array(rows.iter().map(|r| r.retrieved_done_files)),
            "models" => utf8_array(rows.iter().map(|r| r.models.as_deref())),
            "runHash" => utf8_array(rows.iter().map(|r| Some(r.run_hash.as_str()))),
            "calculatorVersion" => {
                utf8_array(rows.iter().map(|r| Some(r.calculator_version.as_str())))
            }
            other => return Err(Error::OutputSchemaMismatch(other.to_string())),
        };
        arrays.push(array);
    }
    let schema = arrow_schema(columns);
    RecordBatch::try_new(schema, arrays).map_err(Error::Arrow)
}

fn emission_record_batch(
    columns: &[OutputColumn],
    rows: &[&EmissionRecord],
) -> Result<RecordBatch> {
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
    for col in columns {
        let array: ArrayRef = match col.name {
            "MOVESRunID" => i16_array(rows.iter().map(|r| Some(r.moves_run_id))),
            "iterationID" => i16_array(rows.iter().map(|r| r.iteration_id)),
            "yearID" => i16_array(rows.iter().map(|r| r.year_id)),
            "monthID" => i16_array(rows.iter().map(|r| r.month_id)),
            "dayID" => i16_array(rows.iter().map(|r| r.day_id)),
            "hourID" => i16_array(rows.iter().map(|r| r.hour_id)),
            "stateID" => i16_array(rows.iter().map(|r| r.state_id)),
            "countyID" => i32_array(rows.iter().map(|r| r.county_id)),
            "zoneID" => i32_array(rows.iter().map(|r| r.zone_id)),
            "linkID" => i32_array(rows.iter().map(|r| r.link_id)),
            "pollutantID" => i16_array(rows.iter().map(|r| r.pollutant_id)),
            "processID" => i16_array(rows.iter().map(|r| r.process_id)),
            "sourceTypeID" => i16_array(rows.iter().map(|r| r.source_type_id)),
            "regClassID" => i16_array(rows.iter().map(|r| r.reg_class_id)),
            "fuelTypeID" => i16_array(rows.iter().map(|r| r.fuel_type_id)),
            "fuelSubTypeID" => i16_array(rows.iter().map(|r| r.fuel_sub_type_id)),
            "modelYearID" => i16_array(rows.iter().map(|r| r.model_year_id)),
            "roadTypeID" => i16_array(rows.iter().map(|r| r.road_type_id)),
            "SCC" => utf8_array(rows.iter().map(|r| r.scc.as_deref())),
            "engTechID" => i16_array(rows.iter().map(|r| r.eng_tech_id)),
            "sectorID" => i16_array(rows.iter().map(|r| r.sector_id)),
            "hpID" => i16_array(rows.iter().map(|r| r.hp_id)),
            "emissionQuant" => f64_array(rows.iter().map(|r| r.emission_quant)),
            "emissionRate" => f64_array(rows.iter().map(|r| r.emission_rate)),
            "runHash" => utf8_array(rows.iter().map(|r| Some(r.run_hash.as_str()))),
            other => return Err(Error::OutputSchemaMismatch(other.to_string())),
        };
        arrays.push(array);
    }
    let schema = arrow_schema(columns);
    RecordBatch::try_new(schema, arrays).map_err(Error::Arrow)
}

fn activity_record_batch(
    columns: &[OutputColumn],
    rows: &[&ActivityRecord],
) -> Result<RecordBatch> {
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
    for col in columns {
        let array: ArrayRef = match col.name {
            "MOVESRunID" => i16_array(rows.iter().map(|r| Some(r.moves_run_id))),
            "iterationID" => i16_array(rows.iter().map(|r| r.iteration_id)),
            "yearID" => i16_array(rows.iter().map(|r| r.year_id)),
            "monthID" => i16_array(rows.iter().map(|r| r.month_id)),
            "dayID" => i16_array(rows.iter().map(|r| r.day_id)),
            "hourID" => i16_array(rows.iter().map(|r| r.hour_id)),
            "stateID" => i16_array(rows.iter().map(|r| r.state_id)),
            "countyID" => i32_array(rows.iter().map(|r| r.county_id)),
            "zoneID" => i32_array(rows.iter().map(|r| r.zone_id)),
            "linkID" => i32_array(rows.iter().map(|r| r.link_id)),
            "sourceTypeID" => i16_array(rows.iter().map(|r| r.source_type_id)),
            "regClassID" => i16_array(rows.iter().map(|r| r.reg_class_id)),
            "fuelTypeID" => i16_array(rows.iter().map(|r| r.fuel_type_id)),
            "fuelSubTypeID" => i16_array(rows.iter().map(|r| r.fuel_sub_type_id)),
            "modelYearID" => i16_array(rows.iter().map(|r| r.model_year_id)),
            "roadTypeID" => i16_array(rows.iter().map(|r| r.road_type_id)),
            "SCC" => utf8_array(rows.iter().map(|r| r.scc.as_deref())),
            "engTechID" => i16_array(rows.iter().map(|r| r.eng_tech_id)),
            "sectorID" => i16_array(rows.iter().map(|r| r.sector_id)),
            "hpID" => i16_array(rows.iter().map(|r| r.hp_id)),
            "activityTypeID" => i16_array(rows.iter().map(|r| r.activity_type_id)),
            "activity" => f64_array(rows.iter().map(|r| r.activity)),
            "runHash" => utf8_array(rows.iter().map(|r| Some(r.run_hash.as_str()))),
            other => return Err(Error::OutputSchemaMismatch(other.to_string())),
        };
        arrays.push(array);
    }
    let schema = arrow_schema(columns);
    RecordBatch::try_new(schema, arrays).map_err(Error::Arrow)
}

fn i16_array<I: IntoIterator<Item = Option<i16>>>(items: I) -> ArrayRef {
    let iter = items.into_iter();
    let (lower, _) = iter.size_hint();
    let mut b = Int16Builder::with_capacity(lower);
    for v in iter {
        match v {
            None => b.append_null(),
            Some(x) => b.append_value(x),
        }
    }
    Arc::new(b.finish())
}

fn i32_array<I: IntoIterator<Item = Option<i32>>>(items: I) -> ArrayRef {
    let iter = items.into_iter();
    let (lower, _) = iter.size_hint();
    let mut b = Int32Builder::with_capacity(lower);
    for v in iter {
        match v {
            None => b.append_null(),
            Some(x) => b.append_value(x),
        }
    }
    Arc::new(b.finish())
}

fn f64_array<I: IntoIterator<Item = Option<f64>>>(items: I) -> ArrayRef {
    let iter = items.into_iter();
    let (lower, _) = iter.size_hint();
    let mut b = Float64Builder::with_capacity(lower);
    for v in iter {
        match v {
            None => b.append_null(),
            Some(x) => b.append_value(x),
        }
    }
    Arc::new(b.finish())
}

fn utf8_array<'a, I: IntoIterator<Item = Option<&'a str>>>(items: I) -> ArrayRef {
    let iter = items.into_iter();
    let (lower, _) = iter.size_hint();
    let mut b = StringBuilder::with_capacity(lower, lower * 16);
    for v in iter {
        match v {
            None => b.append_null(),
            Some(s) => b.append_value(s),
        }
    }
    Arc::new(b.finish())
}

fn encode_parquet(columns: &[OutputColumn], batch: &RecordBatch) -> Result<Vec<u8>> {
    let schema = arrow_schema(columns);
    let props = WriterProperties::builder()
        .set_created_by(PARQUET_CREATED_BY.to_string())
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_writer_version(WriterVersion::PARQUET_1_0)
        .build();
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer =
            ArrowWriter::try_new(&mut buf, schema, Some(props)).map_err(Error::Parquet)?;
        if batch.num_rows() > 0 {
            writer.write(batch).map_err(Error::Parquet)?;
        }
        writer.close().map_err(Error::Parquet)?;
    }
    Ok(buf)
}

fn write_record_batch_atomic(
    path: &Path,
    columns: &[OutputColumn],
    batch: &RecordBatch,
) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| Error::Io {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    let bytes = encode_parquet(columns, batch)?;
    let tmp = with_extension_suffix(path, ".tmp");
    std::fs::write(&tmp, &bytes).map_err(|source| Error::Io {
        path: tmp.clone(),
        source,
    })?;
    std::fs::rename(&tmp, path).map_err(|source| Error::Io {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(())
}

fn with_extension_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut s = path.as_os_str().to_owned();
    s.push(suffix);
    PathBuf::from(s)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use tempfile::tempdir;

    fn fixture_run() -> MovesRunRecord {
        MovesRunRecord {
            moves_run_id: 1,
            output_time_period: Some("Year".to_string()),
            time_units: Some("hours".to_string()),
            distance_units: Some("miles".to_string()),
            mass_units: Some("grams".to_string()),
            energy_units: Some("KJ".to_string()),
            run_spec_file_name: Some("/runs/sample.mrs".to_string()),
            run_spec_description: Some("fixture run".to_string()),
            run_spec_file_date_time: Some("2026-05-12T10:00:00".to_string()),
            run_date_time: Some("2026-05-12T10:01:00".to_string()),
            scale: Some("Default".to_string()),
            minutes_duration: Some(0.5),
            default_database_used: Some("movesdb20241112".to_string()),
            master_version: Some("moves.rs 0.1.0".to_string()),
            master_computer_id: Some("test-host".to_string()),
            master_id_number: Some("0".to_string()),
            domain: Some("NATIONAL".to_string()),
            domain_county_id: None,
            domain_county_name: None,
            domain_database_server: None,
            domain_database_name: None,
            expected_done_files: Some(0),
            retrieved_done_files: Some(0),
            models: Some("onroad".to_string()),
            run_hash: "abc123".to_string(),
            calculator_version: "moves-rs 0.1.0+test".to_string(),
        }
    }

    fn emission(year: Option<i16>, month: Option<i16>, quant: f64) -> EmissionRecord {
        EmissionRecord {
            moves_run_id: 1,
            iteration_id: Some(1),
            year_id: year,
            month_id: month,
            day_id: Some(5),
            hour_id: Some(12),
            state_id: Some(17),
            county_id: Some(17031),
            zone_id: Some(170310),
            link_id: Some(1),
            pollutant_id: Some(2),
            process_id: Some(1),
            source_type_id: Some(21),
            reg_class_id: Some(20),
            fuel_type_id: Some(1),
            fuel_sub_type_id: Some(10),
            model_year_id: Some(2018),
            road_type_id: Some(3),
            scc: Some("2201001110".to_string()),
            eng_tech_id: Some(1),
            sector_id: None,
            hp_id: None,
            emission_quant: Some(quant),
            emission_rate: Some(quant / 100.0),
            run_hash: "abc123".to_string(),
        }
    }

    fn activity(year: Option<i16>, month: Option<i16>, value: f64) -> ActivityRecord {
        ActivityRecord {
            moves_run_id: 1,
            iteration_id: Some(1),
            year_id: year,
            month_id: month,
            day_id: Some(5),
            hour_id: Some(12),
            state_id: Some(17),
            county_id: Some(17031),
            zone_id: Some(170310),
            link_id: Some(1),
            source_type_id: Some(21),
            reg_class_id: Some(20),
            fuel_type_id: Some(1),
            fuel_sub_type_id: Some(10),
            model_year_id: Some(2018),
            road_type_id: Some(3),
            scc: Some("2201001110".to_string()),
            eng_tech_id: Some(1),
            sector_id: None,
            hp_id: None,
            activity_type_id: Some(1),
            activity: Some(value),
            run_hash: "abc123".to_string(),
        }
    }

    fn read_parquet(path: &Path) -> RecordBatch {
        let bytes = std::fs::read(path).expect("read parquet");
        let bytes = Bytes::from(bytes);
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .expect("reader")
            .build()
            .expect("build");
        reader
            .next()
            .expect("at least one batch")
            .expect("batch ok")
    }

    #[test]
    fn partition_path_for_run_is_singleton() {
        let p = OutputProcessor::partition_path(OutputTable::Run, None, None);
        assert_eq!(p, PathBuf::from("MOVESRun.parquet"));
    }

    #[test]
    fn partition_path_for_emissions_uses_year_then_month() {
        let p = OutputProcessor::partition_path(OutputTable::Emissions, Some(2020), Some(7));
        assert_eq!(
            p,
            PathBuf::from("MOVESOutput/yearID=2020/monthID=7/part.parquet")
        );
    }

    #[test]
    fn partition_path_with_null_year_or_month() {
        let p = OutputProcessor::partition_path(OutputTable::Activity, None, Some(7));
        assert_eq!(
            p,
            PathBuf::from("MOVESActivityOutput/yearID=__NULL__/monthID=7/part.parquet")
        );

        let p = OutputProcessor::partition_path(OutputTable::Emissions, Some(2020), None);
        assert_eq!(
            p,
            PathBuf::from("MOVESOutput/yearID=2020/monthID=__NULL__/part.parquet")
        );
    }

    #[test]
    fn new_writes_moves_run_parquet() {
        let dir = tempdir().unwrap();
        OutputProcessor::new(dir.path(), &fixture_run()).unwrap();
        let path = dir.path().join("MOVESRun.parquet");
        assert!(path.exists(), "MOVESRun.parquet should exist");
        let batch = read_parquet(&path);
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), MOVES_RUN_COLUMNS_LEN);

        // Validate a few representative columns
        let col_idx_for = |name: &str| {
            batch
                .schema()
                .index_of(name)
                .unwrap_or_else(|_| panic!("column {name} missing"))
        };
        let id = batch
            .column(col_idx_for("MOVESRunID"))
            .as_any()
            .downcast_ref::<arrow::array::Int16Array>()
            .unwrap();
        assert_eq!(id.value(0), 1);
        let hash = batch
            .column(col_idx_for("runHash"))
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(hash.value(0), "abc123");
        let ver = batch
            .column(col_idx_for("calculatorVersion"))
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(ver.value(0), "moves-rs 0.1.0+test");
    }

    #[test]
    fn emissions_are_partitioned_and_round_trip() {
        let dir = tempdir().unwrap();
        let proc = OutputProcessor::new(dir.path(), &fixture_run()).unwrap();
        let rows = vec![
            emission(Some(2020), Some(1), 1.0),
            emission(Some(2020), Some(1), 2.0),
            emission(Some(2020), Some(7), 3.0),
            emission(Some(2021), Some(1), 4.0),
        ];
        let written = proc.write_emissions(&rows).unwrap();
        assert_eq!(written.len(), 3, "three (year, month) buckets");

        let p1 = dir
            .path()
            .join("MOVESOutput/yearID=2020/monthID=1/part.parquet");
        assert!(p1.exists());
        let b1 = read_parquet(&p1);
        assert_eq!(b1.num_rows(), 2);

        let p2 = dir
            .path()
            .join("MOVESOutput/yearID=2020/monthID=7/part.parquet");
        assert!(p2.exists());
        let b2 = read_parquet(&p2);
        assert_eq!(b2.num_rows(), 1);

        let p3 = dir
            .path()
            .join("MOVESOutput/yearID=2021/monthID=1/part.parquet");
        assert!(p3.exists());
        let b3 = read_parquet(&p3);
        assert_eq!(b3.num_rows(), 1);
    }

    #[test]
    fn activity_round_trip_and_partitioning() {
        let dir = tempdir().unwrap();
        let proc = OutputProcessor::new(dir.path(), &fixture_run()).unwrap();
        let rows = vec![
            activity(Some(2020), Some(1), 100.0),
            activity(Some(2020), Some(2), 200.0),
        ];
        let written = proc.write_activity(&rows).unwrap();
        assert_eq!(written.len(), 2);
        for path in &written {
            assert!(path.exists(), "{path:?} should exist");
            let batch = read_parquet(path);
            assert_eq!(batch.num_rows(), 1);
        }
    }

    #[test]
    fn null_year_month_lands_in_null_partition() {
        let dir = tempdir().unwrap();
        let proc = OutputProcessor::new(dir.path(), &fixture_run()).unwrap();
        let rows = vec![emission(None, None, 1.0)];
        let written = proc.write_emissions(&rows).unwrap();
        assert_eq!(written.len(), 1);
        let expected = dir
            .path()
            .join("MOVESOutput/yearID=__NULL__/monthID=__NULL__/part.parquet");
        assert_eq!(written[0], expected);
        assert!(expected.exists());
    }

    #[test]
    fn writer_is_deterministic() {
        let run = fixture_run();
        let rows = vec![
            emission(Some(2020), Some(1), 1.5),
            emission(Some(2020), Some(1), 2.5),
        ];

        let dir1 = tempdir().unwrap();
        let dir2 = tempdir().unwrap();
        let p1 = OutputProcessor::new(dir1.path(), &run).unwrap();
        let p2 = OutputProcessor::new(dir2.path(), &run).unwrap();
        p1.write_emissions(&rows).unwrap();
        p2.write_emissions(&rows).unwrap();

        for rel in [
            "MOVESRun.parquet",
            "MOVESOutput/yearID=2020/monthID=1/part.parquet",
        ] {
            let a = std::fs::read(dir1.path().join(rel)).unwrap();
            let b = std::fs::read(dir2.path().join(rel)).unwrap();
            assert_eq!(a, b, "byte drift at {rel}");
        }
    }

    #[test]
    fn rewrite_replaces_partition_file() {
        let dir = tempdir().unwrap();
        let proc = OutputProcessor::new(dir.path(), &fixture_run()).unwrap();
        let first = vec![emission(Some(2020), Some(1), 1.0)];
        proc.write_emissions(&first).unwrap();
        let second = vec![
            emission(Some(2020), Some(1), 5.0),
            emission(Some(2020), Some(1), 6.0),
        ];
        proc.write_emissions(&second).unwrap();

        let path = dir
            .path()
            .join("MOVESOutput/yearID=2020/monthID=1/part.parquet");
        let batch = read_parquet(&path);
        // Second write replaces the first — observed row count reflects
        // the rewrite-don't-append semantics.
        assert_eq!(batch.num_rows(), 2);
    }

    #[test]
    fn empty_input_writes_nothing() {
        let dir = tempdir().unwrap();
        let proc = OutputProcessor::new(dir.path(), &fixture_run()).unwrap();
        let written = proc.write_emissions(&[]).unwrap();
        assert!(written.is_empty());
        let written = proc.write_activity(&[]).unwrap();
        assert!(written.is_empty());
        // MOVESRun.parquet still exists from the new() call.
        assert!(dir.path().join("MOVESRun.parquet").exists());
    }

    #[test]
    fn emission_column_count_matches_schema() {
        let row = emission(Some(2020), Some(1), 1.0);
        let batch = emission_record_batch(MOVES_OUTPUT_COLUMNS, &[&row]).unwrap();
        assert_eq!(batch.num_columns(), MOVES_OUTPUT_COLUMNS.len());
    }

    #[test]
    fn activity_column_count_matches_schema() {
        let row = activity(Some(2020), Some(1), 1.0);
        let batch = activity_record_batch(MOVES_ACTIVITY_OUTPUT_COLUMNS, &[&row]).unwrap();
        assert_eq!(batch.num_columns(), MOVES_ACTIVITY_OUTPUT_COLUMNS.len());
    }

    /// `AggregationInputs` for a Year + Nation run — the maximally-collapsing
    /// configuration, so the roll-up half is easy to read back off disk.
    fn year_nation_inputs<'a>(
        models: &'a [moves_runspec::model::Model],
        breakdown: &'a moves_runspec::model::OutputBreakdown,
    ) -> crate::aggregation::AggregationInputs<'a> {
        use moves_runspec::model::{GeographicOutputDetail, ModelScale, OutputTimestep};
        crate::aggregation::AggregationInputs {
            timestep: OutputTimestep::Year,
            geographic_output_detail: GeographicOutputDetail::Nation,
            scale: ModelScale::Macro,
            domain: None,
            models,
            breakdown,
            output_population: false,
            reg_class_id: false,
            fuel_sub_type: false,
            eng_tech_id: false,
            sector: false,
        }
    }

    #[test]
    fn write_aggregated_emissions_rolls_up_then_writes_partition() {
        use crate::aggregation::emission_aggregation;
        use crate::output_aggregate::UnitScaling;
        use moves_runspec::model::{Model, OutputBreakdown};

        let dir = tempdir().unwrap();
        let proc = OutputProcessor::new(dir.path(), &fixture_run()).unwrap();

        let breakdown = OutputBreakdown::default();
        let plan = emission_aggregation(&year_nation_inputs(&[Model::Onroad], &breakdown));

        // Three rows across two months — Year aggregation collapses month
        // and geography, so all three roll into one row.
        let rows = vec![
            emission(Some(2020), Some(1), 1.0),
            emission(Some(2020), Some(1), 2.0),
            emission(Some(2020), Some(7), 4.0),
        ];
        let written = proc
            .write_aggregated_emissions(&plan, &rows, &UnitScaling)
            .unwrap();
        assert_eq!(written.len(), 1, "all rows roll into one partition");

        // monthID collapsed to NULL → the __NULL__ partition directory.
        let expected = dir
            .path()
            .join("MOVESOutput/yearID=2020/monthID=__NULL__/part.parquet");
        assert_eq!(written[0], expected);

        let batch = read_parquet(&expected);
        assert_eq!(batch.num_rows(), 1);
        let quant = batch
            .column(batch.schema().index_of("emissionQuant").unwrap())
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert_eq!(quant.value(0), 7.0, "1.0 + 2.0 + 4.0");
        // monthID is null in the rolled-up row.
        assert!(
            batch
                .column(batch.schema().index_of("monthID").unwrap())
                .is_null(0),
            "monthID collapses to NULL under Year aggregation"
        );
    }

    #[test]
    fn write_aggregated_activity_rolls_up_then_writes_partition() {
        use crate::aggregation::activity_aggregation;
        use crate::output_aggregate::UnitScaling;
        use moves_runspec::model::{Model, OutputBreakdown};

        let dir = tempdir().unwrap();
        let proc = OutputProcessor::new(dir.path(), &fixture_run()).unwrap();

        let breakdown = OutputBreakdown::default();
        let plan = activity_aggregation(&year_nation_inputs(&[Model::Onroad], &breakdown));

        let rows = vec![
            activity(Some(2020), Some(1), 100.0),
            activity(Some(2020), Some(2), 200.0),
        ];
        let written = proc
            .write_aggregated_activity(&plan, &rows, &UnitScaling)
            .unwrap();
        assert_eq!(written.len(), 1);

        let expected = dir
            .path()
            .join("MOVESActivityOutput/yearID=2020/monthID=__NULL__/part.parquet");
        assert_eq!(written[0], expected);

        let batch = read_parquet(&expected);
        assert_eq!(batch.num_rows(), 1);
        let activity_col = batch
            .column(batch.schema().index_of("activity").unwrap())
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert_eq!(activity_col.value(0), 300.0);
    }

    #[test]
    fn write_aggregated_emissions_rejects_activity_plan() {
        use crate::aggregation::activity_aggregation;
        use crate::output_aggregate::UnitScaling;
        use moves_runspec::model::{Model, OutputBreakdown};

        let dir = tempdir().unwrap();
        let proc = OutputProcessor::new(dir.path(), &fixture_run()).unwrap();
        let breakdown = OutputBreakdown::default();
        let activity_plan = activity_aggregation(&year_nation_inputs(&[Model::Onroad], &breakdown));

        let err = proc
            .write_aggregated_emissions(&activity_plan, &[], &UnitScaling)
            .unwrap_err();
        assert!(
            matches!(err, Error::AggregationPlanMismatch(_)),
            "got {err:?}"
        );
    }

    // Use locally to assert against schema length in `new_writes_moves_run_parquet`
    // without re-importing the slice.
    const MOVES_RUN_COLUMNS_LEN: usize = moves_data::output_schema::MOVES_RUN_COLUMNS.len();
    use moves_data::output_schema::{MOVES_ACTIVITY_OUTPUT_COLUMNS, MOVES_OUTPUT_COLUMNS};
}
