//! Total Activity Generator —.
//!
//! Pure-Rust port of
//! `gov/epa/otaq/moves/master/implementation/ghg/TotalActivityGenerator.java`
//! (2,793 lines of Java + embedded SQL). This is **the single most important
//! generator**: every onroad emission is `rate × activity`, and this
//! generator computes the activity — total VMT, source hours operating,
//! engine starts, source hours parked, and hotelling hours — that every
//! running-, start-, evap- and extended-idle calculator multiplies its rate
//! against.
//!
//! # What the Java did, and what this port keeps
//!
//! The Java generator ran inside the master loop: once per calendar year it
//! grew the vehicle population and VMT forward from the nearest base year,
//! split them across road type / source type / age / hour, and converted
//! the result to an activity basis; once per zone it allocated that activity
//! onto individual road links. It did all of this through `INSERT … SELECT`
//! statements against a MariaDB execution database.
//!
//! The port keeps the **computation** — every growth recurrence, join,
//! weighting, and aggregation — and replaces the database I/O with plain
//! values: a [`TotalActivityInputs`] in, a [`TotalActivityOutput`] out. The
//! `CREATE TABLE` / `TRUNCATE` scaffolding has no algorithmic content and no
//! analogue here.
//!
//! # Module map
//!
//! | Module | Ports | Algorithm steps |
//! |--------|-------|-----------------|
//! | [`inputs`] | the default-DB / RunSpec input tables | — |
//! | [`model`] | the working and output tables | — |
//! | [`population`] | `determineBaseYear`, `calculateBaseYearPopulation`, `growPopulationToAnalysisYear` | 110-139 |
//! | [`travel`] | `calculateFractionOfTravelUsingHPMS`, `growVMTToAnalysisYear` | 140-159 |
//! | [`vmt`] | `allocateVMTByRoadTypeSourceAge`, `calculateVMTByRoadwayHour` | 160-179 |
//! | [`activity`] | `convertVMTToTotalActivityBasis` | 180-189 |
//! | [`allocation`] | the pure kernels of `allocateTotalActivityBasis`, `calculateDistance` | 190-209 |
//!
//! [`TotalActivityGenerator::run`] chains steps 110-189 — the year/zone
//! activity computation — into a [`TotalActivityOutput`]. Steps 190-209 are
//! the *spatial allocation* of that activity onto links: their arithmetic is
//! ported as the standalone pure kernels in [`allocation`], but the master
//! loop's per-`(process, zone, link)` sequencing of those kernels — together
//! with the three external `database/Adjust*.sql` scripts the Java shells
//! out to — is orchestration that lands with the `execute` wiring,
//! exactly as `SourceBinDistributionGenerator` deferred its
//! per-callback dedup state.
//!
//! # Data-plane status
//!
//! [`TotalActivityGenerator::run`] is the numerical entry point and is fully
//! exercised by this crate's tests. The [`Generator`] trait's
//! [`execute`](Generator::execute) method is a shell: the
//! [`CalculatorContext`] it receives exposes only the placeholder
//! `ExecutionTables` / `ScratchNamespace`, which have no row storage yet.
//! (`DataFrameStore`) lands that storage; `execute` will then
//! materialise a [`TotalActivityInputs`] from the context, call
//! [`run`](TotalActivityGenerator::run), and write the
//! [`TotalActivityOutput`] back into the scratch namespace. Until then
//! `execute` returns an empty [`CalculatorOutput`] and the metadata methods
//! carry the real wiring information the registry needs.

pub mod activity;
pub mod allocation;
pub mod inputs;
pub mod model;
pub mod population;
pub mod travel;
pub mod vmt;

use std::sync::OnceLock;

use moves_calculator_info::{Granularity, Priority};
use moves_data::EmissionProcess;
use moves_framework::{
    CalculatorContext, CalculatorOutput, CalculatorSubscription, DataFrameStore,
    DataFrameStoreTyped, Error, Generator, InMemoryStore, IntoDataFrame, TableRow,
};
use polars::prelude::{DataFrame, DataType, NamedFrom, PolarsResult, Schema, Series};

pub use inputs::TotalActivityInputs;
pub use model::TotalActivityOutput;

// ── Data-plane helpers ───────────────────────────────────────────────────────

/// Construct a row-extraction error.
fn row_err(table: &'static str, row: usize, column: &'static str, msg: String) -> Error {
    Error::RowExtraction {
        table: table.into(),
        row,
        column: column.into(),
        message: msg,
    }
}

/// Read a table that may be absent; return an empty Vec when not present.
fn iter_optional<R: TableRow>(store: &InMemoryStore, name: &str) -> Result<Vec<R>, Error> {
    if store.contains(name) {
        store.iter_typed(name)
    } else {
        Ok(Vec::new())
    }
}

// ── TableRow implementations — inputs ────────────────────────────────────────

impl TableRow for inputs::YearRow {
    fn table_name() -> &'static str {
        "Year"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("yearID".into(), DataType::Int32),
            ("isBaseYear".into(), DataType::Boolean),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "yearID".into(),
                    rows.iter().map(|r| r.year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "isBaseYear".into(),
                    rows.iter().map(|r| r.is_base_year).collect::<Vec<bool>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "Year";
        let year_id = df
            .column("yearID")
            .map_err(|e| row_err(T, 0, "yearID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(T, 0, "yearID", e.to_string()))?;
        let is_base_year = df
            .column("isBaseYear")
            .map_err(|e| row_err(T, 0, "isBaseYear", e.to_string()))?
            .bool()
            .map_err(|e| row_err(T, 0, "isBaseYear", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(T, i, col, "null value".into());
                Ok(inputs::YearRow {
                    year_id: year_id.get(i).ok_or_else(|| null("yearID"))?,
                    // Canonical MOVES SQL filters Year via `isBaseYear IN ('Y','y')`,
                    // so NULL is semantically "not a base year". Match that here.
                    is_base_year: is_base_year.get(i).unwrap_or(false),
                })
            })
            .collect()
    }
}

impl TableRow for inputs::SourceTypeYearRow {
    fn table_name() -> &'static str {
        "SourceTypeYear"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("yearID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("sourceTypePopulation".into(), DataType::Float64),
            ("migrationRate".into(), DataType::Float64),
            ("salesGrowthFactor".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "yearID".into(),
                    rows.iter().map(|r| r.year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypePopulation".into(),
                    rows.iter()
                        .map(|r| r.source_type_population)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "migrationRate".into(),
                    rows.iter().map(|r| r.migration_rate).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "salesGrowthFactor".into(),
                    rows.iter()
                        .map(|r| r.sales_growth_factor)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "SourceTypeYear";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(T, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(T, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(T, 0, col, e.to_string()))?
                .f64()
                .map_err(|e| row_err(T, 0, col, e.to_string()))
        };
        let year_id = get_i32("yearID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let source_type_population = get_f64("sourceTypePopulation")?;
        let migration_rate = get_f64("migrationRate")?;
        let sales_growth_factor = get_f64("salesGrowthFactor")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(T, i, col, "null value".into());
                Ok(inputs::SourceTypeYearRow {
                    year_id: year_id.get(i).ok_or_else(|| null("yearID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    source_type_population: source_type_population
                        .get(i)
                        .ok_or_else(|| null("sourceTypePopulation"))?,
                    migration_rate: migration_rate.get(i).ok_or_else(|| null("migrationRate"))?,
                    sales_growth_factor: sales_growth_factor
                        .get(i)
                        .ok_or_else(|| null("salesGrowthFactor"))?,
                })
            })
            .collect()
    }
}

impl TableRow for inputs::SourceTypeAgeDistributionRow {
    fn table_name() -> &'static str {
        "SourceTypeAgeDistribution"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("yearID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
            ("ageFraction".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "yearID".into(),
                    rows.iter().map(|r| r.year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ageID".into(),
                    rows.iter().map(|r| r.age_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ageFraction".into(),
                    rows.iter().map(|r| r.age_fraction).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "SourceTypeAgeDistribution";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(T, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(T, 0, col, e.to_string()))
        };
        let source_type_id = get_i32("sourceTypeID")?;
        let year_id = get_i32("yearID")?;
        let age_id = get_i32("ageID")?;
        let age_fraction = df
            .column("ageFraction")
            .map_err(|e| row_err(T, 0, "ageFraction", e.to_string()))?
            .f64()
            .map_err(|e| row_err(T, 0, "ageFraction", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(T, i, col, "null value".into());
                Ok(inputs::SourceTypeAgeDistributionRow {
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    year_id: year_id.get(i).ok_or_else(|| null("yearID"))?,
                    age_id: age_id.get(i).ok_or_else(|| null("ageID"))?,
                    age_fraction: age_fraction.get(i).ok_or_else(|| null("ageFraction"))?,
                })
            })
            .collect()
    }
}

impl TableRow for inputs::SourceTypeAgeRow {
    fn table_name() -> &'static str {
        "SourceTypeAge"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
            ("survivalRate".into(), DataType::Float64),
            ("relativeMAR".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ageID".into(),
                    rows.iter().map(|r| r.age_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "survivalRate".into(),
                    rows.iter().map(|r| r.survival_rate).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "relativeMAR".into(),
                    rows.iter().map(|r| r.relative_mar).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "SourceTypeAge";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(T, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(T, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(T, 0, col, e.to_string()))?
                .f64()
                .map_err(|e| row_err(T, 0, col, e.to_string()))
        };
        let source_type_id = get_i32("sourceTypeID")?;
        let age_id = get_i32("ageID")?;
        let survival_rate = get_f64("survivalRate")?;
        let relative_mar = get_f64("relativeMAR")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(T, i, col, "null value".into());
                Ok(inputs::SourceTypeAgeRow {
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    age_id: age_id.get(i).ok_or_else(|| null("ageID"))?,
                    survival_rate: survival_rate.get(i).ok_or_else(|| null("survivalRate"))?,
                    relative_mar: relative_mar.get(i).ok_or_else(|| null("relativeMAR"))?,
                })
            })
            .collect()
    }
}

impl TableRow for inputs::SourceUseTypeRow {
    fn table_name() -> &'static str {
        "SourceUseType"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("HPMSVTypeID".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "HPMSVTypeID".into(),
                    rows.iter().map(|r| r.hpms_v_type_id).collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "SourceUseType";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(T, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(T, 0, col, e.to_string()))
        };
        let source_type_id = get_i32("sourceTypeID")?;
        let hpms_v_type_id = get_i32("HPMSVTypeID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(T, i, col, "null value".into());
                Ok(inputs::SourceUseTypeRow {
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    hpms_v_type_id: hpms_v_type_id.get(i).ok_or_else(|| null("HPMSVTypeID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for inputs::HpmsVTypeYearRow {
    fn table_name() -> &'static str {
        "HPMSVTypeYear"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("yearID".into(), DataType::Int32),
            ("HPMSVTypeID".into(), DataType::Int32),
            ("HPMSBaseYearVMT".into(), DataType::Float64),
            ("VMTGrowthFactor".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "yearID".into(),
                    rows.iter().map(|r| r.year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "HPMSVTypeID".into(),
                    rows.iter().map(|r| r.hpms_v_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "HPMSBaseYearVMT".into(),
                    rows.iter()
                        .map(|r| r.hpms_base_year_vmt)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "VMTGrowthFactor".into(),
                    rows.iter()
                        .map(|r| r.vmt_growth_factor)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "HPMSVTypeYear";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(T, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(T, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(T, 0, col, e.to_string()))?
                .f64()
                .map_err(|e| row_err(T, 0, col, e.to_string()))
        };
        let year_id = get_i32("yearID")?;
        let hpms_v_type_id = get_i32("HPMSVTypeID")?;
        let hpms_base_year_vmt = get_f64("HPMSBaseYearVMT")?;
        let vmt_growth_factor = get_f64("VMTGrowthFactor")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(T, i, col, "null value".into());
                Ok(inputs::HpmsVTypeYearRow {
                    year_id: year_id.get(i).ok_or_else(|| null("yearID"))?,
                    hpms_v_type_id: hpms_v_type_id.get(i).ok_or_else(|| null("HPMSVTypeID"))?,
                    hpms_base_year_vmt: hpms_base_year_vmt
                        .get(i)
                        .ok_or_else(|| null("HPMSBaseYearVMT"))?,
                    vmt_growth_factor: vmt_growth_factor
                        .get(i)
                        .ok_or_else(|| null("VMTGrowthFactor"))?,
                })
            })
            .collect()
    }
}

impl TableRow for inputs::RunSpecSourceTypeRow {
    fn table_name() -> &'static str {
        "RunSpecSourceType"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([("sourceTypeID".into(), DataType::Int32)])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![Series::new(
                "sourceTypeID".into(),
                rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
            )
            .into()],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "RunSpecSourceType";
        let source_type_id = df
            .column("sourceTypeID")
            .map_err(|e| row_err(T, 0, "sourceTypeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(T, 0, "sourceTypeID", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(T, i, col, "null value".into());
                Ok(inputs::RunSpecSourceTypeRow {
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for inputs::SourceTypeYearVmtRow {
    fn table_name() -> &'static str {
        "SourceTypeYearVMT"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("yearID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("VMT".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "yearID".into(),
                    rows.iter().map(|r| r.year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "VMT".into(),
                    rows.iter().map(|r| r.vmt).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "SourceTypeYearVMT";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(T, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(T, 0, col, e.to_string()))
        };
        let year_id = get_i32("yearID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let vmt = df
            .column("VMT")
            .map_err(|e| row_err(T, 0, "VMT", e.to_string()))?
            .f64()
            .map_err(|e| row_err(T, 0, "VMT", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(T, i, col, "null value".into());
                Ok(inputs::SourceTypeYearVmtRow {
                    year_id: year_id.get(i).ok_or_else(|| null("yearID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    vmt: vmt.get(i).ok_or_else(|| null("VMT"))?,
                })
            })
            .collect()
    }
}

impl TableRow for inputs::RoadTypeRow {
    fn table_name() -> &'static str {
        "RoadType"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([("roadTypeID".into(), DataType::Int32)])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![Series::new(
                "roadTypeID".into(),
                rows.iter().map(|r| r.road_type_id).collect::<Vec<i32>>(),
            )
            .into()],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "RoadType";
        let road_type_id = df
            .column("roadTypeID")
            .map_err(|e| row_err(T, 0, "roadTypeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(T, 0, "roadTypeID", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(T, i, col, "null value".into());
                Ok(inputs::RoadTypeRow {
                    road_type_id: road_type_id.get(i).ok_or_else(|| null("roadTypeID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for inputs::RoadTypeDistributionRow {
    fn table_name() -> &'static str {
        "RoadTypeDistribution"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("roadTypeID".into(), DataType::Int32),
            ("roadTypeVMTFraction".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "roadTypeID".into(),
                    rows.iter().map(|r| r.road_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "roadTypeVMTFraction".into(),
                    rows.iter()
                        .map(|r| r.road_type_vmt_fraction)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "RoadTypeDistribution";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(T, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(T, 0, col, e.to_string()))
        };
        let source_type_id = get_i32("sourceTypeID")?;
        let road_type_id = get_i32("roadTypeID")?;
        let road_type_vmt_fraction = df
            .column("roadTypeVMTFraction")
            .map_err(|e| row_err(T, 0, "roadTypeVMTFraction", e.to_string()))?
            .f64()
            .map_err(|e| row_err(T, 0, "roadTypeVMTFraction", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(T, i, col, "null value".into());
                Ok(inputs::RoadTypeDistributionRow {
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    road_type_id: road_type_id.get(i).ok_or_else(|| null("roadTypeID"))?,
                    road_type_vmt_fraction: road_type_vmt_fraction
                        .get(i)
                        .ok_or_else(|| null("roadTypeVMTFraction"))?,
                })
            })
            .collect()
    }
}

impl TableRow for inputs::SourceTypeDayVmtRow {
    fn table_name() -> &'static str {
        "SourceTypeDayVMT"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("yearID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("dayID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("VMT".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "yearID".into(),
                    rows.iter().map(|r| r.year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "monthID".into(),
                    rows.iter().map(|r| r.month_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "dayID".into(),
                    rows.iter().map(|r| r.day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "VMT".into(),
                    rows.iter().map(|r| r.vmt).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "SourceTypeDayVMT";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(T, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(T, 0, col, e.to_string()))
        };
        let year_id = get_i32("yearID")?;
        let month_id = get_i32("monthID")?;
        let day_id = get_i32("dayID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let vmt = df
            .column("VMT")
            .map_err(|e| row_err(T, 0, "VMT", e.to_string()))?
            .f64()
            .map_err(|e| row_err(T, 0, "VMT", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(T, i, col, "null value".into());
                Ok(inputs::SourceTypeDayVmtRow {
                    year_id: year_id.get(i).ok_or_else(|| null("yearID"))?,
                    month_id: month_id.get(i).ok_or_else(|| null("monthID"))?,
                    day_id: day_id.get(i).ok_or_else(|| null("dayID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    vmt: vmt.get(i).ok_or_else(|| null("VMT"))?,
                })
            })
            .collect()
    }
}

impl TableRow for inputs::HpmsVTypeDayRow {
    fn table_name() -> &'static str {
        "HPMSVTypeDay"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("yearID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("dayID".into(), DataType::Int32),
            ("HPMSVTypeID".into(), DataType::Int32),
            ("VMT".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "yearID".into(),
                    rows.iter().map(|r| r.year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "monthID".into(),
                    rows.iter().map(|r| r.month_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "dayID".into(),
                    rows.iter().map(|r| r.day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "HPMSVTypeID".into(),
                    rows.iter().map(|r| r.hpms_v_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "VMT".into(),
                    rows.iter().map(|r| r.vmt).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "HPMSVTypeDay";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(T, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(T, 0, col, e.to_string()))
        };
        let year_id = get_i32("yearID")?;
        let month_id = get_i32("monthID")?;
        let day_id = get_i32("dayID")?;
        let hpms_v_type_id = get_i32("HPMSVTypeID")?;
        let vmt = df
            .column("VMT")
            .map_err(|e| row_err(T, 0, "VMT", e.to_string()))?
            .f64()
            .map_err(|e| row_err(T, 0, "VMT", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(T, i, col, "null value".into());
                Ok(inputs::HpmsVTypeDayRow {
                    year_id: year_id.get(i).ok_or_else(|| null("yearID"))?,
                    month_id: month_id.get(i).ok_or_else(|| null("monthID"))?,
                    day_id: day_id.get(i).ok_or_else(|| null("dayID"))?,
                    hpms_v_type_id: hpms_v_type_id.get(i).ok_or_else(|| null("HPMSVTypeID"))?,
                    vmt: vmt.get(i).ok_or_else(|| null("VMT"))?,
                })
            })
            .collect()
    }
}

impl TableRow for inputs::MonthVmtFractionRow {
    fn table_name() -> &'static str {
        "MonthVMTFraction"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("monthVMTFraction".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "monthID".into(),
                    rows.iter().map(|r| r.month_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "monthVMTFraction".into(),
                    rows.iter()
                        .map(|r| r.month_vmt_fraction)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "MonthVMTFraction";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(T, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(T, 0, col, e.to_string()))
        };
        let source_type_id = get_i32("sourceTypeID")?;
        let month_id = get_i32("monthID")?;
        let month_vmt_fraction = df
            .column("monthVMTFraction")
            .map_err(|e| row_err(T, 0, "monthVMTFraction", e.to_string()))?
            .f64()
            .map_err(|e| row_err(T, 0, "monthVMTFraction", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(T, i, col, "null value".into());
                Ok(inputs::MonthVmtFractionRow {
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    month_id: month_id.get(i).ok_or_else(|| null("monthID"))?,
                    month_vmt_fraction: month_vmt_fraction
                        .get(i)
                        .ok_or_else(|| null("monthVMTFraction"))?,
                })
            })
            .collect()
    }
}

impl TableRow for inputs::DayVmtFractionRow {
    fn table_name() -> &'static str {
        "DayVMTFraction"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("roadTypeID".into(), DataType::Int32),
            ("dayID".into(), DataType::Int32),
            ("dayVMTFraction".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "monthID".into(),
                    rows.iter().map(|r| r.month_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "roadTypeID".into(),
                    rows.iter().map(|r| r.road_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "dayID".into(),
                    rows.iter().map(|r| r.day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "dayVMTFraction".into(),
                    rows.iter()
                        .map(|r| r.day_vmt_fraction)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "DayVMTFraction";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(T, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(T, 0, col, e.to_string()))
        };
        let source_type_id = get_i32("sourceTypeID")?;
        let month_id = get_i32("monthID")?;
        let road_type_id = get_i32("roadTypeID")?;
        let day_id = get_i32("dayID")?;
        let day_vmt_fraction = df
            .column("dayVMTFraction")
            .map_err(|e| row_err(T, 0, "dayVMTFraction", e.to_string()))?
            .f64()
            .map_err(|e| row_err(T, 0, "dayVMTFraction", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(T, i, col, "null value".into());
                Ok(inputs::DayVmtFractionRow {
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    month_id: month_id.get(i).ok_or_else(|| null("monthID"))?,
                    road_type_id: road_type_id.get(i).ok_or_else(|| null("roadTypeID"))?,
                    day_id: day_id.get(i).ok_or_else(|| null("dayID"))?,
                    day_vmt_fraction: day_vmt_fraction
                        .get(i)
                        .ok_or_else(|| null("dayVMTFraction"))?,
                })
            })
            .collect()
    }
}

impl TableRow for inputs::HourVmtFractionRow {
    fn table_name() -> &'static str {
        "HourVMTFraction"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("roadTypeID".into(), DataType::Int32),
            ("dayID".into(), DataType::Int32),
            ("hourID".into(), DataType::Int32),
            ("hourVMTFraction".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "roadTypeID".into(),
                    rows.iter().map(|r| r.road_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "dayID".into(),
                    rows.iter().map(|r| r.day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "hourID".into(),
                    rows.iter().map(|r| r.hour_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "hourVMTFraction".into(),
                    rows.iter()
                        .map(|r| r.hour_vmt_fraction)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "HourVMTFraction";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(T, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(T, 0, col, e.to_string()))
        };
        let source_type_id = get_i32("sourceTypeID")?;
        let road_type_id = get_i32("roadTypeID")?;
        let day_id = get_i32("dayID")?;
        let hour_id = get_i32("hourID")?;
        let hour_vmt_fraction = df
            .column("hourVMTFraction")
            .map_err(|e| row_err(T, 0, "hourVMTFraction", e.to_string()))?
            .f64()
            .map_err(|e| row_err(T, 0, "hourVMTFraction", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(T, i, col, "null value".into());
                Ok(inputs::HourVmtFractionRow {
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    road_type_id: road_type_id.get(i).ok_or_else(|| null("roadTypeID"))?,
                    day_id: day_id.get(i).ok_or_else(|| null("dayID"))?,
                    hour_id: hour_id.get(i).ok_or_else(|| null("hourID"))?,
                    hour_vmt_fraction: hour_vmt_fraction
                        .get(i)
                        .ok_or_else(|| null("hourVMTFraction"))?,
                })
            })
            .collect()
    }
}

impl TableRow for inputs::HourDayRow {
    fn table_name() -> &'static str {
        "HourDay"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("hourDayID".into(), DataType::Int32),
            ("hourID".into(), DataType::Int32),
            ("dayID".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "hourDayID".into(),
                    rows.iter().map(|r| r.hour_day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "hourID".into(),
                    rows.iter().map(|r| r.hour_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "dayID".into(),
                    rows.iter().map(|r| r.day_id).collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "HourDay";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(T, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(T, 0, col, e.to_string()))
        };
        let hour_day_id = get_i32("hourDayID")?;
        let hour_id = get_i32("hourID")?;
        let day_id = get_i32("dayID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(T, i, col, "null value".into());
                Ok(inputs::HourDayRow {
                    hour_day_id: hour_day_id.get(i).ok_or_else(|| null("hourDayID"))?,
                    hour_id: hour_id.get(i).ok_or_else(|| null("hourID"))?,
                    day_id: day_id.get(i).ok_or_else(|| null("dayID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for inputs::DayOfAnyWeekRow {
    fn table_name() -> &'static str {
        "DayOfAnyWeek"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("dayID".into(), DataType::Int32),
            ("noOfRealDays".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "dayID".into(),
                    rows.iter().map(|r| r.day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "noOfRealDays".into(),
                    rows.iter().map(|r| r.no_of_real_days).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "DayOfAnyWeek";
        let day_id = df
            .column("dayID")
            .map_err(|e| row_err(T, 0, "dayID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(T, 0, "dayID", e.to_string()))?;
        let no_of_real_days = df
            .column("noOfRealDays")
            .map_err(|e| row_err(T, 0, "noOfRealDays", e.to_string()))?
            .f64()
            .map_err(|e| row_err(T, 0, "noOfRealDays", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(T, i, col, "null value".into());
                Ok(inputs::DayOfAnyWeekRow {
                    day_id: day_id.get(i).ok_or_else(|| null("dayID"))?,
                    no_of_real_days: no_of_real_days.get(i).ok_or_else(|| null("noOfRealDays"))?,
                })
            })
            .collect()
    }
}

impl TableRow for inputs::MonthOfAnyYearRow {
    fn table_name() -> &'static str {
        "MonthOfAnyYear"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("monthID".into(), DataType::Int32),
            ("noOfDays".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "monthID".into(),
                    rows.iter().map(|r| r.month_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "noOfDays".into(),
                    rows.iter().map(|r| r.no_of_days).collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "MonthOfAnyYear";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(T, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(T, 0, col, e.to_string()))
        };
        let month_id = get_i32("monthID")?;
        let no_of_days = get_i32("noOfDays")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(T, i, col, "null value".into());
                Ok(inputs::MonthOfAnyYearRow {
                    month_id: month_id.get(i).ok_or_else(|| null("monthID"))?,
                    no_of_days: no_of_days.get(i).ok_or_else(|| null("noOfDays"))?,
                })
            })
            .collect()
    }
}

impl TableRow for inputs::SourceTypeHourRow {
    fn table_name() -> &'static str {
        "SourceTypeHour"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("hourDayID".into(), DataType::Int32),
            ("idleSHOFactor".into(), DataType::Float64),
            ("hotellingDist".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "hourDayID".into(),
                    rows.iter().map(|r| r.hour_day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "idleSHOFactor".into(),
                    rows.iter().map(|r| r.idle_sho_factor).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "hotellingDist".into(),
                    rows.iter().map(|r| r.hotelling_dist).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "SourceTypeHour";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(T, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(T, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(T, 0, col, e.to_string()))?
                .f64()
                .map_err(|e| row_err(T, 0, col, e.to_string()))
        };
        let source_type_id = get_i32("sourceTypeID")?;
        let hour_day_id = get_i32("hourDayID")?;
        let idle_sho_factor = get_f64("idleSHOFactor")?;
        let hotelling_dist = get_f64("hotellingDist")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(T, i, col, "null value".into());
                Ok(inputs::SourceTypeHourRow {
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    hour_day_id: hour_day_id.get(i).ok_or_else(|| null("hourDayID"))?,
                    idle_sho_factor: idle_sho_factor
                        .get(i)
                        .ok_or_else(|| null("idleSHOFactor"))?,
                    hotelling_dist: hotelling_dist.get(i).ok_or_else(|| null("hotellingDist"))?,
                })
            })
            .collect()
    }
}

impl TableRow for inputs::RunSpecDayRow {
    fn table_name() -> &'static str {
        "RunSpecDay"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([("dayID".into(), DataType::Int32)])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![Series::new(
                "dayID".into(),
                rows.iter().map(|r| r.day_id).collect::<Vec<i32>>(),
            )
            .into()],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "RunSpecDay";
        let day_id = df
            .column("dayID")
            .map_err(|e| row_err(T, 0, "dayID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(T, 0, "dayID", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(T, i, col, "null value".into());
                Ok(inputs::RunSpecDayRow {
                    day_id: day_id.get(i).ok_or_else(|| null("dayID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for inputs::AvgSpeedBinRow {
    fn table_name() -> &'static str {
        "AvgSpeedBin"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("avgSpeedBinID".into(), DataType::Int32),
            ("avgBinSpeed".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "avgSpeedBinID".into(),
                    rows.iter()
                        .map(|r| r.avg_speed_bin_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "avgBinSpeed".into(),
                    rows.iter().map(|r| r.avg_bin_speed).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "AvgSpeedBin";
        let avg_speed_bin_id = df
            .column("avgSpeedBinID")
            .map_err(|e| row_err(T, 0, "avgSpeedBinID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(T, 0, "avgSpeedBinID", e.to_string()))?;
        let avg_bin_speed = df
            .column("avgBinSpeed")
            .map_err(|e| row_err(T, 0, "avgBinSpeed", e.to_string()))?
            .f64()
            .map_err(|e| row_err(T, 0, "avgBinSpeed", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(T, i, col, "null value".into());
                Ok(inputs::AvgSpeedBinRow {
                    avg_speed_bin_id: avg_speed_bin_id
                        .get(i)
                        .ok_or_else(|| null("avgSpeedBinID"))?,
                    avg_bin_speed: avg_bin_speed.get(i).ok_or_else(|| null("avgBinSpeed"))?,
                })
            })
            .collect()
    }
}

impl TableRow for inputs::AvgSpeedDistributionRow {
    fn table_name() -> &'static str {
        "AvgSpeedDistribution"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("roadTypeID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("hourDayID".into(), DataType::Int32),
            ("avgSpeedBinID".into(), DataType::Int32),
            ("avgSpeedFraction".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "roadTypeID".into(),
                    rows.iter().map(|r| r.road_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "hourDayID".into(),
                    rows.iter().map(|r| r.hour_day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "avgSpeedBinID".into(),
                    rows.iter()
                        .map(|r| r.avg_speed_bin_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "avgSpeedFraction".into(),
                    rows.iter()
                        .map(|r| r.avg_speed_fraction)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "AvgSpeedDistribution";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(T, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(T, 0, col, e.to_string()))
        };
        let road_type_id = get_i32("roadTypeID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let hour_day_id = get_i32("hourDayID")?;
        let avg_speed_bin_id = get_i32("avgSpeedBinID")?;
        let avg_speed_fraction = df
            .column("avgSpeedFraction")
            .map_err(|e| row_err(T, 0, "avgSpeedFraction", e.to_string()))?
            .f64()
            .map_err(|e| row_err(T, 0, "avgSpeedFraction", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(T, i, col, "null value".into());
                Ok(inputs::AvgSpeedDistributionRow {
                    road_type_id: road_type_id.get(i).ok_or_else(|| null("roadTypeID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    hour_day_id: hour_day_id.get(i).ok_or_else(|| null("hourDayID"))?,
                    avg_speed_bin_id: avg_speed_bin_id
                        .get(i)
                        .ok_or_else(|| null("avgSpeedBinID"))?,
                    avg_speed_fraction: avg_speed_fraction
                        .get(i)
                        .ok_or_else(|| null("avgSpeedFraction"))?,
                })
            })
            .collect()
    }
}

impl TableRow for inputs::HourOfAnyDayRow {
    fn table_name() -> &'static str {
        "HourOfAnyDay"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([("hourID".into(), DataType::Int32)])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![Series::new(
                "hourID".into(),
                rows.iter().map(|r| r.hour_id).collect::<Vec<i32>>(),
            )
            .into()],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "HourOfAnyDay";
        let hour_id = df
            .column("hourID")
            .map_err(|e| row_err(T, 0, "hourID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(T, 0, "hourID", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(T, i, col, "null value".into());
                Ok(inputs::HourOfAnyDayRow {
                    hour_id: hour_id.get(i).ok_or_else(|| null("hourID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for inputs::ZoneRoadTypeRow {
    fn table_name() -> &'static str {
        "ZoneRoadType"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("zoneID".into(), DataType::Int32),
            ("roadTypeID".into(), DataType::Int32),
            ("SHOAllocFactor".into(), DataType::Float64),
            ("SHPAllocFactor".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "zoneID".into(),
                    rows.iter().map(|r| r.zone_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "roadTypeID".into(),
                    rows.iter().map(|r| r.road_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "SHOAllocFactor".into(),
                    rows.iter()
                        .map(|r| r.sho_alloc_factor)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "SHPAllocFactor".into(),
                    rows.iter()
                        .map(|r| r.shp_alloc_factor)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "ZoneRoadType";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(T, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(T, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(T, 0, col, e.to_string()))?
                .f64()
                .map_err(|e| row_err(T, 0, col, e.to_string()))
        };
        let zone_id = get_i32("zoneID")?;
        let road_type_id = get_i32("roadTypeID")?;
        let sho_alloc_factor = get_f64("SHOAllocFactor")?;
        // Canonical MOVES `ZoneRoadType` (CreateDefault.sql: zoneID, roadTypeID,
        // SHOAllocFactor only) has no SHPAllocFactor column; SHPAllocFactor lives on the
        // `Zone` table and is read from ZoneRow at allocation time (allocation.rs SHP
        // step, matching TotalActivityGenerator.java step 190 `INNER JOIN Zone z`). This
        // ZoneRoadType field is vestigial, so an absent column is faithful and defaults to
        // 0.0; but if the column is present it must be the correct type — surface a
        // mistyped column rather than silently zeroing every row.
        let shp_opt = match df.column("SHPAllocFactor") {
            Ok(s) => Some(
                s.f64()
                    .map_err(|e| row_err(T, 0, "SHPAllocFactor", e.to_string()))?,
            ),
            Err(_) => None,
        };
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(T, i, col, "null value".into());
                Ok(inputs::ZoneRoadTypeRow {
                    zone_id: zone_id.get(i).ok_or_else(|| null("zoneID"))?,
                    road_type_id: road_type_id.get(i).ok_or_else(|| null("roadTypeID"))?,
                    sho_alloc_factor: sho_alloc_factor
                        .get(i)
                        .ok_or_else(|| null("SHOAllocFactor"))?,
                    shp_alloc_factor: shp_opt.as_ref().and_then(|ca| ca.get(i)).unwrap_or(0.0),
                })
            })
            .collect()
    }
}

impl TableRow for inputs::HotellingCalendarYearRow {
    fn table_name() -> &'static str {
        "hotellingCalendarYear"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("yearID".into(), DataType::Int32),
            ("hotellingRate".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "yearID".into(),
                    rows.iter().map(|r| r.year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "hotellingRate".into(),
                    rows.iter().map(|r| r.hotelling_rate).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "hotellingCalendarYear";
        let year_id = df
            .column("yearID")
            .map_err(|e| row_err(T, 0, "yearID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(T, 0, "yearID", e.to_string()))?;
        let hotelling_rate = df
            .column("hotellingRate")
            .map_err(|e| row_err(T, 0, "hotellingRate", e.to_string()))?
            .f64()
            .map_err(|e| row_err(T, 0, "hotellingRate", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(T, i, col, "null value".into());
                Ok(inputs::HotellingCalendarYearRow {
                    year_id: year_id.get(i).ok_or_else(|| null("yearID"))?,
                    hotelling_rate: hotelling_rate.get(i).ok_or_else(|| null("hotellingRate"))?,
                })
            })
            .collect()
    }
}

impl TableRow for inputs::SampleVehicleDayRow {
    fn table_name() -> &'static str {
        "SampleVehicleDay"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("vehID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("dayID".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "vehID".into(),
                    rows.iter().map(|r| r.veh_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "dayID".into(),
                    rows.iter().map(|r| r.day_id).collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "SampleVehicleDay";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(T, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(T, 0, col, e.to_string()))
        };
        let veh_id = get_i32("vehID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let day_id = get_i32("dayID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(T, i, col, "null value".into());
                Ok(inputs::SampleVehicleDayRow {
                    veh_id: veh_id.get(i).ok_or_else(|| null("vehID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    day_id: day_id.get(i).ok_or_else(|| null("dayID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for inputs::SampleVehicleTripRow {
    fn table_name() -> &'static str {
        "SampleVehicleTrip"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("vehID".into(), DataType::Int32),
            ("dayID".into(), DataType::Int32),
            ("hourID".into(), DataType::Int32),
            ("keyOnTime".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "vehID".into(),
                    rows.iter().map(|r| r.veh_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "dayID".into(),
                    rows.iter().map(|r| r.day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "hourID".into(),
                    rows.iter().map(|r| r.hour_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "keyOnTime".into(),
                    rows.iter()
                        .map(|r| if r.has_key_on_time { Some(1i32) } else { None })
                        .collect::<Vec<Option<i32>>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "SampleVehicleTrip";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(T, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(T, 0, col, e.to_string()))
        };
        let veh_id = get_i32("vehID")?;
        let day_id = get_i32("dayID")?;
        let hour_id = get_i32("hourID")?;
        let key_on_time = get_i32("keyOnTime")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(T, i, col, "null value".into());
                Ok(inputs::SampleVehicleTripRow {
                    veh_id: veh_id.get(i).ok_or_else(|| null("vehID"))?,
                    day_id: day_id.get(i).ok_or_else(|| null("dayID"))?,
                    hour_id: hour_id.get(i).ok_or_else(|| null("hourID"))?,
                    has_key_on_time: key_on_time.get(i).is_some(),
                })
            })
            .collect()
    }
}

impl TableRow for inputs::StartsPerVehicleRow {
    fn table_name() -> &'static str {
        "StartsPerVehicle"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("hourDayID".into(), DataType::Int32),
            ("startsPerVehicle".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "hourDayID".into(),
                    rows.iter().map(|r| r.hour_day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "startsPerVehicle".into(),
                    rows.iter()
                        .map(|r| r.starts_per_vehicle)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "StartsPerVehicle";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(T, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(T, 0, col, e.to_string()))
        };
        let source_type_id = get_i32("sourceTypeID")?;
        let hour_day_id = get_i32("hourDayID")?;
        let starts_per_vehicle = df
            .column("startsPerVehicle")
            .map_err(|e| row_err(T, 0, "startsPerVehicle", e.to_string()))?
            .f64()
            .map_err(|e| row_err(T, 0, "startsPerVehicle", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(T, i, col, "null value".into());
                Ok(inputs::StartsPerVehicleRow {
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    hour_day_id: hour_day_id.get(i).ok_or_else(|| null("hourDayID"))?,
                    starts_per_vehicle: starts_per_vehicle
                        .get(i)
                        .ok_or_else(|| null("startsPerVehicle"))?,
                })
            })
            .collect()
    }
}

// ── TableRow implementations — model (outputs) ───────────────────────────────

impl TableRow for model::SourceTypeAgePopulationRow {
    fn table_name() -> &'static str {
        "SourceTypeAgePopulation"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("yearID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
            ("population".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "yearID".into(),
                    rows.iter().map(|r| r.year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ageID".into(),
                    rows.iter().map(|r| r.age_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "population".into(),
                    rows.iter().map(|r| r.population).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "SourceTypeAgePopulation";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(T, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(T, 0, col, e.to_string()))
        };
        let year_id = get_i32("yearID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let age_id = get_i32("ageID")?;
        let population = df
            .column("population")
            .map_err(|e| row_err(T, 0, "population", e.to_string()))?
            .f64()
            .map_err(|e| row_err(T, 0, "population", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(T, i, col, "null value".into());
                Ok(model::SourceTypeAgePopulationRow {
                    year_id: year_id.get(i).ok_or_else(|| null("yearID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    age_id: age_id.get(i).ok_or_else(|| null("ageID"))?,
                    population: population.get(i).ok_or_else(|| null("population"))?,
                })
            })
            .collect()
    }
}

impl TableRow for model::TravelFractionRow {
    fn table_name() -> &'static str {
        "TravelFraction"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("yearID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
            ("fraction".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "yearID".into(),
                    rows.iter().map(|r| r.year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ageID".into(),
                    rows.iter().map(|r| r.age_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fraction".into(),
                    rows.iter().map(|r| r.fraction).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "TravelFraction";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(T, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(T, 0, col, e.to_string()))
        };
        let year_id = get_i32("yearID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let age_id = get_i32("ageID")?;
        let fraction = df
            .column("fraction")
            .map_err(|e| row_err(T, 0, "fraction", e.to_string()))?
            .f64()
            .map_err(|e| row_err(T, 0, "fraction", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(T, i, col, "null value".into());
                Ok(model::TravelFractionRow {
                    year_id: year_id.get(i).ok_or_else(|| null("yearID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    age_id: age_id.get(i).ok_or_else(|| null("ageID"))?,
                    fraction: fraction.get(i).ok_or_else(|| null("fraction"))?,
                })
            })
            .collect()
    }
}

impl TableRow for model::AnalysisYearVmtRow {
    fn table_name() -> &'static str {
        "AnalysisYearVMT"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("yearID".into(), DataType::Int32),
            ("HPMSVTypeID".into(), DataType::Int32),
            ("VMT".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "yearID".into(),
                    rows.iter().map(|r| r.year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "HPMSVTypeID".into(),
                    rows.iter().map(|r| r.hpms_v_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "VMT".into(),
                    rows.iter().map(|r| r.vmt).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "AnalysisYearVMT";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(T, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(T, 0, col, e.to_string()))
        };
        let year_id = get_i32("yearID")?;
        let hpms_v_type_id = get_i32("HPMSVTypeID")?;
        let vmt = df
            .column("VMT")
            .map_err(|e| row_err(T, 0, "VMT", e.to_string()))?
            .f64()
            .map_err(|e| row_err(T, 0, "VMT", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(T, i, col, "null value".into());
                Ok(model::AnalysisYearVmtRow {
                    year_id: year_id.get(i).ok_or_else(|| null("yearID"))?,
                    hpms_v_type_id: hpms_v_type_id.get(i).ok_or_else(|| null("HPMSVTypeID"))?,
                    vmt: vmt.get(i).ok_or_else(|| null("VMT"))?,
                })
            })
            .collect()
    }
}

impl TableRow for model::AnnualVmtByAgeRoadwayRow {
    fn table_name() -> &'static str {
        "AnnualVMTByAgeRoadway"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("yearID".into(), DataType::Int32),
            ("roadTypeID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
            ("VMT".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "yearID".into(),
                    rows.iter().map(|r| r.year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "roadTypeID".into(),
                    rows.iter().map(|r| r.road_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ageID".into(),
                    rows.iter().map(|r| r.age_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "VMT".into(),
                    rows.iter().map(|r| r.vmt).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "AnnualVMTByAgeRoadway";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(T, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(T, 0, col, e.to_string()))
        };
        let year_id = get_i32("yearID")?;
        let road_type_id = get_i32("roadTypeID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let age_id = get_i32("ageID")?;
        let vmt = df
            .column("VMT")
            .map_err(|e| row_err(T, 0, "VMT", e.to_string()))?
            .f64()
            .map_err(|e| row_err(T, 0, "VMT", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(T, i, col, "null value".into());
                Ok(model::AnnualVmtByAgeRoadwayRow {
                    year_id: year_id.get(i).ok_or_else(|| null("yearID"))?,
                    road_type_id: road_type_id.get(i).ok_or_else(|| null("roadTypeID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    age_id: age_id.get(i).ok_or_else(|| null("ageID"))?,
                    vmt: vmt.get(i).ok_or_else(|| null("VMT"))?,
                })
            })
            .collect()
    }
}

impl TableRow for model::VmtByAgeRoadwayHourRow {
    fn table_name() -> &'static str {
        "VMTByAgeRoadwayHour"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("yearID".into(), DataType::Int32),
            ("roadTypeID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("dayID".into(), DataType::Int32),
            ("hourID".into(), DataType::Int32),
            ("hourDayID".into(), DataType::Int32),
            ("VMT".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "yearID".into(),
                    rows.iter().map(|r| r.year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "roadTypeID".into(),
                    rows.iter().map(|r| r.road_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ageID".into(),
                    rows.iter().map(|r| r.age_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "monthID".into(),
                    rows.iter().map(|r| r.month_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "dayID".into(),
                    rows.iter().map(|r| r.day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "hourID".into(),
                    rows.iter().map(|r| r.hour_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "hourDayID".into(),
                    rows.iter().map(|r| r.hour_day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "VMT".into(),
                    rows.iter().map(|r| r.vmt).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "VMTByAgeRoadwayHour";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(T, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(T, 0, col, e.to_string()))
        };
        let year_id = get_i32("yearID")?;
        let road_type_id = get_i32("roadTypeID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let age_id = get_i32("ageID")?;
        let month_id = get_i32("monthID")?;
        let day_id = get_i32("dayID")?;
        let hour_id = get_i32("hourID")?;
        let hour_day_id = get_i32("hourDayID")?;
        let vmt = df
            .column("VMT")
            .map_err(|e| row_err(T, 0, "VMT", e.to_string()))?
            .f64()
            .map_err(|e| row_err(T, 0, "VMT", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(T, i, col, "null value".into());
                Ok(model::VmtByAgeRoadwayHourRow {
                    year_id: year_id.get(i).ok_or_else(|| null("yearID"))?,
                    road_type_id: road_type_id.get(i).ok_or_else(|| null("roadTypeID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    age_id: age_id.get(i).ok_or_else(|| null("ageID"))?,
                    month_id: month_id.get(i).ok_or_else(|| null("monthID"))?,
                    day_id: day_id.get(i).ok_or_else(|| null("dayID"))?,
                    hour_id: hour_id.get(i).ok_or_else(|| null("hourID"))?,
                    hour_day_id: hour_day_id.get(i).ok_or_else(|| null("hourDayID"))?,
                    vmt: vmt.get(i).ok_or_else(|| null("VMT"))?,
                })
            })
            .collect()
    }
}

impl TableRow for model::VmtByMyRoadHourFractionRow {
    fn table_name() -> &'static str {
        "vmtByMYRoadHourFraction"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("yearID".into(), DataType::Int32),
            ("roadTypeID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("modelYearID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("hourID".into(), DataType::Int32),
            ("dayID".into(), DataType::Int32),
            ("hourDayID".into(), DataType::Int32),
            ("vmtFraction".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "yearID".into(),
                    rows.iter().map(|r| r.year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "roadTypeID".into(),
                    rows.iter().map(|r| r.road_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "modelYearID".into(),
                    rows.iter().map(|r| r.model_year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "monthID".into(),
                    rows.iter().map(|r| r.month_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "hourID".into(),
                    rows.iter().map(|r| r.hour_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "dayID".into(),
                    rows.iter().map(|r| r.day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "hourDayID".into(),
                    rows.iter().map(|r| r.hour_day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "vmtFraction".into(),
                    rows.iter().map(|r| r.vmt_fraction).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "vmtByMYRoadHourFraction";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(T, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(T, 0, col, e.to_string()))
        };
        let year_id = get_i32("yearID")?;
        let road_type_id = get_i32("roadTypeID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let model_year_id = get_i32("modelYearID")?;
        let month_id = get_i32("monthID")?;
        let hour_id = get_i32("hourID")?;
        let day_id = get_i32("dayID")?;
        let hour_day_id = get_i32("hourDayID")?;
        let vmt_fraction = df
            .column("vmtFraction")
            .map_err(|e| row_err(T, 0, "vmtFraction", e.to_string()))?
            .f64()
            .map_err(|e| row_err(T, 0, "vmtFraction", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(T, i, col, "null value".into());
                Ok(model::VmtByMyRoadHourFractionRow {
                    year_id: year_id.get(i).ok_or_else(|| null("yearID"))?,
                    road_type_id: road_type_id.get(i).ok_or_else(|| null("roadTypeID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    model_year_id: model_year_id.get(i).ok_or_else(|| null("modelYearID"))?,
                    month_id: month_id.get(i).ok_or_else(|| null("monthID"))?,
                    hour_id: hour_id.get(i).ok_or_else(|| null("hourID"))?,
                    day_id: day_id.get(i).ok_or_else(|| null("dayID"))?,
                    hour_day_id: hour_day_id.get(i).ok_or_else(|| null("hourDayID"))?,
                    vmt_fraction: vmt_fraction.get(i).ok_or_else(|| null("vmtFraction"))?,
                })
            })
            .collect()
    }
}

impl TableRow for model::ShoByAgeRoadwayHourRow {
    fn table_name() -> &'static str {
        "SHOByAgeRoadwayHour"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("yearID".into(), DataType::Int32),
            ("roadTypeID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("dayID".into(), DataType::Int32),
            ("hourID".into(), DataType::Int32),
            ("hourDayID".into(), DataType::Int32),
            ("SHO".into(), DataType::Float64),
            ("VMT".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "yearID".into(),
                    rows.iter().map(|r| r.year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "roadTypeID".into(),
                    rows.iter().map(|r| r.road_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ageID".into(),
                    rows.iter().map(|r| r.age_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "monthID".into(),
                    rows.iter().map(|r| r.month_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "dayID".into(),
                    rows.iter().map(|r| r.day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "hourID".into(),
                    rows.iter().map(|r| r.hour_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "hourDayID".into(),
                    rows.iter().map(|r| r.hour_day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "SHO".into(),
                    rows.iter().map(|r| r.sho).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "VMT".into(),
                    rows.iter().map(|r| r.vmt).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "SHOByAgeRoadwayHour";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(T, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(T, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(T, 0, col, e.to_string()))?
                .f64()
                .map_err(|e| row_err(T, 0, col, e.to_string()))
        };
        let year_id = get_i32("yearID")?;
        let road_type_id = get_i32("roadTypeID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let age_id = get_i32("ageID")?;
        let month_id = get_i32("monthID")?;
        let day_id = get_i32("dayID")?;
        let hour_id = get_i32("hourID")?;
        let hour_day_id = get_i32("hourDayID")?;
        let sho = get_f64("SHO")?;
        let vmt = get_f64("VMT")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(T, i, col, "null value".into());
                Ok(model::ShoByAgeRoadwayHourRow {
                    year_id: year_id.get(i).ok_or_else(|| null("yearID"))?,
                    road_type_id: road_type_id.get(i).ok_or_else(|| null("roadTypeID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    age_id: age_id.get(i).ok_or_else(|| null("ageID"))?,
                    month_id: month_id.get(i).ok_or_else(|| null("monthID"))?,
                    day_id: day_id.get(i).ok_or_else(|| null("dayID"))?,
                    hour_id: hour_id.get(i).ok_or_else(|| null("hourID"))?,
                    hour_day_id: hour_day_id.get(i).ok_or_else(|| null("hourDayID"))?,
                    sho: sho.get(i).ok_or_else(|| null("SHO"))?,
                    vmt: vmt.get(i).ok_or_else(|| null("VMT"))?,
                })
            })
            .collect()
    }
}

impl TableRow for model::VmtByAgeRoadwayDayRow {
    fn table_name() -> &'static str {
        "VMTByAgeRoadwayDay"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("yearID".into(), DataType::Int32),
            ("roadTypeID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("dayID".into(), DataType::Int32),
            ("VMT".into(), DataType::Float64),
            ("hotellingHours".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "yearID".into(),
                    rows.iter().map(|r| r.year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "roadTypeID".into(),
                    rows.iter().map(|r| r.road_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ageID".into(),
                    rows.iter().map(|r| r.age_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "monthID".into(),
                    rows.iter().map(|r| r.month_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "dayID".into(),
                    rows.iter().map(|r| r.day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "VMT".into(),
                    rows.iter().map(|r| r.vmt).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "hotellingHours".into(),
                    rows.iter().map(|r| r.hotelling_hours).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "VMTByAgeRoadwayDay";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(T, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(T, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(T, 0, col, e.to_string()))?
                .f64()
                .map_err(|e| row_err(T, 0, col, e.to_string()))
        };
        let year_id = get_i32("yearID")?;
        let road_type_id = get_i32("roadTypeID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let age_id = get_i32("ageID")?;
        let month_id = get_i32("monthID")?;
        let day_id = get_i32("dayID")?;
        let vmt = get_f64("VMT")?;
        let hotelling_hours = get_f64("hotellingHours")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(T, i, col, "null value".into());
                Ok(model::VmtByAgeRoadwayDayRow {
                    year_id: year_id.get(i).ok_or_else(|| null("yearID"))?,
                    road_type_id: road_type_id.get(i).ok_or_else(|| null("roadTypeID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    age_id: age_id.get(i).ok_or_else(|| null("ageID"))?,
                    month_id: month_id.get(i).ok_or_else(|| null("monthID"))?,
                    day_id: day_id.get(i).ok_or_else(|| null("dayID"))?,
                    vmt: vmt.get(i).ok_or_else(|| null("VMT"))?,
                    hotelling_hours: hotelling_hours
                        .get(i)
                        .ok_or_else(|| null("hotellingHours"))?,
                })
            })
            .collect()
    }
}

impl TableRow for model::IdleHoursByAgeHourRow {
    fn table_name() -> &'static str {
        "IdleHoursByAgeHour"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("yearID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("dayID".into(), DataType::Int32),
            ("hourID".into(), DataType::Int32),
            ("idleHours".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "yearID".into(),
                    rows.iter().map(|r| r.year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ageID".into(),
                    rows.iter().map(|r| r.age_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "monthID".into(),
                    rows.iter().map(|r| r.month_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "dayID".into(),
                    rows.iter().map(|r| r.day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "hourID".into(),
                    rows.iter().map(|r| r.hour_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "idleHours".into(),
                    rows.iter().map(|r| r.idle_hours).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "IdleHoursByAgeHour";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(T, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(T, 0, col, e.to_string()))
        };
        let year_id = get_i32("yearID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let age_id = get_i32("ageID")?;
        let month_id = get_i32("monthID")?;
        let day_id = get_i32("dayID")?;
        let hour_id = get_i32("hourID")?;
        let idle_hours = df
            .column("idleHours")
            .map_err(|e| row_err(T, 0, "idleHours", e.to_string()))?
            .f64()
            .map_err(|e| row_err(T, 0, "idleHours", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(T, i, col, "null value".into());
                Ok(model::IdleHoursByAgeHourRow {
                    year_id: year_id.get(i).ok_or_else(|| null("yearID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    age_id: age_id.get(i).ok_or_else(|| null("ageID"))?,
                    month_id: month_id.get(i).ok_or_else(|| null("monthID"))?,
                    day_id: day_id.get(i).ok_or_else(|| null("dayID"))?,
                    hour_id: hour_id.get(i).ok_or_else(|| null("hourID"))?,
                    idle_hours: idle_hours.get(i).ok_or_else(|| null("idleHours"))?,
                })
            })
            .collect()
    }
}

impl TableRow for model::StartsByAgeHourRow {
    fn table_name() -> &'static str {
        "StartsByAgeHour"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("yearID".into(), DataType::Int32),
            ("hourDayID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
            ("starts".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "yearID".into(),
                    rows.iter().map(|r| r.year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "hourDayID".into(),
                    rows.iter().map(|r| r.hour_day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ageID".into(),
                    rows.iter().map(|r| r.age_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "starts".into(),
                    rows.iter().map(|r| r.starts).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "StartsByAgeHour";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(T, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(T, 0, col, e.to_string()))
        };
        let source_type_id = get_i32("sourceTypeID")?;
        let year_id = get_i32("yearID")?;
        let hour_day_id = get_i32("hourDayID")?;
        let age_id = get_i32("ageID")?;
        let starts = df
            .column("starts")
            .map_err(|e| row_err(T, 0, "starts", e.to_string()))?
            .f64()
            .map_err(|e| row_err(T, 0, "starts", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(T, i, col, "null value".into());
                Ok(model::StartsByAgeHourRow {
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    year_id: year_id.get(i).ok_or_else(|| null("yearID"))?,
                    hour_day_id: hour_day_id.get(i).ok_or_else(|| null("hourDayID"))?,
                    age_id: age_id.get(i).ok_or_else(|| null("ageID"))?,
                    starts: starts.get(i).ok_or_else(|| null("starts"))?,
                })
            })
            .collect()
    }
}

impl TableRow for model::ShpByAgeHourRow {
    fn table_name() -> &'static str {
        "SHPByAgeHour"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("yearID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("dayID".into(), DataType::Int32),
            ("hourID".into(), DataType::Int32),
            ("SHP".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "yearID".into(),
                    rows.iter().map(|r| r.year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ageID".into(),
                    rows.iter().map(|r| r.age_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "monthID".into(),
                    rows.iter().map(|r| r.month_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "dayID".into(),
                    rows.iter().map(|r| r.day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "hourID".into(),
                    rows.iter().map(|r| r.hour_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "SHP".into(),
                    rows.iter().map(|r| r.shp).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        const T: &str = "SHPByAgeHour";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(T, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(T, 0, col, e.to_string()))
        };
        let year_id = get_i32("yearID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let age_id = get_i32("ageID")?;
        let month_id = get_i32("monthID")?;
        let day_id = get_i32("dayID")?;
        let hour_id = get_i32("hourID")?;
        let shp = df
            .column("SHP")
            .map_err(|e| row_err(T, 0, "SHP", e.to_string()))?
            .f64()
            .map_err(|e| row_err(T, 0, "SHP", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(T, i, col, "null value".into());
                Ok(model::ShpByAgeHourRow {
                    year_id: year_id.get(i).ok_or_else(|| null("yearID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    age_id: age_id.get(i).ok_or_else(|| null("ageID"))?,
                    month_id: month_id.get(i).ok_or_else(|| null("monthID"))?,
                    day_id: day_id.get(i).ok_or_else(|| null("dayID"))?,
                    hour_id: hour_id.get(i).ok_or_else(|| null("hourID"))?,
                    shp: shp.get(i).ok_or_else(|| null("SHP"))?,
                })
            })
            .collect()
    }
}

/// Stable module name in the calculator-chain DAG.
const GENERATOR_NAME: &str = "TotalActivityGenerator";

/// The processes the Java `subscribeToMe` signs up for, paired with the
/// `MasterLoopPriority` it uses for each.
///
/// Running Exhaust subscribes at `GENERATOR-3` ("Run after BaseRateGenerator"
/// the Java comment); every other process at plain `GENERATOR`.
/// `"Evap Non-Fuel Vapors"` has no row in the MOVES process table, so
/// [`EmissionProcess::find_by_name`] drops it — the exact behaviour of the
/// Java `if (process != null)` guard. The nine that resolve match the
/// `TotalActivityGenerator` subscription set in
/// `characterization/calculator-chains/calculator-dag.json`.
///
/// **Fidelity note.** That DAG, reconstructed from `CalculatorInfo.txt`,
/// records Running Exhaust at plain `GENERATOR`; the Java `subscribeToMe`/// the runtime source of truth — overrides it to `GENERATOR-3` so the
/// generator runs after `BaseRateGenerator` (`GENERATOR-2`). This port
/// follows the Java. generator-integration validation reconciles
/// the metadata.
const SUBSCRIBED_PROCESSES: [(&str, &str); 10] = [
    ("Running Exhaust", "GENERATOR-3"),
    ("Start Exhaust", "GENERATOR"),
    ("Extended Idle Exhaust", "GENERATOR"),
    ("Auxiliary Power Exhaust", "GENERATOR"),
    ("Evap Permeation", "GENERATOR"),
    ("Evap Fuel Vapor Venting", "GENERATOR"),
    ("Evap Fuel Leaks", "GENERATOR"),
    ("Evap Non-Fuel Vapors", "GENERATOR"),
    ("Brakewear", "GENERATOR"),
    ("Tirewear", "GENERATOR"),
];

/// Default-DB and RunSpec tables [`TotalActivityGenerator::run`] reads.
/// Names match the casing used in the MOVES default database.
static INPUT_TABLES: &[&str] = &[
    "Year",
    "SourceTypeYear",
    "SourceTypeAgeDistribution",
    "SourceTypeAge",
    "SourceUseType",
    "HPMSVTypeYear",
    "RunSpecSourceType",
    "SourceTypeYearVMT",
    "RoadType",
    "RoadTypeDistribution",
    "SourceTypeDayVMT",
    "HPMSVTypeDay",
    "MonthVMTFraction",
    "DayVMTFraction",
    "HourVMTFraction",
    "HourDay",
    "DayOfAnyWeek",
    "MonthOfAnyYear",
    "SourceTypeHour",
    "RunSpecDay",
    "AvgSpeedBin",
    "AvgSpeedDistribution",
    "HourOfAnyDay",
    "ZoneRoadType",
    "hotellingCalendarYear",
    "hotellingHoursPerDay",
    "SampleVehicleDay",
    "SampleVehicleTrip",
    "StartsPerVehicle",
];

/// Scratch tables the generator writes for downstream calculators — the
/// year/zone activity tables [`run`](TotalActivityGenerator::run) produces.
/// Their per-link spatial allocation (`SHO`, `SHP`, `SourceHours`,
/// `hotellingHours`) is sequenced by the master loop from the
/// [`allocation`] kernels once the data plane lands.
static OUTPUT_TABLES: &[&str] = &[
    "SourceTypeAgePopulation",
    "SourceTypeAgeDistribution",
    "TravelFraction",
    "AnalysisYearVMT",
    "AnnualVMTByAgeRoadway",
    "VMTByAgeRoadwayHour",
    "vmtByMYRoadHourFraction",
    "SHOByAgeRoadwayHour",
    "VMTByAgeRoadwayDay",
    "IdleHoursByAgeHour",
    "StartsByAgeHour",
    "SHPByAgeHour",
];

/// Resolve [`SUBSCRIBED_PROCESSES`] into the generator's subscription set:
/// every resolvable process, at `YEAR` granularity, with its declared
/// priority.
fn build_subscriptions() -> Vec<CalculatorSubscription> {
    SUBSCRIBED_PROCESSES
        .iter()
        .filter_map(|&(name, priority)| {
            let process = EmissionProcess::find_by_name(name)?;
            let priority =
                Priority::parse(priority).expect("SUBSCRIBED_PROCESSES priorities are well-formed");
            Some(CalculatorSubscription::new(
                process.id,
                Granularity::Year,
                priority,
            ))
        })
        .collect()
}

/// The Total Activity Generator.
///
/// A zero-sized value type: the generator owns no per-run state, exactly as
/// the [`Generator`] trait contract requires. All run-varying input flows
/// through [`TotalActivityInputs`].
#[derive(Debug, Clone, Copy, Default)]
pub struct TotalActivityGenerator;

impl TotalActivityGenerator {
    /// Stable module name — matches the `TotalActivityGenerator` entry in
    /// the calculator-chain DAG.
    pub const NAME: &'static str = GENERATOR_NAME;

    /// Compute the year/zone activity tables — algorithm steps 110-189.
    ///
    /// Ports the year- and zone-scoped body of `executeLoop`: determine the
    /// base year, grow the vehicle population and HPMS-typed VMT forward to
    /// the analysis year, split VMT across road type / source type / age /
    /// hour, and convert it to a total-activity basis (`SHO`, hotelling
    /// hours, starts, `SHP`).
    ///
    /// When no base year is at or below [`TotalActivityInputs::analysis_year`]
    /// the Java logs the failure and abandons the year; this port returns an
    /// empty [`TotalActivityOutput`] in that case.
    #[must_use]
    pub fn run(&self, inputs: &TotalActivityInputs) -> TotalActivityOutput {
        let analysis_year = inputs.analysis_year;

        // Steps 110-139 — population.
        let Some(base_year) = population::determine_base_year(&inputs.year, analysis_year) else {
            return TotalActivityOutput::default();
        };
        let base_population = population::calculate_base_year_population(
            &inputs.source_type_year,
            &inputs.source_type_age_distribution,
            base_year,
        );
        let grown = population::grow_population_to_analysis_year(
            &base_population,
            &inputs.source_type_year,
            &inputs.source_type_age,
            &inputs.source_type_age_distribution,
            base_year,
            analysis_year,
        );

        // Steps 140-159 — HPMS travel fraction and VMT growth.
        let vmt_by_source_type =
            !inputs.source_type_day_vmt.is_empty() || !inputs.source_type_year_vmt.is_empty();
        let travel = travel::calculate_fraction_of_travel_using_hpms(
            &grown.population,
            &inputs.source_use_type,
            &inputs.source_type_age,
            analysis_year,
            vmt_by_source_type,
        );
        let analysis_year_vmt = travel::grow_vmt_to_analysis_year(
            &inputs.hpms_v_type_year,
            &inputs.run_spec_source_type,
            &inputs.source_use_type,
            base_year,
            analysis_year,
        );

        // Steps 160-179 — VMT allocation by road type, source, age, hour.
        let annual_vmt = vmt::allocate_vmt_by_road_type_source_age(
            &travel.travel_fraction,
            &inputs.road_type,
            &inputs.road_type_distribution,
            &analysis_year_vmt,
            &inputs.source_use_type,
            &inputs.source_type_year_vmt,
            analysis_year,
        );
        let from_annual = vmt::hourly_vmt_from_annual(
            &annual_vmt,
            &inputs.month_vmt_fraction,
            &inputs.day_vmt_fraction,
            &inputs.hour_vmt_fraction,
            &inputs.hour_day,
            &inputs.month_of_any_year,
        );
        let daily_tables = vmt::DailyVmtJoinTables {
            road_type_distribution: &inputs.road_type_distribution,
            hour_day: &inputs.hour_day,
            hour_vmt_fraction: &inputs.hour_vmt_fraction,
            travel_fraction: &travel.travel_fraction,
            day_of_any_week: &inputs.day_of_any_week,
        };
        let from_source_type_day = vmt::hourly_vmt_from_source_type_day(
            &inputs.source_type_day_vmt,
            &daily_tables,
            analysis_year,
        );
        let from_hpms_day = vmt::hourly_vmt_from_hpms_day(
            &inputs.hpms_v_type_day,
            &inputs.source_use_type,
            &daily_tables,
            analysis_year,
        );
        let vmt_by_age_roadway_hour =
            vmt::combine_hourly_vmt(from_annual, from_source_type_day, from_hpms_day);
        let vmt_by_my_road_hour_fraction =
            vmt::vmt_by_my_road_hour_fraction(&vmt_by_age_roadway_hour);

        // Steps 180-189 — conversion to total-activity basis.
        let source_type_hour_2 = activity::source_type_hour_expanded(
            &inputs.source_type_hour,
            &inputs.hour_day,
            &inputs.run_spec_day,
        );
        let average_speed = activity::average_speed(
            &inputs.road_type,
            &inputs.run_spec_source_type,
            &inputs.run_spec_day,
            &inputs.hour_of_any_day,
            &inputs.avg_speed_bin,
            &inputs.avg_speed_distribution,
            &inputs.hour_day,
        );
        let sho_by_age_roadway_hour =
            activity::sho_by_age_roadway_hour(&vmt_by_age_roadway_hour, &average_speed);
        let vmt_by_age_roadway_day = activity::vmt_by_age_roadway_day(
            &vmt_by_age_roadway_hour,
            &inputs.zone_road_type,
            &inputs.hotelling_calendar_year,
            inputs.zone_id,
            inputs.has_hotelling_hours_per_day_input,
        );
        let idle_hours_by_age_hour =
            activity::idle_hours_by_age_hour(&vmt_by_age_roadway_day, &source_type_hour_2);
        let starts_per_sample_vehicle = activity::starts_per_sample_vehicle(
            &inputs.sample_vehicle_day,
            &inputs.sample_vehicle_trip,
            &inputs.hour_day,
            &inputs.day_of_any_week,
        );
        let new_starts_per_vehicle = activity::starts_per_vehicle(
            &inputs.sample_vehicle_day,
            &starts_per_sample_vehicle,
            &inputs.starts_per_vehicle,
        );
        // StartsByAgeHour joins the full StartsPerVehicle table — the rows
        // already present plus the ones just computed.
        let mut starts_per_vehicle_full = inputs.starts_per_vehicle.clone();
        starts_per_vehicle_full.extend(new_starts_per_vehicle);
        let starts_by_age_hour =
            activity::starts_by_age_hour(&grown.population, &starts_per_vehicle_full);
        let shp_by_age_hour = activity::shp_by_age_hour(
            &sho_by_age_roadway_hour,
            &grown.population,
            &inputs.day_of_any_week,
        );

        TotalActivityOutput {
            source_type_age_population: grown.population,
            source_type_age_distribution_additions: grown.age_distribution_additions,
            travel_fraction: travel.travel_fraction,
            analysis_year_vmt,
            annual_vmt_by_age_roadway: annual_vmt,
            vmt_by_age_roadway_hour,
            vmt_by_my_road_hour_fraction,
            sho_by_age_roadway_hour,
            vmt_by_age_roadway_day,
            idle_hours_by_age_hour,
            starts_by_age_hour,
            shp_by_age_hour,
        }
    }
}

/// Construct the generator as a boxed trait object — matches the engine's
/// generator-factory signature so the calculator registry can register it.
#[must_use]
pub fn factory() -> Box<dyn Generator> {
    Box::new(TotalActivityGenerator)
}

impl Generator for TotalActivityGenerator {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn subscriptions(&self) -> &[CalculatorSubscription] {
        static SUBSCRIPTIONS: OnceLock<Vec<CalculatorSubscription>> = OnceLock::new();
        SUBSCRIPTIONS.get_or_init(build_subscriptions).as_slice()
    }

    fn input_tables(&self) -> &[&'static str] {
        INPUT_TABLES
    }

    fn output_tables(&self) -> &[&'static str] {
        OUTPUT_TABLES
    }

    /// Execute the generator: read all input tables from `ctx.tables()`,
    /// run the activity-computation chain, and write the 12 output tables
    /// to `ctx.scratch()`.
    fn execute(&self, ctx: &mut CalculatorContext) -> Result<CalculatorOutput, Error> {
        let pos = ctx.position();
        let analysis_year = pos
            .time
            .year
            .ok_or_else(|| Error::Polars("no year in iteration position".into()))
            .map(i32::from)?;
        let zone_id = pos
            .location
            .zone_id
            .ok_or_else(|| Error::Polars("no zone_id in iteration position".into()))
            .map(|z| z as i32)?;

        let has_hotelling = ctx
            .tables()
            .get("hotellingHoursPerDay")
            .is_some_and(|df| df.height() > 0);

        let inputs = TotalActivityInputs {
            analysis_year,
            zone_id,
            has_hotelling_hours_per_day_input: has_hotelling,
            year: ctx.tables().iter_typed("Year")?,
            source_type_year: ctx.tables().iter_typed("SourceTypeYear")?,
            source_type_age_distribution: ctx.tables().iter_typed("SourceTypeAgeDistribution")?,
            source_type_age: ctx.tables().iter_typed("SourceTypeAge")?,
            source_use_type: ctx.tables().iter_typed("SourceUseType")?,
            hpms_v_type_year: ctx.tables().iter_typed("HPMSVTypeYear")?,
            run_spec_source_type: ctx.tables().iter_typed("RunSpecSourceType")?,
            source_type_year_vmt: iter_optional(ctx.tables(), "SourceTypeYearVMT")?,
            road_type: ctx.tables().iter_typed("RoadType")?,
            road_type_distribution: ctx.tables().iter_typed("RoadTypeDistribution")?,
            source_type_day_vmt: iter_optional(ctx.tables(), "SourceTypeDayVMT")?,
            hpms_v_type_day: iter_optional(ctx.tables(), "HPMSVTypeDay")?,
            month_vmt_fraction: ctx.tables().iter_typed("MonthVMTFraction")?,
            day_vmt_fraction: ctx.tables().iter_typed("DayVMTFraction")?,
            hour_vmt_fraction: ctx.tables().iter_typed("HourVMTFraction")?,
            hour_day: ctx.tables().iter_typed("HourDay")?,
            day_of_any_week: ctx.tables().iter_typed("DayOfAnyWeek")?,
            month_of_any_year: ctx.tables().iter_typed("MonthOfAnyYear")?,
            source_type_hour: iter_optional(ctx.tables(), "SourceTypeHour")?,
            run_spec_day: ctx.tables().iter_typed("RunSpecDay")?,
            avg_speed_bin: ctx.tables().iter_typed("AvgSpeedBin")?,
            avg_speed_distribution: ctx.tables().iter_typed("AvgSpeedDistribution")?,
            hour_of_any_day: ctx.tables().iter_typed("HourOfAnyDay")?,
            zone_road_type: iter_optional(ctx.tables(), "ZoneRoadType")?,
            hotelling_calendar_year: iter_optional(ctx.tables(), "hotellingCalendarYear")?,
            sample_vehicle_day: iter_optional(ctx.tables(), "SampleVehicleDay")?,
            sample_vehicle_trip: iter_optional(ctx.tables(), "SampleVehicleTrip")?,
            starts_per_vehicle: iter_optional(ctx.tables(), "StartsPerVehicle")?,
        };

        let output = self.run(&inputs);

        // Write all 12 output tables to scratch.
        macro_rules! write_scratch {
            ($rows:expr, $name:literal) => {{
                let df = $rows
                    .into_dataframe()
                    .map_err(|e| Error::Polars(e.to_string()))?;
                ctx.scratch_mut().store.insert($name, df);
            }};
        }

        write_scratch!(output.source_type_age_population, "SourceTypeAgePopulation");
        write_scratch!(
            output.source_type_age_distribution_additions,
            "SourceTypeAgeDistribution"
        );
        write_scratch!(output.travel_fraction, "TravelFraction");
        write_scratch!(output.analysis_year_vmt, "AnalysisYearVMT");
        write_scratch!(output.annual_vmt_by_age_roadway, "AnnualVMTByAgeRoadway");
        write_scratch!(output.vmt_by_age_roadway_hour, "VMTByAgeRoadwayHour");
        write_scratch!(
            output.vmt_by_my_road_hour_fraction,
            "vmtByMYRoadHourFraction"
        );
        write_scratch!(output.sho_by_age_roadway_hour, "SHOByAgeRoadwayHour");
        write_scratch!(output.vmt_by_age_roadway_day, "VMTByAgeRoadwayDay");
        write_scratch!(output.idle_hours_by_age_hour, "IdleHoursByAgeHour");
        write_scratch!(output.starts_by_age_hour, "StartsByAgeHour");
        write_scratch!(output.shp_by_age_hour, "SHPByAgeHour");

        Ok(CalculatorOutput::empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use moves_data::ProcessId;

    /// Build a minimal one-source-type, one-base-year input that exercises
    /// the population → travel → VMT → activity chain end to end.
    fn minimal_inputs() -> TotalActivityInputs {
        use inputs::{
            AvgSpeedBinRow, AvgSpeedDistributionRow, DayOfAnyWeekRow, DayVmtFractionRow,
            HourDayRow, HourOfAnyDayRow, HourVmtFractionRow, MonthOfAnyYearRow,
            MonthVmtFractionRow, RoadTypeDistributionRow, RoadTypeRow, RunSpecDayRow,
            RunSpecSourceTypeRow, SourceTypeAgeDistributionRow, SourceTypeAgeRow,
            SourceTypeYearRow, SourceTypeYearVmtRow, SourceUseTypeRow, YearRow,
        };

        TotalActivityInputs {
            analysis_year: 2020,
            zone_id: 100,
            has_hotelling_hours_per_day_input: false,
            year: vec![YearRow {
                year_id: 2020,
                is_base_year: true,
            }],
            source_type_year: vec![SourceTypeYearRow {
                year_id: 2020,
                source_type_id: 21,
                source_type_population: 1000.0,
                migration_rate: 1.0,
                sales_growth_factor: 1.0,
            }],
            source_type_age_distribution: vec![
                SourceTypeAgeDistributionRow {
                    source_type_id: 21,
                    year_id: 2020,
                    age_id: 0,
                    age_fraction: 0.6,
                },
                SourceTypeAgeDistributionRow {
                    source_type_id: 21,
                    year_id: 2020,
                    age_id: 1,
                    age_fraction: 0.4,
                },
            ],
            source_type_age: vec![
                SourceTypeAgeRow {
                    source_type_id: 21,
                    age_id: 0,
                    survival_rate: 1.0,
                    relative_mar: 1.0,
                },
                SourceTypeAgeRow {
                    source_type_id: 21,
                    age_id: 1,
                    survival_rate: 1.0,
                    relative_mar: 1.0,
                },
            ],
            source_use_type: vec![SourceUseTypeRow {
                source_type_id: 21,
                hpms_v_type_id: 10,
            }],
            hpms_v_type_year: vec![],
            run_spec_source_type: vec![RunSpecSourceTypeRow { source_type_id: 21 }],
            // VMT supplied by source type.
            source_type_year_vmt: vec![SourceTypeYearVmtRow {
                year_id: 2020,
                source_type_id: 21,
                vmt: 8400.0,
            }],
            road_type: vec![RoadTypeRow { road_type_id: 2 }],
            road_type_distribution: vec![RoadTypeDistributionRow {
                source_type_id: 21,
                road_type_id: 2,
                road_type_vmt_fraction: 1.0,
            }],
            source_type_day_vmt: vec![],
            hpms_v_type_day: vec![],
            month_vmt_fraction: vec![MonthVmtFractionRow {
                source_type_id: 21,
                month_id: 1,
                month_vmt_fraction: 1.0,
            }],
            day_vmt_fraction: vec![DayVmtFractionRow {
                source_type_id: 21,
                month_id: 1,
                road_type_id: 2,
                day_id: 5,
                day_vmt_fraction: 1.0,
            }],
            hour_vmt_fraction: vec![HourVmtFractionRow {
                source_type_id: 21,
                road_type_id: 2,
                day_id: 5,
                hour_id: 8,
                hour_vmt_fraction: 1.0,
            }],
            hour_day: vec![HourDayRow {
                hour_day_id: 85,
                hour_id: 8,
                day_id: 5,
            }],
            day_of_any_week: vec![DayOfAnyWeekRow {
                day_id: 5,
                no_of_real_days: 1.0,
            }],
            month_of_any_year: vec![MonthOfAnyYearRow {
                month_id: 1,
                no_of_days: 7,
            }],
            source_type_hour: vec![],
            run_spec_day: vec![RunSpecDayRow { day_id: 5 }],
            avg_speed_bin: vec![AvgSpeedBinRow {
                avg_speed_bin_id: 1,
                avg_bin_speed: 60.0,
            }],
            avg_speed_distribution: vec![AvgSpeedDistributionRow {
                road_type_id: 2,
                source_type_id: 21,
                hour_day_id: 85,
                avg_speed_bin_id: 1,
                avg_speed_fraction: 1.0,
            }],
            hour_of_any_day: vec![HourOfAnyDayRow { hour_id: 8 }],
            zone_road_type: vec![],
            hotelling_calendar_year: vec![],
            sample_vehicle_day: vec![],
            sample_vehicle_trip: vec![],
            starts_per_vehicle: vec![],
        }
    }

    #[test]
    fn name_matches_dag_module() {
        assert_eq!(TotalActivityGenerator.name(), "TotalActivityGenerator");
    }

    #[test]
    fn subscribes_to_nine_year_granularity_processes() {
        let gen = TotalActivityGenerator;
        let subs = gen.subscriptions();
        // Ten processes are listed; "Evap Non-Fuel Vapors" does not resolve.
        assert_eq!(subs.len(), 9);
        assert!(subs.iter().all(|s| s.granularity == Granularity::Year));
    }

    #[test]
    fn running_exhaust_subscribes_after_baserategenerator() {
        let gen = TotalActivityGenerator;
        // Running Exhaust is processID 1; the Java subscribes it at
        // GENERATOR-3 so it runs after BaseRateGenerator (GENERATOR-2).
        let running = gen
            .subscriptions()
            .iter()
            .find(|s| s.process_id == ProcessId(1))
            .expect("Running Exhaust subscription present");
        assert_eq!(running.priority.display(), "GENERATOR-3");
        // Start Exhaust (processID 2) stays at plain GENERATOR.
        let start = gen
            .subscriptions()
            .iter()
            .find(|s| s.process_id == ProcessId(2))
            .expect("Start Exhaust subscription present");
        assert_eq!(start.priority.display(), "GENERATOR");
    }

    #[test]
    fn output_tables_are_declared() {
        let gen = TotalActivityGenerator;
        assert!(gen.output_tables().contains(&"SHOByAgeRoadwayHour"));
        assert!(gen.output_tables().contains(&"StartsByAgeHour"));
        assert!(gen.input_tables().contains(&"SourceTypeYear"));
    }

    #[test]
    fn execute_writes_activity_tables_to_scratch() {
        use moves_framework::execution::execution_db::{
            ExecutionLocation, ExecutionTime, IterationPosition,
        };
        use moves_framework::{DataFrameStore, InMemoryStore, IntoDataFrame};

        let inp = minimal_inputs();
        let mut store = InMemoryStore::new();

        // Load every required table into the slow-tier store.
        store.insert("Year", inp.year.clone().into_dataframe().unwrap());
        store.insert(
            "SourceTypeYear",
            inp.source_type_year.clone().into_dataframe().unwrap(),
        );
        store.insert(
            "SourceTypeAgeDistribution",
            inp.source_type_age_distribution
                .clone()
                .into_dataframe()
                .unwrap(),
        );
        store.insert(
            "SourceTypeAge",
            inp.source_type_age.clone().into_dataframe().unwrap(),
        );
        store.insert(
            "SourceUseType",
            inp.source_use_type.clone().into_dataframe().unwrap(),
        );
        store.insert(
            "HPMSVTypeYear",
            inp.hpms_v_type_year.clone().into_dataframe().unwrap(),
        );
        store.insert(
            "RunSpecSourceType",
            inp.run_spec_source_type.clone().into_dataframe().unwrap(),
        );
        store.insert(
            "SourceTypeYearVMT",
            inp.source_type_year_vmt.clone().into_dataframe().unwrap(),
        );
        store.insert("RoadType", inp.road_type.clone().into_dataframe().unwrap());
        store.insert(
            "RoadTypeDistribution",
            inp.road_type_distribution.clone().into_dataframe().unwrap(),
        );
        store.insert(
            "MonthVMTFraction",
            inp.month_vmt_fraction.clone().into_dataframe().unwrap(),
        );
        store.insert(
            "DayVMTFraction",
            inp.day_vmt_fraction.clone().into_dataframe().unwrap(),
        );
        store.insert(
            "HourVMTFraction",
            inp.hour_vmt_fraction.clone().into_dataframe().unwrap(),
        );
        store.insert("HourDay", inp.hour_day.clone().into_dataframe().unwrap());
        store.insert(
            "DayOfAnyWeek",
            inp.day_of_any_week.clone().into_dataframe().unwrap(),
        );
        store.insert(
            "MonthOfAnyYear",
            inp.month_of_any_year.clone().into_dataframe().unwrap(),
        );
        store.insert(
            "RunSpecDay",
            inp.run_spec_day.clone().into_dataframe().unwrap(),
        );
        store.insert(
            "AvgSpeedBin",
            inp.avg_speed_bin.clone().into_dataframe().unwrap(),
        );
        store.insert(
            "AvgSpeedDistribution",
            inp.avg_speed_distribution.clone().into_dataframe().unwrap(),
        );
        store.insert(
            "HourOfAnyDay",
            inp.hour_of_any_day.clone().into_dataframe().unwrap(),
        );

        let mut ctx = CalculatorContext::with_position_and_tables(
            IterationPosition {
                iteration: 0,
                process_id: None,
                location: ExecutionLocation {
                    state_id: None,
                    county_id: None,
                    zone_id: Some(100),
                    link_id: None,
                    road_type_id: None,
                },
                time: ExecutionTime {
                    year: Some(2020),
                    month: None,
                    day_id: None,
                    hour: None,
                },
            },
            store,
        );

        let gen = TotalActivityGenerator;
        let result = gen.execute(&mut ctx);
        assert!(result.is_ok(), "execute failed: {:?}", result.err());

        assert!(ctx.scratch().store.contains("SHOByAgeRoadwayHour"));
        assert!(ctx.scratch().store.contains("StartsByAgeHour"));
        assert!(ctx.scratch().store.contains("SHPByAgeHour"));
        assert!(ctx.scratch().store.contains("SourceTypeAgePopulation"));
        assert!(ctx.scratch().store.contains("VMTByAgeRoadwayHour"));
    }

    #[test]
    fn run_without_a_base_year_yields_empty_output() {
        let mut inputs = minimal_inputs();
        // No base year at or below the analysis year.
        inputs.year = vec![inputs::YearRow {
            year_id: 2030,
            is_base_year: true,
        }];
        let out = TotalActivityGenerator.run(&inputs);
        assert_eq!(out, TotalActivityOutput::default());
    }

    #[test]
    fn run_produces_the_activity_chain() {
        let out = TotalActivityGenerator.run(&minimal_inputs());

        // Population: 1000 vehicles split 60/40 across two ages.
        assert_eq!(out.source_type_age_population.len(), 2);
        let age0 = out
            .source_type_age_population
            .iter()
            .find(|r| r.age_id == 0)
            .unwrap();
        assert!((age0.population - 600.0).abs() < 1e-9);

        // VMT flows all the way to the single hour cell:
        // 8400 annual VMT, all on road 2, month/day/hour fractions all 1,
        // 7-day month -> weeksPerMonth 1 -> 8400 hourly VMT.
        assert_eq!(out.vmt_by_age_roadway_hour.len(), 2); // one row per age
        let total_vmt: f64 = out.vmt_by_age_roadway_hour.iter().map(|r| r.vmt).sum();
        assert!((total_vmt - 8400.0).abs() < 1e-6);

        // SHO = VMT / averageSpeed; averageSpeed = 60.
        let total_sho: f64 = out.sho_by_age_roadway_hour.iter().map(|r| r.sho).sum();
        assert!((total_sho - 8400.0 / 60.0).abs() < 1e-6);

        // The analysis-year age distribution was rebuilt.
        assert_eq!(out.source_type_age_distribution_additions.len(), 0);
    }
}
