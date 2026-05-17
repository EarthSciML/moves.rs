//! Output aggregation planning — ports `AggregationSQLGenerator.java`.
//!
//! The Java class builds SQL `INSERT … SELECT … GROUP BY` statements that
//! roll the per-iteration emission, activity, and base-rate tables up to
//! the granularity the RunSpec asks for (county/zone/link, hour/day/month/
//! year, source-type, model-year, …). The Worker side also rescales daily
//! emissions to weekly/monthly totals via a `SUM(metric * factor)` term.
//!
//! In Rust there is no Worker/Master split: a single in-process aggregation
//! pass runs at the end of the master loop, so the three Java code paths
//! collapse to one. The deliverable here is the **column-shape plan** —
//! the list of group-by keys, aggregated-away columns, and `SUM(...)`
//! expressions — for each of the three output tables. Task 26
//! (`OutputProcessor`) consumes an [`AggregationPlan`] and applies it to
//! the concrete DataFrame.
//!
//! The plan is data-only (`&'static str` column names), allocation-light,
//! and pure — every public function below is referentially transparent on
//! its [`AggregationInputs`] argument. That keeps it easy to unit-test
//! without a data plane: a Phase 2 test asserts the GROUP BY columns for
//! a given RunSpec configuration; Phase 3 wires the plan into the real
//! aggregator.
//!
//! See `moves-rust-migration-plan.md`, Task 25.
//!
//! # Mapping from RunSpec
//!
//! [`OutputBreakdown`] flags toggle
//! whether each dimension survives aggregation:
//!
//! | Flag                 | Column kept when `true` |
//! |----------------------|-------------------------|
//! | `emission_process`   | `processID`             |
//! | `fuel_type`          | `fuelTypeID`            |
//! | `model_year`         | `modelYearID`           |
//! | `source_use_type`    | `sourceTypeID`          |
//! | `onroad_scc`         | `SCC`                   |
//! | `road_type`          | `roadTypeID`            |
//! | `hp_class`           | `hpID` (Nonroad only)   |
//!
//! Java-side flags without a Rust-`OutputBreakdown` counterpart
//! (`fuelSubType`, `regClassID`, `engTechID`, `sector`) appear directly on
//! [`AggregationInputs`]; callers thread them in from wherever they live in
//! the run state.
//!
//! [`OutputTimestep`] selects which
//! of `yearID`/`monthID`/`dayID`/`hourID` survive aggregation and whether a
//! day-to-week or week-to-month [`TemporalScaling`] factor multiplies the
//! summed metric.
//!
//! [`GeographicOutputDetail`]
//! plus [`ModelScale`] /
//! [`ModelDomain`] select which of
//! `stateID`/`countyID`/`zoneID`/`linkID` survive aggregation. As in Java
//! line 968–970, a Macroscale non-Project run that asks for `Link`-level
//! output is silently downgraded to `County`.

use moves_runspec::model::{
    GeographicOutputDetail, Model, ModelDomain, ModelScale, OutputBreakdown, OutputTimestep,
};

/// Which output table the aggregation targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregationTable {
    /// `MOVESOutput` — pollutant emission quantities (and per-worker rates).
    Emission,
    /// `MOVESActivityOutput` — distance / hours / starts / population
    /// rows, keyed by `activityTypeID`.
    Activity,
    /// `BaseRateOutput` — rates-mode base rates and emission rates.
    BaseRate,
}

/// Inputs that drive aggregation planning. A bundle so callers can build it
/// once from the run state and reuse it across all three table plans.
///
/// Field naming mirrors the legacy `OutputEmissionsBreakdownSelection`
/// Java toggles; the four `*_id` booleans live here rather than on
/// [`OutputBreakdown`] because
/// the Rust XML schema doesn't (yet) surface them.
#[derive(Debug, Clone, Copy)]
pub struct AggregationInputs<'a> {
    /// Output time granularity (Hour/Day/Month/Year). Controls which of
    /// `yearID`/`monthID`/`dayID`/`hourID` survive and whether a temporal
    /// scaling factor multiplies the summed metric.
    pub timestep: OutputTimestep,
    /// Geographic output detail level (Nation/State/County/Zone/Link).
    pub geographic_output_detail: GeographicOutputDetail,
    /// Run scale (Macro/Inventory/Rates).
    pub scale: ModelScale,
    /// Run domain (Default/Single/Project), if any.
    pub domain: Option<ModelDomain>,
    /// Active models (Onroad/Nonroad). The mix changes some defaults.
    pub models: &'a [Model],
    /// Emission breakdown selection.
    pub breakdown: &'a OutputBreakdown,
    /// `outputPopulation` — when true, activity row 6 (population) is
    /// aggregated only by spatial keys, mirroring Java's
    /// `selectActivityNoScaleSQL` path.
    pub output_population: bool,
    /// Java's `regClassID` breakdown flag — keep `regClassID` when true.
    pub reg_class_id: bool,
    /// Java's `fuelSubType` breakdown flag — keep `fuelSubTypeID` when
    /// true. Matched the `ALLOW_FUELSUBTYPE_OUTPUT` compilation flag in
    /// Java.
    pub fuel_sub_type: bool,
    /// Java's `engTechID` breakdown flag — Nonroad-only; keep `engTechID`
    /// when true *and* `models` contains `Nonroad`.
    pub eng_tech_id: bool,
    /// Java's `sector` breakdown flag — Nonroad-only; keep `sectorID`
    /// when true *and* `models` contains `Nonroad`.
    pub sector: bool,
}

/// Multiplier applied to summed metrics when the daily output is rolled up
/// past the day boundary.
///
/// Mirrors Java's `WeeksInMonthHelper` clauses — the actual factor expression
/// is left abstract here so the consumer (Task 26 / `OutputProcessor`) can
/// plug in its own concrete Polars/Arrow expression once the data plane
/// lands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TemporalScaling {
    /// No rescaling — `SUM(metric) AS metric`.
    None,
    /// `SUM(metric * weeksPerMonth(yearID, monthID)) AS metric`. Applies
    /// when the timestep collapses days into month/year output.
    WeeksPerMonth,
    /// `SUM(metric * portionOfWeekPerDay(dayID)) AS metric`. Applies
    /// when the timestep collapses portion-of-week into a classical
    /// 24-hour day (or to hour).
    PortionOfWeekPerDay,
}

/// One column slot in the aggregation `SELECT` list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregationColumn {
    /// `column AS column` — a group-by key. The column flows through.
    Key(&'static str),
    /// `NULL AS column` — aggregated away. Most "drop this dimension"
    /// columns are emitted this way so the destination schema stays fixed.
    Null(&'static str),
    /// `0 AS column` — like [`Null`](Self::Null) but with a zero literal.
    /// `BaseRateOutput` prefers `0` over `NULL` for its integer keys.
    Zero(&'static str),
    /// `'' AS column` — like [`Null`](Self::Null) but with an empty
    /// string literal. `BaseRateOutput` uses this for `SCC`.
    EmptyString(&'static str),
    /// `SUM(metric [* scale]) AS metric` — the aggregated metric column.
    Sum {
        /// Source/destination column name (e.g. `"emissionQuant"`).
        column: &'static str,
        /// Optional temporal rescaling factor.
        scaling: TemporalScaling,
    },
}

impl AggregationColumn {
    /// Name of the output column, regardless of variant.
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Self::Key(n)
            | Self::Null(n)
            | Self::Zero(n)
            | Self::EmptyString(n)
            | Self::Sum { column: n, .. } => n,
        }
    }

    /// True when this column appears in the `GROUP BY` clause.
    #[must_use]
    pub fn is_group_key(&self) -> bool {
        matches!(self, Self::Key(_))
    }
}

/// A column-shape plan for one output table.
///
/// Columns appear in the destination-schema order — Task 26 consumes the
/// list to (a) build the GROUP BY clause from the [`Key`](AggregationColumn::Key)
/// entries and (b) build the SELECT projection from the entire list.
#[derive(Debug, Clone)]
pub struct AggregationPlan {
    /// Which table this plan targets.
    pub table: AggregationTable,
    /// Output columns, in destination-schema order.
    pub columns: Vec<AggregationColumn>,
    /// Whether the Nonroad activity-weighting pre-pass is needed before
    /// aggregation. When `true`, callers must run the weight pass (see
    /// `AggregationSQLGenerator.nrActivityWeightSQL` in Java for the
    /// reference algorithm) before applying this plan.
    pub needs_nonroad_activity_weight: bool,
}

impl AggregationPlan {
    /// Column names that participate in the `GROUP BY` clause, in order.
    #[must_use]
    pub fn group_by(&self) -> Vec<&'static str> {
        self.columns
            .iter()
            .filter_map(|c| {
                if let AggregationColumn::Key(name) = c {
                    Some(*name)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Names of the columns that are aggregated away (NULL/0/'' filled).
    #[must_use]
    pub fn aggregated_columns(&self) -> Vec<&'static str> {
        self.columns
            .iter()
            .filter_map(|c| match c {
                AggregationColumn::Null(n)
                | AggregationColumn::Zero(n)
                | AggregationColumn::EmptyString(n) => Some(*n),
                _ => None,
            })
            .collect()
    }

    /// `(column, scaling)` pairs for every `SUM(...)` metric.
    #[must_use]
    pub fn sum_columns(&self) -> Vec<(&'static str, TemporalScaling)> {
        self.columns
            .iter()
            .filter_map(|c| match c {
                AggregationColumn::Sum { column, scaling } => Some((*column, *scaling)),
                _ => None,
            })
            .collect()
    }

    /// All output column names, in order.
    #[must_use]
    pub fn output_columns(&self) -> Vec<&'static str> {
        self.columns.iter().map(AggregationColumn::name).collect()
    }
}

/// Build the aggregation plan for `MOVESOutput` (emission quantities).
///
/// Columns appear in the canonical destination order (Java's
/// `masterOutputTableFields`, lines 276–298 of `AggregationSQLGenerator.java`):
///
/// `MOVESRunID, iterationID, yearID, monthID, dayID, hourID, stateID,
/// countyID, zoneID, linkID, pollutantID, roadTypeID, processID,
/// sourceTypeID, regClassID, fuelTypeID, [fuelSubTypeID,] modelYearID,
/// SCC, engTechID, sectorID, hpID, emissionQuant`
///
/// Each column is a [`Key`](AggregationColumn::Key) (group-by) or
/// [`Null`](AggregationColumn::Null) depending on the timestep / geographic
/// detail / breakdown flags. The terminal column is always a
/// [`Sum`](AggregationColumn::Sum) on `emissionQuant`, with temporal
/// scaling when the timestep collapses days into months/years.
#[must_use]
pub fn emission_aggregation(inputs: &AggregationInputs<'_>) -> AggregationPlan {
    let mut state = PlanState::new(AggregationTable::Emission);
    let geo = effective_geographic_detail(inputs);
    let temporal = add_time_columns(&mut state, inputs);
    add_geographic_columns(&mut state, inputs, geo);
    state.push(AggregationColumn::Key("pollutantID"));
    add_road_type_column(&mut state, inputs, geo);
    add_optional_dimension(&mut state, "processID", inputs.breakdown.emission_process);
    add_optional_dimension(&mut state, "sourceTypeID", inputs.breakdown.source_use_type);
    add_optional_dimension(&mut state, "regClassID", inputs.reg_class_id);
    add_fuel_type_column(&mut state, inputs);
    if inputs.fuel_sub_type {
        // fuelSubType only appears when ALLOW_FUELSUBTYPE_OUTPUT is on;
        // treat the input flag as the gate.
        state.push(AggregationColumn::Key("fuelSubTypeID"));
    }
    add_model_year_column(&mut state, inputs);
    add_scc_column(&mut state, inputs);
    add_nonroad_dimension(&mut state, "engTechID", inputs.eng_tech_id, inputs);
    add_nonroad_dimension(&mut state, "sectorID", inputs.sector, inputs);
    add_nonroad_dimension(&mut state, "hpID", inputs.breakdown.hp_class, inputs);
    state.push(AggregationColumn::Sum {
        column: "emissionQuant",
        scaling: temporal,
    });
    state.into_plan()
}

/// Build the aggregation plan for `MOVESActivityOutput`.
///
/// Columns appear in the canonical destination order (Java's
/// `outputActivityTableFields`, lines 332–353 of
/// `AggregationSQLGenerator.java`):
///
/// `MOVESRunID, iterationID, yearID, monthID, dayID, hourID, stateID,
/// countyID, zoneID, linkID, roadTypeID, sourceTypeID, regClassID,
/// fuelTypeID, [fuelSubTypeID,] modelYearID, SCC, engTechID, sectorID,
/// hpID, activityTypeID, activity`
///
/// Activity rows omit `pollutantID` and `processID` (activity doesn't carry
/// either), add `activityTypeID` near the tail, and SUM `activity` with the
/// same temporal scaling as emission output.
#[must_use]
pub fn activity_aggregation(inputs: &AggregationInputs<'_>) -> AggregationPlan {
    let mut state = PlanState::new(AggregationTable::Activity);
    let geo = effective_geographic_detail(inputs);
    let temporal = add_time_columns(&mut state, inputs);
    add_geographic_columns(&mut state, inputs, geo);
    add_road_type_column(&mut state, inputs, geo);
    add_optional_dimension(&mut state, "sourceTypeID", inputs.breakdown.source_use_type);
    add_optional_dimension(&mut state, "regClassID", inputs.reg_class_id);
    add_fuel_type_column(&mut state, inputs);
    if inputs.fuel_sub_type {
        state.push(AggregationColumn::Key("fuelSubTypeID"));
    }
    add_model_year_column(&mut state, inputs);
    add_scc_column(&mut state, inputs);
    add_nonroad_dimension(&mut state, "engTechID", inputs.eng_tech_id, inputs);
    add_nonroad_dimension(&mut state, "sectorID", inputs.sector, inputs);
    add_nonroad_dimension(&mut state, "hpID", inputs.breakdown.hp_class, inputs);
    state.push(AggregationColumn::Key("activityTypeID"));
    state.push(AggregationColumn::Sum {
        column: "activity",
        scaling: temporal,
    });
    state.into_plan()
}

/// Build the aggregation plan for `BaseRateOutput` (rates mode).
///
/// Columns appear in the canonical destination order (Java's
/// `outputBaseRateOutputTableFields`, lines 358–375 of
/// `AggregationSQLGenerator.java`):
///
/// `MOVESRunID, iterationID, zoneID, linkID, sourceTypeID, SCC, roadTypeID,
/// avgSpeedBinID, monthID, hourDayID, pollutantID, processID, modelYearID,
/// yearID, fuelTypeID, regClassID, meanBaseRate, emissionRate`
///
/// `BaseRateOutput` carries fewer time columns (`yearID, monthID, hourDayID`
/// — no separate `dayID`/`hourID`), prefers `0`/`''` literals over `NULL`
/// for dropped columns, and always sums two metrics. Per Java the BaseRate
/// table never carries `engTechID`/`sectorID`/`hpID`/`activityTypeID`.
#[must_use]
pub fn base_rate_aggregation(inputs: &AggregationInputs<'_>) -> AggregationPlan {
    let mut state = PlanState::new(AggregationTable::BaseRate);
    let geo = effective_geographic_detail(inputs);
    add_base_rate_geographic_columns(&mut state, geo);
    add_base_rate_source_type_column(&mut state, inputs);
    add_base_rate_scc_column(&mut state, inputs);
    add_base_rate_road_type_column(&mut state, inputs, geo);
    state.push(AggregationColumn::Key("avgSpeedBinID"));
    add_base_rate_time_columns(&mut state, inputs);
    state.push(AggregationColumn::Key("pollutantID"));
    state.push(AggregationColumn::Key("processID"));
    add_base_rate_model_year_column(&mut state, inputs);
    add_base_rate_year_column(&mut state, inputs);
    add_base_rate_fuel_type_column(&mut state, inputs);
    add_base_rate_reg_class_column(&mut state, inputs);
    state.push(AggregationColumn::Sum {
        column: "meanBaseRate",
        scaling: TemporalScaling::None,
    });
    state.push(AggregationColumn::Sum {
        column: "emissionRate",
        scaling: TemporalScaling::None,
    });
    state.into_plan()
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

struct PlanState {
    table: AggregationTable,
    columns: Vec<AggregationColumn>,
    needs_nr_weight: bool,
}

impl PlanState {
    fn new(table: AggregationTable) -> Self {
        let mut s = Self {
            table,
            columns: Vec::with_capacity(24),
            needs_nr_weight: false,
        };
        s.columns.push(AggregationColumn::Key("MOVESRunID"));
        s.columns.push(AggregationColumn::Key("iterationID"));
        s
    }

    fn push(&mut self, col: AggregationColumn) {
        self.columns.push(col);
    }

    fn mark_needs_nr_weight(&mut self) {
        self.needs_nr_weight = true;
    }

    fn into_plan(self) -> AggregationPlan {
        AggregationPlan {
            table: self.table,
            columns: self.columns,
            needs_nonroad_activity_weight: self.needs_nr_weight,
        }
    }
}

/// Apply Java's silent downgrade: Macroscale + non-Project + Link → County
/// (line 968 of `AggregationSQLGenerator.java`).
fn effective_geographic_detail(inputs: &AggregationInputs<'_>) -> GeographicOutputDetail {
    let requested = inputs.geographic_output_detail;
    if matches!(inputs.scale, ModelScale::Macro)
        && requested == GeographicOutputDetail::Link
        && inputs.domain != Some(ModelDomain::Project)
    {
        GeographicOutputDetail::County
    } else {
        requested
    }
}

/// Add the four time columns (`yearID, monthID, dayID, hourID`) and return
/// the temporal scaling that should multiply summed metrics.
fn add_time_columns(state: &mut PlanState, inputs: &AggregationInputs<'_>) -> TemporalScaling {
    let ts = inputs.timestep;
    // Java only applies week-to-month / portion-of-week scaling on Worker
    // SQL and never for Nonroad. In the Rust port there's no Worker/Master
    // split, so we keep the scaling iff Nonroad is *not* selected.
    let is_nonroad = inputs.models.contains(&Model::Nonroad);
    match ts {
        OutputTimestep::Year => {
            state.push(AggregationColumn::Key("yearID"));
            state.push(AggregationColumn::Null("monthID"));
            state.push(AggregationColumn::Null("dayID"));
            state.push(AggregationColumn::Null("hourID"));
            if is_nonroad {
                state.mark_needs_nr_weight();
                TemporalScaling::None
            } else {
                TemporalScaling::WeeksPerMonth
            }
        }
        OutputTimestep::Month => {
            state.push(AggregationColumn::Key("yearID"));
            state.push(AggregationColumn::Key("monthID"));
            state.push(AggregationColumn::Null("dayID"));
            state.push(AggregationColumn::Null("hourID"));
            if is_nonroad {
                state.mark_needs_nr_weight();
                TemporalScaling::None
            } else {
                TemporalScaling::WeeksPerMonth
            }
        }
        OutputTimestep::Day => {
            // 24-hour day timestep: usesClassicalDay = true, so portion-of-week
            // scaling applies.
            state.push(AggregationColumn::Key("yearID"));
            state.push(AggregationColumn::Key("monthID"));
            state.push(AggregationColumn::Key("dayID"));
            state.push(AggregationColumn::Null("hourID"));
            if is_nonroad {
                TemporalScaling::None
            } else {
                TemporalScaling::PortionOfWeekPerDay
            }
        }
        OutputTimestep::Hour => {
            state.push(AggregationColumn::Key("yearID"));
            state.push(AggregationColumn::Key("monthID"));
            state.push(AggregationColumn::Key("dayID"));
            state.push(AggregationColumn::Key("hourID"));
            if is_nonroad {
                TemporalScaling::None
            } else {
                TemporalScaling::PortionOfWeekPerDay
            }
        }
    }
}

fn add_geographic_columns(
    state: &mut PlanState,
    inputs: &AggregationInputs<'_>,
    geo: GeographicOutputDetail,
) {
    let is_nonroad = inputs.models.contains(&Model::Nonroad);
    match geo {
        GeographicOutputDetail::Nation => {
            state.push(AggregationColumn::Null("stateID"));
            state.push(AggregationColumn::Null("countyID"));
            state.push(AggregationColumn::Null("zoneID"));
            state.push(AggregationColumn::Null("linkID"));
            state.mark_needs_nr_weight();
        }
        GeographicOutputDetail::State => {
            // Nonroad workers retain countyID for downstream LF/avgHP
            // weighting at the master; the Rust port has no worker split,
            // so we follow the master-side semantics (countyID aggregated
            // away). The needs-NR-weight flag still fires.
            state.push(AggregationColumn::Key("stateID"));
            state.push(AggregationColumn::Null("countyID"));
            state.push(AggregationColumn::Null("zoneID"));
            state.push(AggregationColumn::Null("linkID"));
            if is_nonroad {
                state.mark_needs_nr_weight();
            }
        }
        GeographicOutputDetail::County => {
            state.push(AggregationColumn::Key("stateID"));
            state.push(AggregationColumn::Key("countyID"));
            state.push(AggregationColumn::Null("zoneID"));
            state.push(AggregationColumn::Null("linkID"));
        }
        GeographicOutputDetail::Zone => {
            state.push(AggregationColumn::Key("stateID"));
            state.push(AggregationColumn::Key("countyID"));
            state.push(AggregationColumn::Key("zoneID"));
            // linkID always starts as Null here; for Macroscale + Zone +
            // roadType the road-type pass promotes it to Key
            // (Java lines 1192–1209), so this position is the same in
            // both Macroscale and non-Macroscale code paths.
            state.push(AggregationColumn::Null("linkID"));
        }
        GeographicOutputDetail::Link => {
            state.push(AggregationColumn::Key("stateID"));
            state.push(AggregationColumn::Key("countyID"));
            state.push(AggregationColumn::Key("zoneID"));
            state.push(AggregationColumn::Key("linkID"));
        }
    }
}

/// Add `roadTypeID`. For Macroscale + Zone the linkID/roadTypeID pair has
/// a special interaction with the previously-added linkID column.
fn add_road_type_column(
    state: &mut PlanState,
    inputs: &AggregationInputs<'_>,
    geo: GeographicOutputDetail,
) {
    let kept = inputs.breakdown.road_type;
    let macroscale = matches!(inputs.scale, ModelScale::Macro);
    if macroscale && geo == GeographicOutputDetail::Zone {
        // Special case from Java lines 1164–1227: at Macroscale + Zone,
        // linkID's group-key status is tied to road_type's status. The
        // previously-pushed linkID is a Null; if road_type is kept we
        // promote it to a Key.
        if kept {
            promote_link_to_key(state);
            state.push(AggregationColumn::Key("roadTypeID"));
        } else {
            state.push(AggregationColumn::Null("roadTypeID"));
        }
    } else if kept {
        state.push(AggregationColumn::Key("roadTypeID"));
    } else {
        state.push(AggregationColumn::Null("roadTypeID"));
    }
}

fn promote_link_to_key(state: &mut PlanState) {
    for col in state.columns.iter_mut().rev() {
        if matches!(col, AggregationColumn::Null(n) if *n == "linkID") {
            *col = AggregationColumn::Key("linkID");
            return;
        }
    }
}

fn add_optional_dimension(state: &mut PlanState, name: &'static str, kept: bool) {
    if kept {
        state.push(AggregationColumn::Key(name));
    } else {
        state.push(AggregationColumn::Null(name));
    }
}

/// Add the `fuelTypeID` column. Dropping fuel type triggers the Nonroad
/// activity-weighting pre-pass.
fn add_fuel_type_column(state: &mut PlanState, inputs: &AggregationInputs<'_>) {
    if inputs.breakdown.fuel_type {
        state.push(AggregationColumn::Key("fuelTypeID"));
    } else {
        state.push(AggregationColumn::Null("fuelTypeID"));
        if inputs.models.contains(&Model::Nonroad) {
            state.mark_needs_nr_weight();
        }
    }
}

/// Add the `modelYearID` column. Dropping model year triggers the Nonroad
/// activity-weighting pre-pass.
fn add_model_year_column(state: &mut PlanState, inputs: &AggregationInputs<'_>) {
    if inputs.breakdown.model_year {
        state.push(AggregationColumn::Key("modelYearID"));
    } else {
        state.push(AggregationColumn::Null("modelYearID"));
        if inputs.models.contains(&Model::Nonroad) {
            state.mark_needs_nr_weight();
        }
    }
}

/// Add the `SCC` column. Dropping SCC triggers the Nonroad activity-weighting
/// pre-pass.
fn add_scc_column(state: &mut PlanState, inputs: &AggregationInputs<'_>) {
    if inputs.breakdown.onroad_scc {
        state.push(AggregationColumn::Key("SCC"));
    } else {
        state.push(AggregationColumn::Null("SCC"));
        if inputs.models.contains(&Model::Nonroad) {
            state.mark_needs_nr_weight();
        }
    }
}

/// Add a Nonroad-gated dimension. It can be a Key only when Nonroad is
/// active *and* its breakdown flag is set; otherwise it's NULL and (per
/// Java lines 1429, 1451, 1473) the Nonroad activity-weighting pre-pass
/// fires.
fn add_nonroad_dimension(
    state: &mut PlanState,
    name: &'static str,
    flag: bool,
    inputs: &AggregationInputs<'_>,
) {
    let is_nonroad = inputs.models.contains(&Model::Nonroad);
    if is_nonroad && flag {
        state.push(AggregationColumn::Key(name));
    } else {
        state.push(AggregationColumn::Null(name));
        if is_nonroad {
            state.mark_needs_nr_weight();
        }
    }
}

// ---- BaseRate-specific helpers --------------------------------------------

/// Add the `monthID, hourDayID` pair. BaseRate's `yearID` is added
/// separately by [`add_base_rate_year_column`] because the canonical
/// schema splits the time columns: monthID/hourDayID appear near the
/// front, yearID appears later between modelYearID and fuelTypeID.
fn add_base_rate_time_columns(state: &mut PlanState, inputs: &AggregationInputs<'_>) {
    match inputs.timestep {
        OutputTimestep::Year => {
            state.push(AggregationColumn::Zero("monthID"));
            state.push(AggregationColumn::Zero("hourDayID"));
        }
        OutputTimestep::Month => {
            state.push(AggregationColumn::Key("monthID"));
            state.push(AggregationColumn::Zero("hourDayID"));
        }
        OutputTimestep::Day | OutputTimestep::Hour => {
            state.push(AggregationColumn::Key("monthID"));
            state.push(AggregationColumn::Key("hourDayID"));
        }
    }
}

/// Add the BaseRate `yearID` column. Always a Key regardless of timestep
/// (Year/Month/Day/Hour all preserve yearID in the Java code; lines 873,
/// 887, 902, 917).
fn add_base_rate_year_column(state: &mut PlanState, _inputs: &AggregationInputs<'_>) {
    state.push(AggregationColumn::Key("yearID"));
}

fn add_base_rate_geographic_columns(state: &mut PlanState, geo: GeographicOutputDetail) {
    // BaseRate only carries (zoneID, linkID). State/County/Nation collapse
    // both to 0.
    match geo {
        GeographicOutputDetail::Nation
        | GeographicOutputDetail::State
        | GeographicOutputDetail::County => {
            state.push(AggregationColumn::Zero("zoneID"));
            state.push(AggregationColumn::Zero("linkID"));
        }
        GeographicOutputDetail::Zone => {
            state.push(AggregationColumn::Key("zoneID"));
            state.push(AggregationColumn::Zero("linkID"));
        }
        GeographicOutputDetail::Link => {
            state.push(AggregationColumn::Key("zoneID"));
            state.push(AggregationColumn::Key("linkID"));
        }
    }
}

fn add_base_rate_road_type_column(
    state: &mut PlanState,
    inputs: &AggregationInputs<'_>,
    geo: GeographicOutputDetail,
) {
    let macroscale = matches!(inputs.scale, ModelScale::Macro);
    let kept = inputs.breakdown.road_type;
    if macroscale && geo == GeographicOutputDetail::Zone && kept {
        // Promote linkID to Key when road type is also kept (Macroscale +
        // Zone + roadType).
        for col in state.columns.iter_mut().rev() {
            if matches!(col, AggregationColumn::Zero(n) if *n == "linkID") {
                *col = AggregationColumn::Key("linkID");
                break;
            }
        }
        state.push(AggregationColumn::Key("roadTypeID"));
    } else if kept {
        state.push(AggregationColumn::Key("roadTypeID"));
    } else {
        state.push(AggregationColumn::Zero("roadTypeID"));
    }
}

fn add_base_rate_model_year_column(state: &mut PlanState, inputs: &AggregationInputs<'_>) {
    if inputs.breakdown.model_year {
        state.push(AggregationColumn::Key("modelYearID"));
    } else {
        state.push(AggregationColumn::Zero("modelYearID"));
    }
}

fn add_base_rate_fuel_type_column(state: &mut PlanState, inputs: &AggregationInputs<'_>) {
    if inputs.breakdown.fuel_type {
        state.push(AggregationColumn::Key("fuelTypeID"));
    } else {
        state.push(AggregationColumn::Zero("fuelTypeID"));
    }
}

fn add_base_rate_source_type_column(state: &mut PlanState, inputs: &AggregationInputs<'_>) {
    if inputs.breakdown.source_use_type {
        state.push(AggregationColumn::Key("sourceTypeID"));
    } else {
        state.push(AggregationColumn::Zero("sourceTypeID"));
    }
}

fn add_base_rate_reg_class_column(state: &mut PlanState, inputs: &AggregationInputs<'_>) {
    if inputs.reg_class_id {
        state.push(AggregationColumn::Key("regClassID"));
    } else {
        state.push(AggregationColumn::Zero("regClassID"));
    }
}

fn add_base_rate_scc_column(state: &mut PlanState, inputs: &AggregationInputs<'_>) {
    if inputs.breakdown.onroad_scc {
        state.push(AggregationColumn::Key("SCC"));
    } else {
        state.push(AggregationColumn::EmptyString("SCC"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use moves_runspec::model::OutputBreakdown;

    /// All-false breakdown — used as a baseline for "everything aggregates
    /// away".
    fn empty_breakdown() -> OutputBreakdown {
        OutputBreakdown::default()
    }

    /// Maximally-detailed breakdown (every dimension kept).
    fn full_breakdown() -> OutputBreakdown {
        OutputBreakdown {
            model_year: true,
            fuel_type: true,
            emission_process: true,
            distinguish_particulates: true,
            onroad_offroad: true,
            road_type: true,
            source_use_type: true,
            moves_vehicle_type: true,
            onroad_scc: true,
            offroad_scc: true,
            estimate_uncertainty: false,
            segment: true,
            hp_class: true,
        }
    }

    fn inputs<'a>(
        timestep: OutputTimestep,
        geo: GeographicOutputDetail,
        scale: ModelScale,
        models: &'a [Model],
        breakdown: &'a OutputBreakdown,
    ) -> AggregationInputs<'a> {
        AggregationInputs {
            timestep,
            geographic_output_detail: geo,
            scale,
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
    fn emission_year_nation_drops_everything_below_year() {
        // Year + Nation + empty breakdown: nothing survives below the year
        // level except the run/iteration IDs and the SUM.
        let b = empty_breakdown();
        let i = inputs(
            OutputTimestep::Year,
            GeographicOutputDetail::Nation,
            ModelScale::Macro,
            &[Model::Onroad],
            &b,
        );
        let plan = emission_aggregation(&i);
        assert_eq!(plan.table, AggregationTable::Emission);
        // Group-by keys: MOVESRunID, iterationID, yearID, pollutantID.
        // (pollutantID is always a key — it's not breakdown-flagged.)
        let keys = plan.group_by();
        assert_eq!(
            keys,
            vec!["MOVESRunID", "iterationID", "yearID", "pollutantID"]
        );
        // monthID, dayID, hourID, all geographic, road, process, fuelType,
        // modelYear, sourceType, regClass, SCC, engTech, sector, hp aggregate
        // away.
        let aggr = plan.aggregated_columns();
        assert!(aggr.contains(&"monthID"));
        assert!(aggr.contains(&"dayID"));
        assert!(aggr.contains(&"hourID"));
        assert!(aggr.contains(&"stateID"));
        assert!(aggr.contains(&"countyID"));
        assert!(aggr.contains(&"zoneID"));
        assert!(aggr.contains(&"linkID"));
        assert!(aggr.contains(&"roadTypeID"));
        assert!(aggr.contains(&"processID"));
        assert!(aggr.contains(&"fuelTypeID"));
        assert!(aggr.contains(&"modelYearID"));
        assert!(aggr.contains(&"sourceTypeID"));
        assert!(aggr.contains(&"SCC"));
        // SUM expression: emissionQuant with weeks-per-month scaling (the
        // Year timestep collapses days into months).
        let sums = plan.sum_columns();
        assert_eq!(
            sums,
            vec![("emissionQuant", TemporalScaling::WeeksPerMonth)]
        );
    }

    #[test]
    fn emission_hour_link_project_keeps_everything() {
        // Hour + Link + Project domain + full breakdown: every dimension
        // survives.
        let b = full_breakdown();
        let mut i = inputs(
            OutputTimestep::Hour,
            GeographicOutputDetail::Link,
            ModelScale::Macro,
            &[Model::Onroad],
            &b,
        );
        i.domain = Some(ModelDomain::Project);
        i.reg_class_id = true;
        let plan = emission_aggregation(&i);
        let keys = plan.group_by();
        // Time + geo + breakdown columns should all be keys.
        for expected in [
            "MOVESRunID",
            "iterationID",
            "yearID",
            "monthID",
            "dayID",
            "hourID",
            "pollutantID",
            "stateID",
            "countyID",
            "zoneID",
            "linkID",
            "roadTypeID",
            "processID",
            "fuelTypeID",
            "modelYearID",
            "sourceTypeID",
            "regClassID",
            "SCC",
        ] {
            assert!(
                keys.contains(&expected),
                "missing key {expected} in {:?}",
                keys
            );
        }
        // Hour timestep uses portion-of-week scaling on the SUM.
        let sums = plan.sum_columns();
        assert_eq!(
            sums,
            vec![("emissionQuant", TemporalScaling::PortionOfWeekPerDay)]
        );
    }

    #[test]
    fn macroscale_non_project_link_downgrades_to_county() {
        // Java line 968: Macroscale + non-Project + Link → County. Verify
        // the silent downgrade.
        let b = full_breakdown();
        let i = inputs(
            OutputTimestep::Hour,
            GeographicOutputDetail::Link,
            ModelScale::Macro,
            &[Model::Onroad],
            &b,
        );
        // No project domain — should downgrade.
        let plan = emission_aggregation(&i);
        let keys = plan.group_by();
        // linkID is aggregated away (downgraded to County).
        assert!(
            !keys.contains(&"linkID"),
            "linkID survived downgrade: {:?}",
            keys
        );
        assert!(plan.aggregated_columns().contains(&"linkID"));
        // countyID still survives.
        assert!(keys.contains(&"countyID"));
    }

    #[test]
    fn macroscale_zone_with_road_type_keeps_link() {
        // Java lines 1192–1209: at Macroscale + Zone with roadType=true,
        // linkID is implicit-keyed because zone + roadType uniquely
        // identifies a link.
        let b = OutputBreakdown {
            road_type: true,
            ..OutputBreakdown::default()
        };
        let i = inputs(
            OutputTimestep::Hour,
            GeographicOutputDetail::Zone,
            ModelScale::Macro,
            &[Model::Onroad],
            &b,
        );
        let plan = emission_aggregation(&i);
        let keys = plan.group_by();
        assert!(keys.contains(&"zoneID"));
        assert!(keys.contains(&"roadTypeID"));
        assert!(
            keys.contains(&"linkID"),
            "linkID should be promoted: {:?}",
            keys
        );
    }

    #[test]
    fn macroscale_zone_without_road_type_drops_link() {
        // Same scenario but road_type = false → linkID should not be a key.
        let b = empty_breakdown();
        let i = inputs(
            OutputTimestep::Hour,
            GeographicOutputDetail::Zone,
            ModelScale::Macro,
            &[Model::Onroad],
            &b,
        );
        let plan = emission_aggregation(&i);
        let keys = plan.group_by();
        assert!(keys.contains(&"zoneID"));
        assert!(!keys.contains(&"roadTypeID"));
        assert!(!keys.contains(&"linkID"));
    }

    #[test]
    fn activity_plan_has_activity_type_id_and_no_pollutant() {
        // Activity rows replace pollutantID/processID with activityTypeID
        // and SUM `activity` instead of `emissionQuant`.
        let b = full_breakdown();
        let i = inputs(
            OutputTimestep::Hour,
            GeographicOutputDetail::County,
            ModelScale::Macro,
            &[Model::Onroad],
            &b,
        );
        let plan = activity_aggregation(&i);
        assert_eq!(plan.table, AggregationTable::Activity);
        let cols = plan.output_columns();
        assert!(!cols.contains(&"pollutantID"));
        assert!(!cols.contains(&"processID"));
        assert!(cols.contains(&"activityTypeID"));
        // The metric is `activity`.
        let sums: Vec<_> = plan.sum_columns().iter().map(|(c, _)| *c).collect();
        assert_eq!(sums, vec!["activity"]);
    }

    #[test]
    fn base_rate_uses_zero_literals_instead_of_null() {
        // BaseRate prefers 0/'' over NULL for dropped columns.
        let b = empty_breakdown();
        let i = inputs(
            OutputTimestep::Hour,
            GeographicOutputDetail::Nation,
            ModelScale::Macro,
            &[Model::Onroad],
            &b,
        );
        let plan = base_rate_aggregation(&i);
        assert_eq!(plan.table, AggregationTable::BaseRate);
        // Dropped columns must be Zero (or EmptyString for SCC), not Null.
        for col in &plan.columns {
            assert!(
                !matches!(col, AggregationColumn::Null(_)),
                "BaseRate plan should not contain Null columns: {col:?}"
            );
        }
        // SCC drop must be EmptyString.
        assert!(plan
            .columns
            .iter()
            .any(|c| matches!(c, AggregationColumn::EmptyString(n) if *n == "SCC")));
    }

    #[test]
    fn base_rate_carries_two_sum_metrics() {
        // meanBaseRate AND emissionRate must both be summed.
        let b = full_breakdown();
        let i = inputs(
            OutputTimestep::Hour,
            GeographicOutputDetail::Link,
            ModelScale::Rates,
            &[Model::Onroad],
            &b,
        );
        let plan = base_rate_aggregation(&i);
        let sums: Vec<_> = plan.sum_columns().iter().map(|(c, _)| *c).collect();
        assert_eq!(sums, vec!["meanBaseRate", "emissionRate"]);
    }

    #[test]
    fn nonroad_year_does_not_apply_temporal_scaling() {
        // Java sets the SUM clause to plain SUM(emissionQuant) when the
        // model includes Nonroad; the weeks-per-month/portion-of-week
        // scaling only fires for Onroad.
        let b = empty_breakdown();
        let i = inputs(
            OutputTimestep::Year,
            GeographicOutputDetail::Nation,
            ModelScale::Inventory,
            &[Model::Nonroad],
            &b,
        );
        let plan = emission_aggregation(&i);
        let (_, scale) = plan.sum_columns()[0];
        assert_eq!(scale, TemporalScaling::None);
        // Dropping multiple Nonroad-relevant dimensions should also flip
        // the activity-weight flag.
        assert!(plan.needs_nonroad_activity_weight);
    }

    #[test]
    fn onroad_year_uses_weeks_per_month_scaling() {
        // Onroad + Year → weeks-per-month temporal scaling.
        let b = empty_breakdown();
        let i = inputs(
            OutputTimestep::Year,
            GeographicOutputDetail::Nation,
            ModelScale::Macro,
            &[Model::Onroad],
            &b,
        );
        let plan = emission_aggregation(&i);
        let (col, scale) = plan.sum_columns()[0];
        assert_eq!(col, "emissionQuant");
        assert_eq!(scale, TemporalScaling::WeeksPerMonth);
    }

    #[test]
    fn nonroad_keeps_eng_tech_and_sector_when_flagged() {
        // engTechID, sectorID, hpID are Nonroad-only keys. When models
        // include Nonroad and the breakdown flag is true, they become keys.
        let b = OutputBreakdown {
            hp_class: true,
            ..OutputBreakdown::default()
        };
        let mut i = inputs(
            OutputTimestep::Hour,
            GeographicOutputDetail::County,
            ModelScale::Inventory,
            &[Model::Nonroad],
            &b,
        );
        i.eng_tech_id = true;
        i.sector = true;
        let plan = emission_aggregation(&i);
        let keys = plan.group_by();
        assert!(keys.contains(&"engTechID"));
        assert!(keys.contains(&"sectorID"));
        assert!(keys.contains(&"hpID"));
    }

    #[test]
    fn onroad_never_keys_nonroad_dimensions() {
        // Onroad-only run: engTechID/sectorID/hpID are NULL even when the
        // breakdown flag is true, since the Java guard `models contains
        // Nonroad` is false.
        let b = OutputBreakdown {
            hp_class: true,
            ..OutputBreakdown::default()
        };
        let mut i = inputs(
            OutputTimestep::Hour,
            GeographicOutputDetail::County,
            ModelScale::Macro,
            &[Model::Onroad],
            &b,
        );
        i.eng_tech_id = true;
        i.sector = true;
        let plan = emission_aggregation(&i);
        let keys = plan.group_by();
        assert!(!keys.contains(&"engTechID"));
        assert!(!keys.contains(&"sectorID"));
        assert!(!keys.contains(&"hpID"));
    }

    #[test]
    fn emission_columns_match_canonical_master_output_table_fields() {
        // Spot-check the destination-schema order against Java's
        // `masterOutputTableFields` constant (lines 276–298 of
        // AggregationSQLGenerator.java). All columns are Keys here
        // because we asked for a maximally-detailed breakdown.
        let b = full_breakdown();
        let mut i = inputs(
            OutputTimestep::Hour,
            GeographicOutputDetail::Link,
            ModelScale::Macro,
            &[Model::Onroad],
            &b,
        );
        i.domain = Some(ModelDomain::Project);
        i.reg_class_id = true;
        let plan = emission_aggregation(&i);
        let cols = plan.output_columns();
        assert_eq!(
            cols,
            vec![
                "MOVESRunID",
                "iterationID",
                "yearID",
                "monthID",
                "dayID",
                "hourID",
                "stateID",
                "countyID",
                "zoneID",
                "linkID",
                "pollutantID",
                "roadTypeID",
                "processID",
                "sourceTypeID",
                "regClassID",
                "fuelTypeID",
                "modelYearID",
                "SCC",
                "engTechID",
                "sectorID",
                "hpID",
                "emissionQuant",
            ]
        );
    }

    #[test]
    fn activity_columns_match_canonical_output_activity_table_fields() {
        // Spot-check the destination-schema order against Java's
        // `outputActivityTableFields` constant (lines 332–353 of
        // AggregationSQLGenerator.java). activityTypeID sits between hpID
        // and the activity metric.
        let b = full_breakdown();
        let mut i = inputs(
            OutputTimestep::Hour,
            GeographicOutputDetail::Link,
            ModelScale::Macro,
            &[Model::Onroad],
            &b,
        );
        i.domain = Some(ModelDomain::Project);
        i.reg_class_id = true;
        let plan = activity_aggregation(&i);
        let cols = plan.output_columns();
        assert_eq!(
            cols,
            vec![
                "MOVESRunID",
                "iterationID",
                "yearID",
                "monthID",
                "dayID",
                "hourID",
                "stateID",
                "countyID",
                "zoneID",
                "linkID",
                "roadTypeID",
                "sourceTypeID",
                "regClassID",
                "fuelTypeID",
                "modelYearID",
                "SCC",
                "engTechID",
                "sectorID",
                "hpID",
                "activityTypeID",
                "activity",
            ]
        );
    }

    #[test]
    fn base_rate_columns_match_canonical_output_base_rate_table_fields() {
        // Spot-check against Java's `outputBaseRateOutputTableFields`
        // (lines 358–375 of AggregationSQLGenerator.java).
        let b = full_breakdown();
        let mut i = inputs(
            OutputTimestep::Hour,
            GeographicOutputDetail::Link,
            ModelScale::Rates,
            &[Model::Onroad],
            &b,
        );
        i.reg_class_id = true;
        let plan = base_rate_aggregation(&i);
        let cols = plan.output_columns();
        assert_eq!(
            cols,
            vec![
                "MOVESRunID",
                "iterationID",
                "zoneID",
                "linkID",
                "sourceTypeID",
                "SCC",
                "roadTypeID",
                "avgSpeedBinID",
                "monthID",
                "hourDayID",
                "pollutantID",
                "processID",
                "modelYearID",
                "yearID",
                "fuelTypeID",
                "regClassID",
                "meanBaseRate",
                "emissionRate",
            ]
        );
    }

    #[test]
    fn day_timestep_keeps_day_drops_hour() {
        // Day timestep: yearID, monthID, dayID are keys; hourID is NULL.
        let b = empty_breakdown();
        let i = inputs(
            OutputTimestep::Day,
            GeographicOutputDetail::County,
            ModelScale::Macro,
            &[Model::Onroad],
            &b,
        );
        let plan = emission_aggregation(&i);
        let keys = plan.group_by();
        assert!(keys.contains(&"yearID"));
        assert!(keys.contains(&"monthID"));
        assert!(keys.contains(&"dayID"));
        assert!(!keys.contains(&"hourID"));
        // Onroad + day timestep → portion-of-week scaling.
        let (_, scale) = plan.sum_columns()[0];
        assert_eq!(scale, TemporalScaling::PortionOfWeekPerDay);
    }

    #[test]
    fn month_timestep_drops_day_and_hour() {
        let b = empty_breakdown();
        let i = inputs(
            OutputTimestep::Month,
            GeographicOutputDetail::County,
            ModelScale::Macro,
            &[Model::Onroad],
            &b,
        );
        let plan = emission_aggregation(&i);
        let keys = plan.group_by();
        assert!(keys.contains(&"yearID"));
        assert!(keys.contains(&"monthID"));
        assert!(!keys.contains(&"dayID"));
        assert!(!keys.contains(&"hourID"));
        // Onroad + month timestep → weeks-per-month scaling.
        let (_, scale) = plan.sum_columns()[0];
        assert_eq!(scale, TemporalScaling::WeeksPerMonth);
    }

    #[test]
    fn fuel_sub_type_flag_adds_fuel_sub_type_key() {
        let b = full_breakdown();
        let mut i = inputs(
            OutputTimestep::Hour,
            GeographicOutputDetail::County,
            ModelScale::Macro,
            &[Model::Onroad],
            &b,
        );
        i.fuel_sub_type = true;
        let plan = emission_aggregation(&i);
        assert!(plan.group_by().contains(&"fuelSubTypeID"));
    }

    #[test]
    fn aggregation_column_helpers() {
        let key = AggregationColumn::Key("yearID");
        let null = AggregationColumn::Null("hourID");
        let zero = AggregationColumn::Zero("monthID");
        let empty = AggregationColumn::EmptyString("SCC");
        let sum = AggregationColumn::Sum {
            column: "emissionQuant",
            scaling: TemporalScaling::None,
        };
        assert_eq!(key.name(), "yearID");
        assert_eq!(null.name(), "hourID");
        assert_eq!(zero.name(), "monthID");
        assert_eq!(empty.name(), "SCC");
        assert_eq!(sum.name(), "emissionQuant");
        assert!(key.is_group_key());
        assert!(!null.is_group_key());
        assert!(!zero.is_group_key());
        assert!(!empty.is_group_key());
        assert!(!sum.is_group_key());
    }

    #[test]
    fn base_rate_year_uses_zero_for_month_and_hourday() {
        // BaseRate at Year-timestep collapses both monthID and hourDayID
        // to Zero.
        let b = full_breakdown();
        let i = inputs(
            OutputTimestep::Year,
            GeographicOutputDetail::Link,
            ModelScale::Rates,
            &[Model::Onroad],
            &b,
        );
        let plan = base_rate_aggregation(&i);
        let zero_cols: Vec<&str> = plan
            .columns
            .iter()
            .filter_map(|c| match c {
                AggregationColumn::Zero(n) => Some(*n),
                _ => None,
            })
            .collect();
        assert!(zero_cols.contains(&"monthID"));
        assert!(zero_cols.contains(&"hourDayID"));
    }

    #[test]
    fn nation_geographic_marks_nonroad_weight_needed() {
        // Java line 986: NATION geographic detail flips
        // nrNeedsActivityWeight = true regardless of model. Verify the
        // flag fires even for Onroad runs (Java also sets it).
        let b = full_breakdown();
        let i = inputs(
            OutputTimestep::Hour,
            GeographicOutputDetail::Nation,
            ModelScale::Macro,
            &[Model::Onroad],
            &b,
        );
        let plan = emission_aggregation(&i);
        assert!(plan.needs_nonroad_activity_weight);
    }

    #[test]
    fn plan_accessors_partition_columns_correctly() {
        // Sanity check: every column is reachable via exactly one of the
        // partitioning accessors (group_by / aggregated_columns /
        // sum_columns).
        let b = full_breakdown();
        let mut i = inputs(
            OutputTimestep::Day,
            GeographicOutputDetail::County,
            ModelScale::Macro,
            &[Model::Onroad],
            &b,
        );
        i.reg_class_id = true;
        let plan = emission_aggregation(&i);
        let total = plan.columns.len();
        let g = plan.group_by().len();
        let a = plan.aggregated_columns().len();
        let s = plan.sum_columns().len();
        assert_eq!(
            g + a + s,
            total,
            "columns should partition: {:?}",
            plan.columns
        );
    }
}
