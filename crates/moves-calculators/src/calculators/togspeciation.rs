//! Port of `TOGSpeciationCalculator.java` and
//! `database/TOGSpeciationCalculator.sql` — .
//!
//! `TOGSpeciationCalculator` completes a requested **chemical-mechanism
//! speciation** of organic-gas emissions by computing the **NonHAPTOG**
//! (pollutant 88) residual — the organic gas left after the individually
//! modelled mechanism species have been subtracted out.
//!
//! It is the third member of the speciation family. The
//! [`hcspeciation`](super::hcspeciation) calculator and its Nonroad
//! counterpart [`nrhcspeciation`](super::nrhcspeciation) split total
//! hydrocarbons into the five gross categories MOVES always reports (methane,
//! NMHC, NMOG, TOG, VOC). This calculator works one level finer: it closes a
//! mechanism speciation into individual hydrocarbon species (the CB05 set//! `CB05_PAR`, `CB05_ALD2`, `CB05_OLE`, …).
//!
//! # Chained calculator
//!
//! `TOGSpeciationCalculator` is a *chained* calculator.
//! `TOGSpeciationCalculator.subscribeToMe` does **not** subscribe to the
//! MasterLoop; it chains itself onto the calculators that produce the species
//! and organic-gas inputs it needs and runs inside the same master-loop pass.
//! `calculator-dag.json` records this as `subscribes_directly: false`,
//! `subscriptions: []`,
//! `depends_on: ["AirToxicsCalculator", "CrankcaseEmissionCalculatorNonPM",
//! "HCSpeciationCalculator"]`; the [`Calculator`] metadata methods mirror it
//! ([`subscriptions`](Calculator::subscriptions) is empty,
//! [`upstream`](Calculator::upstream) names the three).
//!
//! # What it computes
//!
//! A chemical mechanism (e.g. CB05) assigns the organic-gas mass of an
//! emission to a fixed set of named species. MOVES models the individually
//! named species upstream; this calculator produces the leftover — `NonHAPTOG`
//! so that the named species plus the residual reconstitute the whole. For
//! one mechanism's *integrated species set* and one output dimensional cell:
//!
//! ```text
//! NonHAPTOG = NMOG − Σ (integrated-species emission), clamped to ≥ 0
//! ```
//!
//! `NMOG` (non-methane organic gas, pollutant 80) is the organic-gas total the
//! residual is measured against; the *integrated species* are the mechanism
//! species enumerated, per mechanism, in the `integratedSpeciesSet` table. Each
//! emission carries both a quantity and a rate; the signed sum and the
//! non-negative clamp apply to *both* channels independently.
//!
//! # Algorithm — the SQL "Processing" section
//!
//! [`TogSpeciationCalculator::calculate`] ports the `Section Processing` of
//! `TOGSpeciationCalculator.sql`. The SQL stages rows through a
//! `TOGWorkerOutput` table in five statements (two are table mechanics — a
//! `CREATE`/`TRUNCATE` and an index `ALTER`); the port folds the three
//! algorithm-bearing statements into one accumulation pass:
//!
//! 1. **Synthesise NMOG** (SQL `insert ignore … select distinct … 80 …`).
//! Every distinct `(mechanismID, integratedSpeciesSetID)` that lists any
//! non-NMOG pollutant gains an NMOG (80) row if it lacks one. NMOG is the
//! positive term of the integration, so it must belong to every set.
//! `insert ignore` skips on the `(mechanism, set, pollutant)` primary key,
//! so an already-present NMOG row is never duplicated.
//! 2. **Sign and fan out** (SQL stages the input pool as `mechanismID = 0`
//! rows, then `inner join integratedSpeciesSet using (pollutantID)`). Each
//! `MOVESWorkerOutput` row whose pollutant appears in `integratedSpeciesSet`
//! contributes to *every* `(mechanism, set)` that lists that pollutant. The
//! contribution is `+emission` when the pollutant is NMOG (80) and
//! `−emission` for any other (integrated-species) pollutant.
//! 3. **Sum and clamp** (SQL `insert into MOVESWorkerOutput … select …
//! greatest(sum(…), 0) … group by …`). The signed contributions are grouped
//! by `(mechanism, set)` and the output dimensional cell, summed, and
//! clamped to `≥ 0`. Each grouped result is one `NonHAPTOG` (88)
//! `MOVESWorkerOutput` row.
//!
//! The SQL's `mechanismID = 0` is an internal staging sentinel; its final sum
//! filters `where mechanismID <> 0` to drop the staged input pool. The
//! extracted `integratedSpeciesSet` only ever carries real mechanism ids (the
//! Java derives `mechanismID = 1 + (databaseKey − 1000) / 500`, always `≥ 1`),
//! so the port — which stages nothing and accumulates only the signed fan-out
//! needs no equivalent of that filter.
//!
//! # Registrations vs. computed output
//!
//! `CalculatorInfo.txt` records 184 `Registration` directives for this
//! calculator — the 16 CB05 mechanism pseudo-pollutants (1000, 1001, …, 1018)
//! and `NonHAPTOG` (88) across the 12 organic-gas processes (running and start
//! exhaust, the three evaporative and two refuelling processes, the three
//! crankcase processes, extended-idle and auxiliary-power exhaust).
//! [`registrations`](Calculator::registrations) returns all 184 — the
//! project's calculator-metadata convention is to derive registrations from
//! `CalculatorInfo.txt`, not the Java constructor.
//!
//! The `Section Processing` algorithm itself, however, only ever produces
//! pollutant 88: the individual mechanism species are computed upstream (by
//! `AirToxicsCalculator`), and the mechanism pseudo-pollutants are chain
//! bookkeeping, not emission rows. The registrations are chain-wiring
//! metadata; the computed output is the `NonHAPTOG` residual alone.
//!
//! # Scope of this port
//!
//! [`calculate`](TogSpeciationCalculator::calculate) is the `Section
//! Processing` algorithm. Its [`TogInputs`] argument is the pair of tables the
//! SQL consumes: the extracted `integratedSpeciesSet` (the SQL's "Extract
//! Data" section, filtered to the run's mechanisms) and the `MOVESWorkerOutput`
//! rows the upstream calculators have already produced on the worker. A future
//! (`DataFrameStore`) wiring populates [`TogInputs`] from the run
//! context.
//!
//! Three things the Java/SQL do are execution wiring, not the algorithm, and
//! are left to that wiring:
//!
//! * The Java constructor and `subscribeToMe` query `pollutant`,
//! `rocspeciation` and `integratedSpeciesSet` to decide which
//! `(pollutant, process)` pairs to register and which calculators to chain
//! off. Those decisions are pre-resolved in `CalculatorInfo.txt` and
//! `calculator-dag.json`; this port reads its metadata methods from there.
//! * `doExecute` builds the `##mechanismIDs##` filter that narrows the
//! `integratedSpeciesSet` extract to the run's requested mechanisms — the
//! inputs reach `calculate` already filtered.
//! * `needsFinalAggregation` flags whether a context was processed twice and a
//! final re-aggregation pass is owed. That is master-loop orchestration; one
//! `calculate` invocation already sums within itself.
//!
//! The `integratedSpeciesSet.useISSyn` column is **not** consulted by the
//! `Section Processing` SQL — its `inner join … using (pollutantID)` carries
//! no `useISSyn` predicate. `useISSyn` only gates the Java `subscribeToMe`
//! chaining query, so it is not modelled on [`IntegratedSpeciesRow`].
//!
//! # Fidelity notes
//!
//! `MOVESWorkerOutput.emissionQuant` / `.emissionRate` are `FLOAT` (32-bit)
//! columns, so the worker-output rows `calculate` consumes are already
//! `f32`-quantised — the wiring widens them to `f64`. The intermediate
//! `TOGWorkerOutput` table is `DOUBLE`, which the port's `f64` accumulation
//! matches exactly. The final `insert into MOVESWorkerOutput` truncates the
//! `DOUBLE` group sum back to `FLOAT`; the port returns the un-truncated `f64`
//! result and leaves that store-time truncation — a sub-`1e-7` relative drift
//! to the output wiring, matching the `SO2Calculator` / /
//! precedent. Whether to reproduce it bug-for-bug is the calculator
//! integration validation call.
//!
//! The SQL's final `group by` lists 19 columns but its `select` also carries
//! `iterationID`, `engTechID`, `sectorID` and `hpID`, which are *not* grouping
//! keys. MariaDB (with `ONLY_FULL_GROUP_BY` off, as MOVES runs) returns an
//! implementation-defined value for each from somewhere in the group. This
//! port carries those four columns from the first worker-output row that lands
//! in a group. For onroad mechanism data — the only data this chained
//! calculator sees — `sectorID` / `hpID` are the Nonroad keys and are absent,
//! and `iterationID` is the uncertainty iteration (constant outside
//! uncertainty mode), so the four are invariant within a group and the choice
//! is well-defined in practice.
//!
//! `pollutantID` is itself a `group by` column, but every signed contribution
//! is relabelled to `NonHAPTOG` (88) before grouping, so it is constant across
//! the whole result and is not part of the port's group key.
//!
//! # Data plane
//!
//! [`Calculator::execute`] receives a [`CalculatorContext`] whose
//! `ExecutionTables` / `ScratchNamespace` are placeholders until the
//! `DataFrameStore` lands (), so `execute` cannot yet
//! read `integratedSpeciesSet` / `MOVESWorkerOutput` nor write the residual
//! rows back. The numeric algorithm is fully ported and unit-tested on
//! [`calculate`](TogSpeciationCalculator::calculate); `execute` is a documented
//! shell returning an empty [`CalculatorOutput`]. Once the data plane exists,
//! `execute` materialises a [`TogInputs`] from `ctx.tables()`, calls
//! [`calculate`](TogSpeciationCalculator::calculate), and merges the rows into
//! `MOVESWorkerOutput`.

use std::collections::{HashMap, HashSet};

use moves_data::{PollutantId, PollutantProcessAssociation, ProcessId};
use moves_framework::{
    Calculator, CalculatorContext, CalculatorOutput, CalculatorSubscription, DataFrameStoreTyped,
    Error, TableRow,
};
use polars::prelude::{DataFrame, DataType, NamedFrom, PolarsResult, Schema, Series};

/// Stable module name — matches the Java class and the `TOGSpeciationCalculator`
/// entry in the calculator-chain DAG (`calculator-dag.json`).
const CALCULATOR_NAME: &str = "TOGSpeciationCalculator";

/// Non-methane organic gas — `Pollutant` id 80. The positive term of the
/// integration: an NMOG worker-output row contributes `+emission`. The SQL
/// synthesises an NMOG membership row into every integrated species set.
const NMOG_POLLUTANT_ID: i32 = 80;

/// NonHAPTOG — `Pollutant` id 88, the residual this calculator produces. Every
/// output row carries this pollutant; the SQL relabels each signed
/// contribution to 88 before the grouping sum.
const NONHAP_TOG_POLLUTANT_ID: i32 = 88;

// ===========================================================================
// Input tables — plain Rust mirrors of the tables `TOGSpeciationCalculator.sql`
// reads. Following the convention, every `INT`/`SMALLINT` identifier is
// an `i32`; `SCC` is a `CHAR(10)` code, modelled as a `String`. Only the
// columns the algorithm reads are modelled.
// ===========================================================================

/// One `integratedSpeciesSet` row — a pollutant's membership in one mechanism's
/// integrated species set.
///
/// The `CreateDefault.sql` table has a fourth column, `useISSyn`, with primary
/// key `(mechanismID, integratedSpeciesSetID, pollutantID)`. `useISSyn` gates
/// only the Java `subscribeToMe` chaining query, never the `Section Processing`
/// SQL, so it is not modelled (see the [module documentation](self)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IntegratedSpeciesRow {
 /// `mechanismID` — the chemical mechanism this membership belongs to.
    pub mechanism_id: i32,
 /// `integratedSpeciesSetID` — the species set within the mechanism. A
 /// mechanism may carry more than one set; each is integrated separately.
    pub integrated_species_set_id: i32,
 /// `pollutantID` — a member pollutant of the set. NMOG (80) is the positive
 /// integration term; every other member is an integrated species.
    pub pollutant_id: i32,
}

/// One `MOVESWorkerOutput` row — an emission record, used both as the
/// calculator's input pool and as the shape of the rows it produces.
///
/// The dimension columns are `SMALLINT`/`INTEGER` keys (`NULL`-able in the
/// schema; a populated worker-output row carries real ids, modelled here as
/// `i32`). `SCC` is the `CHAR(10)` source-classification code. `emissionQuant`
/// / `emissionRate` are `FLOAT` in the table (see the fidelity note in the
/// [module documentation](self)) and modelled as `f64`.
#[derive(Debug, Clone, PartialEq)]
pub struct WorkerOutputRow {
 /// `MOVESRunID` — a grouping key of the residual sum.
    pub moves_run_id: i32,
 /// `iterationID` — the uncertainty iteration. Carried through but *not* a
 /// grouping key (see the [module documentation](self)).
    pub iteration_id: i32,
 /// `yearID`.
    pub year_id: i32,
 /// `monthID`.
    pub month_id: i32,
 /// `dayID`.
    pub day_id: i32,
 /// `hourID`.
    pub hour_id: i32,
 /// `stateID`.
    pub state_id: i32,
 /// `countyID`.
    pub county_id: i32,
 /// `zoneID`.
    pub zone_id: i32,
 /// `linkID`.
    pub link_id: i32,
 /// `pollutantID` — the emission's pollutant. On an input row this selects
 /// whether the row joins a species set and its integration sign; on an
 /// output row it is always `NonHAPTOG` (88).
    pub pollutant_id: i32,
 /// `processID` — the emission process.
    pub process_id: i32,
 /// `sourceTypeID`.
    pub source_type_id: i32,
 /// `regClassID`.
    pub reg_class_id: i32,
 /// `fuelTypeID`.
    pub fuel_type_id: i32,
 /// `modelYearID`.
    pub model_year_id: i32,
 /// `roadTypeID`.
    pub road_type_id: i32,
 /// `SCC` — the source-classification code; a grouping key of the sum.
    pub scc: String,
 /// `engTechID` — Nonroad engine-technology key. Carried through but *not* a
 /// grouping key.
    pub eng_tech_id: i32,
 /// `sectorID` — Nonroad sector key. Carried through but *not* a grouping
 /// key.
    pub sector_id: i32,
 /// `hpID` — Nonroad horsepower key. Carried through but *not* a grouping
 /// key.
    pub hp_id: i32,
 /// `emissionQuant` — the emission quantity (mass).
    pub emission_quant: f64,
 /// `emissionRate` — the emission rate.
    pub emission_rate: f64,
}

/// Inputs to [`TogSpeciationCalculator::calculate`] — the two tables the SQL's
/// `Section Processing` reads.
///
/// A future (`DataFrameStore`) wiring populates this from the per-run
/// execution database; until then it is the explicit data-plane contract the
/// unit tests build directly.
#[derive(Debug, Clone, Default)]
pub struct TogInputs {
 /// The `integratedSpeciesSet` rows, already filtered to the run's
 /// mechanisms by the SQL's `where mechanismID in (##mechanismIDs##)`
 /// extract.
    pub integrated_species_set: Vec<IntegratedSpeciesRow>,
 /// The `MOVESWorkerOutput` rows the upstream calculators have produced.
 /// `calculate` reads only the rows whose pollutant belongs to a species
 /// set; any other row is ignored, exactly as the SQL's
 /// `where pollutantID in (select … integratedSpeciesSet)` filter does.
    pub worker_output: Vec<WorkerOutputRow>,
}

/// The `group by` key of the SQL's final residual sum.
///
/// Holds the 18 *varying* `group by` columns. The 19th, `pollutantID`, is the
/// literal `NonHAPTOG` (88) for every signed contribution and so is constant
/// across the result. Field declaration order is the [`Ord`] order, chosen so
/// the output sorts in a readable dimensional order — see
/// [`TogSpeciationCalculator::calculate`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct GroupKey {
    year_id: i32,
    month_id: i32,
    day_id: i32,
    hour_id: i32,
    state_id: i32,
    county_id: i32,
    zone_id: i32,
    link_id: i32,
    process_id: i32,
    source_type_id: i32,
    reg_class_id: i32,
    fuel_type_id: i32,
    model_year_id: i32,
    road_type_id: i32,
    scc: String,
    moves_run_id: i32,
    mechanism_id: i32,
    integrated_species_set_id: i32,
}

impl GroupKey {
 /// The group a `(mechanism, set)` fan-out of `raw` belongs to.
    fn for_row(mechanism_id: i32, integrated_species_set_id: i32, raw: &WorkerOutputRow) -> Self {
        Self {
            year_id: raw.year_id,
            month_id: raw.month_id,
            day_id: raw.day_id,
            hour_id: raw.hour_id,
            state_id: raw.state_id,
            county_id: raw.county_id,
            zone_id: raw.zone_id,
            link_id: raw.link_id,
            process_id: raw.process_id,
            source_type_id: raw.source_type_id,
            reg_class_id: raw.reg_class_id,
            fuel_type_id: raw.fuel_type_id,
            model_year_id: raw.model_year_id,
            road_type_id: raw.road_type_id,
            scc: raw.scc.clone(),
            moves_run_id: raw.moves_run_id,
            mechanism_id,
            integrated_species_set_id,
        }
    }
}

/// One in-progress residual sum — the signed `emissionQuant` / `emissionRate`
/// totals plus the first-encountered row that fixes the non-grouped columns.
#[derive(Debug, Clone)]
struct Accumulator {
 /// The first worker-output row that landed in this group, with its
 /// pollutant relabelled to `NonHAPTOG`. Supplies every output column except
 /// the two emission values.
    template: WorkerOutputRow,
 /// Running `Σ ± emissionQuant`.
    emission_quant: f64,
 /// Running `Σ ± emissionRate`.
    emission_rate: f64,
}

/// The MOVES total-organic-gas speciation calculator.
///
/// A zero-sized value type: it owns no per-run state, exactly as the
/// [`Calculator`] trait contract requires. All run-varying input flows through
/// the [`TogInputs`] argument to [`calculate`](Self::calculate).
#[derive(Debug, Clone, Copy, Default)]
pub struct TogSpeciationCalculator;

impl TogSpeciationCalculator {
 /// Stable module name — matches the Java class and the chain-DAG entry.
    pub const NAME: &'static str = CALCULATOR_NAME;

 /// Compute the `NonHAPTOG` (88) residual rows — the port of the
 /// `TOGSpeciationCalculator.sql` `Section Processing`.
 ///
 /// Returns no rows when no worker-output pollutant belongs to a species
 /// set: the SQL's `inner join integratedSpeciesSet using (pollutantID)`
 /// drops every row that fails to join. The result is sorted by the
 /// `GroupKey` ordering — the dimensional cell, then `(mechanism, set)` — so
 /// the output is deterministic; MOVES leaves `MOVESWorkerOutput` physically
 /// unordered (the SQL `INSERT … SELECT` carries `order by null`).
    #[must_use]
    pub fn calculate(&self, inputs: &TogInputs) -> Vec<WorkerOutputRow> {
 // Step 1: synthesise the NMOG (80) membership row into every set.
        let species = Self::with_synthesized_nmog(&inputs.integrated_species_set);
 // The step-2 fan-out index: pollutantID → the (mechanism, set) pairs
 // that list it. One worker-output row fans to every such pair.
        let sets_by_pollutant = Self::index_sets_by_pollutant(&species);

 // Steps 2 and 3: sign each worker-output row, fan it across its sets,
 // and accumulate the per-group residual sum.
        let mut groups: HashMap<GroupKey, Accumulator> = HashMap::new();
        for raw in &inputs.worker_output {
            let Some(sets) = sets_by_pollutant.get(&raw.pollutant_id) else {
                continue;
            };
 // NMOG is the positive integration term; every other set member is
 // an integrated species and is subtracted.
            let sign = if raw.pollutant_id == NMOG_POLLUTANT_ID {
                1.0
            } else {
                -1.0
            };
            for &(mechanism_id, integrated_species_set_id) in sets {
                let key = GroupKey::for_row(mechanism_id, integrated_species_set_id, raw);
                let acc = groups.entry(key).or_insert_with(|| Accumulator {
                    template: WorkerOutputRow {
                        pollutant_id: NONHAP_TOG_POLLUTANT_ID,
                        emission_quant: 0.0,
                        emission_rate: 0.0,
                        ..raw.clone()
                    },
                    emission_quant: 0.0,
                    emission_rate: 0.0,
                });
                acc.emission_quant += sign * raw.emission_quant;
                acc.emission_rate += sign * raw.emission_rate;
            }
        }

 // Sort by GroupKey for a deterministic result, then materialise each
 // group: the SQL clamps the sum to non-negative with `greatest(…, 0)`.
        let mut entries: Vec<(GroupKey, Accumulator)> = groups.into_iter().collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        entries
            .into_iter()
            .map(|(_, acc)| WorkerOutputRow {
                emission_quant: acc.emission_quant.max(0.0),
                emission_rate: acc.emission_rate.max(0.0),
                ..acc.template
            })
            .collect()
    }

 /// Port of the SQL's NMOG synthesis (`insert ignore … select distinct
 /// mechanismID, integratedSpeciesSetID, 80 … where pollutantID <> 80`).
 ///
 /// Every distinct `(mechanismID, integratedSpeciesSetID)` carrying a
 /// non-NMOG pollutant gains an NMOG (80) row, unless it already has one.
 /// `insert ignore` skips on the `(mechanism, set, pollutant)` primary key;
 /// the `HashSet` of present `(mechanism, set, pollutant)` triples
 /// reproduces that, so an explicit NMOG row is never duplicated.
    fn with_synthesized_nmog(extracted: &[IntegratedSpeciesRow]) -> Vec<IntegratedSpeciesRow> {
        let mut species = extracted.to_vec();
        let mut present: HashSet<(i32, i32, i32)> = species
            .iter()
            .map(|r| (r.mechanism_id, r.integrated_species_set_id, r.pollutant_id))
            .collect();
        for row in extracted {
            if row.pollutant_id == NMOG_POLLUTANT_ID {
                continue;
            }
            let nmog_key = (
                row.mechanism_id,
                row.integrated_species_set_id,
                NMOG_POLLUTANT_ID,
            );
            if present.insert(nmog_key) {
                species.push(IntegratedSpeciesRow {
                    mechanism_id: row.mechanism_id,
                    integrated_species_set_id: row.integrated_species_set_id,
                    pollutant_id: NMOG_POLLUTANT_ID,
                });
            }
        }
        species
    }

 /// Index the species set by pollutant — the lookup side of the SQL's
 /// `inner join integratedSpeciesSet using (pollutantID)`.
 ///
 /// A pollutant maps to every `(mechanism, set)` that lists it, in
 /// first-seen order, so the fan-out — and therefore the first-encountered
 /// row each group keeps — is deterministic for a given input order. The
 /// `integratedSpeciesSet` primary key guarantees one row per
 /// `(mechanism, set, pollutant)`, so no `(mechanism, set)` repeats.
    fn index_sets_by_pollutant(species: &[IntegratedSpeciesRow]) -> HashMap<i32, Vec<(i32, i32)>> {
        let mut index: HashMap<i32, Vec<(i32, i32)>> = HashMap::new();
        for row in species {
            index
                .entry(row.pollutant_id)
                .or_default()
                .push((row.mechanism_id, row.integrated_species_set_id));
        }
        index
    }
}

fn row_err(table: &'static str, row: usize, column: &'static str, msg: String) -> Error {
    Error::RowExtraction {
        table: table.into(),
        row,
        column: column.into(),
        message: msg,
    }
}

impl TableRow for IntegratedSpeciesRow {
    fn table_name() -> &'static str {
        "integratedSpeciesSet"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("mechanismID".into(), DataType::Int32),
            ("integratedSpeciesSetID".into(), DataType::Int32),
            ("pollutantID".into(), DataType::Int32),
        ])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "mechanismID".into(),
                    rows.iter().map(|r| r.mechanism_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "integratedSpeciesSetID".into(),
                    rows.iter()
                        .map(|r| r.integrated_species_set_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "pollutantID".into(),
                    rows.iter().map(|r| r.pollutant_id).collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "integratedSpeciesSet";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let mechanism = get_i32("mechanismID")?;
        let set_id = get_i32("integratedSpeciesSetID")?;
        let pollutant = get_i32("pollutantID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(IntegratedSpeciesRow {
                    mechanism_id: mechanism.get(i).ok_or_else(|| null("mechanismID"))?,
                    integrated_species_set_id: set_id
                        .get(i)
                        .ok_or_else(|| null("integratedSpeciesSetID"))?,
                    pollutant_id: pollutant.get(i).ok_or_else(|| null("pollutantID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for WorkerOutputRow {
    fn table_name() -> &'static str {
        "MOVESWorkerOutput"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("MOVESRunID".into(), DataType::Int32),
            ("iterationID".into(), DataType::Int32),
            ("yearID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("dayID".into(), DataType::Int32),
            ("hourID".into(), DataType::Int32),
            ("stateID".into(), DataType::Int32),
            ("countyID".into(), DataType::Int32),
            ("zoneID".into(), DataType::Int32),
            ("linkID".into(), DataType::Int32),
            ("pollutantID".into(), DataType::Int32),
            ("processID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("regClassID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("modelYearID".into(), DataType::Int32),
            ("roadTypeID".into(), DataType::Int32),
            ("SCC".into(), DataType::String),
            ("engTechID".into(), DataType::Int32),
            ("sectorID".into(), DataType::Int32),
            ("hpID".into(), DataType::Int32),
            ("emissionQuant".into(), DataType::Float64),
            ("emissionRate".into(), DataType::Float64),
        ])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "MOVESRunID".into(),
                    rows.iter().map(|r| r.moves_run_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "iterationID".into(),
                    rows.iter().map(|r| r.iteration_id).collect::<Vec<i32>>(),
                )
                .into(),
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
                    "hourID".into(),
                    rows.iter().map(|r| r.hour_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "stateID".into(),
                    rows.iter().map(|r| r.state_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "countyID".into(),
                    rows.iter().map(|r| r.county_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "zoneID".into(),
                    rows.iter().map(|r| r.zone_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "linkID".into(),
                    rows.iter().map(|r| r.link_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "pollutantID".into(),
                    rows.iter().map(|r| r.pollutant_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "processID".into(),
                    rows.iter().map(|r| r.process_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "regClassID".into(),
                    rows.iter().map(|r| r.reg_class_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelTypeID".into(),
                    rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "modelYearID".into(),
                    rows.iter().map(|r| r.model_year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "roadTypeID".into(),
                    rows.iter().map(|r| r.road_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "SCC".into(),
                    rows.iter().map(|r| r.scc.clone()).collect::<Vec<String>>(),
                )
                .into(),
                Series::new(
                    "engTechID".into(),
                    rows.iter().map(|r| r.eng_tech_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sectorID".into(),
                    rows.iter().map(|r| r.sector_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "hpID".into(),
                    rows.iter().map(|r| r.hp_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "emissionQuant".into(),
                    rows.iter().map(|r| r.emission_quant).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "emissionRate".into(),
                    rows.iter().map(|r| r.emission_rate).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "MOVESWorkerOutput";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .f64()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let moves_run = get_i32("MOVESRunID")?;
        let iteration = get_i32("iterationID")?;
        let year = get_i32("yearID")?;
        let month = get_i32("monthID")?;
        let day = get_i32("dayID")?;
        let hour = get_i32("hourID")?;
        let state = get_i32("stateID")?;
        let county = get_i32("countyID")?;
        let zone = get_i32("zoneID")?;
        let link = get_i32("linkID")?;
        let pollutant = get_i32("pollutantID")?;
        let process = get_i32("processID")?;
        let src_type = get_i32("sourceTypeID")?;
        let reg_class = get_i32("regClassID")?;
        let fuel_type = get_i32("fuelTypeID")?;
        let model_year = get_i32("modelYearID")?;
        let road_type = get_i32("roadTypeID")?;
        let scc_ca = df
            .column("SCC")
            .map_err(|e| row_err(t, 0, "SCC", e.to_string()))?
            .str()
            .map_err(|e| row_err(t, 0, "SCC", e.to_string()))?;
        let eng_tech = get_i32("engTechID")?;
        let sector = get_i32("sectorID")?;
        let hp = get_i32("hpID")?;
        let emission_quant = get_f64("emissionQuant")?;
        let emission_rate = get_f64("emissionRate")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(WorkerOutputRow {
                    moves_run_id: moves_run.get(i).ok_or_else(|| null("MOVESRunID"))?,
                    iteration_id: iteration.get(i).ok_or_else(|| null("iterationID"))?,
                    year_id: year.get(i).ok_or_else(|| null("yearID"))?,
                    month_id: month.get(i).ok_or_else(|| null("monthID"))?,
                    day_id: day.get(i).ok_or_else(|| null("dayID"))?,
                    hour_id: hour.get(i).ok_or_else(|| null("hourID"))?,
                    state_id: state.get(i).ok_or_else(|| null("stateID"))?,
                    county_id: county.get(i).ok_or_else(|| null("countyID"))?,
                    zone_id: zone.get(i).ok_or_else(|| null("zoneID"))?,
                    link_id: link.get(i).ok_or_else(|| null("linkID"))?,
                    pollutant_id: pollutant.get(i).ok_or_else(|| null("pollutantID"))?,
                    process_id: process.get(i).ok_or_else(|| null("processID"))?,
                    source_type_id: src_type.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    reg_class_id: reg_class.get(i).ok_or_else(|| null("regClassID"))?,
                    fuel_type_id: fuel_type.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    model_year_id: model_year.get(i).ok_or_else(|| null("modelYearID"))?,
                    road_type_id: road_type.get(i).ok_or_else(|| null("roadTypeID"))?,
                    scc: scc_ca
                        .get(i)
                        .map(|s| s.to_string())
                        .ok_or_else(|| null("SCC"))?,
                    eng_tech_id: eng_tech.get(i).ok_or_else(|| null("engTechID"))?,
                    sector_id: sector.get(i).ok_or_else(|| null("sectorID"))?,
                    hp_id: hp.get(i).ok_or_else(|| null("hpID"))?,
                    emission_quant: emission_quant.get(i).ok_or_else(|| null("emissionQuant"))?,
                    emission_rate: emission_rate.get(i).ok_or_else(|| null("emissionRate"))?,
                })
            })
            .collect()
    }
}

/// No subscriptions — `TOGSpeciationCalculator` is a chained calculator
/// (`calculator-dag.json`: `subscribes_directly: false`, `subscriptions: []`).
static NO_SUBSCRIPTIONS: &[CalculatorSubscription] = &[];

/// Count of `Registration` directives `CalculatorInfo.txt` records for
/// `TOGSpeciationCalculator` — `registrations_count: 184` in
/// `calculator-dag.json`.
const REGISTRATION_COUNT: usize = 184;

/// The `(pollutantID, processID)` pairs of every `Registration` directive
/// `CalculatorInfo.txt` records for `TOGSpeciationCalculator`, in file order.
///
/// Per process: the 16 CB05 mechanism pseudo-pollutants (1000, 1001, 1002,
/// 1005, 1006, 1008–1018) and `NonHAPTOG` (88), minus the few mechanism
/// pollutants a given process does not carry — hence 13–17 pairs per process.
/// Built into [`REGISTRATIONS`] by [`build_registrations`].
const REGISTRATION_PAIRS: [(u16, u16); REGISTRATION_COUNT] = [
 // Running Exhaust (1) — 17 pollutants
    (1000, 1),
    (88, 1),
    (1001, 1),
    (1002, 1),
    (1005, 1),
    (1006, 1),
    (1008, 1),
    (1009, 1),
    (1012, 1),
    (1013, 1),
    (1015, 1),
    (1017, 1),
    (1018, 1),
    (1010, 1),
    (1011, 1),
    (1014, 1),
    (1016, 1),
 // Start Exhaust (2) — 17 pollutants
    (1000, 2),
    (88, 2),
    (1001, 2),
    (1002, 2),
    (1005, 2),
    (1006, 2),
    (1008, 2),
    (1009, 2),
    (1012, 2),
    (1013, 2),
    (1015, 2),
    (1017, 2),
    (1018, 2),
    (1010, 2),
    (1011, 2),
    (1014, 2),
    (1016, 2),
 // Evap Permeation (11) — 16 pollutants
    (1000, 11),
    (88, 11),
    (1001, 11),
    (1002, 11),
    (1005, 11),
    (1008, 11),
    (1009, 11),
    (1010, 11),
    (1011, 11),
    (1012, 11),
    (1013, 11),
    (1014, 11),
    (1015, 11),
    (1017, 11),
    (1018, 11),
    (1006, 11),
 // Evap Fuel Vapor Venting (12) — 13 pollutants
    (1000, 12),
    (88, 12),
    (1001, 12),
    (1002, 12),
    (1008, 12),
    (1009, 12),
    (1010, 12),
    (1012, 12),
    (1013, 12),
    (1015, 12),
    (1017, 12),
    (1018, 12),
    (1014, 12),
 // Evap Fuel Leaks (13) — 13 pollutants
    (1000, 13),
    (88, 13),
    (1001, 13),
    (1002, 13),
    (1008, 13),
    (1009, 13),
    (1010, 13),
    (1012, 13),
    (1013, 13),
    (1015, 13),
    (1017, 13),
    (1018, 13),
    (1014, 13),
 // Crankcase Running Exhaust (15) — 17 pollutants
    (1000, 15),
    (88, 15),
    (1001, 15),
    (1002, 15),
    (1005, 15),
    (1006, 15),
    (1008, 15),
    (1009, 15),
    (1012, 15),
    (1013, 15),
    (1015, 15),
    (1017, 15),
    (1018, 15),
    (1010, 15),
    (1011, 15),
    (1014, 15),
    (1016, 15),
 // Crankcase Start Exhaust (16) — 17 pollutants
    (1000, 16),
    (88, 16),
    (1001, 16),
    (1002, 16),
    (1005, 16),
    (1006, 16),
    (1008, 16),
    (1009, 16),
    (1012, 16),
    (1013, 16),
    (1015, 16),
    (1017, 16),
    (1018, 16),
    (1010, 16),
    (1011, 16),
    (1014, 16),
    (1016, 16),
 // Crankcase Extended Idle Exhaust (17) — 16 pollutants
    (1000, 17),
    (88, 17),
    (1001, 17),
    (1002, 17),
    (1005, 17),
    (1006, 17),
    (1008, 17),
    (1009, 17),
    (1010, 17),
    (1012, 17),
    (1013, 17),
    (1014, 17),
    (1015, 17),
    (1016, 17),
    (1017, 17),
    (1018, 17),
 // Refueling Displacement Vapor Loss (18) — 13 pollutants
    (1000, 18),
    (88, 18),
    (1001, 18),
    (1002, 18),
    (1008, 18),
    (1009, 18),
    (1010, 18),
    (1012, 18),
    (1013, 18),
    (1014, 18),
    (1015, 18),
    (1017, 18),
    (1018, 18),
 // Refueling Spillage Loss (19) — 13 pollutants
    (1000, 19),
    (88, 19),
    (1001, 19),
    (1002, 19),
    (1008, 19),
    (1009, 19),
    (1010, 19),
    (1012, 19),
    (1013, 19),
    (1015, 19),
    (1017, 19),
    (1018, 19),
    (1014, 19),
 // Extended Idle Exhaust (90) — 16 pollutants
    (1000, 90),
    (88, 90),
    (1001, 90),
    (1002, 90),
    (1005, 90),
    (1006, 90),
    (1008, 90),
    (1009, 90),
    (1010, 90),
    (1012, 90),
    (1013, 90),
    (1014, 90),
    (1015, 90),
    (1016, 90),
    (1017, 90),
    (1018, 90),
 // Auxiliary Power Exhaust (91) — 16 pollutants
    (1000, 91),
    (88, 91),
    (1001, 91),
    (1002, 91),
    (1005, 91),
    (1006, 91),
    (1008, 91),
    (1009, 91),
    (1010, 91),
    (1012, 91),
    (1013, 91),
    (1014, 91),
    (1015, 91),
    (1016, 91),
    (1017, 91),
    (1018, 91),
];

/// Build [`REGISTRATIONS`] from the compact [`REGISTRATION_PAIRS`] data at
/// compile time — one [`PollutantProcessAssociation`] per `(pollutant,
/// process)` pair.
const fn build_registrations() -> [PollutantProcessAssociation; REGISTRATION_COUNT] {
    let mut regs = [PollutantProcessAssociation {
        pollutant_id: PollutantId(0),
        process_id: ProcessId(0),
    }; REGISTRATION_COUNT];
    let mut i = 0;
    while i < REGISTRATION_COUNT {
        let (pollutant, process) = REGISTRATION_PAIRS[i];
        regs[i] = PollutantProcessAssociation {
            pollutant_id: PollutantId(pollutant),
            process_id: ProcessId(process),
        };
        i += 1;
    }
    regs
}

/// The `(pollutant, process)` pairs `TOGSpeciationCalculator` registers — the
/// 184 `Registration` directives `CalculatorInfo.txt` records for it
/// (`calculator-dag.json`: `registrations_count: 184`).
static REGISTRATIONS: [PollutantProcessAssociation; REGISTRATION_COUNT] = build_registrations();

/// The calculators `TOGSpeciationCalculator` chains off — `calculator-dag.json`
/// records `depends_on: ["AirToxicsCalculator",
/// "CrankcaseEmissionCalculatorNonPM", "HCSpeciationCalculator"]`. They produce
/// the mechanism species and organic-gas inputs the integration consumes.
static UPSTREAM: &[&str] = &[
    "AirToxicsCalculator",
    "CrankcaseEmissionCalculatorNonPM",
    "HCSpeciationCalculator",
];

/// Tables the `Section Processing` reads — `integratedSpeciesSet` (the SQL's
/// "Extract Data" section) and `MOVESWorkerOutput` (the upstream calculators'
/// output, already present on the worker).
static INPUT_TABLES: &[&str] = &["MOVESWorkerOutput", "integratedSpeciesSet"];

impl Calculator for TogSpeciationCalculator {
    fn name(&self) -> &'static str {
        Self::NAME
    }

 /// `TOGSpeciationCalculator` is a chained calculator: it does not subscribe
 /// to the MasterLoop directly but fires when its upstream calculators do.
 /// `calculator-dag.json` records `subscribes_directly: false` and an empty
 /// `subscriptions` list.
    fn subscriptions(&self) -> &[CalculatorSubscription] {
        NO_SUBSCRIPTIONS
    }

    fn registrations(&self) -> &[PollutantProcessAssociation] {
        &REGISTRATIONS
    }

 /// `TOGSpeciationCalculator` chains off `AirToxicsCalculator`,
 /// `CrankcaseEmissionCalculatorNonPM` and `HCSpeciationCalculator` /// `calculator-dag.json` records them as `depends_on`.
    fn upstream(&self) -> &[&'static str] {
        UPSTREAM
    }

    fn input_tables(&self) -> &[&'static str] {
        INPUT_TABLES
    }

    fn execute(&self, ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        let tables = ctx.tables();
        let inputs = TogInputs {
            integrated_species_set: tables.iter_typed("integratedSpeciesSet")?,
            worker_output: tables.iter_typed("MOVESWorkerOutput")?,
        };
        let rows = self.calculate(&inputs);
        crate::wiring::emit_rows(rows)
    }
}

/// Construct the calculator as a boxed trait object — matches the engine's
/// calculator-factory signature so the registry can register it.
#[must_use]
pub fn factory() -> Box<dyn Calculator> {
    Box::new(TogSpeciationCalculator)
}

#[cfg(test)]
mod tests {
    use super::*;

 /// A worker-output row for the canonical test dimensional cell, carrying
 /// `pollutant_id` and the emission `quant` / `rate`. Tests override
 /// individual dimension fields with struct-update syntax to move a row to a
 /// different cell.
    fn raw(pollutant_id: i32, quant: f64, rate: f64) -> WorkerOutputRow {
        WorkerOutputRow {
            moves_run_id: 1,
            iteration_id: 1,
            year_id: 2020,
            month_id: 7,
            day_id: 5,
            hour_id: 8,
            state_id: 26,
            county_id: 26_161,
            zone_id: 261_610,
            link_id: 2_616_101,
            pollutant_id,
            process_id: 1,
            source_type_id: 21,
            reg_class_id: 30,
            fuel_type_id: 1,
            model_year_id: 2018,
            road_type_id: 4,
            scc: "2201001110".to_string(),
            eng_tech_id: 0,
            sector_id: 0,
            hp_id: 0,
            emission_quant: quant,
            emission_rate: rate,
        }
    }

 /// One `integratedSpeciesSet` membership row.
    fn iss(mechanism_id: i32, set_id: i32, pollutant_id: i32) -> IntegratedSpeciesRow {
        IntegratedSpeciesRow {
            mechanism_id,
            integrated_species_set_id: set_id,
            pollutant_id,
        }
    }

 /// Assert `actual` matches `expected` within `f64` slack.
    fn assert_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 1e-9,
            "{actual} != expected {expected}",
        );
    }

    #[test]
    fn calculate_integrates_nmog_minus_the_species() {
 // One mechanism, one set with two integrated species (ALD2, PAR).
 // NonHAPTOG = NMOG − ALD2 − PAR = 100 − 20 − 30 = 50, on both channels.
        let inputs = TogInputs {
            integrated_species_set: vec![iss(1, 1, 1001), iss(1, 1, 1013)],
            worker_output: vec![
                raw(NMOG_POLLUTANT_ID, 100.0, 10.0),
                raw(1001, 20.0, 2.0),
                raw(1013, 30.0, 3.0),
            ],
        };
        let rows = TogSpeciationCalculator.calculate(&inputs);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].pollutant_id, NONHAP_TOG_POLLUTANT_ID);
        assert_close(rows[0].emission_quant, 50.0);
        assert_close(rows[0].emission_rate, 5.0);
    }

    #[test]
    fn calculate_clamps_a_negative_residual_to_zero() {
 // Integrated species outweigh NMOG: 40 − 30 − 30 = −20 → clamped to 0.
        let inputs = TogInputs {
            integrated_species_set: vec![iss(1, 1, 1001), iss(1, 1, 1013)],
            worker_output: vec![
                raw(NMOG_POLLUTANT_ID, 40.0, 4.0),
                raw(1001, 30.0, 3.0),
                raw(1013, 30.0, 3.0),
            ],
        };
        let rows = TogSpeciationCalculator.calculate(&inputs);
        assert_eq!(rows.len(), 1);
        assert_close(rows[0].emission_quant, 0.0);
        assert_close(rows[0].emission_rate, 0.0);
    }

    #[test]
    fn calculate_synthesises_nmog_when_the_set_omits_it() {
 // The set lists only a species. The SQL synthesises an NMOG (80)
 // membership row, so an NMOG worker row still joins and is added.
 // NonHAPTOG = 100 − 20 = 80.
        let inputs = TogInputs {
            integrated_species_set: vec![iss(1, 1, 1001)],
            worker_output: vec![raw(NMOG_POLLUTANT_ID, 100.0, 0.0), raw(1001, 20.0, 0.0)],
        };
        let rows = TogSpeciationCalculator.calculate(&inputs);
        assert_eq!(rows.len(), 1);
        assert_close(rows[0].emission_quant, 80.0);
    }

    #[test]
    fn calculate_does_not_double_count_an_explicit_nmog_membership() {
 // The set already lists NMOG explicitly; the `insert ignore` must not
 // add a second one. NonHAPTOG = 100 − 20 = 80, not 100 + 100 − 20.
        let inputs = TogInputs {
            integrated_species_set: vec![iss(1, 1, NMOG_POLLUTANT_ID), iss(1, 1, 1001)],
            worker_output: vec![raw(NMOG_POLLUTANT_ID, 100.0, 0.0), raw(1001, 20.0, 0.0)],
        };
        let rows = TogSpeciationCalculator.calculate(&inputs);
        assert_eq!(rows.len(), 1);
        assert_close(rows[0].emission_quant, 80.0);
    }

    #[test]
    fn calculate_ignores_a_pollutant_absent_from_every_set() {
 // A CO (2) worker row joins no species set — the SQL's
 // `where pollutantID in (select … integratedSpeciesSet)` drops it.
        let inputs = TogInputs {
            integrated_species_set: vec![iss(1, 1, 1001)],
            worker_output: vec![
                raw(NMOG_POLLUTANT_ID, 100.0, 0.0),
                raw(1001, 20.0, 0.0),
                raw(2, 999.0, 99.0),
            ],
        };
        let rows = TogSpeciationCalculator.calculate(&inputs);
        assert_eq!(rows.len(), 1);
        assert_close(rows[0].emission_quant, 80.0);
        assert!(rows
            .iter()
            .all(|r| r.pollutant_id == NONHAP_TOG_POLLUTANT_ID));
    }

    #[test]
    fn calculate_fans_one_mechanism_across_its_species_sets() {
 // Mechanism 1 has two sets: set 1 = {ALD2}, set 2 = {PAR}. NMOG is
 // synthesised into both. Each set yields its own NonHAPTOG row.
        let inputs = TogInputs {
            integrated_species_set: vec![iss(1, 1, 1001), iss(1, 2, 1013)],
            worker_output: vec![
                raw(NMOG_POLLUTANT_ID, 100.0, 0.0),
                raw(1001, 20.0, 0.0),
                raw(1013, 30.0, 0.0),
            ],
        };
        let rows = TogSpeciationCalculator.calculate(&inputs);
        assert_eq!(rows.len(), 2);
 // Both rows share the dimensional cell; they differ only by the
 // (now-dropped) mechanism/set. Set 1: 100 − 20; set 2: 100 − 30.
        let mut quants: Vec<f64> = rows.iter().map(|r| r.emission_quant).collect();
        quants.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_close(quants[0], 70.0);
        assert_close(quants[1], 80.0);
    }

    #[test]
    fn calculate_fans_a_species_shared_by_two_sets() {
 // ALD2 (1001) belongs to two sets of mechanism 1. One ALD2 worker row
 // is subtracted from both sets' residuals.
        let inputs = TogInputs {
            integrated_species_set: vec![iss(1, 1, 1001), iss(1, 2, 1001)],
            worker_output: vec![raw(NMOG_POLLUTANT_ID, 100.0, 0.0), raw(1001, 20.0, 0.0)],
        };
        let rows = TogSpeciationCalculator.calculate(&inputs);
        assert_eq!(rows.len(), 2);
        for r in &rows {
            assert_close(r.emission_quant, 80.0);
        }
    }

    #[test]
    fn calculate_sums_repeated_rows_and_keeps_the_first_non_grouped_columns() {
 // Two NMOG rows in one cell, differing only in engTechID/iterationID
 // (not grouping keys): they sum, and the output carries the first
 // row's non-grouped columns. NonHAPTOG = (100 + 40) − 20 = 120.
        let first_nmog = WorkerOutputRow {
            eng_tech_id: 7,
            iteration_id: 3,
            ..raw(NMOG_POLLUTANT_ID, 100.0, 0.0)
        };
        let second_nmog = WorkerOutputRow {
            eng_tech_id: 9,
            iteration_id: 4,
            ..raw(NMOG_POLLUTANT_ID, 40.0, 0.0)
        };
        let inputs = TogInputs {
            integrated_species_set: vec![iss(1, 1, 1001)],
            worker_output: vec![first_nmog, second_nmog, raw(1001, 20.0, 0.0)],
        };
        let rows = TogSpeciationCalculator.calculate(&inputs);
        assert_eq!(rows.len(), 1);
        assert_close(rows[0].emission_quant, 120.0);
        assert_eq!(rows[0].eng_tech_id, 7);
        assert_eq!(rows[0].iteration_id, 3);
    }

    #[test]
    fn calculate_separates_distinct_dimensional_cells() {
 // Two NMOG rows differing in a grouping key (linkID, then SCC) stay in
 // separate residual rows.
        let inputs = TogInputs {
            integrated_species_set: vec![iss(1, 1, 1001)],
            worker_output: vec![
                raw(NMOG_POLLUTANT_ID, 100.0, 0.0),
                WorkerOutputRow {
                    link_id: 9_999_999,
                    ..raw(NMOG_POLLUTANT_ID, 60.0, 0.0)
                },
                WorkerOutputRow {
                    scc: "2201020000".to_string(),
                    ..raw(NMOG_POLLUTANT_ID, 25.0, 0.0)
                },
            ],
        };
        let rows = TogSpeciationCalculator.calculate(&inputs);
 // Three NMOG cells, no species rows → three residuals (each = NMOG).
        assert_eq!(rows.len(), 3);
        let mut quants: Vec<f64> = rows.iter().map(|r| r.emission_quant).collect();
        quants.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_close(quants[0], 25.0);
        assert_close(quants[1], 60.0);
        assert_close(quants[2], 100.0);
    }

    #[test]
    fn calculate_carries_the_dimensional_cell_through() {
        let inputs = TogInputs {
            integrated_species_set: vec![iss(1, 1, 1001)],
            worker_output: vec![raw(NMOG_POLLUTANT_ID, 100.0, 0.0), raw(1001, 20.0, 0.0)],
        };
        let rows = TogSpeciationCalculator.calculate(&inputs);
        assert_eq!(rows.len(), 1);
        let r = &rows[0];
        assert_eq!(r.moves_run_id, 1);
        assert_eq!(r.year_id, 2020);
        assert_eq!(r.month_id, 7);
        assert_eq!(r.day_id, 5);
        assert_eq!(r.hour_id, 8);
        assert_eq!(r.state_id, 26);
        assert_eq!(r.county_id, 26_161);
        assert_eq!(r.zone_id, 261_610);
        assert_eq!(r.link_id, 2_616_101);
        assert_eq!(r.process_id, 1);
        assert_eq!(r.source_type_id, 21);
        assert_eq!(r.reg_class_id, 30);
        assert_eq!(r.fuel_type_id, 1);
        assert_eq!(r.model_year_id, 2018);
        assert_eq!(r.road_type_id, 4);
        assert_eq!(r.scc, "2201001110");
    }

    #[test]
    fn calculate_yields_no_rows_without_a_species_set() {
 // Worker rows but no integratedSpeciesSet → nothing joins.
        let inputs = TogInputs {
            integrated_species_set: vec![],
            worker_output: vec![raw(NMOG_POLLUTANT_ID, 100.0, 0.0), raw(1001, 20.0, 0.0)],
        };
        assert!(TogSpeciationCalculator.calculate(&inputs).is_empty());
    }

    #[test]
    fn calculate_empty_inputs_yield_no_rows() {
        assert!(TogSpeciationCalculator
            .calculate(&TogInputs::default())
            .is_empty());
    }

    #[test]
    fn calculate_output_is_sorted_and_deterministic() {
 // Input rows in an unsorted order across two cells and two mechanisms;
 // the result must be byte-identical across runs and dimension-sorted.
        let inputs = TogInputs {
            integrated_species_set: vec![iss(2, 1, 1001), iss(1, 1, 1001)],
            worker_output: vec![
                WorkerOutputRow {
                    link_id: 9_999_999,
                    ..raw(NMOG_POLLUTANT_ID, 60.0, 0.0)
                },
                raw(NMOG_POLLUTANT_ID, 100.0, 0.0),
                raw(1001, 20.0, 0.0),
            ],
        };
        let first = TogSpeciationCalculator.calculate(&inputs);
        let second = TogSpeciationCalculator.calculate(&inputs);
        assert_eq!(first, second, "calculate is not deterministic");
        assert!(
            first.windows(2).all(|w| w[0].link_id <= w[1].link_id),
            "output is not sorted by dimensional cell",
        );
    }

    #[test]
    fn calculator_name_matches_the_dag_module() {
        assert_eq!(TogSpeciationCalculator.name(), "TOGSpeciationCalculator");
        assert_eq!(TogSpeciationCalculator::NAME, "TOGSpeciationCalculator");
    }

    #[test]
    fn calculator_is_chained_with_no_subscriptions() {
 // calculator-dag.json: subscribes_directly false, subscriptions [].
        assert!(TogSpeciationCalculator.subscriptions().is_empty());
    }

    #[test]
    fn registrations_match_the_calculator_info_directives() {
 // calculator-dag.json records registrations_count 184.
        let regs = TogSpeciationCalculator.registrations();
        assert_eq!(regs.len(), 184);

 // Twelve organic-gas processes, each registering NonHAPTOG (88).
        let mut counts: HashMap<u16, usize> = HashMap::new();
        for r in regs {
 *counts.entry(r.process_id.0).or_default() += 1;
        }
        let mut processes: Vec<u16> = counts.keys().copied().collect();
        processes.sort_unstable();
        assert_eq!(
            processes,
            vec![1, 2, 11, 12, 13, 15, 16, 17, 18, 19, 90, 91]
        );
 // Per-process pollutant counts, in the same process order.
        let per_process: Vec<usize> = processes.iter().map(|p| counts[p]).collect();
        assert_eq!(
            per_process,
            vec![17, 17, 16, 13, 13, 17, 17, 16, 13, 13, 16, 16]
        );

 // Every process registers NonHAPTOG (88) — the pollutant the algorithm
 // actually produces.
        for p in processes {
            assert!(
                regs.iter()
                    .any(|r| r.process_id.0 == p && r.pollutant_id.0 == 88),
                "process {p} does not register NonHAPTOG",
            );
        }
 // Pollutants are the mechanism pseudo-pollutants plus NonHAPTOG.
        let allowed: HashSet<u16> = [
            88, 1000, 1001, 1002, 1005, 1006, 1008, 1009, 1010, 1011, 1012, 1013, 1014, 1015, 1016,
            1017, 1018,
        ]
        .into_iter()
        .collect();
        assert!(regs.iter().all(|r| allowed.contains(&r.pollutant_id.0)));
    }

    #[test]
    fn calculator_chains_off_the_three_upstream_calculators() {
 // calculator-dag.json records the depends_on triple.
        assert_eq!(
            TogSpeciationCalculator.upstream(),
            &[
                "AirToxicsCalculator",
                "CrankcaseEmissionCalculatorNonPM",
                "HCSpeciationCalculator",
            ],
        );
    }

    #[test]
    fn calculator_declares_input_tables() {
        let tables = TogSpeciationCalculator.input_tables();
        for expected in ["MOVESWorkerOutput", "integratedSpeciesSet"] {
            assert!(tables.contains(&expected), "missing input table {expected}");
        }
    }

    #[test]
    fn execute_wires_through_data_plane() {
        use moves_framework::DataFrameStore;
 // One NMOG (80) row and one integrated species (ALD2=1001) in mechanism 1,
 // set 1. NonHAPTOG = 100 - 20 = 80.
        let worker_rows = vec![raw(NMOG_POLLUTANT_ID, 100.0, 10.0), raw(1001, 20.0, 2.0)];
        let iss_rows = vec![iss(1, 1, 1001)];
        let mut store = moves_framework::InMemoryStore::new();
        store.insert(
            "MOVESWorkerOutput",
            WorkerOutputRow::into_dataframe(worker_rows).unwrap(),
        );
        store.insert(
            "integratedSpeciesSet",
            IntegratedSpeciesRow::into_dataframe(iss_rows).unwrap(),
        );
        let ctx = CalculatorContext::with_tables(store);
        let out = TogSpeciationCalculator.execute(&ctx).expect("execute ok");
        let df = out.dataframe().expect("output should contain a DataFrame");
        assert_eq!(
            df.height(),
            1,
            "minimal inputs produce exactly one NonHAPTOG row"
        );
        let quant = df
            .column("emissionQuant")
            .unwrap()
            .f64()
            .unwrap()
            .get(0)
            .unwrap();
        let rate = df
            .column("emissionRate")
            .unwrap()
            .f64()
            .unwrap()
            .get(0)
            .unwrap();
        assert!((quant - 80.0).abs() < 1e-9, "emissionQuant {quant} != 80.0");
        assert!((rate - 8.0).abs() < 1e-9, "emissionRate {rate} != 8.0");
        let pollutant = df
            .column("pollutantID")
            .unwrap()
            .i32()
            .unwrap()
            .get(0)
            .unwrap();
        assert_eq!(pollutant, NONHAP_TOG_POLLUTANT_ID);
    }

    #[test]
    fn factory_builds_a_named_calculator() {
        assert_eq!(factory().name(), "TOGSpeciationCalculator");
    }

    #[test]
    fn calculator_is_object_safe() {
 // The registry stores calculators as Box<dyn Calculator>.
        let calc: Box<dyn Calculator> = Box::new(TogSpeciationCalculator);
        assert_eq!(calc.name(), "TOGSpeciationCalculator");
    }
}
