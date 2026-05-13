//! In-memory equivalent of MOVES's MariaDB execution database.
//!
//! Ports `gov.epa.otaq.moves.master.framework.ExecutionDatabase`-style state
//! into Rust. The Java port stores the execution database in two tiers:
//!
//! 1. **Slow tier** — filtered slices of the default database, loaded once
//!    per run by `InputDataManager` (Task 24). These are the "input tables"
//!    every calculator reads from.
//! 2. **Scratch tier** — per-bundle scratch tables produced by upstream
//!    generators and consumed by downstream calculators. In legacy MOVES,
//!    these flowed through MariaDB worker scratch schemas plus on-disk
//!    bundle handoffs; the Rust port collapses both onto in-memory
//!    DataFrame values.
//!
//! Together with the current MasterLoop position (iteration/location/time)
//! these three pieces form the [`CalculatorContext`] every calculator and
//! generator sees in its [`execute`](crate::Calculator::execute) call.
//!
//! The [`ExecutionDatabaseSchema`] type describes *which* tables may appear
//! in the slow tier (their canonical names plus where they originate from).
//! Task 24 (`InputDataManager`) consumes a schema instance to drive the
//! initial load; Task 19 (`CalculatorRegistry`) checks calculator
//! [`input_tables`](crate::Calculator::input_tables) declarations against it
//! to catch typos at startup.
//!
//! # Storage placeholders
//!
//! The actual DataFrame containers ([`ExecutionTables`], [`ScratchNamespace`])
//! remain shape-only structs in this commit. Task 50 (`DataFrameStore`)
//! lands the concrete Polars-backed storage; calculators committed to the
//! [`CalculatorContext::tables`] / [`CalculatorContext::scratch`] accessor
//! shape today will not have to rewrite when the data plane materialises.
//!
//! Fixing the *position* types ([`IterationPosition`], [`ExecutionLocation`],
//! [`ExecutionTime`]) concretely means Phase 3 calculator authors can read
//! the current county/zone/link/hour from `ctx.position()` immediately —
//! none of those values depend on the deferred data plane.

use moves_data::ProcessId;

/// `(state, county, zone, link)` quadruple identifying the current spatial
/// location in the master loop. Rust equivalent of `ExecutionLocation.java`.
///
/// MOVES geography is hierarchical: each state owns one or more counties,
/// each county owns one or more zones, and each zone owns one or more
/// links. The master loop iterates these in nested order
/// (state → county → zone → link), so a finer-granularity calculator can
/// read the coarser id directly from the same struct rather than chasing
/// back-references.
///
/// All four ids are stored as raw integers matching the default-DB primary
/// keys (`State.stateID`, `County.countyID`, `Zone.zoneID`, `Link.linkID`).
///
/// `None` means "the loop has not yet entered iteration at that
/// granularity." A subscription firing at PROCESS granularity sees all
/// four ids as `None`; a subscription firing at LINK granularity sees all
/// four populated.
///
/// Task 16 (`ExecutionLocationProducer`) will introduce the iterator that
/// emits a sequence of populated values; this type is the per-step record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ExecutionLocation {
    /// `State.stateID` of the state currently iterating. `None` outside
    /// STATE-or-finer granularity scopes.
    pub state_id: Option<u32>,
    /// `County.countyID` of the county currently iterating. `None` outside
    /// COUNTY-or-finer granularity scopes.
    pub county_id: Option<u32>,
    /// `Zone.zoneID` of the zone currently iterating. `None` outside
    /// ZONE-or-finer granularity scopes.
    pub zone_id: Option<u32>,
    /// `Link.linkID` of the link currently iterating. `None` outside LINK
    /// granularity scopes.
    pub link_id: Option<u32>,
}

impl ExecutionLocation {
    /// Construct a location with no ids set — the state when the loop is
    /// firing at PROCESS or coarser granularity.
    #[must_use]
    pub const fn none() -> Self {
        Self {
            state_id: None,
            county_id: None,
            zone_id: None,
            link_id: None,
        }
    }

    /// Construct a location with only the state set.
    #[must_use]
    pub const fn state(state_id: u32) -> Self {
        Self {
            state_id: Some(state_id),
            county_id: None,
            zone_id: None,
            link_id: None,
        }
    }

    /// Construct a location with state and county set.
    #[must_use]
    pub const fn county(state_id: u32, county_id: u32) -> Self {
        Self {
            state_id: Some(state_id),
            county_id: Some(county_id),
            zone_id: None,
            link_id: None,
        }
    }

    /// Construct a fully-populated `(state, county, zone, link)` location.
    #[must_use]
    pub const fn link(state_id: u32, county_id: u32, zone_id: u32, link_id: u32) -> Self {
        Self {
            state_id: Some(state_id),
            county_id: Some(county_id),
            zone_id: Some(zone_id),
            link_id: Some(link_id),
        }
    }
}

/// `(year, month, day, hour)` quadruple identifying the current temporal
/// position in the master loop.
///
/// Fields follow MOVES default-DB conventions:
/// * `year` — calendar year (e.g. 2020). Always present once the loop has
///   entered YEAR granularity.
/// * `month` — `MonthOfAnyYear.monthID` (1–12). `None` outside MONTH or
///   finer granularity scopes.
/// * `day_id` — `DayOfAnyWeek.dayID` (5 = weekday, 2 = weekend in MOVES5).
///   `None` outside DAY or finer granularity scopes.
/// * `hour` — `HourOfAnyDay.hourID` (1–24). `None` outside HOUR scope.
///
/// As with [`ExecutionLocation`], `None` is the explicit "not yet at this
/// granularity" signal — preferable to a sentinel zero that calculators
/// would have to recognise.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ExecutionTime {
    /// Calendar year. `None` only before the loop enters YEAR granularity.
    pub year: Option<u16>,
    /// `MonthOfAnyYear.monthID` (1–12). `None` outside MONTH-or-finer scope.
    pub month: Option<u8>,
    /// `DayOfAnyWeek.dayID` (MOVES5: 2 = weekend, 5 = weekday).
    /// `None` outside DAY-or-finer scope.
    pub day_id: Option<u8>,
    /// `HourOfAnyDay.hourID` (1–24). `None` outside HOUR scope.
    pub hour: Option<u8>,
}

impl ExecutionTime {
    /// Construct a time with no fields set — the state when the loop is
    /// firing at PROCESS or coarser-than-YEAR granularity.
    #[must_use]
    pub const fn none() -> Self {
        Self {
            year: None,
            month: None,
            day_id: None,
            hour: None,
        }
    }

    /// Construct a year-only time (no month / day / hour).
    #[must_use]
    pub const fn year(year: u16) -> Self {
        Self {
            year: Some(year),
            month: None,
            day_id: None,
            hour: None,
        }
    }

    /// Construct a fully-populated `(year, month, day_id, hour)` time.
    #[must_use]
    pub const fn hour(year: u16, month: u8, day_id: u8, hour: u8) -> Self {
        Self {
            year: Some(year),
            month: Some(month),
            day_id: Some(day_id),
            hour: Some(hour),
        }
    }
}

/// MasterLoop iteration counter plus the `(process, location, time)`
/// triple. Together these identify exactly which iteration of the nested
/// loop is currently firing.
///
/// `iteration` is the outermost MasterLoop counter, advanced once per
/// pass over the calculator graph (see `MOVESEngine.numIterations`).
/// For RunSpecs with a single iteration (the common case) it is always 0.
/// Inventory mode runs use one iteration; rate-mode and some chained
/// calculator configurations use multiple.
///
/// `process_id` is the [`crate::ProcessId`] currently active. The master
/// loop iterates one process at a time, so this is always concrete once
/// past PROCESS-bucket dispatch.
///
/// [`location`](Self::location) and [`time`](Self::time) carry the spatial
/// and temporal position, each with their own "not yet populated"
/// (`None`) fields per granularity level. See [`ExecutionLocation`] and
/// [`ExecutionTime`] for the field-by-field semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct IterationPosition {
    /// MasterLoop outer iteration counter (0-based). Most RunSpecs have a
    /// single iteration.
    pub iteration: u32,
    /// MOVES process the loop is currently dispatching. Stored as
    /// `Option` so the default-constructed value (used in tests) does not
    /// have to commit to a specific process.
    pub process_id: Option<ProcessId>,
    /// Current spatial location. Field-level `None`s identify which
    /// granularity the loop has entered.
    pub location: ExecutionLocation,
    /// Current temporal position. Field-level `None`s identify which
    /// granularity the loop has entered.
    pub time: ExecutionTime,
}

impl IterationPosition {
    /// Construct the position at the start of a run — iteration 0, no
    /// process set, no location, no time. Subsequent master-loop levels
    /// fill in the fields one by one as the loop descends.
    #[must_use]
    pub const fn start() -> Self {
        Self {
            iteration: 0,
            process_id: None,
            location: ExecutionLocation::none(),
            time: ExecutionTime::none(),
        }
    }
}

/// Per-run filtered default-DB tables, loaded once at run start.
///
/// **Phase 2 skeleton.** Task 50 (`DataFrameStore`) replaces this with a
/// concrete keyed DataFrame store. Calculators committed to the
/// [`CalculatorContext::tables`] accessor shape today will not need to
/// change when the data plane materialises.
///
/// The slow tier is "loaded once per run" — a calculator firing at any
/// granularity reads the same table contents throughout the run. Filtering
/// happens at load time via [`ExecutionDatabaseSchema`] + RunSpec
/// selections; calculators do not re-filter on every call.
#[derive(Debug, Default)]
pub struct ExecutionTables {
    // Task 50 lands the DataFrame-keyed map.
    _private: (),
}

impl ExecutionTables {
    /// Construct an empty tables container. Used by tests and by the
    /// Task 19 registry stub until the data plane lands.
    #[must_use]
    pub fn empty() -> Self {
        Self { _private: () }
    }
}

/// Inter-calculator scratch namespace.
///
/// **Phase 2 skeleton.** Task 50 (`DataFrameStore`) replaces the placeholder
/// with a concrete name-keyed DataFrame store with appropriate interior
/// mutability so generators can write and downstream calculators can read.
///
/// The scratch tier is "rapidly-changing per-bundle" data: each generator
/// fires at its registered granularity, writes one or more named tables
/// (declared in [`crate::Generator::output_tables`]), and the downstream
/// calculator's [`crate::Calculator::input_tables`] declaration names them
/// to drive dependency analysis.
#[derive(Debug, Default)]
pub struct ScratchNamespace {
    // Task 50 lands the DataFrame-keyed map with interior mutability.
    _private: (),
}

impl ScratchNamespace {
    /// Construct an empty scratch namespace. Used by tests and by the
    /// Task 19 registry stub until the data plane lands.
    #[must_use]
    pub fn empty() -> Self {
        Self { _private: () }
    }
}

/// Origin of a table in the execution database — whether it is loaded from
/// the default DB at run start (the slow tier) or produced by a generator
/// during the run (the scratch tier).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableSource {
    /// Loaded once from the default database at run start, then read-only.
    /// Task 24 (`InputDataManager`) drives the load.
    DefaultDb,
    /// Produced during the run by a generator's
    /// [`crate::Generator::execute`] body. Lifecycle managed per-iteration
    /// by the registry; downstream calculators read via
    /// [`crate::Calculator::input_tables`].
    Scratch,
}

/// Declaration of one table eligible to appear in the execution database.
///
/// A schema entry records the *contract* — the table's canonical name and
/// its origin — not the data itself. Task 24 (`InputDataManager`) reads
/// the [`DefaultDb`](TableSource::DefaultDb) entries to drive loading; the
/// registry (Task 19) validates that every
/// [`crate::Calculator::input_tables`] / [`crate::Generator::output_tables`]
/// declaration names a known entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionTableSpec {
    /// Canonical default-DB table name (e.g. `"sourceUseTypePopulation"`).
    /// Names match the casing used in the MOVES default DB so that the
    /// snapshot Parquet files (Phase 4) can be located by the same string.
    pub name: &'static str,
    /// Where the table comes from — default DB or scratch.
    pub source: TableSource,
}

impl ExecutionTableSpec {
    /// Construct a spec for a table sourced from the default DB.
    #[must_use]
    pub const fn default_db(name: &'static str) -> Self {
        Self {
            name,
            source: TableSource::DefaultDb,
        }
    }

    /// Construct a spec for a scratch table produced during the run.
    #[must_use]
    pub const fn scratch(name: &'static str) -> Self {
        Self {
            name,
            source: TableSource::Scratch,
        }
    }
}

/// Registry of canonical execution-database table specs.
///
/// Acts as the schema of the in-memory execution DB — the set of named
/// tables a calculator may reference in its
/// [`crate::Calculator::input_tables`] /
/// [`crate::Generator::output_tables`] declaration.
///
/// The empty-by-default form is what tests and stubs use. Task 24
/// (`InputDataManager`) constructs a populated schema from the RunSpec and
/// the default DB catalogue (`characterization/default-db-schema/tables.json`,
/// 240 tables).
#[derive(Debug, Default, Clone)]
pub struct ExecutionDatabaseSchema {
    tables: Vec<ExecutionTableSpec>,
}

impl ExecutionDatabaseSchema {
    /// Construct an empty schema. Tests and the Task 19 registry stub use
    /// this form until Task 24 lands real population.
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Append a spec to the schema. Used by Task 24 as it discovers
    /// per-run table requirements.
    pub fn push(&mut self, spec: ExecutionTableSpec) {
        self.tables.push(spec);
    }

    /// All registered table specs, in insertion order.
    #[must_use]
    pub fn tables(&self) -> &[ExecutionTableSpec] {
        &self.tables
    }

    /// Total number of registered tables.
    #[must_use]
    pub fn len(&self) -> usize {
        self.tables.len()
    }

    /// `true` when no tables are registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    /// Look up the first spec with the given name. Case-sensitive — names
    /// must match the casing in the default DB / scratch declarations.
    #[must_use]
    pub fn find(&self, name: &str) -> Option<&ExecutionTableSpec> {
        self.tables.iter().find(|t| t.name == name)
    }

    /// `true` when a spec with the given name is registered.
    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        self.find(name).is_some()
    }
}

impl FromIterator<ExecutionTableSpec> for ExecutionDatabaseSchema {
    /// Build a schema from any iterable of specs. The caller is
    /// responsible for ensuring names are unique — duplicate entries do
    /// not error here, but [`ExecutionDatabaseSchema::find`] returns the
    /// first match.
    fn from_iter<I: IntoIterator<Item = ExecutionTableSpec>>(specs: I) -> Self {
        Self {
            tables: specs.into_iter().collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execution_location_none_round_trip() {
        let loc = ExecutionLocation::none();
        assert!(loc.state_id.is_none());
        assert!(loc.county_id.is_none());
        assert!(loc.zone_id.is_none());
        assert!(loc.link_id.is_none());
        // Default matches none() for ergonomics in test fixtures.
        assert_eq!(loc, ExecutionLocation::default());
    }

    #[test]
    fn execution_location_state_only_sets_state() {
        let loc = ExecutionLocation::state(40);
        assert_eq!(loc.state_id, Some(40));
        assert!(loc.county_id.is_none());
    }

    #[test]
    fn execution_location_county_sets_state_and_county() {
        let loc = ExecutionLocation::county(40, 40_001);
        assert_eq!(loc.state_id, Some(40));
        assert_eq!(loc.county_id, Some(40_001));
        assert!(loc.zone_id.is_none());
        assert!(loc.link_id.is_none());
    }

    #[test]
    fn execution_location_link_populates_all_four() {
        let loc = ExecutionLocation::link(40, 40_001, 400_011, 4_000_111);
        assert_eq!(loc.state_id, Some(40));
        assert_eq!(loc.county_id, Some(40_001));
        assert_eq!(loc.zone_id, Some(400_011));
        assert_eq!(loc.link_id, Some(4_000_111));
    }

    #[test]
    fn execution_time_none_has_no_fields() {
        let t = ExecutionTime::none();
        assert!(t.year.is_none());
        assert!(t.month.is_none());
        assert!(t.day_id.is_none());
        assert!(t.hour.is_none());
        assert_eq!(t, ExecutionTime::default());
    }

    #[test]
    fn execution_time_year_only_sets_year() {
        let t = ExecutionTime::year(2020);
        assert_eq!(t.year, Some(2020));
        assert!(t.month.is_none());
    }

    #[test]
    fn execution_time_hour_populates_all_four() {
        // MOVES5 dayID 5 = weekday, hourID 8 = 7am–8am.
        let t = ExecutionTime::hour(2020, 7, 5, 8);
        assert_eq!(t.year, Some(2020));
        assert_eq!(t.month, Some(7));
        assert_eq!(t.day_id, Some(5));
        assert_eq!(t.hour, Some(8));
    }

    #[test]
    fn iteration_position_start_is_zeroed() {
        let p = IterationPosition::start();
        assert_eq!(p.iteration, 0);
        assert!(p.process_id.is_none());
        assert_eq!(p.location, ExecutionLocation::none());
        assert_eq!(p.time, ExecutionTime::none());
        assert_eq!(p, IterationPosition::default());
    }

    #[test]
    fn iteration_position_can_carry_all_fields() {
        // Shape a position as if mid-loop at HOUR granularity for Running
        // Exhaust (process 1) in some state/county/zone/link.
        let p = IterationPosition {
            iteration: 0,
            process_id: Some(ProcessId(1)),
            location: ExecutionLocation::link(40, 40_001, 400_011, 4_000_111),
            time: ExecutionTime::hour(2020, 7, 5, 8),
        };
        assert_eq!(p.process_id, Some(ProcessId(1)));
        assert_eq!(p.location.state_id, Some(40));
        assert_eq!(p.location.county_id, Some(40_001));
        assert_eq!(p.time.hour, Some(8));
    }

    #[test]
    fn execution_tables_empty_constructs() {
        // Placeholder until Task 50; this is a smoke check that the
        // accessor exists.
        let _t = ExecutionTables::empty();
    }

    #[test]
    fn scratch_namespace_empty_constructs() {
        let _s = ScratchNamespace::empty();
    }

    #[test]
    fn execution_table_spec_default_db_constructor() {
        let spec = ExecutionTableSpec::default_db("sourceUseTypePopulation");
        assert_eq!(spec.name, "sourceUseTypePopulation");
        assert_eq!(spec.source, TableSource::DefaultDb);
    }

    #[test]
    fn execution_table_spec_scratch_constructor() {
        let spec = ExecutionTableSpec::scratch("sourceBinDistribution");
        assert_eq!(spec.name, "sourceBinDistribution");
        assert_eq!(spec.source, TableSource::Scratch);
    }

    #[test]
    fn execution_database_schema_empty_is_empty() {
        let s = ExecutionDatabaseSchema::empty();
        assert!(s.is_empty());
        assert_eq!(s.len(), 0);
        assert!(s.find("sourceUseTypePopulation").is_none());
    }

    #[test]
    fn execution_database_schema_from_iter_preserves_order() {
        let specs = [
            ExecutionTableSpec::default_db("sourceUseTypePopulation"),
            ExecutionTableSpec::default_db("emissionRateByAge"),
            ExecutionTableSpec::scratch("sourceBinDistribution"),
        ];
        let s: ExecutionDatabaseSchema = specs.iter().copied().collect();
        assert_eq!(s.len(), 3);
        assert_eq!(s.tables()[0].name, "sourceUseTypePopulation");
        assert_eq!(s.tables()[2].name, "sourceBinDistribution");
        assert_eq!(s.tables()[2].source, TableSource::Scratch);
    }

    #[test]
    fn execution_database_schema_push_and_find() {
        let mut s = ExecutionDatabaseSchema::empty();
        s.push(ExecutionTableSpec::default_db("sourceUseTypePopulation"));
        s.push(ExecutionTableSpec::scratch("sourceBinDistribution"));
        assert_eq!(s.len(), 2);
        assert!(s.contains("sourceUseTypePopulation"));
        assert!(s.contains("sourceBinDistribution"));
        assert!(!s.contains("notATable"));
        let found = s.find("sourceBinDistribution").unwrap();
        assert_eq!(found.source, TableSource::Scratch);
    }

    #[test]
    fn execution_database_schema_find_returns_first_on_duplicate() {
        // We don't error on duplicates; the caller (Task 24) is responsible
        // for de-duplication. Document the actual behaviour: first match wins.
        let mut s = ExecutionDatabaseSchema::empty();
        s.push(ExecutionTableSpec::default_db("dup"));
        s.push(ExecutionTableSpec::scratch("dup"));
        let found = s.find("dup").unwrap();
        assert_eq!(found.source, TableSource::DefaultDb);
    }
}
