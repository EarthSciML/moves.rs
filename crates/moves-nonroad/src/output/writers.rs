//! Per-record output-file writers (Task 114).
//!
//! Ports the five NONROAD routines that emit one record (or one
//! header) at a time into the model's output files:
//!
//! | Fortran | Lines | Rust | Output file |
//! |---|---|---|---|
//! | `wrthdr.f` | 292 | [`write_data_header`] | `.OUT` data-file header |
//! | `wrtdat.f` | 249 | [`write_data_record`] | `.OUT` data-file record |
//! | `hdrbmy.f` | 213 | [`write_bmy_header`]  | `.BMY`/`.EVBMY` header |
//! | `wrtbmy.f` | 280 | [`write_bmy_record`]  | `.BMY`/`.EVBMY` record |
//! | `wrtams.f` | 161 | [`write_ams`]         | EPS2 `.AMS` workfile |
//!
//! # I/O policy
//!
//! Per `ARCHITECTURE.md` § 4.3 each writer takes a `&mut impl Write`
//! and returns [`std::io::Result`]: the Fortran integer unit numbers
//! (`IOWDAT`, `IOWBMY`, …) and the `ERR=7000` jump that names the
//! output file are gone. The orchestrating layer (the Task 117
//! integration step) owns the output paths and is responsible for
//! turning an [`std::io::Error`] from one of these writers into a
//! [`crate::Error::Io`] carrying the file path the Fortran error
//! message would have named.
//!
//! Records are terminated with a single `\n`; the Fortran `WRITE`
//! statement's record separator was platform-dependent (CRLF on the
//! Windows reference build). Byte-exact line endings are a Task 115
//! fidelity checkpoint, not a Task 114 concern.
//!
//! # The two output formats
//!
//! The migration plan (Task 114) calls for two output encodings: the
//! legacy NONROAD text format — ported here — and Apache Parquet on
//! the unified Phase 4 output schema (`moves-data`'s `output_schema`,
//! Task 89). The structured record types in this module
//! ([`OutputRecord`], [`ByModelYearRecord`], [`AmsCountyEmissions`])
//! are the format-neutral seam between the two: the text writers
//! below consume them directly, and the Parquet encoding consumes the
//! same records once the cross-crate wiring lands in the Task 117
//! NONROAD–MOVES integration step, which is where the plan places the
//! onroad/nonroad output-schema convergence. Keeping `moves-nonroad`
//! free of the `parquet` dependency preserves the WASM-compatibility
//! posture of `ARCHITECTURE.md` § 4.4.

use std::io::{self, Write};

use crate::common::consts::MXPOL;
use crate::output::fortran_fmt::{fortran_a, fortran_e, fortran_i};
use crate::output::strutil::strmin;

// ===========================================================================
// Shared emission-column layout
// ===========================================================================

/// Column headings for the 14 emission species the `.OUT` data file
/// carries, in output order — `wrthdr.f` :225–258 (`namesp(order(i))`
/// for `i = IDXTHC..IDXRLS-3`).
///
/// The matching value columns are emitted by [`write_data_record`] in
/// the same order; see [`OUT_EMISSION_SLOTS`].
const OUT_SPECIES_COLUMNS: [&str; 14] = [
    "THC-Exhaust",
    "CO-Exhaust",
    "NOx-Exhaust",
    "CO2-Exhaust",
    "SO2-Exhaust",
    "PM-Exhaust",
    "Crankcase",
    "Hot-Soaks",
    "Diurnal",
    "Displacement",
    "Spillage",
    "RunLoss",
    "TankPerm",
    "HosePerm",
];

/// 0-based pollutant slots for the 14 `.OUT` emission columns, in
/// output order — `wrthdr.f`/`wrtdat.f`'s `order` array
/// (`1,2,3,4,5,6,7,14,8,15,16,17,9,10`, converted to 0-based).
///
/// The values are read from the hose-combined `tmpemis` array (see
/// [`combine_hose`]); slot 9 (`HosePerm`) therefore carries the sum
/// of the four hose-permeation pollutants.
const OUT_EMISSION_SLOTS: [usize; 14] = [0, 1, 2, 3, 4, 5, 6, 13, 7, 14, 15, 16, 8, 9];

/// 0-based pollutant slots for the seven exhaust by-model-year
/// columns — `wrtbmy.f` :169–174 (`i = IDXTHC..IDXCRA`).
const BMY_EXHAUST_SLOTS: [usize; 7] = [0, 1, 2, 3, 4, 5, 6];

/// 0-based pollutant slots for the seven evaporative by-model-year
/// columns — `wrtbmy.f` :175–190 (`IDXSOK, IDXDIU, IDXDIS, IDXSPL,
/// IDXRLS, IDXTKP, IDXHOS`).
const BMY_EVAP_SLOTS: [usize; 7] = [13, 7, 14, 15, 16, 8, 9];

/// Build NONROAD's `tmpemis` array — `wrtdat.f` :123–133,
/// `wrtbmy.f` :123–133.
///
/// The three rec-marine hose-permeation pollutants (`IDXNCK`,
/// `IDXSR`, `IDXVNT`) are folded into the generic hose-permeation
/// slot (`IDXHOS`); the Fortran then zeroes the three folded slots.
/// Output routines never read those slots, so the zeroing is
/// cosmetic, but it is reproduced so the array matches the source.
fn combine_hose(emissions: &[f32; MXPOL]) -> [f32; MXPOL] {
    let mut tmp = *emissions;
    // 1-based IDXHOS=10, IDXNCK=11, IDXSR=12, IDXVNT=13 → 0-based 9..=12.
    tmp[9] = emissions[9] + emissions[10] + emissions[11] + emissions[12];
    tmp[10] = 0.0;
    tmp[11] = 0.0;
    tmp[12] = 0.0;
    tmp
}

/// Append `,value` (a comma then the `E15.8` value) when `value` is
/// non-negative, or a bare `,` otherwise — the `9001`/`9002`
/// alternative shared by `wrtdat.f` and `wrtbmy.f`.
///
/// A negative value is NONROAD's "no data" sentinel (`RMISS = -9.0`);
/// the Fortran writers emit an empty field for it.
fn push_optional(line: &mut String, value: f32) {
    line.push(',');
    if value >= 0.0 {
        line.push_str(&fortran_e(value, 15, 8));
    }
}

// ===========================================================================
// Run period / summary classification (wrthdr period string)
// ===========================================================================

/// How emissions are summed over the reporting period — Fortran
/// `ismtyp`, one of the `IDXTYP`/`IDXTOT` parameters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SummaryKind {
    /// `IDXTYP` — a typical day within the period.
    TypicalDay,
    /// `IDXTOT` — the period total.
    PeriodTotal,
}

/// Which typical day a [`SummaryKind::TypicalDay`] run reports —
/// Fortran `iday`, one of `IDXWKD`/`IDXWKE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DayKind {
    /// `IDXWKD` — a typical weekday.
    Weekday,
    /// `IDXWKE` — a typical weekend day.
    WeekendDay,
}

/// The four seasons — Fortran `iseasn` (`IDXWTR`/`IDXSPR`/`IDXSUM`/
/// `IDXFAL`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeasonKind {
    /// `IDXWTR`.
    Winter,
    /// `IDXSPR`.
    Spring,
    /// `IDXSUM`.
    Summer,
    /// `IDXFAL`.
    Fall,
}

impl SeasonKind {
    /// Season name as it appears in `wrthdr.f`'s period string
    /// (`:163–171` — note `Fall`, not `Autumn`).
    fn name(self) -> &'static str {
        match self {
            SeasonKind::Winter => "Winter",
            SeasonKind::Spring => "Spring",
            SeasonKind::Summer => "Summer",
            SeasonKind::Fall => "Fall",
        }
    }
}

/// The twelve months — Fortran `imonth` (`IDXJAN..IDXDEC`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum MonthKind {
    January,
    February,
    March,
    April,
    May,
    June,
    July,
    August,
    September,
    October,
    November,
    December,
}

impl MonthKind {
    /// Month name as written into `wrthdr.f`'s period string
    /// (`:174–197`).
    fn name(self) -> &'static str {
        match self {
            MonthKind::January => "January",
            MonthKind::February => "February",
            MonthKind::March => "March",
            MonthKind::April => "April",
            MonthKind::May => "May",
            MonthKind::June => "June",
            MonthKind::July => "July",
            MonthKind::August => "August",
            MonthKind::September => "September",
            MonthKind::October => "October",
            MonthKind::November => "November",
            MonthKind::December => "December",
        }
    }
}

/// The reporting period a run covers — Fortran `iprtyp`
/// (`IDXANN`/`IDXSES`/`IDXMTH`), carrying the selected season or
/// month.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeriodKind {
    /// `IDXANN` — a full calendar year.
    Annual,
    /// `IDXSES` — one season.
    Seasonal(SeasonKind),
    /// `IDXMTH` — one month.
    Monthly(MonthKind),
}

/// Build `wrthdr.f`'s period description string and its emission
/// units string — `wrthdr.f` :144–207.
///
/// Returns `(period_string, units_string)`, e.g.
/// `("Total for year: 2020", "Tons/Year")` or
/// `("Typical weekday for January, 2020", "Tons/Day")`.
pub fn period_string(
    summary: SummaryKind,
    day: DayKind,
    period: PeriodKind,
    year: i32,
) -> (String, String) {
    let base = match (summary, day) {
        (SummaryKind::PeriodTotal, _) => "Total for",
        (SummaryKind::TypicalDay, DayKind::Weekday) => "Typical weekday for",
        (SummaryKind::TypicalDay, DayKind::WeekendDay) => "Typical weekend day for",
    };
    let year4 = fortran_i(i64::from(year), 4);
    let suffix = match period {
        PeriodKind::Annual => format!("year: {year4}"),
        PeriodKind::Seasonal(s) => format!("{} Season, {year4}", s.name()),
        PeriodKind::Monthly(m) => format!("{}, {year4}", m.name()),
    };
    let units = match (summary, period) {
        (SummaryKind::TypicalDay, _) => "Tons/Day",
        (SummaryKind::PeriodTotal, PeriodKind::Annual) => "Tons/Year",
        (SummaryKind::PeriodTotal, PeriodKind::Seasonal(_)) => "Tons/Season",
        (SummaryKind::PeriodTotal, PeriodKind::Monthly(_)) => "Tons/Month",
    };
    (format!("{base} {suffix}"), units.to_string())
}

// ===========================================================================
// wrthdr — .OUT data-file header
// ===========================================================================

/// Run-level fields the `.OUT` data-file header echoes — the
/// `wrthdr.f` inputs drawn from the `/iochr/`, `/usrchr/` and
/// `/perdat/` COMMON blocks.
#[derive(Debug, Clone, Copy)]
pub struct RunHeader<'a> {
    /// Local date/time string (Fortran `cdate` from `getime`; see
    /// [`crate::util::time::format_now`]).
    pub date: &'a str,
    /// First run-title line (Fortran `title1`).
    pub title1: &'a str,
    /// Second run-title line (Fortran `title2`).
    pub title2: &'a str,
    /// Name of the options file used (Fortran `sysfl`).
    pub options_file: &'a str,
    /// How emissions are summed over the period (Fortran `ismtyp`).
    pub summary: SummaryKind,
    /// Which typical day, when [`SummaryKind::TypicalDay`] (Fortran
    /// `iday`); ignored for [`SummaryKind::PeriodTotal`].
    pub day: DayKind,
    /// The reporting period (Fortran `iprtyp`).
    pub period: PeriodKind,
    /// Episode year (Fortran `iepyr`).
    pub year: i32,
}

/// Write the `.OUT` data-file header — `wrthdr.f`.
///
/// `retrofit` is the Fortran `lrtrftfl` flag: when set, the header
/// row carries the two extra retrofit columns. The header comprises
/// the leading dummy "shape" record, the program/version/date and
/// title lines, the options-file, period and units lines, and the
/// column-name row.
pub fn write_data_header<W: Write>(
    w: &mut W,
    header: &RunHeader<'_>,
    retrofit: bool,
) -> io::Result<()> {
    // --- leading dummy record: blank key fields, zero HP, and one
    //     F3.0 zero per emission/quantity column (wrthdr.f :112–123).
    //     The post-processor reads it to count the columns to come.
    let dummy_quantities = 14 + if retrofit { 7 } else { 5 };
    let mut dummy = String::from(" , , ,");
    dummy.push_str(&fortran_i(0, 5)); // idum
    dummy.push(',');
    for _ in 0..dummy_quantities {
        dummy.push_str(" 0.,"); // F3.0 of 0.0, then COMMA
    }
    writeln!(w, "{dummy}")?;

    // --- program name, version, date (wrthdr.f :127–130). ---
    writeln!(w, "\"{}\"", fortran_a(crate::driver::run::PROGRAM_NAME, 30))?;
    writeln!(w, "\"{}\"", fortran_a(crate::driver::run::VERSION, 30))?;
    writeln!(w, "\"{}\"", &header.date[..strmin(header.date)])?;

    // --- title lines, options file (wrthdr.f :134–140). ---
    writeln!(w, "\"{}\"", &header.title1[..strmin(header.title1)])?;
    writeln!(w, "\"{}\"", &header.title2[..strmin(header.title2)])?;
    writeln!(
        w,
        "Options file used: {}",
        &header.options_file[..strmin(header.options_file)]
    )?;

    // --- period string and units (wrthdr.f :144–207). ---
    let (period, units) = period_string(header.summary, header.day, header.period, header.year);
    writeln!(w, "\"{period}\"")?;
    writeln!(w, "\"{units}\"")?;

    // --- column-name row (wrthdr.f :209–258). ---
    let mut row = String::new();
    for name in ["Cnty", "SubR"] {
        row.push_str(&fortran_a(name, 5));
        row.push(',');
    }
    row.push_str(&fortran_a("SCC", 10));
    row.push(',');
    row.push_str(&fortran_a("HP", 5));
    row.push(',');
    row.push_str(&fortran_a("Population", 15));
    row.push(',');
    for species in OUT_SPECIES_COLUMNS {
        row.push_str(&fortran_a(species, 15));
        row.push(',');
    }
    let mut trailing = vec!["FuelCons.", "Activity", "LF", "HPAvg"];
    if retrofit {
        trailing.push("FracRetro");
        trailing.push("UnitsRetro");
    }
    for name in trailing {
        row.push_str(&fortran_a(name, 15));
        row.push(',');
    }
    writeln!(w, "{row}")
}

// ===========================================================================
// wrtdat — .OUT data-file record
// ===========================================================================

/// One `.OUT` data-file record — the `wrtdat.f` argument list.
#[derive(Debug, Clone, Copy)]
pub struct OutputRecord<'a> {
    /// County FIPS code (Fortran `fipin`, `character*5`).
    pub fips: &'a str,
    /// Subregion code (Fortran `subin`, `character*5`).
    pub subregion: &'a str,
    /// SCC code (Fortran `ascin`, `character*10`).
    pub scc: &'a str,
    /// Horsepower category; written as a truncated integer
    /// (`INT(hpin)`).
    pub hp: f32,
    /// Equipment population (Fortran `popin`); negative ⇒ empty field.
    pub population: f32,
    /// Fuel consumption (Fortran `fulin`); negative ⇒ empty field.
    pub fuel_consumption: f32,
    /// Activity (Fortran `actin`); negative ⇒ empty field.
    pub activity: f32,
    /// Load factor (Fortran `ldfcin`); negative ⇒ empty field.
    pub load_factor: f32,
    /// Average horsepower (Fortran `hpavin`); negative ⇒ empty field.
    pub hp_avg: f32,
    /// Fraction of the population retrofitted (Fortran `fracretro`);
    /// emitted only when `retrofit` is set, negative ⇒ empty field.
    pub frac_retrofit: f32,
    /// Count of units retrofitted (Fortran `unitsretro`); emitted
    /// only when `retrofit` is set, negative ⇒ empty field.
    pub units_retrofit: f32,
    /// Per-pollutant emissions, indexed by 0-based pollutant slot
    /// ([`crate::emissions::exhaust::PollutantIndex::slot`]).
    pub emissions: [f32; MXPOL],
}

/// Write one `.OUT` data-file record — `wrtdat.f`.
///
/// `retrofit` is the Fortran `lrtrftfl` flag; when set, the two
/// retrofit columns are appended. A negative population, emission,
/// or quantity is NONROAD's "no data" marker and is written as an
/// empty field (just the separating comma).
pub fn write_data_record<W: Write>(
    w: &mut W,
    record: &OutputRecord<'_>,
    retrofit: bool,
) -> io::Result<()> {
    let mut line = String::new();

    // --- key identifiers (wrtdat.f :137–138, format 9000). ---
    line.push_str(&fortran_a(record.fips, 5));
    line.push(',');
    line.push_str(&fortran_a(record.subregion, 5));
    line.push(',');
    line.push_str(&fortran_a(record.scc, 10));
    line.push(',');
    line.push_str(&fortran_i(record.hp.trunc() as i64, 5));

    // --- population (wrtdat.f :140–144). ---
    push_optional(&mut line, record.population);

    // --- emission columns, hose-combined and re-ordered
    //     (wrtdat.f :155–164). ---
    let tmpemis = combine_hose(&record.emissions);
    for slot in OUT_EMISSION_SLOTS {
        push_optional(&mut line, tmpemis[slot]);
    }

    // --- fuel, activity, load factor, average HP
    //     (wrtdat.f :173–196). ---
    push_optional(&mut line, record.fuel_consumption);
    push_optional(&mut line, record.activity);
    push_optional(&mut line, record.load_factor);
    push_optional(&mut line, record.hp_avg);

    // --- retrofit columns (wrtdat.f :197–210). ---
    if retrofit {
        push_optional(&mut line, record.frac_retrofit);
        push_optional(&mut line, record.units_retrofit);
    }

    // --- the record carries a trailing comma (wrtdat.f :214). ---
    line.push(',');
    writeln!(w, "{line}")
}

// ===========================================================================
// hdrbmy / wrtbmy — by-model-year files
// ===========================================================================

/// Which by-model-year file a record belongs to — Fortran `iexev`.
///
/// NONROAD writes exhaust and evaporative emissions to separate
/// by-model-year files (`.BMY` and `.EVBMY`) with different column
/// sets; this selects between them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ByModelYearKind {
    /// `iexev = 1` — the exhaust by-model-year file (`bmyfl`).
    Exhaust,
    /// `iexev = 2` — the evaporative by-model-year file (`evbmyfl`).
    Evaporative,
}

impl ByModelYearKind {
    /// The seven emission-column heading/slot pairs for this file.
    fn species(self) -> (&'static [&'static str], &'static [usize; 7]) {
        match self {
            ByModelYearKind::Exhaust => (
                &[
                    "THC-Exhaust",
                    "CO-Exhaust",
                    "NOx-Exhaust",
                    "CO2-Exhaust",
                    "SO2-Exhaust",
                    "PM-Exhaust",
                    "Crankcase",
                ],
                &BMY_EXHAUST_SLOTS,
            ),
            ByModelYearKind::Evaporative => (
                &[
                    "Hot-Soaks",
                    "Diurnal",
                    "Displacement",
                    "Spillage",
                    "RunLoss",
                    "TankPerm",
                    "HosePerm",
                ],
                &BMY_EVAP_SLOTS,
            ),
        }
    }
}

/// Write a by-model-year file header — `hdrbmy.f`.
///
/// `kind` selects the exhaust or evaporative column set; `retrofit`
/// is the Fortran `lrtrftfl` flag (the two retrofit columns are
/// exhaust-only and appear only when it is set).
pub fn write_bmy_header<W: Write>(
    w: &mut W,
    kind: ByModelYearKind,
    retrofit: bool,
) -> io::Result<()> {
    let mut row = String::new();

    // --- key columns (hdrbmy.f :104–111, format 9000). ---
    for (name, width) in [
        ("Cnty", 5),
        ("SubR", 5),
        ("SCC", 10),
        ("HP", 5),
        ("TechType", 10),
        ("MYr", 4),
        ("Population", 15),
    ] {
        row.push_str(&fortran_a(name, width));
        row.push(',');
    }

    // --- emission species (hdrbmy.f :137–156). ---
    let (species, _) = kind.species();
    for name in species {
        row.push_str(&fortran_a(name, 15));
        row.push(',');
    }

    // --- fuel/activity, and (exhaust only) LF/HPAvg/retrofit
    //     (hdrbmy.f :158–177). ---
    let mut trailing = vec!["FuelCons.", "Activity"];
    if kind == ByModelYearKind::Exhaust {
        trailing.push("LF");
        trailing.push("HPAvg");
        if retrofit {
            trailing.push("FracRetro");
            trailing.push("UnitsRetro");
        }
    }
    for name in trailing {
        row.push_str(&fortran_a(name, 15));
        row.push(',');
    }
    writeln!(w, "{row}")
}

/// One by-model-year record — the `wrtbmy.f` argument list.
#[derive(Debug, Clone, Copy)]
pub struct ByModelYearRecord<'a> {
    /// County FIPS code (Fortran `fipin`, `character*5`).
    pub fips: &'a str,
    /// Subregion code (Fortran `subin`, `character*5`).
    pub subregion: &'a str,
    /// SCC code (Fortran `ascin`, `character*10`).
    pub scc: &'a str,
    /// Horsepower category; written as a truncated integer
    /// (`INT(hpin)`).
    pub hp: f32,
    /// Technology-type name (Fortran `tecin`, `character*10`).
    pub tech_type: &'a str,
    /// Model year (Fortran `iyrin`).
    pub model_year: i32,
    /// Equipment population (Fortran `popin`); always written.
    pub population: f32,
    /// Fuel consumption (Fortran `fulin`); negative ⇒ empty field.
    pub fuel_consumption: f32,
    /// Activity (Fortran `actin`); negative ⇒ empty field.
    pub activity: f32,
    /// Load factor (Fortran `ldfcin`, exhaust only); negative ⇒
    /// empty field.
    pub load_factor: f32,
    /// Average horsepower (Fortran `hpavin`, exhaust only); negative
    /// ⇒ empty field.
    pub hp_avg: f32,
    /// Fraction retrofitted (Fortran `fracretro`, exhaust + retrofit
    /// only); negative ⇒ empty field.
    pub frac_retrofit: f32,
    /// Count retrofitted (Fortran `unitsretro`, exhaust + retrofit
    /// only); negative ⇒ empty field.
    pub units_retrofit: f32,
    /// Per-pollutant emissions, indexed by 0-based pollutant slot.
    pub emissions: [f32; MXPOL],
}

/// Write one by-model-year record — `wrtbmy.f`.
///
/// `kind` selects the exhaust or evaporative file; `retrofit` is the
/// Fortran `lrtrftfl` flag. The seven emission columns are always
/// written (no "no data" suppression); fuel, activity and the
/// exhaust-only quantity columns suppress a negative value to an
/// empty field.
pub fn write_bmy_record<W: Write>(
    w: &mut W,
    record: &ByModelYearRecord<'_>,
    kind: ByModelYearKind,
    retrofit: bool,
) -> io::Result<()> {
    let mut line = String::new();

    // --- key identifiers (wrtbmy.f :151–157, format 9000). ---
    line.push_str(&fortran_a(record.fips, 5));
    line.push(',');
    line.push_str(&fortran_a(record.subregion, 5));
    line.push(',');
    line.push_str(&fortran_a(record.scc, 10));
    line.push(',');
    line.push_str(&fortran_i(record.hp.trunc() as i64, 5));
    line.push(',');
    line.push_str(&fortran_a(record.tech_type, 10));
    line.push(',');
    line.push_str(&fortran_i(i64::from(record.model_year), 4));
    line.push(',');
    line.push_str(&fortran_e(record.population, 15, 8));

    // --- emission columns: hose-combined, always written
    //     (wrtbmy.f :169–190). ---
    let tmpemis = combine_hose(&record.emissions);
    let (_, slots) = kind.species();
    for &slot in slots {
        line.push(',');
        line.push_str(&fortran_e(tmpemis[slot], 15, 8));
    }

    // --- fuel and activity (wrtbmy.f :197–209). ---
    push_optional(&mut line, record.fuel_consumption);
    push_optional(&mut line, record.activity);

    // --- exhaust-only quantity columns (wrtbmy.f :211–241). ---
    if kind == ByModelYearKind::Exhaust {
        push_optional(&mut line, record.load_factor);
        push_optional(&mut line, record.hp_avg);
        if retrofit {
            push_optional(&mut line, record.frac_retrofit);
            push_optional(&mut line, record.units_retrofit);
        }
    }

    // --- trailing comma (wrtbmy.f :245). ---
    line.push(',');
    writeln!(w, "{line}")
}

// ===========================================================================
// wrtams — EPS2 AMS workfile
// ===========================================================================

/// SAROAD pollutant codes written into the AMS workfile —
/// `nonrdprm.inc` :366–370 (`ISCTHC`, `ISCNOX`, `ISCCO`, `ISCSOX`,
/// `ISCPM`).
const AMS_SAROAD: [(i64, &str); 5] = [
    (43101, "THC"),
    (42603, "NOx"),
    (42101, "CO"),
    (42401, "SOx"),
    (81102, "PM"),
];

/// Run-level header fields for the EPS2 AMS workfile — the AMS
/// COMMON blocks `/epschr/` and `/epsdat/` (`nonrdeqp.inc`).
#[derive(Debug, Clone, Copy)]
pub struct AmsHeader<'a> {
    /// Inventory type (Fortran `itype`, `character*1`).
    pub inventory_type: &'a str,
    /// Reference year of the inventory (Fortran `irefyr`, written
    /// `I2` — the EPS2 format expects a two-digit year).
    pub reference_year: i32,
    /// Base year of the inventory (Fortran `ibasyr`, written `I2`).
    pub base_year: i32,
    /// Emissions type (Fortran `inetyp`, `character*2`).
    pub emission_type: &'a str,
    /// Inventory period code (Fortran `iperod`, `character*2`).
    pub period_code: &'a str,
    /// Inventory begin date/time (Fortran `ibegdt`).
    pub begin_date: i32,
    /// Inventory end date/time (Fortran `ienddt`).
    pub end_date: i32,
    /// Factor converting emissions to the AMS reporting period
    /// (Fortran `cvtams`).
    pub period_conversion: f32,
}

/// One county's emissions for the AMS workfile — a row of the
/// Fortran `emsams(NCNTY,MXPOL)` array plus its FIPS code.
#[derive(Debug, Clone, Copy)]
pub struct AmsCountyEmissions<'a> {
    /// County FIPS code (Fortran `fipcod(idxfip)`, `character*5`).
    pub fips: &'a str,
    /// Per-pollutant emissions for this county, indexed by 0-based
    /// pollutant slot.
    pub emissions: [f32; MXPOL],
}

/// AMS criteria-pollutant totals for one county — `wrtams.f`
/// :78–127.
///
/// HC sums the six hydrocarbon-bearing pollutants; the other four
/// are taken directly. Every total is scaled by the AMS
/// period-conversion factor.
fn ams_pollutant_totals(emissions: &[f32; MXPOL], period_conversion: f32) -> [f32; 5] {
    // 0-based slots: THC=0, CO=1, NOX=2, PM=5, CRA=6, DIU=7,
    // DIS=14, SPL=15, RLS=16, SOX=4.
    let hc =
        emissions[0] + emissions[6] + emissions[7] + emissions[14] + emissions[15] + emissions[16];
    [
        hc * period_conversion,           // THC
        emissions[2] * period_conversion, // NOx
        emissions[1] * period_conversion, // CO
        emissions[4] * period_conversion, // SOx
        emissions[5] * period_conversion, // PM
    ]
}

/// Write the EPS2 AMS workfile records for one SCC — `wrtams.f`.
///
/// One record is emitted per `(county, criteria pollutant)` pair
/// whose scaled emission total is strictly positive. `counties` is
/// the set the run selected — the Fortran loop's `lfipcd` filter
/// over all `NCNTY` counties is the caller's responsibility, exactly
/// as `ARCHITECTURE.md` § 4.3 places table population in the
/// orchestrating layer.
///
/// Each record carries the AMS criteria-pollutant SAROAD code and 36
/// trailing `-9` "not applicable" fillers, faithful to the legacy
/// EPS2 layout.
pub fn write_ams<W: Write>(
    w: &mut W,
    header: &AmsHeader<'_>,
    scc: &str,
    counties: &[AmsCountyEmissions<'_>],
) -> io::Result<()> {
    for county in counties {
        let totals = ams_pollutant_totals(&county.emissions, header.period_conversion);
        for (&(saroad, _), &value) in AMS_SAROAD.iter().zip(totals.iter()) {
            if value > 0.0 {
                writeln!(w, "{}", ams_record(header, scc, county.fips, saroad, value))?;
            }
        }
    }
    Ok(())
}

/// Format a single AMS workfile record — `wrtams.f` format `9000`.
fn ams_record(header: &AmsHeader<'_>, scc: &str, fips: &str, saroad: i64, value: f32) -> String {
    let mut line = String::new();
    line.push_str(&fortran_a(header.inventory_type, 1)); // A1 itype
    line.push_str(&fortran_i(i64::from(header.reference_year), 2)); // I2 irefyr
    line.push(' '); // 1X
    line.push_str(&fortran_i(i64::from(header.base_year), 2)); // I2 ibasyr
    line.push_str(&fortran_a(header.emission_type, 2)); // A2 inetyp
    line.push_str(&fortran_a(fips, 5)); // A5 fipcod
    line.push(' '); // 1X
    line.push_str(&fortran_a(" ", 5)); // A5 isbrg (blank)
    line.push(' '); // 1X
    line.push_str(&fortran_a(scc.get(..4).unwrap_or(scc), 4)); // A4 asccod(1:4)
    line.push(' '); // 1X
    line.push_str(&fortran_a(scc, 10)); // A10 asccod
    line.push(' '); // 1X
    line.push_str(&fortran_a(header.period_code, 2)); // A2 iperod
    line.push(' '); // 1X
    line.push_str(&fortran_i(i64::from(header.begin_date), 8)); // I8 ibegdt
    line.push(' '); // 1X
    line.push_str(&fortran_i(i64::from(header.end_date), 8)); // I8 ienddt
    line.push(' '); // 1X
    line.push_str(&fortran_i(saroad, 5)); // I5 SAROAD code
    line.push(' '); // 1X
    line.push_str(&fortran_e(value, 10, 4)); // E10.4 crtpol
    line.push(' '); // 1X
    line.push_str(&fortran_a(" ", 3)); // A3 ' '

    // 36 trailing `-9` fillers: 3(1X,I5), 1X,I3, 28(I5,1X), then
    // four of 5(1X,I10) — the list of 36 runs out one item into the
    // last group (wrtams.f format 9000, data `(-9,i=1,36)`).
    for _ in 0..3 {
        line.push(' ');
        line.push_str(&fortran_i(-9, 5));
    }
    line.push(' ');
    line.push_str(&fortran_i(-9, 3));
    line.push(' ');
    for _ in 0..28 {
        line.push_str(&fortran_i(-9, 5));
        line.push(' ');
    }
    for _ in 0..4 {
        line.push(' ');
        line.push_str(&fortran_i(-9, 10));
    }
    line
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Decode a writer's output to a `String` for assertions.
    fn rendered<F>(f: F) -> String
    where
        F: FnOnce(&mut Vec<u8>) -> io::Result<()>,
    {
        let mut buf = Vec::new();
        f(&mut buf).expect("writer must not fail to an in-memory buffer");
        String::from_utf8(buf).expect("writer output is ASCII text")
    }

    fn zero_emissions() -> [f32; MXPOL] {
        [0.0; MXPOL]
    }

    // ---- period_string ----

    #[test]
    fn period_string_total_annual() {
        let (period, units) = period_string(
            SummaryKind::PeriodTotal,
            DayKind::Weekday,
            PeriodKind::Annual,
            2020,
        );
        assert_eq!(period, "Total for year: 2020");
        assert_eq!(units, "Tons/Year");
    }

    #[test]
    fn period_string_typical_weekday_monthly() {
        let (period, units) = period_string(
            SummaryKind::TypicalDay,
            DayKind::Weekday,
            PeriodKind::Monthly(MonthKind::January),
            2020,
        );
        assert_eq!(period, "Typical weekday for January, 2020");
        assert_eq!(units, "Tons/Day");
    }

    #[test]
    fn period_string_typical_weekend_seasonal() {
        let (period, units) = period_string(
            SummaryKind::TypicalDay,
            DayKind::WeekendDay,
            PeriodKind::Seasonal(SeasonKind::Fall),
            2018,
        );
        assert_eq!(period, "Typical weekend day for Fall Season, 2018");
        assert_eq!(units, "Tons/Day");
    }

    #[test]
    fn period_string_total_seasonal_and_monthly_units() {
        let (_, units) = period_string(
            SummaryKind::PeriodTotal,
            DayKind::Weekday,
            PeriodKind::Seasonal(SeasonKind::Winter),
            2020,
        );
        assert_eq!(units, "Tons/Season");
        let (_, units) = period_string(
            SummaryKind::PeriodTotal,
            DayKind::Weekday,
            PeriodKind::Monthly(MonthKind::July),
            2020,
        );
        assert_eq!(units, "Tons/Month");
    }

    // ---- combine_hose ----

    #[test]
    fn combine_hose_folds_rec_marine_into_hose_slot() {
        let mut emissions = zero_emissions();
        emissions[9] = 1.0; // IDXHOS
        emissions[10] = 2.0; // IDXNCK
        emissions[11] = 3.0; // IDXSR
        emissions[12] = 4.0; // IDXVNT
        let tmp = combine_hose(&emissions);
        assert_eq!(tmp[9], 10.0);
        assert_eq!(tmp[10], 0.0);
        assert_eq!(tmp[11], 0.0);
        assert_eq!(tmp[12], 0.0);
    }

    // ---- wrthdr ----

    #[test]
    fn data_header_has_ten_lines_without_retrofit() {
        let header = RunHeader {
            date: "Jan 01 00:00:00: 2020",
            title1: "Test run",
            title2: "Second title",
            options_file: "test.opt",
            summary: SummaryKind::PeriodTotal,
            day: DayKind::Weekday,
            period: PeriodKind::Annual,
            year: 2020,
        };
        let out = rendered(|w| write_data_header(w, &header, false));
        let lines: Vec<&str> = out.lines().collect();
        // dummy, program, version, date, title1, title2, options,
        // period, units, column row.
        assert_eq!(lines.len(), 10);
        assert!(lines[1].contains("NONROAD Emissions Model"));
        assert_eq!(lines[3], "\"Jan 01 00:00:00: 2020\"");
        assert_eq!(lines[4], "\"Test run\"");
        assert_eq!(lines[5], "\"Second title\"");
        assert_eq!(lines[6], "Options file used: test.opt");
        assert_eq!(lines[7], "\"Total for year: 2020\"");
        assert_eq!(lines[8], "\"Tons/Year\"");
        // Column row: 5 key columns + 14 species + 4 trailing.
        assert!(lines[9].starts_with("Cnty ,SubR ,SCC       ,HP   ,Population     ,"));
        assert!(lines[9].contains("THC-Exhaust    ,"));
        assert!(lines[9].contains("HosePerm       ,"));
        assert!(lines[9].trim_end().ends_with("HPAvg          ,"));
    }

    #[test]
    fn data_header_dummy_record_column_count() {
        let header = RunHeader {
            date: "Jan 01 00:00:00: 2020",
            title1: "t1",
            title2: "t2",
            options_file: "o.opt",
            summary: SummaryKind::PeriodTotal,
            day: DayKind::Weekday,
            period: PeriodKind::Annual,
            year: 2020,
        };
        let without = rendered(|w| write_data_header(w, &header, false));
        let with = rendered(|w| write_data_header(w, &header, true));
        // Dummy record: " , , ," + I5 + "," then one " 0.," per
        // quantity column — 19 without retrofit, 21 with.
        assert_eq!(without.lines().next().unwrap().matches(" 0.,").count(), 19);
        assert_eq!(with.lines().next().unwrap().matches(" 0.,").count(), 21);
        // Retrofit header carries the two extra column names.
        assert!(with.lines().last().unwrap().contains("FracRetro"));
        assert!(with.lines().last().unwrap().contains("UnitsRetro"));
    }

    // ---- wrtdat ----

    #[test]
    fn data_record_key_and_trailing_comma() {
        let record = OutputRecord {
            fips: "06037",
            subregion: "",
            scc: "2270001010",
            hp: 25.7,
            population: 100.0,
            fuel_consumption: 5.0,
            activity: 3.0,
            load_factor: 0.5,
            hp_avg: 24.0,
            frac_retrofit: -9.0,
            units_retrofit: -9.0,
            emissions: zero_emissions(),
        };
        let out = rendered(|w| write_data_record(w, &record, false));
        let line = out.trim_end_matches('\n');
        // HP is INT-truncated to 25.
        assert!(line.starts_with("06037,     ,2270001010,   25,"));
        assert!(line.ends_with(','));
        // 3 key separators + population + 14 emissions + 4 quantity
        // fields + 1 trailing comma.
        assert_eq!(line.matches(',').count(), 3 + 1 + 14 + 4 + 1);
    }

    #[test]
    fn data_record_negative_value_is_empty_field() {
        let mut record = OutputRecord {
            fips: "06037",
            subregion: "SUB1 ",
            scc: "2270001010",
            hp: 10.0,
            population: -9.0, // no-data marker
            fuel_consumption: 1.0,
            activity: -9.0,
            load_factor: -9.0,
            hp_avg: -9.0,
            frac_retrofit: 0.25,
            units_retrofit: 40.0,
            emissions: zero_emissions(),
        };
        record.emissions[0] = 1.5; // THC
        let out = rendered(|w| write_data_record(w, &record, true));
        let line = out.trim_end_matches('\n');
        // Population negative ⇒ empty: the field after the HP key is
        // just ",," (comma to open the field, comma to close it).
        assert!(line.contains(",   10,,"));
        // THC emission column present and formatted E15.8.
        assert!(line.contains(&fortran_e(1.5, 15, 8)));
        // Retrofit columns are emitted (retrofit = true).
        assert!(line.contains(&fortran_e(0.25, 15, 8)));
        assert!(line.contains(&fortran_e(40.0, 15, 8)));
    }

    #[test]
    fn data_record_emission_columns_use_hose_combined_order() {
        let mut emissions = zero_emissions();
        emissions[9] = 1.0; // IDXHOS
        emissions[10] = 1.0; // IDXNCK — folds into HosePerm
        emissions[11] = 1.0; // IDXSR
        emissions[12] = 1.0; // IDXVNT
        let record = OutputRecord {
            fips: "48201",
            subregion: "",
            scc: "2270002000",
            hp: 50.0,
            population: 1.0,
            fuel_consumption: 1.0,
            activity: 1.0,
            load_factor: 1.0,
            hp_avg: 1.0,
            frac_retrofit: -9.0,
            units_retrofit: -9.0,
            emissions,
        };
        let out = rendered(|w| write_data_record(w, &record, false));
        // The final emission column (HosePerm) is the sum 1+1+1+1.
        assert!(out.contains(&fortran_e(4.0, 15, 8)));
    }

    // ---- hdrbmy ----

    #[test]
    fn bmy_header_exhaust_columns() {
        let out = rendered(|w| write_bmy_header(w, ByModelYearKind::Exhaust, false));
        let line = out.trim_end_matches('\n');
        assert!(line.starts_with("Cnty ,SubR ,SCC       ,HP   ,TechType  ,MYr ,Population     ,"));
        assert!(line.contains("Crankcase      ,"));
        assert!(line.contains("FuelCons.      ,"));
        assert!(line.trim_end().ends_with("HPAvg          ,"));
        assert!(!line.contains("FracRetro"));
    }

    #[test]
    fn bmy_header_evap_has_no_exhaust_only_columns() {
        let out = rendered(|w| write_bmy_header(w, ByModelYearKind::Evaporative, true));
        let line = out.trim_end_matches('\n');
        assert!(line.contains("Diurnal        ,"));
        assert!(line.contains("HosePerm       ,"));
        assert!(line.trim_end().ends_with("Activity       ,"));
        // LF / HPAvg / retrofit are exhaust-only.
        assert!(!line.contains("HPAvg"));
        assert!(!line.contains("FracRetro"));
    }

    #[test]
    fn bmy_header_exhaust_retrofit_columns() {
        let out = rendered(|w| write_bmy_header(w, ByModelYearKind::Exhaust, true));
        assert!(out.contains("FracRetro"));
        assert!(out.contains("UnitsRetro"));
    }

    // ---- wrtbmy ----

    fn sample_bmy_record() -> ByModelYearRecord<'static> {
        ByModelYearRecord {
            fips: "06037",
            subregion: "",
            scc: "2270001010",
            hp: 25.0,
            tech_type: "T2",
            model_year: 2015,
            population: 100.0,
            fuel_consumption: 5.0,
            activity: 3.0,
            load_factor: 0.5,
            hp_avg: 24.0,
            frac_retrofit: -9.0,
            units_retrofit: -9.0,
            emissions: [0.0; MXPOL],
        }
    }

    #[test]
    fn bmy_record_exhaust_has_seven_emission_columns() {
        let record = sample_bmy_record();
        let out = rendered(|w| write_bmy_record(w, &record, ByModelYearKind::Exhaust, false));
        let line = out.trim_end_matches('\n');
        assert!(line.starts_with("06037,     ,2270001010,   25,T2        ,2015,"));
        assert!(line.ends_with(','));
        // 6 key separators + 7 emissions + 4 quantity fields
        // (fuel/activity/LF/HPAvg) + 1 trailing comma. Population is
        // written inline with the key and adds no comma of its own.
        assert_eq!(line.matches(',').count(), 6 + 7 + 4 + 1);
    }

    #[test]
    fn bmy_record_evaporative_has_no_exhaust_only_columns() {
        let record = sample_bmy_record();
        let out = rendered(|w| write_bmy_record(w, &record, ByModelYearKind::Evaporative, false));
        let line = out.trim_end_matches('\n');
        // 6 key separators + 7 emissions + fuel + activity + 1
        // trailing comma; no exhaust-only LF/HPAvg/retrofit columns.
        assert_eq!(line.matches(',').count(), 6 + 7 + 2 + 1);
    }

    #[test]
    fn bmy_record_population_always_written() {
        let mut record = sample_bmy_record();
        record.population = -9.0; // negative, but wrtbmy writes it anyway
        let out = rendered(|w| write_bmy_record(w, &record, ByModelYearKind::Exhaust, false));
        assert!(out.contains(&fortran_e(-9.0, 15, 8)));
    }

    // ---- wrtams ----

    fn sample_ams_header() -> AmsHeader<'static> {
        AmsHeader {
            inventory_type: "A",
            reference_year: 20,
            base_year: 20,
            emission_type: "TY",
            period_code: "DS",
            begin_date: 1234,
            end_date: 5678,
            period_conversion: 1.0,
        }
    }

    #[test]
    fn ams_emits_one_record_per_positive_pollutant() {
        let mut emissions = [0.0_f32; MXPOL];
        emissions[2] = 5.0; // NOx > 0
        emissions[1] = 2.0; // CO > 0
        let counties = [AmsCountyEmissions {
            fips: "06037",
            emissions,
        }];
        let out = rendered(|w| write_ams(w, &sample_ams_header(), "2270001010", &counties));
        // NOx and CO are positive; the other three pollutants are 0.
        assert_eq!(out.lines().count(), 2);
        assert!(out.contains("42603")); // ISCNOX SAROAD code
        assert!(out.contains("42101")); // ISCCO SAROAD code
    }

    #[test]
    fn ams_hc_total_sums_six_pollutants() {
        let mut emissions = [0.0_f32; MXPOL];
        emissions[0] = 1.0; // THC
        emissions[6] = 1.0; // Crankcase
        emissions[7] = 1.0; // Diurnal
        emissions[14] = 1.0; // Displacement
        emissions[15] = 1.0; // Spillage
        emissions[16] = 1.0; // RunLoss
        let totals = ams_pollutant_totals(&emissions, 1.0);
        assert_eq!(totals[0], 6.0);
    }

    #[test]
    fn ams_skips_county_with_no_positive_emissions() {
        let counties = [AmsCountyEmissions {
            fips: "06037",
            emissions: [0.0; MXPOL],
        }];
        let out = rendered(|w| write_ams(w, &sample_ams_header(), "2270001010", &counties));
        assert!(out.is_empty());
    }

    #[test]
    fn ams_record_has_36_filler_nines() {
        let mut emissions = [0.0_f32; MXPOL];
        emissions[2] = 1.0;
        let line = ams_record(&sample_ams_header(), "2270001010", "06037", 42603, 1.0);
        // 3 + 1 + 28 + 4 = 36 `-9` fillers.
        assert_eq!(line.matches("-9").count(), 36);
        assert!(line.starts_with("A20 20"));
        let _ = emissions;
    }
}
