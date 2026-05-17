//! Message-file writers — `wrtmsg.f` and `wrtsum.f` (Task 114).
//!
//! Two routines write to NONROAD's human-readable `.MSG` file:
//!
//! | Fortran | Lines | Rust |
//! |---|---|---|
//! | `wrtmsg.f` | 242 | [`write_message_report`] |
//! | `wrtsum.f` | 117 | [`write_population_summary`] |
//!
//! `wrtmsg.f` echoes the run's `/OPTIONS/`, `/PERIOD/`, `/REGION/`
//! and `/SOURCE CATEGORY/` packets; `wrtsum.f` reports how many
//! population records were found per state and county. Both lay
//! their lines out by column with the Fortran `T` (tab) descriptor —
//! see [`FortranLine`].
//!
//! Following the COMMON-block decoupling pattern of the rest of the
//! crate, each routine takes a structured input ([`MessageReport`],
//! the `wrtsum` state/county slices) rather than reading global
//! state; the Task 117 integration step populates it. See the
//! `writers` module for the shared I/O policy.

use std::io::{self, Write};

use crate::output::fortran_fmt::{fortran_a, fortran_f, fortran_i, FortranLine};
use crate::output::strutil::strmin;
use crate::output::writers::{DayKind, MonthKind, PeriodKind, SeasonKind, SummaryKind};

// ===========================================================================
// Shared line layout (the wrtmsg.f 9000–9004 formats)
// ===========================================================================

/// A section-header line: text starting at column 20 — `wrtmsg.f`
/// format `9000` (`T20,A`).
fn header_line(text: &str) -> String {
    let mut line = FortranLine::new();
    line.tab(20);
    line.text(text);
    line.finish()
}

/// A label-only line: a label starting at column 10 — `wrtmsg.f`
/// format `9001` with no value (the `:` colon descriptor ends the
/// record after the label).
fn label_line(label: &str) -> String {
    let mut line = FortranLine::new();
    line.tab(10);
    line.text(label);
    line.finish()
}

/// A `label : value` line — `wrtmsg.f` formats `9001`–`9004`: the
/// label at column 10, a `:` at column 30, the value from column 31.
///
/// When the label runs past column 29 the `:` overwrites into it,
/// faithfully to the Fortran `T30` back-tab.
fn labeled_line(label: &str, value: &str) -> String {
    let mut line = FortranLine::new();
    line.tab(10);
    line.text(label);
    line.tab(30);
    line.text(":");
    line.text(value);
    line.finish()
}

// ===========================================================================
// wrtmsg — echo the run parameters
// ===========================================================================

/// The region the run covers — the `wrtmsg.f` `/REGION/`-packet
/// branches (`:157–196`).
#[derive(Debug, Clone, Copy)]
pub enum RegionOfInterest<'a> {
    /// `reglvl = USTOT` — `wrtmsg.f` has no branch for the US-total
    /// level, so only the section header is written.
    UsTotal,
    /// `reglvl = NATION`, or `STATE` with `reglst(1) = "00000"` —
    /// state-level estimates for all 50 states.
    AllStates,
    /// `reglvl = STATE` with an explicit list of states.
    States(&'a [StateOfInterest<'a>]),
    /// `reglvl = COUNTY` — an explicit list of counties.
    Counties(&'a [CountyOfInterest<'a>]),
    /// `reglvl = SUBCTY` — an explicit list of sub-county region
    /// codes (Fortran `reglst`).
    Subcounties(&'a [&'a str]),
}

/// A state in the `/REGION/` packet's explicit state list — Fortran
/// `statcd(i)` / `statnm(i)`.
#[derive(Debug, Clone, Copy)]
pub struct StateOfInterest<'a> {
    /// State code (Fortran `statcd`, `character*5`).
    pub code: &'a str,
    /// State name (Fortran `statnm`).
    pub name: &'a str,
}

/// A county in the `/REGION/` packet's explicit county list.
#[derive(Debug, Clone, Copy)]
pub struct CountyOfInterest<'a> {
    /// County FIPS code (Fortran `fipcod`, `character*5`).
    pub fips: &'a str,
    /// County name (Fortran `cntynm`).
    pub name: &'a str,
    /// Name of the state the county belongs to, when known. The
    /// Fortran resolves this from the FIPS state prefix (`fndchr`
    /// against `statcd`); `Some(name)` appends `", name"`, `None`
    /// reproduces the Fortran's blank fallback.
    pub state_name: Option<&'a str>,
}

/// The run's equipment selection — the `wrtmsg.f` `/SOURCE
/// CATEGORY/`-packet branch (`:203–209`).
#[derive(Debug, Clone, Copy)]
pub enum EquipmentSelection<'a> {
    /// Fortran `lascal` set — all equipment types are included.
    All,
    /// An explicit list of selected SCC codes (Fortran `eqpcod(i)`
    /// for each `i` with `lascat(i)` set).
    Selected(&'a [&'a str]),
}

/// The run parameters `wrtmsg.f` echoes to the message file.
///
/// Each field mirrors a COMMON-block variable the Fortran reads from
/// the `/OPTIONS/`, `/PERIOD/`, `/REGION/` and `/SOURCE CATEGORY/`
/// packets.
#[derive(Debug, Clone, Copy)]
pub struct MessageReport<'a> {
    // --- /OPTIONS/ packet ---
    /// First run-title line (Fortran `title1`).
    pub title1: &'a str,
    /// Second run-title line (Fortran `title2`).
    pub title2: &'a str,
    /// Fuel RVP, psi (Fortran `fulrvp`).
    pub fuel_rvp: f32,
    /// Fuel oxygen content, weight % (Fortran `oxypct`).
    pub oxygen_weight_pct: f32,
    /// Gasoline sulfur % (Fortran `soxgas`).
    pub sulfur_gasoline: f32,
    /// Diesel sulfur % (Fortran `soxdsl`).
    pub sulfur_diesel: f32,
    /// Marine-diesel sulfur % (Fortran `soxdsm`).
    pub sulfur_marine_diesel: f32,
    /// LPG/CNG sulfur % (Fortran `soxcng`).
    pub sulfur_lpg_cng: f32,
    /// Minimum temperature (Fortran `tempmn`).
    pub temp_min: f32,
    /// Maximum temperature (Fortran `tempmx`).
    pub temp_max: f32,
    /// Average ambient temperature (Fortran `amtemp`).
    pub temp_ambient: f32,
    /// High-altitude region flag (Fortran `lhigh`).
    pub high_altitude: bool,
    /// Stage II factor (Fortran `stg2fac`); echoed as the control
    /// percentage `100 - stg2fac * 100`.
    pub stage2_factor: f32,
    /// Ethanol blend market share % (Fortran `ethmkt`).
    pub ethanol_market_pct: f32,
    /// Ethanol volume % (Fortran `ethvpct`).
    pub ethanol_volume_pct: f32,
    // --- /PERIOD/ packet ---
    /// Inventory year (Fortran `iepyr`).
    pub year: i32,
    /// Reporting period (Fortran `iprtyp`, with `iseasn`/`imonth`).
    pub period: PeriodKind,
    /// How emissions are summed (Fortran `ismtyp`).
    pub summary: SummaryKind,
    /// Typical day, echoed only for [`SummaryKind::TypicalDay`]
    /// (Fortran `iday`).
    pub day: DayKind,
    /// Year of the growth calculation (Fortran `igryr`).
    pub growth_year: i32,
    /// Year of technology selection (Fortran `itchyr`).
    pub tech_year: i32,
    // --- /REGION/ and /SOURCE CATEGORY/ packets ---
    /// The region the run covers (Fortran `reglvl` and its lists).
    pub region: RegionOfInterest<'a>,
    /// The run's equipment selection (Fortran `lascal`/`lascat`).
    pub equipment: EquipmentSelection<'a>,
}

/// Period-type name as `wrtmsg.f` echoes it (`:111–115`): `PERANN`,
/// `PERSES`, `PERMTH` trimmed, with `" period"` appended.
fn period_name(period: PeriodKind) -> &'static str {
    match period {
        PeriodKind::Annual => "ANNUAL period",
        PeriodKind::Seasonal(_) => "SEASONAL period",
        PeriodKind::Monthly(_) => "MONTHLY period",
    }
}

/// Season name as `wrtmsg.f` echoes it (`:121–124` — `SESFAL` is
/// `AUTUMN`, unlike the `Fall` of `wrthdr`'s period string).
fn season_name(season: SeasonKind) -> &'static str {
    match season {
        SeasonKind::Winter => "WINTER",
        SeasonKind::Spring => "SPRING",
        SeasonKind::Summer => "SUMMER",
        SeasonKind::Fall => "AUTUMN",
    }
}

/// Month name as `wrtmsg.f` echoes it (`:128–139` — the upper-case
/// `MONJAN..MONDEC` parameters).
fn month_name(month: MonthKind) -> &'static str {
    match month {
        MonthKind::January => "JANUARY",
        MonthKind::February => "FEBRUARY",
        MonthKind::March => "MARCH",
        MonthKind::April => "APRIL",
        MonthKind::May => "MAY",
        MonthKind::June => "JUNE",
        MonthKind::July => "JULY",
        MonthKind::August => "AUGUST",
        MonthKind::September => "SEPTEMBER",
        MonthKind::October => "OCTOBER",
        MonthKind::November => "NOVEMBER",
        MonthKind::December => "DECEMBER",
    }
}

/// Write the `/OPTIONS/` packet — `wrtmsg.f` :80–103.
fn write_options<W: Write>(w: &mut W, report: &MessageReport<'_>) -> io::Result<()> {
    writeln!(w)?;
    writeln!(w, "{}", header_line("*** Scenario Specific Parameters ***"))?;
    writeln!(w)?;
    writeln!(
        w,
        "{}",
        labeled_line("First Title line", &report.title1[..strmin(report.title1)])
    )?;
    writeln!(
        w,
        "{}",
        labeled_line("Second Title line", &report.title2[..strmin(report.title2)])
    )?;
    let f6 = |v: f32| fortran_f(v, 6, 2);
    let f8 = |v: f32| fortran_f(v, 8, 4);
    writeln!(
        w,
        "{}",
        labeled_line("Fuel RVP (psi)", &f6(report.fuel_rvp))
    )?;
    writeln!(
        w,
        "{}",
        labeled_line("Fuel Oxygen weight %", &f6(report.oxygen_weight_pct))
    )?;
    writeln!(
        w,
        "{}",
        labeled_line("Gasoline Sulfur %", &f8(report.sulfur_gasoline))
    )?;
    writeln!(
        w,
        "{}",
        labeled_line("Diesel Sulfur %", &f8(report.sulfur_diesel))
    )?;
    writeln!(
        w,
        "{}",
        labeled_line("Marine Diesel Sulfur %", &f8(report.sulfur_marine_diesel))
    )?;
    writeln!(
        w,
        "{}",
        labeled_line("LPG/CNG Sulfur %", &f8(report.sulfur_lpg_cng))
    )?;
    writeln!(
        w,
        "{}",
        labeled_line("Minimum Temperature", &f6(report.temp_min))
    )?;
    writeln!(
        w,
        "{}",
        labeled_line("Maximum Temperature", &f6(report.temp_max))
    )?;
    writeln!(
        w,
        "{}",
        labeled_line("Average Ambient Temp", &f6(report.temp_ambient))
    )?;
    // wrtmsg.f :96–100 — FLAGHI / FLAGLO are the 5-character
    // 'HIGH ' / 'LOW  ' parameters.
    let altitude = if report.high_altitude {
        "HIGH "
    } else {
        "LOW  "
    };
    writeln!(w, "{}", labeled_line("Altitude of region", altitude))?;
    // wrtmsg.f :101 — the Stage II control percentage.
    let stage2 = 100.0_f32 - report.stage2_factor * 100.0;
    writeln!(w, "{}", labeled_line("Stage II Control %", &f6(stage2)))?;
    writeln!(
        w,
        "{}",
        labeled_line("EtOH Blend % Mkt", &f6(report.ethanol_market_pct))
    )?;
    writeln!(
        w,
        "{}",
        labeled_line("EtOH Vol %", &f6(report.ethanol_volume_pct))
    )
}

/// Write the `/PERIOD/` packet — `wrtmsg.f` :107–150.
fn write_period<W: Write>(w: &mut W, report: &MessageReport<'_>) -> io::Result<()> {
    writeln!(w)?;
    writeln!(w, "{}", header_line("*** Period Parameters ***"))?;
    writeln!(w)?;
    let i4 = |v: i32| fortran_i(i64::from(v), 4);
    writeln!(w, "{}", labeled_line("Year of Inventory", &i4(report.year)))?;
    writeln!(
        w,
        "{}",
        labeled_line("Inventory for", period_name(report.period))
    )?;
    let summary = match report.summary {
        SummaryKind::TypicalDay => "TYPICAL DAY",
        SummaryKind::PeriodTotal => "PERIOD TOTAL",
    };
    writeln!(w, "{}", labeled_line("Emissions summed for", summary))?;
    // wrtmsg.f :120–142 — a season or month line, period-dependent.
    match report.period {
        PeriodKind::Seasonal(season) => {
            writeln!(w, "{}", labeled_line("Season", season_name(season)))?;
        }
        PeriodKind::Monthly(month) => {
            writeln!(w, "{}", labeled_line("Month", month_name(month)))?;
        }
        PeriodKind::Annual => {}
    }
    // wrtmsg.f :143–148 — the day-of-week line, typical-day runs only.
    if report.summary == SummaryKind::TypicalDay {
        let day = match report.day {
            DayKind::Weekday => "WEEKDAY",
            DayKind::WeekendDay => "WEEKEND",
        };
        writeln!(w, "{}", labeled_line("Day of week", day))?;
    }
    writeln!(
        w,
        "{}",
        labeled_line("Year of Growth Calc", &i4(report.growth_year))
    )?;
    writeln!(
        w,
        "{}",
        labeled_line("Year of Tech Sel", &i4(report.tech_year))
    )
}

/// Write the `/REGION/` packet — `wrtmsg.f` :154–196.
fn write_region<W: Write>(w: &mut W, region: &RegionOfInterest<'_>) -> io::Result<()> {
    writeln!(w)?;
    writeln!(w, "{}", header_line("*** Region of Interest ***"))?;
    writeln!(w)?;
    match region {
        RegionOfInterest::UsTotal => {}
        RegionOfInterest::AllStates => {
            writeln!(
                w,
                "{}",
                labeled_line("Region level", " State-level estimates")
            )?;
            writeln!(w, "{}", labeled_line("States of Interest", "All 50 states"))?;
        }
        RegionOfInterest::States(states) => {
            writeln!(
                w,
                "{}",
                labeled_line("Region level", " State-level estimates")
            )?;
            writeln!(w, "{}", label_line("States of Interest"))?;
            for state in *states {
                let value = format!("{} - {}", fortran_a(state.code, 5), state.name);
                writeln!(w, "{}", labeled_line(" ", &value))?;
            }
        }
        RegionOfInterest::Counties(counties) => {
            writeln!(
                w,
                "{}",
                labeled_line("Region level", " County-level estimates")
            )?;
            writeln!(w, "{}", label_line("Counties of Interest"))?;
            for county in *counties {
                // wrtmsg.f :181–185 — the state suffix is ", name"
                // when the FIPS prefix resolves, else a single blank.
                let suffix = match county.state_name {
                    Some(name) => format!(", {name}"),
                    None => " ".to_string(),
                };
                let value = format!("{} - {}{}", fortran_a(county.fips, 5), county.name, suffix);
                writeln!(w, "{}", labeled_line(" ", &value))?;
            }
        }
        RegionOfInterest::Subcounties(areas) => {
            writeln!(
                w,
                "{}",
                labeled_line("Region level", " Sub-County-level estimates")
            )?;
            writeln!(w, "{}", label_line("Areas of Interest"))?;
            for area in *areas {
                writeln!(w, "{}", labeled_line(" ", area))?;
            }
        }
    }
    Ok(())
}

/// Write the `/SOURCE CATEGORY/` packet — `wrtmsg.f` :200–209.
fn write_source_category<W: Write>(
    w: &mut W,
    equipment: &EquipmentSelection<'_>,
) -> io::Result<()> {
    writeln!(w)?;
    writeln!(w, "{}", header_line("*** Equipment Types ***"))?;
    writeln!(w)?;
    match equipment {
        EquipmentSelection::All => {
            writeln!(w, "{}", label_line("All equipment types."))?;
        }
        EquipmentSelection::Selected(codes) => {
            writeln!(w, "{}", label_line("SCC codes Selected"))?;
            for code in *codes {
                writeln!(w, "{}", labeled_line(" ", &fortran_a(code, 10)))?;
            }
        }
    }
    Ok(())
}

/// Echo the run parameters to the message file — `wrtmsg.f`.
///
/// Writes the `/OPTIONS/`, `/PERIOD/`, `/REGION/` and `/SOURCE
/// CATEGORY/` packet sections, each introduced by a blank line, a
/// `*** … ***` header at column 20, and a second blank line.
pub fn write_message_report<W: Write>(w: &mut W, report: &MessageReport<'_>) -> io::Result<()> {
    write_options(w, report)?;
    write_period(w, report)?;
    write_region(w, &report.region)?;
    write_source_category(w, &report.equipment)
}

// ===========================================================================
// wrtsum — population-record-count summary
// ===========================================================================

/// One county's population-record count — `wrtsum.f`'s inner-loop
/// row (`fipcod(j)`, `cntynm(j)`, `nctyrc(j)`).
#[derive(Debug, Clone, Copy)]
pub struct CountyRecordCount<'a> {
    /// County FIPS code (Fortran `fipcod`, `character*5`).
    pub fips: &'a str,
    /// County name (Fortran `cntynm`).
    pub name: &'a str,
    /// Population records found for the county (Fortran `nctyrc`).
    pub records: i32,
}

/// One state's population-record count and its counties —
/// `wrtsum.f`'s outer-loop row (`statcd(i)`, `statnm(i)`,
/// `nstarc(i)`).
#[derive(Debug, Clone, Copy)]
pub struct StateRecordCount<'a> {
    /// State code (Fortran `statcd`, `character*5`).
    pub code: &'a str,
    /// State name (Fortran `statnm`).
    pub name: &'a str,
    /// Population records found for the state (Fortran `nstarc`).
    pub records: i32,
    /// The state's counties that have county-specific records — the
    /// rows the Fortran `lfipcd(j) .AND. lctlev(j)` filter keeps.
    pub counties: &'a [CountyRecordCount<'a>],
}

/// One `wrtsum.f` record-count line: a code/label and a name, then
/// a `:` and the count `I5` at `colon_column` — `wrtsum.f`'s
/// `(20X,3A,T<n>,A,I5)` formats.
///
/// `field1` is laid out in `width1` columns and `field2` in `width2`;
/// when the two run past `colon_column` the `:` overwrites into
/// them, faithful to the Fortran `T` back-tab (`wrtsum.f`'s
/// national-record line does exactly this).
fn record_count_line(
    field1: &str,
    width1: usize,
    field2: &str,
    width2: usize,
    colon_column: usize,
    count: i32,
) -> String {
    let mut line = FortranLine::new();
    line.skip(20); // 20X
    line.text(&fortran_a(field1, width1));
    line.text(" ");
    line.text(&fortran_a(field2, width2));
    line.tab(colon_column);
    line.text(":");
    line.text(&fortran_i(i64::from(count), 5));
    line.finish()
}

/// Write the population-record-count summary — `wrtsum.f`.
///
/// `national_records` is the Fortran `nnatrc`; its line is emitted
/// only when it is positive. `states` carries the per-state counts
/// and, nested, the per-county counts — the rows kept by the Fortran
/// `lstacd .AND. lstlev` and `lfipcd .AND. lctlev` filters, which the
/// caller applies (see `ARCHITECTURE.md` § 4.3).
pub fn write_population_summary<W: Write>(
    w: &mut W,
    national_records: i32,
    states: &[StateRecordCount<'_>],
) -> io::Result<()> {
    // wrtsum.f :57–58 — a blank line, the heading, a blank line.
    writeln!(w)?;
    writeln!(w, "   **** Number of Population Records Found ****")?;
    writeln!(w)?;

    // wrtsum.f :62–65 — the national-record line, T45.
    if national_records > 0 {
        writeln!(
            w,
            "{}",
            record_count_line(
                "Entire U.S.",
                11,
                "National Record",
                15,
                45,
                national_records
            )
        )?;
    }

    // wrtsum.f :69–86 — per state, then per county, T77.
    for state in states {
        writeln!(
            w,
            "{}",
            record_count_line(state.code, 5, state.name, 50, 77, state.records)
        )?;
        for county in state.counties {
            writeln!(
                w,
                "{}",
                record_count_line(county.fips, 5, county.name, 50, 77, county.records)
            )?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_report() -> MessageReport<'static> {
        MessageReport {
            title1: "Run title one",
            title2: "Run title two",
            fuel_rvp: 8.7,
            oxygen_weight_pct: 2.0,
            sulfur_gasoline: 0.003,
            sulfur_diesel: 0.05,
            sulfur_marine_diesel: 0.05,
            sulfur_lpg_cng: 0.003,
            temp_min: 60.0,
            temp_max: 84.0,
            temp_ambient: 72.0,
            high_altitude: false,
            stage2_factor: 1.0,
            ethanol_market_pct: 0.0,
            ethanol_volume_pct: 0.0,
            year: 2020,
            period: PeriodKind::Annual,
            summary: SummaryKind::PeriodTotal,
            day: DayKind::Weekday,
            growth_year: 2020,
            tech_year: 2020,
            region: RegionOfInterest::AllStates,
            equipment: EquipmentSelection::All,
        }
    }

    fn rendered<F>(f: F) -> String
    where
        F: FnOnce(&mut Vec<u8>) -> io::Result<()>,
    {
        let mut buf = Vec::new();
        f(&mut buf).expect("writer must not fail to an in-memory buffer");
        String::from_utf8(buf).expect("writer output is ASCII text")
    }

    // ---- line helpers ----

    #[test]
    fn labeled_line_places_label_and_colon() {
        let line = labeled_line("Fuel RVP (psi)", "  8.70");
        // Label from column 10, ':' at column 30, value from 31.
        assert_eq!(&line[0..9], "         ");
        assert_eq!(&line[9..23], "Fuel RVP (psi)");
        assert_eq!(&line[29..30], ":");
        assert_eq!(&line[30..], "  8.70");
    }

    #[test]
    fn labeled_line_long_label_overwrites_at_colon() {
        // "Marine Diesel Sulfur %" is 22 chars: column 10 + 22 runs
        // to column 31, so the T30 ':' overwrites column 30.
        let line = labeled_line("Marine Diesel Sulfur %", "  0.0500");
        assert_eq!(&line[29..30], ":");
    }

    #[test]
    fn header_line_starts_at_column_20() {
        let line = header_line("*** Period Parameters ***");
        assert_eq!(&line[0..19], &" ".repeat(19));
        assert_eq!(&line[19..], "*** Period Parameters ***");
    }

    // ---- wrtmsg ----

    #[test]
    fn message_report_has_four_section_headers() {
        let out = rendered(|w| write_message_report(w, &sample_report()));
        assert!(out.contains("*** Scenario Specific Parameters ***"));
        assert!(out.contains("*** Period Parameters ***"));
        assert!(out.contains("*** Region of Interest ***"));
        assert!(out.contains("*** Equipment Types ***"));
    }

    #[test]
    fn message_report_echoes_options_values() {
        let out = rendered(|w| write_message_report(w, &sample_report()));
        assert!(out.contains("Run title one"));
        // Altitude low ⇒ the 'LOW  ' flag string.
        assert!(out.contains("Altitude of region"));
        // Stage II control % = 100 - 1.0*100 = 0.
        let stage2 = out
            .lines()
            .find(|l| l.contains("Stage II Control %"))
            .unwrap();
        assert!(stage2.ends_with(&fortran_f(0.0, 6, 2)));
    }

    #[test]
    fn message_report_annual_run_has_no_season_or_day_line() {
        let out = rendered(|w| write_message_report(w, &sample_report()));
        assert!(!out.contains("Season"));
        assert!(!out.contains("Month"));
        assert!(!out.contains("Day of week"));
    }

    #[test]
    fn message_report_seasonal_typical_day_adds_lines() {
        let mut report = sample_report();
        report.period = PeriodKind::Seasonal(SeasonKind::Fall);
        report.summary = SummaryKind::TypicalDay;
        report.day = DayKind::WeekendDay;
        let out = rendered(|w| write_message_report(w, &report));
        // wrtmsg uses AUTUMN for the fall season.
        assert!(out.contains("AUTUMN"));
        assert!(out.contains("Day of week"));
        assert!(out.contains("WEEKEND"));
        assert!(out.contains("SEASONAL period"));
    }

    #[test]
    fn message_report_state_list_is_enumerated() {
        let mut report = sample_report();
        let states = [
            StateOfInterest {
                code: "06000",
                name: "California",
            },
            StateOfInterest {
                code: "48000",
                name: "Texas",
            },
        ];
        report.region = RegionOfInterest::States(&states);
        let out = rendered(|w| write_message_report(w, &report));
        assert!(out.contains("06000 - California"));
        assert!(out.contains("48000 - Texas"));
    }

    #[test]
    fn message_report_county_list_includes_state_suffix() {
        let mut report = sample_report();
        let counties = [CountyOfInterest {
            fips: "06037",
            name: "Los Angeles",
            state_name: Some("California"),
        }];
        report.region = RegionOfInterest::Counties(&counties);
        let out = rendered(|w| write_message_report(w, &report));
        assert!(out.contains("06037 - Los Angeles, California"));
        assert!(out.contains("County-level estimates"));
    }

    #[test]
    fn message_report_selected_equipment_is_listed() {
        let mut report = sample_report();
        let codes = ["2260001010", "2265001010"];
        report.equipment = EquipmentSelection::Selected(&codes);
        let out = rendered(|w| write_message_report(w, &report));
        assert!(out.contains("SCC codes Selected"));
        assert!(out.contains("2260001010"));
        assert!(out.contains("2265001010"));
    }

    // ---- wrtsum ----

    #[test]
    fn population_summary_heading_and_blank_lines() {
        let out = rendered(|w| write_population_summary(w, 0, &[]));
        let lines: Vec<&str> = out.split('\n').collect();
        assert_eq!(lines[0], "");
        assert_eq!(lines[1], "   **** Number of Population Records Found ****");
        assert_eq!(lines[2], "");
    }

    #[test]
    fn population_summary_omits_national_line_when_zero() {
        let out = rendered(|w| write_population_summary(w, 0, &[]));
        assert!(!out.contains("Entire U.S."));
        let out = rendered(|w| write_population_summary(w, 5, &[]));
        assert!(out.contains("Entire U.S."));
    }

    #[test]
    fn population_summary_national_line_back_tabs_the_colon() {
        // wrtsum.f's T45 tabs back into "National Record": the ':'
        // lands at column 45, overwriting the second 'o' of "Record".
        let out = rendered(|w| write_population_summary(w, 42, &[]));
        let national = out.lines().find(|l| l.contains("Entire U.S.")).unwrap();
        assert_eq!(&national[44..45], ":");
        assert!(national.contains("National Rec:"));
        assert!(national.ends_with(&fortran_i(42, 5)));
    }

    #[test]
    fn population_summary_lists_states_and_counties() {
        let counties = [CountyRecordCount {
            fips: "06037",
            name: "Los Angeles",
            records: 12,
        }];
        let states = [StateRecordCount {
            code: "06000",
            name: "California",
            records: 30,
            counties: &counties,
        }];
        let out = rendered(|w| write_population_summary(w, 0, &states));
        assert!(out.contains("California"));
        assert!(out.contains("Los Angeles"));
        // The state line ends with the I5-formatted record count.
        let state_line = out.lines().find(|l| l.contains("California")).unwrap();
        assert!(state_line.ends_with(&fortran_i(30, 5)));
        let county_line = out.lines().find(|l| l.contains("Los Angeles")).unwrap();
        assert!(county_line.ends_with(&fortran_i(12, 5)));
    }
}
