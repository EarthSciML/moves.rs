//! Top-level orchestration — `nonroad.f` (397 lines).
//!
//! `nonroad.f` is NONROAD's `program` unit: it reads the options
//! file, loads every input, then runs a two-level loop —
//!
//! - the **outer loop** (`nonroad.f` label `111`) calls `getpop` to
//!   pull one SCC's worth of population records at a time, gates the
//!   SCC against the requested equipment list, and resets the AMS
//!   accumulator;
//! - the **inner loop** (label `333`) walks that SCC group's records,
//!   detects growth-record pairs, resolves the fuel and region, and
//!   dispatches each record to one of the six geography routines.
//!
//! When all SCC groups are processed it writes the summary and the
//! optional SI report and prints a completion banner.
//!
//! # What this module ports, and what it does not
//!
//! The output writers (`wrthdr`, `wrtsum`, `wrtsi`, `wrtams`,
//! `clsnon`) are **Task 114**; assembling the populated callback
//! context the six geography routines need is the **Task 117**
//! integration step (see the `driver` module docs). Neither is
//! available yet, so — exactly as the `geography` module did with the
//! writers — this module ports the driver loop's *decision logic* and
//! *control flow* as pure, tested functions and leaves the execution
//! wiring to the consuming task:
//!
//! - [`fuel_for_scc`] — the SCC-prefix → fuel classification
//!   (`nonroad.f` :240–260).
//! - [`classify_region`] — the region-code → [`RegionShape`]
//!   classification (`nonroad.f` :167–168, :226–227, :265, :282).
//! - [`dispatch_for`] — the `(region shape, run level)` → geography
//!   routine decision (`nonroad.f` :265–323).
//! - [`growth_pair`] — the growth-record-pair lookahead and rate
//!   (`nonroad.f` :206–219).
//! - [`plan_scc_group`] — the inner record loop (`333`): the
//!   skip-the-growth-partner iteration, per-record region selection,
//!   and the ordered dispatch decisions, returned as a [`SccGroupPlan`].
//! - [`completion_message`] — the closing banner (`nonroad.f` :346–356).
//!
//! The outer loop's `getpop` iteration and the equipment-list SCC
//! gate (`nonroad.f` :159–161, which needs `getpop` wired up) are part
//! of the Task 117 integration; [`plan_scc_group`] is the inner loop
//! for one SCC group the outer loop has already accepted.

use crate::emissions::exhaust::FuelKind;

/// Program name — Fortran `PROGNM` parameter from `nonrdio.inc` :39.
///
/// Reproduced verbatim, including the leading space and the backtick
/// the Fortran source uses in place of an apostrophe (a `'` cannot be
/// embedded in a Fortran `'...'` literal). Used by [`completion_message`].
pub const PROGRAM_NAME: &str = " EPA`s NONROAD Emissions Model";

/// Model version string — Fortran `VERSON` parameter from
/// `nonrdio.inc` :40.
pub const VERSION: &str = "Core Model ver 2008a, 07/06/09";

// ---------------------------------------------------------------------------
// Region level
// ---------------------------------------------------------------------------

/// The geographic level a run operates at — Fortran `reglvl`, set
/// from the `/REGION/` packet to one of the `USTOT`/`NATION`/`STATE`/
/// `COUNTY`/`SUBCTY` parameters in `nonrdusr.inc`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegionLevel {
    /// `USTOT` (`'US TOTAL  '`) — a single US-total result.
    UsTotal,
    /// `NATION` (`'50STATE   '`) — national totals, allocated to
    /// states.
    Nation,
    /// `STATE` (`'STATE     '`) — state-level results.
    State,
    /// `COUNTY` (`'COUNTY    '`) — county-level results.
    County,
    /// `SUBCTY` (`'SUBCOUNTY '`) — subcounty-level results.
    Subcounty,
}

impl RegionLevel {
    /// Parse the Fortran `reglvl` COMMON-block string.
    ///
    /// Accepts the `nonrdusr.inc` parameter values
    /// (`'US TOTAL  '`, `'50STATE   '`, `'STATE     '`,
    /// `'COUNTY    '`, `'SUBCOUNTY '`); leading/trailing blanks are
    /// ignored and the match is case-insensitive. Returns `None` for
    /// any other string.
    pub fn from_reglvl(s: &str) -> Option<RegionLevel> {
        match s.trim().to_ascii_uppercase().as_str() {
            "US TOTAL" => Some(RegionLevel::UsTotal),
            "50STATE" => Some(RegionLevel::Nation),
            "STATE" => Some(RegionLevel::State),
            "COUNTY" => Some(RegionLevel::County),
            "SUBCOUNTY" => Some(RegionLevel::Subcounty),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Region shape
// ---------------------------------------------------------------------------

/// The shape of a population record's 5-character region code, as
/// `nonroad.f` classifies it before dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegionShape {
    /// The all-zero code `"00000"` — a national record
    /// (`nonroad.f` :265).
    National,
    /// A state code: county digits (chars 3–5) are `"000"`
    /// (`nonroad.f` :282).
    StateCode,
    /// A county code: anything else (`nonroad.f` :297).
    CountyCode,
}

/// Classify a region code's shape — `nonroad.f` :167/:226/:265/:282.
///
/// The driver inspects only the first five characters: the whole
/// 5-char field for the national test, and characters 3–5 for the
/// state-vs-county test. Codes shorter than five characters are
/// classified defensively as [`RegionShape::CountyCode`] (a
/// production region code is always at least five characters).
pub fn classify_region(region_code: &str) -> RegionShape {
    let code5 = region_code.get(..5).unwrap_or(region_code);
    if code5 == "00000" {
        RegionShape::National
    } else if region_code.get(2..5) == Some("000") {
        RegionShape::StateCode
    } else {
        RegionShape::CountyCode
    }
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

/// One geography routine the driver dispatches a record to.
///
/// Each variant names the Fortran routine and the `crate::geography`
/// entry point that ports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dispatch {
    /// `prcus` — US-total processing
    /// ([`crate::geography::process_us_total_record`]).
    UsTotal,
    /// `prcnat` — national-to-state processing
    /// ([`crate::geography::process_national_record`]).
    National,
    /// `prcsta` — state-to-county processing
    /// ([`crate::geography::process_state_to_county_record`]).
    StateToCounty,
    /// `prc1st` — state-from-national processing
    /// ([`crate::geography::process_state_from_national_record`]).
    StateFromNational,
    /// `prccty` — county-level processing
    /// ([`crate::geography::process_county`]).
    County,
    /// `prcsub` — subcounty-level processing
    /// ([`crate::geography::process_subcounty`]).
    Subcounty,
}

/// Region tables the driver consults while planning an SCC group.
///
/// These mirror the Fortran COMMON-block arrays the record loop reads
/// (`statcd`/`lstacd`, `fipcod`/`lfipcd`, `reglst`). The Task 117
/// integration layer fills them from the loaded `/REGION/` packet;
/// [`Default`] (all empty) is convenient for unit tests.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunRegions {
    /// 5-character state codes selected for the run — Fortran
    /// `statcd` filtered by the `lstacd` selection flag. A state-code
    /// record whose code is absent here is skipped (`nonroad.f`
    /// :228–230).
    pub selected_states: Vec<String>,
    /// 5-character county FIPS codes selected for the run — Fortran
    /// `fipcod` filtered by the `lfipcd` selection flag. A county-code
    /// record whose code is absent here is skipped (`nonroad.f`
    /// :232–234).
    pub selected_counties: Vec<String>,
    /// The region list (`reglst`) — consulted only at
    /// [`RegionLevel::Subcounty`] to split a county record into a
    /// whole-county (`prccty`) and/or a subcounty (`prcsub`) dispatch.
    /// Entries are region codes; a bare 5-character county code marks
    /// a whole-county region, a longer code a subcounty region.
    pub region_list: Vec<String>,
}

/// Decide which geography routine(s) a record dispatches to —
/// `nonroad.f` :265–323.
///
/// Returns the routines in dispatch order. The result is empty when
/// the record's [`RegionShape`] and the run's [`RegionLevel`]
/// correspond to no branch of `nonroad.f`'s dispatch (for example a
/// national record on a county-level run); the Fortran source simply
/// falls through to the next record in that case.
///
/// The [`RegionLevel::Subcounty`] + [`RegionShape::CountyCode`]
/// combination is the one case that can return *two* routines: see
/// the module-level discussion of `nonroad.f` :308–322. `regions` is
/// consulted only for that case.
pub fn dispatch_for(region_code: &str, level: RegionLevel, regions: &RunRegions) -> Vec<Dispatch> {
    match (classify_region(region_code), level) {
        // nonroad.f :266–270 — '00000' at US-TOTAL ⇒ prcus.
        (RegionShape::National, RegionLevel::UsTotal) => vec![Dispatch::UsTotal],
        // nonroad.f :271–277 — '00000' at STATE/NATION ⇒ prcnat.
        (RegionShape::National, RegionLevel::Nation | RegionLevel::State) => {
            vec![Dispatch::National]
        }
        // nonroad.f :283–287 — state code at COUNTY ⇒ prcsta.
        (RegionShape::StateCode, RegionLevel::County) => vec![Dispatch::StateToCounty],
        // nonroad.f :288–292 — state code at STATE/NATION ⇒ prc1st.
        (RegionShape::StateCode, RegionLevel::Nation | RegionLevel::State) => {
            vec![Dispatch::StateFromNational]
        }
        // nonroad.f :298–304 — county code at COUNTY ⇒ prccty.
        (RegionShape::CountyCode, RegionLevel::County) => vec![Dispatch::County],
        // nonroad.f :308–322 — county code at SUBCOUNTY ⇒ region-list
        // driven (whole-county prccty and/or subcounty prcsub).
        (RegionShape::CountyCode, RegionLevel::Subcounty) => {
            subcounty_dispatch(region_code, regions)
        }
        // Every other shape/level pair matches no dispatch branch;
        // nonroad.f falls straight through to `goto 333`.
        _ => Vec::new(),
    }
}

/// `nonroad.f` :308–322 — resolve a county-code record at SUBCOUNTY
/// level against the region list.
///
/// Two independent `fndchr` lookups, faithful to the Fortran's
/// truncating-comparison lengths:
///
/// - the 10-character match (`nonroad.f` :309) — the region list
///   holds the county code padded with blanks — selects `prccty`;
/// - the 5-character match (`nonroad.f` :316) — any region-list entry
///   whose first five characters are the county code — selects
///   `prcsub`. Because a whole-county entry's first five characters
///   are also the county code, a county with *only* a whole-county
///   region entry dispatches to *both* routines; this matches the
///   Fortran's `ilen = 5` truncation exactly.
fn subcounty_dispatch(region_code: &str, regions: &RunRegions) -> Vec<Dispatch> {
    let code5 = region_code.get(..5).unwrap_or(region_code);
    let mut out = Vec::new();
    // nonroad.f :309 — whole-county region: an entry that is exactly
    // the 5-char county code (Fortran's code + blank padding).
    if regions.region_list.iter().any(|r| r.trim() == code5) {
        out.push(Dispatch::County);
    }
    // nonroad.f :316 — any entry whose first 5 characters are the
    // county code (Fortran's 5-char fndchr truncation).
    if regions
        .region_list
        .iter()
        .any(|r| r.get(..5).unwrap_or(r.as_str()) == code5)
    {
        out.push(Dispatch::Subcounty);
    }
    out
}

// ---------------------------------------------------------------------------
// Fuel classification
// ---------------------------------------------------------------------------

/// Classify a record's fuel from its SCC code — `nonroad.f` :240–260.
///
/// `nonroad.f` sets `ifuel` from fixed SCC prefixes: a 4-character
/// prefix or one of several 7-character prefixes. Returns `None` for
/// the Fortran `ifuel = 0` default (no prefix matched) — diesel,
/// gasoline, etc. all have explicit prefixes, so an unmatched SCC
/// genuinely has no fuel assigned.
pub fn fuel_for_scc(scc: &str) -> Option<FuelKind> {
    let p4 = scc.get(..4).unwrap_or(scc);
    let p7 = scc.get(..7).unwrap_or(scc);
    if p4 == "2260" || p7 == "2282005" || p7 == "2285003" {
        Some(FuelKind::Gasoline2Stroke)
    } else if p4 == "2265" || p7 == "2282010" || p7 == "2285004" {
        Some(FuelKind::Gasoline4Stroke)
    } else if p4 == "2268" || p7 == "2285008" {
        Some(FuelKind::Cng)
    } else if p4 == "2267" || p7 == "2285006" {
        Some(FuelKind::Lpg)
    } else if p4 == "2270" || p7 == "2280002" || p7 == "2282020" || p7 == "2285002" {
        Some(FuelKind::Diesel)
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// Population records and the inner record loop
// ---------------------------------------------------------------------------

/// One population record, reduced to the fields `nonroad.f`'s record
/// loop inspects.
///
/// The geography routines receive a fuller per-record input (the
/// `geography` module's `PopulationRecord`); the driver loop itself
/// only needs the region code, average HP, population, and population
/// year — those four drive growth-pair detection and region
/// classification.
#[derive(Debug, Clone, PartialEq)]
pub struct DriverRecord {
    /// Region code — Fortran `regncd(icurec)`. The driver inspects
    /// the first five characters (state + county) and characters 3–5
    /// (county).
    pub region_code: String,
    /// Average horsepower for the record — Fortran `avghpc(icurec)`.
    pub hp_avg: f32,
    /// Equipment population — Fortran `popeqp(icurec)`.
    pub population: f32,
    /// Population-input year — Fortran `ipopyr(icurec)`.
    pub pop_year: i32,
}

/// Detect a growth-record pair and return its growth rate —
/// `nonroad.f` :206–219.
///
/// Two consecutive population records form a growth pair when they
/// describe the same region and average HP but a different
/// population year, and the first record has a positive population.
/// The pair encodes a base-year population and a projection year; the
/// returned value is the per-year fractional growth rate
///
/// ```text
/// (pop_next - pop_current) / (pop_current * (year_next - year_current))
/// ```
///
/// When the records are a pair, the second is consumed as the growth
/// partner and the driver loop skips over it. `None` is the Fortran
/// `growth = -9` "no growth record" sentinel.
///
/// The average-HP test is an exact floating-point comparison; this is
/// deliberate and matches `nonroad.f` :210 (the source explicitly
/// notes the values come straight from the input file, so equal
/// inputs compare equal).
pub fn growth_pair(current: &DriverRecord, next: &DriverRecord) -> Option<f32> {
    if current.region_code == next.region_code
        && current.hp_avg == next.hp_avg
        && current.population > 0.0
        && current.pop_year != next.pop_year
    {
        let year_span = (next.pop_year - current.pop_year) as f32;
        Some((next.population - current.population) / (current.population * year_span))
    } else {
        None
    }
}

/// What the driver loop does with one population record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StepOutcome {
    /// The record's state or county is not in the run's selection;
    /// the record is skipped (`nonroad.f` :224–236, `goto 333`).
    NotSelected,
    /// The record is dispatched to the listed geography routines.
    /// Empty means its region shape and the run level match no
    /// dispatch branch; two entries is the SUBCOUNTY whole-county +
    /// subcounty case (see [`dispatch_for`]).
    Dispatched(Vec<Dispatch>),
}

/// One iteration of `nonroad.f`'s inner record loop (label `333`).
#[derive(Debug, Clone, PartialEq)]
pub struct DriverStep {
    /// 0-based index into the SCC group's records of the record this
    /// iteration processed (Fortran `icurec`, 1-based, minus one).
    pub record_index: usize,
    /// Growth rate when this record paired with the *next* one as a
    /// growth record (see [`growth_pair`]). `Some` means the next
    /// record was consumed as the growth partner — the following
    /// loop iteration skips over it.
    pub growth: Option<f32>,
    /// What the driver does with the record.
    pub outcome: StepOutcome,
}

/// The result of planning one SCC group's record loop —
/// [`plan_scc_group`].
#[derive(Debug, Clone, PartialEq)]
pub struct SccGroupPlan {
    /// The fuel resolved from the group's SCC ([`fuel_for_scc`]);
    /// `None` is the Fortran `ifuel = 0` default.
    pub fuel: Option<FuelKind>,
    /// `true` when `nonroad.f`'s record-1 region pre-check
    /// (:165–177) rejected the whole SCC group before the record
    /// loop ran. When set, `steps` is empty.
    pub group_skipped: bool,
    /// One entry per inner-loop iteration, in order. Empty when
    /// `group_skipped` is set or the group has no records.
    pub steps: Vec<DriverStep>,
}

/// Plan one SCC group's inner record loop — `nonroad.f` label `333`.
///
/// `records` is one SCC's worth of population records, in file order,
/// as `getpop` would return them. `level` is the run's region level
/// and `regions` carries the selection / region-list tables.
///
/// The returned [`SccGroupPlan`] is the ordered list of dispatch
/// decisions the Fortran loop produces: the growth-record-pair
/// lookahead (which makes the loop skip the consumed partner), the
/// per-record region-selection filter, and the geography-routine
/// dispatch. It does **not** execute the routines or touch the
/// output writers — running each [`DriverStep`] against the geography
/// routines (and handling their `ISKIP`/error returns) is the Task
/// 117 integration step.
///
/// `nonroad.f`'s record-1 pre-check (:165–177) is reproduced: when the
/// first record's region is not selected the whole group is rejected
/// ([`SccGroupPlan::group_skipped`]). National first records are
/// never rejected — the pre-check, like the per-record check, only
/// filters state and county codes.
pub fn plan_scc_group(
    scc: &str,
    records: &[DriverRecord],
    level: RegionLevel,
    regions: &RunRegions,
) -> SccGroupPlan {
    let fuel = fuel_for_scc(scc);

    // nonroad.f :165–177 — record-1 region pre-check. A non-selected
    // first record skips the whole SCC group.
    if let Some(first) = records.first() {
        if !region_selected(&first.region_code, regions) {
            return SccGroupPlan {
                fuel,
                group_skipped: true,
                steps: Vec::new(),
            };
        }
    }

    // nonroad.f label 333 — the inner record loop.
    //
    // `icurec` is the Fortran 1-based record cursor; `lskip` carries
    // over from the previous iteration's growth-pair detection and,
    // when set, makes this iteration skip the consumed growth partner
    // (the extra `icurec += 1` at :187–189).
    let n = records.len();
    let mut steps = Vec::new();
    let mut icurec: usize = 0;
    let mut lskip = false;

    loop {
        if lskip {
            icurec += 1; // nonroad.f :187–189 — skip the growth partner.
        }
        icurec += 1; // nonroad.f :193
        if icurec > n {
            break; // nonroad.f :194 — past the last record.
        }
        let idx = icurec - 1; // 0-based index of this iteration's record.

        // nonroad.f :206–219 — growth-record-pair lookahead. Runs
        // before the selection check, so a record that later fails
        // selection still arms `lskip` for its growth partner.
        let mut growth = None;
        lskip = false;
        if icurec < n {
            if let Some(rate) = growth_pair(&records[idx], &records[idx + 1]) {
                growth = Some(rate);
                lskip = true;
            }
        }

        // nonroad.f :224–236 — per-record region selection, then
        // :265–323 — the geography dispatch.
        let outcome = if region_selected(&records[idx].region_code, regions) {
            StepOutcome::Dispatched(dispatch_for(&records[idx].region_code, level, regions))
        } else {
            StepOutcome::NotSelected
        };

        steps.push(DriverStep {
            record_index: idx,
            growth,
            outcome,
        });
    }

    SccGroupPlan {
        fuel,
        group_skipped: false,
        steps,
    }
}

/// `nonroad.f` :224–236 — is a record's region selected for the run?
///
/// National records (`"00000"`) are never filtered — the Fortran
/// guards the whole check with `if regncd != '00000'`. State and
/// county codes must appear in the run's selection list.
fn region_selected(region_code: &str, regions: &RunRegions) -> bool {
    match classify_region(region_code) {
        RegionShape::National => true,
        RegionShape::StateCode => {
            let code5 = region_code.get(..5).unwrap_or(region_code);
            regions.selected_states.iter().any(|s| s == code5)
        }
        RegionShape::CountyCode => {
            let code5 = region_code.get(..5).unwrap_or(region_code);
            regions.selected_counties.iter().any(|c| c == code5)
        }
    }
}

// ---------------------------------------------------------------------------
// Completion banner
// ---------------------------------------------------------------------------

/// Build the run-completion banner — `nonroad.f` :346–356.
///
/// With no warnings the Fortran writes a single "Successful
/// completion" line; with warnings it writes a "Completion" line plus
/// a second line giving the warning count. The two-space gap in
/// `"warnings.  Review"` is reproduced from the Fortran format string.
pub fn completion_message(warning_count: i32) -> String {
    if warning_count == 0 {
        format!("Successful completion of {PROGRAM_NAME}, {VERSION}")
    } else {
        format!(
            "Completion of {PROGRAM_NAME}, {VERSION}\n\
             There were {warning_count} warnings.  Review message file."
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(region: &str, hp: f32, pop: f32, year: i32) -> DriverRecord {
        DriverRecord {
            region_code: region.to_string(),
            hp_avg: hp,
            population: pop,
            pop_year: year,
        }
    }

    // ---- RegionLevel::from_reglvl ----

    #[test]
    fn region_level_parses_fortran_strings() {
        assert_eq!(
            RegionLevel::from_reglvl("US TOTAL  "),
            Some(RegionLevel::UsTotal)
        );
        assert_eq!(
            RegionLevel::from_reglvl("50STATE   "),
            Some(RegionLevel::Nation)
        );
        assert_eq!(RegionLevel::from_reglvl("STATE"), Some(RegionLevel::State));
        assert_eq!(
            RegionLevel::from_reglvl("county"),
            Some(RegionLevel::County)
        );
        assert_eq!(
            RegionLevel::from_reglvl(" SubCounty "),
            Some(RegionLevel::Subcounty)
        );
        assert_eq!(RegionLevel::from_reglvl("ZONE"), None);
    }

    // ---- classify_region ----

    #[test]
    fn classify_region_national() {
        assert_eq!(classify_region("00000"), RegionShape::National);
        assert_eq!(classify_region("0000012345"), RegionShape::National);
    }

    #[test]
    fn classify_region_state_code() {
        // chars 3-5 == "000" and not all-zero.
        assert_eq!(classify_region("06000"), RegionShape::StateCode);
        assert_eq!(classify_region("17000"), RegionShape::StateCode);
    }

    #[test]
    fn classify_region_county_code() {
        assert_eq!(classify_region("06037"), RegionShape::CountyCode);
        assert_eq!(classify_region("17031"), RegionShape::CountyCode);
    }

    #[test]
    fn classify_region_short_code_defaults_to_county() {
        // Defensive: a code with no chars 3-5 cannot be a state code.
        assert_eq!(classify_region("06"), RegionShape::CountyCode);
        assert_eq!(classify_region(""), RegionShape::CountyCode);
    }

    // ---- fuel_for_scc ----

    #[test]
    fn fuel_from_four_digit_prefixes() {
        assert_eq!(fuel_for_scc("2260001010"), Some(FuelKind::Gasoline2Stroke));
        assert_eq!(fuel_for_scc("2265001010"), Some(FuelKind::Gasoline4Stroke));
        assert_eq!(fuel_for_scc("2268001010"), Some(FuelKind::Cng));
        assert_eq!(fuel_for_scc("2267001010"), Some(FuelKind::Lpg));
        assert_eq!(fuel_for_scc("2270001010"), Some(FuelKind::Diesel));
    }

    #[test]
    fn fuel_from_seven_digit_prefixes() {
        assert_eq!(fuel_for_scc("2282005000"), Some(FuelKind::Gasoline2Stroke));
        assert_eq!(fuel_for_scc("2285003000"), Some(FuelKind::Gasoline2Stroke));
        assert_eq!(fuel_for_scc("2282010000"), Some(FuelKind::Gasoline4Stroke));
        assert_eq!(fuel_for_scc("2285004000"), Some(FuelKind::Gasoline4Stroke));
        assert_eq!(fuel_for_scc("2285008000"), Some(FuelKind::Cng));
        assert_eq!(fuel_for_scc("2285006000"), Some(FuelKind::Lpg));
        assert_eq!(fuel_for_scc("2280002000"), Some(FuelKind::Diesel));
        assert_eq!(fuel_for_scc("2282020000"), Some(FuelKind::Diesel));
        assert_eq!(fuel_for_scc("2285002000"), Some(FuelKind::Diesel));
    }

    #[test]
    fn fuel_none_for_unmatched_scc() {
        assert_eq!(fuel_for_scc("2275001000"), None); // aircraft — no fuel prefix
        assert_eq!(fuel_for_scc("0000000000"), None);
        assert_eq!(fuel_for_scc(""), None);
    }

    #[test]
    fn fuel_seven_digit_does_not_collide_with_four_digit() {
        // 2282xxx splits three ways by the 7-digit prefix even though
        // all share the "2282" 4-digit prefix (which matches nothing).
        assert_eq!(fuel_for_scc("2282005000"), Some(FuelKind::Gasoline2Stroke));
        assert_eq!(fuel_for_scc("2282010000"), Some(FuelKind::Gasoline4Stroke));
        assert_eq!(fuel_for_scc("2282020000"), Some(FuelKind::Diesel));
        assert_eq!(fuel_for_scc("2282999000"), None); // unmatched 2282 subtype
    }

    // ---- growth_pair ----

    #[test]
    fn growth_pair_detected_for_matching_records() {
        // Same region + HP, population grows 100 → 120 over 2 years.
        let a = rec("06037", 25.0, 100.0, 2000);
        let b = rec("06037", 25.0, 120.0, 2002);
        let g = growth_pair(&a, &b).unwrap();
        // (120 - 100) / (100 * 2) = 0.1
        assert!((g - 0.1).abs() < 1e-6);
    }

    #[test]
    fn growth_pair_none_when_region_differs() {
        let a = rec("06037", 25.0, 100.0, 2000);
        let b = rec("06038", 25.0, 120.0, 2002);
        assert_eq!(growth_pair(&a, &b), None);
    }

    #[test]
    fn growth_pair_none_when_hp_differs() {
        let a = rec("06037", 25.0, 100.0, 2000);
        let b = rec("06037", 40.0, 120.0, 2002);
        assert_eq!(growth_pair(&a, &b), None);
    }

    #[test]
    fn growth_pair_none_when_same_year() {
        let a = rec("06037", 25.0, 100.0, 2000);
        let b = rec("06037", 25.0, 120.0, 2000);
        assert_eq!(growth_pair(&a, &b), None);
    }

    #[test]
    fn growth_pair_none_when_first_population_not_positive() {
        let a = rec("06037", 25.0, 0.0, 2000);
        let b = rec("06037", 25.0, 120.0, 2002);
        assert_eq!(growth_pair(&a, &b), None);
    }

    #[test]
    fn growth_pair_can_be_negative() {
        // A declining population gives a negative growth rate.
        let a = rec("06037", 25.0, 200.0, 2000);
        let b = rec("06037", 25.0, 100.0, 2005);
        let g = growth_pair(&a, &b).unwrap();
        // (100 - 200) / (200 * 5) = -0.1
        assert!((g + 0.1).abs() < 1e-6);
    }

    // ---- dispatch_for ----

    #[test]
    fn dispatch_national_record() {
        let r = RunRegions::default();
        assert_eq!(
            dispatch_for("00000", RegionLevel::UsTotal, &r),
            vec![Dispatch::UsTotal]
        );
        assert_eq!(
            dispatch_for("00000", RegionLevel::Nation, &r),
            vec![Dispatch::National]
        );
        assert_eq!(
            dispatch_for("00000", RegionLevel::State, &r),
            vec![Dispatch::National]
        );
        // No dispatch branch for a national record on a county run.
        assert!(dispatch_for("00000", RegionLevel::County, &r).is_empty());
        assert!(dispatch_for("00000", RegionLevel::Subcounty, &r).is_empty());
    }

    #[test]
    fn dispatch_state_code_record() {
        let r = RunRegions::default();
        assert_eq!(
            dispatch_for("06000", RegionLevel::County, &r),
            vec![Dispatch::StateToCounty]
        );
        assert_eq!(
            dispatch_for("06000", RegionLevel::Nation, &r),
            vec![Dispatch::StateFromNational]
        );
        assert_eq!(
            dispatch_for("06000", RegionLevel::State, &r),
            vec![Dispatch::StateFromNational]
        );
        assert!(dispatch_for("06000", RegionLevel::UsTotal, &r).is_empty());
        assert!(dispatch_for("06000", RegionLevel::Subcounty, &r).is_empty());
    }

    #[test]
    fn dispatch_county_code_at_county_level() {
        let r = RunRegions::default();
        assert_eq!(
            dispatch_for("06037", RegionLevel::County, &r),
            vec![Dispatch::County]
        );
        // County code with no matching level branch.
        assert!(dispatch_for("06037", RegionLevel::UsTotal, &r).is_empty());
        assert!(dispatch_for("06037", RegionLevel::Nation, &r).is_empty());
        assert!(dispatch_for("06037", RegionLevel::State, &r).is_empty());
    }

    #[test]
    fn dispatch_subcounty_whole_county_region() {
        // A whole-county region entry (bare 5-char code) triggers both
        // prccty (10-char match) and prcsub (5-char match) — see the
        // subcounty_dispatch docs.
        let r = RunRegions {
            region_list: vec!["06037".to_string()],
            ..Default::default()
        };
        assert_eq!(
            dispatch_for("06037", RegionLevel::Subcounty, &r),
            vec![Dispatch::County, Dispatch::Subcounty]
        );
    }

    #[test]
    fn dispatch_subcounty_subcounty_only_region() {
        // A subcounty entry (county code + marker) triggers prcsub
        // only — its trimmed form is not the bare county code.
        let r = RunRegions {
            region_list: vec!["06037DOWNT".to_string()],
            ..Default::default()
        };
        assert_eq!(
            dispatch_for("06037", RegionLevel::Subcounty, &r),
            vec![Dispatch::Subcounty]
        );
    }

    #[test]
    fn dispatch_subcounty_no_region_entry() {
        // A county absent from the region list dispatches to nothing.
        let r = RunRegions {
            region_list: vec!["48201DOWNT".to_string()],
            ..Default::default()
        };
        assert!(dispatch_for("06037", RegionLevel::Subcounty, &r).is_empty());
    }

    // ---- plan_scc_group ----

    fn counties(codes: &[&str]) -> RunRegions {
        RunRegions {
            selected_counties: codes.iter().map(|c| c.to_string()).collect(),
            ..Default::default()
        }
    }

    #[test]
    fn plan_simple_county_group() {
        let recs = vec![
            rec("06037", 25.0, 100.0, 2000),
            rec("06038", 25.0, 200.0, 2000),
        ];
        let regions = counties(&["06037", "06038"]);
        let plan = plan_scc_group("2270001010", &recs, RegionLevel::County, &regions);
        assert_eq!(plan.fuel, Some(FuelKind::Diesel));
        assert!(!plan.group_skipped);
        assert_eq!(plan.steps.len(), 2);
        assert_eq!(plan.steps[0].record_index, 0);
        assert_eq!(plan.steps[0].growth, None);
        assert_eq!(
            plan.steps[0].outcome,
            StepOutcome::Dispatched(vec![Dispatch::County])
        );
        assert_eq!(plan.steps[1].record_index, 1);
    }

    #[test]
    fn plan_skips_growth_partner_record() {
        // Records 0 and 1 are a growth pair (same region + HP, years
        // differ): the loop processes record 0, records the growth
        // rate, and the next iteration jumps over record 1 to record 2.
        let recs = vec![
            rec("06037", 25.0, 100.0, 2000),
            rec("06037", 25.0, 150.0, 2010),
            rec("06038", 25.0, 300.0, 2000),
        ];
        let regions = counties(&["06037", "06038"]);
        let plan = plan_scc_group("2270001010", &recs, RegionLevel::County, &regions);
        assert_eq!(plan.steps.len(), 2);
        // First step: record 0, with a growth rate.
        assert_eq!(plan.steps[0].record_index, 0);
        assert!(plan.steps[0].growth.is_some());
        // Second step jumps straight to record 2 — record 1 consumed.
        assert_eq!(plan.steps[1].record_index, 2);
        assert_eq!(plan.steps[1].growth, None);
    }

    #[test]
    fn plan_marks_unselected_records() {
        let recs = vec![
            rec("06037", 25.0, 100.0, 2000),
            rec("06099", 25.0, 200.0, 2000), // not selected
            rec("06038", 25.0, 300.0, 2000),
        ];
        // Only 06037 and 06038 are selected.
        let regions = counties(&["06037", "06038"]);
        let plan = plan_scc_group("2270001010", &recs, RegionLevel::County, &regions);
        assert_eq!(plan.steps.len(), 3);
        assert_eq!(plan.steps[1].outcome, StepOutcome::NotSelected);
        assert!(matches!(plan.steps[0].outcome, StepOutcome::Dispatched(_)));
    }

    #[test]
    fn plan_record_one_precheck_skips_whole_group() {
        // nonroad.f :165–177 — first record's county not selected ⇒
        // the entire SCC group is skipped before the loop runs.
        let recs = vec![
            rec("06099", 25.0, 100.0, 2000), // not selected
            rec("06037", 25.0, 200.0, 2000), // would be selected
        ];
        let regions = counties(&["06037"]);
        let plan = plan_scc_group("2270001010", &recs, RegionLevel::County, &regions);
        assert!(plan.group_skipped);
        assert!(plan.steps.is_empty());
        // Fuel is still classified even for a skipped group.
        assert_eq!(plan.fuel, Some(FuelKind::Diesel));
    }

    #[test]
    fn plan_national_first_record_passes_precheck() {
        // A national first record is never selection-filtered, so the
        // group is not skipped (nonroad.f guards the check with
        // `if regncd != '00000'`).
        let recs = vec![rec("00000", 25.0, 100.0, 2000)];
        let plan = plan_scc_group(
            "2270001010",
            &recs,
            RegionLevel::UsTotal,
            &RunRegions::default(),
        );
        assert!(!plan.group_skipped);
        assert_eq!(plan.steps.len(), 1);
        assert_eq!(
            plan.steps[0].outcome,
            StepOutcome::Dispatched(vec![Dispatch::UsTotal])
        );
    }

    #[test]
    fn plan_empty_group_has_no_steps() {
        let plan = plan_scc_group(
            "2270001010",
            &[],
            RegionLevel::County,
            &RunRegions::default(),
        );
        assert!(!plan.group_skipped);
        assert!(plan.steps.is_empty());
    }

    #[test]
    fn plan_growth_pair_still_arms_skip_when_record_unselected() {
        // The growth-pair lookahead runs before the selection check,
        // so even an unselected record consumes its growth partner.
        // Record 0 is selected (so the record-1 pre-check passes);
        // records 1 and 2 are an unselected growth pair; record 3 is
        // selected.
        let recs = vec![
            rec("06037", 25.0, 100.0, 2000),
            rec("06099", 25.0, 100.0, 2000), // unselected, pairs with rec 2
            rec("06099", 25.0, 150.0, 2010), // growth partner
            rec("06038", 25.0, 300.0, 2000),
        ];
        let regions = counties(&["06037", "06038"]); // 06099 not selected
        let plan = plan_scc_group("2270001010", &recs, RegionLevel::County, &regions);
        assert!(!plan.group_skipped);
        // Records 0, 1, 3 are visited; record 2 is consumed as the
        // growth partner of the (unselected) record 1.
        assert_eq!(plan.steps.len(), 3);
        assert_eq!(plan.steps[0].record_index, 0);
        assert_eq!(plan.steps[1].record_index, 1);
        assert_eq!(plan.steps[1].outcome, StepOutcome::NotSelected);
        assert!(plan.steps[1].growth.is_some());
        assert_eq!(plan.steps[2].record_index, 3);
    }

    #[test]
    fn plan_consecutive_growth_pairs() {
        // Four records forming two back-to-back growth pairs:
        // (0,1) and (2,3). The loop visits records 0 and 2 only.
        let recs = vec![
            rec("06037", 25.0, 100.0, 2000),
            rec("06037", 25.0, 110.0, 2005),
            rec("06038", 40.0, 200.0, 2000),
            rec("06038", 40.0, 240.0, 2008),
        ];
        let regions = counties(&["06037", "06038"]);
        let plan = plan_scc_group("2260001010", &recs, RegionLevel::County, &regions);
        assert_eq!(plan.steps.len(), 2);
        assert_eq!(plan.steps[0].record_index, 0);
        assert!(plan.steps[0].growth.is_some());
        assert_eq!(plan.steps[1].record_index, 2);
        assert!(plan.steps[1].growth.is_some());
    }

    // ---- completion_message ----

    #[test]
    fn completion_message_success() {
        let msg = completion_message(0);
        assert!(msg.starts_with("Successful completion of"));
        assert!(msg.contains(PROGRAM_NAME));
        assert!(msg.contains(VERSION));
        assert!(!msg.contains("warnings"));
    }

    #[test]
    fn completion_message_with_warnings() {
        let msg = completion_message(7);
        assert!(msg.starts_with("Completion of"));
        assert!(msg.contains("There were 7 warnings."));
        assert!(msg.contains("Review message file."));
        assert!(msg.contains(VERSION));
    }
}
