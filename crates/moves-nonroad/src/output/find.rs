//! Find/lookup utility routines (`fnd*.f`).
//!
//! Task 101. Ports NONROAD's family of linear-search-with-fallback
//! lookup helpers. The Fortran source has 16 `fnd*.f` files (~2,450
//! lines combined); each walks a Fortran COMMON-block array and
//! returns a 1-based index (0 = no match) or a similar sentinel.
//!
//! # Return convention
//!
//! Functions in this module return [`Option<usize>`] with `Some(i)`
//! being a 0-based index into the input slice and `None` indicating
//! no match. Callers translate to Fortran's 1-based sentinel when
//! emitting legacy output. Lookups that return values (rather than
//! indices) use [`Option<T>`] of the value type directly.
//!
//! # Hierarchy semantics
//!
//! Most NONROAD lookup helpers walk a "best match by precision" tree
//! across three SCC granularities (10-digit exact, 7-digit prefix
//! with trailing `000`, 4-digit prefix with trailing `000000`) and
//! across exact-versus-global region/tech codes. The Fortran code
//! tracks the "last winner" per tier and applies a fixed precedence
//! after the walk. The Rust ports preserve this semantics byte-for-byte;
//! deviations are flagged in each function's docs.
//!
//! # What's implemented
//!
//! | Routine | Rust function | Inputs available |
//! |---|---|---|
//! | `fndchr.f`  | [`find_string`]            | (utility — any `&[String]`) |
//! | `fndasc.f`  | [`find_scc_hierarchy`]     | (utility — any `&[String]`) |
//! | `fndhpc.f`  | [`find_hp_category`]       | (utility — uses [`HPCLEV`]) |
//! | `fndscrp.f` | [`find_scrappage_percent`] | [`input::scrappage::ScrappagePoint`](crate::input::scrappage::ScrappagePoint) |
//! | `fndreg.f`  | [`find_region`]            | [`input::region_def::RegionDefinitions`](crate::input::region_def::RegionDefinitions) |
//! | `fnddet.f`  | [`find_deterioration`]     | [`input::deterioration::DeteriorationRecord`](crate::input::deterioration::DeteriorationRecord) |
//! | `fndact.f`  | [`find_activity`]          | [`input::activity::ActivityRecord`](crate::input::activity::ActivityRecord) |
//! | `fndrfm.f`  | [`find_refueling_mode`]    | [`input::spillage::SpillageRecord`](crate::input::spillage::SpillageRecord) |
//!
//! # What's deferred
//!
//! Seven `fnd*.f` routines port the lookup logic over data tables
//! that are produced by parsers not yet ported in Phase 5:
//!
//! | Routine | Blocking task | Reason |
//! |---|---|---|
//! | `fndefc.f`    | Task 96 (`rdemfc.f` `.EMF` parser)            | Needs the EMF record type |
//! | `fndevefc.f`  | Task 96 (`rdevemfc.f` `.EVEMF` parser)        | Needs the evap EF record type |
//! | `fndtch.f`    | Task 96 (`rdtech.f` `.TCH` parser)            | Needs the tech-fraction record type |
//! | `fndevtch.f`  | Task 96 (`rdevtech.f` `.EVTCH` parser)        | Needs the evap tech-fraction record type |
//! | `fndrtrft.f`  | Task 98 (`rdrtrft.f` retrofit parser)         | Needs the retrofit record type and filter arrays |
//! | `fndgxf.f`    | Task 95 follow-up (`rdgxrf.f` cross-reference) | Current [`input::gxr`] parses a different `.GXR` layout (factor grid) than the cross-reference array (FIPS × SCC × HP × tech × indicator) that `fndgxf` queries |
//! | `fndtpm.f`    | Task 95 follow-up (`.DAT` temporal profiles)   | Current [`input::seasonal::SeasonalRecord`] stores monthly factors per equipment, not the `(SCC, subregion, monthly-profile-code, daily-profile-code)` lookup table fndtpm walks |
//!
//! `fndkey.f` searches a *file* (Fortran unit number) for a keyword
//! by reading lines. This has no Rust equivalent in this design:
//! per the I/O policy in `ARCHITECTURE.md` § 4.3, parsers consume
//! [`std::io::BufRead`] directly and handle their own
//! keyword/section dispatch inline. There is no shared lookup
//! helper to provide.
//!
//! Once the blocking input parsers land, follow-up tasks (in this
//! module) add the remaining lookups in the same shape as the eight
//! routines below.
//!
//! # Indexing
//!
//! Per the migration-plan task description, the long-term plan is
//! to replace many of these linear walks with `HashMap` / `BTreeMap`
//! indices. The current ports preserve the linear-walk semantics
//! one-for-one because:
//!
//! * Most lookups carry multi-criteria precedence (SCC hierarchy ×
//!   region × HP range × year) where a single `HashMap` key won't
//!   suffice and the precedence resolution still requires walking
//!   candidates.
//! * Fixture-table sizes are typically a few hundred to a few thousand
//!   records — linear walks complete in microseconds.
//! * The numerical-fidelity gate (Task 115) needs the port to behave
//!   identically to the Fortran source; an indexed lookup that picks
//!   a different "best" match on equal-precedence ties would be a
//!   silent divergence.
//!
//! Callers that want pre-indexing build a `HashMap<&str, usize>` over
//! the SCC/key field of their record slice and pass it alongside the
//! slice. The lookups in this module remain the canonical reference.

use crate::input::activity::ActivityRecord;
use crate::input::deterioration::DeteriorationRecord;
use crate::input::region_def::RegionDefinitions;
use crate::input::scrappage::ScrappagePoint;
use crate::input::spillage::{RangeIndicator, SpillageRecord};

/// Horsepower category boundaries, in HP.
///
/// Original Fortran: `data hpclev /1., 3., 6., 11., 16., 25., 40., 50.,
/// 75., 100., 175., 300., 600., 750., 1000., 1200., 2000., 3000./`
/// in `blknon.f`. 18 entries match
/// [`MXHPC`](crate::common::consts::MXHPC).
pub const HPCLEV: [f32; 18] = [
    1.0, 3.0, 6.0, 11.0, 16.0, 25.0, 40.0, 50.0, 75.0, 100.0, 175.0, 300.0, 600.0, 750.0, 1000.0,
    1200.0, 2000.0, 3000.0,
];

/// Sentinel value used by NONROAD to indicate "matches all tech types".
///
/// The Fortran parameter `TECDEF = 'ALL'` (in `nonrdprm.inc`) marks an
/// EF or activity record as applying when no exact tech-type match is
/// found. Several deferred lookups (`fndefc`, `fndevefc`) consult this;
/// it lives here so it has one canonical home for when those lookups
/// land.
pub const TECH_DEFAULT: &str = "ALL";

// ---------------------------------------------------------------------------
// SCC hierarchy helpers
// ---------------------------------------------------------------------------

/// Slice the first `n` characters of an SCC code, padding with `pad` if
/// the input is shorter. NONROAD SCCs are 10-character codes; the
/// hierarchy lookups truncate at fixed positions and pad with zeros.
fn scc_prefix(scc: &str, n: usize) -> &str {
    if scc.len() >= n {
        &scc[..n]
    } else {
        scc
    }
}

/// 7-digit SCC global form: `{first 7 chars}000`.
fn scc_global_7(scc: &str) -> String {
    let mut out = String::with_capacity(10);
    out.push_str(scc_prefix(scc, 7));
    while out.len() < 10 {
        out.push('0');
    }
    out
}

/// 4-digit SCC global form: `{first 4 chars}000000`.
fn scc_global_4(scc: &str) -> String {
    let mut out = String::with_capacity(10);
    out.push_str(scc_prefix(scc, 4));
    while out.len() < 10 {
        out.push('0');
    }
    out
}

// ---------------------------------------------------------------------------
// fndchr — pure string lookup
// ---------------------------------------------------------------------------

/// Find the first index of `needle` in `haystack` (exact string equality).
///
/// Ports `fndchr.f`: `function fndchr(string, ilen, array, nchr)`.
/// The Fortran routine truncates each candidate to `ilen` characters
/// before comparison; in Rust we compare full strings because all
/// callers in the NONROAD source pass `ilen` equal to the declared
/// length of the array elements (i.e. they compare full records).
///
/// Returns `Some(i)` for a 0-based index, `None` for no match. The
/// Fortran sentinel `0` corresponds to `None`; otherwise the
/// 0-based index is the Fortran index minus one.
pub fn find_string(needle: &str, haystack: &[String]) -> Option<usize> {
    haystack.iter().position(|s| s == needle)
}

// ---------------------------------------------------------------------------
// fndasc — SCC hierarchy match
// ---------------------------------------------------------------------------

/// SCC hierarchy match against `haystack`.
///
/// Ports `fndasc.f`. The algorithm walks `haystack` once:
///
/// 1. If an entry equals the input SCC exactly, return that index immediately.
/// 2. Otherwise record the *last* occurrence of the 7-digit global form
///    (`{first 7 chars}000`) and the *last* occurrence of the 4-digit
///    global form (`{first 4 chars}000000`).
/// 3. After the walk, prefer the 7-digit match; fall back to the 4-digit.
///
/// The "last occurrence" semantics matters: when multiple records share
/// the same global SCC, NONROAD picks the latest one, matching the
/// load order of the original `.POP`/`.ALO`/etc input file.
pub fn find_scc_hierarchy(needle: &str, haystack: &[String]) -> Option<usize> {
    let g7 = scc_global_7(needle);
    let g4 = scc_global_4(needle);
    let mut idx_7 = None;
    let mut idx_4 = None;
    for (i, code) in haystack.iter().enumerate() {
        if code == needle {
            return Some(i);
        }
        if code == &g7 {
            idx_7 = Some(i);
        }
        if code == &g4 {
            idx_4 = Some(i);
        }
    }
    idx_7.or(idx_4)
}

// ---------------------------------------------------------------------------
// fndhpc — HP category lookup
// ---------------------------------------------------------------------------

/// Find the index into [`HPCLEV`] matching `hp` exactly.
///
/// Ports `fndhpc.f`. NONROAD's source explicitly relies on
/// floating-point equality here because the HP values arrive from
/// input files and must be exactly one of `0`, `9999`, or one of
/// the [`HPCLEV`] entries (none of which are computed at runtime).
/// The Rust port preserves the exact `==` semantics for fidelity.
pub fn find_hp_category(hp: f32) -> Option<usize> {
    HPCLEV.iter().position(|&v| v == hp)
}

// ---------------------------------------------------------------------------
// fndscrp — scrappage curve lookup
// ---------------------------------------------------------------------------

/// Find the scrappage percentage for the given fraction-of-median-life-used.
///
/// Ports `fndscrp.f`. The Fortran walks `MXSCRP-1` entries comparing
/// `scrpbin(i+1) > frcmlusd`; when true, returns `scrppct(i)`. If the
/// fraction is below `scrpbin(1)`, returns `scrppct(1)`. If above the
/// last bin, returns `scrppct(MXSCRP)`.
///
/// The Rust port operates on the dynamic [`ScrappagePoint`] vector
/// produced by [`crate::input::scrappage::read_scrp`]. Returns `None`
/// only if the points slice is empty.
pub fn find_scrappage_percent(frac_life_used: f32, points: &[ScrappagePoint]) -> Option<f32> {
    let first = points.first()?;
    if frac_life_used < first.bin {
        return Some(first.percent);
    }
    for win in points.windows(2) {
        if win[1].bin > frac_life_used {
            return Some(win[0].percent);
        }
    }
    points.last().map(|p| p.percent)
}

// ---------------------------------------------------------------------------
// fndreg — region for FIPS code
// ---------------------------------------------------------------------------

/// Find the region code containing the given FIPS county code.
///
/// Ports `fndreg.f`. Precedence:
///
/// 1. Exact 5-digit FIPS entry in some region's state list.
/// 2. Otherwise, the *last* region containing a state-wildcard entry
///    (FIPS with `"000"` suffix, first 2 digits matching the input).
/// 3. Otherwise, the *last* region containing the national wildcard `"00000"`.
///
/// The Fortran source returns a 1-based region index that callers then
/// translate to a region code via `rgdfcd(idxreg)`. In the Rust port,
/// the [`RegionDefinitions`] table stores region codes directly; this
/// function returns the region code (`&str`) to skip one indirection.
pub fn find_region<'a>(fips: &str, defs: &'a RegionDefinitions) -> Option<&'a str> {
    let state_prefix = fips.get(..2).unwrap_or("");
    let mut idx_state: Option<&str> = None;
    let mut idx_national: Option<&str> = None;
    for region_code in &defs.region_order {
        let Some(states) = defs.regions.get(region_code) else {
            continue;
        };
        for entry in states {
            if entry == fips {
                return Some(region_code.as_str());
            }
            if entry.len() == 5
                && &entry[2..5] == "000"
                && !entry.starts_with("00")
                && &entry[..2] == state_prefix
            {
                idx_state = Some(region_code.as_str());
            }
            if entry == "00000" {
                idx_national = Some(region_code.as_str());
            }
        }
    }
    idx_state.or(idx_national)
}

// ---------------------------------------------------------------------------
// fnddet — deterioration factor lookup
// ---------------------------------------------------------------------------

/// Find the deterioration record matching the given tech type and pollutant.
///
/// Ports `fnddet.f`. The Fortran indexes deterioration arrays per
/// pollutant (`tecdet(i, idxpol)`); here records are a flat
/// [`Vec<DeteriorationRecord>`] keyed by `(tech_type, pollutant)`.
/// Comparison is case-insensitive against the upper-cased fields
/// produced by [`crate::input::deterioration::read_detr`].
pub fn find_deterioration(
    tech: &str,
    pollutant: &str,
    records: &[DeteriorationRecord],
) -> Option<usize> {
    let pollutant_upper = pollutant.to_ascii_uppercase();
    let tech_upper = tech.to_ascii_uppercase();
    records
        .iter()
        .position(|r| r.pollutant == pollutant_upper && r.tech_type == tech_upper)
}

// ---------------------------------------------------------------------------
// fndact — activity record lookup
// ---------------------------------------------------------------------------

/// Find the activity record matching SCC + region + HP.
///
/// Ports `fndact.f`. Precedence (highest first):
///
/// 1. exact SCC + matching region
/// 2. exact SCC + global region (`sub` blank in the source record)
/// 3. 7-digit SCC + matching region
/// 4. 7-digit SCC + global region
/// 5. 4-digit SCC + matching region
/// 6. 4-digit SCC + global region
/// 7. all-zero SCC + matching region
/// 8. all-zero SCC + global region
///
/// Region is computed from the FIPS code via [`find_region`]. The
/// Fortran "region match" compares against the 5-character `subact(i)`
/// field. An entry whose `sub` trims to empty is treated as the
/// global-region match; any other value must equal the region code
/// found from the FIPS code.
///
/// The Fortran source loops `i = MXGLB..1` (3 down to 1) so the
/// "more specific" global takes precedence — 7-digit beats 4-digit,
/// 4-digit beats all-zero. This module preserves that order.
pub fn find_activity(
    scc: &str,
    fips: &str,
    hp: f32,
    records: &[ActivityRecord],
    regions: &RegionDefinitions,
) -> Option<usize> {
    let g7 = scc_global_7(scc);
    let g4 = scc_global_4(scc);
    let g0 = "0000000000";
    let rgncd = find_region(fips, regions).unwrap_or("");

    let mut iexact: Option<usize> = None;
    let mut iexreg: Option<usize> = None;
    let mut idxglb_7: Option<usize> = None;
    let mut idxrgb_7: Option<usize> = None;
    let mut idxglb_4: Option<usize> = None;
    let mut idxrgb_4: Option<usize> = None;
    let mut idxglb_0: Option<usize> = None;
    let mut idxrgb_0: Option<usize> = None;

    for (i, rec) in records.iter().enumerate() {
        if hp < rec.hp_min || hp > rec.hp_max {
            continue;
        }
        let sub_trim = rec.sub.trim();
        let sub_blank = sub_trim.is_empty();
        let sub_matches = !sub_blank && !rgncd.is_empty() && sub_trim == rgncd;
        if rec.scc == scc {
            if sub_blank {
                iexact = Some(i);
            } else if sub_matches {
                iexreg = Some(i);
            }
        } else if rec.scc == g7 {
            if sub_blank {
                idxglb_7 = Some(i);
            } else if sub_matches {
                idxrgb_7 = Some(i);
            }
        } else if rec.scc == g4 {
            if sub_blank {
                idxglb_4 = Some(i);
            } else if sub_matches {
                idxrgb_4 = Some(i);
            }
        } else if rec.scc == g0 {
            if sub_blank {
                idxglb_0 = Some(i);
            } else if sub_matches {
                idxrgb_0 = Some(i);
            }
        }
    }

    iexreg
        .or(iexact)
        .or(idxrgb_7)
        .or(idxglb_7)
        .or(idxrgb_4)
        .or(idxglb_4)
        .or(idxrgb_0)
        .or(idxglb_0)
}

// ---------------------------------------------------------------------------
// fndrfm — refueling-mode spillage lookup
// ---------------------------------------------------------------------------

/// Find the refueling-mode spillage record matching SCC + HP + tech type.
///
/// Ports `fndrfm.f`. The Fortran walks the spillage table; for each
/// record it requires the tech-type to match exactly, the HP to fall
/// inside `[splpcb, splpce]`, and the SCC to match one of the three
/// hierarchy forms. The "best" match minimizes the SCC hierarchy
/// index (smaller = more specific), breaking ties by the HP-range
/// span:
///
/// ```text
///     idiff = max(int(hp - splpcb), int(splpce - hp))
/// ```
///
/// where a smaller `idiff` means the HP range is more tightly centered
/// around `hp`. Equality on both produces a stable "first-wins" pick.
///
/// Per the current upstream source, only [`RangeIndicator::Horsepower`]
/// records are considered (the tank-volume branch is commented out
/// in `fndrfm.f`). Tank records are silently skipped.
pub fn find_refueling_mode(
    scc: &str,
    hp: f32,
    tech: &str,
    records: &[SpillageRecord],
) -> Option<usize> {
    let tech_upper = tech.to_ascii_uppercase();
    let globals = [
        scc.to_string(),   // exact (0)
        scc_global_7(scc), // 7-digit (1)
        scc_global_4(scc), // 4-digit (2)
    ];

    let mut best: Option<usize> = None;
    let mut best_iasc: usize = usize::MAX;
    let mut best_idfhpc: i32 = i32::MAX;

    for (i, rec) in records.iter().enumerate() {
        if rec.tech_type.to_ascii_uppercase() != tech_upper {
            continue;
        }
        if !matches!(rec.indicator, RangeIndicator::Horsepower) {
            continue;
        }
        if hp < rec.hp_min || hp > rec.hp_max {
            continue;
        }
        let Some(idxasc) = globals.iter().position(|g| g == &rec.scc) else {
            continue;
        };
        let lo = (hp - rec.hp_min) as i32;
        let hi = (rec.hp_max - hp) as i32;
        let idiff = lo.max(hi);
        if idxasc < best_iasc {
            best = Some(i);
            best_iasc = idxasc;
            best_idfhpc = idiff;
        } else if idxasc == best_iasc && idiff < best_idfhpc {
            best = Some(i);
            best_idfhpc = idiff;
        }
    }
    best
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::activity::{ActivityRecord, ActivityUnits};
    use crate::input::deterioration::DeteriorationRecord;
    use crate::input::region_def::RegionDefinitions;
    use crate::input::scrappage::ScrappagePoint;
    use crate::input::spillage::{RangeIndicator, RefuelingMode, SpillageRecord, SpillageUnits};

    fn s(v: &str) -> String {
        v.to_string()
    }

    // ---- find_string ----

    #[test]
    fn find_string_matches_first_occurrence() {
        let haystack = vec![s("ALPHA"), s("BETA"), s("ALPHA")];
        assert_eq!(find_string("ALPHA", &haystack), Some(0));
        assert_eq!(find_string("BETA", &haystack), Some(1));
        assert_eq!(find_string("ZETA", &haystack), None);
    }

    #[test]
    fn find_string_empty_haystack_is_none() {
        let haystack: Vec<String> = vec![];
        assert_eq!(find_string("X", &haystack), None);
    }

    // ---- find_scc_hierarchy ----

    #[test]
    fn scc_hierarchy_exact_match_short_circuits() {
        let haystack = vec![
            s("2265001000"),
            s("2265001010"), // exact target
            s("2265000000"),
        ];
        assert_eq!(find_scc_hierarchy("2265001010", &haystack), Some(1));
    }

    #[test]
    fn scc_hierarchy_falls_back_to_7_digit() {
        let haystack = vec![
            s("2265000000"), // 4-digit global
            s("2265001000"), // 7-digit global
        ];
        assert_eq!(find_scc_hierarchy("2265001010", &haystack), Some(1));
    }

    #[test]
    fn scc_hierarchy_falls_back_to_4_digit_when_no_7() {
        let haystack = vec![s("2265000000"), s("2270001000")];
        assert_eq!(find_scc_hierarchy("2265001010", &haystack), Some(0));
    }

    #[test]
    fn scc_hierarchy_returns_last_occurrence_of_global() {
        // Fortran semantics: when multiple records share the same global
        // SCC form, the last one wins (Fortran "last write" sets the index).
        let haystack = vec![
            s("2265001000"), // first 7-digit
            s("2265001000"), // second 7-digit — should win
        ];
        assert_eq!(find_scc_hierarchy("2265001010", &haystack), Some(1));
    }

    #[test]
    fn scc_hierarchy_no_match() {
        let haystack = vec![s("9999999999")];
        assert_eq!(find_scc_hierarchy("2265001010", &haystack), None);
    }

    // ---- find_hp_category ----

    #[test]
    fn hp_category_exact_match() {
        assert_eq!(find_hp_category(1.0), Some(0));
        assert_eq!(find_hp_category(25.0), Some(5));
        assert_eq!(find_hp_category(3000.0), Some(17));
    }

    #[test]
    fn hp_category_no_match_for_non_canonical_value() {
        assert_eq!(find_hp_category(2.5), None);
        assert_eq!(find_hp_category(0.0), None);
        assert_eq!(find_hp_category(9999.0), None);
    }

    // ---- find_scrappage_percent ----

    fn sample_scrappage() -> Vec<ScrappagePoint> {
        vec![
            ScrappagePoint {
                bin: 25.0,
                percent: 10.0,
            },
            ScrappagePoint {
                bin: 50.0,
                percent: 40.0,
            },
            ScrappagePoint {
                bin: 75.0,
                percent: 75.0,
            },
            ScrappagePoint {
                bin: 100.0,
                percent: 100.0,
            },
        ]
    }

    #[test]
    fn scrappage_below_first_bin_returns_first_percent() {
        let pts = sample_scrappage();
        assert_eq!(find_scrappage_percent(10.0, &pts), Some(10.0));
    }

    #[test]
    fn scrappage_within_range_returns_lower_bin_percent() {
        let pts = sample_scrappage();
        // 60.0 falls between bin 50 (40%) and bin 75 (75%) → returns 40%
        assert_eq!(find_scrappage_percent(60.0, &pts), Some(40.0));
    }

    #[test]
    fn scrappage_at_first_bin_returns_first_percent() {
        let pts = sample_scrappage();
        // frac == first.bin: not < first.bin, so falls into the windowed
        // search. Bin 50 > 25, so returns percent at index 0 = 10%.
        assert_eq!(find_scrappage_percent(25.0, &pts), Some(10.0));
    }

    #[test]
    fn scrappage_above_last_bin_returns_last_percent() {
        let pts = sample_scrappage();
        assert_eq!(find_scrappage_percent(125.0, &pts), Some(100.0));
    }

    #[test]
    fn scrappage_empty_returns_none() {
        let pts: Vec<ScrappagePoint> = vec![];
        assert_eq!(find_scrappage_percent(50.0, &pts), None);
    }

    // ---- find_region ----

    fn sample_regions() -> RegionDefinitions {
        let mut defs = RegionDefinitions::default();
        defs.region_order.extend([
            "EAST".to_string(),
            "WEST".to_string(),
            "NATIONAL".to_string(),
        ]);
        defs.regions.insert(
            "EAST".to_string(),
            vec![s("17031"), s("18000")], // Cook County, IL; Indiana state-wide
        );
        defs.regions.insert(
            "WEST".to_string(),
            vec![s("06037"), s("06000")], // Los Angeles, CA; California state-wide
        );
        defs.regions
            .insert("NATIONAL".to_string(), vec![s("00000")]);
        defs
    }

    #[test]
    fn region_exact_county_match() {
        let defs = sample_regions();
        assert_eq!(find_region("17031", &defs), Some("EAST"));
        assert_eq!(find_region("06037", &defs), Some("WEST"));
    }

    #[test]
    fn region_state_wildcard_match() {
        let defs = sample_regions();
        // Indiana state-wide via "18000"
        assert_eq!(find_region("18045", &defs), Some("EAST"));
    }

    #[test]
    fn region_national_wildcard_match() {
        let defs = sample_regions();
        // Texas (not in any explicit region) falls through to NATIONAL
        assert_eq!(find_region("48201", &defs), Some("NATIONAL"));
    }

    #[test]
    fn region_no_match_returns_none() {
        let mut defs = RegionDefinitions::default();
        defs.region_order.push(s("EAST"));
        defs.regions.insert(s("EAST"), vec![s("17031")]);
        assert_eq!(find_region("48201", &defs), None);
    }

    // ---- find_deterioration ----

    fn sample_det() -> Vec<DeteriorationRecord> {
        vec![
            DeteriorationRecord {
                tech_type: s("T2"),
                a: 0.01,
                b: 1.0,
                cap: 1.5,
                pollutant: s("HC"),
            },
            DeteriorationRecord {
                tech_type: s("T2"),
                a: 0.02,
                b: 1.0,
                cap: 1.5,
                pollutant: s("CO"),
            },
            DeteriorationRecord {
                tech_type: s("T3"),
                a: 0.03,
                b: 1.0,
                cap: 1.5,
                pollutant: s("HC"),
            },
        ]
    }

    #[test]
    fn deterioration_matches_tech_and_pollutant() {
        let recs = sample_det();
        assert_eq!(find_deterioration("T2", "HC", &recs), Some(0));
        assert_eq!(find_deterioration("T2", "CO", &recs), Some(1));
        assert_eq!(find_deterioration("T3", "HC", &recs), Some(2));
    }

    #[test]
    fn deterioration_case_insensitive() {
        let recs = sample_det();
        assert_eq!(find_deterioration("t2", "hc", &recs), Some(0));
    }

    #[test]
    fn deterioration_no_match() {
        let recs = sample_det();
        assert_eq!(find_deterioration("T3", "CO", &recs), None);
    }

    // ---- find_activity ----

    fn act_rec(scc: &str, sub: &str, hp_lo: f32, hp_hi: f32) -> ActivityRecord {
        ActivityRecord {
            scc: s(scc),
            sub: s(sub),
            hp_min: hp_lo,
            hp_max: hp_hi,
            load_factor: 0.5,
            units: ActivityUnits::HoursPerYear,
            activity_level: 1.0,
            age_curve_id: s("DEFAULT"),
        }
    }

    fn east_only_region_defs() -> RegionDefinitions {
        let mut defs = RegionDefinitions::default();
        defs.region_order.push(s("EAST"));
        defs.regions.insert(s("EAST"), vec![s("17031")]);
        defs
    }

    #[test]
    fn activity_exact_scc_exact_region_wins() {
        let defs = east_only_region_defs();
        let recs = vec![
            act_rec("2265001010", "", 0.0, 50.0),     // exact + global
            act_rec("2265001010", "EAST", 0.0, 50.0), // exact + region
            act_rec("2265001000", "EAST", 0.0, 50.0), // 7-digit + region
        ];
        assert_eq!(
            find_activity("2265001010", "17031", 25.0, &recs, &defs),
            Some(1)
        );
    }

    #[test]
    fn activity_exact_scc_global_region_beats_7digit() {
        let defs = east_only_region_defs();
        let recs = vec![
            act_rec("2265001010", "", 0.0, 50.0),     // exact + global
            act_rec("2265001000", "EAST", 0.0, 50.0), // 7-digit + region
        ];
        // No exact+region available; the next tier is exact+global.
        assert_eq!(
            find_activity("2265001010", "17031", 25.0, &recs, &defs),
            Some(0)
        );
    }

    #[test]
    fn activity_7digit_beats_4digit() {
        let defs = east_only_region_defs();
        let recs = vec![
            act_rec("2265000000", "", 0.0, 50.0), // 4-digit + global
            act_rec("2265001000", "", 0.0, 50.0), // 7-digit + global
        ];
        assert_eq!(
            find_activity("2265001010", "17031", 25.0, &recs, &defs),
            Some(1)
        );
    }

    #[test]
    fn activity_hp_range_filter() {
        let defs = east_only_region_defs();
        let recs = vec![
            act_rec("2265001010", "EAST", 0.0, 10.0), // hp_max too low
            act_rec("2265001010", "EAST", 11.0, 50.0),
        ];
        assert_eq!(
            find_activity("2265001010", "17031", 25.0, &recs, &defs),
            Some(1)
        );
    }

    #[test]
    fn activity_no_match_returns_none() {
        let defs = east_only_region_defs();
        let recs = vec![act_rec("3333333333", "", 0.0, 50.0)];
        assert_eq!(
            find_activity("2265001010", "17031", 25.0, &recs, &defs),
            None
        );
    }

    #[test]
    fn activity_falls_through_all_zero_match() {
        let defs = east_only_region_defs();
        let recs = vec![
            act_rec("0000000000", "", 0.0, 100.0), // catch-all
        ];
        assert_eq!(
            find_activity("2265001010", "17031", 25.0, &recs, &defs),
            Some(0)
        );
    }

    // ---- find_refueling_mode ----

    fn spill_rec(
        scc: &str,
        tech: &str,
        ind: RangeIndicator,
        hp_lo: f32,
        hp_hi: f32,
    ) -> SpillageRecord {
        SpillageRecord {
            scc: s(scc),
            mode: RefuelingMode::Pump,
            indicator: ind,
            hp_min: hp_lo,
            hp_max: hp_hi,
            tech_type: s(tech),
            units: SpillageUnits::Gallons,
            tank_volume: 1.0,
            tank_full: 1.0,
            tank_metal_pct: 0.0,
            hose_len: 0.0,
            hose_dia: 0.0,
            hose_metal_pct: 0.0,
            neck_len: 0.0,
            neck_dia: 0.0,
            sr_len: 0.0,
            sr_dia: 0.0,
            vent_len: 0.0,
            vent_dia: 0.0,
            hot_soak_per_hr: 0.0,
            diurnal: [0.0; 5],
            tank_e10: 1.0,
            hose_e10: 1.0,
            neck_e10: 1.0,
            sr_e10: 1.0,
            vent_e10: 1.0,
        }
    }

    #[test]
    fn refueling_skips_wrong_tech() {
        let recs = vec![spill_rec(
            "2265001010",
            "T1",
            RangeIndicator::Horsepower,
            0.0,
            50.0,
        )];
        assert_eq!(find_refueling_mode("2265001010", 25.0, "T2", &recs), None);
    }

    #[test]
    fn refueling_skips_tank_indicator() {
        let recs = vec![spill_rec(
            "2265001010",
            "T1",
            RangeIndicator::Tank,
            0.0,
            50.0,
        )];
        assert_eq!(find_refueling_mode("2265001010", 25.0, "T1", &recs), None);
    }

    #[test]
    fn refueling_skips_out_of_hp_range() {
        let recs = vec![spill_rec(
            "2265001010",
            "T1",
            RangeIndicator::Horsepower,
            50.0,
            100.0,
        )];
        assert_eq!(find_refueling_mode("2265001010", 25.0, "T1", &recs), None);
    }

    #[test]
    fn refueling_prefers_exact_scc() {
        let recs = vec![
            spill_rec("2265000000", "T1", RangeIndicator::Horsepower, 0.0, 50.0), // 4-digit global
            spill_rec("2265001010", "T1", RangeIndicator::Horsepower, 0.0, 50.0), // exact
        ];
        assert_eq!(
            find_refueling_mode("2265001010", 25.0, "T1", &recs),
            Some(1)
        );
    }

    #[test]
    fn refueling_prefers_tighter_hp_range_on_tied_scc() {
        let recs = vec![
            spill_rec("2265001010", "T1", RangeIndicator::Horsepower, 0.0, 100.0), // wider
            spill_rec("2265001010", "T1", RangeIndicator::Horsepower, 20.0, 30.0), // tighter
        ];
        assert_eq!(
            find_refueling_mode("2265001010", 25.0, "T1", &recs),
            Some(1)
        );
    }

    #[test]
    fn refueling_falls_back_to_7digit_global() {
        let recs = vec![
            spill_rec("2265000000", "T1", RangeIndicator::Horsepower, 0.0, 50.0), // 4-digit
            spill_rec("2265001000", "T1", RangeIndicator::Horsepower, 0.0, 50.0), // 7-digit
        ];
        assert_eq!(
            find_refueling_mode("2265001010", 25.0, "T1", &recs),
            Some(1)
        );
    }

    #[test]
    fn refueling_case_insensitive_tech() {
        let recs = vec![spill_rec(
            "2265001010",
            "t1",
            RangeIndicator::Horsepower,
            0.0,
            50.0,
        )];
        assert_eq!(
            find_refueling_mode("2265001010", 25.0, "T1", &recs),
            Some(0)
        );
    }

    // ---- helpers ----

    #[test]
    fn scc_global_forms_pad_correctly() {
        assert_eq!(scc_global_7("2265001010"), "2265001000");
        assert_eq!(scc_global_4("2265001010"), "2265000000");
        // Short input gets padded out to 10 chars.
        assert_eq!(scc_global_7("226"), "2260000000");
        assert_eq!(scc_global_4("22"), "2200000000");
    }
}
