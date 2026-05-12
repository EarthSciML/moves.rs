//! Retrofit input parser (`rdrtrft.f`) and field/record validators.
//!
//! Task 98. Parses an `.RTR` retrofit input file describing
//! voluntary retrofit programs that reduce emissions for specific
//! engine populations. Each record cross-references several other
//! NONROAD inputs:
//!
//! - SCC against the equipment-code list (`eqpcod`/`lascat`),
//! - tech type against the THC deterioration table (`tecdet`),
//! - HP against the standard horsepower categories (`hpclev`),
//! - pollutant against the four valid retrofit pollutants
//!   (HC/CO/NOX/PM, see [`crate::population::retrofit::RetrofitPollutant`]).
//!
//! # Format
//!
//! Records live inside a `/RETROFIT/` packet, terminated by `/END/`.
//! Each record is one fixed-width line:
//!
//! ```text
//! /RETROFIT/
//! RYst RYen MYst MYen SCC        TechType   HPmn HPmx AnnualFracOrN      Effect Pollutant  RetID
//! ---- ---- ---- ---- ---------- ---------- ---------- ------------------ ------ ---------- -----
//! 2008 2009 1996 1997 2270002000 ALL           50  300               0.05   0.50 PM             1
//! /END/
//! ```
//!
//! Column layout (1-based, inclusive — matches `rdrtrft.f` lines
//! 184–223):
//!
//! | Cols    | Field                                         |
//! |---------|-----------------------------------------------|
//! | 1–4     | Retrofit year start (`I4`)                    |
//! | 6–9     | Retrofit year end   (`I4`)                    |
//! | 11–14   | Model year start    (`I4`)                    |
//! | 16–19   | Model year end      (`I4`)                    |
//! | 21–30   | SCC code (10 chars)                           |
//! | 32–41   | Tech type (10 chars; `ALL` is a wildcard)     |
//! | 43–47   | Minimum HP (`F5.0`, non-inclusive)            |
//! | 48–52   | Maximum HP (`F5.0`, inclusive)                |
//! | 54–71   | Annual fraction OR count retrofitted (`F18.0`)|
//! | 73–78   | Effectiveness (`F6.0`, range 0.0–1.0)         |
//! | 80–89   | Pollutant code (10 chars; HC/CO/NOX/PM)       |
//! | 91–95   | Retrofit ID (`I5`, > 0)                       |
//! | 96+     | Free-form description (ignored)               |
//!
//! # Skip-filtering and warnings
//!
//! Field-validation failures are returned as fatal [`Error::Parse`].
//! Three conditions instead drop the record with a warning:
//!
//! 1. evaluation year < retrofit year start;
//! 2. evaluation year < model year start;
//! 3. SCC is not `ALL` and is not requested for this run
//!    (`chkasc` with `skipunreq=.TRUE.`).
//!
//! Surviving records carrying `annual_frac_or_count > 1.0` (i.e. a
//! count rather than a fraction) raise an `n_units` warning when
//! they affect more than one engine — multiple retrofit/model years,
//! a wildcard SCC, a 4-/7-digit-global SCC, a wildcard tech type, or
//! more than one HP category.
//!
//! # Recordset validation
//!
//! [`validate_retrofit_recordset`] ports `vldrtrftrecs.f` — the
//! cross-record consistency check that runs after parsing. It groups
//! records by retrofit ID and ensures that every pair of records
//! within a group whose engine sets overlap shares the same retrofit
//! year range, model year range, and (per-pollutant) effectiveness;
//! and that the sum of `annual_frac_or_count` matches across
//! pollutants within an ID (within 0.0049). Returns errors via
//! [`Error::Parse`] and warnings via the result.
//!
//! # Fortran source
//!
//! Ports `rdrtrft.f` (710 lines), plus the validators
//! `vldrtrftrecs.f` (432 lines), `vldrtrfthp.f`, `vldrtrftscc.f`,
//! and `vldrtrfttchtyp.f`. The supporting helper `cnthpcat.f` is
//! ported here as [`count_hp_categories`] because the `rdrtrft.f`
//! N-units warning depends on it; the broader Task 102
//! string-utility cluster will re-export it once it lands.

use std::io::BufRead;
use std::path::{Path, PathBuf};

use crate::common::consts::MXRTRFT;
use crate::population::retrofit::{
    engine_overlap, sort_retrofits, Comparison, RetrofitPollutant, RetrofitRecord, RTRFTSCC_ALL,
    RTRFTTECHTYPE_ALL,
};
use crate::{Error, Result};

/// Inclusive range of allowed retrofit calendar years.
///
/// Mirrors `MINRTRFTYEAR..=MAXRTRFTYEAR` in `nonrdrtrft.inc`, which
/// alias `MINYEAR..=MAXYEAR` from `nonrdprm.inc` (1970..=2060).
pub const RETROFIT_YEAR_RANGE: std::ops::RangeInclusive<i32> = 1970..=2060;

/// Inclusive range of allowed retrofit model years.
///
/// Mirrors `MINRTRFTMDLYEAR..=MAXRTRFTMDLYEAR` in `nonrdrtrft.inc`
/// (1900..=`MAXYEAR`).
pub const RETROFIT_MODEL_YEAR_RANGE: std::ops::RangeInclusive<i32> = 1900..=2060;

/// Standard horsepower category boundaries (`hpclev` in
/// `nonrdeqp.inc`, initialised in `blknon.f`).
///
/// 18 values matching `MXHPC = 18`. The retrofit input parser uses
/// these via [`validate_retrofit_hp`] and [`count_hp_categories`].
pub const HP_LEVELS: [f32; 18] = [
    1.0, 3.0, 6.0, 11.0, 16.0, 25.0, 40.0, 50.0, 75.0, 100.0, 175.0, 300.0, 600.0, 750.0, 1000.0,
    1200.0, 2000.0, 3000.0,
];

/// Tolerance for the per-pollutant fraction-retrofitted equality
/// check in [`validate_retrofit_recordset`].
///
/// Matches the literal `0.0049` in `vldrtrftrecs.f` :273.
pub const FRAC_RETRO_TOLERANCE: f32 = 0.0049;

/// Run-time context required by [`read_retrofit`] for cross-input
/// validation and skip-filtering.
///
/// Mirrors the COMMON-block state read by `rdrtrft.f`: the current
/// evaluation year (`iepyr` from `nonrdusr.inc`), the equipment-code
/// list (`eqpcod`/`lascat` from `nonrdeqp.inc`), and the THC
/// deterioration tech-type list (`tecdet` from `nonrdtch.inc`).
#[derive(Debug, Clone)]
pub struct RetrofitContext<'a> {
    /// Evaluation year. Records that wouldn't take effect until a
    /// later year are skipped (`rdrtrft.f` :351–366).
    pub eval_year: i32,
    /// Equipment codes loaded for the run, paired with whether each
    /// is requested (`eqpcod`/`lascat`). Used by both the SCC
    /// validator (`vldrtrftscc.f`, ignores the `requested` flag) and
    /// the skip filter (`chkasc` with `skipunreq=.TRUE.`, requires
    /// the flag).
    pub equipment_codes: &'a [(String, bool)],
    /// Tech types loaded into the THC deterioration table
    /// (`fnddet(IDXTHC, ...)`).
    pub valid_tech_types: &'a [String],
}

/// Outcome of parsing a retrofit input file.
#[derive(Debug, Clone, Default)]
pub struct RetrofitParseResult {
    /// Records that survived field validation and skip-filtering.
    pub records: Vec<RetrofitRecord>,
    /// One entry per record dropped by the skip filter.
    pub skip_warnings: Vec<RetrofitSkipWarning>,
    /// One entry per surviving record that retrofits an absolute
    /// count (`annual_frac_or_count > 1`) yet affects more than one
    /// engine.
    pub n_units_warnings: Vec<RetrofitNUnitsWarning>,
    /// True when the packet contained no records at all (matches the
    /// "no retrofit records read" warning in `rdrtrft.f` :438).
    pub no_records: bool,
}

/// One skip-filter warning recorded by [`read_retrofit`].
#[derive(Debug, Clone, PartialEq)]
pub struct RetrofitSkipWarning {
    /// 1-based input record number.
    pub record_number: usize,
    /// Human-readable reason for skipping.
    pub reason: String,
}

/// One "N units" warning recorded by [`read_retrofit`].
#[derive(Debug, Clone, PartialEq)]
pub struct RetrofitNUnitsWarning {
    /// 1-based input record number.
    pub record_number: usize,
}

/// One non-fatal warning produced by [`validate_retrofit_recordset`].
#[derive(Debug, Clone, PartialEq)]
pub struct RetrofitRecordsetWarning {
    /// Retrofit ID where the duplicate was observed.
    pub retrofit_id: i32,
    /// Pollutant that appeared more than once.
    pub pollutant: String,
}

/// `vldrtrfthp.f` — is `hp` valid for a retrofit specification?
///
/// Valid values are the 18 boundaries in [`HP_LEVELS`], plus the
/// sentinels `0` (used for the minimum HP of a 0..=1 range) and
/// `9999` (used for the maximum HP of an above-3000 range).
pub fn validate_retrofit_hp(hp: f32) -> bool {
    if hp == 0.0 || hp == 9999.0 {
        return true;
    }
    HP_LEVELS.contains(&hp)
}

/// `vldrtrftscc.f` — is `scc` valid for a retrofit specification?
///
/// True if `scc` is the [`RTRFTSCC_ALL`] wildcard, or if it matches
/// any code in the equipment-code list (with the same exact /
/// 4-digit-global / 7-digit-global semantics as `chkasc.f`,
/// `skipunreq=.FALSE.`).
pub fn validate_retrofit_scc(scc: &str, equipment_codes: &[(String, bool)]) -> bool {
    if scc == RTRFTSCC_ALL {
        return true;
    }
    chkasc(scc, equipment_codes, false)
}

/// `vldrtrfttchtyp.f` — is `tech_type` valid for a retrofit
/// specification?
///
/// True if `tech_type` is the [`RTRFTTECHTYPE_ALL`] wildcard, or if
/// it appears in `valid_tech_types` (which the caller fills from the
/// THC deterioration table — `tecdet` indexed by `IDXTHC`).
pub fn validate_retrofit_tech_type(tech_type: &str, valid_tech_types: &[String]) -> bool {
    if tech_type == RTRFTTECHTYPE_ALL {
        return true;
    }
    valid_tech_types
        .iter()
        .any(|known| known.eq_ignore_ascii_case(tech_type))
}

/// `cnthpcat.f` — number of HP categories spanned by `[hp_min, hp_max]`.
///
/// Returns 0 when either endpoint isn't one of the recognised values
/// (sentinels `0`/`9999` or one of the [`HP_LEVELS`] boundaries).
/// Otherwise returns `index(hp_max) - index(hp_min)`, where the
/// sentinel `0` maps to category index `0` and `9999` to `MXHPC + 1`.
pub fn count_hp_categories(hp_min: f32, hp_max: f32) -> i32 {
    fn category_index(hp: f32) -> Option<i32> {
        if hp == 0.0 {
            Some(0)
        } else if hp == 9999.0 {
            Some(HP_LEVELS.len() as i32 + 1)
        } else {
            HP_LEVELS
                .iter()
                .position(|&level| level == hp)
                .map(|i| i as i32 + 1)
        }
    }

    match (category_index(hp_min), category_index(hp_max)) {
        (Some(lo), Some(hi)) => hi - lo,
        _ => 0,
    }
}

/// Parse a retrofit input file (`rdrtrft.f`).
pub fn read_retrofit<R: BufRead>(reader: R, ctx: &RetrofitContext) -> Result<RetrofitParseResult> {
    let path = PathBuf::from(".RTR");
    let mut result = RetrofitParseResult::default();

    let mut iter = reader.lines().enumerate().map(|(idx, res)| {
        res.map(|line| (idx + 1, line)).map_err(|e| Error::Io {
            path: path.clone(),
            source: e,
        })
    });

    let mut last_line_num: usize = 0;
    let mut in_packet = false;
    for next in iter.by_ref() {
        let (line_num, line) = next?;
        last_line_num = line_num;
        if is_keyword(&line, "/RETROFIT/") {
            in_packet = true;
            break;
        }
    }
    if !in_packet {
        return Err(Error::Parse {
            file: path,
            line: last_line_num,
            message: "missing /RETROFIT/ packet marker".to_string(),
        });
    }

    let mut record_number: usize = 0;
    let mut found_end = false;
    for next in iter.by_ref() {
        let (line_num, line) = next?;
        last_line_num = line_num;

        if is_keyword(&line, "/END/") {
            found_end = true;
            break;
        }

        record_number += 1;
        if record_number > MXRTRFT {
            return Err(Error::Parse {
                file: path,
                line: line_num,
                message: format!("maximum retrofit record count of {MXRTRFT} exceeded"),
            });
        }

        let parsed = parse_record(&line, line_num, &path)?;

        // --- field-level validation (matching the goto-7005..7014 path) ---
        validate_year(
            parsed.ryst,
            "retrofit year start",
            &RETROFIT_YEAR_RANGE,
            line_num,
            &path,
        )?;
        validate_year(
            parsed.ryen,
            "retrofit year end",
            &RETROFIT_YEAR_RANGE,
            line_num,
            &path,
        )?;
        if parsed.ryst > parsed.ryen {
            return Err(parse_err(
                &path,
                line_num,
                format!(
                    "retrofit year start ({}) must be <= retrofit year end ({})",
                    parsed.ryst, parsed.ryen
                ),
            ));
        }
        validate_year(
            parsed.myst,
            "model year start",
            &RETROFIT_MODEL_YEAR_RANGE,
            line_num,
            &path,
        )?;
        validate_year(
            parsed.myen,
            "model year end",
            &RETROFIT_MODEL_YEAR_RANGE,
            line_num,
            &path,
        )?;
        if parsed.myst > parsed.myen {
            return Err(parse_err(
                &path,
                line_num,
                format!(
                    "model year start ({}) must be <= model year end ({})",
                    parsed.myst, parsed.myen
                ),
            ));
        }
        if !validate_retrofit_scc(&parsed.scc, ctx.equipment_codes) {
            return Err(parse_err(
                &path,
                line_num,
                format!("invalid SCC value {:?}: unknown SCC", parsed.scc),
            ));
        }
        if !validate_retrofit_tech_type(&parsed.tech_type, ctx.valid_tech_types) {
            return Err(parse_err(
                &path,
                line_num,
                format!(
                    "invalid tech type value {:?}: unknown tech type",
                    parsed.tech_type
                ),
            ));
        }
        if !validate_retrofit_hp(parsed.hp_min) {
            return Err(parse_err(
                &path,
                line_num,
                format!(
                    "invalid minimum HP value {}: unknown HP level",
                    parsed.hp_min
                ),
            ));
        }
        if !validate_retrofit_hp(parsed.hp_max) {
            return Err(parse_err(
                &path,
                line_num,
                format!(
                    "invalid maximum HP value {}: unknown HP level",
                    parsed.hp_max
                ),
            ));
        }
        if parsed.hp_min >= parsed.hp_max {
            return Err(parse_err(
                &path,
                line_num,
                format!(
                    "invalid minimum HP value {}: must be less than maximum HP value {}",
                    parsed.hp_min, parsed.hp_max
                ),
            ));
        }
        if parsed.annual_frac_or_count < 0.0 {
            return Err(parse_err(
                &path,
                line_num,
                format!(
                    "invalid annual fraction or N value {}: must be >= 0",
                    parsed.annual_frac_or_count
                ),
            ));
        }
        if !(0.0..=1.0).contains(&parsed.effectiveness) {
            return Err(parse_err(
                &path,
                line_num,
                format!(
                    "invalid effectiveness value {}: must be between 0 and 1",
                    parsed.effectiveness
                ),
            ));
        }
        let pollutant = match RetrofitPollutant::for_name(&parsed.pollutant) {
            Some(p) => p,
            None => {
                return Err(parse_err(
                    &path,
                    line_num,
                    format!(
                        "invalid pollutant value {:?}: not a valid retrofit pollutant",
                        parsed.pollutant
                    ),
                ));
            }
        };
        if parsed.id <= 0 {
            return Err(parse_err(
                &path,
                line_num,
                format!("invalid ID value {}: must be greater than 0", parsed.id),
            ));
        }

        // --- skip-filter check (rdrtrft.f :350–367) ---
        if let Some(reason) = skip_reason(&parsed, ctx) {
            result.skip_warnings.push(RetrofitSkipWarning {
                record_number,
                reason,
            });
            continue;
        }

        // --- N-units warning (rdrtrft.f :394–413) ---
        if parsed.annual_frac_or_count > 1.0 && affects_multiple_engines(&parsed) {
            result
                .n_units_warnings
                .push(RetrofitNUnitsWarning { record_number });
        }

        result.records.push(RetrofitRecord {
            record_index: record_number,
            id: parsed.id,
            year_retrofit_start: parsed.ryst,
            year_retrofit_end: parsed.ryen,
            year_model_start: parsed.myst,
            year_model_end: parsed.myen,
            scc: parsed.scc,
            tech_type: parsed.tech_type,
            hp_min: parsed.hp_min,
            hp_max: parsed.hp_max,
            annual_frac_or_count: parsed.annual_frac_or_count,
            effectiveness: parsed.effectiveness,
            pollutant: pollutant.canonical_name().to_string(),
            pollutant_idx: pollutant.pollutant_index(),
        });
    }
    if !found_end {
        return Err(Error::Parse {
            file: path,
            line: last_line_num,
            message: "missing /END/ marker after /RETROFIT/ packet".to_string(),
        });
    }

    if record_number == 0 {
        result.no_records = true;
    }

    Ok(result)
}

/// `vldrtrftrecs.f` — cross-record consistency validation.
///
/// Sorts `records` in place by `(retrofit_id, pollutant_idx,
/// record_index)` (matching the call to `srtrtrft(1, ...)` at
/// `vldrtrftrecs.f` :120) and walks each retrofit-ID group looking
/// for engine-overlap conflicts:
///
/// 1. Records that overlap on engines must share the same retrofit
///    year range and model year range.
/// 2. Records that overlap on engines and target the same pollutant
///    must share the same effectiveness.
/// 3. The summed `annual_frac_or_count` per pollutant within a
///    retrofit ID must match across pollutants (within
///    [`FRAC_RETRO_TOLERANCE`]).
///
/// Duplicate-pollutant occurrences within an ID are non-fatal —
/// they accumulate as [`RetrofitRecordsetWarning`] entries.
pub fn validate_retrofit_recordset(
    records: &mut [RetrofitRecord],
) -> Result<Vec<RetrofitRecordsetWarning>> {
    let mut warnings = Vec::new();
    if records.is_empty() {
        return Ok(warnings);
    }

    let last = records.len() - 1;
    sort_retrofits(records, Comparison::IdPollutantRecord, 0, last);

    // Group boundaries by retrofit ID.
    let mut group_starts: Vec<usize> = Vec::new();
    let mut prev_id: i32 = i32::MIN;
    for (i, rec) in records.iter().enumerate() {
        if rec.id != prev_id {
            group_starts.push(i);
            prev_id = rec.id;
        }
    }

    for (g, &start) in group_starts.iter().enumerate() {
        let end = if g + 1 < group_starts.len() {
            group_starts[g + 1] - 1
        } else {
            records.len() - 1
        };
        if start == end {
            continue;
        }

        // Re-emit at most one duplicate-pollutant warning per
        // (group, pollutant) — `vldrtrftrecs.f` :170, :238–254.
        let mut warned_pollutants: Vec<String> = Vec::new();

        // The Fortran source's outer loop iterates `j` over the
        // group; the inner loop over `k` rebuilds per-pollutant
        // accumulators each time, so the recordset-level error
        // checks observe the *first* `j` they fire on. We mirror
        // that loop nesting exactly.
        for j in start..=end {
            // Per-pollutant accumulators (sized to NRTRFTPLLTNT, one
            // slot per pollutant in canonical order HC/CO/NOX/PM).
            let mut frac_sum = [0.0f32; 4];
            // -1 sentinel = "first occurrence not yet seen".
            let mut effect_seen = [-1.0f32; 4];
            // Counts of overlapping records of the same pollutant
            // as `records[j]` — only crosses 1 if there's a duplicate.
            let mut same_pollutant_overlap: usize = 0;

            for k in start..=end {
                if !engine_overlap(records, j, k) {
                    continue;
                }
                if records[k].year_retrofit_start != records[j].year_retrofit_start
                    || records[k].year_retrofit_end != records[j].year_retrofit_end
                {
                    let (lo, hi) = order_pair(records, j, k);
                    return Err(Error::Parse {
                        file: PathBuf::from(".RTR"),
                        line: records[lo].record_index,
                        message: format!(
                            "different retrofit year range for same retrofit ID and \
                             overlapping engines: record {} (RYst={}, RYen={}) vs record {} \
                             (RYst={}, RYen={}) for ID {}",
                            records[lo].record_index,
                            records[lo].year_retrofit_start,
                            records[lo].year_retrofit_end,
                            records[hi].record_index,
                            records[hi].year_retrofit_start,
                            records[hi].year_retrofit_end,
                            records[lo].id,
                        ),
                    });
                }
                if records[k].year_model_start != records[j].year_model_start
                    || records[k].year_model_end != records[j].year_model_end
                {
                    let (lo, hi) = order_pair(records, j, k);
                    return Err(Error::Parse {
                        file: PathBuf::from(".RTR"),
                        line: records[lo].record_index,
                        message: format!(
                            "different model year range for same retrofit ID and \
                             overlapping engines: record {} (MYst={}, MYen={}) vs record {} \
                             (MYst={}, MYen={}) for ID {}",
                            records[lo].record_index,
                            records[lo].year_model_start,
                            records[lo].year_model_end,
                            records[hi].record_index,
                            records[hi].year_model_start,
                            records[hi].year_model_end,
                            records[lo].id,
                        ),
                    });
                }

                let pollutant_slot = pollutant_slot(records[k].pollutant_idx);
                frac_sum[pollutant_slot] += records[k].annual_frac_or_count;

                if effect_seen[pollutant_slot] < 0.0 {
                    effect_seen[pollutant_slot] = records[k].effectiveness;
                } else if records[k].effectiveness != effect_seen[pollutant_slot] {
                    let (lo, hi) = order_pair(records, j, k);
                    return Err(Error::Parse {
                        file: PathBuf::from(".RTR"),
                        line: records[lo].record_index,
                        message: format!(
                            "different effectiveness for same retrofit ID, pollutant, and \
                             overlapping engines: record {} (effect={}) vs record {} \
                             (effect={}) for ID {} pollutant {}",
                            records[lo].record_index,
                            records[lo].effectiveness,
                            records[hi].record_index,
                            records[hi].effectiveness,
                            records[lo].id,
                            records[lo].pollutant,
                        ),
                    });
                }

                if records[k].pollutant_idx == records[j].pollutant_idx {
                    same_pollutant_overlap += 1;
                    if same_pollutant_overlap > 1
                        && !warned_pollutants.contains(&records[j].pollutant)
                    {
                        warned_pollutants.push(records[j].pollutant.clone());
                        warnings.push(RetrofitRecordsetWarning {
                            retrofit_id: records[j].id,
                            pollutant: records[j].pollutant.clone(),
                        });
                    }
                }
            }

            // --- per-pollutant fraction-sum equality (vldrtrftrecs.f :262–281) ---
            let mut expected: Option<(usize, f32)> = None;
            for (slot, &sum) in frac_sum.iter().enumerate() {
                if sum > 0.0 {
                    if let Some((_, expected_sum)) = expected {
                        if (sum - expected_sum).abs() > FRAC_RETRO_TOLERANCE {
                            return Err(Error::Parse {
                                file: PathBuf::from(".RTR"),
                                line: records[j].record_index,
                                message: format!(
                                    "sum of fraction-or-N retrofitted per pollutant not the same \
                                     for retrofit ID {}: pollutant {} = {}, pollutant {} = {}",
                                    records[j].id,
                                    pollutant_canonical_name(expected.unwrap().0),
                                    expected_sum,
                                    pollutant_canonical_name(slot),
                                    sum,
                                ),
                            });
                        }
                    } else {
                        expected = Some((slot, sum));
                    }
                }
            }
        }
    }

    Ok(warnings)
}

// =============================================================================
// Internal helpers
// =============================================================================

/// Fields from one record of the input, before they are validated
/// and converted into a [`RetrofitRecord`].
struct ParsedFields {
    ryst: i32,
    ryen: i32,
    myst: i32,
    myen: i32,
    scc: String,
    tech_type: String,
    hp_min: f32,
    hp_max: f32,
    annual_frac_or_count: f32,
    effectiveness: f32,
    pollutant: String,
    id: i32,
}

fn parse_record(line: &str, line_num: usize, path: &Path) -> Result<ParsedFields> {
    let ryst = parse_i32(line, 1, 4, "Retrofit Year Start", line_num, path)?;
    let ryen = parse_i32(line, 6, 9, "Retrofit Year End", line_num, path)?;
    let myst = parse_i32(line, 11, 14, "Model Year Start", line_num, path)?;
    let myen = parse_i32(line, 16, 19, "Model Year End", line_num, path)?;
    let scc = trim_upper(column(line, 21, 30));
    let tech_type = trim_upper(column(line, 32, 41));
    let hp_min = parse_f32(line, 43, 47, "Minimum HP", line_num, path)?;
    let hp_max = parse_f32(line, 48, 52, "Maximum HP", line_num, path)?;
    let annual_frac_or_count = parse_f32(line, 54, 71, "Annual Fraction or N", line_num, path)?;
    let effectiveness = parse_f32(line, 73, 78, "Effectiveness", line_num, path)?;
    let pollutant = trim_upper(column(line, 80, 89));
    let id = parse_i32(line, 91, 95, "ID", line_num, path)?;
    Ok(ParsedFields {
        ryst,
        ryen,
        myst,
        myen,
        scc,
        tech_type,
        hp_min,
        hp_max,
        annual_frac_or_count,
        effectiveness,
        pollutant,
        id,
    })
}

fn parse_i32(
    line: &str,
    start: usize,
    end: usize,
    field: &str,
    line_num: usize,
    path: &Path,
) -> Result<i32> {
    let raw = column(line, start, end);
    raw.trim().parse::<i32>().map_err(|_| Error::Parse {
        file: path.to_path_buf(),
        line: line_num,
        message: format!("invalid {field}: {raw:?}"),
    })
}

fn parse_f32(
    line: &str,
    start: usize,
    end: usize,
    field: &str,
    line_num: usize,
    path: &Path,
) -> Result<f32> {
    let raw = column(line, start, end);
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        // Fortran F-format reads blanks as 0.
        return Ok(0.0);
    }
    trimmed.parse::<f32>().map_err(|_| Error::Parse {
        file: path.to_path_buf(),
        line: line_num,
        message: format!("invalid {field}: {raw:?}"),
    })
}

fn column(line: &str, start_1based: usize, end_1based: usize) -> &str {
    let start = start_1based.saturating_sub(1);
    let end = end_1based.min(line.len());
    if start >= end {
        return "";
    }
    &line[start..end]
}

fn trim_upper(s: &str) -> String {
    s.trim().to_ascii_uppercase()
}

fn is_keyword(line: &str, keyword: &str) -> bool {
    line.trim_start()
        .get(..keyword.len())
        .map(|s| s.eq_ignore_ascii_case(keyword))
        .unwrap_or(false)
}

fn parse_err(path: &Path, line: usize, message: String) -> Error {
    Error::Parse {
        file: path.to_path_buf(),
        line,
        message,
    }
}

fn validate_year(
    year: i32,
    name: &str,
    range: &std::ops::RangeInclusive<i32>,
    line_num: usize,
    path: &Path,
) -> Result<()> {
    if !range.contains(&year) {
        return Err(parse_err(
            path,
            line_num,
            format!(
                "invalid {name} value {year}: valid range is {} to {}",
                range.start(),
                range.end()
            ),
        ));
    }
    Ok(())
}

/// `chkasc.f` — does `scc` match anything in `equipment_codes`?
fn chkasc(scc: &str, equipment_codes: &[(String, bool)], skip_unrequested: bool) -> bool {
    let scc_4global = scc.len() >= 10 && &scc[4..10] == "000000";
    let scc_7global = !scc_4global && scc.len() >= 10 && &scc[7..10] == "000";
    for (code, requested) in equipment_codes {
        if skip_unrequested && !*requested {
            continue;
        }
        if scc == code {
            return true;
        }
        if scc_4global && code.len() >= 4 && scc.len() >= 4 && code[..4] == scc[..4] {
            return true;
        }
        if scc_7global && code.len() >= 7 && scc.len() >= 7 && code[..7] == scc[..7] {
            return true;
        }
    }
    false
}

fn skip_reason(parsed: &ParsedFields, ctx: &RetrofitContext) -> Option<String> {
    if ctx.eval_year < parsed.ryst {
        return Some(format!(
            "evaluation year {} less than retrofit year start {}",
            ctx.eval_year, parsed.ryst
        ));
    }
    if ctx.eval_year < parsed.myst {
        return Some(format!(
            "evaluation year {} less than model year start {}",
            ctx.eval_year, parsed.myst
        ));
    }
    if parsed.scc != RTRFTSCC_ALL && !chkasc(&parsed.scc, ctx.equipment_codes, true) {
        return Some(format!("SCC {} not requested for this run", parsed.scc));
    }
    None
}

fn affects_multiple_engines(parsed: &ParsedFields) -> bool {
    parsed.ryst != parsed.ryen
        || parsed.myst != parsed.myen
        || parsed.scc == RTRFTSCC_ALL
        || (parsed.scc.len() >= 10 && &parsed.scc[4..10] == "000000")
        || (parsed.scc.len() >= 10 && &parsed.scc[7..10] == "000")
        || parsed.tech_type == RTRFTTECHTYPE_ALL
        || count_hp_categories(parsed.hp_min, parsed.hp_max) > 1
}

/// Map a pollutant index (`IDXTHC`/`IDXCO`/`IDXNOX`/`IDXPM`) to a
/// dense 0-based slot in the per-pollutant accumulator arrays.
fn pollutant_slot(pollutant_idx: i32) -> usize {
    // RetrofitPollutant order: HC=0, CO=1, NOX=2, PM=3. The Fortran
    // indexes are non-contiguous (HC=1, CO=2, NOX=3, PM=6) so we map
    // through RetrofitPollutant.
    match pollutant_idx {
        x if x == RetrofitPollutant::Hc.pollutant_index() => 0,
        x if x == RetrofitPollutant::Co.pollutant_index() => 1,
        x if x == RetrofitPollutant::Nox.pollutant_index() => 2,
        x if x == RetrofitPollutant::Pm.pollutant_index() => 3,
        _ => unreachable!("invalid retrofit pollutant index {pollutant_idx}"),
    }
}

fn pollutant_canonical_name(slot: usize) -> &'static str {
    match slot {
        0 => RetrofitPollutant::Hc.canonical_name(),
        1 => RetrofitPollutant::Co.canonical_name(),
        2 => RetrofitPollutant::Nox.canonical_name(),
        3 => RetrofitPollutant::Pm.canonical_name(),
        _ => unreachable!(),
    }
}

/// Order two record indexes by their input-record number, matching
/// `vldrtrftrecs.f` :302–306 — error messages report the lower
/// record number first.
fn order_pair(records: &[RetrofitRecord], a: usize, b: usize) -> (usize, usize) {
    if records[a].record_index <= records[b].record_index {
        (a, b)
    } else {
        (b, a)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Default context that accepts every well-formed SCC and tech
    /// type. Use [`make_ctx`] for tests that need the skip filter to
    /// fire.
    fn permissive_ctx<'a>(eqp: &'a [(String, bool)], techs: &'a [String]) -> RetrofitContext<'a> {
        RetrofitContext {
            eval_year: 2020,
            equipment_codes: eqp,
            valid_tech_types: techs,
        }
    }

    fn make_ctx<'a>(
        year: i32,
        eqp: &'a [(String, bool)],
        techs: &'a [String],
    ) -> RetrofitContext<'a> {
        RetrofitContext {
            eval_year: year,
            equipment_codes: eqp,
            valid_tech_types: techs,
        }
    }

    /// Build a single retrofit record line at exact column positions
    /// matching `rdrtrft.f`.
    #[allow(clippy::too_many_arguments)]
    fn line(
        ryst: i32,
        ryen: i32,
        myst: i32,
        myen: i32,
        scc: &str,
        tech: &str,
        hp_min: &str,
        hp_max: &str,
        frac_or_n: &str,
        effect: &str,
        pollutant: &str,
        id: i32,
    ) -> String {
        let mut buf = vec![b' '; 95];
        let put_str = |buf: &mut [u8], col_1based: usize, value: &str, width: usize| {
            let start = col_1based - 1;
            let bytes = value.as_bytes();
            let n = bytes.len().min(width);
            buf[start..start + n].copy_from_slice(&bytes[..n]);
        };
        // 4-digit ints right-justified in a 4-wide slot.
        let put_int4 = |buf: &mut [u8], col_1based: usize, value: i32| {
            let s = format!("{value:>4}");
            put_str(buf, col_1based, &s, 4);
        };
        put_int4(&mut buf, 1, ryst);
        put_int4(&mut buf, 6, ryen);
        put_int4(&mut buf, 11, myst);
        put_int4(&mut buf, 16, myen);
        put_str(&mut buf, 21, scc, 10);
        put_str(&mut buf, 32, tech, 10);
        put_str(&mut buf, 43, &format!("{hp_min:>5}"), 5);
        put_str(&mut buf, 48, &format!("{hp_max:>5}"), 5);
        put_str(&mut buf, 54, &format!("{frac_or_n:>18}"), 18);
        put_str(&mut buf, 73, &format!("{effect:>6}"), 6);
        put_str(&mut buf, 80, pollutant, 10);
        put_str(&mut buf, 91, &format!("{id:>5}"), 5);
        String::from_utf8(buf).unwrap()
    }

    #[test]
    fn validate_hp_accepts_levels_and_sentinels() {
        assert!(validate_retrofit_hp(0.0));
        assert!(validate_retrofit_hp(9999.0));
        assert!(validate_retrofit_hp(50.0));
        assert!(validate_retrofit_hp(3000.0));
        assert!(!validate_retrofit_hp(60.0));
        assert!(!validate_retrofit_hp(-1.0));
    }

    #[test]
    fn validate_scc_accepts_all_and_exact_match() {
        let codes = vec![
            ("2270002003".to_string(), true),
            ("2270005015".to_string(), true),
        ];
        assert!(validate_retrofit_scc("ALL", &codes));
        assert!(validate_retrofit_scc("2270002003", &codes));
        assert!(!validate_retrofit_scc("9999999999", &codes));
    }

    #[test]
    fn validate_scc_accepts_4digit_global() {
        let codes = vec![("2270002003".to_string(), true)];
        // 2270000000 = 4-digit global for the 2270* family.
        assert!(validate_retrofit_scc("2270000000", &codes));
        // 2265000000 != 2270* — should fail.
        assert!(!validate_retrofit_scc("2265000000", &codes));
    }

    #[test]
    fn validate_scc_accepts_7digit_global() {
        let codes = vec![("2270002003".to_string(), true)];
        // 2270002000 = 7-digit global for 2270002* — should match.
        assert!(validate_retrofit_scc("2270002000", &codes));
        // 2270003000 != 2270002* — should fail.
        assert!(!validate_retrofit_scc("2270003000", &codes));
    }

    #[test]
    fn validate_scc_unrequested_still_valid() {
        // Validation ignores the `requested` flag: only the skip
        // filter (chkasc with skipunreq=.TRUE.) honours it.
        let codes = vec![("2270002003".to_string(), false)];
        assert!(validate_retrofit_scc("2270002003", &codes));
    }

    #[test]
    fn validate_tech_type_accepts_all_and_known() {
        let techs = vec!["BASE".to_string(), "T2".to_string()];
        assert!(validate_retrofit_tech_type("ALL", &techs));
        assert!(validate_retrofit_tech_type("BASE", &techs));
        assert!(validate_retrofit_tech_type("base", &techs));
        assert!(!validate_retrofit_tech_type("UNKNOWN", &techs));
    }

    #[test]
    fn count_hp_categories_basic_ranges() {
        // Sentinel 0 = idx 0; 50 hp = idx 8; 100 hp = idx 10; 9999 = MXHPC+1 = 19.
        assert_eq!(count_hp_categories(0.0, 9999.0), 19);
        assert_eq!(count_hp_categories(50.0, 100.0), 2);
        assert_eq!(count_hp_categories(50.0, 75.0), 1);
        // Invalid endpoint → 0.
        assert_eq!(count_hp_categories(60.0, 100.0), 0);
    }

    #[test]
    fn parses_a_complete_record() {
        let l = line(
            2008,
            2009,
            1996,
            1997,
            "2270002000",
            "ALL",
            "50",
            "300",
            "0.05",
            "0.50",
            "PM",
            1,
        );
        let body = format!("/RETROFIT/\n{l}\n/END/\n");
        let codes = vec![("2270002069".to_string(), true)];
        let techs: Vec<String> = vec![];
        let ctx = permissive_ctx(&codes, &techs);
        let result = read_retrofit(body.as_bytes(), &ctx).unwrap();
        assert_eq!(result.records.len(), 1);
        let r = &result.records[0];
        assert_eq!(r.year_retrofit_start, 2008);
        assert_eq!(r.year_retrofit_end, 2009);
        assert_eq!(r.year_model_start, 1996);
        assert_eq!(r.year_model_end, 1997);
        assert_eq!(r.scc, "2270002000");
        assert_eq!(r.tech_type, "ALL");
        assert!((r.hp_min - 50.0).abs() < 1e-6);
        assert!((r.hp_max - 300.0).abs() < 1e-6);
        assert!((r.annual_frac_or_count - 0.05).abs() < 1e-6);
        assert!((r.effectiveness - 0.50).abs() < 1e-6);
        assert_eq!(r.pollutant, "PM");
        assert_eq!(r.pollutant_idx, RetrofitPollutant::Pm.pollutant_index());
        assert_eq!(r.id, 1);
        assert!(result.skip_warnings.is_empty());
        assert!(result.n_units_warnings.is_empty());
    }

    #[test]
    fn missing_packet_marker_errors() {
        let body = "no packet here\n/END/\n";
        let codes: Vec<(String, bool)> = vec![];
        let techs: Vec<String> = vec![];
        let ctx = permissive_ctx(&codes, &techs);
        let err = read_retrofit(body.as_bytes(), &ctx).unwrap_err();
        match err {
            Error::Parse { message, .. } => assert!(message.contains("/RETROFIT/")),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn missing_end_marker_errors() {
        let l = line(
            2008, 2009, 1996, 1997, "ALL", "ALL", "50", "300", "0.05", "0.50", "PM", 1,
        );
        let body = format!("/RETROFIT/\n{l}\n");
        let codes: Vec<(String, bool)> = vec![];
        let techs: Vec<String> = vec![];
        let ctx = permissive_ctx(&codes, &techs);
        let err = read_retrofit(body.as_bytes(), &ctx).unwrap_err();
        match err {
            Error::Parse { message, .. } => assert!(message.contains("/END/")),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn empty_packet_sets_no_records_flag() {
        let body = "/RETROFIT/\n/END/\n";
        let codes: Vec<(String, bool)> = vec![];
        let techs: Vec<String> = vec![];
        let ctx = permissive_ctx(&codes, &techs);
        let result = read_retrofit(body.as_bytes(), &ctx).unwrap();
        assert!(result.records.is_empty());
        assert!(result.no_records);
    }

    #[test]
    fn skip_filter_drops_record_before_eval_year() {
        // ryst > eval_year → skip.
        let l = line(
            2030, 2030, 1996, 1997, "ALL", "ALL", "50", "300", "0.05", "0.50", "PM", 1,
        );
        let body = format!("/RETROFIT/\n{l}\n/END/\n");
        let codes: Vec<(String, bool)> = vec![];
        let techs: Vec<String> = vec![];
        let ctx = make_ctx(2020, &codes, &techs);
        let result = read_retrofit(body.as_bytes(), &ctx).unwrap();
        assert!(result.records.is_empty());
        assert_eq!(result.skip_warnings.len(), 1);
        assert!(result.skip_warnings[0]
            .reason
            .contains("retrofit year start"));
    }

    #[test]
    fn skip_filter_drops_unrequested_scc() {
        let l = line(
            2008,
            2008,
            1996,
            1996,
            "2270002069",
            "ALL",
            "175",
            "600",
            "0.06",
            "0.05",
            "NOX",
            2,
        );
        let body = format!("/RETROFIT/\n{l}\n/END/\n");
        // SCC is valid (matches eqpcod) but not requested.
        let codes = vec![("2270002069".to_string(), false)];
        let techs: Vec<String> = vec![];
        let ctx = make_ctx(2020, &codes, &techs);
        let result = read_retrofit(body.as_bytes(), &ctx).unwrap();
        assert!(result.records.is_empty());
        assert_eq!(result.skip_warnings.len(), 1);
        assert!(result.skip_warnings[0].reason.contains("not requested"));
    }

    #[test]
    fn n_units_warning_fires_for_count_with_wildcard_scc() {
        // annual = 5 (count, > 1) AND SCC = ALL → warning.
        let l = line(
            2008, 2008, 1996, 1996, "ALL", "ALL", "50", "75", "5", "0.50", "PM", 1,
        );
        let body = format!("/RETROFIT/\n{l}\n/END/\n");
        let codes: Vec<(String, bool)> = vec![];
        let techs: Vec<String> = vec![];
        let ctx = permissive_ctx(&codes, &techs);
        let result = read_retrofit(body.as_bytes(), &ctx).unwrap();
        assert_eq!(result.records.len(), 1);
        assert_eq!(result.n_units_warnings.len(), 1);
    }

    #[test]
    fn n_units_warning_silent_for_single_engine() {
        // count > 1 but record affects exactly one (RY, MY, SCC,
        // tech, HP-cat) — no warning.
        let l = line(
            2008,
            2008,
            1996,
            1996,
            "2270002069",
            "T1",
            "50",
            "75",
            "5",
            "0.50",
            "PM",
            1,
        );
        let body = format!("/RETROFIT/\n{l}\n/END/\n");
        let codes = vec![("2270002069".to_string(), true)];
        let techs = vec!["T1".to_string()];
        let ctx = permissive_ctx(&codes, &techs);
        let result = read_retrofit(body.as_bytes(), &ctx).unwrap();
        assert_eq!(result.records.len(), 1);
        assert!(result.n_units_warnings.is_empty());
    }

    #[test]
    fn invalid_year_is_fatal() {
        let l = line(
            1900, 2009, 1996, 1996, "ALL", "ALL", "50", "75", "0.05", "0.50", "PM", 1,
        );
        let body = format!("/RETROFIT/\n{l}\n/END/\n");
        let codes: Vec<(String, bool)> = vec![];
        let techs: Vec<String> = vec![];
        let ctx = permissive_ctx(&codes, &techs);
        let err = read_retrofit(body.as_bytes(), &ctx).unwrap_err();
        match err {
            Error::Parse { message, .. } => assert!(message.contains("retrofit year start")),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn invalid_year_range_is_fatal() {
        // ryst > ryen
        let l = line(
            2010, 2009, 1996, 1996, "ALL", "ALL", "50", "75", "0.05", "0.50", "PM", 1,
        );
        let body = format!("/RETROFIT/\n{l}\n/END/\n");
        let codes: Vec<(String, bool)> = vec![];
        let techs: Vec<String> = vec![];
        let ctx = permissive_ctx(&codes, &techs);
        let err = read_retrofit(body.as_bytes(), &ctx).unwrap_err();
        match err {
            Error::Parse { message, .. } => assert!(message.contains("must be <=")),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn invalid_hp_is_fatal() {
        let l = line(
            2008, 2008, 1996, 1996, "ALL", "ALL", "60", "75", "0.05", "0.50", "PM", 1,
        );
        let body = format!("/RETROFIT/\n{l}\n/END/\n");
        let codes: Vec<(String, bool)> = vec![];
        let techs: Vec<String> = vec![];
        let ctx = permissive_ctx(&codes, &techs);
        let err = read_retrofit(body.as_bytes(), &ctx).unwrap_err();
        match err {
            Error::Parse { message, .. } => assert!(message.contains("HP")),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn unknown_pollutant_is_fatal() {
        let l = line(
            2008, 2008, 1996, 1996, "ALL", "ALL", "50", "75", "0.05", "0.50", "CH4", 1,
        );
        let body = format!("/RETROFIT/\n{l}\n/END/\n");
        let codes: Vec<(String, bool)> = vec![];
        let techs: Vec<String> = vec![];
        let ctx = permissive_ctx(&codes, &techs);
        let err = read_retrofit(body.as_bytes(), &ctx).unwrap_err();
        match err {
            Error::Parse { message, .. } => assert!(message.contains("pollutant")),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn nonpositive_id_is_fatal() {
        let l = line(
            2008, 2008, 1996, 1996, "ALL", "ALL", "50", "75", "0.05", "0.50", "PM", 0,
        );
        let body = format!("/RETROFIT/\n{l}\n/END/\n");
        let codes: Vec<(String, bool)> = vec![];
        let techs: Vec<String> = vec![];
        let ctx = permissive_ctx(&codes, &techs);
        let err = read_retrofit(body.as_bytes(), &ctx).unwrap_err();
        match err {
            Error::Parse { message, .. } => assert!(message.contains("ID")),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn effectiveness_out_of_range_is_fatal() {
        let l = line(
            2008, 2008, 1996, 1996, "ALL", "ALL", "50", "75", "0.05", "1.50", "PM", 1,
        );
        let body = format!("/RETROFIT/\n{l}\n/END/\n");
        let codes: Vec<(String, bool)> = vec![];
        let techs: Vec<String> = vec![];
        let ctx = permissive_ctx(&codes, &techs);
        let err = read_retrofit(body.as_bytes(), &ctx).unwrap_err();
        match err {
            Error::Parse { message, .. } => assert!(message.contains("effectiveness")),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parses_the_canonical_retrotst_packet() {
        // Mirrors the 4 records in DATA/RETROFIT/retrotst.dat.
        let l1 = line(
            2008,
            2009,
            1996,
            1997,
            "2270002000",
            "ALL",
            "50",
            "300",
            "0.05",
            "0.50",
            "PM",
            1,
        );
        let l2 = line(
            2008,
            2008,
            1991,
            1996,
            "2270002069",
            "T1",
            "175",
            "600",
            "0.06",
            "0.05",
            "NOX",
            2,
        );
        let l3 = line(
            2007,
            2008,
            1996,
            1997,
            "2270005015",
            "ALL",
            "100",
            "300",
            "0.04",
            "0.80",
            "PM",
            3,
        );
        let l4 = line(
            2007,
            2008,
            1996,
            1997,
            "2270005015",
            "ALL",
            "100",
            "300",
            "0.04",
            "0.03",
            "NOX",
            3,
        );
        let body = format!("/RETROFIT/\n{l1}\n{l2}\n{l3}\n{l4}\n/END/\n");

        let codes = vec![
            ("2270002069".to_string(), true),
            ("2270005015".to_string(), true),
        ];
        let techs = vec!["T1".to_string()];
        let ctx = permissive_ctx(&codes, &techs);
        let result = read_retrofit(body.as_bytes(), &ctx).unwrap();
        assert_eq!(result.records.len(), 4);
        assert!(result.skip_warnings.is_empty());
        assert!(result.n_units_warnings.is_empty());
    }

    // ---- recordset validation ----

    #[allow(clippy::too_many_arguments)]
    fn rec(
        idx: usize,
        id: i32,
        ryst: i32,
        ryen: i32,
        myst: i32,
        myen: i32,
        scc: &str,
        tech: &str,
        hp_min: f32,
        hp_max: f32,
        frac: f32,
        effect: f32,
        pollutant: RetrofitPollutant,
    ) -> RetrofitRecord {
        RetrofitRecord {
            record_index: idx,
            id,
            year_retrofit_start: ryst,
            year_retrofit_end: ryen,
            year_model_start: myst,
            year_model_end: myen,
            scc: scc.to_string(),
            tech_type: tech.to_string(),
            hp_min,
            hp_max,
            annual_frac_or_count: frac,
            effectiveness: effect,
            pollutant: pollutant.canonical_name().to_string(),
            pollutant_idx: pollutant.pollutant_index(),
        }
    }

    #[test]
    fn validate_recordset_passes_on_consistent_pair() {
        // Two records, same retrofit ID, different pollutants, same
        // effect range, equal fraction sums.
        let mut records = vec![
            rec(
                1,
                3,
                2007,
                2008,
                1996,
                1997,
                "2270005015",
                "ALL",
                100.0,
                300.0,
                0.04,
                0.80,
                RetrofitPollutant::Pm,
            ),
            rec(
                2,
                3,
                2007,
                2008,
                1996,
                1997,
                "2270005015",
                "ALL",
                100.0,
                300.0,
                0.04,
                0.03,
                RetrofitPollutant::Nox,
            ),
        ];
        let warns = validate_retrofit_recordset(&mut records).unwrap();
        assert!(warns.is_empty());
    }

    #[test]
    fn validate_recordset_rejects_different_year_range() {
        let mut records = vec![
            rec(
                1,
                5,
                2007,
                2008,
                1996,
                1997,
                "2270005015",
                "ALL",
                100.0,
                300.0,
                0.04,
                0.80,
                RetrofitPollutant::Pm,
            ),
            rec(
                2,
                5,
                2010,
                2012,
                1996,
                1997,
                "2270005015",
                "ALL",
                100.0,
                300.0,
                0.04,
                0.03,
                RetrofitPollutant::Nox,
            ),
        ];
        let err = validate_retrofit_recordset(&mut records).unwrap_err();
        match err {
            Error::Parse { message, .. } => assert!(message.contains("retrofit year range")),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn validate_recordset_rejects_different_model_year_range() {
        let mut records = vec![
            rec(
                1,
                5,
                2007,
                2008,
                1996,
                1997,
                "2270005015",
                "ALL",
                100.0,
                300.0,
                0.04,
                0.80,
                RetrofitPollutant::Pm,
            ),
            rec(
                2,
                5,
                2007,
                2008,
                1990,
                1995,
                "2270005015",
                "ALL",
                100.0,
                300.0,
                0.04,
                0.03,
                RetrofitPollutant::Nox,
            ),
        ];
        let err = validate_retrofit_recordset(&mut records).unwrap_err();
        match err {
            Error::Parse { message, .. } => assert!(message.contains("model year range")),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn validate_recordset_rejects_different_effectiveness_for_same_pollutant() {
        // Two PM records with overlapping engines under same ID but
        // different effect values.
        let mut records = vec![
            rec(
                1,
                5,
                2007,
                2008,
                1996,
                1997,
                "2270005015",
                "ALL",
                100.0,
                300.0,
                0.04,
                0.80,
                RetrofitPollutant::Pm,
            ),
            rec(
                2,
                5,
                2007,
                2008,
                1996,
                1997,
                "2270005015",
                "ALL",
                100.0,
                300.0,
                0.04,
                0.50,
                RetrofitPollutant::Pm,
            ),
        ];
        let err = validate_retrofit_recordset(&mut records).unwrap_err();
        match err {
            Error::Parse { message, .. } => assert!(message.contains("effectiveness")),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn validate_recordset_rejects_mismatched_fraction_sums() {
        // PM sum = 0.04, NOX sum = 0.10 → outside tolerance.
        let mut records = vec![
            rec(
                1,
                5,
                2007,
                2008,
                1996,
                1997,
                "2270005015",
                "ALL",
                100.0,
                300.0,
                0.04,
                0.80,
                RetrofitPollutant::Pm,
            ),
            rec(
                2,
                5,
                2007,
                2008,
                1996,
                1997,
                "2270005015",
                "ALL",
                100.0,
                300.0,
                0.10,
                0.03,
                RetrofitPollutant::Nox,
            ),
        ];
        let err = validate_retrofit_recordset(&mut records).unwrap_err();
        match err {
            Error::Parse { message, .. } => assert!(message.contains("fraction-or-N")),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn validate_recordset_warns_on_duplicate_pollutant() {
        // Two PM records, overlapping engines, same effect — the
        // duplication is allowed but should warn.
        let mut records = vec![
            rec(
                1,
                5,
                2007,
                2008,
                1996,
                1997,
                "2270005015",
                "ALL",
                100.0,
                300.0,
                0.02,
                0.80,
                RetrofitPollutant::Pm,
            ),
            rec(
                2,
                5,
                2007,
                2008,
                1996,
                1997,
                "2270005015",
                "ALL",
                100.0,
                300.0,
                0.02,
                0.80,
                RetrofitPollutant::Pm,
            ),
        ];
        let warns = validate_retrofit_recordset(&mut records).unwrap();
        assert_eq!(warns.len(), 1);
        assert_eq!(warns[0].retrofit_id, 5);
        assert_eq!(warns[0].pollutant, "PM");
    }

    #[test]
    fn validate_recordset_independent_groups() {
        // Two retrofit IDs, neither overlapping with the other →
        // both validate.
        let mut records = vec![
            rec(
                1,
                1,
                2008,
                2008,
                1996,
                1996,
                "ALL",
                "ALL",
                0.0,
                100.0,
                0.05,
                0.5,
                RetrofitPollutant::Pm,
            ),
            rec(
                2,
                2,
                2009,
                2009,
                1990,
                1990,
                "ALL",
                "ALL",
                0.0,
                100.0,
                0.10,
                0.3,
                RetrofitPollutant::Nox,
            ),
        ];
        let warns = validate_retrofit_recordset(&mut records).unwrap();
        assert!(warns.is_empty());
    }

    #[test]
    fn validate_recordset_empty_is_ok() {
        let mut records: Vec<RetrofitRecord> = vec![];
        let warns = validate_retrofit_recordset(&mut records).unwrap();
        assert!(warns.is_empty());
    }
}
