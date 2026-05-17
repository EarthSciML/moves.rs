//! Day-of-year loop bounds — `dayloop.f` (126 lines).
//!
//! Picks the `[begin_day, end_day]` day-of-year span the emission
//! calculation iterates over, plus an optional "winter skip" span
//! that is *excluded* from the iteration.
//!
//! # When the span is used
//!
//! The span is meaningful only when optional daily temperature / RVP
//! data was loaded (`ldayfl`). Without it the calculation uses the
//! single temperature and RVP from the `/OPTIONS/` packet and the
//! day loop collapses to a single iteration — `dayloop.f` returns
//! `begin_day = end_day = 1` and no skip in that case.
//!
//! # The winter skip
//!
//! A *seasonal winter* run is the one case where the iteration span
//! is not contiguous. Winter wraps the year boundary (it is the days
//! *before* spring plus the days *after* fall), so `dayloop.f`
//! returns the full-year span `1..=365` together with a skip span
//! `[skip_begin, skip_end]` covering spring + summer + fall; the
//! caller iterates `begin_day..=end_day` and skips any day inside
//! `[skip_begin, skip_end]`. The other three seasons are contiguous
//! and need no skip.
//!
//! # Leap years
//!
//! `dayloop.f`'s leap-year correction was commented out on
//! 2005-02-03 ("all years will now have 365 days, per EPA"); every
//! year is treated as 365 days. The Rust port has no leap-year
//! handling either, matching that decision.

use crate::common::consts::MXDAYS;

/// First day-of-year (1-based) of each month, plus a 13th entry for
/// the first day *after* December.
///
/// Fortran `dayloop.f` :58–59: `daynum = (/1,32,60,91,121,152,182,
/// 213,244,274,305,335,366/)`. Non-leap-year cumulative month
/// starts; `MONTH_START[12]` (Fortran `daynum(13)` = 366) is the
/// day after the last day of the year.
const MONTH_START: [i32; 13] = [1, 32, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366];

/// Season selector for a seasonal run — Fortran `iseasn` with the
/// `IDXWTR`/`IDXSPR`/`IDXSUM`/`IDXFAL` parameters from `nonrdprm.inc`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Season {
    /// Winter — `IDXWTR = 1`. The wrap-around season; produces a skip
    /// span (see the module docs).
    Winter,
    /// Spring — `IDXSPR = 2`.
    Spring,
    /// Summer — `IDXSUM = 3`.
    Summer,
    /// Fall — `IDXFAL = 4`.
    Fall,
}

/// Reporting period for the run — Fortran `iprtyp` with the
/// `IDXANN`/`IDXMTH`/`IDXSES` parameters from `nonrdprm.inc`.
///
/// The month / season payload travels with the variant because each
/// is meaningful only inside its own branch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DayLoopPeriod {
    /// Annual — `IDXANN = 1`. Spans the whole year.
    Annual,
    /// Monthly — `IDXMTH = 2`. The payload is the 1-based month
    /// (`imonth`, 1 = January … 12 = December).
    Monthly(u32),
    /// Seasonal — `IDXSES = 3`.
    Seasonal(Season),
}

/// Output of [`day_loop`] — the `jbday`/`jeday`/`jbskip`/`jeskip`/
/// `lskip` quintet `dayloop.f` produces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DayRange {
    /// First day-of-year to iterate, 1-based (`jbday`).
    pub begin_day: i32,
    /// Last day-of-year to iterate, 1-based, inclusive (`jeday`).
    pub end_day: i32,
    /// First day-of-year of the excluded skip span (`jbskip`); `0`
    /// when [`has_skip`](DayRange::has_skip) is `false`.
    pub skip_begin: i32,
    /// Last day-of-year of the excluded skip span (`jeskip`),
    /// inclusive; `0` when [`has_skip`](DayRange::has_skip) is `false`.
    pub skip_end: i32,
    /// `true` when `[skip_begin, skip_end]` must be excluded from the
    /// `begin_day..=end_day` iteration (`lskip`). Set only for a
    /// seasonal winter run.
    pub has_skip: bool,
}

impl DayRange {
    /// The single-iteration default `dayloop.f` returns when daily
    /// temperature / RVP data was not loaded: `begin_day = end_day =
    /// 1`, no skip.
    const SINGLE: DayRange = DayRange {
        begin_day: 1,
        end_day: 1,
        skip_begin: 0,
        skip_end: 0,
        has_skip: false,
    };
}

/// Compute the day-of-year iteration bounds — `dayloop.f` equivalent.
///
/// `daily_temps_loaded` is the Fortran `ldayfl` flag (optional daily
/// temperature / RVP data was supplied). When it is `false` the day
/// loop collapses to a single iteration regardless of `period`.
pub fn day_loop(daily_temps_loaded: bool, period: DayLoopPeriod) -> DayRange {
    // dayloop.f :73 — no daily data, single-iteration default.
    if !daily_temps_loaded {
        return DayRange::SINGLE;
    }

    let last_day = MXDAYS as i32; // 365

    match period {
        // dayloop.f :77–79
        DayLoopPeriod::Annual => DayRange {
            begin_day: 1,
            end_day: last_day,
            skip_begin: 0,
            skip_end: 0,
            has_skip: false,
        },
        // dayloop.f :97–100
        DayLoopPeriod::Monthly(month) => {
            // Fortran indexes daynum(imonth) / daynum(imonth+1); clamp
            // to a valid month so an out-of-range caller can never
            // index out of bounds. Production callers always pass
            // 1..=12 (the option-file parser validates the month).
            let month = month.clamp(1, 12) as usize;
            DayRange {
                begin_day: MONTH_START[month - 1],
                end_day: MONTH_START[month] - 1,
                skip_begin: 0,
                skip_end: 0,
                has_skip: false,
            }
        }
        // dayloop.f :80–96
        DayLoopPeriod::Seasonal(season) => match season {
            // Winter wraps the year: full-year span with a skip over
            // spring + summer + fall (dayloop.f :81–86).
            Season::Winter => DayRange {
                begin_day: 1,
                end_day: last_day,
                skip_begin: MONTH_START[2],    // daynum(3) = 60
                skip_end: MONTH_START[11] - 1, // daynum(12) - 1 = 334
                has_skip: true,
            },
            // dayloop.f :87–89
            Season::Spring => DayRange {
                begin_day: MONTH_START[2],   // daynum(3) = 60
                end_day: MONTH_START[5] - 1, // daynum(6) - 1 = 151
                skip_begin: 0,
                skip_end: 0,
                has_skip: false,
            },
            // dayloop.f :90–92
            Season::Summer => DayRange {
                begin_day: MONTH_START[5],   // daynum(6) = 152
                end_day: MONTH_START[8] - 1, // daynum(9) - 1 = 243
                skip_begin: 0,
                skip_end: 0,
                has_skip: false,
            },
            // dayloop.f :93–95
            Season::Fall => DayRange {
                begin_day: MONTH_START[8],    // daynum(9) = 244
                end_day: MONTH_START[11] - 1, // daynum(12) - 1 = 334
                skip_begin: 0,
                skip_end: 0,
                has_skip: false,
            },
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_daily_data_collapses_to_single_iteration() {
        // Regardless of period, ldayfl = false ⇒ the single-iteration
        // default (dayloop.f :65–73).
        for period in [
            DayLoopPeriod::Annual,
            DayLoopPeriod::Monthly(7),
            DayLoopPeriod::Seasonal(Season::Winter),
        ] {
            let r = day_loop(false, period);
            assert_eq!(r.begin_day, 1);
            assert_eq!(r.end_day, 1);
            assert_eq!(r.skip_begin, 0);
            assert_eq!(r.skip_end, 0);
            assert!(!r.has_skip);
        }
    }

    #[test]
    fn annual_spans_whole_year() {
        let r = day_loop(true, DayLoopPeriod::Annual);
        assert_eq!(r.begin_day, 1);
        assert_eq!(r.end_day, 365);
        assert!(!r.has_skip);
    }

    #[test]
    fn winter_spans_year_with_spring_summer_fall_skip() {
        let r = day_loop(true, DayLoopPeriod::Seasonal(Season::Winter));
        assert_eq!(r.begin_day, 1);
        assert_eq!(r.end_day, 365);
        assert!(r.has_skip);
        assert_eq!(r.skip_begin, 60); // daynum(3)
        assert_eq!(r.skip_end, 334); // daynum(12) - 1
    }

    #[test]
    fn spring_summer_fall_are_contiguous() {
        let spring = day_loop(true, DayLoopPeriod::Seasonal(Season::Spring));
        assert_eq!((spring.begin_day, spring.end_day), (60, 151));
        assert!(!spring.has_skip);

        let summer = day_loop(true, DayLoopPeriod::Seasonal(Season::Summer));
        assert_eq!((summer.begin_day, summer.end_day), (152, 243));
        assert!(!summer.has_skip);

        let fall = day_loop(true, DayLoopPeriod::Seasonal(Season::Fall));
        assert_eq!((fall.begin_day, fall.end_day), (244, 334));
        assert!(!fall.has_skip);
    }

    #[test]
    fn seasons_plus_winter_skip_tile_the_year() {
        // Winter (1..59 and 335..365) + spring + summer + fall must
        // exactly cover days 1..=365 with no gap or overlap.
        let spring = day_loop(true, DayLoopPeriod::Seasonal(Season::Spring));
        let summer = day_loop(true, DayLoopPeriod::Seasonal(Season::Summer));
        let fall = day_loop(true, DayLoopPeriod::Seasonal(Season::Fall));
        // Spring starts the day after winter's pre-skip stretch.
        assert_eq!(spring.begin_day, 60);
        // Each season abuts the next.
        assert_eq!(summer.begin_day, spring.end_day + 1);
        assert_eq!(fall.begin_day, summer.end_day + 1);
        // Winter's skip span is exactly spring ∪ summer ∪ fall.
        let winter = day_loop(true, DayLoopPeriod::Seasonal(Season::Winter));
        assert_eq!(winter.skip_begin, spring.begin_day);
        assert_eq!(winter.skip_end, fall.end_day);
    }

    #[test]
    fn monthly_january_and_december() {
        let jan = day_loop(true, DayLoopPeriod::Monthly(1));
        assert_eq!((jan.begin_day, jan.end_day), (1, 31));
        assert!(!jan.has_skip);

        let dec = day_loop(true, DayLoopPeriod::Monthly(12));
        assert_eq!((dec.begin_day, dec.end_day), (335, 365));
    }

    #[test]
    fn monthly_mid_year() {
        // July: daynum(7) = 182 .. daynum(8) - 1 = 212.
        let jul = day_loop(true, DayLoopPeriod::Monthly(7));
        assert_eq!((jul.begin_day, jul.end_day), (182, 212));
        // February (non-leap): 32 .. 59.
        let feb = day_loop(true, DayLoopPeriod::Monthly(2));
        assert_eq!((feb.begin_day, feb.end_day), (32, 59));
    }

    #[test]
    fn monthly_out_of_range_clamps() {
        // Defensive: a 0 or 13+ month must not panic. Clamp to the
        // nearest valid month.
        assert_eq!(day_loop(true, DayLoopPeriod::Monthly(0)).begin_day, 1);
        assert_eq!(day_loop(true, DayLoopPeriod::Monthly(99)).end_day, 365);
    }

    #[test]
    fn every_month_length_matches_calendar() {
        let lengths = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
        for (i, &len) in lengths.iter().enumerate() {
            let r = day_loop(true, DayLoopPeriod::Monthly(i as u32 + 1));
            assert_eq!(r.end_day - r.begin_day + 1, len, "month {} length", i + 1);
        }
    }
}
