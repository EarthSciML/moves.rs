//! Monthly → daily activity factors — `daymthf.f` (194 lines).
//!
//! For one equipment type, spreads the monthly activity profile
//! across every day of the year and derives the scalar month and
//! day-of-week adjustment factors the emission calculation needs.
//!
//! # The `fndtpm` lookup is the caller's job
//!
//! `daymthf.f` opens by calling `fndtpm` to pick the temporal
//! profile for `(SCC, FIPS)`: a month-of-year profile index
//! (`idxtpm`) and a day-of-week profile index (`idxtpd`). It then
//! loads either the matched profile (`mthfac(:,idxtpm)` /
//! `dayfac(:,idxtpd)`) or the defaults (`defmth` / `defday`).
//!
//! `fndtpm.f` is not yet ported — it walks a `(SCC, subregion,
//! monthly-profile-code, daily-profile-code)` table that the current
//! `.DAT` parser does not build (see `output::find`'s deferred-list).
//! So [`day_month_factors`] takes the **already-resolved** monthly
//! and daily factor vectors as inputs; resolving them via `fndtpm`
//! is the caller's responsibility once that lookup lands.
//!
//! # The `nmths` / `iprtyp` dead code
//!
//! `daymthf.f` :124–130 computes `nmths` from `iprtyp`, but every use
//! of it (and the alternate `daymthfac` formulas at :141–143, :151–157)
//! is commented out (`ctst`/`ctstd`/`cc` prefixes). The live routine
//! never reads `iprtyp` or `nmths`, so the Rust port omits both.

/// Days in each month, January … December (non-leap year).
///
/// Fortran `daymthf.f` :70: `data modays /31,28,31,30,31,30,31,31,
/// 30,31,30,31/`. Sums to 365 ([`MXDAYS`](crate::common::consts::MXDAYS)).
const MONTH_DAYS: [i32; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

/// Output of [`day_month_factors`] — the `daymthfac`/`mthf`/`dayf`/
/// `ndays` quartet `daymthf.f` produces.
#[derive(Debug, Clone, PartialEq)]
pub struct DayMonthFactors {
    /// Per-day activity factor for every day of the year
    /// (`daymthfac`). Length 365. Every entry is `1.0` when daily
    /// data was not loaded; otherwise each day in month `m` carries
    /// `monthly[m] / days_in_month(m)`.
    pub day_factors: Vec<f32>,
    /// Sum of the monthly factors over the selected months
    /// (`mthf`) — the month-vs-annual adjustment factor.
    pub month_factor: f32,
    /// Day-of-week adjustment factor (`dayf`). `1.0` in total mode;
    /// otherwise seven times the weekday or weekend daily factor.
    pub day_of_week_factor: f32,
    /// Number of days spanned by the selected months (`ndays`) —
    /// used by the diurnal & permeation evaporative calculations.
    pub n_days: i32,
}

/// Compute the monthly/daily activity factors — `daymthf.f` equivalent.
///
/// # Inputs
///
/// - `monthly`: the 12 month-of-year activity factors (`mthin`),
///   already resolved via `fndtpm` (see the module docs). Index 0 is
///   January.
/// - `daily`: the 2 day-of-week activity factors (`dayin`), already
///   resolved. Index 0 is weekday (`IDXWKD`), index 1 weekend
///   (`IDXWKE`).
/// - `months_selected`: which months are in the run period
///   (`lmonth`). Index 0 is January.
/// - `weekday_selected`: `true` when weekdays are in the run period
///   (`ldays(IDXWKD)`); selects which `daily` slot feeds `dayf`.
/// - `daily_file_loaded`: the `ldayfl` flag. When `false`,
///   `day_factors` stays all `1.0`.
/// - `total_mode`: `true` when the run is in "total" emission mode
///   (`ismtyp == IDXTOT`), which forces `day_of_week_factor` to `1.0`.
pub fn day_month_factors(
    monthly: &[f32; 12],
    daily: &[f32; 2],
    months_selected: &[bool; 12],
    weekday_selected: bool,
    daily_file_loaded: bool,
    total_mode: bool,
) -> DayMonthFactors {
    // daymthf.f :79–81 — every day starts at the neutral factor 1.0.
    let mut day_factors = vec![1.0_f32; 365];

    // daymthf.f :135–164 — accumulate the month / day totals and, when
    // daily data is loaded, write the per-day factor for each month.
    let mut month_factor = 0.0_f32;
    let mut n_days: i32 = 0;
    let mut day_begin: usize = 0; // 0-based; Fortran jbeg starts at 1

    for month in 0..12 {
        if months_selected[month] {
            month_factor += monthly[month];
            n_days += MONTH_DAYS[month];
        }
        if daily_file_loaded {
            let days_in_month = MONTH_DAYS[month] as usize;
            // `day_end` is the exclusive end of this month's day span.
            // daymthf.f :155 — every day in the month gets the same
            // value: the monthly factor over the month's length.
            let day_end = day_begin + days_in_month;
            let per_day = monthly[month] / MONTH_DAYS[month] as f32;
            for slot in &mut day_factors[day_begin..day_end] {
                *slot = per_day;
            }
            day_begin = day_end;
        }
    }

    // daymthf.f :168–183 — day-of-week adjustment.
    let day_of_week_factor = if total_mode {
        // Total mode: day-of-week adjustment is the identity.
        1.0
    } else {
        // Typical-day mode: pick the weekday or weekend factor, then
        // scale to a week's worth (daymthf.f :177–182).
        let base = if weekday_selected { daily[0] } else { daily[1] };
        7.0 * base
    };

    DayMonthFactors {
        day_factors,
        month_factor,
        day_of_week_factor,
        n_days,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL_MONTHS: [bool; 12] = [true; 12];
    const NO_MONTHS: [bool; 12] = [false; 12];

    #[test]
    fn day_factors_all_one_without_daily_data() {
        let r = day_month_factors(
            &[5.0; 12],
            &[2.0, 3.0],
            &ALL_MONTHS,
            true,
            false, // daily data not loaded
            false,
        );
        assert_eq!(r.day_factors.len(), 365);
        assert!(r.day_factors.iter().all(|&v| v == 1.0));
    }

    #[test]
    fn day_factors_spread_monthly_over_days_when_loaded() {
        // monthly[January] = 31 ⇒ each January day = 31 / 31 = 1.0.
        // monthly[February] = 28 ⇒ each February day = 28 / 28 = 1.0.
        let mut monthly = [0.0_f32; 12];
        monthly[0] = 31.0;
        monthly[1] = 28.0;
        let r = day_month_factors(&monthly, &[1.0, 1.0], &ALL_MONTHS, true, true, false);
        // Day 1 (index 0) is in January.
        assert_eq!(r.day_factors[0], 1.0);
        // Day 32 (index 31) is the first day of February.
        assert_eq!(r.day_factors[31], 1.0);
        // March monthly factor is 0 ⇒ March days are 0.0.
        assert_eq!(r.day_factors[59], 0.0); // index 59 = day 60 = Mar 1
    }

    #[test]
    fn day_factor_value_is_monthly_over_month_length() {
        let mut monthly = [0.0_f32; 12];
        monthly[6] = 62.0; // July, 31 days
        let r = day_month_factors(&monthly, &[1.0, 1.0], &ALL_MONTHS, true, true, false);
        // July starts at day 182 (index 181) and runs 31 days.
        let expected = 62.0_f32 / 31.0;
        for idx in 181..212 {
            assert!(
                (r.day_factors[idx] - expected).abs() < 1e-6,
                "day index {idx}"
            );
        }
    }

    #[test]
    fn last_day_of_year_is_filled() {
        // December has a non-zero factor; index 364 (day 365) must be
        // written, confirming the per-month fill tiles the whole year.
        let mut monthly = [0.0_f32; 12];
        monthly[11] = 93.0; // December, 31 days
        let r = day_month_factors(&monthly, &[1.0, 1.0], &ALL_MONTHS, true, true, false);
        assert!((r.day_factors[364] - 3.0).abs() < 1e-6);
    }

    #[test]
    fn month_factor_sums_selected_months_only() {
        let monthly = [
            1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0,
        ];
        // Select only March (idx 2) and December (idx 11).
        let mut selected = NO_MONTHS;
        selected[2] = true;
        selected[11] = true;
        let r = day_month_factors(&monthly, &[1.0, 1.0], &selected, true, false, false);
        assert_eq!(r.month_factor, 3.0 + 12.0);
    }

    #[test]
    fn n_days_sums_selected_month_lengths() {
        // Select January (31) + February (28) + April (30) = 89.
        let mut selected = NO_MONTHS;
        selected[0] = true;
        selected[1] = true;
        selected[3] = true;
        let r = day_month_factors(&[1.0; 12], &[1.0, 1.0], &selected, true, false, false);
        assert_eq!(r.n_days, 89);
    }

    #[test]
    fn n_days_is_365_for_a_full_year() {
        let r = day_month_factors(&[1.0; 12], &[1.0, 1.0], &ALL_MONTHS, true, false, false);
        assert_eq!(r.n_days, 365);
    }

    #[test]
    fn day_of_week_factor_is_one_in_total_mode() {
        let r = day_month_factors(
            &[1.0; 12],
            &[2.0, 5.0],
            &ALL_MONTHS,
            true,
            false,
            true, // total mode
        );
        assert_eq!(r.day_of_week_factor, 1.0);
    }

    #[test]
    fn day_of_week_factor_uses_weekday_slot_when_weekday_selected() {
        let r = day_month_factors(
            &[1.0; 12],
            &[2.0, 5.0], // weekday = 2.0, weekend = 5.0
            &ALL_MONTHS,
            true, // weekday selected
            false,
            false, // typical-day mode
        );
        assert_eq!(r.day_of_week_factor, 7.0 * 2.0);
    }

    #[test]
    fn day_of_week_factor_uses_weekend_slot_when_weekday_not_selected() {
        let r = day_month_factors(
            &[1.0; 12],
            &[2.0, 5.0],
            &ALL_MONTHS,
            false, // weekday not selected ⇒ weekend slot
            false,
            false,
        );
        assert_eq!(r.day_of_week_factor, 7.0 * 5.0);
    }

    #[test]
    fn month_and_day_totals_are_independent_of_daily_flag() {
        // mthf / ndays must be the same whether or not ldayfl is set.
        let monthly = [
            1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0,
        ];
        let with = day_month_factors(&monthly, &[1.0, 1.0], &ALL_MONTHS, true, true, false);
        let without = day_month_factors(&monthly, &[1.0, 1.0], &ALL_MONTHS, true, false, false);
        assert_eq!(with.month_factor, without.month_factor);
        assert_eq!(with.n_days, without.n_days);
    }
}
