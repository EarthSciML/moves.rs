//! Date/time formatting helper (`getime.f`).
//!
//! Task 99. The Fortran `getime.f` writes the current local
//! date/time into a Fortran character variable using the format
//! `"Mon DD HH:MM:SS: YYYY"` (3-letter month, day-of-month, 24-hour
//! time, year — all separated by single spaces). The Rust port
//! preserves the format and exposes both the pure formatter
//! ([`format_components`]) and a system-clock wrapper
//! ([`format_now`]).
//!
//! The wrapper uses [`std::time::SystemTime::now`] interpreted as
//! UTC; the Fortran source returns local time. The discrepancy is
//! immaterial for message-file headers (the only consumer in the
//! NONROAD pipeline) and avoids pulling in a timezone-aware
//! dependency that would inflate the WASM build.
//!
//! # Fortran source
//!
//! Ports `getime.f` (87 lines).

const MONTHS: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

/// Format the supplied civil-time components into the Fortran header
/// string. Returns the same shape `getime.f` produces.
pub fn format_components(
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
) -> String {
    let month_name = if (1..=12).contains(&month) {
        MONTHS[(month - 1) as usize]
    } else {
        "???"
    };
    format!(
        "{m} {d:02} {h:02}:{mn:02}:{s:02}: {y:>4}",
        m = month_name,
        d = day,
        h = hour,
        mn = minute,
        s = second,
        y = year,
    )
}

/// Format the current wall-clock time using the Fortran header style.
///
/// Computed against UTC; see the module docstring for the rationale.
#[cfg(not(target_arch = "wasm32"))]
pub fn format_now() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let (year, month, day, hour, minute, second) = unix_seconds_to_civil(secs);
    format_components(year, month, day, hour, minute, second)
}

/// Convert seconds-since-UNIX-epoch to `(year, month, day, hour, minute, second)`
/// using the Hinnant civil-time algorithm. Always interprets `secs`
/// as UTC.
///
/// Documented at <https://howardhinnant.github.io/date_algorithms.html>.
fn unix_seconds_to_civil(secs: i64) -> (i32, u32, u32, u32, u32, u32) {
    let days = secs.div_euclid(86_400);
    let time_of_day = secs.rem_euclid(86_400) as u32;
    let hour = time_of_day / 3600;
    let minute = (time_of_day % 3600) / 60;
    let second = time_of_day % 60;
    let (year, month, day) = civil_from_days(days);
    (year, month, day, hour, minute, second)
}

fn civil_from_days(z: i64) -> (i32, u32, u32) {
    let z = z + 719_468; // 1970-01-01 as days from era epoch (0000-03-01).
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u32; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe as i32 + era as i32 * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_canonical_example() {
        // Mar 14 12:34:56:  2025 — straightforward case.
        let s = format_components(2025, 3, 14, 12, 34, 56);
        assert_eq!(s, "Mar 14 12:34:56: 2025");
    }

    #[test]
    fn pads_single_digit_fields() {
        let s = format_components(2025, 1, 3, 9, 5, 7);
        assert_eq!(s, "Jan 03 09:05:07: 2025");
    }

    #[test]
    fn pads_short_year() {
        let s = format_components(99, 12, 31, 23, 59, 59);
        // Fortran I4 right-justifies on width 4 with leading spaces.
        assert_eq!(s, "Dec 31 23:59:59:   99");
    }

    #[test]
    fn handles_out_of_range_month() {
        let s = format_components(2025, 13, 1, 0, 0, 0);
        assert!(s.starts_with("???"));
    }

    #[test]
    fn epoch_civil_conversion_matches_known_points() {
        // 1970-01-01T00:00:00Z
        assert_eq!(unix_seconds_to_civil(0), (1970, 1, 1, 0, 0, 0));
        // 2000-01-01T00:00:00Z = 946_684_800
        assert_eq!(unix_seconds_to_civil(946_684_800), (2000, 1, 1, 0, 0, 0));
        // 2020-02-29T12:34:56Z = 1_582_979_696
        assert_eq!(
            unix_seconds_to_civil(1_582_979_696),
            (2020, 2, 29, 12, 34, 56)
        );
        // 2024-12-31T23:59:59Z = 1_735_689_599
        assert_eq!(
            unix_seconds_to_civil(1_735_689_599),
            (2024, 12, 31, 23, 59, 59)
        );
    }
}
