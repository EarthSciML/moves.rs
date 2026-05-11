//! Static FIPS state-code table (`in1fip.f`–`in5fip.f`).
//!
//! In the Fortran source, five subroutines (`in1fip`–`in5fip`) populate
//! the global character arrays `statcd(53)` and `statnm(53)` with the
//! 5-character FIPS state codes (e.g. `"01000"` for Alabama) and the
//! corresponding state or territory names. The split into five files
//! is a 1997-era compiler workaround documented in the source headers;
//! semantically it is one ~600-line block of hard-coded initialization
//! that runs once at NONROAD startup.
//!
//! Per the migration plan (Task 100), the table moves to a compile-time
//! Rust static. Callers that previously indexed `statcd(N)` / `statnm(N)`
//! now consult [`STATES`] directly or use [`lookup_by_code`] /
//! [`lookup_by_name`]. The Fortran indices are 1-based; here [`STATES`]
//! is 0-based, so `statcd(N)` ↔ `STATES[N - 1].code`.
//!
//! County codes and per-state county counts (the historical `fipcod`,
//! `cntynm`, `nconty`, `idxcty` arrays) are intentionally *not* part of
//! this static table: those were removed from the FIPS initializers in
//! April 2005 — they are read at runtime from `data/allocate/FIPS.DAT`
//! by the input-side parser (`input::fips`).

use crate::common::consts::NSTATE;

/// One U.S. state or territory entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StateEntry {
    /// 5-character FIPS state code, e.g. `"01000"` for Alabama. The
    /// trailing `"000"` is the county portion, always zero in this
    /// table because these are state-level (not county) entries.
    pub code: &'static str,
    /// State or territory display name. Matches the spelling used in
    /// the original Fortran arrays (e.g. `"Wash DC"`, `"US Virgin Islands"`).
    pub name: &'static str,
}

/// All 53 U.S. states and territories tracked by NONROAD, in canonical
/// (Fortran-source) order. The array is sorted by [`StateEntry::code`].
///
/// Index 0 corresponds to `statcd(1)` / `statnm(1)` in the Fortran
/// source (Alabama); index 52 corresponds to `statcd(53)` (US Virgin
/// Islands).
pub static STATES: [StateEntry; NSTATE] = [
    // in1fip.f — entries 1..=14
    StateEntry { code: "01000", name: "Alabama" },
    StateEntry { code: "02000", name: "Alaska" },
    StateEntry { code: "04000", name: "Arizona" },
    StateEntry { code: "05000", name: "Arkansas" },
    StateEntry { code: "06000", name: "California" },
    StateEntry { code: "08000", name: "Colorado" },
    StateEntry { code: "09000", name: "Connecticut" },
    StateEntry { code: "10000", name: "Delaware" },
    StateEntry { code: "11000", name: "Wash DC" },
    StateEntry { code: "12000", name: "Florida" },
    StateEntry { code: "13000", name: "Georgia" },
    StateEntry { code: "15000", name: "Hawaii" },
    StateEntry { code: "16000", name: "Idaho" },
    StateEntry { code: "17000", name: "Illinois" },
    // in2fip.f — entries 15..=24
    StateEntry { code: "18000", name: "Indiana" },
    StateEntry { code: "19000", name: "Iowa" },
    StateEntry { code: "20000", name: "Kansas" },
    StateEntry { code: "21000", name: "Kentucky" },
    StateEntry { code: "22000", name: "Louisiana" },
    StateEntry { code: "23000", name: "Maine" },
    StateEntry { code: "24000", name: "Maryland" },
    StateEntry { code: "25000", name: "Massachusetts" },
    StateEntry { code: "26000", name: "Michigan" },
    StateEntry { code: "27000", name: "Minnesota" },
    // in3fip.f — entries 25..=35
    StateEntry { code: "28000", name: "Mississippi" },
    StateEntry { code: "29000", name: "Missouri" },
    StateEntry { code: "30000", name: "Montana" },
    StateEntry { code: "31000", name: "Nebraska" },
    StateEntry { code: "32000", name: "Nevada" },
    StateEntry { code: "33000", name: "New Hampshire" },
    StateEntry { code: "34000", name: "New Jersey" },
    StateEntry { code: "35000", name: "New Mexico" },
    StateEntry { code: "36000", name: "New York" },
    StateEntry { code: "37000", name: "North Carolina" },
    StateEntry { code: "38000", name: "North Dakota" },
    // in4fip.f — entries 36..=44
    StateEntry { code: "39000", name: "Ohio" },
    StateEntry { code: "40000", name: "Oklahoma" },
    StateEntry { code: "41000", name: "Oregon" },
    StateEntry { code: "42000", name: "Pennsylvania" },
    StateEntry { code: "44000", name: "Rhode Island" },
    StateEntry { code: "45000", name: "South Carolina" },
    StateEntry { code: "46000", name: "South Dakota" },
    StateEntry { code: "47000", name: "Tennessee" },
    StateEntry { code: "48000", name: "Texas" },
    // in5fip.f — entries 45..=53
    StateEntry { code: "49000", name: "Utah" },
    StateEntry { code: "50000", name: "Vermont" },
    StateEntry { code: "51000", name: "Virginia" },
    StateEntry { code: "53000", name: "Washington" },
    StateEntry { code: "54000", name: "West Virginia" },
    StateEntry { code: "55000", name: "Wisconsin" },
    StateEntry { code: "56000", name: "Wyoming" },
    StateEntry { code: "72000", name: "Puerto Rico" },
    StateEntry { code: "78000", name: "US Virgin Islands" },
];

/// Look up a state by its 5-character FIPS state code.
///
/// The lookup matches exactly on [`StateEntry::code`]. To accept a
/// 2-character state prefix (e.g. `"17"` for Illinois) or a full
/// county-bearing FIPS code (e.g. `"17031"` for Cook County, IL),
/// use [`lookup_by_state_prefix`] instead.
pub fn lookup_by_code(code: &str) -> Option<&'static StateEntry> {
    STATES
        .binary_search_by(|s| s.code.cmp(code))
        .ok()
        .map(|i| &STATES[i])
}

/// Look up a state by the leading two characters of a FIPS code.
///
/// Accepts both bare 2-character prefixes (`"17"`) and longer codes
/// (`"17000"`, `"17031"`) — only the first two characters are
/// inspected. This mirrors what NONROAD code does when it strips a
/// county FIPS down to its state portion before consulting `statcd`.
pub fn lookup_by_state_prefix(fips: &str) -> Option<&'static StateEntry> {
    if fips.len() < 2 {
        return None;
    }
    let prefix = &fips[..2];
    STATES.iter().find(|s| &s.code[..2] == prefix)
}

/// Look up a state by name (case-insensitive).
///
/// Spelling must match the entry in [`STATES`] — quirks like
/// `"Wash DC"` and `"US Virgin Islands"` are preserved from the
/// original Fortran arrays.
pub fn lookup_by_name(name: &str) -> Option<&'static StateEntry> {
    STATES
        .iter()
        .find(|s| s.name.eq_ignore_ascii_case(name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_has_53_entries() {
        assert_eq!(STATES.len(), NSTATE);
        assert_eq!(STATES.len(), 53);
    }

    #[test]
    fn codes_are_sorted_and_unique() {
        for pair in STATES.windows(2) {
            assert!(
                pair[0].code < pair[1].code,
                "codes not strictly sorted: {} >= {}",
                pair[0].code,
                pair[1].code,
            );
        }
    }

    #[test]
    fn every_code_is_5_chars_ending_000() {
        for s in &STATES {
            assert_eq!(s.code.len(), 5, "code {:?} is not 5 chars", s.code);
            assert!(s.code.ends_with("000"), "code {:?} lacks 000 county suffix", s.code);
            assert!(
                s.code.chars().all(|c| c.is_ascii_digit()),
                "code {:?} has non-digit",
                s.code,
            );
        }
    }

    #[test]
    fn no_blank_names() {
        for s in &STATES {
            assert!(!s.name.trim().is_empty(), "blank name at code {}", s.code);
        }
    }

    #[test]
    fn lookup_by_code_finds_known_states() {
        assert_eq!(lookup_by_code("01000").unwrap().name, "Alabama");
        assert_eq!(lookup_by_code("17000").unwrap().name, "Illinois");
        assert_eq!(lookup_by_code("48000").unwrap().name, "Texas");
        assert_eq!(lookup_by_code("78000").unwrap().name, "US Virgin Islands");
    }

    #[test]
    fn lookup_by_code_rejects_unknown() {
        assert!(lookup_by_code("14000").is_none()); // gap between IA-OH territory codes
        assert!(lookup_by_code("99999").is_none());
        assert!(lookup_by_code("").is_none());
        assert!(lookup_by_code("17031").is_none()); // a county code, not a state code
    }

    #[test]
    fn lookup_by_state_prefix_accepts_county_codes() {
        assert_eq!(lookup_by_state_prefix("17031").unwrap().name, "Illinois");
        assert_eq!(lookup_by_state_prefix("06037").unwrap().name, "California");
        assert_eq!(lookup_by_state_prefix("17").unwrap().name, "Illinois");
        assert!(lookup_by_state_prefix("99").is_none());
        assert!(lookup_by_state_prefix("1").is_none());
    }

    #[test]
    fn lookup_by_name_is_case_insensitive() {
        assert_eq!(lookup_by_name("Alabama").unwrap().code, "01000");
        assert_eq!(lookup_by_name("alabama").unwrap().code, "01000");
        assert_eq!(lookup_by_name("ALABAMA").unwrap().code, "01000");
        assert_eq!(lookup_by_name("New York").unwrap().code, "36000");
        assert_eq!(lookup_by_name("wash dc").unwrap().code, "11000");
        assert!(lookup_by_name("Atlantis").is_none());
    }

    #[test]
    fn fortran_1_based_index_round_trip() {
        // statcd(1) == "01000" Alabama in the Fortran source
        assert_eq!(STATES[0].code, "01000");
        assert_eq!(STATES[0].name, "Alabama");
        // statcd(53) == "78000" US Virgin Islands
        assert_eq!(STATES[NSTATE - 1].code, "78000");
        assert_eq!(STATES[NSTATE - 1].name, "US Virgin Islands");
    }
}
