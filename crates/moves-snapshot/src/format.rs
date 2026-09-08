//! Canonical snapshot format constants and primitive helpers.
//!
//! See the crate-level docs in `lib.rs` for the on-disk layout.

use serde::{Deserialize, Serialize};

/// On-disk format version this writer produces. Bumped on incompatible
/// changes.
///
/// * `moves-snapshot/v1` stored floats as fixed-decimal strings with
///   [`V1_FLOAT_DECIMALS`] places after the point.
/// * `moves-snapshot/v2` stores floats as the shortest decimal string that
///   parses back to the identical f64 — see [`float_to_canonical`].
pub const FORMAT_VERSION: &str = "moves-snapshot/v2";

/// Every format version this crate can *read*. The writer only ever emits
/// [`FORMAT_VERSION`], but the reader accepts older snapshots so an existing
/// corpus stays diffable across a format bump.
pub const SUPPORTED_FORMAT_VERSIONS: &[&str] = &["moves-snapshot/v1", "moves-snapshot/v2"];

/// Decimal places `moves-snapshot/v1` used when stringifying floats.
///
/// Retained so v1 snapshots can still be read, described and compared. Do not
/// use it for new captures: twelve places *after the point* is not twelve
/// significant digits, so the number of digits a value kept depended on its
/// magnitude. A value near 1e-9 kept four; one near 1e-11 kept two; anything
/// below 5e-13 was flushed to `0.000000000000`. See
/// `characterization/audit-results/20260908T0948-float-precision-blast-radius.md`
/// for the measured cost across the corpus.
pub const V1_FLOAT_DECIMALS: u32 = 12;

/// Upper bound on the significant decimal digits `moves-snapshot/v2` will
/// emit. 17 digits is enough to round-trip every finite f64 (IEEE 754
/// binary64 needs at most 17), so the search in [`float_to_canonical`] always
/// terminates with an exact answer.
pub const MAX_SIGNIFICANT_DIGITS: u32 = 17;

/// Identifier this writer stamps into the parquet footer's `created_by`
/// field. Hardcoded to keep parquet bytes byte-identical across builds.
pub const PARQUET_CREATED_BY: &str = "moves-snapshot";

/// How a snapshot encodes `float64` cells, recorded per table in the
/// `.meta.json` sidecar so a reader can tell which rule produced the bytes it
/// is holding without guessing from the format version string.
///
/// Downstream comparison tools use [`FloatEncoding::absolute_quantum`] /
/// [`FloatEncoding::relative_quantum`] to floor their tolerances at whatever
/// the *capture* was capable of recording.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FloatEncoding {
    /// `moves-snapshot/v1`: `format!("{:.*}", decimals, round(x))`. Absolute
    /// quantum `0.5 * 10^-decimals`, independent of magnitude.
    FixedDecimals { decimals: u32 },
    /// `moves-snapshot/v2`: the shortest correctly-rounded decimal string that
    /// parses back to the identical f64 bit pattern, in normalized scientific
    /// notation. Lossless: the stored string and the source double denote the
    /// same number.
    ShortestRoundTrip { max_significant_digits: u32 },
}

impl FloatEncoding {
    /// The encoding `moves-snapshot/v2` writes.
    pub const CURRENT: FloatEncoding = FloatEncoding::ShortestRoundTrip {
        max_significant_digits: MAX_SIGNIFICANT_DIGITS,
    };

    /// The encoding `moves-snapshot/v1` wrote.
    pub const V1: FloatEncoding = FloatEncoding::FixedDecimals {
        decimals: V1_FLOAT_DECIMALS,
    };

    /// Largest absolute difference the storage rule can introduce between the
    /// source double and the stored string, for a value of magnitude `x`.
    ///
    /// This is the floor of any meaningful absolute comparison tolerance: two
    /// implementations that agree to better than this cannot be told apart in
    /// the corpus.
    pub fn absolute_quantum(self, x: f64) -> f64 {
        match self {
            FloatEncoding::FixedDecimals { decimals } => 0.5 * 10f64.powi(-(decimals as i32)),
            // Lossless — the only error left is the f64 the capture already
            // held, which is the same object the consumer is compared against.
            FloatEncoding::ShortestRoundTrip { .. } => {
                let _ = x;
                0.0
            }
        }
    }

    /// Largest *relative* difference the storage rule can introduce for a
    /// value of magnitude `x`. Returns `f64::INFINITY` for a value the rule
    /// flushes to zero.
    pub fn relative_quantum(self, x: f64) -> f64 {
        let q = self.absolute_quantum(x);
        if q == 0.0 {
            return 0.0;
        }
        let m = x.abs();
        if m == 0.0 || m < q {
            f64::INFINITY
        } else {
            q / m
        }
    }

    /// Encode `x` under this rule.
    pub fn encode(self, x: f64) -> String {
        match self {
            FloatEncoding::FixedDecimals { decimals } => float_to_fixed_decimal(x, decimals),
            FloatEncoding::ShortestRoundTrip { .. } => float_to_canonical(x),
        }
    }
}

/// A format version paired with the float rule that version uses. Threaded
/// through the writer so a snapshot loaded as v1 is rewritten as v1 (bytes
/// unchanged) while a fresh capture is written as v2.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FormatProfile {
    pub version: String,
    pub float_encoding: FloatEncoding,
}

impl FormatProfile {
    /// The profile new captures are written with.
    pub fn current() -> Self {
        Self {
            version: FORMAT_VERSION.to_string(),
            float_encoding: FloatEncoding::CURRENT,
        }
    }

    /// Resolve a profile from a manifest's `format_version`. Returns `None`
    /// for a version this crate cannot read.
    pub fn for_version(version: &str) -> Option<Self> {
        let float_encoding = match version {
            "moves-snapshot/v1" => FloatEncoding::V1,
            "moves-snapshot/v2" => FloatEncoding::CURRENT,
            _ => return None,
        };
        Some(Self {
            version: version.to_string(),
            float_encoding,
        })
    }
}

impl Default for FormatProfile {
    fn default() -> Self {
        Self::current()
    }
}

/// Logical column kind. The wire format depends on the kind:
///
/// | kind | parquet stored type | rationale |
/// |----------|---------------------|----------------------------------------|
/// | Int64 | INT64 | exact representation |
/// | Float64 | BYTE_ARRAY (utf8) | canonical decimal string |
/// | Utf8 | BYTE_ARRAY (utf8) | direct |
/// | Boolean | BOOLEAN | direct |
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnKind {
    Int64,
    Float64,
    Utf8,
    Boolean,
}

impl ColumnKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ColumnKind::Int64 => "int64",
            ColumnKind::Float64 => "float64",
            ColumnKind::Utf8 => "utf8",
            ColumnKind::Boolean => "boolean",
        }
    }
}

/// One column in a table schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnSpec {
    pub name: String,
    pub kind: ColumnKind,
}

/// Encode `x` as the canonical `moves-snapshot/v2` decimal string.
///
/// The rule, stated so it does not depend on any particular formatting
/// implementation:
///
/// > Let `k` be the smallest integer in `1..=17` such that the decimal with
/// > `k` significant digits **correctly rounded** from `x` parses back to a
/// > bit-identical f64. Write that decimal in normalized scientific notation:
/// > one digit before the point, the remaining `k-1` after it (omitting the
/// > point entirely when `k == 1`), then `e`, then the exponent with an
/// > explicit sign and at least two digits.
///
/// Every step is mathematically pinned:
///
/// * Correctly-rounded fixed-precision decimal output is *unique* (Rust's
///   `{:.*e}` computes it exactly, with a bignum path, and round-half-to-even
///   at the tie). Unlike shortest-representation printing, there is no
///   "which of several equally short candidates" freedom to resolve.
/// * f64 parsing of a decimal literal is correctly rounded and equally
///   unique.
/// * Round-trip is verified rather than assumed: the search *selects* on it,
///   and 17 digits always succeeds, so the function cannot return a string
///   that loses information.
/// * There is no locale, no float-formatting mode, no platform dependence,
///   and no dependence on the total ordering of anything.
///
/// `-0.0` canonicalizes to `0e+00`, NaN to `"NaN"`, and the infinities to
/// `"Infinity"` / `"-Infinity"`, matching v1.
///
/// ```
/// # use moves_snapshot::format::float_to_canonical;
/// assert_eq!(float_to_canonical(1.5), "1.5e+00");
/// assert_eq!(float_to_canonical(1.105e-9), "1.105e-09");
/// assert_eq!(float_to_canonical(0.0), "0e+00");
/// // The v1 rule kept two significant digits here; v2 keeps all of them.
/// assert_eq!(float_to_canonical(1.9e-11), "1.9e-11");
/// ```
pub fn float_to_canonical(x: f64) -> String {
    if x.is_nan() {
        return "NaN".to_string();
    }
    if x.is_infinite() {
        return if x > 0.0 {
            "Infinity".to_string()
        } else {
            "-Infinity".to_string()
        };
    }
    // Covers both +0.0 and -0.0; the sign of zero is not information the
    // corpus carries.
    if x == 0.0 {
        return "0e+00".to_string();
    }

    // Round-trip success is monotone in the digit count: if `k` digits
    // identify `x`, so does `k + 1` (the extra digit can only shrink the
    // interval around `x`). So binary-search the smallest `k` rather than
    // scanning — 5 format+parse pairs instead of up to 17.
    let mut lo = 1u32;
    let mut hi = MAX_SIGNIFICANT_DIGITS;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if round_trips(x, mid) {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    debug_assert!(
        round_trips(x, lo),
        "17 significant digits must round-trip every finite f64; {x:?} did not"
    );

    canonicalize_exponential(&format!("{:.*e}", (lo - 1) as usize, x))
}

fn round_trips(x: f64, digits: u32) -> bool {
    let s = format!("{:.*e}", (digits - 1) as usize, x);
    match s.parse::<f64>() {
        Ok(back) => back.to_bits() == x.to_bits(),
        Err(_) => false,
    }
}

/// Re-lay-out `<mantissa>e<exp>` into the canonical spelling. Done here rather
/// than trusting the formatter's exponent conventions, so the on-disk shape is
/// owned by this crate.
fn canonicalize_exponential(raw: &str) -> String {
    let (mantissa, exp) = raw
        .split_once('e')
        .expect("`{:e}` always emits an `e` separator");
    let exp: i32 = exp.parse().expect("`{:e}` always emits a decimal exponent");
    let sign = if exp < 0 { '-' } else { '+' };
    format!("{mantissa}e{sign}{:02}", exp.unsigned_abs())
}

/// Format a float as a fixed-decimal string with `decimals` digits after the
/// dot — the `moves-snapshot/v1` rule.
///
/// Retained to read, re-emit and reason about v1 snapshots. **Lossy**: see
/// [`V1_FLOAT_DECIMALS`]. New captures use [`float_to_canonical`].
///
/// Canonicalizes `-0` to `0`, NaN to `"NaN"`, and infinities to `"Infinity"` /
/// `"-Infinity"`.
pub fn float_to_fixed_decimal(x: f64, decimals: u32) -> String {
    if x.is_nan() {
        return "NaN".to_string();
    }
    if x.is_infinite() {
        return if x > 0.0 {
            "Infinity".to_string()
        } else {
            "-Infinity".to_string()
        };
    }

    // Round at the requested precision. Multiplying by 10^decimals can lose
    // precision for very large values, but for any value within the f64
    // representable range the resulting string is well-defined and
    // deterministic — which is what byte-identical round-trip requires.
    let scale = 10f64.powi(decimals as i32);
    let rounded = (x * scale).round() / scale;

    // Canonicalize -0.0 to 0.0 so the leading sign doesn't depend on the
    // sign of zero.
    let canonical = if rounded == 0.0 { 0.0 } else { rounded };

    format!("{:.*}", decimals as usize, canonical)
}

/// Parse a stored float cell back to f64, for diffing and tolerance checks.
///
/// Accepts both the v1 fixed-decimal spelling and the v2 canonical scientific
/// spelling — `str::parse::<f64>` handles either — plus the shared
/// NaN/Infinity sentinels. Returns `None` for anything else.
pub fn parse_canonical_float(s: &str) -> Option<f64> {
    match s {
        "NaN" => Some(f64::NAN),
        "Infinity" => Some(f64::INFINITY),
        "-Infinity" => Some(f64::NEG_INFINITY),
        other => other.parse::<f64>().ok(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- v1 rule (retained for reading the existing corpus) ----------------

    #[test]
    fn v1_float_canonicalization() {
        assert_eq!(float_to_fixed_decimal(0.0, 12), "0.000000000000");
        assert_eq!(float_to_fixed_decimal(-0.0, 12), "0.000000000000");
        assert_eq!(float_to_fixed_decimal(1.5, 12), "1.500000000000");
        assert_eq!(float_to_fixed_decimal(-1.5, 12), "-1.500000000000");
        assert_eq!(float_to_fixed_decimal(f64::NAN, 12), "NaN");
        assert_eq!(float_to_fixed_decimal(f64::INFINITY, 12), "Infinity");
        assert_eq!(float_to_fixed_decimal(f64::NEG_INFINITY, 12), "-Infinity");
    }

    #[test]
    fn v1_rounds_to_precision() {
        // 1e-13 perturbation should round away.
        let lhs = float_to_fixed_decimal(1.0, 12);
        let rhs = float_to_fixed_decimal(1.0 + 1e-13, 12);
        assert_eq!(lhs, rhs);
    }

    /// The defect this format version exists to fix, pinned as a test so the
    /// v1 rule's behaviour stays documented rather than remembered. These are
    /// the two values actually stored in
    /// `nrdioxinemissionrate.meanBaseRate` in the committed corpus.
    #[test]
    fn v1_destroys_small_magnitude_precision() {
        assert_eq!(float_to_fixed_decimal(1.105e-9, 12), "0.000000001105");
        assert_eq!(float_to_fixed_decimal(1.9e-11, 12), "0.000000000019");
        // Below the quantum: gone entirely.
        assert_eq!(float_to_fixed_decimal(4.7e-13, 12), "0.000000000000");
    }

    // ---- v2 rule -----------------------------------------------------------

    #[test]
    fn canonical_specials_and_zero() {
        assert_eq!(float_to_canonical(0.0), "0e+00");
        assert_eq!(float_to_canonical(-0.0), "0e+00");
        assert_eq!(float_to_canonical(f64::NAN), "NaN");
        assert_eq!(float_to_canonical(f64::INFINITY), "Infinity");
        assert_eq!(float_to_canonical(f64::NEG_INFINITY), "-Infinity");
    }

    /// Golden strings. These lock the on-disk spelling: if a toolchain bump
    /// ever changed the formatter's digit selection this fails loudly instead
    /// of silently producing a corpus that no longer matches the committed
    /// hashes.
    #[test]
    fn canonical_golden_spellings() {
        let cases: &[(f64, &str)] = &[
            (1.0, "1e+00"),
            (-1.0, "-1e+00"),
            (1.5, "1.5e+00"),
            (-1.5, "-1.5e+00"),
            (10.0, "1e+01"),
            (100.0, "1e+02"),
            (0.5, "5e-01"),
            (0.1, "1e-01"),
            (1.0 / 3.0, "3.333333333333333e-01"),
            (std::f64::consts::PI, "3.141592653589793e+00"),
            (1.105e-9, "1.105e-09"),
            (1.9e-11, "1.9e-11"),
            (4.7e-13, "4.7e-13"),
            (1e-300, "1e-300"),
            (1e300, "1e+300"),
            (f64::MAX, "1.7976931348623157e+308"),
            (f64::MIN_POSITIVE, "2.2250738585072014e-308"),
            // Smallest subnormal.
            (5e-324, "5e-324"),
            (f64::from_bits(1), "5e-324"),
        ];
        for &(v, expect) in cases {
            assert_eq!(float_to_canonical(v), expect, "value {v:?}");
        }
    }

    /// Round-trip fidelity is the whole point: every finite f64 must survive.
    #[test]
    fn canonical_round_trips_exactly() {
        let mut values: Vec<f64> = vec![
            0.0,
            -0.0,
            1.0,
            -1.0,
            0.1,
            1.0 / 3.0,
            std::f64::consts::PI,
            std::f64::consts::E,
            1e-300,
            1e300,
            f64::MAX,
            f64::MIN,
            f64::MIN_POSITIVE,
            // Denormals.
            f64::from_bits(1),
            f64::from_bits(2),
            f64::from_bits(0x000f_ffff_ffff_ffff),
            // The corpus's problem cases.
            1.105e-9,
            1.9e-11,
            4.7e-13,
        ];
        // A deterministic pseudo-random sweep over the whole exponent range,
        // including denormal and huge magnitudes.
        let mut state: u64 = 0x2545_f491_4f6c_dd1d;
        for _ in 0..20_000 {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let v = f64::from_bits(state);
            if v.is_finite() {
                values.push(v);
            }
        }
        for v in values {
            let s = float_to_canonical(v);
            let back = parse_canonical_float(&s).unwrap();
            assert_eq!(
                back.to_bits(),
                // -0.0 deliberately canonicalizes to +0.0.
                if v == 0.0 {
                    0.0f64.to_bits()
                } else {
                    v.to_bits()
                },
                "{v:?} -> {s} -> {back:?}"
            );
        }
    }

    /// The selected digit count is minimal: dropping one digit must break the
    /// round-trip. Guards against a search bug silently emitting 17 digits for
    /// everything (which would still round-trip, but bloat the corpus and make
    /// diffs unreadable).
    #[test]
    fn canonical_digit_count_is_minimal() {
        let mut state: u64 = 0x9e37_79b9_7f4a_7c15;
        let mut checked = 0;
        for _ in 0..5_000 {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let v = f64::from_bits(state);
            if !v.is_finite() || v == 0.0 {
                continue;
            }
            let s = float_to_canonical(v);
            let mantissa = s.split('e').next().unwrap();
            let digits = mantissa.chars().filter(char::is_ascii_digit).count() as u32;
            assert!((1..=MAX_SIGNIFICANT_DIGITS).contains(&digits));
            if digits > 1 {
                assert!(
                    !round_trips(v, digits - 1),
                    "{v:?} -> {s} is not minimal: {} digits also round-trips",
                    digits - 1
                );
            }
            // A minimal spelling never ends in a redundant zero.
            assert!(!(mantissa.contains('.') && mantissa.ends_with('0')), "{s}");
            checked += 1;
        }
        assert!(checked > 4_000);
    }

    /// Same input, same bytes — as many times as you like, from any order of
    /// calls. The corpus's determinism contract in miniature.
    #[test]
    fn canonical_is_deterministic() {
        let mut state: u64 = 0x1234_5678_9abc_def0;
        let mut vals = Vec::new();
        for _ in 0..2_000 {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let v = f64::from_bits(state);
            if v.is_finite() {
                vals.push(v);
            }
        }
        let first: Vec<String> = vals.iter().map(|&v| float_to_canonical(v)).collect();
        // Reverse order, interleaved with other work, then compare.
        let mut second: Vec<String> = vals
            .iter()
            .rev()
            .map(|&v| {
                let _ = float_to_canonical(v * 3.0);
                float_to_canonical(v)
            })
            .collect();
        second.reverse();
        assert_eq!(first, second);
    }

    /// The v2 spelling is the same *length* as the stdlib's own
    /// shortest-round-trip printer, and both round-trip — an independent
    /// cross-check on the binary search, since the two reach the digit count
    /// by different algorithms.
    ///
    /// They do not always agree on the final *digit*, which is exactly why
    /// this crate does not delegate to `{:e}` — see
    /// [`canonical_breaks_exact_ties_to_even`].
    #[test]
    fn canonical_matches_stdlib_shortest_length() {
        let mut state: u64 = 0xdead_beef_cafe_1234;
        let mut differing = 0;
        let mut checked = 0;
        for _ in 0..5_000 {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let v = f64::from_bits(state);
            if !v.is_finite() || v == 0.0 {
                continue;
            }
            let ours = float_to_canonical(v);
            let theirs = canonicalize_exponential(&format!("{v:e}"));
            assert_eq!(ours.len(), theirs.len(), "{v:?}: {ours} vs {theirs}");
            assert_eq!(parse_canonical_float(&ours).unwrap().to_bits(), v.to_bits());
            if ours != theirs {
                // Only ever the last digit, and only by one.
                let a = ours.as_bytes();
                let b = theirs.as_bytes();
                let diffs: Vec<usize> = (0..a.len()).filter(|&i| a[i] != b[i]).collect();
                assert_eq!(diffs.len(), 1, "{ours} vs {theirs}");
                assert_eq!(a[diffs[0]].abs_diff(b[diffs[0]]), 1, "{ours} vs {theirs}");
                differing += 1;
            }
            checked += 1;
        }
        assert!(checked > 4_000);
        // Ties are rare but real; if this ever hits zero the cross-check has
        // stopped exercising the interesting case.
        assert!(differing > 0, "expected some tie disagreements");
    }

    /// The reason the encoder is defined as "correctly rounded to k digits"
    /// rather than "whatever the shortest printer emits".
    ///
    /// `1871524575217767.3` is exactly `1871524575217767.25` as an f64. At 17
    /// significant digits that is an exact tie, and the two spellings
    /// `...7672` and `...7673` both round-trip. Round-half-to-even — the IEEE
    /// default and what correctly-rounded fixed-precision formatting does —
    /// picks `...7672`. Rust's shortest-round-trip printer picks `...7673`.
    /// Both are "correct" as round-trips; only one is *specified*, and the
    /// corpus needs the specified one.
    #[test]
    fn canonical_breaks_exact_ties_to_even() {
        let v = 1871524575217767.3_f64;
        assert_eq!(float_to_canonical(v), "1.8715245752177672e+15");
        assert_eq!(format!("{v:e}"), "1.8715245752177673e15");
        // Both round-trip; the tie is real, not a bug in either.
        assert_eq!("1.8715245752177672e+15".parse::<f64>().unwrap(), v);
        assert_eq!("1.8715245752177673e+15".parse::<f64>().unwrap(), v);
    }

    // ---- encoding descriptor ----------------------------------------------

    #[test]
    fn encoding_quanta() {
        let v1 = FloatEncoding::V1;
        assert_eq!(v1.absolute_quantum(1.0), 0.5e-12);
        assert_eq!(v1.absolute_quantum(1e-9), 0.5e-12);
        // 1e-9 keeps ~4 significant digits: relative quantum 5e-4.
        assert!((v1.relative_quantum(1e-9) - 5e-4).abs() < 1e-18);
        // Below the quantum entirely: no relative information survives.
        assert_eq!(v1.relative_quantum(1e-14), f64::INFINITY);
        assert_eq!(v1.relative_quantum(0.0), f64::INFINITY);

        let v2 = FloatEncoding::CURRENT;
        assert_eq!(v2.absolute_quantum(1e-300), 0.0);
        assert_eq!(v2.relative_quantum(1e-300), 0.0);
        assert_eq!(v2.relative_quantum(0.0), 0.0);
    }

    #[test]
    fn encoding_serde_is_self_describing() {
        let v1 = serde_json::to_string(&FloatEncoding::V1).unwrap();
        assert_eq!(v1, r#"{"kind":"fixed_decimals","decimals":12}"#);
        let v2 = serde_json::to_string(&FloatEncoding::CURRENT).unwrap();
        assert_eq!(
            v2,
            r#"{"kind":"shortest_round_trip","max_significant_digits":17}"#
        );
        assert_eq!(
            serde_json::from_str::<FloatEncoding>(&v1).unwrap(),
            FloatEncoding::V1
        );
        assert_eq!(
            serde_json::from_str::<FloatEncoding>(&v2).unwrap(),
            FloatEncoding::CURRENT
        );
    }

    #[test]
    fn profiles_resolve_by_version() {
        assert_eq!(
            FormatProfile::for_version("moves-snapshot/v1")
                .unwrap()
                .float_encoding,
            FloatEncoding::V1
        );
        assert_eq!(
            FormatProfile::for_version("moves-snapshot/v2")
                .unwrap()
                .float_encoding,
            FloatEncoding::CURRENT
        );
        assert!(FormatProfile::for_version("moves-snapshot/v3").is_none());
        assert_eq!(FormatProfile::current().version, FORMAT_VERSION);
        for v in SUPPORTED_FORMAT_VERSIONS {
            assert!(FormatProfile::for_version(v).is_some());
        }
    }

    #[test]
    fn parse_accepts_both_spellings() {
        assert_eq!(parse_canonical_float("0.000000001105"), Some(1.105e-9));
        assert_eq!(parse_canonical_float("1.105e-09"), Some(1.105e-9));
        assert_eq!(parse_canonical_float("0e+00"), Some(0.0));
        assert_eq!(parse_canonical_float("0.000000000000"), Some(0.0));
        assert!(parse_canonical_float("NaN").unwrap().is_nan());
        assert_eq!(parse_canonical_float("Infinity"), Some(f64::INFINITY));
        assert_eq!(parse_canonical_float("-Infinity"), Some(f64::NEG_INFINITY));
        assert_eq!(parse_canonical_float("not a number"), None);
    }
}
