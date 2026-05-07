//! Canonical snapshot format constants and primitive helpers.
//!
//! See the crate-level docs in `lib.rs` for the on-disk layout.

use serde::{Deserialize, Serialize};

/// On-disk format version for the manifest. Bumped on incompatible changes.
pub const FORMAT_VERSION: &str = "moves-snapshot/v1";

/// Decimal places used when stringifying float values for storage.
///
/// 12 places gives `1e-12` absolute precision — well under the ~15-17
/// significant decimal digits an f64 can represent — while leaving headroom
/// for downstream comparisons. Round-trip: format(round(x, 1e-12)) is stable
/// across platforms and float-formatting libraries because the output is a
/// fixed-decimal string, not platform-formatted scientific notation.
pub const FLOAT_DECIMALS: u32 = 12;

/// Identifier this writer stamps into the parquet footer's `created_by`
/// field. Hardcoded to keep parquet bytes byte-identical across builds.
pub const PARQUET_CREATED_BY: &str = "moves-snapshot";

/// Logical column kind. The wire format depends on the kind:
///
/// | kind     | parquet stored type | rationale                              |
/// |----------|---------------------|----------------------------------------|
/// | Int64    | INT64               | exact representation                   |
/// | Float64  | BYTE_ARRAY (utf8)   | rounded + fixed-decimal string         |
/// | Utf8     | BYTE_ARRAY (utf8)   | direct                                 |
/// | Boolean  | BOOLEAN             | direct                                 |
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

/// Format a float as a fixed-decimal string with `decimals` digits after the
/// dot. Canonicalizes `-0` to `0`, NaN to `"NaN"`, and infinities to
/// `"Infinity"` / `"-Infinity"`. Round-trip stable: parsing the output back to
/// f64 and reformatting yields the same string.
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

/// Inverse of `float_to_fixed_decimal` for diffing. Returns `None` for the
/// canonicalized "missing" string, otherwise an f64 (which may be NaN/Inf).
pub fn parse_fixed_decimal(s: &str) -> Option<f64> {
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

    #[test]
    fn float_canonicalization() {
        assert_eq!(float_to_fixed_decimal(0.0, 12), "0.000000000000");
        assert_eq!(float_to_fixed_decimal(-0.0, 12), "0.000000000000");
        assert_eq!(float_to_fixed_decimal(1.5, 12), "1.500000000000");
        assert_eq!(float_to_fixed_decimal(-1.5, 12), "-1.500000000000");
        assert_eq!(float_to_fixed_decimal(f64::NAN, 12), "NaN");
        assert_eq!(float_to_fixed_decimal(f64::INFINITY, 12), "Infinity");
        assert_eq!(float_to_fixed_decimal(f64::NEG_INFINITY, 12), "-Infinity");
    }

    #[test]
    fn float_rounds_to_precision() {
        // 1e-13 perturbation should round away.
        let lhs = float_to_fixed_decimal(1.0, 12);
        let rhs = float_to_fixed_decimal(1.0 + 1e-13, 12);
        assert_eq!(lhs, rhs);
    }

    #[test]
    fn parse_round_trip() {
        for v in [0.0_f64, 1.5, -1.5, 1e-12, -1e10, std::f64::consts::PI] {
            let s = float_to_fixed_decimal(v, 12);
            let parsed = parse_fixed_decimal(&s).unwrap();
            assert!((parsed - v).abs() < 1e-9, "{v} -> {s} -> {parsed}");
        }
    }
}
