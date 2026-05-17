//! Parser for the `dbgemit` intermediate-state TSV format.
//!
//! The Phase 5 numerical-fidelity baseline is captured by the four
//! `dbgemit` instrumentation patches in
//! `characterization/nonroad-build/patches/`. When the locally-fixed
//! gfortran NONROAD runs with `NRDBG_FILE` set, each patched call
//! site appends one tab-separated record per emitted variable:
//!
//! ```text
//! <phase>\t<context>\t<label>\t<count>\t<v1>\t<v2>...
//! ```
//!
//! | Field     | Meaning                                              |
//! |-----------|------------------------------------------------------|
//! | `phase`   | `GETPOP`, `AGEDIST`, `GRWFAC`, or `CLCEMS`           |
//! | `context` | `key=val,key=val` tag string (fips, scc, year, call) |
//! | `label`   | Variable name for the values on this line            |
//! | `count`   | Number of values that follow                         |
//! | `v1, v2…` | Tab-separated values (`real*4` or `integer*4`)       |
//!
//! See `characterization/nonroad-build/README.md` § "dbgemit
//! instrumentation" for the authoritative format description.
//!
//! This module is the *reference side* of the fidelity harness: it
//! turns a captured baseline TSV into structured [`ReferenceRecord`]s
//! that [`super::divergence`] diffs against the Rust port's output.
//! The same [`ReferenceRecord`] type carries the *actual* side too —
//! [`super::adapter`] builds them from live `moves-nonroad` outputs —
//! so the comparison engine works on one uniform record type.

use std::fmt;
use std::io::BufRead;

/// One of the four instrumented NONROAD subsystems.
///
/// Each variant corresponds to exactly one `dbgemit` patch and one
/// `moves-nonroad` module (see [`super::adapter`] for the mapping).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize)]
pub enum Phase {
    /// `getpop.f` — per-SCC population apportionment.
    Getpop,
    /// `agedist.f` — age-distribution growth (the migration plan's
    /// flagged numerical-fidelity risk; see ARCHITECTURE.md § 2.3).
    Agedist,
    /// `grwfac.f` — growth-factor application.
    Grwfac,
    /// `clcems.f` — exhaust-emissions calculation.
    Clcems,
}

impl Phase {
    /// Parse the `phase` field. Accepts the canonical upper-case
    /// spelling and is tolerant of surrounding whitespace and case.
    pub fn parse(field: &str) -> Option<Phase> {
        match field.trim().to_ascii_uppercase().as_str() {
            "GETPOP" => Some(Phase::Getpop),
            "AGEDIST" => Some(Phase::Agedist),
            "GRWFAC" => Some(Phase::Grwfac),
            "CLCEMS" => Some(Phase::Clcems),
            _ => None,
        }
    }

    /// The canonical upper-case label, as emitted by `dbgemit.f`.
    pub fn as_str(self) -> &'static str {
        match self {
            Phase::Getpop => "GETPOP",
            Phase::Agedist => "AGEDIST",
            Phase::Grwfac => "GRWFAC",
            Phase::Clcems => "CLCEMS",
        }
    }

    /// All four phases, in `dbgemit` declaration order. Lets the
    /// harness iterate the phase space without hard-coding the list.
    pub fn all() -> [Phase; 4] {
        [Phase::Getpop, Phase::Agedist, Phase::Grwfac, Phase::Clcems]
    }
}

impl fmt::Display for Phase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The parsed `context` tag string: an ordered list of `key=val`
/// pairs identifying *which* computation a record came from (FIPS,
/// SCC, year, and the per-call-site `call=N` counter).
///
/// Order is preserved as it appeared in the TSV for faithful
/// display; [`Context::canonical`] produces the key-sorted form used
/// to pair a reference record with its `moves-nonroad` counterpart.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Context {
    pairs: Vec<(String, String)>,
}

impl Context {
    /// Parse a `key=val,key=val` context string. Empty pieces (from
    /// a stray `,`) are dropped; a piece with no `=` becomes a key
    /// with an empty value. An empty input yields an empty context.
    pub fn parse(field: &str) -> Context {
        let mut pairs = Vec::new();
        for piece in field.split(',') {
            let piece = piece.trim();
            if piece.is_empty() {
                continue;
            }
            match piece.split_once('=') {
                Some((k, v)) => pairs.push((k.trim().to_string(), v.trim().to_string())),
                None => pairs.push((piece.to_string(), String::new())),
            }
        }
        Context { pairs }
    }

    /// Build a context directly from key/value pairs, without going
    /// through a `key=val` string. The constructor port-side
    /// instrumentation uses to tag a record with run-loop state
    /// (FIPS, SCC, year, call counter) it already holds.
    pub fn from_pairs<K, V, I>(pairs: I) -> Context
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        Context {
            pairs: pairs
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        }
    }

    /// The raw value for `key`, if present (first match wins).
    pub fn get(&self, key: &str) -> Option<&str> {
        self.pairs
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    /// `key`'s value parsed as an integer.
    pub fn get_i64(&self, key: &str) -> Option<i64> {
        self.get(key).and_then(|v| v.trim().parse().ok())
    }

    /// The `call=N` per-call-site counter, if the record carries one.
    pub fn call(&self) -> Option<i64> {
        self.get_i64("call")
    }

    /// `true` when the context carries no pairs.
    pub fn is_empty(&self) -> bool {
        self.pairs.is_empty()
    }

    /// Key-sorted `key=val,key=val` rendering. Two contexts that
    /// differ only in pair order produce the same canonical string,
    /// so this is the stable key for pairing reference and actual
    /// records in [`super::divergence`].
    pub fn canonical(&self) -> String {
        let mut sorted = self.pairs.clone();
        sorted.sort();
        sorted
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(",")
    }
}

impl fmt::Display for Context {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.pairs.is_empty() {
            return f.write_str("(none)");
        }
        let rendered = self
            .pairs
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(",");
        f.write_str(&rendered)
    }
}

/// One emitted variable: a phase, the context it came from, the
/// variable's `label`, and its `count` values.
///
/// A `dbgemit` line such as `AGEDIST  call=1,fips=26000  mdyrfrc  51
/// 0.10  0.50 …` becomes a `ReferenceRecord` with `phase =
/// Agedist`, `label = "mdyrfrc"`, and 51 `values`.
#[derive(Debug, Clone, PartialEq)]
pub struct ReferenceRecord {
    /// Which instrumented subsystem produced the record.
    pub phase: Phase,
    /// The `key=val` tag string identifying the computation.
    pub context: Context,
    /// The emitted variable's name.
    pub label: String,
    /// The emitted values (`real*4`/`integer*4`, widened to `f64`;
    /// both fit exactly — `i32` is well within `f64`'s 53-bit
    /// integer range and an `f32` widens losslessly).
    pub values: Vec<f64>,
}

impl ReferenceRecord {
    /// Construct a record. Used by [`super::adapter`] and tests.
    pub fn new(phase: Phase, context: Context, label: impl Into<String>, values: Vec<f64>) -> Self {
        ReferenceRecord {
            phase,
            context,
            label: label.into(),
            values,
        }
    }

    /// The pairing key: `(phase, canonical-context, label)`. Two
    /// records — one from the reference TSV, one from the Rust port
    /// — describe the same measurement exactly when their keys match.
    pub fn key(&self) -> RecordKey {
        RecordKey {
            phase: self.phase,
            context: self.context.canonical(),
            label: self.label.clone(),
        }
    }
}

/// The identity of a measurement, independent of its values. See
/// [`ReferenceRecord::key`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize)]
pub struct RecordKey {
    /// Originating phase.
    pub phase: Phase,
    /// Canonical (key-sorted) context string.
    pub context: String,
    /// Variable label.
    pub label: String,
}

impl fmt::Display for RecordKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} [{}] {}", self.phase, self.context, self.label)
    }
}

/// A parse failure, carrying the 1-based line number so a malformed
/// baseline can be pinpointed in a multi-megabyte capture.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    /// 1-based line number of the offending record.
    pub line: usize,
    /// Human-readable description of the fault.
    pub message: String,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "dbgemit parse error, line {}: {}",
            self.line, self.message
        )
    }
}

impl std::error::Error for ParseError {}

/// Parse a single floating-point value field.
///
/// Trims surrounding whitespace, then falls back to swapping a
/// Fortran `D`-exponent (`1.5D+03`) for `E` if the direct parse
/// fails — `dbgemit.f` emits `real*4` with an `E` exponent, but the
/// fallback keeps the parser robust against double-precision-style
/// formatting. Non-finite spellings (`NaN`, `Inf`) parse through;
/// the tolerance layer is responsible for flagging them.
fn parse_value(field: &str) -> Option<f64> {
    let trimmed = field.trim();
    if let Ok(v) = trimmed.parse::<f64>() {
        return Some(v);
    }
    let swapped: String = trimmed
        .chars()
        .map(|c| match c {
            'D' => 'E',
            'd' => 'e',
            other => other,
        })
        .collect();
    swapped.parse::<f64>().ok()
}

/// Parse a captured `dbgemit` baseline into [`ReferenceRecord`]s.
///
/// Blank lines and `#`-prefixed comment lines are skipped. Every
/// other line must have at least four tab-separated fields — phase,
/// context, label, count — followed by exactly `count` value fields.
/// Trailing empty value fields (a stray terminating tab) are
/// tolerated; an interior empty or unparseable value is an error.
///
/// # Errors
///
/// Returns the first [`ParseError`] encountered. The capture is
/// processed line by line, so an error reports the exact source line.
pub fn parse_reference<R: BufRead>(reader: R) -> Result<Vec<ReferenceRecord>, ParseError> {
    let mut records = Vec::new();
    for (idx, line) in reader.lines().enumerate() {
        let lineno = idx + 1;
        let line = line.map_err(|e| ParseError {
            line: lineno,
            message: format!("I/O error reading line: {e}"),
        })?;
        let line = line.trim_end_matches(['\r', '\n']);
        if line.trim().is_empty() || line.trim_start().starts_with('#') {
            continue;
        }

        let fields: Vec<&str> = line.split('\t').collect();
        if fields.len() < 4 {
            return Err(ParseError {
                line: lineno,
                message: format!(
                    "expected at least 4 tab-separated fields (phase, context, label, count), \
                     got {}",
                    fields.len()
                ),
            });
        }

        let phase = Phase::parse(fields[0]).ok_or_else(|| ParseError {
            line: lineno,
            message: format!(
                "unknown phase {:?}; expected one of GETPOP, AGEDIST, GRWFAC, CLCEMS",
                fields[0].trim()
            ),
        })?;

        let context = Context::parse(fields[1]);

        let label = fields[2].trim().to_string();
        if label.is_empty() {
            return Err(ParseError {
                line: lineno,
                message: "empty label field".to_string(),
            });
        }

        let count: usize = fields[3].trim().parse().map_err(|_| ParseError {
            line: lineno,
            message: format!(
                "count field {:?} is not a non-negative integer",
                fields[3].trim()
            ),
        })?;

        // Drop a stray trailing empty field (terminating tab) before
        // validating the count — but keep interior empties so a real
        // count mismatch still surfaces.
        let mut value_fields: Vec<&str> = fields[4..].to_vec();
        while value_fields.last().map(|s| s.trim().is_empty()) == Some(true) {
            value_fields.pop();
        }
        if value_fields.len() != count {
            return Err(ParseError {
                line: lineno,
                message: format!(
                    "count is {count} but {} value field(s) follow",
                    value_fields.len()
                ),
            });
        }

        let mut values = Vec::with_capacity(count);
        for (vidx, field) in value_fields.iter().enumerate() {
            match parse_value(field) {
                Some(v) => values.push(v),
                None => {
                    return Err(ParseError {
                        line: lineno,
                        message: format!("value #{} {:?} is not a number", vidx + 1, field.trim()),
                    })
                }
            }
        }

        records.push(ReferenceRecord {
            phase,
            context,
            label,
            values,
        });
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn parse(text: &str) -> Result<Vec<ReferenceRecord>, ParseError> {
        parse_reference(Cursor::new(text))
    }

    #[test]
    fn phase_parses_canonical_and_lenient_spellings() {
        assert_eq!(Phase::parse("AGEDIST"), Some(Phase::Agedist));
        assert_eq!(Phase::parse("  clcems "), Some(Phase::Clcems));
        assert_eq!(Phase::parse("GetPop"), Some(Phase::Getpop));
        assert_eq!(Phase::parse("nonroad"), None);
        assert_eq!(Phase::Grwfac.as_str(), "GRWFAC");
        assert_eq!(Phase::all().len(), 4);
    }

    #[test]
    fn context_parses_pairs_and_canonicalizes() {
        let ctx = Context::parse("call=2,fips=26000,year=2021");
        assert_eq!(ctx.get("fips"), Some("26000"));
        assert_eq!(ctx.get_i64("year"), Some(2021));
        assert_eq!(ctx.call(), Some(2));
        assert_eq!(ctx.get("missing"), None);
        // Canonical form is key-sorted regardless of input order.
        let reordered = Context::parse("year=2021,fips=26000,call=2");
        assert_eq!(ctx.canonical(), reordered.canonical());
        assert_eq!(ctx.canonical(), "call=2,fips=26000,year=2021");
    }

    #[test]
    fn context_tolerates_messy_input() {
        let ctx = Context::parse("  fips = 26000 , , bare ,call=1 ");
        assert_eq!(ctx.get("fips"), Some("26000"));
        assert_eq!(ctx.get("bare"), Some(""));
        assert_eq!(ctx.call(), Some(1));
        assert!(Context::parse("").is_empty());
        assert_eq!(format!("{}", Context::parse("")), "(none)");
    }

    #[test]
    fn context_from_pairs_builds_without_a_string() {
        let ctx = Context::from_pairs([("call", "1"), ("fips", "26000")]);
        assert_eq!(ctx.get("fips"), Some("26000"));
        assert_eq!(ctx.call(), Some(1));
        // Equivalent to the parsed form.
        assert_eq!(
            ctx.canonical(),
            Context::parse("fips=26000,call=1").canonical()
        );
    }

    #[test]
    fn parses_a_well_formed_multi_phase_capture() {
        let text = "\
# a comment line, skipped
GETPOP\tcall=1,scc=2270002000\tpopeqp\t2\t100.0\t250.5

AGEDIST\tcall=1,fips=26000\tbaspop\t1\t1000.0
AGEDIST\tcall=1,fips=26000\tmdyrfrc\t3\t0.1\t0.5\t0.4
GRWFAC\tcall=1,fips=26000\tfactor\t1\t0.025
";
        let records = parse(text).expect("well-formed capture must parse");
        assert_eq!(records.len(), 4);
        assert_eq!(records[0].phase, Phase::Getpop);
        assert_eq!(records[0].label, "popeqp");
        assert_eq!(records[0].values, vec![100.0, 250.5]);
        assert_eq!(records[2].label, "mdyrfrc");
        assert_eq!(records[2].values, vec![0.1, 0.5, 0.4]);
        assert_eq!(records[3].phase, Phase::Grwfac);
    }

    #[test]
    fn count_zero_record_has_no_values() {
        let records = parse("CLCEMS\tcall=1\temsday\t0").unwrap();
        assert_eq!(records.len(), 1);
        assert!(records[0].values.is_empty());
    }

    #[test]
    fn tolerates_trailing_tab() {
        let records = parse("GRWFAC\tcall=1\tfactor\t1\t0.5\t").unwrap();
        assert_eq!(records[0].values, vec![0.5]);
    }

    #[test]
    fn accepts_fortran_d_exponent() {
        let records = parse("CLCEMS\tcall=1\temsbmy\t1\t1.25D+03").unwrap();
        assert_eq!(records[0].values, vec![1250.0]);
    }

    #[test]
    fn non_finite_values_parse_through() {
        let records = parse("CLCEMS\tcall=1\temsday\t2\tNaN\tInf").unwrap();
        assert!(records[0].values[0].is_nan());
        assert!(records[0].values[1].is_infinite());
    }

    #[test]
    fn rejects_too_few_fields() {
        let err = parse("AGEDIST\tcall=1\tbaspop").unwrap_err();
        assert_eq!(err.line, 1);
        assert!(err.message.contains("at least 4"));
    }

    #[test]
    fn rejects_unknown_phase() {
        let err = parse("GETPOP\tcall=1\tpopeqp\t1\t1.0\nBOGUS\tcall=1\tx\t1\t1.0").unwrap_err();
        assert_eq!(err.line, 2);
        assert!(err.message.contains("unknown phase"));
    }

    #[test]
    fn rejects_count_mismatch() {
        let err = parse("AGEDIST\tcall=1\tmdyrfrc\t3\t0.1\t0.5").unwrap_err();
        assert_eq!(err.line, 1);
        assert!(err.message.contains("count is 3"));
    }

    #[test]
    fn rejects_unparseable_value() {
        let err = parse("CLCEMS\tcall=1\temsday\t1\tnot_a_number").unwrap_err();
        assert!(err.message.contains("is not a number"));
    }

    #[test]
    fn rejects_empty_label() {
        let err = parse("CLCEMS\tcall=1\t\t1\t1.0").unwrap_err();
        assert!(err.message.contains("empty label"));
    }

    #[test]
    fn record_key_pairs_across_context_order() {
        let a = ReferenceRecord::new(
            Phase::Agedist,
            Context::parse("fips=26000,call=1"),
            "baspop",
            vec![1.0],
        );
        let b = ReferenceRecord::new(
            Phase::Agedist,
            Context::parse("call=1,fips=26000"),
            "baspop",
            vec![2.0],
        );
        assert_eq!(a.key(), b.key());
    }
}
