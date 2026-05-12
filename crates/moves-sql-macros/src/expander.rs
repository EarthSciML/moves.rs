//! Rust port of `gov/epa/otaq/moves/master/framework/SQLMacroExpander.java`
//! (318 lines, EPA MOVES 25dc6c83).
//!
//! The Java original is a static singleton wired to a JDBC `Connection`. It
//! ran SQL queries against the execution database and stashed the results as
//! "value sets" keyed by macro names of the form `##macro.PREFIXcolumn##` and
//! `##macro.csv.COLUMN##`. Calculator SQL scripts then sprinkled those macro
//! names through their statements; before the SQL went to the worker, every
//! line was passed through `SQLMacroExpander.expandAndAdd`, which produced
//! the cartesian product of all value-set rows the line referenced.
//!
//! The Rust port preserves the algorithm exactly but rotates the API in two
//! ways that matter for a port-time documentation tool:
//!
//! 1. **No DB coupling.** [`MacroExpander::add_data`] takes pre-collected rows
//!    directly. Callers (CLI config files, calculator-port tests) provide the
//!    rows that the JDBC query would have returned.
//! 2. **No static state.** All state lives on a [`MacroExpander`] value.
//!
//! See module [`crate::sections`] for the section-marker preprocessor that runs
//! after [`MacroExpander::expand_and_add`] in the MOVES pipeline.

use std::collections::BTreeSet;

use crate::error::{Error, Result};

/// One named bag of substitution values. Each column produces a macro of the
/// form `##macro.PREFIXcolumn##`; rows are stored row-major.
///
/// Mirrors `SQLMacroExpander.ValueSet` in the Java original.
#[derive(Debug, Clone)]
pub(crate) struct ValueSet {
    /// Identity key — equal to `prefix + "|" + sql_id` for data sets, or
    /// `"csv|" + sql_id` / `"csv.all|" + sql_id` for CSV sets. Compared
    /// case-insensitively when deciding whether [`MacroExpander::upsert`]
    /// replaces an existing set.
    pub(crate) id: String,
    /// Mixed-case macro names (used when substituting values back into a SQL
    /// line so the case in the output matches the case in the template).
    pub(crate) names: Vec<String>,
    /// Lowercase form of [`Self::names`] used for the substring search inside
    /// [`MacroExpander::expand_and_add`]. Mirrors `lowerCaseNames` in Java.
    pub(crate) lower_case_names: Vec<String>,
    /// Number of rows. Stored explicitly so a default-only CSV set with one
    /// synthetic row can be distinguished from a no-data set with zero rows.
    pub(crate) rows: usize,
    /// Row-major flat data. `data[row * names.len() + col]` is the value for
    /// row `row`, column `col`. Empty when [`Self::rows`] is zero.
    pub(crate) data: Vec<String>,
}

impl ValueSet {
    /// Number of rows. Used by the permutation iterator as the dimension size.
    fn rows(&self) -> usize {
        self.rows
    }

    /// Value at `(row, col)`. Returns `""` if [`Self::rows`] is zero
    /// (matches the Java `applyIndex` behavior of leaving `currentValues`
    /// empty when no data was loaded).
    fn value(&self, row: usize, col: usize) -> &str {
        if self.rows == 0 {
            ""
        } else {
            &self.data[row * self.names.len() + col]
        }
    }
}

/// A bag of named substitution sets plus the algorithm that turns each macro-
/// templated SQL line into one or more concrete lines.
///
/// # Usage
///
/// 1. Build with [`MacroExpander::new`].
/// 2. Register value sets with [`MacroExpander::add_data`] and
///    [`MacroExpander::add_csv_data`].
/// 3. (Optional, Java parity) Call [`MacroExpander::compile`] once after all
///    sets have been registered. In Rust this is a no-op — the lowercase
///    name cache is filled at insertion time — but the method exists so
///    porters following the Java call site verbatim get a 1:1 mapping.
/// 4. Call [`MacroExpander::expand_and_add`] for each raw SQL line, accumulating
///    expanded lines into a caller-provided `Vec<String>`.
///
/// # Algorithm parity
///
/// The cartesian-product order, the case-insensitive substring search, and
/// the transitive `do_replacements` loop all mirror the Java semantics
/// byte-for-byte. The integration test
/// `tests/macro_expander.rs::expand_matches_java_documented_behavior`
/// pins the exact output for the documented Java examples.
#[derive(Debug, Default, Clone)]
pub struct MacroExpander {
    sets: Vec<ValueSet>,
}

impl MacroExpander {
    /// New, empty expander. Equivalent to `SQLMacroExpander.reset()` on a
    /// fresh class load.
    pub fn new() -> Self {
        Self::default()
    }

    /// Drop all registered value sets. Mirrors `SQLMacroExpander.reset()`.
    pub fn reset(&mut self) {
        self.sets.clear();
    }

    /// Java parity no-op. The Java version stashes a flat name→set map at
    /// this point; Rust keeps lookups linear over the registered sets (the
    /// live MOVES `csvsets + sets` list peaks at ~30 entries, so linear is
    /// fine and avoids a second `HashMap` to keep in sync).
    pub fn compile(&self) {}

    /// Number of currently-registered value sets. Useful in tests.
    pub fn len(&self) -> usize {
        self.sets.len()
    }

    /// True iff no value sets are registered.
    pub fn is_empty(&self) -> bool {
        self.sets.is_empty()
    }

    /// Register a multi-column data set. Each column produces a macro of the
    /// form `##macro.{prefix}{column}##`. Use an empty `prefix` for the bare
    /// `##macro.{column}##` form.
    ///
    /// `sql_id` is an arbitrary string used only to derive the set's identity
    /// key (Java uses the SQL statement text). Registering with the same
    /// `(prefix, sql_id)` pair replaces the previous set, matching Java's
    /// `findOrCreate(id, true)` behavior.
    ///
    /// Mirrors `SQLMacroExpander.addData(prefix, db, sql)`. Where the Java
    /// method runs the SQL and pulls column names from `ResultSetMetaData`,
    /// the Rust API takes the column list and rows directly.
    ///
    /// # Errors
    ///
    /// * [`Error::EmptyColumns`] — `columns` is empty.
    /// * [`Error::RowWidthMismatch`] — some row's width differs from
    ///   `columns.len()`.
    pub fn add_data(
        &mut self,
        prefix: &str,
        sql_id: &str,
        columns: &[&str],
        rows: &[Vec<String>],
    ) -> Result<()> {
        let id = format!("{prefix}|{sql_id}");
        if columns.is_empty() {
            return Err(Error::EmptyColumns { id });
        }

        let mut names = Vec::with_capacity(columns.len());
        let mut lower_case_names = Vec::with_capacity(columns.len());
        for c in columns {
            let n = format!("##macro.{prefix}{c}##");
            lower_case_names.push(n.to_lowercase());
            names.push(n);
        }

        let row_count = rows.len();
        let mut data = Vec::with_capacity(row_count.saturating_mul(columns.len()));
        for (idx, row) in rows.iter().enumerate() {
            if row.len() != columns.len() {
                return Err(Error::RowWidthMismatch {
                    id,
                    row: idx,
                    width: row.len(),
                    columns: columns.len(),
                });
            }
            for v in row {
                data.push(v.clone());
            }
        }

        self.upsert(ValueSet {
            id,
            names,
            lower_case_names,
            rows: row_count,
            data,
        });
        Ok(())
    }

    /// Register a CSV-style aggregation set. Produces *two* sets:
    ///
    /// * `##macro.csv.{column_name}##` — one row per `max_length`-bounded
    ///   chunk. SQL `IN (…)` clauses split across multiple statements use
    ///   this form.
    /// * `##macro.csv.all.{column_name}##` — single row holding the full
    ///   comma-separated list. SQL `IN (…)` clauses that are guaranteed to
    ///   fit in one statement use this form.
    ///
    /// Mirrors `SQLMacroExpander.addCSVData(db, sql, maxLength, shouldAddQuotes,
    /// useDefaultValueInData, defaultValue)`. Java pulls the column name and
    /// values from a JDBC query; the Rust API takes them directly. The CSV
    /// algorithm (sort + dedupe by `TreeSet<String>` natural order, optional
    /// default-value insertion, optional MySQL-style escaping, length-bounded
    /// chunking) is replicated exactly.
    ///
    /// # Arguments
    ///
    /// * `sql_id` — identity key (Java passes the SQL string).
    /// * `column_name` — produces the macro name suffix.
    /// * `values` — unsorted, possibly duplicated raw column values. Empty
    ///   and `None` entries are skipped, matching the Java
    ///   `query.rs.getString(1) == null || length <= 0` filter.
    /// * `max_length` — maximum length of each chunked row (`0` = no chunking;
    ///   all values are joined into one row).
    /// * `should_add_quotes` — wrap each value with `'…'` (MySQL-escaped)
    ///   before joining. Use `true` for textual columns, `false` for numeric
    ///   IDs.
    /// * `use_default_value_in_data` — keep `default_value` in the data even
    ///   when real rows are present. Set to `true` when the default doubles
    ///   as a wildcard sentinel in the database.
    /// * `default_value` — value to emit when the value list collapses to
    ///   empty (prevents zero-element SQL `IN ()` syntax errors). Pass
    ///   `None` for no default. An empty string is treated as "no default"
    ///   unless `should_add_quotes` is set, matching the Java
    ///   `hasDefaultValue` predicate exactly.
    // The argument count (8) intentionally mirrors Java's
    // `addCSVData(db, sql, maxLength, shouldAddQuotes, useDefaultValueInData,
    // defaultValue)`. Bundling them into a builder or options struct would
    // hide the Java parity, which is the whole point of this doc tool.
    #[allow(clippy::too_many_arguments)]
    pub fn add_csv_data(
        &mut self,
        sql_id: &str,
        column_name: &str,
        values: &[&str],
        max_length: usize,
        should_add_quotes: bool,
        use_default_value_in_data: bool,
        default_value: Option<&str>,
    ) {
        // hasDefaultValue: Java treats a null defaultValue as "no default",
        // and a zero-length string as "no default unless we're adding quotes"
        // (empty-but-quoted is `''`, which IS valid SQL). The matching Rust
        // shape: Some(s) where shouldAddQuotes || !s.is_empty().
        let has_default_value = match default_value {
            Some(s) => should_add_quotes || !s.is_empty(),
            None => false,
        };

        let escaped_default = if has_default_value && should_add_quotes {
            // Safe because has_default_value implies Some.
            Some(escape_sql(default_value.unwrap(), true))
        } else {
            default_value.map(|s| s.to_string())
        };

        // Build the unique sorted set (Java: TreeSet<String> natural order).
        let mut unique_values: BTreeSet<String> = BTreeSet::new();
        if let Some(d) = &escaped_default {
            if has_default_value {
                unique_values.insert(d.clone());
            }
        }

        for &t in values {
            if t.is_empty() && !should_add_quotes {
                // Java: `if (t == null || (!shouldAddQuotes && t.length() <= 0)) continue;`
                continue;
            }
            // Java: `if (t == null) continue;` — we model `null` as not-supplied,
            // i.e. the caller doesn't include it in `values`. We only see &str.
            let escaped = if should_add_quotes {
                escape_sql(t, true)
            } else {
                t.to_string()
            };
            unique_values.insert(escaped);
        }

        // Java: `if (!useDefaultValueInData && hasDefaultValue) uniqueValues.remove(defaultValue);`
        // The default was added unconditionally above to participate in sort
        // ordering; remove it here if it shouldn't show up in the rows.
        if !use_default_value_in_data && has_default_value {
            if let Some(d) = &escaped_default {
                unique_values.remove(d);
            }
        }

        // Walk sorted set, accumulating `all` (comma-joined full list) and
        // emitting chunks bounded by `max_length`.
        let mut all = String::new();
        let mut current = String::new();
        let mut rows: Vec<String> = Vec::new();
        for t in &unique_values {
            if !all.is_empty() {
                all.push(',');
            }
            all.push_str(t);

            if !current.is_empty() {
                current.push(',');
            }
            current.push_str(t);
            if max_length > 0 && current.len() >= max_length {
                rows.push(std::mem::take(&mut current));
            }
        }
        if !current.is_empty() {
            rows.push(std::mem::take(&mut current));
        }

        // Java: if rows is empty and we have a default, emit a single-row set
        // with the default value (and `all` becomes the default too).
        if rows.is_empty() && has_default_value {
            if let Some(d) = &escaped_default {
                rows.push(d.clone());
                all = d.clone();
            }
        }

        // First set: `##macro.csv.COLUMN##` — one row per chunk.
        {
            let id = format!("csv|{sql_id}");
            let name = format!("##macro.csv.{column_name}##");
            let lower = name.to_lowercase();
            let row_count = rows.len();
            // Flat data: one column, `row_count` rows.
            let data = if row_count > 0 { rows } else { Vec::new() };
            self.upsert(ValueSet {
                id,
                names: vec![name],
                lower_case_names: vec![lower],
                rows: row_count,
                data,
            });
        }

        // Second set: `##macro.csv.all.COLUMN##` — single row with full list.
        // Java only ADDs this set when `all.length() > 0`; otherwise it ADDs
        // an empty `ValueSet` (no names, no rows). We preserve that behavior
        // by storing a names-less empty set so [`Self::expand_and_add`]
        // skips it during scanning.
        {
            let id = format!("csv.all|{sql_id}");
            if !all.is_empty() {
                let name = format!("##macro.csv.all.{column_name}##");
                let lower = name.to_lowercase();
                self.upsert(ValueSet {
                    id,
                    names: vec![name],
                    lower_case_names: vec![lower],
                    rows: 1,
                    data: vec![all],
                });
            } else {
                self.upsert(ValueSet {
                    id,
                    names: Vec::new(),
                    lower_case_names: Vec::new(),
                    rows: 0,
                    data: Vec::new(),
                });
            }
        }
    }

    /// Expand one raw SQL line into one or more substituted lines, appending
    /// to `out`.
    ///
    /// Fast path: if `raw_line` does not contain the literal `"##macro."`,
    /// it is pushed verbatim with no further work — matching Java's
    /// `if (rawLine.indexOf("##macro.") < 0)` early return.
    ///
    /// Otherwise, every value set whose any-column lowercase name appears as
    /// a substring of `raw_line` (lowercased) is treated as a dimension. The
    /// cartesian product of row indices over those dimensions is iterated in
    /// `PermutationCreator` order (innermost dimension cycles fastest), and
    /// for each combination [`do_replacements`] substitutes every macro name
    /// in every selected set with the current row's value.
    ///
    /// Mirrors `SQLMacroExpander.expandAndAdd`.
    pub fn expand_and_add(&self, raw_line: &str, out: &mut Vec<String>) {
        if !raw_line.contains("##macro.") {
            out.push(raw_line.to_string());
            return;
        }

        let lower_line = raw_line.to_lowercase();
        let mut used: Vec<&ValueSet> = Vec::new();
        for set in &self.sets {
            // Java: `for (int j=0; j<set.lowerCaseNames.length; j++) if (...) { p.add(set); break; }`
            // — adding the set as a whole once any of its column names appears.
            // An empty (names-less) CSV-all set is skipped naturally.
            for n in &set.lower_case_names {
                if lower_line.contains(n) {
                    used.push(set);
                    break;
                }
            }
        }

        if used.is_empty() {
            out.push(raw_line.to_string());
            return;
        }

        // Cartesian product over `used`. Dimension `i` ranges over
        // `0..used[i].rows()`. A dimension with `rows == 0` would make the
        // product empty; preserve Java's behavior, which is to fill
        // `currentValues` with `""` for that dimension and emit one combo.
        let mut counters = vec![0usize; used.len()];

        loop {
            let mut result = raw_line.to_string();
            let mut lower_result = lower_line.clone();
            // Java `doReplacements`: loop until a full pass produces no
            // replacements. For each set, replace ALL of its column names
            // with the current-row values (case-insensitive substring).
            let mut done = false;
            while !done {
                done = true;
                for (dim_idx, set) in used.iter().enumerate() {
                    let row = counters[dim_idx];
                    for col_idx in 0..set.names.len() {
                        let key = &set.lower_case_names[col_idx];
                        let value = set.value(row, col_idx);
                        if let Some(pos) = lower_result.find(key.as_str()) {
                            // Replace in both views — the value may
                            // contain a substring that matches another key.
                            let after = pos + key.len();
                            let mut new_result =
                                String::with_capacity(result.len() - key.len() + value.len());
                            new_result.push_str(&result[..pos]);
                            new_result.push_str(value);
                            new_result.push_str(&result[after..]);
                            result = new_result;

                            let mut new_lower =
                                String::with_capacity(lower_result.len() - key.len() + value.len());
                            new_lower.push_str(&lower_result[..pos]);
                            // The replacement value may contain mixed-case
                            // characters; lowercase for the search view.
                            new_lower.push_str(&value.to_lowercase());
                            new_lower.push_str(&lower_result[after..]);
                            lower_result = new_lower;

                            done = false;
                        }
                    }
                }
            }
            out.push(result);

            if !advance(&mut counters, &used) {
                break;
            }
        }
    }

    /// Insert a [`ValueSet`], replacing in-place any existing set whose id
    /// matches case-insensitively. Mirrors the combined effect of Java's
    /// `findOrCreate(id, true) + add(set)`.
    fn upsert(&mut self, new_set: ValueSet) {
        for existing in &mut self.sets {
            if existing.id.eq_ignore_ascii_case(&new_set.id) {
                *existing = new_set;
                return;
            }
        }
        self.sets.push(new_set);
    }
}

/// Advance the cartesian-product counter. Returns `false` when there is no
/// next permutation. Mirrors `PermutationCreator.next` (innermost dimension
/// — index `0` — cycles fastest).
fn advance(counters: &mut [usize], dims: &[&ValueSet]) -> bool {
    let mut idx = 0;
    while idx < counters.len() {
        counters[idx] += 1;
        // A zero-row dimension has dimension size 0; treat its `applyIndex`
        // as a no-op (Java prints empty strings in that case). To preserve
        // termination, treat dim_size 0 as 1-cycle.
        let dim_size = dims[idx].rows().max(1);
        if counters[idx] >= dim_size {
            counters[idx] = 0;
            idx += 1;
            continue;
        }
        return true;
    }
    false
}

/// Port of `DatabaseUtilities.escapeSQL(sql, addOuterQuotes)`.
///
/// MySQL-style escaping: every `'` becomes `\'`, every `\` becomes `\\`,
/// and the result is wrapped in single quotes when `add_outer_quotes` is
/// `true`. This is a doc-tool fidelity helper — for runtime SQL building
/// inside the Rust port, use parameter binding or a Polars expression
/// instead of textual escape.
pub fn escape_sql(sql: &str, add_outer_quotes: bool) -> String {
    // Java fast path: `if (sql.indexOf('\'') < 0 && sql.indexOf('\\') < 0)`
    if !sql.contains('\'') && !sql.contains('\\') {
        return if add_outer_quotes {
            format!("'{sql}'")
        } else {
            sql.to_string()
        };
    }
    let mut out = String::with_capacity(sql.len() + 2);
    for c in sql.chars() {
        match c {
            '\'' => out.push_str("\\'"),
            '\\' => out.push_str("\\\\"),
            other => out.push(other),
        }
    }
    if add_outer_quotes {
        format!("'{out}'")
    } else {
        out
    }
}

/// Port of `StringUtilities.doReplacements(input, replacements)`.
///
/// Performs a case-insensitive substring replacement of every key in
/// `replacements` with its value, looping until a full pass produces no
/// more replacements. This is exposed because the section-marker
/// preprocessor uses the same semantics for `##context.*##`-style
/// replacements that aren't part of the macro engine. Mirrors the Java
/// double-buffer (raw + lowercase view) approach for transitive
/// substitutions.
pub fn do_replacements(input: &str, replacements: &[(String, String)]) -> String {
    let mut result = input.to_string();
    let mut lower_result = result.to_lowercase();
    let lower_keys: Vec<String> = replacements.iter().map(|(k, _)| k.to_lowercase()).collect();

    let mut done = false;
    while !done {
        done = true;
        for (key_idx, (_, value)) in replacements.iter().enumerate() {
            let key = &lower_keys[key_idx];
            if let Some(pos) = lower_result.find(key.as_str()) {
                let after = pos + key.len();
                let mut new_result = String::with_capacity(result.len() - key.len() + value.len());
                new_result.push_str(&result[..pos]);
                new_result.push_str(value);
                new_result.push_str(&result[after..]);
                result = new_result;

                let mut new_lower =
                    String::with_capacity(lower_result.len() - key.len() + value.len());
                new_lower.push_str(&lower_result[..pos]);
                new_lower.push_str(&value.to_lowercase());
                new_lower.push_str(&lower_result[after..]);
                lower_result = new_lower;
                done = false;
            }
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fast_path_passes_unmodified_when_no_macro_marker() {
        let m = MacroExpander::new();
        let mut out = Vec::new();
        m.expand_and_add("SELECT 1 FROM dual;", &mut out);
        assert_eq!(out, vec!["SELECT 1 FROM dual;".to_string()]);
    }

    #[test]
    fn line_with_macro_but_no_matching_set_passes_unmodified() {
        let m = MacroExpander::new();
        let mut out = Vec::new();
        m.expand_and_add("SELECT ##macro.unknown.x## FROM t;", &mut out);
        assert_eq!(out, vec!["SELECT ##macro.unknown.x## FROM t;".to_string()]);
    }

    #[test]
    fn single_column_set_expands_to_one_line_per_row() {
        let mut m = MacroExpander::new();
        m.add_data(
            "",
            "select sourceTypeID from RunSpecSourceType",
            &["sourceTypeID"],
            &[
                vec!["21".to_string()],
                vec!["31".to_string()],
                vec!["42".to_string()],
            ],
        )
        .unwrap();
        let mut out = Vec::new();
        m.expand_and_add("DROP TABLE t##macro.sourceTypeID##;", &mut out);
        assert_eq!(
            out,
            vec![
                "DROP TABLE t21;".to_string(),
                "DROP TABLE t31;".to_string(),
                "DROP TABLE t42;".to_string(),
            ]
        );
    }

    #[test]
    fn multi_column_set_substitutes_all_columns_per_row() {
        let mut m = MacroExpander::new();
        m.add_data(
            "mya.",
            "select yearID, modelYearID, ageID from RunSpecModelYearAge",
            &["yearID", "modelYearID", "ageID"],
            &[
                vec!["2030".to_string(), "2025".to_string(), "5".to_string()],
                vec!["2030".to_string(), "2030".to_string(), "0".to_string()],
            ],
        )
        .unwrap();
        let mut out = Vec::new();
        m.expand_and_add(
            "INSERT INTO mya VALUES (##macro.mya.yearID##, ##macro.mya.modelYearID##, ##macro.mya.ageID##);",
            &mut out,
        );
        assert_eq!(
            out,
            vec![
                "INSERT INTO mya VALUES (2030, 2025, 5);".to_string(),
                "INSERT INTO mya VALUES (2030, 2030, 0);".to_string(),
            ]
        );
    }

    #[test]
    fn cartesian_product_across_two_sets_iterates_inner_dimension_fastest() {
        // Java PermutationCreator increments counters[0] first, then carries
        // into counters[1] — so dim 0 (the first added set) cycles fastest.
        // The order of `used` in our scan = the order sets appear in `sets`,
        // which is insertion order.
        let mut m = MacroExpander::new();
        m.add_data(
            "",
            "fuel",
            &["fuelTypeID"],
            &[vec!["1".to_string()], vec!["2".to_string()]],
        )
        .unwrap();
        m.add_data(
            "",
            "month",
            &["monthID"],
            &[vec!["1".to_string()], vec!["12".to_string()]],
        )
        .unwrap();
        let mut out = Vec::new();
        m.expand_and_add("x = ##macro.fuelTypeID##, m = ##macro.monthID##;", &mut out);
        // Expect inner (fuel) cycles fastest: (1,1),(2,1),(1,12),(2,12)
        assert_eq!(
            out,
            vec![
                "x = 1, m = 1;".to_string(),
                "x = 2, m = 1;".to_string(),
                "x = 1, m = 12;".to_string(),
                "x = 2, m = 12;".to_string(),
            ]
        );
    }

    #[test]
    fn csv_all_produces_single_in_clause() {
        let mut m = MacroExpander::new();
        m.add_csv_data(
            "select distinct fuelTypeID",
            "fuelTypeID",
            &["1", "2", "9"],
            5000,
            false,
            false,
            Some("0"),
        );
        let mut out = Vec::new();
        m.expand_and_add(
            "WHERE fuelTypeID in (##macro.csv.all.fuelTypeID##);",
            &mut out,
        );
        assert_eq!(out, vec!["WHERE fuelTypeID in (1,2,9);".to_string()]);
    }

    #[test]
    fn csv_chunked_emits_one_in_clause_per_chunk() {
        let mut m = MacroExpander::new();
        // max_length 3 forces a flush as soon as `current` reaches 3 chars.
        // Sorted values: ["1","2","3","4"]. After "1": len=1, no flush.
        // After "1,2": len=3, flush "1,2", current=""; after "3": len=1,
        // no flush; after "3,4": len=3, flush; final flush empty.
        m.add_csv_data("ids", "id", &["1", "2", "3", "4"], 3, false, false, None);
        let mut out = Vec::new();
        m.expand_and_add("DELETE FROM t WHERE id in (##macro.csv.id##);", &mut out);
        assert_eq!(
            out,
            vec![
                "DELETE FROM t WHERE id in (1,2);".to_string(),
                "DELETE FROM t WHERE id in (3,4);".to_string(),
            ]
        );
    }

    #[test]
    fn csv_empty_with_default_emits_default_row() {
        let mut m = MacroExpander::new();
        m.add_csv_data("empty", "id", &[], 5000, false, false, Some("0"));
        let mut out = Vec::new();
        m.expand_and_add("WHERE id in (##macro.csv.id##);", &mut out);
        assert_eq!(out, vec!["WHERE id in (0);".to_string()]);

        let mut out2 = Vec::new();
        m.expand_and_add("WHERE id in (##macro.csv.all.id##);", &mut out2);
        assert_eq!(out2, vec!["WHERE id in (0);".to_string()]);
    }

    #[test]
    fn csv_empty_without_default_drops_macro_all() {
        // When there's no default and no data, the `##macro.csv.all.*##` set
        // is registered with zero rows and zero names; expand_and_add should
        // skip it and leave the macro substring in place.
        let mut m = MacroExpander::new();
        m.add_csv_data("empty", "id", &[], 5000, false, false, None);
        let mut out = Vec::new();
        m.expand_and_add("WHERE id in (##macro.csv.all.id##);", &mut out);
        assert_eq!(out, vec!["WHERE id in (##macro.csv.all.id##);".to_string()]);
    }

    #[test]
    fn csv_quoted_values_escape_single_quotes_and_backslashes() {
        let mut m = MacroExpander::new();
        m.add_csv_data(
            "names",
            "name",
            &["Bob's", r"path\to"],
            5000,
            true,
            false,
            None,
        );
        let mut out = Vec::new();
        m.expand_and_add("WHERE name in (##macro.csv.all.name##);", &mut out);
        // Sorted alphabetically by escaped form: 'Bob\'s' < 'path\\to'.
        // Each value is wrapped in `'…'` and inner `'` / `\` get backslash-
        // escaped (matches `DatabaseUtilities.escapeSQL(t, true)`).
        assert_eq!(
            out,
            vec![r"WHERE name in ('Bob\'s','path\\to');".to_string()]
        );
    }

    #[test]
    fn csv_quoted_default_value_is_emitted_when_no_data() {
        // Even an empty-string default counts when shouldAddQuotes is true
        // (Java `hasDefaultValue` predicate). With no real rows, the empty
        // default produces a single-row set with value `''`.
        let mut m = MacroExpander::new();
        m.add_csv_data("names_empty", "name", &[], 5000, true, false, Some(""));
        let mut out = Vec::new();
        m.expand_and_add("WHERE name in (##macro.csv.all.name##);", &mut out);
        assert_eq!(out, vec!["WHERE name in ('');".to_string()]);
    }

    #[test]
    fn csv_use_default_value_in_data_keeps_default_alongside_real_rows() {
        let mut m = MacroExpander::new();
        // Default "0" + real value "1" -> sorted ["0","1"] -> all = "0,1".
        m.add_csv_data("with_def", "id", &["1"], 5000, false, true, Some("0"));
        let mut out = Vec::new();
        m.expand_and_add("WHERE id in (##macro.csv.all.id##);", &mut out);
        assert_eq!(out, vec!["WHERE id in (0,1);".to_string()]);
    }

    #[test]
    fn macro_name_match_is_case_insensitive_past_fast_path() {
        // The fast-path `contains("##macro.")` is byte-literal and case-
        // sensitive — that mirrors Java's `rawLine.indexOf("##macro.")`. Past
        // the fast path, the column-name match uses lower_case_names, so the
        // column case in the line need not match the case in the value set.
        let mut m = MacroExpander::new();
        m.add_data("", "v", &["FoO"], &[vec!["BAR".to_string()]])
            .unwrap();
        let mut out = Vec::new();
        m.expand_and_add("hello ##macro.FOO## world", &mut out);
        assert_eq!(out, vec!["hello BAR world".to_string()]);
    }

    #[test]
    fn uppercase_macro_prefix_skips_fast_path_unchanged() {
        // Documenting the Java-equivalent quirk: an uppercase `##MACRO.`
        // prefix bypasses macro expansion entirely because the fast-path
        // check is case-sensitive.
        let mut m = MacroExpander::new();
        m.add_data("", "v", &["foo"], &[vec!["BAR".to_string()]])
            .unwrap();
        let mut out = Vec::new();
        m.expand_and_add("hello ##MACRO.foo## world", &mut out);
        assert_eq!(out, vec!["hello ##MACRO.foo## world".to_string()]);
    }

    #[test]
    fn add_data_replaces_set_with_same_prefix_and_sql_id() {
        let mut m = MacroExpander::new();
        m.add_data("", "ids", &["id"], &[vec!["1".to_string()]])
            .unwrap();
        m.add_data(
            "",
            "ids",
            &["id"],
            &[vec!["99".to_string()], vec!["100".to_string()]],
        )
        .unwrap();
        assert_eq!(m.len(), 1);
        let mut out = Vec::new();
        m.expand_and_add("v=##macro.id##", &mut out);
        assert_eq!(out, vec!["v=99".to_string(), "v=100".to_string()]);
    }

    #[test]
    fn add_data_rejects_empty_columns() {
        let mut m = MacroExpander::new();
        let err = m.add_data("", "x", &[], &[]).unwrap_err();
        assert!(matches!(err, Error::EmptyColumns { .. }));
    }

    #[test]
    fn add_data_rejects_row_width_mismatch() {
        let mut m = MacroExpander::new();
        let err = m
            .add_data(
                "",
                "x",
                &["a", "b"],
                &[
                    vec!["1".to_string(), "2".to_string()],
                    vec!["solo".to_string()],
                ],
            )
            .unwrap_err();
        match err {
            Error::RowWidthMismatch {
                row,
                width,
                columns,
                ..
            } => {
                assert_eq!(row, 1);
                assert_eq!(width, 1);
                assert_eq!(columns, 2);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn reset_clears_all_sets() {
        let mut m = MacroExpander::new();
        m.add_data("", "x", &["a"], &[vec!["1".to_string()]])
            .unwrap();
        assert_eq!(m.len(), 1);
        m.reset();
        assert!(m.is_empty());
    }

    #[test]
    fn escape_sql_fast_path_for_clean_string() {
        assert_eq!(escape_sql("hello", true), "'hello'");
        assert_eq!(escape_sql("hello", false), "hello");
    }

    #[test]
    fn escape_sql_handles_quotes_and_backslashes() {
        assert_eq!(escape_sql("a'b", true), r"'a\'b'");
        assert_eq!(escape_sql(r"a\b", true), r"'a\\b'");
        assert_eq!(escape_sql(r"a'\b", true), r"'a\'\\b'");
    }

    #[test]
    fn do_replacements_is_transitive() {
        // Java doReplacements loops until no more matches — `##A##` → `##B##`
        // → `final` in one call.
        let repl = vec![
            ("##A##".to_string(), "##B##".to_string()),
            ("##B##".to_string(), "final".to_string()),
        ];
        assert_eq!(do_replacements("x ##A## y", &repl), "x final y");
    }

    #[test]
    fn do_replacements_is_case_insensitive() {
        let repl = vec![("##Year##".to_string(), "2030".to_string())];
        assert_eq!(do_replacements("y=##YEAR##", &repl), "y=2030");
        assert_eq!(do_replacements("y=##year##", &repl), "y=2030");
    }
}
