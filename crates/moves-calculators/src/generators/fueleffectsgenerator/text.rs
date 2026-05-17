//! Pure string helpers ported from `FuelEffectsGenerator.java`.
//!
//! These three functions are `static` in the Java class — they touch no
//! database connection and no instance state, so they port verbatim and
//! are exercised directly by `FuelEffectsGeneratorTest`
//! (`testRewriteCmpExpressionToIncludeStdDev`, `testGetCSV`,
//! `testGetPolProcessIDsNotAlreadyDone`).
//!
//! All three operate on ASCII-only MOVES expression / CSV text, so the
//! Java code-unit indexing maps directly onto Rust byte indexing.

use std::collections::BTreeSet;

/// Rewrite a complex-model-parameter expression so every
/// `(... - fp_NAME.center)` term is divided by `fp_NAME.stddev`.
///
/// Ports `FuelEffectsGenerator.rewriteCmpExpressionToIncludeStdDev`. The
/// complex-model coefficients in the MOVES default database are stored
/// against *centered* fuel-property values; the predictive-model engine
/// additionally needs them *standardized* (centered **and** scaled by the
/// standard deviation). Rather than restating every coefficient, MOVES
/// rewrites the expression text: each `(expr - fp_NAME.center)` becomes
/// `((expr - fp_NAME.center)/fp_NAME.stddev)`.
///
/// The algorithm scans left-to-right for the literal `".center)"`, then
/// for each hit:
///
/// 1. backtracks with a parenthesis-depth counter to the `(` that opens
///    the centered term,
/// 2. reads the variable name from the nearest preceding `fp_` token,
/// 3. inserts `/fp_NAME.stddev)` immediately after `.center)` and an
///    extra `(` in front of the opening paren.
///
/// A term with no matching `(` or no preceding `fp_` is left untouched and
/// scanning stops — exactly as the Java does (`return expression`).
#[must_use]
pub fn rewrite_cmp_expression_to_include_std_dev(cmp_expression: &str) -> String {
    let mut start_index: usize = 0;
    let mut expression = cmp_expression.to_string();
    loop {
        // index = expression.indexOf(".center)", startIndex)
        let index = match expression
            .get(start_index..)
            .and_then(|s| s.find(".center)"))
        {
            Some(rel) => start_index + rel,
            None => return expression,
        };

        // Backtrack to find the matching "(", counting nested parens.
        let bytes = expression.as_bytes();
        let mut paren_start_index: Option<usize> = None;
        let mut parens_needed: i32 = 1;
        let mut i = index as isize;
        while i >= 0 {
            match bytes[i as usize] {
                b'(' => {
                    parens_needed -= 1;
                    if parens_needed <= 0 {
                        paren_start_index = Some(i as usize);
                        break;
                    }
                }
                b')' => parens_needed += 1,
                _ => {}
            }
            i -= 1;
        }
        // No matching "(" found — do nothing more.
        let Some(paren_start_index) = paren_start_index else {
            return expression;
        };

        // Variable name: Java's `lastIndexOf("fp_", index)` finds the last
        // "fp_" starting at an offset <= index. Searching `[..index + 3]`
        // and taking `rfind` reproduces that bound exactly.
        let search_end = (index + 3).min(expression.len());
        let Some(prior_index) = expression[..search_end].rfind("fp_") else {
            return expression;
        };
        let name = expression[prior_index..index].to_string();

        // Insert "/NAME.stddev)" right after ".center)" (8 chars long).
        let after_center = index + 8;
        let mut rewritten = String::with_capacity(expression.len() + name.len() + 12);
        rewritten.push_str(&expression[..after_center]);
        rewritten.push('/');
        rewritten.push_str(&name);
        rewritten.push_str(".stddev)");
        rewritten.push_str(&expression[after_center..]);
        expression = rewritten;
        // Java sets `startIndex = index` (the post-`+= 8` value) here,
        // *before* the second insertion shifts the string — the resulting
        // one-char drift is what stops the next scan re-matching this term.
        start_index = after_center;

        // Insert the extra "(" before the term's opening paren.
        let mut rewritten = String::with_capacity(expression.len() + 1);
        rewritten.push_str(&expression[..paren_start_index]);
        rewritten.push('(');
        rewritten.push_str(&expression[paren_start_index..]);
        expression = rewritten;
    }
}

/// Render a sorted set of integers as a comma-separated string.
///
/// Ports `FuelEffectsGenerator.getCSV`. The Java parameter is a
/// `TreeSet<Integer>`, so the output is always in ascending order; a
/// [`BTreeSet`] gives the same ordering for free. An empty set yields
/// `"0"` (not the empty string) so the result can be dropped into a SQL
/// `IN (...)` clause without a length guard.
#[must_use]
pub fn get_csv(values: &BTreeSet<i32>) -> String {
    if values.is_empty() {
        return "0".to_string();
    }
    let mut result = String::new();
    for value in values {
        if !result.is_empty() {
            result.push(',');
        }
        result.push_str(&value.to_string());
    }
    result
}

/// Filter a comma-separated pollutant/process ID list down to the IDs not
/// yet seen, recording the survivors in `already_done`.
///
/// Ports `FuelEffectsGenerator.getPolProcessIDsNotAlreadyDone`. Returns
/// `None` (Java `null`) when the input is empty or every ID has already
/// been processed. If any field fails to parse as an integer the original
/// string is returned unchanged — the Java `catch` block's behaviour, kept
/// so a caller handed a non-integer list still gets a usable value back.
///
/// `already_done` is updated in place with each freshly seen ID, matching
/// the Java method's documented side effect on its `TreeSet` argument.
#[must_use]
pub fn get_pol_process_ids_not_already_done(
    pol_process_ids_csv: &str,
    already_done: &mut BTreeSet<i32>,
) -> Option<String> {
    if pol_process_ids_csv.is_empty() {
        return None;
    }
    // Java's `String.split(",")` discards trailing empty fields but keeps
    // leading/internal ones; mirror that so a trailing comma is tolerated
    // while an internal blank still trips the parse-failure path.
    let mut parts: Vec<&str> = pol_process_ids_csv.split(',').collect();
    while parts.last() == Some(&"") {
        parts.pop();
    }

    let mut result = String::new();
    for part in parts {
        let Ok(id) = part.trim().parse::<i32>() else {
            return Some(pol_process_ids_csv.to_string());
        };
        if already_done.insert(id) {
            if !result.is_empty() {
                result.push(',');
            }
            result.push_str(&id.to_string());
        }
    }
    if result.is_empty() {
        None
    } else {
        Some(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The sixteen fuel-property names `testRewriteCmpExpressionToIncludeStdDev`
    /// iterates over.
    const NAMES: &[&str] = &[
        "Oxygen",
        "Sulfur",
        "RVP",
        "E200",
        "E300",
        "Aromatics",
        "Benzene",
        "Olefins",
        "MTBE",
        "ETBE",
        "TAME",
        "Ethanol",
        "Intercept",
        "Hi",
        "T50",
        "T90",
    ];

    #[test]
    fn rewrite_single_centered_term() {
        // Ports the first assert of the Java test's name loop.
        for name in NAMES {
            let original = format!("(cmp.coeff*(fp.{name}-fp_{name}.center))");
            let expected = format!("(cmp.coeff*((fp.{name}-fp_{name}.center)/fp_{name}.stddev))");
            assert_eq!(
                rewrite_cmp_expression_to_include_std_dev(&original),
                expected
            );
        }
    }

    #[test]
    fn rewrite_single_centered_term_with_power() {
        // Second assert of the Java loop — the trailing `^2` rides along.
        for name in NAMES {
            let original = format!("(cmp.coeff*(fp.{name}-fp_{name}.center)^2)");
            let expected = format!("(cmp.coeff*((fp.{name}-fp_{name}.center)/fp_{name}.stddev)^2)");
            assert_eq!(
                rewrite_cmp_expression_to_include_std_dev(&original),
                expected
            );
        }
    }

    #[test]
    fn rewrite_multiple_centered_terms() {
        // Third assert of the Java loop — two centered terms in one product.
        for name in NAMES {
            let original =
                format!("(cmp.coeff*(fp.{name}-fp_{name}.center)*(fp.E200-fp_E200.center))");
            let expected = format!(
                "(cmp.coeff*((fp.{name}-fp_{name}.center)/fp_{name}.stddev)\
                 *((fp.E200-fp_E200.center)/fp_E200.stddev))"
            );
            assert_eq!(
                rewrite_cmp_expression_to_include_std_dev(&original),
                expected
            );
        }
    }

    #[test]
    fn rewrite_complex_nested_expression() {
        // The two post-loop asserts: a centered term whose inner value is
        // itself an `if(...)` call.
        let original =
            "(cmp.coeff*(if(and(fp.Oxygen>3.5,maxModelYear<=2001),3.5,fp.Oxygen)-fp_Oxygen.center))";
        let expected = "(cmp.coeff*((if(and(fp.Oxygen>3.5,maxModelYear<=2001),3.5,fp.Oxygen)-fp_Oxygen.center)/fp_Oxygen.stddev))";
        assert_eq!(
            rewrite_cmp_expression_to_include_std_dev(original),
            expected
        );

        let original = "(cmp.coeff*(if(and(fp.Oxygen>3.5,maxModelYear<=2001),3.5,fp.Oxygen)-fp_Oxygen.center)^2)";
        let expected = "(cmp.coeff*((if(and(fp.Oxygen>3.5,maxModelYear<=2001),3.5,fp.Oxygen)-fp_Oxygen.center)/fp_Oxygen.stddev)^2)";
        assert_eq!(
            rewrite_cmp_expression_to_include_std_dev(original),
            expected
        );
    }

    #[test]
    fn rewrite_leaves_expression_without_center_untouched() {
        let original = "(cmp.coeff*fp.Oxygen)";
        assert_eq!(
            rewrite_cmp_expression_to_include_std_dev(original),
            original
        );
    }

    #[test]
    fn get_csv_sorts_and_joins() {
        // Ports testGetCSV: an unordered insert still yields ascending CSV.
        let values: BTreeSet<i32> = [5, 6, 1, 2].into_iter().collect();
        assert_eq!(get_csv(&values), "1,2,5,6");
    }

    #[test]
    fn get_csv_empty_set_is_zero() {
        assert_eq!(get_csv(&BTreeSet::new()), "0");
    }

    #[test]
    fn pol_process_ids_not_already_done_filters_and_records() {
        // Ports testGetPolProcessIDsNotAlreadyDone.
        let mut done: BTreeSet<i32> = BTreeSet::new();

        assert_eq!(
            get_pol_process_ids_not_already_done("101,102", &mut done).as_deref(),
            Some("101,102")
        );
        // Re-running the same list yields nothing new.
        assert_eq!(
            get_pol_process_ids_not_already_done("101,102", &mut done),
            None
        );
        // Only the unseen IDs survive, in input order.
        assert_eq!(
            get_pol_process_ids_not_already_done("201,202,101,103", &mut done).as_deref(),
            Some("201,202,103")
        );
    }

    #[test]
    fn pol_process_ids_empty_input_is_none() {
        let mut done = BTreeSet::new();
        assert_eq!(get_pol_process_ids_not_already_done("", &mut done), None);
    }

    #[test]
    fn pol_process_ids_non_integer_returns_input_unchanged() {
        // The Java `catch` path: a non-integer field aborts filtering.
        let mut done = BTreeSet::new();
        assert_eq!(
            get_pol_process_ids_not_already_done("101,abc", &mut done).as_deref(),
            Some("101,abc")
        );
    }

    #[test]
    fn pol_process_ids_tolerates_trailing_comma() {
        // Java's split drops the trailing empty field; "101," parses as "101".
        let mut done = BTreeSet::new();
        assert_eq!(
            get_pol_process_ids_not_already_done("101,", &mut done).as_deref(),
            Some("101")
        );
    }
}
