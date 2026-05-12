//! Section-marker preprocessor — Rust port of the first section-handling
//! pass inside `BundleUtilities.readAndHandleScriptedCalculations` (the
//! Java method in `gov/epa/otaq/moves/master/framework/BundleUtilities.java`,
//! ~line 264 onwards in the EPA MOVES 25dc6c83 release).
//!
//! MOVES SQL scripts are organised into nested, named sections of the form:
//!
//! ```text
//! -- Section Foo
//!   ... SQL ...
//!   -- Section Bar
//!     ... nested SQL ...
//!   -- End Section Bar
//! -- End Section Foo
//! ```
//!
//! The runtime decides — based on RunSpec selections — which section names
//! are "enabled" for the current bundle and discards any SQL whose enclosing
//! section is not in the enabled set. A disabled outer section disables every
//! nested section regardless of its own name, matching the Java stack-based
//! algorithm exactly.
//!
//! This module ports the first pass of `readAndHandleScriptedCalculations` —
//! the part that turns the raw line list into a section-filtered line list,
//! with optional `##context.*##`-style replacements applied to non-marker
//! lines. The second pass (routing surviving sections into
//! `sqlForWorker.{processingSQL, dataExportSQL, cleanupSQL}` and folding in
//! `DefaultDataMaker` output) is runtime-specific and lives outside this
//! documentation tool.

use crate::expander::do_replacements;

/// Result of [`process_sections`]: the section-filtered SQL plus a count of
/// enabled/disabled sections crossed, for the CLI to report on.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SectionProcessOutput {
    /// Lines that survived section filtering, in original order, with
    /// section-marker lines retained where the enclosing context was
    /// enabled (matching Java behavior: those markers stay in the output
    /// so the downstream second pass can act on them).
    pub lines: Vec<String>,
    /// Total number of `-- Section X` markers encountered.
    pub sections_seen: usize,
    /// Sections whose contents were emitted because the enclosing context
    /// was enabled AND the name was in the enabled set.
    pub sections_kept: usize,
    /// Sections whose contents were dropped because either the enclosing
    /// context was disabled OR the name was missing from the enabled set.
    pub sections_dropped: usize,
}

/// Process a list of raw SQL lines through the MOVES section-marker
/// algorithm.
///
/// # Behavior
///
/// For each line, trimmed:
///
/// * `"-- Section <name>"` — push the current `is_in_enabled_section` state
///   onto a stack, then update the state:
///   if the current state is `true`, set the new state to whether `<name>`
///   is in `enabled_sections`; otherwise the new state stays `false` (nested
///   sections inside a disabled section are disabled regardless of their
///   own name). The marker line is emitted **only when the post-update
///   state is `true`** (Java: `if (isInEnabledSection) replacedSQL.add(sql)`).
/// * `"-- End Section"` — emit the marker if the section was enabled, then
///   pop the stack, restoring the parent state.
/// * any other non-empty line — emit it, with `replacements` applied via
///   [`do_replacements`], but only when the current state is `true`.
///
/// The leading-stack value is `true` (the script's outermost scope is always
/// enabled). Blank lines and lines that trim to empty are dropped silently,
/// matching Java's `if (sql.length() > 0)` guard.
///
/// # Arguments
///
/// * `lines` — raw SQL lines, in script order. Each entry is one statement
///   or comment (the way `appendSQLScriptToList` produces it in Java).
/// * `enabled_sections` — section names the runtime decided to keep.
///   Membership test is case-insensitive (`TreeSetIgnoreCase` in Java).
/// * `replacements` — `(macro_name, value)` pairs applied to non-marker
///   lines via [`do_replacements`]. Pass `&[]` for no replacements.
pub fn process_sections(
    lines: &[String],
    enabled_sections: &[&str],
    replacements: &[(String, String)],
) -> SectionProcessOutput {
    let enabled_lower: Vec<String> = enabled_sections.iter().map(|s| s.to_lowercase()).collect();
    let is_enabled = |name: &str| {
        let name_lower = name.to_lowercase();
        enabled_lower.iter().any(|e| e == &name_lower)
    };

    let mut out = SectionProcessOutput::default();
    let mut stack: Vec<bool> = vec![true];
    let mut is_in_enabled_section = true;

    for raw_line in lines {
        let trimmed = raw_line.trim();
        if trimmed.is_empty() {
            continue;
        }

        if let Some(name) = strip_section_prefix(trimmed) {
            out.sections_seen += 1;
            // Java: only re-evaluate the enable bit when we're CURRENTLY in
            // an enabled context. Once we descend into a disabled section,
            // every nested section stays disabled (the inner name doesn't
            // override the outer disable).
            if is_in_enabled_section {
                is_in_enabled_section = is_enabled(name);
            }
            stack.push(is_in_enabled_section);
            if is_in_enabled_section {
                out.sections_kept += 1;
                out.lines.push(trimmed.to_string());
            } else {
                out.sections_dropped += 1;
            }
        } else if strip_end_section_prefix(trimmed).is_some() {
            if is_in_enabled_section {
                out.lines.push(trimmed.to_string());
            }
            // Pop the section-open state and restore the parent.
            stack.pop();
            // The leading `true` we pushed before iteration guarantees the
            // stack is never empty here for well-formed input. For ill-formed
            // input (more `End Section` than `Section`), preserve the Java
            // behavior — Java would have thrown an EmptyStackException; we
            // fall back to `true` so the rest of the script still gets
            // processed, which is more useful than panicking in a docs tool.
            is_in_enabled_section = *stack.last().unwrap_or(&true);
        } else if is_in_enabled_section {
            let line = if replacements.is_empty() {
                trimmed.to_string()
            } else {
                do_replacements(trimmed, replacements)
            };
            out.lines.push(line);
        }
    }

    out
}

/// If `trimmed` begins with `"-- Section "` (a literal hyphen-hyphen-space-
/// Section-space prefix), return the rest with surrounding whitespace
/// removed. Java is byte-literal here (`sql.startsWith("-- Section")` +
/// `sql.substring(10).trim()`); the Rust port keeps the same prefix length
/// of 10 characters so a line like `"-- SectionFoo"` is matched as section
/// name `"Foo"` (an edge case Java accepts).
fn strip_section_prefix(trimmed: &str) -> Option<&str> {
    // `-- End Section` also starts with `-- ` but has `E` at byte 3, so it
    // doesn't match `"-- Section"`. No special-case needed; Java's
    // `if/else if` on `startsWith` exploits the same property.
    trimmed.strip_prefix("-- Section").map(str::trim)
}

/// If `trimmed` begins with `"-- End Section"`, return the rest with
/// surrounding whitespace removed. Used so callers can tell `End Section Foo`
/// from `End Section` for diagnostics; the section processor itself only
/// needs the presence check.
fn strip_end_section_prefix(trimmed: &str) -> Option<&str> {
    trimmed.strip_prefix("-- End Section").map(str::trim)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lines(input: &str) -> Vec<String> {
        input.lines().map(|s| s.to_string()).collect()
    }

    #[test]
    fn top_level_lines_always_emit() {
        let out = process_sections(&lines("SELECT 1;\nDROP TABLE t;"), &[], &[]);
        assert_eq!(
            out.lines,
            vec!["SELECT 1;".to_string(), "DROP TABLE t;".to_string()]
        );
        assert_eq!(out.sections_seen, 0);
    }

    #[test]
    fn enabled_section_keeps_body_and_markers() {
        let sql = "\
SELECT 1;
-- Section Foo
DROP TABLE foo;
-- End Section Foo
SELECT 2;";
        let out = process_sections(&lines(sql), &["Foo"], &[]);
        assert_eq!(
            out.lines,
            vec![
                "SELECT 1;".to_string(),
                "-- Section Foo".to_string(),
                "DROP TABLE foo;".to_string(),
                "-- End Section Foo".to_string(),
                "SELECT 2;".to_string(),
            ]
        );
        assert_eq!(out.sections_seen, 1);
        assert_eq!(out.sections_kept, 1);
        assert_eq!(out.sections_dropped, 0);
    }

    #[test]
    fn disabled_section_drops_body_and_markers() {
        let sql = "\
SELECT 1;
-- Section Foo
DROP TABLE foo;
-- End Section Foo
SELECT 2;";
        let out = process_sections(&lines(sql), &[], &[]);
        assert_eq!(
            out.lines,
            vec!["SELECT 1;".to_string(), "SELECT 2;".to_string()]
        );
        assert_eq!(out.sections_seen, 1);
        assert_eq!(out.sections_kept, 0);
        assert_eq!(out.sections_dropped, 1);
    }

    #[test]
    fn nested_enabled_inside_enabled_is_kept() {
        let sql = "\
-- Section Outer
A;
-- Section Inner
B;
-- End Section Inner
C;
-- End Section Outer";
        let out = process_sections(&lines(sql), &["Outer", "Inner"], &[]);
        assert_eq!(
            out.lines,
            vec![
                "-- Section Outer".to_string(),
                "A;".to_string(),
                "-- Section Inner".to_string(),
                "B;".to_string(),
                "-- End Section Inner".to_string(),
                "C;".to_string(),
                "-- End Section Outer".to_string(),
            ]
        );
    }

    #[test]
    fn nested_disabled_inside_enabled_drops_inner() {
        let sql = "\
-- Section Outer
A;
-- Section Inner
B;
-- End Section Inner
C;
-- End Section Outer";
        let out = process_sections(&lines(sql), &["Outer"], &[]);
        assert_eq!(
            out.lines,
            vec![
                "-- Section Outer".to_string(),
                "A;".to_string(),
                // Inner section is dropped entirely — markers and body.
                "C;".to_string(),
                "-- End Section Outer".to_string(),
            ]
        );
    }

    #[test]
    fn nested_enabled_inside_disabled_stays_disabled() {
        // Even with "Inner" in enabled, the outer "Outer" being disabled
        // forces everything inside to drop. Mirrors the Java rule:
        // re-evaluate the enable bit ONLY when currently in an enabled
        // context.
        let sql = "\
-- Section Outer
A;
-- Section Inner
B;
-- End Section Inner
C;
-- End Section Outer
D;";
        let out = process_sections(&lines(sql), &["Inner"], &[]);
        assert_eq!(out.lines, vec!["D;".to_string()]);
    }

    #[test]
    fn section_name_match_is_case_insensitive() {
        let sql = "\
-- Section WithRegClassID
A;
-- End Section WithRegClassID";
        let out = process_sections(&lines(sql), &["withregclassid"], &[]);
        assert_eq!(
            out.lines,
            vec![
                "-- Section WithRegClassID".to_string(),
                "A;".to_string(),
                "-- End Section WithRegClassID".to_string(),
            ]
        );
    }

    #[test]
    fn replacements_apply_to_non_marker_lines_only() {
        let sql = "\
-- Section Foo
SELECT ##context.year##;
-- End Section Foo
##context.year##";
        let repl = vec![("##context.year##".to_string(), "2030".to_string())];
        let out = process_sections(&lines(sql), &["Foo"], &repl);
        assert_eq!(
            out.lines,
            vec![
                "-- Section Foo".to_string(),
                "SELECT 2030;".to_string(),
                "-- End Section Foo".to_string(),
                "2030".to_string(),
            ]
        );
    }

    #[test]
    fn blank_lines_are_silently_dropped() {
        let sql = "\
SELECT 1;


SELECT 2;";
        let out = process_sections(&lines(sql), &[], &[]);
        assert_eq!(
            out.lines,
            vec!["SELECT 1;".to_string(), "SELECT 2;".to_string()]
        );
    }

    #[test]
    fn end_section_without_matching_open_does_not_panic() {
        // Documentation-tool defensive behavior: feeding malformed input
        // should still produce something, not panic.
        let out = process_sections(&lines("-- End Section Foo\nSELECT 1;"), &[], &[]);
        // We still emit the marker because the leading stack value is `true`.
        assert_eq!(
            out.lines,
            vec!["-- End Section Foo".to_string(), "SELECT 1;".to_string()]
        );
    }

    #[test]
    fn section_name_with_spaces_matches() {
        let sql = "\
-- Section Create Remote Tables for Extracted Data
A;
-- End Section Create Remote Tables for Extracted Data";
        let out = process_sections(
            &lines(sql),
            &["Create Remote Tables for Extracted Data"],
            &[],
        );
        assert_eq!(
            out.lines,
            vec![
                "-- Section Create Remote Tables for Extracted Data".to_string(),
                "A;".to_string(),
                "-- End Section Create Remote Tables for Extracted Data".to_string(),
            ]
        );
    }

    #[test]
    fn same_section_appearing_twice_is_handled_independently() {
        let sql = "\
-- Section Foo
A;
-- End Section Foo
B;
-- Section Foo
C;
-- End Section Foo";
        let out = process_sections(&lines(sql), &["Foo"], &[]);
        assert_eq!(
            out.lines,
            vec![
                "-- Section Foo".to_string(),
                "A;".to_string(),
                "-- End Section Foo".to_string(),
                "B;".to_string(),
                "-- Section Foo".to_string(),
                "C;".to_string(),
                "-- End Section Foo".to_string(),
            ]
        );
        assert_eq!(out.sections_seen, 2);
        assert_eq!(out.sections_kept, 2);
    }
}
