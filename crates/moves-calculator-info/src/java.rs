//! Light Java-source scanner that recovers MasterLoop subscription style
//! and declared granularity / priority for each calculator class.
//!
//! Java parsing here is **deliberately limited** — we don't build an AST.
//! The scanner walks each `*.java` file once and looks for a small set of
//! syntactic landmarks that the MOVES codebase uses consistently:
//!
//! * `extends GenericCalculatorBase` — the calculator inherits subscription
//!   plumbing. Its constructor's `super(...)` call carries the granularity
//!   (2nd positional arg) and priority adjustment (3rd positional arg).
//! * `targetLoop.subscribe(this, <process>, MasterLoopGranularity.X,
//!   MasterLoopPriority.Y)` inside `subscribeToMe()` — the calculator
//!   subscribes itself directly.
//! * `// This is a chained calculator, so don't subscribe to the MasterLoop`
//!   plus `c.chainCalculator(this)` — the calculator is chained-only;
//!   granularity comes from whatever upstream calculator triggers it.
//!
//! The scanner returns one or more [`JavaSubscription`] records per class.
//! For the GenericCalculatorBase path, the granularity covers every
//! pollutant-process the constructor declares. For explicit
//! `targetLoop.subscribe` calls, each call becomes one record with the
//! process placeholder filled in literally (it's almost always a variable
//! reference, not a hard-coded process id — Phase 2 resolves it at
//! runtime).
//!
//! This is a best-effort fill-in for calculators that are absent from
//! `CalculatorInfo.txt`'s `Subscribe` directives (typically because the
//! fixture suite that produced the file didn't trigger them). When both
//! sources are available, the runtime log wins on conflict — it captures
//! the actual subscription that fired, including any RunSpec-derived
//! gating logic the static reader can't see.

use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::loop_meta::{Granularity, Priority, PriorityBase};

/// How a calculator hooks into the MasterLoop.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubscribeStyle {
    /// Inherits `subscribeToMe` from `GenericCalculatorBase`; granularity
    /// comes from the `super(...)` call.
    GenericBase,
    /// Overrides `subscribeToMe` with explicit `targetLoop.subscribe(...)`
    /// calls.
    Explicit,
    /// Overrides `subscribeToMe` to chain itself via
    /// `c.chainCalculator(this)` and does NOT call `targetLoop.subscribe`.
    /// Granularity is whatever the upstream trigger declares.
    ChainedOnly,
    /// `subscribeToMe` neither subscribed nor chained — likely
    /// abstract/utility class or a base we shouldn't scan from.
    Unknown,
}

/// One subscription record recovered from Java source.
///
/// `process_expr` is the literal text between the `subscribe(this,` and the
/// next comma — usually a variable name like `process` or
/// `pollutants.process`, not a numeric id. We keep it as a string for
/// transparency; Phase 2 resolves it during DAG instantiation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JavaSubscription {
    pub calculator: String,
    pub java_path: PathBuf,
    pub style: SubscribeStyle,
    /// `None` for chained-only calculators.
    pub granularity: Option<Granularity>,
    /// `None` for chained-only calculators.
    pub priority: Option<Priority>,
    /// Free-form description of the process selector. Empty for chained
    /// calculators or for GenericCalculatorBase (which subscribes against
    /// every entry in the constructor's `pollutantProcessIDs` array).
    pub process_expr: String,
}

/// Walk every `*.java` file under `source_dir` (recursively), keep the
/// ones that look like a calculator class (`extends *Calculator`,
/// `extends *CalculatorBase`, or `extends GenericCalculatorBase`), and
/// return one [`JavaSubscription`] per recoverable record. Files that
/// don't look like calculator classes are skipped silently.
///
/// `java_path` on each returned record is relative to `source_dir` so the
/// output JSON doesn't bake in any absolute build path. Determinism: the
/// returned list is sorted by `(calculator, java_path, process_expr)`.
pub fn scan_source_dir(source_dir: &Path) -> Result<Vec<JavaSubscription>> {
    let mut hits: Vec<JavaSubscription> = Vec::new();
    visit_dir(source_dir, &mut |path: &Path| {
        if path.extension().and_then(|e| e.to_str()) != Some("java") {
            return Ok(());
        }
        let bytes = fs::read(path).map_err(|e| Error::Io {
            path: path.to_path_buf(),
            source: e,
        })?;
        let Ok(text) = std::str::from_utf8(&bytes) else {
            return Ok(());
        };
        let relative = path.strip_prefix(source_dir).unwrap_or(path).to_path_buf();
        let parsed = parse_java_subscriptions(text, &relative);
        hits.extend(parsed);
        Ok(())
    })?;
    hits.sort_by(|a, b| {
        a.calculator
            .cmp(&b.calculator)
            .then_with(|| a.java_path.cmp(&b.java_path))
            .then_with(|| a.process_expr.cmp(&b.process_expr))
    });
    Ok(hits)
}

fn visit_dir(dir: &Path, f: &mut dyn FnMut(&Path) -> Result<()>) -> Result<()> {
    let entries = fs::read_dir(dir).map_err(|e| Error::Io {
        path: dir.to_path_buf(),
        source: e,
    })?;
    let mut paths: Vec<PathBuf> = entries.filter_map(|e| e.ok().map(|de| de.path())).collect();
    paths.sort();
    for path in paths {
        if path.is_dir() {
            visit_dir(&path, f)?;
        } else {
            f(&path)?;
        }
    }
    Ok(())
}

/// Parse subscription metadata from a single Java source file's text.
///
/// Returns:
/// * Zero records if the file isn't a calculator class.
/// * One record per explicit `targetLoop.subscribe(...)` call inside
///   `subscribeToMe()` for an `EmissionCalculator` subclass.
/// * One record for the constructor's `super(...)` granularity + priority
///   for a `GenericCalculatorBase` subclass.
/// * One record with style `ChainedOnly` for a class whose `subscribeToMe`
///   only chains.
pub fn parse_java_subscriptions(text: &str, path: &Path) -> Vec<JavaSubscription> {
    let Some(class_name) = find_top_level_class_name(text) else {
        return Vec::new();
    };
    let Some(base) = find_base_class(text, &class_name) else {
        return Vec::new();
    };

    let mut out = Vec::new();

    if base == "GenericCalculatorBase" {
        if let Some((g, p)) = find_generic_base_subscription(text) {
            out.push(JavaSubscription {
                calculator: class_name.clone(),
                java_path: path.to_path_buf(),
                style: SubscribeStyle::GenericBase,
                granularity: Some(g),
                priority: Some(p),
                process_expr: String::new(),
            });
        }
        return out;
    }

    // Scan inside subscribeToMe() for explicit subscribe calls.
    if let Some(body) = extract_subscribe_to_me_body(text) {
        let mut explicit_found = false;
        for hit in find_target_loop_subscribe_calls(body) {
            explicit_found = true;
            out.push(JavaSubscription {
                calculator: class_name.clone(),
                java_path: path.to_path_buf(),
                style: SubscribeStyle::Explicit,
                granularity: Some(hit.granularity),
                priority: Some(hit.priority),
                process_expr: hit.process_expr,
            });
        }
        if !explicit_found
            && (body.contains("chainCalculator(") || body.contains("This is a chained calculator"))
        {
            out.push(JavaSubscription {
                calculator: class_name.clone(),
                java_path: path.to_path_buf(),
                style: SubscribeStyle::ChainedOnly,
                granularity: None,
                priority: None,
                process_expr: String::new(),
            });
        }
        if out.is_empty() {
            out.push(JavaSubscription {
                calculator: class_name,
                java_path: path.to_path_buf(),
                style: SubscribeStyle::Unknown,
                granularity: None,
                priority: None,
                process_expr: String::new(),
            });
        }
    }
    out
}

/// Find the first `public class NAME` declaration in the file.
fn find_top_level_class_name(text: &str) -> Option<String> {
    let needle = "public class ";
    let idx = text.find(needle)?;
    let after = &text[idx + needle.len()..];
    let end = after
        .find(|c: char| !is_java_ident_char(c))
        .unwrap_or(after.len());
    let name = &after[..end];
    if name.is_empty() {
        None
    } else {
        Some(name.to_string())
    }
}

fn is_java_ident_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_' || c == '$'
}

/// Return the symbol name following `extends` for `class CLASS_NAME ... extends X`.
/// Stops at any non-identifier character (which excludes generic parameters
/// — we don't need them).
fn find_base_class(text: &str, class_name: &str) -> Option<String> {
    let class_decl = format!("class {class_name}");
    let class_idx = text.find(&class_decl)?;
    let rest = &text[class_idx + class_decl.len()..];
    // We allow `extends` to appear within the next line or two.
    let stop = rest.find('{').unwrap_or(rest.len());
    let header = &rest[..stop];
    let ext_idx = header.find("extends ")?;
    let after = &header[ext_idx + "extends ".len()..];
    let end = after
        .find(|c: char| !is_java_ident_char(c))
        .unwrap_or(after.len());
    let name = &after[..end];
    if name.is_empty() {
        None
    } else {
        Some(name.to_string())
    }
}

/// Find the granularity + priority a `GenericCalculatorBase` subclass
/// passes to its `super(...)` call. The constructor's signature is
/// `super(String[] pollutantProcessIDs, MasterLoopGranularity granularity,
/// int priorityAdjustment, ...)`, so we look for `MasterLoopGranularity.X`
/// followed by `, N,` (an integer literal) within the same super() call.
fn find_generic_base_subscription(text: &str) -> Option<(Granularity, Priority)> {
    let super_idx = find_outermost_super_call_start(text)?;
    let super_body = &text[super_idx..];
    let end = match_paren(super_body, '(', ')')?;
    let body = &super_body[..end];
    let (gname, after_g) = find_granularity_constant(body)?;
    let priority_offset = find_first_int_after(after_g)?;
    let priority = Priority {
        base: PriorityBase::EmissionCalculator,
        offset: priority_offset,
    };
    let granularity = Granularity::from_str(&gname).ok()?;
    Some((granularity, priority))
}

/// Locate the first `super(` in the file (whitespace + `(` allowed).
/// Returns the byte offset of the opening `(`.
fn find_outermost_super_call_start(text: &str) -> Option<usize> {
    let mut search_from = 0;
    while let Some(rel) = text[search_from..].find("super") {
        let idx = search_from + rel;
        let after = &text[idx + 5..];
        let trimmed = after.trim_start();
        if trimmed.starts_with('(') {
            return Some(idx + 5 + (after.len() - trimmed.len()));
        }
        search_from = idx + 5;
    }
    None
}

/// Given `body` starting at the opening `(`, return the index INTO `body`
/// of the matching closing `)`. Skips string literals and char literals.
fn match_paren(body: &str, open: char, close: char) -> Option<usize> {
    let bytes = body.as_bytes();
    if bytes.is_empty() || bytes[0] as char != open {
        return None;
    }
    let mut depth = 0i32;
    let mut in_str = false;
    let mut in_char = false;
    let mut in_line_comment = false;
    let mut in_block_comment = false;
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i] as char;
        let next = bytes.get(i + 1).map(|&b| b as char).unwrap_or('\0');
        if in_line_comment {
            if c == '\n' {
                in_line_comment = false;
            }
        } else if in_block_comment {
            if c == '*' && next == '/' {
                in_block_comment = false;
                i += 1;
            }
        } else if in_str {
            if c == '\\' {
                i += 1;
            } else if c == '"' {
                in_str = false;
            }
        } else if in_char {
            if c == '\\' {
                i += 1;
            } else if c == '\'' {
                in_char = false;
            }
        } else if c == '/' && next == '/' {
            in_line_comment = true;
            i += 1;
        } else if c == '/' && next == '*' {
            in_block_comment = true;
            i += 1;
        } else if c == '"' {
            in_str = true;
        } else if c == '\'' {
            in_char = true;
        } else if c == open {
            depth += 1;
        } else if c == close {
            depth -= 1;
            if depth == 0 {
                return Some(i + 1);
            }
        }
        i += 1;
    }
    None
}

/// Locate `MasterLoopGranularity.X` and return `(X, &after)`.
fn find_granularity_constant(text: &str) -> Option<(String, &str)> {
    let needle = "MasterLoopGranularity.";
    let idx = text.find(needle)?;
    let after_dot = &text[idx + needle.len()..];
    let end = after_dot
        .find(|c: char| !is_java_ident_char(c))
        .unwrap_or(after_dot.len());
    let name = &after_dot[..end];
    Some((name.to_string(), &after_dot[end..]))
}

/// Scan forward in `text` for the first integer literal. Skips whitespace,
/// commas, and end-of-line comments before the integer.
fn find_first_int_after(text: &str) -> Option<i32> {
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i] as char;
        if c.is_whitespace() || c == ',' {
            i += 1;
            continue;
        }
        if c == '/' && bytes.get(i + 1).map(|&b| b as char) == Some('/') {
            // line comment — skip to end of line
            while i < bytes.len() && (bytes[i] as char) != '\n' {
                i += 1;
            }
            continue;
        }
        if c == '/' && bytes.get(i + 1).map(|&b| b as char) == Some('*') {
            // block comment — skip to closing */
            i += 2;
            while i + 1 < bytes.len()
                && !((bytes[i] as char) == '*' && (bytes[i + 1] as char) == '/')
            {
                i += 1;
            }
            i += 2;
            continue;
        }
        // Otherwise expect optional sign + digits.
        let start = i;
        if c == '-' || c == '+' {
            i += 1;
        }
        let digit_start = i;
        while i < bytes.len() && (bytes[i] as char).is_ascii_digit() {
            i += 1;
        }
        if i == digit_start {
            return None;
        }
        let literal = &text[start..i];
        return literal.parse::<i32>().ok();
    }
    None
}

/// Locate the body of `subscribeToMe(MasterLoop targetLoop)` if present.
fn extract_subscribe_to_me_body(text: &str) -> Option<&str> {
    let needle = "subscribeToMe(";
    let idx = text.find(needle)?;
    let after = &text[idx..];
    let open_brace = after.find('{')?;
    let body = &after[open_brace..];
    let close = match_paren(body, '{', '}')?;
    Some(&body[1..close - 1])
}

struct ExplicitSubscribe {
    granularity: Granularity,
    priority: Priority,
    process_expr: String,
}

/// Find every `targetLoop.subscribe(this, <expr>, MasterLoopGranularity.X,
/// MasterLoopPriority.Y[+/-N])` call inside `body`.
fn find_target_loop_subscribe_calls(body: &str) -> Vec<ExplicitSubscribe> {
    let mut out = Vec::new();
    let mut cursor = 0;
    while let Some(rel) = body[cursor..].find(".subscribe(") {
        let open = cursor + rel + ".subscribe".len();
        let call_body = &body[open..];
        let Some(close) = match_paren(call_body, '(', ')') else {
            break;
        };
        let args_text = &call_body[1..close - 1];
        let args = split_top_level_args(args_text);
        cursor = open + close;
        if args.len() != 4 {
            continue;
        }
        if args[0].trim() != "this" {
            continue;
        }
        let process_expr = args[1].trim().to_string();
        let Some((gname, _)) = find_granularity_constant(args[2]) else {
            continue;
        };
        let Ok(granularity) = Granularity::from_str(&gname) else {
            continue;
        };
        let priority = match parse_priority_expr(args[3]) {
            Some(p) => p,
            None => continue,
        };
        out.push(ExplicitSubscribe {
            granularity,
            priority,
            process_expr,
        });
    }
    out
}

/// Parse a Java expression like `MasterLoopPriority.EMISSION_CALCULATOR`
/// or `MasterLoopPriority.EMISSION_CALCULATOR+priorityAdjustment` or
/// `MasterLoopPriority.GENERATOR+1`.
fn parse_priority_expr(arg: &str) -> Option<Priority> {
    let needle = "MasterLoopPriority.";
    let idx = arg.find(needle)?;
    let after = &arg[idx + needle.len()..];
    let end = after
        .find(|c: char| !is_java_ident_char(c))
        .unwrap_or(after.len());
    let base_name = &after[..end];
    let base = match base_name {
        "INTERNAL_CONTROL_STRATEGY" => PriorityBase::InternalControlStrategy,
        "GENERATOR" => PriorityBase::Generator,
        "EMISSION_CALCULATOR" => PriorityBase::EmissionCalculator,
        _ => return None,
    };
    let tail = after[end..].trim_start();
    if tail.is_empty() {
        return Some(Priority { base, offset: 0 });
    }
    // tail starts with '+' or '-' then either an int literal or a variable
    // we can't statically resolve.
    let sign = tail.as_bytes()[0] as char;
    if sign != '+' && sign != '-' {
        return Some(Priority { base, offset: 0 });
    }
    let after_sign = tail[1..].trim_start();
    let digit_end = after_sign
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(after_sign.len());
    if digit_end == 0 {
        // Symbolic offset (e.g. `+priorityAdjustment`) — we can't resolve
        // it statically. Treat as the unadjusted base.
        return Some(Priority { base, offset: 0 });
    }
    let n: i32 = after_sign[..digit_end].parse().ok()?;
    let offset = if sign == '+' { n } else { -n };
    Some(Priority { base, offset })
}

/// Split a comma-separated argument list, respecting nested parens / brackets / strings.
fn split_top_level_args(args: &str) -> Vec<&str> {
    let bytes = args.as_bytes();
    let mut out = Vec::new();
    let mut start = 0;
    let mut depth_p = 0i32;
    let mut depth_b = 0i32;
    let mut depth_c = 0i32;
    let mut in_str = false;
    let mut in_char = false;
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i] as char;
        if in_str {
            if c == '\\' {
                i += 2;
                continue;
            }
            if c == '"' {
                in_str = false;
            }
        } else if in_char {
            if c == '\\' {
                i += 2;
                continue;
            }
            if c == '\'' {
                in_char = false;
            }
        } else {
            match c {
                '"' => in_str = true,
                '\'' => in_char = true,
                '(' => depth_p += 1,
                ')' => depth_p -= 1,
                '[' => depth_b += 1,
                ']' => depth_b -= 1,
                '{' => depth_c += 1,
                '}' => depth_c -= 1,
                ',' if depth_p == 0 && depth_b == 0 && depth_c == 0 => {
                    out.push(&args[start..i]);
                    start = i + 1;
                }
                _ => {}
            }
        }
        i += 1;
    }
    if start <= args.len() {
        out.push(&args[start..]);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(java: &str) -> Vec<JavaSubscription> {
        parse_java_subscriptions(java, Path::new("test.java"))
    }

    #[test]
    fn generic_base_constructor_extracts_granularity_and_offset() {
        let src = r#"
package gov.epa.otaq.moves.master.implementation.ghg;
public class FooCalc extends GenericCalculatorBase {
    public FooCalc() {
        super(new String[] { "11801" },
            MasterLoopGranularity.YEAR,
            2, // priority adjustment
            "database/Foo.sql",
            null);
    }
}
"#;
        let hits = parse(src);
        assert_eq!(hits.len(), 1);
        let h = &hits[0];
        assert_eq!(h.calculator, "FooCalc");
        assert_eq!(h.style, SubscribeStyle::GenericBase);
        assert_eq!(h.granularity, Some(Granularity::Year));
        assert_eq!(h.priority.unwrap().value(), 12);
    }

    #[test]
    fn explicit_subscribe_in_subscribe_to_me() {
        let src = r#"
public class BarCalc extends EmissionCalculator {
    public void subscribeToMe(MasterLoop targetLoop) {
        EmissionProcess process = something();
        targetLoop.subscribe(this, process, MasterLoopGranularity.MONTH,
                MasterLoopPriority.EMISSION_CALCULATOR);
    }
}
"#;
        let hits = parse(src);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].style, SubscribeStyle::Explicit);
        assert_eq!(hits[0].granularity, Some(Granularity::Month));
        assert_eq!(hits[0].priority.unwrap().display(), "EMISSION_CALCULATOR");
        assert_eq!(hits[0].process_expr, "process");
    }

    #[test]
    fn explicit_subscribe_priority_with_int_offset() {
        let src = r#"
public class BazCalc extends EmissionCalculator {
    public void subscribeToMe(MasterLoop targetLoop) {
        targetLoop.subscribe(this, p, MasterLoopGranularity.DAY,
                MasterLoopPriority.GENERATOR+1);
    }
}
"#;
        let hits = parse(src);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].priority.unwrap().display(), "GENERATOR+1");
    }

    #[test]
    fn explicit_subscribe_priority_with_symbolic_offset() {
        // Boilerplate inside GenericCalculatorBase itself — its
        // subscribeToMe uses MasterLoopPriority.EMISSION_CALCULATOR+priorityAdjustment.
        let src = r#"
public class GenericCalculatorBase extends EmissionCalculator {
    public void subscribeToMe(MasterLoop targetLoop) {
        targetLoop.subscribe(this, pollutants.process, granularity,
                MasterLoopPriority.EMISSION_CALCULATOR+priorityAdjustment);
    }
}
"#;
        let hits = parse(src);
        // The base class isn't a GenericCalculatorBase subclass itself —
        // and granularity is a variable. We don't try to resolve.
        assert_eq!(hits.len(), 1);
        // granularity is None because `granularity` (lowercase) isn't a
        // MasterLoopGranularity.X reference.
        assert_eq!(hits[0].granularity, None);
        // Style falls through to Unknown because we couldn't parse the
        // call as an explicit subscribe at a static granularity.
        assert_eq!(hits[0].style, SubscribeStyle::Unknown);
    }

    #[test]
    fn chained_only_calculator_detected() {
        let src = r#"
public class HCSpeciationCalculator extends EmissionCalculator {
    public void subscribeToMe(MasterLoop targetLoop) {
        // This is a chained calculator, so don't subscribe to the MasterLoop.
        for (EmissionCalculator c : upstream) {
            c.chainCalculator(this);
        }
    }
}
"#;
        let hits = parse(src);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].style, SubscribeStyle::ChainedOnly);
        assert!(hits[0].granularity.is_none());
    }

    #[test]
    fn ignores_non_calculator_files() {
        let src = r#"
public class JustAUtility {
    public static int answer() { return 42; }
}
"#;
        let hits = parse(src);
        assert!(hits.is_empty());
    }

    #[test]
    fn scan_source_dir_is_sorted_and_deterministic() {
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        // Write three calc files in non-alphabetical disk order.
        fs::write(
            dir.path().join("BBB.java"),
            r#"public class BBB extends GenericCalculatorBase {
                    public BBB() { super(null, MasterLoopGranularity.MONTH, 0); }
                }"#,
        )
        .unwrap();
        fs::write(
            dir.path().join("AAA.java"),
            r#"public class AAA extends GenericCalculatorBase {
                    public AAA() { super(null, MasterLoopGranularity.YEAR, 1); }
                }"#,
        )
        .unwrap();
        fs::write(
            dir.path().join("CCC.java"),
            r#"public class CCC extends EmissionCalculator {
                    public void subscribeToMe(MasterLoop targetLoop) {
                        // This is a chained calculator
                        upstream.chainCalculator(this);
                    }
                }"#,
        )
        .unwrap();
        let hits1 = scan_source_dir(dir.path()).unwrap();
        let hits2 = scan_source_dir(dir.path()).unwrap();
        assert_eq!(hits1, hits2);
        let names: Vec<_> = hits1.iter().map(|h| h.calculator.as_str()).collect();
        assert_eq!(names, vec!["AAA", "BBB", "CCC"]);
    }
}
