//! Command-line argument helper (`getsys.f`).
//!
//! Task 99. The Fortran `getsys.f` reads the first argument as the
//! options filename, falling back to an interactive prompt when no
//! argument is supplied. In the Rust port the CLI is the caller's
//! responsibility — this module exposes a small helper that returns
//! either the first non-program-name argument or [`Outcome::Prompt`]
//! when none is present.
//!
//! WASM-compatibility: this module avoids `std::process` and works
//! against any `IntoIterator<Item=String>`, so the WASM target can
//! supply an alternative source of arguments at the seam.
//!
//! # Fortran source
//!
//! Ports `getsys.f` (98 lines).

/// Outcome of resolving an options-file argument.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The first command-line argument, taken to be the options file.
    Argument(String),
    /// No argument supplied — the Fortran source then prompted the
    /// user via `IORSTD`. The Rust port leaves the prompt to the
    /// caller and just signals that the argument was absent.
    Prompt,
}

/// Resolve the options-file argument from an iterator of program
/// arguments. The first element of `args` is assumed to be the
/// program name (matching `std::env::args()`'s convention) and is
/// skipped.
pub fn resolve<I, S>(args: I) -> Outcome
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let mut iter = args.into_iter().map(Into::into);
    // Drop argv[0].
    let _ = iter.next();
    match iter.next() {
        Some(s) if !s.trim().is_empty() => Outcome::Argument(trimmed_first_token(&s)),
        _ => Outcome::Prompt,
    }
}

/// Convenience wrapper around [`resolve`] that reads from
/// [`std::env::args`].
///
/// The wrapper is gated against `wasm32-unknown-unknown` because
/// `std::env::args` aborts there; WASM callers should call
/// [`resolve`] directly with whatever argument source they have.
#[cfg(not(target_arch = "wasm32"))]
pub fn get_options_filename() -> Outcome {
    resolve(std::env::args())
}

fn trimmed_first_token(s: &str) -> String {
    // Mirrors the Fortran `lftjst` + `INDEX(cline, ' ')` slice: trim
    // leading whitespace then take everything up to the first
    // embedded space.
    let stripped = s.trim_start();
    match stripped.find(char::is_whitespace) {
        Some(idx) => stripped[..idx].to_string(),
        None => stripped.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn returns_first_real_argument() {
        let argv = vec!["nonroad", "demo.opt"];
        assert_eq!(
            resolve(argv.into_iter()),
            Outcome::Argument("demo.opt".to_string())
        );
    }

    #[test]
    fn drops_trailing_space_padded_argument() {
        // Fortran semantics: the first whitespace splits the argument.
        let argv = vec!["nonroad", "demo.opt extra"];
        assert_eq!(
            resolve(argv.into_iter()),
            Outcome::Argument("demo.opt".to_string())
        );
    }

    #[test]
    fn missing_argument_signals_prompt() {
        let argv = vec!["nonroad"];
        assert_eq!(resolve(argv.into_iter()), Outcome::Prompt);
    }

    #[test]
    fn empty_argument_signals_prompt() {
        let argv = vec!["nonroad", "   "];
        assert_eq!(resolve(argv.into_iter()), Outcome::Prompt);
    }
}
