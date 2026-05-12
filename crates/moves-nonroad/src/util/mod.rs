//! Small platform / environment helpers ported from `getsys.f`,
//! `getime.f`, and (cross-referenced) `getind.f`.
//!
//! Task 99. These three Fortran files are interactive-shell and
//! current-date plumbing that the Rust port handles with the standard
//! library:
//!
//! | Fortran source | Rust replacement |
//! |---|---|
//! | `getsys.f` | [`args::get_options_filename`] |
//! | `getime.f` | [`time::format_now`] |
//! | `getind.f` | [`crate::input::indicator::IndicatorTable::lookup`] |
//!
//! `getind.f` is implemented as a streaming-file search in the
//! Fortran source; the Rust port collapses it to the
//! [`crate::input::indicator::IndicatorTable`] hash lookup, whose
//! docstring documents the year-selection rule line-by-line against
//! the Fortran code.

pub mod args;
pub mod time;
