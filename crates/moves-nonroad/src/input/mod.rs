//! NONROAD input-file parsers (`rd*.f`).
//!
//! Cluster 6 of the seven functional groups (see
//! `ARCHITECTURE.md` § 2.6). The Fortran source has ~30 readers,
//! one per input format (`.POP`, `.ALO`, `.GRW`, `.EMF`, `.DAT`,
//! etc.); each parses a fixed-width or column-aligned text format
//! using Fortran `READ` statements with explicit format strings.
//!
//! # I/O policy
//!
//! Per `ARCHITECTURE.md` § 4.3, parsers consume
//! [`std::io::BufRead`] rather than Fortran-style integer unit
//! numbers. The first concrete parser (Task 94: `.POP`) establishes
//! the function-signature pattern; subsequent parsers conform.
//! Buffering is the caller's responsibility — parsers accept
//! pre-buffered readers to avoid double-buffering.
//!
//! # Roadmap
//!
//! | Task | Files |
//! |---|---|
//! | 94 (.POP, .ALO)                | `rdpop.f`, `rdalo.f` |
//! | 95 (.GRW, .DAT, .GXR, .DAY)    | `rdgrow.f`, `rdgxrf.f`, `rdseas.f`, `rdday.f` |
//! | 96 (.EMF, .TCH, evap variants) | `rdemfc.f`, `rdevemfc.f`, `rdtech.f`, `rdtech_moves.f`, `rdevtech.f`, `rdevtech_moves.f` |
//! | 97 (activity, deterioration, …) | `rdact.f`, `rddetr.f`, `rdspil.f`, `rdsulf.f`, `rdrgndf.f`, `rdscrp.f`, `rdstg2.f`, `rdalt.f`, `rdbsfc.f`, `rdefls.f`, `rdfips.f`, `rdind.f`, `rdnropt.f`, `rdnrper.f`, `rdnrreg.f`, `rdnrsrc.f` |
//! | 98 (retrofit)                  | `rdrtrft.f` |
//!
//! # Status
//!
//! Phase 5 skeleton — no code yet. Sub-modules
//! (e.g. `input::pop`, `input::alo`) get added in their respective
//! tasks; each carries rustdoc that names the Fortran source it
//! ports.
