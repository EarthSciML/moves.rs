//! NONROAD output writers (`wrt*.f`) and shared utilities
//! (lookups, validators, string utilities, FIPS initializers).
//!
//! Cluster 7 of the seven functional groups (see
//! `ARCHITECTURE.md` § 2.7). The Fortran source has ~7 writer
//! routines plus ~50 small helpers (`fnd*.f` lookups, `chk*.f`
//! validators, string utilities, FIPS initializers).
//!
//! # I/O policy
//!
//! Per `ARCHITECTURE.md` § 4.3, writers accept [`std::io::Write`]
//! rather than Fortran-style integer unit numbers, and return
//! [`std::io::Result`] — the orchestrating layer owns the output
//! paths and maps a failure to [`crate::Error::Io`].
//!
//! # The two output formats
//!
//! The migration plan (Task 114) calls for two output encodings:
//!
//! * the original NONROAD text format, for backwards compatibility
//!   with downstream tools — ported by the [`writers`], [`message`]
//!   and [`si_report`] modules; and
//! * Apache Parquet on the unified Phase 4 output schema
//!   (`moves-data`'s `output_schema`, Task 89).
//!
//! The structured record types ([`writers::OutputRecord`],
//! [`writers::ByModelYearRecord`], [`writers::AmsCountyEmissions`],
//! [`si_report::SiReport`]) are the format-neutral seam between the
//! two: the text writers consume them directly, and the Parquet
//! encoding consumes the same records once the cross-crate wiring
//! lands in the Task 117 NONROAD–MOVES integration step — the point
//! at which the plan places the onroad/nonroad output-schema
//! convergence. Keeping `moves-nonroad` free of the `parquet`
//! dependency preserves the WASM-compatibility posture of
//! `ARCHITECTURE.md` § 4.4.
//!
//! # Roadmap
//!
//! | Task | Files |
//! |---|---|
//! | 100 (FIPS init)        | `in1fip.f`–`in5fip.f` (`phf` static lookup tables) |
//! | 101 (find/lookup)      | `fndchr.f`, `fndasc.f`, `fndact.f`, `fnddet.f`, `fndefc.f`, `fndevefc.f`, `fndevtch.f`, `fndgxf.f`, `fndhpc.f`, `fndkey.f`, `fndreg.f`, `fndrfm.f`, `fndrtrft.f`, `fndscrp.f`, `fndtch.f`, `fndtpm.f` (replaced by `HashMap`/`BTreeMap`) |
//! | 102 (string utilities) | `strlen.f`, `strmin.f`, `lftjst.f`, `rgtjst.f`, `low2up.f`, `chrsrt.f`, `wadeeq.f`, `cnthpcat.f` |
//! | 114 (writers)          | `wrtams.f`, `wrtbmy.f`, `wrtdat.f`, `wrthdr.f`, `wrtmsg.f`, `wrtsi.f`, `wrtsum.f`, `hdrbmy.f`, `sitot.f`, `chkasc.f`, `chkwrn.f`, `clsnon.f`, `blknon.f` |
//!
//! # Status
//!
//! Sub-modules get added in their respective tasks; each carries
//! rustdoc that names the Fortran source it ports.
//!
//! Task 100 implemented:
//! - [`fips`] — static state FIPS-code table (`in1fip.f`–`in5fip.f`)
//!
//! Task 101 implemented (partial — see [`find`] module docs for the
//! list of routines deferred until their backing input parsers land):
//! - [`find`] — lookup helpers (`fndchr.f`, `fndasc.f`, `fndhpc.f`,
//!   `fndscrp.f`, `fndreg.f`, `fnddet.f`, `fndact.f`, `fndrfm.f`)
//!
//! Task 102 implemented:
//! - [`strutil`] — string and miscellaneous helpers
//!   (`strlen.f`, `strmin.f`, `lftjst.f`, `rgtjst.f`, `low2up.f`,
//!   `chrsrt.f`, `wadeeq.f`, `cnthpcat.f`).
//!
//! Task 114 implemented (the legacy-text output writers):
//! - [`fortran_fmt`] — Fortran `Ew.d`/`Fw.d`/`Iw`/`Aw` edit-
//!   descriptor formatting and the column-positioned
//!   [`FortranLine`](fortran_fmt::FortranLine) shared by the writers;
//! - [`writers`] — the `.OUT` data file and the by-model-year files
//!   (`wrthdr.f`, `wrtdat.f`, `hdrbmy.f`, `wrtbmy.f`) and the EPS2
//!   AMS workfile (`wrtams.f`);
//! - [`message`] — the message-file echo writers (`wrtmsg.f`,
//!   `wrtsum.f`);
//! - [`si_report`] — the SI-report accumulator and writer
//!   (`sitot.f`, `wrtsi.f`);
//! - [`validate`] — the SCC validator (`chkasc.f`), the warning
//!   tally (`chkwrn.f`), and the file-close routine (`clsnon.f`);
//! - [`statics`] — the `BLOCK DATA` static tables (`blknon.f`).

pub mod find;
pub mod fips;
pub mod fortran_fmt;
pub mod message;
pub mod si_report;
pub mod statics;
pub mod strutil;
pub mod validate;
pub mod writers;
