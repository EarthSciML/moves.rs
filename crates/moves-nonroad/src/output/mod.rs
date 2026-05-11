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
//! Per `ARCHITECTURE.md` § 4.3, writers accept
//! [`std::io::Write`] rather than Fortran-style integer unit
//! numbers. The Rust port emits two output formats:
//!
//! * the original NONROAD text format for backwards compatibility
//!   with downstream tools (Task 114), and
//! * Apache Parquet, sharing the unified Phase 4 output schema
//!   defined by Task 89 (Task 114).
//!
//! Writers are independent implementations against the same
//! `Write` interface; the caller chooses which format(s) to emit.
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

pub mod find;
pub mod fips;
pub mod strutil;
