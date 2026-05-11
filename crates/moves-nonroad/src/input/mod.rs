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
//! Task 94 parsers implemented:
//! - `pop` — `.POP` population parser (`rdpop.f`)
//! - `alo` — `.ALO` allocation parser (`rdalo.f`)
//!
//! Task 95 parsers implemented:
//! - `growth` — `.GRW` growth factor parser (`rdgrow.f`)
//! - `gxr` — `.GXR` growth extrapolation parser (`rdgxrf.f`)
//! - `seasonal` — `.DAT` seasonal and `.DAY` day-of-year parsers (`rdseas.f`, `rdday.f`)
//!
//! Task 96 parsers implemented:
//! - `emfc` — exhaust emission factor `.EMF` parser (`rdemfc.f`,
//!   also serves the BSFC dispatcher)
//! - `evemfc` — evap emission factor `.EMF` parser (`rdevemfc.f`)
//! - `tech` — `/TECH FRAC/` packet (`rdtech.f`)
//! - `tech_moves` — `/MOVES TECH FRAC/` packet (`rdtech_moves.f`)
//! - `evtech` — `/EVAP TECH FRAC/` packet (`rdevtech.f`)
//! - `evtech_moves` — `/MOVES EVAP TECH FRAC/` packet
//!   (`rdevtech_moves.f`)
//!
//! Task 97 parsers implemented:
//! - `activity` — activity file (`rdact.f`)
//! - `deterioration` — deterioration factors (`rddetr.f`)
//! - `spillage` — spillage / permeation factors (`rdspil.f`)
//! - `sulfur` — `/PM BASE SULFUR/` packet (`rdsulf.f`)
//! - `region_def` — region definitions (`rdrgndf.f`)
//! - `scrappage` — scrappage curve (`rdscrp.f`)
//! - `stage2` — `/STAGE II/` packet (`rdstg2.f`)
//! - `alt_scrap` — alternate scrappage curves (`rdalt.f`)
//! - `bsfc` — BSFC dispatcher, wired to `emfc::read_bsfc`
//!   (`rdbsfc.f`)
//! - `efls` — emission-factor-files dispatcher (`rdefls.f`)
//! - `fips` — county FIPS data (`rdfips.f`)
//! - `indicator` — spatial indicator records (`rdind.f`)
//! - `options` — `/OPTIONS/` packet (`rdnropt.f`)
//! - `period` — `/PERIOD/` packet (`rdnrper.f`)
//! - `region` — `/REGION/` packet (`rdnrreg.f`)
//! - `source_cat` — `/SOURCE CATEGORY/` packet (`rdnrsrc.f`)
//!
//! Task 98 parsers implemented:
//! - `retrofit` — retrofit input (`rdrtrft.f`) plus the four
//!   validators (`vldrtrft{recs,hp,scc,tchtyp}.f`)

pub mod activity;
pub mod alo;
pub mod alt_scrap;
pub mod bsfc;
pub mod deterioration;
pub mod efls;
pub mod emfc;
pub mod evemfc;
pub mod evtech;
pub mod evtech_moves;
pub mod fips;
pub mod growth;
pub mod gxr;
pub mod indicator;
pub mod options;
pub mod period;
pub mod pop;
pub mod region;
pub mod region_def;
pub mod retrofit;
pub mod scrappage;
pub mod seasonal;
pub mod source_cat;
pub mod spillage;
pub mod stage2;
pub mod sulfur;
pub mod tech;
pub mod tech_moves;
