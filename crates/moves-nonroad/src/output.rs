//! Output writers and utility helpers.
//!
//! Legacy text-format output (`wrtams.f`, `wrtbmy.f`, `wrtdat.f`, `wrthdr.f`,
//! `wrtmsg.f`, `wrtsi.f`, `wrtsum.f`) and Parquet output for native MOVES
//! consumption.
//!
//! Also includes lookups (`fnd*.f`), validators (`chk*.f`), and string
//! utilities (`strlen.f`, `low2up.f`, etc.).

pub mod writers;
pub mod lookup;
pub mod strings;
