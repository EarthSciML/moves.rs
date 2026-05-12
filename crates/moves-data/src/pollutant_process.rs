//! [`PollutantProcessAssociation`] and the `polProcessID` composite key.
//!
//! Ports `gov/epa/otaq/moves/master/framework/PollutantProcessAssociation.java`.
//! The Java class wraps a `(Pollutant, EmissionProcess)` pair plus the runtime
//! `chainedTo` adjacency lists that `PollutantProcessLoader` populates from
//! the `PollutantProcessAssoc` default-DB table.
//!
//! This module ports the **identity** layer of that class:
//!
//! * [`PolProcessId`] — the composite `polProcessID = pollutantID*100 +
//!   processID` newtype.
//! * [`PollutantProcessAssociation`] — the `(pollutant_id, process_id)`
//!   value type, plus canonical `phf` maps over every valid pair that
//!   appears in the MOVES default DB (extracted from the calculator-chain
//!   characterization snapshot — see crate docs).
//!
//! The `chainedTo`/`nrChainedTo` adjacency lists are runtime data (driven
//! by `PollutantProcessAssoc.chainedto1`, `chainedto2`, `nrChainedTo1`,
//! `nrChainedTo2`) and land with the rest of the data plane in Task 50
//! (Phase 4). When that arrives, it composes with the static identity
//! layer here — the static map answers "is `(20, 1)` a legal pair?" and
//! the runtime layer answers "does `(20, 1)` depend on `(1, 1)`?".

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

use crate::error::Error;
use crate::pollutant::{Pollutant, PollutantId};
use crate::process::{EmissionProcess, ProcessId};

/// MOVES composite `polProcessID = pollutantID * 100 + processID`.
///
/// Stored on disk as `int` (`PollutantProcessAssoc.polProcessID`); we widen
/// to `u32` to accommodate the four-digit CB05/dioxin pollutants whose
/// composite ids exceed `u16::MAX`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
#[serde(transparent)]
pub struct PolProcessId(pub u32);

impl PolProcessId {
    /// Compose a `polProcessID` from its pollutant and process parts.
    ///
    /// Matches `PollutantProcessAssociation.getDatabaseKey()` exactly:
    /// `id = pollutantID * 100 + processID`. The Java code constrains
    /// `processID` to two digits; we do not re-check here, since callers
    /// build a [`ProcessId`] from a constrained id source upstream.
    #[must_use]
    pub const fn new(pollutant_id: PollutantId, process_id: ProcessId) -> Self {
        Self(pollutant_id.0 as u32 * 100 + process_id.0 as u32)
    }

    /// Extract the pollutant id half of the composite.
    ///
    /// Java reads this as `polProcessID / 100`; we mirror that, returning
    /// the upper digits as a [`PollutantId`]. For canonical inputs the
    /// quotient fits in `u16`; if a hostile caller passes a `u32` outside
    /// that range, the high bits are truncated (matching Java's silent
    /// `int` cast).
    #[must_use]
    pub const fn pollutant_id(self) -> PollutantId {
        PollutantId((self.0 / 100) as u16)
    }

    /// Extract the process id half of the composite.
    ///
    /// Java reads this as `polProcessID % 100`; always two digits.
    #[must_use]
    pub const fn process_id(self) -> ProcessId {
        ProcessId((self.0 % 100) as u16)
    }
}

impl fmt::Display for PolProcessId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<u32> for PolProcessId {
    fn from(value: u32) -> Self {
        Self(value)
    }
}

impl From<PolProcessId> for u32 {
    fn from(value: PolProcessId) -> Self {
        value.0
    }
}

impl FromStr for PolProcessId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<u32>().map(Self).map_err(|source| Error::ParseId {
            kind: "polProcess",
            input: s.to_owned(),
            source,
        })
    }
}

/// A legal pollutant/process combination as recorded in the default-DB
/// `PollutantProcessAssoc` table.
///
/// Pure identity: just the `(pollutant_id, process_id)` pair. Runtime
/// flags (`isAffectedByExhaustIM`, `isAffectedByEvapIM`, `isAffectedByOnroad`,
/// `isAffectedByNonroad`) and chaining (`chainedto1`, `chainedto2`,
/// `nrChainedTo1`, `nrChainedTo2`) live in the data plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PollutantProcessAssociation {
    /// Pollutant id half of the composite key.
    pub pollutant_id: PollutantId,
    /// Process id half of the composite key.
    pub process_id: ProcessId,
}

impl PollutantProcessAssociation {
    /// Compose the `polProcessID` for this association.
    #[must_use]
    pub const fn polproc_id(self) -> PolProcessId {
        PolProcessId::new(self.pollutant_id, self.process_id)
    }

    /// Look up the canonical association with the given composite id.
    ///
    /// Mirrors `PollutantProcessAssociation.createByID(int polProcessID)`
    /// — except this returns `None` for ids that are not legal pairings
    /// in the MOVES default DB, where the Java factory eagerly fabricates
    /// an association from any decomposable pair.
    #[must_use]
    pub fn find_by_polproc_id(id: PolProcessId) -> Option<Self> {
        BY_POLPROC_ID.get(&id.0).copied()
    }

    /// Look up the canonical association for the given `(pollutant, process)`
    /// ids.
    ///
    /// Mirrors `PollutantProcessAssociation.createByID(int, int)`.
    #[must_use]
    pub fn find_by_ids(pollutant_id: PollutantId, process_id: ProcessId) -> Option<Self> {
        Self::find_by_polproc_id(PolProcessId::new(pollutant_id, process_id))
    }

    /// Look up the canonical association for the given `(pollutant, process)`
    /// names.
    ///
    /// Mirrors `PollutantProcessAssociation.findByName(...)`. Resolves both
    /// names case-insensitively (with the numeric-id fallback from
    /// [`Pollutant::find_by_name`] / [`EmissionProcess::find_by_name`]),
    /// then checks the canonical pair table.
    #[must_use]
    pub fn find_by_names(pollutant_name: &str, process_name: &str) -> Option<Self> {
        let pollutant = Pollutant::find_by_name(pollutant_name)?;
        let process = EmissionProcess::find_by_name(process_name)?;
        Self::find_by_ids(pollutant.id, process.id)
    }

    /// Iterate every canonical association in `(pollutant_id, process_id)`
    /// order.
    pub fn all() -> impl Iterator<Item = Self> {
        ALL_ASSOCIATIONS.iter().copied()
    }
}

impl fmt::Display for PollutantProcessAssociation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let pollutant = Pollutant::find_by_id(self.pollutant_id)
            .map(|p| p.name)
            .unwrap_or("?");
        let process = EmissionProcess::find_by_id(self.process_id)
            .map(|p| p.name)
            .unwrap_or("?");
        write!(f, "{process} {pollutant}")
    }
}

const ASSOC_COUNT: usize = 960;

pub static ALL_ASSOCIATIONS: [PollutantProcessAssociation; ASSOC_COUNT] = [
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1),
        process_id: ProcessId(9),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1),
        process_id: ProcessId(10),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1),
        process_id: ProcessId(11),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1),
        process_id: ProcessId(12),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1),
        process_id: ProcessId(13),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1),
        process_id: ProcessId(18),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1),
        process_id: ProcessId(19),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1),
        process_id: ProcessId(20),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1),
        process_id: ProcessId(21),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1),
        process_id: ProcessId(30),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1),
        process_id: ProcessId(31),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1),
        process_id: ProcessId(32),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(2),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(2),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(2),
        process_id: ProcessId(9),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(2),
        process_id: ProcessId(10),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(2),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(2),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(2),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(2),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(2),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(3),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(3),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(3),
        process_id: ProcessId(9),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(3),
        process_id: ProcessId(10),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(3),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(3),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(3),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(3),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(3),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(5),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(5),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(5),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(5),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(5),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(5),
        process_id: ProcessId(18),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(5),
        process_id: ProcessId(19),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(5),
        process_id: ProcessId(20),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(5),
        process_id: ProcessId(21),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(5),
        process_id: ProcessId(30),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(5),
        process_id: ProcessId(31),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(5),
        process_id: ProcessId(32),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(5),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(5),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(6),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(6),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(6),
        process_id: ProcessId(9),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(6),
        process_id: ProcessId(10),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(6),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(6),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(6),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(6),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(6),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(20),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(20),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(20),
        process_id: ProcessId(11),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(20),
        process_id: ProcessId(12),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(20),
        process_id: ProcessId(13),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(20),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(20),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(20),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(20),
        process_id: ProcessId(18),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(20),
        process_id: ProcessId(19),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(20),
        process_id: ProcessId(20),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(20),
        process_id: ProcessId(21),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(20),
        process_id: ProcessId(22),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(20),
        process_id: ProcessId(23),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(20),
        process_id: ProcessId(24),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(20),
        process_id: ProcessId(30),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(20),
        process_id: ProcessId(31),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(20),
        process_id: ProcessId(32),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(20),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(20),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(21),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(21),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(21),
        process_id: ProcessId(11),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(21),
        process_id: ProcessId(12),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(21),
        process_id: ProcessId(13),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(21),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(21),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(21),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(21),
        process_id: ProcessId(18),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(21),
        process_id: ProcessId(19),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(21),
        process_id: ProcessId(20),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(21),
        process_id: ProcessId(21),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(21),
        process_id: ProcessId(22),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(21),
        process_id: ProcessId(23),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(21),
        process_id: ProcessId(24),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(21),
        process_id: ProcessId(30),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(21),
        process_id: ProcessId(31),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(21),
        process_id: ProcessId(32),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(22),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(22),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(22),
        process_id: ProcessId(11),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(22),
        process_id: ProcessId(12),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(22),
        process_id: ProcessId(13),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(22),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(22),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(22),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(22),
        process_id: ProcessId(18),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(22),
        process_id: ProcessId(19),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(22),
        process_id: ProcessId(20),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(22),
        process_id: ProcessId(21),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(22),
        process_id: ProcessId(22),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(22),
        process_id: ProcessId(23),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(22),
        process_id: ProcessId(24),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(22),
        process_id: ProcessId(30),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(22),
        process_id: ProcessId(31),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(22),
        process_id: ProcessId(32),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(23),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(23),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(23),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(23),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(23),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(23),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(23),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(24),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(24),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(24),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(24),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(24),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(24),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(24),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(25),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(25),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(25),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(25),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(25),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(25),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(25),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(26),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(26),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(26),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(26),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(26),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(26),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(26),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(27),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(27),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(27),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(27),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(27),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(27),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(27),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(30),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(30),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(30),
        process_id: ProcessId(9),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(30),
        process_id: ProcessId(10),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(30),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(30),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(30),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(30),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(30),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(31),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(31),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(31),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(31),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(31),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(31),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(31),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(32),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(32),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(32),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(32),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(32),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(32),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(32),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(33),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(33),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(33),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(33),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(33),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(33),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(33),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(34),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(34),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(34),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(34),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(34),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(34),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(34),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(35),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(35),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(35),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(35),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(35),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(35),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(35),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(36),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(36),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(36),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(36),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(36),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(36),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(36),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(40),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(40),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(40),
        process_id: ProcessId(11),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(40),
        process_id: ProcessId(12),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(40),
        process_id: ProcessId(13),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(40),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(40),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(40),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(40),
        process_id: ProcessId(18),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(40),
        process_id: ProcessId(19),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(40),
        process_id: ProcessId(20),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(40),
        process_id: ProcessId(21),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(40),
        process_id: ProcessId(22),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(40),
        process_id: ProcessId(23),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(40),
        process_id: ProcessId(24),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(40),
        process_id: ProcessId(30),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(40),
        process_id: ProcessId(31),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(40),
        process_id: ProcessId(32),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(40),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(40),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(41),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(41),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(41),
        process_id: ProcessId(11),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(41),
        process_id: ProcessId(12),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(41),
        process_id: ProcessId(13),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(41),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(41),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(41),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(41),
        process_id: ProcessId(18),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(41),
        process_id: ProcessId(19),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(41),
        process_id: ProcessId(20),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(41),
        process_id: ProcessId(21),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(41),
        process_id: ProcessId(22),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(41),
        process_id: ProcessId(23),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(41),
        process_id: ProcessId(24),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(41),
        process_id: ProcessId(30),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(41),
        process_id: ProcessId(31),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(41),
        process_id: ProcessId(32),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(41),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(41),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(42),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(42),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(42),
        process_id: ProcessId(11),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(42),
        process_id: ProcessId(12),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(42),
        process_id: ProcessId(13),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(42),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(42),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(42),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(42),
        process_id: ProcessId(18),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(42),
        process_id: ProcessId(19),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(42),
        process_id: ProcessId(20),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(42),
        process_id: ProcessId(21),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(42),
        process_id: ProcessId(22),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(42),
        process_id: ProcessId(23),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(42),
        process_id: ProcessId(24),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(42),
        process_id: ProcessId(30),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(42),
        process_id: ProcessId(31),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(42),
        process_id: ProcessId(32),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(42),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(42),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(43),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(43),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(43),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(43),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(43),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(43),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(43),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(44),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(44),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(44),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(44),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(44),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(44),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(44),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(45),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(45),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(45),
        process_id: ProcessId(11),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(45),
        process_id: ProcessId(12),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(45),
        process_id: ProcessId(13),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(45),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(45),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(45),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(45),
        process_id: ProcessId(18),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(45),
        process_id: ProcessId(19),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(45),
        process_id: ProcessId(20),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(45),
        process_id: ProcessId(21),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(45),
        process_id: ProcessId(22),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(45),
        process_id: ProcessId(23),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(45),
        process_id: ProcessId(24),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(45),
        process_id: ProcessId(30),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(45),
        process_id: ProcessId(31),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(45),
        process_id: ProcessId(32),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(45),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(45),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(46),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(46),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(46),
        process_id: ProcessId(11),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(46),
        process_id: ProcessId(12),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(46),
        process_id: ProcessId(13),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(46),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(46),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(46),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(46),
        process_id: ProcessId(18),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(46),
        process_id: ProcessId(19),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(46),
        process_id: ProcessId(20),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(46),
        process_id: ProcessId(21),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(46),
        process_id: ProcessId(22),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(46),
        process_id: ProcessId(23),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(46),
        process_id: ProcessId(24),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(46),
        process_id: ProcessId(30),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(46),
        process_id: ProcessId(31),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(46),
        process_id: ProcessId(32),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(46),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(46),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(51),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(51),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(51),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(51),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(51),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(51),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(51),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(52),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(52),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(52),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(52),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(52),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(52),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(52),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(53),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(53),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(53),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(53),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(53),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(53),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(53),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(54),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(54),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(54),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(54),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(54),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(54),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(54),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(55),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(55),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(55),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(55),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(55),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(55),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(55),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(56),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(56),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(56),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(56),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(56),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(56),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(56),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(57),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(57),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(57),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(57),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(57),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(57),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(57),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(58),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(58),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(58),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(58),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(58),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(58),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(58),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(59),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(59),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(59),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(59),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(59),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(59),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(59),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(60),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(61),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(62),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(63),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(65),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(66),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(67),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(68),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(68),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(68),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(68),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(68),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(69),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(69),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(69),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(69),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(69),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(70),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(70),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(70),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(70),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(70),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(71),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(71),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(71),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(71),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(71),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(72),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(72),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(72),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(72),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(72),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(73),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(73),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(73),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(73),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(73),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(74),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(74),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(74),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(74),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(74),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(75),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(75),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(75),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(75),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(75),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(76),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(76),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(76),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(76),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(76),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(77),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(77),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(77),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(77),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(77),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(78),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(78),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(78),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(78),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(78),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(79),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(79),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(79),
        process_id: ProcessId(11),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(79),
        process_id: ProcessId(12),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(79),
        process_id: ProcessId(13),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(79),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(79),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(79),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(79),
        process_id: ProcessId(18),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(79),
        process_id: ProcessId(19),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(79),
        process_id: ProcessId(20),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(79),
        process_id: ProcessId(21),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(79),
        process_id: ProcessId(30),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(79),
        process_id: ProcessId(31),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(79),
        process_id: ProcessId(32),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(79),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(79),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(80),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(80),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(80),
        process_id: ProcessId(11),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(80),
        process_id: ProcessId(12),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(80),
        process_id: ProcessId(13),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(80),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(80),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(80),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(80),
        process_id: ProcessId(18),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(80),
        process_id: ProcessId(19),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(80),
        process_id: ProcessId(20),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(80),
        process_id: ProcessId(21),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(80),
        process_id: ProcessId(30),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(80),
        process_id: ProcessId(31),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(80),
        process_id: ProcessId(32),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(80),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(80),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(81),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(81),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(81),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(81),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(81),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(82),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(82),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(82),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(82),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(82),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(83),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(83),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(83),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(83),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(83),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(84),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(84),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(84),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(84),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(84),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(86),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(86),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(86),
        process_id: ProcessId(11),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(86),
        process_id: ProcessId(12),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(86),
        process_id: ProcessId(13),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(86),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(86),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(86),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(86),
        process_id: ProcessId(18),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(86),
        process_id: ProcessId(19),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(86),
        process_id: ProcessId(20),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(86),
        process_id: ProcessId(21),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(86),
        process_id: ProcessId(30),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(86),
        process_id: ProcessId(31),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(86),
        process_id: ProcessId(32),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(86),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(86),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(87),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(87),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(87),
        process_id: ProcessId(11),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(87),
        process_id: ProcessId(12),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(87),
        process_id: ProcessId(13),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(87),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(87),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(87),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(87),
        process_id: ProcessId(18),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(87),
        process_id: ProcessId(19),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(87),
        process_id: ProcessId(20),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(87),
        process_id: ProcessId(21),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(87),
        process_id: ProcessId(30),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(87),
        process_id: ProcessId(31),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(87),
        process_id: ProcessId(32),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(87),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(87),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(88),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(88),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(88),
        process_id: ProcessId(11),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(88),
        process_id: ProcessId(12),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(88),
        process_id: ProcessId(13),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(88),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(88),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(88),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(88),
        process_id: ProcessId(18),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(88),
        process_id: ProcessId(19),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(88),
        process_id: ProcessId(20),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(88),
        process_id: ProcessId(21),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(88),
        process_id: ProcessId(30),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(88),
        process_id: ProcessId(31),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(88),
        process_id: ProcessId(32),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(88),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(88),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(90),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(90),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(90),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(90),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(91),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(91),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(91),
        process_id: ProcessId(9),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(91),
        process_id: ProcessId(10),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(91),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(91),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(92),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(92),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(92),
        process_id: ProcessId(9),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(92),
        process_id: ProcessId(10),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(92),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(92),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(93),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(93),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(93),
        process_id: ProcessId(9),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(93),
        process_id: ProcessId(10),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(93),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(93),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(98),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(98),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(98),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(98),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(99),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(100),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(100),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(100),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(100),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(100),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(100),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(100),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(106),
        process_id: ProcessId(9),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(107),
        process_id: ProcessId(10),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(110),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(110),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(110),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(110),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(110),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(110),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(110),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(111),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(111),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(111),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(111),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(111),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(111),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(111),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(112),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(112),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(112),
        process_id: ProcessId(9),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(112),
        process_id: ProcessId(10),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(112),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(112),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(112),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(112),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(112),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(115),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(115),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(115),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(115),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(115),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(115),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(115),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(116),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(116),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(116),
        process_id: ProcessId(9),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(116),
        process_id: ProcessId(10),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(116),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(116),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(117),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(117),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(117),
        process_id: ProcessId(9),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(117),
        process_id: ProcessId(10),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(117),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(117),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(118),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(118),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(118),
        process_id: ProcessId(9),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(118),
        process_id: ProcessId(10),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(118),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(118),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(118),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(118),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(118),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(119),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(119),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(119),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(119),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(119),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(119),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(119),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(121),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(121),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(121),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(121),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(121),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(121),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(121),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(122),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(122),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(122),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(122),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(122),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(122),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(122),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(130),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(131),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(132),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(133),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(134),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(135),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(136),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(137),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(138),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(139),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(140),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(141),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(142),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(143),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(144),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(145),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(146),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(168),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(168),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(168),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(168),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(168),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(168),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(168),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(169),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(169),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(169),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(169),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(169),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(169),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(169),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(170),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(170),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(170),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(170),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(170),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(170),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(170),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(171),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(171),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(171),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(171),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(171),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(171),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(171),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(172),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(172),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(172),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(172),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(172),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(172),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(172),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(173),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(173),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(173),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(173),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(173),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(173),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(173),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(174),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(174),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(174),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(174),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(174),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(174),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(174),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(175),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(175),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(175),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(175),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(175),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(175),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(175),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(176),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(176),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(176),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(176),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(176),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(176),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(176),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(177),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(177),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(177),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(177),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(177),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(177),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(177),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(178),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(178),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(178),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(178),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(178),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(178),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(178),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(181),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(181),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(181),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(181),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(181),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(181),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(181),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(182),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(182),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(182),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(182),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(182),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(182),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(182),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(183),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(183),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(183),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(183),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(183),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(183),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(183),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(184),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(184),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(184),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(184),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(184),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(184),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(184),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(185),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(185),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(185),
        process_id: ProcessId(11),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(185),
        process_id: ProcessId(12),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(185),
        process_id: ProcessId(13),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(185),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(185),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(185),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(185),
        process_id: ProcessId(18),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(185),
        process_id: ProcessId(19),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(185),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(185),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1000),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1000),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1000),
        process_id: ProcessId(11),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1000),
        process_id: ProcessId(12),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1000),
        process_id: ProcessId(13),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1000),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1000),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1000),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1000),
        process_id: ProcessId(18),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1000),
        process_id: ProcessId(19),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1000),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1000),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1001),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1001),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1001),
        process_id: ProcessId(11),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1001),
        process_id: ProcessId(12),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1001),
        process_id: ProcessId(13),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1001),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1001),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1001),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1001),
        process_id: ProcessId(18),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1001),
        process_id: ProcessId(19),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1001),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1001),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1002),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1002),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1002),
        process_id: ProcessId(11),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1002),
        process_id: ProcessId(12),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1002),
        process_id: ProcessId(13),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1002),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1002),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1002),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1002),
        process_id: ProcessId(18),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1002),
        process_id: ProcessId(19),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1002),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1002),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1005),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1005),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1005),
        process_id: ProcessId(11),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1005),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1005),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1005),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1005),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1005),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1006),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1006),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1006),
        process_id: ProcessId(11),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1006),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1006),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1006),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1006),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1006),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1008),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1008),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1008),
        process_id: ProcessId(11),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1008),
        process_id: ProcessId(12),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1008),
        process_id: ProcessId(13),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1008),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1008),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1008),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1008),
        process_id: ProcessId(18),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1008),
        process_id: ProcessId(19),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1008),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1008),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1009),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1009),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1009),
        process_id: ProcessId(11),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1009),
        process_id: ProcessId(12),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1009),
        process_id: ProcessId(13),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1009),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1009),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1009),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1009),
        process_id: ProcessId(18),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1009),
        process_id: ProcessId(19),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1009),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1009),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1010),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1010),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1010),
        process_id: ProcessId(11),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1010),
        process_id: ProcessId(12),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1010),
        process_id: ProcessId(13),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1010),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1010),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1010),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1010),
        process_id: ProcessId(18),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1010),
        process_id: ProcessId(19),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1010),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1010),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1011),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1011),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1011),
        process_id: ProcessId(11),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1011),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1011),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1012),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1012),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1012),
        process_id: ProcessId(11),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1012),
        process_id: ProcessId(12),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1012),
        process_id: ProcessId(13),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1012),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1012),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1012),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1012),
        process_id: ProcessId(18),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1012),
        process_id: ProcessId(19),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1012),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1012),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1013),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1013),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1013),
        process_id: ProcessId(11),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1013),
        process_id: ProcessId(12),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1013),
        process_id: ProcessId(13),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1013),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1013),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1013),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1013),
        process_id: ProcessId(18),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1013),
        process_id: ProcessId(19),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1013),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1013),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1014),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1014),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1014),
        process_id: ProcessId(11),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1014),
        process_id: ProcessId(12),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1014),
        process_id: ProcessId(13),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1014),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1014),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1014),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1014),
        process_id: ProcessId(18),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1014),
        process_id: ProcessId(19),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1014),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1014),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1015),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1015),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1015),
        process_id: ProcessId(11),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1015),
        process_id: ProcessId(12),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1015),
        process_id: ProcessId(13),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1015),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1015),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1015),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1015),
        process_id: ProcessId(18),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1015),
        process_id: ProcessId(19),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1015),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1015),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1016),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1016),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1016),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1016),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1016),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1016),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1016),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1017),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1017),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1017),
        process_id: ProcessId(11),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1017),
        process_id: ProcessId(12),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1017),
        process_id: ProcessId(13),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1017),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1017),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1017),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1017),
        process_id: ProcessId(18),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1017),
        process_id: ProcessId(19),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1017),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1017),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1018),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1018),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1018),
        process_id: ProcessId(11),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1018),
        process_id: ProcessId(12),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1018),
        process_id: ProcessId(13),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1018),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1018),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1018),
        process_id: ProcessId(17),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1018),
        process_id: ProcessId(18),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1018),
        process_id: ProcessId(19),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1018),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(1018),
        process_id: ProcessId(91),
    },
];

static BY_POLPROC_ID: phf::Map<u32, PollutantProcessAssociation> = phf::phf_map! {
    101u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1), process_id: ProcessId(1) },
    102u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1), process_id: ProcessId(2) },
    109u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1), process_id: ProcessId(9) },
    110u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1), process_id: ProcessId(10) },
    111u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1), process_id: ProcessId(11) },
    112u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1), process_id: ProcessId(12) },
    113u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1), process_id: ProcessId(13) },
    115u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1), process_id: ProcessId(15) },
    116u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1), process_id: ProcessId(16) },
    117u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1), process_id: ProcessId(17) },
    118u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1), process_id: ProcessId(18) },
    119u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1), process_id: ProcessId(19) },
    120u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1), process_id: ProcessId(20) },
    121u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1), process_id: ProcessId(21) },
    130u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1), process_id: ProcessId(30) },
    131u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1), process_id: ProcessId(31) },
    132u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1), process_id: ProcessId(32) },
    190u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1), process_id: ProcessId(90) },
    191u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1), process_id: ProcessId(91) },
    201u32 => PollutantProcessAssociation { pollutant_id: PollutantId(2), process_id: ProcessId(1) },
    202u32 => PollutantProcessAssociation { pollutant_id: PollutantId(2), process_id: ProcessId(2) },
    209u32 => PollutantProcessAssociation { pollutant_id: PollutantId(2), process_id: ProcessId(9) },
    210u32 => PollutantProcessAssociation { pollutant_id: PollutantId(2), process_id: ProcessId(10) },
    215u32 => PollutantProcessAssociation { pollutant_id: PollutantId(2), process_id: ProcessId(15) },
    216u32 => PollutantProcessAssociation { pollutant_id: PollutantId(2), process_id: ProcessId(16) },
    217u32 => PollutantProcessAssociation { pollutant_id: PollutantId(2), process_id: ProcessId(17) },
    290u32 => PollutantProcessAssociation { pollutant_id: PollutantId(2), process_id: ProcessId(90) },
    291u32 => PollutantProcessAssociation { pollutant_id: PollutantId(2), process_id: ProcessId(91) },
    301u32 => PollutantProcessAssociation { pollutant_id: PollutantId(3), process_id: ProcessId(1) },
    302u32 => PollutantProcessAssociation { pollutant_id: PollutantId(3), process_id: ProcessId(2) },
    309u32 => PollutantProcessAssociation { pollutant_id: PollutantId(3), process_id: ProcessId(9) },
    310u32 => PollutantProcessAssociation { pollutant_id: PollutantId(3), process_id: ProcessId(10) },
    315u32 => PollutantProcessAssociation { pollutant_id: PollutantId(3), process_id: ProcessId(15) },
    316u32 => PollutantProcessAssociation { pollutant_id: PollutantId(3), process_id: ProcessId(16) },
    317u32 => PollutantProcessAssociation { pollutant_id: PollutantId(3), process_id: ProcessId(17) },
    390u32 => PollutantProcessAssociation { pollutant_id: PollutantId(3), process_id: ProcessId(90) },
    391u32 => PollutantProcessAssociation { pollutant_id: PollutantId(3), process_id: ProcessId(91) },
    501u32 => PollutantProcessAssociation { pollutant_id: PollutantId(5), process_id: ProcessId(1) },
    502u32 => PollutantProcessAssociation { pollutant_id: PollutantId(5), process_id: ProcessId(2) },
    515u32 => PollutantProcessAssociation { pollutant_id: PollutantId(5), process_id: ProcessId(15) },
    516u32 => PollutantProcessAssociation { pollutant_id: PollutantId(5), process_id: ProcessId(16) },
    517u32 => PollutantProcessAssociation { pollutant_id: PollutantId(5), process_id: ProcessId(17) },
    518u32 => PollutantProcessAssociation { pollutant_id: PollutantId(5), process_id: ProcessId(18) },
    519u32 => PollutantProcessAssociation { pollutant_id: PollutantId(5), process_id: ProcessId(19) },
    520u32 => PollutantProcessAssociation { pollutant_id: PollutantId(5), process_id: ProcessId(20) },
    521u32 => PollutantProcessAssociation { pollutant_id: PollutantId(5), process_id: ProcessId(21) },
    530u32 => PollutantProcessAssociation { pollutant_id: PollutantId(5), process_id: ProcessId(30) },
    531u32 => PollutantProcessAssociation { pollutant_id: PollutantId(5), process_id: ProcessId(31) },
    532u32 => PollutantProcessAssociation { pollutant_id: PollutantId(5), process_id: ProcessId(32) },
    590u32 => PollutantProcessAssociation { pollutant_id: PollutantId(5), process_id: ProcessId(90) },
    591u32 => PollutantProcessAssociation { pollutant_id: PollutantId(5), process_id: ProcessId(91) },
    601u32 => PollutantProcessAssociation { pollutant_id: PollutantId(6), process_id: ProcessId(1) },
    602u32 => PollutantProcessAssociation { pollutant_id: PollutantId(6), process_id: ProcessId(2) },
    609u32 => PollutantProcessAssociation { pollutant_id: PollutantId(6), process_id: ProcessId(9) },
    610u32 => PollutantProcessAssociation { pollutant_id: PollutantId(6), process_id: ProcessId(10) },
    615u32 => PollutantProcessAssociation { pollutant_id: PollutantId(6), process_id: ProcessId(15) },
    616u32 => PollutantProcessAssociation { pollutant_id: PollutantId(6), process_id: ProcessId(16) },
    617u32 => PollutantProcessAssociation { pollutant_id: PollutantId(6), process_id: ProcessId(17) },
    690u32 => PollutantProcessAssociation { pollutant_id: PollutantId(6), process_id: ProcessId(90) },
    691u32 => PollutantProcessAssociation { pollutant_id: PollutantId(6), process_id: ProcessId(91) },
    2001u32 => PollutantProcessAssociation { pollutant_id: PollutantId(20), process_id: ProcessId(1) },
    2002u32 => PollutantProcessAssociation { pollutant_id: PollutantId(20), process_id: ProcessId(2) },
    2011u32 => PollutantProcessAssociation { pollutant_id: PollutantId(20), process_id: ProcessId(11) },
    2012u32 => PollutantProcessAssociation { pollutant_id: PollutantId(20), process_id: ProcessId(12) },
    2013u32 => PollutantProcessAssociation { pollutant_id: PollutantId(20), process_id: ProcessId(13) },
    2015u32 => PollutantProcessAssociation { pollutant_id: PollutantId(20), process_id: ProcessId(15) },
    2016u32 => PollutantProcessAssociation { pollutant_id: PollutantId(20), process_id: ProcessId(16) },
    2017u32 => PollutantProcessAssociation { pollutant_id: PollutantId(20), process_id: ProcessId(17) },
    2018u32 => PollutantProcessAssociation { pollutant_id: PollutantId(20), process_id: ProcessId(18) },
    2019u32 => PollutantProcessAssociation { pollutant_id: PollutantId(20), process_id: ProcessId(19) },
    2020u32 => PollutantProcessAssociation { pollutant_id: PollutantId(20), process_id: ProcessId(20) },
    2021u32 => PollutantProcessAssociation { pollutant_id: PollutantId(20), process_id: ProcessId(21) },
    2022u32 => PollutantProcessAssociation { pollutant_id: PollutantId(20), process_id: ProcessId(22) },
    2023u32 => PollutantProcessAssociation { pollutant_id: PollutantId(20), process_id: ProcessId(23) },
    2024u32 => PollutantProcessAssociation { pollutant_id: PollutantId(20), process_id: ProcessId(24) },
    2030u32 => PollutantProcessAssociation { pollutant_id: PollutantId(20), process_id: ProcessId(30) },
    2031u32 => PollutantProcessAssociation { pollutant_id: PollutantId(20), process_id: ProcessId(31) },
    2032u32 => PollutantProcessAssociation { pollutant_id: PollutantId(20), process_id: ProcessId(32) },
    2090u32 => PollutantProcessAssociation { pollutant_id: PollutantId(20), process_id: ProcessId(90) },
    2091u32 => PollutantProcessAssociation { pollutant_id: PollutantId(20), process_id: ProcessId(91) },
    2101u32 => PollutantProcessAssociation { pollutant_id: PollutantId(21), process_id: ProcessId(1) },
    2102u32 => PollutantProcessAssociation { pollutant_id: PollutantId(21), process_id: ProcessId(2) },
    2111u32 => PollutantProcessAssociation { pollutant_id: PollutantId(21), process_id: ProcessId(11) },
    2112u32 => PollutantProcessAssociation { pollutant_id: PollutantId(21), process_id: ProcessId(12) },
    2113u32 => PollutantProcessAssociation { pollutant_id: PollutantId(21), process_id: ProcessId(13) },
    2115u32 => PollutantProcessAssociation { pollutant_id: PollutantId(21), process_id: ProcessId(15) },
    2116u32 => PollutantProcessAssociation { pollutant_id: PollutantId(21), process_id: ProcessId(16) },
    2117u32 => PollutantProcessAssociation { pollutant_id: PollutantId(21), process_id: ProcessId(17) },
    2118u32 => PollutantProcessAssociation { pollutant_id: PollutantId(21), process_id: ProcessId(18) },
    2119u32 => PollutantProcessAssociation { pollutant_id: PollutantId(21), process_id: ProcessId(19) },
    2120u32 => PollutantProcessAssociation { pollutant_id: PollutantId(21), process_id: ProcessId(20) },
    2121u32 => PollutantProcessAssociation { pollutant_id: PollutantId(21), process_id: ProcessId(21) },
    2122u32 => PollutantProcessAssociation { pollutant_id: PollutantId(21), process_id: ProcessId(22) },
    2123u32 => PollutantProcessAssociation { pollutant_id: PollutantId(21), process_id: ProcessId(23) },
    2124u32 => PollutantProcessAssociation { pollutant_id: PollutantId(21), process_id: ProcessId(24) },
    2130u32 => PollutantProcessAssociation { pollutant_id: PollutantId(21), process_id: ProcessId(30) },
    2131u32 => PollutantProcessAssociation { pollutant_id: PollutantId(21), process_id: ProcessId(31) },
    2132u32 => PollutantProcessAssociation { pollutant_id: PollutantId(21), process_id: ProcessId(32) },
    2201u32 => PollutantProcessAssociation { pollutant_id: PollutantId(22), process_id: ProcessId(1) },
    2202u32 => PollutantProcessAssociation { pollutant_id: PollutantId(22), process_id: ProcessId(2) },
    2211u32 => PollutantProcessAssociation { pollutant_id: PollutantId(22), process_id: ProcessId(11) },
    2212u32 => PollutantProcessAssociation { pollutant_id: PollutantId(22), process_id: ProcessId(12) },
    2213u32 => PollutantProcessAssociation { pollutant_id: PollutantId(22), process_id: ProcessId(13) },
    2215u32 => PollutantProcessAssociation { pollutant_id: PollutantId(22), process_id: ProcessId(15) },
    2216u32 => PollutantProcessAssociation { pollutant_id: PollutantId(22), process_id: ProcessId(16) },
    2217u32 => PollutantProcessAssociation { pollutant_id: PollutantId(22), process_id: ProcessId(17) },
    2218u32 => PollutantProcessAssociation { pollutant_id: PollutantId(22), process_id: ProcessId(18) },
    2219u32 => PollutantProcessAssociation { pollutant_id: PollutantId(22), process_id: ProcessId(19) },
    2220u32 => PollutantProcessAssociation { pollutant_id: PollutantId(22), process_id: ProcessId(20) },
    2221u32 => PollutantProcessAssociation { pollutant_id: PollutantId(22), process_id: ProcessId(21) },
    2222u32 => PollutantProcessAssociation { pollutant_id: PollutantId(22), process_id: ProcessId(22) },
    2223u32 => PollutantProcessAssociation { pollutant_id: PollutantId(22), process_id: ProcessId(23) },
    2224u32 => PollutantProcessAssociation { pollutant_id: PollutantId(22), process_id: ProcessId(24) },
    2230u32 => PollutantProcessAssociation { pollutant_id: PollutantId(22), process_id: ProcessId(30) },
    2231u32 => PollutantProcessAssociation { pollutant_id: PollutantId(22), process_id: ProcessId(31) },
    2232u32 => PollutantProcessAssociation { pollutant_id: PollutantId(22), process_id: ProcessId(32) },
    2301u32 => PollutantProcessAssociation { pollutant_id: PollutantId(23), process_id: ProcessId(1) },
    2302u32 => PollutantProcessAssociation { pollutant_id: PollutantId(23), process_id: ProcessId(2) },
    2315u32 => PollutantProcessAssociation { pollutant_id: PollutantId(23), process_id: ProcessId(15) },
    2316u32 => PollutantProcessAssociation { pollutant_id: PollutantId(23), process_id: ProcessId(16) },
    2317u32 => PollutantProcessAssociation { pollutant_id: PollutantId(23), process_id: ProcessId(17) },
    2390u32 => PollutantProcessAssociation { pollutant_id: PollutantId(23), process_id: ProcessId(90) },
    2391u32 => PollutantProcessAssociation { pollutant_id: PollutantId(23), process_id: ProcessId(91) },
    2401u32 => PollutantProcessAssociation { pollutant_id: PollutantId(24), process_id: ProcessId(1) },
    2402u32 => PollutantProcessAssociation { pollutant_id: PollutantId(24), process_id: ProcessId(2) },
    2415u32 => PollutantProcessAssociation { pollutant_id: PollutantId(24), process_id: ProcessId(15) },
    2416u32 => PollutantProcessAssociation { pollutant_id: PollutantId(24), process_id: ProcessId(16) },
    2417u32 => PollutantProcessAssociation { pollutant_id: PollutantId(24), process_id: ProcessId(17) },
    2490u32 => PollutantProcessAssociation { pollutant_id: PollutantId(24), process_id: ProcessId(90) },
    2491u32 => PollutantProcessAssociation { pollutant_id: PollutantId(24), process_id: ProcessId(91) },
    2501u32 => PollutantProcessAssociation { pollutant_id: PollutantId(25), process_id: ProcessId(1) },
    2502u32 => PollutantProcessAssociation { pollutant_id: PollutantId(25), process_id: ProcessId(2) },
    2515u32 => PollutantProcessAssociation { pollutant_id: PollutantId(25), process_id: ProcessId(15) },
    2516u32 => PollutantProcessAssociation { pollutant_id: PollutantId(25), process_id: ProcessId(16) },
    2517u32 => PollutantProcessAssociation { pollutant_id: PollutantId(25), process_id: ProcessId(17) },
    2590u32 => PollutantProcessAssociation { pollutant_id: PollutantId(25), process_id: ProcessId(90) },
    2591u32 => PollutantProcessAssociation { pollutant_id: PollutantId(25), process_id: ProcessId(91) },
    2601u32 => PollutantProcessAssociation { pollutant_id: PollutantId(26), process_id: ProcessId(1) },
    2602u32 => PollutantProcessAssociation { pollutant_id: PollutantId(26), process_id: ProcessId(2) },
    2615u32 => PollutantProcessAssociation { pollutant_id: PollutantId(26), process_id: ProcessId(15) },
    2616u32 => PollutantProcessAssociation { pollutant_id: PollutantId(26), process_id: ProcessId(16) },
    2617u32 => PollutantProcessAssociation { pollutant_id: PollutantId(26), process_id: ProcessId(17) },
    2690u32 => PollutantProcessAssociation { pollutant_id: PollutantId(26), process_id: ProcessId(90) },
    2691u32 => PollutantProcessAssociation { pollutant_id: PollutantId(26), process_id: ProcessId(91) },
    2701u32 => PollutantProcessAssociation { pollutant_id: PollutantId(27), process_id: ProcessId(1) },
    2702u32 => PollutantProcessAssociation { pollutant_id: PollutantId(27), process_id: ProcessId(2) },
    2715u32 => PollutantProcessAssociation { pollutant_id: PollutantId(27), process_id: ProcessId(15) },
    2716u32 => PollutantProcessAssociation { pollutant_id: PollutantId(27), process_id: ProcessId(16) },
    2717u32 => PollutantProcessAssociation { pollutant_id: PollutantId(27), process_id: ProcessId(17) },
    2790u32 => PollutantProcessAssociation { pollutant_id: PollutantId(27), process_id: ProcessId(90) },
    2791u32 => PollutantProcessAssociation { pollutant_id: PollutantId(27), process_id: ProcessId(91) },
    3001u32 => PollutantProcessAssociation { pollutant_id: PollutantId(30), process_id: ProcessId(1) },
    3002u32 => PollutantProcessAssociation { pollutant_id: PollutantId(30), process_id: ProcessId(2) },
    3009u32 => PollutantProcessAssociation { pollutant_id: PollutantId(30), process_id: ProcessId(9) },
    3010u32 => PollutantProcessAssociation { pollutant_id: PollutantId(30), process_id: ProcessId(10) },
    3015u32 => PollutantProcessAssociation { pollutant_id: PollutantId(30), process_id: ProcessId(15) },
    3016u32 => PollutantProcessAssociation { pollutant_id: PollutantId(30), process_id: ProcessId(16) },
    3017u32 => PollutantProcessAssociation { pollutant_id: PollutantId(30), process_id: ProcessId(17) },
    3090u32 => PollutantProcessAssociation { pollutant_id: PollutantId(30), process_id: ProcessId(90) },
    3091u32 => PollutantProcessAssociation { pollutant_id: PollutantId(30), process_id: ProcessId(91) },
    3101u32 => PollutantProcessAssociation { pollutant_id: PollutantId(31), process_id: ProcessId(1) },
    3102u32 => PollutantProcessAssociation { pollutant_id: PollutantId(31), process_id: ProcessId(2) },
    3115u32 => PollutantProcessAssociation { pollutant_id: PollutantId(31), process_id: ProcessId(15) },
    3116u32 => PollutantProcessAssociation { pollutant_id: PollutantId(31), process_id: ProcessId(16) },
    3117u32 => PollutantProcessAssociation { pollutant_id: PollutantId(31), process_id: ProcessId(17) },
    3190u32 => PollutantProcessAssociation { pollutant_id: PollutantId(31), process_id: ProcessId(90) },
    3191u32 => PollutantProcessAssociation { pollutant_id: PollutantId(31), process_id: ProcessId(91) },
    3201u32 => PollutantProcessAssociation { pollutant_id: PollutantId(32), process_id: ProcessId(1) },
    3202u32 => PollutantProcessAssociation { pollutant_id: PollutantId(32), process_id: ProcessId(2) },
    3215u32 => PollutantProcessAssociation { pollutant_id: PollutantId(32), process_id: ProcessId(15) },
    3216u32 => PollutantProcessAssociation { pollutant_id: PollutantId(32), process_id: ProcessId(16) },
    3217u32 => PollutantProcessAssociation { pollutant_id: PollutantId(32), process_id: ProcessId(17) },
    3290u32 => PollutantProcessAssociation { pollutant_id: PollutantId(32), process_id: ProcessId(90) },
    3291u32 => PollutantProcessAssociation { pollutant_id: PollutantId(32), process_id: ProcessId(91) },
    3301u32 => PollutantProcessAssociation { pollutant_id: PollutantId(33), process_id: ProcessId(1) },
    3302u32 => PollutantProcessAssociation { pollutant_id: PollutantId(33), process_id: ProcessId(2) },
    3315u32 => PollutantProcessAssociation { pollutant_id: PollutantId(33), process_id: ProcessId(15) },
    3316u32 => PollutantProcessAssociation { pollutant_id: PollutantId(33), process_id: ProcessId(16) },
    3317u32 => PollutantProcessAssociation { pollutant_id: PollutantId(33), process_id: ProcessId(17) },
    3390u32 => PollutantProcessAssociation { pollutant_id: PollutantId(33), process_id: ProcessId(90) },
    3391u32 => PollutantProcessAssociation { pollutant_id: PollutantId(33), process_id: ProcessId(91) },
    3401u32 => PollutantProcessAssociation { pollutant_id: PollutantId(34), process_id: ProcessId(1) },
    3402u32 => PollutantProcessAssociation { pollutant_id: PollutantId(34), process_id: ProcessId(2) },
    3415u32 => PollutantProcessAssociation { pollutant_id: PollutantId(34), process_id: ProcessId(15) },
    3416u32 => PollutantProcessAssociation { pollutant_id: PollutantId(34), process_id: ProcessId(16) },
    3417u32 => PollutantProcessAssociation { pollutant_id: PollutantId(34), process_id: ProcessId(17) },
    3490u32 => PollutantProcessAssociation { pollutant_id: PollutantId(34), process_id: ProcessId(90) },
    3491u32 => PollutantProcessAssociation { pollutant_id: PollutantId(34), process_id: ProcessId(91) },
    3501u32 => PollutantProcessAssociation { pollutant_id: PollutantId(35), process_id: ProcessId(1) },
    3502u32 => PollutantProcessAssociation { pollutant_id: PollutantId(35), process_id: ProcessId(2) },
    3515u32 => PollutantProcessAssociation { pollutant_id: PollutantId(35), process_id: ProcessId(15) },
    3516u32 => PollutantProcessAssociation { pollutant_id: PollutantId(35), process_id: ProcessId(16) },
    3517u32 => PollutantProcessAssociation { pollutant_id: PollutantId(35), process_id: ProcessId(17) },
    3590u32 => PollutantProcessAssociation { pollutant_id: PollutantId(35), process_id: ProcessId(90) },
    3591u32 => PollutantProcessAssociation { pollutant_id: PollutantId(35), process_id: ProcessId(91) },
    3601u32 => PollutantProcessAssociation { pollutant_id: PollutantId(36), process_id: ProcessId(1) },
    3602u32 => PollutantProcessAssociation { pollutant_id: PollutantId(36), process_id: ProcessId(2) },
    3615u32 => PollutantProcessAssociation { pollutant_id: PollutantId(36), process_id: ProcessId(15) },
    3616u32 => PollutantProcessAssociation { pollutant_id: PollutantId(36), process_id: ProcessId(16) },
    3617u32 => PollutantProcessAssociation { pollutant_id: PollutantId(36), process_id: ProcessId(17) },
    3690u32 => PollutantProcessAssociation { pollutant_id: PollutantId(36), process_id: ProcessId(90) },
    3691u32 => PollutantProcessAssociation { pollutant_id: PollutantId(36), process_id: ProcessId(91) },
    4001u32 => PollutantProcessAssociation { pollutant_id: PollutantId(40), process_id: ProcessId(1) },
    4002u32 => PollutantProcessAssociation { pollutant_id: PollutantId(40), process_id: ProcessId(2) },
    4011u32 => PollutantProcessAssociation { pollutant_id: PollutantId(40), process_id: ProcessId(11) },
    4012u32 => PollutantProcessAssociation { pollutant_id: PollutantId(40), process_id: ProcessId(12) },
    4013u32 => PollutantProcessAssociation { pollutant_id: PollutantId(40), process_id: ProcessId(13) },
    4015u32 => PollutantProcessAssociation { pollutant_id: PollutantId(40), process_id: ProcessId(15) },
    4016u32 => PollutantProcessAssociation { pollutant_id: PollutantId(40), process_id: ProcessId(16) },
    4017u32 => PollutantProcessAssociation { pollutant_id: PollutantId(40), process_id: ProcessId(17) },
    4018u32 => PollutantProcessAssociation { pollutant_id: PollutantId(40), process_id: ProcessId(18) },
    4019u32 => PollutantProcessAssociation { pollutant_id: PollutantId(40), process_id: ProcessId(19) },
    4020u32 => PollutantProcessAssociation { pollutant_id: PollutantId(40), process_id: ProcessId(20) },
    4021u32 => PollutantProcessAssociation { pollutant_id: PollutantId(40), process_id: ProcessId(21) },
    4022u32 => PollutantProcessAssociation { pollutant_id: PollutantId(40), process_id: ProcessId(22) },
    4023u32 => PollutantProcessAssociation { pollutant_id: PollutantId(40), process_id: ProcessId(23) },
    4024u32 => PollutantProcessAssociation { pollutant_id: PollutantId(40), process_id: ProcessId(24) },
    4030u32 => PollutantProcessAssociation { pollutant_id: PollutantId(40), process_id: ProcessId(30) },
    4031u32 => PollutantProcessAssociation { pollutant_id: PollutantId(40), process_id: ProcessId(31) },
    4032u32 => PollutantProcessAssociation { pollutant_id: PollutantId(40), process_id: ProcessId(32) },
    4090u32 => PollutantProcessAssociation { pollutant_id: PollutantId(40), process_id: ProcessId(90) },
    4091u32 => PollutantProcessAssociation { pollutant_id: PollutantId(40), process_id: ProcessId(91) },
    4101u32 => PollutantProcessAssociation { pollutant_id: PollutantId(41), process_id: ProcessId(1) },
    4102u32 => PollutantProcessAssociation { pollutant_id: PollutantId(41), process_id: ProcessId(2) },
    4111u32 => PollutantProcessAssociation { pollutant_id: PollutantId(41), process_id: ProcessId(11) },
    4112u32 => PollutantProcessAssociation { pollutant_id: PollutantId(41), process_id: ProcessId(12) },
    4113u32 => PollutantProcessAssociation { pollutant_id: PollutantId(41), process_id: ProcessId(13) },
    4115u32 => PollutantProcessAssociation { pollutant_id: PollutantId(41), process_id: ProcessId(15) },
    4116u32 => PollutantProcessAssociation { pollutant_id: PollutantId(41), process_id: ProcessId(16) },
    4117u32 => PollutantProcessAssociation { pollutant_id: PollutantId(41), process_id: ProcessId(17) },
    4118u32 => PollutantProcessAssociation { pollutant_id: PollutantId(41), process_id: ProcessId(18) },
    4119u32 => PollutantProcessAssociation { pollutant_id: PollutantId(41), process_id: ProcessId(19) },
    4120u32 => PollutantProcessAssociation { pollutant_id: PollutantId(41), process_id: ProcessId(20) },
    4121u32 => PollutantProcessAssociation { pollutant_id: PollutantId(41), process_id: ProcessId(21) },
    4122u32 => PollutantProcessAssociation { pollutant_id: PollutantId(41), process_id: ProcessId(22) },
    4123u32 => PollutantProcessAssociation { pollutant_id: PollutantId(41), process_id: ProcessId(23) },
    4124u32 => PollutantProcessAssociation { pollutant_id: PollutantId(41), process_id: ProcessId(24) },
    4130u32 => PollutantProcessAssociation { pollutant_id: PollutantId(41), process_id: ProcessId(30) },
    4131u32 => PollutantProcessAssociation { pollutant_id: PollutantId(41), process_id: ProcessId(31) },
    4132u32 => PollutantProcessAssociation { pollutant_id: PollutantId(41), process_id: ProcessId(32) },
    4190u32 => PollutantProcessAssociation { pollutant_id: PollutantId(41), process_id: ProcessId(90) },
    4191u32 => PollutantProcessAssociation { pollutant_id: PollutantId(41), process_id: ProcessId(91) },
    4201u32 => PollutantProcessAssociation { pollutant_id: PollutantId(42), process_id: ProcessId(1) },
    4202u32 => PollutantProcessAssociation { pollutant_id: PollutantId(42), process_id: ProcessId(2) },
    4211u32 => PollutantProcessAssociation { pollutant_id: PollutantId(42), process_id: ProcessId(11) },
    4212u32 => PollutantProcessAssociation { pollutant_id: PollutantId(42), process_id: ProcessId(12) },
    4213u32 => PollutantProcessAssociation { pollutant_id: PollutantId(42), process_id: ProcessId(13) },
    4215u32 => PollutantProcessAssociation { pollutant_id: PollutantId(42), process_id: ProcessId(15) },
    4216u32 => PollutantProcessAssociation { pollutant_id: PollutantId(42), process_id: ProcessId(16) },
    4217u32 => PollutantProcessAssociation { pollutant_id: PollutantId(42), process_id: ProcessId(17) },
    4218u32 => PollutantProcessAssociation { pollutant_id: PollutantId(42), process_id: ProcessId(18) },
    4219u32 => PollutantProcessAssociation { pollutant_id: PollutantId(42), process_id: ProcessId(19) },
    4220u32 => PollutantProcessAssociation { pollutant_id: PollutantId(42), process_id: ProcessId(20) },
    4221u32 => PollutantProcessAssociation { pollutant_id: PollutantId(42), process_id: ProcessId(21) },
    4222u32 => PollutantProcessAssociation { pollutant_id: PollutantId(42), process_id: ProcessId(22) },
    4223u32 => PollutantProcessAssociation { pollutant_id: PollutantId(42), process_id: ProcessId(23) },
    4224u32 => PollutantProcessAssociation { pollutant_id: PollutantId(42), process_id: ProcessId(24) },
    4230u32 => PollutantProcessAssociation { pollutant_id: PollutantId(42), process_id: ProcessId(30) },
    4231u32 => PollutantProcessAssociation { pollutant_id: PollutantId(42), process_id: ProcessId(31) },
    4232u32 => PollutantProcessAssociation { pollutant_id: PollutantId(42), process_id: ProcessId(32) },
    4290u32 => PollutantProcessAssociation { pollutant_id: PollutantId(42), process_id: ProcessId(90) },
    4291u32 => PollutantProcessAssociation { pollutant_id: PollutantId(42), process_id: ProcessId(91) },
    4301u32 => PollutantProcessAssociation { pollutant_id: PollutantId(43), process_id: ProcessId(1) },
    4302u32 => PollutantProcessAssociation { pollutant_id: PollutantId(43), process_id: ProcessId(2) },
    4315u32 => PollutantProcessAssociation { pollutant_id: PollutantId(43), process_id: ProcessId(15) },
    4316u32 => PollutantProcessAssociation { pollutant_id: PollutantId(43), process_id: ProcessId(16) },
    4317u32 => PollutantProcessAssociation { pollutant_id: PollutantId(43), process_id: ProcessId(17) },
    4390u32 => PollutantProcessAssociation { pollutant_id: PollutantId(43), process_id: ProcessId(90) },
    4391u32 => PollutantProcessAssociation { pollutant_id: PollutantId(43), process_id: ProcessId(91) },
    4401u32 => PollutantProcessAssociation { pollutant_id: PollutantId(44), process_id: ProcessId(1) },
    4402u32 => PollutantProcessAssociation { pollutant_id: PollutantId(44), process_id: ProcessId(2) },
    4415u32 => PollutantProcessAssociation { pollutant_id: PollutantId(44), process_id: ProcessId(15) },
    4416u32 => PollutantProcessAssociation { pollutant_id: PollutantId(44), process_id: ProcessId(16) },
    4417u32 => PollutantProcessAssociation { pollutant_id: PollutantId(44), process_id: ProcessId(17) },
    4490u32 => PollutantProcessAssociation { pollutant_id: PollutantId(44), process_id: ProcessId(90) },
    4491u32 => PollutantProcessAssociation { pollutant_id: PollutantId(44), process_id: ProcessId(91) },
    4501u32 => PollutantProcessAssociation { pollutant_id: PollutantId(45), process_id: ProcessId(1) },
    4502u32 => PollutantProcessAssociation { pollutant_id: PollutantId(45), process_id: ProcessId(2) },
    4511u32 => PollutantProcessAssociation { pollutant_id: PollutantId(45), process_id: ProcessId(11) },
    4512u32 => PollutantProcessAssociation { pollutant_id: PollutantId(45), process_id: ProcessId(12) },
    4513u32 => PollutantProcessAssociation { pollutant_id: PollutantId(45), process_id: ProcessId(13) },
    4515u32 => PollutantProcessAssociation { pollutant_id: PollutantId(45), process_id: ProcessId(15) },
    4516u32 => PollutantProcessAssociation { pollutant_id: PollutantId(45), process_id: ProcessId(16) },
    4517u32 => PollutantProcessAssociation { pollutant_id: PollutantId(45), process_id: ProcessId(17) },
    4518u32 => PollutantProcessAssociation { pollutant_id: PollutantId(45), process_id: ProcessId(18) },
    4519u32 => PollutantProcessAssociation { pollutant_id: PollutantId(45), process_id: ProcessId(19) },
    4520u32 => PollutantProcessAssociation { pollutant_id: PollutantId(45), process_id: ProcessId(20) },
    4521u32 => PollutantProcessAssociation { pollutant_id: PollutantId(45), process_id: ProcessId(21) },
    4522u32 => PollutantProcessAssociation { pollutant_id: PollutantId(45), process_id: ProcessId(22) },
    4523u32 => PollutantProcessAssociation { pollutant_id: PollutantId(45), process_id: ProcessId(23) },
    4524u32 => PollutantProcessAssociation { pollutant_id: PollutantId(45), process_id: ProcessId(24) },
    4530u32 => PollutantProcessAssociation { pollutant_id: PollutantId(45), process_id: ProcessId(30) },
    4531u32 => PollutantProcessAssociation { pollutant_id: PollutantId(45), process_id: ProcessId(31) },
    4532u32 => PollutantProcessAssociation { pollutant_id: PollutantId(45), process_id: ProcessId(32) },
    4590u32 => PollutantProcessAssociation { pollutant_id: PollutantId(45), process_id: ProcessId(90) },
    4591u32 => PollutantProcessAssociation { pollutant_id: PollutantId(45), process_id: ProcessId(91) },
    4601u32 => PollutantProcessAssociation { pollutant_id: PollutantId(46), process_id: ProcessId(1) },
    4602u32 => PollutantProcessAssociation { pollutant_id: PollutantId(46), process_id: ProcessId(2) },
    4611u32 => PollutantProcessAssociation { pollutant_id: PollutantId(46), process_id: ProcessId(11) },
    4612u32 => PollutantProcessAssociation { pollutant_id: PollutantId(46), process_id: ProcessId(12) },
    4613u32 => PollutantProcessAssociation { pollutant_id: PollutantId(46), process_id: ProcessId(13) },
    4615u32 => PollutantProcessAssociation { pollutant_id: PollutantId(46), process_id: ProcessId(15) },
    4616u32 => PollutantProcessAssociation { pollutant_id: PollutantId(46), process_id: ProcessId(16) },
    4617u32 => PollutantProcessAssociation { pollutant_id: PollutantId(46), process_id: ProcessId(17) },
    4618u32 => PollutantProcessAssociation { pollutant_id: PollutantId(46), process_id: ProcessId(18) },
    4619u32 => PollutantProcessAssociation { pollutant_id: PollutantId(46), process_id: ProcessId(19) },
    4620u32 => PollutantProcessAssociation { pollutant_id: PollutantId(46), process_id: ProcessId(20) },
    4621u32 => PollutantProcessAssociation { pollutant_id: PollutantId(46), process_id: ProcessId(21) },
    4622u32 => PollutantProcessAssociation { pollutant_id: PollutantId(46), process_id: ProcessId(22) },
    4623u32 => PollutantProcessAssociation { pollutant_id: PollutantId(46), process_id: ProcessId(23) },
    4624u32 => PollutantProcessAssociation { pollutant_id: PollutantId(46), process_id: ProcessId(24) },
    4630u32 => PollutantProcessAssociation { pollutant_id: PollutantId(46), process_id: ProcessId(30) },
    4631u32 => PollutantProcessAssociation { pollutant_id: PollutantId(46), process_id: ProcessId(31) },
    4632u32 => PollutantProcessAssociation { pollutant_id: PollutantId(46), process_id: ProcessId(32) },
    4690u32 => PollutantProcessAssociation { pollutant_id: PollutantId(46), process_id: ProcessId(90) },
    4691u32 => PollutantProcessAssociation { pollutant_id: PollutantId(46), process_id: ProcessId(91) },
    5101u32 => PollutantProcessAssociation { pollutant_id: PollutantId(51), process_id: ProcessId(1) },
    5102u32 => PollutantProcessAssociation { pollutant_id: PollutantId(51), process_id: ProcessId(2) },
    5115u32 => PollutantProcessAssociation { pollutant_id: PollutantId(51), process_id: ProcessId(15) },
    5116u32 => PollutantProcessAssociation { pollutant_id: PollutantId(51), process_id: ProcessId(16) },
    5117u32 => PollutantProcessAssociation { pollutant_id: PollutantId(51), process_id: ProcessId(17) },
    5190u32 => PollutantProcessAssociation { pollutant_id: PollutantId(51), process_id: ProcessId(90) },
    5191u32 => PollutantProcessAssociation { pollutant_id: PollutantId(51), process_id: ProcessId(91) },
    5201u32 => PollutantProcessAssociation { pollutant_id: PollutantId(52), process_id: ProcessId(1) },
    5202u32 => PollutantProcessAssociation { pollutant_id: PollutantId(52), process_id: ProcessId(2) },
    5215u32 => PollutantProcessAssociation { pollutant_id: PollutantId(52), process_id: ProcessId(15) },
    5216u32 => PollutantProcessAssociation { pollutant_id: PollutantId(52), process_id: ProcessId(16) },
    5217u32 => PollutantProcessAssociation { pollutant_id: PollutantId(52), process_id: ProcessId(17) },
    5290u32 => PollutantProcessAssociation { pollutant_id: PollutantId(52), process_id: ProcessId(90) },
    5291u32 => PollutantProcessAssociation { pollutant_id: PollutantId(52), process_id: ProcessId(91) },
    5301u32 => PollutantProcessAssociation { pollutant_id: PollutantId(53), process_id: ProcessId(1) },
    5302u32 => PollutantProcessAssociation { pollutant_id: PollutantId(53), process_id: ProcessId(2) },
    5315u32 => PollutantProcessAssociation { pollutant_id: PollutantId(53), process_id: ProcessId(15) },
    5316u32 => PollutantProcessAssociation { pollutant_id: PollutantId(53), process_id: ProcessId(16) },
    5317u32 => PollutantProcessAssociation { pollutant_id: PollutantId(53), process_id: ProcessId(17) },
    5390u32 => PollutantProcessAssociation { pollutant_id: PollutantId(53), process_id: ProcessId(90) },
    5391u32 => PollutantProcessAssociation { pollutant_id: PollutantId(53), process_id: ProcessId(91) },
    5401u32 => PollutantProcessAssociation { pollutant_id: PollutantId(54), process_id: ProcessId(1) },
    5402u32 => PollutantProcessAssociation { pollutant_id: PollutantId(54), process_id: ProcessId(2) },
    5415u32 => PollutantProcessAssociation { pollutant_id: PollutantId(54), process_id: ProcessId(15) },
    5416u32 => PollutantProcessAssociation { pollutant_id: PollutantId(54), process_id: ProcessId(16) },
    5417u32 => PollutantProcessAssociation { pollutant_id: PollutantId(54), process_id: ProcessId(17) },
    5490u32 => PollutantProcessAssociation { pollutant_id: PollutantId(54), process_id: ProcessId(90) },
    5491u32 => PollutantProcessAssociation { pollutant_id: PollutantId(54), process_id: ProcessId(91) },
    5501u32 => PollutantProcessAssociation { pollutant_id: PollutantId(55), process_id: ProcessId(1) },
    5502u32 => PollutantProcessAssociation { pollutant_id: PollutantId(55), process_id: ProcessId(2) },
    5515u32 => PollutantProcessAssociation { pollutant_id: PollutantId(55), process_id: ProcessId(15) },
    5516u32 => PollutantProcessAssociation { pollutant_id: PollutantId(55), process_id: ProcessId(16) },
    5517u32 => PollutantProcessAssociation { pollutant_id: PollutantId(55), process_id: ProcessId(17) },
    5590u32 => PollutantProcessAssociation { pollutant_id: PollutantId(55), process_id: ProcessId(90) },
    5591u32 => PollutantProcessAssociation { pollutant_id: PollutantId(55), process_id: ProcessId(91) },
    5601u32 => PollutantProcessAssociation { pollutant_id: PollutantId(56), process_id: ProcessId(1) },
    5602u32 => PollutantProcessAssociation { pollutant_id: PollutantId(56), process_id: ProcessId(2) },
    5615u32 => PollutantProcessAssociation { pollutant_id: PollutantId(56), process_id: ProcessId(15) },
    5616u32 => PollutantProcessAssociation { pollutant_id: PollutantId(56), process_id: ProcessId(16) },
    5617u32 => PollutantProcessAssociation { pollutant_id: PollutantId(56), process_id: ProcessId(17) },
    5690u32 => PollutantProcessAssociation { pollutant_id: PollutantId(56), process_id: ProcessId(90) },
    5691u32 => PollutantProcessAssociation { pollutant_id: PollutantId(56), process_id: ProcessId(91) },
    5701u32 => PollutantProcessAssociation { pollutant_id: PollutantId(57), process_id: ProcessId(1) },
    5702u32 => PollutantProcessAssociation { pollutant_id: PollutantId(57), process_id: ProcessId(2) },
    5715u32 => PollutantProcessAssociation { pollutant_id: PollutantId(57), process_id: ProcessId(15) },
    5716u32 => PollutantProcessAssociation { pollutant_id: PollutantId(57), process_id: ProcessId(16) },
    5717u32 => PollutantProcessAssociation { pollutant_id: PollutantId(57), process_id: ProcessId(17) },
    5790u32 => PollutantProcessAssociation { pollutant_id: PollutantId(57), process_id: ProcessId(90) },
    5791u32 => PollutantProcessAssociation { pollutant_id: PollutantId(57), process_id: ProcessId(91) },
    5801u32 => PollutantProcessAssociation { pollutant_id: PollutantId(58), process_id: ProcessId(1) },
    5802u32 => PollutantProcessAssociation { pollutant_id: PollutantId(58), process_id: ProcessId(2) },
    5815u32 => PollutantProcessAssociation { pollutant_id: PollutantId(58), process_id: ProcessId(15) },
    5816u32 => PollutantProcessAssociation { pollutant_id: PollutantId(58), process_id: ProcessId(16) },
    5817u32 => PollutantProcessAssociation { pollutant_id: PollutantId(58), process_id: ProcessId(17) },
    5890u32 => PollutantProcessAssociation { pollutant_id: PollutantId(58), process_id: ProcessId(90) },
    5891u32 => PollutantProcessAssociation { pollutant_id: PollutantId(58), process_id: ProcessId(91) },
    5901u32 => PollutantProcessAssociation { pollutant_id: PollutantId(59), process_id: ProcessId(1) },
    5902u32 => PollutantProcessAssociation { pollutant_id: PollutantId(59), process_id: ProcessId(2) },
    5915u32 => PollutantProcessAssociation { pollutant_id: PollutantId(59), process_id: ProcessId(15) },
    5916u32 => PollutantProcessAssociation { pollutant_id: PollutantId(59), process_id: ProcessId(16) },
    5917u32 => PollutantProcessAssociation { pollutant_id: PollutantId(59), process_id: ProcessId(17) },
    5990u32 => PollutantProcessAssociation { pollutant_id: PollutantId(59), process_id: ProcessId(90) },
    5991u32 => PollutantProcessAssociation { pollutant_id: PollutantId(59), process_id: ProcessId(91) },
    6001u32 => PollutantProcessAssociation { pollutant_id: PollutantId(60), process_id: ProcessId(1) },
    6101u32 => PollutantProcessAssociation { pollutant_id: PollutantId(61), process_id: ProcessId(1) },
    6201u32 => PollutantProcessAssociation { pollutant_id: PollutantId(62), process_id: ProcessId(1) },
    6301u32 => PollutantProcessAssociation { pollutant_id: PollutantId(63), process_id: ProcessId(1) },
    6501u32 => PollutantProcessAssociation { pollutant_id: PollutantId(65), process_id: ProcessId(1) },
    6601u32 => PollutantProcessAssociation { pollutant_id: PollutantId(66), process_id: ProcessId(1) },
    6701u32 => PollutantProcessAssociation { pollutant_id: PollutantId(67), process_id: ProcessId(1) },
    6801u32 => PollutantProcessAssociation { pollutant_id: PollutantId(68), process_id: ProcessId(1) },
    6802u32 => PollutantProcessAssociation { pollutant_id: PollutantId(68), process_id: ProcessId(2) },
    6815u32 => PollutantProcessAssociation { pollutant_id: PollutantId(68), process_id: ProcessId(15) },
    6816u32 => PollutantProcessAssociation { pollutant_id: PollutantId(68), process_id: ProcessId(16) },
    6817u32 => PollutantProcessAssociation { pollutant_id: PollutantId(68), process_id: ProcessId(17) },
    6901u32 => PollutantProcessAssociation { pollutant_id: PollutantId(69), process_id: ProcessId(1) },
    6902u32 => PollutantProcessAssociation { pollutant_id: PollutantId(69), process_id: ProcessId(2) },
    6915u32 => PollutantProcessAssociation { pollutant_id: PollutantId(69), process_id: ProcessId(15) },
    6916u32 => PollutantProcessAssociation { pollutant_id: PollutantId(69), process_id: ProcessId(16) },
    6917u32 => PollutantProcessAssociation { pollutant_id: PollutantId(69), process_id: ProcessId(17) },
    7001u32 => PollutantProcessAssociation { pollutant_id: PollutantId(70), process_id: ProcessId(1) },
    7002u32 => PollutantProcessAssociation { pollutant_id: PollutantId(70), process_id: ProcessId(2) },
    7015u32 => PollutantProcessAssociation { pollutant_id: PollutantId(70), process_id: ProcessId(15) },
    7016u32 => PollutantProcessAssociation { pollutant_id: PollutantId(70), process_id: ProcessId(16) },
    7017u32 => PollutantProcessAssociation { pollutant_id: PollutantId(70), process_id: ProcessId(17) },
    7101u32 => PollutantProcessAssociation { pollutant_id: PollutantId(71), process_id: ProcessId(1) },
    7102u32 => PollutantProcessAssociation { pollutant_id: PollutantId(71), process_id: ProcessId(2) },
    7115u32 => PollutantProcessAssociation { pollutant_id: PollutantId(71), process_id: ProcessId(15) },
    7116u32 => PollutantProcessAssociation { pollutant_id: PollutantId(71), process_id: ProcessId(16) },
    7117u32 => PollutantProcessAssociation { pollutant_id: PollutantId(71), process_id: ProcessId(17) },
    7201u32 => PollutantProcessAssociation { pollutant_id: PollutantId(72), process_id: ProcessId(1) },
    7202u32 => PollutantProcessAssociation { pollutant_id: PollutantId(72), process_id: ProcessId(2) },
    7215u32 => PollutantProcessAssociation { pollutant_id: PollutantId(72), process_id: ProcessId(15) },
    7216u32 => PollutantProcessAssociation { pollutant_id: PollutantId(72), process_id: ProcessId(16) },
    7217u32 => PollutantProcessAssociation { pollutant_id: PollutantId(72), process_id: ProcessId(17) },
    7301u32 => PollutantProcessAssociation { pollutant_id: PollutantId(73), process_id: ProcessId(1) },
    7302u32 => PollutantProcessAssociation { pollutant_id: PollutantId(73), process_id: ProcessId(2) },
    7315u32 => PollutantProcessAssociation { pollutant_id: PollutantId(73), process_id: ProcessId(15) },
    7316u32 => PollutantProcessAssociation { pollutant_id: PollutantId(73), process_id: ProcessId(16) },
    7317u32 => PollutantProcessAssociation { pollutant_id: PollutantId(73), process_id: ProcessId(17) },
    7401u32 => PollutantProcessAssociation { pollutant_id: PollutantId(74), process_id: ProcessId(1) },
    7402u32 => PollutantProcessAssociation { pollutant_id: PollutantId(74), process_id: ProcessId(2) },
    7415u32 => PollutantProcessAssociation { pollutant_id: PollutantId(74), process_id: ProcessId(15) },
    7416u32 => PollutantProcessAssociation { pollutant_id: PollutantId(74), process_id: ProcessId(16) },
    7417u32 => PollutantProcessAssociation { pollutant_id: PollutantId(74), process_id: ProcessId(17) },
    7501u32 => PollutantProcessAssociation { pollutant_id: PollutantId(75), process_id: ProcessId(1) },
    7502u32 => PollutantProcessAssociation { pollutant_id: PollutantId(75), process_id: ProcessId(2) },
    7515u32 => PollutantProcessAssociation { pollutant_id: PollutantId(75), process_id: ProcessId(15) },
    7516u32 => PollutantProcessAssociation { pollutant_id: PollutantId(75), process_id: ProcessId(16) },
    7517u32 => PollutantProcessAssociation { pollutant_id: PollutantId(75), process_id: ProcessId(17) },
    7601u32 => PollutantProcessAssociation { pollutant_id: PollutantId(76), process_id: ProcessId(1) },
    7602u32 => PollutantProcessAssociation { pollutant_id: PollutantId(76), process_id: ProcessId(2) },
    7615u32 => PollutantProcessAssociation { pollutant_id: PollutantId(76), process_id: ProcessId(15) },
    7616u32 => PollutantProcessAssociation { pollutant_id: PollutantId(76), process_id: ProcessId(16) },
    7617u32 => PollutantProcessAssociation { pollutant_id: PollutantId(76), process_id: ProcessId(17) },
    7701u32 => PollutantProcessAssociation { pollutant_id: PollutantId(77), process_id: ProcessId(1) },
    7702u32 => PollutantProcessAssociation { pollutant_id: PollutantId(77), process_id: ProcessId(2) },
    7715u32 => PollutantProcessAssociation { pollutant_id: PollutantId(77), process_id: ProcessId(15) },
    7716u32 => PollutantProcessAssociation { pollutant_id: PollutantId(77), process_id: ProcessId(16) },
    7717u32 => PollutantProcessAssociation { pollutant_id: PollutantId(77), process_id: ProcessId(17) },
    7801u32 => PollutantProcessAssociation { pollutant_id: PollutantId(78), process_id: ProcessId(1) },
    7802u32 => PollutantProcessAssociation { pollutant_id: PollutantId(78), process_id: ProcessId(2) },
    7815u32 => PollutantProcessAssociation { pollutant_id: PollutantId(78), process_id: ProcessId(15) },
    7816u32 => PollutantProcessAssociation { pollutant_id: PollutantId(78), process_id: ProcessId(16) },
    7817u32 => PollutantProcessAssociation { pollutant_id: PollutantId(78), process_id: ProcessId(17) },
    7901u32 => PollutantProcessAssociation { pollutant_id: PollutantId(79), process_id: ProcessId(1) },
    7902u32 => PollutantProcessAssociation { pollutant_id: PollutantId(79), process_id: ProcessId(2) },
    7911u32 => PollutantProcessAssociation { pollutant_id: PollutantId(79), process_id: ProcessId(11) },
    7912u32 => PollutantProcessAssociation { pollutant_id: PollutantId(79), process_id: ProcessId(12) },
    7913u32 => PollutantProcessAssociation { pollutant_id: PollutantId(79), process_id: ProcessId(13) },
    7915u32 => PollutantProcessAssociation { pollutant_id: PollutantId(79), process_id: ProcessId(15) },
    7916u32 => PollutantProcessAssociation { pollutant_id: PollutantId(79), process_id: ProcessId(16) },
    7917u32 => PollutantProcessAssociation { pollutant_id: PollutantId(79), process_id: ProcessId(17) },
    7918u32 => PollutantProcessAssociation { pollutant_id: PollutantId(79), process_id: ProcessId(18) },
    7919u32 => PollutantProcessAssociation { pollutant_id: PollutantId(79), process_id: ProcessId(19) },
    7920u32 => PollutantProcessAssociation { pollutant_id: PollutantId(79), process_id: ProcessId(20) },
    7921u32 => PollutantProcessAssociation { pollutant_id: PollutantId(79), process_id: ProcessId(21) },
    7930u32 => PollutantProcessAssociation { pollutant_id: PollutantId(79), process_id: ProcessId(30) },
    7931u32 => PollutantProcessAssociation { pollutant_id: PollutantId(79), process_id: ProcessId(31) },
    7932u32 => PollutantProcessAssociation { pollutant_id: PollutantId(79), process_id: ProcessId(32) },
    7990u32 => PollutantProcessAssociation { pollutant_id: PollutantId(79), process_id: ProcessId(90) },
    7991u32 => PollutantProcessAssociation { pollutant_id: PollutantId(79), process_id: ProcessId(91) },
    8001u32 => PollutantProcessAssociation { pollutant_id: PollutantId(80), process_id: ProcessId(1) },
    8002u32 => PollutantProcessAssociation { pollutant_id: PollutantId(80), process_id: ProcessId(2) },
    8011u32 => PollutantProcessAssociation { pollutant_id: PollutantId(80), process_id: ProcessId(11) },
    8012u32 => PollutantProcessAssociation { pollutant_id: PollutantId(80), process_id: ProcessId(12) },
    8013u32 => PollutantProcessAssociation { pollutant_id: PollutantId(80), process_id: ProcessId(13) },
    8015u32 => PollutantProcessAssociation { pollutant_id: PollutantId(80), process_id: ProcessId(15) },
    8016u32 => PollutantProcessAssociation { pollutant_id: PollutantId(80), process_id: ProcessId(16) },
    8017u32 => PollutantProcessAssociation { pollutant_id: PollutantId(80), process_id: ProcessId(17) },
    8018u32 => PollutantProcessAssociation { pollutant_id: PollutantId(80), process_id: ProcessId(18) },
    8019u32 => PollutantProcessAssociation { pollutant_id: PollutantId(80), process_id: ProcessId(19) },
    8020u32 => PollutantProcessAssociation { pollutant_id: PollutantId(80), process_id: ProcessId(20) },
    8021u32 => PollutantProcessAssociation { pollutant_id: PollutantId(80), process_id: ProcessId(21) },
    8030u32 => PollutantProcessAssociation { pollutant_id: PollutantId(80), process_id: ProcessId(30) },
    8031u32 => PollutantProcessAssociation { pollutant_id: PollutantId(80), process_id: ProcessId(31) },
    8032u32 => PollutantProcessAssociation { pollutant_id: PollutantId(80), process_id: ProcessId(32) },
    8090u32 => PollutantProcessAssociation { pollutant_id: PollutantId(80), process_id: ProcessId(90) },
    8091u32 => PollutantProcessAssociation { pollutant_id: PollutantId(80), process_id: ProcessId(91) },
    8101u32 => PollutantProcessAssociation { pollutant_id: PollutantId(81), process_id: ProcessId(1) },
    8102u32 => PollutantProcessAssociation { pollutant_id: PollutantId(81), process_id: ProcessId(2) },
    8115u32 => PollutantProcessAssociation { pollutant_id: PollutantId(81), process_id: ProcessId(15) },
    8116u32 => PollutantProcessAssociation { pollutant_id: PollutantId(81), process_id: ProcessId(16) },
    8117u32 => PollutantProcessAssociation { pollutant_id: PollutantId(81), process_id: ProcessId(17) },
    8201u32 => PollutantProcessAssociation { pollutant_id: PollutantId(82), process_id: ProcessId(1) },
    8202u32 => PollutantProcessAssociation { pollutant_id: PollutantId(82), process_id: ProcessId(2) },
    8215u32 => PollutantProcessAssociation { pollutant_id: PollutantId(82), process_id: ProcessId(15) },
    8216u32 => PollutantProcessAssociation { pollutant_id: PollutantId(82), process_id: ProcessId(16) },
    8217u32 => PollutantProcessAssociation { pollutant_id: PollutantId(82), process_id: ProcessId(17) },
    8301u32 => PollutantProcessAssociation { pollutant_id: PollutantId(83), process_id: ProcessId(1) },
    8302u32 => PollutantProcessAssociation { pollutant_id: PollutantId(83), process_id: ProcessId(2) },
    8315u32 => PollutantProcessAssociation { pollutant_id: PollutantId(83), process_id: ProcessId(15) },
    8316u32 => PollutantProcessAssociation { pollutant_id: PollutantId(83), process_id: ProcessId(16) },
    8317u32 => PollutantProcessAssociation { pollutant_id: PollutantId(83), process_id: ProcessId(17) },
    8401u32 => PollutantProcessAssociation { pollutant_id: PollutantId(84), process_id: ProcessId(1) },
    8402u32 => PollutantProcessAssociation { pollutant_id: PollutantId(84), process_id: ProcessId(2) },
    8415u32 => PollutantProcessAssociation { pollutant_id: PollutantId(84), process_id: ProcessId(15) },
    8416u32 => PollutantProcessAssociation { pollutant_id: PollutantId(84), process_id: ProcessId(16) },
    8417u32 => PollutantProcessAssociation { pollutant_id: PollutantId(84), process_id: ProcessId(17) },
    8601u32 => PollutantProcessAssociation { pollutant_id: PollutantId(86), process_id: ProcessId(1) },
    8602u32 => PollutantProcessAssociation { pollutant_id: PollutantId(86), process_id: ProcessId(2) },
    8611u32 => PollutantProcessAssociation { pollutant_id: PollutantId(86), process_id: ProcessId(11) },
    8612u32 => PollutantProcessAssociation { pollutant_id: PollutantId(86), process_id: ProcessId(12) },
    8613u32 => PollutantProcessAssociation { pollutant_id: PollutantId(86), process_id: ProcessId(13) },
    8615u32 => PollutantProcessAssociation { pollutant_id: PollutantId(86), process_id: ProcessId(15) },
    8616u32 => PollutantProcessAssociation { pollutant_id: PollutantId(86), process_id: ProcessId(16) },
    8617u32 => PollutantProcessAssociation { pollutant_id: PollutantId(86), process_id: ProcessId(17) },
    8618u32 => PollutantProcessAssociation { pollutant_id: PollutantId(86), process_id: ProcessId(18) },
    8619u32 => PollutantProcessAssociation { pollutant_id: PollutantId(86), process_id: ProcessId(19) },
    8620u32 => PollutantProcessAssociation { pollutant_id: PollutantId(86), process_id: ProcessId(20) },
    8621u32 => PollutantProcessAssociation { pollutant_id: PollutantId(86), process_id: ProcessId(21) },
    8630u32 => PollutantProcessAssociation { pollutant_id: PollutantId(86), process_id: ProcessId(30) },
    8631u32 => PollutantProcessAssociation { pollutant_id: PollutantId(86), process_id: ProcessId(31) },
    8632u32 => PollutantProcessAssociation { pollutant_id: PollutantId(86), process_id: ProcessId(32) },
    8690u32 => PollutantProcessAssociation { pollutant_id: PollutantId(86), process_id: ProcessId(90) },
    8691u32 => PollutantProcessAssociation { pollutant_id: PollutantId(86), process_id: ProcessId(91) },
    8701u32 => PollutantProcessAssociation { pollutant_id: PollutantId(87), process_id: ProcessId(1) },
    8702u32 => PollutantProcessAssociation { pollutant_id: PollutantId(87), process_id: ProcessId(2) },
    8711u32 => PollutantProcessAssociation { pollutant_id: PollutantId(87), process_id: ProcessId(11) },
    8712u32 => PollutantProcessAssociation { pollutant_id: PollutantId(87), process_id: ProcessId(12) },
    8713u32 => PollutantProcessAssociation { pollutant_id: PollutantId(87), process_id: ProcessId(13) },
    8715u32 => PollutantProcessAssociation { pollutant_id: PollutantId(87), process_id: ProcessId(15) },
    8716u32 => PollutantProcessAssociation { pollutant_id: PollutantId(87), process_id: ProcessId(16) },
    8717u32 => PollutantProcessAssociation { pollutant_id: PollutantId(87), process_id: ProcessId(17) },
    8718u32 => PollutantProcessAssociation { pollutant_id: PollutantId(87), process_id: ProcessId(18) },
    8719u32 => PollutantProcessAssociation { pollutant_id: PollutantId(87), process_id: ProcessId(19) },
    8720u32 => PollutantProcessAssociation { pollutant_id: PollutantId(87), process_id: ProcessId(20) },
    8721u32 => PollutantProcessAssociation { pollutant_id: PollutantId(87), process_id: ProcessId(21) },
    8730u32 => PollutantProcessAssociation { pollutant_id: PollutantId(87), process_id: ProcessId(30) },
    8731u32 => PollutantProcessAssociation { pollutant_id: PollutantId(87), process_id: ProcessId(31) },
    8732u32 => PollutantProcessAssociation { pollutant_id: PollutantId(87), process_id: ProcessId(32) },
    8790u32 => PollutantProcessAssociation { pollutant_id: PollutantId(87), process_id: ProcessId(90) },
    8791u32 => PollutantProcessAssociation { pollutant_id: PollutantId(87), process_id: ProcessId(91) },
    8801u32 => PollutantProcessAssociation { pollutant_id: PollutantId(88), process_id: ProcessId(1) },
    8802u32 => PollutantProcessAssociation { pollutant_id: PollutantId(88), process_id: ProcessId(2) },
    8811u32 => PollutantProcessAssociation { pollutant_id: PollutantId(88), process_id: ProcessId(11) },
    8812u32 => PollutantProcessAssociation { pollutant_id: PollutantId(88), process_id: ProcessId(12) },
    8813u32 => PollutantProcessAssociation { pollutant_id: PollutantId(88), process_id: ProcessId(13) },
    8815u32 => PollutantProcessAssociation { pollutant_id: PollutantId(88), process_id: ProcessId(15) },
    8816u32 => PollutantProcessAssociation { pollutant_id: PollutantId(88), process_id: ProcessId(16) },
    8817u32 => PollutantProcessAssociation { pollutant_id: PollutantId(88), process_id: ProcessId(17) },
    8818u32 => PollutantProcessAssociation { pollutant_id: PollutantId(88), process_id: ProcessId(18) },
    8819u32 => PollutantProcessAssociation { pollutant_id: PollutantId(88), process_id: ProcessId(19) },
    8820u32 => PollutantProcessAssociation { pollutant_id: PollutantId(88), process_id: ProcessId(20) },
    8821u32 => PollutantProcessAssociation { pollutant_id: PollutantId(88), process_id: ProcessId(21) },
    8830u32 => PollutantProcessAssociation { pollutant_id: PollutantId(88), process_id: ProcessId(30) },
    8831u32 => PollutantProcessAssociation { pollutant_id: PollutantId(88), process_id: ProcessId(31) },
    8832u32 => PollutantProcessAssociation { pollutant_id: PollutantId(88), process_id: ProcessId(32) },
    8890u32 => PollutantProcessAssociation { pollutant_id: PollutantId(88), process_id: ProcessId(90) },
    8891u32 => PollutantProcessAssociation { pollutant_id: PollutantId(88), process_id: ProcessId(91) },
    9001u32 => PollutantProcessAssociation { pollutant_id: PollutantId(90), process_id: ProcessId(1) },
    9002u32 => PollutantProcessAssociation { pollutant_id: PollutantId(90), process_id: ProcessId(2) },
    9090u32 => PollutantProcessAssociation { pollutant_id: PollutantId(90), process_id: ProcessId(90) },
    9091u32 => PollutantProcessAssociation { pollutant_id: PollutantId(90), process_id: ProcessId(91) },
    9101u32 => PollutantProcessAssociation { pollutant_id: PollutantId(91), process_id: ProcessId(1) },
    9102u32 => PollutantProcessAssociation { pollutant_id: PollutantId(91), process_id: ProcessId(2) },
    9109u32 => PollutantProcessAssociation { pollutant_id: PollutantId(91), process_id: ProcessId(9) },
    9110u32 => PollutantProcessAssociation { pollutant_id: PollutantId(91), process_id: ProcessId(10) },
    9190u32 => PollutantProcessAssociation { pollutant_id: PollutantId(91), process_id: ProcessId(90) },
    9191u32 => PollutantProcessAssociation { pollutant_id: PollutantId(91), process_id: ProcessId(91) },
    9201u32 => PollutantProcessAssociation { pollutant_id: PollutantId(92), process_id: ProcessId(1) },
    9202u32 => PollutantProcessAssociation { pollutant_id: PollutantId(92), process_id: ProcessId(2) },
    9209u32 => PollutantProcessAssociation { pollutant_id: PollutantId(92), process_id: ProcessId(9) },
    9210u32 => PollutantProcessAssociation { pollutant_id: PollutantId(92), process_id: ProcessId(10) },
    9290u32 => PollutantProcessAssociation { pollutant_id: PollutantId(92), process_id: ProcessId(90) },
    9291u32 => PollutantProcessAssociation { pollutant_id: PollutantId(92), process_id: ProcessId(91) },
    9301u32 => PollutantProcessAssociation { pollutant_id: PollutantId(93), process_id: ProcessId(1) },
    9302u32 => PollutantProcessAssociation { pollutant_id: PollutantId(93), process_id: ProcessId(2) },
    9309u32 => PollutantProcessAssociation { pollutant_id: PollutantId(93), process_id: ProcessId(9) },
    9310u32 => PollutantProcessAssociation { pollutant_id: PollutantId(93), process_id: ProcessId(10) },
    9390u32 => PollutantProcessAssociation { pollutant_id: PollutantId(93), process_id: ProcessId(90) },
    9391u32 => PollutantProcessAssociation { pollutant_id: PollutantId(93), process_id: ProcessId(91) },
    9801u32 => PollutantProcessAssociation { pollutant_id: PollutantId(98), process_id: ProcessId(1) },
    9802u32 => PollutantProcessAssociation { pollutant_id: PollutantId(98), process_id: ProcessId(2) },
    9890u32 => PollutantProcessAssociation { pollutant_id: PollutantId(98), process_id: ProcessId(90) },
    9891u32 => PollutantProcessAssociation { pollutant_id: PollutantId(98), process_id: ProcessId(91) },
    9901u32 => PollutantProcessAssociation { pollutant_id: PollutantId(99), process_id: ProcessId(1) },
    10001u32 => PollutantProcessAssociation { pollutant_id: PollutantId(100), process_id: ProcessId(1) },
    10002u32 => PollutantProcessAssociation { pollutant_id: PollutantId(100), process_id: ProcessId(2) },
    10015u32 => PollutantProcessAssociation { pollutant_id: PollutantId(100), process_id: ProcessId(15) },
    10016u32 => PollutantProcessAssociation { pollutant_id: PollutantId(100), process_id: ProcessId(16) },
    10017u32 => PollutantProcessAssociation { pollutant_id: PollutantId(100), process_id: ProcessId(17) },
    10090u32 => PollutantProcessAssociation { pollutant_id: PollutantId(100), process_id: ProcessId(90) },
    10091u32 => PollutantProcessAssociation { pollutant_id: PollutantId(100), process_id: ProcessId(91) },
    10609u32 => PollutantProcessAssociation { pollutant_id: PollutantId(106), process_id: ProcessId(9) },
    10710u32 => PollutantProcessAssociation { pollutant_id: PollutantId(107), process_id: ProcessId(10) },
    11001u32 => PollutantProcessAssociation { pollutant_id: PollutantId(110), process_id: ProcessId(1) },
    11002u32 => PollutantProcessAssociation { pollutant_id: PollutantId(110), process_id: ProcessId(2) },
    11015u32 => PollutantProcessAssociation { pollutant_id: PollutantId(110), process_id: ProcessId(15) },
    11016u32 => PollutantProcessAssociation { pollutant_id: PollutantId(110), process_id: ProcessId(16) },
    11017u32 => PollutantProcessAssociation { pollutant_id: PollutantId(110), process_id: ProcessId(17) },
    11090u32 => PollutantProcessAssociation { pollutant_id: PollutantId(110), process_id: ProcessId(90) },
    11091u32 => PollutantProcessAssociation { pollutant_id: PollutantId(110), process_id: ProcessId(91) },
    11101u32 => PollutantProcessAssociation { pollutant_id: PollutantId(111), process_id: ProcessId(1) },
    11102u32 => PollutantProcessAssociation { pollutant_id: PollutantId(111), process_id: ProcessId(2) },
    11115u32 => PollutantProcessAssociation { pollutant_id: PollutantId(111), process_id: ProcessId(15) },
    11116u32 => PollutantProcessAssociation { pollutant_id: PollutantId(111), process_id: ProcessId(16) },
    11117u32 => PollutantProcessAssociation { pollutant_id: PollutantId(111), process_id: ProcessId(17) },
    11190u32 => PollutantProcessAssociation { pollutant_id: PollutantId(111), process_id: ProcessId(90) },
    11191u32 => PollutantProcessAssociation { pollutant_id: PollutantId(111), process_id: ProcessId(91) },
    11201u32 => PollutantProcessAssociation { pollutant_id: PollutantId(112), process_id: ProcessId(1) },
    11202u32 => PollutantProcessAssociation { pollutant_id: PollutantId(112), process_id: ProcessId(2) },
    11209u32 => PollutantProcessAssociation { pollutant_id: PollutantId(112), process_id: ProcessId(9) },
    11210u32 => PollutantProcessAssociation { pollutant_id: PollutantId(112), process_id: ProcessId(10) },
    11215u32 => PollutantProcessAssociation { pollutant_id: PollutantId(112), process_id: ProcessId(15) },
    11216u32 => PollutantProcessAssociation { pollutant_id: PollutantId(112), process_id: ProcessId(16) },
    11217u32 => PollutantProcessAssociation { pollutant_id: PollutantId(112), process_id: ProcessId(17) },
    11290u32 => PollutantProcessAssociation { pollutant_id: PollutantId(112), process_id: ProcessId(90) },
    11291u32 => PollutantProcessAssociation { pollutant_id: PollutantId(112), process_id: ProcessId(91) },
    11501u32 => PollutantProcessAssociation { pollutant_id: PollutantId(115), process_id: ProcessId(1) },
    11502u32 => PollutantProcessAssociation { pollutant_id: PollutantId(115), process_id: ProcessId(2) },
    11515u32 => PollutantProcessAssociation { pollutant_id: PollutantId(115), process_id: ProcessId(15) },
    11516u32 => PollutantProcessAssociation { pollutant_id: PollutantId(115), process_id: ProcessId(16) },
    11517u32 => PollutantProcessAssociation { pollutant_id: PollutantId(115), process_id: ProcessId(17) },
    11590u32 => PollutantProcessAssociation { pollutant_id: PollutantId(115), process_id: ProcessId(90) },
    11591u32 => PollutantProcessAssociation { pollutant_id: PollutantId(115), process_id: ProcessId(91) },
    11601u32 => PollutantProcessAssociation { pollutant_id: PollutantId(116), process_id: ProcessId(1) },
    11602u32 => PollutantProcessAssociation { pollutant_id: PollutantId(116), process_id: ProcessId(2) },
    11609u32 => PollutantProcessAssociation { pollutant_id: PollutantId(116), process_id: ProcessId(9) },
    11610u32 => PollutantProcessAssociation { pollutant_id: PollutantId(116), process_id: ProcessId(10) },
    11690u32 => PollutantProcessAssociation { pollutant_id: PollutantId(116), process_id: ProcessId(90) },
    11691u32 => PollutantProcessAssociation { pollutant_id: PollutantId(116), process_id: ProcessId(91) },
    11701u32 => PollutantProcessAssociation { pollutant_id: PollutantId(117), process_id: ProcessId(1) },
    11702u32 => PollutantProcessAssociation { pollutant_id: PollutantId(117), process_id: ProcessId(2) },
    11709u32 => PollutantProcessAssociation { pollutant_id: PollutantId(117), process_id: ProcessId(9) },
    11710u32 => PollutantProcessAssociation { pollutant_id: PollutantId(117), process_id: ProcessId(10) },
    11790u32 => PollutantProcessAssociation { pollutant_id: PollutantId(117), process_id: ProcessId(90) },
    11791u32 => PollutantProcessAssociation { pollutant_id: PollutantId(117), process_id: ProcessId(91) },
    11801u32 => PollutantProcessAssociation { pollutant_id: PollutantId(118), process_id: ProcessId(1) },
    11802u32 => PollutantProcessAssociation { pollutant_id: PollutantId(118), process_id: ProcessId(2) },
    11809u32 => PollutantProcessAssociation { pollutant_id: PollutantId(118), process_id: ProcessId(9) },
    11810u32 => PollutantProcessAssociation { pollutant_id: PollutantId(118), process_id: ProcessId(10) },
    11815u32 => PollutantProcessAssociation { pollutant_id: PollutantId(118), process_id: ProcessId(15) },
    11816u32 => PollutantProcessAssociation { pollutant_id: PollutantId(118), process_id: ProcessId(16) },
    11817u32 => PollutantProcessAssociation { pollutant_id: PollutantId(118), process_id: ProcessId(17) },
    11890u32 => PollutantProcessAssociation { pollutant_id: PollutantId(118), process_id: ProcessId(90) },
    11891u32 => PollutantProcessAssociation { pollutant_id: PollutantId(118), process_id: ProcessId(91) },
    11901u32 => PollutantProcessAssociation { pollutant_id: PollutantId(119), process_id: ProcessId(1) },
    11902u32 => PollutantProcessAssociation { pollutant_id: PollutantId(119), process_id: ProcessId(2) },
    11915u32 => PollutantProcessAssociation { pollutant_id: PollutantId(119), process_id: ProcessId(15) },
    11916u32 => PollutantProcessAssociation { pollutant_id: PollutantId(119), process_id: ProcessId(16) },
    11917u32 => PollutantProcessAssociation { pollutant_id: PollutantId(119), process_id: ProcessId(17) },
    11990u32 => PollutantProcessAssociation { pollutant_id: PollutantId(119), process_id: ProcessId(90) },
    11991u32 => PollutantProcessAssociation { pollutant_id: PollutantId(119), process_id: ProcessId(91) },
    12101u32 => PollutantProcessAssociation { pollutant_id: PollutantId(121), process_id: ProcessId(1) },
    12102u32 => PollutantProcessAssociation { pollutant_id: PollutantId(121), process_id: ProcessId(2) },
    12115u32 => PollutantProcessAssociation { pollutant_id: PollutantId(121), process_id: ProcessId(15) },
    12116u32 => PollutantProcessAssociation { pollutant_id: PollutantId(121), process_id: ProcessId(16) },
    12117u32 => PollutantProcessAssociation { pollutant_id: PollutantId(121), process_id: ProcessId(17) },
    12190u32 => PollutantProcessAssociation { pollutant_id: PollutantId(121), process_id: ProcessId(90) },
    12191u32 => PollutantProcessAssociation { pollutant_id: PollutantId(121), process_id: ProcessId(91) },
    12201u32 => PollutantProcessAssociation { pollutant_id: PollutantId(122), process_id: ProcessId(1) },
    12202u32 => PollutantProcessAssociation { pollutant_id: PollutantId(122), process_id: ProcessId(2) },
    12215u32 => PollutantProcessAssociation { pollutant_id: PollutantId(122), process_id: ProcessId(15) },
    12216u32 => PollutantProcessAssociation { pollutant_id: PollutantId(122), process_id: ProcessId(16) },
    12217u32 => PollutantProcessAssociation { pollutant_id: PollutantId(122), process_id: ProcessId(17) },
    12290u32 => PollutantProcessAssociation { pollutant_id: PollutantId(122), process_id: ProcessId(90) },
    12291u32 => PollutantProcessAssociation { pollutant_id: PollutantId(122), process_id: ProcessId(91) },
    13001u32 => PollutantProcessAssociation { pollutant_id: PollutantId(130), process_id: ProcessId(1) },
    13101u32 => PollutantProcessAssociation { pollutant_id: PollutantId(131), process_id: ProcessId(1) },
    13201u32 => PollutantProcessAssociation { pollutant_id: PollutantId(132), process_id: ProcessId(1) },
    13301u32 => PollutantProcessAssociation { pollutant_id: PollutantId(133), process_id: ProcessId(1) },
    13401u32 => PollutantProcessAssociation { pollutant_id: PollutantId(134), process_id: ProcessId(1) },
    13501u32 => PollutantProcessAssociation { pollutant_id: PollutantId(135), process_id: ProcessId(1) },
    13601u32 => PollutantProcessAssociation { pollutant_id: PollutantId(136), process_id: ProcessId(1) },
    13701u32 => PollutantProcessAssociation { pollutant_id: PollutantId(137), process_id: ProcessId(1) },
    13801u32 => PollutantProcessAssociation { pollutant_id: PollutantId(138), process_id: ProcessId(1) },
    13901u32 => PollutantProcessAssociation { pollutant_id: PollutantId(139), process_id: ProcessId(1) },
    14001u32 => PollutantProcessAssociation { pollutant_id: PollutantId(140), process_id: ProcessId(1) },
    14101u32 => PollutantProcessAssociation { pollutant_id: PollutantId(141), process_id: ProcessId(1) },
    14201u32 => PollutantProcessAssociation { pollutant_id: PollutantId(142), process_id: ProcessId(1) },
    14301u32 => PollutantProcessAssociation { pollutant_id: PollutantId(143), process_id: ProcessId(1) },
    14401u32 => PollutantProcessAssociation { pollutant_id: PollutantId(144), process_id: ProcessId(1) },
    14501u32 => PollutantProcessAssociation { pollutant_id: PollutantId(145), process_id: ProcessId(1) },
    14601u32 => PollutantProcessAssociation { pollutant_id: PollutantId(146), process_id: ProcessId(1) },
    16801u32 => PollutantProcessAssociation { pollutant_id: PollutantId(168), process_id: ProcessId(1) },
    16802u32 => PollutantProcessAssociation { pollutant_id: PollutantId(168), process_id: ProcessId(2) },
    16815u32 => PollutantProcessAssociation { pollutant_id: PollutantId(168), process_id: ProcessId(15) },
    16816u32 => PollutantProcessAssociation { pollutant_id: PollutantId(168), process_id: ProcessId(16) },
    16817u32 => PollutantProcessAssociation { pollutant_id: PollutantId(168), process_id: ProcessId(17) },
    16890u32 => PollutantProcessAssociation { pollutant_id: PollutantId(168), process_id: ProcessId(90) },
    16891u32 => PollutantProcessAssociation { pollutant_id: PollutantId(168), process_id: ProcessId(91) },
    16901u32 => PollutantProcessAssociation { pollutant_id: PollutantId(169), process_id: ProcessId(1) },
    16902u32 => PollutantProcessAssociation { pollutant_id: PollutantId(169), process_id: ProcessId(2) },
    16915u32 => PollutantProcessAssociation { pollutant_id: PollutantId(169), process_id: ProcessId(15) },
    16916u32 => PollutantProcessAssociation { pollutant_id: PollutantId(169), process_id: ProcessId(16) },
    16917u32 => PollutantProcessAssociation { pollutant_id: PollutantId(169), process_id: ProcessId(17) },
    16990u32 => PollutantProcessAssociation { pollutant_id: PollutantId(169), process_id: ProcessId(90) },
    16991u32 => PollutantProcessAssociation { pollutant_id: PollutantId(169), process_id: ProcessId(91) },
    17001u32 => PollutantProcessAssociation { pollutant_id: PollutantId(170), process_id: ProcessId(1) },
    17002u32 => PollutantProcessAssociation { pollutant_id: PollutantId(170), process_id: ProcessId(2) },
    17015u32 => PollutantProcessAssociation { pollutant_id: PollutantId(170), process_id: ProcessId(15) },
    17016u32 => PollutantProcessAssociation { pollutant_id: PollutantId(170), process_id: ProcessId(16) },
    17017u32 => PollutantProcessAssociation { pollutant_id: PollutantId(170), process_id: ProcessId(17) },
    17090u32 => PollutantProcessAssociation { pollutant_id: PollutantId(170), process_id: ProcessId(90) },
    17091u32 => PollutantProcessAssociation { pollutant_id: PollutantId(170), process_id: ProcessId(91) },
    17101u32 => PollutantProcessAssociation { pollutant_id: PollutantId(171), process_id: ProcessId(1) },
    17102u32 => PollutantProcessAssociation { pollutant_id: PollutantId(171), process_id: ProcessId(2) },
    17115u32 => PollutantProcessAssociation { pollutant_id: PollutantId(171), process_id: ProcessId(15) },
    17116u32 => PollutantProcessAssociation { pollutant_id: PollutantId(171), process_id: ProcessId(16) },
    17117u32 => PollutantProcessAssociation { pollutant_id: PollutantId(171), process_id: ProcessId(17) },
    17190u32 => PollutantProcessAssociation { pollutant_id: PollutantId(171), process_id: ProcessId(90) },
    17191u32 => PollutantProcessAssociation { pollutant_id: PollutantId(171), process_id: ProcessId(91) },
    17201u32 => PollutantProcessAssociation { pollutant_id: PollutantId(172), process_id: ProcessId(1) },
    17202u32 => PollutantProcessAssociation { pollutant_id: PollutantId(172), process_id: ProcessId(2) },
    17215u32 => PollutantProcessAssociation { pollutant_id: PollutantId(172), process_id: ProcessId(15) },
    17216u32 => PollutantProcessAssociation { pollutant_id: PollutantId(172), process_id: ProcessId(16) },
    17217u32 => PollutantProcessAssociation { pollutant_id: PollutantId(172), process_id: ProcessId(17) },
    17290u32 => PollutantProcessAssociation { pollutant_id: PollutantId(172), process_id: ProcessId(90) },
    17291u32 => PollutantProcessAssociation { pollutant_id: PollutantId(172), process_id: ProcessId(91) },
    17301u32 => PollutantProcessAssociation { pollutant_id: PollutantId(173), process_id: ProcessId(1) },
    17302u32 => PollutantProcessAssociation { pollutant_id: PollutantId(173), process_id: ProcessId(2) },
    17315u32 => PollutantProcessAssociation { pollutant_id: PollutantId(173), process_id: ProcessId(15) },
    17316u32 => PollutantProcessAssociation { pollutant_id: PollutantId(173), process_id: ProcessId(16) },
    17317u32 => PollutantProcessAssociation { pollutant_id: PollutantId(173), process_id: ProcessId(17) },
    17390u32 => PollutantProcessAssociation { pollutant_id: PollutantId(173), process_id: ProcessId(90) },
    17391u32 => PollutantProcessAssociation { pollutant_id: PollutantId(173), process_id: ProcessId(91) },
    17401u32 => PollutantProcessAssociation { pollutant_id: PollutantId(174), process_id: ProcessId(1) },
    17402u32 => PollutantProcessAssociation { pollutant_id: PollutantId(174), process_id: ProcessId(2) },
    17415u32 => PollutantProcessAssociation { pollutant_id: PollutantId(174), process_id: ProcessId(15) },
    17416u32 => PollutantProcessAssociation { pollutant_id: PollutantId(174), process_id: ProcessId(16) },
    17417u32 => PollutantProcessAssociation { pollutant_id: PollutantId(174), process_id: ProcessId(17) },
    17490u32 => PollutantProcessAssociation { pollutant_id: PollutantId(174), process_id: ProcessId(90) },
    17491u32 => PollutantProcessAssociation { pollutant_id: PollutantId(174), process_id: ProcessId(91) },
    17501u32 => PollutantProcessAssociation { pollutant_id: PollutantId(175), process_id: ProcessId(1) },
    17502u32 => PollutantProcessAssociation { pollutant_id: PollutantId(175), process_id: ProcessId(2) },
    17515u32 => PollutantProcessAssociation { pollutant_id: PollutantId(175), process_id: ProcessId(15) },
    17516u32 => PollutantProcessAssociation { pollutant_id: PollutantId(175), process_id: ProcessId(16) },
    17517u32 => PollutantProcessAssociation { pollutant_id: PollutantId(175), process_id: ProcessId(17) },
    17590u32 => PollutantProcessAssociation { pollutant_id: PollutantId(175), process_id: ProcessId(90) },
    17591u32 => PollutantProcessAssociation { pollutant_id: PollutantId(175), process_id: ProcessId(91) },
    17601u32 => PollutantProcessAssociation { pollutant_id: PollutantId(176), process_id: ProcessId(1) },
    17602u32 => PollutantProcessAssociation { pollutant_id: PollutantId(176), process_id: ProcessId(2) },
    17615u32 => PollutantProcessAssociation { pollutant_id: PollutantId(176), process_id: ProcessId(15) },
    17616u32 => PollutantProcessAssociation { pollutant_id: PollutantId(176), process_id: ProcessId(16) },
    17617u32 => PollutantProcessAssociation { pollutant_id: PollutantId(176), process_id: ProcessId(17) },
    17690u32 => PollutantProcessAssociation { pollutant_id: PollutantId(176), process_id: ProcessId(90) },
    17691u32 => PollutantProcessAssociation { pollutant_id: PollutantId(176), process_id: ProcessId(91) },
    17701u32 => PollutantProcessAssociation { pollutant_id: PollutantId(177), process_id: ProcessId(1) },
    17702u32 => PollutantProcessAssociation { pollutant_id: PollutantId(177), process_id: ProcessId(2) },
    17715u32 => PollutantProcessAssociation { pollutant_id: PollutantId(177), process_id: ProcessId(15) },
    17716u32 => PollutantProcessAssociation { pollutant_id: PollutantId(177), process_id: ProcessId(16) },
    17717u32 => PollutantProcessAssociation { pollutant_id: PollutantId(177), process_id: ProcessId(17) },
    17790u32 => PollutantProcessAssociation { pollutant_id: PollutantId(177), process_id: ProcessId(90) },
    17791u32 => PollutantProcessAssociation { pollutant_id: PollutantId(177), process_id: ProcessId(91) },
    17801u32 => PollutantProcessAssociation { pollutant_id: PollutantId(178), process_id: ProcessId(1) },
    17802u32 => PollutantProcessAssociation { pollutant_id: PollutantId(178), process_id: ProcessId(2) },
    17815u32 => PollutantProcessAssociation { pollutant_id: PollutantId(178), process_id: ProcessId(15) },
    17816u32 => PollutantProcessAssociation { pollutant_id: PollutantId(178), process_id: ProcessId(16) },
    17817u32 => PollutantProcessAssociation { pollutant_id: PollutantId(178), process_id: ProcessId(17) },
    17890u32 => PollutantProcessAssociation { pollutant_id: PollutantId(178), process_id: ProcessId(90) },
    17891u32 => PollutantProcessAssociation { pollutant_id: PollutantId(178), process_id: ProcessId(91) },
    18101u32 => PollutantProcessAssociation { pollutant_id: PollutantId(181), process_id: ProcessId(1) },
    18102u32 => PollutantProcessAssociation { pollutant_id: PollutantId(181), process_id: ProcessId(2) },
    18115u32 => PollutantProcessAssociation { pollutant_id: PollutantId(181), process_id: ProcessId(15) },
    18116u32 => PollutantProcessAssociation { pollutant_id: PollutantId(181), process_id: ProcessId(16) },
    18117u32 => PollutantProcessAssociation { pollutant_id: PollutantId(181), process_id: ProcessId(17) },
    18190u32 => PollutantProcessAssociation { pollutant_id: PollutantId(181), process_id: ProcessId(90) },
    18191u32 => PollutantProcessAssociation { pollutant_id: PollutantId(181), process_id: ProcessId(91) },
    18201u32 => PollutantProcessAssociation { pollutant_id: PollutantId(182), process_id: ProcessId(1) },
    18202u32 => PollutantProcessAssociation { pollutant_id: PollutantId(182), process_id: ProcessId(2) },
    18215u32 => PollutantProcessAssociation { pollutant_id: PollutantId(182), process_id: ProcessId(15) },
    18216u32 => PollutantProcessAssociation { pollutant_id: PollutantId(182), process_id: ProcessId(16) },
    18217u32 => PollutantProcessAssociation { pollutant_id: PollutantId(182), process_id: ProcessId(17) },
    18290u32 => PollutantProcessAssociation { pollutant_id: PollutantId(182), process_id: ProcessId(90) },
    18291u32 => PollutantProcessAssociation { pollutant_id: PollutantId(182), process_id: ProcessId(91) },
    18301u32 => PollutantProcessAssociation { pollutant_id: PollutantId(183), process_id: ProcessId(1) },
    18302u32 => PollutantProcessAssociation { pollutant_id: PollutantId(183), process_id: ProcessId(2) },
    18315u32 => PollutantProcessAssociation { pollutant_id: PollutantId(183), process_id: ProcessId(15) },
    18316u32 => PollutantProcessAssociation { pollutant_id: PollutantId(183), process_id: ProcessId(16) },
    18317u32 => PollutantProcessAssociation { pollutant_id: PollutantId(183), process_id: ProcessId(17) },
    18390u32 => PollutantProcessAssociation { pollutant_id: PollutantId(183), process_id: ProcessId(90) },
    18391u32 => PollutantProcessAssociation { pollutant_id: PollutantId(183), process_id: ProcessId(91) },
    18401u32 => PollutantProcessAssociation { pollutant_id: PollutantId(184), process_id: ProcessId(1) },
    18402u32 => PollutantProcessAssociation { pollutant_id: PollutantId(184), process_id: ProcessId(2) },
    18415u32 => PollutantProcessAssociation { pollutant_id: PollutantId(184), process_id: ProcessId(15) },
    18416u32 => PollutantProcessAssociation { pollutant_id: PollutantId(184), process_id: ProcessId(16) },
    18417u32 => PollutantProcessAssociation { pollutant_id: PollutantId(184), process_id: ProcessId(17) },
    18490u32 => PollutantProcessAssociation { pollutant_id: PollutantId(184), process_id: ProcessId(90) },
    18491u32 => PollutantProcessAssociation { pollutant_id: PollutantId(184), process_id: ProcessId(91) },
    18501u32 => PollutantProcessAssociation { pollutant_id: PollutantId(185), process_id: ProcessId(1) },
    18502u32 => PollutantProcessAssociation { pollutant_id: PollutantId(185), process_id: ProcessId(2) },
    18511u32 => PollutantProcessAssociation { pollutant_id: PollutantId(185), process_id: ProcessId(11) },
    18512u32 => PollutantProcessAssociation { pollutant_id: PollutantId(185), process_id: ProcessId(12) },
    18513u32 => PollutantProcessAssociation { pollutant_id: PollutantId(185), process_id: ProcessId(13) },
    18515u32 => PollutantProcessAssociation { pollutant_id: PollutantId(185), process_id: ProcessId(15) },
    18516u32 => PollutantProcessAssociation { pollutant_id: PollutantId(185), process_id: ProcessId(16) },
    18517u32 => PollutantProcessAssociation { pollutant_id: PollutantId(185), process_id: ProcessId(17) },
    18518u32 => PollutantProcessAssociation { pollutant_id: PollutantId(185), process_id: ProcessId(18) },
    18519u32 => PollutantProcessAssociation { pollutant_id: PollutantId(185), process_id: ProcessId(19) },
    18590u32 => PollutantProcessAssociation { pollutant_id: PollutantId(185), process_id: ProcessId(90) },
    18591u32 => PollutantProcessAssociation { pollutant_id: PollutantId(185), process_id: ProcessId(91) },
    100001u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1000), process_id: ProcessId(1) },
    100002u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1000), process_id: ProcessId(2) },
    100011u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1000), process_id: ProcessId(11) },
    100012u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1000), process_id: ProcessId(12) },
    100013u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1000), process_id: ProcessId(13) },
    100015u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1000), process_id: ProcessId(15) },
    100016u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1000), process_id: ProcessId(16) },
    100017u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1000), process_id: ProcessId(17) },
    100018u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1000), process_id: ProcessId(18) },
    100019u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1000), process_id: ProcessId(19) },
    100090u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1000), process_id: ProcessId(90) },
    100091u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1000), process_id: ProcessId(91) },
    100101u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1001), process_id: ProcessId(1) },
    100102u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1001), process_id: ProcessId(2) },
    100111u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1001), process_id: ProcessId(11) },
    100112u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1001), process_id: ProcessId(12) },
    100113u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1001), process_id: ProcessId(13) },
    100115u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1001), process_id: ProcessId(15) },
    100116u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1001), process_id: ProcessId(16) },
    100117u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1001), process_id: ProcessId(17) },
    100118u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1001), process_id: ProcessId(18) },
    100119u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1001), process_id: ProcessId(19) },
    100190u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1001), process_id: ProcessId(90) },
    100191u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1001), process_id: ProcessId(91) },
    100201u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1002), process_id: ProcessId(1) },
    100202u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1002), process_id: ProcessId(2) },
    100211u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1002), process_id: ProcessId(11) },
    100212u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1002), process_id: ProcessId(12) },
    100213u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1002), process_id: ProcessId(13) },
    100215u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1002), process_id: ProcessId(15) },
    100216u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1002), process_id: ProcessId(16) },
    100217u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1002), process_id: ProcessId(17) },
    100218u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1002), process_id: ProcessId(18) },
    100219u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1002), process_id: ProcessId(19) },
    100290u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1002), process_id: ProcessId(90) },
    100291u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1002), process_id: ProcessId(91) },
    100501u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1005), process_id: ProcessId(1) },
    100502u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1005), process_id: ProcessId(2) },
    100511u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1005), process_id: ProcessId(11) },
    100515u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1005), process_id: ProcessId(15) },
    100516u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1005), process_id: ProcessId(16) },
    100517u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1005), process_id: ProcessId(17) },
    100590u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1005), process_id: ProcessId(90) },
    100591u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1005), process_id: ProcessId(91) },
    100601u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1006), process_id: ProcessId(1) },
    100602u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1006), process_id: ProcessId(2) },
    100611u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1006), process_id: ProcessId(11) },
    100615u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1006), process_id: ProcessId(15) },
    100616u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1006), process_id: ProcessId(16) },
    100617u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1006), process_id: ProcessId(17) },
    100690u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1006), process_id: ProcessId(90) },
    100691u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1006), process_id: ProcessId(91) },
    100801u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1008), process_id: ProcessId(1) },
    100802u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1008), process_id: ProcessId(2) },
    100811u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1008), process_id: ProcessId(11) },
    100812u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1008), process_id: ProcessId(12) },
    100813u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1008), process_id: ProcessId(13) },
    100815u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1008), process_id: ProcessId(15) },
    100816u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1008), process_id: ProcessId(16) },
    100817u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1008), process_id: ProcessId(17) },
    100818u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1008), process_id: ProcessId(18) },
    100819u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1008), process_id: ProcessId(19) },
    100890u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1008), process_id: ProcessId(90) },
    100891u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1008), process_id: ProcessId(91) },
    100901u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1009), process_id: ProcessId(1) },
    100902u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1009), process_id: ProcessId(2) },
    100911u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1009), process_id: ProcessId(11) },
    100912u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1009), process_id: ProcessId(12) },
    100913u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1009), process_id: ProcessId(13) },
    100915u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1009), process_id: ProcessId(15) },
    100916u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1009), process_id: ProcessId(16) },
    100917u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1009), process_id: ProcessId(17) },
    100918u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1009), process_id: ProcessId(18) },
    100919u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1009), process_id: ProcessId(19) },
    100990u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1009), process_id: ProcessId(90) },
    100991u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1009), process_id: ProcessId(91) },
    101001u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1010), process_id: ProcessId(1) },
    101002u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1010), process_id: ProcessId(2) },
    101011u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1010), process_id: ProcessId(11) },
    101012u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1010), process_id: ProcessId(12) },
    101013u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1010), process_id: ProcessId(13) },
    101015u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1010), process_id: ProcessId(15) },
    101016u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1010), process_id: ProcessId(16) },
    101017u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1010), process_id: ProcessId(17) },
    101018u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1010), process_id: ProcessId(18) },
    101019u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1010), process_id: ProcessId(19) },
    101090u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1010), process_id: ProcessId(90) },
    101091u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1010), process_id: ProcessId(91) },
    101101u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1011), process_id: ProcessId(1) },
    101102u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1011), process_id: ProcessId(2) },
    101111u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1011), process_id: ProcessId(11) },
    101115u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1011), process_id: ProcessId(15) },
    101116u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1011), process_id: ProcessId(16) },
    101201u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1012), process_id: ProcessId(1) },
    101202u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1012), process_id: ProcessId(2) },
    101211u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1012), process_id: ProcessId(11) },
    101212u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1012), process_id: ProcessId(12) },
    101213u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1012), process_id: ProcessId(13) },
    101215u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1012), process_id: ProcessId(15) },
    101216u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1012), process_id: ProcessId(16) },
    101217u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1012), process_id: ProcessId(17) },
    101218u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1012), process_id: ProcessId(18) },
    101219u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1012), process_id: ProcessId(19) },
    101290u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1012), process_id: ProcessId(90) },
    101291u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1012), process_id: ProcessId(91) },
    101301u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1013), process_id: ProcessId(1) },
    101302u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1013), process_id: ProcessId(2) },
    101311u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1013), process_id: ProcessId(11) },
    101312u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1013), process_id: ProcessId(12) },
    101313u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1013), process_id: ProcessId(13) },
    101315u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1013), process_id: ProcessId(15) },
    101316u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1013), process_id: ProcessId(16) },
    101317u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1013), process_id: ProcessId(17) },
    101318u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1013), process_id: ProcessId(18) },
    101319u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1013), process_id: ProcessId(19) },
    101390u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1013), process_id: ProcessId(90) },
    101391u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1013), process_id: ProcessId(91) },
    101401u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1014), process_id: ProcessId(1) },
    101402u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1014), process_id: ProcessId(2) },
    101411u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1014), process_id: ProcessId(11) },
    101412u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1014), process_id: ProcessId(12) },
    101413u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1014), process_id: ProcessId(13) },
    101415u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1014), process_id: ProcessId(15) },
    101416u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1014), process_id: ProcessId(16) },
    101417u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1014), process_id: ProcessId(17) },
    101418u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1014), process_id: ProcessId(18) },
    101419u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1014), process_id: ProcessId(19) },
    101490u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1014), process_id: ProcessId(90) },
    101491u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1014), process_id: ProcessId(91) },
    101501u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1015), process_id: ProcessId(1) },
    101502u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1015), process_id: ProcessId(2) },
    101511u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1015), process_id: ProcessId(11) },
    101512u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1015), process_id: ProcessId(12) },
    101513u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1015), process_id: ProcessId(13) },
    101515u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1015), process_id: ProcessId(15) },
    101516u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1015), process_id: ProcessId(16) },
    101517u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1015), process_id: ProcessId(17) },
    101518u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1015), process_id: ProcessId(18) },
    101519u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1015), process_id: ProcessId(19) },
    101590u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1015), process_id: ProcessId(90) },
    101591u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1015), process_id: ProcessId(91) },
    101601u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1016), process_id: ProcessId(1) },
    101602u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1016), process_id: ProcessId(2) },
    101615u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1016), process_id: ProcessId(15) },
    101616u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1016), process_id: ProcessId(16) },
    101617u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1016), process_id: ProcessId(17) },
    101690u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1016), process_id: ProcessId(90) },
    101691u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1016), process_id: ProcessId(91) },
    101701u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1017), process_id: ProcessId(1) },
    101702u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1017), process_id: ProcessId(2) },
    101711u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1017), process_id: ProcessId(11) },
    101712u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1017), process_id: ProcessId(12) },
    101713u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1017), process_id: ProcessId(13) },
    101715u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1017), process_id: ProcessId(15) },
    101716u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1017), process_id: ProcessId(16) },
    101717u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1017), process_id: ProcessId(17) },
    101718u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1017), process_id: ProcessId(18) },
    101719u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1017), process_id: ProcessId(19) },
    101790u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1017), process_id: ProcessId(90) },
    101791u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1017), process_id: ProcessId(91) },
    101801u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1018), process_id: ProcessId(1) },
    101802u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1018), process_id: ProcessId(2) },
    101811u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1018), process_id: ProcessId(11) },
    101812u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1018), process_id: ProcessId(12) },
    101813u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1018), process_id: ProcessId(13) },
    101815u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1018), process_id: ProcessId(15) },
    101816u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1018), process_id: ProcessId(16) },
    101817u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1018), process_id: ProcessId(17) },
    101818u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1018), process_id: ProcessId(18) },
    101819u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1018), process_id: ProcessId(19) },
    101890u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1018), process_id: ProcessId(90) },
    101891u32 => PollutantProcessAssociation { pollutant_id: PollutantId(1018), process_id: ProcessId(91) },
};

#[cfg(test)]
mod tests {
    use super::*;

    // Ports the spirit of PollutantProcessAssociationTest.java.

    #[test]
    fn polproc_id_roundtrips_through_compose_and_decompose() {
        let id = PolProcessId::new(PollutantId(2), ProcessId(15));
        assert_eq!(id.0, 215);
        assert_eq!(id.pollutant_id(), PollutantId(2));
        assert_eq!(id.process_id(), ProcessId(15));
    }

    #[test]
    fn polproc_id_handles_four_digit_pollutants() {
        // The CB05 mechanism + Auxiliary Power Exhaust composes to 101891,
        // which exceeds u16::MAX (65535). Verify we don't truncate.
        let id = PolProcessId::new(PollutantId(1018), ProcessId(91));
        assert_eq!(id.0, 101891);
        assert_eq!(id.pollutant_id(), PollutantId(1018));
        assert_eq!(id.process_id(), ProcessId(91));
    }

    #[test]
    fn find_by_polproc_id_returns_canonical_match() {
        // THC + Running Exhaust = 101 is a canonical pair.
        let assoc = PollutantProcessAssociation::find_by_polproc_id(PolProcessId(101))
            .expect("THC + Running Exhaust is canonical");
        assert_eq!(assoc.pollutant_id, PollutantId(1));
        assert_eq!(assoc.process_id, ProcessId(1));
    }

    #[test]
    fn find_by_polproc_id_returns_none_for_illegal_pair() {
        // CO (id 2) + Evap Permeation (id 11) doesn't exist in MOVES —
        // CO is exhaust-only.
        assert!(PollutantProcessAssociation::find_by_polproc_id(PolProcessId(211)).is_none());
    }

    #[test]
    fn find_by_ids_matches_find_by_polproc_id() {
        let by_ids =
            PollutantProcessAssociation::find_by_ids(PollutantId(1), ProcessId(1)).unwrap();
        let by_polproc =
            PollutantProcessAssociation::find_by_polproc_id(PolProcessId(101)).unwrap();
        assert_eq!(by_ids, by_polproc);
    }

    #[test]
    fn find_by_names_resolves_both_components() {
        let by_names = PollutantProcessAssociation::find_by_names(
            "Total Gaseous Hydrocarbons",
            "Running Exhaust",
        )
        .unwrap();
        assert_eq!(by_names.pollutant_id, PollutantId(1));
        assert_eq!(by_names.process_id, ProcessId(1));
    }

    #[test]
    fn find_by_names_is_case_insensitive() {
        let canon =
            PollutantProcessAssociation::find_by_names("Carbon Monoxide (CO)", "Start Exhaust")
                .unwrap();
        let lower =
            PollutantProcessAssociation::find_by_names("carbon monoxide (co)", "start exhaust")
                .unwrap();
        assert_eq!(canon, lower);
    }

    #[test]
    fn find_by_names_returns_none_for_illegal_pair() {
        // Component names resolve, but the pair is not canonical.
        assert!(PollutantProcessAssociation::find_by_names(
            "Carbon Monoxide (CO)",
            "Evap Permeation"
        )
        .is_none());
    }

    #[test]
    fn find_by_names_returns_none_for_unknown_component() {
        assert!(PollutantProcessAssociation::find_by_names("no-such", "Running Exhaust").is_none());
        assert!(PollutantProcessAssociation::find_by_names(
            "Total Gaseous Hydrocarbons",
            "no-such"
        )
        .is_none());
    }

    #[test]
    fn distinct_associations_are_distinguishable() {
        let a = PollutantProcessAssociation::find_by_ids(PollutantId(1), ProcessId(1)).unwrap();
        let b = PollutantProcessAssociation::find_by_ids(PollutantId(2), ProcessId(2)).unwrap();
        let c = PollutantProcessAssociation::find_by_ids(PollutantId(1), ProcessId(2)).unwrap();
        assert_ne!(a, b);
        assert_ne!(a, c);
        assert_ne!(b, c);
    }

    #[test]
    fn all_canonical_polproc_ids_decompose_round_trip() {
        // Every canonical pair must survive `polproc_id().pollutant_id()` /
        // `.process_id()` round-trips with no loss.
        for assoc in PollutantProcessAssociation::all() {
            let id = assoc.polproc_id();
            assert_eq!(id.pollutant_id(), assoc.pollutant_id);
            assert_eq!(id.process_id(), assoc.process_id);
            assert_eq!(
                PollutantProcessAssociation::find_by_polproc_id(id),
                Some(assoc)
            );
        }
    }

    #[test]
    fn polproc_id_parses_from_str() {
        assert_eq!("215".parse::<PolProcessId>().unwrap(), PolProcessId(215));
        assert!("not-a-number".parse::<PolProcessId>().is_err());
    }

    #[test]
    fn display_renders_process_then_pollutant() {
        let assoc = PollutantProcessAssociation::find_by_ids(PollutantId(1), ProcessId(1)).unwrap();
        // Java's toString: `emissionProcess.toString() + " " + pollutant.toString()`.
        assert_eq!(
            assoc.to_string(),
            "Running Exhaust Total Gaseous Hydrocarbons"
        );
    }
}
