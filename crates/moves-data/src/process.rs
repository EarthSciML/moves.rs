//! [`EmissionProcess`] type plus canonical `phf` lookup tables.
//!
//! Ports `gov/epa/otaq/moves/master/framework/EmissionProcess.java`. As
//! with [`crate::pollutant`], the mutable Java registry collapses into
//! compile-time `phf` maps.
//!
//! Runtime metadata not stored on the static records (`occursOnRealRoads`,
//! `processDisplayGroupID`, `isAffectedByOnroad`, `isAffectedByNonroad`,
//! `SCCProcID`) lives in the data plane.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

use crate::error::Error;

/// MOVES emission-process primary key (`EmissionProcess.processID`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
#[serde(transparent)]
pub struct ProcessId(pub u16);

impl fmt::Display for ProcessId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<u16> for ProcessId {
    fn from(value: u16) -> Self {
        Self(value)
    }
}

impl From<ProcessId> for u16 {
    fn from(value: ProcessId) -> Self {
        value.0
    }
}

impl FromStr for ProcessId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<u16>().map(Self).map_err(|source| Error::ParseId {
            kind: "process",
            input: s.to_owned(),
            source,
        })
    }
}

/// Canonical identity of a MOVES emission process.
///
/// Mirrors the `EmissionProcess.java` (id, name) pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EmissionProcess {
    /// Database key (`processID`).
    pub id: ProcessId,
    /// Display name as it appears in the default DB.
    pub name: &'static str,
}

impl EmissionProcess {
    /// Look up the canonical process with the given id.
    #[must_use]
    pub fn find_by_id(id: ProcessId) -> Option<Self> {
        BY_ID.get(&id.0).copied()
    }

    /// Look up the canonical process with the given name.
    ///
    /// Case-insensitive (ASCII), with the same numeric-id fallback as
    /// [`crate::Pollutant::find_by_name`].
    #[must_use]
    pub fn find_by_name(name: &str) -> Option<Self> {
        let key = name.to_ascii_lowercase();
        if let Some(hit) = BY_NAME_LOWER.get(key.as_str()).copied() {
            return Some(hit);
        }
        name.parse::<u16>()
            .ok()
            .and_then(|n| Self::find_by_id(ProcessId(n)))
    }

    /// Iterate every canonical process in ascending-id order.
    pub fn all() -> impl Iterator<Item = Self> {
        ALL_PROCESSES.iter().copied()
    }
}

impl fmt::Display for EmissionProcess {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name)
    }
}

const PROCESS_COUNT: usize = 22;

pub static ALL_PROCESSES: [EmissionProcess; PROCESS_COUNT] = [
    EmissionProcess {
        id: ProcessId(1),
        name: "Running Exhaust",
    },
    EmissionProcess {
        id: ProcessId(2),
        name: "Start Exhaust",
    },
    EmissionProcess {
        id: ProcessId(9),
        name: "Brakewear",
    },
    EmissionProcess {
        id: ProcessId(10),
        name: "Tirewear",
    },
    EmissionProcess {
        id: ProcessId(11),
        name: "Evap Permeation",
    },
    EmissionProcess {
        id: ProcessId(12),
        name: "Evap Fuel Vapor Venting",
    },
    EmissionProcess {
        id: ProcessId(13),
        name: "Evap Fuel Leaks",
    },
    EmissionProcess {
        id: ProcessId(15),
        name: "Crankcase Running Exhaust",
    },
    EmissionProcess {
        id: ProcessId(16),
        name: "Crankcase Start Exhaust",
    },
    EmissionProcess {
        id: ProcessId(17),
        name: "Crankcase Extended Idle Exhaust",
    },
    EmissionProcess {
        id: ProcessId(18),
        name: "Refueling Displacement Vapor Loss",
    },
    EmissionProcess {
        id: ProcessId(19),
        name: "Refueling Spillage Loss",
    },
    EmissionProcess {
        id: ProcessId(20),
        name: "Evap Tank Permeation",
    },
    EmissionProcess {
        id: ProcessId(21),
        name: "Evap Hose Permeation",
    },
    EmissionProcess {
        id: ProcessId(22),
        name: "Evap RecMar Neck Hose Permeation",
    },
    EmissionProcess {
        id: ProcessId(23),
        name: "Evap RecMar Supply/Ret Hose Permeation",
    },
    EmissionProcess {
        id: ProcessId(24),
        name: "Evap RecMar Vent Hose Permeation",
    },
    EmissionProcess {
        id: ProcessId(30),
        name: "Diurnal Fuel Vapor Venting",
    },
    EmissionProcess {
        id: ProcessId(31),
        name: "HotSoak Fuel Vapor Venting",
    },
    EmissionProcess {
        id: ProcessId(32),
        name: "RunningLoss Fuel Vapor Venting",
    },
    EmissionProcess {
        id: ProcessId(90),
        name: "Extended Idle Exhaust",
    },
    EmissionProcess {
        id: ProcessId(91),
        name: "Auxiliary Power Exhaust",
    },
];

static BY_ID: phf::Map<u16, EmissionProcess> = phf::phf_map! {
    1u16 => EmissionProcess { id: ProcessId(1), name: "Running Exhaust" },
    2u16 => EmissionProcess { id: ProcessId(2), name: "Start Exhaust" },
    9u16 => EmissionProcess { id: ProcessId(9), name: "Brakewear" },
    10u16 => EmissionProcess { id: ProcessId(10), name: "Tirewear" },
    11u16 => EmissionProcess { id: ProcessId(11), name: "Evap Permeation" },
    12u16 => EmissionProcess { id: ProcessId(12), name: "Evap Fuel Vapor Venting" },
    13u16 => EmissionProcess { id: ProcessId(13), name: "Evap Fuel Leaks" },
    15u16 => EmissionProcess { id: ProcessId(15), name: "Crankcase Running Exhaust" },
    16u16 => EmissionProcess { id: ProcessId(16), name: "Crankcase Start Exhaust" },
    17u16 => EmissionProcess { id: ProcessId(17), name: "Crankcase Extended Idle Exhaust" },
    18u16 => EmissionProcess { id: ProcessId(18), name: "Refueling Displacement Vapor Loss" },
    19u16 => EmissionProcess { id: ProcessId(19), name: "Refueling Spillage Loss" },
    20u16 => EmissionProcess { id: ProcessId(20), name: "Evap Tank Permeation" },
    21u16 => EmissionProcess { id: ProcessId(21), name: "Evap Hose Permeation" },
    22u16 => EmissionProcess { id: ProcessId(22), name: "Evap RecMar Neck Hose Permeation" },
    23u16 => EmissionProcess { id: ProcessId(23), name: "Evap RecMar Supply/Ret Hose Permeation" },
    24u16 => EmissionProcess { id: ProcessId(24), name: "Evap RecMar Vent Hose Permeation" },
    30u16 => EmissionProcess { id: ProcessId(30), name: "Diurnal Fuel Vapor Venting" },
    31u16 => EmissionProcess { id: ProcessId(31), name: "HotSoak Fuel Vapor Venting" },
    32u16 => EmissionProcess { id: ProcessId(32), name: "RunningLoss Fuel Vapor Venting" },
    90u16 => EmissionProcess { id: ProcessId(90), name: "Extended Idle Exhaust" },
    91u16 => EmissionProcess { id: ProcessId(91), name: "Auxiliary Power Exhaust" },
};

static BY_NAME_LOWER: phf::Map<&'static str, EmissionProcess> = phf::phf_map! {
    "running exhaust" => EmissionProcess { id: ProcessId(1), name: "Running Exhaust" },
    "start exhaust" => EmissionProcess { id: ProcessId(2), name: "Start Exhaust" },
    "brakewear" => EmissionProcess { id: ProcessId(9), name: "Brakewear" },
    "tirewear" => EmissionProcess { id: ProcessId(10), name: "Tirewear" },
    "evap permeation" => EmissionProcess { id: ProcessId(11), name: "Evap Permeation" },
    "evap fuel vapor venting" => EmissionProcess { id: ProcessId(12), name: "Evap Fuel Vapor Venting" },
    "evap fuel leaks" => EmissionProcess { id: ProcessId(13), name: "Evap Fuel Leaks" },
    "crankcase running exhaust" => EmissionProcess { id: ProcessId(15), name: "Crankcase Running Exhaust" },
    "crankcase start exhaust" => EmissionProcess { id: ProcessId(16), name: "Crankcase Start Exhaust" },
    "crankcase extended idle exhaust" => EmissionProcess { id: ProcessId(17), name: "Crankcase Extended Idle Exhaust" },
    "refueling displacement vapor loss" => EmissionProcess { id: ProcessId(18), name: "Refueling Displacement Vapor Loss" },
    "refueling spillage loss" => EmissionProcess { id: ProcessId(19), name: "Refueling Spillage Loss" },
    "evap tank permeation" => EmissionProcess { id: ProcessId(20), name: "Evap Tank Permeation" },
    "evap hose permeation" => EmissionProcess { id: ProcessId(21), name: "Evap Hose Permeation" },
    "evap recmar neck hose permeation" => EmissionProcess { id: ProcessId(22), name: "Evap RecMar Neck Hose Permeation" },
    "evap recmar supply/ret hose permeation" => EmissionProcess { id: ProcessId(23), name: "Evap RecMar Supply/Ret Hose Permeation" },
    "evap recmar vent hose permeation" => EmissionProcess { id: ProcessId(24), name: "Evap RecMar Vent Hose Permeation" },
    "diurnal fuel vapor venting" => EmissionProcess { id: ProcessId(30), name: "Diurnal Fuel Vapor Venting" },
    "hotsoak fuel vapor venting" => EmissionProcess { id: ProcessId(31), name: "HotSoak Fuel Vapor Venting" },
    "runningloss fuel vapor venting" => EmissionProcess { id: ProcessId(32), name: "RunningLoss Fuel Vapor Venting" },
    "extended idle exhaust" => EmissionProcess { id: ProcessId(90), name: "Extended Idle Exhaust" },
    "auxiliary power exhaust" => EmissionProcess { id: ProcessId(91), name: "Auxiliary Power Exhaust" },
};

#[cfg(test)]
mod tests {
    use super::*;

    // Ports the spirit of EmissionProcessTest.java.

    #[test]
    fn find_by_id_returns_canonical_match() {
        let running = EmissionProcess::find_by_id(ProcessId(1)).unwrap();
        assert_eq!(running.id, ProcessId(1));
        assert_eq!(running.name, "Running Exhaust");
    }

    #[test]
    fn find_by_id_returns_none_for_unknown() {
        assert!(EmissionProcess::find_by_id(ProcessId(7777)).is_none());
    }

    #[test]
    fn find_by_name_is_case_insensitive() {
        let canon = EmissionProcess::find_by_name("Start Exhaust").unwrap();
        let lower = EmissionProcess::find_by_name("start exhaust").unwrap();
        let upper = EmissionProcess::find_by_name("START EXHAUST").unwrap();
        assert_eq!(canon, lower);
        assert_eq!(canon, upper);
        assert_eq!(canon.id, ProcessId(2));
    }

    #[test]
    fn find_by_name_accepts_numeric_id_fallback() {
        let by_name = EmissionProcess::find_by_name("Running Exhaust").unwrap();
        let by_numeric = EmissionProcess::find_by_name("1").unwrap();
        assert_eq!(by_name, by_numeric);
    }

    #[test]
    fn find_by_name_returns_none_for_unknown() {
        assert!(EmissionProcess::find_by_name("this does not exist").is_none());
    }

    #[test]
    fn distinct_processes_are_distinguishable() {
        let p2 = EmissionProcess::find_by_name("Start Exhaust").unwrap();
        let p3 = EmissionProcess::find_by_name("Brakewear").unwrap();
        assert_ne!(p2, p3);
        assert!(p2.cmp(&p3) != std::cmp::Ordering::Equal);
    }

    #[test]
    fn all_iter_returns_every_canonical_entry_in_id_order() {
        let ids: Vec<u16> = EmissionProcess::all().map(|p| p.id.0).collect();
        assert_eq!(ids.len(), ALL_PROCESSES.len());
        assert!(ids.windows(2).all(|w| w[0] < w[1]));
    }

    #[test]
    fn by_id_and_by_name_agree_on_every_canonical_entry() {
        for p in EmissionProcess::all() {
            assert_eq!(EmissionProcess::find_by_id(p.id), Some(p));
            assert_eq!(EmissionProcess::find_by_name(p.name), Some(p));
        }
    }
}
