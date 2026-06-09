//! [`SourceType`] type plus canonical `phf` lookup tables.
//!
//! MOVES does not ship a `SourceType.java` class — `sourceTypeID` is an
//! `int` field threaded through `OnRoadVehicleSelection` and friends, with
//! the catalog of legal values living in the default-DB `SourceUseType`
//! table. This module gives that catalog a proper Rust home so RunSpec
//! parsing, validation, and downstream calculator dispatch share one
//! definition.
//!
//! Names match the `sourceTypeName` column of the default-DB `SourceUseType`
//! table.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

use crate::error::Error;

/// MOVES source-type primary key (`SourceUseType.sourceTypeID`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
#[serde(transparent)]
pub struct SourceTypeId(pub u16);

impl fmt::Display for SourceTypeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<u16> for SourceTypeId {
    fn from(value: u16) -> Self {
        Self(value)
    }
}

impl From<SourceTypeId> for u16 {
    fn from(value: SourceTypeId) -> Self {
        value.0
    }
}

impl FromStr for SourceTypeId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<u16>().map(Self).map_err(|source| Error::ParseId {
            kind: "source type",
            input: s.to_owned(),
            source,
        })
    }
}

/// Canonical identity of a MOVES on-road source type (HPMS vehicle category).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SourceType {
    /// Database key (`sourceTypeID`).
    pub id: SourceTypeId,
    /// Display name (`sourceTypeName` in the default DB).
    pub name: &'static str,
}

impl SourceType {
    /// Look up the canonical source type with the given id.
    #[must_use]
    pub fn find_by_id(id: SourceTypeId) -> Option<Self> {
        BY_ID.get(&id.0).copied()
    }

    /// Look up the canonical source type by name (case-insensitive ASCII).
    ///
    /// Java MOVES doesn't ship a `SourceType.findByName` to mirror, so we
    /// follow the [`RoadType::find_by_name`](crate::RoadType::find_by_name)
    /// pattern: pure name matching, no numeric-id fallback.
    #[must_use]
    pub fn find_by_name(name: &str) -> Option<Self> {
        let key = name.to_ascii_lowercase();
        BY_NAME_LOWER.get(key.as_str()).copied()
    }

    /// Iterate every canonical source type in ascending-id order.
    pub fn all() -> impl Iterator<Item = Self> {
        ALL_SOURCE_TYPES.iter().copied()
    }
}

impl fmt::Display for SourceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name)
    }
}

const SOURCE_TYPE_COUNT: usize = 13;

/// All canonical source types, sorted by id.
pub static ALL_SOURCE_TYPES: [SourceType; SOURCE_TYPE_COUNT] = [
    SourceType {
        id: SourceTypeId(11),
        name: "Motorcycle",
    },
    SourceType {
        id: SourceTypeId(21),
        name: "Passenger Car",
    },
    SourceType {
        id: SourceTypeId(31),
        name: "Passenger Truck",
    },
    SourceType {
        id: SourceTypeId(32),
        name: "Light Commercial Truck",
    },
    SourceType {
        id: SourceTypeId(41),
        name: "Other Buses",
    },
    SourceType {
        id: SourceTypeId(42),
        name: "Transit Bus",
    },
    SourceType {
        id: SourceTypeId(43),
        name: "School Bus",
    },
    SourceType {
        id: SourceTypeId(51),
        name: "Refuse Truck",
    },
    SourceType {
        id: SourceTypeId(52),
        name: "Single Unit Short-haul Truck",
    },
    SourceType {
        id: SourceTypeId(53),
        name: "Single Unit Long-haul Truck",
    },
    SourceType {
        id: SourceTypeId(54),
        name: "Motor Home",
    },
    SourceType {
        id: SourceTypeId(61),
        name: "Combination Short-haul Truck",
    },
    SourceType {
        id: SourceTypeId(62),
        name: "Combination Long-haul Truck",
    },
];

static BY_ID: phf::Map<u16, SourceType> = phf::phf_map! {
    11u16 => SourceType { id: SourceTypeId(11), name: "Motorcycle" },
    21u16 => SourceType { id: SourceTypeId(21), name: "Passenger Car" },
    31u16 => SourceType { id: SourceTypeId(31), name: "Passenger Truck" },
    32u16 => SourceType { id: SourceTypeId(32), name: "Light Commercial Truck" },
    41u16 => SourceType { id: SourceTypeId(41), name: "Other Buses" },
    42u16 => SourceType { id: SourceTypeId(42), name: "Transit Bus" },
    43u16 => SourceType { id: SourceTypeId(43), name: "School Bus" },
    51u16 => SourceType { id: SourceTypeId(51), name: "Refuse Truck" },
    52u16 => SourceType { id: SourceTypeId(52), name: "Single Unit Short-haul Truck" },
    53u16 => SourceType { id: SourceTypeId(53), name: "Single Unit Long-haul Truck" },
    54u16 => SourceType { id: SourceTypeId(54), name: "Motor Home" },
    61u16 => SourceType { id: SourceTypeId(61), name: "Combination Short-haul Truck" },
    62u16 => SourceType { id: SourceTypeId(62), name: "Combination Long-haul Truck" },
};

static BY_NAME_LOWER: phf::Map<&'static str, SourceType> = phf::phf_map! {
    "motorcycle" => SourceType { id: SourceTypeId(11), name: "Motorcycle" },
    "passenger car" => SourceType { id: SourceTypeId(21), name: "Passenger Car" },
    "passenger truck" => SourceType { id: SourceTypeId(31), name: "Passenger Truck" },
    "light commercial truck" => SourceType { id: SourceTypeId(32), name: "Light Commercial Truck" },
    "other buses" => SourceType { id: SourceTypeId(41), name: "Other Buses" },
    "transit bus" => SourceType { id: SourceTypeId(42), name: "Transit Bus" },
    "school bus" => SourceType { id: SourceTypeId(43), name: "School Bus" },
    "refuse truck" => SourceType { id: SourceTypeId(51), name: "Refuse Truck" },
    "single unit short-haul truck" => SourceType { id: SourceTypeId(52), name: "Single Unit Short-haul Truck" },
    "single unit long-haul truck" => SourceType { id: SourceTypeId(53), name: "Single Unit Long-haul Truck" },
    "motor home" => SourceType { id: SourceTypeId(54), name: "Motor Home" },
    "combination short-haul truck" => SourceType { id: SourceTypeId(61), name: "Combination Short-haul Truck" },
    "combination long-haul truck" => SourceType { id: SourceTypeId(62), name: "Combination Long-haul Truck" },
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_by_id_returns_canonical_match() {
        let pc = SourceType::find_by_id(SourceTypeId(21)).unwrap();
        assert_eq!(pc.name, "Passenger Car");
    }

    #[test]
    fn find_by_name_is_case_insensitive() {
        let canon = SourceType::find_by_name("Motorcycle").unwrap();
        let lower = SourceType::find_by_name("motorcycle").unwrap();
        let upper = SourceType::find_by_name("MOTORCYCLE").unwrap();
        assert_eq!(canon, lower);
        assert_eq!(canon, upper);
    }

    #[test]
    fn find_by_name_returns_none_for_unknown() {
        assert!(SourceType::find_by_name("Spaceship").is_none());
    }

    #[test]
    fn find_by_name_does_not_accept_numeric_id() {
        // Matches the RoadType / Java convention: name-only lookup.
        assert!(SourceType::find_by_name("21").is_none());
    }

    #[test]
    fn all_iter_returns_every_canonical_entry_in_id_order() {
        let ids: Vec<u16> = SourceType::all().map(|s| s.id.0).collect();
        assert_eq!(
            ids,
            vec![11, 21, 31, 32, 41, 42, 43, 51, 52, 53, 54, 61, 62]
        );
    }

    #[test]
    fn by_id_and_by_name_agree() {
        for s in SourceType::all() {
            assert_eq!(SourceType::find_by_id(s.id), Some(s));
            assert_eq!(SourceType::find_by_name(s.name), Some(s));
        }
    }
}
