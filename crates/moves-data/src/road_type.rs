//! [`RoadType`] type plus canonical `phf` lookup tables.
//!
//! Ports `gov/epa/otaq/moves/master/framework/RoadType.java`. MOVES models
//! emissions across five road types; the data is small enough that all
//! canonical entries live directly here.
//!
//! Names match the `roadDesc` column of the default-DB `RoadType` table.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

use crate::error::Error;

/// MOVES road-type primary key (`RoadType.roadTypeID`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
#[serde(transparent)]
pub struct RoadTypeId(pub u16);

impl fmt::Display for RoadTypeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<u16> for RoadTypeId {
    fn from(value: u16) -> Self {
        Self(value)
    }
}

impl From<RoadTypeId> for u16 {
    fn from(value: RoadTypeId) -> Self {
        value.0
    }
}

impl FromStr for RoadTypeId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<u16>().map(Self).map_err(|source| Error::ParseId {
            kind: "road type",
            input: s.to_owned(),
            source,
        })
    }
}

/// Canonical identity of a MOVES road type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RoadType {
    /// Database key (`roadTypeID`).
    pub id: RoadTypeId,
    /// Display name (`roadDesc` in the default DB).
    pub name: &'static str,
}

impl RoadType {
    /// Look up the canonical road type with the given id.
    #[must_use]
    pub fn find_by_id(id: RoadTypeId) -> Option<Self> {
        BY_ID.get(&id.0).copied()
    }

    /// Look up the canonical road type by name (case-insensitive ASCII).
    ///
    /// Java `RoadType.findByName` is name-only — it does not fall back to
    /// matching the numeric id as text, unlike `Pollutant.findByName` and
    /// `EmissionProcess.findByName`.
    #[must_use]
    pub fn find_by_name(name: &str) -> Option<Self> {
        let key = name.to_ascii_lowercase();
        BY_NAME_LOWER.get(key.as_str()).copied()
    }

    /// Iterate every canonical road type in ascending-id order.
    pub fn all() -> impl Iterator<Item = Self> {
        ALL_ROAD_TYPES.iter().copied()
    }
}

impl fmt::Display for RoadType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name)
    }
}

const ROAD_TYPE_COUNT: usize = 5;

/// All canonical road types, sorted by id.
pub static ALL_ROAD_TYPES: [RoadType; ROAD_TYPE_COUNT] = [
    RoadType {
        id: RoadTypeId(1),
        name: "Off-Network",
    },
    RoadType {
        id: RoadTypeId(2),
        name: "Rural Restricted Access",
    },
    RoadType {
        id: RoadTypeId(3),
        name: "Rural Unrestricted Access",
    },
    RoadType {
        id: RoadTypeId(4),
        name: "Urban Restricted Access",
    },
    RoadType {
        id: RoadTypeId(5),
        name: "Urban Unrestricted Access",
    },
];

static BY_ID: phf::Map<u16, RoadType> = phf::phf_map! {
    1u16 => RoadType { id: RoadTypeId(1), name: "Off-Network" },
    2u16 => RoadType { id: RoadTypeId(2), name: "Rural Restricted Access" },
    3u16 => RoadType { id: RoadTypeId(3), name: "Rural Unrestricted Access" },
    4u16 => RoadType { id: RoadTypeId(4), name: "Urban Restricted Access" },
    5u16 => RoadType { id: RoadTypeId(5), name: "Urban Unrestricted Access" },
};

static BY_NAME_LOWER: phf::Map<&'static str, RoadType> = phf::phf_map! {
    "off-network" => RoadType { id: RoadTypeId(1), name: "Off-Network" },
    "rural restricted access" => RoadType { id: RoadTypeId(2), name: "Rural Restricted Access" },
    "rural unrestricted access" => RoadType { id: RoadTypeId(3), name: "Rural Unrestricted Access" },
    "urban restricted access" => RoadType { id: RoadTypeId(4), name: "Urban Restricted Access" },
    "urban unrestricted access" => RoadType { id: RoadTypeId(5), name: "Urban Unrestricted Access" },
};

#[cfg(test)]
mod tests {
    use super::*;

    // Ports the spirit of RoadTypeTest.java.

    #[test]
    fn find_by_name_returns_canonical_match() {
        let off_network = RoadType::find_by_name("Off-Network").unwrap();
        assert_eq!(off_network.id, RoadTypeId(1));
    }

    #[test]
    fn find_by_name_is_case_insensitive() {
        let canon = RoadType::find_by_name("Urban Restricted Access").unwrap();
        let lower = RoadType::find_by_name("urban restricted access").unwrap();
        assert_eq!(canon, lower);
        assert_eq!(canon.id, RoadTypeId(4));
    }

    #[test]
    fn find_by_name_returns_none_for_unknown() {
        // RoadTypeTest.java checks that `findByName(" ")` returns null;
        // our equivalent is `find_by_name(" ")`.
        assert!(RoadType::find_by_name(" ").is_none());
        assert!(RoadType::find_by_name("type1").is_none());
    }

    #[test]
    fn find_by_name_does_not_accept_numeric_id() {
        // Java's RoadType.findByName is name-only, unlike Pollutant /
        // EmissionProcess. "1" must not resolve to Off-Network.
        assert!(RoadType::find_by_name("1").is_none());
    }

    #[test]
    fn all_iter_returns_every_canonical_entry_in_id_order() {
        let ids: Vec<u16> = RoadType::all().map(|r| r.id.0).collect();
        assert_eq!(ids, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn distinct_road_types_are_distinguishable() {
        let rt1 = RoadType::find_by_name("Off-Network").unwrap();
        let rt2 = RoadType::find_by_name("Rural Restricted Access").unwrap();
        assert_ne!(rt1, rt2);
        assert!(rt1.cmp(&rt2) != std::cmp::Ordering::Equal);
    }

    #[test]
    fn by_id_and_by_name_agree() {
        for r in RoadType::all() {
            assert_eq!(RoadType::find_by_id(r.id), Some(r));
            assert_eq!(RoadType::find_by_name(r.name), Some(r));
        }
    }
}
