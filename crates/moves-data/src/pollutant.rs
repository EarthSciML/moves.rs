//! [`Pollutant`] type plus canonical `phf` lookup tables.
//!
//! Ports `gov/epa/otaq/moves/master/framework/Pollutant.java`. The Java class
//! is a mutable global registry seeded from the `Pollutant` default-DB table
//! at startup; the Rust port replaces that with compile-time `phf` maps over
//! the canonical 5.0.1 entries.
//!
//! Runtime metadata not stored on the static records (display group,
//! `isAffectedByOnroad`/`Nonroad`, etc.) lives in the data plane and is
//! reconciled at runtime by `moves-framework` (Task 50/89).

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

use crate::error::Error;

/// MOVES pollutant primary key (`Pollutant.pollutantID`).
///
/// Stable across MOVES releases. The default DB declares this column as
/// `smallint`; we widen to `u16` so `0..=u16::MAX` is representable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
#[serde(transparent)]
pub struct PollutantId(pub u16);

impl fmt::Display for PollutantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<u16> for PollutantId {
    fn from(value: u16) -> Self {
        Self(value)
    }
}

impl From<PollutantId> for u16 {
    fn from(value: PollutantId) -> Self {
        value.0
    }
}

impl FromStr for PollutantId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<u16>().map(Self).map_err(|source| Error::ParseId {
            kind: "pollutant",
            input: s.to_owned(),
            source,
        })
    }
}

/// Canonical identity of a MOVES pollutant.
///
/// Mirrors the `Pollutant.java` (id, name) pair. Other metadata
/// (`energyOrMass`, `pollutantDisplayGroupID`, `isAffectedByOnroad`,
/// `isAffectedByNonroad`, `GlobalWarmingPotential`, `NEIPollutantCode`)
/// is runtime data and lives in the default-DB Parquet snapshot rather
/// than on this static record.
///
/// Construct via [`Pollutant::find_by_id`] or [`Pollutant::find_by_name`];
/// the fields are public so consumers can pattern-match.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Pollutant {
    /// Database key (`pollutantID`).
    pub id: PollutantId,
    /// Display name as it appears in the default DB.
    pub name: &'static str,
}

impl Pollutant {
    /// Look up the canonical pollutant with the given id.
    ///
    /// Returns `None` if no canonical pollutant has the id.
    #[must_use]
    pub fn find_by_id(id: PollutantId) -> Option<Self> {
        BY_ID.get(&id.0).copied()
    }

    /// Look up the canonical pollutant with the given name.
    ///
    /// Case-insensitive (ASCII). Also accepts a numeric id encoded as a
    /// string — `"1"` resolves to "Total Gaseous Hydrocarbons" — matching
    /// the Java [`Pollutant.findByName`] fallback that lets RunSpec text
    /// fields carry either form.
    ///
    /// [`Pollutant.findByName`]: https://github.com/USEPA/EPA_MOVES_Model/blob/main/gov/epa/otaq/moves/master/framework/Pollutant.java
    #[must_use]
    pub fn find_by_name(name: &str) -> Option<Self> {
        let key = name.to_ascii_lowercase();
        if let Some(hit) = BY_NAME_LOWER.get(key.as_str()).copied() {
            return Some(hit);
        }
        // Java accepts the numeric id as a name too — `"1"` is a valid lookup.
        name.parse::<u16>()
            .ok()
            .and_then(|n| Self::find_by_id(PollutantId(n)))
    }

    /// Iterate every canonical pollutant in ascending-id order.
    pub fn all() -> impl Iterator<Item = Self> {
        ALL_POLLUTANTS.iter().copied()
    }
}

impl fmt::Display for Pollutant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name)
    }
}

const POLLUTANT_COUNT: usize = 131;

/// Array of all canonical pollutants, sorted by ID.
pub static ALL_POLLUTANTS: [Pollutant; POLLUTANT_COUNT] = [
    Pollutant {
        id: PollutantId(1),
        name: "Total Gaseous Hydrocarbons",
    },
    Pollutant {
        id: PollutantId(2),
        name: "Carbon Monoxide (CO)",
    },
    Pollutant {
        id: PollutantId(3),
        name: "Oxides of Nitrogen (NOx)",
    },
    Pollutant {
        id: PollutantId(5),
        name: "Methane (CH4)",
    },
    Pollutant {
        id: PollutantId(6),
        name: "Nitrous Oxide (N2O)",
    },
    Pollutant {
        id: PollutantId(20),
        name: "Benzene",
    },
    Pollutant {
        id: PollutantId(21),
        name: "Ethanol",
    },
    Pollutant {
        id: PollutantId(22),
        name: "MTBE",
    },
    Pollutant {
        id: PollutantId(23),
        name: "Naphthalene particle",
    },
    Pollutant {
        id: PollutantId(24),
        name: "1,3-Butadiene",
    },
    Pollutant {
        id: PollutantId(25),
        name: "Formaldehyde",
    },
    Pollutant {
        id: PollutantId(26),
        name: "Acetaldehyde",
    },
    Pollutant {
        id: PollutantId(27),
        name: "Acrolein",
    },
    Pollutant {
        id: PollutantId(30),
        name: "Ammonia (NH3)",
    },
    Pollutant {
        id: PollutantId(31),
        name: "Sulfur Dioxide (SO2)",
    },
    Pollutant {
        id: PollutantId(32),
        name: "Nitrogen Oxide (NO)",
    },
    Pollutant {
        id: PollutantId(33),
        name: "Nitrogen Dioxide (NO2)",
    },
    Pollutant {
        id: PollutantId(34),
        name: "Nitrous Acid (HONO)",
    },
    Pollutant {
        id: PollutantId(35),
        name: "Nitrate (NO3)",
    },
    Pollutant {
        id: PollutantId(36),
        name: "Ammonium (NH4)",
    },
    Pollutant {
        id: PollutantId(40),
        name: "2,2,4-Trimethylpentane",
    },
    Pollutant {
        id: PollutantId(41),
        name: "Ethyl Benzene",
    },
    Pollutant {
        id: PollutantId(42),
        name: "Hexane",
    },
    Pollutant {
        id: PollutantId(43),
        name: "Propionaldehyde",
    },
    Pollutant {
        id: PollutantId(44),
        name: "Styrene",
    },
    Pollutant {
        id: PollutantId(45),
        name: "Toluene",
    },
    Pollutant {
        id: PollutantId(46),
        name: "Xylene",
    },
    Pollutant {
        id: PollutantId(51),
        name: "Chloride",
    },
    Pollutant {
        id: PollutantId(52),
        name: "Sodium",
    },
    Pollutant {
        id: PollutantId(53),
        name: "Potassium",
    },
    Pollutant {
        id: PollutantId(54),
        name: "Magnesium",
    },
    Pollutant {
        id: PollutantId(55),
        name: "Calcium",
    },
    Pollutant {
        id: PollutantId(56),
        name: "Titanium",
    },
    Pollutant {
        id: PollutantId(57),
        name: "Silicon",
    },
    Pollutant {
        id: PollutantId(58),
        name: "Aluminum",
    },
    Pollutant {
        id: PollutantId(59),
        name: "Iron",
    },
    Pollutant {
        id: PollutantId(60),
        name: "Mercury Elemental Gaseous",
    },
    Pollutant {
        id: PollutantId(61),
        name: "Mercury Divalent Gaseous",
    },
    Pollutant {
        id: PollutantId(62),
        name: "Mercury Particulate",
    },
    Pollutant {
        id: PollutantId(63),
        name: "Arsenic Compounds",
    },
    Pollutant {
        id: PollutantId(65),
        name: "Chromium 6+",
    },
    Pollutant {
        id: PollutantId(66),
        name: "Manganese Compounds",
    },
    Pollutant {
        id: PollutantId(67),
        name: "Nickel Compounds",
    },
    Pollutant {
        id: PollutantId(68),
        name: "Dibenzo(a,h)anthracene particle",
    },
    Pollutant {
        id: PollutantId(69),
        name: "Fluoranthene particle",
    },
    Pollutant {
        id: PollutantId(70),
        name: "Acenaphthene particle",
    },
    Pollutant {
        id: PollutantId(71),
        name: "Acenaphthylene particle",
    },
    Pollutant {
        id: PollutantId(72),
        name: "Anthracene particle",
    },
    Pollutant {
        id: PollutantId(73),
        name: "Benz(a)anthracene particle",
    },
    Pollutant {
        id: PollutantId(74),
        name: "Benzo(a)pyrene particle",
    },
    Pollutant {
        id: PollutantId(75),
        name: "Benzo(b)fluoranthene particle",
    },
    Pollutant {
        id: PollutantId(76),
        name: "Benzo(g,h,i)perylene particle",
    },
    Pollutant {
        id: PollutantId(77),
        name: "Benzo(k)fluoranthene particle",
    },
    Pollutant {
        id: PollutantId(78),
        name: "Chrysene particle",
    },
    Pollutant {
        id: PollutantId(79),
        name: "Non-Methane Hydrocarbons",
    },
    Pollutant {
        id: PollutantId(80),
        name: "Non-Methane Organic Gases",
    },
    Pollutant {
        id: PollutantId(81),
        name: "Fluorene particle",
    },
    Pollutant {
        id: PollutantId(82),
        name: "Indeno(1,2,3,c,d)pyrene particle",
    },
    Pollutant {
        id: PollutantId(83),
        name: "Phenanthrene particle",
    },
    Pollutant {
        id: PollutantId(84),
        name: "Pyrene particle",
    },
    Pollutant {
        id: PollutantId(86),
        name: "Total Organic Gases",
    },
    Pollutant {
        id: PollutantId(87),
        name: "Volatile Organic Compounds",
    },
    Pollutant {
        id: PollutantId(88),
        name: "NonHAPTOG",
    },
    Pollutant {
        id: PollutantId(90),
        name: "Atmospheric CO2",
    },
    Pollutant {
        id: PollutantId(91),
        name: "Total Energy Consumption",
    },
    Pollutant {
        id: PollutantId(92),
        name: "Petroleum Energy Consumption",
    },
    Pollutant {
        id: PollutantId(93),
        name: "Fossil Fuel Energy Consumption",
    },
    Pollutant {
        id: PollutantId(98),
        name: "CO2 Equivalent",
    },
    Pollutant {
        id: PollutantId(99),
        name: "Brake Specific Fuel Consumption (BSFC)",
    },
    Pollutant {
        id: PollutantId(100),
        name: "Primary Exhaust PM10  - Total",
    },
    Pollutant {
        id: PollutantId(106),
        name: "Primary PM10 - Brakewear Particulate",
    },
    Pollutant {
        id: PollutantId(107),
        name: "Primary PM10 - Tirewear Particulate",
    },
    Pollutant {
        id: PollutantId(110),
        name: "Primary Exhaust PM2.5 - Total",
    },
    Pollutant {
        id: PollutantId(111),
        name: "Organic Carbon",
    },
    Pollutant {
        id: PollutantId(112),
        name: "Elemental Carbon",
    },
    Pollutant {
        id: PollutantId(115),
        name: "Sulfate Particulate",
    },
    Pollutant {
        id: PollutantId(116),
        name: "Primary PM2.5 - Brakewear Particulate",
    },
    Pollutant {
        id: PollutantId(117),
        name: "Primary PM2.5 - Tirewear Particulate",
    },
    Pollutant {
        id: PollutantId(118),
        name: "Composite - NonECPM",
    },
    Pollutant {
        id: PollutantId(119),
        name: "H2O (aerosol)",
    },
    Pollutant {
        id: PollutantId(121),
        name: "CMAQ5.0 Unspeciated (PMOTHR)",
    },
    Pollutant {
        id: PollutantId(122),
        name: "Non-carbon Organic Matter (NCOM)",
    },
    Pollutant {
        id: PollutantId(130),
        name: "1,2,3,7,8,9-Hexachlorodibenzo-p-Dioxin",
    },
    Pollutant {
        id: PollutantId(131),
        name: "Octachlorodibenzo-p-dioxin",
    },
    Pollutant {
        id: PollutantId(132),
        name: "1,2,3,4,6,7,8-Heptachlorodibenzo-p-Dioxin",
    },
    Pollutant {
        id: PollutantId(133),
        name: "Octachlorodibenzofuran",
    },
    Pollutant {
        id: PollutantId(134),
        name: "1,2,3,4,7,8-Hexachlorodibenzo-p-Dioxin",
    },
    Pollutant {
        id: PollutantId(135),
        name: "1,2,3,7,8-Pentachlorodibenzo-p-Dioxin",
    },
    Pollutant {
        id: PollutantId(136),
        name: "2,3,7,8-Tetrachlorodibenzofuran",
    },
    Pollutant {
        id: PollutantId(137),
        name: "1,2,3,4,7,8,9-Heptachlorodibenzofuran",
    },
    Pollutant {
        id: PollutantId(138),
        name: "2,3,4,7,8-Pentachlorodibenzofuran",
    },
    Pollutant {
        id: PollutantId(139),
        name: "1,2,3,7,8-Pentachlorodibenzofuran",
    },
    Pollutant {
        id: PollutantId(140),
        name: "1,2,3,6,7,8-Hexachlorodibenzofuran",
    },
    Pollutant {
        id: PollutantId(141),
        name: "1,2,3,6,7,8-Hexachlorodibenzo-p-Dioxin",
    },
    Pollutant {
        id: PollutantId(142),
        name: "2,3,7,8-Tetrachlorodibenzo-p-Dioxin",
    },
    Pollutant {
        id: PollutantId(143),
        name: "2,3,4,6,7,8-Hexachlorodibenzofuran",
    },
    Pollutant {
        id: PollutantId(144),
        name: "1,2,3,4,6,7,8-Heptachlorodibenzofuran",
    },
    Pollutant {
        id: PollutantId(145),
        name: "1,2,3,4,7,8-Hexachlorodibenzofuran",
    },
    Pollutant {
        id: PollutantId(146),
        name: "1,2,3,7,8,9-Hexachlorodibenzofuran",
    },
    Pollutant {
        id: PollutantId(168),
        name: "Dibenzo(a,h)anthracene gas",
    },
    Pollutant {
        id: PollutantId(169),
        name: "Fluoranthene gas",
    },
    Pollutant {
        id: PollutantId(170),
        name: "Acenaphthene gas",
    },
    Pollutant {
        id: PollutantId(171),
        name: "Acenaphthylene gas",
    },
    Pollutant {
        id: PollutantId(172),
        name: "Anthracene gas",
    },
    Pollutant {
        id: PollutantId(173),
        name: "Benz(a)anthracene gas",
    },
    Pollutant {
        id: PollutantId(174),
        name: "Benzo(a)pyrene gas",
    },
    Pollutant {
        id: PollutantId(175),
        name: "Benzo(b)fluoranthene gas",
    },
    Pollutant {
        id: PollutantId(176),
        name: "Benzo(g,h,i)perylene gas",
    },
    Pollutant {
        id: PollutantId(177),
        name: "Benzo(k)fluoranthene gas",
    },
    Pollutant {
        id: PollutantId(178),
        name: "Chrysene gas",
    },
    Pollutant {
        id: PollutantId(181),
        name: "Fluorene gas",
    },
    Pollutant {
        id: PollutantId(182),
        name: "Indeno(1,2,3,c,d)pyrene gas",
    },
    Pollutant {
        id: PollutantId(183),
        name: "Phenanthrene gas",
    },
    Pollutant {
        id: PollutantId(184),
        name: "Pyrene gas",
    },
    Pollutant {
        id: PollutantId(185),
        name: "Naphthalene gas",
    },
    Pollutant {
        id: PollutantId(1000),
        name: "CB05 Mechanism",
    },
    Pollutant {
        id: PollutantId(1001),
        name: "CB05_ALD2",
    },
    Pollutant {
        id: PollutantId(1002),
        name: "CB05_ALDX",
    },
    Pollutant {
        id: PollutantId(1005),
        name: "CB05_ETH",
    },
    Pollutant {
        id: PollutantId(1006),
        name: "CB05_ETHA",
    },
    Pollutant {
        id: PollutantId(1008),
        name: "CB05_FORM",
    },
    Pollutant {
        id: PollutantId(1009),
        name: "CB05_IOLE",
    },
    Pollutant {
        id: PollutantId(1010),
        name: "CB05_ISOP",
    },
    Pollutant {
        id: PollutantId(1011),
        name: "CB05_MEOH",
    },
    Pollutant {
        id: PollutantId(1012),
        name: "CB05_OLE",
    },
    Pollutant {
        id: PollutantId(1013),
        name: "CB05_PAR",
    },
    Pollutant {
        id: PollutantId(1014),
        name: "CB05_TERP",
    },
    Pollutant {
        id: PollutantId(1015),
        name: "CB05_TOL",
    },
    Pollutant {
        id: PollutantId(1016),
        name: "CB05_UNK",
    },
    Pollutant {
        id: PollutantId(1017),
        name: "CB05_UNR",
    },
    Pollutant {
        id: PollutantId(1018),
        name: "CB05_XYL",
    },
];

static BY_ID: phf::Map<u16, Pollutant> = phf::phf_map! {
    1u16 => Pollutant { id: PollutantId(1), name: "Total Gaseous Hydrocarbons" },
    2u16 => Pollutant { id: PollutantId(2), name: "Carbon Monoxide (CO)" },
    3u16 => Pollutant { id: PollutantId(3), name: "Oxides of Nitrogen (NOx)" },
    5u16 => Pollutant { id: PollutantId(5), name: "Methane (CH4)" },
    6u16 => Pollutant { id: PollutantId(6), name: "Nitrous Oxide (N2O)" },
    20u16 => Pollutant { id: PollutantId(20), name: "Benzene" },
    21u16 => Pollutant { id: PollutantId(21), name: "Ethanol" },
    22u16 => Pollutant { id: PollutantId(22), name: "MTBE" },
    23u16 => Pollutant { id: PollutantId(23), name: "Naphthalene particle" },
    24u16 => Pollutant { id: PollutantId(24), name: "1,3-Butadiene" },
    25u16 => Pollutant { id: PollutantId(25), name: "Formaldehyde" },
    26u16 => Pollutant { id: PollutantId(26), name: "Acetaldehyde" },
    27u16 => Pollutant { id: PollutantId(27), name: "Acrolein" },
    30u16 => Pollutant { id: PollutantId(30), name: "Ammonia (NH3)" },
    31u16 => Pollutant { id: PollutantId(31), name: "Sulfur Dioxide (SO2)" },
    32u16 => Pollutant { id: PollutantId(32), name: "Nitrogen Oxide (NO)" },
    33u16 => Pollutant { id: PollutantId(33), name: "Nitrogen Dioxide (NO2)" },
    34u16 => Pollutant { id: PollutantId(34), name: "Nitrous Acid (HONO)" },
    35u16 => Pollutant { id: PollutantId(35), name: "Nitrate (NO3)" },
    36u16 => Pollutant { id: PollutantId(36), name: "Ammonium (NH4)" },
    40u16 => Pollutant { id: PollutantId(40), name: "2,2,4-Trimethylpentane" },
    41u16 => Pollutant { id: PollutantId(41), name: "Ethyl Benzene" },
    42u16 => Pollutant { id: PollutantId(42), name: "Hexane" },
    43u16 => Pollutant { id: PollutantId(43), name: "Propionaldehyde" },
    44u16 => Pollutant { id: PollutantId(44), name: "Styrene" },
    45u16 => Pollutant { id: PollutantId(45), name: "Toluene" },
    46u16 => Pollutant { id: PollutantId(46), name: "Xylene" },
    51u16 => Pollutant { id: PollutantId(51), name: "Chloride" },
    52u16 => Pollutant { id: PollutantId(52), name: "Sodium" },
    53u16 => Pollutant { id: PollutantId(53), name: "Potassium" },
    54u16 => Pollutant { id: PollutantId(54), name: "Magnesium" },
    55u16 => Pollutant { id: PollutantId(55), name: "Calcium" },
    56u16 => Pollutant { id: PollutantId(56), name: "Titanium" },
    57u16 => Pollutant { id: PollutantId(57), name: "Silicon" },
    58u16 => Pollutant { id: PollutantId(58), name: "Aluminum" },
    59u16 => Pollutant { id: PollutantId(59), name: "Iron" },
    60u16 => Pollutant { id: PollutantId(60), name: "Mercury Elemental Gaseous" },
    61u16 => Pollutant { id: PollutantId(61), name: "Mercury Divalent Gaseous" },
    62u16 => Pollutant { id: PollutantId(62), name: "Mercury Particulate" },
    63u16 => Pollutant { id: PollutantId(63), name: "Arsenic Compounds" },
    65u16 => Pollutant { id: PollutantId(65), name: "Chromium 6+" },
    66u16 => Pollutant { id: PollutantId(66), name: "Manganese Compounds" },
    67u16 => Pollutant { id: PollutantId(67), name: "Nickel Compounds" },
    68u16 => Pollutant { id: PollutantId(68), name: "Dibenzo(a,h)anthracene particle" },
    69u16 => Pollutant { id: PollutantId(69), name: "Fluoranthene particle" },
    70u16 => Pollutant { id: PollutantId(70), name: "Acenaphthene particle" },
    71u16 => Pollutant { id: PollutantId(71), name: "Acenaphthylene particle" },
    72u16 => Pollutant { id: PollutantId(72), name: "Anthracene particle" },
    73u16 => Pollutant { id: PollutantId(73), name: "Benz(a)anthracene particle" },
    74u16 => Pollutant { id: PollutantId(74), name: "Benzo(a)pyrene particle" },
    75u16 => Pollutant { id: PollutantId(75), name: "Benzo(b)fluoranthene particle" },
    76u16 => Pollutant { id: PollutantId(76), name: "Benzo(g,h,i)perylene particle" },
    77u16 => Pollutant { id: PollutantId(77), name: "Benzo(k)fluoranthene particle" },
    78u16 => Pollutant { id: PollutantId(78), name: "Chrysene particle" },
    79u16 => Pollutant { id: PollutantId(79), name: "Non-Methane Hydrocarbons" },
    80u16 => Pollutant { id: PollutantId(80), name: "Non-Methane Organic Gases" },
    81u16 => Pollutant { id: PollutantId(81), name: "Fluorene particle" },
    82u16 => Pollutant { id: PollutantId(82), name: "Indeno(1,2,3,c,d)pyrene particle" },
    83u16 => Pollutant { id: PollutantId(83), name: "Phenanthrene particle" },
    84u16 => Pollutant { id: PollutantId(84), name: "Pyrene particle" },
    86u16 => Pollutant { id: PollutantId(86), name: "Total Organic Gases" },
    87u16 => Pollutant { id: PollutantId(87), name: "Volatile Organic Compounds" },
    88u16 => Pollutant { id: PollutantId(88), name: "NonHAPTOG" },
    90u16 => Pollutant { id: PollutantId(90), name: "Atmospheric CO2" },
    91u16 => Pollutant { id: PollutantId(91), name: "Total Energy Consumption" },
    92u16 => Pollutant { id: PollutantId(92), name: "Petroleum Energy Consumption" },
    93u16 => Pollutant { id: PollutantId(93), name: "Fossil Fuel Energy Consumption" },
    98u16 => Pollutant { id: PollutantId(98), name: "CO2 Equivalent" },
    99u16 => Pollutant { id: PollutantId(99), name: "Brake Specific Fuel Consumption (BSFC)" },
    100u16 => Pollutant { id: PollutantId(100), name: "Primary Exhaust PM10  - Total" },
    106u16 => Pollutant { id: PollutantId(106), name: "Primary PM10 - Brakewear Particulate" },
    107u16 => Pollutant { id: PollutantId(107), name: "Primary PM10 - Tirewear Particulate" },
    110u16 => Pollutant { id: PollutantId(110), name: "Primary Exhaust PM2.5 - Total" },
    111u16 => Pollutant { id: PollutantId(111), name: "Organic Carbon" },
    112u16 => Pollutant { id: PollutantId(112), name: "Elemental Carbon" },
    115u16 => Pollutant { id: PollutantId(115), name: "Sulfate Particulate" },
    116u16 => Pollutant { id: PollutantId(116), name: "Primary PM2.5 - Brakewear Particulate" },
    117u16 => Pollutant { id: PollutantId(117), name: "Primary PM2.5 - Tirewear Particulate" },
    118u16 => Pollutant { id: PollutantId(118), name: "Composite - NonECPM" },
    119u16 => Pollutant { id: PollutantId(119), name: "H2O (aerosol)" },
    121u16 => Pollutant { id: PollutantId(121), name: "CMAQ5.0 Unspeciated (PMOTHR)" },
    122u16 => Pollutant { id: PollutantId(122), name: "Non-carbon Organic Matter (NCOM)" },
    130u16 => Pollutant { id: PollutantId(130), name: "1,2,3,7,8,9-Hexachlorodibenzo-p-Dioxin" },
    131u16 => Pollutant { id: PollutantId(131), name: "Octachlorodibenzo-p-dioxin" },
    132u16 => Pollutant { id: PollutantId(132), name: "1,2,3,4,6,7,8-Heptachlorodibenzo-p-Dioxin" },
    133u16 => Pollutant { id: PollutantId(133), name: "Octachlorodibenzofuran" },
    134u16 => Pollutant { id: PollutantId(134), name: "1,2,3,4,7,8-Hexachlorodibenzo-p-Dioxin" },
    135u16 => Pollutant { id: PollutantId(135), name: "1,2,3,7,8-Pentachlorodibenzo-p-Dioxin" },
    136u16 => Pollutant { id: PollutantId(136), name: "2,3,7,8-Tetrachlorodibenzofuran" },
    137u16 => Pollutant { id: PollutantId(137), name: "1,2,3,4,7,8,9-Heptachlorodibenzofuran" },
    138u16 => Pollutant { id: PollutantId(138), name: "2,3,4,7,8-Pentachlorodibenzofuran" },
    139u16 => Pollutant { id: PollutantId(139), name: "1,2,3,7,8-Pentachlorodibenzofuran" },
    140u16 => Pollutant { id: PollutantId(140), name: "1,2,3,6,7,8-Hexachlorodibenzofuran" },
    141u16 => Pollutant { id: PollutantId(141), name: "1,2,3,6,7,8-Hexachlorodibenzo-p-Dioxin" },
    142u16 => Pollutant { id: PollutantId(142), name: "2,3,7,8-Tetrachlorodibenzo-p-Dioxin" },
    143u16 => Pollutant { id: PollutantId(143), name: "2,3,4,6,7,8-Hexachlorodibenzofuran" },
    144u16 => Pollutant { id: PollutantId(144), name: "1,2,3,4,6,7,8-Heptachlorodibenzofuran" },
    145u16 => Pollutant { id: PollutantId(145), name: "1,2,3,4,7,8-Hexachlorodibenzofuran" },
    146u16 => Pollutant { id: PollutantId(146), name: "1,2,3,7,8,9-Hexachlorodibenzofuran" },
    168u16 => Pollutant { id: PollutantId(168), name: "Dibenzo(a,h)anthracene gas" },
    169u16 => Pollutant { id: PollutantId(169), name: "Fluoranthene gas" },
    170u16 => Pollutant { id: PollutantId(170), name: "Acenaphthene gas" },
    171u16 => Pollutant { id: PollutantId(171), name: "Acenaphthylene gas" },
    172u16 => Pollutant { id: PollutantId(172), name: "Anthracene gas" },
    173u16 => Pollutant { id: PollutantId(173), name: "Benz(a)anthracene gas" },
    174u16 => Pollutant { id: PollutantId(174), name: "Benzo(a)pyrene gas" },
    175u16 => Pollutant { id: PollutantId(175), name: "Benzo(b)fluoranthene gas" },
    176u16 => Pollutant { id: PollutantId(176), name: "Benzo(g,h,i)perylene gas" },
    177u16 => Pollutant { id: PollutantId(177), name: "Benzo(k)fluoranthene gas" },
    178u16 => Pollutant { id: PollutantId(178), name: "Chrysene gas" },
    181u16 => Pollutant { id: PollutantId(181), name: "Fluorene gas" },
    182u16 => Pollutant { id: PollutantId(182), name: "Indeno(1,2,3,c,d)pyrene gas" },
    183u16 => Pollutant { id: PollutantId(183), name: "Phenanthrene gas" },
    184u16 => Pollutant { id: PollutantId(184), name: "Pyrene gas" },
    185u16 => Pollutant { id: PollutantId(185), name: "Naphthalene gas" },
    1000u16 => Pollutant { id: PollutantId(1000), name: "CB05 Mechanism" },
    1001u16 => Pollutant { id: PollutantId(1001), name: "CB05_ALD2" },
    1002u16 => Pollutant { id: PollutantId(1002), name: "CB05_ALDX" },
    1005u16 => Pollutant { id: PollutantId(1005), name: "CB05_ETH" },
    1006u16 => Pollutant { id: PollutantId(1006), name: "CB05_ETHA" },
    1008u16 => Pollutant { id: PollutantId(1008), name: "CB05_FORM" },
    1009u16 => Pollutant { id: PollutantId(1009), name: "CB05_IOLE" },
    1010u16 => Pollutant { id: PollutantId(1010), name: "CB05_ISOP" },
    1011u16 => Pollutant { id: PollutantId(1011), name: "CB05_MEOH" },
    1012u16 => Pollutant { id: PollutantId(1012), name: "CB05_OLE" },
    1013u16 => Pollutant { id: PollutantId(1013), name: "CB05_PAR" },
    1014u16 => Pollutant { id: PollutantId(1014), name: "CB05_TERP" },
    1015u16 => Pollutant { id: PollutantId(1015), name: "CB05_TOL" },
    1016u16 => Pollutant { id: PollutantId(1016), name: "CB05_UNK" },
    1017u16 => Pollutant { id: PollutantId(1017), name: "CB05_UNR" },
    1018u16 => Pollutant { id: PollutantId(1018), name: "CB05_XYL" },
};

static BY_NAME_LOWER: phf::Map<&'static str, Pollutant> = phf::phf_map! {
    "total gaseous hydrocarbons" => Pollutant { id: PollutantId(1), name: "Total Gaseous Hydrocarbons" },
    "carbon monoxide (co)" => Pollutant { id: PollutantId(2), name: "Carbon Monoxide (CO)" },
    "oxides of nitrogen (nox)" => Pollutant { id: PollutantId(3), name: "Oxides of Nitrogen (NOx)" },
    "methane (ch4)" => Pollutant { id: PollutantId(5), name: "Methane (CH4)" },
    "nitrous oxide (n2o)" => Pollutant { id: PollutantId(6), name: "Nitrous Oxide (N2O)" },
    "benzene" => Pollutant { id: PollutantId(20), name: "Benzene" },
    "ethanol" => Pollutant { id: PollutantId(21), name: "Ethanol" },
    "mtbe" => Pollutant { id: PollutantId(22), name: "MTBE" },
    "naphthalene particle" => Pollutant { id: PollutantId(23), name: "Naphthalene particle" },
    "1,3-butadiene" => Pollutant { id: PollutantId(24), name: "1,3-Butadiene" },
    "formaldehyde" => Pollutant { id: PollutantId(25), name: "Formaldehyde" },
    "acetaldehyde" => Pollutant { id: PollutantId(26), name: "Acetaldehyde" },
    "acrolein" => Pollutant { id: PollutantId(27), name: "Acrolein" },
    "ammonia (nh3)" => Pollutant { id: PollutantId(30), name: "Ammonia (NH3)" },
    "sulfur dioxide (so2)" => Pollutant { id: PollutantId(31), name: "Sulfur Dioxide (SO2)" },
    "nitrogen oxide (no)" => Pollutant { id: PollutantId(32), name: "Nitrogen Oxide (NO)" },
    "nitrogen dioxide (no2)" => Pollutant { id: PollutantId(33), name: "Nitrogen Dioxide (NO2)" },
    "nitrous acid (hono)" => Pollutant { id: PollutantId(34), name: "Nitrous Acid (HONO)" },
    "nitrate (no3)" => Pollutant { id: PollutantId(35), name: "Nitrate (NO3)" },
    "ammonium (nh4)" => Pollutant { id: PollutantId(36), name: "Ammonium (NH4)" },
    "2,2,4-trimethylpentane" => Pollutant { id: PollutantId(40), name: "2,2,4-Trimethylpentane" },
    "ethyl benzene" => Pollutant { id: PollutantId(41), name: "Ethyl Benzene" },
    "hexane" => Pollutant { id: PollutantId(42), name: "Hexane" },
    "propionaldehyde" => Pollutant { id: PollutantId(43), name: "Propionaldehyde" },
    "styrene" => Pollutant { id: PollutantId(44), name: "Styrene" },
    "toluene" => Pollutant { id: PollutantId(45), name: "Toluene" },
    "xylene" => Pollutant { id: PollutantId(46), name: "Xylene" },
    "chloride" => Pollutant { id: PollutantId(51), name: "Chloride" },
    "sodium" => Pollutant { id: PollutantId(52), name: "Sodium" },
    "potassium" => Pollutant { id: PollutantId(53), name: "Potassium" },
    "magnesium" => Pollutant { id: PollutantId(54), name: "Magnesium" },
    "calcium" => Pollutant { id: PollutantId(55), name: "Calcium" },
    "titanium" => Pollutant { id: PollutantId(56), name: "Titanium" },
    "silicon" => Pollutant { id: PollutantId(57), name: "Silicon" },
    "aluminum" => Pollutant { id: PollutantId(58), name: "Aluminum" },
    "iron" => Pollutant { id: PollutantId(59), name: "Iron" },
    "mercury elemental gaseous" => Pollutant { id: PollutantId(60), name: "Mercury Elemental Gaseous" },
    "mercury divalent gaseous" => Pollutant { id: PollutantId(61), name: "Mercury Divalent Gaseous" },
    "mercury particulate" => Pollutant { id: PollutantId(62), name: "Mercury Particulate" },
    "arsenic compounds" => Pollutant { id: PollutantId(63), name: "Arsenic Compounds" },
    "chromium 6+" => Pollutant { id: PollutantId(65), name: "Chromium 6+" },
    "manganese compounds" => Pollutant { id: PollutantId(66), name: "Manganese Compounds" },
    "nickel compounds" => Pollutant { id: PollutantId(67), name: "Nickel Compounds" },
    "dibenzo(a,h)anthracene particle" => Pollutant { id: PollutantId(68), name: "Dibenzo(a,h)anthracene particle" },
    "fluoranthene particle" => Pollutant { id: PollutantId(69), name: "Fluoranthene particle" },
    "acenaphthene particle" => Pollutant { id: PollutantId(70), name: "Acenaphthene particle" },
    "acenaphthylene particle" => Pollutant { id: PollutantId(71), name: "Acenaphthylene particle" },
    "anthracene particle" => Pollutant { id: PollutantId(72), name: "Anthracene particle" },
    "benz(a)anthracene particle" => Pollutant { id: PollutantId(73), name: "Benz(a)anthracene particle" },
    "benzo(a)pyrene particle" => Pollutant { id: PollutantId(74), name: "Benzo(a)pyrene particle" },
    "benzo(b)fluoranthene particle" => Pollutant { id: PollutantId(75), name: "Benzo(b)fluoranthene particle" },
    "benzo(g,h,i)perylene particle" => Pollutant { id: PollutantId(76), name: "Benzo(g,h,i)perylene particle" },
    "benzo(k)fluoranthene particle" => Pollutant { id: PollutantId(77), name: "Benzo(k)fluoranthene particle" },
    "chrysene particle" => Pollutant { id: PollutantId(78), name: "Chrysene particle" },
    "non-methane hydrocarbons" => Pollutant { id: PollutantId(79), name: "Non-Methane Hydrocarbons" },
    "non-methane organic gases" => Pollutant { id: PollutantId(80), name: "Non-Methane Organic Gases" },
    "fluorene particle" => Pollutant { id: PollutantId(81), name: "Fluorene particle" },
    "indeno(1,2,3,c,d)pyrene particle" => Pollutant { id: PollutantId(82), name: "Indeno(1,2,3,c,d)pyrene particle" },
    "phenanthrene particle" => Pollutant { id: PollutantId(83), name: "Phenanthrene particle" },
    "pyrene particle" => Pollutant { id: PollutantId(84), name: "Pyrene particle" },
    "total organic gases" => Pollutant { id: PollutantId(86), name: "Total Organic Gases" },
    "volatile organic compounds" => Pollutant { id: PollutantId(87), name: "Volatile Organic Compounds" },
    "nonhaptog" => Pollutant { id: PollutantId(88), name: "NonHAPTOG" },
    "atmospheric co2" => Pollutant { id: PollutantId(90), name: "Atmospheric CO2" },
    "total energy consumption" => Pollutant { id: PollutantId(91), name: "Total Energy Consumption" },
    "petroleum energy consumption" => Pollutant { id: PollutantId(92), name: "Petroleum Energy Consumption" },
    "fossil fuel energy consumption" => Pollutant { id: PollutantId(93), name: "Fossil Fuel Energy Consumption" },
    "co2 equivalent" => Pollutant { id: PollutantId(98), name: "CO2 Equivalent" },
    "brake specific fuel consumption (bsfc)" => Pollutant { id: PollutantId(99), name: "Brake Specific Fuel Consumption (BSFC)" },
    "primary exhaust pm10  - total" => Pollutant { id: PollutantId(100), name: "Primary Exhaust PM10  - Total" },
    "primary pm10 - brakewear particulate" => Pollutant { id: PollutantId(106), name: "Primary PM10 - Brakewear Particulate" },
    "primary pm10 - tirewear particulate" => Pollutant { id: PollutantId(107), name: "Primary PM10 - Tirewear Particulate" },
    "primary exhaust pm2.5 - total" => Pollutant { id: PollutantId(110), name: "Primary Exhaust PM2.5 - Total" },
    "organic carbon" => Pollutant { id: PollutantId(111), name: "Organic Carbon" },
    "elemental carbon" => Pollutant { id: PollutantId(112), name: "Elemental Carbon" },
    "sulfate particulate" => Pollutant { id: PollutantId(115), name: "Sulfate Particulate" },
    "primary pm2.5 - brakewear particulate" => Pollutant { id: PollutantId(116), name: "Primary PM2.5 - Brakewear Particulate" },
    "primary pm2.5 - tirewear particulate" => Pollutant { id: PollutantId(117), name: "Primary PM2.5 - Tirewear Particulate" },
    "composite - nonecpm" => Pollutant { id: PollutantId(118), name: "Composite - NonECPM" },
    "h2o (aerosol)" => Pollutant { id: PollutantId(119), name: "H2O (aerosol)" },
    "cmaq5.0 unspeciated (pmothr)" => Pollutant { id: PollutantId(121), name: "CMAQ5.0 Unspeciated (PMOTHR)" },
    "non-carbon organic matter (ncom)" => Pollutant { id: PollutantId(122), name: "Non-carbon Organic Matter (NCOM)" },
    "1,2,3,7,8,9-hexachlorodibenzo-p-dioxin" => Pollutant { id: PollutantId(130), name: "1,2,3,7,8,9-Hexachlorodibenzo-p-Dioxin" },
    "octachlorodibenzo-p-dioxin" => Pollutant { id: PollutantId(131), name: "Octachlorodibenzo-p-dioxin" },
    "1,2,3,4,6,7,8-heptachlorodibenzo-p-dioxin" => Pollutant { id: PollutantId(132), name: "1,2,3,4,6,7,8-Heptachlorodibenzo-p-Dioxin" },
    "octachlorodibenzofuran" => Pollutant { id: PollutantId(133), name: "Octachlorodibenzofuran" },
    "1,2,3,4,7,8-hexachlorodibenzo-p-dioxin" => Pollutant { id: PollutantId(134), name: "1,2,3,4,7,8-Hexachlorodibenzo-p-Dioxin" },
    "1,2,3,7,8-pentachlorodibenzo-p-dioxin" => Pollutant { id: PollutantId(135), name: "1,2,3,7,8-Pentachlorodibenzo-p-Dioxin" },
    "2,3,7,8-tetrachlorodibenzofuran" => Pollutant { id: PollutantId(136), name: "2,3,7,8-Tetrachlorodibenzofuran" },
    "1,2,3,4,7,8,9-heptachlorodibenzofuran" => Pollutant { id: PollutantId(137), name: "1,2,3,4,7,8,9-Heptachlorodibenzofuran" },
    "2,3,4,7,8-pentachlorodibenzofuran" => Pollutant { id: PollutantId(138), name: "2,3,4,7,8-Pentachlorodibenzofuran" },
    "1,2,3,7,8-pentachlorodibenzofuran" => Pollutant { id: PollutantId(139), name: "1,2,3,7,8-Pentachlorodibenzofuran" },
    "1,2,3,6,7,8-hexachlorodibenzofuran" => Pollutant { id: PollutantId(140), name: "1,2,3,6,7,8-Hexachlorodibenzofuran" },
    "1,2,3,6,7,8-hexachlorodibenzo-p-dioxin" => Pollutant { id: PollutantId(141), name: "1,2,3,6,7,8-Hexachlorodibenzo-p-Dioxin" },
    "2,3,7,8-tetrachlorodibenzo-p-dioxin" => Pollutant { id: PollutantId(142), name: "2,3,7,8-Tetrachlorodibenzo-p-Dioxin" },
    "2,3,4,6,7,8-hexachlorodibenzofuran" => Pollutant { id: PollutantId(143), name: "2,3,4,6,7,8-Hexachlorodibenzofuran" },
    "1,2,3,4,6,7,8-heptachlorodibenzofuran" => Pollutant { id: PollutantId(144), name: "1,2,3,4,6,7,8-Heptachlorodibenzofuran" },
    "1,2,3,4,7,8-hexachlorodibenzofuran" => Pollutant { id: PollutantId(145), name: "1,2,3,4,7,8-Hexachlorodibenzofuran" },
    "1,2,3,7,8,9-hexachlorodibenzofuran" => Pollutant { id: PollutantId(146), name: "1,2,3,7,8,9-Hexachlorodibenzofuran" },
    "dibenzo(a,h)anthracene gas" => Pollutant { id: PollutantId(168), name: "Dibenzo(a,h)anthracene gas" },
    "fluoranthene gas" => Pollutant { id: PollutantId(169), name: "Fluoranthene gas" },
    "acenaphthene gas" => Pollutant { id: PollutantId(170), name: "Acenaphthene gas" },
    "acenaphthylene gas" => Pollutant { id: PollutantId(171), name: "Acenaphthylene gas" },
    "anthracene gas" => Pollutant { id: PollutantId(172), name: "Anthracene gas" },
    "benz(a)anthracene gas" => Pollutant { id: PollutantId(173), name: "Benz(a)anthracene gas" },
    "benzo(a)pyrene gas" => Pollutant { id: PollutantId(174), name: "Benzo(a)pyrene gas" },
    "benzo(b)fluoranthene gas" => Pollutant { id: PollutantId(175), name: "Benzo(b)fluoranthene gas" },
    "benzo(g,h,i)perylene gas" => Pollutant { id: PollutantId(176), name: "Benzo(g,h,i)perylene gas" },
    "benzo(k)fluoranthene gas" => Pollutant { id: PollutantId(177), name: "Benzo(k)fluoranthene gas" },
    "chrysene gas" => Pollutant { id: PollutantId(178), name: "Chrysene gas" },
    "fluorene gas" => Pollutant { id: PollutantId(181), name: "Fluorene gas" },
    "indeno(1,2,3,c,d)pyrene gas" => Pollutant { id: PollutantId(182), name: "Indeno(1,2,3,c,d)pyrene gas" },
    "phenanthrene gas" => Pollutant { id: PollutantId(183), name: "Phenanthrene gas" },
    "pyrene gas" => Pollutant { id: PollutantId(184), name: "Pyrene gas" },
    "naphthalene gas" => Pollutant { id: PollutantId(185), name: "Naphthalene gas" },
    "cb05 mechanism" => Pollutant { id: PollutantId(1000), name: "CB05 Mechanism" },
    "cb05_ald2" => Pollutant { id: PollutantId(1001), name: "CB05_ALD2" },
    "cb05_aldx" => Pollutant { id: PollutantId(1002), name: "CB05_ALDX" },
    "cb05_eth" => Pollutant { id: PollutantId(1005), name: "CB05_ETH" },
    "cb05_etha" => Pollutant { id: PollutantId(1006), name: "CB05_ETHA" },
    "cb05_form" => Pollutant { id: PollutantId(1008), name: "CB05_FORM" },
    "cb05_iole" => Pollutant { id: PollutantId(1009), name: "CB05_IOLE" },
    "cb05_isop" => Pollutant { id: PollutantId(1010), name: "CB05_ISOP" },
    "cb05_meoh" => Pollutant { id: PollutantId(1011), name: "CB05_MEOH" },
    "cb05_ole" => Pollutant { id: PollutantId(1012), name: "CB05_OLE" },
    "cb05_par" => Pollutant { id: PollutantId(1013), name: "CB05_PAR" },
    "cb05_terp" => Pollutant { id: PollutantId(1014), name: "CB05_TERP" },
    "cb05_tol" => Pollutant { id: PollutantId(1015), name: "CB05_TOL" },
    "cb05_unk" => Pollutant { id: PollutantId(1016), name: "CB05_UNK" },
    "cb05_unr" => Pollutant { id: PollutantId(1017), name: "CB05_UNR" },
    "cb05_xyl" => Pollutant { id: PollutantId(1018), name: "CB05_XYL" },
};

#[cfg(test)]
mod tests {
    use super::*;

    // Ports the spirit of `gov/epa/otaq/moves/master/framework/PollutantTest.java`.
    // The Java test creates synthetic mutable instances; the Rust port replaces
    // the mutable registry with compile-time canonical phf maps, so the test
    // covers the canonical lookups instead.

    #[test]
    fn find_by_id_returns_canonical_match() {
        let thc = Pollutant::find_by_id(PollutantId(1)).expect("THC is canonical");
        assert_eq!(thc.id, PollutantId(1));
        assert_eq!(thc.name, "Total Gaseous Hydrocarbons");
    }

    #[test]
    fn find_by_id_returns_none_for_unknown() {
        // 7777 sits in the gap between canonical ids — explicitly not assigned.
        assert!(Pollutant::find_by_id(PollutantId(7777)).is_none());
    }

    #[test]
    fn find_by_name_is_case_insensitive() {
        let canon = Pollutant::find_by_name("Carbon Monoxide (CO)").unwrap();
        let lower = Pollutant::find_by_name("carbon monoxide (co)").unwrap();
        let upper = Pollutant::find_by_name("CARBON MONOXIDE (CO)").unwrap();
        assert_eq!(canon, lower);
        assert_eq!(canon, upper);
        assert_eq!(canon.id, PollutantId(2));
    }

    #[test]
    fn find_by_name_accepts_numeric_id_fallback() {
        // The Java findByName falls back to numeric-id matching when the
        // input parses as a number — runspec text fields may carry either form.
        let by_name = Pollutant::find_by_name("Total Gaseous Hydrocarbons").unwrap();
        let by_numeric = Pollutant::find_by_name("1").unwrap();
        assert_eq!(by_name, by_numeric);
    }

    #[test]
    fn find_by_name_returns_none_for_unknown() {
        assert!(Pollutant::find_by_name("this does not exist").is_none());
    }

    #[test]
    fn distinct_pollutants_are_distinguishable() {
        let p2 = Pollutant::find_by_name("Carbon Monoxide (CO)").unwrap();
        let p3 = Pollutant::find_by_name("Oxides of Nitrogen (NOx)").unwrap();
        assert_ne!(p2, p3);
        assert!(p2.cmp(&p3) != std::cmp::Ordering::Equal);
    }

    #[test]
    fn all_iter_returns_every_canonical_entry_in_id_order() {
        let ids: Vec<u16> = Pollutant::all().map(|p| p.id.0).collect();
        assert_eq!(ids.len(), ALL_POLLUTANTS.len());
        assert!(
            ids.windows(2).all(|w| w[0] < w[1]),
            "ids must be sorted ascending"
        );
    }

    #[test]
    fn by_id_and_by_name_agree_on_every_canonical_entry() {
        for p in Pollutant::all() {
            assert_eq!(Pollutant::find_by_id(p.id), Some(p));
            assert_eq!(Pollutant::find_by_name(p.name), Some(p));
        }
    }

    #[test]
    fn pollutant_id_parses_from_str() {
        assert_eq!("42".parse::<PollutantId>().unwrap(), PollutantId(42));
        assert!("not-a-number".parse::<PollutantId>().is_err());
    }

    #[test]
    fn pollutant_id_round_trips_through_u16() {
        let id = PollutantId::from(91);
        let raw: u16 = id.into();
        assert_eq!(raw, 91);
    }

    #[test]
    fn display_uses_canonical_name() {
        let p = Pollutant::find_by_id(PollutantId(91)).unwrap();
        assert_eq!(p.to_string(), "Total Energy Consumption");
    }
}
