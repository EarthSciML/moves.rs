//! The MasterLoop enums MOVES textualises into `CalculatorInfo.txt`.
//!
//! Mirrors the two Java constants:
//!
//! * `gov.epa.otaq.moves.master.framework.MasterLoopGranularity`
//! * `gov.epa.otaq.moves.master.framework.MasterLoopPriority`
//!
//! Encoding them in Rust avoids re-deriving the numeric ordering at every
//! consumer site. Priority comparisons drive execution order inside a single
//! granularity bucket; granularity comparisons drive bucket order.
//!
//! Granularity ordering note: Java's
//! [`MasterLoopGranularity.compareTo`](https://github.com/USEPA/EPA_MOVES_Model/blob/HEAD/gov/epa/otaq/moves/master/framework/MasterLoopGranularity.java)
//! flips the natural integer order so that the `TreeSet` sorted-low-to-high
//! ordering puts COARSER granularities first. We bake that flip into
//! [`Granularity::execution_index`].

use std::cmp::Ordering;
use std::str::FromStr;

use serde::{de, Deserialize, Deserializer, Serialize, Serializer};

/// Granularity at which a MasterLoopable subscribes. Coarsest is `Process`,
/// finest is `Hour`. `MatchFinest` is the special case used by chained
/// calculators that fire immediately after their upstream generator.
///
/// Serializes as the canonical Java string form (`"HOUR"`, `"MATCH_FINEST"`,
/// …) so the JSON document matches `CalculatorInfo.txt` byte-for-byte
/// when describing a subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Granularity {
    Hour,
    Day,
    Month,
    Year,
    Link,
    Zone,
    County,
    State,
    Process,
    MatchFinest,
}

impl Granularity {
    /// Symbolic name as it appears in `CalculatorInfo.txt`.
    pub fn as_str(self) -> &'static str {
        match self {
            Granularity::Hour => "HOUR",
            Granularity::Day => "DAY",
            Granularity::Month => "MONTH",
            Granularity::Year => "YEAR",
            Granularity::Link => "LINK",
            Granularity::Zone => "ZONE",
            Granularity::County => "COUNTY",
            Granularity::State => "STATE",
            Granularity::Process => "PROCESS",
            Granularity::MatchFinest => "MATCH_FINEST",
        }
    }

    /// Mirror of Java's
    /// [`MasterLoopGranularity.granularityValue`](https://github.com/USEPA/EPA_MOVES_Model/blob/HEAD/gov/epa/otaq/moves/master/framework/MasterLoopGranularity.java)
    /// — coarser-grained constants get higher integers. Use
    /// [`execution_index`](Self::execution_index) for sort keys.
    pub fn granularity_value(self) -> i32 {
        match self {
            Granularity::Hour => 1,
            Granularity::Day => 2,
            Granularity::Month => 3,
            Granularity::Year => 4,
            Granularity::Link => 5,
            Granularity::Zone => 6,
            Granularity::County => 7,
            Granularity::State => 8,
            Granularity::Process => 9,
            Granularity::MatchFinest => 0,
        }
    }

    /// Sort key for execution order: lower = fires earlier. Java's
    /// `compareTo` returns `other.value - this.value` so that the
    /// `TreeSet` natural order places COARSER granularities first inside
    /// the MasterLoop — we mirror that here by flipping the sign.
    /// `MatchFinest` is a special case: it pins a calculator to the finest
    /// granularity in play, so it sorts last (largest `execution_index`).
    pub fn execution_index(self) -> i32 {
        match self {
            Granularity::MatchFinest => i32::MAX,
            other => -other.granularity_value(),
        }
    }
}

impl FromStr for Granularity {
    type Err = ();

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(match s {
            "HOUR" => Granularity::Hour,
            "DAY" => Granularity::Day,
            "MONTH" => Granularity::Month,
            "YEAR" => Granularity::Year,
            "LINK" => Granularity::Link,
            "ZONE" => Granularity::Zone,
            "COUNTY" => Granularity::County,
            "STATE" => Granularity::State,
            "PROCESS" => Granularity::Process,
            "MATCH_FINEST" => Granularity::MatchFinest,
            _ => return Err(()),
        })
    }
}

impl Serialize for Granularity {
    fn serialize<S: Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for Granularity {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let raw = String::deserialize(d)?;
        Granularity::from_str(&raw)
            .map_err(|_| de::Error::custom(format!("unknown granularity '{raw}'")))
    }
}

/// Priority within a granularity bucket. Higher integer = earlier execution.
/// Stored as a `(base, offset)` pair so the original `EMISSION_CALCULATOR+1`
/// vs `EMISSION_CALCULATOR-2` distinctions round-trip through the JSON
/// without losing information.
///
/// Serializes as the canonical `MasterLoopPriority.decode(int)` string
/// (`"EMISSION_CALCULATOR"`, `"EMISSION_CALCULATOR+1"`, `"42"`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Priority {
    /// One of `INTERNAL_CONTROL_STRATEGY` / `GENERATOR` / `EMISSION_CALCULATOR`
    /// or the integer literal if it lay outside every band.
    pub base: PriorityBase,
    /// Offset from the named base. Positive means earlier execution.
    pub offset: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PriorityBase {
    InternalControlStrategy,
    Generator,
    EmissionCalculator,
    /// Raw integer literal that didn't fall inside any decode band.
    Other,
}

impl PriorityBase {
    pub fn base_value(self) -> i32 {
        match self {
            PriorityBase::InternalControlStrategy => 1000,
            PriorityBase::Generator => 100,
            PriorityBase::EmissionCalculator => 10,
            PriorityBase::Other => 0,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            PriorityBase::InternalControlStrategy => "INTERNAL_CONTROL_STRATEGY",
            PriorityBase::Generator => "GENERATOR",
            PriorityBase::EmissionCalculator => "EMISSION_CALCULATOR",
            PriorityBase::Other => "OTHER",
        }
    }
}

impl Priority {
    /// Total integer value, matching Java's `MasterLoopPriority` constants.
    pub fn value(self) -> i32 {
        self.base.base_value() + self.offset
    }

    /// Reconstruct the `MasterLoopPriority.decode(int)` textual form, e.g.
    /// `"EMISSION_CALCULATOR"`, `"EMISSION_CALCULATOR+1"`, `"GENERATOR-2"`.
    pub fn display(self) -> String {
        if self.base == PriorityBase::Other {
            self.value().to_string()
        } else if self.offset == 0 {
            self.base.as_str().to_string()
        } else if self.offset > 0 {
            format!("{}+{}", self.base.as_str(), self.offset)
        } else {
            format!("{}-{}", self.base.as_str(), -self.offset)
        }
    }

    /// Parse one of the textual forms produced by
    /// [`MasterLoopPriority.decode`](https://github.com/USEPA/EPA_MOVES_Model/blob/HEAD/gov/epa/otaq/moves/master/framework/MasterLoopPriority.java).
    /// Returns `None` for inputs that aren't recognisable.
    pub fn parse(input: &str) -> Option<Self> {
        let bases = [
            (
                "INTERNAL_CONTROL_STRATEGY",
                PriorityBase::InternalControlStrategy,
            ),
            ("GENERATOR", PriorityBase::Generator),
            ("EMISSION_CALCULATOR", PriorityBase::EmissionCalculator),
        ];
        for (name, base) in bases {
            if input == name {
                return Some(Priority { base, offset: 0 });
            }
            if let Some(rest) = input.strip_prefix(name) {
                if let Some(num) = rest.strip_prefix('+') {
                    if let Ok(n) = num.parse::<i32>() {
                        return Some(Priority { base, offset: n });
                    }
                } else if let Some(num) = rest.strip_prefix('-') {
                    if let Ok(n) = num.parse::<i32>() {
                        return Some(Priority { base, offset: -n });
                    }
                }
            }
        }
        if let Ok(n) = input.parse::<i32>() {
            return Some(Priority {
                base: PriorityBase::Other,
                offset: n,
            });
        }
        None
    }
}

impl Serialize for Priority {
    fn serialize<S: Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(&self.display())
    }
}

impl<'de> Deserialize<'de> for Priority {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let raw = String::deserialize(d)?;
        Priority::parse(&raw).ok_or_else(|| de::Error::custom(format!("unknown priority '{raw}'")))
    }
}

impl PartialOrd for Priority {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Priority {
    /// Higher integer value sorts greater. Use `Reverse(priority)` when
    /// you want "earliest-fires-first" iteration order.
    fn cmp(&self, other: &Self) -> Ordering {
        self.value().cmp(&other.value())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn granularity_round_trip() {
        for g in [
            Granularity::Hour,
            Granularity::Day,
            Granularity::Month,
            Granularity::Year,
            Granularity::Link,
            Granularity::Zone,
            Granularity::County,
            Granularity::State,
            Granularity::Process,
            Granularity::MatchFinest,
        ] {
            assert_eq!(Granularity::from_str(g.as_str()).unwrap(), g);
        }
    }

    #[test]
    fn granularity_execution_order_coarse_first() {
        // Process is coarser than Month — fires earlier — smaller execution_index.
        assert!(Granularity::Process.execution_index() < Granularity::Month.execution_index());
        assert!(Granularity::Month.execution_index() < Granularity::Hour.execution_index());
        // MatchFinest is special — fires last.
        assert!(Granularity::Hour.execution_index() < Granularity::MatchFinest.execution_index());
    }

    #[test]
    fn priority_round_trip_named() {
        for raw in &[
            "EMISSION_CALCULATOR",
            "EMISSION_CALCULATOR+1",
            "EMISSION_CALCULATOR-2",
            "GENERATOR",
            "GENERATOR+1",
            "GENERATOR-1",
            "INTERNAL_CONTROL_STRATEGY",
            "INTERNAL_CONTROL_STRATEGY+5",
        ] {
            let p = Priority::parse(raw).expect("parse priority");
            assert_eq!(p.display(), *raw, "round-trip {raw}");
        }
    }

    #[test]
    fn priority_round_trip_raw_int() {
        let p = Priority::parse("42").unwrap();
        assert_eq!(p.value(), 42);
        assert_eq!(p.display(), "42");
    }

    #[test]
    fn priority_unknown_returns_none() {
        assert!(Priority::parse("WAT").is_none());
    }

    #[test]
    fn priority_ordering_matches_value() {
        let lo = Priority::parse("EMISSION_CALCULATOR").unwrap();
        let mid = Priority::parse("EMISSION_CALCULATOR+1").unwrap();
        let hi = Priority::parse("GENERATOR").unwrap();
        assert!(lo < mid);
        assert!(mid < hi);
    }
}
