//! The location iterator — port of `ExecutionLocationProducer.java`.
//!
//! Ports `gov.epa.otaq.moves.master.framework.ExecutionLocationProducer`.
//! Each geographic selection in a [`RunSpec`](moves_runspec::RunSpec)
//! (`NATION` / `STATE` / `COUNTY` / `ZONE` / `LINK`) names many individual
//! link-granularity locations; the producer expands the selections into the
//! flat, sorted, de-duplicated set of `(state, county, zone, link)` tuples
//! the MasterLoop iterates over. This is the set Task 16 of
//! `moves-rust-migration-plan.md` calls "the sequence of
//! (state, county, zone, link) tuples the master loop iterates over."
//!
//! # Java SQL replaced by an in-memory filter
//!
//! The Java producer builds five `PreparedStatement`s — one per selection
//! granularity — and `SELECT`s
//! `County.stateID, County.countyID, Link.zoneID, Link.linkID, Link.roadTypeID`
//! from the `County` ⋈ `Link` join (plus a `County`-only query for the
//! NONROAD pseudo-road-type). The Rust port keeps the same dispatch shape
//! but takes the geography as data: [`build_execution_locations`] is a pure
//! function of the RunSpec's selections, the effective road-type filter,
//! and a [`GeographyTables`] value holding the `Link` ⋈ `County` rows and
//! `County` rows.
//!
//! Phase 2 has no data plane (Task 50, `DataFrameStore`), so callers build
//! [`GeographyTables`] from fixtures; Task 50 will build it from the
//! `Link` / `County` Parquet snapshots. Keeping the producer a pure
//! function means every branch — selection dispatch, the road-type
//! filter, the NONROAD synthesis — is exercised by tests today, exactly as
//! the Java `addLinkLocations` / `addZoneLocations` / … methods were
//! exercised against a fixture database.
//!
//! [`build_execution_locations`]: ExecutionLocationProducer::build_execution_locations
//!
//! # Road type
//!
//! The Java `ExecutionLocation` carries a fifth field, `roadTypeRecordID`.
//! The Rust [`ExecutionLocation`] is the four-field
//! `(state, county, zone, link)` value Task 23 introduced for the
//! MasterLoop position and Task 15 commits the `execution_locations` set
//! of [`ExecutionRunSpec`](crate::ExecutionRunSpec) to — and the migration
//! plan itself describes Task 16's output as
//! *(state, county, zone, link) tuples*. Road type therefore enters this
//! module only as a **filter**: [`RoadTypeFilter`] ports
//! `buildRoadTypesSQL`'s onroad / NONROAD split. It is not stored on the
//! produced tuple. (Phase 3 calculators read `roadTypeRecordID` off the
//! iteration position; threading road type onto the position is a
//! MasterLoop concern for whichever task ports those calculators.)
//!
//! # Entry point
//!
//! [`ExecutionRunSpec::build_execution_locations`](crate::ExecutionRunSpec::build_execution_locations)
//! is the integration point — it ports the
//! `ExecutionLocationProducer` invocation in
//! `ExecutionRunSpec.initializeBeforeExecutionDatabase`: construct the
//! producer from the run, expand the locations, then re-derive the
//! `states` / `counties` / `zones` / `links` projections.
//!
//! ```
//! use moves_framework::{ExecutionLocationProducer, GeographyTables, LinkRow};
//! use moves_runspec::{GeoKind, GeographicSelection};
//!
//! // One onroad link in county 24001, zone 240010, on road type 2.
//! let geo = GeographyTables::new(
//!     vec![LinkRow {
//!         state_id: 24,
//!         county_id: 24001,
//!         zone_id: 240010,
//!         link_id: 2400100,
//!         road_type_id: 2,
//!     }],
//!     vec![],
//! );
//! let selections = vec![GeographicSelection {
//!     kind: GeoKind::County,
//!     key: 24001,
//!     description: "Test County".into(),
//! }];
//! // Onroad road type 2 is selected; no NONROAD.
//! let producer = ExecutionLocationProducer::new(selections, [2_u32], None);
//! let locations = producer.build_execution_locations(&geo);
//! assert_eq!(locations.len(), 1);
//! ```

use std::collections::BTreeSet;

use moves_runspec::{GeoKind, GeographicSelection};

use super::execution_db::ExecutionLocation;

/// The `roadTypeID` of the NONROAD pseudo road type.
///
/// MOVES has no real "road" for off-highway equipment; the NONROAD model
/// reuses the link machinery with a sentinel road type. The Java producer
/// hard-codes `100` in `buildRoadTypesSQL` (where it sets `hasNonroad`)
/// and in the `addCountyLocations` / `addStateLocations` /
/// `addNationLocations` NONROAD `SELECT`s (`100 as roadTypeID`).
pub const NONROAD_ROAD_TYPE_ID: u32 = 100;

/// One row of the default-DB `Link` table joined to its owning `County`.
///
/// Mirrors the five-column projection every onroad branch of the Java
/// producer `SELECT`s:
/// `County.stateID, County.countyID, Link.zoneID, Link.linkID, Link.roadTypeID`.
/// The `County` ⋈ `Link` join (`County.countyID = Link.countyID`) is done
/// by whoever builds the [`GeographyTables`] — fixtures in Phase 2, the
/// Task 50 data plane later — so a `LinkRow` already carries the full
/// `state → county → zone → link` ancestry of one onroad link.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LinkRow {
    /// `County.stateID` — the state owning the county.
    pub state_id: u32,
    /// `Link.countyID` / `County.countyID` — the county owning the zone.
    pub county_id: u32,
    /// `Link.zoneID` — the zone owning the link.
    pub zone_id: u32,
    /// `Link.linkID` — the link's primary key (globally unique).
    pub link_id: u32,
    /// `Link.roadTypeID` — the link's road type (1–5 for onroad links).
    pub road_type_id: u32,
}

impl LinkRow {
    /// Project this onroad link to its [`ExecutionLocation`].
    ///
    /// Drops `road_type_id`: the produced tuple is `(state, county, zone,
    /// link)` only — see the module docs on road type.
    #[must_use]
    pub fn location(&self) -> ExecutionLocation {
        ExecutionLocation::link(self.state_id, self.county_id, self.zone_id, self.link_id)
    }
}

/// One row of the default-DB `County` table.
///
/// The Java producer's NONROAD branches (`addCountyLocations`,
/// `addStateLocations`, `addNationLocations`) `SELECT` from `County`
/// alone — there is no off-highway `Link` table — so the producer needs
/// the `County` rows independently of the `Link` ⋈ `County` join.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CountyRow {
    /// `County.stateID` — the state owning the county.
    pub state_id: u32,
    /// `County.countyID` — the county's primary key.
    pub county_id: u32,
}

impl CountyRow {
    /// Synthesise the NONROAD [`ExecutionLocation`] for this county.
    ///
    /// Ports the Java NONROAD `SELECT`'s column aliasing
    /// `County.countyID as zoneID, County.countyID as linkID` — the county
    /// id stands in as both the zone id and the link id, so off-highway
    /// equipment iterates one synthetic link per county.
    #[must_use]
    pub fn nonroad_location(&self) -> ExecutionLocation {
        ExecutionLocation::link(
            self.state_id,
            self.county_id,
            self.county_id,
            self.county_id,
        )
    }
}

/// The geography tables the producer reads — the `Link` ⋈ `County` join
/// and the `County` table.
///
/// **Phase 2 input.** The Java producer queries a live database; the Rust
/// port takes the rows as data. Phase 2 callers (and the tests in this
/// module) build a `GeographyTables` from fixtures; Task 50's data plane
/// will build it by scanning the `Link` / `County` Parquet snapshots.
///
/// The Java `addNationLocations` onroad `SELECT` additionally joins the
/// `State` table (`FROM State, County, Link`), but only to drop counties
/// whose state is absent — every [`LinkRow`] already carries a
/// `state_id`, so the port needs no separate state table.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct GeographyTables {
    links: Vec<LinkRow>,
    counties: Vec<CountyRow>,
}

impl GeographyTables {
    /// Build a `GeographyTables` from `Link` ⋈ `County` rows and `County`
    /// rows. Row order is irrelevant — the producer scans both fully and
    /// returns a sorted set.
    #[must_use]
    pub fn new(links: Vec<LinkRow>, counties: Vec<CountyRow>) -> Self {
        Self { links, counties }
    }

    /// The `Link` ⋈ `County` rows — every onroad link with its ancestry.
    #[must_use]
    pub fn links(&self) -> &[LinkRow] {
        &self.links
    }

    /// The `County` rows — the source for NONROAD synthesis.
    #[must_use]
    pub fn counties(&self) -> &[CountyRow] {
        &self.counties
    }

    /// `true` when both tables are empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.links.is_empty() && self.counties.is_empty()
    }
}

/// The road-type filter — port of `ExecutionLocationProducer.buildRoadTypesSQL`.
///
/// The Java method walks the effective road-type set
/// ([`ExecutionRunSpec::execution_road_types`](crate::ExecutionRunSpec::execution_road_types))
/// once and splits it two ways:
///
/// * road type [`NONROAD_ROAD_TYPE_ID`] sets the `hasNonroad` flag and is
///   otherwise dropped;
/// * every other (onroad) road type sets the `hasOnroad` flag and is added
///   to the `roadTypesSQL` `WHERE` fragment
///   (`" AND (roadTypeID=1 OR roadTypeID=2 …)"`).
///
/// [`allows_onroad_road_type`](Self::allows_onroad_road_type) replaces that
/// SQL fragment with a set-membership test; [`has_onroad`](Self::has_onroad)
/// and [`has_nonroad`](Self::has_nonroad) gate the two query families just
/// as Java's `if(hasOnroad)` / `if(hasNonroad)` blocks do.
///
/// An empty road-type selection leaves both flags `false`, so the producer
/// emits no locations at all — faithfully matching Java, where every
/// `add*Locations` method's body sits inside one of those `if` guards.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoadTypeFilter {
    /// The selected onroad (non-NONROAD) road type ids.
    onroad: BTreeSet<u32>,
    /// Whether road type [`NONROAD_ROAD_TYPE_ID`] was selected.
    has_nonroad: bool,
}

impl RoadTypeFilter {
    /// Split a set of road-type ids into the onroad / NONROAD filter.
    ///
    /// Ports `buildRoadTypesSQL`. Duplicate ids collapse (the Java input
    /// is a `TreeSet`); [`NONROAD_ROAD_TYPE_ID`] is recorded as the
    /// `has_nonroad` flag rather than added to the onroad set.
    #[must_use]
    pub fn from_road_type_ids(ids: impl IntoIterator<Item = u32>) -> Self {
        let mut onroad = BTreeSet::new();
        let mut has_nonroad = false;
        for id in ids {
            if id == NONROAD_ROAD_TYPE_ID {
                has_nonroad = true;
            } else {
                onroad.insert(id);
            }
        }
        Self {
            onroad,
            has_nonroad,
        }
    }

    /// Whether any onroad road type was selected — Java's `hasOnroad`.
    ///
    /// Gates the onroad `Link` queries. `false` for a NONROAD-only run.
    #[must_use]
    pub fn has_onroad(&self) -> bool {
        !self.onroad.is_empty()
    }

    /// Whether the NONROAD road type was selected — Java's `hasNonroad`.
    ///
    /// Gates the NONROAD `County` synthesis (county / state / nation
    /// selections only).
    #[must_use]
    pub fn has_nonroad(&self) -> bool {
        self.has_nonroad
    }

    /// Whether an onroad link with this `roadTypeID` survives the filter.
    ///
    /// Replaces the `roadTypesSQL` `WHERE` fragment with set membership.
    #[must_use]
    pub fn allows_onroad_road_type(&self, road_type_id: u32) -> bool {
        self.onroad.contains(&road_type_id)
    }

    /// The selected onroad road type ids, ascending.
    pub fn onroad_road_type_ids(&self) -> impl Iterator<Item = u32> + '_ {
        self.onroad.iter().copied()
    }
}

/// Expands a RunSpec's geographic selections into the set of
/// link-granularity execution locations — port of
/// `ExecutionLocationProducer.java`.
///
/// Construct one with [`new`](Self::new), or let
/// [`ExecutionRunSpec::build_execution_locations`](crate::ExecutionRunSpec::build_execution_locations)
/// construct and drive it. Then call
/// [`build_execution_locations`](Self::build_execution_locations) with the
/// run's [`GeographyTables`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionLocationProducer {
    /// The RunSpec's geographic selections, in RunSpec order.
    geographic_selections: Vec<GeographicSelection>,
    /// When the run is a custom domain, the generic county's id. The Java
    /// `buildExecutionLocations` early-branches on `runSpec.isCustomDomain()`
    /// and expands that single county instead of `geographicSelections`.
    ///
    /// The Rust [`RunSpec`](moves_runspec::RunSpec) does not yet carry the
    /// `genericCounty` field `isCustomDomain` depends on (a Task 12
    /// follow-up), so
    /// [`ExecutionRunSpec::build_execution_locations`](crate::ExecutionRunSpec::build_execution_locations)
    /// always passes `None`. The field and code path are kept so the
    /// custom-domain behaviour is a faithful, tested port the moment the
    /// RunSpec gains the field.
    custom_domain_county: Option<u32>,
    /// The onroad / NONROAD filter derived from the effective road types.
    road_type_filter: RoadTypeFilter,
}

impl ExecutionLocationProducer {
    /// Build a producer from a RunSpec's geographic selections, the
    /// effective road-type ids, and an optional custom-domain county.
    ///
    /// `road_type_ids` is the *effective* set —
    /// [`ExecutionRunSpec::execution_road_types`](crate::ExecutionRunSpec::execution_road_types),
    /// which already applies the Off-Network-Idle expansion. Pass
    /// `custom_domain_county = Some(id)` to expand a single generic county
    /// and ignore `geographic_selections`; `None` for a normal run.
    #[must_use]
    pub fn new(
        geographic_selections: Vec<GeographicSelection>,
        road_type_ids: impl IntoIterator<Item = u32>,
        custom_domain_county: Option<u32>,
    ) -> Self {
        Self {
            geographic_selections,
            custom_domain_county,
            road_type_filter: RoadTypeFilter::from_road_type_ids(road_type_ids),
        }
    }

    /// The onroad / NONROAD road-type filter — Java's `buildRoadTypesSQL`
    /// state. Exposed for inspection and testing.
    #[must_use]
    pub fn road_type_filter(&self) -> &RoadTypeFilter {
        &self.road_type_filter
    }

    /// Expand the selections into the sorted, de-duplicated set of
    /// link-granularity execution locations.
    ///
    /// Ports `buildExecutionLocations(Connection)`: the custom-domain
    /// early branch, then the per-selection dispatch. The returned
    /// [`BTreeSet`] gives the sort + de-duplication the Java `TreeSet`
    /// provided — locations order by `(state, county, zone, link)`, and
    /// overlapping selections (e.g. a `NATION` plus a `STATE`) collapse to
    /// one entry per link.
    #[must_use]
    pub fn build_execution_locations(
        &self,
        geography: &GeographyTables,
    ) -> BTreeSet<ExecutionLocation> {
        let mut results = BTreeSet::new();
        if let Some(county_id) = self.custom_domain_county {
            // `runSpec.isCustomDomain()` branch: expand the generic county.
            self.add_county_locations(county_id, geography, &mut results);
        } else {
            for selection in &self.geographic_selections {
                self.add_for_selection(selection, geography, &mut results);
            }
        }
        results
    }

    /// Dispatch one geographic selection by granularity — ports the
    /// `buildExecutionLocations(GeographicSelection)` `if`/`else` ladder.
    fn add_for_selection(
        &self,
        selection: &GeographicSelection,
        geography: &GeographyTables,
        results: &mut BTreeSet<ExecutionLocation>,
    ) {
        match selection.kind {
            GeoKind::Link => self.add_link_locations(selection.key, geography, results),
            GeoKind::Zone => self.add_zone_locations(selection.key, geography, results),
            GeoKind::County => self.add_county_locations(selection.key, geography, results),
            GeoKind::State => self.add_state_locations(selection.key, geography, results),
            GeoKind::Nation => self.add_nation_locations(geography, results),
        }
    }

    /// Onroad links matching `link_id` — ports `addLinkLocations`.
    ///
    /// There is no NONROAD `Link` table, so a `LINK` selection produces
    /// onroad locations only, even on a NONROAD run.
    fn add_link_locations(
        &self,
        link_id: u32,
        geography: &GeographyTables,
        results: &mut BTreeSet<ExecutionLocation>,
    ) {
        if !self.road_type_filter.has_onroad() {
            return;
        }
        for row in &geography.links {
            if row.link_id == link_id
                && self
                    .road_type_filter
                    .allows_onroad_road_type(row.road_type_id)
            {
                results.insert(row.location());
            }
        }
    }

    /// Onroad links in `zone_id` — ports `addZoneLocations`.
    ///
    /// As with [`add_link_locations`](Self::add_link_locations), a `ZONE`
    /// selection has no NONROAD branch.
    fn add_zone_locations(
        &self,
        zone_id: u32,
        geography: &GeographyTables,
        results: &mut BTreeSet<ExecutionLocation>,
    ) {
        if !self.road_type_filter.has_onroad() {
            return;
        }
        for row in &geography.links {
            if row.zone_id == zone_id
                && self
                    .road_type_filter
                    .allows_onroad_road_type(row.road_type_id)
            {
                results.insert(row.location());
            }
        }
    }

    /// Onroad links in `county_id` plus the NONROAD synthesis — ports
    /// `addCountyLocations`.
    fn add_county_locations(
        &self,
        county_id: u32,
        geography: &GeographyTables,
        results: &mut BTreeSet<ExecutionLocation>,
    ) {
        if self.road_type_filter.has_onroad() {
            for row in &geography.links {
                if row.county_id == county_id
                    && self
                        .road_type_filter
                        .allows_onroad_road_type(row.road_type_id)
                {
                    results.insert(row.location());
                }
            }
        }
        if self.road_type_filter.has_nonroad() {
            for county in &geography.counties {
                if county.county_id == county_id {
                    results.insert(county.nonroad_location());
                }
            }
        }
    }

    /// Onroad links in `state_id` plus the NONROAD synthesis for every
    /// county in the state — ports `addStateLocations`.
    fn add_state_locations(
        &self,
        state_id: u32,
        geography: &GeographyTables,
        results: &mut BTreeSet<ExecutionLocation>,
    ) {
        if self.road_type_filter.has_onroad() {
            for row in &geography.links {
                if row.state_id == state_id
                    && self
                        .road_type_filter
                        .allows_onroad_road_type(row.road_type_id)
                {
                    results.insert(row.location());
                }
            }
        }
        if self.road_type_filter.has_nonroad() {
            for county in &geography.counties {
                if county.state_id == state_id {
                    results.insert(county.nonroad_location());
                }
            }
        }
    }

    /// Every onroad link plus the NONROAD synthesis for every county —
    /// ports `addNationLocations`.
    fn add_nation_locations(
        &self,
        geography: &GeographyTables,
        results: &mut BTreeSet<ExecutionLocation>,
    ) {
        if self.road_type_filter.has_onroad() {
            for row in &geography.links {
                if self
                    .road_type_filter
                    .allows_onroad_road_type(row.road_type_id)
                {
                    results.insert(row.location());
                }
            }
        }
        if self.road_type_filter.has_nonroad() {
            for county in &geography.counties {
                results.insert(county.nonroad_location());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- fixtures ----------------------------------------------------------

    /// A small onroad geography: two counties in state 24, one in state 51.
    /// County 24001 has two links (road types 2 and 3) across two zones;
    /// county 24003 has one link (road type 5); county 51001 has one link
    /// (road type 2).
    fn sample_geography() -> GeographyTables {
        GeographyTables::new(
            vec![
                LinkRow {
                    state_id: 24,
                    county_id: 24001,
                    zone_id: 240010,
                    link_id: 2400100,
                    road_type_id: 2,
                },
                LinkRow {
                    state_id: 24,
                    county_id: 24001,
                    zone_id: 240011,
                    link_id: 2400110,
                    road_type_id: 3,
                },
                LinkRow {
                    state_id: 24,
                    county_id: 24003,
                    zone_id: 240030,
                    link_id: 2400300,
                    road_type_id: 5,
                },
                LinkRow {
                    state_id: 51,
                    county_id: 51001,
                    zone_id: 510010,
                    link_id: 5100100,
                    road_type_id: 2,
                },
            ],
            vec![
                CountyRow {
                    state_id: 24,
                    county_id: 24001,
                },
                CountyRow {
                    state_id: 24,
                    county_id: 24003,
                },
                CountyRow {
                    state_id: 51,
                    county_id: 51001,
                },
            ],
        )
    }

    fn selection(kind: GeoKind, key: u32) -> GeographicSelection {
        GeographicSelection {
            kind,
            key,
            description: String::new(),
        }
    }

    // ---- ExecutionLocation ordering (ports ExecutionLocationTest.java) -----

    #[test]
    fn execution_location_compare_to() {
        // Direct port of `ExecutionLocationTest.testExecLocCompareTo`. The
        // Java fixture builds nine `ExecutionLocation`s (state, county,
        // zone, link) and compares element 0 against each; the produced
        // tuple sorts by `(state, county, zone, link)`, exactly the order
        // the producer's `BTreeSet` output relies on.
        use std::cmp::Ordering;
        let exec_locs = [
            ExecutionLocation::link(2, 2, 2, 2), // 0
            ExecutionLocation::link(2, 2, 2, 1), // 1
            ExecutionLocation::link(2, 2, 2, 3), // 2
            ExecutionLocation::link(2, 2, 1, 1), // 3
            ExecutionLocation::link(2, 2, 3, 3), // 4
            ExecutionLocation::link(2, 1, 4, 1), // 5
            ExecutionLocation::link(2, 3, 1, 4), // 6
            ExecutionLocation::link(1, 1, 1, 4), // 7
            ExecutionLocation::link(3, 1, 1, 4), // 8
        ];
        // Java `expectedOutcomes = {0, 1,-1, 1,-1, 1,-1, 1,-1}`.
        let expected = [
            Ordering::Equal,
            Ordering::Greater,
            Ordering::Less,
            Ordering::Greater,
            Ordering::Less,
            Ordering::Greater,
            Ordering::Less,
            Ordering::Greater,
            Ordering::Less,
        ];
        for (i, want) in expected.iter().enumerate() {
            assert_eq!(
                exec_locs[0].cmp(&exec_locs[i]),
                *want,
                "ExecutionLocation compare to index {i}"
            );
        }
    }

    // ---- RoadTypeFilter (ports buildRoadTypesSQL) --------------------------

    #[test]
    fn road_type_filter_empty_has_neither() {
        let f = RoadTypeFilter::from_road_type_ids([]);
        assert!(!f.has_onroad());
        assert!(!f.has_nonroad());
    }

    #[test]
    fn road_type_filter_onroad_only() {
        let f = RoadTypeFilter::from_road_type_ids([2, 3, 5]);
        assert!(f.has_onroad());
        assert!(!f.has_nonroad());
        assert!(f.allows_onroad_road_type(3));
        assert!(!f.allows_onroad_road_type(4));
        assert_eq!(f.onroad_road_type_ids().collect::<Vec<_>>(), vec![2, 3, 5]);
    }

    #[test]
    fn road_type_filter_nonroad_only() {
        // Road type 100 sets `hasNonroad` and is dropped from the onroad
        // set, so `hasOnroad` stays false — a NONROAD-only run.
        let f = RoadTypeFilter::from_road_type_ids([NONROAD_ROAD_TYPE_ID]);
        assert!(!f.has_onroad());
        assert!(f.has_nonroad());
        assert!(!f.allows_onroad_road_type(NONROAD_ROAD_TYPE_ID));
    }

    #[test]
    fn road_type_filter_mixed_onroad_and_nonroad() {
        let f = RoadTypeFilter::from_road_type_ids([1, 2, NONROAD_ROAD_TYPE_ID]);
        assert!(f.has_onroad());
        assert!(f.has_nonroad());
        assert_eq!(f.onroad_road_type_ids().collect::<Vec<_>>(), vec![1, 2]);
    }

    #[test]
    fn road_type_filter_dedups_repeated_ids() {
        let f = RoadTypeFilter::from_road_type_ids([2, 2, 3, 3, 3]);
        assert_eq!(f.onroad_road_type_ids().collect::<Vec<_>>(), vec![2, 3]);
    }

    // ---- selection dispatch ------------------------------------------------

    #[test]
    fn link_selection_yields_only_that_link() {
        let producer = ExecutionLocationProducer::new(
            vec![selection(GeoKind::Link, 2400100)],
            [2, 3, 5],
            None,
        );
        let locs = producer.build_execution_locations(&sample_geography());
        assert_eq!(
            locs.into_iter().collect::<Vec<_>>(),
            vec![ExecutionLocation::link(24, 24001, 240010, 2400100)]
        );
    }

    #[test]
    fn zone_selection_yields_links_in_zone() {
        // Zone 240010 owns exactly link 2400100.
        let producer =
            ExecutionLocationProducer::new(vec![selection(GeoKind::Zone, 240010)], [2, 3, 5], None);
        let locs = producer.build_execution_locations(&sample_geography());
        assert_eq!(
            locs.into_iter().collect::<Vec<_>>(),
            vec![ExecutionLocation::link(24, 24001, 240010, 2400100)]
        );
    }

    #[test]
    fn county_selection_yields_all_links_in_county() {
        // County 24001 owns two links across two zones.
        let producer = ExecutionLocationProducer::new(
            vec![selection(GeoKind::County, 24001)],
            [2, 3, 5],
            None,
        );
        let locs = producer.build_execution_locations(&sample_geography());
        assert_eq!(
            locs.into_iter().collect::<Vec<_>>(),
            vec![
                ExecutionLocation::link(24, 24001, 240010, 2400100),
                ExecutionLocation::link(24, 24001, 240011, 2400110),
            ]
        );
    }

    #[test]
    fn state_selection_yields_all_links_in_state() {
        // State 24 owns counties 24001 (two links) and 24003 (one link).
        let producer =
            ExecutionLocationProducer::new(vec![selection(GeoKind::State, 24)], [2, 3, 5], None);
        let locs = producer.build_execution_locations(&sample_geography());
        assert_eq!(
            locs.into_iter().collect::<Vec<_>>(),
            vec![
                ExecutionLocation::link(24, 24001, 240010, 2400100),
                ExecutionLocation::link(24, 24001, 240011, 2400110),
                ExecutionLocation::link(24, 24003, 240030, 2400300),
            ]
        );
    }

    #[test]
    fn nation_selection_yields_every_link() {
        let producer =
            ExecutionLocationProducer::new(vec![selection(GeoKind::Nation, 0)], [2, 3, 5], None);
        let locs = producer.build_execution_locations(&sample_geography());
        assert_eq!(locs.len(), 4);
        // BTreeSet ordering: state 24 before state 51.
        assert_eq!(
            locs.into_iter().collect::<Vec<_>>(),
            vec![
                ExecutionLocation::link(24, 24001, 240010, 2400100),
                ExecutionLocation::link(24, 24001, 240011, 2400110),
                ExecutionLocation::link(24, 24003, 240030, 2400300),
                ExecutionLocation::link(51, 51001, 510010, 5100100),
            ]
        );
    }

    // ---- road-type filtering -----------------------------------------------

    #[test]
    fn links_with_unselected_road_type_are_excluded() {
        // Only road type 2 selected: county 24001's road-type-3 link drops.
        let producer =
            ExecutionLocationProducer::new(vec![selection(GeoKind::County, 24001)], [2], None);
        let locs = producer.build_execution_locations(&sample_geography());
        assert_eq!(
            locs.into_iter().collect::<Vec<_>>(),
            vec![ExecutionLocation::link(24, 24001, 240010, 2400100)]
        );
    }

    #[test]
    fn empty_road_types_yield_no_locations() {
        // Both `hasOnroad` and `hasNonroad` false: every `add*` guard
        // fails, so even a NATION selection produces nothing.
        let producer =
            ExecutionLocationProducer::new(vec![selection(GeoKind::Nation, 0)], [], None);
        assert!(producer
            .build_execution_locations(&sample_geography())
            .is_empty());
    }

    // ---- NONROAD synthesis -------------------------------------------------

    #[test]
    fn nonroad_county_selection_synthesises_one_link_per_county() {
        // NONROAD-only run: county 24001 becomes link (24, 24001, 24001,
        // 24001); no onroad links appear.
        let producer = ExecutionLocationProducer::new(
            vec![selection(GeoKind::County, 24001)],
            [NONROAD_ROAD_TYPE_ID],
            None,
        );
        let locs = producer.build_execution_locations(&sample_geography());
        assert_eq!(
            locs.into_iter().collect::<Vec<_>>(),
            vec![ExecutionLocation::link(24, 24001, 24001, 24001)]
        );
    }

    #[test]
    fn nonroad_state_selection_synthesises_links_for_state_counties() {
        let producer = ExecutionLocationProducer::new(
            vec![selection(GeoKind::State, 24)],
            [NONROAD_ROAD_TYPE_ID],
            None,
        );
        let locs = producer.build_execution_locations(&sample_geography());
        assert_eq!(
            locs.into_iter().collect::<Vec<_>>(),
            vec![
                ExecutionLocation::link(24, 24001, 24001, 24001),
                ExecutionLocation::link(24, 24003, 24003, 24003),
            ]
        );
    }

    #[test]
    fn nonroad_nation_selection_synthesises_links_for_every_county() {
        let producer = ExecutionLocationProducer::new(
            vec![selection(GeoKind::Nation, 0)],
            [NONROAD_ROAD_TYPE_ID],
            None,
        );
        let locs = producer.build_execution_locations(&sample_geography());
        assert_eq!(
            locs.into_iter().collect::<Vec<_>>(),
            vec![
                ExecutionLocation::link(24, 24001, 24001, 24001),
                ExecutionLocation::link(24, 24003, 24003, 24003),
                ExecutionLocation::link(51, 51001, 51001, 51001),
            ]
        );
    }

    #[test]
    fn nonroad_zone_and_link_selections_produce_nothing() {
        // Java `addZoneLocations` / `addLinkLocations` have no `hasNonroad`
        // branch — a ZONE or LINK selection on a NONROAD-only run is empty.
        let geo = sample_geography();
        for (kind, key) in [(GeoKind::Zone, 240010), (GeoKind::Link, 2400100)] {
            let producer = ExecutionLocationProducer::new(
                vec![selection(kind, key)],
                [NONROAD_ROAD_TYPE_ID],
                None,
            );
            assert!(
                producer.build_execution_locations(&geo).is_empty(),
                "{kind:?} selection should yield nothing on a NONROAD-only run"
            );
        }
    }

    #[test]
    fn mixed_run_emits_both_onroad_links_and_nonroad_synthesis() {
        // County 24001: two onroad links plus the synthetic NONROAD link.
        let producer = ExecutionLocationProducer::new(
            vec![selection(GeoKind::County, 24001)],
            [2, 3, NONROAD_ROAD_TYPE_ID],
            None,
        );
        let locs = producer.build_execution_locations(&sample_geography());
        assert_eq!(
            locs.into_iter().collect::<Vec<_>>(),
            vec![
                ExecutionLocation::link(24, 24001, 24001, 24001),
                ExecutionLocation::link(24, 24001, 240010, 2400100),
                ExecutionLocation::link(24, 24001, 240011, 2400110),
            ]
        );
    }

    // ---- de-duplication ----------------------------------------------------

    #[test]
    fn overlapping_selections_deduplicate() {
        // A NATION selection plus a redundant STATE selection: the state's
        // links appear once, not twice.
        let producer = ExecutionLocationProducer::new(
            vec![selection(GeoKind::Nation, 0), selection(GeoKind::State, 24)],
            [2, 3, 5],
            None,
        );
        let locs = producer.build_execution_locations(&sample_geography());
        assert_eq!(locs.len(), 4);
    }

    #[test]
    fn no_selections_yield_no_locations() {
        let producer = ExecutionLocationProducer::new(vec![], [2, 3, 5], None);
        assert!(producer
            .build_execution_locations(&sample_geography())
            .is_empty());
    }

    // ---- custom domain -----------------------------------------------------

    #[test]
    fn custom_domain_expands_the_generic_county_and_ignores_selections() {
        // A custom-domain producer ignores `geographic_selections`
        // entirely and expands only the generic county — here 24003.
        let producer = ExecutionLocationProducer::new(
            vec![selection(GeoKind::Nation, 0)],
            [2, 3, 5],
            Some(24003),
        );
        let locs = producer.build_execution_locations(&sample_geography());
        assert_eq!(
            locs.into_iter().collect::<Vec<_>>(),
            vec![ExecutionLocation::link(24, 24003, 240030, 2400300)]
        );
    }

    #[test]
    fn custom_domain_includes_nonroad_synthesis() {
        let producer =
            ExecutionLocationProducer::new(vec![], [2, NONROAD_ROAD_TYPE_ID], Some(24001));
        let locs = producer.build_execution_locations(&sample_geography());
        assert_eq!(
            locs.into_iter().collect::<Vec<_>>(),
            vec![
                ExecutionLocation::link(24, 24001, 24001, 24001),
                ExecutionLocation::link(24, 24001, 240010, 2400100),
            ]
        );
    }

    // ---- accessors ---------------------------------------------------------

    #[test]
    fn geography_tables_accessors() {
        let geo = sample_geography();
        assert_eq!(geo.links().len(), 4);
        assert_eq!(geo.counties().len(), 3);
        assert!(!geo.is_empty());
        assert!(GeographyTables::default().is_empty());
    }

    #[test]
    fn link_and_county_rows_project_to_locations() {
        let link = LinkRow {
            state_id: 24,
            county_id: 24001,
            zone_id: 240010,
            link_id: 2400100,
            road_type_id: 2,
        };
        assert_eq!(
            link.location(),
            ExecutionLocation::link(24, 24001, 240010, 2400100)
        );
        let county = CountyRow {
            state_id: 24,
            county_id: 24001,
        };
        // NONROAD synthesis: county id stands in as zone and link.
        assert_eq!(
            county.nonroad_location(),
            ExecutionLocation::link(24, 24001, 24001, 24001)
        );
    }

    #[test]
    fn producer_exposes_its_road_type_filter() {
        let producer = ExecutionLocationProducer::new(vec![], [1, 2, NONROAD_ROAD_TYPE_ID], None);
        let filter = producer.road_type_filter();
        assert!(filter.has_onroad());
        assert!(filter.has_nonroad());
    }
}
