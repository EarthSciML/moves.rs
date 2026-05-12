//! TOML (de)serialization of [`RunSpec`].
//!
//! Defines a shadow [`RunSpecToml`] tree whose key names are deliberately
//! shorter and more idiomatic than the XML element/attribute names
//! (`[[geo]]` instead of `<geographicselections><geographicselection>`,
//! `pollutant` instead of `pollutantname`, etc.) and whose enum values
//! are TOML-style kebab-case (`scale = "macro"`) rather than the legacy
//! XML strings (`MACROSCALE`). See `docs/runspec-toml.md` for the full
//! field-by-field mapping.
//!
//! The TOML shape exists alongside the XML shape so that:
//!
//! 1. Hand-authored RunSpecs use the friendlier surface;
//! 2. The XML compatibility format can mutate independently of the TOML
//!    surface as Task 12 grows it out (additional optional elements,
//!    deprecated attributes, etc.);
//! 3. Both surfaces converge on the canonical [`RunSpec`] model, so a
//!    XML→model→TOML→model→XML round-trip is model-identical by
//!    construction.

use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::model::*;

pub fn parse(input: &str) -> Result<RunSpec> {
    let raw: RunSpecToml = toml::from_str(input)?;
    Ok(raw.into_model())
}

pub fn to_string(spec: &RunSpec) -> Result<String> {
    let raw = RunSpecToml::from_model(spec);
    Ok(toml::to_string_pretty(&raw)?)
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunSpecToml {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    pub run: RunSection,

    #[serde(default, rename = "geo", skip_serializing_if = "Vec::is_empty")]
    pub geographic_selections: Vec<GeoToml>,

    pub time: TimeSection,

    #[serde(default, rename = "onroad", skip_serializing_if = "Vec::is_empty")]
    pub onroad_vehicle_selections: Vec<OnroadToml>,

    #[serde(default, rename = "offroad", skip_serializing_if = "Vec::is_empty")]
    pub offroad_vehicle_selections: Vec<OffroadToml>,

    #[serde(default, rename = "offroad_scc", skip_serializing_if = "Vec::is_empty")]
    pub offroad_vehicle_sccs: Vec<OffroadSccToml>,

    #[serde(default, rename = "road_type", skip_serializing_if = "Vec::is_empty")]
    pub road_types: Vec<RoadTypeToml>,

    #[serde(
        default,
        rename = "pollutant_process",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub pollutant_process: Vec<PollutantProcessToml>,

    #[serde(
        default,
        rename = "database_selection",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub database_selections: Vec<DatabaseSelectionToml>,

    #[serde(
        default,
        rename = "internal_control_strategy",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub internal_control_strategies: Vec<InternalControlStrategyToml>,

    pub input_db: DatabaseRefToml,

    pub uncertainty: UncertaintyToml,

    pub output: OutputSection,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunSection {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub models: Vec<Model>,
    pub scale: ModelScale,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub domain: Option<ModelDomain>,
    #[serde(default)]
    pub pm_size: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GeoToml {
    #[serde(rename = "type")]
    pub kind: GeoKind,
    pub key: u32,
    pub description: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeSection {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub years: Vec<u32>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub months: Vec<u32>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub days: Vec<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub begin_hour: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_hour: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aggregate_by: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnroadToml {
    pub fuel_id: u32,
    pub fuel: String,
    pub source_type_id: u32,
    pub source_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OffroadToml {
    pub fuel_id: u32,
    pub fuel: String,
    pub sector_id: u32,
    pub sector: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OffroadSccToml {
    pub scc: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoadTypeToml {
    pub id: u32,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_combination: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PollutantProcessToml {
    pub pollutant_id: u32,
    pub pollutant: String,
    pub process_id: u32,
    pub process: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatabaseSelectionToml {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InternalControlStrategyToml {}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatabaseRefToml {
    #[serde(default)]
    pub server: String,
    #[serde(default)]
    pub database: String,
    #[serde(default)]
    pub description: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UncertaintyToml {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub runs_per_simulation: u32,
    #[serde(default)]
    pub simulations: u32,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputSection {
    pub detail: GeographicOutputDetail,
    pub timestep: OutputTimestep,
    #[serde(default)]
    pub vmt_data: bool,
    pub db: DatabaseRefToml,
    pub scale_input_db: DatabaseRefToml,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nonroad: Option<NonroadToml>,
    pub breakdown: BreakdownToml,
    pub factors: FactorsToml,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NonroadToml {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sho: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sh: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shp: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shidling: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub starts: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub population: Option<bool>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BreakdownToml {
    #[serde(default)]
    pub model_year: bool,
    #[serde(default)]
    pub fuel_type: bool,
    #[serde(default)]
    pub emission_process: bool,
    #[serde(default)]
    pub distinguish_particulates: bool,
    #[serde(default)]
    pub onroad_offroad: bool,
    #[serde(default)]
    pub road_type: bool,
    #[serde(default)]
    pub source_use_type: bool,
    #[serde(default)]
    pub moves_vehicle_type: bool,
    #[serde(default)]
    pub onroad_scc: bool,
    #[serde(default)]
    pub offroad_scc: bool,
    #[serde(default)]
    pub estimate_uncertainty: bool,
    #[serde(default)]
    pub segment: bool,
    #[serde(default)]
    pub hp_class: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactorsToml {
    pub time: TimeFactorToml,
    pub distance: DistanceFactorToml,
    pub mass: MassFactorToml,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeFactorToml {
    #[serde(default)]
    pub enabled: bool,
    pub units: TimeUnit,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DistanceFactorToml {
    #[serde(default)]
    pub enabled: bool,
    pub units: DistanceUnit,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MassFactorToml {
    #[serde(default)]
    pub enabled: bool,
    pub units: MassUnit,
    pub energy_units: EnergyUnit,
}

// -- Conversions ---------------------------------------------------------

impl RunSpecToml {
    fn into_model(self) -> RunSpec {
        let nonroad = self.output.nonroad.unwrap_or_default();
        RunSpec {
            version: self.version,
            description: self.description.filter(|s| !s.is_empty()),
            models: self.run.models,
            scale: self.run.scale,
            domain: self.run.domain,
            geographic_selections: self
                .geographic_selections
                .into_iter()
                .map(|g| GeographicSelection {
                    kind: g.kind,
                    key: g.key,
                    description: g.description,
                })
                .collect(),
            timespan: Timespan {
                years: self.time.years,
                months: self.time.months,
                days: self.time.days,
                begin_hour: self.time.begin_hour,
                end_hour: self.time.end_hour,
                aggregate_by: self.time.aggregate_by,
            },
            onroad_vehicle_selections: self
                .onroad_vehicle_selections
                .into_iter()
                .map(|o| OnroadVehicleSelection {
                    fuel_type_id: o.fuel_id,
                    fuel_type_name: o.fuel,
                    source_type_id: o.source_type_id,
                    source_type_name: o.source_type,
                })
                .collect(),
            offroad_vehicle_selections: self
                .offroad_vehicle_selections
                .into_iter()
                .map(|o| OffroadVehicleSelection {
                    fuel_type_id: o.fuel_id,
                    fuel_type_name: o.fuel,
                    sector_id: o.sector_id,
                    sector_name: o.sector,
                })
                .collect(),
            offroad_vehicle_sccs: self
                .offroad_vehicle_sccs
                .into_iter()
                .map(|s| OffroadVehicleScc {
                    scc: s.scc,
                    description: s.description,
                })
                .collect(),
            road_types: self
                .road_types
                .into_iter()
                .map(|r| RoadType {
                    road_type_id: r.id,
                    road_type_name: r.name,
                    model_combination: r.model_combination,
                })
                .collect(),
            pollutant_process_associations: self
                .pollutant_process
                .into_iter()
                .map(|p| PollutantProcessAssociation {
                    pollutant_id: p.pollutant_id,
                    pollutant_name: p.pollutant,
                    process_id: p.process_id,
                    process_name: p.process,
                })
                .collect(),
            database_selections: self
                .database_selections
                .into_iter()
                .map(|_| DatabaseSelection {})
                .collect(),
            internal_control_strategies: self
                .internal_control_strategies
                .into_iter()
                .map(|_| InternalControlStrategy {})
                .collect(),
            input_database: DatabaseRef {
                server: self.input_db.server,
                database: self.input_db.database,
                description: self.input_db.description,
            },
            uncertainty: UncertaintyParameters {
                enabled: self.uncertainty.enabled,
                runs_per_simulation: self.uncertainty.runs_per_simulation,
                simulations: self.uncertainty.simulations,
            },
            geographic_output_detail: self.output.detail,
            output_breakdown: OutputBreakdown {
                model_year: self.output.breakdown.model_year,
                fuel_type: self.output.breakdown.fuel_type,
                emission_process: self.output.breakdown.emission_process,
                distinguish_particulates: self.output.breakdown.distinguish_particulates,
                onroad_offroad: self.output.breakdown.onroad_offroad,
                road_type: self.output.breakdown.road_type,
                source_use_type: self.output.breakdown.source_use_type,
                moves_vehicle_type: self.output.breakdown.moves_vehicle_type,
                onroad_scc: self.output.breakdown.onroad_scc,
                offroad_scc: self.output.breakdown.offroad_scc,
                estimate_uncertainty: self.output.breakdown.estimate_uncertainty,
                segment: self.output.breakdown.segment,
                hp_class: self.output.breakdown.hp_class,
            },
            output_database: DatabaseRef {
                server: self.output.db.server,
                database: self.output.db.database,
                description: self.output.db.description,
            },
            output_timestep: self.output.timestep,
            output_vmt_data: self.output.vmt_data,
            output_nonroad: NonroadOutputFlags {
                sho: nonroad.sho,
                sh: nonroad.sh,
                shp: nonroad.shp,
                shidling: nonroad.shidling,
                starts: nonroad.starts,
                population: nonroad.population,
            },
            scale_input_database: DatabaseRef {
                server: self.output.scale_input_db.server,
                database: self.output.scale_input_db.database,
                description: self.output.scale_input_db.description,
            },
            pm_size: self.run.pm_size,
            output_factors: OutputFactors {
                time: TimeFactor {
                    enabled: self.output.factors.time.enabled,
                    units: self.output.factors.time.units,
                },
                distance: DistanceFactor {
                    enabled: self.output.factors.distance.enabled,
                    units: self.output.factors.distance.units,
                },
                mass: MassFactor {
                    enabled: self.output.factors.mass.enabled,
                    units: self.output.factors.mass.units,
                    energy_units: self.output.factors.mass.energy_units,
                },
            },
        }
    }

    fn from_model(spec: &RunSpec) -> Self {
        let output_nonroad = if spec.output_nonroad.is_present() {
            Some(NonroadToml {
                sho: spec.output_nonroad.sho,
                sh: spec.output_nonroad.sh,
                shp: spec.output_nonroad.shp,
                shidling: spec.output_nonroad.shidling,
                starts: spec.output_nonroad.starts,
                population: spec.output_nonroad.population,
            })
        } else {
            None
        };

        RunSpecToml {
            version: spec.version.clone(),
            description: spec.description.clone(),
            run: RunSection {
                models: spec.models.clone(),
                scale: spec.scale,
                domain: spec.domain,
                pm_size: spec.pm_size,
            },
            geographic_selections: spec
                .geographic_selections
                .iter()
                .map(|g| GeoToml {
                    kind: g.kind,
                    key: g.key,
                    description: g.description.clone(),
                })
                .collect(),
            time: TimeSection {
                years: spec.timespan.years.clone(),
                months: spec.timespan.months.clone(),
                days: spec.timespan.days.clone(),
                begin_hour: spec.timespan.begin_hour,
                end_hour: spec.timespan.end_hour,
                aggregate_by: spec.timespan.aggregate_by.clone(),
            },
            onroad_vehicle_selections: spec
                .onroad_vehicle_selections
                .iter()
                .map(|o| OnroadToml {
                    fuel_id: o.fuel_type_id,
                    fuel: o.fuel_type_name.clone(),
                    source_type_id: o.source_type_id,
                    source_type: o.source_type_name.clone(),
                })
                .collect(),
            offroad_vehicle_selections: spec
                .offroad_vehicle_selections
                .iter()
                .map(|o| OffroadToml {
                    fuel_id: o.fuel_type_id,
                    fuel: o.fuel_type_name.clone(),
                    sector_id: o.sector_id,
                    sector: o.sector_name.clone(),
                })
                .collect(),
            offroad_vehicle_sccs: spec
                .offroad_vehicle_sccs
                .iter()
                .map(|s| OffroadSccToml {
                    scc: s.scc.clone(),
                    description: s.description.clone(),
                })
                .collect(),
            road_types: spec
                .road_types
                .iter()
                .map(|r| RoadTypeToml {
                    id: r.road_type_id,
                    name: r.road_type_name.clone(),
                    model_combination: r.model_combination.clone(),
                })
                .collect(),
            pollutant_process: spec
                .pollutant_process_associations
                .iter()
                .map(|p| PollutantProcessToml {
                    pollutant_id: p.pollutant_id,
                    pollutant: p.pollutant_name.clone(),
                    process_id: p.process_id,
                    process: p.process_name.clone(),
                })
                .collect(),
            database_selections: spec
                .database_selections
                .iter()
                .map(|_| DatabaseSelectionToml {})
                .collect(),
            internal_control_strategies: spec
                .internal_control_strategies
                .iter()
                .map(|_| InternalControlStrategyToml {})
                .collect(),
            input_db: DatabaseRefToml {
                server: spec.input_database.server.clone(),
                database: spec.input_database.database.clone(),
                description: spec.input_database.description.clone(),
            },
            uncertainty: UncertaintyToml {
                enabled: spec.uncertainty.enabled,
                runs_per_simulation: spec.uncertainty.runs_per_simulation,
                simulations: spec.uncertainty.simulations,
            },
            output: OutputSection {
                detail: spec.geographic_output_detail,
                timestep: spec.output_timestep,
                vmt_data: spec.output_vmt_data,
                db: DatabaseRefToml {
                    server: spec.output_database.server.clone(),
                    database: spec.output_database.database.clone(),
                    description: spec.output_database.description.clone(),
                },
                scale_input_db: DatabaseRefToml {
                    server: spec.scale_input_database.server.clone(),
                    database: spec.scale_input_database.database.clone(),
                    description: spec.scale_input_database.description.clone(),
                },
                nonroad: output_nonroad,
                breakdown: BreakdownToml {
                    model_year: spec.output_breakdown.model_year,
                    fuel_type: spec.output_breakdown.fuel_type,
                    emission_process: spec.output_breakdown.emission_process,
                    distinguish_particulates: spec.output_breakdown.distinguish_particulates,
                    onroad_offroad: spec.output_breakdown.onroad_offroad,
                    road_type: spec.output_breakdown.road_type,
                    source_use_type: spec.output_breakdown.source_use_type,
                    moves_vehicle_type: spec.output_breakdown.moves_vehicle_type,
                    onroad_scc: spec.output_breakdown.onroad_scc,
                    offroad_scc: spec.output_breakdown.offroad_scc,
                    estimate_uncertainty: spec.output_breakdown.estimate_uncertainty,
                    segment: spec.output_breakdown.segment,
                    hp_class: spec.output_breakdown.hp_class,
                },
                factors: FactorsToml {
                    time: TimeFactorToml {
                        enabled: spec.output_factors.time.enabled,
                        units: spec.output_factors.time.units,
                    },
                    distance: DistanceFactorToml {
                        enabled: spec.output_factors.distance.enabled,
                        units: spec.output_factors.distance.units,
                    },
                    mass: MassFactorToml {
                        enabled: spec.output_factors.mass.enabled,
                        units: spec.output_factors.mass.units,
                        energy_units: spec.output_factors.mass.energy_units,
                    },
                },
            },
        }
    }
}
