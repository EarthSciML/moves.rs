//! XML (de)serialization of [`RunSpec`].
//!
//! Maps the canonical model to/from the legacy MOVES `.mrs` / `.xml`
//! format. The element ordering here matches what canonical MOVES emits;
//! the existing characterization fixtures all conform to this layout.
//!
//! This module is deliberately narrow: it covers what Task 13 needs
//! (round-trip through the in-memory model) and what every fixture
//! under `characterization/fixtures/` requires. Task 12 (RunSpec XML
//! parser) extends the coverage to the full 23-file Java source set —
//! validation, GUI-specific elements, and byte-identical re-serialization.

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::model::*;

pub fn parse(xml: &str) -> Result<RunSpec> {
    let raw: XmlRunSpec = quick_xml::de::from_str(xml)?;
    raw.into_model()
}

pub fn to_string(spec: &RunSpec) -> Result<String> {
    let raw = XmlRunSpec::from_model(spec);
    let mut out = String::new();
    quick_xml::se::to_writer(&mut out, &raw)?;
    Ok(out)
}

/// Root `<runspec>` element. Field order matches canonical MOVES output.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename = "runspec")]
struct XmlRunSpec {
    #[serde(rename = "@version", default, skip_serializing_if = "Option::is_none")]
    version: Option<String>,
    description: XmlDescription,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    models: Option<XmlModels>,
    modelscale: XmlValueStr,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    modeldomain: Option<XmlValueStr>,
    geographicselections: XmlGeoSelections,
    timespan: XmlTimespan,
    onroadvehicleselections: XmlOnroadVehicleSelections,
    offroadvehicleselections: XmlOffroadVehicleSelections,
    offroadvehiclesccs: XmlOffroadVehicleSccs,
    roadtypes: XmlRoadTypes,
    pollutantprocessassociations: XmlPollutantProcessAssociations,
    databaseselections: XmlDatabaseSelections,
    internalcontrolstrategies: XmlInternalControlStrategies,
    inputdatabase: XmlDatabaseRef,
    uncertaintyparameters: XmlUncertaintyParameters,
    geographicoutputdetail: XmlGeographicOutputDetail,
    outputemissionsbreakdownselection: XmlOutputBreakdown,
    outputdatabase: XmlDatabaseRef,
    outputtimestep: XmlValueStr,
    outputvmtdata: XmlValueBool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    outputsho: Option<XmlValueBool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    outputsh: Option<XmlValueBool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    outputshp: Option<XmlValueBool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    outputshidling: Option<XmlValueBool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    outputstarts: Option<XmlValueBool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    outputpopulation: Option<XmlValueBool>,
    scaleinputdatabase: XmlDatabaseRef,
    pmsize: XmlValueU32,
    outputfactors: XmlOutputFactors,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct XmlDescription {
    #[serde(rename = "$text", default, skip_serializing_if = "Option::is_none")]
    text: Option<String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct XmlModels {
    #[serde(rename = "model", default, skip_serializing_if = "Vec::is_empty")]
    items: Vec<XmlValueStr>,
}

#[derive(Debug, Serialize, Deserialize)]
struct XmlValueStr {
    #[serde(rename = "@value")]
    value: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct XmlValueU32 {
    #[serde(rename = "@value")]
    value: u32,
}

#[derive(Debug, Serialize, Deserialize)]
struct XmlValueBool {
    #[serde(rename = "@value")]
    value: bool,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct XmlGeoSelections {
    #[serde(
        rename = "geographicselection",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    items: Vec<XmlGeoSelection>,
}

#[derive(Debug, Serialize, Deserialize)]
struct XmlGeoSelection {
    #[serde(rename = "@type")]
    kind: String,
    #[serde(rename = "@key")]
    key: u32,
    #[serde(rename = "@description")]
    description: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct XmlTimespan {
    #[serde(rename = "year", default, skip_serializing_if = "Vec::is_empty")]
    years: Vec<XmlKeyU32>,
    #[serde(rename = "month", default, skip_serializing_if = "Vec::is_empty")]
    months: Vec<XmlKeyU32>,
    #[serde(rename = "day", default, skip_serializing_if = "Vec::is_empty")]
    days: Vec<XmlKeyU32>,
    #[serde(rename = "beginhour", default, skip_serializing_if = "Option::is_none")]
    begin_hour: Option<XmlKeyU32>,
    #[serde(rename = "endhour", default, skip_serializing_if = "Option::is_none")]
    end_hour: Option<XmlKeyU32>,
    #[serde(
        rename = "aggregateBy",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    aggregate_by: Option<XmlKeyStr>,
}

#[derive(Debug, Serialize, Deserialize)]
struct XmlKeyU32 {
    #[serde(rename = "@key")]
    key: u32,
}

#[derive(Debug, Serialize, Deserialize)]
struct XmlKeyStr {
    #[serde(rename = "@key")]
    key: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct XmlOnroadVehicleSelections {
    #[serde(
        rename = "onroadvehicleselection",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    items: Vec<XmlOnroadVehicleSelection>,
}

#[derive(Debug, Serialize, Deserialize)]
struct XmlOnroadVehicleSelection {
    #[serde(rename = "@fueltypeid")]
    fuel_type_id: u32,
    #[serde(rename = "@fueltypedesc")]
    fuel_type_name: String,
    #[serde(rename = "@sourcetypeid")]
    source_type_id: u32,
    #[serde(rename = "@sourcetypename")]
    source_type_name: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct XmlOffroadVehicleSelections {
    #[serde(
        rename = "offroadvehicleselection",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    items: Vec<XmlOffroadVehicleSelection>,
}

#[derive(Debug, Serialize, Deserialize)]
struct XmlOffroadVehicleSelection {
    #[serde(rename = "@fueltypeid")]
    fuel_type_id: u32,
    #[serde(rename = "@fueltypedesc")]
    fuel_type_name: String,
    #[serde(rename = "@sectorid")]
    sector_id: u32,
    #[serde(rename = "@sectorname")]
    sector_name: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct XmlOffroadVehicleSccs {
    #[serde(
        rename = "offroadvehiclescc",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    items: Vec<XmlOffroadVehicleScc>,
}

#[derive(Debug, Serialize, Deserialize)]
struct XmlOffroadVehicleScc {
    #[serde(rename = "@scc")]
    scc: String,
    #[serde(
        rename = "@description",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    description: Option<String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct XmlRoadTypes {
    #[serde(rename = "roadtype", default, skip_serializing_if = "Vec::is_empty")]
    items: Vec<XmlRoadType>,
}

#[derive(Debug, Serialize, Deserialize)]
struct XmlRoadType {
    #[serde(rename = "@roadtypeid")]
    road_type_id: u32,
    #[serde(rename = "@roadtypename")]
    road_type_name: String,
    #[serde(
        rename = "@modelCombination",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    model_combination: Option<String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct XmlPollutantProcessAssociations {
    #[serde(
        rename = "pollutantprocessassociation",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    items: Vec<XmlPollutantProcessAssociation>,
}

#[derive(Debug, Serialize, Deserialize)]
struct XmlPollutantProcessAssociation {
    #[serde(rename = "@pollutantkey")]
    pollutant_id: u32,
    #[serde(rename = "@pollutantname")]
    pollutant_name: String,
    #[serde(rename = "@processkey")]
    process_id: u32,
    #[serde(rename = "@processname")]
    process_name: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct XmlDatabaseSelections {
    #[serde(
        rename = "databaseselection",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    items: Vec<XmlDatabaseSelection>,
}

#[derive(Debug, Serialize, Deserialize)]
struct XmlDatabaseSelection {}

#[derive(Debug, Default, Serialize, Deserialize)]
struct XmlInternalControlStrategies {
    #[serde(
        rename = "internalcontrolstrategy",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    items: Vec<XmlInternalControlStrategy>,
}

#[derive(Debug, Serialize, Deserialize)]
struct XmlInternalControlStrategy {}

#[derive(Debug, Default, Serialize, Deserialize)]
struct XmlDatabaseRef {
    #[serde(rename = "@servername", default)]
    servername: String,
    #[serde(rename = "@databasename", default)]
    databasename: String,
    #[serde(rename = "@description", default)]
    description: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct XmlUncertaintyParameters {
    #[serde(rename = "@uncertaintymodeenabled", default)]
    enabled: bool,
    #[serde(rename = "@numberofrunspersimulation", default)]
    runs_per_simulation: u32,
    #[serde(rename = "@numberofsimulations", default)]
    simulations: u32,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct XmlGeographicOutputDetail {
    #[serde(rename = "@description", default)]
    description: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct XmlOutputBreakdown {
    modelyear: XmlSelected,
    fueltype: XmlSelected,
    emissionprocess: XmlSelected,
    distinguishparticulates: XmlSelected,
    onroadoffroad: XmlSelected,
    roadtype: XmlSelected,
    sourceusetype: XmlSelected,
    movesvehicletype: XmlSelected,
    onroadscc: XmlSelected,
    offroadscc: XmlSelected,
    estimateuncertainty: XmlSelected,
    segment: XmlSelected,
    hpclass: XmlSelected,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct XmlSelected {
    #[serde(rename = "@selected", default)]
    selected: bool,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct XmlOutputFactors {
    timefactors: XmlTimeFactor,
    distancefactors: XmlDistanceFactor,
    massfactors: XmlMassFactor,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct XmlTimeFactor {
    #[serde(rename = "@selected", default)]
    selected: bool,
    #[serde(rename = "@units", default)]
    units: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct XmlDistanceFactor {
    #[serde(rename = "@selected", default)]
    selected: bool,
    #[serde(rename = "@units", default)]
    units: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct XmlMassFactor {
    #[serde(rename = "@selected", default)]
    selected: bool,
    #[serde(rename = "@units", default)]
    units: String,
    #[serde(rename = "@energyunits", default)]
    energy_units: String,
}

// -- Conversion to/from the canonical model -------------------------------

impl XmlRunSpec {
    fn into_model(self) -> Result<RunSpec> {
        let models = self
            .models
            .map(|m| {
                m.items
                    .into_iter()
                    .map(|v| match v.value.as_str() {
                        "ONROAD" => Ok(Model::Onroad),
                        "NONROAD" => Ok(Model::Nonroad),
                        other => Err(Error::InvalidEnumValue {
                            field: "models.model.value",
                            value: other.to_string(),
                        }),
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .transpose()?
            .unwrap_or_default();

        let scale = ModelScale::from_xml_value(&self.modelscale.value).ok_or_else(|| {
            Error::InvalidEnumValue {
                field: "modelscale.value",
                value: self.modelscale.value.clone(),
            }
        })?;

        let domain = self
            .modeldomain
            .map(|d| {
                ModelDomain::from_xml_value(&d.value).ok_or(Error::InvalidEnumValue {
                    field: "modeldomain.value",
                    value: d.value,
                })
            })
            .transpose()?;

        let geographic_selections = self
            .geographicselections
            .items
            .into_iter()
            .map(|x| {
                let kind = GeoKind::from_xml_value(&x.kind).ok_or(Error::InvalidEnumValue {
                    field: "geographicselection.type",
                    value: x.kind,
                })?;
                Ok(GeographicSelection {
                    kind,
                    key: x.key,
                    description: x.description,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let timespan = Timespan {
            years: self.timespan.years.into_iter().map(|x| x.key).collect(),
            months: self.timespan.months.into_iter().map(|x| x.key).collect(),
            days: self.timespan.days.into_iter().map(|x| x.key).collect(),
            begin_hour: self.timespan.begin_hour.map(|x| x.key),
            end_hour: self.timespan.end_hour.map(|x| x.key),
            aggregate_by: self.timespan.aggregate_by.map(|x| x.key),
        };

        let onroad_vehicle_selections = self
            .onroadvehicleselections
            .items
            .into_iter()
            .map(|x| OnroadVehicleSelection {
                fuel_type_id: x.fuel_type_id,
                fuel_type_name: x.fuel_type_name,
                source_type_id: x.source_type_id,
                source_type_name: x.source_type_name,
            })
            .collect();

        let offroad_vehicle_selections = self
            .offroadvehicleselections
            .items
            .into_iter()
            .map(|x| OffroadVehicleSelection {
                fuel_type_id: x.fuel_type_id,
                fuel_type_name: x.fuel_type_name,
                sector_id: x.sector_id,
                sector_name: x.sector_name,
            })
            .collect();

        let offroad_vehicle_sccs = self
            .offroadvehiclesccs
            .items
            .into_iter()
            .map(|x| OffroadVehicleScc {
                scc: x.scc,
                description: x.description,
            })
            .collect();

        let road_types = self
            .roadtypes
            .items
            .into_iter()
            .map(|x| RoadType {
                road_type_id: x.road_type_id,
                road_type_name: x.road_type_name,
                model_combination: x.model_combination,
            })
            .collect();

        let pollutant_process_associations = self
            .pollutantprocessassociations
            .items
            .into_iter()
            .map(|x| PollutantProcessAssociation {
                pollutant_id: x.pollutant_id,
                pollutant_name: x.pollutant_name,
                process_id: x.process_id,
                process_name: x.process_name,
            })
            .collect();

        let database_selections = self
            .databaseselections
            .items
            .into_iter()
            .map(|_| DatabaseSelection {})
            .collect();
        let internal_control_strategies = self
            .internalcontrolstrategies
            .items
            .into_iter()
            .map(|_| InternalControlStrategy {})
            .collect();

        let input_database = DatabaseRef {
            server: self.inputdatabase.servername,
            database: self.inputdatabase.databasename,
            description: self.inputdatabase.description,
        };

        let uncertainty = UncertaintyParameters {
            enabled: self.uncertaintyparameters.enabled,
            runs_per_simulation: self.uncertaintyparameters.runs_per_simulation,
            simulations: self.uncertaintyparameters.simulations,
        };

        let geographic_output_detail =
            GeographicOutputDetail::from_xml_value(&self.geographicoutputdetail.description)
                .ok_or_else(|| Error::InvalidEnumValue {
                    field: "geographicoutputdetail.description",
                    value: self.geographicoutputdetail.description.clone(),
                })?;

        let b = self.outputemissionsbreakdownselection;
        let output_breakdown = OutputBreakdown {
            model_year: b.modelyear.selected,
            fuel_type: b.fueltype.selected,
            emission_process: b.emissionprocess.selected,
            distinguish_particulates: b.distinguishparticulates.selected,
            onroad_offroad: b.onroadoffroad.selected,
            road_type: b.roadtype.selected,
            source_use_type: b.sourceusetype.selected,
            moves_vehicle_type: b.movesvehicletype.selected,
            onroad_scc: b.onroadscc.selected,
            offroad_scc: b.offroadscc.selected,
            estimate_uncertainty: b.estimateuncertainty.selected,
            segment: b.segment.selected,
            hp_class: b.hpclass.selected,
        };

        let output_database = DatabaseRef {
            server: self.outputdatabase.servername,
            database: self.outputdatabase.databasename,
            description: self.outputdatabase.description,
        };

        let output_timestep = OutputTimestep::from_xml_value(&self.outputtimestep.value)
            .ok_or_else(|| Error::InvalidEnumValue {
                field: "outputtimestep.value",
                value: self.outputtimestep.value.clone(),
            })?;

        let output_nonroad = NonroadOutputFlags {
            sho: self.outputsho.map(|v| v.value),
            sh: self.outputsh.map(|v| v.value),
            shp: self.outputshp.map(|v| v.value),
            shidling: self.outputshidling.map(|v| v.value),
            starts: self.outputstarts.map(|v| v.value),
            population: self.outputpopulation.map(|v| v.value),
        };

        let scale_input_database = DatabaseRef {
            server: self.scaleinputdatabase.servername,
            database: self.scaleinputdatabase.databasename,
            description: self.scaleinputdatabase.description,
        };

        let f = self.outputfactors;
        let output_factors = OutputFactors {
            time: TimeFactor {
                enabled: f.timefactors.selected,
                units: TimeUnit::from_xml_value(&f.timefactors.units).ok_or_else(|| {
                    Error::InvalidEnumValue {
                        field: "outputfactors.timefactors.units",
                        value: f.timefactors.units.clone(),
                    }
                })?,
            },
            distance: DistanceFactor {
                enabled: f.distancefactors.selected,
                units: DistanceUnit::from_xml_value(&f.distancefactors.units).ok_or_else(|| {
                    Error::InvalidEnumValue {
                        field: "outputfactors.distancefactors.units",
                        value: f.distancefactors.units.clone(),
                    }
                })?,
            },
            mass: MassFactor {
                enabled: f.massfactors.selected,
                units: MassUnit::from_xml_value(&f.massfactors.units).ok_or_else(|| {
                    Error::InvalidEnumValue {
                        field: "outputfactors.massfactors.units",
                        value: f.massfactors.units.clone(),
                    }
                })?,
                energy_units: EnergyUnit::from_xml_value(&f.massfactors.energy_units).ok_or_else(
                    || Error::InvalidEnumValue {
                        field: "outputfactors.massfactors.energyunits",
                        value: f.massfactors.energy_units.clone(),
                    },
                )?,
            },
        };

        Ok(RunSpec {
            version: self.version,
            description: self.description.text.filter(|s| !s.is_empty()),
            models,
            scale,
            domain,
            geographic_selections,
            timespan,
            onroad_vehicle_selections,
            offroad_vehicle_selections,
            offroad_vehicle_sccs,
            road_types,
            pollutant_process_associations,
            database_selections,
            internal_control_strategies,
            input_database,
            uncertainty,
            geographic_output_detail,
            output_breakdown,
            output_database,
            output_timestep,
            output_vmt_data: self.outputvmtdata.value,
            output_nonroad,
            scale_input_database,
            pm_size: self.pmsize.value,
            output_factors,
        })
    }

    fn from_model(spec: &RunSpec) -> Self {
        let models = if spec.models.is_empty() {
            None
        } else {
            Some(XmlModels {
                items: spec
                    .models
                    .iter()
                    .map(|m| XmlValueStr {
                        value: match m {
                            Model::Onroad => "ONROAD",
                            Model::Nonroad => "NONROAD",
                        }
                        .to_string(),
                    })
                    .collect(),
            })
        };

        XmlRunSpec {
            version: spec.version.clone(),
            description: XmlDescription {
                text: spec.description.clone(),
            },
            models,
            modelscale: XmlValueStr {
                value: spec.scale.xml_value().to_string(),
            },
            modeldomain: spec.domain.map(|d| XmlValueStr {
                value: d.xml_value().to_string(),
            }),
            geographicselections: XmlGeoSelections {
                items: spec
                    .geographic_selections
                    .iter()
                    .map(|g| XmlGeoSelection {
                        kind: g.kind.xml_value().to_string(),
                        key: g.key,
                        description: g.description.clone(),
                    })
                    .collect(),
            },
            timespan: XmlTimespan {
                years: spec
                    .timespan
                    .years
                    .iter()
                    .map(|&k| XmlKeyU32 { key: k })
                    .collect(),
                months: spec
                    .timespan
                    .months
                    .iter()
                    .map(|&k| XmlKeyU32 { key: k })
                    .collect(),
                days: spec
                    .timespan
                    .days
                    .iter()
                    .map(|&k| XmlKeyU32 { key: k })
                    .collect(),
                begin_hour: spec.timespan.begin_hour.map(|k| XmlKeyU32 { key: k }),
                end_hour: spec.timespan.end_hour.map(|k| XmlKeyU32 { key: k }),
                aggregate_by: spec
                    .timespan
                    .aggregate_by
                    .clone()
                    .map(|k| XmlKeyStr { key: k }),
            },
            onroadvehicleselections: XmlOnroadVehicleSelections {
                items: spec
                    .onroad_vehicle_selections
                    .iter()
                    .map(|o| XmlOnroadVehicleSelection {
                        fuel_type_id: o.fuel_type_id,
                        fuel_type_name: o.fuel_type_name.clone(),
                        source_type_id: o.source_type_id,
                        source_type_name: o.source_type_name.clone(),
                    })
                    .collect(),
            },
            offroadvehicleselections: XmlOffroadVehicleSelections {
                items: spec
                    .offroad_vehicle_selections
                    .iter()
                    .map(|o| XmlOffroadVehicleSelection {
                        fuel_type_id: o.fuel_type_id,
                        fuel_type_name: o.fuel_type_name.clone(),
                        sector_id: o.sector_id,
                        sector_name: o.sector_name.clone(),
                    })
                    .collect(),
            },
            offroadvehiclesccs: XmlOffroadVehicleSccs {
                items: spec
                    .offroad_vehicle_sccs
                    .iter()
                    .map(|s| XmlOffroadVehicleScc {
                        scc: s.scc.clone(),
                        description: s.description.clone(),
                    })
                    .collect(),
            },
            roadtypes: XmlRoadTypes {
                items: spec
                    .road_types
                    .iter()
                    .map(|r| XmlRoadType {
                        road_type_id: r.road_type_id,
                        road_type_name: r.road_type_name.clone(),
                        model_combination: r.model_combination.clone(),
                    })
                    .collect(),
            },
            pollutantprocessassociations: XmlPollutantProcessAssociations {
                items: spec
                    .pollutant_process_associations
                    .iter()
                    .map(|p| XmlPollutantProcessAssociation {
                        pollutant_id: p.pollutant_id,
                        pollutant_name: p.pollutant_name.clone(),
                        process_id: p.process_id,
                        process_name: p.process_name.clone(),
                    })
                    .collect(),
            },
            databaseselections: XmlDatabaseSelections {
                items: spec
                    .database_selections
                    .iter()
                    .map(|_| XmlDatabaseSelection {})
                    .collect(),
            },
            internalcontrolstrategies: XmlInternalControlStrategies {
                items: spec
                    .internal_control_strategies
                    .iter()
                    .map(|_| XmlInternalControlStrategy {})
                    .collect(),
            },
            inputdatabase: XmlDatabaseRef {
                servername: spec.input_database.server.clone(),
                databasename: spec.input_database.database.clone(),
                description: spec.input_database.description.clone(),
            },
            uncertaintyparameters: XmlUncertaintyParameters {
                enabled: spec.uncertainty.enabled,
                runs_per_simulation: spec.uncertainty.runs_per_simulation,
                simulations: spec.uncertainty.simulations,
            },
            geographicoutputdetail: XmlGeographicOutputDetail {
                description: spec.geographic_output_detail.xml_value().to_string(),
            },
            outputemissionsbreakdownselection: XmlOutputBreakdown {
                modelyear: XmlSelected {
                    selected: spec.output_breakdown.model_year,
                },
                fueltype: XmlSelected {
                    selected: spec.output_breakdown.fuel_type,
                },
                emissionprocess: XmlSelected {
                    selected: spec.output_breakdown.emission_process,
                },
                distinguishparticulates: XmlSelected {
                    selected: spec.output_breakdown.distinguish_particulates,
                },
                onroadoffroad: XmlSelected {
                    selected: spec.output_breakdown.onroad_offroad,
                },
                roadtype: XmlSelected {
                    selected: spec.output_breakdown.road_type,
                },
                sourceusetype: XmlSelected {
                    selected: spec.output_breakdown.source_use_type,
                },
                movesvehicletype: XmlSelected {
                    selected: spec.output_breakdown.moves_vehicle_type,
                },
                onroadscc: XmlSelected {
                    selected: spec.output_breakdown.onroad_scc,
                },
                offroadscc: XmlSelected {
                    selected: spec.output_breakdown.offroad_scc,
                },
                estimateuncertainty: XmlSelected {
                    selected: spec.output_breakdown.estimate_uncertainty,
                },
                segment: XmlSelected {
                    selected: spec.output_breakdown.segment,
                },
                hpclass: XmlSelected {
                    selected: spec.output_breakdown.hp_class,
                },
            },
            outputdatabase: XmlDatabaseRef {
                servername: spec.output_database.server.clone(),
                databasename: spec.output_database.database.clone(),
                description: spec.output_database.description.clone(),
            },
            outputtimestep: XmlValueStr {
                value: spec.output_timestep.xml_value().to_string(),
            },
            outputvmtdata: XmlValueBool {
                value: spec.output_vmt_data,
            },
            outputsho: spec.output_nonroad.sho.map(|value| XmlValueBool { value }),
            outputsh: spec.output_nonroad.sh.map(|value| XmlValueBool { value }),
            outputshp: spec.output_nonroad.shp.map(|value| XmlValueBool { value }),
            outputshidling: spec
                .output_nonroad
                .shidling
                .map(|value| XmlValueBool { value }),
            outputstarts: spec
                .output_nonroad
                .starts
                .map(|value| XmlValueBool { value }),
            outputpopulation: spec
                .output_nonroad
                .population
                .map(|value| XmlValueBool { value }),
            scaleinputdatabase: XmlDatabaseRef {
                servername: spec.scale_input_database.server.clone(),
                databasename: spec.scale_input_database.database.clone(),
                description: spec.scale_input_database.description.clone(),
            },
            pmsize: XmlValueU32 {
                value: spec.pm_size,
            },
            outputfactors: XmlOutputFactors {
                timefactors: XmlTimeFactor {
                    selected: spec.output_factors.time.enabled,
                    units: spec.output_factors.time.units.xml_value().to_string(),
                },
                distancefactors: XmlDistanceFactor {
                    selected: spec.output_factors.distance.enabled,
                    units: spec.output_factors.distance.units.xml_value().to_string(),
                },
                massfactors: XmlMassFactor {
                    selected: spec.output_factors.mass.enabled,
                    units: spec.output_factors.mass.units.xml_value().to_string(),
                    energy_units: spec
                        .output_factors
                        .mass
                        .energy_units
                        .xml_value()
                        .to_string(),
                },
            },
        }
    }
}
