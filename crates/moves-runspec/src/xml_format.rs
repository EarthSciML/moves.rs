//! XML (de)serialization of [`RunSpec`].
//!
//! Maps the canonical model to/from the legacy MOVES `.mrs` / `.xml`
//! format. The element ordering matches what canonical MOVES emits;
//! all the characterization fixtures under `characterization/fixtures/`
//! conform to this layout.
//!
//! The parser is permissive about whitespace and attribute order; the
//! serializer always emits the canonical Java-style format from
//! `gov.epa.otaq.moves.master.runspec.RunSpecXML.save`: tab indentation,
//! one element per line, CDATA-wrapped non-empty `<description>`, explicit
//! open+close tags for empty container elements, and the same element
//! order MOVES itself writes. That makes serialize → parse → serialize
//! byte-stable: a model round-tripped once produces XML that, when fed
//! back through the parser and re-serialized, is byte-identical.

use std::fmt::Write as _;

use crate::error::{Error, Result};
use crate::model::*;

pub fn parse(xml: &str) -> Result<RunSpec> {
    let raw: XmlRunSpec = quick_xml::de::from_str(xml)?;
    raw.into_model()
}

pub fn to_string(spec: &RunSpec) -> Result<String> {
    let mut out = String::new();
    write_runspec(&mut out, spec).map_err(Error::from_fmt)?;
    Ok(out)
}

// --- Canonical writer ----------------------------------------------------

fn write_runspec(out: &mut String, spec: &RunSpec) -> std::fmt::Result {
    match &spec.version {
        Some(v) => writeln!(out, "<runspec version=\"{}\">", escape_attr(v))?,
        None => writeln!(out, "<runspec>")?,
    }

    write_description(out, spec.description.as_deref())?;
    write_models(out, &spec.models)?;
    writeln!(out, "\t<modelscale value=\"{}\"/>", spec.scale.xml_value())?;
    if let Some(d) = spec.domain {
        writeln!(out, "\t<modeldomain value=\"{}\"/>", d.xml_value())?;
    }
    write_geographic_selections(out, &spec.geographic_selections)?;
    write_timespan(out, &spec.timespan)?;
    write_onroad_selections(out, &spec.onroad_vehicle_selections)?;
    write_offroad_selections(out, &spec.offroad_vehicle_selections)?;
    write_offroad_sccs(out, &spec.offroad_vehicle_sccs)?;
    write_road_types(out, &spec.road_types)?;
    write_pollutant_process_associations(out, &spec.pollutant_process_associations)?;
    write_database_selections(out, &spec.database_selections)?;
    write_internal_control_strategies(out, &spec.internal_control_strategies)?;
    write_database_ref(out, "inputdatabase", &spec.input_database)?;
    write_uncertainty(out, &spec.uncertainty)?;
    writeln!(
        out,
        "\t<geographicoutputdetail description=\"{}\"/>",
        spec.geographic_output_detail.xml_value()
    )?;
    write_output_breakdown(out, &spec.output_breakdown)?;
    write_database_ref(out, "outputdatabase", &spec.output_database)?;
    writeln!(
        out,
        "\t<outputtimestep value=\"{}\"/>",
        spec.output_timestep.xml_value()
    )?;
    writeln!(
        out,
        "\t<outputvmtdata value=\"{}\"/>",
        bool_xml(spec.output_vmt_data)
    )?;
    write_nonroad_output_flags(out, &spec.output_nonroad)?;
    write_database_ref(out, "scaleinputdatabase", &spec.scale_input_database)?;
    writeln!(out, "\t<pmsize value=\"{}\"/>", spec.pm_size)?;
    write_output_factors(out, &spec.output_factors)?;
    writeln!(out, "</runspec>")
}

fn write_description(out: &mut String, desc: Option<&str>) -> std::fmt::Result {
    match desc {
        Some(text) if !text.is_empty() => {
            writeln!(out, "\t<description><![CDATA[{text}]]></description>")
        }
        _ => writeln!(out, "\t<description></description>"),
    }
}

fn write_models(out: &mut String, models: &[Model]) -> std::fmt::Result {
    if models.is_empty() {
        return Ok(());
    }
    writeln!(out, "\t<models>")?;
    for m in models {
        let v = match m {
            Model::Onroad => "ONROAD",
            Model::Nonroad => "NONROAD",
        };
        writeln!(out, "\t\t<model value=\"{v}\"/>")?;
    }
    writeln!(out, "\t</models>")
}

fn write_geographic_selections(
    out: &mut String,
    items: &[GeographicSelection],
) -> std::fmt::Result {
    if items.is_empty() {
        writeln!(out, "\t<geographicselections>")?;
        writeln!(out, "\t</geographicselections>")?;
        return Ok(());
    }
    writeln!(out, "\t<geographicselections>")?;
    for sel in items {
        writeln!(
            out,
            "\t\t<geographicselection type=\"{}\" key=\"{}\" description=\"{}\"/>",
            sel.kind.xml_value(),
            sel.key,
            escape_attr(&sel.description),
        )?;
    }
    writeln!(out, "\t</geographicselections>")
}

fn write_timespan(out: &mut String, ts: &Timespan) -> std::fmt::Result {
    writeln!(out, "\t<timespan>")?;
    for y in &ts.years {
        writeln!(out, "\t\t<year key=\"{y}\"/>")?;
    }
    for m in &ts.months {
        writeln!(out, "\t\t<month key=\"{m}\"/>")?;
    }
    for d in &ts.days {
        writeln!(out, "\t\t<day key=\"{d}\"/>")?;
    }
    if let Some(h) = ts.begin_hour {
        writeln!(out, "\t\t<beginhour key=\"{h}\"/>")?;
    }
    if let Some(h) = ts.end_hour {
        writeln!(out, "\t\t<endhour key=\"{h}\"/>")?;
    }
    if let Some(a) = &ts.aggregate_by {
        writeln!(out, "\t\t<aggregateBy key=\"{}\"/>", escape_attr(a))?;
    }
    writeln!(out, "\t</timespan>")
}

fn write_onroad_selections(out: &mut String, items: &[OnroadVehicleSelection]) -> std::fmt::Result {
    if items.is_empty() {
        writeln!(out, "\t<onroadvehicleselections>")?;
        writeln!(out, "\t</onroadvehicleselections>")?;
        return Ok(());
    }
    writeln!(out, "\t<onroadvehicleselections>")?;
    for v in items {
        writeln!(
            out,
            "\t\t<onroadvehicleselection fueltypeid=\"{}\" fueltypedesc=\"{}\" sourcetypeid=\"{}\" sourcetypename=\"{}\"/>",
            v.fuel_type_id,
            escape_attr(&v.fuel_type_name),
            v.source_type_id,
            escape_attr(&v.source_type_name),
        )?;
    }
    writeln!(out, "\t</onroadvehicleselections>")
}

fn write_offroad_selections(
    out: &mut String,
    items: &[OffroadVehicleSelection],
) -> std::fmt::Result {
    if items.is_empty() {
        writeln!(out, "\t<offroadvehicleselections>")?;
        writeln!(out, "\t</offroadvehicleselections>")?;
        return Ok(());
    }
    writeln!(out, "\t<offroadvehicleselections>")?;
    for v in items {
        writeln!(
            out,
            "\t\t<offroadvehicleselection fueltypeid=\"{}\" fueltypedesc=\"{}\" sectorid=\"{}\" sectorname=\"{}\"/>",
            v.fuel_type_id,
            escape_attr(&v.fuel_type_name),
            v.sector_id,
            escape_attr(&v.sector_name),
        )?;
    }
    writeln!(out, "\t</offroadvehicleselections>")
}

fn write_offroad_sccs(out: &mut String, items: &[OffroadVehicleScc]) -> std::fmt::Result {
    if items.is_empty() {
        writeln!(out, "\t<offroadvehiclesccs>")?;
        writeln!(out, "\t</offroadvehiclesccs>")?;
        return Ok(());
    }
    writeln!(out, "\t<offroadvehiclesccs>")?;
    for s in items {
        match &s.description {
            Some(d) => writeln!(
                out,
                "\t\t<offroadvehiclescc scc=\"{}\" description=\"{}\"/>",
                escape_attr(&s.scc),
                escape_attr(d),
            )?,
            None => writeln!(
                out,
                "\t\t<offroadvehiclescc scc=\"{}\"/>",
                escape_attr(&s.scc),
            )?,
        }
    }
    writeln!(out, "\t</offroadvehiclesccs>")
}

fn write_road_types(out: &mut String, items: &[RoadType]) -> std::fmt::Result {
    if items.is_empty() {
        writeln!(out, "\t<roadtypes>")?;
        writeln!(out, "\t</roadtypes>")?;
        return Ok(());
    }
    writeln!(out, "\t<roadtypes>")?;
    for r in items {
        match &r.model_combination {
            Some(mc) => writeln!(
                out,
                "\t\t<roadtype roadtypeid=\"{}\" roadtypename=\"{}\" modelCombination=\"{}\"/>",
                r.road_type_id,
                escape_attr(&r.road_type_name),
                escape_attr(mc),
            )?,
            None => writeln!(
                out,
                "\t\t<roadtype roadtypeid=\"{}\" roadtypename=\"{}\"/>",
                r.road_type_id,
                escape_attr(&r.road_type_name),
            )?,
        }
    }
    writeln!(out, "\t</roadtypes>")
}

fn write_pollutant_process_associations(
    out: &mut String,
    items: &[PollutantProcessAssociation],
) -> std::fmt::Result {
    if items.is_empty() {
        writeln!(out, "\t<pollutantprocessassociations>")?;
        writeln!(out, "\t</pollutantprocessassociations>")?;
        return Ok(());
    }
    writeln!(out, "\t<pollutantprocessassociations>")?;
    for ppa in items {
        writeln!(
            out,
            "\t\t<pollutantprocessassociation pollutantkey=\"{}\" pollutantname=\"{}\" processkey=\"{}\" processname=\"{}\"/>",
            ppa.pollutant_id,
            escape_attr(&ppa.pollutant_name),
            ppa.process_id,
            escape_attr(&ppa.process_name),
        )?;
    }
    writeln!(out, "\t</pollutantprocessassociations>")
}

fn write_database_selections(out: &mut String, items: &[DatabaseSelection]) -> std::fmt::Result {
    if items.is_empty() {
        writeln!(out, "\t<databaseselections>")?;
        writeln!(out, "\t</databaseselections>")?;
        return Ok(());
    }
    writeln!(out, "\t<databaseselections>")?;
    for _ in items {
        writeln!(out, "\t\t<databaseselection/>")?;
    }
    writeln!(out, "\t</databaseselections>")
}

fn write_internal_control_strategies(
    out: &mut String,
    items: &[InternalControlStrategy],
) -> std::fmt::Result {
    if items.is_empty() {
        writeln!(out, "\t<internalcontrolstrategies>")?;
        writeln!(out, "\t</internalcontrolstrategies>")?;
        return Ok(());
    }
    writeln!(out, "\t<internalcontrolstrategies>")?;
    for _ in items {
        writeln!(out, "\t\t<internalcontrolstrategy/>")?;
    }
    writeln!(out, "\t</internalcontrolstrategies>")
}

fn write_database_ref(out: &mut String, tag: &str, db: &DatabaseRef) -> std::fmt::Result {
    writeln!(
        out,
        "\t<{tag} servername=\"{}\" databasename=\"{}\" description=\"{}\"/>",
        escape_attr(&db.server),
        escape_attr(&db.database),
        escape_attr(&db.description),
    )
}

fn write_uncertainty(out: &mut String, u: &UncertaintyParameters) -> std::fmt::Result {
    writeln!(
        out,
        "\t<uncertaintyparameters uncertaintymodeenabled=\"{}\" numberofrunspersimulation=\"{}\" numberofsimulations=\"{}\"/>",
        bool_xml(u.enabled),
        u.runs_per_simulation,
        u.simulations,
    )
}

fn write_output_breakdown(out: &mut String, b: &OutputBreakdown) -> std::fmt::Result {
    writeln!(out, "\t<outputemissionsbreakdownselection>")?;
    writeln!(
        out,
        "\t\t<modelyear selected=\"{}\"/>",
        bool_xml(b.model_year)
    )?;
    writeln!(
        out,
        "\t\t<fueltype selected=\"{}\"/>",
        bool_xml(b.fuel_type)
    )?;
    writeln!(
        out,
        "\t\t<emissionprocess selected=\"{}\"/>",
        bool_xml(b.emission_process)
    )?;
    writeln!(
        out,
        "\t\t<distinguishparticulates selected=\"{}\"/>",
        bool_xml(b.distinguish_particulates)
    )?;
    writeln!(
        out,
        "\t\t<onroadoffroad selected=\"{}\"/>",
        bool_xml(b.onroad_offroad)
    )?;
    writeln!(
        out,
        "\t\t<roadtype selected=\"{}\"/>",
        bool_xml(b.road_type)
    )?;
    writeln!(
        out,
        "\t\t<sourceusetype selected=\"{}\"/>",
        bool_xml(b.source_use_type)
    )?;
    writeln!(
        out,
        "\t\t<movesvehicletype selected=\"{}\"/>",
        bool_xml(b.moves_vehicle_type)
    )?;
    writeln!(
        out,
        "\t\t<onroadscc selected=\"{}\"/>",
        bool_xml(b.onroad_scc)
    )?;
    writeln!(
        out,
        "\t\t<offroadscc selected=\"{}\"/>",
        bool_xml(b.offroad_scc)
    )?;
    writeln!(
        out,
        "\t\t<estimateuncertainty selected=\"{}\"/>",
        bool_xml(b.estimate_uncertainty)
    )?;
    writeln!(out, "\t\t<segment selected=\"{}\"/>", bool_xml(b.segment))?;
    writeln!(out, "\t\t<hpclass selected=\"{}\"/>", bool_xml(b.hp_class))?;
    writeln!(out, "\t</outputemissionsbreakdownselection>")
}

fn write_nonroad_output_flags(out: &mut String, f: &NonroadOutputFlags) -> std::fmt::Result {
    if let Some(v) = f.sho {
        writeln!(out, "\t<outputsho value=\"{}\"/>", bool_xml(v))?;
    }
    if let Some(v) = f.sh {
        writeln!(out, "\t<outputsh value=\"{}\"/>", bool_xml(v))?;
    }
    if let Some(v) = f.shp {
        writeln!(out, "\t<outputshp value=\"{}\"/>", bool_xml(v))?;
    }
    if let Some(v) = f.shidling {
        writeln!(out, "\t<outputshidling value=\"{}\"/>", bool_xml(v))?;
    }
    if let Some(v) = f.starts {
        writeln!(out, "\t<outputstarts value=\"{}\"/>", bool_xml(v))?;
    }
    if let Some(v) = f.population {
        writeln!(out, "\t<outputpopulation value=\"{}\"/>", bool_xml(v))?;
    }
    Ok(())
}

fn write_output_factors(out: &mut String, f: &OutputFactors) -> std::fmt::Result {
    writeln!(out, "\t<outputfactors>")?;
    writeln!(
        out,
        "\t\t<timefactors selected=\"{}\" units=\"{}\"/>",
        bool_xml(f.time.enabled),
        f.time.units.xml_value(),
    )?;
    writeln!(
        out,
        "\t\t<distancefactors selected=\"{}\" units=\"{}\"/>",
        bool_xml(f.distance.enabled),
        f.distance.units.xml_value(),
    )?;
    writeln!(
        out,
        "\t\t<massfactors selected=\"{}\" units=\"{}\" energyunits=\"{}\"/>",
        bool_xml(f.mass.enabled),
        f.mass.units.xml_value(),
        f.mass.energy_units.xml_value(),
    )?;
    writeln!(out, "\t</outputfactors>")
}

fn bool_xml(b: bool) -> &'static str {
    if b {
        "true"
    } else {
        "false"
    }
}

fn escape_attr(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(c),
        }
    }
    out
}

// --- Parser shadow types -------------------------------------------------

use serde::{Deserialize, Serialize};

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

// -- Conversion from the canonical model ----------------------------------

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
}
