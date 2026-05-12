//! XML → [`RunSpec`] parser.
//!
//! Implements the same element-by-element walk as
//! `gov.epa.otaq.moves.master.runspec.RunSpecXML.load`. The parser is
//! attribute-driven and tolerant of unknown elements/attributes so it can
//! round-trip the existing fixtures even when they include MOVES5-specific
//! tags the Java code ignores.

use std::str;

use quick_xml::events::attributes::{Attribute, Attributes};
use quick_xml::events::Event;
use quick_xml::name::QName;
use quick_xml::reader::Reader;

use crate::error::{Error, Result};
use crate::types::*;

/// Parses a RunSpec from an XML byte slice.
pub fn parse_runspec(xml: &[u8]) -> Result<RunSpec> {
    let mut reader = Reader::from_reader(xml);
    let cfg = reader.config_mut();
    cfg.trim_text(false);
    cfg.expand_empty_elements = false;

    let mut buf = Vec::new();
    let spec: Option<RunSpec>;

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) if eq_ci(e.name(), b"runspec") => {
                let mut rs = RunSpec {
                    version: attr_string(&e.attributes(), b"version")?.unwrap_or_default(),
                    ..RunSpec::default()
                };
                parse_runspec_children(&mut reader, &mut rs)?;
                spec = Some(rs);
                break;
            }
            Event::Empty(e) if eq_ci(e.name(), b"runspec") => {
                let rs = RunSpec {
                    version: attr_string(&e.attributes(), b"version")?.unwrap_or_default(),
                    ..RunSpec::default()
                };
                spec = Some(rs);
                break;
            }
            Event::Eof => return Err(Error::Malformed("missing <runspec> root".into())),
            Event::Start(e) | Event::Empty(e) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).into_owned();
                return Err(Error::RootMismatch(name));
            }
            _ => {} // skip prolog/whitespace
        }
        buf.clear();
    }
    spec.ok_or_else(|| Error::Malformed("missing <runspec> root".into()))
}

fn parse_runspec_children(reader: &mut Reader<&[u8]>, spec: &mut RunSpec) -> Result<()> {
    let mut buf = Vec::new();
    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) => {
                let name = local_name(e.name()).to_ascii_lowercase();
                let attrs_snapshot = clone_attributes(&e);
                handle_block(reader, spec, &name, &attrs_snapshot)?;
            }
            Event::Empty(e) => {
                let name = local_name(e.name()).to_ascii_lowercase();
                let attrs_snapshot = clone_attributes(&e);
                handle_empty(spec, &name, &attrs_snapshot)?;
            }
            Event::End(e) if eq_ci(e.name(), b"runspec") => return Ok(()),
            Event::Eof => return Ok(()),
            _ => {}
        }
        buf.clear();
    }
}

fn handle_block(
    reader: &mut Reader<&[u8]>,
    spec: &mut RunSpec,
    name: &str,
    attrs: &[(String, String)],
) -> Result<()> {
    match name {
        "description" => {
            spec.description = read_text(reader, "description")?;
        }
        "models" => {
            spec.models.clear();
            walk_children(reader, "models", |child_name, child_attrs, _| {
                if child_name == "model" {
                    if let Some(v) = get_attr(child_attrs, "value") {
                        if let Some(m) = Model::from_str_ci(&v) {
                            spec.models.push(m);
                        }
                    }
                }
                Ok(())
            })?;
        }
        "geographicselections" => {
            walk_children(
                reader,
                "geographicselections",
                |child_name, child_attrs, _| {
                    if child_name == "geographicselection" {
                        let type_str = get_attr(child_attrs, "type").unwrap_or_default();
                        let key = parse_int_attr(child_attrs, "key")?;
                        let desc = get_attr(child_attrs, "description").unwrap_or_default();
                        let ty = GeographicSelectionType::from_str_ci(&type_str);
                        if let Some(ty) = ty {
                            if ty == GeographicSelectionType::Nation
                                || (key != 0 && !desc.is_empty())
                            {
                                spec.geographic_selections.insert(GeographicSelection {
                                    type_: ty,
                                    database_key: key,
                                    text_description: desc,
                                });
                            }
                        }
                    }
                    Ok(())
                },
            )?;
        }
        "timespan" => {
            let mut ts = TimeSpan::default();
            walk_children(reader, "timespan", |child_name, child_attrs, _| {
                match child_name.as_str() {
                    "year" => {
                        let key = parse_int_attr(child_attrs, "key")?;
                        if key != 0 {
                            ts.years.insert(key);
                        }
                    }
                    "month" | "day" => {
                        // Java accepts both `key` (index-into-list) and `id`
                        // (database id). With no runtime DB the two indices
                        // are interchangeable here; preserve whichever the
                        // source supplied so round-trips stay byte-stable.
                        let id = parse_int_attr(child_attrs, "key")
                            .ok()
                            .filter(|v| *v != 0)
                            .or_else(|| parse_int_attr(child_attrs, "id").ok())
                            .unwrap_or(0);
                        if id != 0 {
                            if child_name == "month" {
                                ts.months.insert(id);
                            } else {
                                ts.days.insert(id);
                            }
                        }
                    }
                    "beginhour" | "endhour" => {
                        let id = parse_int_attr(child_attrs, "key")
                            .ok()
                            .filter(|v| *v != 0)
                            .or_else(|| parse_int_attr(child_attrs, "id").ok())
                            .unwrap_or(0);
                        if id != 0 {
                            if child_name == "beginhour" {
                                ts.begin_hour_id = id;
                            } else {
                                ts.end_hour_id = id;
                            }
                        }
                    }
                    "aggregateby" => {
                        if let Some(v) = get_attr(child_attrs, "key") {
                            ts.aggregate_by = OutputTimeStep::from_str_ci(&v);
                        }
                    }
                    _ => {}
                }
                Ok(())
            })?;
            spec.time_span = ts;
        }
        "onroadvehicleselections" => {
            walk_children(
                reader,
                "onroadvehicleselections",
                |child_name, child_attrs, _| {
                    if child_name == "onroadvehicleselection" {
                        let mut sel = OnRoadVehicleSelection {
                            fuel_type_id: parse_int_attr(child_attrs, "fueltypeid")?,
                            fuel_type_desc: get_attr(child_attrs, "fueltypedesc")
                                .unwrap_or_default(),
                            source_type_id: parse_int_attr(child_attrs, "sourcetypeid")?,
                            source_type_name: get_attr(child_attrs, "sourcetypename")
                                .unwrap_or_default(),
                        };
                        if sel.source_type_name.eq_ignore_ascii_case("Intercity Bus") {
                            sel.source_type_name = "Other Buses".to_string();
                            spec.had_intercity_buses = true;
                        }
                        if sel.fuel_type_id != 0 && sel.source_type_id != 0 {
                            spec.onroad_vehicle_selections.insert(sel);
                        }
                    }
                    Ok(())
                },
            )?;
        }
        "offroadvehicleselections" => {
            walk_children(
                reader,
                "offroadvehicleselections",
                |child_name, child_attrs, _| {
                    if child_name == "offroadvehicleselection" {
                        let sel = OffRoadVehicleSelection {
                            fuel_type_id: parse_int_attr(child_attrs, "fueltypeid")?,
                            fuel_type_desc: get_attr(child_attrs, "fueltypedesc")
                                .unwrap_or_default(),
                            sector_id: parse_int_attr(child_attrs, "sectorid")?,
                            sector_name: get_attr(child_attrs, "sectorname").unwrap_or_default(),
                        };
                        if sel.fuel_type_id != 0 && sel.sector_id != 0 {
                            spec.offroad_vehicle_selections.insert(sel);
                        }
                    }
                    Ok(())
                },
            )?;
        }
        "offroadvehiclesccs" => {
            walk_children(
                reader,
                "offroadvehiclesccs",
                |child_name, child_attrs, _| {
                    if child_name == "scc" {
                        if let Some(code) = get_attr(child_attrs, "code") {
                            if !code.is_empty() {
                                spec.offroad_vehicle_sccs.insert(Scc { code });
                            }
                        }
                    }
                    Ok(())
                },
            )?;
        }
        "roadtypes" => {
            walk_children(reader, "roadtypes", |child_name, child_attrs, _| {
                if child_name == "roadtype" {
                    let id = parse_int_attr(child_attrs, "roadtypeid")?;
                    if id > 0 {
                        let name = get_attr(child_attrs, "roadtypename").unwrap_or_default();
                        let mc = get_attr(child_attrs, "modelcombination")
                            .map(|s| ModelCombination::from_str_ci(&s))
                            .unwrap_or(ModelCombination::M1);
                        spec.road_types.insert(RoadType {
                            road_type_id: id,
                            road_type_name: name,
                            model_combination: mc,
                        });
                    }
                }
                Ok(())
            })?;
        }
        "pollutantprocessassociations" => {
            walk_children(
                reader,
                "pollutantprocessassociations",
                |child_name, child_attrs, _| {
                    if child_name == "pollutantprocessassociation" {
                        let pn = get_attr(child_attrs, "pollutantname").unwrap_or_default();
                        let pk = parse_int_attr(child_attrs, "pollutantkey")?;
                        let proc_n = get_attr(child_attrs, "processname").unwrap_or_default();
                        let proc_k = parse_int_attr(child_attrs, "processkey")?;
                        if !pn.is_empty() && pk != 0 && !proc_n.is_empty() && proc_k != 0 {
                            spec.pollutant_process_associations.insert(
                                PollutantProcessAssociation {
                                    pollutant_key: pk,
                                    pollutant_name: pn,
                                    process_key: proc_k,
                                    process_name: proc_n,
                                },
                            );
                        }
                    }
                    Ok(())
                },
            )?;
        }
        "databaseselections" => {
            walk_children(
                reader,
                "databaseselections",
                |child_name, child_attrs, _| {
                    if child_name == "databaseselection" {
                        let server = get_attr(child_attrs, "servername");
                        let dbname = get_attr(child_attrs, "databasename");
                        let descr = get_attr(child_attrs, "description").unwrap_or_default();
                        if let (Some(server), Some(dbname)) = (server, dbname) {
                            spec.database_selections.push(DatabaseSelection {
                                server_name: server,
                                database_name: dbname,
                                description: descr,
                            });
                        }
                    }
                    Ok(())
                },
            )?;
        }
        "internalcontrolstrategies" => {
            // Each <internalcontrolstrategy classname="...">body</internalcontrolstrategy>
            // is read as opaque text. We don't interpret the bodies — they're
            // round-tripped verbatim so the larger XML remains stable.
            parse_internal_control_strategies(reader, spec)?;
        }
        "outputemissionsbreakdownselection" => {
            let mut sel = OutputEmissionsBreakdownSelection::default();
            walk_children(
                reader,
                "outputemissionsbreakdownselection",
                |child_name, child_attrs, _| {
                    match child_name.as_str() {
                        "modelyear" => sel.model_year = bool_attr(child_attrs, "selected"),
                        "fueltype" => sel.fuel_type = bool_attr(child_attrs, "selected"),
                        "fuelsubtype" => {
                            sel.fuel_sub_type = bool_attr(child_attrs, "selected");
                            sel.had_fuel_sub_type = true;
                        }
                        "emissionprocess" => {
                            sel.emission_process = bool_attr(child_attrs, "selected")
                        }
                        "onroadoffroad" => sel.onroad_offroad = bool_attr(child_attrs, "selected"),
                        "roadtype" => sel.road_type = bool_attr(child_attrs, "selected"),
                        "sourceusetype" => sel.source_use_type = bool_attr(child_attrs, "selected"),
                        "movesvehicletype" => {
                            sel.moves_vehicle_type = bool_attr(child_attrs, "selected")
                        }
                        "onroadscc" => sel.onroad_scc = bool_attr(child_attrs, "selected"),
                        "offroadscc" => {
                            // Ignored by Java; we don't track it.
                        }
                        "estimateuncertainty" => {
                            sel.estimate_uncertainty = bool_attr(child_attrs, "selected");
                            sel.number_of_iterations =
                                parse_int_attr_or(child_attrs, "numberOfIterations", 2);
                            sel.keep_sampled_data = bool_attr(child_attrs, "keepSampledData");
                            sel.keep_iterations = bool_attr(child_attrs, "keepIterations");
                        }
                        "sector" | "segment" => sel.sector = bool_attr(child_attrs, "selected"),
                        "engtechid" => {
                            sel.eng_tech_id = bool_attr(child_attrs, "selected");
                            sel.had_eng_tech_id = true;
                        }
                        "hpclass" => sel.hp_class = bool_attr(child_attrs, "selected"),
                        "regclassid" => {
                            sel.reg_class_id = bool_attr(child_attrs, "selected");
                            sel.had_reg_class_id = true;
                        }
                        "distinguishparticulates" => {
                            // Used by the GUI only; flag it as understood so
                            // the round-trip preserves it via fall-through.
                        }
                        _ => {}
                    }
                    Ok(())
                },
            )?;
            spec.output_emissions_breakdown_selection = sel;
        }
        "outputfactors" => {
            let mut of = OutputFactors::default();
            walk_children(reader, "outputfactors", |child_name, child_attrs, _| {
                match child_name.as_str() {
                    "timefactors" => {
                        of.time_factors_selected = bool_attr(child_attrs, "selected");
                        of.time_measurement_system = get_attr(child_attrs, "units")
                            .and_then(|u| TimeMeasurementSystem::from_str_ci(&u));
                    }
                    "distancefactors" => {
                        of.distance_factors_selected = bool_attr(child_attrs, "selected");
                        of.distance_measurement_system = get_attr(child_attrs, "units")
                            .and_then(|u| DistanceMeasurementSystem::from_str_ci(&u));
                    }
                    "massfactors" => {
                        of.mass_factors_selected = bool_attr(child_attrs, "selected");
                        of.mass_measurement_system = get_attr(child_attrs, "units")
                            .and_then(|u| MassMeasurementSystem::from_str_ci(&u));
                        of.energy_measurement_system = get_attr(child_attrs, "energyunits")
                            .and_then(|u| EnergyMeasurementSystem::from_str_ci(&u));
                    }
                    _ => {}
                }
                Ok(())
            })?;
            spec.output_factors = of;
        }
        "savedata" => {
            walk_children(reader, "savedata", |child_name, child_attrs, _| {
                if child_name == "class" {
                    if let Some(n) = get_attr(child_attrs, "name") {
                        spec.classes_to_save_data.push(n);
                    }
                }
                Ok(())
            })?;
        }
        "donotexecute" => {
            walk_children(reader, "donotexecute", |child_name, child_attrs, _| {
                if child_name == "class" {
                    if let Some(n) = get_attr(child_attrs, "name") {
                        spec.classes_not_to_execute.push(n);
                    }
                }
                Ok(())
            })?;
        }
        "genericcounty" => {
            let mut g = GenericCounty {
                short_county_id: 0,
                state_id: 0,
                description: String::new(),
                high_altitude: false,
                gpa_fraction: 0.0,
                barometric_pressure: 0.0,
                refueling_vapor_program_adjust: 0.0,
                refueling_spill_program_adjust: 0.0,
            };
            walk_children(reader, "genericcounty", |child_name, child_attrs, _| {
                match child_name.as_str() {
                    "shortid" => g.short_county_id = parse_int_attr_or(child_attrs, "value", 0),
                    "stateid" => g.state_id = parse_int_attr_or(child_attrs, "value", 0),
                    "description" => {
                        g.description = get_attr(child_attrs, "value").unwrap_or_default()
                    }
                    "altitude" => {
                        g.high_altitude = get_attr(child_attrs, "value")
                            .map(|v| v.eq_ignore_ascii_case("H"))
                            .unwrap_or(false)
                    }
                    "gpafraction" => {
                        g.gpa_fraction = parse_float_attr_or(child_attrs, "value", 0.0)
                    }
                    "barometricpressure" => {
                        g.barometric_pressure = parse_float_attr_or(child_attrs, "value", 0.0)
                    }
                    "refuelvaporadjust" => {
                        g.refueling_vapor_program_adjust =
                            parse_float_attr_or(child_attrs, "value", 0.0)
                    }
                    "refuelspilladjust" => {
                        g.refueling_spill_program_adjust =
                            parse_float_attr_or(child_attrs, "value", 0.0)
                    }
                    _ => {}
                }
                Ok(())
            })?;
            if (1..=999).contains(&g.short_county_id)
                && (1..=99).contains(&g.state_id)
                && (0.0..=1.0).contains(&g.gpa_fraction)
                && g.barometric_pressure >= 0.0
            {
                spec.generic_county = Some(g);
            }
        }
        // Trivial leaf nodes that were opened (rather than empty) — skip the
        // body and let the End event close them naturally.
        "modelscale"
        | "modeldomain"
        | "inputdatabase"
        | "outputdatabase"
        | "outputtimestep"
        | "outputvmtdata"
        | "outputsho"
        | "outputsh"
        | "outputshp"
        | "outputshidling"
        | "outputstarts"
        | "outputpopulation"
        | "scaleinputdatabase"
        | "pmsize"
        | "uncertaintyparameters"
        | "geographicoutputdetail"
        | "generatordatabase"
        | "donotperformfinalaggregation"
        | "lookuptableflags"
        | "skipdomaindatabasevalidation" => {
            handle_empty(spec, name, attrs)?;
            skip_to_close(reader, name)?;
        }
        _ => {
            // Unknown element — skip body.
            skip_to_close(reader, name)?;
        }
    }
    Ok(())
}

fn handle_empty(spec: &mut RunSpec, name: &str, attrs: &[(String, String)]) -> Result<()> {
    match name {
        "modelscale" => {
            if let Some(v) = get_attr(attrs, "value") {
                spec.scale = ModelScale::from_str_ci(&v);
            }
        }
        "modeldomain" => {
            if let Some(v) = get_attr(attrs, "value") {
                spec.domain = ModelDomain::from_str_ci(&v);
            }
        }
        "inputdatabase" => {
            spec.input_database = DatabaseSelection {
                server_name: get_attr(attrs, "servername").unwrap_or_default(),
                database_name: get_attr(attrs, "databasename").unwrap_or_default(),
                description: get_attr(attrs, "description").unwrap_or_default(),
            };
        }
        "outputdatabase" => {
            spec.output_database = DatabaseSelection {
                server_name: get_attr(attrs, "servername").unwrap_or_default(),
                database_name: get_attr(attrs, "databasename").unwrap_or_default(),
                description: get_attr(attrs, "description").unwrap_or_default(),
            };
        }
        "scaleinputdatabase" => {
            spec.scale_input_database = DatabaseSelection {
                server_name: get_attr(attrs, "servername").unwrap_or_default(),
                database_name: get_attr(attrs, "databasename").unwrap_or_default(),
                description: get_attr(attrs, "description").unwrap_or_default(),
            };
        }
        "uncertaintyparameters" => {
            spec.uncertainty_parameters = UncertaintyParameters {
                uncertainty_mode_enabled: bool_attr(attrs, "uncertaintymodeenabled"),
                number_of_runs_per_simulation: parse_int_attr_or(
                    attrs,
                    "numberofrunspersimulation",
                    0,
                ),
                number_of_simulations: parse_int_attr_or(attrs, "numberofsimulations", 0),
            };
        }
        "geographicoutputdetail" => {
            if let Some(v) = get_attr(attrs, "description") {
                spec.geographic_output_detail = GeographicOutputDetailLevel::from_str_ci(&v);
            }
        }
        "outputtimestep" => {
            if let Some(v) = get_attr(attrs, "value") {
                spec.output_time_step = OutputTimeStep::from_str_ci(&v);
            }
        }
        "outputvmtdata" => spec.output_vmt_data = value_bool(attrs),
        "outputsho" => spec.output_sho = OptionalBool::present(value_bool(attrs)),
        "outputsh" => spec.output_sh = OptionalBool::present(value_bool(attrs)),
        "outputshp" => spec.output_shp = OptionalBool::present(value_bool(attrs)),
        "outputshidling" => spec.output_sh_idling = OptionalBool::present(value_bool(attrs)),
        "outputstarts" => spec.output_starts = OptionalBool::present(value_bool(attrs)),
        "outputpopulation" => spec.output_population = OptionalBool::present(value_bool(attrs)),
        "pmsize" => spec.pm_size = parse_int_attr_or(attrs, "value", 0),
        "generatordatabase" => {
            spec.should_copy_saved_generator_data = bool_attr(attrs, "shouldsave");
            let server = get_attr(attrs, "servername");
            let dbname = get_attr(attrs, "databasename");
            let descr = get_attr(attrs, "description").unwrap_or_default();
            if server.is_some() || dbname.is_some() {
                spec.generator_database = Some(DatabaseSelection {
                    server_name: server.unwrap_or_default(),
                    database_name: dbname.unwrap_or_default(),
                    description: descr,
                });
            }
        }
        "donotperformfinalaggregation" => {
            spec.do_not_perform_final_aggregation =
                OptionalBool::present(bool_attr(attrs, "selected"));
        }
        "lookuptableflags" => {
            spec.has_lookup_table_flags = true;
            spec.should_truncate_moves_output = bool_attr_default(attrs, "truncateoutput", true);
            spec.should_truncate_moves_activity_output =
                bool_attr_default(attrs, "truncateactivity", true);
            spec.should_truncate_base_rate_output =
                bool_attr_default(attrs, "truncatebaserates", true);
            spec.scenario_id = get_attr(attrs, "scenarioid").unwrap_or_default();
        }
        "skipdomaindatabasevalidation" => {
            spec.skip_domain_database_validation =
                OptionalBool::present(bool_attr(attrs, "selected"));
        }
        // For self-closing collection containers, do nothing.
        "geographicselections"
        | "onroadvehicleselections"
        | "offroadvehicleselections"
        | "offroadvehiclesccs"
        | "roadtypes"
        | "pollutantprocessassociations"
        | "databaseselections"
        | "internalcontrolstrategies"
        | "savedata"
        | "donotexecute"
        | "outputfactors"
        | "outputemissionsbreakdownselection"
        | "timespan"
        | "models"
        | "description"
        | "genericcounty" => {}
        _ => {}
    }
    Ok(())
}

fn parse_internal_control_strategies(reader: &mut Reader<&[u8]>, spec: &mut RunSpec) -> Result<()> {
    let mut buf = Vec::new();
    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) if eq_ci(e.name(), b"internalcontrolstrategy") => {
                let class_name = attr_string(&e.attributes(), b"classname")?.unwrap_or_default();
                let (body, is_cdata) = read_strategy_body(reader)?;
                let strategy = InternalControlStrategy {
                    class_name: class_name.clone(),
                    body,
                    is_cdata,
                };
                spec.internal_control_strategies
                    .entry(class_name)
                    .or_default()
                    .push(strategy);
            }
            Event::Empty(_) => {
                // Self-closing strategy entries are malformed in MOVES but
                // accepted silently for robustness.
            }
            Event::End(e) if eq_ci(e.name(), b"internalcontrolstrategies") => return Ok(()),
            Event::Eof => return Ok(()),
            _ => {}
        }
        buf.clear();
    }
}

fn read_strategy_body(reader: &mut Reader<&[u8]>) -> Result<(String, bool)> {
    let mut buf = Vec::new();
    let mut body = String::new();
    let mut is_cdata = false;
    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Text(t) => body.push_str(&t.unescape()?),
            Event::CData(t) => {
                is_cdata = true;
                body.push_str(str::from_utf8(t.as_ref()).map_err(|_| {
                    Error::Malformed("non-UTF-8 CDATA in internalcontrolstrategy".into())
                })?);
            }
            Event::End(e) if eq_ci(e.name(), b"internalcontrolstrategy") => {
                return Ok((body, is_cdata));
            }
            Event::Eof => {
                return Err(Error::Malformed(
                    "unclosed <internalcontrolstrategy>".into(),
                ));
            }
            _ => {}
        }
        buf.clear();
    }
}

fn read_text(reader: &mut Reader<&[u8]>, tag: &str) -> Result<String> {
    let mut buf = Vec::new();
    let mut out = String::new();
    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Text(t) => out.push_str(&t.unescape()?),
            Event::CData(t) => out.push_str(
                str::from_utf8(t.as_ref())
                    .map_err(|_| Error::Malformed(format!("non-UTF-8 CDATA inside <{tag}>")))?,
            ),
            Event::End(e) if eq_ci(e.name(), tag.as_bytes()) => return Ok(out),
            Event::Eof => return Err(Error::Malformed(format!("unclosed <{tag}>"))),
            _ => {}
        }
        buf.clear();
    }
}

fn walk_children<F>(reader: &mut Reader<&[u8]>, container: &str, mut on_child: F) -> Result<()>
where
    F: FnMut(String, &[(String, String)], bool) -> Result<()>,
{
    let mut buf = Vec::new();
    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) => {
                let name = local_name(e.name()).to_ascii_lowercase();
                let attrs = clone_attributes(&e);
                on_child(name.clone(), &attrs, false)?;
                skip_to_close(reader, &name)?;
            }
            Event::Empty(e) => {
                let name = local_name(e.name()).to_ascii_lowercase();
                let attrs = clone_attributes(&e);
                on_child(name, &attrs, true)?;
            }
            Event::End(e) if eq_ci(e.name(), container.as_bytes()) => return Ok(()),
            Event::Eof => return Ok(()),
            _ => {}
        }
        buf.clear();
    }
}

fn skip_to_close(reader: &mut Reader<&[u8]>, container: &str) -> Result<()> {
    let mut buf = Vec::new();
    let mut depth = 1i32;
    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(_) => depth += 1,
            Event::End(e) => {
                depth -= 1;
                if depth == 0 && eq_ci(e.name(), container.as_bytes()) {
                    return Ok(());
                }
            }
            Event::Eof => return Ok(()),
            _ => {}
        }
        buf.clear();
    }
}

// ---------- Attribute helpers ----------

fn eq_ci(name: QName<'_>, expected: &[u8]) -> bool {
    name.as_ref().eq_ignore_ascii_case(expected)
}

fn local_name(name: QName<'_>) -> String {
    String::from_utf8_lossy(name.as_ref()).into_owned()
}

fn clone_attributes(e: &quick_xml::events::BytesStart) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for attr in e.attributes() {
        let Ok(Attribute { key, value }) = attr else {
            continue;
        };
        let k = String::from_utf8_lossy(key.as_ref()).to_ascii_lowercase();
        let v = match str::from_utf8(value.as_ref()) {
            Ok(s) => unescape_xml(s),
            Err(_) => String::from_utf8_lossy(value.as_ref()).into_owned(),
        };
        out.push((k, v));
    }
    out
}

fn attr_string(attrs: &Attributes, name: &[u8]) -> Result<Option<String>> {
    for attr in attrs.clone() {
        let attr = attr?;
        if attr.key.as_ref().eq_ignore_ascii_case(name) {
            let raw = match str::from_utf8(attr.value.as_ref()) {
                Ok(s) => s.to_string(),
                Err(_) => String::from_utf8_lossy(attr.value.as_ref()).into_owned(),
            };
            return Ok(Some(unescape_xml(&raw)));
        }
    }
    Ok(None)
}

fn get_attr(attrs: &[(String, String)], key: &str) -> Option<String> {
    attrs
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(key))
        .map(|(_, v)| v.clone())
}

fn parse_int_attr(attrs: &[(String, String)], key: &str) -> Result<i32> {
    match get_attr(attrs, key) {
        Some(v) if v.is_empty() => Ok(0),
        Some(v) => v.trim().parse::<i32>().map_err(|_| Error::InvalidInt {
            element: String::new(),
            attr: key.into(),
            value: v,
        }),
        None => Ok(0),
    }
}

fn parse_int_attr_or(attrs: &[(String, String)], key: &str, default: i32) -> i32 {
    parse_int_attr(attrs, key).unwrap_or(default)
}

fn parse_float_attr_or(attrs: &[(String, String)], key: &str, default: f32) -> f32 {
    match get_attr(attrs, key) {
        Some(v) if v.is_empty() => default,
        Some(v) => v.trim().parse::<f32>().unwrap_or(default),
        None => default,
    }
}

fn bool_attr(attrs: &[(String, String)], key: &str) -> bool {
    bool_attr_default(attrs, key, false)
}

fn bool_attr_default(attrs: &[(String, String)], key: &str, default: bool) -> bool {
    match get_attr(attrs, key) {
        Some(v) => v.trim().eq_ignore_ascii_case("true"),
        None => default,
    }
}

fn value_bool(attrs: &[(String, String)]) -> bool {
    bool_attr(attrs, "value")
}

fn unescape_xml(s: &str) -> String {
    // Quick-xml `Attribute::value` is the raw post-escape bytes for serialized
    // attributes; `attributes()` returns decoded keys but raw values, so we
    // normalise the common entities here. Avoids pulling in a heavier XML
    // decoder for what amounts to four named entities and the numeric form.
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'&' {
            if let Some(end_rel) = bytes[i + 1..].iter().position(|&b| b == b';') {
                let entity = &s[i + 1..i + 1 + end_rel];
                let replacement = match entity {
                    "amp" => Some('&'),
                    "lt" => Some('<'),
                    "gt" => Some('>'),
                    "quot" => Some('"'),
                    "apos" => Some('\''),
                    _ => None,
                };
                if let Some(ch) = replacement {
                    out.push(ch);
                    i += end_rel + 2;
                    continue;
                }
            }
        }
        // Append one UTF-8 character starting at i.
        let ch_len = utf8_char_len(bytes[i]);
        // Safe: `i` is at a UTF-8 boundary and `ch_len` covers a full codepoint.
        out.push_str(&s[i..i + ch_len]);
        i += ch_len;
    }
    out
}

fn utf8_char_len(b: u8) -> usize {
    // Continuation bytes (0x80..=0xBF) and ASCII (<0x80) are both one byte
    // for our purposes since we only ever start at a code-point boundary.
    if b < 0xC0 {
        1
    } else if b < 0xE0 {
        2
    } else if b < 0xF0 {
        3
    } else {
        4
    }
}
