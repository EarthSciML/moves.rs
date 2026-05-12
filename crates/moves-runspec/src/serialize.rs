//! [`RunSpec`] → XML serializer.
//!
//! Produces a canonical Java-style RunSpec XML: tab indentation, lowercase tag
//! names matching `RunSpecXML.save`, CDATA-wrapped description, and TreeSet
//! traversal order for ordered collections. Whitespace in the output is
//! deterministic so round-trips through the parser are idempotent.

use std::fmt::Write as _;

use crate::error::Result;
use crate::types::*;

/// Serializes a RunSpec to its canonical XML byte form.
pub fn serialize_runspec(spec: &RunSpec) -> Result<Vec<u8>> {
    let mut out = String::new();
    // The Java RunSpecXML.save unconditionally injects the current
    // MOVES_VERSION; we preserve whatever the source RunSpec carried so
    // round-tripping is stable, including the no-version-attribute case
    // some hand-authored fixtures use.
    if spec.version.is_empty() {
        writeln!(out, "<runspec>")?;
    } else {
        writeln!(out, "<runspec version=\"{}\">", escape_attr(&spec.version))?;
    }
    writeln!(
        out,
        "\t<description><![CDATA[{}]]></description>",
        spec.description
    )?;

    writeln!(out, "\t<models>")?;
    for m in &spec.models {
        writeln!(out, "\t\t<model value=\"{}\"/>", m.as_str())?;
    }
    writeln!(out, "\t</models>")?;

    if let Some(scale) = spec.scale {
        writeln!(out, "\t<modelscale value=\"{}\"/>", scale.as_str())?;
    } else {
        writeln!(out, "\t<modelscale value=\"\"/>")?;
    }
    if let Some(domain) = spec.domain {
        writeln!(out, "\t<modeldomain value=\"{}\"/>", domain.as_str())?;
    } else {
        writeln!(out, "\t<modeldomain value=\"\"/>")?;
    }

    if let Some(g) = &spec.generic_county {
        writeln!(out, "\t<genericcounty>")?;
        writeln!(out, "\t\t<shortid value=\"{}\"/>", g.short_county_id)?;
        writeln!(out, "\t\t<stateid value=\"{}\"/>", g.state_id)?;
        writeln!(
            out,
            "\t\t<description value=\"{}\"/>",
            escape_attr(&g.description)
        )?;
        writeln!(out, "\t\t<gpafraction value=\"{}\"/>", g.gpa_fraction)?;
        writeln!(
            out,
            "\t\t<barometricpressure value=\"{}\"/>",
            g.barometric_pressure
        )?;
        writeln!(
            out,
            "\t\t<refuelvaporadjust value=\"{}\"/>",
            g.refueling_vapor_program_adjust
        )?;
        writeln!(
            out,
            "\t\t<refuelspilladjust value=\"{}\"/>",
            g.refueling_spill_program_adjust
        )?;
        writeln!(out, "\t</genericcounty>")?;
    }

    writeln!(out, "\t<geographicselections>")?;
    for sel in &spec.geographic_selections {
        writeln!(
            out,
            "\t\t<geographicselection type=\"{}\" key=\"{}\" description=\"{}\"/>",
            sel.type_.as_str(),
            sel.database_key,
            escape_attr(&sel.text_description)
        )?;
    }
    writeln!(out, "\t</geographicselections>")?;

    writeln!(out, "\t<timespan>")?;
    for y in &spec.time_span.years {
        writeln!(out, "\t\t<year key=\"{}\"/>", y)?;
    }
    for m in &spec.time_span.months {
        writeln!(out, "\t\t<month key=\"{}\"/>", m)?;
    }
    for d in &spec.time_span.days {
        writeln!(out, "\t\t<day key=\"{}\"/>", d)?;
    }
    writeln!(
        out,
        "\t\t<beginhour key=\"{}\"/>",
        spec.time_span.begin_hour_id
    )?;
    writeln!(out, "\t\t<endhour key=\"{}\"/>", spec.time_span.end_hour_id)?;
    if let Some(agg) = spec.time_span.aggregate_by {
        writeln!(out, "\t\t<aggregateBy key=\"{}\"/>", agg.as_str())?;
    }
    writeln!(out, "\t</timespan>")?;

    writeln!(out, "\t<onroadvehicleselections>")?;
    for sel in &spec.onroad_vehicle_selections {
        writeln!(
            out,
            "\t\t<onroadvehicleselection fueltypeid=\"{}\" fueltypedesc=\"{}\" sourcetypeid=\"{}\" sourcetypename=\"{}\"/>",
            sel.fuel_type_id,
            escape_attr(&sel.fuel_type_desc),
            sel.source_type_id,
            escape_attr(&sel.source_type_name)
        )?;
    }
    writeln!(out, "\t</onroadvehicleselections>")?;

    writeln!(out, "\t<offroadvehicleselections>")?;
    for sel in &spec.offroad_vehicle_selections {
        writeln!(
            out,
            "\t\t<offroadvehicleselection fueltypeid=\"{}\" fueltypedesc=\"{}\" sectorid=\"{}\" sectorname=\"{}\"/>",
            sel.fuel_type_id,
            escape_attr(&sel.fuel_type_desc),
            sel.sector_id,
            escape_attr(&sel.sector_name)
        )?;
    }
    writeln!(out, "\t</offroadvehicleselections>")?;

    writeln!(out, "\t<offroadvehiclesccs>")?;
    for scc in &spec.offroad_vehicle_sccs {
        writeln!(out, "\t\t<scc code=\"{}\"/>", escape_attr(&scc.code))?;
    }
    writeln!(out, "\t</offroadvehiclesccs>")?;

    writeln!(out, "\t<roadtypes>")?;
    for rt in &spec.road_types {
        writeln!(
            out,
            "\t\t<roadtype roadtypeid=\"{}\" roadtypename=\"{}\" modelCombination=\"{}\"/>",
            rt.road_type_id,
            escape_attr(&rt.road_type_name),
            rt.model_combination.as_str()
        )?;
    }
    writeln!(out, "\t</roadtypes>")?;

    writeln!(out, "\t<pollutantprocessassociations>")?;
    for ppa in &spec.pollutant_process_associations {
        writeln!(
            out,
            "\t\t<pollutantprocessassociation pollutantkey=\"{}\" pollutantname=\"{}\" processkey=\"{}\" processname=\"{}\"/>",
            ppa.pollutant_key,
            escape_attr(&ppa.pollutant_name),
            ppa.process_key,
            escape_attr(&ppa.process_name)
        )?;
    }
    writeln!(out, "\t</pollutantprocessassociations>")?;

    writeln!(out, "\t<databaseselections>")?;
    for db in &spec.database_selections {
        writeln!(
            out,
            "\t\t<databaseselection servername=\"{}\" databasename=\"{}\" description=\"{}\"/>",
            escape_attr(&db.server_name),
            escape_attr(&db.database_name),
            escape_attr(&db.description)
        )?;
    }
    writeln!(out, "\t</databaseselections>")?;

    writeln!(out, "\t<internalcontrolstrategies>")?;
    for (class_name, strategies) in &spec.internal_control_strategies {
        for s in strategies {
            if s.is_cdata {
                write!(
                    out,
                    "\t\t<internalcontrolstrategy classname=\"{}\"><![CDATA[",
                    escape_attr(class_name)
                )?;
                out.push_str(&s.body);
                writeln!(out, "]]></internalcontrolstrategy>")?;
            } else {
                write!(
                    out,
                    "\t\t<internalcontrolstrategy classname=\"{}\">",
                    escape_attr(class_name)
                )?;
                out.push_str(&s.body);
                writeln!(out, "</internalcontrolstrategy>")?;
            }
        }
    }
    writeln!(out, "\t</internalcontrolstrategies>")?;

    writeln!(
        out,
        "\t<inputdatabase servername=\"{}\" databasename=\"{}\" description=\"{}\"/>",
        escape_attr(&spec.input_database.server_name),
        escape_attr(&spec.input_database.database_name),
        escape_attr(&spec.input_database.description)
    )?;
    writeln!(
        out,
        "\t<uncertaintyparameters uncertaintymodeenabled=\"{}\" numberofrunspersimulation=\"{}\" numberofsimulations=\"{}\"/>",
        spec.uncertainty_parameters.uncertainty_mode_enabled,
        spec.uncertainty_parameters.number_of_runs_per_simulation,
        spec.uncertainty_parameters.number_of_simulations
    )?;
    let gout = spec
        .geographic_output_detail
        .map(|g| g.as_str())
        .unwrap_or("");
    writeln!(out, "\t<geographicoutputdetail description=\"{}\"/>", gout)?;

    write_emissions_breakdown(&mut out, &spec.output_emissions_breakdown_selection)?;

    writeln!(
        out,
        "\t<outputdatabase servername=\"{}\" databasename=\"{}\" description=\"{}\"/>",
        escape_attr(&spec.output_database.server_name),
        escape_attr(&spec.output_database.database_name),
        escape_attr(&spec.output_database.description)
    )?;
    let timestep = spec.output_time_step.map(|t| t.as_str()).unwrap_or("");
    writeln!(out, "\t<outputtimestep value=\"{}\"/>", timestep)?;
    writeln!(out, "\t<outputvmtdata value=\"{}\"/>", spec.output_vmt_data)?;
    if spec.output_sho.present {
        writeln!(out, "\t<outputsho value=\"{}\"/>", spec.output_sho.value)?;
    }
    if spec.output_sh.present {
        writeln!(out, "\t<outputsh value=\"{}\"/>", spec.output_sh.value)?;
    }
    if spec.output_shp.present {
        writeln!(out, "\t<outputshp value=\"{}\"/>", spec.output_shp.value)?;
    }
    if spec.output_sh_idling.present {
        writeln!(
            out,
            "\t<outputshidling value=\"{}\"/>",
            spec.output_sh_idling.value
        )?;
    }
    if spec.output_starts.present {
        writeln!(
            out,
            "\t<outputstarts value=\"{}\"/>",
            spec.output_starts.value
        )?;
    }
    if spec.output_population.present {
        writeln!(
            out,
            "\t<outputpopulation value=\"{}\"/>",
            spec.output_population.value
        )?;
    }

    writeln!(
        out,
        "\t<scaleinputdatabase servername=\"{}\" databasename=\"{}\" description=\"{}\"/>",
        escape_attr(&spec.scale_input_database.server_name),
        escape_attr(&spec.scale_input_database.database_name),
        escape_attr(&spec.scale_input_database.description)
    )?;
    writeln!(out, "\t<pmsize value=\"{}\"/>", spec.pm_size)?;

    write_output_factors(&mut out, &spec.output_factors)?;

    if !spec.classes_to_save_data.is_empty() {
        writeln!(out, "\t<savedata>")?;
        for c in &spec.classes_to_save_data {
            writeln!(out, "\t\t<class name=\"{}\"/>", escape_attr(c))?;
        }
        writeln!(out, "\t</savedata>")?;
    }
    if !spec.classes_not_to_execute.is_empty() {
        writeln!(out, "\t<donotexecute>")?;
        for c in &spec.classes_not_to_execute {
            writeln!(out, "\t\t<class name=\"{}\"/>", escape_attr(c))?;
        }
        writeln!(out, "\t</donotexecute>")?;
    }

    if let Some(gdb) = &spec.generator_database {
        writeln!(
            out,
            "\t<generatordatabase shouldsave=\"{}\" servername=\"{}\" databasename=\"{}\" description=\"{}\"/>",
            spec.should_copy_saved_generator_data,
            escape_attr(&gdb.server_name),
            escape_attr(&gdb.database_name),
            escape_attr(&gdb.description)
        )?;
    }
    if spec.do_not_perform_final_aggregation.present {
        writeln!(
            out,
            "\t<donotperformfinalaggregation selected=\"{}\"/>",
            spec.do_not_perform_final_aggregation.value
        )?;
    }
    if spec.has_lookup_table_flags {
        writeln!(
            out,
            "\t<lookuptableflags scenarioid=\"{}\" truncateoutput=\"{}\" truncateactivity=\"{}\" truncatebaserates=\"{}\"/>",
            escape_attr(&spec.scenario_id),
            spec.should_truncate_moves_output,
            spec.should_truncate_moves_activity_output,
            spec.should_truncate_base_rate_output
        )?;
    }
    if spec.skip_domain_database_validation.present {
        writeln!(
            out,
            "\t<skipdomaindatabasevalidation selected=\"{}\"/>",
            spec.skip_domain_database_validation.value
        )?;
    }

    writeln!(out, "</runspec>")?;
    Ok(out.into_bytes())
}

fn write_emissions_breakdown(
    out: &mut String,
    sel: &OutputEmissionsBreakdownSelection,
) -> Result<()> {
    writeln!(out, "\t<outputemissionsbreakdownselection>")?;
    writeln!(out, "\t\t<modelyear selected=\"{}\"/>", sel.model_year)?;
    writeln!(out, "\t\t<fueltype selected=\"{}\"/>", sel.fuel_type)?;
    if sel.had_fuel_sub_type {
        writeln!(out, "\t\t<fuelsubtype selected=\"{}\"/>", sel.fuel_sub_type)?;
    }
    writeln!(
        out,
        "\t\t<emissionprocess selected=\"{}\"/>",
        sel.emission_process
    )?;
    writeln!(
        out,
        "\t\t<onroadoffroad selected=\"{}\"/>",
        sel.onroad_offroad
    )?;
    writeln!(out, "\t\t<roadtype selected=\"{}\"/>", sel.road_type)?;
    writeln!(
        out,
        "\t\t<sourceusetype selected=\"{}\"/>",
        sel.source_use_type
    )?;
    writeln!(
        out,
        "\t\t<movesvehicletype selected=\"{}\"/>",
        sel.moves_vehicle_type
    )?;
    writeln!(out, "\t\t<onroadscc selected=\"{}\"/>", sel.onroad_scc)?;
    writeln!(
        out,
        "\t\t<estimateuncertainty selected=\"{}\" numberOfIterations=\"{}\" keepSampledData=\"{}\" keepIterations=\"{}\"/>",
        sel.estimate_uncertainty,
        sel.number_of_iterations,
        sel.keep_sampled_data,
        sel.keep_iterations
    )?;
    writeln!(out, "\t\t<sector selected=\"{}\"/>", sel.sector)?;
    if sel.had_eng_tech_id {
        writeln!(out, "\t\t<engtechid selected=\"{}\"/>", sel.eng_tech_id)?;
    }
    writeln!(out, "\t\t<hpclass selected=\"{}\"/>", sel.hp_class)?;
    if sel.had_reg_class_id {
        writeln!(out, "\t\t<regclassid selected=\"{}\"/>", sel.reg_class_id)?;
    }
    writeln!(out, "\t</outputemissionsbreakdownselection>")?;
    Ok(())
}

fn write_output_factors(out: &mut String, f: &OutputFactors) -> Result<()> {
    let time_units = f.time_measurement_system.map(|t| t.as_str()).unwrap_or("");
    let dist_units = f
        .distance_measurement_system
        .map(|d| d.as_str())
        .unwrap_or("");
    let mass_units = f.mass_measurement_system.map(|m| m.as_str()).unwrap_or("");
    let energy_units = f
        .energy_measurement_system
        .map(|e| e.as_str())
        .unwrap_or("");
    writeln!(out, "\t<outputfactors>")?;
    writeln!(
        out,
        "\t\t<timefactors selected=\"{}\" units=\"{}\"/>",
        f.time_factors_selected, time_units
    )?;
    writeln!(
        out,
        "\t\t<distancefactors selected=\"{}\" units=\"{}\"/>",
        f.distance_factors_selected, dist_units
    )?;
    writeln!(
        out,
        "\t\t<massfactors selected=\"{}\" units=\"{}\" energyunits=\"{}\"/>",
        f.mass_factors_selected, mass_units, energy_units
    )?;
    writeln!(out, "\t</outputfactors>")?;
    Ok(())
}

fn escape_attr(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(ch),
        }
    }
    out
}
