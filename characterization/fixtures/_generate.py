#!/usr/bin/env python3
"""Regenerate the RunSpec XML fixtures in this directory.

The fixture *files* are the committed artifacts (Phase 0 Task 5/6 acceptance).
This script is the source of truth for *how* they were derived: a single
spec table plus a templating function. Re-run after editing the table:

    python3 characterization/fixtures/_generate.py

`sample-runspec.xml` is **not** regenerated — it is a byte-identical copy of
`testdata/SampleRunSpec.xml` from the pinned canonical-MOVES tree
(MOVES5.0.1 @ 25dc6c83). Treating it as input rather than output preserves
provenance for fixture #1.

Each fixture targets one or two specific dimensions of the MOVES coverage
space (see coverage-matrix.md). Fields you don't override here use the
ONROAD inventory defaults below.
"""
from __future__ import annotations

import os
import sys
import textwrap
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

HERE = Path(__file__).resolve().parent
PRESERVE = {"sample-runspec.xml"}


# ---------------------------------------------------------------------------
# Reference tables (subset of movesdb20241112; values verified against the
# pinned canonical-MOVES seed during fixture authoring).
# ---------------------------------------------------------------------------

POLLUTANTS = {
    1:   "Total Gaseous Hydrocarbons",
    2:   "Carbon Monoxide (CO)",
    3:   "Oxides of Nitrogen (NOx)",
    5:   "Methane (CH4)",
    6:   "Nitrous Oxide (N2O)",
    20:  "Benzene",
    24:  "1,3-Butadiene",
    25:  "Formaldehyde",
    31:  "Sulfur Dioxide (SO2)",
    79:  "Non-Methane Hydrocarbons",
    86:  "Total Organic Gases",
    87:  "Volatile Organic Compounds",
    91:  "Total Energy Consumption",
    92:  "Petroleum Energy Consumption",
    93:  "Fossil Fuel Energy Consumption",
    100: "Primary Exhaust PM10  - Total",
    106: "Primary PM10 - Brakewear Particulate",
    107: "Primary PM10 - Tirewear Particulate",
    110: "Primary Exhaust PM2.5 - Total",
    116: "Primary PM2.5 - Brakewear Particulate",
    117: "Primary PM2.5 - Tirewear Particulate",
}

PROCESSES = {
    1:  "Running Exhaust",
    2:  "Start Exhaust",
    9:  "Brakewear",
    10: "Tirewear",
    11: "Evap Permeation",
    12: "Evap Fuel Vapor Venting",
    13: "Evap Fuel Leaks",
    15: "Crankcase Running Exhaust",
    16: "Crankcase Start Exhaust",
    17: "Crankcase Extended Idle Exhaust",
    18: "Refueling Displacement Vapor Loss",
    19: "Refueling Spillage Loss",
    20: "Evap Tank Permeation",
    21: "Evap Hose Permeation",
    22: "Evap RecMar Neck Hose Permeation",
    23: "Evap RecMar Supply/Ret Hose Permeation",
    24: "Evap RecMar Vent Hose Permeation",
    30: "Diurnal Fuel Vapor Venting",
    31: "HotSoak Fuel Vapor Venting",
    32: "RunningLoss Fuel Vapor Venting",
    40: "Nonroad",
    90: "Extended Idle Exhaust",
    91: "Auxiliary Power Exhaust",
    99: "Well-to-Pump",
}

FUEL_TYPES = {1: "Gasoline", 2: "Diesel Fuel", 3: "Compressed Natural Gas (CNG)",
              5: "Ethanol (E-85)", 9: "Electricity"}

ONROAD_SOURCE_TYPES = {
    11: "Motorcycle",
    21: "Passenger Car",
    31: "Passenger Truck",
    32: "Light Commercial Truck",
    41: "Intercity Bus",
    42: "Transit Bus",
    43: "School Bus",
    51: "Refuse Truck",
    52: "Single Unit Short-haul Truck",
    53: "Single Unit Long-haul Truck",
    54: "Motor Home",
    61: "Combination Short-haul Truck",
    62: "Combination Long-haul Truck",
}

ROAD_TYPES = {
    1: "Off-Network",
    2: "Rural Restricted Access",
    3: "Rural Unrestricted Access",
    4: "Urban Restricted Access",
    5: "Urban Unrestricted Access",
}

NONROAD_SECTORS = {
    1:  "Recreational",
    2:  "Construction",
    3:  "Industrial",
    4:  "Lawn/Garden",
    5:  "Agriculture",
    6:  "Commercial",
    7:  "Logging",
    8:  "Airport Support",
    9:  "Underground Mining",
    10: "Oil Field",
    11: "Pleasure Craft",
    12: "Railroad Support",
}


# ---------------------------------------------------------------------------
# Defaults that drive the fixture template. Override per-fixture below.
# ---------------------------------------------------------------------------

@dataclass
class Geo:
    type: str            # COUNTY, STATE, NATION, ZONE, LINK
    key: int
    description: str

@dataclass
class TimeSpan:
    year: int = 2020
    months: tuple[int, ...] = (7,)         # July
    days: tuple[int, ...] = (5,)           # Weekdays
    begin_hour: int = 6                    # hour-of-day index per hourofanyday
    end_hour: int = 6
    aggregate_by: Optional[str] = None     # "Hour"/"Day"/"Month"/"Year"

@dataclass
class FixtureSpec:
    name: str
    description: str
    coverage: tuple[str, ...]                                      # tags for the matrix
    models: tuple[str, ...] = ("ONROAD",)
    # `scale` and `domain` use the description-string forms accepted by
    # `ModelScale.getByName` / `ModelDomain.getByName` in canonical MOVES
    # (see `gov.epa.otaq.moves.common.{ModelScale,ModelDomain}`). Note
    # that `testdata/SampleRunSpec.xml` ships with `value="MACROSCALE"`,
    # which fails the description match and logs a warning — MOVES then
    # silently defaults the scale. Modern MOVES-emitted RunSpecs use
    # the description ("Inv" / "Rates"), and so do these fixtures.
    scale: str = "Inv"                                              # Inv|Rates
    domain: str = "DEFAULT"                                         # DEFAULT|SINGLE|PROJECT
    geographic: tuple[Geo, ...] = (Geo("COUNTY", 26161,
                                       "MICHIGAN - Washtenaw County"),)
    timespan: TimeSpan = field(default_factory=TimeSpan)
    onroad_selections: tuple[tuple[int, int], ...] = ((1, 21),)    # (fuelTypeID, sourceTypeID)
    offroad_selections: tuple[tuple[int, int], ...] = ()           # (fuelTypeID, sectorID)
    offroad_sccs: tuple[str, ...] = ()
    road_types: tuple[int, ...] = (4,)
    pp_assocs: tuple[tuple[int, int], ...] = ()                    # (pollutantID, processID)
    geographic_output_detail: str = "COUNTY"
    output_db: str = ""                                             # default per-fixture
    output_timestep: str = "Hour"
    pm_size: int = 0
    aggregate_by: Optional[str] = None
    extra_root_elements: str = ""                                   # raw XML appended at end

DEFAULT_OUTPUT_BREAKDOWN = textwrap.dedent("""\
    <outputemissionsbreakdownselection>
        <modelyear selected="true"/>
        <fueltype selected="true"/>
        <emissionprocess selected="true"/>
        <distinguishparticulates selected="true"/>
        <onroadoffroad selected="true"/>
        <roadtype selected="true"/>
        <sourceusetype selected="false"/>
        <movesvehicletype selected="false"/>
        <onroadscc selected="true"/>
        <offroadscc selected="false"/>
        <estimateuncertainty selected="false"/>
        <segment selected="false"/>
        <hpclass selected="false"/>
    </outputemissionsbreakdownselection>""").strip()


# ---------------------------------------------------------------------------
# XML emission
# ---------------------------------------------------------------------------

def render(spec: FixtureSpec) -> str:
    out = []
    out.append('<runspec version="MOVES5.0.1">')
    out.append(f'\t<description><![CDATA[{spec.description}]]></description>')
    out.append('\t<models>')
    for m in spec.models:
        out.append(f'\t\t<model value="{m}"/>')
    out.append('\t</models>')
    out.append(f'\t<modelscale value="{spec.scale}"/>')
    out.append(f'\t<modeldomain value="{spec.domain}"/>')

    out.append('\t<geographicselections>')
    for g in spec.geographic:
        out.append(f'\t\t<geographicselection type="{g.type}" key="{g.key}"'
                   f' description="{g.description}"/>')
    out.append('\t</geographicselections>')

    ts = spec.timespan
    out.append('\t<timespan>')
    out.append(f'\t\t<year key="{ts.year}"/>')
    for m in ts.months:
        out.append(f'\t\t<month key="{m}"/>')
    for d in ts.days:
        out.append(f'\t\t<day key="{d}"/>')
    out.append(f'\t\t<beginhour key="{ts.begin_hour}"/>')
    out.append(f'\t\t<endhour key="{ts.end_hour}"/>')
    if ts.aggregate_by:
        out.append(f'\t\t<aggregateBy key="{ts.aggregate_by}"/>')
    out.append('\t</timespan>')

    out.append('\t<onroadvehicleselections>')
    for fuel_id, src_id in spec.onroad_selections:
        out.append(f'\t\t<onroadvehicleselection fueltypeid="{fuel_id}"'
                   f' fueltypedesc="{FUEL_TYPES[fuel_id]}"'
                   f' sourcetypeid="{src_id}"'
                   f' sourcetypename="{ONROAD_SOURCE_TYPES[src_id]}"/>')
    out.append('\t</onroadvehicleselections>')

    out.append('\t<offroadvehicleselections>')
    for fuel_id, sector_id in spec.offroad_selections:
        out.append(f'\t\t<offroadvehicleselection fueltypeid="{fuel_id}"'
                   f' fueltypedesc="{FUEL_TYPES[fuel_id]}"'
                   f' sectorid="{sector_id}"'
                   f' sectorname="{NONROAD_SECTORS[sector_id]}"/>')
    out.append('\t</offroadvehicleselections>')

    out.append('\t<offroadvehiclesccs>')
    for scc in spec.offroad_sccs:
        out.append(f'\t\t<scc code="{scc}"/>')
    out.append('\t</offroadvehiclesccs>')

    out.append('\t<roadtypes>')
    for rt in spec.road_types:
        out.append(f'\t\t<roadtype roadtypeid="{rt}" roadtypename="{ROAD_TYPES[rt]}"'
                   f' modelCombination="M1"/>')
    out.append('\t</roadtypes>')

    out.append('\t<pollutantprocessassociations>')
    for pol_id, proc_id in spec.pp_assocs:
        out.append(f'\t\t<pollutantprocessassociation'
                   f' pollutantkey="{pol_id}" pollutantname="{POLLUTANTS[pol_id]}"'
                   f' processkey="{proc_id}" processname="{PROCESSES[proc_id]}"/>')
    out.append('\t</pollutantprocessassociations>')

    out.append('\t<databaseselections>')
    out.append('\t</databaseselections>')
    out.append('\t<internalcontrolstrategies>')
    out.append('\t</internalcontrolstrategies>')
    out.append('\t<inputdatabase servername="" databasename="" description=""/>')
    out.append('\t<uncertaintyparameters uncertaintymodeenabled="false"'
               ' numberofrunspersimulation="0" numberofsimulations="0"/>')
    out.append(f'\t<geographicoutputdetail description="{spec.geographic_output_detail}"/>')
    for line in DEFAULT_OUTPUT_BREAKDOWN.splitlines():
        out.append(f'\t{line}')
    out_db = spec.output_db or f"out_{spec.name.replace('-', '_')}"
    out.append(f'\t<outputdatabase servername="" databasename="{out_db}"'
               f' description=""/>')
    out.append(f'\t<outputtimestep value="{spec.output_timestep}"/>')
    out.append('\t<outputvmtdata value="false"/>')
    out.append('\t<outputsho value="false"/>')
    out.append('\t<outputsh value="false"/>')
    out.append('\t<outputshp value="false"/>')
    out.append('\t<outputshidling value="false"/>')
    out.append('\t<outputstarts value="false"/>')
    out.append('\t<outputpopulation value="false"/>')
    out.append('\t<scaleinputdatabase servername="" databasename="" description=""/>')
    out.append(f'\t<pmsize value="{spec.pm_size}"/>')
    out.append('\t<outputfactors>')
    out.append('\t\t<timefactors selected="true" units="Seconds"/>')
    out.append('\t\t<distancefactors selected="true" units="Miles"/>')
    out.append('\t\t<massfactors selected="true" units="Grams" energyunits="Million BTU"/>')
    out.append('\t</outputfactors>')
    if spec.extra_root_elements:
        out.append(spec.extra_root_elements)
    out.append('</runspec>')
    return "\n".join(out) + "\n"


# ---------------------------------------------------------------------------
# Coverage tags — used by coverage-matrix.md generation. Keep canonical.
# ---------------------------------------------------------------------------

# scale tags: scale-default, scale-county, scale-project, scale-rates
# process tags: proc-1 .. proc-99 (numeric process IDs, exhaustive)
# chain tags: chain-airtoxics, chain-co2ae, chain-crankcase, chain-evappermeation,
#             chain-hcspeciation, chain-liquidleaking, chain-no, chain-no2,
#             chain-nrairtoxics, chain-nrhcspeciation, chain-nremission,
#             chain-pm10braketire, chain-pm10emission, chain-refuelingloss,
#             chain-so2, chain-sulfatepm, chain-tankvaporventing,
#             chain-togspeciation, chain-baserate
# expansion tags: expand-day, expand-month, expand-counties, expand-fueltype,
#                 expand-sourcetype, expand-criteria
# nonroad tags: nr, nr-county, nr-state, nr-nation


# ---------------------------------------------------------------------------
# Fixture catalogue (33 fixtures — sample-runspec is preserved separately).
# ---------------------------------------------------------------------------

ENERGY_RUN_START_EXTIDLE_WTP = (
    (93, 90), (93, 1), (93, 2),
    (92, 90), (92, 1), (92, 2),
    (91, 90), (91, 1), (91, 2), (91, 99),
)

CRITERIA_RUN_START = (
    (3, 1), (3, 2),     # NOx
    (2, 1), (2, 2),     # CO
    (1, 1), (1, 2),     # THC
    (31, 1), (31, 2),   # SO2
)

PM_EXHAUST = (
    (100, 1), (110, 1),     # PM10/PM2.5 exhaust running
)

PM_BRAKETIRE = (
    (106, 9), (116, 9),     # PM10/PM2.5 brakewear
    (107, 10), (117, 10),   # PM10/PM2.5 tirewear
)

AIRTOXICS_RUN = (
    (20, 1),  # Benzene running
    (24, 1),  # 1,3-Butadiene running
    (25, 1),  # Formaldehyde running
)

EVAP_FAMILY = ((86, 11), (86, 12), (86, 13))   # VOC, evap perm/fvv/leaks

REFUELING = ((86, 18), (86, 19), (1, 18), (1, 19))

CRANKCASE_RUNNING = ((3, 15), (1, 15), (2, 15))         # NOx, THC, CO crankcase running
CRANKCASE_START = ((3, 16), (1, 16), (2, 16))
CRANKCASE_EXTIDLE = ((3, 17), (1, 17), (2, 17))

APU = ((91, 91),)                                       # energy via APU
TOG_SPECIATION = ((86, 1), (5, 1), (79, 1))             # TOG, CH4, NMHC running


FIXTURES: list[FixtureSpec] = [
    # -------------------------- Expansion fixtures ----------------------------
    FixtureSpec(
        name="expand-day",
        description="Sample expanded to a full day (hours 1-24, weekday + weekend).",
        coverage=("expand-day", "scale-default", "chain-co2ae", "chain-baserate",
                  "proc-1", "proc-2", "proc-90", "proc-99"),
        timespan=TimeSpan(year=2020, months=(7,), days=(2, 5),
                          begin_hour=1, end_hour=24, aggregate_by="Hour"),
        pp_assocs=ENERGY_RUN_START_EXTIDLE_WTP,
    ),
    FixtureSpec(
        name="expand-month",
        description="Sample expanded across four months (Jan/Apr/Jul/Oct).",
        coverage=("expand-month", "scale-default", "chain-co2ae", "chain-baserate",
                  "proc-1", "proc-2", "proc-90", "proc-99"),
        timespan=TimeSpan(year=2020, months=(1, 4, 7, 10), days=(5,),
                          begin_hour=6, end_hour=6, aggregate_by="Month"),
        pp_assocs=ENERGY_RUN_START_EXTIDLE_WTP,
    ),
    FixtureSpec(
        name="expand-counties",
        description="Sample expanded across three diverse counties (Washtenaw MI, "
                    "Cook IL, Los Angeles CA).",
        coverage=("expand-counties", "scale-default", "chain-co2ae", "chain-baserate",
                  "proc-1", "proc-2", "proc-90", "proc-99"),
        geographic=(
            Geo("COUNTY", 26161, "MICHIGAN - Washtenaw County"),
            Geo("COUNTY", 17031, "ILLINOIS - Cook County"),
            Geo("COUNTY",  6037, "CALIFORNIA - Los Angeles County"),
        ),
        pp_assocs=ENERGY_RUN_START_EXTIDLE_WTP,
    ),
    FixtureSpec(
        name="expand-fueltype-diesel",
        description="Sample expanded to include diesel for passenger car + light "
                    "commercial truck.",
        coverage=("expand-fueltype", "scale-default", "chain-co2ae", "chain-baserate",
                  "proc-1", "proc-2", "proc-90", "proc-99"),
        onroad_selections=((1, 21), (2, 21), (1, 32), (2, 32)),
        pp_assocs=ENERGY_RUN_START_EXTIDLE_WTP,
    ),
    FixtureSpec(
        name="expand-sourcetype",
        description="Sample expanded across multiple onroad source types "
                    "(motorcycle, pass car, passenger truck, refuse truck, "
                    "long-haul combo).",
        coverage=("expand-sourcetype", "scale-default", "chain-co2ae", "chain-baserate",
                  "proc-1", "proc-2", "proc-90", "proc-99"),
        onroad_selections=((1, 11), (1, 21), (1, 31), (1, 51), (2, 62)),
        pp_assocs=ENERGY_RUN_START_EXTIDLE_WTP,
    ),
    FixtureSpec(
        name="expand-criteria",
        description="Sample swapped to criteria pollutants (NOx, CO, THC, SO2) "
                    "for running + start exhaust.",
        coverage=("expand-criteria", "scale-default",
                  "chain-baserate", "chain-no", "chain-no2", "chain-so2",
                  "chain-hcspeciation",
                  "proc-1", "proc-2"),
        pp_assocs=CRITERIA_RUN_START,
    ),

    # -------------------------- Process fixtures ------------------------------
    FixtureSpec(
        name="process-brakewear",
        description="Brakewear PM10 + PM2.5 — exercises PM10BrakeTireCalculator chain.",
        coverage=("scale-default", "chain-pm10braketire", "chain-baserate", "proc-9"),
        pp_assocs=((106, 9), (116, 9)),
    ),
    FixtureSpec(
        name="process-tirewear",
        description="Tirewear PM10 + PM2.5 — exercises PM10BrakeTireCalculator chain.",
        coverage=("scale-default", "chain-pm10braketire", "chain-baserate", "proc-10"),
        pp_assocs=((107, 10), (117, 10)),
    ),
    FixtureSpec(
        name="process-pm-exhaust",
        description="Onroad PM exhaust (PM10 + PM2.5 totals) — exercises "
                    "PM10EmissionCalculator + SulfatePMCalculator chains.",
        coverage=("scale-default", "chain-pm10emission", "chain-sulfatepm",
                  "chain-baserate", "proc-1"),
        pp_assocs=((100, 1), (110, 1)),
    ),
    FixtureSpec(
        name="process-evap-permeation",
        description="Evap Permeation (process 11) running fuels — exercises "
                    "EvaporativePermeationCalculator + HCSpeciationCalculator chains.",
        coverage=("scale-default", "chain-evappermeation", "chain-hcspeciation",
                  "chain-baserate", "proc-11"),
        pp_assocs=((86, 11), (87, 11), (1, 11)),
    ),
    FixtureSpec(
        name="process-evap-fvv",
        description="Evap Fuel Vapor Venting (process 12) — exercises "
                    "TankVaporVentingCalculator + HCSpeciationCalculator chains.",
        coverage=("scale-default", "chain-tankvaporventing", "chain-hcspeciation",
                  "chain-baserate", "proc-12"),
        pp_assocs=((86, 12), (87, 12), (1, 12)),
    ),
    FixtureSpec(
        name="process-evap-leaks",
        description="Evap Fuel Leaks (process 13) — exercises LiquidLeakingCalculator "
                    "+ HCSpeciationCalculator chains.",
        coverage=("scale-default", "chain-liquidleaking", "chain-hcspeciation",
                  "chain-baserate", "proc-13"),
        pp_assocs=((86, 13), (87, 13), (1, 13)),
    ),
    FixtureSpec(
        name="process-refueling",
        description="Refueling Displacement (18) + Spillage (19) — exercises "
                    "RefuelingLossCalculator chain.",
        coverage=("scale-default", "chain-refuelingloss", "chain-hcspeciation",
                  "chain-baserate", "proc-18", "proc-19"),
        pp_assocs=REFUELING,
    ),
    FixtureSpec(
        name="process-crankcase-running",
        description="Crankcase Running Exhaust (process 15) criteria pollutants — "
                    "exercises CrankcaseEmissionCalculatorNonPM chain.",
        coverage=("scale-default", "chain-crankcase", "chain-baserate", "proc-15"),
        pp_assocs=CRANKCASE_RUNNING,
    ),
    FixtureSpec(
        name="process-crankcase-start",
        description="Crankcase Start Exhaust (process 16) criteria pollutants.",
        coverage=("scale-default", "chain-crankcase", "chain-baserate", "proc-16"),
        pp_assocs=CRANKCASE_START,
    ),
    FixtureSpec(
        name="process-crankcase-extidle",
        description="Crankcase Extended Idle Exhaust (process 17) criteria "
                    "pollutants.",
        coverage=("scale-default", "chain-crankcase", "chain-baserate", "proc-17"),
        pp_assocs=CRANKCASE_EXTIDLE,
    ),
    FixtureSpec(
        name="process-apu",
        description="Auxiliary Power Exhaust (process 91) — exercises APU code path "
                    "for combo long-haul truck.",
        coverage=("scale-default", "chain-co2ae", "chain-baserate", "proc-91"),
        onroad_selections=((2, 62),),  # diesel combo long-haul truck
        pp_assocs=((91, 91),),
    ),
    FixtureSpec(
        name="process-airtoxics",
        description="Onroad Air Toxics (Benzene, 1,3-Butadiene, Formaldehyde) "
                    "running exhaust — exercises AirToxicsCalculator chain.",
        coverage=("scale-default", "chain-airtoxics", "chain-hcspeciation",
                  "chain-baserate", "proc-1"),
        pp_assocs=AIRTOXICS_RUN,
    ),

    # -------------------------- Chain fixtures --------------------------------
    FixtureSpec(
        name="chain-tog-speciation",
        description="TOG (Total Organic Gases) + CH4 + NMHC running exhaust — "
                    "exercises TOGSpeciationCalculator chain endpoint.",
        coverage=("scale-default", "chain-togspeciation", "chain-hcspeciation",
                  "chain-baserate", "proc-1"),
        pp_assocs=TOG_SPECIATION,
    ),

    # -------------------------- Scale fixtures --------------------------------
    FixtureSpec(
        name="scale-county",
        description="County-domain inventory (model_domain=SINGLE) for Washtenaw — "
                    "requires a county data manager input database supplied at "
                    "snapshot-capture time.",
        coverage=("scale-county", "chain-co2ae", "chain-baserate",
                  "proc-1", "proc-2", "proc-90", "proc-99"),
        domain="SINGLE",
        pp_assocs=ENERGY_RUN_START_EXTIDLE_WTP,
    ),
    FixtureSpec(
        name="scale-project",
        description="Project-domain run (model_domain=PROJECT) anchored to "
                    "Washtenaw County. PROJECT mode resolves links and zones "
                    "from a user-supplied scale-input database; the RunSpec "
                    "carries the host-county selection only. Snapshot capture "
                    "supplies the project link/zone DB.",
        coverage=("scale-project", "chain-co2ae", "chain-baserate",
                  "proc-1", "proc-2"),
        domain="PROJECT",
        pp_assocs=((93, 1), (93, 2), (91, 1), (91, 2)),
        timespan=TimeSpan(year=2020, months=(7,), days=(5,),
                          begin_hour=8, end_hour=8),
        geographic_output_detail="LINK",
    ),
    FixtureSpec(
        name="scale-rates",
        description="Emission-rates-lookup mode (model_scale=MESOSCALE_LOOKUP / Rates).",
        coverage=("scale-rates", "chain-co2ae", "chain-baserate",
                  "proc-1", "proc-2"),
        scale="Rates",
        pp_assocs=((93, 1), (93, 2), (91, 1), (91, 2)),
    ),

    # -------------------------- NONROAD fixtures (10) -------------------------
    FixtureSpec(
        name="nr-recreational-county",
        description="NONROAD Recreational sector (snowmobiles/ATVs/etc.), "
                    "Washtenaw County.",
        coverage=("nr", "nr-county", "chain-nremission", "chain-baserate",
                  "proc-40"),
        models=("NONROAD",),
        onroad_selections=(),
        offroad_selections=((1, 1),),    # Gasoline, sector 1
        road_types=(),
        pp_assocs=((1, 40), (2, 40), (3, 40), (31, 40), (100, 40)),
        timespan=TimeSpan(year=2020, months=(7,), days=(5,),
                          begin_hour=6, end_hour=6),
    ),
    FixtureSpec(
        name="nr-construction-state",
        description="NONROAD Construction sector at state geography (Michigan).",
        coverage=("nr", "nr-state", "chain-nremission", "chain-baserate",
                  "proc-40"),
        models=("NONROAD",),
        geographic=(Geo("STATE", 26, "MICHIGAN"),),
        onroad_selections=(),
        offroad_selections=((1, 2), (2, 2)),    # Gas + Diesel construction
        road_types=(),
        pp_assocs=((1, 40), (2, 40), (3, 40), (31, 40), (100, 40)),
        geographic_output_detail="STATE",
    ),
    FixtureSpec(
        name="nr-industrial-county",
        description="NONROAD Industrial sector, Cook County IL.",
        coverage=("nr", "nr-county", "chain-nremission", "chain-baserate",
                  "proc-40"),
        models=("NONROAD",),
        geographic=(Geo("COUNTY", 17031, "ILLINOIS - Cook County"),),
        onroad_selections=(),
        offroad_selections=((2, 3),),    # Diesel industrial
        road_types=(),
        pp_assocs=((2, 40), (3, 40), (100, 40)),
    ),
    FixtureSpec(
        name="nr-lawn-garden-county",
        description="NONROAD Lawn/Garden sector, Washtenaw County.",
        coverage=("nr", "nr-county", "chain-nremission", "chain-baserate",
                  "proc-40"),
        models=("NONROAD",),
        onroad_selections=(),
        offroad_selections=((1, 4),),    # Gasoline lawn/garden
        road_types=(),
        pp_assocs=((1, 40), (2, 40), (3, 40), (100, 40)),
    ),
    FixtureSpec(
        name="nr-agriculture-state",
        description="NONROAD Agriculture sector at state geography (Iowa).",
        coverage=("nr", "nr-state", "chain-nremission", "chain-baserate",
                  "proc-40"),
        models=("NONROAD",),
        geographic=(Geo("STATE", 19, "IOWA"),),
        onroad_selections=(),
        offroad_selections=((2, 5),),    # Diesel agriculture
        road_types=(),
        pp_assocs=((2, 40), (3, 40), (31, 40), (100, 40)),
        geographic_output_detail="STATE",
    ),
    FixtureSpec(
        name="nr-commercial-nation",
        description="NONROAD Commercial sector at national rollup (US-total).",
        coverage=("nr", "nr-nation", "chain-nremission", "chain-baserate",
                  "proc-40"),
        models=("NONROAD",),
        geographic=(Geo("NATION", 0, "Nation total (US)"),),
        onroad_selections=(),
        offroad_selections=((1, 6), (2, 6)),
        road_types=(),
        pp_assocs=((1, 40), (2, 40), (3, 40), (100, 40)),
        geographic_output_detail="NATION",
    ),
    FixtureSpec(
        name="nr-logging-county",
        description="NONROAD Logging sector, Washtenaw County (chain saws etc).",
        coverage=("nr", "nr-county", "chain-nremission", "chain-baserate",
                  "proc-40"),
        models=("NONROAD",),
        onroad_selections=(),
        offroad_selections=((1, 7), (2, 7)),
        road_types=(),
        pp_assocs=((1, 40), (2, 40), (3, 40), (100, 40)),
    ),
    FixtureSpec(
        name="nr-airport-support-county",
        description="NONROAD Airport Support sector, Cook County IL (O'Hare).",
        coverage=("nr", "nr-county", "chain-nremission", "chain-baserate",
                  "proc-40"),
        models=("NONROAD",),
        geographic=(Geo("COUNTY", 17031, "ILLINOIS - Cook County"),),
        onroad_selections=(),
        offroad_selections=((2, 8),),
        road_types=(),
        pp_assocs=((2, 40), (3, 40), (31, 40), (100, 40)),
    ),
    FixtureSpec(
        name="nr-pleasure-craft-state",
        description="NONROAD Pleasure Craft sector (Recreational Marine), "
                    "state geography (Florida) — exercises RecMar evap "
                    "permeation chain in Phase-5 NONROAD-NR-rewrite reference.",
        coverage=("nr", "nr-state", "chain-nremission", "chain-nrhcspeciation",
                  "chain-nrairtoxics", "chain-baserate",
                  "proc-22", "proc-23", "proc-24", "proc-40"),
        models=("NONROAD",),
        geographic=(Geo("STATE", 12, "FLORIDA"),),
        onroad_selections=(),
        offroad_selections=((1, 11),),
        road_types=(),
        pp_assocs=((1, 40), (2, 40), (3, 40), (100, 40),
                   (20, 22), (20, 23), (20, 24)),
        geographic_output_detail="STATE",
    ),
    FixtureSpec(
        name="nr-railroad-support-nation",
        description="NONROAD Railroad Support sector at national rollup.",
        coverage=("nr", "nr-nation", "chain-nremission", "chain-baserate",
                  "proc-40"),
        models=("NONROAD",),
        geographic=(Geo("NATION", 0, "Nation total (US)"),),
        onroad_selections=(),
        offroad_selections=((2, 12),),
        road_types=(),
        pp_assocs=((2, 40), (3, 40), (31, 40), (100, 40)),
        geographic_output_detail="NATION",
    ),
]


# ---------------------------------------------------------------------------
# Coverage-matrix entry for the canonical sample-runspec.xml. Hand-derived
# (the file is not regenerated; see PRESERVE).
# ---------------------------------------------------------------------------

SAMPLE_RUNSPEC_COVERAGE = (
    "scale-default", "chain-co2ae", "chain-baserate",
    "proc-1", "proc-2", "proc-90", "proc-99",
)
SAMPLE_RUNSPEC_DESCRIPTION = (
    "Canonical MOVES SampleRunSpec.xml — single county/hour gasoline "
    "passenger car energy consumption. Byte-identical copy of "
    "testdata/SampleRunSpec.xml from the pinned canonical-MOVES tree."
)


# ---------------------------------------------------------------------------
# Coverage-matrix renderer
# ---------------------------------------------------------------------------

# Calculator role taxonomy w.r.t. CalculatorInfo.txt. Reference is
# `gov/epa/otaq/moves/master/framework/InterconnectionTracker.java` —
# `Chain<TAB>Output<TAB>Input` means Output depends on Input. A leaf
# (data-flow sink) is a calculator that appears as Output but never as
# Input. A foundation is one that appears as Input but never as Output
# (everyone depends on it; it consumes nothing else's output).
CHAIN_LABELS = {
    "chain-airtoxics":          ("AirToxicsCalculator",          ""),
    "chain-baserate":           ("BaseRateCalculator",           "foundation"),
    "chain-co2ae":              ("CO2AERunningStartExtendedIdleCalculator", "leaf"),
    "chain-crankcase":          ("CrankcaseEmissionCalculatorNonPM", ""),
    "chain-evappermeation":     ("EvaporativePermeationCalculator",  ""),
    "chain-hcspeciation":       ("HCSpeciationCalculator",       ""),
    "chain-liquidleaking":      ("LiquidLeakingCalculator",      ""),
    "chain-no":                 ("NOCalculator",                 ""),
    "chain-no2":                ("NO2Calculator",                ""),
    "chain-nrairtoxics":        ("NRAirToxicsCalculator",        "leaf"),
    "chain-nrhcspeciation":     ("NRHCSpeciationCalculator",     ""),
    "chain-nremission":         ("NonroadEmissionCalculator",    ""),
    "chain-pm10braketire":      ("PM10BrakeTireCalculator",      "leaf"),
    "chain-pm10emission":       ("PM10EmissionCalculator",       "leaf"),
    "chain-refuelingloss":      ("RefuelingLossCalculator",      ""),
    "chain-so2":                ("SO2Calculator",                ""),
    "chain-sulfatepm":          ("SulfatePMCalculator",          ""),
    "chain-tankvaporventing":   ("TankVaporVentingCalculator",   ""),
    "chain-togspeciation":      ("TOGSpeciationCalculator",      "leaf"),
}

SCALE_LABELS = {
    "scale-default":  "Default (national, Inv)",
    "scale-county":   "County (SINGLE, Inv)",
    "scale-project":  "Project (PROJECT, Inv)",
    "scale-rates":    "Rates (Rates / MESOSCALE_LOOKUP)",
}


def render_matrix(specs: list[FixtureSpec]) -> str:
    rows: list[tuple[str, str, list[int], list[str], list[str], str]] = []
    rows.append((
        "sample-runspec",
        SAMPLE_RUNSPEC_DESCRIPTION,
        [int(t.split("-")[1]) for t in SAMPLE_RUNSPEC_COVERAGE if t.startswith("proc-")],
        [SCALE_LABELS[t] for t in SAMPLE_RUNSPEC_COVERAGE if t in SCALE_LABELS],
        [CHAIN_LABELS[t][0] for t in SAMPLE_RUNSPEC_COVERAGE if t in CHAIN_LABELS],
        "ONROAD",
    ))
    for s in specs:
        procs = sorted({int(t.split("-")[1]) for t in s.coverage if t.startswith("proc-")})
        scales = [SCALE_LABELS[t] for t in s.coverage if t in SCALE_LABELS]
        chains = [CHAIN_LABELS[t][0] for t in s.coverage if t in CHAIN_LABELS]
        if any(t == "nr" for t in s.coverage):
            geog = next((t for t in s.coverage if t.startswith("nr-") and t != "nr"), "")
            scales = [f"NONROAD/{geog.split('-', 1)[1]}"] if geog else ["NONROAD"]
        model = "+".join(s.models)
        rows.append((s.name, s.description, procs, scales, chains, model))

    out = []
    out.append("# Phase 0 fixture coverage matrix")
    out.append("")
    out.append("Generated by `_generate.py`. Do **not** edit by hand — re-run the")
    out.append("generator after editing the fixture spec table.")
    out.append("")
    out.append("Column legend:")
    out.append("")
    out.append("- **Processes**: comma-separated `emissionprocess.processID` values "
               "exercised. See MOVES `emissionprocess` table for names.")
    out.append("- **Scale/Domain**: ModelScale (`<modelscale>`) × ModelDomain "
               "(`<modeldomain>`) coordinate; for NONROAD fixtures it is the "
               "geographic-aggregation level.")
    out.append("- **Calculators**: subset of `CalculatorInfo.txt` registrations the "
               "fixture forces MOVES to instantiate. Chain leaves "
               "(data-flow endpoints — produced by something, consumed "
               "by nothing) are in **bold**; the foundation calculator "
               "(BaseRate, depended on by everything) is _italicized_.")
    out.append("- **Model**: `<model>` selection (ONROAD / NONROAD).")
    out.append("")
    out.append("| Fixture | Description | Processes | Scale/Domain | Calculators | Model |")
    out.append("|---------|-------------|-----------|--------------|-------------|-------|")
    for name, desc, procs, scales, chains, model in rows:
        proc_str = ", ".join(str(p) for p in procs) if procs else "—"
        scale_str = "; ".join(scales) if scales else "—"
        chain_pieces = []
        for c in chains:
            tag = next((t for t, (cn, _) in CHAIN_LABELS.items() if cn == c), "")
            role = CHAIN_LABELS.get(tag, ("", ""))[1]
            if role == "leaf":
                chain_pieces.append(f"**{c}**")
            elif role == "foundation":
                chain_pieces.append(f"_{c}_")
            else:
                chain_pieces.append(c)
        chain_str = "; ".join(chain_pieces) if chain_pieces else "—"
        out.append(f"| `{name}.xml` | {desc} | {proc_str} | {scale_str} | "
                   f"{chain_str} | {model} |")

    out.append("")
    out.append("## Process coverage (forward index)")
    out.append("")
    out.append("Each ID below appears in at least one fixture above. Rows where "
               "the process never reaches the calculator (e.g. unsupported "
               "`pollutant × process × source-type` triple) will not contribute "
               "rows to the snapshot, but the registration will still be "
               "exercised.")
    out.append("")
    out.append("| Process ID | Name | Fixture(s) |")
    out.append("|------------|------|------------|")
    proc_to_fixtures: dict[int, list[str]] = {}
    for name, _, procs, _, _, _ in rows:
        for p in procs:
            proc_to_fixtures.setdefault(p, []).append(name)
    for pid in sorted(proc_to_fixtures):
        names = ", ".join(f"`{n}`" for n in proc_to_fixtures[pid])
        out.append(f"| {pid} | {PROCESSES.get(pid, '?')} | {names} |")

    out.append("")
    out.append("## Calculator coverage (forward index)")
    out.append("")
    out.append("Roles per `InterconnectionTracker.recordChain` semantics: a "
               "**leaf** is a calculator that produces output but has no "
               "downstream consumer (the data-flow endpoint of its chain); a "
               "**foundation** is one that everyone else depends on but "
               "consumes nothing itself. Hitting every leaf guarantees the "
               "chain rooted at it has been wired up.")
    out.append("")
    out.append("| Calculator | Role | Fixture(s) |")
    out.append("|------------|------|------------|")
    calc_to_fixtures: dict[str, list[str]] = {}
    for name, _, _, _, chains, _ in rows:
        for c in chains:
            calc_to_fixtures.setdefault(c, []).append(name)
    for tag, (calc, kind) in sorted(CHAIN_LABELS.items(), key=lambda kv: kv[1][0]):
        names = ", ".join(f"`{n}`" for n in calc_to_fixtures.get(calc, []))
        if not names:
            names = "_(missing)_"
        out.append(f"| {calc} | {kind or '—'} | {names} |")

    out.append("")
    return "\n".join(out) + "\n"


def main() -> int:
    written = 0
    seen_names = set()
    for spec in FIXTURES:
        if spec.name in seen_names:
            print(f"FATAL: duplicate fixture name {spec.name!r}", file=sys.stderr)
            return 1
        seen_names.add(spec.name)
        path = HERE / f"{spec.name}.xml"
        path.write_text(render(spec), encoding="utf-8")
        written += 1
    matrix_path = HERE / "coverage-matrix.md"
    matrix_path.write_text(render_matrix(FIXTURES), encoding="utf-8")
    print(f"[generate] wrote {written} fixtures + preserved {len(PRESERVE)} canonical")
    print(f"[generate] wrote coverage matrix to {matrix_path.name}")
    print(f"[generate] dir = {HERE}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
