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

Catalogue drift
---------------
The committed fixture set has outgrown this table: several XMLs were
hand-edited after capture (carrier pollutants added, NONROAD process IDs
corrected from the non-existent "40" to the real Running Exhaust "1"),
and a second wave of fixtures
(`nr-mixed-nonroad`, `mixed-onroad`, `chain-nonhaptog`, the `*-single`
county fixtures, the `error-bad-*` fixtures, ...) was added to the
directory without a spec entry here. Regenerating those XMLs would
invalidate their captured snapshots, so `main()` refuses to overwrite a
committed file that differs from what this table renders; it reports them
as "drifted" and moves on. Pass `--force` to overwrite anyway, or name
individual fixtures on the command line to regenerate just those:

    python3 characterization/fixtures/_generate.py nr-airtoxics-lawn-garden-county
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
    21:  "Ethanol",
    23:  "Naphthalene particle",
    24:  "1,3-Butadiene",
    25:  "Formaldehyde",
    26:  "Acetaldehyde",
    27:  "Acrolein",
    31:  "Sulfur Dioxide (SO2)",
    32:  "Nitrogen Oxide (NO)",
    33:  "Nitrogen Dioxide (NO2)",
    34:  "Nitrous Acid (HONO)",
    40:  "2,2,4-Trimethylpentane",
    41:  "Ethyl Benzene",
    42:  "Hexane",
    43:  "Propionaldehyde",
    44:  "Styrene",
    45:  "Toluene",
    46:  "Xylene",
    60:  "Mercury Elemental Gaseous",
    63:  "Arsenic Compounds",
    65:  "Chromium 6+",
    66:  "Manganese Compounds",
    67:  "Nickel Compounds",
    69:  "Fluoranthene particle",
    79:  "Non-Methane Hydrocarbons",
    80:  "Non-Methane Organic Gases",
    86:  "Total Organic Gases",
    87:  "Volatile Organic Compounds",
    88:  "NonHAPTOG",
    90:  "Atmospheric CO2",
    91:  "Total Energy Consumption",
    92:  "Petroleum Energy Consumption",
    93:  "Fossil Fuel Energy Consumption",
    98:  "CO2 Equivalent",
    99:  "Brake Specific Fuel Consumption (BSFC)",
    100: "Primary Exhaust PM10  - Total",
    106: "Primary PM10 - Brakewear Particulate",
    107: "Primary PM10 - Tirewear Particulate",
    110: "Primary Exhaust PM2.5 - Total",
    116: "Primary PM2.5 - Brakewear Particulate",
    117: "Primary PM2.5 - Tirewear Particulate",
    131: "Octachlorodibenzo-p-dioxin",
    142: "2,3,7,8-Tetrachlorodibenzo-p-Dioxin",
    169: "Fluoranthene gas",
    185: "Naphthalene gas",
    # The only 'Mechanisms'-display-group pollutant in movesdb20241112.
    # `MOVESInstantiator` keys TOGSpeciationCalculator off this exact name
    # (process = null, i.e. any process); `TOGSpeciationCalculator.doExecute`
    # derives mechanismID = 1 + (3000-1000)/500 = 5, the sole mechanism in
    # `integratedSpeciesSet`. The CB05 pseudo-pollutants 1000-1018 that
    # CalculatorInfo.txt registers do NOT exist in this default DB
    # (max(pollutantID) = 3000, 116 rows) and cannot be selected.
    3000: "NonHAPTOG Mechanism",
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
    # NONROAD's pseudo road type. MOVES emits it without a modelCombination
    # attribute (see `render`), matching the committed nr-*.xml fixtures.
    100: "Nonroad",
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
    # `<month key=>` is a 0-based INDEX into TimeSpan.allMonths, NOT a monthID
    # (RunSpecXML.java:501-509 -> TimeSpan.getMonthByIndex, TimeSpan.java:156-161)
    # — exactly the trap `day_attr` documents below. So `months=(7,)` renders
    # `<month key="7"/>` and selects allMonths[7] = **August**, monthID 8, not
    # July. Verified against canonical output: the scale-project snapshot's
    # `db__out_scale_project__movesoutput` carries monthID = 8 for a fixture
    # written as month 7.
    #
    # All 54 committed fixtures use the `key` form, so all of them run one
    # month later than their spec entry reads.
    #
    # UNLIKE the `<day key=>` bug, this is NOT a port-vs-canonical divergence
    # and NOTHING needs recapturing. The port resolves the same way canonical
    # does — `XmlIndexedId::to_id` returns `key + 1`
    # (moves-runspec/src/xml_format.rs:667-676) and writes `key = m - 1` back
    # out at :137 — so both sides see monthID 8 and the snapshots are correct
    # and self-consistent. What is wrong is only what the spec entry SAYS.
    #
    # Two different edits get confused here, so be explicit about which one
    # you mean:
    #
    #   `<month id="8"/>`  — provably a NO-OP. It is the same month that
    #                        `key="7"` already selects, in canonical
    #                        (getMonthByID) and in the port alike. It changes
    #                        no output, only `runspec_sha256` in provenance.
    #                        This is the honest spelling and is cheap.
    #   `<month id="7"/>`  — a real change. It would make the corpus mean
    #                        the July it has always CLAIMED to mean, and
    #                        invalidates all 43 published snapshots. That is
    #                        a recapture decision, not an editing one.
    #
    # Until one of those is taken, read `months=(N,)` as "monthID N+1" and
    # write any month-keyed input-DB filter against N+1 (see
    # ../county-inputs/washtenaw-project/setup-project.sql).
    months: tuple[int, ...] = (7,)         # index 7 -> August (monthID 8)
    days: tuple[int, ...] = (5,)           # Weekdays
    begin_hour: int = 6                    # hour-of-day index per hourofanyday
    end_hour: int = 6
    aggregate_by: Optional[str] = None     # "Hour"/"Day"/"Month"/"Year"
    # `<day key=>` is an *index* into TimeSpan.allDays; `<day id=>` is the
    # dayID (see RunSpecXML.processTimeSpan -> getDayByIndex/getDayByID).
    # allDays has two entries ordered by ascending dayID, so the only valid
    # keys are 0 (-> dayID 2, weekend) and 1 (-> dayID 5, weekday). key="5"
    # resolves to null, the day selection is left empty, and MOVES falls
    # back to running every day.
    #
    # This defaulted to "key" until 2026-09-08, which silently made 26 of the
    # 40 populated snapshots two-day runs -- measured in
    # ../audit-results/20260908T1120-day-selection-audit.md. Always emit the
    # dayID form.
    day_attr: str = "id"                   # "id" (dayID) or "key" (index)

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
        out.append(f'\t\t<day {ts.day_attr}="{d}"/>')
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
        if rt == 100:   # NONROAD pseudo road type — no modelCombination
            out.append(f'\t\t<roadtype roadtypeid="{rt}"'
                       f' roadtypename="{ROAD_TYPES[rt]}"/>')
        else:
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

# NONROAD air-toxics selection, all on Running Exhaust (process 1).
#
# Both NR calculators are *chained*, not MasterLoop subscribers, so what
# makes them run is the pollutant set, not the process set:
#
#   NRHCSpeciationCalculator.buildPollutantAndProcessRequirements() keeps an
#   entry only when the runspec has its **output** (pollutant in
#   {5,79,80,86,87} with a nonroad-affected process) — hence the CH4/NMHC/
#   NMOG/TOG/VOC block below.
#
#   NRAirToxicsCalculator.doExecute() keeps an entry only when the runspec
#   has its **input**: VOC (87) for nrATRatio + nrPAHGasRatio, PM2.5 (110)
#   for nrPAHParticleRatio, BSFC (99) for nrDioxinEmissionRate +
#   nrMetalEmissionRate, NMOG (80) for the NonHAPTOG pass. Those inputs are
#   what the earlier nr-*.xml fixtures never selected, which is why
#   nr-pleasure-craft-state loads the class but emits nothing from it.
#
# ExecutionRunSpec.flagRequiredPollutantProcesses() returns early for
# NONROAD ("Nonroad has no silent pollutants/processes added for the user"),
# so every link of the chain has to be listed explicitly.
# ---------------------------------------------------------------------------
# Onroad pollutant sets for the three previously-unreached onroad calculators
# (SO2Calculator, CO2AERunningStartExtendedIdleCalculator,
# TOGSpeciationCalculator). All on Running Exhaust (process 1); every one of
# the three registers there.
# ---------------------------------------------------------------------------

# Inputs each of the three chained calculators gates on in doExecute().
# `SO2Calculator.doExecute` and the Atmospheric-CO2 half of
# `CO2AERunningStartExtendedIdleCalculator.doExecute` both bail out unless
# "Total Energy Consumption" is selected *on the same process*; the CO2e half
# needs CH4 + N2O. ExecutionRunSpec.flagRequiredPollutantProcesses' `needs[]`
# table does NOT cover these, so they must be selected explicitly — that is
# exactly why expand-criteria (SO2 without energy) class-loads SO2Calculator
# and emits no pollutant-31 row.
SO2_CO2E_INPUTS = (
    (91, 1),     # Total Energy Consumption — SO2 + Atmospheric CO2 input
    (5, 1),      # Methane (CH4)            — CO2 Equivalent input
    (6, 1),      # Nitrous Oxide (N2O)      — CO2 Equivalent input
)

SO2_CO2E_OUTPUTS = (
    (31, 1),     # Sulfur Dioxide (SO2)  — SO2Calculator
    (90, 1),     # Atmospheric CO2       — CO2AERunningStartExtendedIdleCalculator
    (98, 1),     # CO2 Equivalent        — CO2AERunningStartExtendedIdleCalculator
)

# HCSpeciationCalculator chain. NMOG (80) is the positive term of the
# TOG-speciation integration; VOC (87) is AirToxicsCalculator's input.
HC_SPECIATION = (
    (1, 1),      # Total Gaseous Hydrocarbons
    (5, 1),      # Methane (CH4)
    (79, 1),     # Non-Methane Hydrocarbons
    (80, 1),     # Non-Methane Organic Gases
    (86, 1),     # Total Organic Gases
    (87, 1),     # Volatile Organic Compounds
)

# The 14 integrated species of mechanism 5, set 4 ("Set 4 OAQPS Jun, 2013 Sub
# from TOG") — every row of movesdb20241112's `integratedSpeciesSet`, all with
# useISSyn='Y' and all onroad-valid on process 1. TOGSpeciationCalculator
# subtracts these from NMOG to get the NonHAPTOG residual; AirToxicsCalculator
# produces them.
TOG_INTEGRATED_SPECIES = (
    (20, 1),     # Benzene
    (21, 1),     # Ethanol
    (24, 1),     # 1,3-Butadiene
    (25, 1),     # Formaldehyde
    (26, 1),     # Acetaldehyde
    (27, 1),     # Acrolein
    (40, 1),     # 2,2,4-Trimethylpentane
    (41, 1),     # Ethyl Benzene
    (42, 1),     # Hexane
    (43, 1),     # Propionaldehyde
    (44, 1),     # Styrene
    (45, 1),     # Toluene
    (46, 1),     # Xylene
    (185, 1),    # Naphthalene gas
)

def _dedupe(*groups: tuple[tuple[int, int], ...]) -> tuple[tuple[int, int], ...]:
    """Concatenate pollutant/process groups, dropping repeats, order preserved.

    The groups below overlap (CH4 is both a CO2-equivalent input and an
    HCSpeciation output), and a repeated <pollutantprocessassociation> is
    noise in the RunSpec — MOVES stores them in a TreeSet, so the duplicate
    has no effect but does make the fixture harder to read.
    """
    seen, out = set(), []
    for g in groups:
        for pp in g:
            if pp not in seen:
                seen.add(pp)
                out.append(pp)
    return tuple(out)


# The mechanism gate plus its product.
TOG_MECHANISM = (
    (3000, 1),   # NonHAPTOG Mechanism — instantiates TOGSpeciationCalculator
                 # and is the pollutant doExecute() checks for
    (88, 1),     # NonHAPTOG — the only pollutant the calculator's SQL emits
                 # (ExecutionRunSpec.addLumpedSpecies adds it implicitly too)
)

NR_AIRTOXICS_INPUTS = (
    (1, 1),      # THC   — NonroadEmissionCalculator, feeds NMHC/CH4
    (99, 1),     # BSFC  — feeds nrDioxinEmissionRate + nrMetalEmissionRate
    (100, 1),    # PM10  — NonroadEmissionCalculator
    (110, 1),    # PM2.5 — feeds nrPAHParticleRatio
)

NR_HC_SPECIATION = (
    (5, 1),      # Methane        \
    (79, 1),     # NMHC            | NRHCSpeciationCalculator outputs;
    (80, 1),     # NMOG            | NMOG + VOC are NRAirToxics inputs
    (86, 1),     # TOG             |
    (87, 1),     # VOC            /
)

NR_AIRTOXICS_OUTPUTS = (
    # nrATRatio (input VOC 87)
    (20, 1), (21, 1), (24, 1), (25, 1), (26, 1), (27, 1), (45, 1), (46, 1),
    # nrPAHGasRatio (input VOC 87)
    (169, 1), (185, 1),
    # nrPAHParticleRatio (input PM2.5 110)
    (23, 1), (69, 1),
    # nrMetalEmissionRate (input BSFC 99)
    (60, 1), (63, 1), (65, 1), (66, 1), (67, 1),
    # nrDioxinEmissionRate (input BSFC 99)
    (131, 1), (142, 1),
    # NonHAPTOG pass (input NMOG 80)
    (88, 1),
)

NR_AIRTOXICS = NR_AIRTOXICS_INPUTS + NR_HC_SPECIATION + NR_AIRTOXICS_OUTPUTS


FIXTURES: list[FixtureSpec] = [
    # -------------------------- Expansion fixtures ----------------------------
    FixtureSpec(
        name="expand-day",
        description="Sample expanded to a full day (hours 1-24, weekday + weekend).",
        coverage=("expand-day", "scale-default", "proc-1", "proc-2", "proc-90",
                  "proc-99", "chain-baserate"),
        timespan=TimeSpan(year=2020, months=(7,), days=(2, 5),
                          begin_hour=1, end_hour=24, aggregate_by="Hour"),
        pp_assocs=ENERGY_RUN_START_EXTIDLE_WTP,
    ),
    FixtureSpec(
        name="expand-month",
        description="Sample expanded across four months (Jan/Apr/Jul/Oct).",
        coverage=("expand-month", "scale-default", "proc-1", "proc-2", "proc-90",
                  "proc-99", "chain-baserate"),
        timespan=TimeSpan(year=2020, months=(1, 4, 7, 10), days=(5,),
                          begin_hour=6, end_hour=6, aggregate_by="Month"),
        pp_assocs=ENERGY_RUN_START_EXTIDLE_WTP,
    ),
    FixtureSpec(
        name="expand-counties",
        description="Sample expanded across three diverse counties (Washtenaw MI, "
                    "Cook IL, Los Angeles CA).",
        coverage=("expand-counties", "scale-default", "proc-1", "proc-2",
                  "proc-90", "proc-99", "chain-baserate"),
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
        coverage=("expand-fueltype", "scale-default", "proc-1", "proc-2",
                  "proc-90", "proc-99", "chain-baserate"),
        onroad_selections=((1, 21), (2, 21), (1, 32), (2, 32)),
        pp_assocs=ENERGY_RUN_START_EXTIDLE_WTP,
    ),
    FixtureSpec(
        name="expand-sourcetype",
        description="Sample expanded across multiple onroad source types "
                    "(motorcycle, pass car, passenger truck, refuse truck, "
                    "long-haul combo).",
        coverage=("expand-sourcetype", "scale-default", "proc-1", "proc-2",
                  "proc-90", "proc-99", "chain-baserate"),
        onroad_selections=((1, 11), (1, 21), (1, 31), (1, 51), (2, 62)),
        pp_assocs=ENERGY_RUN_START_EXTIDLE_WTP,
    ),
    FixtureSpec(
        name="expand-criteria",
        description="Sample swapped to criteria pollutants (NOx, CO, THC, SO2) "
                    "for running + start exhaust.",
        coverage=("expand-criteria", "scale-default", "proc-1", "proc-2",
                  "chain-baserate"),
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
        coverage=("scale-default", "proc-11", "chain-evappermeation"),
        pp_assocs=((86, 11), (87, 11), (1, 11)),
    ),
    FixtureSpec(
        name="process-evap-fvv",
        description="Evap Fuel Vapor Venting (process 12) — exercises "
                    "TankVaporVentingCalculator + HCSpeciationCalculator chains.",
        coverage=("scale-default", "proc-12", "chain-tankvaporventing"),
        pp_assocs=((86, 12), (87, 12), (1, 12)),
    ),
    FixtureSpec(
        name="process-evap-leaks",
        description="Evap Fuel Leaks (process 13) — exercises LiquidLeakingCalculator "
                    "+ HCSpeciationCalculator chains.",
        coverage=("scale-default", "proc-13", "chain-liquidleaking"),
        pp_assocs=((86, 13), (87, 13), (1, 13)),
    ),
    FixtureSpec(
        name="process-refueling",
        description="Refueling Displacement (18) + Spillage (19) — exercises "
                    "RefuelingLossCalculator chain.",
        coverage=("scale-default", "proc-18", "proc-19", "chain-refuelingloss"),
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
        coverage=("scale-default", "proc-16"),
        pp_assocs=CRANKCASE_START,
    ),
    FixtureSpec(
        name="process-crankcase-extidle",
        description="Crankcase Extended Idle Exhaust (process 17) criteria "
                    "pollutants.",
        coverage=("scale-default", "proc-17"),
        pp_assocs=CRANKCASE_EXTIDLE,
    ),
    FixtureSpec(
        name="process-apu",
        description="Auxiliary Power Exhaust (process 91) — exercises APU code path "
                    "for combo long-haul truck.",
        coverage=("scale-default", "proc-91"),
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
        coverage=("scale-default", "proc-1", "chain-baserate",
                  "chain-hcspeciation"),
        pp_assocs=TOG_SPECIATION,
    ),

    # -------------------------- Scale fixtures --------------------------------
    FixtureSpec(
        name="scale-county",
        description="County-domain inventory (model_domain=SINGLE) for Washtenaw — "
                    "requires a county data manager input database supplied at "
                    "snapshot-capture time.",
        coverage=("scale-county", "proc-1", "proc-2", "proc-90", "proc-99",
                  "chain-baserate"),
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
        coverage=("scale-project", "proc-1", "proc-2", "chain-baserate"),
        domain="PROJECT",
        pp_assocs=((93, 1), (93, 2), (91, 1), (91, 2)),
        timespan=TimeSpan(year=2020, months=(7,), days=(5,),
                          begin_hour=8, end_hour=8),
        geographic_output_detail="LINK",
        # PROJECT-domain input databases are checked by MOVES's domain
        # database validator (MOVESAPI.java:858, gated on
        # RunSpec.skipDomainDatabaseValidation) before the run starts. The
        # validator compares the supplied database against an expected
        # checklist and aborts on any mismatch; the minimal project DB in
        # county-inputs/washtenaw-project/setup-project.sql trips it on
        # fuel-formulation and unused-fuel-type warnings that have no
        # bearing on what this fixture measures. Without this element the
        # run dies before MOVESInstantiator runs and no execution database
        # is created at all (measured: the capture dumps 46 tables instead
        # of 360). The flag skips an input QA gate only — it changes no
        # calculation, and it is the same switch MOVES's own GUI exposes as
        # "Do Not Perform Domain Database Validation" (Alt+8).
        extra_root_elements='\t<skipdomaindatabasevalidation selected="true"/>',
    ),
    FixtureSpec(
        name="scale-rates",
        description="Emission-rates-lookup mode (model_scale=MESOSCALE_LOOKUP / Rates).",
        coverage=("scale-rates", "proc-1", "proc-2", "chain-baserate"),
        scale="Rates",
        pp_assocs=((93, 1), (93, 2), (91, 1), (91, 2)),
    ),

    # -------------------------- NONROAD fixtures (10) -------------------------
    FixtureSpec(
        name="nr-recreational-county",
        description="NONROAD Recreational sector (snowmobiles/ATVs/etc.), "
                    "Washtenaw County.",
        coverage=("nr", "nr-county", "proc-40", "chain-nremission"),
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
        coverage=("nr", "nr-state", "proc-40", "chain-nremission"),
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
        coverage=("nr", "nr-county", "proc-40", "chain-nremission"),
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
        coverage=("nr", "nr-county", "proc-40", "chain-nremission"),
        models=("NONROAD",),
        onroad_selections=(),
        offroad_selections=((1, 4),),    # Gasoline lawn/garden
        road_types=(),
        pp_assocs=((1, 40), (2, 40), (3, 40), (100, 40)),
    ),
    FixtureSpec(
        name="nr-agriculture-state",
        description="NONROAD Agriculture sector at state geography (Iowa).",
        coverage=("nr", "nr-state", "proc-40", "chain-nremission"),
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
        coverage=("nr", "nr-nation", "proc-40", "chain-nremission"),
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
        coverage=("nr", "nr-county", "proc-40", "chain-nremission"),
        models=("NONROAD",),
        onroad_selections=(),
        offroad_selections=((1, 7), (2, 7)),
        road_types=(),
        pp_assocs=((1, 40), (2, 40), (3, 40), (100, 40)),
    ),
    FixtureSpec(
        name="nr-airport-support-county",
        description="NONROAD Airport Support sector, Cook County IL (O'Hare).",
        coverage=("nr", "nr-county", "proc-40", "chain-nremission"),
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
        # Measured against the captured snapshot: NRHCSpeciationCalculator is
        # never even class-loaded here, and NRAirToxicsCalculator loads but
        # emits nothing — this fixture selects Benzene on processes 22/23/24,
        # which is NRAirToxics' *output*, while doExecute() gates on its
        # *input* (VOC 87 / PM2.5 110 / BSFC 99 / NMOG 80). The snapshot's
        # MOVESOutput carries only (1,1), (2,1), (3,1), (100,1). Both chain
        # tags were therefore aspirational; nr-airtoxics-lawn-garden-county
        # is the fixture that actually reaches them.
        coverage=("nr", "nr-state", "proc-22", "proc-23", "proc-24", "proc-40",
                  "chain-nremission"),
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
        coverage=("nr", "nr-nation", "proc-40", "chain-nremission"),
        models=("NONROAD",),
        geographic=(Geo("NATION", 0, "Nation total (US)"),),
        onroad_selections=(),
        offroad_selections=((2, 12),),
        road_types=(),
        pp_assocs=((2, 40), (3, 40), (31, 40), (100, 40)),
        geographic_output_detail="NATION",
    ),

    # ---------------- NONROAD air-toxics fixture (11th nr-*) ------------------
    FixtureSpec(
        name="nr-airtoxics-lawn-garden-county",
        description="NONROAD air toxics — Lawn/Garden sector (gasoline), "
                    "Washtenaw County. Vehicle/geography/time selections are "
                    "identical to nr-lawn-garden-county; only the pollutant set "
                    "differs, so the two snapshots isolate the air-toxics "
                    "chain. Selects the HC-speciation species (CH4/NMHC/NMOG/"
                    "TOG/VOC) that NRHCSpeciationCalculator emits plus the "
                    "VOC/PM2.5/BSFC/NMOG inputs that NRAirToxicsCalculator "
                    "gates on, so both previously-unreached NONROAD "
                    "calculators instantiate and emit.",
        coverage=("nr", "nr-county", "proc-1", "chain-nremission",
                  "chain-nrhcspeciation", "chain-nrairtoxics"),
        models=("NONROAD",),
        onroad_selections=(),
        offroad_selections=((1, 4),),    # Gasoline lawn/garden
        road_types=(100,),
        pp_assocs=NR_AIRTOXICS,
        timespan=TimeSpan(year=2020, months=(7,), days=(5,),
                          begin_hour=6, end_hour=6),
    ),

    # ------- Second-wave fixtures caught up into the table (2026-09-08) ------
    # These two were added to the directory without a spec entry, so the
    # coverage matrix has never listed them even though both have captured
    # snapshots. Their committed XML is authoritative — the renderer's drift
    # guard leaves it alone; the entries exist so the matrix stops
    # under-reporting. NOCalculator/NO2Calculator were listed as "(missing)"
    # once the false expand-criteria tags came off, when in fact
    # process-nox-speciation covers both.
    FixtureSpec(
        name="process-nox-speciation",
        description="NOx speciation running exhaust — NO (32), NO2 (33) and "
                    "HONO (34) from NOx (3). Exercises NOCalculator (NO + "
                    "HONO) and NO2Calculator.",
        coverage=("scale-default", "chain-no", "chain-no2", "chain-baserate",
                  "proc-1"),
        pp_assocs=((32, 1), (33, 1), (34, 1), (3, 1)),
    ),
    FixtureSpec(
        name="chain-nonhaptog",
        description="NonHAPTOG (88) requested on running exhaust alongside the "
                    "HC-speciation chain, but with no 'Mechanisms' pollutant. "
                    "Snapshot proves the gate: TOGSpeciationCalculator emits "
                    "nothing and the output is byte-identical to "
                    "chain-tog-speciation's.",
        coverage=("scale-default", "chain-hcspeciation", "chain-baserate",
                  "proc-1"),
        pp_assocs=((88, 1), (1, 1), (5, 1), (79, 1), (80, 1), (86, 1)),
    ),

    # ------- Onroad fixtures for the last three unreached calculators --------
    # Geography/time/vehicle/road type are byte-identical to chain-nonhaptog
    # and to each other; only the pollutant set differs between the pair, so
    # the two snapshots isolate exactly what the four trigger pollutants
    # (31, 90, 98, 3000) add to the loaded class set.
    FixtureSpec(
        name="chain-so2-co2e-mechanism",
        description="Onroad running exhaust reaching the last three unreached "
                    "MOVES calculators in one run: SO2Calculator (SO2 31 + "
                    "its Total Energy Consumption 91 input), "
                    "CO2AERunningStartExtendedIdleCalculator (Atmospheric CO2 "
                    "90 + CO2 Equivalent 98, with energy/CH4/N2O inputs) and "
                    "TOGSpeciationCalculator (NonHAPTOG Mechanism 3000, the "
                    "sole 'Mechanisms' pollutant in movesdb20241112, plus the "
                    "14 integrated species of mechanism 5 that the NonHAPTOG "
                    "residual subtracts from NMOG). Geography, time, vehicle "
                    "and road type match chain-nonhaptog and "
                    "chain-so2-co2e-mechanism-control exactly.",
        coverage=("scale-default", "chain-so2", "chain-co2ae",
                  "chain-togspeciation", "chain-airtoxics",
                  "chain-hcspeciation", "chain-baserate", "proc-1"),
        pp_assocs=_dedupe(SO2_CO2E_INPUTS, SO2_CO2E_OUTPUTS, HC_SPECIATION,
                          TOG_INTEGRATED_SPECIES, TOG_MECHANISM),
    ),
    FixtureSpec(
        name="chain-so2-co2e-mechanism-control",
        description="Control for chain-so2-co2e-mechanism: identical geography, "
                    "time, vehicle, road type and pollutant set MINUS the four "
                    "trigger pollutants SO2 (31), Atmospheric CO2 (90), CO2 "
                    "Equivalent (98) and NonHAPTOG Mechanism (3000). Total "
                    "Energy Consumption, CH4 and N2O are still selected, so "
                    "the difference in the two snapshots' loaded-class sets is "
                    "attributable to the trigger pollutants alone.",
        coverage=("scale-default", "chain-airtoxics", "chain-hcspeciation",
                  "chain-baserate", "proc-1"),
        pp_assocs=_dedupe(SO2_CO2E_INPUTS, HC_SPECIATION,
                          TOG_INTEGRATED_SPECIES),
    ),
]


# ---------------------------------------------------------------------------
# Coverage-matrix entry for the canonical sample-runspec.xml. Hand-derived
# (the file is not regenerated; see PRESERVE).
# ---------------------------------------------------------------------------

SAMPLE_RUNSPEC_COVERAGE = (
    "scale-default", "chain-baserate",
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

# What a `chain-<calculator>` coverage tag asserts
# ------------------------------------------------
# The fixture makes MOVES **instantiate that calculator AND emit at least one
# MOVESOutput row on a (pollutant, process) pair the calculator owns** in
# `calculator-dag.json`'s registration list. Both halves are required: a
# class-loaded calculator that never emits verifies nothing, and that exact
# confusion has produced two false coverage claims in this corpus
# (nr-pleasure-craft-state, corrected in 2d32946; chain-tog-speciation,
# corrected below).
#
# Every tag was re-derived from the captured snapshots on 2026-09-08 by
# intersecting three measured sets per fixture:
#   * owners of each (pollutantID, processID) in
#     snapshots/<f>/tables/db__out_*__movesoutput.parquet, per
#     calculator-chains/calculator-dag.json;
#   * the class names in snapshots/<f>/execution-trace.json;
#   * the model (a NONROAD run instantiates no onroad calculator and vice
#     versa — note TOGSpeciationCalculator is class-loaded in *every*
#     snapshot because ExecutionRunSpec calls its static
#     needsFinalAggregation(), so for it the class-load entry is not
#     evidence of anything).
# That sweep removed these tags, each contradicted by its own snapshot:
#   * chain-co2ae from sample-runspec, expand-day, expand-month,
#     expand-counties, expand-fueltype-diesel, expand-sourcetype and
#     process-apu: CO2AERunningStartExtendedIdleCalculator is in none of
#     their execution-trace.json class lists. MOVESInstantiator keys it off
#     the pollutant names "Atmospheric CO2" / "CO2 Equivalent"; these
#     fixtures select only Total/Petroleum/Fossil energy (91/92/93), so it
#     never instantiates. Removed from scale-county / scale-project /
#     scale-rates on the same reasoning — those three have no snapshot (they
#     need an input database this environment lacks), so that is inference
#     from the RunSpec, not measurement.
#   * chain-so2, chain-no, chain-no2 and chain-hcspeciation from
#     expand-criteria: its MOVESOutput is (1,1), (2,1), (3,1) only.
#     NOCalculator, NO2Calculator and HCSpeciationCalculator are not even
#     class-loaded; SO2Calculator is, but emits nothing, because
#     SO2Calculator.doExecute bails out unless Total Energy Consumption is
#     selected on the same process and this fixture omits pollutant 91.
#   * chain-togspeciation from chain-tog-speciation: despite the name it
#     emits (1,1) (5,1) (79,1) (80,1) (86,1) — byte-identical to
#     chain-nonhaptog — i.e. HCSpeciationCalculator's outputs and no
#     pollutant 88. TOGSpeciationCalculator.doExecute returns null with no
#     'Mechanisms' pollutant selected. chain-so2-co2e-mechanism is the
#     fixture that actually reaches it.
#   * chain-baserate from the eleven nr-* fixtures: BaseRateCalculator is
#     absent from every NONROAD execution-trace.json.
#   * chain-baserate and chain-hcspeciation from process-evap-permeation,
#     process-evap-fvv, process-evap-leaks and process-refueling: both are
#     instantiated, but the only rows are (1,11) / (1,12) / (1,13) /
#     (1,18)+(1,19), which the evaporative calculators own.
#   * every chain tag from process-apu, process-crankcase-start and
#     process-crankcase-extidle: their MOVESOutput has zero rows. They are
#     structural-gate fixtures; the gate is their value, not a calculator.
#
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
    out.append("- **Calculators**: calculators the fixture makes MOVES "
               "instantiate **and** emit at least one MOVESOutput row for, on "
               "a (pollutant, process) pair the calculator owns in "
               "`CalculatorInfo.txt`. Class-loaded-but-silent does **not** "
               "count — see the note under the table. Chain leaves "
               "(data-flow endpoints — produced by something, consumed "
               "by nothing) are in **bold**; the foundation calculator "
               "(BaseRate, depended on by everything) is _italicized_.")
    out.append("- **Model**: `<model>` selection (ONROAD / NONROAD).")
    out.append("")
    out.append("A row with an empty **Calculators** cell is a *structural-gate* "
               "fixture: MOVES runs, instantiates calculators, and produces "
               "zero MOVESOutput rows. That gate is the fixture's value; it "
               "verifies no calculator.")
    out.append("")
    out.append("Every entry in the Calculators column was re-derived from the "
               "captured snapshots on 2026-09-08 by intersecting, per fixture, "
               "the owners of each emitted `(pollutantID, processID)` "
               "(`calculator-chains/calculator-dag.json`) with the class list "
               "in `snapshots/<fixture>/execution-trace.json`. The sweep "
               "removed eleven `chain-baserate` tags from the NONROAD "
               "fixtures, ten `chain-co2ae` tags, and the `chain-so2` / "
               "`chain-no` / `chain-no2` / `chain-hcspeciation` / "
               "`chain-togspeciation` tags listed in `_generate.py`; each was "
               "contradicted by its own snapshot. Note that "
               "`TOGSpeciationCalculator` is class-loaded in *every* snapshot "
               "(ExecutionRunSpec calls its static `needsFinalAggregation()`), "
               "so for that calculator only emitted rows are evidence.")
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


def main(argv: Optional[list[str]] = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    force = "--force" in argv
    argv = [a for a in argv if a != "--force"]
    wanted = set(argv)   # empty => every fixture in the table

    written, drifted, skipped = [], [], []
    seen_names = set()
    for spec in FIXTURES:
        if spec.name in seen_names:
            print(f"FATAL: duplicate fixture name {spec.name!r}", file=sys.stderr)
            return 1
        seen_names.add(spec.name)
        if wanted and spec.name not in wanted:
            skipped.append(spec.name)
            continue
        path = HERE / f"{spec.name}.xml"
        body = render(spec)
        if path.exists() and path.read_text(encoding="utf-8") != body and not force:
            # The committed XML was hand-edited after this table was written.
            # Overwriting it would silently invalidate its captured snapshot,
            # so leave it alone and say so. Pass --force to overwrite anyway.
            drifted.append(spec.name)
            continue
        path.write_text(body, encoding="utf-8")
        written.append(spec.name)
    matrix_path = HERE / "coverage-matrix.md"
    matrix_path.write_text(render_matrix(FIXTURES), encoding="utf-8")
    print(f"[generate] wrote {len(written)} fixtures "
          f"+ preserved {len(PRESERVE)} canonical")
    if drifted:
        print(f"[generate] KEPT {len(drifted)} drifted fixture(s) — the "
              f"committed XML differs from this table and was NOT overwritten:")
        for n in drifted:
            print(f"[generate]   {n}.xml")
        print("[generate] (re-run with --force to overwrite; see the module "
              "docstring on catalogue drift)")
    if wanted:
        unknown = sorted(wanted - seen_names)
        if unknown:
            print(f"FATAL: unknown fixture name(s): {', '.join(unknown)}",
                  file=sys.stderr)
            return 1
    print(f"[generate] wrote coverage matrix to {matrix_path.name}")
    print(f"[generate] dir = {HERE}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
