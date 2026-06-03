#!/usr/bin/env python3
"""Re-run the EvaporativePermeationCalculator unit tests through the ESI engine.

Mirrors the #[test] cases in
crates/moves-calculators/src/calculators/evaporative_permeation_calculator.rs
(the `minimal_inputs()` fixture + each per-test tweak), feeding the same values
to the ESI pipeline in permeation.esi.

This POC exercises a different capability set from run_poc.py: an Arrhenius
`exp` temperature adjustment, two interval `filter`s (model-year range, ethanol
bin), and null-coalescing.

Run:  python3 esi-poc/run_permeation.py
"""
import copy
import math
import os
import sys

_ENGINE = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "..",
                 "EarthSciInventory", "implementations", "python")
)
if not os.path.isdir(_ENGINE):
    sys.exit(f"ESI engine not found at {_ENGINE}")
sys.path.insert(0, _ENGINE)

import esi  # noqa: E402

HERE = os.path.dirname(__file__)
DOC = esi.load(os.path.join(HERE, "permeation.esi"))
EMPTY = "EMPTY"  # sentinel: scenario expects zero output rows (a filter/inner-join drop)


def base():
    """The `minimal_inputs()` fixture at natural-key granularity (THC evap permeation, p=111)."""
    return {
        "emissionRateByAge": [{"sourceBin": 5000, "polProcess": 111, "rateRow": 1, "meanBaseRate": 2.0}],
        "sourceBinMap":      [{"sourceBin": 5000, "fuelType": 1}],
        "sourceBinDist":     [{"sourceBin": 5000, "polProcess": 111, "sourceBinActivityFraction": 1.0}],
        "tempAdjust":        [{"polProcess": 111, "fuelType": 1, "tempAdjustTermA": 1.0, "tempAdjustTermB": 0.0,
                              "minModelYear": 1990, "maxModelYear": 2060}],
        "avgTankTemp":       [{"opMode": 300, "scenario": 0, "averageTankTemperature": 70.0}],
        "opModeDist":        [{"opMode": 300, "polProcess": 111, "opModeFraction": 1.0}],
        "modelYear":         [{"modelYear": 2018}],
        "fuelSupply":        [{"fuelFormulation": 100, "marketShare": 1.0, "etohVolume": 5.0}],
        "etohBin":           [{"etohThresh": 1, "etohThreshLow": 0.0, "etohThreshHigh": 100.0}],
        "hcPermCoeff":       [{"polProcess": 111, "etohThresh": 1, "fuelAdjustment": 3.0, "fuelAdjustmentGPA": 9.0}],
        "geo":               [{"scenario": 0, "gpaFract": 0.0}],
        "sourceHours":       [{"scenario": 0, "sourceHours": 10.0}],
    }


def minimal():
    return base()


def exponential_temperature_adjustment():
    b = base()
    b["tempAdjust"][0]["tempAdjustTermA"] = 2.0
    b["tempAdjust"][0]["tempAdjustTermB"] = 0.01
    return b


def gpa_fuel_adjustment():
    b = base(); b["geo"][0]["gpaFract"] = 0.25; return b


def sums_source_bin_activity():
    b = base()
    b["sourceBinDist"][0]["sourceBinActivityFraction"] = 0.5
    b["sourceBinMap"].append({"sourceBin": 5001, "fuelType": 1})
    b["sourceBinDist"].append({"sourceBin": 5001, "polProcess": 111, "sourceBinActivityFraction": 0.25})
    b["emissionRateByAge"].append({"sourceBin": 5001, "polProcess": 111, "rateRow": 1, "meanBaseRate": 4.0})
    return b


def sums_rate_across_operating_modes():
    b = base()
    b["emissionRateByAge"].append({"sourceBin": 5000, "polProcess": 111, "rateRow": 2, "meanBaseRate": 1.5})
    return b


def null_etoh_volume_survives():
    b = base(); b["fuelSupply"][0]["etohVolume"] = None; return b


def drops_model_year_out_of_range():
    b = base(); b["tempAdjust"][0]["minModelYear"] = 2025; return b


def drops_etoh_out_of_bin():
    b = base(); b["etohBin"][0]["etohThreshLow"] = 10.0; return b


def drops_null_etoh_below_bin():
    b = base(); b["fuelSupply"][0]["etohVolume"] = None; b["etohBin"][0]["etohThreshLow"] = 1.0; return b


_EXP = 2.0 * math.exp(0.01 * 70.0) * 60.0

SCENARIOS = [
    ("calculate_minimal_input_yields_one_row",               minimal,                          60.0),
    ("calculate_applies_the_exponential_temperature_adjustment", exponential_temperature_adjustment, _EXP),
    ("calculate_weights_gpa_fuel_adjustment_by_county_fraction", gpa_fuel_adjustment,           90.0),
    ("calculate_sums_source_bin_activity_across_bins",       sums_source_bin_activity,         60.0),
    ("calculate_sums_emission_rate_across_operating_modes",  sums_rate_across_operating_modes, 105.0),
    ("calculate_treats_null_etoh_volume_as_zero (survives)", null_etoh_volume_survives,        60.0),
    ("calculate_drops_temperature_adjust_outside_model_year_range", drops_model_year_out_of_range, EMPTY),
    ("calculate_drops_fuel_adjustment_when_etoh_volume_out_of_bin", drops_etoh_out_of_bin,      EMPTY),
    ("calculate_treats_null_etoh_volume_as_zero (drops <bin)", drops_null_etoh_below_bin,       EMPTY),
]


def main() -> int:
    result = esi.validate(DOC)
    if not result.is_valid:
        print("ESI document failed validation:", result.codes)
        return 1
    print("permeation.esi: structurally valid\n")

    width = max(len(n) for n, _, _ in SCENARIOS)
    npass = 0
    for name, builder, expected in SCENARIOS:
        out = esi.run(DOC, "Permeation", inputs=copy.deepcopy(builder()))
        rows = out["emission"].rows
        if expected is EMPTY:
            ok = len(rows) == 0
            shown, detail = "empty", ("" if ok else f"got {len(rows)} rows")
        else:
            got = rows[0]["emission_quant"] if len(rows) == 1 else None
            ok = got is not None and abs(got - expected) < 1e-9
            shown, detail = f"{expected:.4f}", ("" if ok else f"got {got}")
        npass += ok
        print(f"  [{'PASS' if ok else 'FAIL'}] {name:<{width}}  expect {shown:>10}  {detail}")

    print(f"\n{npass}/{len(SCENARIOS)} Permeation tests reproduced through ESI")
    return 0 if npass == len(SCENARIOS) else 1


if __name__ == "__main__":
    raise SystemExit(main())
