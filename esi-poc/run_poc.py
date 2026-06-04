#!/usr/bin/env python3
"""Re-run the CriteriaRunningCalculator unit tests through the ESI engine.

Each scenario mirrors a #[test] in
crates/moves-calculators/src/calculators/criteria_running_calculator.rs:
same fixture values (the `inputs_for`/`minimal_inputs` builder), same per-test
tweak, same expected `emission_quant`. We feed those values to the ESI pipeline
in criteria_running.esi and check it reproduces the asserted number.

Run:  python3 esi-poc/run_poc.py
"""
import copy
import os
import sys

# The ESI reference engine lives in the sibling EarthSciInventory repo.
_ENGINE = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "..",
                 "EarthSciInventory", "implementations", "python")
)
if not os.path.isdir(_ENGINE):
    sys.exit(f"ESI engine not found at {_ENGINE} "
             f"(expected the EarthSciInventory repo beside moves.rs)")
sys.path.insert(0, _ENGINE)

import esi  # noqa: E402

HERE = os.path.dirname(__file__)
DOC = esi.load(os.path.join(HERE, "criteria_running.esi"))


def base(p: int = 201):
    """The `minimal_inputs()` fixture, at natural-key granularity (CO running, p=201)."""
    return {
        "emissionRateByAge": [{"sourceBin": 5000, "opMode": 100, "polProcess": p,
                               "meanBaseRate": 10.0, "meanBaseRateIM": 4.0}],
        "sourceBinMap":      [{"sourceBin": 5000, "fuelType": 1}],
        "sourceBinDist":     [{"sourceBin": 5000, "polProcess": p, "sourceBinActivityFraction": 1.0}],
        "opModeDist":        [{"opMode": 100, "polProcess": p, "opModeFraction": 1.0}],
        "criteriaRatio":     [{"fuelFormulation": 100, "polProcess": p, "ratio": 3.0, "ratioGPA": 9.0}],
        "fuelSupply":        [{"fuelFormulation": 100, "marketShare": 1.0}],
        "geo":               [{"scenario": 0, "gpaFract": 0.0}],
        "met":               [{"scenario": 0, "temperature": 75.0, "heatIndex": 0.0}],
        "acCoef":            [{"scenario": 0, "acTermA": 1.0, "acTermB": 0.0, "acTermC": 0.0}],
        "acPen":             [{"scenario": 0, "acPenetration": 1.0}],
        "acFunc":            [{"scenario": 0, "functioningAc": 1.0}],
        "tempCoef":          [{"fuelType": 1, "polProcess": p, "termA": 0.02, "termB": 0.0004}],
        "fullAc":            [{"opMode": 100, "polProcess": p, "fullAcAdjustment": 1.0}],
        "sho":               [{"scenario": 0, "sho": 100.0}],
        # imAdjustFract = compliance_factor(50)*0.01 * imFactor(1.0) = 0.5
        "imAdjust":          [{"polProcess": p, "fuelType": 1, "imAdjustFract": 0.5}],
    }


def minimal():
    return base()


def temperature_adjustment():
    b = base(); b["met"][0]["temperature"] = 50.0; return b


def air_conditioning():
    b = base(); b["fullAc"][0]["fullAcAdjustment"] = 3.0; return b


def without_im_coverage():
    # No I/M coverage -> no blend, modeled as imAdjustFract = 0 (numerically identical).
    b = base(); b["imAdjust"][0]["imAdjustFract"] = 0.0; return b


def clamps_negative_im_blend():
    b = base()
    b["emissionRateByAge"][0]["meanBaseRate"] = -10.0
    b["emissionRateByAge"][0]["meanBaseRateIM"] = -4.0
    return b


def weights_across_source_bins():
    b = without_im_coverage()
    b["sourceBinMap"].append({"sourceBin": 5001, "fuelType": 1})
    b["emissionRateByAge"].append({"sourceBin": 5001, "opMode": 100, "polProcess": 201,
                                   "meanBaseRate": 20.0, "meanBaseRateIM": 20.0})
    b["sourceBinDist"][0]["sourceBinActivityFraction"] = 0.6
    b["sourceBinDist"].append({"sourceBin": 5001, "polProcess": 201, "sourceBinActivityFraction": 0.4})
    return b


def sums_across_operating_modes():
    b = without_im_coverage()
    b["opModeDist"][0]["opModeFraction"] = 0.7
    b["opModeDist"].append({"opMode": 200, "polProcess": 201, "opModeFraction": 0.3})
    b["emissionRateByAge"].append({"sourceBin": 5000, "opMode": 200, "polProcess": 201,
                                   "meanBaseRate": 20.0, "meanBaseRateIM": 20.0})
    b["fullAc"].append({"opMode": 200, "polProcess": 201, "fullAcAdjustment": 1.0})
    return b


def nox_humidity_passthrough():
    return base(301)  # NOx running; humidity disabled -> same as minimal


SCENARIOS = [
    ("calculate_minimal_input_yields_one_row",            minimal,                    2100.0),
    ("calculate_applies_the_temperature_adjustment",      temperature_adjustment,     1575.0),
    ("calculate_applies_the_air_conditioning_adjustment", air_conditioning,           6300.0),
    ("calculate_without_im_coverage_leaves_unadjusted",   without_im_coverage,        3000.0),
    ("calculate_clamps_negative_im_blend_to_zero",        clamps_negative_im_blend,      0.0),
    ("calculate_weights_emission_rates_across_source_bins", weights_across_source_bins, 4200.0),
    ("calculate_sums_emission_rates_across_operating_modes", sums_across_operating_modes, 3900.0),
    ("calculate_nox_humidity_branch_is_a_passthrough",    nox_humidity_passthrough,   2100.0),
]


def main() -> int:
    # The .esi is structurally valid.
    result = esi.validate(DOC)
    if not result.is_valid:
        print("ESI document failed validation:", result.codes)
        return 1
    print("criteria_running.esi: structurally valid\n")

    width = max(len(n) for n, _, _ in SCENARIOS)
    npass = 0
    for name, builder, expected in SCENARIOS:
        out = esi.run(DOC, "CriteriaRunning", inputs=copy.deepcopy(builder()))
        rows = out["emission"].rows
        got = rows[0]["emission_quant"] if len(rows) == 1 else None
        ok = got is not None and abs(got - expected) < 1e-9
        npass += ok
        mark = "PASS" if ok else "FAIL"
        detail = f"got {got}" if not ok else ""
        print(f"  [{mark}] {name:<{width}}  expect {expected:>8}  {detail}")

    print(f"\n{npass}/{len(SCENARIOS)} CriteriaRunning tests reproduced through ESI")
    return 0 if npass == len(SCENARIOS) else 1


if __name__ == "__main__":
    raise SystemExit(main())
