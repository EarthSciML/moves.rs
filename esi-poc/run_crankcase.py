#!/usr/bin/env python3
"""End-to-end ESI run against a REAL characterization snapshot.

crankcase.esi expresses crankcaseEmission = exhaustEmission * crankcaseRatio.
Here we load the **real captured CrankcaseEmissionRatio Parquet** from
characterization/snapshots/process-crankcase-running via an ESI data-source
loader, drive it with exhaust cells generated from that same table (one per
distinct (polProcess, sourceType, regClass, fuelType) group x a span of model
years, emissionQuant=1.0 so the output equals the looked-up ratio), run the ESI
pipeline, and check every cell against a Polars ground-truth lookup that applies
the same model-year window (summing overlapping windows).

Exercises the ESI Parquet loader on real snapshot data + the model-year interval
theta-join against the real overlapping-window ratio table.

Run:  python3 esi-poc/run_crankcase.py    (needs polars + the EarthSciInventory repo)
"""
import glob
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
from esi.sources import parquet_dir  # noqa: E402

try:
    import polars as pl
except ImportError:
    sys.exit("polars required: /tmp/esi-venv/bin/pip install polars")

HERE = os.path.dirname(__file__)
SNAP = os.path.normpath(os.path.join(
    HERE, "..", "characterization", "snapshots", "process-crankcase-running", "tables"))
DOC = esi.load(os.path.join(HERE, "crankcase.esi"))

# ESI loader: snapshot tables are named <prefix>__<table>.parquet.
LOADER = parquet_dir(SNAP, pattern="*__{table}.parquet")
KEYS = ["polProcessID", "sourceTypeID", "regClassID", "fuelTypeID", "modelYearID"]
MODEL_YEARS = [1960, 1975, 1985, 2000, 2010, 2025, 2040, 2055]


def ratio_frame() -> pl.DataFrame:
    fp = glob.glob(os.path.join(SNAP, "*__crankcaseemissionratio.parquet"))[0]
    return pl.read_parquet(fp).with_columns(pl.col("crankcaseRatio").cast(pl.Float64))


def build_exhaust(ratio: pl.DataFrame) -> list[dict]:
    groups = ratio.select(["polProcessID", "sourceTypeID", "regClassID", "fuelTypeID"]).unique()
    return [{
        "polProcessID": g["polProcessID"], "sourceTypeID": g["sourceTypeID"],
        "regClassID": g["regClassID"], "fuelTypeID": g["fuelTypeID"],
        "modelYearID": my, "emissionQuant": 1.0, "emissionRate": 1.0,
    } for g in groups.iter_rows(named=True) for my in MODEL_YEARS]


def ground_truth(ratio: pl.DataFrame, cell: dict) -> float:
    my = cell["modelYearID"]
    m = ratio.filter(
        (pl.col("polProcessID") == cell["polProcessID"])
        & (pl.col("sourceTypeID") == cell["sourceTypeID"])
        & (pl.col("regClassID") == cell["regClassID"])
        & (pl.col("fuelTypeID") == cell["fuelTypeID"])
        & (pl.col("minModelYearID") <= my) & (pl.col("maxModelYearID") >= my))
    return float(m["crankcaseRatio"].sum())


def main() -> int:
    res = esi.validate(DOC)
    if not res.is_valid:
        print("crankcase.esi invalid:", res.codes); return 1
    ratio = ratio_frame()
    print("crankcase.esi: structurally valid")
    print(f"loaded real CrankcaseEmissionRatio: {ratio.height} rows "
          f"from snapshots/process-crankcase-running\n")

    exhaust = build_exhaust(ratio)
    out = esi.run(DOC, "Crankcase", inputs={"exhaust": exhaust}, loaders={"snapshot": LOADER})
    esi_by_cell = {tuple(r[k] for k in KEYS): r["crankcaseQuant"] for r in out["emission"].rows}

    mism = matched = 0
    for cell in exhaust:
        key = tuple(cell[k] for k in KEYS)
        got = esi_by_cell.get(key, 0.0)            # inner-join drop -> no row -> 0
        want = ground_truth(ratio, cell)
        matched += want != 0.0
        if abs(got - want) > 1e-9:
            mism += 1
            if mism <= 8:
                print(f"  MISMATCH {key}: esi {got} != truth {want}")

    print(f"checked {len(exhaust)} exhaust cells "
          f"({matched} hit a model-year window) against the real ratio table")
    print(f"{len(exhaust) - mism}/{len(exhaust)} cells match the captured ratios "
          f"(ESI Parquet loader + interval theta-join + multiply)\n")

    # Focused demonstration of the model-year window theta-join on real data.
    demo = sorted({c["modelYearID"] for c in exhaust})
    g = ratio.filter((pl.col("polProcessID") == 115) & (pl.col("sourceTypeID") == 21)
                     & (pl.col("regClassID") == 20) & (pl.col("fuelTypeID") == 1))
    if g.height:
        print("model-year window selection on real data "
              "(polProcess 115, sourceType 21, regClass 20, fuelType 1):")
        print(f"  captured windows: "
              + ", ".join(f"[{r['minModelYearID']},{r['maxModelYearID']}]->{r['crankcaseRatio']}"
                          for r in g.sort('minModelYearID').iter_rows(named=True)))
        for my in demo:
            v = esi_by_cell.get((115, 21, 20, 1, my))
            print(f"  MY {my}  ->  ESI crankcaseRatio {v}")

    return 1 if mism else 0


if __name__ == "__main__":
    raise SystemExit(main())
