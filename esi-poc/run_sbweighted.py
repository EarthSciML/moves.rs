#!/usr/bin/env python3
"""Multi-step, multi-table ESI chain on REAL captured snapshot Parquet.

sbweighted.esi computes the source-bin-weighted base rate
  meanBaseRate = weighted_mean(EmissionRateByAge.meanBaseRate, weight = SourceBinActivityFraction)
over source bins, joining three real captured tables (EmissionRateByAge,
SourceBinDistribution, SourceBin) from
characterization/snapshots/process-crankcase-running.

Two checks:
  1. ENGINE correctness — the ESI engine's 3-table join + weighted_mean must equal
     an independent Polars recomputation of the same formula on the same real data.
  2. CANONICAL closeness — how close that formula lands to the captured
     SBWeightedEmissionRateByAge.meanBaseRate (the gap is the BaseRateGenerator's
     sumSBD normalization / model-year-group keying, i.e. the per-generator porting
     that exact fidelity would require).

Run:  python3 esi-poc/run_sbweighted.py     (needs polars + the EarthSciInventory repo)
"""
import glob
import os
import sys

_ENGINE = os.path.normpath(os.path.join(
    os.path.dirname(__file__), "..", "..", "EarthSciInventory", "implementations", "python"))
if not os.path.isdir(_ENGINE):
    sys.exit(f"ESI engine not found at {_ENGINE}")
sys.path.insert(0, _ENGINE)

import esi  # noqa: E402

try:
    import polars as pl
except ImportError:
    sys.exit("polars required")

HERE = os.path.dirname(__file__)
SNAP = os.path.normpath(os.path.join(
    HERE, "..", "characterization", "snapshots", "process-crankcase-running", "tables"))
DOC = esi.load(os.path.join(HERE, "sbweighted.esi"))
ERA_POLPROCESS = [101]   # THC running exhaust — scope the 590k-row table for the pure-Python engine
KEY = ["sourceTypeModelYearID", "polProcessID", "opModeID", "ageGroupID", "fuelTypeID", "regClassID"]

_FLOAT_COLS = {"meanBaseRate", "sourceBinActivityFraction"}


def read(table: str) -> pl.DataFrame:
    df = pl.read_parquet(glob.glob(f"{SNAP}/*__{table}.parquet")[0])
    return df.with_columns([pl.col(c).cast(pl.Float64) for c in df.columns if c in _FLOAT_COLS])


def snapshot_loader(config, table, select):
    """ESI data-source loader over the snapshot's prefixed Parquet files."""
    df = read(table)
    if table == "emissionratebyage":
        df = df.filter(pl.col("polProcessID").is_in(ERA_POLPROCESS))
    if table == "sourcebindistribution":
        df = df.filter(pl.col("sourceBinActivityFraction") > 0.0)   # zero-weight bins don't contribute
    return df.to_dicts()


def polars_truth() -> pl.DataFrame:
    era = read("emissionratebyage").filter(pl.col("polProcessID").is_in(ERA_POLPROCESS))
    sbd = read("sourcebindistribution").filter(pl.col("sourceBinActivityFraction") > 0.0)
    sb = read("sourcebin").select(["sourceBinID", "fuelTypeID", "regClassID"])
    j = era.join(sbd, on=["sourceBinID", "polProcessID"], how="inner").join(sb, on="sourceBinID", how="inner")
    return (j.with_columns((pl.col("meanBaseRate") * pl.col("sourceBinActivityFraction")).alias("n"))
             .group_by(KEY)
             .agg((pl.col("n").sum() / pl.col("sourceBinActivityFraction").sum()).alias("rate")))


def main() -> int:
    res = esi.validate(DOC)
    if not res.is_valid:
        print("sbweighted.esi invalid:", res.codes); return 1
    print("sbweighted.esi: structurally valid")

    out = esi.run(DOC, "SBWeighted", loaders={"snapshot": snapshot_loader})
    esi_rows = out["rate"].rows
    esi_by = {tuple(r[k] for k in KEY): r["meanBaseRate"] for r in esi_rows}
    print(f"ESI joined 3 real tables (EmissionRateByAge polProcess={ERA_POLPROCESS}, "
          f"SourceBinDistribution, SourceBin) -> {len(esi_rows)} weighted-mean cells\n")

    # 1. Engine correctness: ESI == independent Polars recomputation of the same formula.
    truth = polars_truth()
    tby = {tuple(r[k] for k in KEY): r["rate"] for r in truth.iter_rows(named=True)}
    eng_mism = sum(1 for k, v in tby.items() if abs(esi_by.get(k, float("nan")) - v) > 1e-9)
    eng_mism += sum(1 for k in esi_by if k not in tby)
    print(f"[1] ENGINE: ESI vs Polars (same formula, same real data): "
          f"{len(esi_by)} cells, {eng_mism} mismatches -> "
          f"{'EXACT MATCH' if eng_mism == 0 else 'MISMATCH'}")

    # 2. Canonical closeness: vs captured SBWeightedEmissionRateByAge.meanBaseRate.
    canon = read("sbweightedemissionratebyage").with_columns(
        (pl.col("sourceTypeID") * 10000 + pl.col("modelYearID")).alias("sourceTypeModelYearID"),
        pl.col("meanBaseRate").cast(pl.Float64)).filter(pl.col("polProcessID").is_in(ERA_POLPROCESS))
    cby = {tuple(r[k] for k in KEY): r["meanBaseRate"] for r in canon.iter_rows(named=True)}
    common = [k for k in esi_by if k in cby]
    within = sum(1 for k in common
                 if cby[k] != 0 and abs(esi_by[k] - cby[k]) / abs(cby[k]) <= 0.02)
    print(f"[2] CANONICAL: {len(common)} cells overlap with captured SBWeightedEmissionRateByAge; "
          f"{within}/{len(common)} within 2% of the captured meanBaseRate")
    print("    (the residual is the BaseRateGenerator's sumSBD normalization + model-year-group keying;")
    print("     exact reproduction = porting that generator, the per-calculator work the crankcase POC avoided)")

    return 0 if eng_mism == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
