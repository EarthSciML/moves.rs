#!/usr/bin/env python3
"""Measure the precision actually retained by float columns in the snapshot corpus.

The `moves-snapshot/v1` format stores every `float64` column as a
fixed-decimal string with `float_decimals` (12) places *after the point*.
Fixed decimal places are not significant digits: the number of significant
digits a value keeps depends on its magnitude.  For a value whose leading
significant digit sits at 10**e, the string has

    capacity = e + 1 + float_decimals

digit slots between that leading digit and the 1e-12 quantum.  For e = 0
that is 13 slots (more than an f64's ~15.95); for e = -9 it is 4; for
e = -12 it is 1; below that the value is flushed to `0.000000000000`.

This script walks `characterization/snapshots/`, reads every float column
out of the parquet files as strings (streaming, never materializing a whole
table), and reports:

  * the distribution of `capacity` over every stored float value;
  * how many values are "quantum-limited" -- they use the last decimal
    place, so the source double almost certainly had digits below it, and
    fewer than 15 slots survived;
  * how many are stored as exactly zero (an upper bound on flush-to-zero:
    a genuine 0.0 is indistinguishable from a flushed 1e-14);
  * the worst offenders by (table, column).

Usage:
    python3 characterization/audit/float-precision-audit.py \
        [--snapshots characterization/snapshots] [--json out.json]
"""

from __future__ import annotations

import argparse
import collections
import json
import math
import multiprocessing
import os
import sys

import pyarrow.parquet as pq

# f64 carries log10(2**53) = 15.95 significant decimal digits.  A stored
# value with at least this many slots has lost nothing an f64 could hold.
F64_SIG_DIGITS = 16

BATCH_ROWS = 65536

SPECIALS = {"NaN", "Infinity", "-Infinity"}


def decimal_exponent(s: str) -> int | None:
    """floor(log10(|x|)) read straight off the fixed-decimal string.

    Returns None for a value that is exactly zero as stored.  Done on the
    string, not via math.log10, so the answer never depends on float
    rounding at a decade boundary.
    """
    if s.startswith("-"):
        s = s[1:]
    int_part, _, frac_part = s.partition(".")
    int_part = int_part.lstrip("0")
    if int_part:
        return len(int_part) - 1
    for i, ch in enumerate(frac_part):
        if ch != "0":
            return -(i + 1)
    return None


class ColStats:
    __slots__ = (
        "n", "n_null", "n_special", "n_zero", "n_quantum_limited",
        "cap_hist", "min_cap", "min_nonzero", "example_min",
    )

    def __init__(self) -> None:
        self.n = 0
        self.n_null = 0
        self.n_special = 0
        self.n_zero = 0
        self.n_quantum_limited = 0
        self.cap_hist: collections.Counter[int] = collections.Counter()
        self.min_cap: int | None = None
        self.min_nonzero: float | None = None
        self.example_min: str | None = None

    def add(self, s: str | None) -> None:
        self.n += 1
        if s is None:
            self.n_null += 1
            return
        if s in SPECIALS:
            self.n_special += 1
            return
        e = decimal_exponent(s)
        if e is None:
            self.n_zero += 1
            return
        cap = e + 13
        self.cap_hist[cap] += 1
        if self.min_cap is None or cap < self.min_cap:
            self.min_cap = cap
            self.example_min = s
        if cap < F64_SIG_DIGITS and s[-1] != "0":
            self.n_quantum_limited += 1

    def merge(self, other: "ColStats") -> None:
        self.n += other.n
        self.n_null += other.n_null
        self.n_special += other.n_special
        self.n_zero += other.n_zero
        self.n_quantum_limited += other.n_quantum_limited
        self.cap_hist.update(other.cap_hist)
        if other.min_cap is not None and (self.min_cap is None or other.min_cap < self.min_cap):
            self.min_cap = other.min_cap
            self.example_min = other.example_min

    def as_dict(self) -> dict:
        return {
            "n": self.n,
            "n_null": self.n_null,
            "n_special": self.n_special,
            "n_zero": self.n_zero,
            "n_quantum_limited": self.n_quantum_limited,
            "min_capacity": self.min_cap,
            "example_min": self.example_min,
            "capacity_hist": {str(k): v for k, v in sorted(self.cap_hist.items())},
        }


def scan_snapshot(snap_dir: str) -> dict:
    """Return {(table, column): ColStats} for one snapshot directory."""
    tables_dir = os.path.join(snap_dir, "tables")
    out: dict[tuple[str, str], ColStats] = {}
    n_tables = 0
    n_float_cols = 0
    if not os.path.isdir(tables_dir):
        return {"stats": out, "n_tables": 0, "n_float_cols": 0}
    for meta_name in sorted(os.listdir(tables_dir)):
        if not meta_name.endswith(".meta.json"):
            continue
        with open(os.path.join(tables_dir, meta_name)) as fh:
            meta = json.load(fh)
        n_tables += 1
        float_cols = [c["name"] for c in meta["schema"] if c["kind"] == "float64"]
        if not float_cols:
            continue
        n_float_cols += len(float_cols)
        table = meta["name"]
        pq_path = os.path.join(tables_dir, meta_name[: -len(".meta.json")] + ".parquet")
        stats = {c: ColStats() for c in float_cols}
        pf = pq.ParquetFile(pq_path)
        for batch in pf.iter_batches(batch_size=BATCH_ROWS, columns=float_cols):
            for col in float_cols:
                st = stats[col]
                for s in batch.column(col).to_pylist():
                    st.add(s)
        for col, st in stats.items():
            out[(table, col)] = st
    return {"stats": out, "n_tables": n_tables, "n_float_cols": n_float_cols}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshots", default="characterization/snapshots")
    ap.add_argument("--json", default=None, help="write the full per-column report here")
    ap.add_argument("--jobs", type=int, default=8)
    args = ap.parse_args()

    snaps = sorted(
        os.path.join(args.snapshots, d)
        for d in os.listdir(args.snapshots)
        if os.path.isfile(os.path.join(args.snapshots, d, "manifest.json"))
    )
    print(f"populated snapshots: {len(snaps)}", file=sys.stderr)

    with multiprocessing.Pool(args.jobs) as pool:
        results = pool.map(scan_snapshot, snaps)

    per_fixture = {}
    agg: dict[tuple[str, str], ColStats] = {}
    n_tables = n_float_cols = 0
    for snap, res in zip(snaps, results):
        n_tables += res["n_tables"]
        n_float_cols += res["n_float_cols"]
        f_tot = f_q = f_zero = 0
        for key, st in res["stats"].items():
            agg.setdefault(key, ColStats()).merge(st)
            f_tot += st.n - st.n_null - st.n_special
            f_q += st.n_quantum_limited
            f_zero += st.n_zero
        per_fixture[os.path.basename(snap)] = {
            "float_values": f_tot,
            "quantum_limited": f_q,
            "stored_zero": f_zero,
            "float_columns": res["n_float_cols"],
            "tables": res["n_tables"],
        }

    total = collections.Counter()
    cap_hist = collections.Counter()
    for st in agg.values():
        total["values"] += st.n
        total["null"] += st.n_null
        total["special"] += st.n_special
        total["zero"] += st.n_zero
        total["quantum_limited"] += st.n_quantum_limited
        cap_hist.update(st.cap_hist)

    finite_nonzero = sum(cap_hist.values())
    print()
    print(f"tables scanned            : {n_tables}")
    print(f"float column instances    : {n_float_cols}")
    print(f"distinct (table,column)   : {len(agg)}")
    print(f"float cells               : {total['values']}")
    print(f"  null                    : {total['null']}")
    print(f"  NaN/Inf                 : {total['special']}")
    print(f"  stored as 0.000000000000: {total['zero']}")
    print(f"  finite nonzero          : {finite_nonzero}")
    print(f"  quantum-limited         : {total['quantum_limited']}")
    print()
    print("capacity (significant digit slots) histogram over finite nonzero cells:")
    for cap in sorted(cap_hist):
        n = cap_hist[cap]
        print(f"  {cap:3d} slots : {n:12d}  ({100.0*n/finite_nonzero:6.3f}%)")
    lossy = sum(v for k, v in cap_hist.items() if k < F64_SIG_DIGITS)
    print()
    print(f"cells with < {F64_SIG_DIGITS} slots (below f64 capacity): "
          f"{lossy} ({100.0*lossy/finite_nonzero:.3f}%)")
    for thresh in (12, 9, 6, 4, 3, 2, 1):
        n = sum(v for k, v in cap_hist.items() if k <= thresh)
        print(f"  <= {thresh:2d} slots : {n:12d}  ({100.0*n/finite_nonzero:6.3f}%)")

    print()
    print("worst (table, column) by minimum capacity, then by count of <=6-slot cells:")
    rows = []
    for (tbl, col), st in agg.items():
        n_le6 = sum(v for k, v in st.cap_hist.items() if k <= 6)
        rows.append((st.min_cap if st.min_cap is not None else 99, -n_le6, tbl, col, st, n_le6))
    rows.sort(key=lambda r: (r[0], r[1]))
    print(f"{'min':>3} {'<=6slot':>9} {'quantum':>9} {'zeros':>9} {'n':>10}  table.column  (example)")
    for mc, _, tbl, col, st, n_le6 in rows[:40]:
        print(f"{mc:>3} {n_le6:>9} {st.n_quantum_limited:>9} {st.n_zero:>9} {st.n:>10}  "
              f"{tbl}.{col}  ({st.example_min})")

    if args.json:
        payload = {
            "f64_sig_digits": F64_SIG_DIGITS,
            "float_decimals": 12,
            "snapshots": len(snaps),
            "tables_scanned": n_tables,
            "float_column_instances": n_float_cols,
            "totals": dict(total),
            "capacity_hist": {str(k): v for k, v in sorted(cap_hist.items())},
            "per_fixture": per_fixture,
            "columns": {f"{t}.{c}": s.as_dict() for (t, c), s in sorted(agg.items())},
        }
        with open(args.json, "w") as fh:
            json.dump(payload, fh, indent=1, sort_keys=True)
            fh.write("\n")
        print(f"\nwrote {args.json}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
