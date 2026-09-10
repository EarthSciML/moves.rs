#!/usr/bin/env python3
"""Audit the committed snapshots for the "ant exited 0 but MOVES failed" class.

Background. `characterization/apptainer/run-fixture.sh` wraps a canonical-MOVES
run. ant's `<java>` task for MOVES has no `failonerror="true"` (MOVES's own
`build.xml`, target `main1worker`), so a MOVES that dies is reported to the
wrapper as exit 0 and the run log is the only witness. On 2026-09-10 four
project-scale trials failed and the wrapper's log scan printed "MOVES run OK"
on every one of them; only the zero-dumped-table belt stopped a bad snapshot
from being published (issue #56 for the belt, #64 for what the scan still
cannot see).

This script asks: does any snapshot already in `characterization/snapshots/`
carry the signature of such a run?

What it checks, per snapshot:

  tables        number of parquet tables, read from `tables/*.parquet`
  nonzero       how many of them have at least one row
  parquet_ok    every table's parquet row count equals the `row_count`
                recorded in `manifest.json` (the manifest is not trusted;
                the parquet footers are read directly)
  databases     which MariaDB databases the snapshot holds tables from,
                split into execution / worker / output / other. trial1's
                signature is "no execution DB and no output DB, only the
                county input DB" -- that run dumped 27 input tables and the
                capture binary, which keeps only the execution and output
                databases, turned them into a zero-table snapshot.
  emission rows `MOVESOutput`, `MOVESActivityOutput` and `BaseRateOutput`
                row counts. All three zero is the "vacuous" shape that
                `docs/known-divergences.md` records for the start / idle /
                hotelling fixtures; anywhere else it would be a finding.

And, when the run logs of the capture sweep are still on scratch (they are
NOT in the snapshots for any capture taken before 2026-09-09 -- that is the
whole reason `run-fixture.sh` now publishes `moves-run.log` into the
snapshot), it re-scans them with the wrapper's current failure rule.

    THE BOUND. Everything here rules out a GROSS failure -- a run that
    produced no output database, or a snapshot that lost its tables. It
    cannot rule out a PARTIAL one: a MOVES that died part-way through the
    simulation after populating some tables looks, from the snapshot alone,
    exactly like a MOVES that succeeded. The log re-scan narrows that only
    as far as the logs' provenance allows, which is by directory name, and
    only for the pre-simulation error class (MOVES renames in-simulation
    ERROR to RUN_ERROR; see issue #64). Read the "Bound" section of the
    write-up before quoting any number from here.

Usage:
    python3 characterization/audit/snapshot-integrity-audit.py \
        [--snapshots characterization/snapshots] \
        [--log-roots /scratch/$USER/moves-fixture /scratch/$USER/moves-county-fixture] \
        [--json out.json]
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

# The wrapper's current rule, kept in sync with MOVES_FAILURE_PATTERNS in
# characterization/apptainer/run-fixture.sh. Word-boundary anchored so
# MOVES's in-simulation `RUN_ERROR:` category does not match.
FAILURE_PATTERNS = [
    re.compile(r"(^|[^A-Za-z0-9_])ERROR:"),
    re.compile(r"BUILD FAILED"),
    re.compile(r"The specified runspec file does not exist"),
    re.compile(r"A runspec was not provided"),
]
# The old four-literal set, for the differential.
OLD_MARKERS = [
    "ERROR: Error:",
    "BUILD FAILED",
    "The specified runspec file does not exist",
    "ERROR: A runspec was not provided",
]

EMISSION_TABLES = ("movesoutput", "movesactivityoutput", "baserateoutput")


def parquet_rows(path: Path) -> int:
    """Row count from the parquet footer, without trusting manifest.json."""
    import pyarrow.parquet as pq

    return pq.ParquetFile(path).metadata.num_rows


def classify_db(db: str) -> str:
    if db.startswith("movesexecution"):
        return "execution"
    if db.startswith("movesworker"):
        return "worker"
    if db.startswith("out_") or db == "junittestoutput":
        return "output"
    return "other"


def audit_snapshot(d: Path) -> dict:
    manifest = json.loads((d / "manifest.json").read_text())
    provenance = json.loads((d / "provenance.json").read_text())
    dbs: dict[str, int] = {}
    emissions = {k: None for k in EMISSION_TABLES}
    nonzero = 0
    mismatches = []
    for t in manifest["tables"]:
        name = t["name"]
        n = parquet_rows(d / "tables" / f"{name}.parquet")
        if n != t["row_count"]:
            mismatches.append({"table": name, "manifest": t["row_count"], "parquet": n})
        if n > 0:
            nonzero += 1
        # names are db__<database>__<table>
        parts = name.split("__")
        if len(parts) >= 3:
            dbs[parts[1]] = dbs.get(parts[1], 0) + 1
        for k in EMISSION_TABLES:
            if name.endswith("__" + k):
                emissions[k] = n
    by_kind: dict[str, list[str]] = {}
    for db in sorted(dbs):
        by_kind.setdefault(classify_db(db), []).append(db)
    return {
        "fixture": d.name,
        "tables": len(manifest["tables"]),
        "nonzero": nonzero,
        "parquet_manifest_mismatches": mismatches,
        "databases": by_kind,
        "emissions": emissions,
        "output_database": provenance.get("output_database"),
        "scale_input_database": provenance.get("scale_input_database"),
    }


def scan_log(path: Path) -> dict:
    text = path.read_text(errors="replace")
    lines = text.splitlines()
    new_hits = [l.strip() for l in lines if any(p.search(l) for p in FAILURE_PATTERNS)]
    old_hits = [l.strip() for l in lines if any(m in l for m in OLD_MARKERS)]
    substr = [l.strip() for l in lines if "ERROR" in l]
    return {
        "path": str(path),
        "bytes": path.stat().st_size,
        "new_rule_hits": len(new_hits),
        "old_rule_hits": len(old_hits),
        "error_substring_lines": len(substr),
        "first_new_hit": new_hits[0][:200] if new_hits else None,
        "substring_samples": [l[:160] for l in substr[:3]],
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshots", default="characterization/snapshots")
    ap.add_argument(
        "--log-roots",
        nargs="*",
        default=[
            f"/scratch/{os.environ.get('USER','')}/moves-fixture",
            f"/scratch/{os.environ.get('USER','')}/moves-county-fixture",
        ],
    )
    ap.add_argument("--json")
    args = ap.parse_args()

    root = Path(args.snapshots)
    snaps = sorted(p for p in root.iterdir() if (p / "manifest.json").is_file())

    results = [audit_snapshot(p) for p in snaps]

    print(f"{'fixture':34} {'tabs':>5} {'nzero':>5} {'exec':>5} {'out':>4} "
          f"{'MOVESOut':>9} {'Activity':>9} {'BaseRate':>9}")
    for r in results:
        e = r["emissions"]
        print(f"{r['fixture']:34} {r['tables']:5} {r['nonzero']:5} "
              f"{len(r['databases'].get('execution', [])):5} "
              f"{len(r['databases'].get('output', [])):4} "
              f"{str(e['movesoutput']):>9} {str(e['movesactivityoutput']):>9} "
              f"{str(e['baserateoutput']):>9}")

    n = len(results)
    mism = sum(len(r["parquet_manifest_mismatches"]) for r in results)
    no_exec = [r["fixture"] for r in results if not r["databases"].get("execution")]
    no_out = [r["fixture"] for r in results if not r["databases"].get("output")]
    vacuous = [r["fixture"] for r in results
               if all((r["emissions"][k] or 0) == 0 for k in EMISSION_TABLES)]
    print()
    print(f"snapshots ................................. {n}")
    print(f"tables per snapshot ....................... "
          f"{min(r['tables'] for r in results)}-{max(r['tables'] for r in results)}")
    print(f"non-zero tables per snapshot .............. "
          f"{min(r['nonzero'] for r in results)}-{max(r['nonzero'] for r in results)}")
    print(f"parquet vs manifest row_count mismatches .. {mism}")
    print(f"snapshots with no execution database ...... {len(no_exec)} {no_exec}")
    print(f"snapshots with no output database ......... {len(no_out)} {no_out}")
    print(f"snapshots with all emission tables zero ... {len(vacuous)}")
    for f in vacuous:
        print(f"    {f}")

    # ---- run-log re-scan, where the logs still exist on scratch ----
    logs = {}
    for r in results:
        cands = [Path(lr) / r["fixture"] / "moves-run.log" for lr in args.log_roots]
        cands = [c for c in cands if c.is_file()]
        logs[r["fixture"]] = [scan_log(c) for c in cands]
    have = [f for f, v in logs.items() if v]
    ambiguous = [f for f, v in logs.items() if len(v) > 1]
    hits = [(f, s) for f, v in logs.items() for s in v if s["new_rule_hits"]]
    substr = [(f, s) for f, v in logs.items() for s in v if s["error_substring_lines"]]
    print()
    print(f"snapshots with a retained run log on scratch  {len(have)}/{n}")
    print(f"  ...of which more than one candidate log ... {len(ambiguous)} {ambiguous}")
    print(f"logs matching the CURRENT failure rule ...... {len(hits)}")
    for f, s in hits:
        print(f"    {f}: {s['path']}\n      {s['first_new_hit']}")
    print(f"logs containing the substring 'ERROR' ....... {len(substr)}")
    for f, s in substr:
        print(f"    {f}: {s['error_substring_lines']} line(s)")
        for x in s["substring_samples"]:
            print(f"      {x}")

    if args.json:
        Path(args.json).write_text(
            json.dumps({"snapshots": results, "logs": logs}, indent=2, sort_keys=True)
        )
        print(f"\nwrote {args.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
