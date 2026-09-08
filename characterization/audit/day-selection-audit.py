#!/usr/bin/env python3
"""Audit `<day>` selection: what each fixture declares vs. what MOVES ran.

MOVES's `RunSpecXML.processTimeSpan` reads `<day>` two ways:

  * `<day key="N"/>` — N is an **index** into `TimeSpan.allDays`
    (`getDayByIndex`). `allDays` holds two entries, so any key outside
    {0, 1} resolves to null, the day is not added to the selection, and
    MOVES falls back to running every day.
  * `<day id="N"/>` — N is the literal `dayID` (`getDayByID`): 5 =
    weekdays, 2 = weekend.

This script reports, for every fixture XML:

  declared   the `<day>` elements as written (attribute + value)
  resolved   `MOVESExecution.RunSpecDay.dayID` from the captured snapshot --
             MOVES's own record of the day selection it resolved to
  emitted    distinct `dayID` in the output `MOVESOutput` / activity tables
  intent     the fixture catalogue's `TimeSpan.days` (source of truth is
             `_generate.py`), or the README/coverage-matrix description

Usage:
    python3 characterization/audit/day-selection-audit.py \
        [--fixtures characterization/fixtures] \
        [--snapshots characterization/snapshots] [--json out.json]
"""

from __future__ import annotations

import argparse
import ast
import glob
import json
import os
import re
import sys
import xml.etree.ElementTree as ET

import pyarrow.parquet as pq

DAY_NAMES = {2: "weekend", 5: "weekday"}


def declared_days(xml_path: str) -> list[tuple[str, int]]:
    """[(attribute, value)] for each `<day>` element, document order."""
    root = ET.parse(xml_path).getroot()
    out = []
    for ts in root.iter("timespan"):
        for d in ts.findall("day"):
            for attr in ("key", "id"):
                if attr in d.attrib:
                    out.append((attr, int(d.attrib[attr])))
    return out


def catalogue_intent(generate_py: str) -> dict[str, dict]:
    """Pull `name=` / `TimeSpan(...)` pairs out of the generator's spec table.

    Parsed from the AST rather than by regex so a reformat of the table does
    not silently change the answer.
    """
    tree = ast.parse(open(generate_py).read())
    out: dict[str, dict] = {}
    defaults = {"days": None, "day_attr": None}
    for node in ast.walk(tree):
        # TimeSpan dataclass defaults.
        if isinstance(node, ast.ClassDef) and node.name == "TimeSpan":
            for stmt in node.body:
                if isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
                    if stmt.target.id in defaults and stmt.value is not None:
                        defaults[stmt.target.id] = ast.literal_eval(stmt.value)
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and getattr(node.func, "id", "") == "FixtureSpec"):
            continue
        name = None
        days = defaults["days"]
        day_attr = defaults["day_attr"]
        for kw in node.keywords:
            if kw.arg == "name":
                name = ast.literal_eval(kw.value)
            elif kw.arg == "timespan" and isinstance(kw.value, ast.Call):
                for tkw in kw.value.keywords:
                    if tkw.arg == "days":
                        days = ast.literal_eval(tkw.value)
                    elif tkw.arg == "day_attr":
                        day_attr = ast.literal_eval(tkw.value)
        if name:
            out[name] = {"days": list(days), "day_attr": day_attr}
    return out


def distinct_ints(parquet_path: str, column: str) -> list[int] | None:
    if not os.path.exists(parquet_path):
        return None
    pf = pq.ParquetFile(parquet_path)
    if column not in pf.schema_arrow.names:
        return None
    seen = set()
    for batch in pf.iter_batches(batch_size=65536, columns=[column]):
        for v in batch.column(column).to_pylist():
            if v is not None:
                seen.add(int(v))
    return sorted(seen)


def snapshot_days(snap_dir: str) -> dict:
    tables = os.path.join(snap_dir, "tables")
    res: dict[str, list[int] | None] = {}
    if not os.path.isdir(tables):
        return res
    runspecday = glob.glob(os.path.join(tables, "db__movesexecution*__runspecday.parquet"))
    res["runspecday"] = distinct_ints(runspecday[0], "dayID") if runspecday else None
    for key, pattern in (
        ("movesoutput", "db__*__movesoutput.parquet"),
        ("movesactivityoutput", "db__*__movesactivityoutput.parquet"),
    ):
        hits = [
            p
            for p in sorted(glob.glob(os.path.join(tables, pattern)))
            if "movesexecution" not in os.path.basename(p)
            and "movesworker" not in os.path.basename(p)
        ]
        res[key] = distinct_ints(hits[0], "dayID") if hits else None
    return res


def fmt(days) -> str:
    if days is None:
        return "-"
    if not days:
        return "(empty)"
    return ",".join(f"{d}({DAY_NAMES.get(d, '?')})" for d in days)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fixtures", default="characterization/fixtures")
    ap.add_argument("--snapshots", default="characterization/snapshots")
    ap.add_argument("--json", default=None)
    args = ap.parse_args()

    intent = catalogue_intent(os.path.join(args.fixtures, "_generate.py"))
    rows = []
    for xml_path in sorted(glob.glob(os.path.join(args.fixtures, "*.xml"))):
        name = os.path.basename(xml_path)[: -len(".xml")]
        decl = declared_days(xml_path)
        snap = os.path.join(args.snapshots, name)
        observed = snapshot_days(snap) if os.path.isfile(os.path.join(snap, "manifest.json")) else {}
        rows.append(
            {
                "fixture": name,
                "declared": decl,
                "declared_attr": sorted({a for a, _ in decl}),
                "intent": intent.get(name),
                "has_snapshot": bool(observed),
                **{k: v for k, v in observed.items()},
            }
        )

    hdr = f"{'fixture':38s} {'declared':16s} {'intent':14s} {'RunSpecDay':22s} {'MOVESOutput':22s} verdict"
    print(hdr)
    print("-" * len(hdr))
    counts = {"agree": 0, "both_days_run": 0, "no_snapshot": 0, "other": 0}
    for r in rows:
        decl = " ".join(f"{a}={v}" for a, v in r["declared"]) or "(none)"
        it = r["intent"]
        intent_s = (
            f"{','.join(str(d) for d in it['days'])} via {it['day_attr']}" if it else "uncatalogued"
        )
        rsd = r.get("runspecday")
        mo = r.get("movesoutput")
        if not r["has_snapshot"]:
            verdict = "no snapshot"
            counts["no_snapshot"] += 1
        elif mo is not None and len(mo) > 1:
            verdict = "RUNS BOTH DAYS"
            counts["both_days_run"] += 1
        elif mo is not None and it and sorted(mo) == sorted(it["days"]):
            verdict = "ok"
            counts["agree"] += 1
        elif mo is not None and not it:
            verdict = f"single day {fmt(mo)}"
            counts["other"] += 1
        else:
            verdict = "mismatch"
            counts["other"] += 1
        print(f"{r['fixture']:38s} {decl:16s} {intent_s:14s} {fmt(rsd):22s} {fmt(mo):22s} {verdict}")

    print()
    print("summary:", json.dumps(counts))
    by_attr: dict[str, int] = {}
    for r in rows:
        by_attr[",".join(r["declared_attr"]) or "none"] = (
            by_attr.get(",".join(r["declared_attr"]) or "none", 0) + 1
        )
    print("fixtures by <day> attribute:", json.dumps(by_attr))

    # Cross-tab: attribute used vs. number of days MOVES actually emitted.
    tab: dict[str, dict[str, int]] = {}
    for r in rows:
        if not r["has_snapshot"] or r.get("movesoutput") is None:
            continue
        a = ",".join(r["declared_attr"]) or "none"
        k = fmt(r["movesoutput"])
        tab.setdefault(a, {}).setdefault(k, 0)
        tab[a][k] += 1
    print("attribute x emitted dayIDs (snapshotted fixtures only):")
    for a in sorted(tab):
        for k in sorted(tab[a]):
            print(f"  {a:6s} -> {k:24s} {tab[a][k]}")

    if args.json:
        with open(args.json, "w") as fh:
            json.dump(rows, fh, indent=1, sort_keys=True)
            fh.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
