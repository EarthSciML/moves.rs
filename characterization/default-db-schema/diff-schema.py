#!/usr/bin/env python3
"""Diff two MOVES default-DB schema audits (tables.json).

Compare the schema captured from an old MOVES release against one from a
new release and surface every structural change that may require action in
the Rust port:

  * Tables added / removed
  * Columns added, removed, or type-changed
  * Primary-key changes
  * Partition-strategy changes (when the new partition field differs)

Exit codes:
  0  — schemas are identical
  1  — at least one difference detected
  2  — usage / file error
"""

import argparse
import json
import sys
from pathlib import Path


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------

def _load(path: Path):
    doc = json.loads(path.read_text())
    if "tables" not in doc:
        raise ValueError(f"{path}: missing 'tables' key — not a tables.json file")
    by_name = {t["name"].lower(): t for t in doc["tables"]}
    return doc, by_name


# ---------------------------------------------------------------------------
# Diffing helpers
# ---------------------------------------------------------------------------

def _diff_columns(old_cols, new_cols):
    old = {c["name"].lower(): c for c in old_cols}
    new = {c["name"].lower(): c for c in new_cols}

    added   = [c for k, c in new.items() if k not in old]
    removed = [c for k, c in old.items() if k not in new]
    changed = []
    for k in old:
        if k in new and old[k]["type"] != new[k]["type"]:
            changed.append({
                "name":     new[k]["name"],
                "old_type": old[k]["type"],
                "new_type": new[k]["type"],
            })
    return added, removed, changed


def _diff_pk(old_pk, new_pk):
    return [c.lower() for c in old_pk] != [c.lower() for c in new_pk]


def _diff_partition(old_t, new_t):
    old_strat = old_t.get("partition", {}).get("strategy")
    new_strat = new_t.get("partition", {}).get("strategy")
    if old_strat != new_strat:
        return {"old": old_strat, "new": new_strat}
    return None


# ---------------------------------------------------------------------------
# Top-level diff
# ---------------------------------------------------------------------------

def diff_schemas(old_by_name, new_by_name):
    old_keys = set(old_by_name)
    new_keys = set(new_by_name)

    tables_added   = sorted(new_keys - old_keys)
    tables_removed = sorted(old_keys - new_keys)
    tables_changed = []

    for name in sorted(old_keys & new_keys):
        old_t = old_by_name[name]
        new_t = new_by_name[name]

        cols_added, cols_removed, cols_changed = _diff_columns(
            old_t["columns"], new_t["columns"]
        )
        pk_changed = _diff_pk(old_t["primary_key"], new_t["primary_key"])
        partition_change = _diff_partition(old_t, new_t)

        if any([cols_added, cols_removed, cols_changed, pk_changed, partition_change]):
            entry = {"table": new_t["name"]}
            if cols_added:
                entry["columns_added"] = [
                    {"name": c["name"], "type": c["type"]} for c in cols_added
                ]
            if cols_removed:
                entry["columns_removed"] = [
                    {"name": c["name"], "type": c["type"]} for c in cols_removed
                ]
            if cols_changed:
                entry["columns_changed"] = cols_changed
            if pk_changed:
                entry["pk_change"] = {
                    "old": old_t["primary_key"],
                    "new": new_t["primary_key"],
                }
            if partition_change:
                entry["partition_change"] = partition_change
            tables_changed.append(entry)

    return {
        "tables_added":   tables_added,
        "tables_removed": tables_removed,
        "tables_changed": tables_changed,
    }


def has_changes(diff):
    return bool(
        diff["tables_added"] or diff["tables_removed"] or diff["tables_changed"]
    )


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------

def format_text(diff, old_doc, new_doc):
    old_commit = old_doc.get("moves_commit", "?")
    new_commit = new_doc.get("moves_commit", "?")
    lines = [
        f"Schema diff: {old_commit[:12]}  →  {new_commit[:12]}",
        f"  old tables: {old_doc.get('table_count', '?')}",
        f"  new tables: {new_doc.get('table_count', '?')}",
        "",
    ]

    if diff["tables_added"]:
        lines.append(f"Tables added ({len(diff['tables_added'])}):")
        for t in diff["tables_added"]:
            lines.append(f"  + {t}")
        lines.append("")

    if diff["tables_removed"]:
        lines.append(f"Tables removed ({len(diff['tables_removed'])}):")
        for t in diff["tables_removed"]:
            lines.append(f"  - {t}")
        lines.append("")

    if diff["tables_changed"]:
        lines.append(f"Tables changed ({len(diff['tables_changed'])}):")
        for entry in diff["tables_changed"]:
            lines.append(f"  {entry['table']}:")
            for c in entry.get("columns_added", []):
                lines.append(f"    + column  {c['name']}  ({c['type']})")
            for c in entry.get("columns_removed", []):
                lines.append(f"    - column  {c['name']}  ({c['type']})")
            for c in entry.get("columns_changed", []):
                lines.append(
                    f"    ~ column  {c['name']}: "
                    f"{c['old_type']} → {c['new_type']}"
                )
            if "pk_change" in entry:
                o = ", ".join(entry["pk_change"]["old"]) or "(none)"
                n = ", ".join(entry["pk_change"]["new"]) or "(none)"
                lines.append(f"    ~ primary key: [{o}] → [{n}]")
            if "partition_change" in entry:
                pc = entry["partition_change"]
                lines.append(
                    f"    ~ partition strategy: "
                    f"{pc['old']} → {pc['new']}"
                )
        lines.append("")

    if not has_changes(diff):
        lines.append("No schema changes detected.")

    # Guidance for the operator when changes are present.
    if has_changes(diff):
        lines.append("Next steps:")
        if diff["tables_added"] or diff["tables_removed"]:
            lines.append(
                "  1. Review added/removed tables in "
                "characterization/default-db-schema/tables.json."
            )
            lines.append(
                "     New tables may need rows added to the Parquet "
                "conversion plan (partitioning-plan.md)."
            )
            lines.append(
                "     Removed tables may require removing a Parquet "
                "partition or updating readers."
            )
        if diff["tables_changed"]:
            lines.append(
                "  2. For each changed table, check whether the "
                "Rust reader / calculator uses the changed columns."
            )
            lines.append(
                "     Search for the table name in crates/ to find "
                "affected read sites."
            )
        lines.append(
            "  3. Re-run audit-schema.py against the new DDL to "
            "regenerate tables.json."
        )
        lines.append(
            "  4. Re-run the default-DB conversion pipeline "
            "(convert-default-db.sh) with the new SIF."
        )
        lines.append(
            "  5. Re-run the full characterization fixture suite "
            "(run-all-fixtures.sh) and diff snapshots."
        )
        lines.append(
            "  See docs/upstream-tracking.md for the full procedure."
        )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main(argv=None):
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("old", type=Path, help="Old tables.json (current release)")
    parser.add_argument("new", type=Path, help="New tables.json (incoming release)")
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Write output to this file instead of stdout",
    )
    args = parser.parse_args(argv)

    try:
        old_doc, old_by_name = _load(args.old)
        new_doc, new_by_name = _load(args.new)
    except (FileNotFoundError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(2)

    diff = diff_schemas(old_by_name, new_by_name)

    if args.format == "json":
        result = {
            "old_commit":      old_doc.get("moves_commit"),
            "new_commit":      new_doc.get("moves_commit"),
            "has_changes":     has_changes(diff),
            "tables_added":    diff["tables_added"],
            "tables_removed":  diff["tables_removed"],
            "tables_changed":  diff["tables_changed"],
        }
        text = json.dumps(result, indent=2)
    else:
        text = format_text(diff, old_doc, new_doc)

    if args.output:
        args.output.write_text(text + "\n")
    else:
        print(text)

    sys.exit(1 if has_changes(diff) else 0)


if __name__ == "__main__":
    main()
