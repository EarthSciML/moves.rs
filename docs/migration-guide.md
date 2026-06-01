# Migration Guide — moves.rs Polecat Reference

This guide is for polecats implementing a task from the
[`moves-rust-md`](../moves-rust-md). It covers
the non-obvious setup steps that let you work efficiently on a fresh worktree.

---

## 1. Resolve the upstream MOVES source

Each work item names the upstream source files it is porting — for
example, `database/AirToxicsCalculator.sql` (749 lines) or
`calc/airtoxics/airtoxics.go` (448 lines). These paths are **relative to
the root of the upstream `EPA_MOVES_Model` repository**. You need a local
checkout of that tree to read the source.

> **Never** locate those files with `bfs /`, `find /`, or any search rooted
> at `/`. A filesystem-wide walk traverses tens of GB of unrelated worktrees
> and Dolt data, runs for *hours*, balloons to GB-scale RSS, and orphans when
> your session ends — it is the exact failure this section exists to prevent.
> If you must search for a file, scope it to a bounded root: the
> rig directory at most, never `/`.

Resolve the checkout once per worktree with the helper script — it does no
filesystem search:

```bash
MOVES_SRC=$(scripts/resolve_moves_src.sh)
```

`resolve_moves_src.sh` picks the first source that works:

1. `$MOVES_SRC` — explicit override: an existing checkout you point it at.
2. `/opt/moves` — canonical location inside the apptainer SIF, if running there.
3. Clone fallback: `git clone` `USEPA/EPA_MOVES_Model` at the commit pinned in
 `characterization/apptainer/files/versions.env` (`MOVES_COMMIT`) into a
 per-user cache (`$XDG_CACHE_HOME/moves-rs-migration-src/EPA_MOVES_Model`),
 checked out detached at that SHA. The clone is cached — the first polecat
 pays for it, the rest reuse it.

The script prints the resolved absolute path on stdout (diagnostics go to
stderr), so the command substitution above captures just the path. It is
idempotent and concurrency-safe.

The pinned SHA is MOVES5.0.1 (`25dc6c833dd8c88198f82cee93ca30be1456df8b`).
The work item description's source-path line numbers are relative to that commit.
Resolving against a different revision still works, but the script warns that
line numbers may have drifted — confirm you are looking at the right section.

With `$MOVES_SRC` set, the source paths resolve directly — no search:

```bash
# Read the SQL calculator you are porting
cat "$MOVES_SRC/database/AirToxicsCalculator.sql"

# Read the Go calculator variant
cat "$MOVES_SRC/calc/airtoxics/airtoxics.go"

# Browse CalculatorInfo.txt for registration metadata
grep "AirToxicsCalculator" "$MOVES_SRC/CalculatorInfo.txt"

# Browse the Java source for a class
cat "$MOVES_SRC/gov/epa/otaq/moves/master/implementation/ghg/AirToxicsCalculator.java"
```

Override the rev if you need to examine a different commit:

```bash
MOVES_SRC_REV=<sha> MOVES_SRC=$(scripts/resolve_moves_src.sh)
```

---

## 2. What to port

Each work item description lists the upstream source files to port and follows the
corresponding section of the The typical artefacts to read are:

| Artefact type | Path pattern under `$MOVES_SRC` |
|--------------|--------------------------------|
| SQL calculator | `database/<CalculatorName>.sql` |
| Go calculator | `calc/<lowercasename>/<lowercasename>.go` |
| Java calculator | `gov/epa/otaq/moves/master/implementation/.../<ClassName>.java` |
| CalculatorInfo.txt registrations | `CalculatorInfo.txt` (root) |
| Fortran NONROAD | `NONROAD/NR08a/SOURCE/<file>.f` |

The expected line counts are listed in the work item description. Use those counts
as a sanity check that you're reading the right file.

---

## 3. Characterization fixtures

Each calculator port must produce numerically identical results to canonical
MOVES on the fixture suite (within the established tolerance budgets).
The fixture snapshots live under `characterization/snapshots/`; the canonical
MOVES oracle is `characterization/apptainer/canonical-moves.sif`.

See `characterization/apptainer/README.md` for how to run a fixture and
capture intermediate state.
