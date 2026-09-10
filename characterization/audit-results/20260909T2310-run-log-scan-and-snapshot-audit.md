# Run-log failure scan: what it missed, what the rule is now, and what the
# committed snapshots can and cannot be cleared of

Measured 2026-09-09 (local; MOVES's own in-log timestamps are UTC and read
`9/10/26`) against repo state `eb11cb71` — all 42 snapshots of the
`moves-snapshot/v2` recapture sweep in `characterization/snapshots/`.

Reproduce with:

```sh
python3 characterization/audit/snapshot-integrity-audit.py
```

## Why this audit exists

`characterization/apptainer/run-fixture.sh` wraps a canonical-MOVES run. ant
invokes MOVES via `<java fork="yes">` **without** `failonerror="true"` (MOVES's
own `build.xml`, target `main1worker`), so a MOVES JVM that dies is never
reported as a non-zero status to anything above it. The run log is the only
witness, which makes `scan_moves_log()` the primary failure detector and the
table-count checks the belts behind it.

Four project-scale trials failed that night
(`/scratch/$USER/scalescope-trial/trial{1,2,3,4}.log`) and the scan printed
`[run-fixture] MOVES run OK (no failure markers in ...)` on **every one of
them**. Only the zero-dumped-table belt stopped a bad snapshot from being
written. That is the third distinct time a failure string has walked past this
guard, which is what turns "widen the marker list again" into "audit what is
already committed".

## The rule that replaced the marker list

Old, four literal strings, one of them labelled "the general one":

```
'ERROR: Error:'   'BUILD FAILED'
'The specified runspec file does not exist'   'ERROR: A runspec was not provided'
```

`ERROR: Error:` is not general. It matches only `Logger.logError`'s spelling.

New: any line carrying a **word-boundary-anchored `ERROR:` token**,
`(^|[^A-Za-z0-9_])ERROR:`, plus `BUILD FAILED` and the two RunSpec-argument
literals (which MOVES prints outside its Logger). The reasoning is in the
comment above `MOVES_FAILURE_PATTERNS` in `run-fixture.sh`; the short form,
from MOVES's own source inside `moves-fixture.sif`:

* `common/Logger.java:83` renders every message as
  `"<timestamp> <CATEGORY>: <message>"`, so a bare `ERROR:` token is exactly
  `LogMessageCategory.ERROR`, defined at `common/LogMessageCategory.java:41-42`
  as "an error, the program cannot continue to run".
* `common/Logger.java:78-80` rewrites **both** `WARNING` and `ERROR` to the
  `RUN_ERROR` category while `Logger.shouldPromoteErrorLevel` is set, and
  `master/framework/MOVESEngine.java` sets it at line 457 (simulation start)
  and clears it at line 1245 (simulation end). So `ERROR:` is only ever
  emitted **outside** the simulation, where it is unconditionally terminal.

`RUN_ERROR:` is excluded by the word boundary — `_` is a word constituent —
and not by an allowlist. **The allowlist is empty.**

### The warning-shaped `ERROR:` lines, and why they still fail the run

`trial4.log:124-126`:

```
ERROR: Missing: Warning: Fuel formulation 2675 changed fuelSubtypeID from 12 to 13 based on ETOHVolume
ERROR: Missing: Warning: Fuel formulation 2676 changed fuelSubtypeID from 12 to 13 based on ETOHVolume
ERROR: Missing: Warning: Fuel type 3 is imported but will not be used
```

Warnings in substance, logged at ERROR level. They are nonetheless a correct
failure signal, and the call site is the reason —
`master/framework/MOVESAPI.java:870-876`:

```java
result = manager.performAllImporterChecks(runSpec,messages,db);
if(result < 0) {
    // Log the error messages
    for(Iterator<String> i = messages.iterator(); i.hasNext(); ) {
        Logger.log(LogMessageCategory.ERROR, i.next());
    }
    return false;
}
```

The whole importer message list is re-logged at ERROR level **only** on the
`result < 0` branch, which then refuses to launch the run. The `Missing: `
prefix comes from `framework/importers/ImporterBase.java:409-411`, which
prepends it to any quality message not already starting with `error`. So a
warning-shaped `ERROR:` line means "MOVES dumped its importer message list on
the way out": a failure by construction, not a warning that got misfiled.

Allowlisting them would also have blinded the guard to trial4 almost
completely — strip those three lines and `ImporterInstantiator` is the only
evidence left in that log.

### False-positive rate, measured

Corpus: the retained MOVES run log of every one of the 42 published snapshots
(38 under `/scratch/$USER/moves-fixture/<fixture>/moves-run.log`, 4 under
`/scratch/$USER/moves-county-fixture/`; 42/42 resolved by fixture name against
`ls characterization/snapshots/`).

| | count |
|---|---|
| run logs scanned | 42 |
| logs matched by the new rule | **0** |
| logs matched by the old rule | 0 |
| logs containing the substring `ERROR` anywhere | 6 |

These are the counts **after** the one ambiguity is resolved (see §"Bounds",
point 2). `snapshot-integrity-audit.py` scans every candidate file it finds and
so prints the unresolved figures — **43 logs, 1 rule match, 7 substring logs** —
because `process-apu-single` has two logs on scratch and the extra one is the
02:36 run that failed. The tool is not disagreeing with this table; it is
reporting one line per file where this table reports one line per snapshot.

All six near-misses are the same shape:

```
RUN_ERROR: WARNING: Using default formulation <N> for <fuel> in region <R>,
year 2020, month 8. Check your input FuelSupply table for errors.
```

in `process-apu-single` (2 lines), `process-crankcase-extidle-single` (2),
`process-crankcase-start-single` (3), `process-extended-idle-single` (2),
`nr-construction-state` (1) and `nr-pleasure-craft-state` (1). A scan that
greps for the substring `ERROR`, or for `ERROR:` case-insensitively, turns all
six into failures — a 14% false-positive rate, i.e. a guard that gets switched
off within a week. The word boundary is what avoids that.

Confirmed on three **fresh** captures taken for this change with the same SIF
(`sif_sha256 4f92c...15a2f`): see "Fresh captures" below.

### True-positive rate

| log | new rule | old rule | outcome that night |
|---|---|---|---|
| `trial1.log` | 8 | 0 | refused by the zero-table belt |
| `trial2.log` | 3 | 0 | refused by the zero-table belt |
| `trial3.log` | 2 | 0 | refused by the zero-table belt |
| `trial4.log` | 4 | 0 | refused by the zero-table belt |
| `trial5.log` | **0** | 0 | succeeded, published 360 tables |

The strings that walked past the old set, all now caught:

* `ERROR: Unable to validate input database Data Status` (7× in trial1)
* `ERROR: Unable to count offNetwork links`
* `ERROR: ImporterInstantiator is unable to instantiate gov.epa.otaq.moves.master.implementation.importers.GenericImporter`
* `ERROR: ERROR: AVFT table does not exist. You may need to recreate your database to solve this problem.`
* `ERROR: ERROR: AVFT table is not imported.`
* `ERROR: Missing: Warning: ...` (trial4:124-126)

Each has a case in `characterization/apptainer/tests/run-fixture-guards.sh`.

### The header claim that had to go

`run-fixture.sh` previously asserted, as its false-positive evidence:

> across 13 successful captures of this suite (chain-*, expand-*, mixed-onroad,
> process-*), the string `ERROR` does not appear in the run log at all.

That was **unverifiable** when this audit started, because no snapshot retained
a run log. Re-measuring it against the 42-log corpus shows it is also **false**:
`ERROR` appears in 6 of 42, as `RUN_ERROR`. The claim is retracted and replaced
by the table above. This is also why the fix ships with `moves-run.log`
published into every snapshot: the next person to ask this question should not
have to re-run MOVES to answer it.

## Audit of the 42 committed snapshots

Read directly from the parquet footers; `manifest.json`'s `row_count` is not
trusted, it is cross-checked (14,766 tables read, **0** mismatches).

| | value |
|---|---|
| snapshots | 42 |
| tables per snapshot | 316 – 384 |
| non-zero tables per snapshot | 180 – 271 |
| parquet vs `manifest.json` `row_count` mismatches | 0 |
| snapshots with no execution database | 0 |
| snapshots with no output database | 0 |
| snapshots with `MOVESOutput` = `MOVESActivityOutput` = `BaseRateOutput` = 0 | 8 |

**trial1's signature appears nowhere.** That run dumped 27 tables, all of them
from the county input database `washtenaw_cdb`, with no `MOVESExecution*` and
no output database at all; the capture binary keeps only the execution and
output databases, so the snapshot came out with 0 tables and was refused. Every
one of the 42 committed snapshots holds an execution database (286 – 345
tables), a worker database, and an output database (27 or 32 tables).

### The 8 zero-emission snapshots

`process-apu`, `process-crankcase-extidle`, `process-crankcase-start`,
`process-extended-idle` and the four `-single` variants. These are exactly the
start / idle / hotelling fixtures that `docs/known-divergences.md` (lines
93-100) records as asserted **vacuous**: canonical's captured execution DB holds
the base rate but its activity and output tables are empty, so canonical's
authoritative output for the process is zero rows, and the port reproduces that.
The `vacuous` flag in `crates/moves-cli/tests/full_suite_regression.rs` fails
loudly if a recapture ever gives either side a non-zero row. Not a failure.

### Two corrections to the cheap-layer pass

The pre-existing informal audit reported **9** such snapshots, the 8 above plus
`sample-runspec`, described as writing to output database `JUnitTestOutput` with
a "1-row `output_tbl`". Reproducing it independently:

1. The count is **8, not 9**. `sample-runspec` has
   `db__junittestoutput__movesoutput` = **84 rows** (and `finalaggafter`,
   `finalaggbefore`, `temporaryoutputimport`, `unitconvertafter` all 84 as well).
   It is a fully populated snapshot and does not belong in the zero-emission
   group at all. The likely cause of the miscount is a search keyed on
   `out_<fixture>`: `sample-runspec` is the one fixture whose output database is
   not named after the fixture, so a name-pattern scan finds nothing and reports
   zero rather than reporting "not found".
2. The `output_tbl` referred to holds **84 rows, not 1**. There is such a
   table — `moves_temporary__manyworkers__workerfolder__workertemp7__output_tbl`
   — but it is a `MOVESTemporary` worker file, not a database table, and it
   carries the same 84 rows as `MOVESOutput`, which is what a completed run
   looks like. (The 1-row table in that snapshot is
   `db__junittestoutput__translate_county`, a decode table.) `sample-runspec`
   is a fully populated snapshot by every measure available.

Neither correction changes the conclusion. Both are recorded because the
conclusion is only worth what the reproduction is worth.

## Fresh captures

Three fixtures re-captured end-to-end on 2026-09-09 against the same pinned SIF
(`characterization/fixture-image.lock`, `sif_sha256 4f92c5939c86...b415a2f`),
using the wrapper as changed:

```sh
./characterization/apptainer/run-fixture.sh -f \
    --runspec "$PWD/characterization/fixtures/<fixture>.xml" \
    --sif  /projects/.../moves.rs/characterization/apptainer/moves-fixture.sif \
    --workdir    /scratch/$USER/agent-markers/work/<fixture> \
    --output-dir /scratch/$USER/agent-markers/out/<fixture>
```

| fixture | rc | tables | published `moves-run.log` | `ERROR:` matches | `ERROR` substring lines |
|---|---|---|---|---|---|
| `nr-logging-county` | 0 | 324 | 10,905 B | 0 | 0 |
| `process-crankcase-start` | 0 | 316 | 9,545 B | 0 | 0 |
| `chain-tog-speciation` | 0 | 373 | 24,739 B | 0 | 0 |

All three exited 0, published a snapshot, and carry the run log inside it —
which is the point: this is the first time a snapshot in this project has
shipped with the evidence for its own success.

They also reproduce the committed snapshots. Comparing content hashes table by
table against `characterization/snapshots/<fixture>/`:

* `process-crankcase-start` — 316/316 tables shared, **0** content
  differences. Byte-identical.
* `nr-logging-county` — 321 shared tables, **0** content differences. The 3
  unshared ones are the same three `MOVESTemporary` worker tables under a
  different worker-directory index (`workertemp` vs `workertemp3`), which is
  a run-local name, not data.
* `chain-tog-speciation` — 367 shared tables, **1** content difference, and 6
  unshared worker-temp tables. The unshared ones are an artifact of this
  audit: the first attempt at this fixture aborted mid-script, and
  `run-fixture.sh` wipes the MariaDB datadir between runs but not
  `MOVESTemporary/`, so the re-run's snapshot carries both that attempt's
  `workertemp3` set and its own `workertemp` set.

The one real content difference is worth recording even though it is outside
this audit's subject:

```
db__movesexecution..._drivingidlefraction, 1 row both sides
  committed  drivingIdleFraction = 3.0299686857752525e-02
  fresh      drivingIdleFraction = 3.029968685775254e-02
```

A last-bit double-precision difference (~5e-16 relative) in canonical MOVES's
own output, i.e. canonical is not bit-deterministic for this value across
runs of the same SIF on the same host. That is the class
`docs/known-divergences.md` §4.2 and
`characterization/audit-results/20260908T0948-float-precision-blast-radius.md`
already cover; it is noted here because a table-hash comparison of snapshots
will see it, and not chased further.

## Bound

**Read this before quoting any number above.**

What the snapshot audit establishes: none of the 42 committed snapshots is the
product of a run that failed **grossly** — one that produced no output database,
lost its execution database, or came out short of tables. Every snapshot holds
a complete-looking execution DB, worker DB and output DB, and every parquet row
count agrees with its manifest.

What it does **not** establish, and cannot:

1. **A partial failure is invisible to it.** A MOVES that died part-way through
   the simulation, after populating some tables, looks from the snapshot alone
   exactly like a MOVES that succeeded. The table counts, the non-zero counts
   and the manifest cross-check all pass. Nothing in a snapshot records "the run
   completed".
2. **The log re-scan is an inference, not a proof.** All 42 logs found on
   scratch bind to their snapshots by **directory name only**. MOVES never
   echoes the output database name into the run log, so there is no content-level
   tie between a log and the snapshot it supposedly explains. The logs are also
   on `/scratch`, subject to purge, and are overwritten by the next run of the
   same fixture — which is precisely the deficiency being fixed. Treat the
   "0 of 42 matched" line as strong but circumstantial.
   * The one ambiguous case, `process-apu-single`, has two candidate logs: the
     02:36 attempt in `moves-fixture/`, which failed with
     `ERROR: Error: The database does not have the required county.` and produced
     no output database, and the 02:57 re-run in `moves-county-fixture/`, which
     succeeded. The published snapshot holds a 27-table `out_process_apu_single`,
     which only the 02:57 run could have produced, so that is its log — and it
     has 0 matches. The 02:36 log is the historical incident the old marker set
     was widened for, and it is correctly matched by the new rule.
3. **The scan itself is blind to in-simulation errors.** `Logger.java:78-80`
   collapses `WARNING` and `ERROR` into the single rendered token `RUN_ERROR:`
   for the whole duration of a simulation, so an error raised *during* the run is
   textually identical to a nuisance warning. No rule over the log text can
   separate them. Filed as
   [#64](https://github.com/EarthSciML/moves.rs/issues/64), with `MOVESError`
   row-count checking as the most promising fix — MOVES writes run errors to that
   table, and `dump-databases.sh` currently skips it as non-deterministic.

So: **the committed corpus is cleared of gross failure, and of the
pre-simulation error class as far as name-matched scratch logs can show. It is
not cleared of partial in-simulation failure, and cannot be until the corpus is
re-captured with `moves-run.log` published into each snapshot** — which the
wrapper now does, so a future re-run of this audit will have real evidence
rather than an inference. Re-capturing the 42 is roughly 2.5 h of cluster time
and is a separate decision.

This document should not be cited as a clean bill of health. It rules out one
shape of failure and names the shapes it cannot rule out.
