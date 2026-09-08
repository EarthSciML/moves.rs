# `<day>` selection audit: declared vs. resolved vs. intended

Measured 2026-09-08 over all 52 fixtures in `characterization/fixtures/` and
the 40 populated snapshots in `characterization/snapshots/` (repo state
`5de6499`).

Reproduce with:

```sh
python3 characterization/audit/day-selection-audit.py
```

## The mechanism

`RunSpecXML.processTimeSpan` reads `<day>` two different ways:

* `<day key="N"/>` — **N is an index** into `TimeSpan.allDays`
  (`getDayByIndex`).
* `<day id="N"/>` — **N is the literal `dayID`** (`getDayByID`): 5 =
  weekday, 2 = weekend.

`allDays` has two entries, ordered by ascending `dayID`, so the only valid
keys are 0 (→ dayID 2, weekend) and 1 (→ dayID 5, weekday). `key="5"`
resolves to null, no day is added to the selection, the selection is left
empty — and MOVES then runs **every** day.

This was already written down in `_generate.py`'s `TimeSpan` docstring. What
was missing was the count.

### Measured confirmation of the index semantics

`sample-runspec.xml` is the only fixture that uses a key inside the valid
range, `<day key="0"/>`, and its snapshot resolves to `RunSpecDay.dayID = 2`
— weekend. That pins `allDays[0] == 2` from data rather than from reading
the Java, and with it the whole "key is an index" reading.

## Headline numbers

| | count |
|---|---|
| fixture XMLs | 52 |
| declaring `<day key=...>` | **40** |
| declaring `<day id=...>` | **12** (all `nr-*`) |
| populated snapshots | 40 |
| snapshots whose `RunSpecDay` holds **both** dayIDs 2 and 5 | **26** |
| snapshots resolving to exactly one day | 14 |

So the standing belief — "roughly 40 of 52 fixtures run both days" — is
**half right and needs restating**. 40 fixtures carry the broken spelling;
**26 are measured to actually run both days.** The other 14 break down as:

* **12 `nr-*` fixtures** use `<day id="5"/>` and correctly run weekday only.
* **`sample-runspec`** uses `<day key="0"/>`, which is a *valid* index and
  resolves to weekend (dayID 2). It is a byte-identical copy of upstream
  `testdata/SampleRunSpec.xml`, so this is what canonical MOVES itself
  means. Not a defect; do not "fix" it.
* **`expand-month`** uses `<day key="5"/>` *and* `<aggregateBy key="Month"/>`,
  which collapses the day dimension: `RunSpecDay` holds a single row with
  `dayID = 0` and the output carries `dayID = 0`. The broken selection is
  still there — it is simply not observable in the snapshot, and the
  emissions it aggregates are a two-day total rather than the intended
  weekday.

A further **12 fixtures have no snapshot** (`error-bad-geotype`,
`error-bad-model`, `error-bad-modelscale`, `expand-counties-large`,
`expand-fullyear`, `expand-multifuel`, `expand-multiyear`,
`expand-roadtypes`, `rates-minimal`, `scale-county`, `scale-project`,
`scale-rates`). All 12 declare `<day key="5"/>`, so they will run both days
whenever they are captured, but that is inference from the mechanism, not a
measurement.

## Cross-tab (snapshotted fixtures only)

| `<day>` attribute | dayIDs emitted in `MOVESOutput` | fixtures |
|---|---|---|
| `id` | 5 (weekday) | 12 |
| `key` | 2 and 5 | 18 |
| `key` | *(table has 0 rows)* — `RunSpecDay` = {2, 5} | 8 |
| `key` | 0 (day dimension aggregated away) | 1 |
| `key` | 2 (weekend) | 1 |

The eight zero-row cases are the start/idle/hotelling fixtures documented in
`docs/known-divergences.md` §(process-apu / crankcase-extidle /
crankcase-start / extended-idle and their `-single` variants): canonical
MOVES itself emits no rows for those processes at this scale. Their
`RunSpecDay` still holds both days, so they too ran both days — the
`MOVESOutput` table simply cannot show it.

## Full table

`declared` is the literal XML; `intent` is `_generate.py`'s catalogue entry
(`TimeSpan.days` / `day_attr`, defaulting to `days=(5,)  # Weekdays` and
`day_attr="key"`); `RunSpecDay` is MOVES's own resolved selection from
`MOVESExecution.RunSpecDay`; `MOVESOutput` is the distinct `dayID` in the
output table.

| fixture | declared | intent | RunSpecDay | MOVESOutput | verdict |
|---------|----------|--------|------------|-------------|---------|
| chain-nonhaptog | `key=5` | uncatalogued | 2,5 | 2,5 | both days |
| chain-tog-speciation | `key=5` | 5 via key | 2,5 | 2,5 | both days |
| error-bad-geotype | `key=5` | uncatalogued | – | – | no snapshot |
| error-bad-model | `key=5` | uncatalogued | – | – | no snapshot |
| error-bad-modelscale | `key=5` | uncatalogued | – | – | no snapshot |
| expand-counties-large | `key=5` | uncatalogued | – | – | no snapshot |
| expand-counties | `key=5` | 5 via key | 2,5 | 2,5 | both days |
| expand-criteria | `key=5` | 5 via key | 2,5 | 2,5 | both days |
| expand-day | `key=2 key=5` | 2,5 via key | 2,5 | 2,5 | right answer, wrong mechanism |
| expand-fueltype-diesel | `key=5` | 5 via key | 2,5 | 2,5 | both days |
| expand-fullyear | `key=5` | uncatalogued | – | – | no snapshot |
| expand-month | `key=5` | 5 via key | 0 | 0 | aggregated; selection still broken |
| expand-multifuel | `key=5` | uncatalogued | – | – | no snapshot |
| expand-multiyear | `key=5` | uncatalogued | – | – | no snapshot |
| expand-roadtypes | `key=5` | uncatalogued | – | – | no snapshot |
| expand-sourcetype | `key=5` | 5 via key | 2,5 | 2,5 | both days |
| mixed-onroad | `key=5` | uncatalogued | 2,5 | 2,5 | both days |
| nr-agriculture-state | `id=5` | 5 | 5 | 5 | ok |
| nr-airport-support-county | `id=5` | 5 | 5 | 5 | ok |
| nr-airtoxics-lawn-garden-county | `id=5` | 5 via id | 5 | 5 | ok |
| nr-commercial-nation | `id=5` | 5 | 5 | 5 | ok |
| nr-construction-state | `id=5` | 5 | 5 | 5 | ok |
| nr-industrial-county | `id=5` | 5 | 5 | 5 | ok |
| nr-lawn-garden-county | `id=5` | 5 | 5 | 5 | ok |
| nr-logging-county | `id=5` | 5 | 5 | 5 | ok |
| nr-mixed-nonroad | `id=5` | uncatalogued | 5 | 5 | ok |
| nr-pleasure-craft-state | `id=5` | 5 | 5 | 5 | ok |
| nr-railroad-support-nation | `id=5` | 5 | 5 | 5 | ok |
| nr-recreational-county | `id=5` | 5 | 5 | 5 | ok |
| process-airtoxics | `key=5` | 5 via key | 2,5 | 2,5 | both days |
| process-apu-single | `key=5` | uncatalogued | 2,5 | *(0 rows)* | both days |
| process-apu | `key=5` | 5 via key | 2,5 | *(0 rows)* | both days |
| process-brakewear | `key=5` | 5 via key | 2,5 | 2,5 | both days |
| process-crankcase-extidle-single | `key=5` | uncatalogued | 2,5 | *(0 rows)* | both days |
| process-crankcase-extidle | `key=5` | 5 via key | 2,5 | *(0 rows)* | both days |
| process-crankcase-running | `key=5` | 5 via key | 2,5 | 2,5 | both days |
| process-crankcase-start-single | `key=5` | uncatalogued | 2,5 | *(0 rows)* | both days |
| process-crankcase-start | `key=5` | 5 via key | 2,5 | *(0 rows)* | both days |
| process-evap-fvv | `key=5` | 5 via key | 2,5 | 2,5 | both days |
| process-evap-leaks | `key=5` | 5 via key | 2,5 | 2,5 | both days |
| process-evap-permeation | `key=5` | 5 via key | 2,5 | 2,5 | both days |
| process-extended-idle-single | `key=5` | uncatalogued | 2,5 | *(0 rows)* | both days |
| process-extended-idle | `key=5` | uncatalogued | 2,5 | *(0 rows)* | both days |
| process-nox-speciation | `key=5` | uncatalogued | 2,5 | 2,5 | both days |
| process-pm-exhaust | `key=5` | 5 via key | 2,5 | 2,5 | both days |
| process-refueling | `key=5` | 5 via key | 2,5 | 2,5 | both days |
| process-tirewear | `key=5` | 5 via key | 2,5 | 2,5 | both days |
| rates-minimal | `key=5` | uncatalogued | – | – | no snapshot |
| sample-runspec | `key=0` | uncatalogued | 2 | 2 | correct as written (upstream copy) |
| scale-county | `key=5` | 5 via key | – | – | no snapshot |
| scale-project | `key=5` | 5 via key | – | – | no snapshot |
| scale-rates | `key=5` | 5 via key | – | – | no snapshot |

## What it costs

For every `key=5` fixture the run covers two days instead of one, so:

* runtime and snapshot size are roughly doubled on the day dimension;
* any consumer that treats these fixtures as "one weekday" is silently
  summing a weekday and a weekend.

It does **not** make the snapshots wrong as oracles: the snapshot faithfully
records what canonical MOVES did with the RunSpec as written, and the port is
compared against the same RunSpec. It makes the fixtures *not test what the
catalogue says they test*, and it makes them more expensive than intended.

## Recommendation

Change `<day key="N"/>` to `<day id="N"/>` in the 39 affected fixtures —
every `key=` fixture **except `sample-runspec.xml`**, which must stay a
byte-identical copy of the upstream file. `expand-day` should become
`<day id="2"/><day id="5"/>`: it already gets both days, but by accident
rather than by declaration.

Correcting the XML changes the RunSpec bytes, so the 27 affected populated
snapshots must be recaptured. Fold that into the `moves-snapshot/v2`
recapture sweep (`docs/snapshot-v2-migration.md`) rather than running two
sweeps.
