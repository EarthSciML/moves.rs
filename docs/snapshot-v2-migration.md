# Migrating the snapshot corpus from `moves-snapshot/v1` to `/v2`

**Status: EXECUTED, 2026-09-08/09.** All 42 populated snapshots in
`characterization/snapshots/` are `moves-snapshot/v2`. The sweep also
carried the `<day key=> → <day id=>` fixture correction (draft PR #53) and
the coupled port fix (#55), because corrected fixtures change what
canonical emits and the two could not land separately.

### Measured, against the estimates below

| | predicted here | measured |
|---|---|---|
| snapshots to recapture | 40 | **42** (the two `chain-so2-co2e-mechanism*` captures landed after this doc was written) |
| serial wall time | ~1 h 40 m at ~2.5 min each | **~2 h 5 m** of capture; 127–489 s per fixture, median ~145 s |
| corpus size | 2.5–3.6 GB, from 2.9 GB | **2.797 GB**, from 3.200 GB — a **12.6% reduction**, not a growth |
| tables with a float in the natural key | 120 of 14 055 | 3 distinct tables (`evaprvptemperatureadjustment`, `fuelwizardfactors`, `nrscrappagecurve`); only `fuelwizardfactors` actually reordered |
| `--shard I/4` speed-up | ~30 m – 1 h 15 m | **not achievable on one host** — MOVES hard-codes master port 13131 and MariaDB uses 3306, so concurrent captures collide. Sharding needs one machine per shard. |
| pilot cells | `1.105e-09` and `1.9e-11` | `nrdioxinemissionrate.meanBaseRate` = **`1.1045e-09`** (v1 rounded the 4th digit *wrong*); `dioxinemissionrate.meanBaseRate` = `4.7e-11` and **`8.27e-13`** where v1 stored `0.000000000001` — a 21% error |

The size prediction was directionally wrong. The reasoning ("string length
maps straight to bytes", mean v1 cell 14.62 bytes, up to 23 bytes in v2)
only considered cells that *gain* digits. In practice most cells lose them:
`0.500000000000` → `5e-01`, `0.000000000000` → `0e+00`. Part of the 12.6%
is also the day correction removing weekend rows from day-keyed tables, so
the two effects are not separable from these numbers alone —
`nr-airtoxics-lawn-garden-county`, whose row count is unchanged by the day
fix, isolates the encoding at **-3.6%** (139.5 → 134.5 MB excluding the
derived bundle).

### What the sweep found that this plan did not anticipate

Two runs of the same fixture, same SIF and same RunSpec bytes, are **not
byte-identical** — from worker-temp table naming (always true, v1
included) and from one MOVES-side value that is only bit-reproducible to
~6 ULP (`drivingidlefraction`, concealed by v1's twelve-decimal rounding).
`characterization/snapshots/README.md` § "Measured limits of the
determinism contract" has the numbers and the consequences for
`tolerance.toml` and the weekly diff gate.

The reader accepts both versions, so a v1 snapshot restored from history
stays loadable.

## Why

`moves-snapshot/v1` stored each float as a fixed-decimal string with twelve
places **after the point**. Twelve decimal places is not twelve significant
digits, so a value's surviving precision depended on its magnitude: near
1e-9 it kept four significant digits, near 1e-11 two, and below 5e-13
nothing at all. The measured cost across the 40 populated snapshots is in
[`../characterization/audit-results/20260908T0948-float-precision-blast-radius.md`](../characterization/audit-results/20260908T0948-float-precision-blast-radius.md):

* 60 226 821 of 64 656 397 non-zero float cells (93.1%) carry fewer
  significant digits than an f64 holds;
* 472 573 cells (303 columns) carry too few to support a 1e-9 relative
  comparison, 17 234 cells (134 columns) too few for 1e-6;
* 12 505 756 cells are stored as literal `0.000000000000` — an upper bound
  on flush-to-zero that the corpus itself cannot resolve.

This is a defect in the **capture**. No downstream port can reproduce digits
the reference never recorded.

## What changed

`moves-snapshot/v2` stores a float as the shortest correctly-rounded decimal
that parses back to a **bit-identical** f64, in normalized scientific
notation:

| value | v1 | v2 |
|-------|----|----|
| 1.5 | `1.500000000000` | `1.5e+00` |
| 1.105e-9 | `0.000000001105` | `1.105e-09` |
| 1.9e-11 | `0.000000000019` | `1.9e-11` |
| 4.7e-13 | `0.000000000000` | `4.7e-13` |
| 0.0 / -0.0 | `0.000000000000` | `0e+00` |

The rule is stated in `crates/moves-snapshot/src/format.rs`
(`float_to_canonical`) as a property of the *number*, not of any particular
formatter — the smallest `k` in `1..=17` whose correctly-rounded `k`-digit
decimal round-trips. Correctly-rounded fixed-precision decimal output is
unique (round-half-to-even at the tie) and so is correctly-rounded decimal
parsing, so the encoding has no locale, platform, or float-formatting-mode
freedom. It is deliberately **not** delegated to Rust's shortest-round-trip
printer: on an exact tie the two disagree in the last digit
(`1871524575217767.3` → `...7672` correctly rounded vs `...7673` from
`{:e}`), and only the correctly-rounded answer is specified. There is a test
pinning exactly that case.

Two other v2 changes follow from the encoding:

1. **Natural-key sort.** Rows whose key includes a float column now sort by
   IEEE total order instead of by the fixed-decimal string (which put `10.0`
   before `9.0` and could tie two doubles that rounded together). **120 of
   the 14 055 committed tables** have a float in the natural key; their row
   order will change on recapture. Measured with:
   ```sh
   python3 -c "
   import json,glob,collections
   c=collections.Counter()
   for p in glob.glob('characterization/snapshots/*/tables/*.meta.json'):
       m=json.load(open(p)); k={x['name']:x['kind'] for x in m['schema']}
       c['float_in_key' if any(k.get(n)=='float64' for n in m['natural_key']) else 'other'] += 1
   print(c)"
   ```
2. **Manifest self-description.** See the next section.

## The `float_decimals` field — replaced, not kept

**This is the breaking change for consumers.** `tables/<name>.meta.json`
used to carry:

```json
"float_decimals": 12,
```

from which a consumer derived a storage-quantum floor of
`0.5 * 10^-12`. A v2 sidecar **omits `float_decimals` entirely** and carries
instead:

```json
"float_encoding": { "kind": "shortest_round_trip", "max_significant_digits": 17 },
```

The field was replaced rather than kept because there is no fixed decimal
count under the new rule, and any number written into `float_decimals` would
be a lie that a consumer would silently turn into a wrong tolerance floor.
Omitting it makes an un-updated consumer fail loudly (`KeyError` /
`None`) instead.

`moves_snapshot::TableMetadata::float_encoding()` resolves either spelling
into a `FloatEncoding`, which answers the question consumers actually want:

| encoding | `absolute_quantum(x)` | `relative_quantum(x)` |
|----------|----------------------|-----------------------|
| `fixed_decimals{decimals: d}` | `0.5 * 10^-d` | that over \|x\|, `inf` below the quantum |
| `shortest_round_trip` | `0.0` | `0.0` |

### Required change in `../moves.esm` (NOT made here — out of scope)

`compare-output.py` reads `capture_decimals` from each table's
`.meta.json` to derive a storage-quantum floor. It needs, in lockstep with
the recapture sweep:

```python
enc = meta.get("float_encoding")
if enc is None:
    # moves-snapshot/v1: fixed decimal places after the point.
    d = meta["float_decimals"]          # today's `capture_decimals`
    abs_floor = 0.5 * 10 ** -d
elif enc["kind"] == "shortest_round_trip":
    # moves-snapshot/v2: lossless. No storage floor; the only slack left
    # is f64 representation itself.
    abs_floor = 0.0
elif enc["kind"] == "fixed_decimals":
    abs_floor = 0.5 * 10 ** -enc["decimals"]
else:
    raise ValueError(f"unknown float encoding {enc['kind']!r}")
```

Handling both branches lets the same checkout compare against v1 and v2
snapshots while the sweep is in flight. The scoped hold-out that exists
today for the small-magnitude `nrdioxinemissionrate` / NONROAD air-toxics
cells should be **deleted after** the corresponding fixtures are recaptured,
not before — those cells only become comparable once the capture carries the
digits.

## What must be recaptured

All **40** populated snapshots. There is no partial-migration story that
leaves the corpus coherent: the aggregate hash, the per-table content
hashes, and the row order of the 120 float-keyed tables all change.

The 12 fixtures with no snapshot today (`error-bad-geotype`,
`error-bad-model`, `error-bad-modelscale`, `expand-counties-large`,
`expand-fullyear`, `expand-multifuel`, `expand-multiyear`,
`expand-roadtypes`, `rates-minimal`, `scale-county`, `scale-project`,
`scale-rates`) are unaffected — if they gain snapshots they will be v2 from
the start.

### Cost

The most recent single capture (`nr-airtoxics-lawn-garden-county`,
2026-09-07) took **2 min 18 s** wall — 48 s of MOVES plus the MariaDB dump
and snapshot build. `run-all-fixtures.sh`'s own budget note says most
fixtures are under 5 minutes with the NONROAD and multi-county expansion
fixtures taking longer, and the corpus bears that out: the largest snapshot
(`nr-mixed-nonroad`) is 183 MB against 28 MB for the smallest.

| | estimate |
|---|---|
| serial wall time, 40 fixtures at ~2.5 min | ~1 h 40 m |
| serial wall time if the large NONROAD/expansion fixtures run to the 5–10 min budget | 3–5 h |
| with `run-all-fixtures.sh --shard I/N`, N = 4 | ~30 m – 1 h 15 m |
| SIF build, if `moves-fixture.sif` is not already cached | +1–2 h, once |

Treat 3–5 CPU-hours as the reservation. **This is an estimate from one
measured capture and the runner's own budget note, not a measured suite
run.**

### Disk

Parquet here is `compression=UNCOMPRESSED` with dictionaries off (both
required by the determinism contract), so string length maps straight to
bytes. Measured mean v1 float-cell string length is **14.62 bytes** over
9 482 401 cells sampled from `sample-runspec`, `nr-lawn-garden-county` and
`expand-criteria`. A full-precision double needs up to 23 bytes in the v2
spelling (`-d.dddddddddddddddde+ddd`), while short values get *shorter*
(`0e+00` is 5 bytes against 14). With 77.2 M non-null float cells in a 2.9 GB
corpus, expect somewhere between **2.5 GB and 3.6 GB** afterwards. This is
arithmetic on a measured mean, not a measured re-encode — the true v2
lengths cannot be computed from the corpus, because the digits they would
carry are exactly the ones v1 threw away.

## What breaks

### In `characterization/`

| Thing | Effect | Action |
|-------|--------|--------|
| Every `manifest.json` aggregate hash and per-table hash | changes | expected; that is the migration |
| Row order in the 120 float-keyed tables | changes | expected |
| `provenance.json` `snapshot_aggregate_sha256` | changes | rewritten by `moves-fixture-capture` |
| `characterization/snapshots/README.md` acceptance table | still accurate (counts unchanged) | none |
| `tolerance.toml` (`default_float_tolerance = 0.0`) | still correct, and now *stricter in effect* — v1's rounding was silently absorbing sub-quantum differences that v2 will surface | re-run the diff gate after the sweep and widen per column only where a real artifact appears |
| `moves-cli/tests/full_suite_regression.rs` (`canonical_snapshot_diff`) | compares per-pollutant `emissionQuant` **sums** at 1e-3 / 1e-2 relative — far above any storage quantum | no change expected; re-run to confirm |
| `calculator-validation` / `generator-validation` compare harnesses | parse cells with `str::parse::<f64>`, which reads both spellings | no change |
| `characterization/audit/float-precision-audit.py` | its `capacity` metric assumes the v1 rule | after the sweep it should report "lossless" for v2 tables; leave it as the v1 record |
| `execution-db.bundle` | carries the same strings; downstream casts Utf8→Float64 in polars, which reads scientific notation | verify once during the sweep on one fixture before committing 40 |

### Outside

| Thing | Effect | Action |
|-------|--------|--------|
| `../moves.esm` `compare-output.py` | reads `capture_decimals`; the field disappears | apply the patch above **in the same window** as the sweep |
| `../moves.esm` scoped hold-out for small-magnitude cells | becomes unnecessary for recaptured fixtures | remove per fixture, after that fixture is recaptured |
| `.github/workflows/fixture-suite-weekly.yml` | diffs a fresh capture against the committed corpus; a half-migrated corpus fails every week | run the sweep as one landing, not incrementally |

## Coupled change: the corrected `<day id=>` fixtures

A second branch commit corrects `<day key="N"/>` to `<day id="N"/>` in 39
fixture XMLs (see
[`../characterization/audit-results/20260908T1120-day-selection-audit.md`](../characterization/audit-results/20260908T1120-day-selection-audit.md)).
`key` is an *index* into `TimeSpan.allDays`, so `key="5"` resolved to
nothing and MOVES ran every day; 26 of the 40 populated snapshots are
two-day runs as a result.

That correction changes the RunSpec bytes for **27 populated fixtures**, so
those snapshots need recapturing too. Do it in the **same sweep** — the
alternative is capturing 40 snapshots twice.

If the sweep owner would rather not take the day correction, revert that one
commit and the v2 sweep stands alone. The two changes are independent; they
are only cheaper together.

Note before running with the corrected XMLs: `moves-cli/src/run.rs:355`
unconditionally sets the port's execution day set to every `DayOfAnyWeek`
day, so the *port* will still emit both days where corrected canonical
emits one. That has to be settled before the full-suite gate can pass
against a recaptured, day-corrected corpus.

## Recommended order

1. **Land the format change alone** (this branch). Corpus untouched, both
   versions readable, weekly gate still green.
2. **Prepare the consumer.** Patch `../moves.esm`'s `compare-output.py` to
   handle both `float_decimals` and `float_encoding`. Merge it *before* the
   sweep; it is a no-op against a v1 corpus.
3. **Let the in-flight fixture work land**, and decide whether the
   `<day id=>` correction rides along. Another agent is adding RunSpecs and
   capturing them in v1. Recapturing before that settles just means doing it
   twice.
4. **Pilot on one fixture.** Recapture `nr-airtoxics-lawn-garden-county` —
   the worst offender, and the one whose air-toxics cells motivated the
   change — into a scratch directory, not the corpus. Confirm: the
   `nrdioxinemissionrate.meanBaseRate` cells now read `1.105e-09` and
   `1.9e-11`; `moves-snapshot diff` against the v1 snapshot reports only the
   expected float-cell changes; the execution bundle still loads;
   `compare-output.py` on the esm side is happy.
5. **Sweep the remaining 39** with `run-all-fixtures.sh --fakeroot
   --keep-going --shard I/4`, then commit **all 40** in one commit so the
   corpus is never half-migrated.
6. **Re-run the gates**: `moves-snapshot diff` suite-wide,
   `full_suite_regression`, calculator/generator validation. Tighten or
   widen `tolerance.toml` only on evidence from that run.
7. **Then** drop the esm-side hold-out and re-measure the comparison, which
   is the whole point of the exercise.

## Rollback

`moves-snapshot` reads v1 and v2 and remembers which it read, so a v1
snapshot rewritten by a v2 binary keeps its original bytes (there is a test:
`v1_snapshot_loads_and_rewrites_byte_identically`). If the sweep goes wrong,
`git revert` the corpus commit; no code change is needed to keep reading the
restored v1 snapshots.
