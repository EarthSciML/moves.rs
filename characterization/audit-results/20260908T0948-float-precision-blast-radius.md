# Blast radius: fixed-decimal float storage in `moves-snapshot/v1`

Measured 2026-09-08 against the 40 populated snapshots in
`characterization/snapshots/` (repo state `5de6499`).

Reproduce with:

```sh
python3 characterization/audit/float-precision-audit.py \
    --snapshots characterization/snapshots --jobs 8 \
    --json /path/to/float-precision.json
```

Wall time 28.5 s (8 workers, 14 055 tables, 2.9 GB of parquet).

## The defect

`crates/moves-snapshot/src/format.rs` stores every `float64` column as a
fixed-decimal string with `FLOAT_DECIMALS = 12` places **after the point**.
Twelve decimal places is not twelve significant digits. For a value whose
leading significant digit sits at 10^e, the stored string has

    capacity = e + 1 + 12          (digit slots from the leading digit to 1e-12)

slots of information, and the absolute storage quantum is a flat 5e-13, so
the **relative** quantisation bound is

    rel_err <= 5 x 10^-capacity

| value | stored as | capacity | rel. quantum |
|-------|-----------|---------:|-------------:|
| 1.2345678901234567 | `1.234567890123` | 13 | 5e-13 |
| 1.105e-9 (`nrdioxinemissionrate.meanBaseRate`, pollutant 131) | `0.000000001105` | 4 | 5e-4 |
| 1.9e-11 (same column, pollutant 142) | `0.000000000019` | 2 | 5e-2 |
| 4.7e-13 | `0.000000000000` | 0 | total |

An f64 carries log10(2^53) = 15.95 significant decimal digits, so 16 slots
is the point at which the format stops throwing information away.

## Totals

| quantity | count |
|----------|------:|
| populated snapshots | 40 |
| tables scanned | 14 055 |
| float column instances (table x column x fixture) | 23 102 |
| distinct `table.column` pairs | 3 458 |
| float cells | 102 057 205 |
| — SQL `NULL` | 24 895 052 |
| — NaN / Infinity | 0 |
| — stored as literal `0.000000000000` | 12 505 756 |
| — finite non-zero | 64 656 397 |
| — *quantum-limited* (last decimal place non-zero **and** capacity < 16, i.e. the source double demonstrably had digits below the quantum) | 13 840 964 |

## Capacity distribution over the 64 656 397 finite non-zero cells

| slots | cells | share | rel. quantum |
|------:|------:|------:|-------------:|
| 1 | 894 | 0.001% | 5e-1 |
| 2 | 1 453 | 0.002% | 5e-2 |
| 3 | 1 895 | 0.003% | 5e-3 |
| 4 | 2 856 | 0.004% | 5e-4 |
| 5 | 3 090 | 0.005% | 5e-5 |
| 6 | 7 046 | 0.011% | 5e-6 |
| 7 | 33 621 | 0.052% | 5e-7 |
| 8 | 144 270 | 0.223% | 5e-8 |
| 9 | 277 448 | 0.429% | 5e-9 |
| 10 | 1 566 968 | 2.424% | 5e-10 |
| 11 | 6 116 187 | 9.460% | 5e-11 |
| 12 | 17 055 574 | 26.379% | 5e-12 |
| 13 | 6 260 621 | 9.683% | 5e-13 |
| 14 | 24 156 428 | 37.361% | 5e-14 |
| 15 | 4 598 470 | 7.112% | 5e-15 |
| 16 | 1 900 489 | 2.939% | — |
| 17–25 | 2 529 087 | 3.912% | — (digits beyond f64; noise) |

Cumulative:

| threshold | cells | share of finite non-zero | columns (of 3 458) affected |
|-----------|------:|-------------------------:|----------------------------:|
| capacity < 16 (below f64) | 60 226 821 | 93.149% | 1 293 |
| capacity <= 12 (rel. err > 1e-12) | 25 211 302 | 38.993% | 657 |
| capacity <= 9 (rel. err > 1e-9) | 472 573 | 0.731% | 303 |
| capacity <= 6 (rel. err > 1e-6) | 17 234 | 0.027% | 134 |
| capacity <= 4 (rel. err > 1e-4) | 7 098 | 0.011% | 30 |
| capacity <= 2 (rel. err > 1e-2) | 2 347 | 0.004% | 12 |
| capacity <= 1 (one digit) | 894 | 0.001% | 12 |

Reading: a downstream gate at 1e-9 relative tolerance has 472 573 cells
across the corpus that the *capture* cannot support; at 1e-6 it is 17 234.
Two-thirds of the corpus already sits at a relative quantum worse than
1e-13, which is fine for an absolute-magnitude eyeball and wrong for any
relative comparison of small quantities.

## Flush to zero

12 505 756 cells are stored as the literal string `0.000000000000`. **This
is an upper bound, not a measurement of loss**: a genuine 0.0 emission and a
flushed 4e-13 are indistinguishable in the stored corpus, and MOVES emits
many true zeros. The raw MariaDB TSV captures that would settle it are not
retained on this host (`/scratch/$USER/moves-fixture/*/captures/` survives
for exactly one non-corpus run), so this audit cannot separate the two
without a recapture.

Narrowing to columns whose non-zero values reach down to within three
decades of the quantum — where a flushed tail is plausible rather than
hypothetical:

| column min capacity | zero cells | columns |
|--------------------:|-----------:|--------:|
| <= 3 | 14 638 | 19 |
| <= 4 | 17 422 | 30 |
| <= 6 | 591 359 | 134 |

## Worst offenders

Ranked by minimum capacity, then by count of <=6-slot cells. Full
per-column data is in the JSON the script emits.

| min slots | <=6-slot cells | quantum-limited | zeros | n | table.column |
|----------:|---------------:|----------------:|------:|---:|--------------|
| 1 | 1 952 | 1 750 | 1 208 | 14 036 | `db__out_nr_airtoxics_lawn_garden_county__movesoutput.emissionQuant` (and `finalaggafter`, `finalaggbefore`, `temporaryoutputimport`, `unitconvertafter` — identical) |
| 1 | 1 551 | 993 201 | 3 498 | 1 108 107 | `...movesexecution1ccc0232...__sourcetypeagedistribution.ageFraction` |
| 1 | 4 | 4 | 0 | 4 | `...movesexecution1ccc0236...__dioxinemissionrate.meanBaseRate` |
| 2 | 316 | 316 | 0 | 316 | `...movesexecution1ccc0236...__nrdioxinemissionrate.meanBaseRate` |
| 2 | 272 | 270 | 20 | 11 193 | `...movesexecution1ccc0232...__travelfraction.fraction` |
| 2 | 113 | 103 | 250 | 3 245 | `...__sbweightedemissionrate.meanBaseRate` / `.meanBaseRateIM` |
| 4 | 344 | 321 | 0 | 19 648 | `...__baserate_2_2020.opModeFraction` / `.opModeFractionRate` |
| 4 | 77 | 73 | 950 | 9 474 | `...movesworker1ccc0232...__baserateoutput.emissionRate` |
| 4 | 30 | 27 | 320 | 21 068 | `db__out_nr_airport_support_county__movesoutput.emissionQuant` (+4 sibling tables) |
| 5 | 162 | 148 | 960 | 5 952 | `...__baseratebyage_2_2020.meanBaseRateIM` / `.emissionRateIM` |

`nrdioxinemissionrate.meanBaseRate` is the cleanest illustration: **all 316
cells** are quantum-limited, the column holds exactly two distinct values,
and their stored forms are `0.000000001105` (4 significant digits) and
`0.000000000019` (2). Verified directly:

```sh
python3 -c "
import pyarrow.parquet as pq
p='characterization/snapshots/nr-airtoxics-lawn-garden-county/tables/db__movesexecution1ccc0236_campuscluster_illinois_edu__nrdioxinemissionrate.parquet'
b=next(pq.ParquetFile(p).iter_batches(batch_size=400)).to_pydict()
print(sorted(set(zip(b['pollutantID'], b['meanBaseRate']))))"
# [(131, '0.000000001105'), (142, '0.000000000019')]
```

## Per-fixture

Every populated fixture is affected; the NONROAD county fixtures worst.

| fixture | float cells | quantum-limited | stored zero |
|---------|------------:|----------------:|------------:|
| nr-airtoxics-lawn-garden-county | 3 778 748 | 1 532 186 | 651 463 |
| nr-airport-support-county | 4 345 405 | 1 523 269 | 703 192 |
| nr-mixed-nonroad | 5 144 431 | 1 523 150 | 831 696 |
| nr-industrial-county | 4 319 064 | 1 523 139 | 702 792 |
| nr-logging-county | 5 068 123 | 1 523 132 | 840 262 |
| nr-recreational-county | 5 049 224 | 1 523 128 | 823 400 |
| nr-lawn-garden-county | 5 056 106 | 1 523 123 | 824 336 |
| expand-sourcetype | 1 134 081 | 158 417 | 163 104 |
| nr-construction-state | 2 884 101 | 115 082 | 495 893 |
| nr-agriculture-state | 2 147 185 | 115 081 | 375 687 |
| ... (remaining 30 fixtures in the JSON) | | | |
| process-crankcase-start-single | 485 281 | 57 192 | 114 300 |

## Verdict

The defect is real and corpus-wide, but its severity is heavily
concentrated. Ninety-three percent of cells lose *some* f64 precision;
only 0.03% lose enough to break a 1e-6 relative comparison, and those
0.03% are exactly the small-magnitude rate and fraction columns the
NONROAD air-toxics chain depends on. A significant-digit encoding removes
the whole class at no cost to the determinism contract.
