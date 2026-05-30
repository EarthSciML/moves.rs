# moves.rs Audit Report — 20260530T034835

## Summary

| Fixture | Canonical rows | moves.rs rows | Row ratio | Pollutants compared | Max abs delta | Max pct diff | Canonical wall (s) | moves.rs wall (s) | Speedup | moves.rs peak mem (MiB) |
|---|---|---|---|---|---|---|---|---|---|---|
| process-evap-permeation | 128 | 128 | 1 | 1 | 7e-06 | 0% | N/A | 0.8 | N/A | 78.8 |
| process-evap-leaks | 128 | 128 | 1 | 1 | 0.000153 | -0% | N/A | 0.9 | N/A | 77.3 |
| process-evap-fvv | 128 | 128 | 1 | 1 | 0.045956 | -0% | N/A | 0.7 | N/A | 93.8 |
| sample-runspec | 0 | 0 | 0 | 0 | 0 | 0% | N/A | 0.7 | N/A | 83.7 |
| expand-counties | 0 | 0 | 0 | 0 | 0 | 0% | N/A | 0.9 | N/A | 93.1 |
| expand-day | 0 | 0 | 0 | 0 | 0 | 0% | N/A | 0.7 | N/A | 89.3 |
| mixed-onroad-nonroad | 0 | 0 | 0 | 0 | 0 | 0% | N/A | 3.1 | N/A | 438.1 |
| nr-agriculture-state | 0 | 0 | 0 | 0 | 0 | 0% | N/A | 2.5 | N/A | 159.3 |
| nr-commercial-nation | 908 | 0 | 0 | 4 | 6507691599.722199 | -100% | N/A | 2.6 | N/A | 234.1 |

## Per-fixture details

### process-evap-permeation

Canonical wall: N/A s | moves.rs wall: 0.8 s | Speedup: N/A
Canonical peak: N/A | moves.rs peak: 78.8 MiB
Canonical rows: 128 | moves.rs rows: 128 | Row ratio: 1.00

| pollutantID | name | canonical sum | moves.rs sum | delta | pct diff |
|---|---|---|---|---|---|
| 1 | Total Gaseous Hydrocarbons | 3.225675e1 | 3.225675e1 | 6.765119e-6 | 0.0% |

### process-evap-leaks

Canonical wall: N/A s | moves.rs wall: 0.9 s | Speedup: N/A
Canonical peak: N/A | moves.rs peak: 77.3 MiB
Canonical rows: 128 | moves.rs rows: 128 | Row ratio: 1.00

| pollutantID | name | canonical sum | moves.rs sum | delta | pct diff |
|---|---|---|---|---|---|
| 1 | Total Gaseous Hydrocarbons | 9.600612e2 | 9.600610e2 | -1.528340e-4 | -0.0% |

### process-evap-fvv

Canonical wall: N/A s | moves.rs wall: 0.7 s | Speedup: N/A
Canonical peak: N/A | moves.rs peak: 93.8 MiB
Canonical rows: 128 | moves.rs rows: 128 | Row ratio: 1.00

| pollutantID | name | canonical sum | moves.rs sum | delta | pct diff |
|---|---|---|---|---|---|
| 1 | Total Gaseous Hydrocarbons | 5.589718e2 | 5.589259e2 | -4.595615e-2 | -0.0% |

### sample-runspec

Canonical wall: N/A s | moves.rs wall: 0.7 s | Speedup: N/A
Canonical peak: N/A | moves.rs peak: 83.7 MiB
Canonical rows: 0 | moves.rs rows: 0 | Row ratio: 0.00

| pollutantID | name | canonical sum | moves.rs sum | delta | pct diff |
|---|---|---|---|---|---|
| — | *(no emission data in either source)* | — | — | — | — |

### expand-counties

Canonical wall: N/A s | moves.rs wall: 0.9 s | Speedup: N/A
Canonical peak: N/A | moves.rs peak: 93.1 MiB
Canonical rows: 0 | moves.rs rows: 0 | Row ratio: 0.00

| pollutantID | name | canonical sum | moves.rs sum | delta | pct diff |
|---|---|---|---|---|---|
| — | *(no emission data in either source)* | — | — | — | — |

### expand-day

Canonical wall: N/A s | moves.rs wall: 0.7 s | Speedup: N/A
Canonical peak: N/A | moves.rs peak: 89.3 MiB
Canonical rows: 0 | moves.rs rows: 0 | Row ratio: 0.00

| pollutantID | name | canonical sum | moves.rs sum | delta | pct diff |
|---|---|---|---|---|---|
| — | *(no emission data in either source)* | — | — | — | — |

### mixed-onroad-nonroad

Canonical wall: N/A s | moves.rs wall: 3.1 s | Speedup: N/A
Canonical peak: N/A | moves.rs peak: 438.1 MiB
Canonical rows: 0 | moves.rs rows: 0 | Row ratio: 0.00

| pollutantID | name | canonical sum | moves.rs sum | delta | pct diff |
|---|---|---|---|---|---|
| — | *(no emission data in either source)* | — | — | — | — |

### nr-agriculture-state

Canonical wall: N/A s | moves.rs wall: 2.5 s | Speedup: N/A
Canonical peak: N/A | moves.rs peak: 159.3 MiB
Canonical rows: 0 | moves.rs rows: 0 | Row ratio: 0.00

| pollutantID | name | canonical sum | moves.rs sum | delta | pct diff |
|---|---|---|---|---|---|
| — | *(no emission data in either source)* | — | — | — | — |

### nr-commercial-nation

Canonical wall: N/A s | moves.rs wall: 2.6 s | Speedup: N/A
Canonical peak: N/A | moves.rs peak: 234.1 MiB
Canonical rows: 908 | moves.rs rows: 0 | Row ratio: 0.00

| pollutantID | name | canonical sum | moves.rs sum | delta | pct diff |
|---|---|---|---|---|---|
| 1 | Total Gaseous Hydrocarbons | 1.414054e8 | 0.000000e0 | -1.414054e8 | -100.0% |
| 2 | Carbon Monoxide (CO) | 6.507692e9 | 0.000000e0 | -6.507692e9 | -100.0% |
| 3 | Oxides of Nitrogen (NOx) | 4.947104e7 | 0.000000e0 | -4.947104e7 | -100.0% |
| 100 | MSAT Unspeciated HC | 7.818428e6 | 0.000000e0 | -7.818428e6 | -100.0% |

