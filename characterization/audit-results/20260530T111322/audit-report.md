# moves.rs Audit Report — 20260530T111322

## Summary

| Fixture | Canonical rows | moves.rs rows | Row ratio | Pollutants compared | Max abs delta | Max pct diff | Canonical wall (s) | moves.rs wall (s) | Speedup | moves.rs peak mem (MiB) |
|---|---|---|---|---|---|---|---|---|---|---|
| nr-commercial-nation | 908 | 908 | 1 | 4 | 77547968.728451 | 2.8% | N/A | 2.8 | N/A | 259.9 |

## Per-fixture details

### nr-commercial-nation

Canonical wall: N/A s | moves.rs wall: 2.8 s | Speedup: N/A
Canonical peak: N/A | moves.rs peak: 259.9 MiB
Canonical rows: 908 | moves.rs rows: 908 | Row ratio: 1.00

| pollutantID | name | canonical sum | moves.rs sum | delta | pct diff |
|---|---|---|---|---|---|
| 1 | Total Gaseous Hydrocarbons | 1.414054e8 | 1.409017e8 | -5.037334e5 | -0.4% |
| 2 | Carbon Monoxide (CO) | 6.507692e9 | 6.430144e9 | -7.754797e7 | -1.2% |
| 3 | Oxides of Nitrogen (NOx) | 4.947104e7 | 5.087152e7 | 1.400483e6 | 2.8% |
| 100 | MSAT Unspeciated HC | 7.818428e6 | 7.818635e6 | 2.077091e2 | 0.0% |

