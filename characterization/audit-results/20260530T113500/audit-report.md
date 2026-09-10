# moves.rs Audit Report — 20260530T113500

## Summary

| Fixture | Canonical rows | moves.rs rows | Row ratio | Pollutants compared | Max abs delta | Max pct diff | Canonical wall (s) | moves.rs wall (s) | Speedup | moves.rs peak mem (MiB) |
|---|---|---|---|---|---|---|---|---|---|---|
| nr-commercial-nation | 908 | 908 | 1 | 4 | 9243265.826048 | 0.3% | N/A | 1.1 | N/A | 261.2 |

## Per-fixture details

### nr-commercial-nation

Canonical wall: N/A s | moves.rs wall: 1.1 s | Speedup: N/A
Canonical peak: N/A | moves.rs peak: 261.2 MiB
Canonical rows: 908 | moves.rs rows: 908 | Row ratio: 1.00

| pollutantID | name | canonical sum | moves.rs sum | delta | pct diff |
|---|---|---|---|---|---|
| 1 | Total Gaseous Hydrocarbons | 1.414054e8 | 1.413519e8 | -5.346662e4 | -0.0% |
| 2 | Carbon Monoxide (CO) | 6.507692e9 | 6.498448e9 | -9.243266e6 | -0.1% |
| 3 | Oxides of Nitrogen (NOx) | 4.947104e7 | 4.964411e7 | 1.730701e5 | 0.3% |
| 100 | MSAT Unspeciated HC | 7.818428e6 | 7.818635e6 | 2.077091e2 | 0.0% |

