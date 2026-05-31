# moves.rs NONROAD Audit Report — Task 142

## Summary

| Fixture | Canonical rows | moves.rs rows | Row ratio | Pollutants compared | Max pct diff |
|---|---|---|---|---|---|
| nr-commercial-nation | 908 | 908 | 1.00 | 4 | +0.3% (NOx) |

## Verification: nr-commercial-nation at HEAD

Commit: 966a9ce (Merge pull request #6 — fix/snapshot-criteria-toxics)

**Result: PASS — 908 rows, all pollutants within 0.3% of canonical.**

| pollutantID | name | canonical sum | moves.rs sum | pct diff |
|---|---|---|---|---|
| 1 | Total Gaseous Hydrocarbons | 1.414e+08 | 1.414e+08 | -0.0% |
| 2 | Carbon Monoxide (CO) | 6.508e+09 | 6.498e+09 | -0.1% |
| 3 | Oxides of Nitrogen (NOx) | 4.947e+07 | 4.964e+07 | +0.3% |
| 100 | Primary Exhaust PM10 | 7.818e+06 | 7.819e+06 | +0.0% |

Row count: 908 / 908 (ratio = 1.00)
Process: 1 (Running Exhaust) only — the fixture selects no evaporative processes.

## Evaporative Path Audit

### Infrastructure status

| Component | Status | Notes |
|---|---|---|
| `nrevapemissionrate` table loading | ✅ Fixed (this task) | Added to `NONROAD_INPUT_TABLES`; 718 rows available in snapshot |
| `nrretrofitfactors` table loading | ✅ Fixed (this task) | Added to `NONROAD_INPUT_TABLES`; 0 rows in this fixture |
| Evap tech entries (EvapTechEntry) | ⚠️ Zero-fraction stubs | Mirrored from exhaust entries; zero fractions prevent evap loop from calling `compute_evap_iteration` |
| `compute_evap_iteration` (CountyAdapter) | ⚠️ `todo!()` | Requires `nrevapemissionrate` wiring + spillage data loader |
| `nrengtechfraction` processGroupID=2 | ⚠️ Not yet loaded | 648 rows available; would feed real evap tech fractions |

### Why zero-fraction stubs are required

`process.rs` skips the **entire record** (both exhaust AND evap) when
`find_evap_tech(scc, hp_avg)` returns `None`. The evap entry must exist for
every (SCC, hp_avg) that appears in the population — mirroring from exhaust
entries achieves this. Zero fractions ensure the `evtchfrc <= 0.0 → continue`
guard skips all evap tech iterations, so `compute_evap_iteration` (which has
`todo!()`) is never called.

### Path to full evap

To enable evaporative emission computation:
1. Implement `CountyAdapter::compute_evap_iteration` in `executor.rs`
2. Load evap tech fractions from `nrengtechfraction` (processGroupID=2)  
3. Load evap emission factors from `nrevapemissionrate`
4. Load spillage/refueling records (needed for tank/hose/diurnal branches)
5. Test with a fixture that includes evaporative processes (processID 18-21, 30-32)

### Tables audited

| Table | rows | In NONROAD_INPUT_TABLES | Used in loader |
|---|---|---|---|
| nremissionrate | 55471 | ✅ | ✅ exhaust EF |
| nrdeterioration | 424 | ✅ | ✅ deterioration |
| nrengtechfraction | 9554 | ✅ | ✅ exhaust tech (pg=1); evap tech (pg=2) not yet loaded |
| nrsourceusetype | (varies) | ✅ | ✅ population |
| nrbaseyearequippopulation | (varies) | ✅ | ✅ base year |
| nrmonthallocation | 840 | ✅ | ✅ temporal |
| nrdayallocation | (varies) | ✅ | ✅ temporal |
| nrhourallocation | (varies) | ✅ | ✅ ambient temp |
| nrhourallocpattern | (varies) | ✅ | ✅ ambient temp |
| nrhourpatternfinder | (varies) | ✅ | ✅ ambient temp |
| nrgrowthindex | (varies) | ✅ | ✅ growth records |
| nrgrowthpattern | (varies) | ✅ | ✅ growth |
| nrgrowthpatternfinder | (varies) | ✅ | ✅ growth xref |
| nrscrappagecurve | (varies) | ✅ | ✅ scrappage |
| nrfuelsupply | (varies) | ✅ | ✅ oxygenate |
| fuelformulation | (varies) | ✅ | ✅ oxygenate |
| zonemonthhour | (varies) | ✅ | ✅ ambient temp |
| nrevapemissionrate | 718 | ✅ Fixed | ⚠️ not yet wired into evap_tech_entries |
| nrretrofitfactors | 0 | ✅ Fixed | ⚠️ not yet wired into retrofit_records |

## Conclusion

**Task 142 acceptance criteria MET:**
- `nr-commercial-nation` produces 908 non-empty rows matching canonical within 0.3%
- Evaporative path audited: zero-fraction stubs correctly prevent `todo!()` panic
- Missing tables added to NONROAD_INPUT_TABLES for future evap wiring
