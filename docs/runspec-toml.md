# RunSpec — TOML format reference

This page documents the human-friendly TOML surface for MOVES RunSpecs
introduced by Task 13. It is the recommended hand-authored format for
`moves.rs`; the legacy XML format (`<runspec>...</runspec>`, the same
`.mrs` files canonical MOVES emits) is supported for compatibility with
the characterization fixtures via the `moves-runspec` crate.

Both formats project through a single canonical
[`RunSpec`](../crates/moves-runspec/src/model.rs) model, so

```
TOML  <─ to_toml ── RunSpec ── to_xml ─>  XML
 │                    ▲                    │
 └── from_toml ───────┴── from_xml ────────┘
```

round-trips through the model are model-identical by construction
(verified by the integration tests in
`crates/moves-runspec/tests/round_trip.rs`).

The TOML form is **not** byte-equivalent to the XML form — comments,
whitespace, and the choice of named enums vs. legacy ALL-CAPS strings
differ — but every byte of *information* is preserved.

## Design rules

1. **Short table names.** The XML `<geographicselections>` becomes
   `[[geo]]`; `<onroadvehicleselections>` becomes `[[onroad]]`;
   `<pollutantprocessassociations>` becomes `[[pollutant_process]]`.
2. **Named-enum values.** Where the XML uses opaque strings (`MACROSCALE`,
   `COUNTY`) or numeric IDs for closed sets, the TOML uses kebab-case
   slugs (`macro`, `county`).
3. **Open-set IDs keep both forms.** Pollutant, process, fuel-type,
   source-type, sector, and road-type IDs come from the MOVES default
   database and are not closed; the TOML stores `pollutant_id = 91`
   together with `pollutant = "Total Energy Consumption"` so the human
   name is preserved for round-trip stability. Task 14 (`Pollutant` /
   `EmissionProcess` / `SourceType` / `RoadType` definitional code)
   will introduce canonical enums and the `_name` companions become
   derivable.
4. **Comments allowed.** TOML's `#` comments work anywhere — useful for
   annotating fixture selections, citing source plan tasks, etc.
5. **Optional fields are omitted, not nulled.** Anything serde renders
   with `Option::is_none` (NONROAD-only output flags, the `aggregateBy`
   key, the `version` attribute, the optional `model_combination` on
   road types) is just absent from the TOML when not in use.
6. **Empty descriptions normalize to absent.** Both `description = ""`
   (TOML) and `<description></description>` (XML) load as `None` so the
   two surfaces agree on the same model value.

## Top-level keys

| TOML key        | XML element/attribute        | Notes |
|-----------------|------------------------------|-------|
| `version`       | `<runspec version="...">`    | Optional. |
| `description`   | `<description>` (text/CDATA) | Optional. Empty becomes `None`. |
| `[run]`         | `<models>`, `<modelscale>`, `<modeldomain>`, `<pmsize>` | Grouping table; see below. |
| `[[geo]]`       | `<geographicselection>`      | Array of tables. |
| `[time]`        | `<timespan>`                 | |
| `[[onroad]]`    | `<onroadvehicleselection>`   | Array of tables. |
| `[[offroad]]`   | `<offroadvehicleselection>`  | Array of tables. |
| `[[offroad_scc]]` | `<offroadvehiclescc>`      | Empty in current fixtures. |
| `[[road_type]]` | `<roadtype>`                 | Array of tables. |
| `[[pollutant_process]]` | `<pollutantprocessassociation>` | Array of tables. |
| `[[database_selection]]` | `<databaseselection>` | Empty in current fixtures. |
| `[[internal_control_strategy]]` | `<internalcontrolstrategy>` | Empty in current fixtures. |
| `[input_db]`    | `<inputdatabase>`            | |
| `[uncertainty]` | `<uncertaintyparameters>`    | |
| `[output]`      | bag of `<output*>` elements  | See below. |

## `[run]`

| TOML key       | XML mapping                 | Value type |
|----------------|------------------------------|------------|
| `models`       | `<models><model value="X"/></models>`, repeated | `["onroad"]`, `["nonroad"]`, or both |
| `scale`        | `<modelscale value="X"/>`    | `macro` (XML `MACROSCALE`), `inventory` (XML `Inv`), `rates` (XML `Rates`) |
| `domain`       | `<modeldomain value="X"/>`   | `default` / `single` / `project`. Optional. |
| `pm_size`      | `<pmsize value="N"/>`        | unsigned integer |

## `[[geo]]`

```toml
[[geo]]
type = "county"           # nation | state | county | zone | link  (XML uppercase)
key = 26161               # FIPS code (county/state) or zone/link id
description = "MICHIGAN - Washtenaw County"
```

## `[time]`

```toml
[time]
years  = [2001]           # one entry per <year key="..."/>
months = [6]              # ...per <month/>
days   = [0]              # ...per <day/> (0 = weekday/weekend selector; MOVES convention)
begin_hour   = 6          # <beginhour key="..."/>
end_hour     = 6          # <endhour key="..."/>
aggregate_by = "Month"    # optional <aggregateBy key="..."/>
```

## `[[onroad]]` / `[[offroad]]`

```toml
[[onroad]]
fuel_id = 1
fuel = "Gasoline"
source_type_id = 21
source_type = "Passenger Car"

[[offroad]]
fuel_id = 2
fuel = "Diesel Fuel"
sector_id = 8
sector = "Airport Support"
```

XML attribute mapping:

| TOML key         | XML attribute                |
|------------------|------------------------------|
| `fuel_id`        | `fueltypeid`                 |
| `fuel`           | `fueltypedesc`               |
| `source_type_id` | `sourcetypeid`               |
| `source_type`    | `sourcetypename`             |
| `sector_id`      | `sectorid`                   |
| `sector`         | `sectorname`                 |

## `[[road_type]]`

```toml
[[road_type]]
id = 4
name = "Urban Restricted Access"
model_combination = "M1"      # optional; PROJECT-scale runs set this
```

## `[[pollutant_process]]`

```toml
[[pollutant_process]]
pollutant_id = 91
pollutant = "Total Energy Consumption"
process_id = 1
process = "Running Exhaust"
```

The XML uses `pollutantkey`/`pollutantname`/`processkey`/`processname`.

## `[input_db]` / `[output.db]` / `[output.scale_input_db]`

```toml
[input_db]
server = ""
database = ""
description = ""
```

XML maps `servername` → `server`, `databasename` → `database`.

## `[uncertainty]`

```toml
[uncertainty]
enabled = false
runs_per_simulation = 0
simulations = 0
```

XML uses `uncertaintymodeenabled`, `numberofrunspersimulation`,
`numberofsimulations`.

## `[output]`

```toml
[output]
detail = "county"             # XML <geographicoutputdetail description="..."/>
timestep = "hour"             # XML <outputtimestep value="..."/>
vmt_data = false              # XML <outputvmtdata value="..."/>

[output.db]                   # XML <outputdatabase>
server = ""
database = "out_sample"
description = ""

[output.scale_input_db]       # XML <scaleinputdatabase>
server = ""
database = ""
description = ""

[output.nonroad]              # optional; emitted only for NONROAD runs
sho = false
sh = false
shp = false
shidling = false
starts = false
population = false

[output.breakdown]            # XML <outputemissionsbreakdownselection>
model_year = true             # <modelyear selected="..."/>
fuel_type = true              # <fueltype selected="..."/>
emission_process = true       # <emissionprocess selected="..."/>
distinguish_particulates = true
onroad_offroad = true
road_type = true
source_use_type = false       # <sourceusetype selected="..."/>
moves_vehicle_type = false    # <movesvehicletype selected="..."/>
onroad_scc = true             # <onroadscc selected="..."/>
offroad_scc = false           # <offroadscc selected="..."/>
estimate_uncertainty = false
segment = false
hp_class = false              # <hpclass selected="..."/>

[output.factors.time]
enabled = true                # XML @selected
units = "seconds"             # XML "Seconds"

[output.factors.distance]
enabled = true
units = "miles"               # XML "Miles"

[output.factors.mass]
enabled = true
units = "grams"               # XML "Grams"
energy_units = "million-btu"  # XML "Million BTU"
```

## Enum slug ↔ XML literal table

| Field                | TOML slug      | XML literal      |
|----------------------|----------------|------------------|
| `run.models[]`       | `onroad`       | `ONROAD`         |
|                      | `nonroad`      | `NONROAD`        |
| `run.scale`          | `macro`        | `MACROSCALE`     |
|                      | `inventory`    | `Inv`            |
|                      | `rates`        | `Rates`          |
| `run.domain`         | `default`      | `DEFAULT`        |
|                      | `single`       | `SINGLE`         |
|                      | `project`      | `PROJECT`        |
| `geo[].type`         | `nation`       | `NATION`         |
|                      | `state`        | `STATE`          |
|                      | `county`       | `COUNTY`         |
|                      | `zone`         | `ZONE`           |
|                      | `link`         | `LINK`           |
| `output.detail`      | `nation`       | `NATION`         |
|                      | `state`        | `STATE`          |
|                      | `county`       | `COUNTY`         |
|                      | `zone`         | `ZONE`           |
|                      | `link`         | `LINK`           |
| `output.timestep`    | `hour`         | `Hour`           |
|                      | `day`          | `Day`            |
|                      | `month`        | `Month`          |
|                      | `year`         | `Year`           |
| `factors.time.units` | `seconds`      | `Seconds`        |
|                      | `minutes`      | `Minutes`        |
|                      | `hours`        | `Hours`          |
|                      | `days`         | `Days`           |
| `factors.distance.units` | `miles`    | `Miles`          |
|                      | `kilometers`   | `Kilometers`     |
| `factors.mass.units` | `pounds`       | `Pounds`         |
|                      | `kilograms`    | `Kilograms`      |
|                      | `grams`        | `Grams`          |
|                      | `tons-short`   | `Tons (short)`   |
|                      | `tons-metric`  | `Tons (metric)`  |
| `factors.mass.energy_units` | `million-btu` | `Million BTU` |
|                      | `kilo-joules`  | `KiloJoules`     |
|                      | `joules`       | `Joules`         |

## Caveats and intentional non-goals

* **Byte-identical XML round-trip is not a Task 13 promise.** The XML
  formatter emits self-closing tags where possible and preserves
  attribute *content* but not necessarily attribute order, whitespace,
  or `<![CDATA[...]]>` wrapping. Task 12 hardens the XML side; the
  Task 13 contract is model-equivalence.
* **Empty containers serialize as omitted in TOML, present in XML.**
  TOML omits empty `[[geo]]`/`[[onroad]]`/etc. arrays entirely; the
  XML emits empty wrapper elements (`<geographicselections></...>`)
  because that's what canonical MOVES does and Task 12 will need them
  for byte-stable re-serialization.
* **Pollutant/process names are user input today.** They round-trip
  through both formats but aren't validated against the MOVES
  defaultdb until Task 14 lands. Mistyping `pollutant = "CO"` on a
  process where the official name is `Carbon Monoxide (CO)` will
  load fine and serialize back with whatever string was provided.
