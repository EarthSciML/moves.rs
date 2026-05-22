# Calculator / Generator Data-Flow Audit

**Purpose**: Map every Calculator/Generator implementation to one of five
input/output shape patterns (buckets A–E) and record the kernel function,
`*Inputs` field list, and output row type for each, sized for the per-batch
wiring tasks below.

Sources read: `crates/moves-calculators/src/calculators/**/*.rs` and
`crates/moves-calculators/src/generators/**/*.rs`.

---

## Bucket Definitions

| Bucket | Pattern | Wiring implication |
|--------|---------|---------------------|
| **A** | Simple chained or direct-subscriber calculator — one `*Inputs` struct, one `calculate`/`run` kernel, returns `Vec<OutputRow>`. Chains off one upstream or subscribes directly to a single master-loop event. | Same wiring adapter for all; swap Inputs loader per struct. |
| **B** | Multi-process chained — running-exhaust and start-exhaust have **separate** kernel entry points (`calculate_running`/`calculate_start` or separate `*Running`/`*Start` inputs), or covers running+start+extended-idle with a single combined inputs struct. | Needs two-path wiring adapter (dispatch on process). |
| **C** | Speciation / PM transform — consumes an existing `MOVESWorkerOutput` pool or `FuelBlock`, scales or relabels into new pollutant rows. All produce `Vec<WorkerOutputRow>` (or fuel-block equivalents). | Adapter reads from scratch MWO pool, writes scaled rows back. |
| **D** | Fat multi-module calculator — multi-subdirectory layout or large single file (>500 lines) with ≥8 internal pipeline steps. Complex enough to deserve its own wiring task. | Individual wiring task per calculator. |
| **E** | Generator — writes one or more **named scratch tables** consumed downstream by calculators. Implements the `Generator` trait with `input_tables()`/`output_tables()` metadata. | Adapter populates scratch namespace from `output_tables()`. |

---

## Calculators (38 total)

### Bucket A — Simple chained calculator

All six in this bucket have the shape:
`calculate(&*Inputs) -> Vec<OutputRow>`, with a single upstream dependency or
one master-loop subscription. The data-plane adapter is uniform; only the
Inputs loader and the output table target differ.

| # | Struct | File | Kernel | Inputs struct | Inputs fields | Output row type | Notes |
|---|--------|------|--------|---------------|---------------|-----------------|-------|
| 1 | `SO2Calculator` | `calculators/so2_calculator.rs` | `calculate(&So2Inputs)` | `So2Inputs` | fuel_supply, fuel_formulation, fuel_sub_type, fuel_type (ids), year, sulfate_emission_rate, pollutant_process_assoc, run_spec_model_year, month_of_any_year, general_fuel_ratio, energy (MOVESWorkerOutput subset) | `Vec<So2EmissionRow>` | Chained off `BaseRateCalculator`; consumes TotalEnergy (p91). |
| 2 | `DistanceCalculator` | `calculators/distance_calculator.rs` | `calculate(&DistanceInputs)` | `DistanceInputs` | source_bin, source_bin_distribution, source_type_model_year, sho, hour_day, link, county | `Vec<DistanceActivityRow>` | Direct subscriber (Running Exhaust, Year granularity). Skips roadTypeID=1. |
| 3 | `NOCalculator` | `calculators/nitrogen_oxide.rs` | `calculate(&NitrogenOxideInputs)` | `NitrogenOxideInputs` | no_no2_ratio, pollutant_process_assoc, pollutant_process_model_year, source_use_type (ids), worker_output | `Vec<MovesWorkerOutputRow>` | Chained off `BaseRateCalculator`; speciates NOx (p3) → NO (p32) + HONO (p34). |
| 4 | `NO2Calculator` | `calculators/nitrogen_oxide.rs` | `calculate(&NitrogenOxideInputs)` | `NitrogenOxideInputs` | _(same as NOCalculator)_ | `Vec<MovesWorkerOutputRow>` | Chained off `BaseRateCalculator`; speciates NOx (p3) → NO2 (p33). Same processing function as NOCalculator. |
| 5 | `Ch4N2oWtpCalculator` | `calculators/welltopump/ch4n2o.rs` | `calculate(&WtpInputs)` | `WtpInputs` | fuel_supply, fuel_formulation, fuel_sub_type, year, month_of_any_year, greet_well_to_pump | `Vec<WorkerOutputRow>` | Superseded; registrations_count=0. WTP CH4 (p5) + N2O (p6). |
| 6 | `Co2AtmosphericWtpCalculator` | `calculators/welltopump/co2_atmospheric.rs` | `calculate(&WtpInputs)` | `WtpInputs` | _(same as Ch4N2oWtp)_ | `Vec<WorkerOutputRow>` | Superseded; WTP atmospheric CO2 (p90). Non-interpolating factor build. |
| 7 | `WellToPumpProcessor` | `calculators/welltopump/total_energy.rs` | `calculate(&WtpInputs)` | `WtpInputs` | _(same as Ch4N2oWtp)_ | `Vec<WorkerOutputRow>` | Superseded; WTP Total Energy (p91). First of two WTP steps. |
| 8 | `Co2EquivalentWtpCalculator` | `calculators/welltopump/co2_equivalent.rs` | `calculate(&Co2EquivalentWtpInputs)` | `Co2EquivalentWtpInputs` | pollutant_gwp (PollutantGwpRow), worker_output | `Vec<WorkerOutputRow>` | Superseded; second WTP step; GWP-weighted sum of CO2/CH4/N2O → CO2 Equivalent (p98). |

---

### Bucket B — Multi-process chained

Each calculator covers running-exhaust and start-exhaust (and in one case
extended-idle) via separate kernel entry points or a combined inputs struct
that carries process filter fields.

| # | Struct | File | Kernel(s) | Inputs struct(s) | Key Inputs fields | Output row type | Notes |
|---|--------|------|-----------|-----------------|-------------------|-----------------|-------|
| 9 | `Ch4N2oRunningStartCalculator` | `calculators/ch4n2o_running_start.rs` | `calculate_running(&RunningExhaustInputs)` + `calculate_start(&StartExhaustInputs)` | `RunningExhaustInputs` / `StartExhaustInputs` | Running: sho, hour_day, link, county, source_bin, source_bin_distribution, source_type_model_year, emission_rate, pollutant_process_assoc; Start: starts, hour_day, link, county, zone, source_bin, source_bin_distribution, source_type_model_year, emission_rate, pollutant_process_assoc, emission_process | `Vec<EmissionRow>` | Superseded (registrations_count=0); N2O (p6) running+start. |
| 10 | `Nh3RunningCalculator` | `calculators/nh3/running.rs` | `fn run` on `RunningInputs` | `RunningInputs` | sho, hour_day, link, county, source_bin, source_bin_distribution, source_type_model_year, emission_rate, pollutant_process_assoc, op_mode_distribution, im_coverage, im_factor, age_category | `Vec<EmissionRow>` | Superseded (registrations_count=0); NH3 running exhaust. |
| 11 | `Nh3StartCalculator` | `calculators/nh3/start.rs` | `fn run` on `StartInputs` | `StartInputs` | starts, hour_day, link, county, zone, source_bin, source_bin_distribution, source_type_model_year, emission_rate_by_age, pollutant_process_assoc, op_mode_distribution, im_coverage, im_factor, age_category | `Vec<EmissionRow>` | Superseded (registrations_count=0); NH3 start exhaust. |
| 12 | `CrankcaseEmissionCalculatorNonPM` | `calculators/crankcase_emission.rs` | `calculate(&CrankcaseInputs, produce_sulfate_pm10=false)` | `CrankcaseInputs` | worker_output, crankcase_pollutant_process_assoc, crankcase_emission_ratio | `Vec<MovesWorkerOutputRow>` | Chained off 6 upstream calculators; 180 registrations (60 non-PM pollutants × 3 crankcase processes). |
| 13 | `CrankcaseEmissionCalculatorPM` | `calculators/crankcase_emission.rs` | `calculate(&CrankcaseInputs, produce_sulfate_pm10=true)` | `CrankcaseInputs` | _(same as NonPM)_ | `Vec<MovesWorkerOutputRow>` | Superseded (registrations_count=0); PM variant with SulfatePM10 relabel step. |
| 14 | `BasicRunningPmEmissionCalculator` | `calculators/pmexhaust/running.rs` | `BasicPm25Calculator::run(&BasicRunningPmInputs, &RunContext)` | `BasicRunningPmInputs` | op_mode_distribution, emission_rate_by_age, source_bin_distribution, age_category, source_type_model_year, pollutant_process_model_year, source_bin, sho, hour_day, county, link, fuel_supply, fuel_formulation, fuel_sub_type, month_of_any_year, year, run_spec_source_type, general_fuel_ratio, pollutant_process_assoc, temperature_adjustment, zone_month_hour | `Vec<MovesWorkerOutputRow>` | Direct subscriber (Running Exhaust); PM2.5 EC (p112) + composite non-EC (p118). |
| 15 | `BasicStartPmEmissionCalculator` | `calculators/basicstartpm.rs` | `run(&BasicStartPmInputs, &RunConstants)` | `BasicStartPmInputs` | op_mode_distribution, emission_rate_by_age, source_bin_distribution, age_category, source_type_model_year, pollutant_process_model_year, source_bin, starts, hour_day, zone_month_hour, pollutant_process_mapped_model_year, start_temp_adjustment, pollutant_process_assoc, general_fuel_ratio | `Vec<WorkerOutputRow>` | Start Exhaust PM2.5 components. |
| 16 | `CO2AERunningStartExtendedIdleCalculator` | `calculators/co2ae_running_start_extended_idle.rs` | `calculate(&Co2aeInputs)` | `Co2aeInputs` | fuel_supply, fuel_formulation, fuel_subtype, year, month_of_any_year, co2_eq_pollutant, worker_output, step1a_process_ids (Vec\<i32\>), step2_process_ids (Vec\<i32\>) | `Co2aeOutput` (step1a_rows + step2_rows) | Atmospheric CO2 (p90) for running/start/extended-idle; step1a scales TotalEnergy; step2 sums GHG. |

---

### Bucket C — Speciation / PM transform

All consume existing `MOVESWorkerOutput` rows (or FuelBlock wrappers) and emit
scaled or relabelled output into the same table. Output type is
`Vec<WorkerOutputRow>` or `Vec<SpeciatedFuelBlock>` / `Vec<ToxicFuelBlock>`.

| # | Struct | File | Kernel | Inputs type | Key input fields | Output type | Notes |
|---|--------|------|--------|-------------|-----------------|-------------|-------|
| 17 | `HCSpeciationCalculator` | `calculators/hcspeciation.rs` | `HcSpeciation::speciate_block(&FuelBlock, &MethaneThcRatioIndex, &HcSpeciationIndex)` | `FuelBlock` + lookup tables built from `methaneTHCRatio` + `HCSpeciation` | FuelBlock: key (pollutant_id, process_id, source_type_id, reg_class_id, fuel_type_id, model_year_id, fuel_sub_type_id), emissions; E10 alt-THC path for ethanol 2001+ | `Vec<SpeciatedFuelBlock>` | Speciates THC (p1) → CH4 (p5), NMHC (p79), NMOG (p80), TOG (p86), VOC (p87). Port follows Go worker. |
| 18 | `TOGSpeciationCalculator` | `calculators/togspeciation.rs` | `calculate(&TogInputs)` | `TogInputs` | integrated_species_set (Vec\<IntegratedSpeciesRow\>), worker_output (Vec\<WorkerOutputRow\>) | `Vec<WorkerOutputRow>` | Chained off AirToxics+Crankcase+HCSpeciation; produces NonHAPTOG (p88) residual = NMOG − Σ integrated-species, clamped ≥0. |
| 19 | `NrHcSpeciationCalculator` | `calculators/nrhcspeciation.rs` | `NrHcSpeciation::speciate_block(&FuelBlock, &NrHcSpeciationIndex)` | `FuelBlock` + lookup tables from `nrHCSpeciation` | Same FuelBlock shape; no model-year dimension, no oxygenate term, no E10 path | `Vec<SpeciatedFuelBlock>` | Nonroad HC speciation; simpler than onroad (no modelYear key, no oxygenate). |
| 20 | `AirToxicsCalculator` | `calculators/airtoxics.rs` | `AirToxics::air_toxics_block(&FuelBlock, ModuleFlags)` | `FuelBlock` + `AirToxicsExtracts` (built once per run) | AirToxicsExtracts: minor_hap_ratio, pah_gas_ratio, pah_particle_ratio, at_ratio_gas1_chained_to, at_ratio_gas2_chained_to, at_ratio_non_gas_chained_to, at_ratio, at_ratio_gas2, at_ratio_non_gas | `Vec<ToxicFuelBlock>` | Onroad air toxics; six ratio paths applied per FuelBlock. Port follows Go worker. |
| 21 | `NrAirToxicsCalculator` | `calculators/nrairtoxics.rs` | `NrAirToxics::air_toxics_block(&FuelBlock, tables)` | `FuelBlock` + nonroad ratio tables | Nonroad `atRatio` / `nrHCSpeciation` lookup tables | `Vec<ToxicFuelBlock>` | Nonroad air toxics; same per-FuelBlock pattern, different table schema. |
| 22 | `PM10EmissionCalculator` | `calculators/pm10.rs` | `compute_pm10(inputs, &[(PM2.5 p110, PM10 p100)])` | `Pm10Inputs` (internal) | pm10_pollutant_process_assoc, pm10_emission_ratio, worker_output | `Vec<MovesWorkerOutputRow>` | Chained off `SulfatePMCalculator`; scales Total PM2.5 (p110) → Total PM10 (p100). |
| 23 | `PM10BrakeTireCalculator` | `calculators/pm10.rs` | `compute_pm10(inputs, &[(p116,p106),(p117,p107)])` | `Pm10Inputs` (internal) | same | `Vec<MovesWorkerOutputRow>` | Chained off `BaseRateCalculator`; scales Brakewear/Tirewear PM2.5 → PM10. |
| 24 | `SulfatePMCalculator` | `calculators/sulfate_pm_calculator.rs` | `calculate(&SulfatePmInputs)` | `SulfatePmInputs` | worker_output, fuel_supply, fuel_formulation, fuel_sub_type, year, sulfate_emission_rate, pollutant_process_assoc, run_spec_model_year, month_of_any_year, general_fuel_ratio (+ PM10/PM2.5 ratio tables) | `Vec<EmissionRow>` | Chained off `BaseRateCalculator`; produces Sulfate PM2.5 (p115), EC (p112), composite (p118), and totals. |
| 25 | `PmTotalExhaustCalculator` | `calculators/pmexhaust/total.rs` | `calculate(TotalSelection, &[PmWorkerRow])` | `[PmWorkerRow]` slice + `TotalSelection` enum | PmWorkerRow: dimension columns + emission values; TotalSelection: PM25Total or PM10Total | `Vec<PmWorkerRow>` | Chained; re-labels OC/EC/sulfate component rows as PM totals (p100 or p110). |
| 26 | `BasicBrakeWearPmEmissionCalculator` | `calculators/basicbraketirepm.rs` | `BasicPm25Calculator::run(&BasicPm25Inputs, &RunConstants)` | `BasicPm25Inputs` | op_mode_distribution, emission_rate, source_bin_distribution, age_category, source_type_model_year, pollutant_process_model_year, source_bin, sho, hour_day, fuel_supply, fuel_formulation, fuel_sub_type, month_of_any_year, year, run_spec_source_type, general_fuel_ratio, pollutant_process_assoc | `Vec<WorkerOutputRow>` | Brakewear PM2.5; delegates shared kernel to `BasicPm25Calculator`. |
| 27 | `BasicTireWearPmEmissionCalculator` | `calculators/basicbraketirepm.rs` | `BasicPm25Calculator::run(&BasicPm25Inputs, &RunConstants)` | `BasicPm25Inputs` | _(same as brakewear)_ | `Vec<WorkerOutputRow>` | Tirewear PM2.5; same shared kernel, different input extract. |
| 28 | `AirToxicsDistanceCalculator` | `calculators/airtoxicsdistance.rs` | `AirToxicsDistanceCalculator::run(&AirToxicsDistanceInputs)` | `AirToxicsDistanceInputs` | source_bin_distribution, source_bin, source_type_model_year, sho, hour_day, link, county, dioxin_emission_rate, metal_emission_rate | `Vec<WorkerOutputRow>` | Distance-based air toxics (dioxins, metals); uses distance-fraction join like `DistanceCalculator`. |

---

### Bucket D — Fat multi-module calculator

These are too complex for a shared wiring adapter. Each merits a dedicated
wiring task. Multi-subdir calculators have their numerical core in a
`model`/`setup`/`adjust`/`aggregate` layout; large single-file calculators
have ≥8 labelled pipeline steps.

| # | Struct | Location | Kernel entry point | Inputs struct | # input fields | Output type | Notes |
|---|--------|----------|--------------------|---------------|----------------|-------------|-------|
| 29 | `BaseRateCalculator` | `calculators/baseratecalculator/` (4 submodules) | `BaseRateCalculator::run(&BaseRateCalculatorInputs)` | `BaseRateCalculatorInputs` | ~25 (BaseRate, BaseRateByAge, + ~20 adjustment lookup tables from `StartSetup`) | `BaseRateCalculatorOutput` | Port of 1694-line Go worker; rates-first methodology; feeds all downstream chained calculators. |
| 30 | `ActivityCalculator` | `calculators/activitycalculator/` (6 submodules) | `ActivityCalculator::run(&ActivityInputs, &ActivityConfig)` | `ActivityInputs` | ~30 (8 activity types: SHO, SourceHours, Starts, Population, hotellingHours, SHP, ExtendedIdleHours, ONI; each with its own sub-inputs) | `Vec<ActivityRow>` | Port of 964-line SQL; WithRegClassID path only; 8 activityTypeIDs. |
| 31 | `CriteriaRunningCalculator` | `calculators/criteria_running_calculator.rs` | `calculate(&CriteriaRunningInputs, &RunContext)` | `CriteriaRunningInputs` | criteria_ratio, county, criteria_running_temperature_adjustment, zone_month_hour, model_year, ac_on, source_type_model_year, source_bin_distribution, emission_rate, pollutant_process_mapped_model_year, fuel_supply, fuel_formulation, fuel_sub_type, month_of_any_year, year, source_bin, hour_day, link, sho, inspection_maintenance | `Vec<WorkerOutputRow>` | Superseded; THC/CO/NOx running exhaust; 9-step CREC pipeline. |
| 32 | `CriteriaStartCalculator` | `calculators/criteria_start_calculator.rs` | `calculate(&CriteriaStartInputs, &RunContext)` | `CriteriaStartInputs` | criteria_ratio, county, zone_month_hour, source_type_model_year, source_bin_distribution, emission_rate, pollutant_process_mapped_model_year, fuel_supply, fuel_formulation, fuel_sub_type, month_of_any_year, year, source_bin, starts, pollutant_process_assoc, inspection_maintenance | `Vec<WorkerOutputRow>` | Superseded; THC/CO/NOx start exhaust; 8-step CSEC pipeline. |
| 33 | `MultidayTankVaporVentingCalculator` | `calculators/multiday_tank_vapor_venting_calculator.rs` | `calculate(&MultidayTankVaporVentingInputs)` | `MultidayTankVaporVentingInputs` | ~20 (tank temperature, fuel properties, meteorology, source type activity, operating mode, soak shedding, permeation rate tables) | `Vec<EvaporativeEmissionRow>` | Largest evaporative file (~2741 lines); multi-day diurnal + hot-soak + running-loss pipeline. |
| 34 | `EvaporativePermeationCalculator` | `calculators/evaporative_permeation_calculator.rs` | `calculate(&EvaporativePermeationInputs)` | `EvaporativePermeationInputs` | ~18 (fuel properties, source type, soak activity fractions, permeation rates, op mode, temperature) | `Vec<EvaporativeEmissionRow>` | ~1422-line file; permeation evaporative emissions for E0/E10/E85. |
| 35 | `RefuelingLossCalculator` | `calculators/refueling_loss_calculator.rs` | `calculate(&RefuelingLossInputs)` | `RefuelingLossInputs` | ~15 (refueling displacement, spillage rates, fuel properties, source type, starts, sho, age, operating mode, temperature) | `Vec<RefuelingEmissionRow>` | ~916-line file; vapor displacement + liquid spillage during refueling. |
| 36 | `TankVaporVentingCalculator` | `calculators/tank_vapor_venting_calculator.rs` | `calculate(&TankVaporVentingInputs)` | `TankVaporVentingInputs` | ~20 (tank temperature profiles, soak activity, fuel properties, diurnal/hot-soak rates, running loss, source type) | `Vec<EvaporativeEmissionRow>` | Largest single file (~1941 lines); daily diurnal + hot-soak + running-loss. |
| 37 | `LiquidLeakingCalculator` | `calculators/liquid_leaking_calculator.rs` | `calculate(&LiquidLeakingInputs)` | `LiquidLeakingInputs` | ~12 (liquid leak rates, source type, fuel properties, sho, age, temperature) | `Vec<LiquidLeakingEmissionRow>` | ~844-line file; liquid fuel leaking evaporative emissions. |
| 38 | `DummyCalculator` | `calculators/dummy.rs` | `execute(&CalculatorContext)` | _(none; no `*Inputs` struct)_ | — | `CalculatorOutput::empty()` | No-op placeholder implementing `Calculator` trait. |

---

## Generators (16 total)

All generators implement the `Generator` trait, which exposes
`input_tables()` and `output_tables()` metadata. The kernel may be an
internal free function (with unit-tested numerical core) wrapped by a
trait `execute`, or the trait `execute` directly. All output to named
scratch tables.

| # | Struct | File | Kernel | Key input tables | Output scratch tables | Notes |
|---|--------|------|--------|------------------|-----------------------|-------|
| 1 | `BaseRateGenerator` | `generators/baserategenerator/` (5 submodules) | `run(inputs: &BaseRateInputs, flags: &ExternalFlags) -> BaseRateOutput` | EmissionRateByAge, EmissionRate, SourceBinDistribution, opModeDistribution, DrivingIdleFraction, + ~10 more | `BaseRate`, `BaseRateByAge`, `DrivingIdleFraction` | Port of Go `baseratecalculator`; generates emission rates from emission-rate tables + drive-cycle data. Consumed by `BaseRateCalculator`. |
| 2 | `TotalActivityGenerator` | `generators/totalactivitygenerator/` (6 submodules) | `run(&TotalActivityInputs) -> TotalActivityOutput` | SourceTypeYear, SourceTypeAgeDistribution, SHOByAgeRoadwayHour (partial), VMT, AgeFraction | `SHOByAgeRoadwayHour`, `StartsByAgeHour`, `SHO`, `SourceHours`, `Starts`, `Population`, `hotellingHours` | Port of Java `TotalActivityGenerator`; computes SHO, starts, population, hotelling from VMT + source-type year tables. |
| 3 | `FuelEffectsGenerator` | `generators/fueleffectsgenerator/` (4 submodules) | `Generator::execute` (trait) | FuelSupply, FuelFormulation, FuelSubType, GeneralFuelRatio (raw), atRatio | `generalFuelRatio` | Evaluates fuel-adjustment expressions; output consumed by calculators that apply `generalFuelRatio`. |
| 4 | `OperatingModeDistributionGenerator` | `generators/operating_mode_distribution/` (3 submodules) | `Generator::execute` | AvgSpeedDistribution, DriveScheduleSecond, operatingMode, opModePolProcessAssoc | `OpModeDistribution` | Inventory-mode (non-rates-first) op-mode fractions from average-speed distribution + drive schedules. |
| 5 | `LinkOperatingModeDistributionGenerator` | `generators/link_op_mode_distribution.rs` | `LinkOperatingModeDistributionGenerator::run` wrapping `LinkDriveScheduleInputs<'a>` | driveScheduleSecondLink, driveScheduleAssoc, Link, opModePolProcessAssoc | `OpModeDistribution` | Project-domain op-mode fractions from second-by-second drive schedules linked to road links. |
| 6 | `EvaporativeEmissionsOperatingModeDistributionGenerator` | `generators/evap_op_mode_distribution.rs` | Generator::execute wrapping `EvapOpModeInputs<'a>` | SoakActivityFraction, sourceHours, operatingMode, ZoneMonthHour | `OpModeDistribution` | Evaporative op-mode fractions (soak duration buckets); distinct from exhaust op-mode distribution. |
| 7 | `RatesOperatingModeDistributionGenerator` | `generators/rates_op_mode_distribution.rs` | Generator::execute wrapping `OpModeFractionInputs<'a>` | opModePolProcessAssoc, avgSpeedDistribution, operatingMode, SourceBinDistribution | `RatesOpModeDistribution` | Rates-first op-mode fractions aggregated over source-bin distribution. |
| 8 | `AverageSpeedOperatingModeDistributionGenerator` | `generators/avg_speed_op_mode_distribution.rs` | Generator::execute wrapping `RatesFirstInputs<'a>` or `ProjectInputs<'a>` | avgSpeedBin, operatingMode, opModePolProcessAssoc, (Project: driveScheduleSecondLink) | `RatesOpModeDistribution` | Rates-first average-speed op-mode fractions; two input shapes for rates-first vs. project domain. |
| 9 | `StartOperatingModeDistributionGenerator` | `generators/start_operating_mode_distribution.rs` | `Generator::execute` | StartOpModeDistribution, opModePolProcessAssoc | `OpModeDistribution`, `RatesOpModeDistribution` | Start-exhaust op-mode fractions; writes to both tables. |
| 10 | `SourceTypePhysics` | `generators/sourcetypephysics.rs` | `Generator::execute` with `SourceUseTypePhysicsMapping` | sourceUseTypePhysicsMapping, RatesOpModeDistribution | `RatesOpModeDistribution` | Adjusts rates-first op-mode fractions using source-use-type physics (load, road-grade). |
| 11 | `SourceBinDistributionGenerator` | `generators/source_bin_distribution_generator.rs` | `Generator::execute` | SourceBinDistribution (raw), SourceTypeModelYear, SourceBin (raw), + AVFT tables | `SourceBin`, `SourceBinDistribution` | Builds the post-AVFT (alternate-vehicle fuel technology) source-bin distribution; output consumed by most calculators. |
| 12 | `MeteorologyGenerator` | `generators/meteorology.rs` | `Generator::execute` with `ZoneMonthHourInputs` | ZoneMonthHour, Zone, County | `ZoneMonthHour` | Populates the zone-level hourly meteorology table (temperature, humidity); updates in place. |
| 13 | `TankFuelGenerator` | `generators/tank_fuel_generator.rs` | `calculate_average_tank_gasoline(&TankFuelInputs)` + `Generator::execute` | FuelSupply, FuelFormulation, FuelSubType, regionCounty | `AverageTankGasoline` | Computes market-share-weighted average fuel properties per county/month; consumed by evaporative calculators. |
| 14 | `TankTemperatureGenerator` | `generators/tank_temperature_generator.rs` | `generate_tank_temperatures(&TankTemperatureInputs)` + `Generator::execute` | ZoneMonthHour, SampleVehicleTrip, coldSoakInitialHourFraction, AverageTankGasoline, + temperature tables | `ColdSoakTankTemperature`, `HotSoakTankTemperature`, `TankTemperatureRise`, `RunningWarmupTemperature` | Multi-table evaporative temperature output; uses Reid-vapour-pressure and soaking models. |
| 15 | `MesoscaleLookupOperatingModeDistributionGenerator` | `generators/mesoscale_lookup/op_mode_distribution.rs` | `Generator::execute` | avgSpeedBin, averageSpeedDistribution (mesoscale lookup), opModePolProcessAssoc | `OpModeDistribution` | Mesoscale-Lookup domain variant of the op-mode generator (average-speed looked up per link). |
| 16 | `MesoscaleLookupTotalActivityGenerator` | `generators/mesoscale_lookup/total_activity.rs` | `Generator::execute` | SourceTypeYear, SourceTypeAgeDistribution, avgSpeedBin, linkAvgSpeed | `SHO`, `SourceHours` | Mesoscale-Lookup total-activity variant; replaces `TotalActivityGenerator` for mesoscale-lookup runs. |

---

## Cross-reference: `*Inputs` structs → consuming Calculator

| `*Inputs` struct | Defined in | Consumed by |
|-----------------|-----------|-------------|
| `So2Inputs` | `so2_calculator.rs` | `SO2Calculator::calculate` |
| `DistanceInputs` | `distance_calculator.rs` | `DistanceCalculator::calculate` |
| `NitrogenOxideInputs` | `nitrogen_oxide.rs` | `NOCalculator::calculate`, `NO2Calculator::calculate` |
| `WtpInputs` | `welltopump/common.rs` | `WellToPumpProcessor`, `Ch4N2oWtpCalculator`, `Co2AtmosphericWtpCalculator` |
| `Co2EquivalentWtpInputs` | `welltopump/co2_equivalent.rs` | `Co2EquivalentWtpCalculator::calculate` |
| `Co2aeInputs` | `co2ae_running_start_extended_idle.rs` | `CO2AERunningStartExtendedIdleCalculator::calculate` |
| `CrankcaseInputs` | `crankcase_emission.rs` | `CrankcaseEmissionCalculatorNonPM`, `CrankcaseEmissionCalculatorPM` |
| `BasicStartPmInputs` | `basicstartpm.rs` | `BasicStartPmEmissionCalculator::run` |
| `BasicPm25Inputs` | `basicbraketirepm.rs` | `BasicBrakeWearPmEmissionCalculator`, `BasicTireWearPmEmissionCalculator` (via `BasicPm25Calculator`) |
| `BasicRunningPmInputs` | `pmexhaust/running.rs` | `BasicRunningPmEmissionCalculator` |
| `TogInputs` | `togspeciation.rs` | `TogSpeciationCalculator::calculate` |
| `AirToxicsExtracts` | `airtoxics.rs` | `AirToxics::build` → `AirToxicsCalculator::execute` |
| `AirToxicsDistanceInputs` | `airtoxicsdistance.rs` | `AirToxicsDistanceCalculator::run` |
| `SulfatePmInputs` | `sulfate_pm_calculator.rs` | `SulfatePMCalculator::calculate` |
| `CriteriaRunningInputs` | `criteria_running_calculator.rs` | `CriteriaRunningCalculator::calculate` |
| `CriteriaStartInputs` | `criteria_start_calculator.rs` | `CriteriaStartCalculator::calculate` |
| `MultidayTankVaporVentingInputs` | `multiday_tank_vapor_venting_calculator.rs` | `MultidayTankVaporVentingCalculator::calculate` |
| `EvaporativePermeationInputs` | `evaporative_permeation_calculator.rs` | `EvaporativePermeationCalculator::calculate` |
| `RefuelingLossInputs` | `refueling_loss_calculator.rs` | `RefuelingLossCalculator::calculate` |
| `TankVaporVentingInputs` | `tank_vapor_venting_calculator.rs` | `TankVaporVentingCalculator::calculate` |
| `LiquidLeakingInputs` | `liquid_leaking_calculator.rs` | `LiquidLeakingCalculator::calculate` |
| `BaseRateCalculatorInputs` | `baseratecalculator/setup.rs` | `BaseRateCalculator::run` |
| `ActivityInputs` | `activitycalculator/inputs.rs` | `ActivityCalculator::run` |

---

## Superseded calculators (registrations_count=0)

These calculators have been superseded by `BaseRateCalculator` (Task 45) or by
the new WTP data-plane approach. Their algorithms are ported for reference and
cross-validation, but they must **not** be registered at runtime.

| Struct | Superseded by |
|--------|---------------|
| `Ch4N2oRunningStartCalculator` | `BaseRateCalculator` (N2O running+start) |
| `Nh3RunningCalculator` | `BaseRateCalculator` (NH3 running) |
| `Nh3StartCalculator` | `BaseRateCalculator` (NH3 start) |
| `CrankcaseEmissionCalculatorPM` | `SulfatePMCalculator` |
| `CriteriaRunningCalculator` | `BaseRateCalculator` (THC/CO/NOx running) |
| `CriteriaStartCalculator` | `BaseRateCalculator` (THC/CO/NOx start) |
| `Ch4N2oWtpCalculator` | WTP data-plane (process 99 not registered) |
| `Co2AtmosphericWtpCalculator` | WTP data-plane |
| `WellToPumpProcessor` | WTP data-plane |
| `Co2EquivalentWtpCalculator` | WTP data-plane |
