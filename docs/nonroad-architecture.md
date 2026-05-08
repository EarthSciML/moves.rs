# NONROAD Architecture Map

> Phase 5, Task 91 — Design document for the `moves-nonroad` Rust crate.
> Maps the 29k-line NONROAD2008a Fortran codebase to eight Rust modules.

## Module Map

| Fortran group | Rust module | Source files | Est. LOC |
|---|---|---|---|
| Main driver & process loop | `moves-nonroad::main` | `nonroad.f`, `dayloop.f`, `daymthf.f`, `dispit.f` | ~3,500 |
| Geography processing | `moves-nonroad::geography` | `prccty.f`, `prcsta.f`, `prcsub.f`, `prcus.f`, `prc1st.f`, `prcnat.f` | ~5,156 |
| Population, growth, age | `moves-nonroad::population` | `getpop.f`, `getgrw.f`, `grwfac.f`, `agedist.f`, `modyr.f` | ~1,400 |
| Emission factor lookup/calculation | `moves-nonroad::emissions` | `clcems.f`, `clcevems.f`, `emfclc.f`, `evemfclc.f`, `emsadj.f`, `clcrtrft.f`, `unitcf.f`, `intadj.f` | ~3,000 |
| Allocation & spatial apportionment | `moves-nonroad::allocation` | `alocty.f`, `alosta.f`, `alosub.f` | ~530 |
| Input file parsers | `moves-nonroad::input` | ~30 `rd*.f` files | ~7,000 |
| Output writers & utilities | `moves-nonroad::output` | ~50 small files (`wrt*.f`, `fnd*.f`, `chk*.f`, string utils) | ~6,000 |
| COMMON-block replacement | `moves-nonroad::common` | 11 `.inc` files, 65 COMMON blocks | ~2,433 |

## Array-Size Policy

The Fortran source uses fixed-dimension `PARAMETER` constants in `nonrdprm.inc`:

```
MXEQIP=25   ! equipment categories
MXPOL=23    ! pollutants
NSTATE=53   ! states
NCNTY=3400  ! counties
MXTECH=15   ! technology types
MXHPC=18    ! horsepower categories
MXAGYR=51   ! model-year ages
MXDAYS=365  ! days
MXSUBC=300  ! sub-county regions
MXEMFC=13000 ! emission-factor records
MXDTFC=120  ! deterioration-factor records
MXPOP=1000  ! population records
```

**Rust policy:** Replace all fixed-dimension arrays with `Vec<T>`. The original limits
are captured as `pub const` documentation constants with a `_LEGACY_MAX` suffix so the
information is preserved for reference:

```rust
/// Legacy maximum equipment categories (from nonrdprm.inc MXEQIP=25).
pub const MXEQIP_LEGACY_MAX: usize = 25;
```

This removes several MOVES-side capacity workarounds that were needed when a
jurisdiction exceeded a fixed limit.

## Error-Handling Policy

Fortran pattern: integer return codes — `0` for success, nonzero for error.
Callers check with `IF (ISTAT .NE. 0) ...`.

**Rust policy:** Use `Result<T, NonroadError>` throughout. `NonroadError` is an enum
in `common` that covers:

```rust
pub enum NonroadError {
    Io(std::io::Error),
    Parse(String),
    NotFound(String),
    InvalidInput(String),
    NumericOverflow(String),
}
```

Functions that cannot fail (simple lookups, pure computations) remain infallible.
The `main` driver unwraps at the top level after printing a diagnostic.

## I/O Policy

Fortran pattern: `OPEN(UNIT=N, FILE=...)`, `READ(N, FMT)`, `WRITE(N, FMT)`.
Unit numbers are global integers.

**Rust policy:** Use idiomatic `BufRead`/`Write` traits. Parsers receive
`impl BufRead`; writers receive `impl Write`. No unit numbers, no global file state.

```rust
pub fn read_population<R: BufRead>(reader: R) -> Result<Vec<PopulationRecord>, NonroadError>;
pub fn write_emissions<W: Write>(writer: W, emissions: &[EmissionRecord]) -> Result<(), NonroadError>;
```

## WASM Compatibility (Mandatory)

The crate must compile to `wasm32-unknown-unknown` without `cfg(target_arch = "wasm32")`
gates in the runtime path.

**What is forbidden in runtime code:**
- `std::process` — no subprocess invocation
- `std::os` platform-specific modules
- Fortran FFI via `cc-rs` + `gfortran` (gfortran does not target wasm32)
- `std::net`, `std::thread` (not needed by NONROAD's batch architecture)

**What is allowed (guarded by cfg in utility code):**
- Platform-specific `std::fs` for native file I/O — wrapped behind `impl Read`/`impl Write`
  so the WASM target supplies a stub or in-memory reader/writer

The binary entry point (`main`) is native-only. The library exposes
`pub fn run_simulation(opts: &NonroadOptions) -> NonroadOutputs` that is WASM-safe.

## COMMON-Block Replacement

The 65 COMMON blocks in 11 `.inc` files become typed Rust structs in `common`,
owned by a top-level `NonroadContext`:

```rust
pub struct NonroadContext {
    pub equipment: EquipmentState,
    pub pollutants: PollutantState,
    pub geography: GeographyState,
    pub population: PopulationState,
    pub growth: GrowthState,
    pub age_dist: AgeDistributionState,
    pub emission_factors: EmissionFactorState,
    pub allocation: AllocationState,
    pub output: OutputState,
    pub temporal: TemporalState,
}
```

Each `*State` struct replaces one `.inc` file's COMMON blocks. Parameters from
`nonrdprm.inc` become `pub const` items. Context is passed explicitly between
modules — no global mutable state.

## Data Flow

```
NonroadOptions
     │
     ▼
┌─────────────┐     ┌──────────────┐
│   input     │────▶│   common     │◀────────┐
│  (parsers)  │     │ (NonroadCtx) │         │
└─────────────┘     └──────┬───────┘         │
                           │                  │
              ┌────────────┼────────────┐     │
              ▼            ▼            ▼     │
         ┌─────────┐ ┌──────────┐ ┌────────┐ │
         │popul'n  │ │geography │ │alloctn │ │
         └────┬────┘ └────┬─────┘ └───┬────┘ │
              │           │           │      │
              ▼           ▼           ▼      │
         ┌────────────────────────────────┐  │
         │          emissions             │  │
         └──────────────┬─────────────────┘  │
                        ▼                    │
         ┌────────────────────────────────┐  │
         │           output               │  │
         │   (writers + aggregation)      │──┘
         └────────────────────────────────┘
              │
              ▼
      NonroadOutputs
```

1. `input` parses all configuration and data files into `NonroadContext`
2. `population` computes population, growth factors, and age distributions
3. `geography` processes each SCC × geography × year combination
4. `allocation` applies spatial apportionment
5. `emissions` computes exhaust, evaporative, and retrofit emissions
6. `output` writes results (legacy text + Parquet)
7. `main` orchestrates the full sequence; `common` holds shared state

## Module Responsibilities

### `main`
- Entry point: `pub fn run_simulation(opts: &NonroadOptions) -> Result<NonroadOutputs>`
- Iterates the SCC × geography × year loop
- Calls into each module in sequence
- Binary target that parses CLI args and calls `run_simulation`

### `geography`
- Collapses the six Fortran process routines (`prccty.f` etc.) into one parameterized function
- Handles county, state, subcounty, national, and US-total variants
- Allocatable geography types: `County`, `State`, `National`, `SubCounty`

### `population`
- Population apportionment (`getpop`)
- Growth-factor application (`getgrw`, `grwfac`)
- Age-distribution and model-year fraction computation (`agedist`, `modyr`)
- Scrappage and retrofit timing

### `emissions`
- Exhaust emissions: `clcems`, `emfclc`, `emsadj`, `unitcf`
- Evaporative emissions: `clcevems`, `evemfclc`
- Retrofit emissions: `clcrtrft`, validators

### `allocation`
- County allocation: `alocty`
- State allocation: `alosta`
- Subcounty allocation: `alosub`

### `input`
- One submodule per file format: `pop`, `alo`, `grow`, `emfc`, `tech`, `season`, etc.
- Common parsing utilities in `input::util` (column-position extraction, FIPS parsing)

### `output`
- Legacy text-format writers for backward compatibility
- Parquet output for native consumption
- Summary aggregation and diagnostics

### `common`
- `NonroadContext` — top-level container for all shared state
- `NonroadError` — unified error type
- `*State` structs replacing COMMON blocks
- `const` items for former `nonrdprm.inc` parameters
- Re-exported types used by all modules (`EquipmentId`, `PollutantId`, `SccKey`, etc.)
