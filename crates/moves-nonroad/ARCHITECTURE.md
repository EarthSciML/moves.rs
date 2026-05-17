# `moves-nonroad` — Architecture

Phase 5, Task 91. This document maps EPA's NONROAD2008a Fortran source
(118 `.f` files, ~29.4k lines, plus 11 `.inc` files defining 65 named
COMMON blocks) onto the modules of the `moves-nonroad` Rust crate, and
fixes the cross-cutting policies that subsequent Phase 5 tasks
(92–118) must conform to.

The companion crate skeleton (`Cargo.toml`, `src/`) is the executable
half of this design: every module described below has a stub here that
later tasks fill in. Decisions that are deferred to a later task are
called out explicitly so the boundary between "Task 91 fixes this" and
"Task N decides this" stays sharp.

---

## 1. Background

NONROAD computes nonroad-mobile-source emissions (lawn equipment,
construction equipment, recreational marine, etc.) for a US county
× equipment-category × model-year × pollutant grid, given an options
file and a set of fixed-width input files. The current production
release is `NR08a` (NONROAD2008a). MOVES does not link NONROAD as a
library: it generates a `.opt` file and the input data files, ships
them to a worker as a bundle, and invokes `nonroad.exe` as a
subprocess. Java code on either side glues in inputs and ingests the
text output. (See `gov/epa/otaq/moves/master/nonroad/` and
`gov/epa/otaq/moves/worker/framework/Nonroad{OutputDataLoader,
PostProcessor}.java`.)

This crate replaces both `nonroad.exe` *and* the worker-side bridges
with a single Rust library that exposes
`run_simulation(opts: &NonroadOptions, inputs: &NonroadInputs) ->
NonroadOutputs` (Task 117). No subprocess, no scratch files, no
MariaDB ingestion step. Output joins the unified Parquet schema from
Phase 4 Task 89.

The Fortran source is single-threaded batch code — no databases, no
threads, no subprocesses, no platform-specific I/O — so the port is
substantially a transcription. The hard parts are numerical fidelity
(see Tasks 104, 115, 116) and the 65 COMMON blocks (Task 92).

---

## 2. Source-to-module map

The Fortran source clusters into seven functional groups (per the
audit in `moves-rust-migration-plan.md` § Phase 5 / "What's actually
there"). The `moves-nonroad` crate has one Rust module per cluster,
plus a `common` module that replaces the COMMON-block global state.
Module names match the cluster role; the original Fortran filenames
appear in each module's rustdoc as a cross-reference for porters.

| Cluster                         | Files                       | Lines  | Module                  | Phase-5 task(s) |
|---------------------------------|-----------------------------|--------|-------------------------|-----------------|
| Main driver and process loop    | 4 `.f`                      | ~3,500 | `driver`                | 113             |
| Geography processing            | 6 `.f` (`prc*.f`)           | ~5,156 | `geography`             | 109–112         |
| Population, growth, age         | 5 `.f`                      | ~1,400 | `population`            | 103–104         |
| Emission-factor calculation     | 10 `.f` (`clc*.f`, …)       | ~3,000 | `emissions`             | 106–108         |
| Allocation and apportionment    | 3 `.f` (`alo*.f`)           | ~530   | `allocation`            | 105             |
| Input file parsers              | ~30 `rd*.f`                 | ~7,000 | `input`                 | 94–98           |
| Output writers + small helpers  | ~50 `wrt*.f`/`fnd*.f`/…     | ~6,000 | `output`                | 100–102, 114    |
| (COMMON-block replacement)      | 11 `.inc` (65 COMMON blocks) | ~2,433 | `common`                | 92–93           |

Total: 118 `.f` + 11 `.inc` = ~31.8k lines, all reachable from one of
the eight modules.

### 2.1 `driver` (Task 113)

Top-level orchestration: read the options file, drive the
SCC × geography × year iteration, write output records. The
production-side loop nest is six deep in places (see the geography
routines), and `driver` owns the outermost loop only — it dispatches
into `geography` for each spatial level.

| Fortran file | Lines | Role |
|---|---|---|
| `nonroad.f`  | 397 | Main entry point; argv parsing and top-level orchestration |
| `dayloop.f`  | 126 | Day-of-year loop |
| `daymthf.f`  | 194 | Month → day fractioning |
| `dispit.f`   |  50 | Iteration dispatch |
| `mspinit.f`  |   — | State-pollutant iteration init |
| `spinit.f`   |   — | Pollutant iteration init |
| `scrptime.f` | 212 | Scrappage-time accounting |

**Naming note.** The migration plan calls this module
`moves-nonroad::main`. Rust reserves `main` for the binary entry
point (`fn main()` in `src/main.rs`), so the actual module name is
`driver`. The renamed module is otherwise identical in scope.

### 2.2 `geography` (Tasks 109–112)

The bulk of the spatial-allocation logic, currently spread across six
near-duplicate "process" routines that handle different geography
levels:

| Fortran file | Lines | Role |
|---|---|---|
| `prccty.f` |   790 | County-level processing |
| `prcsta.f` | 1,034 | State-level processing |
| `prcsub.f` |   829 | Subcounty-level processing |
| `prcus.f`  |   775 | US-total processing |
| `prc1st.f` |   785 | State-from-national derivation |
| `prcnat.f` |   943 | National-level processing |

Tasks 109–111 port the routines as separate functions for fidelity.
Task 112 then refactors them into a single parameterized routine,
removing ~3,000 lines of duplication. The refactor is gated on
characterization-fixture parity (Phase 0).

### 2.3 `population` (Tasks 103–104)

Population apportionment, growth-factor application, age-distribution
and model-year fraction computation. Task 104 (age distribution and
model year) is flagged as a numerical-fidelity risk because the
algorithm uses iterative or accumulating computations sensitive to
evaluation order.

| Fortran file | Lines | Role |
|---|---|---|
| `getpop.f`  | 285 | Population apportionment |
| `getgrw.f`  | 200 | Growth retrieval |
| `grwfac.f`  | 281 | Growth-factor application |
| `agedist.f` | 193 | Age-distribution computation |
| `modyr.f`   | 216 | Model-year fraction computation |
| `getscrp.f` | 107 | Scrappage retrieval |
| (retrofit-population helpers) |   — | `cmprrtrft.f`, `srtrtrft.f`, `swaprtrft.f`, `rtrftengovrlp.f`, `initrtrft.f` |

### 2.4 `emissions` (Tasks 106–108)

Exhaust, evaporative, and retrofit-emission calculation. The
evaporative file (`clcevems.f`, 721 lines) is the largest single
source file in NONROAD; the exhaust calculator (`clcems.f`, 360
lines) is the most numerically sensitive.

| Fortran file | Lines | Role |
|---|---|---|
| `clcems.f`    | 360 | Exhaust emissions (Task 106) |
| `emfclc.f`    | 314 | Exhaust EF lookup |
| `emsadj.f`    | 343 | Emissions adjustments |
| `unitcf.f`    |  80 | Unit conversion factors |
| `intadj.f`    | 141 | Integer-adjusted EF lookup |
| `clcevems.f`  | 721 | Evaporative emissions (Task 107) |
| `evemfclc.f`  | 370 | Evaporative EF lookup |
| `clcrtrft.f`  | 309 | Retrofit emissions (Task 108) |
| (retrofit validators) |  — | `vldrtrftrecs.f`, `vldrtrfthp.f`, `vldrtrftscc.f`, `vldrtrfttchtyp.f` |

### 2.5 `allocation` (Task 105)

County, state-to-county, and subcounty allocation logic. Smallest
calculation cluster; the routines are similar in structure and may
share helpers in the Rust port.

| Fortran file | Lines | Role |
|---|---|---|
| `alocty.f` | 181 | County allocation |
| `alosta.f` | 176 | State-to-county allocation |
| `alosub.f` | 170 | Subcounty allocation |

### 2.6 `input` (Tasks 94–98)

~30 readers, one per input file format. Each reads a fixed-width or
column-aligned text format using Fortran `READ` statements with
explicit format strings.

| Task | Files |
|---|---|
| 94 (.POP, .ALO)                | `rdpop.f`, `rdalo.f` |
| 95 (.GRW, .DAT, .GXR, .DAY)    | `rdgrow.f`, `rdgxrf.f`, `rdseas.f`, `rdday.f` |
| 96 (.EMF, .TCH, evap variants) | `rdemfc.f`, `rdevemfc.f`, `rdtech.f`, `rdtech_moves.f`, `rdevtech.f`, `rdevtech_moves.f` |
| 97 (activity, deterioration, miscellany) | `rdact.f`, `rddetr.f`, `rdspil.f`, `rdsulf.f`, `rdrgndf.f`, `rdscrp.f`, `rdstg2.f`, `rdalt.f`, `rdbsfc.f`, `rdefls.f`, `rdfips.f`, `rdind.f`, `rdnropt.f`, `rdnrper.f`, `rdnrreg.f`, `rdnrsrc.f` |
| 98 (retrofit)                  | `rdrtrft.f` |

### 2.7 `output` (Tasks 100–102, 114)

Writers (`wrt*.f`), lookup helpers (`fnd*.f`), validators (`chk*.f`),
string utilities (`strlen.f` and family), and the FIPS-code
initializers (`in1fip.f`–`in5fip.f`).

| Task | Files |
|---|---|
| 100 (FIPS init)        | `in1fip.f`–`in5fip.f` |
| 101 (find/lookup)      | `fndchr.f`, `fndasc.f`, `fndact.f`, `fnddet.f`, `fndefc.f`, `fndevefc.f`, `fndevtch.f`, `fndgxf.f`, `fndhpc.f`, `fndkey.f`, `fndreg.f`, `fndrfm.f`, `fndrtrft.f`, `fndscrp.f`, `fndtch.f`, `fndtpm.f` |
| 102 (string utilities) | `strlen.f`, `strmin.f`, `lftjst.f`, `rgtjst.f`, `low2up.f`, `chrsrt.f`, `wadeeq.f`, `cnthpcat.f` |
| 114 (writers)          | `wrtams.f`, `wrtbmy.f`, `wrtdat.f`, `wrthdr.f`, `wrtmsg.f`, `wrtsi.f`, `wrtsum.f`, `hdrbmy.f`, `sitot.f`, `chkasc.f`, `chkwrn.f`, `clsnon.f`, `blknon.f` |

### 2.8 `common` (Tasks 92–93)

Replacement for the 65 named COMMON blocks declared across 11
include files. Task 92 designs the typed Rust structs that hold
this state, grouped one struct per include file (`common::*`).
Task 93 ports `nonrdprm.inc` (parameter declarations and chemical
constants) into `common::consts`.

| Include file | COMMON blocks | Role |
|---|---|---|
| `nonrdprm.inc` | (parameters)        | Array dimensions + chemical/conversion constants |
| `nonrdusr.inc` | user options        | Run-time options from the `.opt` file |
| `nonrdefc.inc` | emission factors    | Loaded `.EMF` data |
| (and 8 more)   | …                   | (see Task 92's design) |

The 65 COMMON blocks become ~10 typed sub-structs (one per include,
with `nonrdprm.inc`'s parameters folded into `const` items in
`common::consts`). All are owned by a top-level
[`NonroadContext`] (see § 6 below) passed explicitly between modules.

---

## 3. Crate layout

```text
crates/moves-nonroad/
├── ARCHITECTURE.md           ← this document
├── Cargo.toml
└── src/
    ├── lib.rs                ← module declarations and public re-exports
    ├── main.rs               ← native binary stub (Task 113 wires up the driver)
    ├── error.rs              ← Error / Result types
    ├── allocation.rs         ← cluster 5
    ├── common/
    │   ├── mod.rs            ← NonroadContext + sub-struct stubs (Task 92)
    │   └── consts.rs         ← original Fortran dimensions + chemistry consts (Task 93)
    ├── driver.rs             ← cluster 1
    ├── emissions.rs          ← cluster 4
    ├── geography.rs          ← cluster 2
    ├── input/
    │   └── mod.rs            ← cluster 6 (sub-modules added per parser as ports land)
    ├── output/
    │   └── mod.rs            ← cluster 7 (sub-modules added per writer as ports land)
    └── population.rs         ← cluster 3
```

Sub-modules (e.g. `input::pop`, `input::alo`, `output::wrtbmy`) get
added in their respective tasks. Each sub-module has rustdoc that
names the Fortran source file it ports.

---

## 4. Cross-cutting policies

These policies apply uniformly across every module. They are
**load-bearing**: tasks 92–118 conform to them; deviations need a
recorded justification in the deviating PR.

### 4.1 Array-size policy

**Fixed Fortran dimensions are replaced by `Vec` (1-D) and `ndarray`
(N-D). The original `MX*`/`N*` parameters are documentation only.**

Rationale:

* The Fortran source pre-dimensions every array to a worst-case
  upper bound: `MXEQIP=25` equipment categories, `MXPOL=23`
  pollutants, `NCNTY=3400` counties, `MXEMFC=13000` emission-factor
  records, etc. In the Rust port, capacities are determined by the
  data actually loaded — `Vec` grows as needed. This eliminates the
  MOVES-side workarounds that exist precisely because the canonical
  upper bounds are exceeded by some real input data.
* `ndarray` is the planned multi-dimensional choice (rows × cols
  for tabular data, 3-D and 4-D for geography × age × pollutant
  computations). It carries shape metadata and integrates with
  Rayon for parallelism (relevant for future Task 134).
* The original constants are preserved in `common::consts` as
  `pub const` items with rustdoc cross-references to the Fortran
  parameter that produced them. They are *not* used as actual
  array dimensions in production code; they exist for:
  - cross-referencing the Rust port against the Fortran source;
  - sanity ceilings in characterization tests
    (e.g. assert that no fixture loads more than `MXEMFC` records,
    confirming we have not silently exceeded the original
    capacity);
  - documentation of the original design's ceiling.

Practical implications:

* No `[T; MXEQIP]`-style arrays in production data structures;
  use `Vec<T>` or `ndarray::Array{1,2,3,…}<T>`.
* Capacity hints (`Vec::with_capacity(n)`) are encouraged where
  the load size is known up front (input-file row counts are
  usually announced in a header).
* When a function previously took a fixed-size `INTEGER ARR(MXEQIP)`
  parameter, its Rust counterpart takes `&[T]` (or `&mut [T]`).

### 4.2 Error-handling policy

**Fortran integer error returns become Rust `Result<T, Error>`. All
fallible operations propagate via `?`; the [`Error`] enum's variants
carry source-location and input-context information sufficient to
identify the offending file, line, or computation.**

Rationale:

* The Fortran source uses an integer convention (0 = success, !=0 =
  failure with an error-code lookup elsewhere). This is structurally
  equivalent to `Result<T, i32>` but loses information at the
  return site — Rust `Result` paired with a typed error enum keeps
  the diagnostic surface intact.
* Errors that originate in input-file parsing carry the path and
  1-based line number of the bad record (`Error::Parse`). This
  matches the format the Fortran source emits to stderr today and
  keeps end-user diagnostics intact.
* Numerical-fidelity work (Tasks 104, 115, 116) needs to detect
  non-finite values that escape past the Fortran source's silent
  acceptance; `Error::NonFinite` is the channel for those.
* `thiserror` is already a workspace dependency
  (`moves-snapshot`, `moves-fixture-capture` use it). The `Error`
  enum here adopts the same pattern.

The skeleton's [`Error`] enum has the four variants needed by the
plumbing (`Io`, `Parse`, `Config`, `NonFinite`); subsequent tasks add
domain-specific variants as they encounter fault modes that don't
fit the existing surface. New variants must continue to encode
enough context to identify the source of the fault.

### 4.3 I/O policy

**Parsers consume `std::io::BufRead`; writers accept `std::io::Write`.
Fortran-style integer unit numbers are not preserved.**

Rationale:

* `BufRead` and `Write` are the standard Rust I/O traits. They are
  generic over native (`File`, `BufReader<File>`), in-memory
  (`Cursor<&[u8]>`, `Vec<u8>`), and WASM-bridged (a `BufRead`
  wrapper around browser File-API reads) sources. This matters for:
  - WASM (no `std::fs`);
  - testing (parsers fed directly from byte slices);
  - integration with the moves-rs orchestrator
    (which can hand parsers an in-memory buffer).
* Fortran unit numbers (e.g. `OPEN(15, FILE=...)`, then
  `READ(15, ...)`) tie the reader to a single global ambient state.
  Replacing them with passed-in trait objects makes data flow
  explicit and removes the global-state coupling.
* Writers similarly take `&mut W` where `W: Write`. The two output
  formats (legacy NONROAD text + Parquet) are independent
  implementations against the same `Write` interface; the caller
  chooses which format(s) to emit.

Practical implications:

* No `OPEN`/`CLOSE` ceremony; ownership of the reader/writer is the
  caller's responsibility.
* Buffering is the caller's responsibility. Parsers accept
  pre-buffered readers (`R: BufRead`) so they don't double-buffer;
  writers accept raw `Write` because they may want to emit small
  unbuffered chunks for header/record alignment.
* Path handling stays in the orchestrating layer (`driver` opens
  files; lower-level routines see only `BufRead`/`Write`). This
  isolates `std::fs` usage to one place per binary, which keeps the
  WASM gating story simple.

### 4.4 WASM-compatibility policy

**The library compiles cleanly to `wasm32-unknown-unknown` from day
one. The runtime path uses no `std::process`, no platform-specific
`std::os::*`, and no Fortran FFI.**

Rationale:

* Phase 7 Task 133 ships `moves-nonroad` to the browser. WASM has
  no subprocess model, no Fortran toolchain (gfortran does not
  target wasm32), and no direct filesystem. Designing for WASM up
  front avoids a costly retrofit.
* The Fortran-FFI escape hatch (used in earlier drafts of the
  migration plan as a release-blocker mitigation for Task 116) is
  unavailable in WASM. This raises the bar for numerical fidelity
  but produces a cleaner long-term position. Per the plan's risk
  register, the worst-case landing is "ship NONROAD with documented
  small numerical divergences", not "ship native-only".
* The CLI binary (`src/main.rs`) is native-only — it parses argv,
  opens files, and writes to stdout — and is a separate
  compilation unit from the library. It is not part of the WASM
  build target.

Practical implications:

* Library code must not import `std::process`, `std::os::*`,
  `std::env::current_exe`, `std::env::args`, or other native-only
  surfaces. The library may use `std::env::var` (works in WASM
  with `getenv` shims, though typically returns `Err`), `std::fs`
  *only* via the orchestrating `driver` layer (which has its own
  WASM gate via the Phase 4 orchestrator), and `std::io::*`.
* Library code may use `std`. (`no_std` is not a goal —
  `wasm32-unknown-unknown` supports `std`.)
* Floating-point math comes from `core::primitive::f64` methods,
  not from a Fortran-libm shim. The native vs. WASM `libm`
  divergence surfaces in Task 115 characterization runs and is
  triaged in Task 116. The skeleton does *not* attempt to mask
  this divergence.
* Threading is single-threaded by default. Phase 7 Task 134 adds
  Rayon-on-Web-Workers; the data structures here (`Vec`,
  `ndarray`) are Rayon-friendly without redesign.
* Random number generation, time-of-day, and system-info APIs are
  not used in the runtime path. (NONROAD's calendar arithmetic
  uses inputs from the `.opt` file, not the system clock.)

A `cargo check --target wasm32-unknown-unknown -p moves-nonroad`
invocation must succeed at every checkpoint after this task. The
target is added to CI in Task 133; until then, the gate is
enforced by code review.

---

## 5. Crate dependencies

The skeleton depends only on `thiserror` (workspace dep, used by
`error.rs`). Subsequent tasks add dependencies as they need them;
the policy is to prefer workspace-pinned versions and to justify
new top-level dependencies in the PR that introduces them. Likely
additions:

| Task | Dependency | Reason |
|---|---|---|
| 92  | `bitflags` (maybe)         | Option-flag tracking, if `nonrdusr.inc` warrants it |
| 94+ | (none, hand-written)        | Parsers stay hand-written; `nom` was considered and rejected as overkill for fixed-width formats |
| 100 | `phf`                       | FIPS-code static lookup tables |
| 101 | (none — `HashMap`/`BTreeMap`) | Replaces linear-search lookup helpers |
| 113 | `ndarray`                   | Multi-dimensional state arrays |
| 114 | `arrow`, `parquet`          | Already workspace deps; reuse for the Parquet writer |
| 134 | `rayon`                     | Parallelism (gated to non-WASM until Task 134 wires up Web Workers) |

No GUI, no networking, no async runtimes. The library is pure
synchronous compute.

---

## 6. `NonroadContext` (preview of Task 92)

Replaces the implicit-global-via-COMMON pattern. Owned at the
top-level by `driver`; passed by `&` (read-only) or `&mut` to every
routine that needs run-time state. Approximate target shape (Task 92
finalizes):

```rust
pub struct NonroadContext {
    pub options: UserOptions,           // nonrdusr.inc
    pub geography: GeographyState,      // geography arrays + tables
    pub population: PopulationState,    // .POP records, growth, age
    pub emissions: EmissionsState,      // .EMF and .EVF tables
    pub technology: TechnologyState,    // .TCH / .EVTCH tables
    pub allocation: AllocationState,    // .ALO records
    pub retrofit: RetrofitState,        // .RTRFT records (if loaded)
    pub seasonal: SeasonalState,        // .DAT / .DAY records
    pub fips: FipsTables,               // FIPS lookup tables
    // (other state buckets per Task 92's audit)
}
```

The split-by-include-file rule is the starting heuristic; Task 92
reconsiders it where COMMON blocks share variables across files and
the natural Rust grouping cuts across includes. The header rule for
splits: each sub-struct should be independently constructable from
its loaded inputs (so unit tests can build a "just enough" context).

---

## 7. Integration with the moves-rs orchestrator (Task 117)

The Phase 2 orchestrator calls `moves-nonroad` directly — the
`simulation` module is the integration layer:

```rust
let outputs: NonroadOutputs = moves_nonroad::run_simulation(
    &options,        // NonroadOptions — the in-memory `.opt` file
    &inputs,         // NonroadInputs — pre-loaded population groups
    &mut geography,  // GeographyExecutor — the geography-routine seam
)?;
```

`run_simulation` walks `nonroad.f`'s two-level driver loop (the outer
`getpop` SCC-group loop and the inner record loop, on top of the
Task 113 planner), dispatches every planned record to `geography`,
and returns a `NonroadOutputs` that the orchestrator merges into the
Phase 4 unified Parquet output. No subprocess, no scratch files, no
MariaDB ingestion step.

**Signature note.** The call takes a third argument beyond the
`(&options, &inputs)` sketched in earlier drafts of this section: the
`GeographyExecutor`. NONROAD's six geography routines take four
different callback-trait families, each populated from loaded
emission-factor / technology / activity / growth / retrofit tables;
assembling those callback contexts is substantial enough to be its
own increment (the `geography` module flagged it as deferred). The
`GeographyExecutor` trait is the seam between the driver loop (Task
117, landed) and that numerical evaluation (a following increment):
`run_simulation` drives the loop and calls `GeographyExecutor::execute`
per dispatch. `PlanRecordingExecutor` is the reference implementation
— it records the dispatch plan and evaluates nothing, which makes the
driver loop fully exercised today and is also the shape of the
recording executor the numerical-fidelity harness (Tasks 115/116)
uses for port-side instrumentation.

The CLI binary (`src/main.rs`) is a thin wrapper for backwards
compatibility with the `nonroad.exe`-style invocation: it will read
inputs from disk, call `run_simulation`, and write the legacy text
output format, for parity testing against the Windows-compiled
reference (Task 115). The disk-side option-file orchestration and the
production `GeographyExecutor` are the remaining wrapper work; the
binary is not part of the runtime path used by the moves-rs
orchestrator.

---

## 8. Phase-5 task roadmap (cross-reference)

| Task | Title | Module(s) | Depends on |
|---|---|---|---|
| 91  | NONROAD architecture map and Rust crate skeleton | (this) | mo-obyw (diff harness) |
| 92  | COMMON block replacement design          | `common`              | 91 |
| 93  | Parameter and constant translation        | `common::consts`      | 92 |
| 94–98 | Input parsers                            | `input::*`            | 92, 93 |
| 99  | Initialization and option-file processing | `input::nropt`, `driver` | 92, 93 |
| 100 | FIPS code initializer                     | `output::fips`        | 92 |
| 101 | Find/lookup utility routines              | `output::find`        | 92 |
| 102 | String utilities                          | `output::strutil`     | (none) |
| 103 | Population and growth core                | `population`          | 92, 94, 95 |
| 104 | Age distribution and model year           | `population`          | 103 |
| 105 | Allocation routines                       | `allocation`          | 92, 94 |
| 106 | Exhaust emissions calculator              | `emissions`           | 92, 96, 97, 101 |
| 107 | Evaporative emissions calculator          | `emissions`           | 92, 96, 97 |
| 108 | Retrofit emission calculator              | `emissions`           | 98, 106 |
| 109–111 | Geography processing (cty/sta/sub/us/1st/nat) | `geography` | 103–108, 105 |
| 112 | Geography processing refactor             | `geography`           | 109–111 |
| 113 | Main driver loop                          | `driver`              | 99, 109–112 |
| 114 | Output writers                            | `output::wrt*`, `output::parquet` | 113 |
| 115 | NONROAD numerical fidelity validation     | (CI fixture)          | 113, 114 |
| 116 | NONROAD numerical-divergence triage       | (varies)              | 115 |
| 117 | NONROAD-MOVES integration                 | (orchestrator side)   | 113, 114 |
| 118 | NONROAD-specific post-processing          | (orchestrator side)   | 117 |

Tasks 91–98 establish foundations; 99–118 fill in the implementation
top-down from the driver, with the geography refactor (112) gated on
characterization parity.

---

## 9. Out of scope / deferred decisions

The following are explicitly out of scope for Task 91. They are
called out so that future tasks know the boundary.

* **Concrete `NonroadContext` fields.** Sketched in § 6 but
  finalized by Task 92. The skeleton's `NonroadContext` is empty.
* **Specific `Error` variants beyond the four in the skeleton.**
  Domain-specific variants (e.g. `EquipmentCategoryUnknown`,
  `RetrofitYearOutOfRange`) get added as porting tasks encounter
  the failure modes. The shape (typed enum, source-location
  carried) is fixed; the variant set is open.
* **Parser combinator vs. hand-written choice.** Task 94 establishes
  the pattern with `.POP` and `.ALO`. The current expectation is
  hand-written line-by-line parsers; `nom` is a fallback if the
  fixed-width formats turn out to need backtracking. Either is
  consistent with the I/O policy in § 4.3.
* **Parquet schema for NONROAD output.** Task 89 (Phase 4) defined
  the unified MOVES output schema; Task 114 wires NONROAD into it.
  The Rust port emits the legacy NONROAD text format *and* Parquet;
  Task 114 owns the schema-mapping decisions.
* **Parallelism.** Single-threaded by default; Task 134 wires up
  Rayon-on-Web-Workers. The library does not depend on `rayon` at
  this stage.
* **Validation against the Windows reference.** Task 115 owns the
  characterization-fixture diffing; the per-pollutant tolerance
  budget lives in `characterization/tolerance.toml` (see Task 7,
  bead `mo-obyw`).

---

## 10. Open questions for review

These are decisions that the design fixes provisionally; reviewers
who disagree should raise them on this task's PR rather than later
in the cycle, when revisiting them is more expensive.

1. **Module name `driver` instead of plan's `main`.** Justified by
   the `fn main()` collision (§ 2.1). Reviewers who prefer
   `main_loop` or `nonroad` are invited to weigh in; the rename is
   reversible until later modules start importing the name.
2. **`output` cluster spans writers, lookups, validators, FIPS, and
   string utilities.** The Fortran source groups all of these in
   `wrt*.f`, `fnd*.f`, `chk*.f`, etc. — small files clustered around
   the writer logic. Rust would normally split lookup helpers
   into a separate `lookup` module. The current grouping mirrors
   the migration-plan audit; Task 101 may re-split if it improves
   ergonomics.
3. **`common` vs. `state`.** Both are reasonable; `common` is used
   here because it makes the COMMON-block heritage explicit to
   anyone cross-referencing the Fortran source. Inside `common`,
   `consts` carries the constants (Task 93) and `mod.rs` will
   carry the typed sub-structs (Task 92).
4. **Workspace placement.** Co-located with `moves-snapshot` and
   `moves-fixture-capture` under `crates/`. Phase 2's orchestrator
   crate (when it lands) will be a sibling.

Open questions that are *not* on the table for this task: the policy
choices in § 4 (array sizes, error handling, I/O, WASM). Those are
inherited from the migration plan and are load-bearing for downstream
tasks.

---

## References

* `moves-rust-migration-plan.md`, § Phase 5 (Tasks 91–118).
* `NONROAD/NR08a/SOURCE/` — original Fortran source (not in this repo).
* Phase 0 deliverables: `moves-snapshot` (canonical fixture format),
  `moves-fixture-capture` (intermediate dump → snapshot), `mo-obyw`
  (diff CLI + CI integration).
