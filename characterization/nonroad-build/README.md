# nonroad-build — Linux gfortran NONROAD build + Phase 5 instrumentation

This directory holds the recipe for compiling NONROAD on Linux with
`gfortran` in a way that avoids the known "near-zero emission" issue
documented in `NONROAD/NR08a/SOURCE/readme.md` upstream, plus source
patches that emit intermediate state (population, age distribution,
growth, emissions) used as the Phase 5 regression baseline for the
Rust port.

This is the HPC adaptation of Phase 0 Task 2 of
[the migration plan](../../moves-rust-migration-plan.md). EPA's
recommendation to use a different Fortran compiler is not viable on
the HPC system this work runs on (gfortran is the only Fortran
compiler available), so we go all-in on gfortran with a flag set
chosen to address the documented near-zero pathway.

## Layout

```
characterization/nonroad-build/
├── README.md                           # this file
├── build.sh                            # build wrapper (entry point)
├── flags.env                           # pinned compiler flags (source of truth)
├── Makefile.linux                      # replacement makefile (drop-in)
├── src/
│   └── dbgemit.f                       # Phase 5 baseline emit subroutines
└── patches/
    ├── 0001-instrument-agedist.patch   # per-call mdyrfrc emit
    ├── 0002-instrument-grwfac.patch    # per-call growth factor emit
    ├── 0003-instrument-getpop.patch    # per-SCC population emit
    └── 0004-instrument-clcems.patch    # per-call emissions emit
```

## Building

`build.sh` takes a path to a MOVES source tree (it does not clone
MOVES — that's the SIF's job, see `../apptainer/`). It copies in
`Makefile.linux` + `dbgemit.f`, applies the four patches, and runs
`make` against the replacement makefile:

```sh
cd characterization/nonroad-build
./build.sh /path/to/moves-src
```

The compiled binary lands at `<moves-tree>/NONROAD/NR08a/NONROAD.exe`
(MOVES expects that exact filename, including the `.exe` suffix even
on Linux — it's a MOVES configuration value, not a Windows artifact).

The script prints the binary's SHA256 on stdout — record it alongside
the SIF SHA256 in `../canonical-image.lock` for downstream
characterization snapshots.

### Environment toggles

| Variable | Default | Effect |
|----------|---------|--------|
| `FLAVOR` | `production` | `production` = `FLAGS_PRODUCTION` from flags.env (-O2 + F77-compat); `audit` = `FLAGS_AUDIT` (-O0 -g -fcheck=all + ffpe-trap, slower; for chasing a divergence) |
| `FC` | `gfortran` (from flags.env) | Override the compiler |
| `JOBS` | `nproc` | Parallel `make -j` jobs |
| `OUTPUT` | `<tree>/NONROAD/NR08a/$NONROAD_BINARY_NAME` | Override binary destination (default name from flags.env) |
| `SKIP_INSTRUMENTATION` | `0` | `1` = skip applying the dbgemit patches; produces a binary with the flag fix only (no Phase 5 capture). Useful for cross-checking that the patches don't affect numerical results |

### Idempotence

`build.sh` re-runs cleanly on a tree it has already touched: it
detects already-applied patches via `patch --dry-run -R` and skips
them. Re-running with the same flags produces a byte-identical
binary (validated with `sha256sum`).

### Integration with the canonical-moves SIF

The Apptainer SIF build (`../apptainer/canonical-moves.def`) invokes
`build.sh` from `%post` after the MOVES tree is cloned and before
`ant compile`. The wired-up call is:

```sh
"${HERE}/../nonroad-build/build.sh" /opt/moves
```

(That hook is added in the next bead — this bead ships the recipe
and validates it builds against a clone of the pinned MOVES commit;
the SIF integration is `mo-i8qc`'s territory.)

## Compiler flags — rationale

`flags.env` is the source of truth and carries the full hypothesis-by-hypothesis rationale (H1 through H7). The short version: the production set is **`-O2 -std=legacy -fno-automatic -finit-local-zero -finit-real=zero -fno-align-commons -ffixed-form -ffixed-line-length-132`**, with `-fno-automatic` doing the heavy lifting against the documented "near-zero emissions" issue (it gives F77-compatible SAVE semantics to locals that NONROAD's accumulator-style code implicitly relies on). See `flags.env` for the analysis of each enabled and rejected flag.

## dbgemit instrumentation (Phase 5 baseline)

The Rust port (Phase 5) needs a regression baseline of NONROAD's
intermediate state — the per-equipment populations, age
distributions, growth factors, and emissions arrays — to validate
that the Rust calculations match the Fortran reference within
tolerance.

`src/dbgemit.f` is a small set of subroutines that append tagged
records to a TSV file when the `NRDBG_FILE` environment variable is
set:

```
<phase>\t<context>\t<label>\t<count>\t<v1>\t<v2>...\n
```

| Field | Meaning |
|-------|---------|
| `phase` | Subsystem label: `GETPOP`, `AGEDIST`, `GRWFAC`, or `CLCEMS` |
| `context` | `key=val,key=val` tag string (FIPS, SCC, year, idx, …) |
| `label` | Variable name for the value(s) on this line |
| `count` | Number of values that follow |
| `v1, v2, …` | Tab-separated values (real*4 or integer*4) |

When `NRDBG_FILE` is unset or empty, all `dbg*` calls are no-ops
(single saved-flag check + return) — production runs pay essentially
zero overhead.

### Patched call sites

Each patch adds **one** new emit point at the success exit of one
subroutine. The instrumentation is intentionally minimal:

| File | Phase | Variables emitted |
|------|-------|-------------------|
| `agedist.f` | `AGEDIST` | `mdyrfrc(MXAGYR)`, `baspop` |
| `grwfac.f`  | `GRWFAC`  | `factor`, `baseyearind`, `growthyearind` |
| `getpop.f`  | `GETPOP`  | `popeqp(npoprc)`, `avghpc(npoprc)`, `usehrs(npoprc)`, `ipopyr(npoprc)` |
| `clcems.f`  | `CLCEMS`  | `emsday(MXPOL)`, `emsbmy(MXPOL)`, `pop`, `mfrac`, `afac`, `dage` |

Each call site also emits a `call=<n>` counter in its `ctx` so the
parser can disambiguate calls within a run. The counter relies on
SAVE semantics (provided by `-fno-automatic`) and zero-initialization
(provided by `-finit-local-zero`).

### Adding more emit points

Each patch is a tightly-scoped unified diff against the pinned MOVES
commit (`25dc6c83…`). To add a new emit point:

1. Edit the relevant `.f` file directly in a clean clone.
2. `git diff <file> > patches/00NN-instrument-<area>.patch` from the
   MOVES tree root.
3. Run `./build.sh /path/to/moves-src` to confirm the new patch
   applies and the result still compiles.

If a patch fails to apply against a future MOVES commit, that's a
deliberate cutover signal — the upstream code has changed at the
emit site and the patch needs to be regenerated.

### Capturing a baseline

```sh
NRDBG_FILE=/scratch/$USER/nonroad-baseline.tsv \
    apptainer exec --bind ... canonical-moves.sif \
    bash -c 'cd /opt/moves && ant crun -Drunspec=<runspec.xml>'
```

The captured TSV is line-oriented and loadable with any tool that
reads TSV (Polars, pandas, awk, …). Rough byte budget on a
representative one-county fixture: ~5–20 MB. Larger fixtures
(multi-county or annual) can produce hundreds of MB — keep them on
scratch, not in the repo.

## Validation gate

This bead's bead description requires:

> Run a representative fixture per equipment category through the
> locally-built NONROAD; outputs must produce non-zero emissions for
> all equipment categories.
>
> Use canonical MOVES (which invokes NONROAD as a subprocess) running
> inside the Apptainer SIF as the cross-check: MOVES output that
> depends on NONROAD must match between (a) MOVES-with-the-locally-
> fixed-NONROAD and (b) MOVES-with-stock-NONROAD on x86_64 within
> tolerance — and must produce non-zero emissions for all equipment
> categories.

That gate requires (1) the canonical-moves SIF built on an HPC
compute node and (2) the fixture set authored by `mo-n2yg`
(downstream of this bead). The build infrastructure in this
directory is what `mo-n2yg`'s validation will exercise; the gate
itself is run there, not here.

### Smoke validation in this bead

What this bead validates locally before handoff:

* `build.sh` against a clean clone of the pinned MOVES commit
  produces an x86_64 ELF binary.
* The binary runs without dynamic-linker errors when invoked with no
  arguments (it prompts for an options file on stdin and exits with
  a non-zero code when stdin is closed — both expected).
* All four patches apply cleanly to the pinned MOVES commit.
* `SKIP_INSTRUMENTATION=1` produces a different binary SHA (confirms
  the patches actually change the produced code).
* `FLAVOR=audit` produces a larger binary with `debug_info` (confirms
  the alternate flag set is wired up).
* `dbgemit` smoke test: standalone driver linking dbgemit.f + strmin.f
  with `NRDBG_FILE` set produces a tab-separated record file; with
  `NRDBG_FILE` unset, no file is created and `dbgon` returns false
  (no-op semantics confirmed).
* Re-running on a patched tree is a no-op (idempotence).

What this bead does **not** validate (deferred to `mo-n2yg`):

* That the binary produces correct emissions for every equipment
  category. Without representative input fixtures + a working MOVES
  runtime, we cannot produce emissions.
* That `MOVES-with-our-NONROAD` matches `MOVES-with-stock-NONROAD`
  within tolerance. That cross-check requires both binaries plus the
  full MOVES Java runtime + MariaDB + a MOVES runspec — i.e. the
  canonical-moves SIF.

## Known divergences

Empty for now — populate as the validation gate (`mo-n2yg`) finds
equipment categories that fall outside tolerance.

The migration plan accepts widened-tolerance documentation as a
fallback for categories that cannot be made fidelity-correct after
exhausting compiler-flag and code-fix options. Record each here as:

```
| SCC | Tolerance | Reason |
|-----|-----------|--------|
| ... | ...       | ...    |
```
