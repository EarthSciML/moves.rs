# Fixture catalogue (Phase 0 Task 5 + 6)

The 33 RunSpec XML files in this directory are the regression fixtures that
every later phase of the moves.rs port verifies against. Each one is a
hand-tuned MOVES input whose snapshot (in `../snapshots/<fixture-name>/`)
is the ground-truth oracle for one slice of the MOVES coverage space.

## Layout

```
characterization/fixtures/
├── README.md                this file
├── coverage-matrix.md       generated table — process × scale × calculator
├── _generate.py             source of truth for the catalogue (generator)
├── sample-runspec.xml       canonical MOVES SampleRunSpec.xml (preserved)
├── expand-*.xml             one-dimension expansions of the canonical sample
├── process-*.xml            one-process focal fixtures
├── chain-*.xml              chain-leaf-focal fixtures
├── scale-*.xml              ModelScale × ModelDomain coverage
└── nr-*.xml                 NONROAD-model fixtures (10)
```

`sample-runspec.xml` is byte-identical to
`testdata/SampleRunSpec.xml` from the pinned canonical-MOVES tree
(MOVES5.0.1 @ `25dc6c83`). Treating it as input rather than generator
output preserves provenance for fixture #1 of the bead.

Every other XML is regenerated from the spec table at the top of
`_generate.py`. Re-run after editing the table:

```sh
python3 characterization/fixtures/_generate.py
```

## Naming conventions

| Prefix      | Meaning                                                   |
|-------------|-----------------------------------------------------------|
| `expand-`   | One-dimension expansion of the canonical sample           |
| `process-`  | Focuses MOVES on a single emission process                |
| `chain-`    | Forces a specific calculator-chain leaf to instantiate    |
| `scale-`    | ModelScale × ModelDomain dimension                        |
| `nr-`       | NONROAD model (`<model value="NONROAD"/>`)                |

The fixture **name** is `<filename without extension>`, lowercased and with
non-`[a-z0-9_-]` characters mapped to `_`. The Rust capture binary
(`moves-fixture-capture`) derives this name automatically from the
RunSpec path; the snapshot directory key matches.

## Producing a snapshot

The host-side machinery for one fixture is:

```sh
characterization/apptainer/run-fixture.sh \
    --fakeroot \
    --runspec characterization/fixtures/<fixture>.xml
```

The wrapper sets up `/scratch/$USER/moves-fixture/<fixture>/`, runs the
patched MOVES inside `moves-fixture.sif`, dumps every non-system MariaDB
schema, mirrors `MOVESTemporary/` and `WorkerFolder/`, then runs
`moves-fixture-capture` to write the deterministic snapshot to
`characterization/snapshots/<fixture>/`. See
`../apptainer/README.md` for SIF build prerequisites and bind-mount
details.

To run the **full suite**, use `../run-all-fixtures.sh`. That wrapper is
the polecat → operations handoff: the polecat ships fixture XMLs +
runner; an HPC compute job runs the suite to populate snapshots.

## Determinism contract (cross-reference)

Two runs of any fixture against the same SIF SHA256 produce
byte-identical files in `characterization/snapshots/<fixture>/`. The
mechanism is documented in `../snapshots/README.md`. If a snapshot file's
bytes change between two such runs, that's the regression-detection
signal Phase 0 is designed to provide.

## Coverage

See [`coverage-matrix.md`](coverage-matrix.md) for the full
fixture × (process, scale, calculator) cross-reference. The matrix is
regenerated every time `_generate.py` is run.

## Acceptance status (Phase 0 Task 5 + 6, bead `mo-n2yg`)

| Acceptance criterion | State |
|----------------------|-------|
| Fixtures live in `characterization/fixtures/` | **Met** — 33 RunSpec XMLs (target: 30–35) |
| Each fixture has a snapshot in `characterization/snapshots/` | **Pending compute-node run** — see below |
| Coverage matrix documents (process × scale × calculator-chain) | **Met** — `coverage-matrix.md` |

The "captured snapshot" leg is gated on the SIF being built and the
fixture suite being executed on an HPC compute node — `run-all-fixtures.sh`
and `../snapshots/POPULATING.md` describe that handoff. Authoring the
fixture set + matrix (this bead's polecat-completable scope) is what
ships in this PR.
