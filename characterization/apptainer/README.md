# canonical-moves Apptainer SIF

This directory holds the recipe for `canonical-moves.sif`, the reference
MOVES environment for the moves.rs migration. The SIF is the canonical
oracle every other phase verifies against — it must be reproducible and
content-addressed.

This is the HPC adaptation of Phase 0 Task 1 of
[the migration plan](../../moves-rust-migration-plan.md). Docker is not
available on the HPC system that drives this work, so we build with
Apptainer instead.

## Layout

```
characterization/
├── apptainer/
│   ├── canonical-moves.def       # Apptainer build recipe (canonical)
│   ├── moves-fixture.def         # Apptainer build recipe (canonical + patch)
│   ├── README.md                 # this file
│   ├── build-sif.sh              # canonical build wrapper; writes lockfile
│   ├── build-fixture-sif.sh      # fixture build wrapper; writes lockfile
│   ├── run-moves.sh              # runtime wrapper with bind mounts
│   └── files/
│       ├── versions.env          # pinned versions (sourced by both)
│       ├── my.cnf                # MOVES-tuned MariaDB config
│       ├── init-mariadb.sh       # first-run seed-data copy
│       ├── start-mariadb-bg.sh   # user-mode MariaDB launcher
│       └── intermediate-state-capture.patch  # Phase 0 Task 3 flag flips
├── canonical-image.lock          # SHA256 of canonical-moves.sif
└── fixture-image.lock            # SHA256 of moves-fixture.sif
```

## Two SIFs: canonical and fixture

The recipe builds **two** SIFs that share most of their content:

| SIF | Built by | Contents | Used by |
|-----|----------|----------|---------|
| `canonical-moves.sif` | `build-sif.sh` | Pinned MOVES, untouched | reference oracle, identity for downstream snapshot pins |
| `moves-fixture.sif` | `build-fixture-sif.sh` | `canonical-moves.sif` + `intermediate-state-capture.patch`, recompiled | fixture runs that need MOVESTemporary/, WorkerFolder/WorkerTempXX/, and external-generator outputs to persist |

The fixture SIF bootstraps from canonical-moves.sif (`Bootstrap: localimage`)
so the parent's MariaDB seed, JDK, Go, and MOVES clone are inherited
unchanged — only the three patched Java fields and their recompiled
`.class` files differ. This keeps the canonical SHA stable across
fixture-tooling churn.

The patch flips three flags so MOVES's normal cleanup paths skip:

| File | Field | Default | Patched |
|------|-------|---------|---------|
| `gov/epa/otaq/moves/master/framework/Generator.java` | `KEEP_EXTERNAL_GENERATOR_FILES` | `false` | `true` |
| `gov/epa/otaq/moves/master/framework/OutputProcessor.java` | `keepDebugData` | `false` | `true` |
| `gov/epa/otaq/moves/worker/framework/RemoteEmissionsCalculator.java` | `isTest` | `false` | `true` |

## Inside the SIF

| Component | Version | Source |
|-----------|---------|--------|
| Base OS | Ubuntu 24.04 LTS | `docker://ubuntu:24.04` |
| MariaDB | 11.4.x (LTS series) | MariaDB official APT repo |
| JDK | Eclipse Temurin 17 | Adoptium APT repo |
| Go | 1.21.13 | go.dev tarball |
| ant | distribution package | Ubuntu apt |
| MOVES | pinned commit (see `files/versions.env`) | `github.com/USEPA/EPA_MOVES_Model` |
| MOVES default DB | `movesdb20241112` (pinned by SHA256) | `database/Setup/` in the source, or `MOVESDB_URL` |

The MOVES sources land at `/opt/moves`. The MariaDB seed data lands at
`/var/lib/mysql-seed` (read-only) and is copied into the writable
`/var/lib/mysql` bind-mount on first run.

## Bind-mount layout (runtime)

The SIF's filesystem is read-only. MariaDB needs writable directories
for its data dir, sockets, and pidfile; MOVES needs writable directories
for its scratch (`MOVESTemporary/`) and worker folders
(`WorkerFolder/WorkerTempXX/`). All four are bind-mounted from host
scratch:

| Container path | Host path (default) | Purpose |
|----------------|---------------------|---------|
| `/var/lib/mysql` | `/scratch/${USER}/moves-canonical/mariadb-data` | MariaDB datadir; writable; seeded on first run |
| `/var/run/mysqld` | `/scratch/${USER}/moves-canonical/run-mysqld` | MariaDB socket + pidfile; writable |
| `/opt/moves/MOVESTemporary` | `/scratch/${USER}/moves-canonical/MOVESTemporary` | MOVES master/worker scratch tables, debug captures |
| `/opt/moves/WorkerFolder` | `/scratch/${USER}/moves-canonical/WorkerFolder` | MOVES worker bundle scratch |

Override host paths via env vars (`MARIADB_DATA`, `MARIADB_SOCK_DIR`,
`MOVES_TEMP`, `WORKER_DIR`) or by editing `run-moves.sh`.

For a tmpfs-backed worker scratch (faster, ephemeral) on hosts with
sufficient RAM, point `WORKER_DIR` at `/dev/shm/${USER}/WorkerFolder` —
MOVES doesn't expect persistence across runs.

## User-namespace handling

MariaDB normally runs as the `mysql` system user. Inside Apptainer the
container runs as the **calling** user, not `mysql`. We support both
common HPC modes:

* **`--fakeroot` (preferred where available).** Apptainer maps the
  calling user to UID 0 inside the container via subuid/subgid. The
  standard `service mariadb start` works as-is, files in
  `/var/lib/mysql` are written as `mysql` (mapped to a per-user UID
  range on the host), and the bead's published validation command
  works verbatim. Most HPC Apptainer installs configure fakeroot via
  `apptainer config fakeroot`.

* **No-root mode (fallback).** When fakeroot isn't configured, the
  container runs as the calling user. MariaDB cannot become `mysql`,
  so we launch `mariadbd` directly with `--user=$(id -un)` via
  `/opt/moves-bin/start-mariadb-bg.sh`. The MariaDB datadir bind-mount
  must be owned by the calling user. `run-moves.sh` (without `-f`)
  follows this path; the `service mariadb start` shorthand from the
  bead is **not** available in this mode.

Pick fakeroot if your HPC supports it; pick no-root mode if it
doesn't. The numerical results from MOVES are identical either way —
only the launch path differs.

## Pinning the MOVES source

EPA does not publish stable Git tags on every release; the canonical
commit is whichever HEAD on `master` corresponds to the desired MOVES
release. To pick a commit:

1. Decide which MOVES release the migration baselines against
   (currently MOVES5 / movesdb20241112).
2. On `github.com/USEPA/EPA_MOVES_Model`, locate the commit that
   landed the corresponding release notes. The first commit with the
   release tag in `Documentation/` is a reliable marker.
3. Copy the 40-hex-char SHA into `files/versions.env`'s
   `MOVES_COMMIT` field (and update `MOVESDB_SHA256` to match the
   default-DB ZIP that release ships).
4. Re-run `build-sif.sh`. The new SIF SHA256 lands in
   `../canonical-image.lock`.

Bumping the MOVES commit is a deliberate cutover. Any downstream
characterization snapshot taken against the prior SIF SHA is no longer
authoritative.

## Building the SIF

`build-sif.sh` validates pins, runs `apptainer build`, and records the
output SHA256 to `../canonical-image.lock`. The pins are tracked in
`files/versions.env` and committed; the typical build is just:

```sh
cd characterization/apptainer
./build-sif.sh
```

To override a pin for a one-off build (without committing to
`versions.env`), pass it as an env var:

```sh
MOVES_COMMIT="<40-hex-char-sha>" \
MOVESDB_SHA256="<sha256-of-movesdb20241112.zip>" \
    ./build-sif.sh
```

`MOVESDB_LOCAL_PATH` (optional) skips the in-clone or hosted
default-DB lookup. Point it at a local copy of `movesdb20241112.zip`
(or an already-unzipped directory containing it); `build-sif.sh`
SHA-verifies and stages the path into the build context via `%files`,
so the build doesn't need to re-fetch the 30 MB ZIP from GitHub.

```sh
MOVESDB_LOCAL_PATH=/scratch/$USER/movesdb20241112.zip ./build-sif.sh
```

The build runs as root inside the build sandbox via Apptainer's
auto-detected privilege mode (setuid where allowed, fakeroot
otherwise). It needs ~30–40 GB scratch under `/tmp` for the unpacked
DB and ant build artifacts (override with `APPTAINER_TMPDIR=` if
`/tmp` is too small or noexec).

Expected build time on a modern HPC compute node: 30–60 minutes,
dominated by the default-DB load (~10 minutes for ~390 MB SQL dump
extracted from the 30 MB ZIP) and `ant compile` (~5 minutes).

## Building the fixture SIF

Once `canonical-moves.sif` exists, build the fixture variant on top:

```sh
cd characterization/apptainer
./build-fixture-sif.sh
```

This bootstraps from `./canonical-moves.sif`, applies
`files/intermediate-state-capture.patch`, runs `ant compileall` over
the patched source, and writes the resulting SIF SHA256 into
`../fixture-image.lock`. Override `PARENT_SIF=<path>` to point at a
canonical SIF in a non-default location.

Expected build time: 5–10 minutes (mostly `ant compileall`). The
default DB and apt installs are inherited unchanged from
`canonical-moves.sif`, so no re-fetch.

## Validating the SIF

The bead's published validation command (HPC compute node, fakeroot):

```sh
apptainer exec --fakeroot \
    --bind /scratch/$USER/moves-canonical/mariadb-data:/var/lib/mysql \
    --bind /scratch/$USER/moves-canonical/run-mysqld:/var/run/mysqld \
    --bind /scratch/$USER/moves-canonical/MOVESTemporary:/opt/moves/MOVESTemporary \
    --bind /scratch/$USER/moves-canonical/WorkerFolder:/opt/moves/WorkerFolder \
    canonical-moves.sif \
    bash -c "/opt/moves-bin/init-mariadb.sh && service mariadb start && \
             cd /opt/moves && ant crun -Drunspec=testdata/SampleRunSpec.xml"
```

Or, equivalently, via the wrapper:

```sh
./run-moves.sh --fakeroot --runspec testdata/SampleRunSpec.xml
```

In no-root mode:

```sh
./run-moves.sh --runspec testdata/SampleRunSpec.xml
```

Verify output landed at `/scratch/$USER/moves-canonical/...`. The
sample RunSpec writes its output database into MariaDB; query it via:

```sh
apptainer exec --bind /scratch/$USER/moves-canonical/mariadb-data:/var/lib/mysql \
    --bind /scratch/$USER/moves-canonical/run-mysqld:/var/run/mysqld \
    canonical-moves.sif \
    bash -c "/opt/moves-bin/start-mariadb-bg.sh && \
             mariadb -uroot -e 'SHOW DATABASES'"
```

You should see `movesdb20241112` plus the per-run output database.

## Validating moves-fixture.sif

Same `run-moves.sh` wrapper, pointed at the fixture SIF:

```sh
SIF=./moves-fixture.sif ./run-moves.sh --fakeroot \
    --runspec testdata/SampleRunSpec.xml
```

After the run, the bind-mounted scratch should retain the
intermediate captures the patches enable:

```sh
ls /scratch/$USER/moves-canonical/MOVESTemporary/
ls /scratch/$USER/moves-canonical/WorkerFolder/
# Expect: WorkerTemp00/, WorkerTemp01/, … with bundle artifacts
# Expect: MOVESTemporary populated with debug tables
```

A clean canonical-moves run leaves these directories empty (or
missing the per-bundle subfolders) once cleanup runs; the fixture
run leaves them populated. That difference is the acceptance signal
for Phase 0 Task 3.

A quick introspection check that the SIF was built from a patched
source tree:

```sh
apptainer exec ./moves-fixture.sif test -f /opt/moves/.intermediate-state-capture.applied
apptainer exec ./moves-fixture.sif cat /opt/moves/.fixture-build-date
```

## Why Apptainer (not Docker)

The HPC system this work runs on doesn't have a Docker daemon, but
does have Apptainer. The migration plan's reference to a "Docker
image" is interpreted here as "a reproducible, content-addressed
container image" — Apptainer SIFs satisfy that contract:

* Single-file artifact (`.sif`)
* SHA256-pinnable
* Runs unprivileged on most HPC sites
* Reads bind-mounted host paths for writability without losing
  immutability of the image content

The def file uses `Bootstrap: docker` to pull `ubuntu:24.04` from
Docker Hub during build, but the resulting SIF has no Docker
dependency at runtime.

## Reproducibility caveats

* `apt-get install` floats within the package's allowed minor versions
  unless we also pin `*.deb` URLs — we don't, because doing so would
  drift the SIF every time Ubuntu's mirrors update. The SIF SHA256 is
  the canonical identity; if a future build produces a different
  hash, that's a deliberate cutover that needs a new lockfile.
* `git clone` fetches whatever blobs match the pinned commit; the
  commit SHA is content-addressed so this is deterministic.
* MOVES's `ant compile` is deterministic in practice but not
  byte-stable across timestamps. We accept this; the runtime behaviour
  is what matters, and that's verified by the characterization
  fixtures (Phase 0 Task 4–7).

If strict byte-reproducible builds become a requirement later, the
upgrade path is `apt-get install <package>=<version>` with explicit
versions and an offline apt cache.
