# Upstream MOVES tracking guide

This document describes how to incorporate a new EPA MOVES release into the
`moves.rs` port.  EPA publishes MOVES approximately annually; each release may
bring default-DB schema changes, updated emission rates, and occasionally new
calculator logic.  This guide covers detection, mechanical update steps,
Rust-side impact assessment, and release tagging.

---

## Overview

The update process has three phases:

| Phase | What you do | Time budget |
|-------|-------------|-------------|
| **Detect** | Identify the new commit and default-DB zip; run the schema diff tool | < 1 hour |
| **Rebuild** | Update version pins, rebuild SIF, regenerate Parquet, re-run fixtures | 2–8 hours (mostly SIF build) |
| **Assess** | Review snapshot drift and schema changes; update Rust code if needed | hours–days depending on scope |

All detection and rebuild steps are automated by `scripts/upstream-update.sh`
(local) and `.github/workflows/upstream-update.yml` (CI).  Assessment is
necessarily manual.

---

## Step 1 — detect the new release

### 1a. Find the new MOVES commit

EPA publishes MOVES at [https://github.com/USEPA/EPA_MOVES_Model](https://github.com/USEPA/EPA_MOVES_Model).
Check for new tags or commits on `master`:

```bash
git ls-remote https://github.com/USEPA/EPA_MOVES_Model.git refs/heads/master
```

Look for commit messages mentioning a new MOVES version (e.g. `MOVES5.1.0
with movesdb20251023`).  Copy the full 40-hex commit SHA.

### 1b. Download the new default-DB zip

EPA distributes the default database as a zip file (e.g. `movesdb20251023.zip`)
either bundled in the MOVES source repository at
`database/Setup/movesdb<DATE>.zip` or available via the EPA website.  Download
it and record its SHA256:

```bash
sha256sum movesdb20251023.zip
```

### 1c. Run the schema diff tool

Before touching any pin files, regenerate the schema audit from the new DDL
and diff it against the committed baseline.  This tells you whether any
default-DB tables changed before you invest hours in a SIF rebuild.

```bash
# Clone the new MOVES source temporarily:
git clone --depth 1 https://github.com/USEPA/EPA_MOVES_Model.git \
    --branch master /tmp/moves-src

# Regenerate the schema audit:
python3 characterization/default-db-schema/audit-schema.py \
    --default-sql    /tmp/moves-src/database/CreateDefault.sql \
    --nr-default-sql /tmp/moves-src/database/CreateNRDefault.sql \
    --moves-commit   <new-commit-sha> \
    --output         /tmp/tables-new.json

# Diff against the committed baseline:
python3 characterization/default-db-schema/diff-schema.py \
    characterization/default-db-schema/tables.json \
    /tmp/tables-new.json
```

Exit code `0` means no schema changes (rates-only update).  Exit code `1`
means structural changes; the diff output names every added/removed/changed
table and column.

For a machine-readable diff (useful for scripting):

```bash
python3 characterization/default-db-schema/diff-schema.py \
    characterization/default-db-schema/tables.json \
    /tmp/tables-new.json \
    --format json --output /tmp/schema-diff.json
```

---

## Step 2 — run the automated update pipeline

### Option A: local orchestration script (HPC node)

```bash
scripts/upstream-update.sh \
    --moves-commit  <40-hex-sha> \
    --movesdb-zip   /path/to/movesdb20251023.zip \
    2>&1 | tee /tmp/update-$(date +%Y%m%d).log
```

The script runs all nine stages in order and prints an **UPDATE REPORT** at the
end listing every detected change and the manual steps required.  See
`scripts/upstream-update.sh --help` for the full option set.

Stages:
1. **Source fetch** — shallow-clone MOVES at the new commit
2. **Schema audit** — regenerate `tables.json` via `audit-schema.py`
3. **Schema diff** — compare with committed `tables.json`
4. **versions.env preview** — generate an updated `versions.env` for review
5. **SIF rebuild** — run `characterization/apptainer/build-sif.sh`
6. **Parquet conversion** — run `characterization/default-db-conversion/convert-default-db.sh`
7. **Fixture suite** — run `characterization/run-all-fixtures.sh`
8. **Snapshot diff** — compare fresh snapshots against committed baselines
9. **Cargo tests** — `cargo test --workspace --all-targets`

Flags for iterative runs:
- `--skip-sif-build` — reuse the existing SIF (schema/Parquet/fixture-only pass)
- `--skip-parquet`   — skip Parquet regeneration (fixture-only pass)
- `--skip-fixtures`  — schema and Parquet update only, no fixture run

### Option B: CI workflow (GitHub Actions)

Trigger the `upstream-update` workflow from the Actions tab:

```
Actions → upstream-update → Run workflow
```

Inputs:
- `moves_commit`   — 40-hex SHA
- `movesdb_label`  — e.g. `movesdb20251023`
- `movesdb_sha256` — SHA256 of the zip
- `movesdb_url`    — (optional) HTTPS URL to download the zip

The workflow runs on the self-hosted `[apptainer, fakeroot]` runner.  It
commits all changed files to a branch named `upstream/<movesdb-label>` and
opens a PR with a schema-diff excerpt and snapshot-drift summary.

---

## Step 3 — assess the changes

### 3a. Schema changes

If `diff-schema.py` reported structural changes, review
`characterization/default-db-schema/tables.json` and
`characterization/default-db-schema/partitioning-plan.md`.

For each **added table**:
1. Decide whether `moves.rs` needs to read it.  Many EPA additions are for
   new control strategies or analysis features; if no existing calculator
   uses the table, it is safe to ignore initially.
2. Assign a partition strategy by appending the table to `partitioning-plan.md`.
   The audit script's estimated row count and the `partition` field in
   `tables.json` give a starting point.

For each **removed table**:
1. Check whether any Rust calculator or importer reads it:
   ```bash
   grep -r "<TableName>" crates/ --include="*.rs"
   ```
2. If a calculator reads a removed table, it must be updated before the port
   can run against the new default DB.  File a bug and block the release on
   the fix.

For each **column change** (added, removed, or type-changed):
1. Search for every Rust read site:
   ```bash
   grep -r "<column_name>" crates/ --include="*.rs" -i
   ```
2. Added columns in tables we read: safe to ignore unless the new column is
   a required input for a calculator (e.g. a new filter dimension that
   changes row selection).
3. Removed columns we read: must be addressed before the release.
4. Type changes: check whether the Arrow type inference in the Parquet reader
   (`crates/moves-data-default/src/`) still handles the new type correctly.

### 3b. Snapshot drift

After the fixture suite runs, each fixture is diffed against the committed
baseline in `characterization/snapshots/<fixture>/`.  Drift falls into two
categories:

| Category | Cause | Action |
|----------|-------|--------|
| **Rate update** | EPA updated default emission rates in the new DB | Accept and update the snapshot baseline; add a note to `docs/known-divergences.md` |
| **Calculator change** | EPA changed calculator logic (new Java code) | Investigate whether the Rust port also needs updating |
| **Determinism break** | Timestamp, sort order, or float precision leaked | Treat as a bug; do not accept the snapshot |

To inspect drift in a single fixture:

```bash
./target/release/moves-snapshot diff \
    characterization/snapshots/<fixture> \
    /tmp/upstream-snapshots/<fixture> \
    --format text \
    --tolerance characterization/tolerance.toml
```

The diff reports per-cell absolute errors for every column.  Compare the
magnitude against the tolerance budget in `tolerance.toml`.

If drift is an expected rate update, copy the new snapshot into the repo:

```bash
cp -r /tmp/upstream-snapshots/<fixture>/. characterization/snapshots/<fixture>/
```

If drift indicates new calculator behavior, investigate the EPA changelog for
the MOVES version to identify which calculator changed and update the
corresponding Rust implementation.

### 3c. Rust calculator updates

When either schema changes or snapshot drift indicate that calculator logic
changed:

1. Read the EPA MOVES changelog or commit history at the new commit for
   hints (`git -C /tmp/moves-src log --oneline ${OLD_COMMIT}..${NEW_COMMIT}`).
2. Identify the changed Java calculator class(es) and locate the Rust
   equivalent in `crates/moves-calculators/src/`.
3. Apply the equivalent logic change to the Rust port.
4. Re-run the fixture suite to verify the snapshot now matches the canonical
   MOVES output.

---

## Step 4 — commit the changes

Commit files in this order so each commit is independently reviewable:

1. `characterization/apptainer/files/versions.env` — version pin bump
2. `characterization/canonical-image.lock` — new SIF SHA (generated by `build-sif.sh`)
3. `characterization/default-db-schema/tables.json` — schema audit (if changed)
4. `characterization/default-db-schema/partitioning-plan.md` — partition plan
   updates (if new/changed tables require it)
5. `crates/*/src/` — Rust calculator or reader changes (if required)
6. `characterization/snapshots/<fixture>/` — updated snapshot baselines
7. `docs/known-divergences.md` — document any new accepted divergences
8. `CHANGELOG.md` — release-level summary of changes

---

## Step 5 — version-tag the release

Once all changes are merged to `main`, tag the release.

### Bump crate versions

For an upstream MOVES database update (no API-breaking changes):
- `minor` version bump (`0.1.0 → 0.2.0`).

If calculator logic changes break the public API or output schema:
- discuss with maintainers; a `minor` or `major` bump may be appropriate.

Edit `Cargo.toml` for every public crate (or use `cargo-release` / `cargo-
workspaces` to bump all at once):

```bash
# Example: bump all workspace crates from 0.1.0 to 0.2.0
find crates -name 'Cargo.toml' -exec sed -i 's/^version = "0.1.0"/version = "0.2.0"/' {} \;
cargo check  # verify the workspace still resolves
```

Update `Cargo.lock`:

```bash
cargo build --workspace
```

### Update CHANGELOG.md

Add a `## [0.2.0] — YYYY-MM-DD` section documenting:
- New MOVES / default-DB version incorporated
- Schema changes (if any) and their impact
- Known divergences accepted in this release
- Any breaking API or output-schema changes

### Create the tag

```bash
git tag -a v0.2.0 -m "moves.rs v0.2.0 — movesdb20251023 (MOVES5.1.0)"
git push origin v0.2.0
```

Pushing the tag triggers the `release` workflow, which builds cross-platform
binaries and creates a GitHub Release.  After the release is created, trigger
the `package-default-db` workflow to generate and attach the converted Parquet
tree:

```
Actions → package-default-db → Run workflow
  tag: v0.2.0
  db_version: movesdb20251023
```

---

## Frequency and planning budget

EPA has released MOVES annually in recent years.  Expect:

| Activity | Time |
|----------|------|
| Schema detection + diff | < 1 hour |
| SIF rebuild (first run; base layers not cached) | 1–2 hours |
| Parquet conversion + validation | 30–60 minutes |
| Full fixture suite | 2–4 hours |
| Snapshot drift review (rates-only update) | 1–2 hours |
| Calculator logic investigation (if needed) | 1–5 days |
| Total (rates-only update, no Rust changes) | ~1 person-day |
| Total (calculator logic update) | ~1 person-week |

---

## Key files reference

| File | Purpose |
|------|---------|
| `characterization/apptainer/files/versions.env` | Version pins: MOVES commit, movesdb filename + SHA256, toolchain |
| `characterization/canonical-image.lock` | SHA256 of the built canonical-moves SIF |
| `characterization/default-db-schema/tables.json` | Machine-readable schema audit; drives Parquet partition plan |
| `characterization/default-db-schema/audit-schema.py` | Generates `tables.json` from MOVES DDL |
| `characterization/default-db-schema/diff-schema.py` | Diffs two `tables.json` files to detect schema changes |
| `characterization/default-db-schema/partitioning-plan.md` | Human-readable rationale for per-table Parquet partition strategy |
| `characterization/default-db-conversion/convert-default-db.sh` | Stage 1+2 conversion pipeline: MariaDB dump → Parquet |
| `characterization/default-db-conversion/validate-default-db.sh` | Validates converted Parquet against source TSV |
| `characterization/run-all-fixtures.sh` | Runs all characterization fixtures via the fixture SIF |
| `characterization/snapshots/` | Committed baseline snapshots for all 34 fixtures |
| `characterization/tolerance.toml` | Float tolerance budget for snapshot diffs |
| `scripts/upstream-update.sh` | Local orchestration script for the full update pipeline |
| `.github/workflows/upstream-update.yml` | CI automation: schema diff → SIF rebuild → Parquet → fixtures → PR |
| `.github/workflows/fixture-suite-weekly.yml` | Weekly regression (catches drift between annual updates) |
| `.github/workflows/package-default-db.yml` | Packages and uploads Parquet tree to a GitHub Release |
| `docs/known-divergences.md` | Documented numerical divergences from canonical MOVES |

---

## Weekly monitoring

The `fixture-suite-weekly.yml` CI job runs every Monday against the **pinned**
SIF and diffs fresh snapshots against the committed baseline.  A weekly failure
can signal:

1. **Upstream EPA patched the default DB** in place (unusual but not
   impossible if they push a hotfix to `master` at the same commit SHA).
2. **Runner environment drift** (MariaDB minor version, Apptainer, gfortran).
3. **Determinism regression** introduced in a recent Rust commit.

Any weekly failure warrants investigation before concluding an upstream update
is needed.  Check the diff artifacts and compare the SIF SHA against
`canonical-image.lock` to rule out a SIF rebuild side effect.
