#!/bin/bash
# run-fixture-guards.sh — regression test for run-fixture.sh's capture guards
# (issue #56: the wrapper exited 0 when MOVES failed and wrote a zero-table
# snapshot that was reported as a successful capture; issue #64: the run-log
# scan that is supposed to catch that first was not general enough, and let
# four project-scale failures through on 2026-09-10).
#
# The test drives the REAL run-fixture.sh and the REAL run-moves.sh; only
# `apptainer` and the `moves-fixture-capture` binary are stubbed, so the code
# under test — status propagation, the "Java Result" log scan, the
# MOVES_FAILURE_PATTERNS log scan, the empty-captures refusal, the
# --min-tables floor, the publishing of moves-run.log into the snapshot, and
# the stage-then-publish swap — is exercised as shipped. No MOVES SIF, no Apptainer and no cargo
# build required; the whole run takes a couple of seconds.
#
# Usage: characterization/apptainer/tests/run-fixture-guards.sh
# Exit code: 0 = every guard behaved, 1 = at least one case regressed.

set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APPTAINER_DIR="$(cd "${HERE}/.." && pwd)"
CHAR_DIR="$(cd "${APPTAINER_DIR}/.." && pwd)"
RUN_FIXTURE="${APPTAINER_DIR}/run-fixture.sh"
RUNSPEC="${CHAR_DIR}/fixtures/sample-runspec.xml"   # absolute: no relative-path quirk

[ -x "${RUN_FIXTURE}" ] || { echo "FATAL: ${RUN_FIXTURE} not executable" >&2; exit 2; }
[ -f "${RUNSPEC}" ]     || { echo "FATAL: ${RUNSPEC} not found" >&2; exit 2; }

TMP="$(mktemp -d "${TMPDIR:-/tmp}/run-fixture-guards.XXXXXX")"
trap 'rm -rf "${TMP}"' EXIT
mkdir -p "${TMP}/bin" "${TMP}/out" "${TMP}/work"
touch "${TMP}/fake.sif"

# ----- Stub apptainer -------------------------------------------------------
# run-fixture.sh makes two apptainer calls: the MOVES run (through
# run-moves.sh) and the database dump. The dump call is the one carrying
# CAPTURES_DIR=/captures. STUB_MODE selects the failure to inject.
cat > "${TMP}/bin/apptainer" <<'STUB'
#!/bin/bash
if [ "$1" = "--version" ]; then echo "apptainer version stub"; exit 0; fi
STUB_MODE="${STUB_MODE:-none}"
is_dump=0
captures=""
for a in "$@"; do
    [ "$a" = "CAPTURES_DIR=/captures" ] && is_dump=1
    case "$a" in *:/captures) captures="${a%:/captures}" ;; esac
done

if [ "${is_dump}" = "0" ]; then
    case "${STUB_MODE}" in
        moves_exit_nonzero)
            echo "[stub] apptainer exec failed"; exit 1 ;;
        java_result)
            # What the real stack does when MOVES dies: ant logs the JVM's
            # status and still exits 0.
            printf '%s\n' \
                "main1worker:" \
                "     [java] The specified runspec file does not exist:  fixtures/x.xml" \
                "     [java] Java Result: 1" \
                "BUILD SUCCESSFUL"
            exit 0 ;;
        *)
            # A normal, successful MOVES run, optionally with extra log
            # lines spliced in. STUB_MOVES_EXTRA is deliberately separate
            # from STUB_MODE so a log-scan case can run with STUB_MODE=tables:
            # the dump then produces a full set of tables, and the only thing
            # that can refuse the capture is the log scan itself, not the
            # zero-table belt behind it.
            echo "main1worker:"
            echo "     [java] MOVES Master starting."
            if [ -n "${STUB_MOVES_EXTRA:-}" ]; then
                printf '%s\n' "${STUB_MOVES_EXTRA}" | while IFS= read -r l; do
                    printf '     [java] 9/10/26, 2:52 AM %s\n' "$l"
                done
            fi
            printf '%s\n' \
                "     [java] MOVES run finished normally." \
                "     [java] Java Result: 0" \
                "BUILD SUCCESSFUL"
            exit 0 ;;
    esac
fi

echo "[stub] dump pass (STUB_MODE=${STUB_MODE})"
if [ "${STUB_MODE}" = "tables" ]; then
    n="${STUB_TABLE_COUNT:-12}"
    mkdir -p "${captures}/databases/junittestoutput"
    i=1
    while [ "${i}" -le "${n}" ]; do
        printf 'id\tint\tPRI\n' > "${captures}/databases/junittestoutput/t${i}.schema.tsv"
        printf '%s\n' "${i}" > "${captures}/databases/junittestoutput/t${i}.tsv"
        i=$((i + 1))
    done
fi
exit 0
STUB

# ----- Stub moves-fixture-capture ------------------------------------------
cat > "${TMP}/bin/capture-stub" <<'STUB'
#!/bin/bash
out=""
while [ $# -gt 0 ]; do
    case "$1" in
        --output-dir) out="$2"; shift 2 ;;
        *) shift ;;
    esac
done
mkdir -p "${out}/tables"
n="${STUB_TABLE_COUNT:-12}"
i=1
while [ "${i}" -le "${n}" ]; do
    : > "${out}/tables/db__junittestoutput__t${i}.parquet"
    printf '{}' > "${out}/tables/db__junittestoutput__t${i}.meta.json"
    i=$((i + 1))
done
printf '{"tables":%s}' "${n}" > "${out}/manifest.json"
printf '{}' > "${out}/provenance.json"
printf '{}' > "${out}/execution-trace.json"
STUB

chmod +x "${TMP}/bin/apptainer" "${TMP}/bin/capture-stub"
export PATH="${TMP}/bin:${PATH}"
export MOVES_FIXTURE_CAPTURE_BIN="${TMP}/bin/capture-stub"

FAILURES=0
pass() { printf 'ok   — %s\n' "$1"; }
bad()  { printf 'FAIL — %s\n' "$1" >&2; FAILURES=$((FAILURES + 1)); }

# run_capture <name> <expect_rc: zero|nonzero> <expected reason substring or ''> [extra args...]
run_capture() {
    local name="$1" expect="$2" reason="$3"; shift 3
    local out="${TMP}/out/${name}" log="${TMP}/${name}.log"
    "${RUN_FIXTURE}" --runspec "${RUNSPEC}" --sif "${TMP}/fake.sif" \
        --workdir "${TMP}/work/${name}" --output-dir "${out}" "$@" \
        > "${log}" 2>&1
    RC=$?
    if [ "${expect}" = "nonzero" ] && [ "${RC}" -eq 0 ]; then
        bad "${name}: expected non-zero exit, got 0"; return
    fi
    if [ "${expect}" = "zero" ] && [ "${RC}" -ne 0 ]; then
        bad "${name}: expected exit 0, got ${RC} (see ${log})"; return
    fi
    if [ "${expect}" = "nonzero" ]; then
        if ! grep -q "CAPTURE FAILED — NO SNAPSHOT WRITTEN" "${log}"; then
            bad "${name}: no CAPTURE FAILED banner in output"; return
        fi
        if [ -n "${reason}" ] && ! grep -qF -- "${reason}" "${log}"; then
            bad "${name}: banner reason did not mention '${reason}'"; return
        fi
    fi
    if ls -a "${TMP}/out" | grep -qE '\.staging\.|\.prev\.'; then
        bad "${name}: left staging/prev debris behind"; return
    fi
    pass "${name}"
}

echo "=== run-fixture.sh guard regression (issues #56, #64) ==="

# 1. run-moves.sh / apptainer exits non-zero.
STUB_MODE=moves_exit_nonzero \
    run_capture moves-exit-nonzero nonzero "run-moves.sh exited 1"
[ -d "${TMP}/out/moves-exit-nonzero" ] && bad "moves-exit-nonzero: wrote a snapshot dir"

# 2. The reported bug's class: MOVES dies, ant reports "Java Result: 1" and
#    everything above it exits 0.
STUB_MODE=java_result \
    run_capture java-result nonzero "non-zero Java Result"
[ -d "${TMP}/out/java-result" ] && bad "java-result: wrote a snapshot dir"

# 3. Run "succeeds" but the dump produces no tables.
STUB_MODE=no_tables \
    run_capture empty-captures nonzero "holds no dumped tables"
[ -d "${TMP}/out/empty-captures" ] && bad "empty-captures: wrote a snapshot dir"

# 4. Snapshot built but below the --min-tables floor; a pre-existing good
#    snapshot at the same path must survive untouched.
mkdir -p "${TMP}/out/min-tables"
echo sentinel > "${TMP}/out/min-tables/GOOD-SNAPSHOT-SENTINEL"
STUB_MODE=tables STUB_TABLE_COUNT=3 \
    run_capture min-tables nonzero "below the --min-tables floor" --min-tables 8
if [ ! -f "${TMP}/out/min-tables/GOOD-SNAPSHOT-SENTINEL" ]; then
    bad "min-tables: destroyed the pre-existing snapshot"
else
    pass "min-tables: pre-existing snapshot preserved"
fi

# 5. Zero-table snapshot is refused even with --min-tables 1.
STUB_MODE=tables STUB_TABLE_COUNT=0 \
    run_capture zero-tables nonzero "holds no dumped tables" --min-tables 1
[ -d "${TMP}/out/zero-tables" ] && bad "zero-tables: wrote a snapshot dir"

# 6. Happy path: publishes, replaces whatever was there, leaves no debris.
mkdir -p "${TMP}/out/publish"
echo stale > "${TMP}/out/publish/STALE-SENTINEL"
STUB_MODE=tables STUB_TABLE_COUNT=12 \
    run_capture publish zero "" --min-tables 8
n_parquet="$(find "${TMP}/out/publish" -name '*.parquet' 2>/dev/null | wc -l)"
if [ "${n_parquet}" -ne 12 ]; then
    bad "publish: expected 12 parquet tables, found ${n_parquet}"
elif [ -f "${TMP}/out/publish/STALE-SENTINEL" ]; then
    bad "publish: stale file from the previous snapshot survived the swap"
else
    pass "publish: 12 tables published, previous snapshot replaced"
fi

# ---------------------------------------------------------------------------
# 7. Log-scan cases. Every string below is one that walked past the old
#    four-literal MOVES_FAILURE_MARKERS set. Each runs with a FULL 12-table
#    dump (STUB_MODE=tables), so the zero-table belt and the --min-tables
#    floor are both satisfied and the log scan is the only thing that can
#    refuse the capture. If someone reverts the scan to literal markers,
#    these go red and the ones above stay green.
#
#    Sources: /scratch/$USER/scalescope-trial/trial{1,2,3,4}.log, the four
#    project-scale runs of 2026-09-10 that all printed "MOVES run OK".
# ---------------------------------------------------------------------------
scan_case() {
    local name="$1" line="$2" needle="$3"
    STUB_MODE=tables STUB_TABLE_COUNT=12 STUB_MOVES_EXTRA="${line}" \
        run_capture "${name}" nonzero "${needle}" --min-tables 8
    if [ -d "${TMP}/out/${name}" ]; then
        bad "${name}: wrote a snapshot dir despite a failure line in the log"
    fi
}

# trial1 (7 occurrences), trials 1-2.
scan_case scan-validate-data-status \
    'ERROR: Unable to validate input database Data Status' \
    'Unable to validate input database Data Status'

# trial1.
scan_case scan-offnetwork-links \
    'ERROR: Unable to count offNetwork links' \
    'Unable to count offNetwork links'

# All four trials. The headless-container Swing failure that started them.
scan_case scan-importer-instantiator \
    'ERROR: ImporterInstantiator is unable to instantiate gov.epa.otaq.moves.master.implementation.importers.GenericImporter' \
    'ImporterInstantiator is unable to instantiate'

# trial2. Note the doubled "ERROR: ERROR:" — MOVESAPI re-logs a message that
# already carries its own "ERROR:" prefix.
scan_case scan-avft-missing \
    'ERROR: ERROR: AVFT table does not exist. You may need to recreate your database to solve this problem.' \
    'AVFT table does not exist'

# trial3.
scan_case scan-avft-not-imported \
    'ERROR: ERROR: AVFT table is not imported.' \
    'AVFT table is not imported'

# The process-apu-single incident the old marker set was widened for. It has
# to keep working through the general rule, not through its own literal.
scan_case scan-required-county \
    'ERROR: Error: The database does not have the required county.' \
    'does not have the required county'

# --- The decision on warning-shaped ERROR lines --------------------------
# trial4:124-126 log data-quality WARNINGS at ERROR level. They are treated
# as failures ON PURPOSE, and this case pins that so nobody "fixes" it into
# an allowlist entry without reading the reasoning in run-fixture.sh.
#
# The reason: MOVESAPI.java:870-876 re-logs the entire importer message list
# at LogMessageCategory.ERROR only inside `if(result < 0) { ... return
# false; }` — the branch that refuses to launch the run. The "Missing: "
# prefix comes from ImporterBase.java:409-411, which prepends it to any
# quality message not already starting with "error". So these lines are
# emitted only on the abort path: warning wording, failure semantics.
scan_case scan-missing-warning-fuel-formulation \
    'ERROR: Missing: Warning: Fuel formulation 2675 changed fuelSubtypeID from 12 to 13 based on ETOHVolume' \
    'Missing: Warning: Fuel formulation 2675'
scan_case scan-missing-warning-fuel-type \
    'ERROR: Missing: Warning: Fuel type 3 is imported but will not be used' \
    'Missing: Warning: Fuel type 3'

# ---------------------------------------------------------------------------
# 8. NEGATIVE case — the false-positive floor. `RUN_ERROR:` is MOVES's
#    in-simulation category (Logger.java:78-80 rewrites WARNING and ERROR to
#    it between MOVESEngine.java:457 and :1245), and it carries benign
#    FuelSupply fallbacks. This exact wording occurs in 6 of the 42 published
#    snapshots' run logs. A scan that greps for a bare `ERROR` substring
#    instead of a word-boundary `ERROR:` token turns all six into failures;
#    this case is what stops that from shipping.
#
#    A wrapper that refuses everything is the same defect with the sign
#    flipped, so this must publish and exit 0.
# ---------------------------------------------------------------------------
STUB_MODE=tables STUB_TABLE_COUNT=12 \
STUB_MOVES_EXTRA='RUN_ERROR: WARNING: Using default formulation 20 for Diesel Fuel in region 270000000, year 2020, month 8. Check your input FuelSupply table for errors.' \
    run_capture run-error-warning-is-not-a-failure zero "" --min-tables 8
if [ ! -f "${TMP}/out/run-error-warning-is-not-a-failure/manifest.json" ]; then
    bad "run-error-warning-is-not-a-failure: RUN_ERROR warning blocked a good capture"
else
    pass "run-error-warning-is-not-a-failure: RUN_ERROR: WARNING: does not trip the scan"
fi

# A second negative pinning the other half of the pattern. This line carries
# `ERROR` in upper case AND immediately followed by a colon — but as the tail
# of the MOVESERROR table name, so the left word boundary is what has to
# reject it, exactly as it rejects RUN_ERROR. It also carries the lower-case
# word "errors" in prose, so a case-insensitive scan fails here too.
STUB_MODE=tables STUB_TABLE_COUNT=12 \
STUB_MOVES_EXTRA='INFO: Checking MOVESERROR: 0 rows, the input database has no errors.' \
    run_capture embedded-error-token-is-not-a-failure zero "" --min-tables 8
if [ ! -f "${TMP}/out/embedded-error-token-is-not-a-failure/manifest.json" ]; then
    bad "embedded-error-token-is-not-a-failure: MOVESERROR:/errors blocked a good capture"
else
    pass "embedded-error-token-is-not-a-failure: MOVESERROR: and prose 'errors' do not trip the scan"
fi

# ---------------------------------------------------------------------------
# 9. The run log must be published INTO the snapshot. This is what makes the
#    whole class auditable later without re-running MOVES, so it is a guard,
#    not a nicety.
# ---------------------------------------------------------------------------
PUBLISHED_LOG="${TMP}/out/publish/moves-run.log"
if [ ! -f "${PUBLISHED_LOG}" ]; then
    bad "publish: snapshot has no moves-run.log"
elif ! grep -q "MOVES run finished normally" "${PUBLISHED_LOG}"; then
    bad "publish: published moves-run.log is not the run log"
else
    pass "publish: moves-run.log published alongside manifest.json"
fi
# ...and a refused capture must leave none behind, since the log rides the
# same stage-then-swap path as everything else.
if [ -e "${TMP}/out/scan-importer-instantiator" ]; then
    bad "scan-importer-instantiator: left a snapshot (and therefore a log) behind"
else
    pass "failed capture leaves no moves-run.log behind"
fi

# ---------------------------------------------------------------------------
# 10. The .gitignore trap. moves-run.log is, by name, exactly what a blanket
#     `*.log` rule eats. If that ever lands above the negation in
#     .gitignore, all 42 published logs vanish from the tree silently — no
#     error, no diff, nothing to notice. This is the tripwire.
#     Skipped (not failed) outside a git checkout.
# ---------------------------------------------------------------------------
if git -C "${CHAR_DIR}" rev-parse --git-dir >/dev/null 2>&1; then
    GI_PROBE="characterization/snapshots/__gitignore_probe__/moves-run.log"
    if git -C "${CHAR_DIR}" check-ignore -q "${GI_PROBE}"; then
        bad "gitignore: ${GI_PROBE} is ignored — a *.log rule is eating published run logs" \
            "(fix: keep the '!characterization/snapshots/*/moves-run.log' negation LAST in .gitignore)"
    else
        pass "gitignore: published snapshot run logs are trackable"
    fi
else
    echo "skip — gitignore check (not a git checkout)"
fi

echo
if [ "${FAILURES}" -eq 0 ]; then
    echo "=== all guards behaved ==="
    exit 0
fi
echo "=== ${FAILURES} guard case(s) regressed ===" >&2
exit 1
