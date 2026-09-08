#!/bin/bash
# run-fixture-guards.sh — regression test for run-fixture.sh's capture guards
# (issue #56: the wrapper exited 0 when MOVES failed and wrote a zero-table
# snapshot that was reported as a successful capture).
#
# The test drives the REAL run-fixture.sh and the REAL run-moves.sh; only
# `apptainer` and the `moves-fixture-capture` binary are stubbed, so the code
# under test — status propagation, the "Java Result" log scan, the
# empty-captures refusal, the --min-tables floor, and the stage-then-publish
# swap — is exercised as shipped. No MOVES SIF, no Apptainer and no cargo
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
            printf '%s\n' \
                "main1worker:" \
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

echo "=== run-fixture.sh guard regression (issue #56) ==="

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

echo
if [ "${FAILURES}" -eq 0 ]; then
    echo "=== all guards behaved ==="
    exit 0
fi
echo "=== ${FAILURES} guard case(s) regressed ===" >&2
exit 1
