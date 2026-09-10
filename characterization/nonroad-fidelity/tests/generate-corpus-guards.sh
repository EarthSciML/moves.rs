#!/bin/bash
# generate-corpus-guards.sh — regression test for generate-corpus.sh's
# capture guards (issue #60: the corpus generator could record a STALE run's
# TSV as a fresh gfortran baseline).
#
# Why this matters more than the #56 case it mirrors: the artifact here is a
# reference corpus. A stale or truncated TSV recorded as a baseline passes the
# corpus' own integrity check — the SHA genuinely matches the bytes — so
# nothing downstream can tell. The only place the lie can be caught is here.
#
# The test drives the REAL generate-corpus.sh and the REAL run-moves.sh.
# Stubbed: `apptainer` (the MOVES run), and `pkill`/`ss`/`lsof`, which the
# real script uses to clear port 3306 and which must never run for real on a
# shared machine or in CI. No SIF, no Apptainer, no cargo, ~2 s.
#
# Usage: characterization/nonroad-fidelity/tests/generate-corpus-guards.sh
# Exit code: 0 = every guard behaved, 1 = at least one case regressed.

set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NR_DIR="$(cd "${HERE}/.." && pwd)"
CHAR_DIR="$(cd "${NR_DIR}/.." && pwd)"
GENERATE="${NR_DIR}/generate-corpus.sh"

# A real fixture: generate-corpus.sh stages ../fixtures/<name>.xml before the
# run, so the name has to exist on disk. nr-logging-county is the smallest
# fixture in the shipped corpus (1266 rows).
FIXTURE="nr-logging-county"

[ -x "${GENERATE}" ] || { echo "FATAL: ${GENERATE} not executable" >&2; exit 2; }
[ -f "${CHAR_DIR}/fixtures/${FIXTURE}.xml" ] \
    || { echo "FATAL: ${CHAR_DIR}/fixtures/${FIXTURE}.xml not found" >&2; exit 2; }

TMP="$(mktemp -d "${TMPDIR:-/tmp}/generate-corpus-guards.XXXXXX")"
trap 'rm -rf "${TMP}"' EXIT
mkdir -p "${TMP}/bin" "${TMP}/scratch" "${TMP}/baselines"
touch "${TMP}/fake.sif" "${TMP}/fake-NONROAD.exe"
printf '%s\n' "${FIXTURE}" > "${TMP}/FIXTURES"

# ----- Stub apptainer -------------------------------------------------------
# generate-corpus.sh -> run-moves.sh -> `apptainer exec ... --bind
# <host>:/opt/moves/MOVESTemporary ... --env NRDBG_FILE=<container path>`.
# The stub recovers the host MOVESTemporary from the bind and writes (or
# pointedly does not write) the capture there. STUB_MODE selects the failure.
cat > "${TMP}/bin/apptainer" <<'STUB'
#!/bin/bash
if [ "$1" = "--version" ]; then echo "apptainer version stub"; exit 0; fi
STUB_MODE="${STUB_MODE:-ok}"

# Both the capture's destination and its name come out of the real argv the
# script built, so the stub follows a multi-fixture run without being told.
moves_temp=""
NRDBG_NAME=""
prev=""
for a in "$@"; do
    if [ "${prev}" = "--bind" ]; then
        case "$a" in *:/opt/moves/MOVESTemporary) moves_temp="${a%:/opt/moves/MOVESTemporary}" ;; esac
    fi
    case "$a" in NRDBG_FILE=*) NRDBG_NAME="${a##*/}" ;; esac
    prev="$a"
done
[ -n "${NRDBG_NAME}" ] || { echo "[stub] no NRDBG_FILE in argv" >&2; exit 91; }
[ -n "${moves_temp}" ] || { echo "[stub] no MOVESTemporary bind in argv" >&2; exit 90; }
tsv="${moves_temp}/${NRDBG_NAME}"

# Write a dbgemit-shaped capture with $1 data rows.
# Captures carry the fixture name so two fixtures differ, as real ones must.
# STUB_IDENTICAL=1 drops it, making two fixtures collide byte for byte.
write_tsv() {
    local n="$1" i=1
    local tag="${NRDBG_NAME%.tsv}"
    [ "${STUB_IDENTICAL:-0}" = "1" ] && tag="same"
    : > "${tsv}"
    while [ "${i}" -le "${n}" ]; do
        printf 'GETPOP\tfips=26161,scc=2270002006,year=2020,fixture=%s\tpopeqp\t1\t%d.0\n' \
            "${tag}" "${i}" >> "${tsv}"
        i=$((i + 1))
    done
}

emit_ok_log() {
    echo "main1worker:"
    echo "     [java] MOVES Master starting."
    if [ -n "${STUB_MOVES_EXTRA:-}" ]; then
        printf '%s\n' "${STUB_MOVES_EXTRA}" | while IFS= read -r l; do
            printf '     [java] 9/10/26, 2:52 AM %s\n' "$l"
        done
    fi
    printf '%s\n' \
        "     [java] Time spent on running nonroad.exe (sec): 4.601" \
        "     [java] MOVES run finished normally." \
        "     [java] Java Result: 0" \
        "BUILD SUCCESSFUL"
}

case "${STUB_MODE}" in
    moves_exit_nonzero)
        echo "[stub] apptainer exec failed"; exit 1 ;;
    java_result)
        # What the real stack does when MOVES dies: ant logs the JVM's status
        # and still exits 0. No capture is written.
        printf '%s\n' \
            "main1worker:" \
            "     [java] The specified runspec file does not exist:  fixtures/x.xml" \
            "     [java] Java Result: 1" \
            "BUILD SUCCESSFUL"
        exit 0 ;;
    no_capture)
        # THE ISSUE #60 SHAPE: MOVES "succeeds" by every signal the old script
        # read, and writes no capture at all.
        emit_ok_log; exit 0 ;;
    empty_capture)
        emit_ok_log; : > "${tsv}"; exit 0 ;;
    short_capture)
        emit_ok_log; write_tsv "${STUB_ROWS:-5}"; exit 0 ;;
    junk_capture)
        emit_ok_log
        printf 'Traceback (most recent call last):\n' > "${tsv}"
        printf 'some other garbage\n' >> "${tsv}"
        i=0; while [ "${i}" -lt 300 ]; do printf 'filler\n' >> "${tsv}"; i=$((i+1)); done
        exit 0 ;;
    nrerrors)
        emit_ok_log
        write_tsv "${STUB_ROWS:-500}"
        wt="${moves_temp}/manyworkers/workerfolder/WorkerTemp"
        mkdir -p "${wt}"
        printf 'ERROR: Fuel RVP is out of range for equipment type 2270002006\n' \
            > "${wt}/nrerrors.txt"
        exit 0 ;;
    *)
        emit_ok_log; write_tsv "${STUB_ROWS:-500}"; exit 0 ;;
esac
STUB

# ----- Stub cp, to simulate an interrupted copy -----------------------------
# The only way to exercise stage-then-swap is to break the copy partway. With
# STUB_CP_TRUNCATE=1 a copy whose destination is a .tsv (or a .tsv staging
# path) writes three lines and fails, exactly as a copy killed mid-write
# would. Every other copy — notably the runspec .xml generate-corpus.sh
# stages before the run — is passed straight through.
cat > "${TMP}/bin/cp" <<'STUB'
#!/bin/bash
if [ "${STUB_CP_TRUNCATE:-0}" = "1" ]; then
    dst="${!#}"
    case "${dst}" in
        *.tsv|*.tsv.staging.*)
            head -n 3 "$1" > "${dst}" 2>/dev/null
            echo "cp: interrupted" >&2
            exit 1 ;;
    esac
fi
exec /bin/cp "$@"
STUB

# ----- Stubs that must never run for real ----------------------------------
# generate-corpus.sh clears orphaned MOVES/MariaDB processes off port 3306.
# On a shared machine that would kill someone else's run; in CI there is
# nothing to kill. Stub them to no-ops.
for prog in pkill lsof; do
    printf '#!/bin/bash\nexit 0\n' > "${TMP}/bin/${prog}"
done
# `ss` must report port 3306 free, so the wait loop breaks on its first pass.
printf '#!/bin/bash\nexit 0\n' > "${TMP}/bin/ss"
chmod +x "${TMP}/bin/"*
export PATH="${TMP}/bin:${PATH}"

FAILURES=0
pass() { printf 'ok   — %s\n' "$1"; }
bad()  { printf 'FAIL — %s\n' "$1" >&2; FAILURES=$((FAILURES + 1)); }

BASELINE="${TMP}/baselines/${FIXTURE}.tsv"
CORPUS_SHA="${TMP}/baselines/corpus.sha"
WORKDIR="${TMP}/scratch/${FIXTURE}"
HOST_TSV="${WORKDIR}/MOVESTemporary/${FIXTURE}.tsv"

reset_state() {
    rm -rf "${TMP}/scratch" "${TMP}/baselines"
    mkdir -p "${TMP}/scratch" "${TMP}/baselines"
}

# run_corpus <name> <expect_rc: zero|nonzero> <expected reason substring or ''>
run_corpus() {
    local name="$1" expect="$2" reason="$3"
    local log="${TMP}/${name}.log"
    FORCE=1 \
    SIF="${TMP}/fake.sif" \
    NONROAD_EXE="${TMP}/fake-NONROAD.exe" \
    SCRATCH="${TMP}/scratch" \
    BASELINES_DIR="${TMP}/baselines" \
    FIXTURES_FILE="${TMP}/FIXTURES" \
    MIN_ROWS="${MIN_ROWS_OVERRIDE:-100}" \
        "${GENERATE}" > "${log}" 2>&1
    RC=$?
    if [ "${expect}" = "nonzero" ] && [ "${RC}" -eq 0 ]; then
        bad "${name}: expected non-zero exit, got 0 (see ${log})"; return 1
    fi
    if [ "${expect}" = "zero" ] && [ "${RC}" -ne 0 ]; then
        bad "${name}: expected exit 0, got ${RC} (see ${log})"
        sed -n '1,25p' "${log}" >&2
        return 1
    fi
    if [ "${expect}" = "nonzero" ]; then
        if ! grep -q "CAPTURE FAILED — NO BASELINE RECORDED" "${log}"; then
            bad "${name}: no CAPTURE FAILED banner in output"; return 1
        fi
        if [ -n "${reason}" ] && ! grep -qF -- "${reason}" "${log}"; then
            bad "${name}: banner reason did not mention '${reason}'"; return 1
        fi
    fi
    if ls -a "${TMP}/baselines" | grep -q '\.staging\.'; then
        bad "${name}: left a staging file behind"; return 1
    fi
    pass "${name}"
    return 0
}

echo "=== generate-corpus.sh capture guards (issue #60) ==="

# ---------------------------------------------------------------------------
# 1. THE REPORTED BUG, end to end and by artifact.
#
#    A previous run's TSV sits in the workdir. MOVES then fails in the way the
#    old script could not see (ant prints "Java Result: 1" and exits 0). The
#    old code's only guard, `[ -f "${nrdbg_host}" ]`, passed on the STALE
#    file and copied it into baselines/ as a fresh gfortran baseline.
#
#    Checked by artifact, not by log line: the stale bytes carry a sentinel,
#    and the assertions are that no baseline file exists, that corpus.sha
#    records nothing, and that the stale bytes are gone from the workdir.
# ---------------------------------------------------------------------------
reset_state
mkdir -p "${WORKDIR}/MOVESTemporary"
{
    printf 'GETPOP\tSTALE-SENTINEL-FROM-A-PREVIOUS-RUN\tpopeqp\t1\t1.0\n'
    i=0; while [ "${i}" -lt 400 ]; do printf 'GETPOP\tfips=1\tpopeqp\t1\t2.0\n'; i=$((i+1)); done
} > "${HOST_TSV}"
STALE_SHA="$(sha256sum "${HOST_TSV}" | awk '{print $1}')"

STUB_MODE=java_result run_corpus stale-tsv-after-java-result nonzero "non-zero Java Result"
if [ -e "${BASELINE}" ]; then
    bad "stale-tsv: a baseline was written — sha $(sha256sum "${BASELINE}" | awk '{print $1}') vs stale ${STALE_SHA}"
elif [ -e "${CORPUS_SHA}" ]; then
    bad "stale-tsv: corpus.sha exists — the failed run was recorded"
elif [ -e "${HOST_TSV}" ]; then
    bad "stale-tsv: the previous run's TSV survived into the run — the pre-run rm did not happen"
else
    pass "stale-tsv: no baseline, no corpus.sha, and the stale TSV was removed before the run"
fi

# 2. run-moves.sh / apptainer exits non-zero.
reset_state
STUB_MODE=moves_exit_nonzero run_corpus moves-exit-nonzero nonzero "run-moves.sh exited 1"
[ -e "${BASELINE}" ] && bad "moves-exit-nonzero: wrote a baseline"

# 3. Everything "succeeds" but no capture is produced. This is the shape the
#    original existence check was meant to catch, now over a clean workdir.
reset_state
STUB_MODE=no_capture run_corpus no-capture nonzero "NRDBG_FILE not produced"
[ -e "${BASELINE}" ] && bad "no-capture: wrote a baseline"

# 4. A zero-byte capture. The old code had no size check.
reset_state
STUB_MODE=empty_capture run_corpus empty-capture nonzero "is empty"
[ -e "${BASELINE}" ] && bad "empty-capture: wrote a baseline"

# 5. A truncated capture: real dbgemit rows, far too few of them. The old code
#    had no row floor, so this became a baseline.
reset_state
STUB_MODE=short_capture STUB_ROWS=5 run_corpus short-capture nonzero "below the MIN_ROWS floor"
[ -e "${BASELINE}" ] && bad "short-capture: wrote a baseline"

# 6. A file of the right size that is not a capture at all — 302 rows, past
#    the row floor, but field 1 is not a dbgemit phase.
reset_state
STUB_MODE=junk_capture run_corpus junk-capture nonzero "does not look like a dbgemit capture"
[ -e "${BASELINE}" ] && bad "junk-capture: wrote a baseline"

# ---------------------------------------------------------------------------
# 7. NONROAD's own failure. This is the case the inherited MOVES log scan
#    provably cannot see: NONROAD.exe's stdout goes to NonroadProcessOutput
#    .txt rather than the ant log, its exit status is discarded, and the
#    errors MOVES does re-log arrive as `RUN_ERROR:` because the worker runs
#    inside the simulation window (Logger.java:78-80, MOVESEngine.java:457).
#    nrerrors.txt is the one signal that survives. The stub writes a FULL,
#    valid 500-row capture alongside it, so every other guard is satisfied
#    and only the nrerrors check can refuse this.
# ---------------------------------------------------------------------------
reset_state
STUB_MODE=nrerrors run_corpus nonroad-errors nonzero "NONROAD reported an error"
if [ -e "${BASELINE}" ]; then
    bad "nonroad-errors: wrote a baseline despite nrerrors.txt"
else
    pass "nonroad-errors: a valid-looking capture is still refused when NONROAD errored"
fi

# 8. A STALE nrerrors.txt from a previous run must not fail a clean run — the
#    same stale-artifact defect, sign-flipped. It is removed before the run.
reset_state
mkdir -p "${WORKDIR}/MOVESTemporary/manyworkers/workerfolder/WorkerTemp"
printf 'ERROR: left over from a previous run\n' \
    > "${WORKDIR}/MOVESTemporary/manyworkers/workerfolder/WorkerTemp/nrerrors.txt"
STUB_MODE=ok STUB_ROWS=500 run_corpus stale-nrerrors zero ""
if [ ! -s "${BASELINE}" ]; then
    bad "stale-nrerrors: a previous run's nrerrors.txt blocked a good capture"
else
    pass "stale-nrerrors: previous run's nrerrors.txt removed, capture published"
fi

# ---------------------------------------------------------------------------
# 9. Log-scan cases. Inherited from run-fixture.sh via lib/moves-log-scan.sh.
#    Each runs with a FULL 500-row capture, so every artifact guard is
#    satisfied and the log scan is the only thing that can refuse.
# ---------------------------------------------------------------------------
scan_case() {
    local name="$1" line="$2" needle="$3"
    reset_state
    STUB_MODE=ok STUB_ROWS=500 STUB_MOVES_EXTRA="${line}" \
        run_corpus "${name}" nonzero "${needle}"
    [ -e "${BASELINE}" ] && bad "${name}: wrote a baseline despite a failure line in the log"
    return 0
}

scan_case scan-validate-data-status \
    'ERROR: Unable to validate input database Data Status' \
    'Unable to validate input database Data Status'

scan_case scan-importer-instantiator \
    'ERROR: ImporterInstantiator is unable to instantiate gov.epa.otaq.moves.master.implementation.importers.GenericImporter' \
    'ImporterInstantiator is unable to instantiate'

scan_case scan-required-county \
    'ERROR: Error: The database does not have the required county.' \
    'does not have the required county'

# ---------------------------------------------------------------------------
# 10. NEGATIVE — the false-positive floor. `RUN_ERROR: WARNING:` is the benign
#     in-simulation FuelSupply fallback that occurs in 6 of the 42 published
#     snapshots' run logs, TWO of them nr-* fixtures this very script runs
#     (nr-construction-state, nr-pleasure-craft-state). A scan that greps for
#     a bare `ERROR` substring turns those two fixtures into permanent corpus
#     failures. A generator that refuses everything is the same defect with
#     the sign flipped.
# ---------------------------------------------------------------------------
reset_state
STUB_MODE=ok STUB_ROWS=500 \
STUB_MOVES_EXTRA='RUN_ERROR: WARNING: Using default formulation 20 for Diesel Fuel in region 270000000, year 2020, month 8. Check your input FuelSupply table for errors.' \
    run_corpus run-error-warning-is-not-a-failure zero ""
if [ ! -s "${BASELINE}" ]; then
    bad "run-error-warning-is-not-a-failure: a RUN_ERROR warning blocked a good capture"
else
    pass "run-error-warning-is-not-a-failure: RUN_ERROR: WARNING: does not trip the scan"
fi

# ---------------------------------------------------------------------------
# 11. HAPPY PATH, by artifact. The published baseline must be byte-identical
#     to what the run produced, and corpus.sha and MANIFEST.toml must agree
#     with the file on disk — the corpus' own integrity contract.
# ---------------------------------------------------------------------------
reset_state
STUB_MODE=ok STUB_ROWS=500 run_corpus publish zero ""
if [ ! -s "${BASELINE}" ]; then
    bad "publish: no baseline written"
else
    src_sha="$(sha256sum "${HOST_TSV}" | awk '{print $1}')"
    out_sha="$(sha256sum "${BASELINE}" | awk '{print $1}')"
    rows="$(wc -l < "${BASELINE}")"
    rec_sha="$(awk -v f="${FIXTURE}" 'BEGIN{FS="\t"} $1==f {print $2}' "${CORPUS_SHA}")"
    rec_rows="$(awk -v f="${FIXTURE}" 'BEGIN{FS="\t"} $1==f {print $3}' "${CORPUS_SHA}")"
    man_sha="$(awk -F'"' '/^sha256/{print $2}' "${TMP}/baselines/MANIFEST.toml")"
    man_rows="$(awk -F'= *' '/^rows/{gsub(/ /,"",$2); print $2}' "${TMP}/baselines/MANIFEST.toml")"
    if [ "${rows}" -ne 500 ]; then
        bad "publish: baseline has ${rows} rows, expected 500"
    elif [ "${src_sha}" != "${out_sha}" ]; then
        bad "publish: baseline sha ${out_sha} != captured sha ${src_sha}"
    elif [ "${rec_sha}" != "${out_sha}" ] || [ "${rec_rows}" != "500" ]; then
        bad "publish: corpus.sha records ${rec_sha}/${rec_rows}, file is ${out_sha}/500"
    elif [ "${man_sha}" != "${out_sha}" ] || [ "${man_rows}" != "500" ]; then
        bad "publish: MANIFEST.toml records ${man_sha}/${man_rows}, file is ${out_sha}/500"
    else
        pass "publish: 500 rows, baseline == capture, corpus.sha and MANIFEST.toml agree"
    fi
fi

# ---------------------------------------------------------------------------
# 12. A failure AFTER a good baseline already exists must leave the good one
#     intact and must not re-record it as this run's result. The old script's
#     bare `cp` into baselines/ had no staging step.
# ---------------------------------------------------------------------------
GOOD_SHA="$(sha256sum "${BASELINE}" | awk '{print $1}')"
STUB_MODE=java_result run_corpus failure-after-good-baseline nonzero "non-zero Java Result"
if [ ! -f "${BASELINE}" ]; then
    bad "failure-after-good-baseline: destroyed the existing good baseline"
elif [ "$(sha256sum "${BASELINE}" | awk '{print $1}')" != "${GOOD_SHA}" ]; then
    bad "failure-after-good-baseline: the existing baseline was overwritten"
else
    pass "failure-after-good-baseline: existing good baseline untouched"
fi

# ---------------------------------------------------------------------------
# 13. Stage-then-swap, the only case that discriminates it. A copy killed
#     partway through writes a short file and exits non-zero. Copying
#     straight into baselines/<fixture>.tsv leaves those three lines sitting
#     where a finished baseline goes — a truncated artifact whose SHA matches
#     its bytes, which is this issue's whole failure mode. Copying into a
#     staging path and mv-ing means the abort leaves baselines/ untouched.
#
#     Verified to discriminate: replacing the staged copy with a plain
#     `cp "${nrdbg_host}" "${baseline}"` takes this case red and no other.
# ---------------------------------------------------------------------------
reset_state
STUB_MODE=ok STUB_ROWS=500 STUB_CP_TRUNCATE=1     run_corpus interrupted-copy nonzero ""
if [ -e "${BASELINE}" ]; then
    bad "interrupted-copy: a partial baseline ($(wc -l < "${BASELINE}") rows) was left in baselines/"
elif [ -e "${CORPUS_SHA}" ]; then
    bad "interrupted-copy: corpus.sha recorded a copy that never completed"
else
    pass "interrupted-copy: baselines/ untouched, no corpus.sha entry"
fi

# ---------------------------------------------------------------------------
# 14. Cross-fixture duplicate captures. Two different fixtures with a
#     byte-identical capture means the corpus reports coverage it does not
#     have: the fidelity gate diffs the port against the same reference
#     twice. This is NOT the #60 failure mode — workdirs are per-fixture, so
#     a stale TSV cannot cross between them — which is why it needs its own
#     check. It is live in the shipped corpus: nr-airport-support-county and
#     nr-industrial-county both record caa856d8…, 49300684 bytes, 312481
#     rows, from RunSpecs selecting different sectors and pollutants.
#
#     A warning rather than a refusal, so the run must still exit 0.
# ---------------------------------------------------------------------------
SECOND_FIXTURE="nr-commercial-nation"
if [ ! -f "${CHAR_DIR}/fixtures/${SECOND_FIXTURE}.xml" ]; then
    bad "duplicate-captures: ${SECOND_FIXTURE}.xml not found — cannot run the two-fixture case"
else
    printf '%s\n%s\n' "${FIXTURE}" "${SECOND_FIXTURE}" > "${TMP}/FIXTURES"

    reset_state
    STUB_MODE=ok STUB_ROWS=500 STUB_IDENTICAL=1 \
        run_corpus duplicate-captures zero ""
    dlog="${TMP}/duplicate-captures.log"
    if ! grep -q 'WARNING: fixtures with IDENTICAL captures' "${dlog}"; then
        bad "duplicate-captures: two identical captures were recorded with no warning"
    elif ! grep -q "${FIXTURE} == ${SECOND_FIXTURE}" "${dlog}"; then
        bad "duplicate-captures: warning did not name both fixtures"
    else
        pass "duplicate-captures: identical captures across two fixtures are called out"
    fi

    # NEGATIVE: two fixtures that genuinely differ must produce no warning.
    reset_state
    STUB_MODE=ok STUB_ROWS=500 run_corpus distinct-captures zero ""
    if grep -q 'WARNING: fixtures with IDENTICAL captures' "${TMP}/distinct-captures.log"; then
        bad "distinct-captures: warned about fixtures whose captures differ"
    elif [ "$(sha256sum "${TMP}/baselines/${FIXTURE}.tsv" | awk '{print $1}')" \
         = "$(sha256sum "${TMP}/baselines/${SECOND_FIXTURE}.tsv" | awk '{print $1}')" ]; then
        bad "distinct-captures: the two baselines are identical — the case tests nothing"
    else
        pass "distinct-captures: differing captures draw no warning"
    fi

    printf '%s\n' "${FIXTURE}" > "${TMP}/FIXTURES"
fi

echo
if [ "${FAILURES}" -eq 0 ]; then
    echo "=== all guards behaved ==="
    exit 0
fi
echo "=== ${FAILURES} guard case(s) regressed ===" >&2
exit 1
