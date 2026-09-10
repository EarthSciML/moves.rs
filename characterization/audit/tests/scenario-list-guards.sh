#!/bin/bash
# scenario-list-guards.sh — regression test for run-comparison.sh's scenario
# pre-flight (issue #63).
#
# The defect: typical-scenarios.txt named `mixed-onroad-nonroad` after
# 2122513e deleted that fixture. run-comparison.sh printed `[SKIP]`, ran the
# survivors, assembled a report, and exited 1 only at the very end — which in
# CI killed the comparison step, so the step that runs regression_gate.sh
# never executed. The audit regression gate was dead, not merely red, and it
# read on every PR as that PR's own numerical regression.
#
# These cases pin the two halves of the fix:
#   * a name that does not resolve is FATAL and IMMEDIATE (exit 2), before the
#     cargo build and before any MOVES run;
#   * the committed typical-scenarios.txt still resolves, entry for entry —
#     the tripwire that makes the next fixture retirement impossible to miss.
#
# Everything runs through `--check-scenarios`, so there is no cargo build, no
# Apptainer and no SIF: the whole suite is well under a second.
#
# Usage: characterization/audit/tests/scenario-list-guards.sh
# Exit code: 0 = every guard behaved, 1 = at least one case regressed.

set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AUDIT_DIR="$(cd "${HERE}/.." && pwd)"
ROOT="$(cd "${AUDIT_DIR}/../.." && pwd)"
RUN_COMPARISON="${AUDIT_DIR}/run-comparison.sh"
SCENARIO_FILE="${AUDIT_DIR}/typical-scenarios.txt"
FIXTURES_DIR="${ROOT}/characterization/fixtures"
SNAPSHOTS_DIR="${ROOT}/characterization/snapshots"

[ -x "${RUN_COMPARISON}" ] || { echo "FATAL: ${RUN_COMPARISON} not executable" >&2; exit 2; }
[ -f "${SCENARIO_FILE}" ]  || { echo "FATAL: ${SCENARIO_FILE} not found" >&2; exit 2; }

TMP="$(mktemp -d "${TMPDIR:-/tmp}/scenario-list-guards.XXXXXX")"
trap 'rm -rf "${TMP}"' EXIT

FAILURES=0
pass() { printf 'ok   — %s\n' "$1"; }
bad()  { printf 'FAIL — %s\n' "$1" >&2; FAILURES=$((FAILURES + 1)); }

# Fail the run if `cargo` is ever reached: the whole point of the pre-flight
# is that a stale list is refused BEFORE the expensive part. A stub named
# `cargo` shadows the real one on PATH and exits non-zero with a marker.
mkdir -p "${TMP}/bin"
cat > "${TMP}/bin/cargo" <<'STUB'
#!/bin/bash
echo "CARGO-WAS-INVOKED" >&2
exit 97
STUB
chmod +x "${TMP}/bin/cargo"
export PATH="${TMP}/bin:${PATH}"

# check <name> <expect_rc> <expected stderr substring or ''> [args...]
check() {
    local name="$1" expect_rc="$2" needle="$3"; shift 3
    local log="${TMP}/${name}.log"
    "${RUN_COMPARISON}" "$@" > "${log}" 2>&1
    local rc=$?
    if [ "${rc}" -ne "${expect_rc}" ]; then
        bad "${name}: expected exit ${expect_rc}, got ${rc} (see ${log})"
        sed -n '1,20p' "${log}" >&2
        return
    fi
    if [ -n "${needle}" ] && ! grep -qF -- "${needle}" "${log}"; then
        bad "${name}: output did not mention '${needle}'"
        return
    fi
    if grep -q 'CARGO-WAS-INVOKED' "${log}"; then
        bad "${name}: reached the cargo build — the pre-flight did not fire first"
        return
    fi
    pass "${name}"
}

echo "=== run-comparison.sh scenario pre-flight guards (issue #63) ==="

# 1. THE REPORTED BUG: the exact retired name. Paired with a name that DOES
#    resolve, deliberately — with the dead name alone the list resolves to
#    zero scenarios, and case 4's empty-list refusal would carry this case
#    even if the stale-name refusal were downgraded back to a skip. The pair
#    forces the refusal to come from the stale name itself.
check dangling-name 2 'SCENARIO LIST IS STALE' \
    --check-scenarios --fixtures mixed-onroad-nonroad,SampleRunSpec
if ! grep -q -- '- mixed-onroad-nonroad' "${TMP}/dangling-name.log"; then
    bad "dangling-name: banner did not name the offending scenario"
else
    pass "dangling-name: banner names the offending scenario"
fi

# 2. Exit 2, not 1. `1` means "a fixture regressed" and demands a completely
#    different response from "the list is wrong". Conflating them is how the
#    dead gate read as a numerical failure for months.
check exit-code-is-config-not-regression 2 '' \
    --check-scenarios --fixtures mixed-onroad-nonroad,SampleRunSpec

# 3. Every dead name in one pass, not one retirement at a time.
check reports-all-missing 2 '3 name(s) do not resolve' \
    --check-scenarios --fixtures ghost-one,mixed-onroad,ghost-two,ghost-three
for ghost in ghost-one ghost-two ghost-three; do
    grep -q -- "- ${ghost}" "${TMP}/reports-all-missing.log" \
        || bad "reports-all-missing: ${ghost} missing from the banner"
done
grep -q -- '- mixed-onroad$' "${TMP}/reports-all-missing.log" \
    && bad "reports-all-missing: listed a name that DOES resolve"

# 4. A list that resolves to nothing must not read as success.
check empty-list 2 'resolved to zero scenarios' --check-scenarios --fixtures ' '

# 5. NEGATIVE. A wrapper that refuses everything is the same defect with the
#    sign flipped, so a good list must pass.
check good-list 0 'resolve to a fixture XML' \
    --check-scenarios --fixtures mixed-onroad,nr-mixed-nonroad,SampleRunSpec

# 6. THE TRIPWIRE. The committed list, exactly as CI reads it. This is the
#    case that goes red the next time a fixture is retired without updating
#    typical-scenarios.txt.
check committed-list 0 'resolve to a fixture XML' --check-scenarios

# 7. The audit-regression-gate job runs with no SIF and no Apptainer, relying
#    on every scenario having a COMMITTED snapshot (see the workflow header).
#    A name that resolves to an XML but has no snapshot would send the job
#    down the run-fixture.sh path and fail on the missing SIF — a different
#    silent-ish hole in the same list. Check the whole list, artifact by
#    artifact, rather than trusting the pre-flight's XML check alone.
normalize() { printf '%s' "$1" | tr '[:upper:]' '[:lower:]' | tr -cd 'a-z0-9'; }
snapshot_gaps=0
checked=0
while IFS= read -r scenario; do
    scenario="$(printf '%s' "${scenario}" | tr -d '[:space:]')"
    [ -z "${scenario}" ] && continue
    target="$(normalize "${scenario}")"
    stem=""
    for f in "${FIXTURES_DIR}"/*.xml; do
        base="${f##*/}"; base="${base%.xml}"
        [ "$(normalize "${base}")" = "${target}" ] && { stem="${base}"; break; }
    done
    if [ -z "${stem}" ]; then
        bad "snapshot-coverage: ${scenario} has no fixture XML (pre-flight should have caught this)"
        snapshot_gaps=$((snapshot_gaps + 1))
        continue
    fi
    fname="$(printf '%s' "${stem}" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9_-]/_/g')"
    checked=$((checked + 1))
    if [ ! -f "${SNAPSHOTS_DIR}/${fname}/manifest.json" ]; then
        bad "snapshot-coverage: ${scenario} -> ${fname} has no committed snapshot manifest.json"
        snapshot_gaps=$((snapshot_gaps + 1))
    fi
done < <(grep -v '^[[:space:]]*\(#\|$\)' "${SCENARIO_FILE}")
if [ "${checked}" -eq 0 ]; then
    bad "snapshot-coverage: read zero scenarios from ${SCENARIO_FILE}"
elif [ "${snapshot_gaps}" -eq 0 ]; then
    pass "snapshot-coverage: all ${checked} committed scenarios have a snapshot manifest.json"
fi

# 8. The retired fixture must be gone for good — not just off the list. If
#    someone re-adds the XML without re-reading 2122513e, case 1 above stops
#    testing anything (the name would resolve) and this says why.
if [ -e "${FIXTURES_DIR}/mixed-onroad-nonroad.xml" ]; then
    bad "retired-fixture: mixed-onroad-nonroad.xml is back — 2122513e retired it because canonical MOVES 5.0.1 cannot run ONROAD+NONROAD in one RunSpec"
else
    pass "retired-fixture: mixed-onroad-nonroad.xml stays retired"
fi

echo
if [ "${FAILURES}" -eq 0 ]; then
    echo "=== all guards behaved ==="
    exit 0
fi
echo "=== ${FAILURES} guard case(s) regressed ===" >&2
exit 1
