#!/bin/bash
# moves-log-scan.sh — the shared MOVES run-log failure scanner.
#
# SOURCE this; do not execute it. It defines MOVES_FAILURE_PATTERNS and
# moves_log_failure_reason() and does nothing else.
#
#   source "<apptainer-dir>/lib/moves-log-scan.sh"
#   if ! reason="$(moves_log_failure_reason "${log}")"; then
#       fail "${reason}"
#   fi
#
# Two callers, one rule, deliberately: both drive the same run-moves.sh
# against the same SIF and both read the same ant log, so a failure shape one
# of them learns about has to reach the other.
#
#   * characterization/apptainer/run-fixture.sh          (issues #56, #64)
#   * characterization/nonroad-fidelity/generate-corpus.sh (issue #60)
#
# The false-positive and true-positive measurements recorded below were taken
# through run-fixture.sh; see generate-corpus.sh's own header for what this
# scan can and cannot see on the NONROAD path, which is a strictly narrower
# claim.

# ----- MOVES failure detection in the run log -------------------------------
#
# ant's <java> task for MOVES has no failonerror="true" (MOVES's own
# build.xml, target main1worker), so a MOVES that dies still reaches this
# script as exit 0. The run log is the only witness, which makes this scan
# the primary detector and the table-count checks the belts behind it.
#
# THE RULE: a line carrying the bare token `ERROR:` is a MOVES failure.
# The pattern is word-boundary anchored on the left, so `RUN_ERROR:` does
# NOT match. Three non-Logger literals are matched as well, for the ant and
# argument-handling paths that never reach MOVES's Logger at all.
#
# Why word-boundary `ERROR:` is the right cut, from MOVES's own source
# (paths are inside moves-fixture.sif under /opt/moves):
#
#   * common/Logger.java:83 renders every message as
#         "<timestamp> <CATEGORY>: <message>"
#     so a bare `ERROR:` token is exactly LogMessageCategory.ERROR, which
#     common/LogMessageCategory.java:41-42 documents as "an error, the
#     program cannot continue to run".
#   * common/Logger.java:78-80 rewrites BOTH WARNING and ERROR to the
#     RUN_ERROR category whenever `Logger.shouldPromoteErrorLevel` is set,
#     and master/framework/MOVESEngine.java sets it at line 457 (simulation
#     start) and clears it at line 1245 (simulation end). `ERROR:` is
#     therefore emitted only OUTSIDE the simulation window — RunSpec
#     parsing, domain-database validation, importer construction,
#     output-database creation — where MOVES has not begun computing and an
#     ERROR is always terminal. See RESIDUAL GAP below for what this costs.
#
# FALSE-POSITIVE RATE, MEASURED. Corpus: the MOVES run log of every one of
# the 42 published snapshots in characterization/snapshots/, i.e. the
# moves-snapshot/v2 recapture sweep of 2026-09-08/09, retained under
# /scratch/$USER/moves-fixture/<fixture>/moves-run.log (38 fixtures) and
# /scratch/$USER/moves-county-fixture/<fixture>/moves-run.log (4 SINGLE-
# domain fixtures). One log per published snapshot; the 42 fixture names
# were matched against `ls characterization/snapshots/` with no gaps.
#
#   run logs scanned ................................ 42
#   logs matched by this rule ....................... 0
#   logs containing the substring "ERROR" anywhere ... 6
#
# All six near-misses are the same shape —
#     RUN_ERROR: WARNING: Using default formulation <N> for <fuel> in
#     region <R>, year 2020, month 8. Check your input FuelSupply table
#     for errors.
# — an in-simulation FuelSupply/NRFuelSupply fallback, in
# process-apu-single (2), process-crankcase-extidle-single (2),
# process-crankcase-start-single (3), process-extended-idle-single (2),
# nr-construction-state (1) and nr-pleasure-craft-state (1). It is the word
# boundary, not an allowlist, that excludes them: `_` is a word
# constituent, so `RUN_ERROR:` contains no `ERROR:` token.
#
# Reproduced on three FRESH captures taken for this change on 2026-09-09
# with the same SIF (see characterization/audit-results/
# 20260909T2310-run-log-scan-and-snapshot-audit.md for the commands and for
# the bound on what any of this establishes):
# nr-logging-county, process-crankcase-start and chain-tog-speciation —
# 0 matches each, all three published a snapshot and exited 0.
#
# TRUE-POSITIVE RATE. The four project-scale trials of 2026-09-09 (their
# in-log timestamps read 9/10/26 because MOVES logs UTC)
# (/scratch/$USER/scalescope-trial/trial{1,2,3,4}.log), every one of which
# the previous four-literal marker set passed as "MOVES run OK" and only
# the zero-table belt stopped: 8, 3, 2 and 4 matching lines respectively.
# trial5.log, the run that actually succeeded and published a 360-table
# snapshot, has 0. The strings that walked past the old set and are caught
# by this one:
#     ERROR: Unable to validate input database Data Status
#     ERROR: Unable to count offNetwork links
#     ERROR: ImporterInstantiator is unable to instantiate ...GenericImporter
#     ERROR: ERROR: AVFT table does not exist. ...
#     ERROR: ERROR: AVFT table is not imported.
#
# THE ALLOWLIST IS EMPTY, deliberately. The one shape that argues for an
# entry is trial4 lines 124-126:
#     ERROR: Missing: Warning: Fuel formulation 2675 changed fuelSubtypeID
#     ERROR: Missing: Warning: Fuel type 3 is imported but will not be used
# which are warnings in substance. They are nonetheless a correct failure
# signal, and the call site says why —
# master/framework/MOVESAPI.java:870-876:
#
#     result = manager.performAllImporterChecks(runSpec,messages,db);
#     if(result < 0) {
#         // Log the error messages
#         for(Iterator<String> i = messages.iterator(); i.hasNext(); ) {
#             Logger.log(LogMessageCategory.ERROR, i.next());
#         }
#         return false;
#     }
#
# The whole importer message list is re-logged at ERROR level ONLY on the
# `result < 0` branch, which then refuses to launch. The "Missing: " prefix
# is added by framework/importers/ImporterBase.java:409-411 to any quality
# message that does not already start with "error". So a warning-shaped
# `ERROR:` line means "MOVES dumped its importer message list on the way
# out" — a failure by construction, not a warning that got misfiled. It
# also never occurs in the 42-log success corpus above. Allowlisting it
# would have blinded the guard to trial4 almost entirely: strip those three
# lines and ImporterInstantiator is the only evidence left.
#
# A legitimate fixture that trips this rule is therefore itself a finding:
# either MOVES really failed, or MOVES logs a terminal category for a
# non-terminal condition and that belongs in known-divergences.md.
#
# RESIDUAL GAP (issue #64): an error raised *during* the simulation prints
# as `RUN_ERROR:` and is textually indistinguishable from an in-simulation
# warning (Logger.java:78-80 above). This scan cannot see that class at
# all. Behind it stand the non-zero "Java Result" scan, the zero-dumped-
# table refusal and the --min-tables floor — all of which only catch gross
# failure. moves-run.log is now published into every snapshot precisely so
# this class can be re-audited later without re-running MOVES.
MOVES_FAILURE_PATTERNS=(
    # LogMessageCategory.ERROR, word-boundary anchored so RUN_ERROR: is not
    # matched. `[^A-Za-z0-9_]` is the boundary because MOVES's own category
    # names use `_`.
    '(^|[^A-Za-z0-9_])ERROR:'
    # ant itself failed (compile error, missing target, ...).
    'BUILD FAILED'
    # MOVESCommandLine argument handling, which prints straight to stdout
    # without going through Logger. Both are the issue #56 repro shapes.
    'The specified runspec file does not exist'
    'A runspec was not provided'
)

# moves_log_failure_reason <logfile>
#   Returns 0 and prints nothing when the log shows no MOVES failure.
#   Returns 1 and prints a one-line human-readable reason when it does.
# Callers supply their own abort path — run-fixture.sh unwinds a staging
# snapshot, generate-corpus.sh unwinds a staged baseline — so this function
# never exits and never cleans up.
moves_log_failure_reason() {
    local log="$1"
    if [ ! -s "${log}" ]; then
        printf 'MOVES produced no output at all (%s is empty or missing)\n' "${log}"
        return 1
    fi

    # Any non-zero "Java Result: N" means the forked MOVES JVM died even
    # though ant reported success.
    local codes
    codes="$(sed -nE 's/.*Java Result:[[:space:]]*(-?[0-9]+).*/\1/p' "${log}" \
             | grep -vx '0' | sort -u | tr '\n' ' ' || true)"
    codes="${codes% }"
    if [ -n "${codes}" ]; then
        printf 'ant reported non-zero Java Result (%s) — MOVES failed while ant exited 0\n' \
            "${codes}"
        return 1
    fi

    # Report the offending line, not just the pattern that matched it: the
    # operator needs to know which failure happened, and a bare regex in the
    # banner is not that. Truncated so one runaway line can't bury the banner.
    local pattern hit
    for pattern in "${MOVES_FAILURE_PATTERNS[@]}"; do
        hit="$(grep -Em1 -- "${pattern}" "${log}" 2>/dev/null | tr -d '\r' \
               | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' \
               | cut -c1-200 || true)"
        if [ -n "${hit}" ]; then
            printf 'MOVES run log contains a failure line: %s\n' "${hit}"
            return 1
        fi
    done

    return 0
}
