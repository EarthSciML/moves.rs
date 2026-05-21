#!/usr/bin/env bash
# Resolve a local checkout of the upstream MOVES *source tree* so a migration
# polecat can read a calculator's source from the path recorded in its bead
# description (e.g. database/AirToxicsCalculator.sql, calc/airtoxics/airtoxics.go)
# WITHOUT running an unbounded `bfs /` / `find /` filesystem search.
#
# WHY this exists (mo-l006):
#   Migration beads reference source files relative to the upstream MOVES repo
#   root. With no known MOVES checkout on disk, polecats fell back to walking
#   the whole filesystem from `/` to locate a file. Each walk traversed tens of
#   GB of unrelated worktrees and Dolt data, ran for *hours*, ballooned to
#   GB-scale RSS, and orphaned (PPID=1) when the launching session ended. This
#   script gives polecats a bounded, cached checkout instead — no search.
#
# Prints the absolute path of the resolved checkout to STDOUT (and nothing
# else to stdout, so a caller can capture it):
#
#   MOVES_SRC=$(scripts/resolve_moves_src.sh)
#   cat "$MOVES_SRC/database/AirToxicsCalculator.sql"
#
# Resolution order (first hit wins):
#   1. $MOVES_SRC          — explicit override; an existing checkout, used as-is.
#   2. /opt/moves          — canonical location inside the apptainer SIF, if present.
#   3. Clone fallback      — git clone USEPA/EPA_MOVES_Model at the commit pinned
#      below into a per-user cache ($XDG_CACHE_HOME/moves-rs-migration-src/
#      EPA_MOVES_Model), checked out detached at that SHA. The clone is cached:
#      the first polecat pays for it, the rest reuse it. Idempotent — re-runs
#      reuse the cached checkout.
#
# Usage:
#   scripts/resolve_moves_src.sh                        # resolve, print path
#   MOVES_SRC=/path/to/EPA_MOVES_Model scripts/resolve_moves_src.sh
#   MOVES_SRC_REV=<sha> scripts/resolve_moves_src.sh   # pin a different rev
#
# Runnable from any directory — the clone fallback caches under $HOME, so the
# script does not depend on the current working directory.

set -euo pipefail

# MOVES5.0.1 release tip (2025-11-14, "MOVES5.0.1 with movesdb20241112").
# Keep this in sync with characterization/apptainer/files/versions.env —
# that file is the canonical pin and the source-path line numbers in beads
# are relative to this commit.
PINNED_SHA="25dc6c833dd8c88198f82cee93ca30be1456df8b"
MOVES_URL="https://github.com/USEPA/EPA_MOVES_Model.git"

# Diagnostics go to stderr; stdout carries only the resolved path.
log() { echo "$@" >&2; }

# A plausible MOVES checkout has CalculatorInfo.txt at root and the Java tree.
is_moves_checkout() {
  [[ -f "$1/CalculatorInfo.txt" && -d "$1/gov/epa/otaq/moves" ]]
}

resolved=""

# --- 1. Explicit override ---------------------------------------------------
if [[ -n "${MOVES_SRC:-}" ]]; then
  if is_moves_checkout "$MOVES_SRC"; then
    resolved="$(cd "$MOVES_SRC" && pwd)"
    log "resolve_moves_src: using \$MOVES_SRC=$resolved"
  else
    log "warn: \$MOVES_SRC=$MOVES_SRC is not a MOVES checkout" \
        "(no CalculatorInfo.txt + gov/epa/otaq/moves/); trying other sources"
  fi
fi

# --- 2. Apptainer SIF location ---------------------------------------------
if [[ -z "$resolved" ]]; then
  if is_moves_checkout "/opt/moves"; then
    resolved="/opt/moves"
    log "resolve_moves_src: using apptainer SIF checkout $resolved"
  fi
fi

# --- 3. Clone fallback (pinned SHA, cached) ---------------------------------
if [[ -z "$resolved" ]]; then
  cache_root="${XDG_CACHE_HOME:-$HOME/.cache}/moves-rs-migration-src"
  cache_dir="$cache_root/EPA_MOVES_Model"
  rev="${MOVES_SRC_REV:-$PINNED_SHA}"
  mkdir -p "$cache_root"

  # A directory left behind by an aborted clone has no .git — clear it.
  if [[ -e "$cache_dir" && ! -d "$cache_dir/.git" ]]; then
    log "resolve_moves_src: $cache_dir is not a git checkout; clearing stale cache"
    rm -rf "$cache_dir"
  fi

  # Populate the cache once. The clone + checkout happen in a private temp
  # dir, then publish atomically with `mv -T` — so concurrent migration
  # polecats never race on a shared .git.
  if [[ ! -d "$cache_dir/.git" ]]; then
    log "resolve_moves_src: cloning $MOVES_URL -> $cache_dir (this takes a few minutes)"
    tmp_clone="$(mktemp -d "$cache_root/.clone.XXXXXX")"
    if git clone --quiet "$MOVES_URL" "$tmp_clone" \
       && git -C "$tmp_clone" checkout --quiet --detach "$rev"; then
      mv -T "$tmp_clone" "$cache_dir" 2>/dev/null \
        || log "resolve_moves_src: $cache_dir already populated by a concurrent run; using it"
      rm -rf "$tmp_clone"
    else
      rm -rf "$tmp_clone"
      log "error: could not clone $MOVES_URL at $rev (network, auth, or bad rev?)."
      log "  Set \$MOVES_SRC to an existing EPA_MOVES_Model checkout and re-run."
      exit 1
    fi
  fi

  # Cache exists. Re-checkout only if it has drifted off the requested rev.
  cache_head="$(git -C "$cache_dir" rev-parse HEAD 2>/dev/null || true)"
  if [[ "$cache_head" != "$rev" ]]; then
    if ! git -C "$cache_dir" cat-file -e "${rev}^{commit}" 2>/dev/null; then
      log "resolve_moves_src: fetching $rev from origin"
      git -C "$cache_dir" fetch --quiet origin
    fi
    log "resolve_moves_src: checking out $rev in $cache_dir"
    git -C "$cache_dir" checkout --quiet --detach "$rev"
  fi

  if is_moves_checkout "$cache_dir"; then
    resolved="$cache_dir"
    log "resolve_moves_src: using cached clone $resolved (at $rev)"
  fi
fi

if [[ -z "$resolved" ]]; then
  log "error: could not resolve a MOVES source checkout."
  log "  Set \$MOVES_SRC to a checkout, or check network access for the clone fallback."
  exit 1
fi

# Soft check: warn (don't fail) if the checkout has drifted off the pinned SHA.
if git -C "$resolved" rev-parse --git-dir >/dev/null 2>&1; then
  head_sha="$(git -C "$resolved" rev-parse HEAD 2>/dev/null || true)"
  if [[ -n "$head_sha" && "$head_sha" != "$PINNED_SHA" && -z "${MOVES_SRC_REV:-}" ]]; then
    log "warn: $resolved is at $head_sha,"
    log "      not the pinned $PINNED_SHA (MOVES5.0.1)."
    log "      Bead source-path line numbers may be off — verify against the migration plan."
  fi
fi

echo "$resolved"
