#!/usr/bin/env python3
"""Aggregate default-db-benchmark.sh outputs into a markdown + CSV report.

Parses each fixture's native-time.txt / wasm-time.txt (/usr/bin/time -v) for
wall-clock + peak RSS, the wasm harness stdout for its per-phase [engine.run()]
timing, and (optionally) the two correctness-gate logs for per-fixture
max_rel_diff vs the canonical MOVES snapshots.

Usage:
  default-db-benchmark-report.py <out_dir> [--native-gate LOG] [--wasm-gate LOG]
"""
import sys, os, re, csv

def parse_time_file(path):
    """Return (wall_seconds, peak_mb) from a /usr/bin/time -v file, or (None,None)."""
    if not os.path.exists(path):
        return None, None
    wall = peak = None
    for line in open(path):
        if "Elapsed (wall clock) time" in line:
            # The label "(h:mm:ss or m:ss):" itself contains colons, so take
            # the value after the LAST ": ".
            v = line.rsplit(": ", 1)[-1].strip()  # H:MM:SS.ss or M:SS.ss
            parts = v.split(":")
            try:
                parts = [float(p) for p in parts]
            except ValueError:
                continue
            sec = 0.0
            for p in parts:
                sec = sec * 60 + p
            wall = sec
        elif "Maximum resident set size" in line:
            m = re.search(r"(\d+)", line)
            if m:
                peak = int(m.group(1)) / 1024.0  # kB -> MB
    return wall, peak

_DUR = re.compile(r"([0-9.]+)\s*(ns|µs|us|ms|s)")
def parse_rust_dur(tok):
    m = _DUR.search(tok)
    if not m:
        return None
    v = float(m.group(1)); u = m.group(2)
    return v * {"ns":1e-9,"us":1e-6,"µs":1e-6,"ms":1e-3,"s":1.0}[u]

def parse_wasm_phases(path):
    """Return dict phase->seconds from the wasm harness stderr/stdout."""
    phases = {}
    if not os.path.exists(path):
        return phases
    for line in open(path):
        m = re.search(r"\[phase\]\s+(.*?):\s+(.+)$", line.strip())
        if m:
            phases[m.group(1).strip()] = parse_rust_dur(m.group(2).strip())
    return phases

def parse_gate_log(path):
    """Map fixture -> max_rel_diff from a gate verdict table line:
       'process-pm-exhaust   1456   1456   -4.579e-7   PASS'"""
    out = {}
    if not path or not os.path.exists(path):
        return out
    for line in open(path):
        m = re.match(r"^([a-z0-9-]+)\s+\d+\s+\d+\s+(-?\d+\.\d+e[+-]?\d+|0\.000e0)\s+\S", line.strip())
        if m:
            try:
                out[m.group(1)] = float(m.group(2))
            except ValueError:
                pass
    return out

def fmt(x, f="{:.2f}"):
    return f.format(x) if isinstance(x, (int, float)) else "n/a"

def main():
    out_dir = sys.argv[1]
    native_gate = wasm_gate = canon_dir = None
    args = sys.argv[2:]
    for i, a in enumerate(args):
        if a == "--native-gate":  native_gate = args[i+1]
        if a == "--wasm-gate":    wasm_gate   = args[i+1]
        if a == "--canonical-dir": canon_dir  = args[i+1]
    ng = parse_gate_log(native_gate)
    wg = parse_gate_log(wasm_gate)

    fixtures = sorted(d for d in os.listdir(out_dir)
                      if os.path.isdir(os.path.join(out_dir, d)))
    rows = []
    for fx in fixtures:
        d = os.path.join(out_dir, fx)
        n_wall, n_peak = parse_time_file(os.path.join(d, "native-time.txt"))
        w_wall, w_peak = parse_time_file(os.path.join(d, "wasm-time.txt"))
        ph = parse_wasm_phases(os.path.join(d, "wasm-stdout.txt"))
        # Canonical: separate output tree (tree-rss-sampled, multi-process).
        c_wall = c_peak = None
        if canon_dir:
            cd = os.path.join(canon_dir, fx)
            c_wall, _ = parse_time_file(os.path.join(cd, "canon-time.txt"))
            tp = os.path.join(cd, "tree-peak-mb.txt")
            if os.path.exists(tp):
                try:
                    c_peak = float(open(tp).read().strip())
                except ValueError:
                    pass
        rows.append(dict(
            fixture=fx,
            c_wall=c_wall, c_peak=c_peak,
            n_wall=n_wall, n_peak=n_peak,
            w_wall=w_wall, w_peak=w_peak,
            w_engine=ph.get("engine.run()"),
            n_diff=ng.get(fx), w_diff=wg.get(fx),
        ))

    # ---- markdown ----
    print()
    print("## default-DB end-to-end benchmark — canonical vs native vs wasm")
    print()
    print("All three run the **same** end-to-end simulation reading the **same** "
          "`movesdb20241112` default DB (MySQL form for canonical; converted "
          "parquet tree for native/wasm), serial config (canonical `main1worker`; "
          "native/wasm `max_parallel_chunks=1`).")
    print()
    print("- **Runtime (s)** — wall clock. Canonical includes per-run MariaDB "
          "start/stop + dual-JVM startup (fixed overhead the single-process Rust "
          "paths don't pay). `wasm eng` = compute-only `engine.run()` phase.")
    print("- **Memory (MB)** — canonical = peak aggregate RSS of the whole "
          "process tree (master JVM + worker JVM + mariadbd + Go calc, sampled); "
          "native/wasm = single-process peak RSS (`/usr/bin/time -v`).")
    print("- **rel_diff** — max per-pollutant relative error vs the canonical "
          "MOVES snapshot (canonical = reference ≡ 0); gate-verified.")
    print()
    hdr = ["fixture", "canon s", "native s", "wasm s", "wasm eng s",
           "canon MB", "native MB", "wasm MB", "native rel_diff", "wasm rel_diff"]
    print("| " + " | ".join(hdr) + " |")
    print("|" + "|".join(["---"] * len(hdr)) + "|")
    agg = {k: [] for k in ("c_wall","n_wall","w_wall","w_engine","c_peak","n_peak","w_peak")}
    for r in rows:
        for k in agg:
            if isinstance(r[k], (int, float)): agg[k].append(r[k])
        print("| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |".format(
            r["fixture"],
            fmt(r["c_wall"]), fmt(r["n_wall"]), fmt(r["w_wall"]), fmt(r["w_engine"]),
            fmt(r["c_peak"], "{:.0f}"), fmt(r["n_peak"], "{:.0f}"), fmt(r["w_peak"], "{:.0f}"),
            fmt(r["n_diff"], "{:.2e}"), fmt(r["w_diff"], "{:.2e}"),
        ))
    def stat(vals):
        return (sum(vals)/len(vals), min(vals), max(vals)) if vals else (None,None,None)
    print()
    print("### Aggregates ({} fixtures)".format(len(rows)))
    print()
    print("| metric | mean | min | max |")
    print("|---|---|---|---|")
    for label, key, f in [
        ("canonical wall s", "c_wall", "{:.1f}"),
        ("native wall s", "n_wall", "{:.2f}"),
        ("wasm wall s", "w_wall", "{:.2f}"),
        ("wasm engine.run s", "w_engine", "{:.2f}"),
        ("canonical tree-peak MB", "c_peak", "{:.0f}"),
        ("native peak MB", "n_peak", "{:.0f}"),
        ("wasm peak MB", "w_peak", "{:.0f}"),
    ]:
        mean, lo, hi = stat(agg[key])
        print("| {} | {} | {} | {} |".format(label, fmt(mean,f), fmt(lo,f), fmt(hi,f)))

    # Speedup summary (means).
    cm = stat(agg["c_wall"])[0]; nm = stat(agg["n_wall"])[0]; wm = stat(agg["w_wall"])[0]
    if cm and nm and wm:
        print()
        print("Mean wall-clock speedup vs canonical: "
              "native **{:.0f}×**, wasm **{:.0f}×**.".format(cm/nm, cm/wm))

    # ---- csv ----
    csv_path = os.path.join(out_dir, "benchmark.csv")
    with open(csv_path, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["fixture","canon_wall_s","native_wall_s","wasm_wall_s","wasm_engine_s",
                    "canon_tree_peak_mb","native_peak_mb","wasm_peak_mb",
                    "native_rel_diff","wasm_rel_diff"])
        for r in rows:
            w.writerow([r["fixture"], r["c_wall"], r["n_wall"], r["w_wall"], r["w_engine"],
                        r["c_peak"], r["n_peak"], r["w_peak"], r["n_diff"], r["w_diff"]])
    print()
    print(f"CSV: {csv_path}")

if __name__ == "__main__":
    main()
