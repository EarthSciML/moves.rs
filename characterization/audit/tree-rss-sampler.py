#!/usr/bin/env python3
"""Sample peak aggregate RSS of a process tree (root PID + all descendants).

Polls /proc every interval, sums VmRSS over the descendant tree rooted at the
given PID, and writes the running max (in MiB) to the output file. Exits when
the root PID disappears. Used to measure canonical MOVES, which is multi-process
(master JVM + worker JVM + mariadbd + NONROAD/Go calculators) and therefore not
captured by /usr/bin/time -v (which only sees the immediate child's peak).

Usage: tree-rss-sampler.py <root_pid> <out_file> [interval_s]
"""
import sys, os, time

def children_map():
    """Return dict ppid -> [pids] by scanning /proc/*/stat."""
    kids = {}
    for name in os.listdir("/proc"):
        if not name.isdigit():
            continue
        try:
            with open(f"/proc/{name}/stat") as fh:
                data = fh.read()
            # field 4 is ppid; comm (field 2) may contain spaces/parens, so
            # split after the last ')'.
            rhs = data[data.rindex(")") + 1:]
            ppid = int(rhs.split()[1])
            kids.setdefault(ppid, []).append(int(name))
        except (IOError, ValueError, OSError):
            continue
    return kids

def rss_kb(pid):
    try:
        with open(f"/proc/{pid}/statm") as fh:
            resident_pages = int(fh.read().split()[1])
        return resident_pages * (os.sysconf("SC_PAGE_SIZE") // 1024)
    except (IOError, ValueError, OSError):
        return 0

def tree_pids(root, kids):
    out, stack = [], [root]
    while stack:
        p = stack.pop()
        out.append(p)
        stack.extend(kids.get(p, []))
    return out

def alive(pid):
    return os.path.exists(f"/proc/{pid}")

def main():
    root = int(sys.argv[1])
    out_file = sys.argv[2]
    interval = float(sys.argv[3]) if len(sys.argv) > 3 else 0.4
    peak_kb = 0
    while alive(root):
        kids = children_map()
        total = sum(rss_kb(p) for p in tree_pids(root, kids))
        if total > peak_kb:
            peak_kb = total
            with open(out_file, "w") as fh:
                fh.write(f"{peak_kb/1024.0:.1f}\n")
        time.sleep(interval)
    # Ensure a value is written even if the process was too short-lived.
    if not os.path.exists(out_file):
        with open(out_file, "w") as fh:
            fh.write(f"{peak_kb/1024.0:.1f}\n")

if __name__ == "__main__":
    main()
