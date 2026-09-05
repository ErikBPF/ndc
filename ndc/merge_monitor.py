"""Merge monitor CSV windows into a results JSON. usage: merge_monitor.py <results.json> <monitor.csv>"""
import json
import os
import sys

CLK = os.sysconf("SC_CLK_TCK")


def window(rows, t0, t1):
    pre = [r for r in rows if r[0] <= t0]
    post = [r for r in rows if r[0] <= t1]
    if not pre or not post:
        return None
    a, b = pre[-1], post[-1]
    win = [r for r in rows if t0 - 1.2 <= r[0] <= t1 + 0.2] or [a, b]
    return {
        "cpu_s": round((b[1] - a[1]) / CLK, 2),
        "peak_rss_mb": round(max(r[2] for r in win) / 1024, 1),
        "read_mb": round((b[3] - a[3]) / 1e6, 1),
        "write_mb": round((b[4] - a[4]) / 1e6, 1),
    }


def main():
    res_path, csv_path = sys.argv[1], sys.argv[2]
    d = json.load(open(res_path))
    rows = [
        [float(x) for x in ln.split(",")]
        for ln in open(csv_path).read().splitlines()[1:] if ln.strip()
    ]
    for r in d["results"]:
        r["res"] = window(rows, r["t0"], r["t1"]) or {}
    json.dump(d, open(res_path, "w"), indent=1)
    ok = sum(1 for r in d["results"] if r["res"])
    print(f"MERGED {ok}/{len(d['results'])} resource windows")


if __name__ == "__main__":
    main()
