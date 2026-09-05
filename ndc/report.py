"""Family-level report for tpch-ndc (D13). usage: report.py <dir-with-results> <out.md>
Reads spark_{vanilla,comet}_{parquet,iceberg,delta}.json; prints family x format
speedups + a per-cell verdict for the 'is my use case better on Comet?' question."""
import json
import os
import statistics
import sys

FAMILIES = {
    "built-in (scan+agg)": lambda q: q.startswith("q") and "depth" not in q,
    "depth 1-8": lambda q: "depth" in q,
    "extended (manipulation)": lambda q: q.startswith("e"),
}


def main():
    d = sys.argv[1]
    cells = {}
    for eng in ("vanilla", "comet"):
        for fmt in ("parquet", "iceberg", "delta"):
            p = os.path.join(d, f"spark_{eng}_{fmt}.json")
            if not os.path.exists(p):
                continue
            j = json.load(open(p))
            med = {}
            for r in j["results"]:
                med.setdefault(r["q"], []).append(r["ms"])
            cells[(eng, fmt)] = {
                "parity": j.get("parity_ok"),
                "spark": j.get("spark_version"),
                "t": {q: statistics.median(v) / 1000 for q, v in sorted(med.items())},
            }
    qs = sorted(next(iter(cells.values()))["t"])
    lines = ["# tpch-ndc report", "",
             f"- queries: {len(qs)} | parity: "
             f"{ {f'{e}/{f}': c['parity'] for (e, f), c in cells.items()} }",
             f"- spark: {next(iter(cells.values()))['spark']} | "
             "cluster/fs: see bench.conf", ""]
    lines.append("| Family | " + " | ".join(f"{'Comet' if e=='comet' else 'Spark'}/{f}" for e, f in cells) + " |")
    lines.append("|---|" + "---:|" * len(cells))
    for fam, pred in FAMILIES.items():
        fam_qs = [q for q in qs if pred(q)]
        if not fam_qs:
            continue
        row = [f"{fam} ({len(fam_qs)}q)"]
        for k in cells:
            s = sum(cells[k]["t"][q] for q in fam_qs)
            row.append(f"{s:.2f}s")
        lines.append("| " + " | ".join(row) + " |")
    tot = [sum(cells[k]["t"].values()) for k in cells]
    lines.append("| **total** | " + " | ".join(f"{t:.2f}s" for t in tot) + " |")
    lines.append("")
    lines.append("| Format | Speedup | Comet verdict |")
    lines.append("|---|---:|---|")
    vs = {f: sum(cells[("vanilla", f)]["t"].values()) for f in ("parquet", "iceberg", "delta") if ("vanilla", f) in cells}
    cs = {f: sum(cells[("comet", f)]["t"].values()) for f in ("parquet", "iceberg", "delta") if ("comet", f) in cells}
    for fmt in ("parquet", "iceberg", "delta"):
        if vs[fmt] is None or cs[fmt] is None:
            continue
        sp = vs[fmt] / cs[fmt]
        verdict = ("Comet wins" if sp >= 1.15 else "near tie" if sp >= 0.95 else "Comet loses")
        lines.append(f"| {fmt} | {sp:.2f}x | {verdict} |")
    out = sys.argv[2]
    open(out, "w").write("\n".join(lines) + "\n")
    print("\n".join(lines[2:]))
    print(f"REPORT -> {out}")


if __name__ == "__main__":
    main()
