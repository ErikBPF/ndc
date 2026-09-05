"""D10 sizing gate: assert live dbgen row counts against the recorded
reference in sizes.csv. usage: check_size.py <gen.sql> <live.csv> <sizes.csv>
The scale factor is parsed from the workspace gen.sql written by bootstrap.
Exits nonzero on any mismatch — a build that changed row counts must fail
here, not surface later as a parity mystery."""
import csv
import re
import sys

gen_path, live_path, sizes_path = sys.argv[1], sys.argv[2], sys.argv[3]

m = re.search(r"dbgen\(sf\s*=\s*([0-9.]+)\s*\)", open(gen_path).read())
if not m:
    sys.exit("SIZE_CHECK_FAIL: no dbgen sf in gen.sql")
sf = m.group(1)

ref = {r["sf"]: r for r in csv.DictReader(open(sizes_path))}
if sf not in ref:
    sys.exit(f"SIZE_CHECK_FAIL: sf {sf} not in {sizes_path}")

live = next(csv.DictReader(open(live_path)))
mismatch = {
    k: (live[k], ref[sf][k])
    for k in live
    if k in ref[sf] and live[k] != ref[sf][k]
}
if mismatch:
    sys.exit(f"SIZE_CHECK_FAIL sf={sf} (live vs reference): {mismatch}")
print(f"SIZE_CHECK_OK sf={sf}")
