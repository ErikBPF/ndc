"""Process-tree resource sampler. usage: monitor.py <root_pid> <out.csv> [interval_s]
Samples cpu jiffies (tree sum), peak driver-tree RSS, io read/write bytes until root exits."""
import os
import sys
import time


def tree(root):
    procs = {}
    for p in os.listdir("/proc"):
        if not p.isdigit():
            continue
        try:
            stat = open(f"/proc/{p}/stat").read()
            fields = stat[stat.rfind(")") + 2:].split()
            procs[int(p)] = int(fields[1])
        except Exception:
            pass
    kids, todo = set(), [root]
    while todo:
        p = todo.pop()
        if p in kids:
            continue
        kids.add(p)
        todo += [c for c, pp in procs.items() if pp == p]
    return kids


def main():
    root, out = int(sys.argv[1]), sys.argv[2]
    iv = float(sys.argv[3]) if len(sys.argv) > 3 else 1.0
    with open(out, "w") as f:
        f.write("ts,cpu_jiffies,rss_kb,read_bytes,write_bytes\n")
        while True:
            cpu = rss = rd = wr = 0
            alive = False
            for p in tree(root):
                try:
                    stat = open(f"/proc/{p}/stat").read()
                    fl = stat[stat.rfind(")") + 2:].split()
                    cpu += int(fl[11]) + int(fl[12])
                    if p == root:
                        alive = True
                        status = open(f"/proc/{p}/status").read()
                        rss = max(rss, int(status.split("VmRSS:")[1].split()[0]))
                    io = dict(
                        (ln.split(": ")[0], int(ln.split(": ")[1].strip()))
                        for ln in open(f"/proc/{p}/io") if ": " in ln
                    )
                    rd += io.get("read_bytes", 0)
                    wr += io.get("write_bytes", 0)
                except Exception:
                    pass
            ts = time.time()
            f.write(f"{ts:.3f},{cpu},{rss},{rd},{wr}\n")
            f.flush()
            if not alive:
                break
            time.sleep(iv)


if __name__ == "__main__":
    main()
