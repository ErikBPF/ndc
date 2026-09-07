"""Linux process-tree sampler. CPU/IO totals survive child exit; RSS is a sampled sum."""
from pathlib import Path
import sys
import time


def sample(root, proc=Path('/proc')):
    processes = {}
    for path in proc.iterdir():
        if not path.name.isdigit():
            continue
        try:
            text = (path/'stat').read_text()
            fields = text[text.rfind(')')+2:].split()
            processes[int(path.name)] = (int(fields[1]), fields, path)
        except (OSError, ValueError, IndexError):
            continue
    descendants, todo = set(), [root]
    while todo:
        pid = todo.pop()
        if pid in descendants:
            continue
        descendants.add(pid)
        todo.extend(p for p,(parent,_,_) in processes.items() if parent == pid)
    result = {'alive': root in processes, 'rss_kb':0, 'processes':{}}
    for pid in descendants & processes.keys():
        _, fields, path = processes[pid]
        try:
            status = (path/'status').read_text()
            rss = int(status.split('VmRSS:')[1].split()[0]) if 'VmRSS:' in status else 0
            io = dict(line.split(':',1) for line in (path/'io').read_text().splitlines())
            result['rss_kb'] += rss
            result['processes'][(pid,fields[19])] = (
                int(fields[11])+int(fields[12]), int(io['read_bytes']),int(io['write_bytes']))
        except (OSError, ValueError, KeyError, IndexError):
            continue
    return result


def main():
    root, out = int(sys.argv[1]), sys.argv[2]
    interval = float(sys.argv[3]) if len(sys.argv)>3 else .2
    previous, totals = {}, [0,0,0]
    with open(out,'w') as f:
        f.write('ts,cpu_jiffies,rss_kb,read_bytes,write_bytes\n')
        while True:
            current=sample(root)
            for identity, values in current['processes'].items():
                old=previous.get(identity, (0,0,0))
                totals=[a+max(0,b-c) for a,b,c in zip(totals,values,old)]
                previous[identity]=values
            f.write(f'{time.time():.6f},{totals[0]},{current["rss_kb"]},{totals[1]},{totals[2]}\n')
            f.flush()
            if not current['alive']:
                break
            time.sleep(interval)


if __name__ == '__main__':
    main()
