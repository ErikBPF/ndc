"""Merge sampled process-tree counters; short/unbracketed windows remain unavailable."""
import json
import os
from pathlib import Path
import sys

CLK = os.sysconf('SC_CLK_TCK')


def window(rows,t0,t1):
    inside=[r for r in rows if t0<=r[0]<=t1]
    pre=[r for r in rows if r[0]<=t0]
    post=[r for r in rows if r[0]>=t1]
    if len(inside)<2 or not pre or not post:
        return None
    a,b=pre[-1],post[0]
    deltas=[b[i]-a[i] for i in (1,3,4)]
    if any(v<0 for v in deltas):
        return None
    return {'cpu_s':deltas[0]/CLK,'peak_rss_mb':max(r[2] for r in inside)/1024,
            'read_bytes':deltas[1],'write_bytes':deltas[2], 'sample_count':len(inside),
            'boundary_slop_s':(t0-a[0])+(b[0]-t1), 'scope':'process-tree, sampled'}


def main():
    path=Path(sys.argv[1]); data=json.loads(path.read_text())
    rows=[[float(x) for x in line.split(',')] for line in Path(sys.argv[2]).read_text().splitlines()[1:]]
    for result in data['results']:
        result['resources']=(window(rows,result['t0'],result['t1'])
                             if 't0' in result and data['comparison']['streams']==1 else None)
    data['resource_scope']='whole process tree; unavailable per query for concurrent streams'
    path.write_text(json.dumps(data,indent=2))


if __name__ == '__main__':
    main()
