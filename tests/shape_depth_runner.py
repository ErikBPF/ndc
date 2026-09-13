"""Public-runner integration: synthetic data, mixed layout and physical scan evidence."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT=Path(__file__).resolve().parents[1]
p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--out',type=Path,required=True)
out=p.parse_args().out.resolve();out.mkdir(parents=True,exist_ok=False)
(out/'scale.txt').write_text('synthetic-v2\n')
subprocess.run([sys.executable,str(ROOT/'datagen/shapes.py'),'--out',str(out/'data'),
                '--leaves','128','--fanout','16'],check=True)
env=dict(os.environ,NDC_WORKSPACE=str(out),SUITE='shape-depth',FORMATS='iceberg',
         ENGINES='vanilla',RUNS='1',WARMUPS='0',LAYOUT_MODE='mixed',NDC_RUN_INTENT='correctness')
for name in ('NDC_CAMPAIGN_DIR','QUERIES'):env.pop(name,None)
for stage in [('build-fmt','iceberg'),('latency',)]:
    with (out/(stage[0]+'.log')).open('w') as log:
        subprocess.run([str(ROOT/'ndc/run.sh'),*stage],env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
result=next((out/'results').glob('*/spark_vanilla_iceberg.json'))
cell=json.loads(result.read_text())
assert cell['parity_ok'] and len(cell['results'])==16
row=next(r for r in cell['results'] if r['q']=='sd_filter_d3')
plan=json.loads((result.parent/row['plan']).read_text())
assert 'local_tpch.shape_depth3_v2' in plan['optimized'],plan['optimized']
print('SHAPE_DEPTH_RUNNER_OK: 16 validated queries; nested table uses Iceberg in mixed mode')
