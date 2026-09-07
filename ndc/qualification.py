"""One fail-closed candidate qualification: prepared tiny data through engine execution."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import uuid


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('engine',choices=('vanilla','comet'))
    p.add_argument('format',choices=('parquet','iceberg','delta'),nargs='?',default='parquet')
    p.add_argument('--runner',default=str(Path(__file__).with_name('run.sh')))
    a=p.parse_args();workspace=Path(os.environ['NDC_WORKSPACE']).resolve()
    directory=Path(os.environ.get('NDC_CAMPAIGN_DIR',workspace/'results'/f'qualification-{uuid.uuid4().hex[:12]}'))
    directory.mkdir(parents=True,exist_ok=False)
    summary={'status':'failed','engine':a.engine,'format':a.format,'suite':'all','checks':[]}
    try:
        if (workspace/'scale.txt').read_text().strip()!='0.0083':
            raise ValueError('candidate qualification requires sf0.0083; use matrix for larger scales')
        env=dict(os.environ,NDC_CAMPAIGN_DIR=str(directory/'campaign'),ENGINES=a.engine,
                 FORMATS=a.format,SUITE='all',RUNS='1',STREAMS='1',WARMUPS='0',
                 NDC_PHASE='qualification',CACHE='uncontrolled',DROP_CACHES='no')
        env.pop('QUERIES',None)
        for stage in ('size-check','invariants','qualify-references','parity','matrix'):
            with open(directory/f'{stage}.log','w') as log:
                result=subprocess.run([a.runner,stage],env=env,stdout=log,stderr=subprocess.STDOUT)
            summary['checks'].append({'stage':stage,'returncode':result.returncode,'log':f'{stage}.log'})
            if result.returncode:break
        else:summary['status']='ok'
    except Exception as error:
        summary['error']=str(error)
    finally:
        (directory/'qualification.json').write_text(json.dumps(summary,indent=2))
    print(f'QUALIFICATION {summary["status"]} -> {directory}/qualification.json')
    return 0 if summary['status']=='ok' else 1


if __name__=='__main__':raise SystemExit(main())
