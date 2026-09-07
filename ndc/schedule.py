"""Resolve suites and freeze complete executable schedules before Spark starts."""
import argparse
import json
from pathlib import Path
import random
import re
from provenance import identity

SUITES=('tpch','read','all','scan','compute','depth','shapes','ds','write','maintenance')
PHASES=('matrix','latency','shared-throughput','maintenance','qualification')


def validate_manifest(manifest):
    if not isinstance(manifest,dict) or not manifest:raise ValueError('empty or invalid manifest')
    for name,spec in manifest.items():
        if not isinstance(name,str) or not re.fullmatch(r'[a-z][a-z0-9_]*',name):
            raise ValueError('invalid query name')
        if not isinstance(spec,dict) or not {'sql','reference','family','operation','layout','ordered'}<=spec.keys():
            raise ValueError(f'{name}: missing explicit query contract')


def load_manifest(root,path):
    root=Path(root).resolve()
    manifest=json.loads(Path(path).read_text())
    validate_manifest(manifest)
    for spec in manifest.values():
        for key in ('sql','reference'):
            sql=(root/spec[key]).resolve()
            if not sql.is_relative_to(root/'queries'):raise ValueError('query path outside queries/')
            spec[key+'_text']=sql.read_text()
    return manifest


def load_suite(root,suite):
    if suite=='full':suite='tpch'  # Compatibility membership; the canonical suite is tpch.
    if suite not in SUITES:raise ValueError(f'unknown suite: {suite}')
    name='full' if suite=='tpch' else 'all' if suite=='read' else suite
    manifest=load_manifest(root,Path(root)/f'queries/manifest-{name}.json')
    return {q:s for q,s in manifest.items() if suite!='read' or 'action' not in s}


def build_plan(manifest,runs=3,streams=1,warmups=1,seed=7,phase='matrix'):
    validate_manifest(manifest)
    if phase not in PHASES:raise ValueError('invalid phase')
    if runs<1 or streams<1 or warmups<0:raise ValueError('invalid runs/streams/warmups')
    if phase in ('latency','maintenance','qualification') and streams!=1:
        raise ValueError(f'{phase} requires one stream')
    actions=[s.get('action') for s in manifest.values()]
    if (streams>1 or phase in ('latency','shared-throughput')) and any(actions):
        raise ValueError('read phases and concurrent streams cannot contain writes')
    if phase=='maintenance' and any(a not in ('update','delete','compact') for a in actions):
        raise ValueError('maintenance requires maintenance workloads')
    samples=[]
    for stream in range(streams):
        for run in range(runs):
            order=sorted(manifest)
            random.Random(seed+stream*1000000+run).shuffle(order)
            samples.extend({'q':q,'run':run,'stream':stream} for q in order)
    return {'version':1,'phase':phase,'manifest':manifest,'manifest_id':identity(manifest),
            'settings':{'runs':runs,'streams':streams,'warmups':warmups,'seed':seed},
            'stream_model':'shared-session' if streams>1 or phase=='shared-throughput' else 'serial',
            'warmups':[{'q':q,'run':run,'stream':0} for run in range(warmups) for q in sorted(manifest)],
            'samples':samples}


def validate_plan(plan):
    if plan!=build_plan(plan['manifest'],phase=plan['phase'],**plan['settings']):
        raise ValueError('inconsistent frozen schedule')
    return plan


def main():
    p=argparse.ArgumentParser(description=__doc__)
    selection=p.add_mutually_exclusive_group()
    selection.add_argument('--suite',choices=(*SUITES,'full'))
    selection.add_argument('--queries')
    p.add_argument('--out',required=True)
    p.add_argument('--phase',choices=PHASES,default='matrix')
    for key,default in (('runs',3),('streams',1),('warmups',1),('seed',7)):
        p.add_argument('--'+key,type=int,default=default)
    a=p.parse_args();root=Path(__file__).resolve().parent
    manifest=load_manifest(root,a.queries) if a.queries else load_suite(root,a.suite or 'all')
    plan=build_plan(manifest,a.runs,a.streams,a.warmups,a.seed,a.phase)
    with open(a.out,'x') as f:json.dump(plan,f,indent=2)
    print(f'PLAN {identity(plan)} -> {a.out}')


if __name__=='__main__':main()
