"""Run trusted candidate commands sequentially with balanced performance passes."""
import argparse
import json
import os
from pathlib import Path
import random
import re
import subprocess

from compare import compare, load_result


def order(labels,passes,seed):
    if not labels or len(set(labels))!=len(labels) or passes<2 or passes%len(labels):
        raise ValueError('performance passes must be at least two and a multiple of candidate count')
    labels=list(labels);random.Random(seed).shuffle(labels)
    return [labels[i%len(labels):]+labels[:i%len(labels)] for i in range(passes)]


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('spec',type=Path)
    p.add_argument('--out',type=Path,required=True)
    p.add_argument('--mode',choices=['correctness','performance'],default='correctness')
    p.add_argument('--passes',type=int)
    p.add_argument('--warmups',type=int)
    p.add_argument('--seed',type=int,default=7)
    p.add_argument('--operator',action='append',default=[])
    p.add_argument('--require-operator',nargs=3,action='append',default=[])
    a=p.parse_args();summary=None
    try:
        spec=json.loads(a.spec.read_text());candidates={}
        for c in spec['candidates']:
            label=c['label'];command=c['command'];env=c.get('env',{})
            if (not isinstance(label,str) or not re.fullmatch(r'[A-Za-z0-9_-]+',label) or label in candidates
                    or not isinstance(command,list) or not command or not all(isinstance(x,str) and x for x in command)
                    or any(not isinstance(c.get(k,'unrecorded'),str) for k in ('revision','build_profile'))
                    or not isinstance(env,dict) or not all(isinstance(k,str) and isinstance(v,str) for k,v in env.items())):
                raise ValueError('invalid candidate label, command or environment')
            candidates[label]=c
        if len(candidates)<2:raise ValueError('at least two candidates required')
        passes=a.passes if a.passes is not None else (2*len(candidates) if a.mode=='performance' else 1)
        warmups=a.warmups if a.warmups is not None else (1 if a.mode=='performance' else 0)
        if warmups<0 or (a.mode=='performance' and warmups<1):raise ValueError('performance mode requires warmups')
        if a.mode=='correctness' and passes!=1:raise ValueError('correctness mode uses one pass')
        schedule=order(candidates,passes,a.seed) if a.mode=='performance' else [list(candidates)]
        out=a.out.resolve();out.mkdir(parents=True,exist_ok=False)
        summary={'status':'failed','mode':a.mode,'seed':a.seed,'warmups':warmups,'order':schedule,'runs':[]}
        groups={label:[] for label in candidates}
        for index,labels in enumerate(schedule):
            for label in labels:
                c=candidates[label];name=f'pass-{index+1}-{label}';directory=out/name
                env=dict(os.environ,**c.get('env',{}))
                env.update(NDC_CAMPAIGN_DIR=str(directory),NDC_RUN_INTENT=a.mode,NDC_CANDIDATE=label,
                           NDC_CANDIDATE_REVISION=c.get('revision','unrecorded'),NDC_BUILD_PROFILE=c.get('build_profile','unrecorded'),
                           RUNS='1',STREAMS='1',WARMUPS=str(warmups),QUERY_SEED=str(a.seed))
                print(f'NDC experiment pass={index+1}/{passes} candidate={label} starting',flush=True)
                entry={'candidate':label,'pass':index+1,'log':name+'.log','status':'failed'}
                summary['runs'].append(entry)
                (out/'experiment.json').write_text(json.dumps(summary,indent=2)+'\n')
                with (out/(name+'.log')).open('x') as log:
                    with subprocess.Popen(c['command'],cwd=a.spec.resolve().parent,env=env,stdout=subprocess.PIPE,
                                          stderr=subprocess.STDOUT,text=True) as process:
                        for line in process.stdout:
                            log.write(line);log.flush()
                            if line.startswith('NDC '):print(line,end='',flush=True)
                        entry['returncode']=process.wait()
                if entry['returncode']:raise ValueError(f'{label}: command failed ({entry["returncode"]}); see {name}.log')
                cells=[]
                for path in directory.glob('*.json'):
                    record=json.loads(path.read_text())
                    if 'results' in record:cells.append(path)
                if len(cells)!=1:raise ValueError(f'{label}: command must produce exactly one result cell')
                result=load_result(cells[0])
                expected={'runs':1,'streams':1,'warmups':warmups,'seed':a.seed}
                if any(result['comparison'].get(k)!=v for k,v in expected.items()) or result.get('run_intent')!=a.mode:
                    raise ValueError(f'{label}: backend did not honor experiment settings')
                groups[label].append(cells[0]);entry.update(status='ok',result=str(cells[0].relative_to(out)))
        report=compare(groups,a.operator,a.require_operator)
        design='Balanced candidate positions across fresh processes.' if a.mode=='performance' else 'Single correctness pass in configured candidate order.'
        (out/'comparison.md').write_text(report+'\nExecution design: '+design+'\n')
        summary['status']='ok'
        print('NDC experiment complete: answers validated; comparison.md written',flush=True)
    except (ValueError,KeyError,TypeError,OSError) as error:
        if summary is not None:summary['error']=str(error)
        p.exit(1,f'EXPERIMENT_FAILED: {error}\n')
    finally:
        if summary is not None:(out/'experiment.json').write_text(json.dumps(summary,indent=2)+'\n')


if __name__=='__main__':main()
