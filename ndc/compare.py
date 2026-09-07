"""Compare named, validated result cells without exposing machine names or private paths."""
import argparse
import html
import json
from pathlib import Path
import re
import statistics
import sys

from report import load_cell


def text(value):
    return html.escape(str(value)).replace('|','&#124;').replace('\n',' ')


def resources(cell):
    quotas=[];memory=[]
    for entry in cell['comparison'].get('cgroup_limits',[]) or []:
        cpu=entry.get('cpu.max','max').split()
        if cpu[0]!='max':quotas.append(int(cpu[0])/int(cpu[1]))
        mem=entry.get('memory.max','max')
        if mem!='max':memory.append(int(mem))
    cfg=cell.get('environment',{}).get('config',{})
    return ', '.join([f'{min(quotas):g} CPUs' if quotas else 'CPU quota unrecorded/unlimited',
                     f'{min(memory)/1024**3:g} GiB' if memory else 'memory cap unrecorded/unlimited',
                     f"threads/master {cell['comparison'].get('master','unknown')}",
                     f"driver {cell['comparison'].get('driver_memory','unknown')}",
                     f"off-heap {cfg.get('spark.memory.offHeap.size','unknown')}"])


def operator_names(path,row):
    target=(path.parent/row.get('plan','')).resolve()
    if not row.get('plan') or not target.is_relative_to(path.parent.resolve()):
        raise ValueError('missing or unsafe operator evidence')
    record=json.loads(target.read_text())
    if not isinstance(record.get('operator_metrics'),list):raise ValueError('missing operator inventory')
    return {node['operator'] for node in record['operator_metrics']}


def load_result(path):
    cell=load_cell(path)
    marker=path.parent/'campaign.json'
    if marker.exists():
        campaign=json.loads(marker.read_text())
        if campaign.get('status')!='ok' or f"{cell['engine']}/{cell['fmt']}" not in campaign.get('cells',[]):
            raise ValueError('failed or inconsistent campaign')
    return cell


def compare(groups,operators=(),required=()):
    if len(groups)<2:raise ValueError('at least two named candidates required')
    loaded={};first=None;answers={};seen=set()
    for label,paths in groups.items():
        if not re.fullmatch(r'[A-Za-z0-9_-]+',label) or not paths:raise ValueError('invalid candidate label or empty candidate')
        loaded[label]=[];campaigns=set()
        for path in paths:
            path=Path(path).resolve()
            if path in seen:raise ValueError('duplicate result file')
            seen.add(path)
            cell=load_result(path)
            identity=(cell['campaign_id'],cell['engine'],cell['fmt'])
            if identity in campaigns:raise ValueError('duplicate candidate campaign')
            campaigns.add(identity)
            if first is None:first=cell
            for key in ('dataset_id','format_id','manifest_id','plan_id','comparison','fmt'):
                if cell.get(key) is None or cell[key]!=first[key]:raise ValueError(f'incompatible or missing {key}')
            source=cell.get('environment',{}).get('source_id')
            if not source or source!=first.get('environment',{}).get('source_id'):raise ValueError('incompatible source snapshot')
            for row in cell['results']:
                if row['status']=='unsupported':raise ValueError('comparison requires supported workloads')
                answer=(row.get('rows'),row.get('answer_id'))
                if not answer[1]:raise ValueError('missing answer identity')
                if row['q'] in answers and answers[row['q']]!=answer:raise ValueError(f'inconsistent answers: {row["q"]}')
                answers[row['q']]=answer
            loaded[label].append((path,cell))
        # A candidate must not silently switch binaries between repetitions.
        provenance=[(c.get('environment',{}).get('artifacts'),c.get('environment',{}).get('candidate'),c.get('engine'),c.get('spark_version'),c.get('engine_version'),c.get('environment',{}).get('config')) for _,c in loaded[label]]
        if any(p!=provenance[0] for p in provenance):raise ValueError('candidate artifacts or metadata changed between passes')
    if len({len(v) for v in loaded.values()})!=1:raise ValueError('unequal candidate pass counts')
    for label,query,operator in required:
        if label not in loaded or query not in answers:raise ValueError('unknown operator expectation target')
        for path,cell in loaded[label]:
            for row in cell['results']:
                if row['q']==query and operator not in operator_names(path,row):
                    raise ValueError(f'{label}/{query}: required operator absent: {operator}')
    lines=['# NDC candidate comparison','','Answers match across all candidates.','',
           'Exploratory timings; no statistical significance or TPC performance claim.','',
           f"Repetitions per candidate: {len(next(iter(loaded.values())))*first['comparison']['runs']}; warmups per process: {first['comparison'].get('warmups',0)}.",'']
    for label,entries in loaded.items():
        c=entries[0][1];env=c.get('environment',{});candidate=env.get('candidate',{})
        lines += [f'**{label}**: {text(resources(c))}. Runtime: {text(c.get("engine_version",c.get("spark_version","unrecorded")))}.',
                  f"Revision: {text(candidate.get('revision','unrecorded'))}; build profile: {text(candidate.get('build_profile','unrecorded'))}; intent: {text(c.get('run_intent','unrecorded'))}."]
        for artifact,digest in sorted(env.get('artifacts',{}).items()):lines.append(f'- {text(artifact)}: `{text(digest)}`')
        lines.append('')
    baseline=next(iter(loaded))
    lines += ['| Query | Candidate | Median ms | Range ms | Baseline / candidate |','|---|---|---:|---|---:|']
    totals=dict.fromkeys(loaded,0.0)
    for query in sorted(answers):
        samples={label:[r['ms'] for _,c in entries for r in c['results'] if r['q']==query] for label,entries in loaded.items()}
        for label,values in samples.items():
            median=statistics.median(values);totals[label]+=median
            lines.append(f'| {query} | {label} | {median:.3f} | {min(values):.3f}–{max(values):.3f} | {statistics.median(samples[baseline])/median:.3f}x |')
    lines += ['', 'Sum of query medians (descriptive): '+', '.join(f'{label} {total/1000:.3f} s' for label,total in totals.items())+'.']
    for operator in operators:
        coverage={}
        for label,entries in loaded.items():
            coverage[label]={q for q in answers if all(operator in operator_names(path,r) for path,c in entries for r in c['results'] if r['q']==q)}
        lines += ['',f'Operator **{text(operator)}**, present in every sample of a query: '+', '.join(f'{label} {len(qs)}/{len(answers)}' for label,qs in coverage.items())+'.']
        for label,queries in coverage.items():
            if label!=baseline:
                lines.append(f'{label}: gained {", ".join(sorted(queries-coverage[baseline])) or "none"}; lost {", ".join(sorted(coverage[baseline]-queries)) or "none"}.')
    return '\n'.join(lines)+'\n'


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('candidates',nargs='+',metavar='LABEL=RESULT.json',help='repeat a label for independent passes; first label is baseline')
    p.add_argument('--out',type=Path)
    p.add_argument('--operator',action='append',default=[])
    p.add_argument('--require-operator',nargs=3,action='append',default=[],metavar=('LABEL','QUERY','OPERATOR'))
    a=p.parse_args();groups={}
    try:
        for item in a.candidates:
            label,path=item.split('=',1);groups.setdefault(label,[]).append(Path(path))
        report=compare(groups,a.operator,a.require_operator)
        if a.out:
            with a.out.open('x') as out:out.write(report)
        else:print(report,end='')
    except (ValueError,KeyError,TypeError,OSError) as error:p.exit(1,f'COMPARE_INVALID: {error}\n')


if __name__=='__main__':main()
