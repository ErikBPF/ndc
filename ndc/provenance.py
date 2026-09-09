"""Content identities and safe, explicit machine/runtime disclosure."""
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess


def digest(path):
    h=hashlib.sha256()
    with open(path,'rb') as f:
        for block in iter(lambda:f.read(1024*1024),b''):
            h.update(block)
    return h.hexdigest()


def identity(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,default=str).encode()).hexdigest()


def dataset(directory):
    root=Path(directory)
    files={str(p.relative_to(root)):{'bytes':p.stat().st_size,'sha256':digest(p)}
           for p in sorted(root.rglob('*')) if p.is_file() and p.name not in ('dataset.json','inventory.json')
           and not p.name.startswith('.')}
    record={'dataset_id':identity(files),'files':files}
    (root/'inventory.json').write_text(json.dumps(record,indent=2))
    return record


def candidate_metadata():
    return {'label':os.environ.get('NDC_CANDIDATE','unrecorded'),
            'revision':os.environ.get('NDC_CANDIDATE_REVISION','unrecorded'),
            'build_profile':os.environ.get('NDC_BUILD_PROFILE','unrecorded')}


def environment(spark, root):
    code={str(p.relative_to(root)):digest(p) for folder in ('ndc','datagen') for p in sorted((root/folder).rglob('*'))
          if p.is_file() and p.suffix in ('.py','.sh','.sql','.json','.out','.csv','.conf')
          and not any(x in p.parts for x in ('data','results','__pycache__'))}
    for name in ('nix/flake.nix','nix/flake.lock'):
        if (root/name).is_file(): code[name]=digest(root/name)
    commit=subprocess.run(['git','-C',str(root),'rev-parse','HEAD'],capture_output=True,text=True)
    artifacts={Path(p).name:digest(p) for p in os.environ.get('NDC_JARS','').split(',') if p}
    allowed=('spark.sql.shuffle.partitions','spark.master','spark.driver.memory',
             'spark.sql.adaptive.enabled','spark.sql.parquet.compression.codec',
             'spark.sql.parquet.enableNestedColumnVectorizedReader','spark.sql.session.timeZone',
             'spark.sql.ansi.enabled','spark.memory.offHeap.size','spark.comet.enabled')
    config={}
    for k in allowed:
        try: config[k]=spark.conf.get(k)
        except Exception: config[k]=None
    return {'git_commit':commit.stdout.strip() or os.environ.get('NDC_SOURCE_COMMIT'),'source_id':identity(code),'source_files':code,
            'candidate':candidate_metadata(),'artifacts':artifacts,'spark':spark.version,'python':platform.python_version(),
            'java':spark.sparkContext._jvm.java.lang.System.getProperty('java.version'),
            'host':platform.node(),'os':platform.platform(),'cpu_count':os.cpu_count(),
            'cpu_affinity':sorted(os.sched_getaffinity(0)),
            'memory_bytes':os.sysconf('SC_PAGE_SIZE')*os.sysconf('SC_PHYS_PAGES'),
            'config':config}


def plans(df):
    qe=df._jdf.queryExecution()
    plan=qe.executedPlan()
    nodes=[]
    def walk(node):
        name=node.nodeName()
        metrics={}
        it=node.metrics().iterator()
        while it.hasNext():
            pair=it.next(); metrics[str(pair._1())]=pair._2().value()
        nodes.append({'operator':name,'metrics':metrics})
        if name=='AdaptiveSparkPlan':
            walk(node.executedPlan())
        elif 'QueryStage' in name:
            walk(node.plan())
        else:
            it=node.children().iterator()
            while it.hasNext(): walk(it.next())
    walk(plan)
    native=sum('Comet' in n['operator'] for n in nodes)
    return {'optimized':qe.optimizedPlan().toString(),'executed':plan.toString(),
            'operator_metrics':nodes,'native_operator_count':native,
            'operator_count':len(nodes),'native_operator_fraction':native/len(nodes) if nodes else None,
            'non_comet_operators':[n['operator'] for n in nodes if 'Comet' not in n['operator']]}


def format_identity(directory, fmt):
    data=Path(directory)
    if fmt=='parquet':
        paths=sorted(p for table in data.glob('*.parquet') for p in ([table] if table.is_file() else table.rglob('*')) if p.is_file())
    else:
        base=data.parent/('data_delta' if fmt=='delta' else 'data_iceberg_wh')
        paths=sorted(p for table in data.glob('*.parquet') for p in (base/table.stem).rglob('*') if p.is_file())
    if not paths:raise ValueError(f'no physical inputs for {fmt}')
    return identity({str(p.relative_to(data.parent)):digest(p) for p in paths})


def limits():
    """Record enforced cgroup limits, without exposing unrelated environment values."""
    rows=Path('/proc/self/cgroup').read_text().splitlines()
    group=next((line.split(':',2)[2] for line in rows if line.startswith('0::')),None)
    if group is None:return {}
    current=Path('/sys/fs/cgroup')/group.lstrip('/')
    result=[]
    while current.is_relative_to('/sys/fs/cgroup'):
        entry={key:(current/key).read_text().strip() for key in ('cpu.max','memory.max','cpuset.cpus.effective') if (current/key).is_file()}
        if entry:result.append(entry)
        current=current.parent
    return result
