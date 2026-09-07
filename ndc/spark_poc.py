"""NDC runner: complete validation, isolated writes, repeatable query streams."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import subprocess
import sys
import time
import uuid

from pyspark.sql import SparkSession

from mutations import inventory, prepare, unsupported_reason
from schedule import build_plan, load_manifest, validate_plan, PHASES
from provenance import dataset, environment, identity, plans, format_identity, limits
from shapes import full_answer, iter_full_answer
from validation import canonical, read_answer, validate, materialize, distributed_validate


def positive(value):
    value=int(value)
    if value<1: raise argparse.ArgumentTypeError('must be positive')
    return value


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--comet',action='store_true')
    p.add_argument('--plan')
    p.add_argument('--phase',choices=PHASES,default='matrix')
    p.add_argument('--validation',choices=['collect','distributed'],default='collect')
    p.add_argument('--data',default='data')
    p.add_argument('--out',required=True)
    p.add_argument('--queries',default=str(Path(__file__).parent/'queries/manifest-full.json'))
    p.add_argument('--runs',type=positive,default=3)
    p.add_argument('--streams',type=positive,default=1)
    p.add_argument('--warmups',type=int,default=1)
    p.add_argument('--seed',type=int,default=7)
    p.add_argument('--fmt',choices=['parquet','iceberg','delta'],default='parquet')
    p.add_argument('--layout-mode',choices=['matched','mixed'],default='matched')
    p.add_argument('--cache',choices=['uncontrolled','warm','cold'],default='uncontrolled')
    p.add_argument('--drop-caches',action='store_true')
    p.add_argument('--campaign-id',default=None)
    p.add_argument('--sf',default='0.0083')
    a=p.parse_args()
    distributed=a.validation=='distributed'
    if a.warmups<0: p.error('warmups must be nonnegative')
    if a.drop_caches: a.cache='cold'
    if a.cache=='cold' and a.streams>1: p.error('cold cache requires a single stream')
    if a.cache=='warm' and a.warmups<1: p.error('warm cache requires a warm-up')
    root=Path(__file__).resolve().parents[1]
    output=Path(a.out).resolve()
    if output.exists(): p.error('output already exists; choose a fresh campaign')
    output.parent.mkdir(parents=True,exist_ok=True)
    if a.plan:
        plan=validate_plan(json.loads(Path(a.plan).read_text()))
        settings={'runs':a.runs,'streams':a.streams,'warmups':a.warmups,'seed':a.seed}
        if plan['settings']!=settings or plan['phase']!=a.phase:
            p.error('arguments differ from frozen schedule')
    else:
        manifest=load_manifest(root/'ndc',a.queries)
        plan=build_plan(manifest,a.runs,a.streams,a.warmups,a.seed,a.phase)
        with open(output.parent/(output.stem+'_plan.json'),'x') as f:json.dump(plan,f,indent=2)
    manifest=plan['manifest']
    builder=(SparkSession.builder.appName('ndc').config('spark.sql.session.timeZone','UTC')
             .config('spark.sql.shuffle.partitions','8').config('spark.ui.enabled','false')
             .config('spark.sql.parquet.datetimeRebaseModeInRead','CORRECTED')
             .config('spark.scheduler.mode','FAIR'))
    if a.comet:
        builder=(builder.config('spark.plugins','org.apache.spark.CometPlugin')
                 .config('spark.shuffle.manager','org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager')
                 .config('spark.comet.explain.fallback.enabled','true')
                 .config('spark.memory.offHeap.enabled','true').config('spark.memory.offHeap.size','2g'))
    spark=builder.getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    if distributed:
        for name in ('validation.py','provenance.py','shapes.py','mutations.py'):
            spark.sparkContext.addPyFile(str(root/'ndc'/name))
    datadir=Path(a.data).resolve()
    for path in sorted(datadir.glob('*.parquet')):
        table=path.stem
        nested=table.startswith('orders_') or table=='shape'
        fmt=a.fmt if a.layout_mode=='matched' or nested else 'parquet'
        if fmt=='parquet': df=spark.read.parquet(str(path))
        elif fmt=='iceberg': df=spark.table(f'local_tpch.{table}')
        else: df=spark.read.format('delta').load(str(datadir.parent/'data_delta'/table))
        df.createOrReplaceTempView(table)
    env=environment(spark,root)
    base_id=dataset(datadir)['dataset_id']
    physical_id=format_identity(datadir,a.fmt)
    if a.fmt!='parquet':
        marker=json.loads((datadir.parent/f'format-{a.fmt}.json').read_text())
        if marker['dataset_id']!=base_id or marker['format_id']!=physical_id:
            raise ValueError('stale or modified table-format inputs; rebuild the format')
    record={'schema_version':2,'campaign_id':a.campaign_id or str(uuid.uuid4()),
            'dataset_id':base_id,'format_id':physical_id,'manifest_id':identity(manifest),'manifest':manifest,'plan':plan,'plan_id':identity(plan),
            'comparison':{'runs':a.runs,'streams':a.streams,'warmups':a.warmups,'seed':a.seed,
                          'cache':a.cache,'layout_mode':a.layout_mode,'sf':a.sf,'validation':a.validation,
                          'phase':a.phase,'stream_model':plan['stream_model'],
                          'host':env['host'],'cpu_affinity':env['cpu_affinity'],'cgroup_limits':limits(),
                          'master':spark.sparkContext.master,'driver_memory':spark.sparkContext.getConf().get('spark.driver.memory')},
            'environment':env,'command':sys.argv,'engine':'comet' if a.comet else 'vanilla','fmt':a.fmt,
            'spark_version':spark.version,'results':[], 'parity_ok':False,
            'disclaimer':'TPC-H-derived and TPC-DS-inspired; not comparable to published TPC results.'}
    plan_dir=output.parent/(output.stem+'_plans');plan_dir.mkdir()
    scratch=output.parent/(output.stem+'_writes');scratch.mkdir()
    if a.fmt=='iceberg':
        spark.conf.set('spark.sql.catalog.ndc_write','org.apache.iceberg.spark.SparkCatalog')
        spark.conf.set('spark.sql.catalog.ndc_write.type','hadoop')
        spark.conf.set('spark.sql.catalog.ndc_write.warehouse',str(scratch))
    references={}
    try:
        for name,spec in manifest.items():
            if 'action' in spec: continue
            reference=spark.sql(spec['reference_text'])
            expected=reference.rdd.map(tuple) if distributed else [tuple(r) for r in reference.collect()]
            oracle='flat-reference'
            if spec.get('oracle')=='python-shape':
                config=json.loads((datadir/'shape.json').read_text())
                # ponytail: one streaming oracle task preserves the original sequential RNG.
                expected=(spark.sparkContext.parallelize([config],1).flatMap(iter_full_answer)
                          if distributed else full_answer(config))
                oracle='independent-python'
            elif a.sf=='0.0083' and spec.get('answer'):
                expected=read_answer(root/'ndc'/spec['answer'],reference.schema)
                oracle='pinned-qualification'
                if distributed:expected=spark.sparkContext.parallelize(expected,1)
            if distributed:
                from pyspark import StorageLevel
                expected=expected.persist(StorageLevel.DISK_ONLY)
                expected.count()
            references[name]=(expected,oracle)
        # Warm-ups are fully executed and validated; never included as timing samples.
        def execute(name,run,stream,warmup=False):
            spec=manifest[name]
            result={'q':name,'run':run,'stream':stream,'family':spec['family'],
                    'operation':spec['operation'],'layout':spec['layout'],'cache':a.cache,
                    'status':'error','valid':False}
            reason=unsupported_reason(spec,a.fmt)
            if reason:
                return dict(result,status='unsupported',reason=reason)
            actual=None
            try:
                if 'action' in spec:
                    target=scratch/f'{name}_{stream}_{run}_{"warm" if warmup else "timed"}'
                    action,read,expected,before=prepare(spark,spec['sql_text'],spec['reference_text'],a.fmt,target,spec['action'],distributed=distributed)
                    oracle='write-round-trip' if spec['action']=='materialize' else 'deterministic-state-transition'
                else:
                    expected,oracle=references[name]
                if a.cache=='cold':
                    cp=subprocess.run(['sudo','-n','sh','-c','sync; echo 3 > /proc/sys/vm/drop_caches'],capture_output=True)
                    if cp.returncode: raise RuntimeError('drop-failed')
                t0=time.time(); start=time.perf_counter()
                if 'action' in spec:
                    action()
                    ms=(time.perf_counter()-start)*1000
                    t1=time.time()
                    df=read()
                    if distributed:actual,row_count=materialize(df)
                    else:actual=[tuple(r) for r in df.collect()];row_count=len(actual)
                else:
                    df=spark.sql(spec['sql_text'])
                    if distributed:actual,row_count=materialize(df)
                    else:actual=[tuple(r) for r in df.collect()];row_count=len(actual)
                    ms=(time.perf_counter()-start)*1000; t1=time.time()
                if distributed:
                    valid,answer_id=distributed_validate(actual,expected,spec['ordered'])
                else:
                    valid=validate(actual,expected,spec['ordered'])
                    normalized=[canonical(row) for row in actual]
                    answer_id=identity(normalized if spec['ordered'] else sorted(normalized,key=repr))
                result.update(ms=ms,t0=t0,t1=t1,oracle=oracle,rows=row_count,
                              valid=valid,status='ok' if valid else 'invalid',answer_id=answer_id)
                if not warmup:
                    plan=plans(df)
                    planfile=plan_dir/f'{name}_{stream}_{run}.json'
                    planfile.write_text(json.dumps(plan,indent=2))
                    result['plan']=str(planfile.relative_to(output.parent))
                    result['native_operator_fraction']=plan['native_operator_fraction']
                    if 'action' in spec:
                        after=inventory(target)
                        result['new_file_bytes']=sum(size for path,size in after.pop('_files').items() if path not in before['_files'])
                        result.update(after)
                        result['logical_changed_rows']=before['logical_changed_rows']
                        result['logical_changed_rows_per_s']=before['logical_changed_rows']/(ms/1000)
                        result['storage_bytes_before']=before['output_bytes']
                        result['storage_growth_bytes']=result['output_bytes']-before['output_bytes']
                        if spec['action']=='materialize': result['output_rows_per_s']=row_count/(ms/1000)
                        result['plan_scope']='verification read; not the write execution plan'
                    else: result['plan_scope']='timed query'
                if warmup and not result['valid']: raise ValueError(f'{name}: warm-up answer mismatch')
            except Exception as error:
                result.update(status='error',valid=False,error=str(error)[:4000])
                if 'drop-failed' in str(error): result['cache']='drop-failed'
            finally:
                if distributed and actual is not None:actual.unpersist()
            return result
        for sample in plan['warmups']:
            name=sample['q']
            result=execute(name,sample['run'],sample['stream'],True)
            if result['status'] not in ('ok','unsupported'):
                record['results'].append(result)
                raise ValueError(f'warm-up failed: {name}: {result.get("error", "invalid")}')
        def stream_run(stream):
            results=[]
            for sample in plan['samples']:
                if sample['stream']!=stream:continue
                name,run=sample['q'],sample['run']
                result=execute(name,run,stream)
                results.append(result)
                print(f'{name} stream={stream} run={run} {result["status"]}',flush=True)
            return results
        with ThreadPoolExecutor(max_workers=a.streams) as pool:
            for results in pool.map(stream_run,range(a.streams)): record['results'].extend(results)
        timed=[r for r in record['results'] if 't0' in r]
        record['timed_elapsed_s']=max(r['t1'] for r in timed)-min(r['t0'] for r in timed) if timed else 0
        record['parity_ok']=bool(record['results']) and all(r['status'] in ('ok','unsupported') for r in record['results'])
    except Exception as error:
        record['error']=str(error)[:4000]
    finally:
        if distributed:
            for expected,_ in references.values():expected.unpersist()
        output.write_text(json.dumps(record,indent=2,default=str))
        spark.stop()
    print(f'VALIDITY {record["parity_ok"]} -> {output}',flush=True)
    return 0 if record['parity_ok'] else 1


if __name__=='__main__':
    sys.exit(main())
