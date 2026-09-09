"""Spark preparation backend. Submit generate.py --backend spark with spark-submit."""
import json
from functools import reduce
from operator import or_

from generate import SCHEMA, identity, file_evidence, parquet_files


def generate(inputs,out,fmt,label):
    from pyspark.sql import SparkSession, functions as F

    paths={name:inputs/(name+'.'+fmt) for name in SCHEMA['tables']}
    for path in paths.values():
        if fmt=='parquet':parquet_files(path)
        elif not path.is_file():raise ValueError(f'missing source table: {path.name}')
    source={name:file_evidence(path) for name,path in paths.items()}
    out.mkdir(parents=True,exist_ok=False)
    marker={'status':'failed','schema_version':2,'schema_id':identity(SCHEMA),
            'source_label':label,'source_format':fmt,'source_files':source}
    manifest=out/'dataset.json'
    manifest.write_text(json.dumps(marker,indent=2)+'\n')
    spark=(SparkSession.builder.appName('ndc-prepare')
           .config('spark.sql.ansi.enabled','true')
           # Sort aggregation spills the input and retains one order's array at a time.
           .config('spark.sql.execution.useObjectHashAggregateExec','false')
           .getOrCreate())
    def reject(frame,message):
        if frame.limit(1).count():raise ValueError(message)
    def write(frame,name,keys):
        (frame.sortWithinPartitions(*keys).write.mode('error')
         .option('compression','zstd').option('parquet.block.size',32*1024*1024)
         .parquet(str(out/(name+'.parquet'))))
        return spark.read.parquet(str(out/(name+'.parquet')))
    try:
        marker['preparation']={'backend':'spark','master':spark.sparkContext.master,
                              'shuffle_partitions':spark.conf.get('spark.sql.shuffle.partitions'),
                              'driver_memory':spark.sparkContext.getConf().get('spark.driver.memory','default'),
                              'executor_memory':spark.sparkContext.getConf().get('spark.executor.memory','default')}
        tables={}
        for name,spec in SCHEMA['tables'].items():
            fields=spec['fields'];names=[f['name'] for f in fields]
            if fmt=='tbl':
                raw=spark.read.text(str(paths[name])).select(F.split('value',r'\|',-1).alias('fields'))
                reject(raw.where((F.size('fields')!=len(fields)+1) | (F.element_at('fields',-1)!='')),
                       f'invalid trailing delimiter or field count: {name}')
                raw=raw.select(*[F.col('fields')[i].alias(n) for i,n in enumerate(names)])
                invalid=[]
                for field in fields:
                    kind=field['type'];pattern=None
                    if kind in ('BIGINT','INTEGER'):pattern=r'^[+-]?[0-9]+$'
                    elif kind.startswith('DECIMAL'):pattern=r'^[+-]?[0-9]+(\.[0-9]{1,2})?$'
                    elif kind=='DATE':pattern=r'^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                    if pattern:invalid.append(~F.col(field['name']).rlike(pattern))
                if invalid:reject(raw.where(reduce(or_,invalid)),f'invalid source value: {name}')
            else:
                raw=spark.read.option('mergeSchema','true').parquet(*[str(p) for p in parquet_files(paths[name])])
                if sorted(raw.columns)!=sorted(names):raise ValueError(f'unexpected source columns: {name}')
                # Compare typed values before persisting so decimal rounding cannot hide a change.
                changed=[~F.col(f['name']).cast(f['type'].replace('VARCHAR','STRING')).eqNullSafe(F.col(f['name'])) for f in fields]
                reject(raw.where(reduce(or_,changed)),f'lossy input cast: {name}')
            typed=raw.select(*[F.col(f['name']).cast(f['type'].replace('VARCHAR','STRING')).alias(f['name']) for f in fields])
            tables[name]=write(typed,name,spec['primary_key'])
            frame=tables[name]
            reject(frame.where(reduce(or_,[F.col(n).isNull() for n in names])),f'invalid null or duplicate key: {name}')
            reject(frame.groupBy(*spec['primary_key']).count().where('count > 1'),f'invalid null or duplicate key: {name}')
        for child,keys,parent,refs in SCHEMA['foreign_keys']:
            c=tables[child].alias('c');p=tables[parent].alias('p')
            condition=reduce(lambda a,b:a & b,[c[k]==p[r] for k,r in zip(keys,refs)])
            reject(c.join(p,condition,'left_anti'),f'orphan key: {child}')
        child=[f['name'] for f in SCHEMA['tables']['lineitem']['fields'] if f['name']!='l_orderkey']
        ordered=['l_linenumber']+[n for n in child if n!='l_linenumber']
        # ponytail: one parent's array must fit in a task; oversized non-TPC-H parents need a separate contract.
        grouped=tables['lineitem'].groupBy('l_orderkey').agg(F.sort_array(F.collect_list(F.struct(*ordered))).alias('items'))
        grouped=grouped.select('l_orderkey',F.transform('items',lambda x:F.struct(*[x[n].alias(n) for n in child])).alias('lineitems'))
        nested=(tables['orders'].join(grouped,F.col('o_orderkey')==F.col('l_orderkey'),'left')
                .drop('l_orderkey').withColumn('lineitems',F.coalesce('lineitems',F.array())))
        nested=write(nested,'orders_nested_v2',['o_orderkey'])
        headers=nested.select(*tables['orders'].columns)
        leaves=nested.select(F.col('o_orderkey').alias('l_orderkey'),F.explode('lineitems').alias('item')).select('l_orderkey','item.*')
        for original,derived in [(tables['orders'],headers),(tables['lineitem'],leaves)]:
            reject(original.exceptAll(derived).unionByName(derived.exceptAll(original)),'lossless roundtrip failed')
        tables['orders_nested_v2']=nested
        evidence={name:{'rows':frame.count(),'sha256':file_evidence(out/(name+'.parquet'))} for name,frame in tables.items()}
        if source!={name:file_evidence(path) for name,path in paths.items()}:raise ValueError('source changed during generation')
        marker.update(status='ok',tables=evidence,dataset_id=identity({'schema_id':marker['schema_id'],'tables':evidence}),writer='Spark '+spark.version)
        manifest.write_text(json.dumps(marker,indent=2)+'\n')
        return marker
    finally:
        spark.stop()
