"""Build a shared, lossless nested Parquet dataset from eight TPC-H source tables."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess

ROOT=Path(__file__).resolve().parent
SCHEMA=json.loads((ROOT/'schema.json').read_text())


def identity(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()


def digest(path):
    with path.open('rb') as stream:return hashlib.file_digest(stream,'sha256').hexdigest()


def literal(value):
    return "'"+str(value).replace("'","''")+"'"


def parquet_files(path):
    files=sorted(path.glob('*.parquet')) if path.is_dir() else [path]
    if not files or any(not f.is_file() or f.is_symlink() for f in files):
        raise ValueError(f'missing or unsafe source table: {path.name}')
    return files


def file_evidence(path):
    if path.is_file():return digest(path)
    return {f.name:digest(f) for f in parquet_files(path)}


def verify(directory):
    record=json.loads((directory/'dataset.json').read_text())
    tables=record.get('tables',{})
    if (record.get('status')!='ok' or record.get('schema_version')!=2
            or record.get('schema_id')!=identity(SCHEMA)
            or set(tables)!=set(SCHEMA['tables'])|{'orders_nested_v2'}):
        raise ValueError('invalid dataset contract')
    for name,table in tables.items():
        if (type(table['rows']) is not int or table['rows']<0
                or file_evidence(directory/(name+'.parquet'))!=table['sha256']):
            raise ValueError(f'invalid table evidence: {name}')
    if record['dataset_id']!=identity({'schema_id':record['schema_id'],'tables':tables}):
        raise ValueError('invalid dataset identity')
    return record


def ddl(engine):
    def field(f):
        kind=f['type'].replace('VARCHAR','STRING') if engine=='spark' else f['type']
        name='"'+f['name']+'"' if engine=='snowflake' else f['name']
        return name+(':' if engine=='spark' else ' ')+kind
    header=',\n    '.join(f["name"]+' '+(f['type'].replace('VARCHAR','STRING') if engine=='spark' else f['type'])+' NOT NULL' for f in SCHEMA['tables']['orders']['fields'])
    child=',\n        '.join(field(f) for f in SCHEMA['tables']['lineitem']['fields'] if f['name']!='l_orderkey')
    nested={'duckdb':f'STRUCT(\n        {child}\n    )[]','spark':f'ARRAY<STRUCT<\n        {child}\n    >>','snowflake':f'ARRAY(OBJECT(\n        {child}\n    ) NOT NULL)'}[engine]
    return f'-- Schema v2; nested non-null and key invariants are validated by datagen.\nCREATE TABLE orders_nested_v2 (\n    {header},\n    lineitems {nested} NOT NULL\n)'+(' USING PARQUET' if engine=='spark' else '')+';\n'


def generate(inputs,out,fmt,label,memory_limit='4GB',key_span=250000):
    if not re.fullmatch(r'[1-9][0-9]*(\.[0-9]+)?(?:KB|MB|GB|TB|KiB|MiB|GiB|TiB)',memory_limit):
        raise ValueError('invalid preparation memory limit (example: 16GiB)')
    paths={name:inputs/f'{name}.{fmt}' for name in SCHEMA['tables']}
    for path in paths.values():
        if fmt=='parquet':parquet_files(path)
        elif not path.is_file():raise ValueError(f'missing source table: {path.name}')
    if key_span<1:raise ValueError('key span must be positive')
    source={name:file_evidence(path) for name,path in paths.items()}
    out.mkdir(parents=True,exist_ok=False)
    marker={'status':'failed','schema_version':2,'schema_id':identity(SCHEMA),'source_label':label,'source_format':fmt,'source_files':source,
            'preparation':{'memory_limit':memory_limit,'threads':4,'key_span':key_span,'backend':'duckdb'}}
    (out/'dataset.json').write_text(json.dumps(marker,indent=2)+'\n')
    statements=[]
    for name,spec in SCHEMA['tables'].items():
        fields=spec['fields']
        if fmt=='tbl':
            types={f['name']:'VARCHAR' for f in fields};types['_end']='VARCHAR'
            mapping='{'+','.join(literal(k)+':'+literal(v) for k,v in types.items())+'}'
            source_sql=f"read_csv({literal(paths[name])},delim='|',header=false,auto_detect=false,quote='',escape='',force_not_null=[{','.join(literal(k) for k in types)}],columns={mapping})"
            statements.append(f"CREATE TABLE raw_{name} AS SELECT * FROM {source_sql};")
            statements.append(f"SELECT CASE WHEN EXISTS(SELECT 1 FROM raw_{name} WHERE _end IS DISTINCT FROM '') THEN error('invalid trailing delimiter: {name}') END;")
            source_sql=f'raw_{name}'
            for f in fields:
                pattern=None
                if f['type'] in ('BIGINT','INTEGER'):pattern=r'[+-]?[0-9]+'
                elif f['type'].startswith('DECIMAL'):pattern=r'[+-]?[0-9]+(\.[0-9]{1,2})?'
                elif f['type']=='DATE':pattern=r'[0-9]{4}-[0-9]{2}-[0-9]{2}'
                if pattern:
                    statements.append(f"SELECT CASE WHEN EXISTS(SELECT 1 FROM {source_sql} WHERE NOT regexp_full_match({f['name']},{literal(pattern)})) THEN error('invalid source value: {name}.{f['name']}') END;")
        else:
            source_sql=f'read_parquet([{",".join(literal(f) for f in parquet_files(paths[name]))}], union_by_name=true)'
            names=','.join(literal(f['name']) for f in fields)
            statements.append(f"SELECT CASE WHEN (SELECT list_sort(list(column_name)) FROM (DESCRIBE SELECT * FROM {source_sql})) != list_sort([{names}]) THEN error('unexpected source columns: {name}') END;")
        casts=','.join(f"CAST({f['name']} AS {f['type']}) AS {f['name']}" for f in fields)
        statements.append(f'CREATE TABLE {name} AS SELECT {casts} FROM {source_sql};')
        nulls=' OR '.join(f["name"]+' IS NULL' for f in fields)
        pk=','.join(spec['primary_key'])
        statements.append(f"SELECT CASE WHEN EXISTS(SELECT 1 FROM {name} WHERE {nulls}) OR EXISTS(SELECT {pk} FROM {name} GROUP BY {pk} HAVING count(*)>1) THEN error('invalid null or duplicate key: {name}') END;")
        # Casts must not silently change values from typed input tables.
        if fmt=='parquet':
            different=' OR '.join(f"CAST({f['name']} AS {f['type']}) IS DISTINCT FROM {f['name']}" for f in fields)
            statements.append(f"SELECT CASE WHEN EXISTS(SELECT 1 FROM {source_sql} WHERE {different}) THEN error('lossy input cast: {name}') END;")
        if fmt=='tbl':statements.append(f'DROP TABLE raw_{name};')
    for child,keys,parent,refs in SCHEMA['foreign_keys']:
        on=' AND '.join(f'c.{k}=p.{r}' for k,r in zip(keys,refs))
        statements.append(f"SELECT CASE WHEN EXISTS(SELECT 1 FROM {child} c WHERE NOT EXISTS(SELECT 1 FROM {parent} p WHERE {on})) THEN error('orphan key: {child}') END;")
    child=[f['name'] for f in SCHEMA['tables']['lineitem']['fields'] if f['name']!='l_orderkey']
    pack=','.join(f'{name}:=l.{name}' for name in child)
    db=out/'.build.duckdb'
    def execute(sql):
        result=subprocess.run(['duckdb',str(db),'-bail'],input=f"SET threads=4; SET memory_limit={literal(memory_limit)};\n"+sql,capture_output=True,text=True)
        if result.returncode:raise ValueError(result.stderr.strip())
    execute('\n'.join(statements))
    statements=[]
    buckets=subprocess.run(['duckdb',str(db),'-json','-c',f"SET memory_limit={literal(memory_limit)}; SET threads=4; SELECT DISTINCT (o_orderkey // {key_span}) - CASE WHEN o_orderkey<0 AND o_orderkey % {key_span} != 0 THEN 1 ELSE 0 END AS bucket FROM orders ORDER BY bucket"],capture_output=True,text=True,check=True)
    nested=out/'orders_nested_v2.parquet';nested.mkdir()
    # One aggregation per key range; completed partitions are immediately flushed.
    for index,row in enumerate(json.loads(buckets.stdout or '[]') or [{'bucket':0}]):
        low=row['bucket']*key_span;high=low+key_span
        query=f"SELECT o.*, coalesce((SELECT list(struct_pack({pack}) ORDER BY l_linenumber) FROM (SELECT * FROM lineitem WHERE l_orderkey >= {low} AND l_orderkey < {high}) l WHERE l.l_orderkey=o.o_orderkey),[]) AS lineitems FROM (SELECT * FROM orders WHERE o_orderkey >= {low} AND o_orderkey < {high}) o;"
        execute(f"COPY ({query.rstrip(';')} ORDER BY o_orderkey) TO {literal(nested/f'part-{index:06d}.parquet')} (FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE 16384);")
    statements.append(f"CREATE VIEW orders_nested_v2 AS SELECT * FROM read_parquet({literal(nested/'*.parquet')});")
    # Exact bag checks cover all headers and all child fields, including duplicates.
    headers=','.join(f['name'] for f in SCHEMA['tables']['orders']['fields'])
    flat=','.join(f['name'] for f in SCHEMA['tables']['lineitem']['fields'])
    leaves=','.join('o_orderkey AS l_orderkey' if f['name']=='l_orderkey' else 'x.'+f['name'] for f in SCHEMA['tables']['lineitem']['fields'])
    statements.append(f'CREATE VIEW leaves AS SELECT {leaves} FROM orders_nested_v2, UNNEST(lineitems) AS u(x);')
    for original,derived in [('SELECT * FROM orders',f'SELECT {headers} FROM orders_nested_v2'),(f'SELECT {flat} FROM lineitem',f'SELECT {flat} FROM leaves')]:
        statements.append(f"SELECT CASE WHEN EXISTS(({original} EXCEPT ALL {derived}) UNION ALL ({derived} EXCEPT ALL {original})) THEN error('lossless roundtrip failed') END;")
    specs={**SCHEMA['tables'],'orders_nested_v2':{'primary_key':['o_orderkey']}}
    for name,spec in SCHEMA['tables'].items():
        statements.append(f"COPY (SELECT * FROM {name} ORDER BY {','.join(spec['primary_key'])}) TO {literal(out/(name+'.parquet'))} (FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE 122880);")
    execute('\n'.join(statements))
    counts=subprocess.run(['duckdb',str(db),'-json','-c',' UNION ALL '.join(f"SELECT '{name}' AS name,count(*) AS rows FROM {name}" for name in specs)],capture_output=True,text=True,check=True)
    tables={r['name']:{'rows':r['rows'],'sha256':file_evidence(out/(r['name']+'.parquet'))} for r in json.loads(counts.stdout)}
    if source!={name:file_evidence(path) for name,path in paths.items()}:raise ValueError('source changed during generation')
    marker.update(status='ok',tables=tables,dataset_id=identity({'schema_id':marker['schema_id'],'tables':tables}),
                  writer=subprocess.check_output(['duckdb','--version'],text=True).strip())
    db.unlink()
    (out/'dataset.json').write_text(json.dumps(marker,indent=2)+'\n')
    return marker


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--input',type=Path);p.add_argument('--out',type=Path)
    p.add_argument('--input-format',choices=['tbl','parquet'],default='tbl')
    p.add_argument('--source-label',default='unrecorded')
    p.add_argument('--memory-limit',default=os.environ.get('NDC_PREP_MEMORY','4GB'),
                   help='DuckDB preparation memory (default: NDC_PREP_MEMORY or 4GB)')
    p.add_argument('--key-span',type=int,default=250000,help='DuckDB order-key range per nested partition')
    p.add_argument('--backend',choices=['duckdb','spark'],default='duckdb')
    p.add_argument('--ddl',choices=['duckdb','spark','snowflake'])
    p.add_argument('--verify',type=Path,help='verify schema and checksums of an existing dataset')
    a=p.parse_args()
    if a.ddl:print(ddl(a.ddl),end='');return
    if not a.verify and (not a.input or not a.out):p.error('--input and --out are required for generation')
    try:
        if a.verify:result=verify(a.verify.resolve())
        elif a.backend=='spark':
            from spark import generate as spark_generate
            result=spark_generate(a.input.resolve(),a.out.resolve(),a.input_format,a.source_label)
        else:result=generate(a.input.resolve(),a.out.resolve(),a.input_format,a.source_label,a.memory_limit,a.key_span)
        print('DATASET_OK',result['dataset_id'])
    except (OSError,ValueError,KeyError,TypeError,subprocess.CalledProcessError) as error:p.exit(1,f'DATASET_INVALID: {error}\n')


if __name__=='__main__':main()
