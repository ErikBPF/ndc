"""Render the bounded shape-depth workload matrix; no consuming-engine datagen."""
import argparse
import json
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
CONTRACT=json.loads((ROOT/'datagen/shape-workloads.json').read_text())
DEPTHS=tuple(CONTRACT['depths'])
OPERATIONS={op:spec['operation'] for op,spec in CONTRACT['operations'].items()}


def query(engine, operation, depth=None):
    if depth is None:
        source='SELECT * FROM shape_leaves_v2'
    else:
        table=f'shape_depth{depth}_v2'
        if engine=='duckdb':
            expansion=' CROSS JOIN UNNEST(children) u1(x1)'
            expansion+=''.join(f' CROSS JOIN UNNEST(x{i-1}.children) u{i}(x{i})' for i in range(2,depth+1))
            source=f'SELECT parent_id,x{depth}.* FROM {table}'+expansion
        else:
            expansion=' LATERAL VIEW explode(children) u1 AS x1'
            expansion+=''.join(f' LATERAL VIEW explode(x{i-1}.children) u{i} AS x{i}' for i in range(2,depth+1))
            source=f'SELECT parent_id,x{depth}.* FROM {table}'+expansion
    expressions={
        'filter':'SELECT count(*) AS item_count, CAST(sum(amount) AS BIGINT) AS total FROM leaves WHERE amount>=50',
        'join':'SELECT p.bucket,count(*) AS item_count,CAST(sum(l.amount) AS BIGINT) AS total FROM leaves l JOIN shape_parents_v2 p ON l.parent_id=p.parent_id WHERE l.amount IS NOT NULL GROUP BY p.bucket ORDER BY p.bucket',
        'topn':'SELECT parent_id,CAST(sum(amount) AS BIGINT) AS total FROM (SELECT parent_id,amount,row_number() OVER (PARTITION BY parent_id ORDER BY amount DESC,leaf_id) AS rn FROM leaves WHERE amount IS NOT NULL) ranked WHERE rn<=2 GROUP BY parent_id ORDER BY parent_id',
        'transform':("SELECT parent_id,list(struct_pack(leaf_id:=leaf_id,adjusted:=amount*2,tag:=tag,padding:=padding) ORDER BY leaf_id) AS selected FROM leaves WHERE amount>=50 GROUP BY parent_id ORDER BY parent_id" if engine=='duckdb' else "SELECT parent_id,sort_array(collect_list(named_struct('leaf_id',leaf_id,'adjusted',amount*2,'tag',tag,'padding',padding))) AS selected FROM leaves WHERE amount>=50 GROUP BY parent_id ORDER BY parent_id")}
    return f'WITH leaves AS ({source})\n{expressions[operation]};\n'


def write():
    manifest={}
    for engine in ('duckdb','spark'):
        for op in OPERATIONS:
            for depth in (None,*DEPTHS):
                name=f'sd_{op}_'+('flat' if depth is None else f'd{depth}')
                text=query(engine,op,depth)
                if engine=='duckdb':
                    (ROOT/f'engines/{engine}/queries/{name}.sql').write_text(text)
                if engine=='spark':
                    (ROOT/f'ndc/queries/{name}.sql').write_text(text)
                    manifest[name]=dict(sql=f'queries/{name}.sql',reference=f'queries/sd_{op}_flat.sql',
                                        family='shape-depth',operation=OPERATIONS[op],layout='flat' if depth is None else 'nested',ordered=True)
    (ROOT/'ndc/queries/manifest-shape-depth.json').write_text(json.dumps(manifest,indent=2)+'\n')


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--write',action='store_true',required=True)
    parser.parse_args()
    write()
