"""Deterministic synthetic nested data, separate from unmodified TPC-H entities."""
import argparse
import json
from pathlib import Path
import random

ITEM = 'struct<pos:int,amount:bigint,tag:string,attrs:map<string,string>>'


def records(parents=128, fanout=64, width=8, seed=7):
    if parents <= 0 or fanout <= 0 or width < 0:
        raise ValueError('parents/fanout must be positive; width nonnegative')
    rng = random.Random(seed)
    rows = []
    for key in range(parents):
        count = [0, 1, min(4, fanout), min(16, fanout), fanout][key % 5]
        items = [dict(pos=j, amount=None if j % 13 == 12 else rng.randrange(100),
                      tag=None if j % 7 == 6 else f'tag{j % 3}',
                      attrs={'source': f'channel{key % 3}'}) for j in range(count)]
        if key % 11 == 3 and items:
            items[-1] = None
        if key % 11 == 0:
            items = None
        values = items or []
        rows.append(dict(id=key, bucket=f'b{key % 4}', items=items,
                         returns=[dict(i, amount=(i['amount'] or 0)//2)
                                  for i in values if i and i['pos'] % 3 == 0],
                         groups=[{'items': values[j:j+2]} for j in range(0, len(values), 2)],
                         amounts=[i['amount'] if i else None for i in values],
                         tags=[i['tag'] if i else None for i in values],
                         padding={f'p{j}': 'x'*64 for j in range(width)}))
    return rows


def build(spark, directory, parents=128, fanout=64, width=8, seed=7):
    path = Path(directory)
    path.mkdir(parents=True, exist_ok=True)
    rows = records(parents, fanout, width, seed)
    # An empty padding struct is not a writable Parquet type; use one absent leaf.
    padding = ','.join(f'p{j}:string' for j in range(max(width, 1)))
    schema = (f'id:bigint,bucket:string,items:array<{ITEM}>,returns:array<{ITEM}>, '
              f'groups:array<struct<items:array<{ITEM}>>>,amounts:array<bigint>,'
              f'tags:array<string>,padding:struct<{padding}>')
    data = spark.createDataFrame(rows, schema)
    data.write.mode('error').parquet(str(path/'shape.parquet'))
    data.createOrReplaceTempView('shape_build')
    for name, array in [('shape_flat','items'), ('shape_returns','returns')]:
        spark.sql(f'SELECT id, bucket, x.* FROM shape_build LATERAL VIEW explode({array}) e AS x '
                  'WHERE x IS NOT NULL').write.mode('error').parquet(str(path/f'{name}.parquet'))
    data.select('id','bucket').write.mode('error').parquet(str(path/'shape_dim.parquet'))
    (path/'shape.json').write_text(json.dumps({'parents':parents,'fanout':fanout,'width':width,'seed':seed}, indent=2))
    return rows


def full_answer(config):
    return [(r['id'], r['items'], r['returns'], r['groups'], r['amounts'], r['tags'],
             r['padding'] or {'p0':None}) for r in records(**config)]


if __name__ == '__main__':
    from pyspark.sql import SparkSession
    p = argparse.ArgumentParser()
    p.add_argument('--data', required=True)
    for key, default in [('parents',128),('fanout',64),('width',8),('seed',7)]:
        p.add_argument(f'--{key}', type=int, default=default)
    args = vars(p.parse_args())
    directory = args.pop('data')
    spark = SparkSession.builder.appName('ndc-shapes').getOrCreate()
    build(spark, directory, **args)
    spark.stop()
