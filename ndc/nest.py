"""Bound legacy nesting by key range while preserving its single-file layout."""
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile


def main():
    memory=os.environ.get('NDC_PREP_MEMORY','8GB')
    if not re.fullmatch(r'[1-9][0-9]*(\.[0-9]+)?(?:KB|MB|GB|TB|KiB|MiB|GiB|TiB)',memory):
        raise ValueError('Invalid NDC_PREP_MEMORY (example: 16GiB)')
    span=int(os.environ.get('NDC_PREP_KEY_SPAN','250000'))
    if span<1:raise ValueError('NDC_PREP_KEY_SPAN must be positive')
    def sql(query,json_output=False):
        result=subprocess.run(['duckdb','tpch.duckdb','-bail',*(['-json'] if json_output else [])],
                              input=f"SET threads=4; SET memory_limit='{memory}';\n"+query,text=True,stdout=subprocess.PIPE,check=True)
        return json.loads(result.stdout or '[]') if json_output else None
    buckets=sql(f'SELECT DISTINCT (o_orderkey // {span}) - CASE WHEN o_orderkey<0 AND o_orderkey % {span} != 0 THEN 1 ELSE 0 END AS bucket FROM orders ORDER BY bucket;',True)
    template=Path(__file__).with_name('nested.sql').read_text().split('COPY (',1)[1].rsplit(") TO ",1)[0]
    with tempfile.TemporaryDirectory(prefix='.nested-',dir='data') as tmp:
        for index,row in enumerate(buckets or [{'bucket':0}]):
            low=row['bucket']*span;high=low+span
            query=template.replace('  GROUP BY ALL',f'  WHERE o_orderkey >= {low} AND o_orderkey < {high} AND l_orderkey >= {low} AND l_orderkey < {high}\n  GROUP BY ALL')
            sql(f"COPY ({query}) TO '{tmp}/part-{index:06d}.parquet' (FORMAT PARQUET,ROW_GROUP_SIZE 16384);")
        sql(f"COPY (SELECT * FROM read_parquet('{tmp}/*.parquet') ORDER BY o_orderkey) TO 'data/orders_nested.parquet' (FORMAT PARQUET,ROW_GROUP_SIZE 16384);")
    print(f'NDC nested preparation: memory_limit={memory} key_span={span} partitions={len(buckets)}')


if __name__=='__main__':main()
