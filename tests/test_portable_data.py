"""Shared dataset contract, exercised through its public CLI and SQL adapters."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[1]
ROWS={
 'region':['50|AFRICA|region comment|'],
 'nation':['40|ALGERIA|50|nation comment|'],
 'supplier':['20|Supplier|address|40|phone|12.34|supplier comment|'],
 'customer':['30|Customer|address|40|phone|56.78|BUILDING|customer comment|'],
 'part':['10|Part|MFGR|BRAND|TYPE|3|BOX|99.01|part comment|'],
 'partsupp':['10|20|100|1.25|supply comment|'],
 'orders':['2|30|O|0.00|1994-01-02|2-HIGH|Clerk|0|empty order|',
           '1|30|F|123.45|1994-01-01|1-URGENT|Clerk|0|order comment|'],
 'lineitem':['1|10|20|2|2.00|20.20|0.02|0.03|N|O|1994-02-01|1994-02-02|1994-02-03|DELIVER|AIR|second comment|',
             '1|10|20|1|1.00|10.10|0.01|0.02|R|F|1994-01-01|1994-01-02|1994-01-03|COLLECT|RAIL|first comment|']}

def sql(query,cwd):
    p=subprocess.run(['duckdb','-json','-c',query],cwd=cwd,text=True,capture_output=True)
    if p.returncode:raise AssertionError(p.stderr)
    return json.loads(p.stdout) if p.stdout.strip() else []

class PortableDataTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.inputs=self.root/'input';self.inputs.mkdir()
        for table,rows in ROWS.items():(self.inputs/f'{table}.tbl').write_text('\n'.join(rows)+'\n')
    def generate(self,out='dataset',*args,env=None):
        return subprocess.run([sys.executable,str(ROOT/'datagen/generate.py'),'--input',str(self.inputs),'--out',str(self.root/out),'--source-label','test fixture',*args],capture_output=True,text=True,env=env)
    def test_preparation_memory_configuration_and_invalid_input(self):
        env=dict(os.environ,NDC_PREP_MEMORY='256MiB')
        result=self.generate('env',env=env)
        self.assertEqual(result.returncode,0,result.stderr)
        record=json.loads((self.root/'env/dataset.json').read_text())
        self.assertEqual(record['preparation']['memory_limit'],'256MiB')
        result=self.generate('cli','--memory-limit','512MiB',env=env)
        self.assertEqual(result.returncode,0,result.stderr)
        other=json.loads((self.root/'cli/dataset.json').read_text())
        self.assertEqual(other['preparation']['memory_limit'],'512MiB')
        self.assertEqual(record['dataset_id'],other['dataset_id'])
        result=self.generate('invalid','--memory-limit',"4GB'; SELECT 1; --")
        self.assertNotEqual(result.returncode,0)
        self.assertFalse((self.root/'invalid').exists())
    def test_lossless_roundtrip_order_empty_parent_and_metadata(self):
        p=self.generate();self.assertEqual(p.returncode,0,p.stderr)
        out=self.root/'dataset';m=json.loads((out/'dataset.json').read_text())
        self.assertEqual(m['status'],'ok');self.assertEqual(m['schema_version'],2)
        self.assertEqual(m['tables']['orders_nested_v2']['rows'],2)
        rows=sql("SELECT o_orderkey,o_comment,lineitems FROM read_parquet('orders_nested_v2.parquet') ORDER BY o_orderkey",out)
        self.assertEqual(rows[0]['o_comment'],'order comment')
        self.assertEqual([x['l_linenumber'] for x in rows[0]['lineitems']],[1,2])
        self.assertEqual(rows[0]['lineitems'][0]['l_suppkey'],20)
        self.assertEqual(rows[0]['lineitems'][0]['l_shipinstruct'],'COLLECT')
        self.assertEqual(rows[0]['lineitems'][0]['l_comment'],'first comment')
        self.assertEqual(rows[1]['lineitems'],[])
        self.assertNotEqual(self.generate().returncode,0)
        p=self.generate('repeat');self.assertEqual(p.returncode,0,p.stderr)
        other=json.loads((self.root/'repeat/dataset.json').read_text())
        self.assertEqual(m['schema_id'],other['schema_id']);self.assertEqual(m['dataset_id'],other['dataset_id'])
        p=subprocess.run([sys.executable,str(ROOT/'datagen/generate.py'),'--input',str(out),'--input-format','parquet','--out',str(self.root/'reimport'),'--source-label','canonical input'],capture_output=True,text=True)
        self.assertEqual(p.returncode,0,p.stderr)
    def test_duplicate_and_orphan_children_fail_without_valid_dataset(self):
        for rows in [ROWS['lineitem']+[ROWS['lineitem'][0]], [ROWS['lineitem'][0].replace('1|10|','999|10|',1)]]:
            with self.subTest(rows=rows):
                (self.inputs/'lineitem.tbl').write_text('\n'.join(rows)+'\n')
                out='bad'+str(len(rows));p=self.generate(out)
                self.assertNotEqual(p.returncode,0)
                marker=self.root/out/'dataset.json'
                self.assertEqual(json.loads(marker.read_text())['status'],'failed')
                self.assertIn('duplicate key' if len(rows)==3 else 'orphan key',p.stderr)
    def test_text_input_cannot_round_decimal_or_integer_values(self):
        for index,old,new in [(0,'20.20','20.205'),(1,'1|10|','1.5|10|')]:
            with self.subTest(value=new):
                rows=list(ROWS['lineitem']);rows[index]=rows[index].replace(old,new,1)
                (self.inputs/'lineitem.tbl').write_text('\n'.join(rows)+'\n')
                result=self.generate('round'+str(index))
                self.assertNotEqual(result.returncode,0)
                self.assertIn('invalid source value: lineitem.',result.stderr)

    def test_parquet_input_rejects_unmodeled_columns(self):
        p=self.generate();self.assertEqual(p.returncode,0,p.stderr)
        out=self.root/'dataset'
        sql("COPY (SELECT *, 42 AS extra FROM read_parquet('orders.parquet')) TO 'changed.parquet';",out)
        (out/'changed.parquet').replace(out/'orders.parquet')
        p=subprocess.run([sys.executable,str(ROOT/'datagen/generate.py'),'--input',str(out),'--input-format','parquet','--out',str(self.root/'extra')],capture_output=True,text=True)
        self.assertNotEqual(p.returncode,0)
        self.assertIn('unexpected source columns: orders',p.stderr)

    def test_verify_rejects_modified_data(self):
        p=self.generate();self.assertEqual(p.returncode,0,p.stderr)
        out=self.root/'dataset'
        command=[sys.executable,str(ROOT/'datagen/generate.py'),'--verify',str(out)]
        p=subprocess.run(command,capture_output=True,text=True)
        self.assertEqual(p.returncode,0,p.stderr)
        with (out/'orders_nested_v2.parquet').open('ab') as f:f.write(b'corruption')
        self.assertNotEqual(subprocess.run(command,capture_output=True).returncode,0)

    def test_published_ddl_matches_the_schema(self):
        for engine in ('duckdb','spark','snowflake'):
            result=subprocess.run([sys.executable,str(ROOT/'datagen/generate.py'),'--ddl',engine],capture_output=True,text=True,check=True)
            self.assertEqual(result.stdout,(ROOT/f'engines/{engine}/ddl.sql').read_text())

    def test_outer_adapter_preserves_null_empty_and_duplicate_elements(self):
        setup="""CREATE TABLE orders_nested_v2(o_orderkey BIGINT,lineitems STRUCT(l_linenumber INTEGER)[]);
        INSERT INTO orders_nested_v2 VALUES (1,NULL),(2,[]),(3,[NULL]),(4,[{'l_linenumber':1},{'l_linenumber':1}]);"""
        query=(ROOT/'engines/duckdb/queries/outer_item_counts.sql').read_text()
        self.assertEqual(sql(setup+query,self.root),[{'o_orderkey':i,'item_count':2 if i==4 else 0} for i in range(1,5)])

    def test_duckdb_ddl_and_queries_use_shared_data(self):
        p=self.generate();self.assertEqual(p.returncode,0,p.stderr)
        out=self.root/'dataset';ddl=(ROOT/'engines/duckdb/ddl.sql').read_text()
        setup=ddl+"\nINSERT INTO orders_nested_v2 SELECT * FROM read_parquet('orders_nested_v2.parquet');\n"
        q=(ROOT/'engines/duckdb/queries/lineitem_totals.sql').read_text()
        self.assertEqual(sql(setup+q,out),[{'o_orderkey':1,'item_count':2,'revenue':'0.5050'}])
        q=(ROOT/'engines/duckdb/queries/outer_item_counts.sql').read_text()
        self.assertEqual(sql(setup+q,out),[{'o_orderkey':1,'item_count':2},{'o_orderkey':2,'item_count':0}])
