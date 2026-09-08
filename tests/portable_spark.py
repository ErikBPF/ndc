"""Cross-engine data/SQL qualification; run with spark-submit in the Nix shell."""
from decimal import Decimal
from pathlib import Path
import subprocess
import sys
import tempfile
from pyspark.sql import SparkSession

sys.path.insert(0,str(Path(__file__).resolve().parent))
from test_portable_data import ROOT, ROWS

with tempfile.TemporaryDirectory() as tmp:
    root=Path(tmp);inputs=root/'input';inputs.mkdir();out=root/'dataset'
    for name,rows in ROWS.items():(inputs/(name+'.tbl')).write_text('\n'.join(rows)+'\n')
    sys.path.insert(0,str(ROOT/'datagen'))
    if '--spark-backend' in sys.argv:
        from spark import generate
        generate(inputs,out,'tbl','qualification fixture')
        subprocess.run([sys.executable,str(ROOT/'datagen/generate.py'),'--input',str(out),'--input-format','parquet','--out',str(root/'reimport'),'--memory-limit','256MiB'],check=True)
    else:
        subprocess.run([sys.executable,str(ROOT/'datagen/generate.py'),'--input',str(inputs),'--out',str(out),'--key-span','1','--source-label','qualification fixture'],check=True)
    spark=(SparkSession.builder.appName('ndc-portable-data').config('spark.sql.warehouse.dir',str(root/'warehouse'))
           .config('spark.sql.shuffle.partitions','2').config('spark.ui.enabled','false').getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    try:
        spark.sql((ROOT/'engines/spark/ddl.sql').read_text())
        spark.read.parquet(str(out/'orders_nested_v2.parquet')).write.insertInto('orders_nested_v2')
        nested=spark.table('orders_nested_v2')
        original=spark.read.parquet(str(out/'orders.parquet'))
        assert nested.drop('lineitems').exceptAll(original).count()==0
        assert original.exceptAll(nested.drop('lineitems')).count()==0
        leaves=spark.sql('SELECT o_orderkey AS l_orderkey,li.* FROM orders_nested_v2 LATERAL VIEW explode(lineitems) u AS li')
        original=spark.read.parquet(str(out/'lineitem.parquet'))
        assert leaves.exceptAll(original).count()==0
        assert original.exceptAll(leaves).count()==0
        assert [x.l_linenumber for x in nested.where('o_orderkey=1').first().lineitems]==[1,2]
        assert nested.where('o_orderkey=2').first().lineitems==[]
        for name,expected in [('lineitem_totals',[(1,2,Decimal('0.5050'))]),('outer_item_counts',[(1,2),(2,0)])]:
            actual=spark.sql((ROOT/f'engines/spark/queries/{name}.sql').read_text()).collect()
            assert [tuple(row) for row in actual]==expected,(name,actual)
        spark.createDataFrame([(1,None),(2,[]),(3,[None]),(4,[(1,),(1,)])],
                              'o_orderkey long,lineitems array<struct<l_linenumber:int>>').createOrReplaceTempView('orders_nested_v2')
        actual=spark.sql((ROOT/'engines/spark/queries/outer_item_counts.sql').read_text()).collect()
        assert [tuple(row) for row in actual]==[(1,0),(2,0),(3,0),(4,2)]
        print('PORTABLE_SPARK_OK: complete roundtrip, ordered children, inner/outer query parity')
    finally:spark.stop()

    if '--spark-backend' in sys.argv:
        import json
        for label,rows,message in [
            ('duplicate',ROWS['lineitem']+[ROWS['lineitem'][0]],'duplicate key'),
            ('orphan',[ROWS['lineitem'][0].replace('1|10|','999|10|',1)],'orphan key'),
            ('precision',[ROWS['lineitem'][0].replace('20.20','20.205',1)],'invalid source value')]:
            (inputs/'lineitem.tbl').write_text('\n'.join(rows)+'\n')
            failed=root/label
            try:generate(inputs,failed,'tbl','invalid fixture')
            except ValueError as error:
                assert message in str(error),str(error)
                assert json.loads((failed/'dataset.json').read_text())['status']=='failed'
            else:raise AssertionError(f'{label} input was accepted')
        print('SPARK_PREPARATION_ERRORS_OK: duplicate, orphan, precision; DuckDB reimport passed')
