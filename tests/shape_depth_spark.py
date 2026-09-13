"""Qualify shared synthetic depth fixtures and SQL using an independent oracle."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from pyspark.sql import SparkSession

ROOT=Path(__file__).resolve().parents[1]
profile=json.loads((ROOT/'datagen/shape-workloads.json').read_text())['profiles']['ci']
spark=(SparkSession.builder.appName('ndc-shape-depth').config('spark.sql.shuffle.partitions','2')
       .config('spark.ui.enabled','false').getOrCreate())
spark.sparkContext.setLogLevel('ERROR')
try:
    with tempfile.TemporaryDirectory() as tmp:
        for seed,layout in [(seed,layout) for seed in profile['data_seeds'] for layout in profile['layouts']]:
            out=Path(tmp)/f'{seed}-{layout}'
            subprocess.run([sys.executable,str(ROOT/'datagen/shapes.py'),'--out',str(out),
                            '--parents',str(profile['parents']),'--fanout',str(profile['fanout']),'--data-seed',str(seed),
                            '--layout',layout,'--null-rate','0.3','--entropy','high'],check=True)
            subprocess.run([sys.executable,str(ROOT/'datagen/shapes.py'),'--verify',str(out)],check=True)
            contract=json.loads((out/'shape-contract.json').read_text())
            for name in contract['tables']:
                spark.read.parquet(str(out/(name+'.parquet'))).createOrReplaceTempView(name)
            for op,evidence in contract['answers'].items():
                expected=[json.loads(line) for line in (out/evidence['file']).read_text().splitlines()]
                for depth in ('flat','d1','d3','d5'):
                    actual=spark.sql((ROOT/f'ndc/queries/sd_{op}_{depth}.sql').read_text()).collect()
                    assert [r.asDict(recursive=True) for r in actual]==expected,(seed,layout,op,depth,actual,expected)
            # Full values and parent membership, not just aggregate checksums.
            for depth in (1,3,5):
                table=spark.table(f'shape_depth{depth}_v2')
                lengths=contract['depths'][str(depth)]['array_lengths']
                assert table.where('children IS NULL').count()==lengths.get('null',0)
                assert table.where('children IS NOT NULL AND size(children)=0').count()==lengths.get('0',0)
                q=(ROOT/f'ndc/queries/sd_filter_d{depth}.sql').read_text().split('\n')[0]+'\nSELECT * FROM leaves'
                actual=spark.sql(q)
                expected=spark.table('shape_leaves_v2')
                assert actual.exceptAll(expected).count()==0
                assert expected.exceptAll(actual).count()==0
            print(f'SHAPE_DEPTH_SPARK_OK seed={seed} layout={layout}: full bags and 16 query answers')
finally:
    spark.stop()
