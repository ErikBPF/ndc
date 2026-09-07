"""Prepare matched flat and nested table-format inputs; not a timed benchmark."""
import argparse
from pathlib import Path
import json
from provenance import dataset, format_identity
from pyspark.sql import SparkSession

p=argparse.ArgumentParser()
p.add_argument('--fmt',required=True,choices=['iceberg','delta'])
p.add_argument('--data',required=True)
a=p.parse_args()
spark=SparkSession.builder.appName('ndc-format-preparation').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
for path in sorted(Path(a.data).glob('*.parquet')):
    table=path.stem
    df=spark.read.parquet(str(path))
    if a.fmt=='iceberg':
        df.writeTo(f'local_tpch.{table}').using('iceberg').createOrReplace()
    else:
        df.write.format('delta').mode('overwrite').save(str(Path(a.data).parent/'data_delta'/table))
    print(f'WROTE {a.fmt}/{table}',flush=True)
data=Path(a.data)
(data.parent/f'format-{a.fmt}.json').write_text(json.dumps({'dataset_id':dataset(data)['dataset_id'],'format_id':format_identity(data,a.fmt),'spark_version':spark.version},indent=2))
spark.stop()
