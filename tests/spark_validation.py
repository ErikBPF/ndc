"""Real Spark checks; run with spark-submit (also exercises a 1 MiB result limit)."""
from decimal import Decimal
from pathlib import Path
import sys
import tempfile
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'ndc'))
from pyspark.sql import SparkSession
import validation

spark=SparkSession.builder.appName('ndc-validation-test').config('spark.driver.maxResultSize','1m').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
try:
    assert hasattr(validation, 'distributed_validate'), 'distributed complete-answer validator missing'
    sc=spark.sparkContext
    a=[(1, {'items':[None, {'x':Decimal('1.00')}]}), (1, {'items':[]}), (1, {'items':[]})]
    b=list(reversed(a))
    actual=sc.parallelize(a,2)
    check=validation.distributed_validate
    good, digest=check(actual,sc.parallelize(b,3))
    assert good
    assert check(sc.parallelize(b,1),actual)[1]==digest, 'identity depends on input partitioning/order'
    assert not check(actual,sc.parallelize(b[:-1]))[0], 'duplicate loss accepted'
    assert not check(actual,sc.parallelize([(1, {'items':['None']})]))[0], 'nested mismatch accepted'
    assert not check(actual,sc.parallelize(b),ordered=True)[0], 'ordered reversal accepted'
    assert check(actual,sc.parallelize(a,3),ordered=True)[0]
    assert check(sc.emptyRDD(),sc.emptyRDD())[0]
    assert not check(actual,None)[0]
    assert not check(sc.parallelize([('x'*2000000,)],1),sc.parallelize([('different',)],1))[0]
    # Large, nonconstant payload exceeds maxResultSize if complete rows reach the driver.
    import hashlib
    rows=sc.range(0,12000,numSlices=4).map(lambda i:(i,''.join(hashlib.sha256(f'{i}:{j}'.encode()).hexdigest() for j in range(8))))
    large=spark.createDataFrame(rows,'id long, payload string')
    try:
        large.collect()
    except Exception as error:
        assert 'maxResultSize' in str(error),str(error)
    else:
        raise AssertionError('fixture did not exceed the driver result limit')
    materialized,count=validation.materialize(large)
    try:
        assert count==12000
        assert check(materialized,rows)[0]
        changed=rows.map(lambda r:(r[0],r[1]+'wrong') if r[0]==11999 else r)
        assert not check(materialized,changed)[0], 'late-row corruption accepted'
    finally:
        materialized.unpersist()
    from mutations import prepare
    from unittest.mock import patch
    source="SELECT id,named_struct('amount',id*10,'items',array(id)) payload FROM range(8)"
    with tempfile.TemporaryDirectory() as tmp:
        # No complete DataFrame may be collected by preparation or verification.
        with patch.object(type(large),'collect',side_effect=AssertionError('driver collect forbidden')):
            for operation in ('materialize','append','compact'):
                action,read,expected,before=prepare(spark,source,source,'parquet',Path(tmp)/operation,
                                                   operation,distributed=True)
                action()
                rows,count=validation.materialize(read())
                try:
                    assert count==(9 if operation=='append' else 8)
                    assert check(rows,expected)[0],operation
                finally:rows.unpersist()
    print('SPARK_VALIDATION_OK: exact nested bags, order, partition-independent identity, 1 MiB driver result limit')
finally:
    spark.stop()
