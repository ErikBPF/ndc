"""Commit-complete writes and deterministic maintenance on per-sample targets."""
from pathlib import Path


def expected_state(base, operation):
    base = [(key, dict(payload)) for key, payload in base]
    if operation == 'append':
        key = max(k for k, _ in base) + 1
        return base + [(key, {'amount': key*10, 'items': []})]
    if operation == 'update':
        return [(k, dict(p, amount=p['amount']+1) if k == 1 else p) for k, p in base]
    if operation == 'delete':
        return [(k,p) for k,p in base if k != 1]
    return base


def inventory(path):
    files = [p for p in Path(path).rglob('*') if p.is_file() and not p.name.startswith('.')]
    return {'output_bytes': sum(p.stat().st_size for p in files), 'output_files': len(files), '_files': {str(p):p.stat().st_size for p in files}}


def prepare(spark, sql, reference, fmt, directory, operation, distributed=False):
    """Setup and oracle construction are outside the measured operation."""
    directory = Path(directory)
    if directory.exists():
        raise ValueError(f'write target already exists: {directory}')
    directory.mkdir(parents=True)
    location = str(directory/'table')
    table = 'ndc_write.' + directory.name.replace('-', '_')
    source = spark.sql(sql)
    reference_df = spark.sql(reference)
    expected = reference_df.rdd.map(tuple) if distributed else [tuple(r) for r in reference_df.collect()]
    active = [location]

    def write(df, append=False):
        if fmt == 'iceberg':
            writer = df.writeTo(table).using('iceberg')
            writer.append() if append else writer.create()
        else:
            df.write.format(fmt).mode('append' if append else 'error').save(location)

    def read():
        return spark.table(table) if fmt == 'iceberg' else spark.read.format(fmt).load(active[0])

    if operation != 'materialize':
        write(source)
        if distributed:
            base = expected.map(lambda r:(r[0],r[1].asDict(recursive=True)))
            if operation == 'append':
                key=base.keys().max()+1
                appended=(key,{'amount':key*10,'items':[]})
                expected=base.union(spark.sparkContext.parallelize([appended],1))
            else:
                expected=base.flatMap(lambda r:expected_state([r],operation))
        else:
            base = [(r[0], r[1].asDict(recursive=True)) for r in expected]
            expected = expected_state(base, operation)
            if operation == 'append':appended=expected[-1]
    before = inventory(directory)
    if operation=='materialize':
        changed=expected.count() if distributed else len(expected)
    elif operation=='append':changed=1
    elif operation in ('update','delete'):
        changed=base.filter(lambda r:r[0]==1).count() if distributed else sum(k==1 for k,_ in base)
    else:changed=0
    before['logical_changed_rows']=changed

    def execute():
        if operation == 'materialize':
            write(spark.sql(sql))
        elif operation == 'append':
            key, payload = appended
            write(spark.createDataFrame([(key, payload)], source.schema), append=True)
        elif operation in ('update', 'delete'):
            target = table if fmt == 'iceberg' else f'delta.`{location}`'
            statement = (f"UPDATE {target} SET payload = named_struct('amount',payload.amount+1,'items',payload.items) WHERE id=1"
                         if operation == 'update' else f'DELETE FROM {target} WHERE id=1')
            spark.sql(statement).collect()
        elif operation == 'compact':
            if fmt == 'iceberg':
                spark.sql(f"CALL ndc_write.system.rewrite_data_files(table => '{table.split('.',1)[1]}', options => map('min-input-files','1'))").collect()
            elif fmt == 'delta':
                spark.sql(f'OPTIMIZE delta.`{location}`').collect()
            else:
                active[0] = str(directory/'compacted')
                spark.read.parquet(location).coalesce(1).write.mode('error').parquet(active[0])
        else:
            raise ValueError(f'unknown mutation {operation}')

    return execute, read, expected, before
