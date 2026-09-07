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


def prepare(spark, sql, reference, fmt, directory, operation):
    """Setup and oracle construction are outside the measured operation."""
    directory = Path(directory)
    if directory.exists():
        raise ValueError(f'write target already exists: {directory}')
    directory.mkdir(parents=True)
    location = str(directory/'table')
    table = 'ndc_write.' + directory.name.replace('-', '_')
    source = spark.sql(sql)
    # ponytail: complete output fits driver memory; use distributed canonical bag checks for larger outputs.
    expected = [tuple(r) for r in spark.sql(reference).collect()]
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
        base = [(r[0], r[1].asDict(recursive=True)) for r in expected]
        expected = expected_state(base, operation)
    before = inventory(directory)
    before['logical_changed_rows'] = (len(expected) if operation=='materialize' else 1 if operation=='append' else int(any(k==1 for k,_ in base)) if operation in ('update','delete') else 0)

    def execute():
        if operation == 'materialize':
            write(spark.sql(sql))
        elif operation == 'append':
            key, payload = expected[-1]
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
