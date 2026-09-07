"""Typed, complete answer comparison. Missing references never qualify a result."""
from collections import Counter
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path


def canonical(value):
    if value is None:
        return ('null',)
    if isinstance(value, bool):
        return ('bool', value)
    if isinstance(value, (int, float, Decimal)):
        number = Decimal(str(value))
        if not number.is_finite():
            raise ValueError('non-finite answer')
        text = format(number, 'f')
        if '.' in text: text = text.rstrip('0').rstrip('.')
        return ('number', Decimal(0) if number == 0 else Decimal(text))
    if isinstance(value, (date, datetime)):
        return ('date', value.isoformat())
    if hasattr(value, "asDict"):
        value = value.asDict()
    if isinstance(value, dict):
        return ('map', tuple(sorted((k, canonical(v)) for k, v in value.items())))
    if isinstance(value, (tuple, list)):
        return ('sequence', tuple(canonical(v) for v in value))
    return ('string', str(value))


def validate(actual, expected, ordered=False):
    if expected is None:
        return False
    a, b = [canonical(r) for r in actual], [canonical(r) for r in expected]
    return a == b if ordered else Counter(a) == Counter(b)


def read_answer(path, schema):
    """Pipe answers use explicit \\N nulls; types come from the query schema."""
    rows = []
    for line in Path(path).read_text().splitlines():
        cells = line.removesuffix('|').split('|')
        if len(cells) != len(schema.fields):
            raise ValueError(f'answer column count: {path}')
        row = []
        for value, field in zip(cells, schema.fields):
            kind = field.dataType.typeName()
            if value == '\\N':
                value = None
            elif kind in ('byte', 'short', 'integer', 'long', 'decimal', 'double', 'float'):
                value = Decimal(value)
            elif kind == 'date':
                value = date.fromisoformat(value)
            elif kind == 'timestamp':
                value = datetime.fromisoformat(value)
            elif kind == 'boolean':
                if value not in ('true', 'false'):
                    raise ValueError('invalid boolean answer')
                value = value == 'true'
            row.append(value)
        rows.append(tuple(row))
    return rows


def materialize(df):
    """Consume every field into executor disk storage; return only a row count."""
    from pyspark import StorageLevel
    rows=df.rdd.map(tuple).persist(StorageLevel.DISK_ONLY)
    try:
        return rows,rows.count()
    except Exception:
        rows.unpersist()
        raise


def row_key(row):
    import json
    return json.dumps(canonical(row),ensure_ascii=True,separators=(',',':'),default=str)


def key_partition(key):
    import hashlib
    return int.from_bytes(hashlib.sha256(key.encode()).digest()[:8],'big')


def partition_digest(rows):
    import hashlib
    h=hashlib.sha256()
    for key,count in rows:
        content=key.encode()
        h.update(len(content).to_bytes(8,'big'));h.update(content)
        h.update(str(count).encode()+b'\n')
    yield h.hexdigest()


def distributed_validate(actual, expected, ordered=False):
    """Exact distributed bag equality. Driver receives only bounded summaries."""
    if expected is None:return False,None
    from operator import add
    from pyspark import StorageLevel
    from provenance import identity
    def keyed(rows):
        if ordered:
            return rows.zipWithIndex().map(lambda pair:(row_key((pair[1],pair[0])),1))
        return rows.map(lambda row:(row_key(row),1))
    counts=keyed(actual).reduceByKey(add,8).persist(StorageLevel.DISK_ONLY)
    try:
        difference=counts.union(keyed(expected).mapValues(lambda n:-n)).reduceByKey(add,8)
        valid=difference.filter(lambda pair:pair[1]!=0).map(lambda _:1).isEmpty()
        # Fixed hash partitions + sorted complete keys keep identities independent of input layout.
        parts=counts.repartitionAndSortWithinPartitions(8,key_partition).mapPartitions(partition_digest).collect()
        return valid,identity(parts)
    finally:
        counts.unpersist()
