"""Prepare versioned synthetic depth fixtures once for all consuming engines."""
import argparse
from collections import Counter
import hashlib
import json
import math
from pathlib import Path
import subprocess

from generate import digest, identity, literal

DEPTHS = tuple(json.loads((Path(__file__).with_name('shape-workloads.json')).read_text())['depths'])
LEAF_TYPE = 'STRUCT(leaf_id BIGINT,amount BIGINT,tag VARCHAR,padding VARCHAR)'


def number(seed, key, field):
    payload = json.dumps([2, seed, key, field], separators=(',', ':')).encode()
    return int.from_bytes(hashlib.sha256(payload).digest()[:8], 'big')


def nesting(rows, depth, layout, branching):
    if depth == 1:
        return rows
    step = len(rows) if layout == 'singleton' else max(1, math.ceil(len(rows) / branching))
    return [{'children': nesting(rows[i:i+step], depth-1, layout, branching)}
            for i in range(0, len(rows), step)]


def generate(out, *, parents=128, leaves=None, fanout=16, data_seed=7,
             shape_seed=7, layout='branching', branching=2, distribution='uniform',
             null_rate=0.1, width=64, entropy='low'):
    if (not 0 < parents <= 2**31 or not 0 < fanout <= 2**32 or branching < 2 or width < 0
            or leaves is not None and not 0 <= leaves <= 2**63
            or not math.isfinite(null_rate) or not 0 <= null_rate <= 1):
        raise ValueError('invalid shape configuration')
    config = dict(parents=parents if leaves is None else None, leaves=leaves,
                  fanout=fanout, data_seed=data_seed, shape_seed=shape_seed,
                  layout=layout, branching=branching, distribution=distribution,
                  null_rate=null_rate, width=width, entropy=entropy)
    out.mkdir(parents=True, exist_ok=False)
    marker = dict(status='failed', generator_version=2, config=config,
                  generator_source_id=identity({name:digest(Path(__file__).with_name(name)) for name in ('shapes.py','generate.py','shape-workloads.json')}),
                  population_mode='fixed-parents' if leaves is None else 'fixed-leaves')
    record = out/'shape-contract.json'
    record.write_text(json.dumps(marker, indent=2)+'\n')
    names = ['shape_leaves_v2', 'shape_parents_v2', *[f'shape_depth{d}_v2' for d in DEPTHS]]
    streams = {n: (out/(n+'.jsonl')).open('w') for n in names}
    answers = {op: (out/('answer-'+op+'.jsonl')).open('w')
               for op in ('filter', 'join', 'topn', 'transform')}
    leaf_hash = hashlib.sha256()
    histogram = {d: Counter() for d in DEPTHS}
    counts = Counter(); total = 0; selected_count = 0; selected_sum = 0
    buckets = Counter(); bucket_counts = Counter()

    def emit(stream, value):
        stream.write(json.dumps(value, separators=(',', ':'))+'\n')

    def lengths(items, depth):
        histogram[depth][len(items) if items is not None else 'null'] += 1
        if items:
            for item in items:
                if 'children' in item:
                    lengths(item['children'], depth)

    try:
        parent = 0
        while (leaves is None and parent < parents) or (leaves is not None and total < leaves):
            draw = number(shape_seed, parent, 'count')
            count = draw % (fanout+1) if distribution == 'uniform' else (fanout if draw % 10 == 0 else draw % (min(fanout, 2)+1))
            if leaves is not None:
                count = min(max(1, count), leaves-total)
            rows = []
            for position in range(count):
                leaf_id = (parent << 32) + position if leaves is None else total + position
                amount = None if number(data_seed, leaf_id, 'null') / 2**64 < null_rate else number(data_seed, leaf_id, 'amount') % 100
                # Hash blocks retain prefixes when padding width changes.
                padding = 'x'*width if entropy == 'low' else ''.join(
                    hashlib.sha256(json.dumps([2, data_seed, leaf_id, 'padding', block]).encode()).hexdigest()
                    for block in range(math.ceil(width/64)))[:width]
                leaf = dict(leaf_id=leaf_id, amount=amount,
                            tag='tag'+str(number(data_seed, leaf_id, 'tag') % 4), padding=padding)
                leaf_hash.update((json.dumps(leaf, sort_keys=True, separators=(',', ':'))+'\n').encode())
                emit(streams['shape_leaves_v2'], dict(parent_id=parent, **leaf))
                rows.append(leaf)
            bucket = parent % 4
            emit(streams['shape_parents_v2'], dict(parent_id=parent, bucket=bucket))
            valid = [r for r in rows if r['amount'] is not None]
            selected = [r for r in valid if r['amount'] >= 50]
            selected_count += len(selected); selected_sum += sum(r['amount'] for r in selected)
            if valid:
                buckets[bucket] += sum(r['amount'] for r in valid)
                bucket_counts[bucket] += len(valid)
                top = sorted(valid, key=lambda r: (-r['amount'], r['leaf_id']))[:2]
                emit(answers['topn'], dict(parent_id=parent, total=sum(r['amount'] for r in top)))
            if selected:
                emit(answers['transform'], dict(parent_id=parent, selected=[dict(leaf_id=r['leaf_id'], adjusted=r['amount']*2, tag=r['tag'], padding=r['padding']) for r in selected]))
            ordered = sorted(rows, key=lambda r: number(shape_seed, r['leaf_id'], 'order'))
            for depth in DEPTHS:
                children = nesting(ordered, depth, layout, branching) if rows else (None if draw % 2 else [])
                emit(streams[f'shape_depth{depth}_v2'], dict(parent_id=parent, children=children))
                lengths(children, depth)
            total += count; parent += 1
            counts[count] += 1
        emit(answers['filter'], dict(item_count=selected_count, total=selected_sum if selected_count else None))
        for bucket in sorted(bucket_counts):
            emit(answers['join'], dict(bucket=bucket, item_count=bucket_counts[bucket], total=buckets[bucket]))
    finally:
        for stream in [*streams.values(), *answers.values()]:
            stream.close()
    statements = ["SET threads=4; SET memory_limit='4GB';"]
    types = {'shape_leaves_v2': {'parent_id':'BIGINT','leaf_id':'BIGINT','amount':'BIGINT','tag':'VARCHAR','padding':'VARCHAR'},
             'shape_parents_v2': {'parent_id':'BIGINT','bucket':'BIGINT'}}
    for depth in DEPTHS:
        kind = LEAF_TYPE
        for _ in range(depth-1):
            kind = f'STRUCT(children {kind}[])'
        types[f'shape_depth{depth}_v2'] = {'parent_id':'BIGINT','children':kind+'[]'}
    for name, columns in types.items():
        source = out/(name+'.jsonl')
        # Empty populations still get an explicit physical schema.
        mapping = '{'+','.join(literal(k)+':'+literal(v) for k,v in columns.items())+'}'
        select = f"SELECT * FROM read_json({literal(source)},format='newline_delimited',columns={mapping})" if source.stat().st_size else 'SELECT '+','.join(f'CAST(NULL AS {v}) AS {k}' for k,v in columns.items())+' WHERE false'
        statements.append(f"COPY ({select}) TO {literal(out/(name+'.parquet'))} (FORMAT PARQUET, COMPRESSION ZSTD);")
    subprocess.run(['duckdb','-bail'], input='\n'.join(statements), text=True, capture_output=True, check=True)
    tables = {n: {'rows':total if n=='shape_leaves_v2' else parent,
                  'bytes':(out/(n+'.parquet')).stat().st_size,
                  'sha256':digest(out/(n+'.parquet'))} for n in names}
    for n in names:
        (out/(n+'.jsonl')).unlink()
    marker.update(status='ok', leaf_population_id=leaf_hash.hexdigest(), parents=parent,
                  leaf_count=total, parent_leaf_counts={str(k):v for k,v in sorted(counts.items())},
                  depths={str(d):dict(array_depth=d, struct_depth=d,
                                     array_lengths={str(k):v for k,v in histogram[d].items()}) for d in DEPTHS},
                  tables=tables, answers={op:dict(file='answer-'+op+'.jsonl',sha256=digest(out/('answer-'+op+'.jsonl'))) for op in answers},
                  writer=subprocess.check_output(['duckdb','--version'],text=True).strip())
    marker['dataset_id'] = identity(marker)
    record.write_text(json.dumps(marker,indent=2)+'\n')
    return marker


def verify(directory):
    record=json.loads((directory/'shape-contract.json').read_text())
    tables={'shape_leaves_v2','shape_parents_v2',*[f'shape_depth{d}_v2' for d in DEPTHS]}
    ops={'filter','join','topn','transform'}
    if (record['status']!='ok' or record['generator_version']!=2
            or set(record['tables'])!=tables or set(record['answers'])!=ops):
        raise ValueError('invalid shape contract')
    if record['dataset_id']!=identity({k:v for k,v in record.items() if k!='dataset_id'}):
        raise ValueError('invalid shape identity')
    for name in tables:
        if digest(directory/(name+'.parquet'))!=record['tables'][name]['sha256']:
            raise ValueError('invalid shape artifact: '+name)
    for op in ops:
        if (record['answers'][op]['file']!='answer-'+op+'.jsonl'
                or digest(directory/('answer-'+op+'.jsonl'))!=record['answers'][op]['sha256']):
            raise ValueError('invalid shape artifact: '+op)
    return record


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--out',type=Path)
    p.add_argument('--verify',type=Path)
    population = p.add_mutually_exclusive_group()
    population.add_argument('--parents',type=int,default=128)
    population.add_argument('--leaves',type=int)
    for name, default in [('fanout',16),('data-seed',7),('shape-seed',7),('branching',2),('width',64)]:
        p.add_argument('--'+name,type=int,default=default)
    p.add_argument('--layout',choices=['singleton','branching'],default='branching')
    p.add_argument('--distribution',choices=['uniform','skewed'],default='uniform')
    p.add_argument('--null-rate',type=float,default=0.1)
    p.add_argument('--entropy',choices=['low','high'],default='low')
    args=vars(p.parse_args());out=args.pop('out');directory=args.pop('verify')
    if not out and not directory:p.error('--out or --verify is required')
    try:
        result=verify(directory.resolve()) if directory else generate(out.resolve(),**args)
        print('SHAPES_OK',result['dataset_id'])
    except (OSError,ValueError,KeyError,TypeError,subprocess.CalledProcessError) as error:
        p.exit(1,f'SHAPES_INVALID: {getattr(error,"stderr",None) or error}\n')


if __name__ == '__main__':
    main()
