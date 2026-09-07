"""Report only validated, comparable schema-v2 cells from one campaign."""
import json
import math
from pathlib import Path
import statistics
import sys

from provenance import identity
from schedule import validate_plan


def load_cells(directory):
    cells = {}
    plan_path=Path(directory)/'plan.json'
    frozen=json.loads(plan_path.read_text()) if plan_path.exists() else None
    for path in sorted(Path(directory).glob('spark_*.json')):
        cell = json.loads(path.read_text())
        if cell.get('schema_version') != 2 or cell.get('parity_ok') is not True:
            raise ValueError(f'{path.name}: invalid or legacy cell')
        if not cell.get('results'):
            raise ValueError(f'{path.name}: empty results')
        manifest = cell.get('manifest')
        if not isinstance(manifest, dict) or not manifest or identity(manifest) != cell.get('manifest_id'):
            raise ValueError(f'{path.name}: missing or inconsistent manifest')
        if 'phase' in cell['comparison'] or 'plan' in cell:
            plan=validate_plan(cell['plan'])
            if (identity(plan)!=cell.get('plan_id') or plan['manifest']!=manifest
                    or plan['phase']!=cell['comparison'].get('phase')
                    or plan['stream_model']!=cell['comparison'].get('stream_model')
                    or any(cell['comparison'].get(k)!=v for k,v in plan['settings'].items())
                    or (frozen is not None and plan!=frozen)):
                raise ValueError(f'{path.name}: incompatible frozen plan')
            observed=[{k:r[k] for k in ('q','run','stream')} for r in cell['results']]
            if observed!=plan['samples']:
                raise ValueError(f'{path.name}: execution order differs from frozen schedule')
        samples = set()
        for r in cell['results']:
            key = (r['q'], r['run'], r.get('stream', 0))
            if key in samples:
                raise ValueError(f'{path.name}: duplicate sample')
            samples.add(key)
            if r['status'] == 'unsupported':
                continue
            if (r['status'] != 'ok' or r.get('valid') is not True
                    or not math.isfinite(r['ms']) or r['ms'] <= 0
                    or r['cache'] == 'drop-failed'):
                raise ValueError(f'{path.name}: invalid sample {key}')
        runs = cell['comparison']['runs']
        streams = cell['comparison']['streams']
        expected = {(q, i, s) for q in manifest
                    for i in range(runs) for s in range(streams)}
        if samples != expected:
            raise ValueError(f'{path.name}: incomplete samples')
        key = (cell['engine'], cell['fmt'])
        if key in cells:
            raise ValueError(f'duplicate cell {key}')
        cells[key] = cell
    if not cells:
        raise ValueError('no result cells')
    first = next(iter(cells.values()))
    for cell in cells.values():
        for key in ('campaign_id', 'dataset_id', 'manifest_id', 'comparison', 'spark_version'):
            if cell.get(key) != first.get(key):
                raise ValueError(f'incompatible {key}')
        schedule = lambda c: {(r['q'], r['run'], r.get('stream', 0), r['cache'])
                              for r in c['results']}
        if cell.get('environment',{}).get('source_id')!=first.get('environment',{}).get('source_id'):
            raise ValueError('incompatible source snapshot')
        if schedule(cell) != schedule(first):
            raise ValueError('incompatible query/sample/cache coverage')
    for fmt in {f for _,f in cells}:
        pair=[c for (e,f),c in cells.items() if f==fmt]
        if len({c.get('format_id') for c in pair})>1: raise ValueError('different physical format inputs')
    answers={}
    for cell in cells.values():
        for r in cell['results']:
            if r['status'] != 'ok': continue
            if not r.get('answer_id'): raise ValueError('missing answer identity')
            if r['q'] in answers and answers[r['q']] != r['answer_id']:
                raise ValueError(f'inconsistent answers: {r["q"]}')
            answers[r['q']]=r['answer_id']
    campaign = Path(directory) / 'campaign.json'
    if campaign.exists():
        expected = json.loads(campaign.read_text())
        if expected.get('status') != 'ok' or set(expected['cells']) != {
                f'{e}/{f}' for e, f in cells}:
            raise ValueError('failed or incomplete campaign')
    return cells


def render(cells):
    comparison=next(iter(cells.values()))['comparison']
    lines = ['# NDC results', '',
             f'Phase: {comparison.get("phase", "legacy")}; stream model: {comparison.get("stream_model", "unspecified")}; validation: {comparison.get("validation", "collect")}.', '',
             'TPC-H-derived; TPC-DS-inspired. Not comparable to published TPC results.', '',
             'Ratios are Spark / Comet medians. No overall score or significance verdict.', '',
             '| Format | Family | Query | Spark ms | Comet ms | Ratio | Samples | Range ms (Spark / Comet) |',
             '|---|---|---|---:|---:|---:|---:|---|']
    for fmt in sorted({f for _, f in cells}):
        vanilla, comet = cells.get(('vanilla', fmt)), cells.get(('comet', fmt))
        if not vanilla or not comet:
            lines.append(f'\n{fmt}: missing engine counterpart; no speedup comparison.')
            continue
        for q in sorted({r['q'] for r in vanilla['results']}):
            groups = [[r for r in c['results'] if r['q'] == q] for c in (vanilla, comet)]
            if any(r['status'] == 'unsupported' for g in groups for r in g):
                lines.append(f'\n{fmt}/{q}: unsupported; no comparison.')
                continue
            a, b = [[r['ms'] for r in g] for g in groups]
            ma, mb = statistics.median(a), statistics.median(b)
            lines.append(f'| {fmt} | {groups[0][0]["family"]} | {q} | {ma:.3f} | {mb:.3f} | '
                         f'{ma/mb:.3f}x | {len(a)} | {min(a):.3f}–{max(a):.3f} / {min(b):.3f}–{max(b):.3f} |')
        for family in sorted({r['family'] for r in vanilla['results']}):
            sums = [sum(statistics.median([r['ms'] for r in c['results']
                                          if r['q'] == q and r['status'] == 'ok'])
                        for q in {r['q'] for r in c['results']
                                  if r['family'] == family and r['status'] == 'ok'})
                    for c in (vanilla, comet)]
            lines.append(f'\n{fmt}/{family}: sum of query medians {sums[0]:.3f} / {sums[1]:.3f} ms (descriptive).')
        if vanilla['comparison']['streams'] > 1:
            for c in (vanilla, comet):
                seconds = c['timed_elapsed_s']
                count = sum(r['status'] == 'ok' for r in c['results'])
                lines.append(f'\n{c["engine"]}/{fmt}: {count/seconds:.3f} completed queries/s over {seconds:.3f}s.')
    return '\n'.join(lines) + '\n'


def main():
    try:
        report = render(load_cells(sys.argv[1]))
    except (ValueError, KeyError, TypeError) as error:
        sys.exit(f'REPORT_INVALID: {error}')
    Path(sys.argv[2]).write_text(report)
    print(f'REPORT -> {sys.argv[2]}')


if __name__ == '__main__':
    main()
