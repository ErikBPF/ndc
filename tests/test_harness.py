"""Regression checks for benchmark validity; no Spark required."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'ndc'))


def module(name):
    path = ROOT / 'ndc' / f'{name}.py'
    assert path.exists(), f'{name} implementation missing'
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class ValidityTests(unittest.TestCase):
    def test_complete_answers_and_explicit_reference(self):
        validate = module('validation').validate
        cases = [
            ([(1,)], [(999,)], False),
            ([(123.45, 1)], [(123.45, 999)], False),
            ([('100.001',)], [('100.004',)], False),
            ([(None,)], [('None',)], False),
            ([(1,), (1,)], [(1,)], False),
            ([(1,), (2,)], [(2,), (1,)], True),
        ]
        for actual, expected, ok in cases:
            with self.subTest(actual=actual, expected=expected):
                self.assertEqual(validate(actual, expected, ordered=False), ok)
        self.assertFalse(validate([(1,)], None))
        self.assertFalse(validate([(1,), (2,)], [(2,), (1,)], ordered=True))

    def test_size_requires_all_tables(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            (d/'gen.sql').write_text('CALL dbgen(sf = 1);')
            (d/'live.csv').write_text('unexpected\n1\n')
            p = subprocess.run([sys.executable, str(ROOT/'ndc/check_size.py'),
                                str(d/'gen.sql'), str(d/'live.csv'), str(ROOT/'ndc/sizes.csv')],
                               capture_output=True, text=True)
            self.assertNotEqual(p.returncode, 0, 'missing required columns accepted')

    def test_size_rejects_incomplete_reference(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            (d/'gen.sql').write_text('CALL dbgen(sf = 1);')
            (d/'live.csv').write_text('lineitem,orders,part,customer,supplier,partsupp,nation,region\n1,1,1,1,1,1,1,1\n')
            (d/'sizes.csv').write_text('sf,lineitem\n1,1\n')
            p = subprocess.run([sys.executable, str(ROOT/'ndc/check_size.py'),
                                str(d/'gen.sql'), str(d/'live.csv'), str(d/'sizes.csv')],
                               capture_output=True, text=True)
            self.assertNotEqual(p.returncode, 0)

    def test_matrix_refuses_existing_campaign(self):
        script=(ROOT/'ndc/run.sh').read_text()
        start=script.index('freeze_plan() {');end=script.index('\ncase ',start)
        with tempfile.TemporaryDirectory() as tmp:
            campaign=Path(tmp)/'campaign';campaign.mkdir()
            marker=campaign/'campaign.json';marker.write_text('original')
            p=subprocess.run(['bash','-c','set -euo pipefail\nspark_run() { return 0; }\n'
                              +script[start:end]+'\nmatrix 1 no ignored\n'],
                             env=dict(os.environ,NDC_WORKSPACE=tmp,NDC_CAMPAIGN_DIR=str(campaign),CODE=str(ROOT/'ndc')),
                             capture_output=True,text=True)
            self.assertNotEqual(p.returncode,0)
            self.assertEqual(marker.read_text(),'original')

    def test_matrix_failure_propagates(self):
        script = (ROOT/'ndc/run.sh').read_text()
        start = script.index('freeze_plan() {')
        end = script.index('\ncase ', start)
        with tempfile.TemporaryDirectory() as tmp:
            p = subprocess.run(['bash', '-c', 'set -euo pipefail\n'
                            'spark_run() { return 17; }\ndropcaches() { :; }\n'
                            + script[start:end] + '\nmatrix 1 no "$CODE/queries/manifest-full.json"\n'],
                               capture_output=True, text=True, cwd=tmp,
                               env=dict(os.environ,NDC_WORKSPACE=tmp,NDC_CAMPAIGN_DIR=tmp+'/campaign',CODE=str(ROOT/'ndc')))
        self.assertNotEqual(p.returncode, 0, 'all failed cells accepted')

    def test_report_failure_marks_campaign_failed(self):
        script=(ROOT/'ndc/run.sh').read_text()
        start=script.index('freeze_plan() {');end=script.index('\ncase ',start)
        with tempfile.TemporaryDirectory() as tmp:
            p=subprocess.run(['bash','-c','set -euo pipefail\nspark_run() { return 0; }\n'
                              +script[start:end]+'\nmatrix 1 no "$CODE/queries/manifest-full.json"\n'],
                             env=dict(os.environ,NDC_WORKSPACE=tmp,NDC_CAMPAIGN_DIR=tmp+'/campaign',CODE=str(ROOT/'ndc')),
                             capture_output=True,text=True)
            self.assertNotEqual(p.returncode,0)
            self.assertEqual(json.loads((Path(tmp)/'campaign/campaign.json').read_text())['status'],'failed')


class ReportTests(unittest.TestCase):
    def report(self, valid=True, formats=('parquet',), mismatch=False, mutate=None):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            for eng in ('vanilla', 'comet'):
                for fmt in formats:
                    result = {'q': 'q6_flat', 'run': 0, 'ms': 100, 'family': 'scan',
                              'status': 'ok', 'valid': valid, 'cache': 'uncontrolled', 'stream': 0, 'answer_id':'expected'}
                    cell = {'schema_version': 2, 'campaign_id': 'test', 'dataset_id': 'data',
                            'manifest': {'q6_flat': {}}, 'manifest_id': module('provenance').identity({'q6_flat': {}}), 'comparison': {'runs': 1, 'streams': 1},
                            'results': [result], 'parity_ok': valid,
                            'fmt': fmt, 'engine': eng, 'spark_version': 'test'}
                    if mismatch and eng == 'comet':
                        cell['dataset_id'] = 'different'
                    if mutate: mutate(cell,eng)
                    (d/f'spark_{eng}_{fmt}.json').write_text(json.dumps(cell))
            p = subprocess.run([sys.executable, str(ROOT/'ndc/report.py'), tmp, str(d/'report.md')],
                               capture_output=True, text=True)
            return p, (d/'report.md').read_text() if (d/'report.md').exists() else ''

    def test_invalid_cells_cannot_receive_verdict(self):
        p, report = self.report(False, ('parquet', 'iceberg', 'delta'))
        self.assertNotEqual(p.returncode, 0)
        self.assertNotIn('near tie', report)

    def test_partial_matrix_is_reportable(self):
        p, report = self.report()
        self.assertEqual(p.returncode, 0, p.stderr)
        self.assertIn('parquet', report)

    def test_reads_cannot_be_disguised_as_unsupported(self):
        p,_=self.report(mutate=lambda c,e:c['results'][0].update(status='unsupported',valid=False))
        self.assertNotEqual(p.returncode,0,'unsupported status bypassed read validation')

    def test_different_answers_cannot_be_compared(self):
        p,_=self.report(mutate=lambda c,e: c['results'][0].update(answer_id=e))
        self.assertNotEqual(p.returncode,0)

    def test_missing_repetition_is_rejected(self):
        p,_=self.report(mutate=lambda c,e: c['comparison'].update(runs=2))
        self.assertNotEqual(p.returncode,0)

    def test_missing_entire_workload_is_rejected(self):
        def mutate(cell, engine):
            cell['manifest']['omitted'] = {}
            cell['manifest_id'] = module('provenance').identity(cell['manifest'])
        p, _ = self.report(mutate=mutate)
        self.assertNotEqual(p.returncode, 0)

    def test_manifest_identity_is_verified(self):
        p, _ = self.report(mutate=lambda c, e: c.update(manifest_id='wrong'))
        self.assertNotEqual(p.returncode, 0)

    def test_new_phase_requires_frozen_plan(self):
        p,_=self.report(mutate=lambda c,e:c['comparison'].update(phase='matrix'))
        self.assertNotEqual(p.returncode,0)

    def test_execution_order_must_match_plan(self):
        def mutate(c,e):
            c['manifest']={'q6_flat':module('schedule').load_suite(ROOT/'ndc','scan')['q6_flat']}
            c['manifest_id']=module('provenance').identity(c['manifest'])
            c['comparison'].update(runs=2,warmups=0,seed=7,phase='matrix',stream_model='serial')
            c['plan']=module('schedule').build_plan(c['manifest'],runs=2,streams=1,warmups=0,seed=7)
            c['plan_id']=module('provenance').identity(c['plan'])
            c['results'].insert(0,dict(c['results'][0],run=1))
        p,_=self.report(mutate=mutate)
        self.assertNotEqual(p.returncode,0)

    def test_duplicate_sample_is_rejected(self):
        p,_=self.report(mutate=lambda c,e: c['results'].append(dict(c['results'][0])))
        self.assertNotEqual(p.returncode,0)

    def test_different_datasets_cannot_be_compared(self):
        p, _ = self.report(mismatch=True)
        self.assertNotEqual(p.returncode, 0)


if __name__ == '__main__':
    unittest.main()
