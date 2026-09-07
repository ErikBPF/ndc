"""Contracts for explicit phases, frozen schedules and candidate qualification."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from test_harness import ROOT, module


class InterfaceTests(unittest.TestCase):
    def test_suites_distinguish_historical_reads_and_all(self):
        api=module('schedule')
        self.assertEqual(len(api.load_suite(ROOT/'ndc','tpch')),24)
        self.assertEqual(len(api.load_suite(ROOT/'ndc','all')),46)
        reads=api.load_suite(ROOT/'ndc','read')
        self.assertEqual(len(reads),39)
        self.assertFalse(any('action' in spec for spec in reads.values()))
        with self.assertRaises(ValueError):api.load_suite(ROOT/'ndc','../all')

    def test_schedule_is_complete_deterministic_and_detects_tampering(self):
        api=module('schedule')
        manifest=api.load_suite(ROOT/'ndc','ds')
        plan=api.build_plan(manifest,runs=2,streams=2,warmups=1,seed=23,phase='shared-throughput')
        self.assertEqual(plan,api.build_plan(manifest,runs=2,streams=2,warmups=1,seed=23,phase='shared-throughput'))
        self.assertEqual(len(plan['samples']),8)
        self.assertEqual(len(plan['warmups']),2)
        self.assertEqual({(s['q'],s['run'],s['stream']) for s in plan['samples']},
                         {(q,r,s) for q in manifest for r in range(2) for s in range(2)})
        api.validate_plan(plan)
        plan['samples'].pop()
        with self.assertRaises(ValueError):api.validate_plan(plan)

    def test_phase_restrictions_prevent_mislabelled_work(self):
        api=module('schedule')
        writes=api.load_suite(ROOT/'ndc','write')
        with self.assertRaises(ValueError):api.build_plan(writes,phase='latency')
        with self.assertRaises(ValueError):api.build_plan(writes,phase='shared-throughput',streams=2)
        with self.assertRaises(ValueError):api.build_plan(api.load_suite(ROOT/'ndc','ds'),phase='latency',streams=2)
        with self.assertRaises(ValueError):api.build_plan(writes,runs=0)

    def test_frozen_plan_rejects_query_path_traversal(self):
        api=module('schedule')
        spec=next(iter(api.load_suite(ROOT/'ndc','ds').values()))
        with self.assertRaises(ValueError):api.build_plan({'../../escape':spec})

    def test_cli_plan_separates_data_seed_and_query_seed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            env=dict(os.environ,NDC_IN_ENV='1',NDC_WORKSPACE=tmp,SUITE='tpch',RUNS='2',
                     STREAMS='1',WARMUPS='0',QUERY_SEED='11',DATA_SEED='1',NDC_PHASE='matrix')
            env.pop('QUERIES',None)
            plans=[]
            for index,data_seed in enumerate(('1','999')):
                path=root/f'plan{index}.json'
                p=subprocess.run([str(ROOT/'ndc/run.sh'),'plan',str(path)],
                                 env=dict(env,DATA_SEED=data_seed),capture_output=True,text=True)
                self.assertEqual(p.returncode,0,p.stderr)
                plans.append(json.loads(path.read_text()))
            self.assertEqual(plans[0],plans[1])
            self.assertEqual(plans[0]['settings']['seed'],11)
            self.assertEqual(len(plans[0]['samples']),48)
            p=subprocess.run([str(ROOT/'ndc/run.sh'),'plan',str(root/'plan0.json')],
                             env=env,capture_output=True,text=True)
            self.assertNotEqual(p.returncode,0)
            self.assertEqual(json.loads((root/'plan0.json').read_text()),plans[0])

    def test_candidate_qualification_records_failure_and_stops(self):
        qualification=ROOT/'ndc/qualification.py'
        self.assertTrue(qualification.exists(),'candidate qualification command missing')
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); (root/'scale.txt').write_text('0.0083')
            runner=root/'run.sh'
            runner.write_text('#!/bin/sh\nprintf "%s\\n" "$1" >> "$NDC_WORKSPACE/calls"\n[ "$1" != invariants ]\n')
            runner.chmod(0o755)
            result=root/'qualification'
            p=subprocess.run([sys.executable,str(qualification),'vanilla','parquet','--runner',str(runner)],
                             env=dict(os.environ,NDC_WORKSPACE=tmp,NDC_CAMPAIGN_DIR=str(result)),capture_output=True,text=True)
            self.assertNotEqual(p.returncode,0)
            summary=json.loads((result/'qualification.json').read_text())
            self.assertEqual(summary['status'],'failed')
            self.assertEqual((root/'calls').read_text().splitlines(),['size-check','invariants'])
            self.assertEqual(summary['checks'][-1]['returncode'],1)
            again=subprocess.run([sys.executable,str(qualification),'vanilla','parquet','--runner',str(runner)],
                                 env=dict(os.environ,NDC_WORKSPACE=tmp,NDC_CAMPAIGN_DIR=str(result)),capture_output=True,text=True)
            self.assertNotEqual(again.returncode,0)
            self.assertEqual(json.loads((result/'qualification.json').read_text()),summary)

if __name__=='__main__':unittest.main()
