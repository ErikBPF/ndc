"""Candidate comparisons and balanced runs must preserve benchmark validity."""
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from test_harness import ROOT, module


def cell():
    plan=module('schedule').build_plan(module('schedule').load_suite(ROOT/'ndc','ds'),runs=1,warmups=1,phase='latency')
    return {'schema_version':2,'campaign_id':'example','dataset_id':'data','format_id':'physical',
            'manifest':plan['manifest'],'manifest_id':plan['manifest_id'],'plan':plan,
            'plan_id':module('provenance').identity(plan),'fmt':'iceberg','engine':'example',
            'parity_ok':True,'run_intent':'performance',
            'comparison':dict(plan['settings'],phase='latency',stream_model='serial',sf='0.5',
                              cache='uncontrolled',validation='collect',cgroup_limits=[{'cpu.max':'400000 100000','memory.max':'17179869184'}]),
            'environment':{'source_id':'source','host':'private-host','artifacts':{'engine.jar':'a'*64},
                           'candidate':{'revision':'revision','build_profile':'ci'}},
            'results':[dict(s,family='ds',status='ok',valid=True,cache='uncontrolled',ms=100,
                            rows=1,answer_id=s['q'],plan=s['q']+'.json') for s in plan['samples']]}


def write_cell(root, value):
    root.mkdir(parents=True)
    path=root/'result.json';path.write_text(json.dumps(value))
    for row in value['results']:
        (root/row['plan']).write_text(json.dumps({'operator_metrics':[{'operator':'NativeScan','metrics':{}}]}))
    return path


class ComparisonTests(unittest.TestCase):
    def test_named_candidates_reuse_validation_and_show_resources_and_operators(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);a=cell();b=copy.deepcopy(a);b['campaign_id']='candidate'
            b['environment']['artifacts']['engine.jar']='b'*64
            for row in b['results']:row['ms']=50
            paths=[write_cell(root/'base',a),write_cell(root/'head',b)]
            p=subprocess.run([sys.executable,str(ROOT/'ndc/compare.py'),f'base={paths[0]}',f'head={paths[1]}','--operator','NativeScan'],capture_output=True,text=True)
            self.assertEqual(p.returncode,0,p.stderr)
            for expected in ('base','head','2.000x','NativeScan','Exploratory','4 CPUs','16 GiB','revision','ci'):
                self.assertIn(expected,p.stdout)
            self.assertNotIn('private-host',p.stdout)
            self.assertNotIn(tmp,p.stdout)

    def test_compare_rejects_incompatible_or_invalid_evidence(self):
        api=module('compare')
        mutations=[lambda x:x.update(dataset_id='other'),lambda x:x.update(format_id='other'),
                   lambda x:x['environment'].update(source_id='other'),
                   lambda x:x['results'][0].update(answer_id='wrong'),
                   lambda x:x['results'].pop(),lambda x:x['results'][0].update(ms=float('nan'))]
        for mutate in mutations:
            with self.subTest(mutate=mutate), tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp);a=cell();b=copy.deepcopy(a);b['campaign_id']='candidate';mutate(b)
                paths=[write_cell(root/'base',a),write_cell(root/'head',b)]
                with self.assertRaises(ValueError):api.compare({'base':[paths[0]],'head':[paths[1]]})

    def test_candidate_runtime_versions_may_differ_but_passes_must_be_stable(self):
        api=module('compare')
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);a=cell();a['spark_version']='3.5';b=copy.deepcopy(a);b['spark_version']='4.1';b['campaign_id']='head'
            paths=[write_cell(root/'a',a),write_cell(root/'b',b)]
            report=api.compare({'base':[paths[0]],'head':[paths[1]]})
            self.assertIn('3.5',report);self.assertIn('4.1',report)
            with self.assertRaises(ValueError):api.compare({'base':[paths[0]],'head':[paths[0]]})

    def test_invalid_answers_fail_individual_cell_validation(self):
        with tempfile.TemporaryDirectory() as tmp:
            value=cell();value['results'][0].pop('answer_id')
            path=write_cell(Path(tmp)/'cell',value)
            with self.assertRaises(ValueError):module('report').load_cell(path)

    def test_required_operator_checks_every_sample_and_rejects_traversal(self):
        api=module('compare')
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);a=cell();b=copy.deepcopy(a);b['campaign_id']='candidate'
            paths=[write_cell(root/'base',a),write_cell(root/'head',b)]
            groups={'base':[paths[0]],'head':[paths[1]]}
            with self.assertRaises(ValueError):api.compare(groups,required=[('head',a['results'][0]['q'],'Missing')])
            b['results'][0]['plan']='../outside.json';paths[1].write_text(json.dumps(b))
            with self.assertRaises(ValueError):api.compare(groups,operators=['NativeScan'])


class ExperimentTests(unittest.TestCase):
    def test_performance_order_is_balanced_and_correctness_is_explicit(self):
        api=module('experiment')
        order=api.order(['a','b','c'],6,7)
        self.assertEqual(order,api.order(['a','b','c'],6,7))
        for position in range(3):
            self.assertEqual({label:sum(row[position]==label for row in order) for label in 'abc'},dict(a=2,b=2,c=2))
        with self.assertRaises(ValueError):api.order(['a','b','c'],4,7)

    def test_failure_stops_remaining_candidates_and_keeps_diagnostics(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);marker=root/'ran';spec=root/'candidates.json';out=root/'experiment'
            spec.write_text(json.dumps({'candidates':[
                {'label':'a','command':[sys.executable,'-c','raise SystemExit(17)']},
                {'label':'b','command':[sys.executable,'-c',f'open({str(marker)!r}, "w").close()']}]}))
            p=subprocess.run([sys.executable,str(ROOT/'ndc/experiment.py'),str(spec),'--out',str(out),'--mode','correctness'],capture_output=True,text=True)
            self.assertNotEqual(p.returncode,0)
            self.assertFalse(marker.exists())
            self.assertEqual(json.loads((out/'experiment.json').read_text())['status'],'failed')

    def test_performance_runs_fresh_balanced_candidates_and_compares(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);fixture=root/'fixture.json';fixture.write_text(json.dumps(cell()))
            runner=root/'runner.py'
            runner.write_text("import json,os,pathlib\np=pathlib.Path(os.environ['NDC_CAMPAIGN_DIR']);p.mkdir()\nc=json.loads(pathlib.Path('fixture.json').read_text())\nc['campaign_id']=p.name\nc['run_intent']=os.environ['NDC_RUN_INTENT']\nc['environment']['candidate']={'label':os.environ['NDC_CANDIDATE'],'revision':os.environ['NDC_CANDIDATE_REVISION'],'build_profile':os.environ['NDC_BUILD_PROFILE']}\nassert os.environ['RUNS']=='1' and os.environ['WARMUPS']=='1'\n(p/'result.json').write_text(json.dumps(c))\nprint('NDC fixture complete',flush=True)\n")
            spec=root/'spec.json';spec.write_text(json.dumps({'candidates':[
                {'label':label,'command':[sys.executable,str(runner)],'revision':label,'build_profile':'release'} for label in ('base','head')]}))
            out=root/'out'
            command=[sys.executable,str(ROOT/'ndc/experiment.py'),str(spec),'--out',str(out),'--mode','performance']
            p=subprocess.run(command,capture_output=True,text=True)
            self.assertEqual(p.returncode,0,p.stderr)
            summary=json.loads((out/'experiment.json').read_text())
            self.assertEqual(summary['status'],'ok');self.assertEqual(len(summary['runs']),8)
            self.assertIn('NDC fixture complete',p.stdout)
            self.assertIn('Repetitions per candidate: 4',(out/'comparison.md').read_text())
            self.assertNotEqual(subprocess.run(command,capture_output=True).returncode,0)

    def test_resource_limits_use_native_scope_and_reject_invalid_cpu(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);tool=root/'systemd-run';tool.write_text('#!/bin/sh\nprintf "%s\\n" "$@"\n');tool.chmod(0o755)
            env=dict(os.environ,NDC_IN_ENV='1',NDC_CPU_LIMIT='4',NDC_MEMORY_LIMIT='16G',
                     NDC_WORKSPACE=str(root/'workspace'),PATH=str(root)+os.pathsep+os.environ['PATH'])
            env.pop('NDC_RESOURCE_SCOPE',None)
            p=subprocess.run([str(ROOT/'ndc/run.sh'),'plan',str(root/'plan.json')],env=env,capture_output=True,text=True)
            self.assertEqual(p.returncode,0,p.stderr)
            self.assertIn('CPUQuota=400%',p.stdout);self.assertIn('MemoryMax=16G',p.stdout)
            p=subprocess.run([str(ROOT/'ndc/run.sh'),'plan',str(root/'invalid.json')],env=dict(env,NDC_CPU_LIMIT='bad'),capture_output=True,text=True)
            self.assertNotEqual(p.returncode,0)
            self.assertFalse((root/'invalid.json').exists())

    def test_query_progress_is_visible_before_process_completion(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);done=root/'done';(root/'scale.txt').write_text('0.5')
            binary=root/'spark-submit';binary.write_text(f'#!/bin/sh\necho "NDC stub-query"\nsleep 1\ntouch "{done}"\nexit 17\n');binary.chmod(0o755)
            env=dict(os.environ,NDC_IN_ENV='1',NDC_WORKSPACE=tmp,SPARK_HOME=str(root/'unused'),
                     PATH=str(root)+os.pathsep+os.environ['PATH'])
            with subprocess.Popen([str(ROOT/'ndc/run.sh'),'spark','vanilla','parquet',str(ROOT/'ndc/queries/manifest-ds.json'),'1'],env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True) as process:
                observed=False
                for line in process.stdout:
                    if 'NDC stub-query' in line:
                        observed=not done.exists();break
                process.communicate()
                self.assertTrue(observed,'query progress arrived only after process exit')
                self.assertNotEqual(process.returncode,0,'progress forwarding hid backend failure')

    def test_candidate_metadata_is_explicit_and_does_not_dump_environment(self):
        from unittest.mock import patch
        with patch.dict(os.environ,{'NDC_CANDIDATE':'head','NDC_CANDIDATE_REVISION':'abc','NDC_BUILD_PROFILE':'release','PRIVATE_TOKEN':'do-not-record'}):
            metadata=module('provenance').candidate_metadata()
        self.assertEqual(metadata,{'label':'head','revision':'abc','build_profile':'release'})

    def test_invalid_inherited_intent_fails_before_creating_output(self):
        import contextlib
        import io
        from unittest.mock import MagicMock, patch
        with tempfile.TemporaryDirectory() as tmp:
            out=Path(tmp)/'result.json';errors=io.StringIO()
            with patch.dict(sys.modules,{'pyspark':MagicMock(),'pyspark.sql':MagicMock()}):
                runner=module('spark_poc')
                runner.SparkSession.builder.appName.side_effect=AssertionError('invalid intent reached Spark startup')
            with patch.dict(os.environ,{'NDC_RUN_INTENT':'typo'}), patch.object(sys,'argv',['spark_poc','--out',str(out)]), contextlib.redirect_stderr(errors):
                with self.assertRaises(SystemExit) as raised:runner.main()
            self.assertEqual(raised.exception.code,2)
            self.assertIn('intent',errors.getvalue())
            self.assertEqual(list(Path(tmp).iterdir()),[])

    def test_qualification_overrides_inherited_performance_intent(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);(root/'scale.txt').write_text('0.0083')
            runner=root/'runner.sh';runner.write_text('#!/bin/sh\n[ "$NDC_RUN_INTENT" = correctness ]\n');runner.chmod(0o755)
            p=subprocess.run([sys.executable,str(ROOT/'ndc/qualification.py'),'vanilla','parquet','--runner',str(runner)],
                             env=dict(os.environ,NDC_WORKSPACE=tmp,NDC_RUN_INTENT='performance'),capture_output=True,text=True)
            self.assertEqual(p.returncode,0,p.stdout+p.stderr)

    def test_compare_does_not_source_workspace_configuration(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);marker=root/'ran';(root/'bench.conf').write_text(f'touch {marker}\n')
            p=subprocess.run([str(ROOT/'ndc/run.sh'),'compare','--help'],capture_output=True,text=True,
                             env=dict(os.environ,NDC_IN_ENV='1',NDC_WORKSPACE=tmp))
            self.assertEqual(p.returncode,0,p.stderr)
            self.assertFalse(marker.exists())
