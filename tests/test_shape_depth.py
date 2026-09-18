"""Portable shape/depth contracts through generated artifacts and real SQL."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[1]

class ShapeDepthTests(unittest.TestCase):
    def generate(self, out, *args):
        return subprocess.run([sys.executable,str(ROOT/'datagen/shapes.py'),'--out',str(out),*map(str,args)],capture_output=True,text=True)

    def test_leaf_values_survive_shape_changes_and_population_growth(self):
        with tempfile.TemporaryDirectory() as tmp:
            populations=[];identities=[]
            for i,args in enumerate([['--leaves',37,'--fanout',4],['--leaves',37,'--fanout',16,'--shape-seed',91,'--distribution','skewed'],['--leaves',50,'--fanout',16]]):
                out=Path(tmp)/str(i);p=self.generate(out,*args)
                self.assertEqual(p.returncode,0,p.stderr)
                rows=json.loads(subprocess.check_output(['duckdb','-json','-c',f"SELECT * EXCLUDE(parent_id) FROM read_parquet('{out}/shape_leaves_v2.parquet') ORDER BY leaf_id"],text=True))
                self.assertEqual(len(rows),[37,37,50][i])
                identities.append(json.loads((out/'shape-contract.json').read_text())['leaf_population_id'])
                populations.append(rows)
            self.assertEqual(identities[0],identities[1])
            self.assertNotEqual(identities[0],identities[2])
            self.assertEqual(populations[0],populations[1])
            self.assertEqual(populations[0],populations[2][:37])

    def test_depth_queries_match_independent_answers_for_multiple_seeds(self):
        with tempfile.TemporaryDirectory() as tmp:
            for seed in (7,19):
                for layout in ('singleton','branching'):
                    out=Path(tmp)/f'{seed}-{layout}'
                    p=self.generate(out,'--parents',9,'--fanout',8,'--layout',layout,'--data-seed',seed,'--null-rate',0.3,'--entropy','high')
                    self.assertEqual(p.returncode,0,p.stderr)
                    m=json.loads((out/'shape-contract.json').read_text())
                    self.assertEqual(m['status'],'ok')
                    self.assertEqual(m['generator_version'],2)
                    self.assertEqual(m['parents'],9)
                    setup=''.join(f"CREATE VIEW {n} AS SELECT * FROM read_parquet('{out}/{n}.parquet');" for n in m['tables'])
                    for depth in (1,3,5):
                        for op,evidence in m['answers'].items():
                            expected=[json.loads(line) for line in (out/evidence['file']).read_text().splitlines()]
                            q=(ROOT/f'engines/duckdb/queries/sd_{op}_d{depth}.sql').read_text()
                            actual=json.loads(subprocess.check_output(['duckdb','-json','-c',setup+q],text=True))
                            self.assertEqual(actual,expected,(depth,op,seed,layout))
                    self.assertNotEqual(self.generate(out).returncode,0)

    def test_invalid_configuration_fails_before_creating_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            for i,args in enumerate([['--fanout',0],['--null-rate',1.1],['--null-rate','nan'],['--leaves',-1],['--parents',0]]):
                out=Path(tmp)/str(i);p=self.generate(out,*args)
                self.assertNotEqual(p.returncode,0)
                self.assertIn('invalid shape configuration',p.stderr)
                self.assertFalse(out.exists())

    def test_runner_accepts_shape_depth_suite(self):
        with tempfile.TemporaryDirectory() as tmp:
            p=subprocess.run([sys.executable,str(ROOT/'ndc/schedule.py'),'--suite','shape-depth','--out',str(Path(tmp)/'plan.json')],capture_output=True,text=True)
            self.assertEqual(p.returncode,0,p.stderr)
            plan=json.loads((Path(tmp)/'plan.json').read_text())
            self.assertEqual(len(plan['manifest']),16)

    def test_verification_rejects_corruption(self):
        with tempfile.TemporaryDirectory() as tmp:
            out=Path(tmp)/'data';p=self.generate(out,'--leaves',0)
            self.assertEqual(p.returncode,0,p.stderr)
            cmd=[sys.executable,str(ROOT/'datagen/shapes.py'),'--verify',str(out)]
            p=subprocess.run(cmd,capture_output=True,text=True)
            self.assertEqual(p.returncode,0,p.stderr)
            with (out/'shape_depth3_v2.parquet').open('ab') as stream:
                stream.write(b'corrupt')
            p=subprocess.run(cmd,capture_output=True,text=True)
            self.assertNotEqual(p.returncode,0)
            self.assertIn('invalid shape artifact',p.stderr)

    def test_fixed_parent_overlap_keeps_leaf_identity_and_values(self):
        with tempfile.TemporaryDirectory() as tmp:
            populations=[]
            for fanout in (4,16):
                out=Path(tmp)/str(fanout);p=self.generate(out,'--parents',16,'--fanout',fanout)
                self.assertEqual(p.returncode,0,p.stderr)
                rows=json.loads(subprocess.check_output(['duckdb','-json','-c',f"SELECT *,row_number() OVER (PARTITION BY parent_id ORDER BY leaf_id) AS pos FROM read_parquet('{out}/shape_leaves_v2.parquet')"],text=True))
                populations.append({(r['parent_id'],r['pos']):r for r in rows})
            common=populations[0].keys() & populations[1].keys()
            self.assertTrue(common)
            for key in common:self.assertEqual(populations[0][key],populations[1][key])

    def test_runner_refuses_modified_shape_contract(self):
        from test_harness import module
        with tempfile.TemporaryDirectory() as tmp:
            out=Path(tmp)/'data';p=self.generate(out,'--parents',4)
            self.assertEqual(p.returncode,0,p.stderr)
            module('provenance').dataset(out)
            with (out/'shape_leaves_v2.parquet').open('ab') as f:f.write(b'corrupt')
            with self.assertRaises(subprocess.CalledProcessError):
                module('provenance').dataset(out)

    def test_contract_identity_survives_json_roundtrip_for_large_fanout(self):
        with tempfile.TemporaryDirectory() as tmp:
            out=Path(tmp)/'data';p=self.generate(out,'--parents',32,'--fanout',64)
            self.assertEqual(p.returncode,0,p.stderr)
            p=subprocess.run([sys.executable,str(ROOT/'datagen/shapes.py'),'--verify',str(out)],capture_output=True,text=True)
            self.assertEqual(p.returncode,0,p.stderr)

    def test_published_queries_match_renderer(self):
        sys.path.insert(0,str(ROOT/'engines'))
        import shape_queries
        for engine in ('duckdb','spark'):
            directory=ROOT/'ndc/queries' if engine=='spark' else ROOT/'engines/duckdb/queries'
            for op in shape_queries.OPERATIONS:
                for depth in (None,1,3,5):
                    name=f'sd_{op}_'+('flat' if depth is None else f'd{depth}')
                    self.assertEqual((directory/(name+'.sql')).read_text(),shape_queries.query(engine,op,depth))

    def test_published_manifest_matches_renderer(self):
        sys.path.insert(0,str(ROOT/'engines'))
        import shape_queries
        published=json.loads((ROOT/'ndc/queries/manifest-shape-depth.json').read_text())
        self.assertEqual(published,shape_queries.manifest_entries())

    def test_data_and_shape_seeds_are_independent(self):
        with tempfile.TemporaryDirectory() as tmp:
            populations=[]
            for i,args in enumerate([[],['--data-seed',19],['--shape-seed',19]]):
                out=Path(tmp)/str(i);p=self.generate(out,'--leaves',32,'--fanout',8,*args)
                self.assertEqual(p.returncode,0,p.stderr)
                populations.append(json.loads(subprocess.check_output(['duckdb','-json','-c',f"SELECT * FROM read_parquet('{out}/shape_leaves_v2.parquet') ORDER BY leaf_id"],text=True)))
            self.assertEqual([r['parent_id'] for r in populations[0]],[r['parent_id'] for r in populations[1]])
            self.assertNotEqual([r['amount'] for r in populations[0]],[r['amount'] for r in populations[1]])
            self.assertNotEqual([r['parent_id'] for r in populations[0]],[r['parent_id'] for r in populations[2]])
            self.assertEqual([{k:v for k,v in r.items() if k!='parent_id'} for r in populations[0]],
                             [{k:v for k,v in r.items() if k!='parent_id'} for r in populations[2]])

    def test_empty_and_all_null_populations_have_exact_answers(self):
        with tempfile.TemporaryDirectory() as tmp:
            for i,args in enumerate([['--leaves',0],['--leaves',16,'--null-rate',1]]):
                out=Path(tmp)/str(i);p=self.generate(out,*args)
                self.assertEqual(p.returncode,0,p.stderr)
                m=json.loads((out/'shape-contract.json').read_text())
                setup=''.join(f"CREATE VIEW {n} AS SELECT * FROM read_parquet('{out}/{n}.parquet');" for n in m['tables'])
                for op in m['answers']:
                    for depth in ('flat','d1','d3','d5'):
                        q=(ROOT/f'engines/duckdb/queries/sd_{op}_{depth}.sql').read_text()
                        actual=json.loads(subprocess.check_output(['duckdb','-json','-c',setup+q],text=True) or '[]')
                        self.assertEqual(actual,[{'item_count':0,'total':None}] if op=='filter' else [])

    def test_transform_materializes_padding_and_tag(self):
        with tempfile.TemporaryDirectory() as tmp:
            out=Path(tmp)/'data';p=self.generate(out,'--parents',8,'--fanout',16,'--entropy','high','--width',12)
            self.assertEqual(p.returncode,0,p.stderr)
            m=json.loads((out/'shape-contract.json').read_text())
            setup=''.join(f"CREATE VIEW {n} AS SELECT * FROM read_parquet('{out}/{n}.parquet');" for n in m['tables'])
            flat=json.loads(subprocess.check_output(['duckdb','-json','-c',setup+'SELECT * FROM shape_leaves_v2 WHERE amount>=50 ORDER BY parent_id,leaf_id'],text=True))
            groups={}
            for r in flat:groups.setdefault(r['parent_id'],[]).append(dict(leaf_id=r['leaf_id'],adjusted=r['amount']*2,tag=r['tag'],padding=r['padding']))
            expected=[dict(parent_id=k,selected=v) for k,v in groups.items()]
            self.assertTrue(expected)
            q=(ROOT/'engines/duckdb/queries/sd_transform_d5.sql').read_text()
            actual=json.loads(subprocess.check_output(['duckdb','-json','-c',setup+q],text=True))
            self.assertEqual(actual,expected)
