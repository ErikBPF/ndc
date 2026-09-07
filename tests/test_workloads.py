import json
from pathlib import Path
import unittest
from test_harness import module, ROOT


class WorkloadTests(unittest.TestCase):
    def test_shape_generation_is_deterministic_and_has_edge_cases(self):
        shapes = module('shapes')
        rows = shapes.records(32, 16, 8, 7)
        self.assertEqual(rows, shapes.records(32, 16, 8, 7))
        self.assertTrue(any(r['items'] is None for r in rows))
        self.assertTrue(any(r['items'] == [] for r in rows))
        self.assertTrue(any(r['items'] and None in r['items'] for r in rows))
        self.assertTrue(any(len(r['items'] or []) == 16 for r in rows))
        self.assertTrue(any(len(r['groups'] or []) > 1 for r in rows))
        self.assertTrue(any(i and i['amount'] is None for r in rows for i in r['items'] or []))
        self.assertEqual(len(rows[0]['padding']), 8)
        for invalid in ((0,16,8,7), (32,0,8,7), (32,16,-1,7)):
            with self.assertRaises(ValueError):
                shapes.records(*invalid)

    def test_manifests_have_explicit_semantics(self):
        for path in (ROOT/'ndc/queries').glob('manifest-*.json'):
            manifest = json.loads(path.read_text())
            for name, query in manifest.items():
                with self.subTest(manifest=path.name, query=name):
                    self.assertIsInstance(query, dict)
                    self.assertTrue({'sql','family','operation','layout','reference','ordered'} <= query.keys())
                    self.assertTrue((ROOT/'ndc'/query['sql']).is_file())
                    self.assertTrue((ROOT/'ndc'/query['reference']).is_file())

    def test_nix_environment_excludes_workspace_data(self):
        self.assertTrue((ROOT/'nix/flake.nix').is_file(), 'Nix source must be a small dedicated directory')
        self.assertIn('path:$ROOT/nix', (ROOT/'ndc/run.sh').read_text())

    def test_required_suites_exist(self):
        for name in ('scan','compute','shapes','write','maintenance','ds'):
            self.assertTrue((ROOT/f'ndc/queries/manifest-{name}.json').is_file(), name)

    def test_measurement_operations_have_deterministic_expected_state(self):
        workloads = module('mutations')
        base = [(k, {'amount':k*10, 'items':[]}) for k in (1,2,3)]
        self.assertEqual(workloads.expected_state(base, 'append'), base + [(4, {'amount':40, 'items':[]})])
        self.assertEqual(workloads.expected_state(base, 'update'), [(1, {'amount':11, 'items':[]})] + base[1:])
        self.assertEqual(workloads.expected_state(base, 'delete'), base[1:])
        self.assertEqual(workloads.expected_state(base, 'compact'), base)


if __name__ == '__main__':
    unittest.main()
