"""Every workload declares the dataset tables it and its reference require."""
import json
import unittest
from test_harness import module, ROOT


class TableContractTests(unittest.TestCase):
    def manifests(self):
        return sorted((ROOT / 'ndc/queries').glob('manifest-*.json'))

    def test_every_workload_declares_known_unique_tables(self):
        api = module('schedule')
        for path in self.manifests():
            manifest = json.loads(path.read_text())
            for name, spec in manifest.items():
                with self.subTest(manifest=path.name, query=name):
                    tables = spec.get('tables')
                    self.assertIsInstance(tables, list)
                    self.assertTrue(tables)
                    self.assertEqual(len(tables), len(set(tables)))
                    self.assertTrue(set(tables) <= api.DATASET_TABLES,
                                    set(tables) - api.DATASET_TABLES)

    def test_required_tables_is_the_minimal_union(self):
        api = module('schedule')
        self.assertEqual(set(api.required_tables(api.load_suite(ROOT/'ndc', 'scan'))),
                         {'orders_nested', 'customer', 'orders', 'lineitem'})
        depth = set(api.required_tables(api.load_suite(ROOT/'ndc', 'depth')))
        self.assertNotIn('orders_nested', depth)
        self.assertNotIn('shape', depth)
        whole = set(api.required_tables(api.load_suite(ROOT/'ndc', 'all')))
        self.assertEqual(len(whole), 17)
        self.assertFalse({'partsupp', 'supplier', 'nation', 'region'} & whole)

    def test_qualification_pins_cover_nested_and_flat(self):
        api = module('schedule')
        pins = (len(api.load_suite(ROOT/'ndc', 'tpch'))
                + len(api.load_suite(ROOT/'ndc', 'flat')))
        self.assertEqual(pins, 24)

    def test_dataset_never_declares_unused_flat_tables(self):
        unused = {'partsupp', 'supplier', 'nation', 'region'}
        for path in self.manifests():
            for name, spec in json.loads(path.read_text()).items():
                with self.subTest(manifest=path.name, query=name):
                    self.assertFalse(unused & set(spec['tables']))

    def test_unknown_declared_table_is_rejected(self):
        api = module('schedule')
        entry = json.loads((ROOT/'ndc/queries/manifest-flat.json').read_text())['q6_flat']
        for bad_tables in (['not_a_table'], [1], 'orders_nested', ['orders_nested', 'orders_nested']):
            with self.subTest(tables=bad_tables):
                with self.assertRaises(ValueError):
                    api.validate_manifest({'q6_flat': dict(entry, tables=bad_tables)})


if __name__ == '__main__':
    unittest.main()
