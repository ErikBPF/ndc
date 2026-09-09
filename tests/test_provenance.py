import json
from pathlib import Path
import tempfile
import unittest
from test_harness import module


class ProvenanceTests(unittest.TestCase):
    def test_inventory_preserves_canonical_manifest_and_is_stable(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);manifest=root/'dataset.json'
            manifest.write_text('{"status":"ok"}')
            (root/'part.parquet').write_bytes(b'data')
            provenance=module('provenance')
            first=provenance.dataset(root)
            self.assertEqual(first,provenance.dataset(root))
            self.assertEqual(manifest.read_text(),'{"status":"ok"}')
            self.assertEqual(set(first['files']),{'part.parquet'})

    def test_changed_format_input_is_detected(self):
        provenance=module('provenance')
        self.assertTrue(hasattr(provenance,'format_identity'),'format fingerprint missing')
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);data=root/'data';data.mkdir()
            (data/'shape.parquet').write_bytes(b'base')
            target=root/'data_delta/shape';target.mkdir(parents=True)
            file=target/'part.parquet';file.write_bytes(b'old')
            first=provenance.format_identity(data,'delta')
            file.write_bytes(b'new')
            self.assertNotEqual(first,provenance.format_identity(data,'delta'))

    def test_bundle_rejects_changed_dataset_inventory(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); campaign=root/'results/campaign';campaign.mkdir(parents=True)
            data=root/'data';data.mkdir()
            (data/'inventory.json').write_text(json.dumps({'dataset_id':'new','files':{}}))
            (campaign/'spark_vanilla_parquet.json').write_text(json.dumps({'dataset_id':'measured'}))
            with self.assertRaisesRegex(ValueError, 'dataset'):
                module('bundle').create(campaign, root/'source', root/'bundles')

    def test_bundle_handles_nested_qualification_campaign(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);campaign=root/'results/qualification/campaign';campaign.mkdir(parents=True)
            data=root/'data';data.mkdir()
            inventory={'files':{},'dataset_id':module('provenance').identity({})}
            (data/'inventory.json').write_text(json.dumps(inventory))
            (campaign/'spark_vanilla_parquet.json').write_text(json.dumps({'dataset_id':inventory['dataset_id']}))
            (campaign.parent/'qualification.json').write_text('{"status":"ok"}')
            archive=module('bundle').create(campaign,root/'source',root/'bundles')
            import tarfile
            with tarfile.open(archive) as tar:
                self.assertIn('dataset/inventory.json',tar.getnames())
                self.assertIn('qualification/qualification.json',tar.getnames())

    def test_evidence_bundle_has_checksums_and_sources(self):
        bundle=module('bundle')
        self.assertTrue(hasattr(bundle,'create'),'evidence bundler missing')
        with tempfile.TemporaryDirectory() as tmp:
            d=Path(tmp); campaign=d/'campaign';campaign.mkdir()
            (campaign/'campaign.json').write_text('{"status":"ok","cells":[]}')
            source=d/'repo';(source/'ndc').mkdir(parents=True)
            (source/'ndc/run.sh').write_text('source')
            (source/'ndc/credentials.secrets.json').write_text('must not ship')
            (source/'ndc/.env').write_text('must not ship')
            archive=bundle.create(campaign,source,d)
            self.assertTrue(Path(str(archive)+'.sha256').is_file())
            import tarfile
            with tarfile.open(archive) as tar:
                names=tar.getnames()
                self.assertIn('source/ndc/run.sh',names)
                self.assertTrue(tar.getmember('source/ndc/run.sh').mode & 0o111)
                self.assertTrue(any('campaign.json' in n for n in names))
                self.assertFalse(any('secrets.json' in n or n.endswith('.env') for n in names))

if __name__=='__main__': unittest.main()
