import tempfile
import hashlib
import io
import json
import shutil
import subprocess
import sys
import tarfile
from pathlib import Path
import unittest
from test_harness import module, ROOT


class SetupTests(unittest.TestCase):
    def test_cached_archive_still_gets_extracted(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);code=root/'code';code.mkdir();base=root/'base';base.mkdir()
            archive=base/'spark-test.tgz'
            with tarfile.open(archive,'w:gz') as tar:
                info=tarfile.TarInfo('spark-test/bin/spark-submit');info.size=4;info.mode=0o755
                tar.addfile(info,io.BytesIO(b'test'))
            lock=[{'path':archive.name,'unpack':'spark-test','url':(root/'offline.tgz').as_uri(),
                   'algorithm':'sha256','digest':hashlib.sha256(archive.read_bytes()).hexdigest()}]
            (code/'artifacts.json').write_text(json.dumps(lock))
            shutil.copyfile(ROOT/'ndc/setup.py',code/'setup.py')
            p=subprocess.run([sys.executable,str(code/'setup.py'),str(base)],capture_output=True,text=True)
            self.assertEqual(p.returncode,0,p.stderr)
            self.assertTrue((base/'spark-test/bin/spark-submit').is_file())

    def test_download_uses_fallback_after_transfer_failure(self):
        setup=module('setup')
        self.assertTrue(hasattr(setup,'download'),'fallback downloader missing')
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);good=root/'good';good.write_bytes(b'locked artifact')
            target=root/'download'
            entry={'url':(root/'missing').as_uri(),'fallback_urls':[good.as_uri()],
                   'algorithm':'sha256','digest':hashlib.sha256(good.read_bytes()).hexdigest()}
            setup.download(entry,target)
            self.assertEqual(target.read_bytes(),good.read_bytes())
            entry['fallback_urls']=[(root/'also-missing').as_uri()]
            with self.assertRaises(subprocess.CalledProcessError):setup.download(entry,target)

    def test_checksum_failure_does_not_try_another_source(self):
        setup=module('setup')
        self.assertTrue(hasattr(setup,'download'),'fallback downloader missing')
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);bad=root/'bad';bad.write_bytes(b'corrupt')
            entry={'url':bad.as_uri(),'fallback_urls':[(root/'missing').as_uri()],
                   'algorithm':'sha256','digest':'0'*64}
            with self.assertRaisesRegex(ValueError,'checksum mismatch'):
                setup.download(entry,root/'download')

    def test_rejects_corrupted_download_without_replacing_target(self):
        setup=module('setup')
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'artifact';p.write_bytes(b'wrong')
            self.assertTrue(hasattr(setup,'verify'), 'checksum verifier missing')
            with self.assertRaises(ValueError): setup.verify(p,'sha256','0'*64)
            self.assertEqual(p.read_bytes(),b'wrong')

if __name__=='__main__': unittest.main()
