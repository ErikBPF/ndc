"""Download locked artifacts; verify before installation. No credentials required."""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile


def verify(path,algorithm,expected):
    h=hashlib.new(algorithm)
    with open(path,'rb') as f:
        for block in iter(lambda:f.read(1024*1024),b''):h.update(block)
    if h.hexdigest()!=expected:raise ValueError(f'checksum mismatch: {path}')


def main():
    base=Path(sys.argv[1]);base.mkdir(parents=True,exist_ok=True)
    lock=json.loads(Path(__file__).with_name('artifacts.json').read_text())
    for entry in lock:
        target=base/entry['path']
        if target.is_file() and not entry.get('unpack'):
            verify(target,entry['algorithm'],entry['digest']);continue
        if entry.get('unpack') and (base/entry['unpack']/'.ndc-verified').exists():
            if (base/entry['unpack']/'.ndc-verified').read_text()==entry['digest']:continue
        with tempfile.TemporaryDirectory(dir=base) as tmp:
            archive=Path(tmp)/'download'
            subprocess.run(['curl','--fail','--location','--retry','3','--max-time','600',
                            '--output',str(archive),entry['url']],check=True)
            verify(archive,entry['algorithm'],entry['digest'])
            if entry.get('unpack'):
                with tarfile.open(archive) as tar:tar.extractall(tmp,filter='data')
                destination=base/entry['unpack']
                if destination.exists():
                    raise ValueError(f'unverified existing distribution: {destination}; use another SPARK41_BASE')
                shutil.move(str(Path(tmp)/entry['unpack']),str(destination))
                (destination/'.ndc-verified').write_text(entry['digest'])
            else:
                target.parent.mkdir(parents=True,exist_ok=True)
                archive.replace(target)
        print(f'VERIFIED {entry["path"]}')


if __name__=='__main__':main()
