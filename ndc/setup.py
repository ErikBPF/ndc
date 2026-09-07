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


def download(entry,target):
    urls=[entry['url'],*entry.get('fallback_urls',[])]
    for index,url in enumerate(urls):
        target.unlink(missing_ok=True)
        print(f'DOWNLOAD {url}',flush=True)
        try:
            subprocess.run(['curl','--fail','--location','--silent','--show-error',
                            '--connect-timeout','20','--max-time','600',
                            '--speed-limit','1048576','--speed-time','30',
                            '--output',str(target),url],check=True)
        except subprocess.CalledProcessError:
            target.unlink(missing_ok=True)
            if index==len(urls)-1:raise
            continue
        verify(target,entry['algorithm'],entry['digest'])
        return


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
            archive=target
            if archive.is_file():
                verify(archive,entry['algorithm'],entry['digest'])
            else:
                downloaded=Path(tmp)/'download'
                download(entry,downloaded)
                target.parent.mkdir(parents=True,exist_ok=True)
                downloaded.replace(target)
            if entry.get('unpack'):
                with tarfile.open(archive) as tar:tar.extractall(tmp,filter='data')
                destination=base/entry['unpack']
                if destination.exists():
                    raise ValueError(f'unverified existing distribution: {destination}; use another SPARK41_BASE')
                shutil.move(str(Path(tmp)/entry['unpack']),str(destination))
                (destination/'.ndc-verified').write_text(entry['digest'])
        print(f'VERIFIED {entry["path"]}')


if __name__=='__main__':main()
