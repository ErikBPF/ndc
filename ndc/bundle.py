"""Portable source + diagnostic evidence, with per-file and archive checksums."""
import hashlib
import io
import json
from pathlib import Path
import sys
import tarfile

from provenance import identity


def create(campaign,source,destination):
    campaign,source,destination=map(Path,(campaign,source,destination))
    destination.mkdir(parents=True,exist_ok=True)
    archive=destination/(campaign.name+'.tar.gz')
    if archive.exists():raise ValueError(f'archive exists: {archive}')
    files={}
    def add_tree(root,prefix,extensions):
        for path in sorted(root.rglob('*')):
            if (not path.is_file() or path.is_symlink() or path.suffix not in extensions
                    or any(part.startswith('.') or part=='__pycache__' for part in path.relative_to(root).parts)
                    or path.name.endswith('.secrets.json') or path.suffix in ('.pem','.key')):continue
            files[prefix+'/'+str(path.relative_to(root))]=path.read_bytes()
    add_tree(campaign,'campaign',{'.json','.csv','.stdout','.md'})
    for folder in ('ndc','tests','docs'):
        add_tree(source/folder,'source/'+folder,{'.py','.sh','.sql','.json','.out','.csv','.conf','.md'})
    add_tree(source/'.github/workflows','source/.github/workflows',{'.yml','.yaml'})
    for name in ('nix/flake.nix','nix/flake.lock','README.md','LICENSE','NOTICE'):
        path=source/name
        if path.is_file():files['source/'+name]=path.read_bytes()
    # Ensure the supplied snapshot really matches the measured implementation.
    for cell in campaign.glob('spark_*.json'):
        record=json.loads(cell.read_text())
        inventory_path=campaign.parent.parent/'data/dataset.json'
        inventory=json.loads(inventory_path.read_text()) if inventory_path.is_file() else {}
        if (inventory.get('dataset_id') != record.get('dataset_id')
                or identity(inventory.get('files')) != record.get('dataset_id')):
            raise ValueError('dataset inventory changed after measurement')
        if record.get('fmt') in ('delta','iceberg'):
            marker=json.loads((campaign.parent.parent/f'format-{record["fmt"]}.json').read_text())
            if marker.get('dataset_id') != record['dataset_id'] or marker.get('format_id') != record['format_id']:
                raise ValueError('format inventory changed after measurement')
        for relative,expected in record.get('environment',{}).get('source_files',{}).items():
            content=files.get('source/'+relative)
            if content is None: raise ValueError(f'source missing from bundle: {relative}')
            actual=hashlib.sha256(content).hexdigest()
            if actual!=expected:raise ValueError(f'source changed after measurement: {relative}')
    for name in ('dataset.json','shape.json'):
        path=campaign.parent.parent/'data'/name
        if path.is_file():files['dataset/'+name]=path.read_bytes()
    for path in campaign.parent.parent.glob('format-*.json'):
        files['dataset/'+path.name]=path.read_bytes()
    checksums={name:hashlib.sha256(content).hexdigest() for name,content in files.items()}
    files['checksums.json']=json.dumps(checksums,indent=2).encode()
    with tarfile.open(archive,'w:gz') as tar:
        for name,content in files.items():
            info=tarfile.TarInfo(name);info.size=len(content);info.mode=0o755 if name.endswith('.sh') else 0o644
            tar.addfile(info,io.BytesIO(content))
    with open(archive,'rb') as f: digest=hashlib.file_digest(f,'sha256').hexdigest()
    Path(str(archive)+'.sha256').write_text(f'{digest}  {archive.name}\n')
    return archive


if __name__=='__main__':
    campaign=Path(sys.argv[1]).resolve()
    print(create(campaign,Path(__file__).resolve().parents[1],campaign.parent))
