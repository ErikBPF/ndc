"""Run after build-scale + matrix. Inject a wrong answer and require a real Spark failure."""
import json
import os
from pathlib import Path
import subprocess
import tempfile

root=Path(__file__).resolve().parents[1]
workspace=Path(os.environ['NDC_WORKSPACE'])
manifest=json.loads((root/'ndc/queries/manifest-full.json').read_text())
with tempfile.TemporaryDirectory(prefix='invalid_',dir=root/'ndc/queries') as tmp:
    qdir=Path(tmp)
    query=qdir/'wrong.sql'
    query.write_text('SELECT CAST(-1 AS BIGINT) AS n, CAST(0 AS DECIMAL(38,2)) AS total')
    spec=dict(manifest['e18_nested'],sql=str(query.relative_to(root/'ndc')))
    file=qdir/'manifest.json';file.write_text(json.dumps({'e18_nested':spec}))
    campaign=Path(tempfile.mkdtemp(prefix='negative_',dir=workspace/'results'))
    env=dict(os.environ,NDC_CAMPAIGN_DIR=str(campaign),WARMUPS='0')
    p=subprocess.run([str(root/'ndc/run.sh'),'spark','vanilla','parquet',str(file),'1'],env=env)
    result=json.loads((campaign/'spark_vanilla_parquet.json').read_text())
    assert p.returncode!=0, 'wrong answer exited successfully'
    assert result['parity_ok'] is False
    assert len(result['results'])==1 and result['results'][0]['status']=='invalid',result
    print('INTEGRATION_OK: actual Spark wrong-answer injection failed validity and process exit')
