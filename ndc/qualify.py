"""Independently validate all TPC-H qualification pins using DuckDB flat queries."""
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
import subprocess
import sys


def rows(text):
    result=[]
    for line in text.splitlines():
        row=[]
        for value in line.removesuffix('|').split('|'):
            if value=='\\N': row.append(None);continue
            try: row.append(Decimal(value))
            except InvalidOperation: row.append(value)
        result.append(tuple(row))
    return result


def main():
    code=Path(__file__).resolve().parent
    manifest=json.loads((code/'queries/manifest-full.json').read_text())
    cache={}
    for name,spec in manifest.items():
        reference=spec['reference']
        if reference not in cache:
            sql=(code/reference).read_text()
            process=subprocess.run(['duckdb','-noheader','-list','-nullvalue','\\N',sys.argv[1],'-c',sql],
                                   capture_output=True,text=True,check=True)
            cache[reference]=rows(process.stdout)
        if cache[reference]!=rows((code/spec['answer']).read_text()):
            sys.exit(f'QUALIFICATION_FAIL: {name}')
    print(f'QUALIFICATION_OK: {len(manifest)} pins, {len(cache)} independent flat queries')


if __name__=='__main__':main()
