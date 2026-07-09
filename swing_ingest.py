"""Append one swing cBot execution row (from a repository_dispatch payload) to
swing-executions.json. Idempotent: dedup by (signal_id, event, ts). Mirrors
ingest_execution.py but targets the ISOLATED swing execution log.
"""
import json, os, sys

PATH = 'swing-executions.json'
raw = os.environ.get('PAYLOAD', '').strip()
if not raw:
    print('no PAYLOAD; nothing to ingest'); sys.exit(0)
try:
    row = json.loads(raw)
    if isinstance(row, str):        # double-encoded → decode once more
        row = json.loads(row)
except Exception as e:
    print(f'::error::malformed PAYLOAD: {e}'); sys.exit(1)
if not isinstance(row, dict) or 'event' not in row:
    print('::error::payload is not an execution row'); sys.exit(1)

try:
    doc = json.load(open(PATH))
    if not isinstance(doc, dict):
        doc = {'schema_version': 1, 'executions': []}
except Exception:
    doc = {'schema_version': 1, 'executions': []}
ex = doc.setdefault('executions', [])

key = (row.get('signal_id'), row.get('event'), row.get('ts'))
if any((e.get('signal_id'), e.get('event'), e.get('ts')) == key for e in ex):
    print('duplicate event — already ingested');
else:
    ex.append(row)
doc['count'] = len(ex)
with open(PATH, 'w') as f:
    json.dump(doc, f, indent=1)
print(f"swing-executions.json now has {len(ex)} rows (last: {row.get('event')} {row.get('signal_id')})")
