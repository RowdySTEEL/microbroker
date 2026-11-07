"""Copy user-owned local configuration/runtime data without overwriting either project."""
import argparse
from pathlib import Path
import re
import sqlite3
from urllib.parse import quote

root = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser()
parser.add_argument('--source',required=True,help='Existing MicroBroker folder')
parser.add_argument('--compose-project',default='microbroker-r1',help='Existing Docker Compose project name')
args = parser.parse_args()
source = Path(args.source).expanduser().resolve()
if source == root or not (source/'.env').is_file():
    raise SystemExit('Select the existing project folder containing your local .env')
if not re.fullmatch(r'[a-z0-9][a-z0-9_-]*',args.compose_project):
    raise SystemExit('Invalid Compose project name')
if (root/'.env').exists():
    raise SystemExit('Destination .env already exists; no files overwritten')
source_db = source/'data/microbroker.db'
dest_db = root/'data/microbroker.db'
if source_db.is_file() and dest_db.exists():
    raise SystemExit('Destination SQLite database exists; no files overwritten')
# Do not print configuration contents or secrets. Preserve an explicit existing project name.
content=(source/'.env').read_text(encoding='utf-8')
has_project=any(line.partition('=')[0].strip()=='COMPOSE_PROJECT_NAME' for line in content.splitlines() if not line.strip().startswith('#'))
if not has_project:
    content+='\nCOMPOSE_PROJECT_NAME='+args.compose_project+'\n'
# Use sqlite's backup API, including committed WAL data, rather than copying a live DB file.
if source_db.is_file():
    dest_db.parent.mkdir(parents=True,exist_ok=True)
    try:
        with sqlite3.connect('file:'+quote(str(source_db))+'?mode=ro',uri=True) as old_db:
            with sqlite3.connect(dest_db) as new_db:
                old_db.backup(new_db)
    except Exception:
        dest_db.unlink(missing_ok=True)
        raise SystemExit('SQLite backup failed; destination .env was not created') from None
with (root/'.env').open('x',encoding='utf-8') as output:
    output.write(content)
(root/'.env').chmod(0o600)
print('Local configuration imported; existing Compose project preserved; original project files unchanged')
print('SQLite backup imported' if source_db.is_file() else 'No default SQLite database to import')
