"""Record repeatable offline verification, with no credential values."""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import sys

root = Path(__file__).resolve().parents[1]
release = sys.argv[1] if len(sys.argv) == 2 else ""
if not re.fullmatch(r"R[0-9]+(?:_[0-9]+)?", release):
    raise SystemExit("Usage: python3 scripts/verify_release.py Rn")
env = os.environ.copy()
env["PYTHONPATH"] = str(root / "src")
command = [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-v"]
started = datetime.now(timezone.utc).isoformat()
result = subprocess.run(command, cwd=root, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
folder = root / "docs" / "releases"
folder.mkdir(parents=True, exist_ok=True)
(folder / f"{release}_TEST_RESULTS.txt").write_text(result.stdout, encoding="utf-8")
(folder / f"{release}_VERIFICATION.json").write_text(json.dumps({"release": release,
    "started_at_utc": started, "finished_at_utc": datetime.now(timezone.utc).isoformat(),
    "python": platform.python_version(), "command": "PYTHONPATH=src python3 -m unittest discover -s tests -v",
    "exit_code": result.returncode, "http": "mocked", "database": "sqlite",
    "local_websocket_transport": "included_in_suite_when_websockets_installed",
    "alpaca_real_connection": "not_part_of_offline_suite", "postgresql": "not_part_of_offline_suite"}, indent=2)+"\n",encoding="utf-8")
print(result.stdout, end="")
raise SystemExit(result.returncode)
