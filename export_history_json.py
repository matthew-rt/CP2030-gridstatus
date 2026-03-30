#!/usr/bin/env python3
"""
Export CP2030 history database to history.json for the replay page.
Intended to run nightly via cron:
  0 0 * * * /opt/cp2030/venv/bin/python3 /opt/cp2030/export_history_json.py

Writes atomically to avoid the replay page reading a partial file.
Override paths with env vars for local testing:
  DB_FILE=/tmp/test.db OUT_FILE=/tmp/history.json python export_history_json.py
"""

import json
import os
import sqlite3

DB_FILE  = os.environ.get("DB_FILE",  "/var/www/cp2030/history.db")
OUT_FILE = os.environ.get("OUT_FILE", "/var/www/cp2030/history.json")

with sqlite3.connect(DB_FILE) as con:
    con.row_factory = sqlite3.Row
    rows = con.execute("SELECT * FROM history ORDER BY timestamp").fetchall()

data = [dict(r) for r in rows]

tmp = OUT_FILE + ".tmp"
with open(tmp, "w") as f:
    json.dump(data, f)
os.replace(tmp, OUT_FILE)

print(f"Exported {len(data)} rows to {OUT_FILE}")
