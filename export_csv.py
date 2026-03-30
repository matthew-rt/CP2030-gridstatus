#!/usr/bin/env python3
"""
Export CP2030 history database to CSV.
Usage: python export_csv.py [output.csv]
       DB_FILE=/path/to/history.db python export_csv.py
Defaults to history.db in the same directory as STATE_FILE.
"""

import csv
import os
import sqlite3
import sys
from datetime import datetime, timezone

DB_FILE = os.environ.get("DB_FILE", "/var/www/cp2030/history.db")

output_path = sys.argv[1] if len(sys.argv) > 1 else f"cp2030_export_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"

with sqlite3.connect(DB_FILE) as con:
    con.row_factory = sqlite3.Row
    rows = con.execute("SELECT * FROM history ORDER BY timestamp").fetchall()

if not rows:
    print("No data in database.")
    sys.exit(0)

with open(output_path, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(dict(r) for r in rows)

print(f"Exported {len(rows)} rows to {output_path}")
