#!/usr/bin/env python3
"""
Migrate school_threats_2026.json into a SQLite database.

Usage:
  python scripts/migrate_to_sqlite.py
  python scripts/migrate_to_sqlite.py --db data/school_threats.db --json data/school_threats_2026.json
"""

import argparse
import json
import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB = PROJECT_ROOT / "data" / "school_threats.db"
DEFAULT_JSON = PROJECT_ROOT / "data" / "school_threats_2026.json"

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS incidents (
    id INTEGER PRIMARY KEY,
    school TEXT,
    school_type TEXT,
    state TEXT,
    region TEXT,
    source TEXT,
    offense_date TEXT,
    time TEXT,
    law_enforcement TEXT,
    threat_type TEXT,
    conveyance TEXT,
    who_threatened TEXT,
    incident_details TEXT,
    lockdown_type TEXT,
    classes_cancelled TEXT,
    precautions TEXT,
    weapons TEXT,
    gender TEXT,
    charged TEXT,
    custody TEXT,
    charges TEXT,
    bond TEXT,
    additional_sources TEXT
);
"""

CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_state ON incidents(state);",
    "CREATE INDEX IF NOT EXISTS idx_threat_type ON incidents(threat_type);",
    "CREATE INDEX IF NOT EXISTS idx_school_type ON incidents(school_type);",
    "CREATE INDEX IF NOT EXISTS idx_region ON incidents(region);",
    "CREATE INDEX IF NOT EXISTS idx_conveyance ON incidents(conveyance);",
    "CREATE INDEX IF NOT EXISTS idx_custody ON incidents(custody);",
]

COLUMNS = [
    "id", "school", "school_type", "state", "region", "source",
    "offense_date", "time", "law_enforcement", "threat_type", "conveyance",
    "who_threatened", "incident_details", "lockdown_type", "classes_cancelled",
    "precautions", "weapons", "gender", "charged", "custody", "charges",
    "bond", "additional_sources",
]


def migrate(json_path: Path, db_path: Path):
    with open(json_path) as f:
        data = json.load(f)

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS incidents;")
    cur.execute(CREATE_TABLE)
    for idx_sql in CREATE_INDEXES:
        cur.execute(idx_sql)

    placeholders = ", ".join(["?"] * len(COLUMNS))
    insert_sql = f"INSERT INTO incidents ({', '.join(COLUMNS)}) VALUES ({placeholders})"

    rows = []
    for record in data:
        row = tuple(record.get(col, "") for col in COLUMNS)
        rows.append(row)

    cur.executemany(insert_sql, rows)
    conn.commit()

    count = cur.execute("SELECT COUNT(*) FROM incidents").fetchone()[0]
    print(f"Migrated {count} incidents to {db_path}")

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate JSON incidents to SQLite")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--json", type=Path, default=DEFAULT_JSON)
    args = parser.parse_args()
    migrate(args.json, args.db)
