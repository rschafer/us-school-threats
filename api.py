#!/usr/bin/env python3
"""
Lightweight Flask API serving school threat data from SQLite.

Usage:
  python api.py                    # Runs on port 5001
  python api.py --port 8080        # Custom port

Endpoints:
  GET /api/incidents               # All incidents (with optional query params)
  GET /api/incidents?state=Florida&threat_type=Bomb
  GET /api/stats                   # Aggregate statistics
  GET /api/filters                 # Available filter values
"""

import argparse
import sqlite3
from pathlib import Path

from flask import Flask, g, jsonify, request
from flask_cors import CORS

PROJECT_ROOT = Path(__file__).resolve().parent
DB_PATH = PROJECT_ROOT / "data" / "school_threats.db"

app = Flask(__name__, static_folder=".", static_url_path="")
CORS(app)

FILTERABLE_FIELDS = [
    "state", "region", "threat_type", "school_type",
    "conveyance", "custody", "gender",
]


def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
    return g.db


@app.teardown_appcontext
def close_db(exc):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def build_where(params):
    clauses = []
    values = []
    for field in FILTERABLE_FIELDS:
        val = params.get(field)
        if val:
            clauses.append(f"{field} = ?")
            values.append(val)
    where = " AND ".join(clauses) if clauses else "1=1"
    return where, values


@app.route("/")
def serve_index():
    return app.send_static_file("index.html")


@app.route("/api/incidents")
def get_incidents():
    where, values = build_where(request.args)
    db = get_db()
    rows = db.execute(f"SELECT * FROM incidents WHERE {where}", values).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/stats")
def get_stats():
    where, values = build_where(request.args)
    db = get_db()

    total = db.execute(f"SELECT COUNT(*) as c FROM incidents WHERE {where}", values).fetchone()["c"]

    by_state = db.execute(
        f"SELECT state, COUNT(*) as count FROM incidents WHERE {where} GROUP BY state ORDER BY count DESC",
        values,
    ).fetchall()

    by_threat = db.execute(
        f"SELECT threat_type, COUNT(*) as count FROM incidents WHERE {where} GROUP BY threat_type ORDER BY count DESC",
        values,
    ).fetchall()

    by_school = db.execute(
        f"SELECT school_type, COUNT(*) as count FROM incidents WHERE {where} GROUP BY school_type ORDER BY count DESC",
        values,
    ).fetchall()

    by_region = db.execute(
        f"SELECT region, COUNT(*) as count FROM incidents WHERE {where} GROUP BY region ORDER BY count DESC",
        values,
    ).fetchall()

    by_conveyance = db.execute(
        f"SELECT conveyance, COUNT(*) as count FROM incidents WHERE {where} GROUP BY conveyance ORDER BY count DESC",
        values,
    ).fetchall()

    return jsonify({
        "total": total,
        "by_state": [dict(r) for r in by_state],
        "by_threat_type": [dict(r) for r in by_threat],
        "by_school_type": [dict(r) for r in by_school],
        "by_region": [dict(r) for r in by_region],
        "by_conveyance": [dict(r) for r in by_conveyance],
    })


@app.route("/api/filters")
def get_filters():
    db = get_db()
    result = {}
    for field in FILTERABLE_FIELDS:
        rows = db.execute(
            f"SELECT DISTINCT {field} FROM incidents WHERE {field} != '' ORDER BY {field}"
        ).fetchall()
        result[field] = [r[0] for r in rows]
    return jsonify(result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=5001)
    args = parser.parse_args()

    if not DB_PATH.exists():
        print(f"Database not found at {DB_PATH}. Run: python scripts/migrate_to_sqlite.py")
        exit(1)

    print(f"Serving on http://localhost:{args.port}")
    app.run(host="0.0.0.0", port=args.port, debug=True)
