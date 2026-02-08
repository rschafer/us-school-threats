#!/usr/bin/env python3
"""
Fetch school threat data from published Google Sheets (CSV format).
Replaces API-based fetching with direct spreadsheet downloads.

Usage:
  python scripts/fetch_google_sheets.py           # Fetch both years
  python scripts/fetch_google_sheets.py --year 2025  # Fetch only 2025
  python scripts/fetch_google_sheets.py --year 2026  # Fetch only 2026
"""

import argparse
import csv
import io
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

try:
    import requests
except ImportError:
    print("Missing requests. Run: pip install requests", file=sys.stderr)
    sys.exit(1)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"

# Google Sheets published CSV URLs
SHEETS = {
    2026: "https://docs.google.com/spreadsheets/d/e/2PACX-1vSdp9tXoBAr9Vm6TiH2MMJlfjAlyerHmWfw1KAMzH9l8ni0ZlMKGXhHhgNMlr_KqDSCaWuKIOlHIEXQ/pub?output=csv",
    2025: "https://docs.google.com/spreadsheets/d/e/2PACX-1vRUhXEChk3c5vWmqqwituanFFqMTERvchQSmUfKwBylY-IMBmggnlMzAGfPuXd0XOtHfFS_p9u7K02J/pub?output=csv",
}

# Column mapping from Google Sheet headers to JSON keys
# Sheet column -> JSON key
COLUMN_MAP = {
    "Article Date": "article_date",
    "School(s)": "school",
    "Type of School": "school_type",
    "State": "state",
    "Region": "region",
    "Source(s)": "source",
    "Offense Date": "offense_date",
    "Time": "time",
    "Law Enforcement Agency": "law_enforcement",
    "Type of Threat": "threat_type",
    "How Threat was Conveyed": "conveyance",
    "Who was Threatened?": "who_threatened",
    "Incident Details": "incident_details",
    "Type of Lockdown": "lockdown_type",
    "Evacuation?": "evacuation",
    "Cancellations/Dismissals/Postponements": "classes_cancelled",
    "Precautions/Resources Available": "precautions",
    "Weapons?": "weapons",
    "Gender": "gender",
    "Person Responsible": "charged",
    "Custody Status/Disposition": "custody",
    "Charges": "charges",
    "Bond": "bond",
    "Additional Sources": "additional_sources",
}

# State abbreviation to full name mapping
STATE_ABBREV = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming", "DC": "Washington D.C.",
}

# Valid full state names (for validation)
VALID_STATES = set(STATE_ABBREV.values())


def normalize_state(raw_state: str) -> str:
    """Normalize state names: handle abbreviations, multi-state, and variations."""
    if not raw_state:
        return ""

    state = raw_state.strip()

    # Handle D.C. variations
    if state.lower() in ("washington d.c", "washington d.c.", "washington dc", "dc", "d.c.", "d.c"):
        return "Washington D.C."

    # If it's already a valid full state name, return it
    if state in VALID_STATES:
        return state

    # Check if it's an abbreviation
    upper = state.upper()
    if upper in STATE_ABBREV:
        return STATE_ABBREV[upper]

    # Handle multi-state entries like "OR and WA", "Vermont and NH"
    # Split on common separators and take the first state
    for sep in [" and ", " & ", "/", ","]:
        if sep in state:
            parts = state.split(sep)
            first_part = parts[0].strip()
            # Recursively normalize the first part
            normalized = normalize_state(first_part)
            if normalized:
                return normalized

    # Try case-insensitive match against valid states
    state_lower = state.lower()
    for valid in VALID_STATES:
        if valid.lower() == state_lower:
            return valid

    # Return original if no match found (will be flagged in data quality)
    return state


def normalize_threat_type(raw_type: str) -> str:
    """Normalize threat types to consistent categories."""
    if not raw_type:
        return ""

    t = raw_type.strip().lower()

    # Combined threats (check first - order matters)
    if ("bomb" in t and "shooting" in t) or ("shooting" in t and "bomb" in t):
        return "Bomb and Shooting"
    if "bomb" in t and "weapon" in t:
        return "Bomb and Shooting"

    # Bomb threats
    if "bomb" in t or "grenade" in t:
        return "Bomb"

    # Shooting threats
    if "shooting" in t or "gun" in t or "firearm" in t:
        return "Shooting"

    # Hit lists
    if "list" in t or "hitlist" in t or "hit list" in t:
        return "Hit List"

    # Swatting
    if "swat" in t:
        return "Swatting"

    # General threat variations - consolidate into "Threat"
    threat_keywords = [
        "threat of violence", "threats of violence", "threat of mass",
        "threats of mass", "violent threat", "violent threats",
        "threatening", "threat to", "threats to", "terroristic",
        "threat", "threats", "theat"  # typo in data
    ]
    for keyword in threat_keywords:
        if keyword in t:
            return "Threat"

    # Concerning comments/posts
    if "concerning" in t:
        return "Threat"

    # Planning/organized violence
    if "planning" in t or "organized" in t:
        return "Threat"

    # Mass violence without specific type
    if "mass violence" in t or "violence" in t:
        return "Threat"

    # Killing threats
    if "kill" in t or "harm" in t:
        return "Threat"

    # Return original with title case if no match
    return raw_type.strip().title()


def fetch_csv(url: str) -> Optional[str]:
    """Fetch CSV content from Google Sheets URL, following redirects."""
    try:
        # Add cache-busting parameter to ensure fresh data
        cache_buster = f"&_cb={int(datetime.utcnow().timestamp())}"
        fetch_url = url + cache_buster
        headers = {
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
        }
        r = requests.get(fetch_url, timeout=30, allow_redirects=True, headers=headers)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"Error fetching {url[:60]}...: {e}", file=sys.stderr)
        return None


def normalize_header(header: str) -> str:
    """Normalize header by stripping whitespace and common variations."""
    return header.strip()


def parse_csv_to_incidents(csv_text: str) -> list[dict]:
    """Parse CSV text and convert to incident dictionaries."""
    incidents = []
    reader = csv.DictReader(io.StringIO(csv_text))

    # Build a mapping from actual headers to our expected headers
    header_map = {}
    if reader.fieldnames:
        for field in reader.fieldnames:
            normalized = normalize_header(field)
            if normalized in COLUMN_MAP:
                header_map[field] = COLUMN_MAP[normalized]
            else:
                # Try partial match for flexibility
                for sheet_col, json_key in COLUMN_MAP.items():
                    if sheet_col.lower() in normalized.lower() or normalized.lower() in sheet_col.lower():
                        header_map[field] = json_key
                        break

    for idx, row in enumerate(reader, start=1):
        # Skip empty rows
        if not any(v.strip() for v in row.values() if v):
            continue

        incident = {"id": idx}

        for csv_col, json_key in header_map.items():
            value = row.get(csv_col, "")
            value = value.strip() if value else ""
            # Normalize state names
            if json_key == "state":
                value = normalize_state(value)
            # Normalize threat types
            if json_key == "threat_type":
                value = normalize_threat_type(value)
            incident[json_key] = value

        # Ensure all expected fields exist (even if empty)
        for json_key in set(COLUMN_MAP.values()):
            if json_key not in incident:
                incident[json_key] = ""

        incidents.append(incident)

    return incidents


def fetch_year(year: int) -> list[dict]:
    """Fetch and parse data for a specific year."""
    url = SHEETS.get(year)
    if not url:
        print(f"No URL configured for year {year}", file=sys.stderr)
        return []

    print(f"Fetching {year} data from Google Sheets...")
    csv_text = fetch_csv(url)
    if not csv_text:
        return []

    incidents = parse_csv_to_incidents(csv_text)
    print(f"  Parsed {len(incidents)} incidents for {year}")
    return incidents


def save_json(data: list[dict], filepath: Path) -> None:
    """Save incidents to JSON file."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    filepath.write_text(json.dumps(data, indent=2), encoding="utf-8")
    print(f"  Saved to {filepath}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch school threat data from Google Sheets")
    parser.add_argument("--year", type=int, choices=[2025, 2026], help="Fetch only this year (default: both)")
    args = parser.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    years_to_fetch = [args.year] if args.year else [2025, 2026]
    total_incidents = 0

    for year in years_to_fetch:
        incidents = fetch_year(year)
        if incidents:
            output_file = DATA_DIR / f"school_threats_{year}.json"
            save_json(incidents, output_file)
            total_incidents += len(incidents)

    print(f"\nTotal: {total_incidents} incidents fetched")
    print(f"Timestamp: {datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
