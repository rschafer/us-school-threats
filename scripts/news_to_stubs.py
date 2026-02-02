#!/usr/bin/env python3
"""
Turn news_feed.json articles into stub incidents for school_threats_2026.json.

Extracts school name, state, and threat type from headlines when possible.
Filters to likely US school threats and skips articles that match existing incidents.

Usage:
  python scripts/news_to_stubs.py                    # Output stubs to data/stub_incidents_from_news.json
  python scripts/news_to_stubs.py --merge            # Append new stubs to school_threats_2026.json
  python scripts/news_to_stubs.py --dry-run          # Show what would be created without writing
"""

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
NEWS_FEED = DATA_DIR / "news_feed.json"
INCIDENTS_FILE = DATA_DIR / "school_threats_2026.json"
STUBS_OUTPUT = DATA_DIR / "stub_incidents_from_news.json"

# US states (full names) for extraction and filtering
US_STATES = [
    "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut",
    "Delaware", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa",
    "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan",
    "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire",
    "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio",
    "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota",
    "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington", "West Virginia",
    "Wisconsin", "Wyoming", "Washington D.C.",
]

STATE_TO_REGION = {
    "Connecticut": "Northeast", "Maine": "Northeast", "Massachusetts": "Northeast",
    "New Hampshire": "Northeast", "Rhode Island": "Northeast", "Vermont": "Northeast",
    "New Jersey": "Northeast", "New York": "Northeast", "Pennsylvania": "Northeast",
    "Alabama": "South", "Arkansas": "South", "Delaware": "South", "Florida": "South",
    "Georgia": "South", "Kentucky": "South", "Louisiana": "South", "Maryland": "South",
    "Mississippi": "South", "North Carolina": "South", "Oklahoma": "South",
    "South Carolina": "South", "Tennessee": "South", "Texas": "South", "Virginia": "South",
    "West Virginia": "South", "Washington D.C.": "South",
    "Illinois": "Midwest", "Indiana": "Midwest", "Iowa": "Midwest", "Kansas": "Midwest",
    "Michigan": "Midwest", "Minnesota": "Midwest", "Missouri": "Midwest", "Nebraska": "Midwest",
    "North Dakota": "Midwest", "Ohio": "Midwest", "South Dakota": "Midwest", "Wisconsin": "Midwest",
    "Alaska": "West", "Arizona": "West", "California": "West", "Colorado": "West",
    "Hawaii": "West", "Idaho": "West", "Montana": "West", "Nevada": "West",
    "New Mexico": "West", "Oregon": "West", "Utah": "West", "Washington": "West", "Wyoming": "West",
}

# Headline patterns that suggest non-US (filter out)
NON_US_INDICATORS = [
    r"\bIndia\b", r"\bCanada\b", r"\bUK\b", r"\bUnited Kingdom\b", r"\bAustralia\b",
    r"\bPakistan\b", r"\bJalandhar\b", r"\bDera Ballan\b", r"\bClaresholm\b",
    r"\bModi\b", r"\bPM Modi\b", r"\bToronto\b", r"\bVancouver\b", r"\bMelbourne\b",
    r"\bSydney\b", r"\bLondon\b", r"\bManchester\b", r"\bBirmingham\b",  # UK cities
    r"\bOntario\b", r"\bAlberta\b", r"\bQuebec\b", r"\bBritish Columbia\b",
]

# School name patterns (Title Case to avoid capturing full sentences)
SCHOOL_PATTERNS = [
    r"([A-Z][a-zA-Z0-9]*(?:\s+[A-Z][a-zA-Z0-9]*){0,3}\s+(?:Junior|Senior)?\s*(?:High|Middle|Elementary)\s+School(?:s)?)",
    r"([A-Z][a-zA-Z0-9]*(?:\s+[A-Z][a-zA-Z0-9]*){0,3}\s+Community\s+School(?:s)?)",
    r"([A-Z][a-zA-Z0-9]*(?:\s+[A-Z][a-zA-Z0-9]*){0,3}\s+(?:High|Middle|Elementary)\s+(?:Campus|Academy|campus))",
    r"([A-Z][a-zA-Z0-9]*(?:\s+[A-Z][a-zA-Z0-9]*){0,2}\s+School(?:s)?)",
    r"([A-Z][a-zA-Z0-9]*(?:\s+[A-Z][a-zA-Z0-9]*){0,2}\s+Academy)",
    r"([A-Z][a-zA-Z0-9]*(?:\s+[A-Z][a-zA-Z0-9]*){0,2}\s+University)",
    r"([A-Z][a-zA-Z0-9]*(?:\s+[A-Z][a-zA-Z0-9]*){0,2}\s+College)",
]

_STOP_WORDS = frozenset(
    "the a an at to for of in on and or but is are was were be been by with from as into through during after before said reported".split()
)


def _title_to_word_set(title: str) -> set[str]:
    text = re.sub(r"[^\w\s]", " ", (title or "").lower())
    return {w for w in text.split() if w and w not in _STOP_WORDS and len(w) > 1}


def _same_incident(words_a: set[str], words_b: set[str], min_overlap: float = 0.6) -> bool:
    if not words_a or not words_b:
        return False
    overlap = len(words_a & words_b)
    return overlap / min(len(words_a), len(words_b)) >= min_overlap


def _strip_html(s: str) -> str:
    """Remove HTML tags from snippet."""
    if not s:
        return ""
    return re.sub(r"<[^>]+>", " ", s).strip()


def _parse_published_to_date(published: str) -> str:
    """Convert ISO/RFC date to '5-Jan' style for offense_date."""
    if not published:
        return "Not Specified"
    s = published.strip()[:19].replace("Z", "")
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(s[: len(fmt)], fmt)
            return f"{dt.day}-{dt.strftime('%b')}"
        except (ValueError, TypeError):
            continue
    try:
        dt = datetime.fromisoformat(published.replace("Z", "+00:00"))
        return f"{dt.day}-{dt.strftime('%b')}"
    except (ValueError, TypeError):
        pass
    return "Not Specified"


def _extract_school(title: str) -> str:
    for pat in SCHOOL_PATTERNS:
        m = re.search(pat, title)
        if m:
            name = m.group(1).strip()
            # Strip leading ordinals (e.g. "Second American Canyon High" -> "American Canyon High")
            name = re.sub(r"^(First|Second|Third|Fourth|Fifth)\s+", "", name, flags=re.IGNORECASE)
            return name
    return ""


# Major US cities/counties mapped to their state for extraction from headlines
CITY_TO_STATE = {
    "Detroit": "Michigan", "Flint": "Michigan", "Ann Arbor": "Michigan", "Grand Rapids": "Michigan",
    "Lansing": "Michigan", "Kalamazoo": "Michigan", "Dearborn": "Michigan",
    "Houston": "Texas", "Dallas": "Texas", "Austin": "Texas", "San Antonio": "Texas",
    "Fort Worth": "Texas", "El Paso": "Texas", "Arlington": "Texas", "Plano": "Texas",
    "Lubbock": "Texas", "Laredo": "Texas", "Irving": "Texas", "Amarillo": "Texas",
    "Los Angeles": "California", "San Francisco": "California", "San Diego": "California",
    "Sacramento": "California", "San Jose": "California", "Fresno": "California",
    "Oakland": "California", "Long Beach": "California", "Bakersfield": "California",
    "Stockton": "California", "Riverside": "California", "Anaheim": "California",
    "Santa Ana": "California", "Irvine": "California", "Oxnard": "California",
    "Chicago": "Illinois", "Springfield": "Illinois", "Rockford": "Illinois", "Aurora": "Illinois",
    "New York City": "New York", "NYC": "New York", "Brooklyn": "New York", "Bronx": "New York",
    "Queens": "New York", "Manhattan": "New York", "Buffalo": "New York", "Rochester": "New York",
    "Syracuse": "New York", "Albany": "New York", "Yonkers": "New York",
    "Philadelphia": "Pennsylvania", "Pittsburgh": "Pennsylvania", "Allentown": "Pennsylvania",
    "Scranton": "Pennsylvania", "Lackawanna": "Pennsylvania", "Erie": "Pennsylvania",
    "Harrisburg": "Pennsylvania", "Reading": "Pennsylvania", "Bethlehem": "Pennsylvania",
    "Miami": "Florida", "Orlando": "Florida", "Tampa": "Florida", "Jacksonville": "Florida",
    "St. Petersburg": "Florida", "Fort Lauderdale": "Florida", "Tallahassee": "Florida",
    "Palm Beach": "Florida", "Broward": "Florida", "Hialeah": "Florida", "Gainesville": "Florida",
    "Pensacola": "Florida", "Daytona": "Florida", "Cape Coral": "Florida",
    "Atlanta": "Georgia", "Savannah": "Georgia", "Augusta": "Georgia", "Macon": "Georgia",
    "Columbus": "Ohio", "Cleveland": "Ohio", "Cincinnati": "Ohio", "Toledo": "Ohio",
    "Akron": "Ohio", "Dayton": "Ohio", "Canton": "Ohio", "Youngstown": "Ohio",
    "Charlotte": "North Carolina", "Raleigh": "North Carolina", "Durham": "North Carolina",
    "Greensboro": "North Carolina", "Winston-Salem": "North Carolina", "Fayetteville": "North Carolina",
    "Phoenix": "Arizona", "Tucson": "Arizona", "Mesa": "Arizona", "Scottsdale": "Arizona",
    "Chandler": "Arizona", "Tempe": "Arizona", "Gilbert": "Arizona", "Glendale": "Arizona",
    "Denver": "Colorado", "Colorado Springs": "Colorado", "Aurora": "Colorado", "Boulder": "Colorado",
    "Fort Collins": "Colorado", "Lakewood": "Colorado", "Pueblo": "Colorado",
    "Seattle": "Washington", "Tacoma": "Washington", "Spokane": "Washington", "Bellevue": "Washington",
    "Nashville": "Tennessee", "Memphis": "Tennessee", "Knoxville": "Tennessee", "Chattanooga": "Tennessee",
    "Indianapolis": "Indiana", "Fort Wayne": "Indiana", "Evansville": "Indiana", "South Bend": "Indiana",
    "Baltimore": "Maryland", "Annapolis": "Maryland", "Silver Spring": "Maryland",
    "Las Vegas": "Nevada", "Reno": "Nevada", "Henderson": "Nevada",
    "Portland": "Oregon", "Salem": "Oregon", "Eugene": "Oregon",
    "Milwaukee": "Wisconsin", "Madison": "Wisconsin", "Green Bay": "Wisconsin",
    "Minneapolis": "Minnesota", "St. Paul": "Minnesota", "Duluth": "Minnesota",
    "Kansas City": "Missouri", "St. Louis": "Missouri", "Springfield": "Missouri",
    "New Orleans": "Louisiana", "Baton Rouge": "Louisiana", "Shreveport": "Louisiana",
    "Louisville": "Kentucky", "Lexington": "Kentucky", "Bowling Green": "Kentucky",
    "Birmingham": "Alabama", "Montgomery": "Alabama", "Huntsville": "Alabama", "Mobile": "Alabama",
    "Oklahoma City": "Oklahoma", "Tulsa": "Oklahoma", "Norman": "Oklahoma",
    "Omaha": "Nebraska", "Lincoln": "Nebraska",
    "Charleston": "South Carolina", "Columbia": "South Carolina", "Greenville": "South Carolina",
    "Richmond": "Virginia", "Virginia Beach": "Virginia", "Norfolk": "Virginia",
    "Arlington": "Virginia", "Alexandria": "Virginia", "Fairfax": "Virginia",
    "Little Rock": "Arkansas", "Fayetteville": "Arkansas",
    "Des Moines": "Iowa", "Cedar Rapids": "Iowa", "Davenport": "Iowa",
    "Jackson": "Mississippi", "Hattiesburg": "Mississippi", "Biloxi": "Mississippi",
    "Hartford": "Connecticut", "New Haven": "Connecticut", "Stamford": "Connecticut", "Bridgeport": "Connecticut",
    "Newark": "New Jersey", "Jersey City": "New Jersey", "Trenton": "New Jersey", "Paterson": "New Jersey",
    "Camden": "New Jersey", "Elizabeth": "New Jersey",
    "Albuquerque": "New Mexico", "Santa Fe": "New Mexico", "Las Cruces": "New Mexico",
    "Honolulu": "Hawaii", "Boise": "Idaho", "Salt Lake City": "Utah", "Provo": "Utah",
    "Providence": "Rhode Island", "Wilmington": "Delaware",
    "Anchorage": "Alaska", "Billings": "Montana", "Cheyenne": "Wyoming",
    "Sioux Falls": "South Dakota", "Fargo": "North Dakota", "Burlington": "Vermont",
    "Wichita": "Kansas", "Topeka": "Kansas", "Overland Park": "Kansas",
}


def _extract_state(title: str) -> Optional[str]:
    # First try direct state name match
    for state in US_STATES:
        if re.search(rf"\b{re.escape(state)}\b", title, re.IGNORECASE):
            return state
    # Then try city/county name match
    for city, state in CITY_TO_STATE.items():
        if re.search(rf"\b{re.escape(city)}\b", title, re.IGNORECASE):
            return state
    return None


def _extract_threat_type(title: str) -> str:
    t = title.lower()
    if "bomb" in t and ("shoot" in t or "gun" in t):
        return "Bomb and Shooting"
    if "bomb" in t:
        return "Bomb"
    if "shoot" in t or "gun" in t:
        return "Shooting"
    if "threat" in t:
        return "Threat"
    if "lockdown" in t:
        return "Threat"  # lockdown usually implies threat
    return "General Threat"


def _extract_school_type(title: str) -> str:
    t = title.lower()
    if "elementary" in t:
        return "Elementary School"
    if "middle" in t or "junior high" in t:
        return "Middle School"
    if "high school" in t or "high" in t:
        return "High School"
    if "university" in t or "college" in t:
        return "University/College"
    if "school" in t:
        return ""
    return ""


def _is_likely_us(title: str) -> bool:
    """True if article appears to be about a US school threat."""
    for pat in NON_US_INDICATORS:
        if re.search(pat, title, re.IGNORECASE):
            return False
    t = title.lower()
    has_school = "school" in t or "campus" in t or "student" in t or "classroom" in t
    threat_terms = ["threat", "lockdown", "bomb", "shooting", "gun", "arrest",
                    "charged", "weapon", "evacuate", "evacuation", "swat", "police"]
    has_threat = any(x in t for x in threat_terms)
    # If both school and threat terms present, include
    if has_school and has_threat:
        return True
    # If has school + a US state/city, include (likely relevant from our search queries)
    if has_school and _extract_state(title):
        return True
    return False


def _is_duplicate_of_existing(news_title: str, existing_incidents: list[dict],
                               candidate_stub: dict = None) -> tuple[bool, float]:
    """Check if article matches an existing incident.

    Uses entity-based matching (school + state + date + type) with fuzzy matching
    when a candidate_stub is provided, falling back to headline-based matching.

    Returns (is_duplicate, confidence_score).
    """
    # Entity-based matching via dedup module
    if candidate_stub:
        try:
            from dedup import find_best_match, HIGH_CONFIDENCE_THRESHOLD, LOW_CONFIDENCE_THRESHOLD
            match = find_best_match(candidate_stub, existing_incidents)
            if match:
                score = match["scores"]["composite"]
                if score >= HIGH_CONFIDENCE_THRESHOLD:
                    return True, score
                if score >= LOW_CONFIDENCE_THRESHOLD:
                    return True, score  # Will be routed to review queue by caller
        except ImportError:
            pass  # Fall through to headline-based matching

    # Headline-based matching (original logic)
    new_words = _title_to_word_set(news_title)
    extracted_school = _extract_school(news_title)
    for inc in existing_incidents:
        school = str(inc.get("school") or "").strip()
        details = str(inc.get("incident_details") or "")
        if extracted_school and school:
            school_words = _title_to_word_set(school)
            if school_words and school_words <= new_words:
                combined = f"{school} {details}"
                existing_words = _title_to_word_set(combined)
                if _same_incident(new_words, existing_words, min_overlap=0.5):
                    return True, 0.9
        combined = f"{school} {details}"
        existing_words = _title_to_word_set(combined)
        if _same_incident(new_words, existing_words, min_overlap=0.8):
            return True, 0.85
    return False, 0.0


def article_to_stub(article: dict, next_id: int) -> dict:
    title = (article.get("title") or "").strip()
    url = (article.get("url") or "").strip()
    published = article.get("published") or ""
    snippet = _strip_html(article.get("snippet") or "")[:500]
    other_urls = article.get("other_sources") or []

    school = _extract_school(title)
    state = _extract_state(title)
    region = STATE_TO_REGION.get(state, "") if state else ""
    threat_type = _extract_threat_type(title)
    school_type = _extract_school_type(title)
    offense_date = _parse_published_to_date(published)
    details = snippet if snippet else title

    return {
        "id": next_id,
        "school": school or "",
        "school_type": school_type,
        "state": state or "",
        "region": region,
        "source": url,
        "offense_date": offense_date,
        "time": "Not Specified",
        "law_enforcement": "Unknown",
        "threat_type": threat_type,
        "conveyance": "Not Disclosed",
        "who_threatened": "School",
        "incident_details": details,
        "lockdown_type": "N/A",
        "classes_cancelled": "N/A",
        "precautions": "N/A",
        "weapons": "Unknown",
        "gender": "Unknown",
        "charged": "Unknown",
        "custody": "Unknown",
        "charges": "Unknown",
        "bond": "N/A",
        "additional_sources": ", ".join(other_urls) if other_urls else "",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Turn news feed into stub incidents")
    parser.add_argument("--merge", action="store_true", help="Append new stubs to school_threats_2026.json")
    parser.add_argument("--dry-run", action="store_true", help="Show stubs without writing files")
    args = parser.parse_args()

    if not NEWS_FEED.exists():
        print(f"Error: {NEWS_FEED} not found. Run fetch_news.py first.", file=sys.stderr)
        return 1

    data = json.loads(NEWS_FEED.read_text(encoding="utf-8"))
    articles = data.get("articles") or []

    existing: list[dict] = []
    max_id = 0
    if INCIDENTS_FILE.exists():
        existing = json.loads(INCIDENTS_FILE.read_text(encoding="utf-8"))
        max_id = max((inc.get("id") or 0 for inc in existing), default=0)

    stubs: list[dict] = []
    next_id = max_id + 1
    skipped_non_us = 0
    skipped_duplicate = 0
    sent_to_review = 0

    # Try to import dedup review queue support
    try:
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        from dedup import (find_best_match, load_review_queue, save_review_queue,
                           HIGH_CONFIDENCE_THRESHOLD, LOW_CONFIDENCE_THRESHOLD)
        has_dedup = True
        review_queue = load_review_queue()
        next_match_id = max((m.get("match_id", 0) for m in review_queue), default=0) + 1
    except ImportError:
        has_dedup = False
        review_queue = []
        next_match_id = 1

    for art in articles:
        title = (art.get("title") or "").strip()
        if not title:
            continue
        if not _is_likely_us(title):
            skipped_non_us += 1
            continue

        # Build stub early so entity-based dedup can use it
        stub = article_to_stub(art, next_id)

        is_dup, confidence = _is_duplicate_of_existing(title, existing, candidate_stub=stub)

        if is_dup:
            if has_dedup and LOW_CONFIDENCE_THRESHOLD <= confidence < HIGH_CONFIDENCE_THRESHOLD:
                # Route to review queue
                match = find_best_match(stub, existing)
                review_queue.append({
                    "match_id": next_match_id,
                    "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "candidate": {
                        "school": stub.get("school", ""),
                        "state": stub.get("state", ""),
                        "date": stub.get("offense_date", ""),
                        "threat_type": stub.get("threat_type", ""),
                        "details": stub.get("incident_details", "")[:200],
                    },
                    "match": match,
                    "confidence": confidence,
                    "decision": "pending_review",
                })
                next_match_id += 1
                sent_to_review += 1
            else:
                skipped_duplicate += 1
            continue

        stubs.append(stub)
        next_id += 1

    if has_dedup and review_queue:
        save_review_queue(review_queue)

    print(f"Processed {len(articles)} articles")
    print(f"  Skipped (non-US): {skipped_non_us}")
    print(f"  Skipped (duplicate, high confidence): {skipped_duplicate}")
    print(f"  Sent to review queue: {sent_to_review}")
    print(f"  New stubs: {len(stubs)}")

    if args.dry_run:
        for s in stubs[:5]:
            print(json.dumps(s, indent=2))
        if len(stubs) > 5:
            print(f"... and {len(stubs) - 5} more")
        return 0

    if not stubs:
        print("No new stubs to write.")
        return 0

    if args.merge:
        all_incidents = json.loads(INCIDENTS_FILE.read_text(encoding="utf-8"))
        all_incidents.extend(stubs)
        INCIDENTS_FILE.write_text(json.dumps(all_incidents, indent=2), encoding="utf-8")
        print(f"Merged {len(stubs)} stubs into {INCIDENTS_FILE}")
    else:
        out = {"generated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"), "count": len(stubs), "stubs": stubs}
        STUBS_OUTPUT.write_text(json.dumps(out, indent=2), encoding="utf-8")
        print(f"Wrote {len(stubs)} stubs to {STUBS_OUTPUT}")
        print("Review the file, then run with --merge to add to school_threats_2026.json")

    return 0


if __name__ == "__main__":
    sys.exit(main())
