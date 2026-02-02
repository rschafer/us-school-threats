#!/usr/bin/env python3
"""
Entity-based deduplication for school threat incidents.

Matches articles by entity tuple (school + state + date + threat_type) with
fuzzy matching for school names, assigns confidence scores, and routes
low-confidence matches to a review queue.

Usage:
  python scripts/dedup.py check                    # Check news stubs against existing incidents
  python scripts/dedup.py check --threshold 0.6    # Custom confidence threshold
  python scripts/dedup.py review                   # Show pending review queue
  python scripts/dedup.py accept <match_id>        # Accept a match (mark as duplicate)
  python scripts/dedup.py reject <match_id>        # Reject a match (keep as new)
  python scripts/dedup.py stats                    # Show dedup statistics
"""

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

try:
    from rapidfuzz import fuzz
except ImportError:
    print("Error: rapidfuzz required. Install with: pip install rapidfuzz", file=sys.stderr)
    sys.exit(1)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
INCIDENTS_FILE = DATA_DIR / "school_threats_2026.json"
STUBS_FILE = DATA_DIR / "stub_incidents_from_news.json"
REVIEW_QUEUE_FILE = DATA_DIR / "dedup_review_queue.json"
DEDUP_LOG_FILE = DATA_DIR / "dedup_log.json"

# Thresholds
HIGH_CONFIDENCE_THRESHOLD = 0.85   # Auto-mark as duplicate
LOW_CONFIDENCE_THRESHOLD = 0.50    # Below this = definitely new
# Between LOW and HIGH = goes to review queue

# Weights for composite score
WEIGHTS = {
    "school_name": 0.40,
    "state": 0.20,
    "date": 0.20,
    "threat_type": 0.20,
}


def normalize_school_name(name: str) -> str:
    """Normalize school name for comparison."""
    if not name:
        return ""
    s = name.strip().lower()
    # Remove common suffixes/prefixes that vary
    s = re.sub(r"\b(the|of|at)\b", " ", s)
    # Normalize school type words
    s = re.sub(r"\bjr\.?\b", "junior", s)
    s = re.sub(r"\bsr\.?\b", "senior", s)
    s = re.sub(r"\belem\.?\b", "elementary", s)
    s = re.sub(r"\bhs\b", "high school", s)
    s = re.sub(r"\bms\b", "middle school", s)
    s = re.sub(r"\bes\b", "elementary school", s)
    # Collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()
    return s


def school_name_score(name_a: str, name_b: str) -> float:
    """Fuzzy match score for two school names (0.0 - 1.0)."""
    a = normalize_school_name(name_a)
    b = normalize_school_name(name_b)
    if not a or not b:
        return 0.0
    # Exact match after normalization
    if a == b:
        return 1.0
    # Use token sort ratio (handles word reordering)
    token_sort = fuzz.token_sort_ratio(a, b) / 100.0
    # Use partial ratio (handles substring matches like "Tucker HS" vs "Tucker High School")
    partial = fuzz.partial_ratio(a, b) / 100.0
    # Take the higher score
    return max(token_sort, partial)


def normalize_date(date_str: str) -> Optional[str]:
    """Normalize date to 'DD-Mon' format for comparison."""
    if not date_str or date_str in ("Not Specified", "N/A", ""):
        return None
    s = date_str.strip()
    # Already in DD-Mon format (e.g., "5-Jan")
    m = re.match(r"^(\d{1,2})-(\w{3})$", s)
    if m:
        return f"{int(m.group(1)):02d}-{m.group(2)[:3].capitalize()}"
    # Try ISO format
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y"):
        try:
            dt = datetime.strptime(s, fmt)
            return f"{dt.day:02d}-{dt.strftime('%b')}"
        except (ValueError, TypeError):
            continue
    return None


def date_score(date_a: str, date_b: str) -> float:
    """Score for date proximity (0.0 - 1.0)."""
    norm_a = normalize_date(date_a)
    norm_b = normalize_date(date_b)
    if norm_a is None or norm_b is None:
        return 0.3  # Unknown dates get neutral score
    if norm_a == norm_b:
        return 1.0
    # Try to parse and compare day proximity
    try:
        months = {"Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
                  "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12}
        da, ma = int(norm_a.split("-")[0]), months.get(norm_a.split("-")[1], 0)
        db, mb = int(norm_b.split("-")[0]), months.get(norm_b.split("-")[1], 0)
        if ma and mb:
            diff = abs((ma * 31 + da) - (mb * 31 + db))
            if diff <= 1:
                return 0.9  # Same or adjacent day
            if diff <= 3:
                return 0.6  # Within 3 days
            if diff <= 7:
                return 0.3  # Within a week
            return 0.0
    except (ValueError, IndexError):
        pass
    return 0.0


THREAT_TYPE_GROUPS = {
    "bomb": {"bomb", "bomb threat", "explosive"},
    "shooting": {"shooting", "gun", "firearm", "active shooter"},
    "threat": {"threat", "threats", "general threat"},
}


def normalize_threat_type(tt: str) -> str:
    t = tt.strip().lower()
    for group, keywords in THREAT_TYPE_GROUPS.items():
        if t in keywords or any(k in t for k in keywords):
            return group
    return t


def threat_type_score(type_a: str, type_b: str) -> float:
    """Score for threat type match (0.0 - 1.0)."""
    a = normalize_threat_type(type_a)
    b = normalize_threat_type(type_b)
    if not a or not b:
        return 0.3
    if a == b:
        return 1.0
    # "bomb and shooting" should partially match both
    if "bomb" in a and "shooting" in a:
        if b in ("bomb", "shooting"):
            return 0.7
    if "bomb" in b and "shooting" in b:
        if a in ("bomb", "shooting"):
            return 0.7
    return 0.0


def state_score(state_a: str, state_b: str) -> float:
    """Score for state match (0.0 or 1.0)."""
    a = (state_a or "").strip().lower()
    b = (state_b or "").strip().lower()
    if not a or not b:
        return 0.3  # Unknown state gets neutral score
    return 1.0 if a == b else 0.0


def compute_match_score(incident_a: dict, incident_b: dict) -> dict:
    """Compute composite match score between two incidents."""
    scores = {
        "school_name": school_name_score(
            incident_a.get("school", ""), incident_b.get("school", "")
        ),
        "state": state_score(
            incident_a.get("state", ""), incident_b.get("state", "")
        ),
        "date": date_score(
            incident_a.get("offense_date", ""), incident_b.get("offense_date", "")
        ),
        "threat_type": threat_type_score(
            incident_a.get("threat_type", ""), incident_b.get("threat_type", "")
        ),
    }
    composite = sum(scores[k] * WEIGHTS[k] for k in WEIGHTS)
    return {"component_scores": scores, "composite": round(composite, 3)}


def find_best_match(candidate: dict, existing: list[dict]) -> Optional[dict]:
    """Find the best matching existing incident for a candidate."""
    best = None
    best_score = 0.0
    for inc in existing:
        result = compute_match_score(candidate, inc)
        if result["composite"] > best_score:
            best_score = result["composite"]
            best = {
                "existing_id": inc.get("id"),
                "existing_school": inc.get("school", ""),
                "existing_state": inc.get("state", ""),
                "existing_date": inc.get("offense_date", ""),
                "scores": result,
            }
    return best


def load_review_queue() -> list[dict]:
    if REVIEW_QUEUE_FILE.exists():
        return json.loads(REVIEW_QUEUE_FILE.read_text(encoding="utf-8"))
    return []


def save_review_queue(queue: list[dict]) -> None:
    REVIEW_QUEUE_FILE.write_text(json.dumps(queue, indent=2), encoding="utf-8")


def load_dedup_log() -> list[dict]:
    if DEDUP_LOG_FILE.exists():
        return json.loads(DEDUP_LOG_FILE.read_text(encoding="utf-8"))
    return []


def save_dedup_log(log: list[dict]) -> None:
    DEDUP_LOG_FILE.write_text(json.dumps(log, indent=2), encoding="utf-8")


def cmd_check(args) -> int:
    """Check stubs against existing incidents."""
    if not STUBS_FILE.exists():
        print(f"Error: {STUBS_FILE} not found. Run news_to_stubs.py first.", file=sys.stderr)
        return 1

    stubs_data = json.loads(STUBS_FILE.read_text(encoding="utf-8"))
    stubs = stubs_data.get("stubs", [])

    existing = []
    if INCIDENTS_FILE.exists():
        existing = json.loads(INCIDENTS_FILE.read_text(encoding="utf-8"))

    if not stubs:
        print("No stubs to check.")
        return 0

    high_conf = args.threshold or HIGH_CONFIDENCE_THRESHOLD
    low_conf = LOW_CONFIDENCE_THRESHOLD

    review_queue = load_review_queue()
    dedup_log = load_dedup_log()
    next_match_id = max((m.get("match_id", 0) for m in review_queue), default=0) + 1

    auto_dupes = 0
    new_incidents = 0
    for_review = 0

    for stub in stubs:
        match = find_best_match(stub, existing)
        if not match:
            new_incidents += 1
            continue

        score = match["scores"]["composite"]
        entry = {
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
            "confidence": score,
        }

        if score >= high_conf:
            entry["decision"] = "auto_duplicate"
            dedup_log.append(entry)
            auto_dupes += 1
        elif score >= low_conf:
            entry["decision"] = "pending_review"
            review_queue.append(entry)
            for_review += 1
        else:
            new_incidents += 1

        next_match_id += 1

    save_review_queue(review_queue)
    save_dedup_log(dedup_log)

    print(f"Checked {len(stubs)} stubs against {len(existing)} existing incidents")
    print(f"  Auto-detected duplicates (>={high_conf:.0%}): {auto_dupes}")
    print(f"  Sent to review queue ({low_conf:.0%}-{high_conf:.0%}): {for_review}")
    print(f"  New incidents (<{low_conf:.0%}): {new_incidents}")
    print(f"\nReview queue: {len(review_queue)} pending items")
    return 0


def cmd_review(args) -> int:
    """Show pending review queue."""
    queue = load_review_queue()
    pending = [m for m in queue if m.get("decision") == "pending_review"]
    if not pending:
        print("Review queue is empty.")
        return 0

    print(f"Pending review: {len(pending)} items\n")
    for item in pending:
        cand = item["candidate"]
        match = item["match"]
        print(f"  Match ID: {item['match_id']}")
        print(f"  Confidence: {item['confidence']:.1%}")
        print(f"  Candidate: {cand['school']}, {cand['state']} ({cand['date']}) - {cand['threat_type']}")
        print(f"  Existing:  {match['existing_school']}, {match['existing_state']} ({match['existing_date']}) [ID {match['existing_id']}]")
        scores = match["scores"]["component_scores"]
        print(f"  Scores: school={scores['school_name']:.2f} state={scores['state']:.2f} date={scores['date']:.2f} type={scores['threat_type']:.2f}")
        print()
    return 0


def cmd_accept(args) -> int:
    """Accept a match as duplicate."""
    match_id = args.match_id
    queue = load_review_queue()
    dedup_log = load_dedup_log()

    found = False
    for item in queue:
        if item["match_id"] == match_id and item.get("decision") == "pending_review":
            item["decision"] = "accepted_duplicate"
            item["reviewed_at"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
            dedup_log.append(item)
            found = True
            break

    if not found:
        print(f"Match ID {match_id} not found in pending review queue.", file=sys.stderr)
        return 1

    # Remove from queue
    queue = [m for m in queue if not (m["match_id"] == match_id and m.get("decision") == "accepted_duplicate")]
    save_review_queue(queue)
    save_dedup_log(dedup_log)
    print(f"Match {match_id} accepted as duplicate.")
    return 0


def cmd_reject(args) -> int:
    """Reject a match (keep as new incident)."""
    match_id = args.match_id
    queue = load_review_queue()
    dedup_log = load_dedup_log()

    found = False
    for item in queue:
        if item["match_id"] == match_id and item.get("decision") == "pending_review":
            item["decision"] = "rejected_not_duplicate"
            item["reviewed_at"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
            dedup_log.append(item)
            found = True
            break

    if not found:
        print(f"Match ID {match_id} not found in pending review queue.", file=sys.stderr)
        return 1

    queue = [m for m in queue if not (m["match_id"] == match_id and m.get("decision") == "rejected_not_duplicate")]
    save_review_queue(queue)
    save_dedup_log(dedup_log)
    print(f"Match {match_id} rejected — will be kept as new incident.")
    return 0


def cmd_stats(args) -> int:
    """Show dedup statistics."""
    log = load_dedup_log()
    queue = load_review_queue()
    pending = [m for m in queue if m.get("decision") == "pending_review"]

    auto_dupes = sum(1 for m in log if m.get("decision") == "auto_duplicate")
    accepted = sum(1 for m in log if m.get("decision") == "accepted_duplicate")
    rejected = sum(1 for m in log if m.get("decision") == "rejected_not_duplicate")
    total_reviewed = accepted + rejected
    false_positive_rate = rejected / total_reviewed if total_reviewed else 0

    print("Dedup Statistics")
    print(f"  Auto-detected duplicates: {auto_dupes}")
    print(f"  Manually accepted duplicates: {accepted}")
    print(f"  Manually rejected (not duplicate): {rejected}")
    print(f"  Pending review: {len(pending)}")
    print(f"  False positive rate: {false_positive_rate:.1%}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Entity-based deduplication for school threats")
    sub = parser.add_subparsers(dest="command")

    check_p = sub.add_parser("check", help="Check stubs against existing incidents")
    check_p.add_argument("--threshold", type=float, help="High confidence threshold (default 0.85)")

    sub.add_parser("review", help="Show pending review queue")

    accept_p = sub.add_parser("accept", help="Accept a match as duplicate")
    accept_p.add_argument("match_id", type=int, help="Match ID to accept")

    reject_p = sub.add_parser("reject", help="Reject a match")
    reject_p.add_argument("match_id", type=int, help="Match ID to reject")

    sub.add_parser("stats", help="Show dedup statistics")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        return 1

    cmds = {"check": cmd_check, "review": cmd_review, "accept": cmd_accept, "reject": cmd_reject, "stats": cmd_stats}
    return cmds[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
