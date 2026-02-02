#!/usr/bin/env python3
"""
Fetch school-threat-related news from Google News RSS and optionally NewsAPI.
Outputs to data/news_feed.json for review; does not require manual data entry for discovery.

Usage:
  python scripts/fetch_news.py              # Recent articles
  python scripts/fetch_news.py --year 2025  # Fetch 2025 historical, merge into feed

Optional env:
  NEWS_API_KEY  - If set, also fetches from NewsAPI.org (free tier: 100 req/day).
  RSS_URL       - If set, also fetches from this RSS feed (e.g. IFTTT from Google Alerts).
"""

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import quote_plus, urlencode

try:
    import feedparser
    import requests
except ImportError:
    print("Missing dependencies. Run: pip install -r requirements.txt", file=sys.stderr)
    sys.exit(1)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_RAW = DATA_DIR / "news_feed.json"
FETCH_STATS_FILE = DATA_DIR / "fetch_stats.json"

# Search terms similar to what you might use in Google Alerts
GOOGLE_NEWS_QUERIES = [
    "school threat USA",
    "school lockdown USA",
    "bomb threat school",
    "school shooting threat",
]

# NewsAPI search query (single string)
NEWSAPI_QUERY = "school threat OR school lockdown OR bomb threat school"


# ---------------------------------------------------------------------------
# Source Plugin Architecture
# ---------------------------------------------------------------------------
class NewsSource:
    """Base class for news source plugins."""
    name: str = "unknown"
    requires_key: bool = False
    env_key: str = ""

    def is_enabled(self) -> bool:
        if self.requires_key:
            return bool(os.environ.get(self.env_key, "").strip())
        return True

    def fetch(self, year: Optional[int] = None) -> list[dict]:
        raise NotImplementedError

    def rate_limit_delay(self) -> float:
        """Seconds to wait between requests to respect rate limits."""
        return 0.0


import time


class RateLimiter:
    """Simple rate limiter that tracks last request time per source."""
    _last_request: dict[str, float] = {}

    @classmethod
    def wait(cls, source_name: str, delay: float) -> None:
        if delay <= 0:
            return
        last = cls._last_request.get(source_name, 0)
        elapsed = time.time() - last
        if elapsed < delay:
            time.sleep(delay - elapsed)
        cls._last_request[source_name] = time.time()

# ---------------------------------------------------------------------------
# Google News RSS (no API key)
# ---------------------------------------------------------------------------
def fetch_google_news_rss(year: Optional[int] = None) -> list[dict]:
    """Fetch articles from Google News RSS search. No API key required.
    If year is set (e.g. 2025), append year to queries to bias toward that year."""
    results = []
    seen_urls = set()
    suffix = f" {year}" if year else ""

    for q in GOOGLE_NEWS_QUERIES:
        query = q + suffix
        # Google News RSS search URL
        params = {"q": query, "hl": "en-US", "gl": "US", "ceid": "US:en"}
        url = "https://news.google.com/rss/search?" + urlencode(params)
        try:
            feed = feedparser.parse(url, request_headers={"User-Agent": "SchoolThreatsBot/1.0"})
        except Exception as e:
            print(f"Google News RSS ({q!r}): {e}", file=sys.stderr)
            continue

        for entry in feed.entries:
            link = getattr(entry, "link", None) or (entry.get("link") if isinstance(entry, dict) else None)
            if not link or link in seen_urls:
                continue
            seen_urls.add(link)

            # Google RSS often uses redirect URLs; prefer id if it's a real URL
            raw_id = getattr(entry, "id", "") or (entry.get("id") if isinstance(entry, dict) else "")
            if raw_id and raw_id.startswith("http"):
                link = raw_id

            title = getattr(entry, "title", "") or (entry.get("title") or "")
            published = getattr(entry, "published", "") or (entry.get("published") or "")
            # Use published_parsed if available (time tuple), else keep raw string
            try:
                parsed = getattr(entry, "published_parsed", None) or entry.get("published_parsed")
                if parsed:
                    from time import struct_time
                    if isinstance(parsed, struct_time):
                        from time import mktime
                        from datetime import datetime as dt_class
                        published_iso = dt_class.utcfromtimestamp(mktime(parsed)).strftime("%Y-%m-%dT%H:%M:%SZ")
                    else:
                        published_iso = published or ""
                else:
                    published_iso = published or ""
            except Exception:
                published_iso = published or ""

            results.append({
                "title": title,
                "url": link,
                "published": published_iso,
                "source": "Google News RSS",
                "snippet": (getattr(entry, "summary", "") or entry.get("summary") or "")[:500],
            })
        print(f"Google News RSS ({query!r}): {len(feed.entries)} items")
    return results


# ---------------------------------------------------------------------------
# NewsAPI (optional, requires API key)
# ---------------------------------------------------------------------------
def fetch_newsapi(api_key: str, from_date: Optional[str] = None, to_date: Optional[str] = None) -> list[dict]:
    """Fetch from NewsAPI.org. Free tier: 100 req/day.
    Use from_date/to_date (YYYY-MM-DD) for historical articles."""
    results = []
    url = "https://newsapi.org/v2/everything"
    params = {
        "q": NEWSAPI_QUERY,
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": 100,
        "apiKey": api_key,
    }
    if from_date:
        params["from"] = from_date
    if to_date:
        params["to"] = to_date
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"NewsAPI: {e}", file=sys.stderr)
        return results

    for art in data.get("articles") or []:
        link = (art.get("url") or "").strip()
        if not link or link == "https://removed.com":
            continue
        results.append({
            "title": (art.get("title") or "").strip(),
            "url": link,
            "published": (art.get("publishedAt") or "")[:19].replace("T", " "),
            "source": art.get("source", {}).get("name", "NewsAPI"),
            "snippet": (art.get("description") or "")[:500],
        })
    print(f"NewsAPI: {len(results)} items")
    return results


# ---------------------------------------------------------------------------
# Generic RSS (e.g. IFTTT feed from Google Alerts)
# ---------------------------------------------------------------------------
def fetch_rss_url(rss_url: str) -> list[dict]:
    """Fetch entries from a single RSS/Atom feed."""
    results = []
    try:
        feed = feedparser.parse(rss_url, request_headers={"User-Agent": "SchoolThreatsBot/1.0"})
    except Exception as e:
        print(f"RSS ({rss_url[:50]}...): {e}", file=sys.stderr)
        return results

    for entry in feed.entries:
        link = getattr(entry, "link", None) or entry.get("link")
        if not link:
            continue
        published = getattr(entry, "published", "") or entry.get("published", "")
        try:
            parsed = getattr(entry, "published_parsed", None) or entry.get("published_parsed")
            if parsed:
                from time import struct_time, mktime
                from datetime import datetime as dt_class
                if isinstance(parsed, struct_time):
                    published_iso = dt_class.utcfromtimestamp(mktime(parsed)).strftime("%Y-%m-%dT%H:%M:%SZ")
                else:
                    published_iso = published or ""
            else:
                published_iso = published or ""
        except Exception:
            published_iso = published or ""

        results.append({
            "title": getattr(entry, "title", "") or entry.get("title", ""),
            "url": link,
            "published": published_iso,
            "source": "RSS",
            "snippet": (getattr(entry, "summary", "") or entry.get("summary", ""))[:500],
        })
    print(f"RSS: {len(results)} items")
    return results


# ---------------------------------------------------------------------------
# GNews API (free tier: 100 req/day, no credit card)
# ---------------------------------------------------------------------------
def fetch_gnews(api_key: str, year: Optional[int] = None) -> list[dict]:
    """Fetch from GNews.io API. Free tier: 100 req/day, 10 articles/request."""
    results = []
    url = "https://gnews.io/api/v4/search"
    queries = ["school threat", "school bomb threat", "school lockdown"]
    for q in queries:
        RateLimiter.wait("gnews", 1.0)  # 1 req/sec to be safe
        params = {
            "q": q,
            "lang": "en",
            "country": "us",
            "max": 10,
            "apikey": api_key,
        }
        if year:
            params["from"] = f"{year}-01-01T00:00:00Z"
            params["to"] = f"{year}-12-31T23:59:59Z"
        try:
            r = requests.get(url, params=params, timeout=15)
            if r.status_code == 429:
                print(f"GNews: rate limited on query {q!r}, skipping remaining", file=sys.stderr)
                break
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print(f"GNews ({q!r}): {e}", file=sys.stderr)
            continue
        for art in data.get("articles") or []:
            link = (art.get("url") or "").strip()
            if not link:
                continue
            results.append({
                "title": (art.get("title") or "").strip(),
                "url": link,
                "published": (art.get("publishedAt") or "")[:19],
                "source": art.get("source", {}).get("name", "GNews"),
                "snippet": (art.get("description") or "")[:500],
            })
    print(f"GNews: {len(results)} items")
    return results


# ---------------------------------------------------------------------------
# Source Registry
# ---------------------------------------------------------------------------
_SOURCE_STATS: dict[str, dict] = {}


def record_source_stats(source_name: str, count: int) -> None:
    _SOURCE_STATS[source_name] = {
        "articles_fetched": count,
        "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def save_fetch_stats() -> None:
    """Save per-source fetch statistics."""
    existing = {}
    if FETCH_STATS_FILE.exists():
        try:
            existing = json.loads(FETCH_STATS_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    runs = existing.get("runs", [])
    runs.append({
        "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "sources": _SOURCE_STATS,
    })
    # Keep last 100 runs
    existing["runs"] = runs[-100:]
    FETCH_STATS_FILE.write_text(json.dumps(existing, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# Dedupe by URL, then by incident (so one count per threat across outlets)
# ---------------------------------------------------------------------------

# Words we strip from titles when comparing (so "Bomb threat at Lincoln High" matches "Lincoln High bomb threat")
_STOP_WORDS = frozenset(
    "the a an at to for of in on and or but is are was were be been by with from as into through during after before said reported".split()
)


def _title_to_word_set(title: str) -> set[str]:
    """Normalize title to a set of significant words for comparing incidents."""
    if not title:
        return set()
    import re
    text = re.sub(r"[^\w\s]", " ", title.lower())
    words = {w for w in text.split() if w and w not in _STOP_WORDS and len(w) > 1}
    return words


def _same_incident(words_a: set[str], words_b: set[str], min_overlap: float = 0.75) -> bool:
    """True if the two title word sets likely refer to the same incident (e.g. same school + threat)."""
    if not words_a or not words_b:
        return False
    overlap = len(words_a & words_b)
    # Require most of the smaller set to appear in the other (so one headline isn't a subset of unrelated one)
    return overlap / min(len(words_a), len(words_b)) >= min_overlap


def merge_and_dedupe(all_items: list[dict]) -> tuple[int, list[dict]]:
    """First dedupe by URL, then by incident so multiple outlets covering same threat count once.
    Returns (count_after_url_dedup, list of unique incidents with other_sources when multiple outlets)."""
    # 1) One entry per URL (earliest publish wins)
    by_url: dict[str, dict] = {}
    for item in all_items:
        url = (item.get("url") or "").strip()
        if not url:
            continue
        if url not in by_url or (item.get("published") or "") < (by_url[url].get("published") or ""):
            by_url[url] = {**item, "other_sources": []}
    items = list(by_url.values())
    url_deduped_count = len(items)

    # 2) Group by same incident (similar titles = same threat, different outlets)
    word_sets = [_title_to_word_set(i.get("title") or "") for i in items]
    n = len(items)
    parent = list(range(n))

    def find(x: int) -> int:
        if parent[x] != x:
            parent[x] = find(parent[x])
        return parent[x]

    def union(x: int, y: int) -> None:
        px, py = find(x), find(y)
        if px != py and _same_incident(word_sets[x], word_sets[y]):
            parent[py] = px

    for i in range(n):
        for j in range(i + 1, n):
            union(i, j)

    # 3) One row per incident: keep earliest article, list other URLs as other_sources
    groups: dict[int, list[dict]] = {}
    for i in range(n):
        root = find(i)
        groups.setdefault(root, []).append(items[i])

    result = []
    for group in groups.values():
        # Sort by published date, earliest first; that one becomes the main entry
        group.sort(key=lambda x: (x.get("published") or ""))
        primary = {**group[0]}
        other_urls = [x.get("url") for x in group[1:] if x.get("url")]
        primary["other_sources"] = other_urls
        result.append(primary)
    result.sort(key=lambda x: (x.get("published") or ""), reverse=True)
    return (url_deduped_count, result)


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch school-threat news")
    parser.add_argument("--year", type=int, help="Fetch historical data for year (e.g. 2025); merges into existing feed")
    args = parser.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    from_date = to_date = None
    year = args.year
    if year:
        from_date = f"{year}-01-01"
        to_date = f"{year}-12-31"
        print(f"Fetching historical data for {year} ({from_date} to {to_date})")

    all_items: list[dict] = []

    # 1) Google News RSS (no key)
    google_items = fetch_google_news_rss(year=year)
    all_items.extend(google_items)
    record_source_stats("google_news_rss", len(google_items))

    # 2) NewsAPI (optional)
    api_key = os.environ.get("NEWS_API_KEY", "").strip()
    if api_key:
        newsapi_items = fetch_newsapi(api_key, from_date=from_date, to_date=to_date)
        all_items.extend(newsapi_items)
        record_source_stats("newsapi", len(newsapi_items))
    else:
        print("NewsAPI: skipped (set NEWS_API_KEY to enable)")

    # 3) GNews API (optional, free tier)
    gnews_key = os.environ.get("GNEWS_API_KEY", "").strip()
    if gnews_key:
        gnews_items = fetch_gnews(gnews_key, year=year)
        all_items.extend(gnews_items)
        record_source_stats("gnews", len(gnews_items))
    else:
        print("GNews: skipped (set GNEWS_API_KEY to enable)")

    # 4) Custom RSS URL (optional, e.g. IFTTT from Google Alerts)
    rss_url = os.environ.get("RSS_URL", "").strip()
    if rss_url:
        rss_items = fetch_rss_url(rss_url)
        all_items.extend(rss_items)
        record_source_stats("custom_rss", len(rss_items))

    # If fetching historical year, merge with existing feed
    if year and OUTPUT_RAW.exists():
        existing = json.loads(OUTPUT_RAW.read_text(encoding="utf-8"))
        prev_articles = existing.get("articles") or []
        prev_urls = {a.get("url", "").strip() for a in prev_articles if a.get("url")}
        added = 0
        for item in all_items:
            url = (item.get("url") or "").strip()
            if url and url not in prev_urls:
                prev_articles.append(item)
                prev_urls.add(url)
                added += 1
        all_items = prev_articles
        print(f"Merged {added} new {year} articles into existing feed ({len(prev_articles)} total before dedup)")

    url_deduped, merged = merge_and_dedupe(all_items)
    incident_count = len(merged)

    out = {
        "fetched_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "count": incident_count,
        "articles_after_url_dedup": url_deduped,
        "articles": merged,
    }
    OUTPUT_RAW.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(f"After removing duplicate URLs: {url_deduped} articles")
    print(f"After grouping same incident across outlets: {incident_count} unique incidents")
    print(f"Wrote {incident_count} incidents to {OUTPUT_RAW}")

    # Save per-source fetch statistics
    save_fetch_stats()
    print(f"Fetch stats logged to {FETCH_STATS_FILE}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
