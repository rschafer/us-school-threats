# Data Rules

- Main dataset: data/school_threats_2026.json (also school_threats_clean.json for 2025)
- 16 fields per incident (see docs/prd.md Section 6 for full schema)
- Normalized fields: threat_type_normalized, school_type_normalized, conveyance_normalized
- News fetch output: data/news_feed.json
- Stub output: data/stub_incidents_from_news.json
- fetch_news.py: Google News RSS (no key), optional NewsAPI, optional custom RSS
- news_to_stubs.py: --dry-run and --merge modes
- Dedup currently headline-based only
