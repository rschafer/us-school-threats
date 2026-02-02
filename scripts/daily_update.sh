#!/bin/bash
# Daily pipeline: fetch news → generate stubs → merge into incidents
# Run via cron or launchd for automatic daily updates.

cd "$(dirname "$0")/.." || exit 1

TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
echo "$TIMESTAMP Starting daily update"

mkdir -p logs

python3 scripts/fetch_news.py || { echo "fetch_news failed"; echo "{\"timestamp\": \"$TIMESTAMP\", \"status\": \"failed\", \"step\": \"fetch_news\"}" >> logs/fetch_log.jsonl; exit 1; }
python3 scripts/news_to_stubs.py || { echo "news_to_stubs failed"; echo "{\"timestamp\": \"$TIMESTAMP\", \"status\": \"failed\", \"step\": \"news_to_stubs\"}" >> logs/fetch_log.jsonl; exit 1; }
python3 scripts/news_to_stubs.py --merge || { echo "merge failed"; echo "{\"timestamp\": \"$TIMESTAMP\", \"status\": \"failed\", \"step\": \"merge\"}" >> logs/fetch_log.jsonl; exit 1; }

ARTICLE_COUNT=$(python3 -c "import json; print(len(json.load(open('data/news_feed.json'))))" 2>/dev/null || echo 0)
STUB_COUNT=$(python3 -c "import json; print(len(json.load(open('data/stub_incidents_from_news.json'))))" 2>/dev/null || echo 0)

echo "{\"timestamp\": \"$TIMESTAMP\", \"articles\": $ARTICLE_COUNT, \"stubs\": $STUB_COUNT, \"status\": \"success\"}" >> logs/fetch_log.jsonl

echo "$TIMESTAMP Daily update complete — $ARTICLE_COUNT articles, $STUB_COUNT stubs"
