#!/bin/bash
set -euo pipefail
cd /Users/robertschafer/cc/school-threats
pip install -r requirements.txt 2>/dev/null || true
echo "Agent memory environment ready"
