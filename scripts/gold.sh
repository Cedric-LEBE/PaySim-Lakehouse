#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
BANKING_SPARK_APP=src/banking/gold.py exec bash scripts/bronze.sh "$@"
