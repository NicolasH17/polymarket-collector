#!/usr/bin/env bash
#
# Continuous CLOB Collector Launcher
#
# Runs the collector with proper environment setup.
# Can be run directly or via launchd/systemd for persistent operation.
#
# Usage:
#   ./bin/run_collector.sh                    # Default: 5-min intervals
#   ./bin/run_collector.sh --interval 60      # 1-min intervals for testing
#

set -euo pipefail

# Get project root (parent of bin/)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

# Activate virtual environment
if [[ -d ".venv" ]]; then
    source .venv/bin/activate
else
    echo "Error: Virtual environment not found at $PROJECT_ROOT/.venv"
    exit 1
fi

# Create output directories
mkdir -p data/raw logs state

# Log startup
echo "============================================================"
echo "POLYMARKET THESIS DATA COLLECTOR"
echo "============================================================"
echo "Project root: $PROJECT_ROOT"
echo "Python: $(which python)"
echo "Started: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
echo "============================================================"

# Run collector with all arguments passed through
exec python scripts/continuous_collector.py \
    --config config/thesis_markets.json \
    --output-dir data/raw \
    --log-dir logs \
    --state-dir state \
    "$@"
