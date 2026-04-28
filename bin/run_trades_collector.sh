#!/usr/bin/env bash
#
# Trades Collector Launcher
#
# Runs the trades collector alongside the order book collector.
# Uses the same market config but writes to data/raw/trades/.
#
# Usage:
#   ./bin/run_trades_collector.sh                    # Default: 5-min intervals
#   ./bin/run_trades_collector.sh --interval 60      # 1-min intervals for testing
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
mkdir -p data/raw/trades logs state

# Log startup
echo "============================================================"
echo "POLYMARKET THESIS TRADES COLLECTOR"
echo "============================================================"
echo "Project root: $PROJECT_ROOT"
echo "Python: $(which python)"
echo "Started: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
echo "============================================================"

# Run trades collector with all arguments passed through
exec python scripts/trades_collector.py \
    --config config/thesis_markets.json \
    --output-dir data/raw/trades \
    --log-dir logs \
    --state-dir state \
    "$@"
