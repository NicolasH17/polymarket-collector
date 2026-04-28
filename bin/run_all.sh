#!/usr/bin/env bash
#
# Combined Launcher — Order Book + Trades Collectors
#
# Starts both collectors as child processes. Ctrl-C (SIGINT) or SIGTERM
# cleanly shuts down both. Suitable for direct invocation, systemd, or
# running inside a tmux/screen session on a VM.
#
# Usage:
#   ./bin/run_all.sh                     # Default intervals (5 min each)
#   ./bin/run_all.sh --trades-interval 120   # Override trades interval
#
# Logs:     logs/collector_YYYYMMDD.log  (order books)
#           logs/trades_collector_YYYYMMDD.log  (trades)
# Data:     data/raw/YYYYMMDD/*.parquet  (order books)
#           data/raw/trades/YYYYMMDD/*.parquet  (trades)
# State:    state/trades_watermarks.json

set -uo pipefail

# ── Resolve project root ─────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

# ── Virtual environment ──────────────────────────────────────────────
if [[ -d ".venv" ]]; then
    source .venv/bin/activate
else
    echo "Error: .venv not found at $PROJECT_ROOT/.venv"
    exit 1
fi

# ── Ensure directories exist ─────────────────────────────────────────
mkdir -p data/raw data/raw/trades logs state

# ── Parse optional overrides ─────────────────────────────────────────
TRADES_EXTRA_ARGS=()
CLOB_EXTRA_ARGS=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --trades-interval)
            TRADES_EXTRA_ARGS+=("--interval" "$2"); shift 2 ;;
        --clob-interval)
            CLOB_EXTRA_ARGS+=("--interval" "$2"); shift 2 ;;
        *)
            echo "Unknown option: $1"; exit 1 ;;
    esac
done

# ── Banner ────────────────────────────────────────────────────────────
echo "============================================================"
echo "POLYMARKET THESIS — COMBINED DATA COLLECTION"
echo "============================================================"
echo "Project root:  $PROJECT_ROOT"
echo "Python:        $(which python)"
echo "Started:       $(date -u +'%Y-%m-%dT%H:%M:%SZ')"
echo "============================================================"

# ── Track child PIDs for cleanup ──────────────────────────────────────
CLOB_PID=""
TRADES_PID=""

cleanup() {
    echo ""
    echo "[run_all] Shutting down collectors..."
    [[ -n "$CLOB_PID"   ]] && kill "$CLOB_PID"   2>/dev/null
    [[ -n "$TRADES_PID" ]] && kill "$TRADES_PID" 2>/dev/null
    wait 2>/dev/null
    echo "[run_all] All collectors stopped."
}
trap cleanup INT TERM

# ── Start Order Book collector ────────────────────────────────────────
echo "[run_all] Starting order book collector..."
python scripts/continuous_collector.py \
    --config config/thesis_markets.json \
    --output-dir data/raw \
    --log-dir logs \
    "${CLOB_EXTRA_ARGS[@]+"${CLOB_EXTRA_ARGS[@]}"}" &
CLOB_PID=$!

# ── Start Trades collector ────────────────────────────────────────────
echo "[run_all] Starting trades collector..."
python scripts/trades_collector.py \
    --config config/thesis_markets.json \
    --output-dir data/raw/trades \
    --log-dir logs \
    --state-dir state \
    "${TRADES_EXTRA_ARGS[@]+"${TRADES_EXTRA_ARGS[@]}"}" &
TRADES_PID=$!

echo "[run_all] Order book PID: $CLOB_PID"
echo "[run_all] Trades PID:     $TRADES_PID"
echo "[run_all] Press Ctrl-C to stop both."
echo ""

# ── Wait for both children (Ctrl-C triggers the trap) ─────────────────
wait
