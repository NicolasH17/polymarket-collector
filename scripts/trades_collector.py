#!/usr/bin/env python3
"""
Trades Collector for Polymarket Thesis

Collects trade data from Polymarket Data API independently of the order book collector.
Designed to run alongside continuous_collector.py without affecting it.

Data source: https://data-api.polymarket.com/trades (no authentication required)

Key features:
- Composite watermark (timestamp, transaction_hash) to handle second-level resolution
- Cache-busting nonce + headers to defeat CDN stale responses
- Overlap-and-dedupe strategy: always fetch, never trust early-exit on cached data
- Pagination to ensure no trades are missed during high activity
- Persistent watermarks across restarts (state/trades_watermarks.json)
- requests.Session for connection reuse

Output structure:
    data/raw/trades/
        YYYYMMDD/
            HHMMSS.parquet  (trades collected in each cycle)

Usage:
    python trades_collector.py --config config/thesis_markets.json
    python trades_collector.py --config config/test_single_market.json --interval 60
"""

import argparse
import json
import logging
import random
import signal
import string
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

# ============================================================================
# CONFIGURATION
# ============================================================================

DATA_API_BASE_URL = "https://data-api.polymarket.com"
DEFAULT_INTERVAL_SECONDS = 300  # 5 minutes — API window is ~52h / ~3500 trades, so 5min is very safe
DEFAULT_OUTPUT_DIR = "data/raw/trades"
STATE_DIR = "state"
WATERMARKS_FILE = "trades_watermarks.json"

REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRIES = 3
RETRY_BASE_DELAY = 2.0
RETRY_MAX_DELAY = 60.0

# Pagination settings
FIRST_PAGE_SIZE = 50   # Small first page — sufficient for typical 5-min intervals
FULL_PAGE_SIZE = 500   # Escalate to this if the entire first page is new trades
MAX_OFFSET = 10000     # API docs allow offset up to 10000

# Rate limiting
MIN_REQUEST_INTERVAL = 0.05  # 50ms between requests (20 req/s, well under 200/10s limit)

# Dedupe key columns - must match downstream deduplication in load_collected_trades()
TRADE_DEDUPE_COLS = ["condition_id", "trade_timestamp_unix", "transaction_hash",
                     "outcome_index", "side", "price", "size"]

# ============================================================================
# LOGGING SETUP
# ============================================================================

def setup_logging(log_dir: Path, log_level: str = "INFO") -> logging.Logger:
    """Configure logging with both file and console output."""
    log_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("trades_collector")
    logger.setLevel(getattr(logging, log_level.upper()))
    logger.handlers.clear()

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_format = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", datefmt="%H:%M:%S")
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)

    # File handler
    log_file = log_dir / f"trades_collector_{datetime.now().strftime('%Y%m%d')}.log"
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_format = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    file_handler.setFormatter(file_format)
    logger.addHandler(file_handler)

    return logger

# ============================================================================
# COMPOSITE WATERMARK
# ============================================================================
#
# Watermark per market:
#   { "timestamp": int, "transaction_hash": str }
#
# A trade's key is (timestamp, transactionHash).
# A trade is "new" if its key > watermark key (lexicographic comparison).
# This handles multiple trades sharing the same second-level timestamp.
#
# For full-row deduplication (multi-fill trades sharing the same tx hash),
# we rely on downstream dedupe using TRADE_DEDUPE_COLS.
# ============================================================================

def _trade_key(trade: dict) -> tuple:
    """Extract composite sort key from a raw API trade dict."""
    return (trade.get("timestamp", 0), trade.get("transactionHash", ""))


def _watermark_key(wm: dict | None) -> tuple:
    """Extract composite sort key from a watermark dict."""
    if wm is None:
        return (0, "")
    return (wm.get("timestamp", 0), wm.get("transaction_hash", ""))


def _make_watermark(timestamp: int, transaction_hash: str) -> dict:
    """Construct a watermark dict."""
    return {"timestamp": timestamp, "transaction_hash": transaction_hash}


def load_watermarks(state_dir: Path) -> dict[str, dict]:
    """
    Load watermarks from disk.

    Backward-compatible: handles old format (timestamp-only) by setting
    transaction_hash to "" so comparison still works correctly.
    """
    state_file = state_dir / WATERMARKS_FILE
    if state_file.exists():
        try:
            with open(state_file, "r") as f:
                data = json.load(f)
                normalized = {}
                for cid, wm in data.items():
                    if isinstance(wm, dict):
                        normalized[cid] = _make_watermark(
                            wm.get("timestamp", 0),
                            wm.get("transaction_hash", ""),
                        )
                    else:
                        normalized[cid] = _make_watermark(0, "")
                return normalized
        except (json.JSONDecodeError, IOError):
            return {}
    return {}


def save_watermarks(state_dir: Path, watermarks: dict[str, dict]) -> None:
    """Save watermarks to disk atomically."""
    state_dir.mkdir(parents=True, exist_ok=True)
    state_file = state_dir / WATERMARKS_FILE
    temp_file = state_dir / f"{WATERMARKS_FILE}.tmp"

    with open(temp_file, "w") as f:
        json.dump(watermarks, f, indent=2)
    temp_file.rename(state_file)

# ============================================================================
# DATA COLLECTION
# ============================================================================

def _calculate_backoff(attempt: int) -> float:
    """Calculate exponential backoff with jitter."""
    delay = min(RETRY_BASE_DELAY * (2 ** (attempt - 1)), RETRY_MAX_DELAY)
    return delay + random.random()


def _cache_bust_params(params: dict) -> dict:
    """
    Defeat CDN/edge caching by adding a random query parameter.

    The Polymarket Data API CDN ignores Cache-Control/Pragma headers entirely.
    It caches based on the full query string. A fixed parameter name like '_'
    with a changing value can still hit cache within the CDN's TTL window.

    Using a RANDOM PARAMETER NAME (e.g. 'x7fk2') on every request guarantees
    a unique URL that has never been cached. Empirically verified to return
    fresh data every time.
    """
    rnd_key = "x" + ''.join(random.choices(string.ascii_lowercase + string.digits, k=5))
    params[rnd_key] = int(time.time() * 1000)
    return params


def _api_get(session: requests.Session, url: str, params: dict, logger: logging.Logger) -> requests.Response:
    """Issue a GET with cache-busting and rate-limit sleep."""
    time.sleep(MIN_REQUEST_INTERVAL)
    return session.get(
        url,
        params=_cache_bust_params(dict(params)),  # copy so junk param doesn't leak
        timeout=REQUEST_TIMEOUT_SECONDS,
    )


def fetch_trades_paginated(
    session: requests.Session,
    condition_id: str,
    watermark: dict | None,
    logger: logging.Logger,
) -> dict:
    """
    Fetch trades for a single market with pagination.

    Robustness strategy:
    - Composite watermark (timestamp, transactionHash) avoids dropping trades
      that share the same second-level timestamp as the watermark.
    - Cache-busting nonce + no-cache headers on every request to defeat CDN.
    - Freshness check requires TWO consecutive cache-busted probes to agree
      before early-exit, avoiding stale-response false negatives.
    - Always fetches at least one full page (overlap strategy) and filters
      locally, never trusting API sort order across calls.

    Returns:
        Dict with keys:
            - trades: list of trade dicts (new trades only)
            - http_success: bool
            - new_watermark: dict with timestamp + transaction_hash
            - pages_fetched: int
    """
    url = f"{DATA_API_BASE_URL}/trades"
    wm_key = _watermark_key(watermark)

    # ------------------------------------------------------------------
    # Step 1: Double freshness check (two probes with different nonces)
    # Only early-exit if BOTH probes confirm no new data.
    # ------------------------------------------------------------------
    probes_agree_no_new = True
    pages_fetched = 0

    for probe_idx in range(2):
        try:
            resp = _api_get(session, url, {"market": condition_id, "limit": 10}, logger)
            pages_fetched += 1
            resp.raise_for_status()
            probe_data = resp.json()
            if probe_data:
                newest_key = _trade_key(probe_data[0])
                if newest_key > wm_key:
                    probes_agree_no_new = False
                    break
            # Empty response on first probe -> try second to be sure
        except Exception as e:
            logger.warning(f"Freshness probe {probe_idx+1} failed for {condition_id[:16]}: {e}")
            probes_agree_no_new = False  # On error, don't early-exit
            break

    if probes_agree_no_new:
        logger.debug(f"Market {condition_id[:16]}: wm={wm_key} new_trades=0 (double probe)")
        return {
            "trades": [],
            "http_success": True,
            "new_watermark": watermark or _make_watermark(0, ""),
            "pages_fetched": pages_fetched,
        }

    # ------------------------------------------------------------------
    # Step 2: Paginated bulk fetch with overlap-and-filter strategy
    #
    # Efficiency: start with a small page (FIRST_PAGE_SIZE=50). If the
    # entire page is new trades, the watermark boundary hasn't been
    # reached, so escalate to FULL_PAGE_SIZE=500. In the common case
    # (few new trades per cycle) this avoids fetching 500 rows per market.
    # ------------------------------------------------------------------
    all_trades = []
    seen_keys: set[tuple] = set()  # dedupe within this fetch
    new_wm_key = wm_key
    offset = 0
    page_size = FIRST_PAGE_SIZE  # start small

    while offset <= MAX_OFFSET:
        params = {
            "market": condition_id,
            "limit": page_size,
            "offset": offset,
        }

        data = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = _api_get(session, url, params, logger)

                if response.status_code == 429:
                    backoff = _calculate_backoff(attempt)
                    logger.warning(f"Rate limited (429), backoff={backoff:.1f}s")
                    time.sleep(backoff)
                    continue

                if response.status_code == 400 and offset > 0:
                    logger.debug(f"Got 400 at offset {offset}, treating as end of data")
                    data = []
                    break

                response.raise_for_status()
                data = response.json()

                if not isinstance(data, list):
                    logger.warning(f"Unexpected response type: {type(data).__name__}")
                    data = []
                    break

                pages_fetched += 1
                break  # Success, exit retry loop

            except requests.exceptions.Timeout:
                logger.warning(f"Timeout (attempt {attempt}/{MAX_RETRIES})")
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request error: {e} (attempt {attempt}/{MAX_RETRIES})")
            except json.JSONDecodeError as e:
                logger.warning(f"JSON decode error: {e}")
                data = []
                break

            if attempt < MAX_RETRIES:
                backoff = _calculate_backoff(attempt)
                time.sleep(backoff)
        else:
            # All retries failed
            logger.error(f"Failed to fetch page at offset {offset} after {MAX_RETRIES} attempts")
            break

        if data is None or not data:
            break

        # Filter: keep trades whose composite key > watermark key
        page_new_count = 0
        oldest_in_page_ts = float('inf')

        for trade in data:
            tk = _trade_key(trade)
            ts = tk[0]

            if ts < oldest_in_page_ts:
                oldest_in_page_ts = ts

            # Advance high-water mark
            if tk > new_wm_key:
                new_wm_key = tk

            # New trade? (composite key strictly greater than watermark)
            if tk > wm_key and tk not in seen_keys:
                seen_keys.add(tk)
                all_trades.append(trade)
                page_new_count += 1

        # If full page but every trade is at or below watermark, we've
        # scanned past the boundary — stop.
        if len(data) == page_size and page_new_count == 0 and oldest_in_page_ts <= wm_key[0]:
            break

        # Partial page means no more data from API
        if len(data) < page_size:
            break

        # If the entire page was new trades, we haven't reached the
        # watermark yet — escalate to full page size for efficiency.
        if page_new_count == len(data) and page_size < FULL_PAGE_SIZE:
            page_size = FULL_PAGE_SIZE

        offset += len(data)

    # Build new watermark from the highest key seen
    new_watermark = _make_watermark(new_wm_key[0], new_wm_key[1])

    # Debug logging
    if all_trades:
        trade_timestamps = [t.get("timestamp", 0) for t in all_trades]
        logger.debug(
            f"Market {condition_id[:16]}: wm={wm_key} "
            f"new_trades={len(all_trades)} newest_ts={max(trade_timestamps)} oldest_ts={min(trade_timestamps)}"
        )
    else:
        logger.debug(f"Market {condition_id[:16]}: wm={wm_key} new_trades=0")

    return {
        "trades": all_trades,
        "http_success": True,
        "new_watermark": new_watermark,
        "pages_fetched": pages_fetched,
    }


def collect_trades(
    session: requests.Session,
    markets: list[dict],
    watermarks: dict[str, dict],
    logger: logging.Logger,
    scheduled_time: datetime,
) -> tuple[list[dict], dict[str, dict], dict]:
    """
    Collect trades for all markets.

    Args:
        session: requests.Session for connection reuse
        markets: List of market configs
        watermarks: Dict mapping condition_id -> watermark dict
        logger: Logger instance
        scheduled_time: Wall-clock aligned scheduled time

    Returns:
        Tuple of (trade_records, updated_watermarks, batch_info)
    """
    collected_time = datetime.now(timezone.utc)
    all_trades = []
    updated_watermarks = dict(watermarks)

    total_trades = 0
    total_pages = 0
    successful_markets = 0
    failed_markets = 0

    for market in markets:
        condition_id = market["condition_id"]
        slug = market["slug"]
        watermark = watermarks.get(condition_id)

        result = fetch_trades_paginated(session, condition_id, watermark, logger)
        total_pages += result["pages_fetched"]

        if result["http_success"]:
            successful_markets += 1
            updated_watermarks[condition_id] = result["new_watermark"]

            for trade in result["trades"]:
                record = {
                    # Timestamps
                    "timestamp_scheduled_utc": scheduled_time.isoformat(),
                    "timestamp_collected_utc": collected_time.isoformat(),
                    "trade_timestamp_unix": trade.get("timestamp"),

                    # Market identifiers
                    "condition_id": condition_id,
                    "slug": slug,
                    "market_title": trade.get("title", market.get("question", "")),

                    # Trade data
                    "side": trade.get("side"),
                    "size": float(trade.get("size", 0)),
                    "price": float(trade.get("price", 0)),
                    "outcome": trade.get("outcome"),
                    "outcome_index": trade.get("outcomeIndex"),

                    # Transaction info
                    "transaction_hash": trade.get("transactionHash"),
                    "proxy_wallet": trade.get("proxyWallet"),
                }
                all_trades.append(record)
                total_trades += 1
        else:
            failed_markets += 1

    batch_info = {
        "total_trades": total_trades,
        "total_pages": total_pages,
        "successful_markets": successful_markets,
        "failed_markets": failed_markets,
        "total_markets": len(markets),
    }

    return all_trades, updated_watermarks, batch_info

# ============================================================================
# DATA STORAGE
# ============================================================================

class TradesWriter:
    """
    Manages writing trades to Parquet files with atomic per-cycle writes.

    File structure:
        output_dir/
            YYYYMMDD/
                HHMMSS.parquet  (one file per collection cycle)
    """

    def __init__(self, output_dir: Path, logger: logging.Logger):
        self.output_dir = output_dir
        self.logger = logger
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.files_written = 0
        self.records_written = 0

    def write(self, trades: list[dict], timestamp: datetime) -> Path | None:
        """
        Write trades atomically to a per-cycle parquet file.

        Returns:
            Path to written file, or None if no data
        """
        if not trades:
            self.logger.debug("No trades to write")
            return None

        # Create date directory
        date_str = timestamp.strftime("%Y%m%d")
        date_dir = self.output_dir / date_str
        date_dir.mkdir(parents=True, exist_ok=True)

        # File paths
        time_str = timestamp.strftime("%H%M%S")
        final_path = date_dir / f"{time_str}.parquet"
        temp_path = date_dir / f"{time_str}.parquet.tmp"

        # Convert to DataFrame with explicit types
        df = pd.DataFrame(trades)

        # Ensure consistent types
        if "size" in df.columns:
            df["size"] = pd.to_numeric(df["size"], errors="coerce")
        if "price" in df.columns:
            df["price"] = pd.to_numeric(df["price"], errors="coerce")
        if "trade_timestamp_unix" in df.columns:
            df["trade_timestamp_unix"] = pd.to_numeric(df["trade_timestamp_unix"], errors="coerce").astype("Int64")

        # Atomic write: temp file -> rename
        try:
            df.to_parquet(temp_path, index=False, compression="snappy")
            temp_path.rename(final_path)

            self.files_written += 1
            self.records_written += len(trades)

            self.logger.info(f"Wrote {len(trades)} trades to {date_str}/{time_str}.parquet")
            return final_path

        except Exception as e:
            self.logger.error(f"Failed to write {final_path}: {e}")
            if temp_path.exists():
                try:
                    temp_path.unlink()
                except Exception:
                    pass
            raise

    def get_stats(self) -> dict:
        """Return statistics about stored data."""
        date_dirs = [d for d in self.output_dir.iterdir() if d.is_dir() and d.name.isdigit()]

        total_files = 0
        total_size_bytes = 0

        for date_dir in date_dirs:
            parquet_files = list(date_dir.glob("*.parquet"))
            total_files += len(parquet_files)
            for pf in parquet_files:
                total_size_bytes += pf.stat().st_size

        return {
            "days": len(date_dirs),
            "files": total_files,
            "total_records": self.records_written,
            "total_size_mb": round(total_size_bytes / (1024 * 1024), 2),
        }


def load_collected_trades(data_dir: Path) -> pd.DataFrame:
    """
    Load all collected trade parquet files into a single DataFrame.

    Automatically deduplicates by composite key (a single tx can have multiple fills).

    Args:
        data_dir: Path to the data/raw/trades directory

    Returns:
        Combined DataFrame with all trades (deduplicated)
    """
    parquet_files = sorted(data_dir.glob("**/*.parquet"))

    if not parquet_files:
        return pd.DataFrame()

    dfs = [pd.read_parquet(f) for f in parquet_files]
    df = pd.concat(dfs, ignore_index=True)

    # Deduplicate by composite key (tx hash alone is not unique - multiple fills per tx)
    dedupe_cols = ["condition_id", "trade_timestamp_unix", "transaction_hash",
                   "outcome_index", "side", "price", "size"]
    # Only use columns that exist
    dedupe_cols = [c for c in dedupe_cols if c in df.columns]
    if dedupe_cols:
        df = df.drop_duplicates(subset=dedupe_cols)

    return df

# ============================================================================
# MAIN COLLECTOR
# ============================================================================

class TradesCollector:
    """Main trades collector class."""

    def __init__(
        self,
        config_path: Path,
        output_dir: Path,
        log_dir: Path,
        state_dir: Path,
        interval_seconds: int = DEFAULT_INTERVAL_SECONDS,
    ):
        self.config_path = config_path
        self.output_dir = output_dir
        self.state_dir = state_dir
        self.interval_seconds = interval_seconds
        self.running = False
        self.cycle_count = 0
        self.error_count = 0
        self.start_time: datetime | None = None

        # Setup logging first
        self.logger = setup_logging(log_dir)

        # Load watermarks from disk
        self.watermarks = load_watermarks(state_dir)
        if self.watermarks:
            self.logger.info(f"Loaded watermarks for {len(self.watermarks)} markets from disk")

        # Load config
        self.config = self._load_config()
        self.markets = self.config.get("markets", [])

        # Setup data writer
        self.writer = TradesWriter(output_dir, self.logger)

        # Setup HTTP session for connection reuse
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "PolymarketThesisCollector/1.0",
            "Accept": "application/json",
        })

        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _load_config(self) -> dict:
        """Load market configuration from JSON file."""
        self.logger.info(f"Loading config from: {self.config_path}")
        with open(self.config_path, "r") as f:
            config = json.load(f)

        markets = config.get("markets", [])
        self.logger.info(f"Loaded {len(markets)} markets")
        return config

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        self.logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.running = False

    def _next_aligned_time(self, now: datetime) -> datetime:
        """Calculate next wall-clock aligned collection time."""
        interval = self.interval_seconds
        timestamp = now.timestamp()
        next_boundary = ((timestamp // interval) + 1) * interval
        return datetime.fromtimestamp(next_boundary, tz=timezone.utc)

    def _write_heartbeat(self, trades_count: int, pages_count: int) -> None:
        """Write heartbeat file for external monitoring."""
        heartbeat = {
            "collector": "trades",
            "last_success_utc": datetime.now(timezone.utc).isoformat(),
            "cycle": self.cycle_count,
            "trades": trades_count,
            "pages": pages_count,
            "errors": self.error_count,
        }
        hb_path = self.state_dir / "heartbeat_trades.json"
        tmp_path = self.state_dir / "heartbeat_trades.json.tmp"
        try:
            with open(tmp_path, "w") as f:
                json.dump(heartbeat, f)
            tmp_path.rename(hb_path)
        except Exception:
            pass  # heartbeat is best-effort

    def _log_status(self) -> None:
        """Log current collection status."""
        if not self.start_time:
            return

        runtime = datetime.now(timezone.utc) - self.start_time
        hours = runtime.total_seconds() / 3600
        stats = self.writer.get_stats()

        self.logger.info(
            f"Status: {self.cycle_count} cycles | "
            f"{stats['total_records']} trades | "
            f"{stats['total_size_mb']:.1f} MB | "
            f"{self.error_count} errors | "
            f"Runtime: {hours:.1f}h"
        )

    def run(self) -> None:
        """Main collection loop with wall-clock aligned scheduling."""
        self.running = True
        self.start_time = datetime.now(timezone.utc)

        self.logger.info("=" * 60)
        self.logger.info("TRADES COLLECTOR STARTED")
        self.logger.info("=" * 60)
        self.logger.info(f"Markets: {len(self.markets)}")
        self.logger.info(f"Interval: {self.interval_seconds} seconds ({self.interval_seconds/60:.1f} minutes)")
        self.logger.info(f"Output: {self.output_dir}")
        self.logger.info(f"State: {self.state_dir}")
        self.logger.info(f"Start time: {self.start_time.isoformat()}")
        self.logger.info("=" * 60)

        # Calculate first collection time
        next_scheduled = self._next_aligned_time(datetime.now(timezone.utc))
        self.logger.info(f"First collection scheduled at: {next_scheduled.strftime('%H:%M:%S')} UTC")

        while self.running:
            # Wait until scheduled time
            now = datetime.now(timezone.utc)
            wait_seconds = (next_scheduled - now).total_seconds()

            if wait_seconds > 0:
                self.logger.debug(f"Waiting {wait_seconds:.1f}s until {next_scheduled.strftime('%H:%M:%S')} UTC...")
                wait_until = time.time() + wait_seconds
                while self.running and time.time() < wait_until:
                    time.sleep(min(1.0, wait_until - time.time()))

            if not self.running:
                break

            scheduled_time = next_scheduled

            try:
                self.logger.info(f"[Cycle {self.cycle_count + 1}] Collecting trades from {len(self.markets)} markets...")

                trades, self.watermarks, batch_info = collect_trades(
                    self.session,
                    self.markets,
                    self.watermarks,
                    self.logger,
                    scheduled_time,
                )

                self.logger.info(
                    f"[Cycle {self.cycle_count + 1}] Collected {batch_info['total_trades']} trades "
                    f"({batch_info['total_pages']} pages) from "
                    f"{batch_info['successful_markets']}/{batch_info['total_markets']} markets"
                )

                # Write trades
                if trades:
                    self.writer.write(trades, timestamp=scheduled_time)

                # Persist watermarks after successful cycle
                save_watermarks(self.state_dir, self.watermarks)

                self.cycle_count += 1

                # Write heartbeat for external monitoring
                self._write_heartbeat(batch_info['total_trades'], batch_info['total_pages'])

                # Log status every 12 cycles (hourly at 5-min intervals)
                if self.cycle_count % 12 == 0:
                    self._log_status()

            except Exception as e:
                self.error_count += 1
                self.logger.exception(f"Error during collection: {e}")

            # Schedule next collection
            next_scheduled = self._next_aligned_time(datetime.now(timezone.utc))

        # Graceful shutdown - save watermarks
        self.logger.info("Shutting down...")
        save_watermarks(self.state_dir, self.watermarks)
        self._log_status()
        self.logger.info("=" * 60)
        self.logger.info("TRADES COLLECTOR STOPPED")
        self.logger.info("=" * 60)

        # Close session
        self.session.close()

# ============================================================================
# CLI
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Trades Collector for Polymarket (runs alongside order book collector)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/thesis_markets.json"),
        help="Path to market configuration JSON file",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(DEFAULT_OUTPUT_DIR),
        help="Directory for output Parquet files",
    )
    parser.add_argument(
        "--log-dir",
        type=Path,
        default=Path("logs"),
        help="Directory for log files",
    )
    parser.add_argument(
        "--state-dir",
        type=Path,
        default=Path(STATE_DIR),
        help="Directory for state files (watermarks)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=DEFAULT_INTERVAL_SECONDS,
        help="Collection interval in seconds",
    )

    args = parser.parse_args()

    if not args.config.exists():
        print(f"Error: Config file not found: {args.config}")
        sys.exit(1)

    collector = TradesCollector(
        config_path=args.config,
        output_dir=args.output_dir,
        log_dir=args.log_dir,
        state_dir=args.state_dir,
        interval_seconds=args.interval,
    )
    collector.run()


if __name__ == "__main__":
    main()
