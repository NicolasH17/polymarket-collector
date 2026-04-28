#!/usr/bin/env python3
"""
Continuous CLOB Order Book Collector for Polymarket Thesis

Collects order book snapshots at regular intervals (default: 5 minutes).
Designed for 2-3 weeks of continuous operation with:
- Parquet output format (efficient for analysis)
- Comprehensive logging for thesis methodology documentation
- Automatic recovery from errors
- Graceful handling of resolved markets
- Daily file rotation for manageable file sizes

Usage:
    python continuous_collector.py --config config/thesis_markets.json
    python continuous_collector.py --config config/thesis_markets.json --interval 300 --output-dir data/collected
"""

import argparse
import json
import logging
import random
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests

# ============================================================================
# CONFIGURATION
# ============================================================================

CLOB_BASE_URL = "https://clob.polymarket.com"
DEFAULT_INTERVAL_SECONDS = 300  # 5 minutes
DEFAULT_OUTPUT_DIR = "data/raw"
REQUEST_TIMEOUT_SECONDS = 15
MAX_RETRIES = 3
RETRY_BASE_DELAY = 2.0  # Base delay for exponential backoff
RETRY_MAX_DELAY = 60.0  # Cap retry delay
SCHEMA_VERSION = 5  # v5: QA flags, cycle health, dual timestamps, wall-clock alignment

# ============================================================================
# LOGGING SETUP
# ============================================================================

def setup_logging(log_dir: Path, log_level: str = "INFO") -> logging.Logger:
    """Configure logging with both file and console output."""
    log_dir.mkdir(parents=True, exist_ok=True)

    # Create logger
    logger = logging.getLogger("clob_collector")
    logger.setLevel(getattr(logging, log_level.upper()))
    logger.handlers.clear()

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_format = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)

    # File handler (daily rotation via filename)
    log_file = log_dir / f"collector_{datetime.now(timezone.utc).strftime('%Y%m%d')}.log"
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_format = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(funcName)s:%(lineno)d | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z"
    )
    file_handler.setFormatter(file_format)
    logger.addHandler(file_handler)

    return logger

# ============================================================================
# DATA COLLECTION
# ============================================================================

def _calculate_backoff(attempt: int) -> float:
    """Calculate exponential backoff with jitter: base * 2^(attempt-1) + random(0,1)."""
    delay = min(RETRY_BASE_DELAY * (2 ** (attempt - 1)), RETRY_MAX_DELAY)
    return delay + random.random()


def fetch_order_books_batch(token_ids: list[str], logger: logging.Logger) -> dict:
    """
    Fetch order books for multiple tokens in a single batch request.

    Uses POST /books endpoint which is ~10x faster than individual requests.
    API supports up to 500 tokens per request.

    Args:
        token_ids: List of token IDs to fetch
        logger: Logger instance

    Returns:
        Dict with keys:
            - books_by_token: dict mapping token_id -> order book data (or None)
            - http_success: bool, whether the HTTP request succeeded
            - http_status: int or None, HTTP status code
            - books_received: int, count of valid books returned
            - books_requested: int, count of tokens requested
            - missing_count: int, tokens not found in response
            - unexpected_ids: list of asset_ids not in our request
            - elapsed_ms: float, request duration
    """
    url = f"{CLOB_BASE_URL}/books"
    payload = [{"token_id": t} for t in token_ids]
    requested_set = set(token_ids)

    # Default failure result
    def failure_result(http_status=None):
        return {
            "books_by_token": {t: None for t in token_ids},
            "http_success": False,
            "http_status": http_status,
            "books_received": 0,
            "books_requested": len(token_ids),
            "missing_count": len(token_ids),
            "unexpected_ids": [],
            "elapsed_ms": 0,
        }

    for attempt in range(1, MAX_RETRIES + 1):
        start_time = time.time()
        http_status = None

        try:
            response = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT_SECONDS * 2)
            http_status = response.status_code
            elapsed_ms = (time.time() - start_time) * 1000

            # Handle rate limiting with backoff
            if http_status == 429:
                retry_after = response.headers.get("Retry-After")
                backoff = _calculate_backoff(attempt)
                if retry_after:
                    logger.warning(f"Rate limited (429), Retry-After={retry_after}s, using backoff={backoff:.1f}s (attempt {attempt}/{MAX_RETRIES})")
                else:
                    logger.warning(f"Rate limited (429), backoff={backoff:.1f}s (attempt {attempt}/{MAX_RETRIES})")
                if attempt < MAX_RETRIES:
                    time.sleep(backoff)
                continue

            response.raise_for_status()
            data = response.json()

            if not isinstance(data, list):
                logger.warning(f"Unexpected batch response type: {type(data).__name__}")
                return failure_result(http_status)

            # Map responses back to token IDs by asset_id field
            books_by_token = {}
            unexpected_ids = []

            for book in data:
                if not isinstance(book, dict):
                    continue
                asset_id = book.get("asset_id")
                if not asset_id:
                    continue

                if asset_id not in requested_set:
                    unexpected_ids.append(asset_id[:20] + "...")
                    continue

                if "bids" in book and "asks" in book:
                    books_by_token[asset_id] = book
                else:
                    books_by_token[asset_id] = None

            # Fill in missing tokens
            missing_count = 0
            for token_id in token_ids:
                if token_id not in books_by_token:
                    books_by_token[token_id] = None
                    missing_count += 1

            books_received = sum(1 for v in books_by_token.values() if v is not None)

            if unexpected_ids:
                logger.warning(f"Batch response contained {len(unexpected_ids)} unexpected asset_ids")
            if missing_count > 0:
                logger.debug(f"Batch response missing {missing_count}/{len(token_ids)} requested tokens")

            logger.debug(f"Batch fetched {books_received}/{len(token_ids)} books in {elapsed_ms:.0f}ms")

            return {
                "books_by_token": books_by_token,
                "http_success": True,
                "http_status": http_status,
                "books_received": books_received,
                "books_requested": len(token_ids),
                "missing_count": missing_count,
                "unexpected_ids": unexpected_ids,
                "elapsed_ms": elapsed_ms,
            }

        except requests.exceptions.Timeout:
            logger.warning(f"Batch request timeout (attempt {attempt}/{MAX_RETRIES})")
        except requests.exceptions.HTTPError:
            # http_status is already set from response.status_code above
            logger.warning(f"Batch HTTP error {http_status} (attempt {attempt}/{MAX_RETRIES})")
        except requests.exceptions.RequestException as e:
            # Connection errors, DNS failures, etc. - no response object
            logger.warning(f"Batch request error: {e} (attempt {attempt}/{MAX_RETRIES})")
        except json.JSONDecodeError as e:
            logger.warning(f"Batch JSON decode error: {e}")
            return failure_result(http_status)

        if attempt < MAX_RETRIES:
            backoff = _calculate_backoff(attempt)
            logger.debug(f"Retrying in {backoff:.1f}s...")
            time.sleep(backoff)

    logger.error(f"Batch fetch failed after {MAX_RETRIES} attempts")
    return failure_result(http_status)


def fetch_order_book(token_id: str, logger: logging.Logger) -> dict | None:
    """
    Fetch order book for a single token from Polymarket CLOB API.

    NOTE: For multiple tokens, use fetch_order_books_batch() which is ~10x faster.

    Returns:
        dict with 'bids' and 'asks' lists, or None on failure
    """
    url = f"{CLOB_BASE_URL}/book"
    params = {"token_id": token_id}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()
            data = response.json()

            # Validate expected response shape
            if not isinstance(data, dict):
                logger.warning(f"Unexpected response type for token {token_id[:20]}...: {type(data).__name__}")
                return None
            if "bids" not in data or "asks" not in data:
                logger.warning(f"Missing bids/asks in response for token {token_id[:20]}...: keys={list(data.keys())}")
                return None

            logger.debug(f"Fetched book for token {token_id[:20]}... ({len(data.get('bids', []))} bids, {len(data.get('asks', []))} asks)")
            return data

        except requests.exceptions.Timeout:
            logger.warning(f"Timeout fetching token {token_id[:20]}... (attempt {attempt}/{MAX_RETRIES})")
        except requests.exceptions.HTTPError:
            # response exists here since raise_for_status() was called
            if response.status_code == 404:
                logger.warning(f"Token not found (404): {token_id[:20]}... - market may be resolved")
                return None
            if response.status_code == 429:
                logger.warning(f"Rate limited (429) for token {token_id[:20]}... (attempt {attempt}/{MAX_RETRIES})")
            else:
                logger.warning(f"HTTP error {response.status_code} for token {token_id[:20]}... (attempt {attempt}/{MAX_RETRIES})")
        except requests.exceptions.RequestException as e:
            # Connection errors, DNS failures, etc. - no response object
            logger.warning(f"Request error for token {token_id[:20]}...: {e} (attempt {attempt}/{MAX_RETRIES})")
        except json.JSONDecodeError as e:
            logger.warning(f"JSON decode error for token {token_id[:20]}...: {e}")
            return None

        if attempt < MAX_RETRIES:
            backoff = _calculate_backoff(attempt)
            time.sleep(backoff)

    logger.error(f"Failed to fetch token {token_id[:20]}... after {MAX_RETRIES} attempts")
    return None

def _parse_level(level: dict) -> tuple[float, float] | None:
    """
    Safely parse a single order book level.

    Returns (price, size) tuple or None if malformed.
    """
    try:
        price = float(level["price"])
        size = float(level["size"])
        return (price, size)
    except (KeyError, ValueError, TypeError):
        return None


def calculate_metrics(book_data: dict | None) -> dict[str, Any]:
    """
    Calculate market microstructure metrics from order book data.

    Returns dict with all metrics, using None for missing values.
    Includes QA flags for data quality assessment:
        - qa_missing_side: bids empty OR asks empty
        - qa_crossed_book: best_bid >= best_ask (invalid state)
        - qa_price_out_of_range: best quote outside [0, 1]
        - qa_malformed_levels: count of skipped malformed levels
    """
    metrics = {
        # Core metrics
        "mid_price": None,
        "best_bid_price": None,
        "best_ask_price": None,
        "spread_absolute": None,
        "spread_relative": None,
        "best_bid_size": None,
        "best_ask_size": None,
        "depth_at_best": None,
        "total_bid_depth": None,
        "total_ask_depth": None,
        "bid_levels": None,
        "ask_levels": None,
        "book_imbalance": None,
        # Depth within X% of midpoint (for liquidity/price impact analysis)
        "depth_bid_5pct": None,
        "depth_ask_5pct": None,
        "depth_bid_10pct": None,
        "depth_ask_10pct": None,
        "depth_bid_20pct": None,
        "depth_ask_20pct": None,
        # QA flags
        "qa_missing_side": False,
        "qa_crossed_book": False,
        "qa_price_out_of_range": False,
        "qa_malformed_levels": 0,
    }

    if not book_data:
        metrics["qa_missing_side"] = True
        return metrics

    raw_bids = book_data.get("bids", [])
    raw_asks = book_data.get("asks", [])

    # Parse levels, tracking malformed ones
    malformed_count = 0
    bids = []
    asks = []

    for level in raw_bids:
        parsed = _parse_level(level)
        if parsed:
            bids.append(parsed)
        else:
            malformed_count += 1

    for level in raw_asks:
        parsed = _parse_level(level)
        if parsed:
            asks.append(parsed)
        else:
            malformed_count += 1

    metrics["qa_malformed_levels"] = malformed_count

    # Count valid levels
    metrics["bid_levels"] = len(bids)
    metrics["ask_levels"] = len(asks)

    # Check for missing side
    if not bids or not asks:
        metrics["qa_missing_side"] = True
        return metrics

    # Best bid/ask - bids sorted by price descending, asks by price ascending
    # Best bid = highest bid price, Best ask = lowest ask price
    best_bid = max(bids, key=lambda x: x[0])
    best_ask = min(asks, key=lambda x: x[0])

    best_bid_price, best_bid_size = best_bid
    best_ask_price, best_ask_size = best_ask

    metrics["best_bid_price"] = best_bid_price
    metrics["best_bid_size"] = best_bid_size
    metrics["best_ask_price"] = best_ask_price
    metrics["best_ask_size"] = best_ask_size

    # QA: Check for crossed book (invalid state)
    if best_bid_price >= best_ask_price:
        metrics["qa_crossed_book"] = True

    # QA: Check for prices outside [0, 1] range
    if not (0 <= best_bid_price <= 1) or not (0 <= best_ask_price <= 1):
        metrics["qa_price_out_of_range"] = True

    # Mid price and spreads
    mid_price = None
    if best_bid_price > 0 and best_ask_price > 0:
        mid_price = (best_bid_price + best_ask_price) / 2
        spread_absolute = best_ask_price - best_bid_price
        spread_relative = spread_absolute / mid_price if mid_price > 0 else None

        metrics["mid_price"] = mid_price
        metrics["spread_absolute"] = spread_absolute
        metrics["spread_relative"] = spread_relative

    # Depth metrics
    metrics["depth_at_best"] = best_bid_size + best_ask_size

    total_bid_depth = sum(size for _, size in bids)
    total_ask_depth = sum(size for _, size in asks)
    metrics["total_bid_depth"] = total_bid_depth
    metrics["total_ask_depth"] = total_ask_depth

    # Book imbalance: (bid_depth - ask_depth) / (bid_depth + ask_depth)
    total_depth = total_bid_depth + total_ask_depth
    if total_depth > 0:
        metrics["book_imbalance"] = (total_bid_depth - total_ask_depth) / total_depth

    # Depth within X% of midpoint
    # For bids: sum volume where price >= mid * (1 - X%)
    # For asks: sum volume where price <= mid * (1 + X%)
    if mid_price and mid_price > 0:
        for pct in [5, 10, 20]:
            pct_decimal = pct / 100
            bid_threshold = mid_price * (1 - pct_decimal)
            ask_threshold = mid_price * (1 + pct_decimal)

            bid_depth_pct = sum(size for price, size in bids if price >= bid_threshold)
            ask_depth_pct = sum(size for price, size in asks if price <= ask_threshold)

            metrics[f"depth_bid_{pct}pct"] = bid_depth_pct
            metrics[f"depth_ask_{pct}pct"] = ask_depth_pct

    return metrics

def collect_snapshot(
    markets: list[dict],
    logger: logging.Logger,
    scheduled_time: datetime | None = None,
) -> tuple[list[dict], dict]:
    """
    Collect a single snapshot of all markets using batch API for efficiency.

    Uses POST /books endpoint to fetch all order books in one request (~10x faster).

    Args:
        markets: List of market configs to collect
        logger: Logger instance
        scheduled_time: Wall-clock aligned scheduled time (for dual timestamps)

    Returns:
        tuple of (snapshots, batch_info) where:
            - snapshots: list of snapshot records, one per market
            - batch_info: dict with batch fetch metrics (http_success, books_received, etc.)
    """
    collected_time = datetime.now(timezone.utc)
    if scheduled_time is None:
        scheduled_time = collected_time

    snapshots = []

    # Collect all token IDs for batch request
    all_token_ids = []
    token_to_market = {}  # Map token_id -> (market, side)

    for market in markets:
        yes_token = market["tokens"]["yes"]
        no_token = market["tokens"]["no"]
        all_token_ids.extend([yes_token, no_token])
        token_to_market[yes_token] = (market, "yes")
        token_to_market[no_token] = (market, "no")

    # Fetch all order books in one batch request
    logger.info(f"Fetching {len(all_token_ids)} order books in batch...")
    batch_result = fetch_order_books_batch(all_token_ids, logger)

    books = batch_result["books_by_token"]

    # Cycle health metrics
    books_requested = batch_result["books_requested"]
    books_received = batch_result["books_received"]
    cycle_success_rate = books_received / books_requested if books_requested > 0 else 0.0

    batch_info = {
        "http_success": batch_result["http_success"],
        "http_status": batch_result["http_status"],
        "books_received": books_received,
        "books_requested": books_requested,
        "missing_count": batch_result["missing_count"],
        "elapsed_ms": batch_result["elapsed_ms"],
        "cycle_success_rate": cycle_success_rate,
    }

    if not batch_result["http_success"]:
        logger.warning(f"Batch fetch failed (http_status={batch_result['http_status']}), snapshot will have no book data")

    for market in markets:
        market_id = market["gamma_market_id"]
        slug = market["slug"]

        logger.debug(f"Processing: {slug}")

        # Get books from batch response
        yes_token = market["tokens"]["yes"]
        no_token = market["tokens"]["no"]
        yes_book = books.get(yes_token)
        no_book = books.get(no_token)

        # Calculate metrics
        yes_metrics = calculate_metrics(yes_book)
        no_metrics = calculate_metrics(no_book)

        # Determine market status - only interpret if cycle was healthy
        # If batch failed entirely, we can't know if market is truly inactive
        market_active = yes_book is not None or no_book is not None

        # Build snapshot record
        record = {
            # Schema version for compatibility detection
            "schema_version": SCHEMA_VERSION,

            # Timestamps (dual: scheduled vs actual collection time)
            "timestamp_scheduled_utc": scheduled_time.isoformat(),
            "timestamp_collected_utc": collected_time.isoformat(),
            "timestamp_unix": collected_time.timestamp(),

            # Cycle health (same for all markets in this snapshot)
            "cycle_http_success": batch_result["http_success"],
            "cycle_success_rate": cycle_success_rate,

            # Market identifiers
            "market_id": market_id,
            "slug": slug,
            "question": market["question"],
            "group": market.get("group", ""),
            "deterministic": market.get("deterministic", None),
            "insider_risk": market.get("insider_risk", ""),
            "end_time": market.get("end_time", ""),

            # Market status
            "market_active": market_active,
            "yes_book_available": yes_book is not None,
            "no_book_available": no_book is not None,

            # YES token metrics
            "yes_mid_price": yes_metrics["mid_price"],
            "yes_best_bid_price": yes_metrics["best_bid_price"],
            "yes_best_ask_price": yes_metrics["best_ask_price"],
            "yes_spread_absolute": yes_metrics["spread_absolute"],
            "yes_spread_relative": yes_metrics["spread_relative"],
            "yes_best_bid_size": yes_metrics["best_bid_size"],
            "yes_best_ask_size": yes_metrics["best_ask_size"],
            "yes_depth_at_best": yes_metrics["depth_at_best"],
            "yes_total_bid_depth": yes_metrics["total_bid_depth"],
            "yes_total_ask_depth": yes_metrics["total_ask_depth"],
            "yes_bid_levels": yes_metrics["bid_levels"],
            "yes_ask_levels": yes_metrics["ask_levels"],
            "yes_book_imbalance": yes_metrics["book_imbalance"],
            "yes_depth_bid_5pct": yes_metrics["depth_bid_5pct"],
            "yes_depth_ask_5pct": yes_metrics["depth_ask_5pct"],
            "yes_depth_bid_10pct": yes_metrics["depth_bid_10pct"],
            "yes_depth_ask_10pct": yes_metrics["depth_ask_10pct"],
            "yes_depth_bid_20pct": yes_metrics["depth_bid_20pct"],
            "yes_depth_ask_20pct": yes_metrics["depth_ask_20pct"],

            # YES token QA flags
            "yes_qa_missing_side": yes_metrics["qa_missing_side"],
            "yes_qa_crossed_book": yes_metrics["qa_crossed_book"],
            "yes_qa_price_out_of_range": yes_metrics["qa_price_out_of_range"],
            "yes_qa_malformed_levels": yes_metrics["qa_malformed_levels"],

            # NO token metrics
            "no_mid_price": no_metrics["mid_price"],
            "no_best_bid_price": no_metrics["best_bid_price"],
            "no_best_ask_price": no_metrics["best_ask_price"],
            "no_spread_absolute": no_metrics["spread_absolute"],
            "no_spread_relative": no_metrics["spread_relative"],
            "no_best_bid_size": no_metrics["best_bid_size"],
            "no_best_ask_size": no_metrics["best_ask_size"],
            "no_depth_at_best": no_metrics["depth_at_best"],
            "no_total_bid_depth": no_metrics["total_bid_depth"],
            "no_total_ask_depth": no_metrics["total_ask_depth"],
            "no_bid_levels": no_metrics["bid_levels"],
            "no_ask_levels": no_metrics["ask_levels"],
            "no_book_imbalance": no_metrics["book_imbalance"],
            "no_depth_bid_5pct": no_metrics["depth_bid_5pct"],
            "no_depth_ask_5pct": no_metrics["depth_ask_5pct"],
            "no_depth_bid_10pct": no_metrics["depth_bid_10pct"],
            "no_depth_ask_10pct": no_metrics["depth_ask_10pct"],
            "no_depth_bid_20pct": no_metrics["depth_bid_20pct"],
            "no_depth_ask_20pct": no_metrics["depth_ask_20pct"],

            # NO token QA flags
            "no_qa_missing_side": no_metrics["qa_missing_side"],
            "no_qa_crossed_book": no_metrics["qa_crossed_book"],
            "no_qa_price_out_of_range": no_metrics["qa_price_out_of_range"],
            "no_qa_malformed_levels": no_metrics["qa_malformed_levels"],
        }

        snapshots.append(record)

    return snapshots, batch_info

# ============================================================================
# DATA STORAGE
# ============================================================================

class DataWriter:
    """
    Manages writing snapshots to Parquet files with atomic per-cycle writes.

    File structure:
        output_dir/
            YYYYMMDD/
                HHMMSS.parquet  (one file per collection cycle)

    Atomic write pattern:
        1. Write to temporary file (HHMMSS.parquet.tmp)
        2. Rename to final path (HHMMSS.parquet)
        This ensures no partial/corrupt files on crash.
    """

    def __init__(self, output_dir: Path, logger: logging.Logger):
        self.output_dir = output_dir
        self.logger = logger
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.files_written = 0
        self.records_written = 0

    def write(self, snapshots: list[dict], timestamp: datetime | None = None) -> Path | None:
        """
        Write snapshots atomically to a per-cycle parquet file.

        Args:
            snapshots: List of snapshot records to write
            timestamp: Timestamp for filename (defaults to now)

        Returns:
            Path to written file, or None if no data
        """
        if not snapshots:
            return None

        if timestamp is None:
            timestamp = datetime.now(timezone.utc)

        # Create date directory
        date_str = timestamp.strftime("%Y%m%d")
        date_dir = self.output_dir / date_str
        date_dir.mkdir(parents=True, exist_ok=True)

        # File paths
        time_str = timestamp.strftime("%H%M%S")
        final_path = date_dir / f"{time_str}.parquet"
        temp_path = date_dir / f"{time_str}.parquet.tmp"

        # Convert to DataFrame
        df = pd.DataFrame(snapshots)

        # Atomic write: temp file -> rename
        try:
            df.to_parquet(temp_path, index=False, compression="snappy")
            temp_path.rename(final_path)

            self.files_written += 1
            self.records_written += len(snapshots)

            self.logger.info(f"Wrote {len(snapshots)} records to {date_str}/{time_str}.parquet")
            return final_path

        except Exception as e:
            self.logger.error(f"Failed to write {final_path}: {e}")
            # Clean up temp file if it exists
            if temp_path.exists():
                try:
                    temp_path.unlink()
                except Exception:
                    pass
            raise

    def get_stats(self) -> dict:
        """Return statistics about stored data."""
        # Find all date directories
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


def load_collected_data(data_dir: Path) -> pd.DataFrame:
    """
    Load all collected parquet files into a single DataFrame.

    This is a simple helper for analysis - just concatenates all files.

    Args:
        data_dir: Path to the data/raw directory

    Returns:
        Combined DataFrame with all snapshots
    """
    parquet_files = sorted(data_dir.glob("**/*.parquet"))

    if not parquet_files:
        return pd.DataFrame()

    dfs = [pd.read_parquet(f) for f in parquet_files]
    return pd.concat(dfs, ignore_index=True)

# ============================================================================
# MAIN COLLECTOR
# ============================================================================

class ContinuousCollector:
    """Main collector class that orchestrates the collection process."""

    def __init__(
        self,
        config_path: Path,
        output_dir: Path,
        log_dir: Path,
        state_dir: Path = Path("state"),
        interval_seconds: int = DEFAULT_INTERVAL_SECONDS,
    ):
        self.config_path = config_path
        self.output_dir = output_dir
        self.state_dir = state_dir
        self.interval_seconds = interval_seconds
        self.running = False
        self.snapshot_count = 0
        self.error_count = 0
        self.start_time: datetime | None = None

        # Setup logging
        self.logger = setup_logging(log_dir)

        # Load config
        self.config = self._load_config()
        self.markets = self.config.get("markets", [])

        # Setup data writer (atomic per-cycle files)
        self.writer = DataWriter(output_dir, self.logger)

        # Track market state for transition detection
        self.market_was_active: dict[str, bool] = {}  # slug -> was_active_last_cycle

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _load_config(self) -> dict:
        """Load market configuration from JSON file."""
        self.logger.info(f"Loading config from: {self.config_path}")
        with open(self.config_path, "r") as f:
            config = json.load(f)

        markets = config.get("markets", [])
        self.logger.info(f"Loaded {len(markets)} markets")

        # Log market groups
        groups = {}
        for m in markets:
            group = m.get("group", "ungrouped")
            groups[group] = groups.get(group, 0) + 1
        for group, count in groups.items():
            self.logger.info(f"  - {group}: {count} markets")

        return config

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        self.logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.running = False

    def _write_heartbeat(self, books_received: int) -> None:
        """Write heartbeat file for external monitoring."""
        heartbeat = {
            "collector": "orderbooks",
            "last_success_utc": datetime.now(timezone.utc).isoformat(),
            "cycle": self.snapshot_count,
            "books": books_received,
            "errors": self.error_count,
        }
        self.state_dir.mkdir(parents=True, exist_ok=True)
        hb_path = self.state_dir / "heartbeat_orderbooks.json"
        tmp_path = self.state_dir / "heartbeat_orderbooks.json.tmp"
        try:
            with open(tmp_path, "w") as f:
                json.dump(heartbeat, f)
            tmp_path.rename(hb_path)
        except Exception:
            pass  # heartbeat is best-effort

    def _validate_markets(self) -> tuple[int, int]:
        """
        Pre-flight validation: check that all token IDs return valid responses.

        Returns:
            (valid_count, invalid_count)
        """
        self.logger.info("Validating market token IDs...")
        valid = 0
        invalid = 0

        for market in self.markets:
            slug = market["slug"]
            yes_token = market["tokens"]["yes"]

            # Just check YES token (if YES works, NO likely works too)
            book = fetch_order_book(yes_token, self.logger)

            if book is not None:
                valid += 1
                self.market_was_active[slug] = True
            else:
                invalid += 1
                self.market_was_active[slug] = False
                self.logger.warning(f"VALIDATION FAILED: {slug} - token may be invalid or market resolved")

        self.logger.info(f"Validation complete: {valid} valid, {invalid} invalid")
        return valid, invalid

    def _check_state_transitions(self, snapshots: list[dict]) -> None:
        """Log when markets transition from active to inactive (potential resolution)."""
        for snap in snapshots:
            slug = snap["slug"]
            is_active = snap["market_active"]
            was_active = self.market_was_active.get(slug, True)

            if was_active and not is_active:
                self.logger.warning(
                    f"MARKET WENT INACTIVE: {slug} - "
                    f"may have resolved or token ID changed"
                )
            elif not was_active and is_active:
                self.logger.info(f"Market became active: {slug}")

            self.market_was_active[slug] = is_active

    def _log_status(self) -> None:
        """Log current collection status."""
        if not self.start_time:
            return

        runtime = datetime.now(timezone.utc) - self.start_time
        hours = runtime.total_seconds() / 3600
        stats = self.writer.get_stats()

        self.logger.info(
            f"Status: {self.snapshot_count} snapshots | "
            f"{stats['total_records']} records | "
            f"{stats['total_size_mb']:.1f} MB | "
            f"{self.error_count} errors | "
            f"Runtime: {hours:.1f}h"
        )

    def _next_aligned_time(self, now: datetime) -> datetime:
        """
        Calculate next wall-clock aligned collection time.

        Aligns to interval boundaries (e.g., :00, :05, :10 for 5-min interval).
        """
        interval = self.interval_seconds
        timestamp = now.timestamp()
        next_boundary = ((timestamp // interval) + 1) * interval
        return datetime.fromtimestamp(next_boundary, tz=timezone.utc)

    def run(self) -> None:
        """Main collection loop with wall-clock aligned scheduling."""
        self.running = True
        self.start_time = datetime.now(timezone.utc)

        self.logger.info("=" * 60)
        self.logger.info("CONTINUOUS CLOB COLLECTOR STARTED")
        self.logger.info("=" * 60)
        self.logger.info(f"Schema version: {SCHEMA_VERSION}")
        self.logger.info(f"Markets: {len(self.markets)}")
        self.logger.info(f"Interval: {self.interval_seconds} seconds ({self.interval_seconds/60:.1f} minutes)")
        self.logger.info(f"Output: {self.output_dir}")
        self.logger.info(f"Start time: {self.start_time.isoformat()}")
        self.logger.info("=" * 60)

        # Pre-flight validation
        valid, invalid = self._validate_markets()
        if invalid > 0:
            self.logger.warning(f"Starting with {invalid} invalid markets - check logs above")
        self.logger.info("=" * 60)

        # Calculate first collection time (next wall-clock boundary)
        next_scheduled = self._next_aligned_time(datetime.now(timezone.utc))
        self.logger.info(f"First collection scheduled at: {next_scheduled.strftime('%H:%M:%S')} UTC")

        while self.running:
            # Wait until scheduled time
            now = datetime.now(timezone.utc)
            wait_seconds = (next_scheduled - now).total_seconds()

            if wait_seconds > 0:
                self.logger.debug(f"Waiting {wait_seconds:.1f}s until {next_scheduled.strftime('%H:%M:%S')} UTC...")
                # Sleep in small increments to respond to shutdown signals
                wait_until = time.time() + wait_seconds
                while self.running and time.time() < wait_until:
                    time.sleep(min(1.0, wait_until - time.time()))

            if not self.running:
                break

            # Record scheduled time for this cycle
            scheduled_time = next_scheduled

            try:
                # Collect snapshot
                self.logger.info(f"[Snapshot {self.snapshot_count + 1}] Collecting {len(self.markets)} markets...")
                snapshots, batch_info = collect_snapshot(
                    self.markets,
                    self.logger,
                    scheduled_time=scheduled_time,
                )

                # Log batch health
                if batch_info["http_success"]:
                    self.logger.info(
                        f"[Snapshot {self.snapshot_count + 1}] Batch OK: "
                        f"{batch_info['books_received']}/{batch_info['books_requested']} books "
                        f"({batch_info['cycle_success_rate']:.0%}) in {batch_info['elapsed_ms']:.0f}ms"
                    )
                else:
                    self.logger.warning(
                        f"[Snapshot {self.snapshot_count + 1}] Batch FAILED: "
                        f"http_status={batch_info['http_status']}"
                    )

                # Count active markets and check for state transitions
                active_count = sum(1 for s in snapshots if s["market_active"])
                self.logger.debug(f"Active markets: {active_count}/{len(snapshots)}")
                self._check_state_transitions(snapshots)

                # Write data atomically (one file per cycle)
                # Use scheduled time for consistent file naming
                self.writer.write(snapshots, timestamp=scheduled_time)

                self.snapshot_count += 1

                # Write heartbeat for external monitoring
                self._write_heartbeat(batch_info['books_received'])

                # Log status every 12 snapshots (hourly at 5-min intervals)
                if self.snapshot_count % 12 == 0:
                    self._log_status()

            except Exception as e:
                self.error_count += 1
                self.logger.exception(f"Error during collection: {e}")

            # Schedule next collection (wall-clock aligned)
            next_scheduled = self._next_aligned_time(datetime.now(timezone.utc))

        # Graceful shutdown (no flush needed - each cycle writes atomically)
        self.logger.info("Shutting down...")
        self._log_status()
        self.logger.info("=" * 60)
        self.logger.info("COLLECTOR STOPPED")
        self.logger.info("=" * 60)

# ============================================================================
# CLI
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Continuous CLOB Order Book Collector for Polymarket",
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
        default=Path("state"),
        help="Directory for heartbeat state file",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=DEFAULT_INTERVAL_SECONDS,
        help="Collection interval in seconds",
    )

    args = parser.parse_args()

    # Validate config exists
    if not args.config.exists():
        print(f"Error: Config file not found: {args.config}")
        sys.exit(1)

    # Create and run collector
    collector = ContinuousCollector(
        config_path=args.config,
        output_dir=args.output_dir,
        log_dir=args.log_dir,
        state_dir=args.state_dir,
        interval_seconds=args.interval,
    )
    collector.run()

if __name__ == "__main__":
    main()
