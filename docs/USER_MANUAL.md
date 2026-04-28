# Polymarket Thesis Data Collection System

## System Manual

---

## Table of Contents

1. [Overview](#1-overview)
2. [Project Structure](#2-project-structure)
3. [How It Works](#3-how-it-works)
4. [Running the Collectors](#4-running-the-collectors)
5. [Data Output](#5-data-output)
6. [Monitoring & Recovery](#6-monitoring--recovery)
7. [Risks & Limitations](#7-risks--limitations)
8. [Deployment](#8-deployment)

---

## 1. Overview

This system collects two types of data from Polymarket for academic research on prediction market microstructure:

1. **Order book snapshots** — CLOB bid/ask depth for both YES and NO tokens
2. **Trades** — all executed trades with price, size, side, and transaction hash

### Key Numbers

| Parameter | Value |
|-----------|-------|
| Markets | 13 (across 8 event groups) |
| Collection interval | 5 minutes (wall-clock aligned) |
| Planned duration | 2-3 weeks continuous |
| Dependencies | Python 3.10+, requests, pandas, pyarrow |

### Design Principles

| Principle | Implementation |
|-----------|----------------|
| **No data loss** | Atomic writes (temp file then rename); crash mid-write leaves no corrupt files |
| **Observability** | Heartbeat files, daily log rotation, status logging every hour |
| **Fault tolerance** | Automatic restart via systemd; retries with exponential backoff on API errors |
| **Simplicity** | Two Python scripts, three pip packages, no database |
| **Deduplication** | Composite watermark `(timestamp, transaction_hash)` prevents duplicate trades |

---

## 2. Project Structure

```
polymarket_thesis/
├── scripts/
│   ├── continuous_collector.py       # Order book collector (CLOB API)
│   ├── trades_collector.py           # Trades collector (Data API)
│   ├── data_utils.py                 # Shared loaders / constants for analysis
│   ├── descriptives.py               # Market summary table and descriptives
│   ├── trade_hour_clustering.py      # Hour-of-day trade-clustering figure
│   ├── h1_regression.py              # H1: lagged OFI → return
│   ├── h2_regression.py              # H2: whale vs. non-whale price impact
│   ├── h3_regression.py              # H3: lagged |OFI| → depth withdrawal
│   └── robustness_volume_buckets.py  # H1 robustness (volume-bucketed intervals)
│
├── config/
│   └── thesis_markets.json       # 13 markets with condition_ids and token_ids
│
├── bin/
│   ├── run_collector.sh          # Order book launcher (venv + args)
│   ├── run_trades_collector.sh   # Trades launcher (venv + args)
│   └── run_all.sh                # Combined launcher (local testing only)
│
├── data/
│   ├── from_server/              # LOCAL analysis input: rsynced copy of the VM's data/raw/
│   │   └── trades/               #   Trade Parquet files mirrored from data/raw/trades/
│   └── processed/                # Derived results (panels, regression CSVs)
│
├── state/
│   ├── trades_watermarks.json    # Persistent watermark per market
│   ├── heartbeat_orderbooks.json # Last successful orderbook cycle
│   └── heartbeat_trades.json     # Last successful trades cycle
│
├── logs/
│   ├── collector_YYYYMMDD.log    # Order book collector daily log
│   └── trades_collector_YYYYMMDD.log  # Trades collector daily log
│
├── docs/
│   ├── USER_MANUAL.md            # This file
│   ├── CLOUD_DEPLOYMENT.md       # VM deployment guide
│   ├── SYSTEM_ARCHITECTURE.md    # Architectural reference
│   ├── PROCESSING_CHECKLIST.md   # Post-collection analysis checklist
│   └── data_collection_section.tex  # Thesis subsection on the pipeline
│
├── output/
│   ├── tables/                   # LaTeX tables consumed by the thesis
│   └── figures/                  # Figures consumed by the thesis
│
├── tests/                        # Unit tests for the collectors
└── requirements.txt              # Python dependencies
```

---

## 3. How It Works

### Order Book Collector (`continuous_collector.py`)

**API:** `clob.polymarket.com/book?token_id=<TOKEN_ID>` (public, no auth)

Each cycle:
1. Fetch YES and NO order books for all 13 markets (26 API calls)
2. Calculate microstructure metrics (mid-price, spread, depth, imbalance)
3. Write all snapshots to a single Parquet file (atomic: temp then rename)
4. Update heartbeat file
5. Sleep until next 5-minute boundary

**Pre-flight validation:** On startup, tests each token_id against the API. Logs warnings for any that fail (market may have resolved).

**State transitions:** Detects when markets go active/inactive between cycles and logs warnings.

### Trades Collector (`trades_collector.py`)

**API:** `data-api.polymarket.com/trades?asset_id=<CONDITION_ID>` (public, no auth)

The API returns a fixed sliding window of ~3,500 most recent trades per market, sorted descending by timestamp. There is no filtering — no `after`, `before`, or `since` parameters.

Each cycle:
1. For each market, fetch the first page of trades (limit=50)
2. Compare against the stored watermark `(timestamp, transaction_hash)`
3. If the entire page is new (burst scenario), escalate to limit=500 and paginate deeper
4. Collect all trades newer than the watermark
5. Update the watermark to the newest trade
6. Write all new trades to a single Parquet file (atomic)
7. Persist watermarks to disk (atomic)
8. Update heartbeat file

**Cache busting:** The API sits behind aggressive CDN caching that ignores Cache-Control headers. Every request uses a randomly-named query parameter (e.g., `x7fk2=<timestamp>`) to guarantee a unique URL that bypasses the cache.

**Double-probe freshness:** Before deciding a market has no new trades, the collector makes two separate requests with different cache-busting parameters to confirm.

**Deduplication:** Composite watermark `(timestamp, transaction_hash)` with lexicographic comparison handles second-level timestamp collisions and multi-fill trades. Downstream dedupe key: `[condition_id, trade_timestamp_unix, transaction_hash, outcome_index, side, price, size]`.

### Identifiers

Both collectors use **immutable on-chain identifiers**, not slugs:
- Order books use `token_id` (64-digit number, one per YES/NO outcome)
- Trades use `condition_id` (0x-prefixed hash, one per market)

Slugs appear in output for readability but are never used for API calls. If Polymarket renames a market's URL, collection continues unaffected.

---

## 4. Running the Collectors

### Manual / Testing

```bash
source .venv/bin/activate

# Order books (90s interval for testing)
python scripts/continuous_collector.py \
  --config config/thesis_markets.json \
  --output-dir data/raw --log-dir logs --state-dir state \
  --interval 90

# Trades (90s interval for testing)
python scripts/trades_collector.py \
  --config config/thesis_markets.json \
  --output-dir data/raw/trades --log-dir logs --state-dir state \
  --interval 90
```

Or use the launcher scripts (activate venv automatically):
```bash
./bin/run_collector.sh --interval 90
./bin/run_trades_collector.sh --interval 90
```

### Production (systemd)

See `docs/CLOUD_DEPLOYMENT.md` for full setup. In production, each collector runs as an independent systemd service with `Restart=always`.

### CLI Options

Both collectors accept:

| Flag | Default | Description |
|------|---------|-------------|
| `--config` | `config/thesis_markets.json` | Market configuration file |
| `--output-dir` | `data/raw` or `data/raw/trades` | Parquet output directory |
| `--log-dir` | `logs` | Log file directory |
| `--state-dir` | `state` | Heartbeat and watermark directory |
| `--interval` | 300 | Collection interval in seconds |

### Startup Sequence (both collectors)

1. Load market config
2. Setup logging (daily rotation)
3. Register SIGINT/SIGTERM handlers
4. Pre-flight validation (order books only)
5. Calculate first wall-clock-aligned collection time
6. Enter main loop

### Graceful Shutdown

Both collectors handle SIGINT and SIGTERM:
- Set `running = False`
- Current cycle completes (sleep is in 1-second increments)
- Trades collector saves watermarks
- Final status logged

Since every cycle writes atomically, a crash (SIGKILL) loses at most one in-progress cycle, never corrupts existing data.

---

## 5. Data Output

> **`data/raw/` vs. `data/from_server/`.** The collectors always write to `data/raw/` **on whichever machine they run on**. In production that machine is the cloud VM described in `CLOUD_DEPLOYMENT.md`; its `data/raw/` tree is the single source of truth. For local analysis the VM's `data/raw/` is rsynced into `data/from_server/` on the analyst's workstation (`data/raw/` does not exist locally — it is excluded from the repo). All analysis scripts read from `data/from_server/`; no script ever reads from a local `data/raw/`. The paths below describe the VM-side layout produced by the collectors; substitute `data/from_server/` when reading from the rsynced local copy.

### Order Book Snapshots

**Location:** `data/raw/YYYYMMDD/HHMMSS.parquet`
**Frequency:** 1 file per 5-minute cycle
**Rows per file:** 13 (one per market)
**Compression:** Snappy

Key columns:

| Column | Description |
|--------|-------------|
| `condition_id` | Market identifier |
| `slug` | Market URL slug (for readability) |
| `group` | Event group name |
| `market_active` | Whether market is still tradeable |
| `yes_mid_price` | (best_bid + best_ask) / 2 for YES token |
| `yes_spread_absolute` | ask - bid |
| `yes_spread_relative` | spread / mid_price |
| `yes_total_bid_depth` | Sum of all bid sizes |
| `yes_book_imbalance` | (bid - ask) / total depth |
| `no_*` | Same metrics for NO token |
| `yes_depth_bid_5pct` | Bid depth within 5% of mid |
| `qa_*` | Quality flags (missing side, crossed book, etc.) |

### Trade Data

**Location:** `data/raw/trades/YYYYMMDD/HHMMSS.parquet`
**Frequency:** 1 file per 5-minute cycle (only if new trades exist)
**Rows per file:** Varies (all new trades across all 13 markets)
**Compression:** Snappy

Key columns:

| Column | Description |
|--------|-------------|
| `condition_id` | Market identifier |
| `slug` | Market URL slug |
| `side` | BUY or SELL |
| `price` | Execution price (0-1) |
| `size` | Trade size |
| `outcome_index` | 0 (YES) or 1 (NO) |
| `trade_timestamp_unix` | Execution time (unix seconds) |
| `transaction_hash` | On-chain transaction hash |

### Loading Data

```python
import pandas as pd
from pathlib import Path

# All order book snapshots
ob_files = sorted(Path("data/raw").glob("[0-9]*/*.parquet"))
df_ob = pd.concat([pd.read_parquet(f) for f in ob_files], ignore_index=True)

# All trades
trade_files = sorted(Path("data/raw/trades").glob("[0-9]*/*.parquet"))
df_trades = pd.concat([pd.read_parquet(f) for f in trade_files], ignore_index=True)

# Filter by market
fed_ob = df_ob[df_ob.slug.str.contains("fed-interest")]
fed_trades = df_trades[df_trades.slug.str.contains("fed-interest")]
```

---

## 6. Monitoring & Recovery

### Heartbeat Files

Both collectors write a heartbeat JSON to `state/` after each successful cycle:

```json
{"collector": "trades", "last_success_utc": "2026-02-13T15:05:00+00:00", "cycle": 42, "trades": 33, "pages": 28, "errors": 0}
```

If the heartbeat file hasn't been updated in >15 minutes, something is wrong.

### Quick Check (from laptop)

```bash
ssh thesis@YOUR_VM_IP 'cat ~/polymarket_thesis/state/heartbeat_*.json'
```

### Log Messages

Normal operation:
```
INFO  | [Cycle 42] Collected 33 trades (28 pages) from 13/13 markets
INFO  | [Snapshot 42] Batch OK: 26/26 books (100%) in 2340ms
```

Warnings (handled automatically):
```
WARN  | Rate limited (429), backoff=2.3s
WARN  | Timeout (attempt 1/3)
WARN  | MARKET WENT INACTIVE: some-market-slug
```

### Recovery

| Scenario | Data Loss | Recovery |
|----------|-----------|----------|
| Graceful stop (Ctrl-C / SIGTERM) | None | Restart; watermarks preserved |
| Crash / SIGKILL | At most 1 cycle | systemd restarts in 30s; watermarks from last successful cycle |
| VM reboot | Same as crash | systemd starts services on boot |
| Network outage | Gap in data | Retries with backoff; resumes on reconnect |
| API down | Gap in data | Retries with backoff; logged as errors |

### No Backfill

The trades API returns a ~52-hour sliding window. If the collector is down for >52 hours, trades in that gap are permanently lost. At 5-minute intervals this provides a ~600x safety margin.

---

## 7. Risks & Limitations

### Known Risks

| Risk | Severity | Mitigation |
|------|----------|------------|
| CDN returns stale data | High (solved) | Random parameter names on every request bypass cache |
| Market resolves mid-collection | Expected | Logged as state transition; other markets continue |
| API format changes | Low | Validation on response structure; logged as errors |
| Disk fills up | Low | Data is small; weekly `du` check recommended |
| Slug changes | None | Collectors use immutable on-chain identifiers |

### Limitations

| Limitation | Impact |
|------------|--------|
| 5-minute resolution | Cannot observe sub-5-minute dynamics |
| No historical backfill | Can only collect from "now" forward |
| ~3,500 trade window | If >3,500 trades in 5 minutes, oldest may be missed |
| Order book is snapshot | No continuous feed; order book changes between snapshots are unobserved |

---

## 8. Deployment

### Production: Cloud VM with systemd (recommended)

See `docs/CLOUD_DEPLOYMENT.md` for complete step-by-step guide.

Summary:
1. DigitalOcean 1GB droplet ($6/month)
2. Two independent systemd services
3. `Restart=always` with crash loop protection
4. Heartbeat-based monitoring cron
5. Data retrieval via `rsync`

### Local Testing

Use the launcher scripts for quick testing:
```bash
./bin/run_collector.sh --interval 90
./bin/run_trades_collector.sh --interval 90
```

Or the combined launcher (starts both as background processes):
```bash
./bin/run_all.sh --clob-interval 90 --trades-interval 90
```

---

*Last updated: 2026-02-13*
