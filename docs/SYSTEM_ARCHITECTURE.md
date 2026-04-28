# System Architecture

A detailed technical description of the data collection system: what it collects, how it collects it, what design problems were encountered, and how they were solved. Written as a methods reference for the thesis.

---

## 1. Objective

The system collects two complementary datasets from the Polymarket prediction market platform:

1. **Order book snapshots** — the full depth of the central limit order book (CLOB) for both YES and NO outcome tokens, captured at regular intervals
2. **Trade records** — every executed trade, with price, size, aggressor side, outcome token, and on-chain transaction hash

These two datasets, when combined, enable analysis of market microstructure: how liquidity is provided (order book depth, spreads, imbalance), how it is consumed (trade flow, order flow imbalance), and how the two interact (price impact, information incorporation).

---

## 2. Platform and API Architecture

### 2.1 Polymarket's Market Structure

Polymarket is a blockchain-based prediction market platform. Each market poses a binary question (e.g., "Will there be no change in Fed interest rates after the March 2026 meeting?") and has two outcome tokens: YES and NO. These tokens trade on a central limit order book (CLOB) operated by Polymarket, with settlement on the Polygon blockchain.

Each market has three key identifiers:
- **`condition_id`**: an on-chain hash (e.g., `0x257b18...`) that uniquely identifies the market. Immutable.
- **`token_id`**: a large integer (e.g., `102559817034...`) that uniquely identifies each outcome token (one for YES, one for NO). Immutable.
- **`slug`**: a human-readable URL fragment (e.g., `will-there-be-no-change-in-fed-interest-rates-after-the-march-2026-meeting`). Mutable — Polymarket can change this at any time.

The collection system uses only `condition_id` (for trades) and `token_id` (for order books) in API calls. Slugs are stored in the output for readability but are never used as query parameters.

### 2.2 APIs Used

**CLOB API** (`clob.polymarket.com`)
- Endpoint: `GET /book?token_id=<TOKEN_ID>`
- Authentication: none required for read-only book queries
- Returns: full order book (all bid and ask levels with price and size)
- Rate limits: generous; 26 requests per 5-minute cycle causes no issues
- Behavior: returns current state; no caching layer observed

**Data API** (`data-api.polymarket.com`)
- Endpoint: `GET /trades?asset_id=<CONDITION_ID>`
- Authentication: none
- Returns: a fixed sliding window of the ~3,500 most recent trades for the given market, sorted descending by timestamp
- Rate limits: not observed at our request volume
- **Critical behavior**: sits behind aggressive CDN caching (see Section 4.1)
- **Limitation**: no time-based filtering. Parameters like `after`, `before`, `since` are accepted syntactically but silently ignored. The API always returns the same ~3,500-trade window regardless of query parameters.

### 2.3 Market Selection

Thirteen markets were selected across eight event groups, classified by information concentration:

| Category | Groups | Markets | Rationale |
|----------|--------|---------|-----------|
| **Concentrated** | US-Iran strikes, Fed decision, Bitcoin price, Khamenei, SpaceX IPO, Gold price, Starmer | 7 | Information arrives through identifiable channels (policy announcements, price feeds, intelligence reports) |
| **Diffuse** | Ukraine ceasefire, US-Iran deal, Colombia election, Brazil election, Texas primary, Second-largest company | 6 | Information is distributed across many sources with no single dominant signal |

The market configuration is stored in `config/thesis_markets.json` as a hand-maintained list of thirteen entries, each carrying the slug, `condition_id`, `gamma_market_id`, YES / NO token IDs, end date, event-group label, and information-concentration classification. Identifiers were resolved from slugs via Polymarket's Gamma API at selection time and then frozen into the config file; the collectors read this file directly and never query Gamma at runtime.

---

## 3. Order Book Collector

### 3.1 Collection Logic

The order book collector (`scripts/continuous_collector.py`) runs in a continuous loop aligned to wall-clock boundaries (e.g., at :00, :05, :10, :15, etc. of each hour at the default 5-minute interval).

Each cycle:
1. Fetch the order book for each token (YES and NO) of each market — 26 API calls total
2. For each order book, compute microstructure metrics:
   - Mid-price: `(best_bid + best_ask) / 2`
   - Spread (absolute and relative)
   - Best bid/ask size
   - Total bid/ask depth (sum of all level sizes)
   - Number of price levels on each side
   - Book imbalance: `(total_bid - total_ask) / (total_bid + total_ask)`
   - Depth at various percentage thresholds (1%, 2%, 5%, 10%, 20% from mid)
3. Run quality checks (missing side, crossed book, price out of range, malformed levels) and flag anomalies
4. Write all 13 market snapshots to a single Parquet file
5. Update the heartbeat file
6. Sleep until the next wall-clock boundary

### 3.2 Pre-Flight Validation

On startup, the collector tests each token_id by making a real API call. Markets that fail validation are logged as warnings but do not prevent the collector from starting. This catches stale token IDs from resolved markets.

### 3.3 State Transition Detection

The collector tracks whether each market was active in the previous cycle. If a market transitions from active to inactive (or vice versa), a warning is logged. This detects market resolutions during the collection period.

### 3.4 Output Schema

Each Parquet file contains one row per market (13 rows per file) with the following columns:

**Metadata columns:**
`schema_version`, `timestamp_scheduled_utc`, `timestamp_collected_utc`, `timestamp_unix`, `cycle_http_success`, `cycle_success_rate`, `market_id`, `slug`, `question`, `group`, `deterministic`, `insider_risk`, `end_time`, `market_active`

**Per-side metrics** (prefixed `yes_` and `no_`, 22 columns each):
`book_available`, `mid_price`, `best_bid_price`, `best_ask_price`, `spread_absolute`, `spread_relative`, `best_bid_size`, `best_ask_size`, `depth_at_best`, `total_bid_depth`, `total_ask_depth`, `bid_levels`, `ask_levels`, `book_imbalance`, `depth_bid_5pct`, `depth_ask_5pct`, `depth_bid_10pct`, `depth_ask_10pct`, `depth_bid_20pct`, `depth_ask_20pct`

**Quality flags** (per side):
`qa_missing_side`, `qa_crossed_book`, `qa_price_out_of_range`, `qa_malformed_levels`

---

## 4. Trades Collector

### 4.1 The CDN Caching Problem

The Data API's `/trades` endpoint sits behind a content delivery network (CDN) that aggressively caches responses. This was the central engineering challenge of the trades collector.

**Discovery process.** Initial testing showed that the first collection cycle would retrieve ~3,500 trades, but all subsequent cycles returned zero new trades — even though the market was actively trading. Systematic investigation revealed:

1. Standard `Cache-Control: no-cache` and `Pragma: no-cache` HTTP headers are **completely ignored** by the CDN.
2. A query parameter with a fixed name but varying value (e.g., `_=1234567890`) sometimes bypasses the cache, but not reliably — the CDN appears to cache by parameter name structure, not just the full URL.
3. The only reliable cache-busting technique is to use a **randomly-generated parameter name** on every request (e.g., `x7fk2=1707849600000`). Since the CDN has never seen this parameter name before, it cannot serve a cached response.

**Implementation.** Every API request to the Data API appends a parameter with a random 6-character name (prefix `x` + 5 random alphanumeric characters) and the current millisecond timestamp as the value. This guarantees a unique URL on every request.

### 4.2 Watermark-Based Incremental Collection

Since the API returns the same ~3,500-trade window on every call (with no time-based filtering), the collector must determine which trades are new since the last cycle.

**Composite watermark.** Each market maintains a watermark consisting of two fields: `(timestamp, transaction_hash)`. After each successful cycle, the watermark is updated to the newest trade's timestamp and hash. On the next cycle, incoming trades are compared against the watermark using lexicographic comparison: a trade is "new" if its `(timestamp, transaction_hash)` tuple is strictly greater than the stored watermark.

This composite approach solves two problems that a timestamp-only watermark cannot:
1. **Second-level collisions**: multiple trades can share the same Unix timestamp. A timestamp-only watermark would either miss trades (if it skips the shared timestamp) or duplicate them (if it re-includes the shared timestamp).
2. **Multi-fill trades**: a single order can generate multiple fills with the same timestamp but different transaction hashes.

**Overlap-and-filter strategy.** Each cycle fetches trades starting from the most recent and works backward through the API's offset pagination. Once a trade at or below the watermark is encountered, fetching stops. Any trades at exactly the watermark timestamp are filtered using the transaction hash to avoid re-including already-collected trades.

### 4.3 Two-Tier Page Size Optimization

To minimize API calls on typical incremental cycles (where only a few new trades exist per market), the collector uses a two-tier fetching strategy:

1. **First page**: fetch with `limit=50`. This is small enough that for most markets in most cycles, it will contain a mix of new and already-seen trades, and the collector can stop after one page.
2. **Escalation**: if all 50 trades on the first page are new (indicating a burst of trading activity), switch to `limit=500` and continue paginating until the watermark is reached.

This reduces the typical incremental cycle from ~88 API pages (at a fixed 500-per-page) to ~27 pages across 13 markets — approximately one small probe per market plus overhead.

### 4.4 Double-Probe Freshness Check

Before concluding that a market has no new trades, the collector makes two separate API requests with different cache-busting parameters. This guards against the possibility that a single request hits a stale CDN edge node. Only if both probes return data at or below the watermark does the collector conclude there are no new trades.

### 4.5 Watermark Persistence

Watermarks are persisted to `state/trades_watermarks.json` after every successful cycle using atomic writes (write to temp file, then rename). On startup, the collector loads existing watermarks from disk, enabling it to resume incremental collection after a restart without re-collecting the full backfill.

The watermark file format:
```json
{
  "0x98c0aa27e741d5bfeb...": {
    "timestamp": 1771055927,
    "transaction_hash": "0xfe102c30bad279c0a5ad..."
  },
  ...
}
```

Keys are `condition_id` values (immutable on-chain market identifiers).

### 4.6 Trade Data Schema

Each trade record contains:

| Field | Description |
|-------|-------------|
| `condition_id` | Market identifier (on-chain hash) |
| `slug` | Market URL slug (for readability) |
| `market_title` | Full question text |
| `side` | `BUY` or `SELL` (taker's perspective) |
| `size` | Trade size in outcome tokens |
| `price` | Execution price (0 to 1) |
| `outcome` | `Yes` or `No` |
| `outcome_index` | `0` (YES) or `1` (NO) |
| `transaction_hash` | On-chain transaction hash |
| `maker_address` | Ethereum address of the maker (resting order) |
| `timestamp_scheduled_utc` | Collection cycle timestamp |
| `timestamp_collected_utc` | Actual collection time |
| `trade_timestamp_unix` | Trade execution time (Unix seconds) |

**Note on trade sidedness.** The API reports each trade from the **taker's perspective** only. A `BUY` record means the taker aggressively lifted the ask (bought outcome tokens); a `SELL` means the taker hit the bid (sold outcome tokens). The maker (counterparty) is not reported as a separate record. For order flow analysis, the taker's side is the relevant signal: it identifies the aggressive party whose action moves the market.

---

## 5. Shared Infrastructure

### 5.1 Retry and Error Handling

Both collectors implement exponential backoff with jitter for transient errors:
- Base delay: 2 seconds, doubling on each retry, capped at 60 seconds
- Random jitter (0-1 seconds) added to prevent thundering herd
- Maximum 3 retries per request
- HTTP 429 (rate limit) triggers backoff with optional `Retry-After` header respect
- Connection errors, timeouts, and HTTP errors all trigger retries
- JSON decode errors are treated as permanent failures (no retry)

### 5.2 Atomic Writes

All persistent writes (Parquet data files, watermark state, heartbeat files) use the same atomic pattern:
1. Write to a temporary file (e.g., `HHMMSS.parquet.tmp`)
2. Rename the temporary file to the final path (e.g., `HHMMSS.parquet`)

On POSIX systems, `rename()` is atomic — the file either appears with its full contents or not at all. This guarantees that a crash or power loss during a write cannot produce a corrupt or partial file.

If the write fails (e.g., disk full), the temporary file is cleaned up in a `finally` block.

### 5.3 Signal Handling and Graceful Shutdown

Both collectors register handlers for `SIGINT` (Ctrl-C) and `SIGTERM` (systemd stop). The handler sets a `running = False` flag. The main loop's sleep phase checks this flag every second (sleep is implemented as a loop of 1-second sleeps), so the collector responds to shutdown signals within 1 second.

On shutdown:
- The trades collector saves watermarks to disk
- Both collectors log a final status summary (total cycles, records, errors, runtime)
- The current cycle is allowed to complete if already in progress

### 5.4 Wall-Clock Aligned Scheduling

Both collectors align their collection times to wall-clock boundaries. At a 300-second (5-minute) interval, collections occur at :00, :05, :10, :15, etc. of each hour. This ensures:
- Predictable file naming (`HHMMSS.parquet`)
- Consistent intervals in the time series
- Easy alignment between order book and trade data during analysis

### 5.5 Logging

Both collectors write to two destinations:
1. **Application log files**: `logs/collector_YYYYMMDD.log` and `logs/trades_collector_YYYYMMDD.log`, with daily rotation
2. **systemd journal**: via `StandardOutput=journal` in the service definition, with `PYTHONUNBUFFERED=1` ensuring immediate flush

Log levels:
- `INFO`: cycle start/end, data volumes, periodic status summaries (every 12 cycles / 1 hour)
- `WARNING`: transient errors (timeouts, rate limits, connection resets), market state transitions
- `ERROR`: persistent failures (all retries exhausted), unexpected exceptions

### 5.6 Data Compression

All Parquet files use Snappy compression. Snappy provides a good balance of compression ratio and speed, and is the default for Parquet. It is also the most widely supported Parquet compression codec across analysis tools.

---

## 6. Configuration

### 6.1 Market Configuration File

`config/thesis_markets.json` contains the full specification of all 13 markets:

```json
{
  "meta": {
    "purpose": "Master thesis data collection",
    "collection_period": "2-3 weeks",
    "snapshot_interval_seconds": 300,
    "trades_interval_seconds": 300,
    "total_markets": 13,
    "concentrated_markets": 7,
    "diffuse_markets": 6
  },
  "markets": [
    {
      "gamma_market_id": "654414",
      "condition_id": "0x257b18...",
      "slug": "will-there-be-no-change-in-fed-interest-rates-after-the-march-2026-meeting",
      "question": "Will there be no change in Fed interest rates after the March 2026 meeting?",
      "tokens": {
        "yes": "102559817034...",
        "no": "114350083305..."
      },
      "end_date": "2026-03-18",
      "group": "Fed Decision In March",
      "information_concentration": "concentrated"
    },
    ...
  ]
}
```

### 6.2 Runtime Dependencies

```
requests>=2.31,<3    # HTTP client
pandas>=2.0,<4       # DataFrame construction
pyarrow>=14,<24      # Parquet I/O
```

No database, message queue, or other infrastructure is required. The system is fully file-based.

---

## 7. Production Environment

### 7.1 Infrastructure

| Component | Specification |
|-----------|--------------|
| Provider | DigitalOcean |
| VM type | Basic Droplet |
| OS | Ubuntu 24.04 LTS |
| CPU | 1 vCPU (shared) |
| RAM | 1 GB |
| Storage | 25 GB SSD |
| Python | 3.12 |
| Process management | systemd |
| Monitoring | Heartbeat files + cron + DigitalOcean dashboard |

### 7.2 Observed Resource Utilization (after 12 hours)

| Resource | Utilization |
|----------|-------------|
| CPU | ~0.2% baseline, 2-3% peaks during collection cycles |
| Memory | ~48% steady state |
| Disk | ~12% used |
| Network | <5 kb/s average |

### 7.3 Data Volume Estimates

At 13 markets with 5-minute intervals:
- **Order books**: 288 files/day, each containing 13 rows — estimated ~10-20 MB/day
- **Trades**: 288 files/day (when new trades exist), variable row counts — depends on market activity
- **Logs**: ~10-30 KB/day per collector

Over a 3-week collection period, total data volume is expected to remain well within the 25 GB disk allocation.

---

## 8. Known Limitations and Failure Modes

### 8.1 API Limitations

- **No historical backfill**: the trades API exposes only a ~52-hour sliding window. Data before the collection start is unavailable.
- **No real-time feed**: both APIs are polled; events between polls are unobserved. At 5-minute intervals, sub-5-minute order book dynamics and the exact sequencing of trades within a window are not captured.
- **Trade window overflow**: if a single market produces >3,500 trades in less than ~52 hours, older trades fall off the API window before the next collection cycle. At 5-minute intervals with the observed trade volumes, this has not occurred.
- **Taker-side only**: trade records represent the taker's perspective. The maker (resting order) is identified by address but does not appear as a separate record.

### 8.2 Collector Limitations

- **No deduplication across restarts**: if watermarks are deleted and the collector restarts, the first cycle will re-collect the full ~3,500-trade backfill, creating duplicates with previously collected data. Downstream deduplication by `transaction_hash` (or the full dedupe key) is required.
- **Single-threaded**: both collectors make API calls sequentially. At 26 calls per cycle (order books) or 13+ calls per cycle (trades), cycle completion time is typically 2-10 seconds — well within the 5-minute interval.
- **No alerting**: the monitoring cron writes to a local log file but does not send notifications (email, SMS, Slack). Manual checking is required.

### 8.3 Infrastructure Risks

- **VM outage**: DigitalOcean does not offer an SLA for Basic Droplets. An unscheduled reboot would cause a brief data gap (systemd restarts services on boot). No such outage occurred during the collection period.
- **Disk failure**: data is not replicated. Periodic `rsync` to the local machine provides a backup.
