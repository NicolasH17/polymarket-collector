[![DOI](https://zenodo.org/badge/1223693739.svg)](https://doi.org/10.5281/zenodo.19858618)

# Polymarket Data Collector

A two-process data collection system for Polymarket prediction markets,
designed to run unattended on a cloud VM. It captures order-book snapshots
and individual trades for a configurable set of markets and writes
Snappy-compressed Parquet files to disk on a fixed cadence.

Originally built to support a bachelor's thesis on intraday order-flow
informativeness; the analysis pipeline that consumes this data is kept in
a separate repository.

## What it collects

- **Order books** (`scripts/continuous_collector.py`): mid-price, best bid/ask,
  depth at best, and full top-of-book snapshots from the Polymarket CLOB,
  polled on a wall-clock-aligned 5-minute cadence.
- **Trades** (`scripts/trades_collector.py`): every executed trade on each
  configured market, polled from the Polymarket Data API with a composite
  `(timestamp, transaction_hash)` watermark to handle second-level
  collisions and multi-fill orders.

Both processes write to `data/raw/YYYYMMDD/HHMMSS.parquet`
(order books) and `data/raw/trades/YYYYMMDD/HHMMSS.parquet` (trades).

## Repository layout

```
bin/        Shell launchers for the two collectors
config/     thesis_markets.json — the market configuration consumed by both collectors
docs/       Operator manual, deployment guide, system architecture, post-collection checklist
scripts/    The two collector processes
tests/      Unit tests for collector error paths
```

## Quick start

Prerequisites: Python 3.10+, an always-on Linux host (a small cloud VM is
plenty), and outbound HTTPS to `clob.polymarket.com` and
`data-api.polymarket.com`.

```bash
git clone <this-repo> polymarket-collector && cd polymarket-collector
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Foreground run (both collectors). Use systemd for production —
# see docs/CLOUD_DEPLOYMENT.md.
./bin/run_all.sh
```

Collected Parquet files accumulate under `data/raw/`. The directory is
gitignored; sync it off the VM however you prefer (rsync works well).

## Configuration

`config/thesis_markets.json` lists the markets to collect, including the
CLOB token IDs for each outcome. To collect a different set of markets,
edit this file — both collectors read from it on startup.

## Documentation

- [`docs/USER_MANUAL.md`](docs/USER_MANUAL.md) — end-to-end operator guide
- [`docs/CLOUD_DEPLOYMENT.md`](docs/CLOUD_DEPLOYMENT.md) — VM provisioning, systemd units, rsync setup
- [`docs/SYSTEM_ARCHITECTURE.md`](docs/SYSTEM_ARCHITECTURE.md) — API details, schema, design rationale
- [`docs/PROCESSING_CHECKLIST.md`](docs/PROCESSING_CHECKLIST.md) — analyst-side checks after sync

## Notes on the Polymarket APIs

The trades collector handles a CDN-caching quirk on
`data-api.polymarket.com/trades`: standard `Cache-Control` headers are
ignored, and a fixed nonce parameter name can still hit cache. The
collector defeats this by randomising the parameter *name* on every
request (see `docs/SYSTEM_ARCHITECTURE.md` for the full discussion).

## License

Code released under the MIT License (see [`LICENSE`](LICENSE)). Collected
Polymarket data is subject to Polymarket's terms of service and is not
redistributed through this repository.
