# Data Processing Checklist

Pre-analysis checklist to ensure clean, reproducible results.

---

## Collection Log

Fill in during/after collection:

| Field | Value |
|-------|-------|
| Collection start (UTC) | _______________ |
| Collection end (UTC) | _______________ |
| Total duration | _______________ |
| Order book snapshots | _______________ |
| Trade records (raw) | _______________ |
| Known gaps/issues | _______________ |

---

## Pre-Processing Checklist

### 1. Data Integrity

- [ ] All parquet files readable (`load_collected_data()` succeeds)
- [ ] No corrupted files (check file sizes, no 0-byte files)
- [ ] Timestamps continuous (no unexpected multi-hour gaps)

```python
# Quick integrity check
from pathlib import Path
import pandas as pd

# Order books
ob_files = sorted(Path("data/raw").glob("**/*.parquet"))
print(f"Order book files: {len(ob_files)}")

# Trades
trade_files = sorted(Path("data/raw/trades").glob("**/*.parquet"))
print(f"Trade files: {len(trade_files)}")
```

### 2. Deduplication

- [ ] Trades deduplicated by `transaction_hash`

```python
# Deduplicate trades
df_trades = load_collected_trades(Path("data/raw/trades"))
print(f"Before dedup: {len(df_trades)}")
df_trades = df_trades.drop_duplicates(subset=["transaction_hash"])
print(f"After dedup: {len(df_trades)}")
```

### 3. Market Resolution Check

- [ ] Identify markets that resolved during collection
- [ ] Decide: exclude or analyze separately

```python
# Markets with end_date before collection end
import json
with open("config/thesis_markets.json") as f:
    config = json.load(f)

collection_end = "2026-03-XX"  # Fill in
for m in config["markets"]:
    if m["end_date"] < collection_end:
        print(f"RESOLVED: {m['group']} - {m['end_date']}")
```

### 4. QA Flag Review

- [ ] Check QA flags in order book data
- [ ] Decide how to handle flagged observations

```python
# QA flag summary
qa_cols = ["qa_missing_side", "qa_crossed_book", "qa_price_out_of_range", "qa_malformed_levels"]
for col in qa_cols:
    if col in df.columns:
        print(f"{col}: {df[col].sum()} flagged")
```

---

## Processing Steps

### Step 1: Load Raw Data

```python
from scripts.continuous_collector import load_collected_data
from scripts.trades_collector import load_collected_trades
from pathlib import Path

df_ob = load_collected_data(Path("data/raw"))
df_trades = load_collected_trades(Path("data/raw/trades"))
```

### Step 2: Clean Trades

```python
# Deduplicate
df_trades = df_trades.drop_duplicates(subset=["transaction_hash"])

# Parse timestamps
df_trades["trade_time"] = pd.to_datetime(df_trades["trade_timestamp_unix"], unit="s", utc=True)

# Filter to collection period
df_trades = df_trades[
    (df_trades["trade_time"] >= collection_start) &
    (df_trades["trade_time"] <= collection_end)
]
```

### Step 3: Compute OFI per 5-min Window

```python
def compute_ofi(trades_window):
    """Compute Order Flow Imbalance for a window of trades."""
    # Sign: +1 for BUY, -1 for SELL
    trades_window = trades_window.copy()
    trades_window["signed_size"] = trades_window["size"] * trades_window["side"].map({"BUY": 1, "SELL": -1})
    return trades_window["signed_size"].sum()

# Aggregate trades into 5-min windows aligned with order book snapshots
df_trades["window"] = df_trades["trade_time"].dt.floor("5min")
ofi_by_window = df_trades.groupby(["condition_id", "window"]).apply(compute_ofi)
```

### Step 4: Merge with Order Books

```python
# Match OFI to order book midpoint changes
# OFI_t predicts Δm_{t+1} = midpoint[t+1] - midpoint[t]
```

### Step 5: Save Processed Data

```python
df_ob_clean.to_parquet("data/processed/orderbooks_cleaned.parquet")
df_trades_clean.to_parquet("data/processed/trades_cleaned.parquet")
df_ofi.to_parquet("data/processed/ofi_merged.parquet")
```

---

## Analysis Checklist

- [ ] Summary statistics by market group (concentrated vs diffuse)
- [ ] Liquidity metrics: spread, depth at 1%/2%/5%
- [ ] OFI regression: `Δm_{t+1} = α + β * OFI_t + ε_t`
- [ ] Compare β coefficients across market types
- [ ] Robustness checks (different window sizes, subperiods)

---

## Common Pitfalls

| Pitfall | How to Avoid |
|---------|--------------|
| Timezone confusion | Keep everything UTC through analysis |
| Duplicate trades | Dedupe by `transaction_hash` early |
| Look-ahead bias | OFI_t must only use trades before t+5min |
| Resolved markets | Filter out or handle separately |
| Missing data | Document gaps, don't interpolate |
| Survivorship bias | Use all markets in sample, not just "interesting" ones |

---

## Output Directory Structure

```
data/
├── raw/                      # IMMUTABLE - never modify
│   ├── YYYYMMDD/
│   │   └── HHMMSS.parquet   (order books)
│   └── trades/
│       └── YYYYMMDD/
│           └── HHMMSS.parquet
├── processed/                # Cleaned, merged
│   ├── orderbooks_cleaned.parquet
│   ├── trades_cleaned.parquet
│   └── ofi_merged.parquet
└── analysis/                 # Results
    ├── summary_stats.csv
    ├── regression_results.csv
    └── figures/
```
