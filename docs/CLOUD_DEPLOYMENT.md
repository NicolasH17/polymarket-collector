# Cloud VM Deployment Guide

Deploy both Polymarket collectors (order books + trades) on a cloud VM for 24/7 reliability.

---

## Recommended Setup

| Provider | Instance | Cost | Notes |
|----------|----------|------|-------|
| **DigitalOcean** | Basic Droplet | $6/month | Simplest setup |
| **Hetzner** | CX11 | €3.29/month | Best value (EU) |
| **AWS** | t3.micro | ~$8/month | Free tier eligible |
| **GCP** | e2-micro | Free tier | Always free (limited) |

**Requirements:** 1 vCPU, **1GB RAM** (minimum), 10GB+ storage. The collectors themselves use ~50MB, but pandas/pyarrow can spike during writes. 512MB may work but 1GB is the safe floor.

---

## Step-by-Step: DigitalOcean

### 1. Create Droplet

1. Go to [digitalocean.com](https://digitalocean.com) and create account
2. Create Droplet:
   - **Image:** Ubuntu 24.04 LTS
   - **Plan:** Basic, $6/month (1GB RAM)
   - **Region:** Any (US/EU)
   - **Authentication:** SSH key (recommended)
3. Note the IP address

### 2. Connect and Setup

```bash
# From your laptop (use your specific key):
ssh -i ~/.ssh/id_ed25519_polymarket_vm root@YOUR_DROPLET_IP

# Update packages ONCE at setup, then leave alone during collection
sudo apt update && sudo apt upgrade -y

# Create non-root user
adduser thesis
usermod -aG sudo thesis

# Copy SSH key for thesis user
mkdir -p /home/thesis/.ssh
cp ~/.ssh/authorized_keys /home/thesis/.ssh/
chown -R thesis:thesis /home/thesis/.ssh
chmod 700 /home/thesis/.ssh
chmod 600 /home/thesis/.ssh/authorized_keys

# Harden SSH (do AFTER confirming thesis login works)
# Edit /etc/ssh/sshd_config:
#   PermitRootLogin no
#   PasswordAuthentication no
# Then: systemctl restart ssh

# Switch to thesis user
su - thesis

# Install Python
sudo apt install -y python3 python3-pip python3-venv

# Copy project from laptop (run FROM your laptop):
# scp -i ~/.ssh/id_ed25519_polymarket_vm -r polymarket_thesis thesis@YOUR_DROPLET_IP:~/

cd polymarket_thesis

# Setup virtual environment
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt

# Create necessary directories
mkdir -p data/raw/trades logs state
```

### 3. Test Collection

```bash
# Test each collector individually (2 cycles each):
source .venv/bin/activate

# Test order books
python scripts/continuous_collector.py \
  --config config/thesis_markets.json \
  --output-dir data/raw --log-dir logs --state-dir state \
  --interval 90
# Wait ~3 minutes, Ctrl-C

# Test trades
python scripts/trades_collector.py \
  --config config/thesis_markets.json \
  --output-dir data/raw/trades --log-dir logs --state-dir state \
  --interval 90
# Wait ~3 minutes, Ctrl-C

# Verify output
ls -la data/raw/$(date -u +%Y%m%d)/    # Order book snapshots
ls -la data/raw/trades/$(date -u +%Y%m%d)/  # Trade data
cat state/heartbeat_*.json              # Heartbeat files

# Reset watermarks for production start
rm -f state/trades_watermarks.json
```

### 4. Confirm NTP / UTC

```bash
# Verify time sync is active (should show NTP synchronized: yes)
timedatectl
# If not: sudo timedatectl set-ntp true
```

### 5. Setup systemd Services

Two services: one for order books, one for trades.

```bash
# Create ORDER BOOK collector service
sudo tee /etc/systemd/system/polymarket-orderbooks.service << 'EOF'
[Unit]
Description=Polymarket Order Book Collector
After=network.target

[Service]
Type=simple
User=thesis
WorkingDirectory=/home/thesis/polymarket_thesis
ExecStart=/home/thesis/polymarket_thesis/.venv/bin/python scripts/continuous_collector.py --config config/thesis_markets.json --output-dir data/raw --log-dir logs --state-dir state
Environment="PYTHONUNBUFFERED=1"
Restart=always
RestartSec=30
StartLimitIntervalSec=300
StartLimitBurst=10
TimeoutStopSec=120
KillSignal=SIGINT
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

# Create TRADES collector service
sudo tee /etc/systemd/system/polymarket-trades.service << 'EOF'
[Unit]
Description=Polymarket Trades Collector
After=network.target

[Service]
Type=simple
User=thesis
WorkingDirectory=/home/thesis/polymarket_thesis
ExecStart=/home/thesis/polymarket_thesis/.venv/bin/python scripts/trades_collector.py --config config/thesis_markets.json --output-dir data/raw/trades --log-dir logs --state-dir state
Environment="PYTHONUNBUFFERED=1"
Restart=always
RestartSec=30
StartLimitIntervalSec=300
StartLimitBurst=10
TimeoutStopSec=120
KillSignal=SIGINT
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

# Enable and start both
sudo systemctl daemon-reload
sudo systemctl enable polymarket-orderbooks polymarket-trades
sudo systemctl start polymarket-orderbooks polymarket-trades
```

### 6. Verify Both Are Running

```bash
# Check service status
sudo systemctl status polymarket-orderbooks polymarket-trades

# View live logs
sudo journalctl -u polymarket-orderbooks -f
sudo journalctl -u polymarket-trades -f

# Check heartbeats (updated every cycle)
cat state/heartbeat_orderbooks.json
cat state/heartbeat_trades.json
```

---

## Data Layout

```
data/raw/
├── YYYYMMDD/
│   └── HHMMSS.parquet        ← order book snapshots (1 file per 5-min cycle)
└── trades/
    └── YYYYMMDD/
        └── HHMMSS.parquet    ← trades (1 file per 5-min cycle)

state/
├── trades_watermarks.json    ← persistent watermark state
├── heartbeat_trades.json     ← last successful trades cycle
└── heartbeat_orderbooks.json ← last successful orderbooks cycle
```

Both contain a `condition_id` and `slug` column for filtering by market:

```python
import pandas as pd
df = pd.read_parquet("data/raw/trades/20260213/084500.parquet")
fed_trades = df[df.slug.str.contains("fed-interest")]
```

All Parquet files use snappy compression and are written atomically (temp file then rename). A crash mid-write leaves no corrupt files.

---

## Monitoring

### Quick Status Check (from laptop)

```bash
# One-liner: are both alive and collecting?
ssh thesis@YOUR_DROPLET_IP 'systemctl is-active polymarket-orderbooks polymarket-trades && cat ~/polymarket_thesis/state/heartbeat_*.json'
```

### Detailed Check

```bash
ssh thesis@YOUR_DROPLET_IP

# Service status
sudo systemctl status polymarket-orderbooks polymarket-trades

# Recent journal logs
sudo journalctl -u polymarket-trades --since "1 hour ago" --no-pager | tail -20
sudo journalctl -u polymarket-orderbooks --since "1 hour ago" --no-pager | tail -20

# Data file counts and size
find ~/polymarket_thesis/data/raw -name "*.parquet" | wc -l
du -sh ~/polymarket_thesis/data/raw/ ~/polymarket_thesis/data/raw/trades/ ~/polymarket_thesis/logs/

# Disk free
df -h /

# Watermark state
cat ~/polymarket_thesis/state/trades_watermarks.json | python3 -m json.tool | head -30
```

### Setup Monitoring Cron (Recommended)

This checks whether **new data is being written** (not just whether the process is alive — systemd already handles restarts).

```bash
cat > ~/check_collectors.sh << 'SCRIPT'
#!/bin/bash
STATE_DIR=/home/thesis/polymarket_thesis/state
STALE_MINUTES=15

for hb in "$STATE_DIR"/heartbeat_*.json; do
    [ -f "$hb" ] || continue
    age_min=$(( ($(date +%s) - $(stat -c %Y "$hb")) / 60 ))
    if [ "$age_min" -gt "$STALE_MINUTES" ]; then
        name=$(basename "$hb" .json)
        echo "$(date -u +%FT%TZ) ALERT: $name stale (${age_min}m old)" >> ~/collector_alerts.log
    fi
done
SCRIPT
chmod +x ~/check_collectors.sh

# Check every 15 minutes
(crontab -l 2>/dev/null; echo "*/15 * * * * /home/thesis/check_collectors.sh") | crontab -
```

### Log Management

Application logs are written daily (`logs/collector_YYYYMMDD.log`, `logs/trades_collector_YYYYMMDD.log`). Both collectors also log to journald.

```bash
# Delete app logs older than 7 days (safe — journald has copies)
find ~/polymarket_thesis/logs/ -name "*.log" -mtime +7 -delete

# Check journald retention (default varies by distro)
journalctl --disk-usage
```

---

## Data Retrieval

### Download Data to Laptop

```bash
# From your laptop — download all data
rsync -avz -e "ssh -i ~/.ssh/id_ed25519_polymarket_vm" \
  thesis@YOUR_DROPLET_IP:~/polymarket_thesis/data/raw/ ./data/from_server/

# Or just trades
rsync -avz -e "ssh -i ~/.ssh/id_ed25519_polymarket_vm" \
  thesis@YOUR_DROPLET_IP:~/polymarket_thesis/data/raw/trades/ ./data/from_server/trades/

# Download state (watermarks, heartbeats)
rsync -avz -e "ssh -i ~/.ssh/id_ed25519_polymarket_vm" \
  thesis@YOUR_DROPLET_IP:~/polymarket_thesis/state/ ./state/from_server/
```

---

## Common Operations

```bash
# Restart both
sudo systemctl restart polymarket-orderbooks polymarket-trades

# Stop both
sudo systemctl stop polymarket-orderbooks polymarket-trades

# View recent application logs
tail -50 logs/collector_$(date -u +%Y%m%d).log
tail -50 logs/trades_collector_$(date -u +%Y%m%d).log

# Check watermark state (trades)
cat state/trades_watermarks.json | python3 -m json.tool

# Check alerts
cat ~/collector_alerts.log
```

### Update Market Config

```bash
# Copy from laptop
scp -i ~/.ssh/id_ed25519_polymarket_vm \
  config/thesis_markets.json thesis@YOUR_DROPLET_IP:~/polymarket_thesis/config/

# Restart to pick up changes
ssh thesis@YOUR_DROPLET_IP 'sudo systemctl restart polymarket-orderbooks polymarket-trades'
```

---

## Troubleshooting

### Service Won't Start

```bash
sudo journalctl -u polymarket-orderbooks -n 50
sudo journalctl -u polymarket-trades -n 50

# Common fixes:
# 1. Check Python path
ls -la /home/thesis/polymarket_thesis/.venv/bin/python

# 2. Check permissions (everything should be owned by thesis)
ls -la /home/thesis/polymarket_thesis/

# 3. Test manually
cd /home/thesis/polymarket_thesis
source .venv/bin/activate
python scripts/continuous_collector.py --config config/thesis_markets.json
```

### Crash Loops

```bash
# Check if hitting StartLimitBurst (10 restarts in 5 min)
sudo systemctl status polymarket-trades
# If "start-limit-hit": fix the root cause, then:
sudo systemctl reset-failed polymarket-trades
sudo systemctl start polymarket-trades
```

### Disk Usage

```bash
df -h /
du -sh ~/polymarket_thesis/data/raw/ ~/polymarket_thesis/data/raw/trades/ ~/polymarket_thesis/logs/

# Actual data sizes depend on trade volume — check weekly with du
# Logs: delete old ones if needed
find ~/polymarket_thesis/logs/ -name "*.log" -mtime +7 -delete
```

---

## Security Notes

- Disable root SSH login after confirming `thesis` user login works
- Disable password authentication (SSH keys only)
- No API keys or secrets are needed — both collectors use public endpoints
- Avoid running `apt upgrade` during active collection — do it during maintenance windows only

---

## Cost Summary

For 3 weeks of collection:

| Item | Cost |
|------|------|
| DigitalOcean $6/month droplet (1GB) | ~$4.50 (prorated) |
| Data transfer | Free (minimal) |
| **Total** | **~$5** |

Destroy the droplet after downloading data to stop charges.

---

## Pre-Flight Checklist

- [ ] VM created and accessible via SSH as `thesis` user
- [ ] Python venv setup (`python3 -m venv .venv && pip install -r requirements.txt`)
- [ ] Market config (`thesis_markets.json`) in place
- [ ] Manual smoke test of each collector (2 cycles each, then Ctrl-C)
- [ ] Watermarks reset for clean production start
- [ ] NTP/time sync confirmed
- [ ] Both systemd services enabled and running
- [ ] Heartbeat files being updated (`state/heartbeat_*.json`)
- [ ] Monitoring cron installed
- [ ] Know how to download data (`rsync`)
- [ ] Root SSH disabled, password auth disabled
