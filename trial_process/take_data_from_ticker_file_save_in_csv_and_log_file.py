#!/usr/bin/env python
"""
Fetch daily OHLCV for a list of symbols on one trade‑date,
save results to CSV, and keep a full success/fail log.

Requires: pip install pandas yfinance python-dotenv
"""

import os, time, random, logging, traceback
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import yfinance as yf
from dotenv import load_dotenv

# ─── 0. CONFIG ─────────────────────────────────────────────────
load_dotenv()                       # only needed if you keep keys in .env

TRADE_DATE      = pd.Timestamp("2025-06-11").date()
TICKERS_CSV     = Path("tickers.csv")              # must have a 'symbol' column
PRICES_OUT_CSV  = Path(f"prices_{TRADE_DATE}.csv")
LOG_FILE        = Path(f"fetch_{TRADE_DATE:%Y%m%d}.log")

BATCH_SIZE      = 200                            # yfinance sweet spot
MAX_RETRIES     = 3
MIN_SLEEP, MAX_SLEEP = 2.0, 5.0                    # polite pause between batches
TIMEOUT_SECS    = 30

# ─── 1. LOGGING ────────────────────────────────────────────────
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    filename=LOG_FILE,
    filemode="w",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(message)s"))
logging.getLogger().addHandler(console)

# ─── 2. LOAD TICKER LIST ──────────────────────────────────────
tickers_df = pd.read_csv(TICKERS_CSV)
symbols = tickers_df["symbol"].str.upper().dropna().unique().tolist()

if not symbols:
    logging.info("❌ No symbols found in %s — exiting.", TICKERS_CSV)
    raise SystemExit

logging.info("ℹ️  Loaded %d symbols from %s", len(symbols), TICKERS_CSV)

# ─── 3. HELPERS ───────────────────────────────────────────────
def chunk(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i : i + size]

def download_batch(batch):
    """Download one‑day OHLCV for a list of symbols."""
    start = TRADE_DATE.strftime("%Y-%m-%d")
    end   = (TRADE_DATE + timedelta(days=1)).strftime("%Y-%m-%d")  # ✅ FIXED here
    return yf.download(
        tickers=" ".join(batch),
        start=start,
        end=end,
        interval="1d",
        group_by="ticker",
        progress=False,
        auto_adjust=False,
        timeout=TIMEOUT_SECS,
    )


def log_result(sym, status, msg=""):
    logging.info("%s | %s | %s", sym.ljust(15), status, msg)

# ─── 4. MAIN LOOP ─────────────────────────────────────────────
records = []                # list of dicts we’ll turn into a DataFrame

for batch in chunk(symbols, BATCH_SIZE):
    attempt = 1
    while attempt <= MAX_RETRIES:
        try:
            logging.info("🔄 Batch of %d symbols (attempt %d)", len(batch), attempt)
            data = download_batch(batch)
            break
        except Exception as e:
            logging.warning("⚠️  Batch failed: %s", e)
            if attempt == MAX_RETRIES:
                for sym in batch:
                    log_result(sym, "FAILED", f"batch-error: {e}")
            else:
                time.sleep(2 * attempt)
            attempt += 1
    else:
        continue   # move to next batch after exhausting retries

    # Process each symbol in batch
    for sym in batch:
        try:
            if sym not in data:
                raise ValueError("symbol not in downloaded data")

            try:
                row = data[sym].loc[TRADE_DATE]
            except KeyError:
                raise ValueError("trade_date not in data")

            if pd.isna(row["Close"]) or pd.isna(row["Volume"]):
                raise ValueError("missing price or volume")

            records.append({
                "symbol":           sym,
                "trade_date":       TRADE_DATE,
                "open":             round(row["Open"], 4),
                "high":             round(row["High"], 4),
                "low":              round(row["Low"], 4),
                "close":            round(row["Close"], 4),
                "adjusted_close":   round(row["Adj Close"], 4),
                "volume":           int(row["Volume"]),
                "dividend_amount":  round(row.get("Dividends", 0.0) or 0.0, 4),
                "split_coeff":      round(row.get("Stock Splits", 1.0) or 1.0, 4),
                "inserted_at":      datetime.now(),
            })
            log_result(sym, "SUCCESS")
        except Exception as e:
            log_result(sym, "FAILED", str(e))

    # polite pause
    sleep_s = random.uniform(MIN_SLEEP, MAX_SLEEP)
    logging.info("⏸️  Sleeping %.1fs …", sleep_s)
    time.sleep(sleep_s)

# ─── 5. WRITE OUTPUT CSV ──────────────────────────────────────
if records:
    out_df = pd.DataFrame.from_records(records)
    # Append if file exists; otherwise write header
    write_header = not PRICES_OUT_CSV.exists()
    out_df.to_csv(PRICES_OUT_CSV, mode="a", index=False, header=write_header)
    logging.info("✅ Saved %d rows to %s", len(out_df), PRICES_OUT_CSV)
else:
    logging.info("⚠️  No successful rows — nothing written.")

logging.info("🎉 Done. Log written to %s", LOG_FILE)
