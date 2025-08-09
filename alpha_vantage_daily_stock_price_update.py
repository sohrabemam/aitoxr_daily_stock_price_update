#!/usr/bin/env python
"""
Note - This script only run for specific date provided in CLI argument. or by default it will run for yesterday's date.
Fetch DAILY prices for the symbols listed in pipelines.stocks_daily_jobs
(status = 'PENDING' on the chosen date) and write them to stocks.daily_prices.
Designed for *incremental* daily runs.
to run this script run below command in terminal:
python alpha_vantage_daily_stock_price_update.py --date YYYY-MM-DD
where YYYY-MM-DD is the date for which you want to fetch daily prices.

"""

import os, sys, time, json, traceback, argparse
from datetime import datetime, date, timedelta
import requests, pandas as pd, psycopg
from psycopg import sql
from dotenv import load_dotenv

# ── 0. ENV & DB ─────────────────────────────────────────────────
load_dotenv()
API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
if not API_KEY:
    raise RuntimeError("ALPHAVANTAGE_API_KEY missing from .env")

PG_CONN = psycopg.connect(
    host=os.environ["PG_HOST"],
    port=os.environ["PG_PORT"],
    dbname=os.environ["PG_DATABASE"],
    user=os.environ["PG_USER"],
    password=os.environ["PG_PASSWORD"],
    autocommit=False,
    options='-c statement_timeout=0'
)

# ── 1. CLI ARGUMENTS ────────────────────────────────────────────
parser = argparse.ArgumentParser(description="Daily Alpha Vantage downloader")
parser.add_argument("--date", help="Trade-date YYYY-MM-DD (default: yesterday)")
args = parser.parse_args()

if args.date:
    trade_date = datetime.strptime(args.date, "%Y-%m-%d").date()
else:
    # choose yesterday (or last business day if you’ve built a helper)
    trade_date = date.today() - timedelta(days=1)

# ── 2. CONSTANTS ────────────────────────────────────────────────
MAX_REQ_PER_MIN = 75          # adjust for your Alpha Vantage plan
COOLDOWN_SECS   = 60
TIME_SERIES_URL = (
    "https://www.alphavantage.co/query"
    "?function=TIME_SERIES_DAILY_ADJUSTED"
    "&symbol={symbol}"
    "&outputsize=compact"      # <── only the last 100 days
    "&apikey={apikey}"
)

INSERT_PRICE_SQL = """
INSERT INTO stocks.daily_prices (
    symbol, trade_date, open, high, low, close, adjusted_close,
    volume, dividend_amount, split_coefficient
) VALUES (
    %(symbol)s, %(trade_date)s, %(open)s, %(high)s, %(low)s, %(close)s,
    %(adjusted_close)s, %(volume)s, %(dividend_amount)s, %(split_coefficient)s
)
ON CONFLICT (symbol, trade_date) DO NOTHING
"""

# ── 3. LOAD PENDING JOBS ────────────────────────────────────────
with PG_CONN.cursor(row_factory=psycopg.rows.dict_row) as cur:
    cur.execute(
       """
    SELECT job_id, symbol, trade_date
    FROM pipelines.stocks_daily_jobs
    WHERE trade_date = %s
      AND status = 'PENDING'
    ORDER BY symbol;
""",
        (trade_date,)
    )
    jobs = cur.fetchall()

if not jobs:
    print("✅ Nothing to do – no pending jobs for", trade_date)
    sys.exit(0)

print(f"ℹ️  Loaded {len(jobs)} pending jobs for {trade_date}")

# ── 4. HELPERS ─────────────────────────────────────────────────
def mark_job(cur, job_id: int, status: str, err: str | None = None):
    cur.execute(
        "UPDATE pipelines.stocks_daily_jobs "
        "SET status = %s, error_message = %s, last_attempted = now() "
        "WHERE job_id = %s",
        (status, err, job_id)
    )

def fetch_symbol_data(symbol: str) -> dict:
    url = TIME_SERIES_URL.format(symbol=symbol, apikey=API_KEY)
    r = requests.get(url, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}")
    j = r.json()
    if "Error Message" in j:
        raise RuntimeError(j["Error Message"])
    if "Note" in j:
        raise RuntimeError(j["Note"])
    try:
        return j["Time Series (Daily)"]
    except KeyError:
        raise RuntimeError("Missing 'Time Series (Daily)' in response")

# ── 5. MAIN LOOP ────────────────────────────────────────────────
series_cache: dict[str, dict] = {}  # symbol ➜ series
req_counter  = 0
minute_start = time.time()

try:
    with PG_CONN.cursor() as cur:
        for job in jobs:
            jid     = job["job_id"]
            symbol  = job["symbol"].upper()

            # fetch/cached series ­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­
            if symbol not in series_cache:
                now = time.time()
                if req_counter >= MAX_REQ_PER_MIN and (now - minute_start) < 60:
                    sleep_for = COOLDOWN_SECS - (now - minute_start)
                    if sleep_for > 0:
                        print(f"⏸️  Rate-limit hit; sleeping {sleep_for:.1f}s …")
                        time.sleep(sleep_for)
                    req_counter  = 0
                    minute_start = time.time()

                try:
                    series_cache[symbol] = fetch_symbol_data(symbol)
                    req_counter += 1
                except Exception as exc:
                    err = f"API error: {exc}"
                    print(f"❌ {symbol}: {err}")
                    mark_job(cur, jid, "FAILED", err)
                    PG_CONN.commit()
                    continue

            series = series_cache[symbol]
            iso_dt = trade_date.isoformat()

            if iso_dt not in series:
                msg = "No data for trade_date (market closed?)"
                mark_job(cur, jid, "FAILED", msg)
                PG_CONN.commit()
                continue

            rec = series[iso_dt]
            row = {
                "symbol":           symbol,
                "trade_date":       trade_date,
                "open":             round(float(rec["1. open"]), 4),
                "high":             round(float(rec["2. high"]), 4),
                "low":              round(float(rec["3. low"]), 4),
                "close":            round(float(rec["4. close"]), 4),
                "adjusted_close":   round(float(rec["5. adjusted close"]), 4),
                "volume":           int(rec["6. volume"]),
                "dividend_amount":  round(float(rec.get("7. dividend amount", 0.0)), 4),
                "split_coefficient":round(float(rec.get("8. split coefficient", 1.0)), 4)
            }

            try:
                cur.execute(INSERT_PRICE_SQL, row)
                mark_job(cur, jid, "SUCCESS")
                PG_CONN.commit()
                print(f"✅ {symbol} {iso_dt} inserted")
            except Exception as exc:
                PG_CONN.rollback()
                err = f"Insert error: {exc}"
                mark_job(cur, jid, "FAILED", err)
                PG_CONN.commit()
                print(f"❌ {symbol} {iso_dt}: {err}")

except KeyboardInterrupt:
    print("\n🛑 Cancelled by user – rolling back")
    PG_CONN.rollback()
finally:
    PG_CONN.close()

print("\n🎉 All pending jobs handled.")
