import os, time, json, traceback
from datetime import datetime
import requests, pandas as pd, psycopg
from psycopg import sql
from dotenv import load_dotenv

# ── 0. LOAD ENV AND CONNECT TO DB ──────────────────────────────
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
    options='-c statement_timeout=0'  # prevent statement timeout error
)

# ── 1. CONFIG ───────────────────────────────────────────────────
MAX_REQ_PER_MIN = 75
COOLDOWN_SECS   = 60
TIME_SERIES_URL = (
    "https://www.alphavantage.co/query"
    "?function=TIME_SERIES_DAILY_ADJUSTED"
    "&symbol={symbol}"
    "&outputsize=full"
    "&apikey={apikey}"
)

# ── 2. FETCH PENDING JOBS ───────────────────────────────────────

with PG_CONN.cursor(row_factory=psycopg.rows.dict_row) as cur:
    cur.execute(
        """
        SELECT job_id, symbol, trade_date
        FROM pipelines.stocks_daily_jobs
        WHERE trade_date = '2025-06-23'
          AND status = 'PENDING'
        ORDER BY symbol, trade_date;
 
        """
    )
    jobs = cur.fetchall()

if not jobs:
    print("✅ No pending jobs — nothing to do.")
    raise SystemExit

jobs_df = pd.DataFrame(jobs)
jobs_df["trade_date"] = pd.to_datetime(jobs_df["trade_date"]).dt.date
print(f"ℹ️  Loaded {len(jobs_df)} pending jobs")

jobs_by_symbol = jobs_df.groupby("symbol")

# ── 3. HELPERS ──────────────────────────────────────────────────
def mark_job(cur, job_id: int, status: str, err: str | None = None):
    cur.execute(
        "UPDATE pipelines.stocks_daily_jobs "
        "SET status = %s, error_message = %s, last_attempted = now() "
        "WHERE job_id = %s",
        (status, err, job_id)
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

def fetch_symbol_data(symbol: str) -> dict:
    url = TIME_SERIES_URL.format(symbol=symbol, apikey=API_KEY)
    try:
        r = requests.get(url, timeout=30)
        if r.status_code != 200:
            raise RuntimeError(f"HTTP {r.status_code}")
        j = r.json()
        if "Error Message" in j:
            raise RuntimeError(j["Error Message"])
        if "Note" in j:
            raise RuntimeError(j["Note"])
        if "Time Series (Daily)" not in j:
            raise RuntimeError(f"Missing 'Time Series (Daily)' in response")
        return j["Time Series (Daily)"]
    except Exception as exc:
        raise RuntimeError(f"{symbol} API error: {exc}")

# ── 4. MAIN LOOP ────────────────────────────────────────────────
req_counter = 0
start_minute = time.time()

try:
    for symbol, group in jobs_by_symbol:
        symbol_upper = symbol.upper()
        job_ids = group["job_id"].tolist()

        now = time.time()
        if req_counter >= MAX_REQ_PER_MIN and (now - start_minute) < 60:
            sleep_for = COOLDOWN_SECS - (now - start_minute)
            if sleep_for > 0:
                print(f"⏸️  Rate limit reached; sleeping {sleep_for:.1f}s …")
                time.sleep(sleep_for)
            req_counter = 0
            start_minute = time.time()

        try:
            series = fetch_symbol_data(symbol_upper)
            req_counter += 1
        except Exception as exc:
            err_msg = f"API error: {exc}"
            print(f"❌ {symbol_upper}: {err_msg}")
            with PG_CONN.cursor() as cur:
                for jid in job_ids:
                    mark_job(cur, jid, "FAILED", err_msg)
                PG_CONN.commit()
            continue

        with PG_CONN.cursor() as cur:
            for jid, dt in zip(job_ids, group["trade_date"]):
                iso_dt = dt.isoformat()
                if iso_dt not in series:
                    msg = "No data for trade_date (market closed?)"
                    mark_job(cur, jid, "FAILED", msg)
                    PG_CONN.commit()
                    continue

                try:
                    rec = series[iso_dt]
                    row = {
                        "symbol": symbol_upper,
                        "trade_date": dt,
                        "open": round(float(rec["1. open"]), 4),
                        "high": round(float(rec["2. high"]), 4),
                        "low": round(float(rec["3. low"]), 4),
                        "close": round(float(rec["4. close"]), 4),
                        "adjusted_close": round(float(rec["5. adjusted close"]), 4),
                        "volume": int(rec["6. volume"]),
                        "dividend_amount": round(float(rec.get("7. dividend amount", 0.0) or 0.0), 4),
                        "split_coefficient": round(float(rec.get("8. split coefficient", 1.0) or 1.0), 4),
                    }

                    cur.execute(INSERT_PRICE_SQL, row)
                    mark_job(cur, jid, "SUCCESS", None)
                    PG_CONN.commit()
                    print(f"✅ {symbol_upper} {iso_dt} inserted")
                except Exception as exc:
                    PG_CONN.rollback()
                    err_msg = f"Insert error: {exc}"
                    mark_job(cur, jid, "FAILED", err_msg)
                    PG_CONN.commit()
                    print(f"❌ {symbol_upper} {iso_dt}: {err_msg}")

except KeyboardInterrupt:
    print("\n🛑 Interrupted by user (Ctrl+C). Exiting safely…")
    try:
        PG_CONN.rollback()
    except:
        pass
    PG_CONN.close()
    raise SystemExit(1)

except psycopg.OperationalError as e:
    print(f"\n🚨 Lost DB connection: {e}")
    try:
        PG_CONN.close()
    except:
        pass
    raise SystemExit(1)

except Exception as e:
    print(f"\n🔥 Unexpected error: {e}")
    traceback.print_exc()
    try:
        PG_CONN.rollback()
        PG_CONN.close()
    except:
        pass
    raise SystemExit(1)

finally:
    if not PG_CONN.closed:
        PG_CONN.close()

print("\n🎉 All pending jobs handled.")
