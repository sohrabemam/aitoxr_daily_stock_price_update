import os, time, traceback
from datetime import datetime, timedelta
import pandas as pd
import psycopg
from dotenv import load_dotenv
import yfinance as yf
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
# ── 0. LOAD ENV AND CONNECT TO DB ──────────────────────────────
load_dotenv()

PG_CONN = psycopg.connect(
    host=os.environ["PG_HOST"],
    port=os.environ["PG_PORT"],
    dbname=os.environ["PG_DATABASE"],
    user=os.environ["PG_USER"],
    password=os.environ["PG_PASSWORD"],
    autocommit=False,
    options='-c statement_timeout=0'
)

# ── 1. FETCH PENDING JOBS ───────────────────────────────────────
with PG_CONN.cursor(row_factory=psycopg.rows.dict_row) as cur:
    cur.execute("""
        SELECT job_id, symbol, trade_date
        FROM pipelines.stocks_daily_jobs
        WHERE status = 'FAILED'
        AND (
            error_message = 'No data for trade_date (market closed?)'
            OR error_message = 'API error: HTTPSConnectionPool(host=''www.alphavantage.co'', port=443): Read timed out. (read timeout=30)'
        )
        ORDER BY symbol, trade_date;
    """)
    jobs = cur.fetchall()

if not jobs:
    print("✅ No pending jobs — nothing to do.")
    raise SystemExit

jobs_df = pd.DataFrame(jobs)
jobs_df["trade_date"] = pd.to_datetime(jobs_df["trade_date"]).dt.date
print(f"ℹ️  Loaded {len(jobs_df)} pending jobs")

jobs_by_symbol = jobs_df.groupby("symbol")

# ── 2. SQL HELPERS ───────────────────────────────────────────────
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

# ── 3. MAIN LOOP ────────────────────────────────────────────────
try:
    for symbol, group in jobs_by_symbol:
        symbol_upper = symbol.upper()
        job_ids = group["job_id"].tolist()
        dates = group["trade_date"].tolist()

        min_date = min(dates)
        max_date = max(dates)

        try:
            df = yf.download(
                symbol_upper,
                start=(min_date - timedelta(days=1)).isoformat(),
                end=(max_date + timedelta(days=1)).isoformat(),
                progress=False,
                interval="1d",
                auto_adjust=False
            )
        except Exception as exc:
            err_msg = f"yfinance error: {exc}"
            print(f"❌ {symbol_upper}: {err_msg}")
            with PG_CONN.cursor() as cur:
                for jid in job_ids:
                    mark_job(cur, jid, "FAILED", err_msg)
                PG_CONN.commit()
            continue

        if df.empty:
            err_msg = f"No data returned from yfinance"
            print(f"❌ {symbol_upper}: {err_msg}")
            with PG_CONN.cursor() as cur:
                for jid in job_ids:
                    mark_job(cur, jid, "FAILED", err_msg)
                PG_CONN.commit()
            continue

        df.index = pd.to_datetime(df.index).date
        df.index = pd.Index(df.index, name="Date")
        df = df.rename(columns={
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adjusted_close",
            "Volume": "volume"
        })

        with PG_CONN.cursor() as cur:
            for jid, dt in zip(job_ids, dates):
                try:
                    row_data = df.loc[dt]
                    row = {
                        "symbol": symbol_upper,
                        "trade_date": dt,
                        "open": round(row_data["open"].iloc[0], 4),
                        "high": round(row_data["high"].iloc[0], 4),
                        "low": round(row_data["low"].iloc[0], 4),
                        "close": round(row_data["close"].iloc[0], 4),
                        "adjusted_close": round(row_data["adjusted_close"].iloc[0], 4),
                        "volume": int(row_data["volume"].iloc[0]),
                        "dividend_amount": 0.0,  # yfinance daily data doesn’t include dividend or split
                        "split_coefficient": 1.0
                    }
                    cur.execute(INSERT_PRICE_SQL, row)
                    mark_job(cur, jid, "SUCCESS", None)
                    PG_CONN.commit()
                    print(f"✅ {symbol_upper} {dt} inserted")

                except KeyError:
                    msg = "No data for trade_date (market closed?)"
                    mark_job(cur, jid, "FAILED", msg)
                    PG_CONN.commit()
                    print(f"❌ {symbol_upper} {dt}: {msg}")
                except Exception as exc:
                    PG_CONN.rollback()
                    err_msg = f"Insert error: {exc}"
                    mark_job(cur, jid, "FAILED", err_msg)
                    PG_CONN.commit()
                    print(f"❌ {symbol_upper} {dt}: {err_msg}")

except KeyboardInterrupt:
    print("\n🛑 Interrupted by user (Ctrl+C). Exiting safely…")
    try: PG_CONN.rollback()
    except: pass
    PG_CONN.close()
    raise SystemExit(1)

except psycopg.OperationalError as e:
    print(f"\n🚨 Lost DB connection: {e}")
    try: PG_CONN.close()
    except: pass
    raise SystemExit(1)

except Exception as e:
    print(f"\n🔥 Unexpected error: {e}")
    traceback.print_exc()
    try:
        PG_CONN.rollback()
        PG_CONN.close()
    except: pass
    raise SystemExit(1)

finally:
    if not PG_CONN.closed:
        PG_CONN.close()

print("\n🎉 All pending jobs handled.")
