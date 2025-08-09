import os, traceback
from dotenv import load_dotenv
import pandas as pd
import psycopg

# ── 0. LOAD ENV & CONNECT TO DB ──────────────────────────────
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
)

# ── 1. FETCH UNIQUE SYMBOLS ───────────────────────────────────
try:
    with PG_CONN.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT DISTINCT symbol
            FROM pipelines.stocks_daily_jobs
            WHERE error_message LIKE 'API error:%';

            """
        )
        rows = cur.fetchall()
        symbols = {row["symbol"] for row in rows}  # using set for uniqueness

    print(f"✅ Found {len(symbols)} unique symbols with API error.")

    # ── 2. SAVE TO CSV ────────────────────────────────────────
    df = pd.DataFrame(sorted(symbols), columns=["symbol"])
    df.to_csv("unique_invalid_symbol.csv", index=False)
    print("📁 Saved to unique_invalid_symbol.csv")

except Exception as e:
    print("❌ Error occurred:", e)
    traceback.print_exc()
finally:
    PG_CONN.close()
