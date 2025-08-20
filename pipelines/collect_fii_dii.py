# pipelines/collect_fii_dii.py
import datetime as dt
import json
import time
import pathlib as pl
import pandas as pd
import requests

DATA = pl.Path("data")
DATA.mkdir(exist_ok=True)

NSE_HOME = "https://www.nseindia.com/"
# Common NSE endpoints used in the wild (they change names occasionally).
# We'll try a couple, first one that returns valid JSON wins.
CANDIDATE_URLS = [
    "https://www.nseindia.com/api/fiidiiTradeReact",  # newer
    "https://www.nseindia.com/api/fiidiiTrade",       # older
]

HEADERS = {
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "accept": "application/json, text/plain, */*",
    "referer": "https://www.nseindia.com/",
    "pragma": "no-cache",
    "cache-control": "no-cache",
}

def fetch_fii_dii():
    """Return a pandas DataFrame with columns: date, segment, fii_net_value_cr, dii_net_value_cr."""
    with requests.Session() as s:
        # warm-up to get cookies
        s.get(NSE_HOME, headers=HEADERS, timeout=15)
        time.sleep(0.5)

        last_err = None
        payload_df = None
        for url in CANDIDATE_URLS:
            try:
                r = s.get(url, headers=HEADERS, timeout=20)
                r.raise_for_status()
                js = r.json()
                # The payload is generally in js["data"] or js itself. Normalize defensively.
                data = js.get("data", js)
                if isinstance(data, dict) and "data" in data:
                    data = data["data"]
                if isinstance(data, list) and data:
                    # Typical keys: "date", "fii_net", "dii_net", "category", etc.
                    # Normalize to our schema.
                    rows = []
                    for row in data:
                        # Accept a variety of key spellings
                        date_str = row.get("date") or row.get("TradeDate") or row.get("TRADE_DATE")
                        if not date_str:
                            continue
                        # Try parse date as DD-MMM-YYYY or YYYY-MM-DD
                        parsed = None
                        for fmt in ("%d-%b-%Y", "%Y-%m-%d", "%d-%m-%Y"):
                            try:
                                parsed = dt.datetime.strptime(date_str.strip(), fmt).date()
                                break
                            except Exception:
                                pass
                        if not parsed:
                            # Last resort: leave as string and skip
                            continue

                        seg = row.get("category") or row.get("Segment") or "Cash"
                        # Net values often in crores; accept multiple key patterns
                        fii_net = (
                            row.get("FII Net Value", row.get("fii_net", row.get("FII_NET", row.get("fii_net_buy_value"))))
                        )
                        dii_net = (
                            row.get("DII Net Value", row.get("dii_net", row.get("DII_NET", row.get("dii_net_buy_value"))))
                        )

                        # Some payloads provide separate buy/sell; compute net if both exist.
                        if fii_net is None and ("fii_buy" in row and "fii_sell" in row):
                            try:
                                fii_net = float(row["fii_buy"]) - float(row["fii_sell"])
                            except Exception:
                                pass
                        if dii_net is None and ("dii_buy" in row and "dii_sell" in row):
                            try:
                                dii_net = float(row["dii_buy"]) - float(row["dii_sell"])
                            except Exception:
                                pass

                        def to_float(x):
                            try:
                                # values sometimes come as strings with commas
                                return float(str(x).replace(",", "").strip())
                            except Exception:
                                return None

                        fii_net = to_float(fii_net)
                        dii_net = to_float(dii_net)

                        rows.append(
                            {
                                "date": parsed.isoformat(),
                                "segment": seg if seg else "Cash",
                                "fii_net_value_cr": fii_net,
                                "dii_net_value_cr": dii_net,
                            }
                        )
                    if rows:
                        payload_df = pd.DataFrame(rows)
                        break
            except Exception as e:
                last_err = e
                time.sleep(0.8)
                continue

        if payload_df is None:
            raise RuntimeError(f"Failed to fetch NSE FII/DII payload. Last error: {last_err}")

        # Keep only Cash segment if multiple segments appear
        # and drop exact dupes; sort by date
        payload_df["segment"] = payload_df["segment"].fillna("Cash")
        return (
            payload_df[payload_df["segment"].str.contains("cash", case=False, na=False) | (payload_df["segment"] == "Cash")]
            .drop_duplicates()
            .sort_values("date")
            .reset_index(drop=True)
        )

def main():
    path = DATA / "fii_dii_agg.parquet"
    try:
        old = pd.read_parquet(path)
    except Exception:
        old = pd.DataFrame(columns=["date", "segment", "fii_net_value_cr", "dii_net_value_cr"])

    new = fetch_fii_dii()
    # upsert on (date, segment)
    merged = pd.concat([old, new], ignore_index=True)
    merged = merged.sort_values(["date", "segment"]).drop_duplicates(["date", "segment"], keep="last")
    merged.to_parquet(path, index=False)
    print(f"Saved {len(merged)} rows → {path}")

if __name__ == "__main__":
    main()
