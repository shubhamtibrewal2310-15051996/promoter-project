# pipelines/collect_fii_dii.py
import datetime as dt
import re
import pathlib as pl
import pandas as pd
import requests

DATA = pl.Path("data")
DATA.mkdir(exist_ok=True)

MC_URL = "https://www.moneycontrol.com/stocks/marketstats/fii_dii_activity/index.php"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"}

def to_date_any(s):
    if s is None: return None
    s = str(s).strip()
    for fmt in ("%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return dt.datetime.strptime(s, fmt).date()
        except Exception:
            pass
    # fallback: match like 20-Aug-2025
    m = re.match(r"(\d{1,2})-([A-Za-z]{3})-(\d{4})", s)
    if m:
        try:
            return dt.datetime.strptime(m.group(0), "%d-%b-%Y").date()
        except Exception:
            return None
    return None

def to_float(x):
    try:
        return float(str(x).replace(",", "").replace("\xa0", " ").strip())
    except Exception:
        return None

def fetch_from_moneycontrol() -> pd.DataFrame:
    # get HTML
    r = requests.get(MC_URL, headers=UA, timeout=30)
    r.raise_for_status()

    # parse all tables on the page
    tables = pd.read_html(r.text)  # requires lxml/html5lib installed
    if not tables:
        raise RuntimeError("No tables found on Moneycontrol page")

    candidate = None
    for t in tables:
        if t.shape[1] < 5:
            continue
        # try first two columns for date-like values
        date_col_idx = None
        for idx in (0, 1):
            try:
                dtest = t.iloc[:, idx].map(to_date_any)
                if dtest.notna().sum() >= max(3, int(len(t) * 0.3)):
                    date_col_idx = idx
                    break
            except Exception:
                continue
        if date_col_idx is None:
            continue
        # check that the row has enough numeric columns overall
        numeric_count = 0
        for col in t.columns:
            # count cells that look numeric
            s = t[col].astype(str)
            numeric_count += s.str.contains(r"[-+]?\d[\d,]*\.?\d*").sum()
        if numeric_count >= len(t) * 3:  # rough heuristic
            candidate = (t, date_col_idx)
            break

    if candidate is None:
        raise RuntimeError("Moneycontrol: could not locate a suitable Cash activity table")

    t, dcol = candidate
    df = pd.DataFrame()
    df["date"] = t.iloc[:, dcol].map(to_date_any)
    df = df.dropna(subset=["date"]).copy()
    df["date"] = df["date"].map(lambda d: d.isoformat())
    df["segment"] = "Cash"

    # Try to locate nets explicitly by column names
    # Common patterns include 'FII Net', 'DII Net', 'Net Investment by FII/FPI', etc.
    cols_lower = {str(c).strip().lower(): c for c in t.columns}

    def find_col(partials):
        for p in partials:
            p = p.lower()
            for k, v in cols_lower.items():
                if p in k:
                    return v
        return None

    col_fii_net = find_col(["fii net", "fii/fpi net", "net investment by fii", "net by fii"])
    col_dii_net = find_col(["dii net", "dii/mf net", "net investment by dii", "net by dii"])

    # If explicit nets aren’t found, try derive from buy/sell
    col_fii_buy = find_col(["fii gross purchase", "fii buy", "gross purchase by fii"])
    col_fii_sell = find_col(["fii gross sales", "fii sell", "gross sales by fii"])
    col_dii_buy = find_col(["dii gross purchase", "dii buy", "gross purchase by dii"])
    col_dii_sell = find_col(["dii gross sales", "dii sell", "gross sales by dii"])

    if col_fii_net and col_dii_net:
        df["fii_net_value_cr"] = t[col_fii_net].map(to_float)
        df["dii_net_value_cr"] = t[col_dii_net].map(to_float)
    elif all([col_fii_buy, col_fii_sell, col_dii_buy, col_dii_sell]):
        df["fii_net_value_cr"] = t[col_fii_buy].map(to_float) - t[col_fii_sell].map(to_float)
        df["dii_net_value_cr"] = t[col_dii_buy].map(to_float) - t[col_dii_sell].map(to_float)
    else:
        # Last resort: pick any numeric-looking 6 columns after the date; assume order buy,sell,net for FII then DII
        # Build a numeric-only frame (dropping the date column)
        num_cols = []
        for c in t.columns:
            if c == t.columns[dcol]:
                continue
            # mark numeric-ish columns
            ser = t[c].astype(str)
            ratio = ser.str.contains(r"[-+]?\d[\d,]*\.?\d*").mean()
            if ratio > 0.5:
                num_cols.append(c)
        if len(num_cols) >= 6:
            tmp = t[num_cols].applymap(to_float)
            df["fii_net_value_cr"] = tmp.iloc[:, 2]
            df["dii_net_value_cr"] = tmp.iloc[:, 5]
        else:
            raise RuntimeError("Moneycontrol: nets not found/derived")

    df["source"] = "Moneycontrol"
    df = df.drop_duplicates(subset=["date"]).sort_values("date").reset_index(drop=True)
    return df[["date", "segment", "fii_net_value_cr", "dii_net_value_cr", "source"]]

def main():
    path = DATA / "fii_dii_agg.parquet"
    try:
        old = pd.read_parquet(path)
    except Exception:
        old = pd.DataFrame(columns=["date", "segment", "fii_net_value_cr", "dii_net_value_cr", "source"])

    new = fetch_from_moneycontrol()
    merged = pd.concat([old, new], ignore_index=True)
    merged = (
        merged.sort_values(["date", "segment"])
        .drop_duplicates(["date", "segment"], keep="last")
        .reset_index(drop=True)
    )
    merged.to_parquet(path, index=False)
    print(f"Saved {len(merged)} rows → {path} (latest row: {merged.tail(1).to_dict('records')[0]})")

if __name__ == "__main__":
    main()
