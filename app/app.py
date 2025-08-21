import os
import pathlib as pl
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Promoter/FII/DII Dashboard", layout="wide")

# ---- data path (Promoter/app/app.py -> Promoter/data) ----
DATA = (pl.Path(__file__).parent.parent / "data").resolve()

# ---- debug sidebar ----
st.sidebar.title("Debug")
st.sidebar.write("Data path:", str(DATA))
try:
    st.sidebar.write("Files in data/:", os.listdir(DATA))
except Exception as e:
    st.sidebar.write("Could not list data/:", e)

@st.cache_data(show_spinner=False)
def load_parquet_safe(filename: str) -> pd.DataFrame:
    p = DATA / filename
    if not p.exists():
        st.sidebar.warning(f"{filename} not found at {p}")
        return pd.DataFrame()
    try:
        df = pd.read_parquet(p)
        st.sidebar.success(f"Loaded {filename} with {len(df)} rows")
        return df
    except Exception as e:
        st.error(f"Failed to read {filename}: {e}")
        return pd.DataFrame()

def show_df(df: pd.DataFrame, placeholder="(no rows)"):
    if df is None or df.empty:
        st.caption(placeholder)
        return
    try:
        st.dataframe(df, use_container_width=True)
    except Exception:
        st.table(df.head(100))

# ================= UI =================
st.title("Promoter • FII • DII — Daily Tracker")

# ---- LOAD DATA (make sure these lines stay above any use) ----
ins = load_parquet_safe("insider_trades.parquet")
bb  = load_parquet_safe("bulk_block.parquet")
fii = load_parquet_safe("fii_dii_agg.parquet")     # <- your existing file
sig = load_parquet_safe("signals.parquet")

# ---- metrics row ----
c1, c2, c3, c4 = st.columns(4)
c1.metric(
    "Promoter Buys (≥₹1cr) — Cos",
    str(sig[sig.get("signal_type", "").str.contains("Promoter Buy", na=False)]["symbol"].nunique()
        if not sig.empty else 0),
)
c2.metric("Bulk/Block Deals — Rows", str(len(bb)))
c3.metric("Net FII (₹ cr)", (fii.tail(1)["fii_net_value_cr"].iloc[0] if not fii.empty else "-"))
c4.metric("Net DII (₹ cr)", (fii.tail(1)["dii_net_value_cr"].iloc[0] if not fii.empty else "-"))

# ---- sections (hide if empty) ----
if not sig.empty:
    st.subheader("Signals"); show_df(sig)
if not ins.empty:
    st.subheader("Promoter/Insider Trades"); show_df(ins)
if not bb.empty:
    st.subheader("Bulk/Block Deals"); show_df(bb)

st.subheader("FII/DII Aggregate")
show_df(fii if not fii.empty else pd.DataFrame(
    columns=["date","segment","fii_net_value_cr","dii_net_value_cr","source"]
))
