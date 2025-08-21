import os
import pathlib as pl
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Promoter/FII/DII Dashboard", layout="wide")

# -------------------------------------------------------------------
# Data folder path: app/app.py -> project root -> data/
# -------------------------------------------------------------------
DATA = (pl.Path(__file__).parent.parent / "data").resolve()

# ----------------------- Debug sidebar ------------------------------
st.sidebar.title("Debug")
st.sidebar.write("Data path:", str(DATA))
try:
    st.sidebar.write("Files in data/:", os.listdir(DATA))
except Exception as e:
    st.sidebar.write("Could not list data/:", e)

# ------------------- Safe parquet loader ----------------------------
@st.cache_data(show_spinner=False)
def load_parquet_safe(name: str) -> pd.DataFrame:
    p = DATA / name
    if not p.exists():
        st.sidebar.warning(f"{name} not found at {p}")
        return pd.DataFrame()
    try:
        df = pd.read_parquet(p)
        st.sidebar.success(f"Loaded {name} with {len(df)} rows")
        return df
    except Exception as e:
        st.error(f"Failed to read {name}: {e}")
        return pd.DataFrame()

# --------------- Helper to render dataframes ------------------------
def show_df(df: pd.DataFrame, placeholder="(no rows)"):
    if df is None or df.empty:
        st.caption(placeholder)
        return
    try:
        st.dataframe(df, use_container_width=True)
    except Exception:
        st.table(df.head(100))

# ======================= Dashboard ==================================
st.title("Promoter • FII • DII — Daily Tracker")

# Load data
ins = load_parquet_safe("insider_trades.parquet")
bb  = load_parquet_safe("bulk_block.parquet")
fii = load_parquet_safe("fii_dii_agg.parquet")
sig = load_parquet_safe("signals.parquet")

# Top metrics
c1, c2, c3, c4 = st.columns(4)
c1.metric(
    "Promoter Buys (≥₹1cr) — Cos",
    str(sig[sig.get("signal_type", "").str.contains("Promoter Buy", na=False)]["symbol"].nunique()
        if not sig.empty else 0),
)
c2.metric("Bulk/Block Deals — Rows", str(len(bb)))
c3.metric("Net FII (₹ cr)", (fii.tail(1)["fii_net_value_cr"].iloc[0] if not fii.empty else "-"))
c4.metric("Net DII (₹ cr)", (fii.tail(1)["dii_net_value_cr"].iloc[0] if not fii.empty else "-"))

# Sections (hide truly empty ones to keep the page clean)
if not sig.empty:
    st.subheader("Signals")
    show_df(sig)

if not ins.empty:
    st.subheader("Promoter/Insider Trades")
    show_df(ins)

if not bb.empty:
    st.subheader("Bulk/Block Deals")
    show_df(bb)

st.subheader("FII/DII Aggregate")
show_df(fii if not fii.empty else pd.DataFrame(
    columns=["date", "segment", "fii_net_value_cr", "dii_net_value_cr", "source"]
))
