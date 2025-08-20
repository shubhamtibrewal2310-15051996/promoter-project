import pathlib as pl
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Promoter/FII/DII Dashboard", layout="wide")
st.title("Promoter • FII • DII — Daily Tracker")

DATA = (pl.Path(__file__).parent.parent / "data").resolve()

@st.cache_data(show_spinner=False)
def load_parquet(name: str) -> pd.DataFrame:
    try:
        p = DATA / name
        if p.exists():
            return pd.read_parquet(p)
        return pd.DataFrame()
    except Exception as e:
        # show error on page instead of blank screen
        st.error(f"Failed to load {name}: {e}")
        return pd.DataFrame()

try:
    ins = load_parquet("insider_trades.parquet")
    bb  = load_parquet("bulk_block.parquet")
    fii = load_parquet("fii_dii_agg.parquet")
    sig = load_parquet("signals.parquet")

    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Promoter Buys (≥₹1cr) — Cos",
              str(sig[sig.get("signal_type","").str.contains("Promoter Buy", na=False)]["symbol"].nunique()
                  if not sig.empty else 0))
    c2.metric("Bulk/Block Deals — Rows", str(len(bb)))
    c3.metric("Net FII (₹ cr)", fii.tail(1)["fii_net_value_cr"].iloc[0] if not fii.empty else "-")
    c4.metric("Net DII (₹ cr)", fii.tail(1)["dii_net_value_cr"].iloc[0] if not fii.empty else "-")

    st.subheader("Signals")
    st.dataframe(sig)

    st.subheader("Promoter/Insider Trades")
    st.dataframe(ins)

    st.subheader("Bulk/Block Deals")
    st.dataframe(bb)

    st.subheader("FII/DII Aggregate")
    st.dataframe(fii)

except Exception as e:
    st.exception(e)
    st.info("If this persists, check that `requirements.txt` is in the repo root and the app path is `app/app.py`.")
