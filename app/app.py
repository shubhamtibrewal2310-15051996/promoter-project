import streamlit as st, pandas as pd, pathlib as pl
DATA = pl.Path('../data')
st.set_page_config(page_title='Promoter/FII/DII Dashboard', layout='wide')

@st.cache_data(show_spinner=False)
def load(name):
    p = DATA/name
    return pd.read_parquet(p) if p.exists() else pd.DataFrame()

def show_df(df, placeholder="(no rows)"):
    if df is None or df.empty:
        st.caption(placeholder)
        return
    try:
        st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.warning("Interactive table blocked by the browser. Falling back to a static table.")
        st.table(df.head(100))

st.title('Promoter • FII • DII — Daily Tracker')

ins = load('insider_trades.parquet')
bb  = load('bulk_block.parquet')
fii = load('fii_dii_agg.parquet')
sig = load('signals.parquet')

c1,c2,c3,c4 = st.columns(4)
c1.metric('Promoter Buys (≥₹1cr) — Cos', str(sig[sig.get('signal_type','').str.contains('Promoter Buy', na=False)]['symbol'].nunique() if not sig.empty else 0))
c2.metric('Bulk/Block Deals — Rows', str(len(bb)))
c3.metric('Net FII (₹ cr)', (fii.tail(1)['fii_net_value_cr'].iloc[0] if not fii.empty else '-'))
c4.metric('Net DII (₹ cr)', (fii.tail(1)['dii_net_value_cr'].iloc[0] if not fii.empty else '-'))

st.subheader('Signals')
show_df(sig)

st.subheader('Promoter/Insider Trades')
show_df(ins)

st.subheader('Bulk/Block Deals')
show_df(bb)

st.subheader('FII/DII Aggregate')
show_df(fii)
