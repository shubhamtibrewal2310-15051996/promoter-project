import pandas as pd, pathlib as pl, datetime as dt
DATA = pl.Path("data")

# insider_trades
it = pd.read_parquet(DATA/"insider_trades.parquet")
it = pd.concat([it, pd.DataFrame([{
    "date": dt.date.today().isoformat(),
    "isin": "INE123A01016",
    "symbol": "DEMOCO",
    "person_name": "Demo Promoter",
    "relation": "Promoter",
    "trade_type": "buy",
    "qty": 100000,
    "avg_price": 125.0,
    "value": 12500000.0,
    "post_holding_%": None,
    "source_url": "https://example.com/filing",
    "raw_text": "Sample seeded row"
}])], ignore_index=True)
it.to_parquet(DATA/"insider_trades.parquet", index=False)

# bulk_block
bb = pd.read_parquet(DATA/"bulk_block.parquet")
bb = pd.concat([bb, pd.DataFrame([{
    "date": dt.date.today().isoformat(),
    "isin": "INE123A01016",
    "symbol": "DEMOCO",
    "deal_type": "bulk",
    "buyer_name": "Demo FII Fund",
    "seller_name": "Demo Seller",
    "qty": 250000,
    "avg_price": 126.5,
    "value": 31625000.0,
    "source_url": "https://example.com/bulk"
}])], ignore_index=True)
bb.to_parquet(DATA/"bulk_block.parquet", index=False)

# signals
sig = pd.read_parquet(DATA/"signals.parquet")
sig = pd.concat([sig, pd.DataFrame([{
    "signal_date": dt.date.today().isoformat(),
    "symbol": "DEMOCO",
    "signal_type": "Promoter Buy (>=₹1cr)",
    "score": 70.0,
    "details_json": '{"value": 1.25e7}'
}])], ignore_index=True)
sig.to_parquet(DATA/"signals.parquet", index=False)

print("Seeded one demo row into insider_trades, bulk_block, and signals.")
