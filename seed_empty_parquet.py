import pandas as pd
import pathlib as pl

# Path to data folder
DATA = (pl.Path(__file__).parent / "data").resolve()
DATA.mkdir(exist_ok=True)

schemas = {
    "insider_trades.parquet": ["date", "symbol", "person", "relation", "type", "qty", "value"],
    "bulk_block.parquet": ["date", "symbol", "exchange", "deal_type", "qty", "price", "value"],
    "signals.parquet": ["date", "symbol", "signal_type", "confidence"]
}

for fname, cols in schemas.items():
    path = DATA / fname
    if not path.exists():
        df = pd.DataFrame(columns=cols)
        df.to_parquet(path, index=False)
        print(f"✅ Created {fname} with {len(cols)} columns")
    else:
        print(f"ℹ️ {fname} already exists at {path}")
