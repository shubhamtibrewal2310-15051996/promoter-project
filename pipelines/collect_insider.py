import pandas as pd, pathlib as pl
DATA = pl.Path('data'); DATA.mkdir(exist_ok=True)

pd.DataFrame(columns=['date','isin','symbol','person_name','relation','trade_type','qty','avg_price','value','post_holding_%','source_url','raw_text']).to_parquet(DATA/'insider_trades.parquet', index=False)
