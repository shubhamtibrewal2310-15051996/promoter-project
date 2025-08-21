import pandas as pd, pathlib as pl
DATA = pl.Path('data'); DATA.mkdir(exist_ok=True)

pd.DataFrame(columns=['signal_date','symbol','signal_type','score','details_json']).to_parquet(DATA/'signals.parquet', index=False)
