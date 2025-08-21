import pandas as pd, datetime as dt, pathlib as pl
DATA = pl.Path('data'); DATA.mkdir(exist_ok=True)

cols = ['date','isin','symbol','deal_type','buyer_name','seller_name','qty','avg_price','value','source_url']

def main():
    today = dt.date.today().isoformat()
    df_new = pd.DataFrame(columns=cols)
    df_new['date'] = []
    path = DATA/'bulk_block.parquet'
    try:
        df_old = pd.read_parquet(path)
        df = pd.concat([df_old, df_new], ignore_index=True).drop_duplicates()
    except FileNotFoundError:
        df = df_new
    df.to_parquet(path, index=False)

if __name__ == '__main__':
    main()
