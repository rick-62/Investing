
from typing import Dict
import investpy
import pandas as pd

from functools import partial

def get_stock_lists():
    '''Retrieve stock lists as dataframe tables, from investpy library'''

    stocks = investpy.get_stocks()
    etfs = investpy.get_etfs()
    indices = investpy.get_indices()

    return stocks, etfs, indices


def download_etfs_historical(etfs: pd.DataFrame, from_date: str) -> Dict:
    '''Download etf historical data'''

    today = pd.to_datetime('today').strftime("%d/%m/%Y")

    parts = {}
    
    for i, row in etfs.iterrows():

        file_name = f"etf_{row['symbol_ft']}_{row['isin']}"

        parts[file_name] = investpy.get_etf_historical_data(
            row['name'], row['country'], 
            from_date=from_date, to_date=today, 
            as_json=False, order='ascending'
        )
    
    return parts





