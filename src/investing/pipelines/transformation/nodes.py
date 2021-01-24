import pandas as pd

from typing import Any, Dict, Iterable, Optional

def cleanse_freetrade(ft: pd.DataFrame, mic_remap: Dict):
    '''Clean freetrade data and prepare for join to investpy stock list'''

    ft.columns = ft.columns.str.lower()
    ft.fractional_enabled.fillna(False, inplace=True)
    ft['stock_exchange'] = [mic_remap.get(x) for x in ft.mic]  # convert mic to exchange e.g. XLON: London
    ft.currency = ft.currency.str.upper()

    return ft


def cleanse_investpy(stocks: pd.DataFrame):
    pass


def join_freetrade_etfs(ft: pd.DataFrame, etfs: pd.DataFrame):
    '''Inner join Freetrade stocks with stock list, to extract only relevant stocks/etfs (Joining key: ISIN)'''

    return pd.merge(ft, etfs, on=['isin', 'currency', 'stock_exchange'], how='inner', suffixes=('_ft', ''))


def cleanse_stock_list(stocks: pd.DataFrame):
    '''After joining Freetrade stocks with investpy stock list, data filtered and cleansed'''
    pass