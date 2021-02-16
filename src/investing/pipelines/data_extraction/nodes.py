
import logging
from time import sleep
from typing import Dict
import warnings

import investpy
import pandas as pd

log = logging.getLogger(__name__)


def get_stock_lists():
    '''Retrieve stock lists as dataframe tables, from investpy library'''

    stocks = investpy.get_stocks()
    etfs = investpy.get_etfs()
    indices = investpy.get_indices()

    return stocks, etfs, indices


def cleanse_freetrade(ft: pd.DataFrame, mic_remap: Dict) -> pd.DataFrame:
    '''Clean freetrade data and prepare for join to investpy stock list'''

    ft.columns = ft.columns.str.lower()
    ft.fractional_enabled.fillna(False, inplace=True)
    ft['stock_exchange'] = [mic_remap.get(x) for x in ft.mic]  # convert mic to exchange e.g. XLON: London
    ft.currency = ft.currency.str.upper()

    return ft


def join_freetrade_etfs(ft: pd.DataFrame, etfs: pd.DataFrame) -> pd.DataFrame:
    '''Inner join Freetrade stocks with stock list, to extract only relevant etfs (Joining key: ISIN)'''

    return pd.merge(ft, etfs, on=['isin', 'currency', 'stock_exchange'], how='inner', suffixes=('_ft', ''))


def download_etfs_historical(etfs: pd.DataFrame, from_date: str) -> Dict:
    '''Download all etf historical data'''

    today = pd.to_datetime('today').strftime("%d/%m/%Y")

    parts = {}
    
    warnings.filterwarnings('ignore')

    for i, row in etfs.iterrows():

        file_name = f"etf_{row.symbol_ft}_{row['isin']}"

        sleep(1)

        try:
            parts[file_name] = investpy.get_etf_historical_data(
                row['name'], row.country, 
                stock_exchange=row.stock_exchange, 
                from_date=from_date, to_date=today, 
                as_json=False, order='ascending'
            )

            log.debug(f"Download complete (ETF historical): {file_name}")

        except:
            log.warning(f"Download FAILED (ETF historical): {file_name}")
            sleep(30)

    warnings.filterwarnings('default')

    log.info(f"{len(parts)} downloaded")
    
    return parts



def download_etf_information(etfs: pd.DataFrame) -> Dict:
    '''Get latest etf information: latest price range, market cap etc.'''

    info = {}

    for i, row in etfs.iterrows():

        file_name = f"etf_{row.symbol_ft}_{row['isin']}"

        sleep(1)

        try:
            info[file_name] = investpy.get_etf_information(row['name'], row.country, as_json=True)
            log.debug(f"Download complete (ETF information): {file_name}")

        except:
            log.warning(f"Download FAILED (ETF information): {file_name}")
            sleep(30)

    warnings.filterwarnings('default')

    log.info(f"{len(info)} downloaded")
    
    return info


def combine_etf_information(data: Dict) -> pd.DataFrame:
    '''Combine stock information into one table, from folder of raw JSON files'''

    lst = []
    for name, info in data.items():
        json = info()
        json['name'] = name
        lst.append(json)

    return pd.DataFrame.from_records(lst)


def cleanse_investments(investments: pd.DataFrame, portfolio_remap: Dict) -> pd.DataFrame:

    # remove .L from symbols
    investments['symbol_ft'] = investments.Symbol.str.replace(r'.L$', '')

    # remap exchange column, to match column used in ETFs
    investments['stock_exchange'] = investments.Exchange.replace(portfolio_remap)

    # remove blank rows (Type is 'Buy', 'Sell' or 'Dividend')
    investments[~investments.Type.isna()]

    return investments


def extract_current_holdings(investments: pd.DataFrame) -> pd.DataFrame:

    # consider only Type buy or sell with intention of extracting only current holdings
    holdings = investments[investments.Type.isin(['Buy', 'Sell'])]

    # convert Quantity to negative when stocks are sold
    holdings['type_num'] = holdings.Type.replace({'Buy': 1, 'Sell': -1})
    holdings.eval('Quantity = type_num * Quantity', inplace=True) 

    # sum total stocks sold and bought to identify current holdings
    holdings = holdings.groupby(['symbol_ft', 'stock_exchange'])['Quantity'].sum()

    # filter only current holdings
    holdings = (
        holdings[holdings > 0]
        .reset_index()
        .rename({'Quantity': 'shares_held'}, axis=1)
    )

    return holdings

    










