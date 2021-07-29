
import logging
import time
from typing import Dict, Iterable, Any
import warnings

import investpy
import pandas as pd

from alpha_vantage.timeseries import TimeSeries

log = logging.getLogger(__name__)


def get_stock_lists():
    '''Retrieve stock lists as dataframe tables, from investpy library'''

    stocks = investpy.get_stocks()
    etfs = investpy.get_etfs()
    indices = investpy.get_indices()

    return stocks, etfs, indices


def cleanse_freetrade(ft: pd.DataFrame, mic_remap: Dict, symbol_suffix: Dict) -> pd.DataFrame:
    '''Clean and prepare freetrade data'''

    ft.columns = ft.columns.str.lower()
    ft.fractional_enabled.fillna(False, inplace=True)
    ft.symbol = ft.symbol.str.replace(".", "")
    ft['stock_exchange'] = [mic_remap.get(x) for x in ft.mic]  # convert mic to exchange e.g. XLON: London (INVESTPY)
    ft['symbol_alphavantage'] = ft.symbol.str.cat([symbol_suffix.get(x, "") for x in ft.mic])
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

        time.sleep(1)

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
            time.sleep(30)

    warnings.filterwarnings('default')

    log.info(f"{len(parts)} downloaded")
    
    return parts



def download_etf_information(etfs: pd.DataFrame) -> Dict:
    '''Get latest etf information: latest price range, market cap etc.'''

    info = {}

    for i, row in etfs.iterrows():

        file_name = f"etf_{row.symbol_ft}_{row['isin']}"

        time.sleep(1)

        try:
            info[file_name] = investpy.get_etf_information(row['name'], row.country, as_json=True)
            log.debug(f"Download complete (ETF information): {file_name}")

        except:
            log.warning(f"Download FAILED (ETF information): {file_name}")
            time.sleep(30)

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


def download_historic_alpha_vantage(stocks: pd.DataFrame, params: Dict[str, Any], access_key: str) -> Dict[str, pd.DataFrame]:
    '''download historic data from Alpha Vantage, including dividends and splits'''

    ts = TimeSeries(access_key)

    data = {}
    for symbol in stocks.symbol_alphavantage:
        time.sleep(params['sleep'])
        try:
            historic, meta = ts.get_daily_adjusted(symbol, outputsize='full')
            data[symbol] = pd.DataFrame.from_dict(historic, orient='index', dtype='float')

            log.debug(f"Download complete (Alpha Vantage historical): {symbol}")

        except:
            log.warning(f"Download FAILED (Alpha Vantage historical): {symbol}")

    log.info(f"{len(data)} downloaded")
        
    return data


def prepare_historic_alpha_vantage(stocks: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    '''prepare Alpha Vantage historic data'''

    data = {}

    for key, item in stocks.items():

        # rename columns
        stock = (item()
            .rename(
                errors='raise',
                columns={
                    'Unnamed: 0': 'date',
                    '1. open': 'open',
                    '2. high': 'high',
                    '3. low' : 'low',
                    '4. close': 'close',
                    '5. adjusted close': 'adjusted_close',
                    '6. volume': 'volume',
                    '7. dividend amount': 'dividend',
                    '8. split coefficient': 'split_coefficient'
                },
            )
            .sort_values('date', ascending=True)
        )

        data[key] = stock

    return data



 



    










