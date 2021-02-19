import logging
from collections import namedtuple
from statistics import median
from typing import Any, Dict, ItemsView

import pandas as pd

log = logging.getLogger(__name__)


def extract_historical_meta(datasets: Dict[str, Any]) -> pd.DataFrame:
    '''extract useful meta data from historical prices, such as expected annual return rate and years of data'''

    Row = namedtuple('Row', 'name volatility age')
    rows = []

    def _volatility(hist, years=5):
        hist['Year'] = hist.Date.dt.year
        xyrs = hist[hist.Year > (max(hist.Year) - years)]
        grouped = xyrs.groupby('Year')
        mn, mx = grouped.Close.min(), grouped.Close.max()
        return median((mx - mn) / mn)

    def _age(hist):
        start, latest = min(hist.Date), max(hist.Date)
        return (latest - start).days / 365

    for name, data in datasets.items():
        hist = data()
        hist.Date = pd.to_datetime(hist.Date)
        volatility = _volatility(hist)
        age = _age(hist)
        rows.append(Row(name, volatility, age))

    df = pd.DataFrame(data=rows)

    return df


def extract_prophet_output(forecasts: Dict) -> pd.DataFrame:
    '''Combine prophet model outputs, selecting todays forecast'''

    today = pd.to_datetime('today').strftime("%Y-%m-%d")

    lst = []
    for name, data in forecasts.items():
        fcast = data()
        fcast = fcast[fcast.ds == today]
        if len(fcast) == 0:
            log.warn(f"Forecast contains no data for today ({today}): {name}")
        fcast['name'] = name
        lst.append(fcast)

    return pd.concat(lst)


def combine_etf_outputs(
    forecast: pd.DataFrame, 
    etfs: pd.DataFrame, 
    etf_information: pd.DataFrame, 
    etf_historical_meta: pd.DataFrame,
    current_holdings: pd.DataFrame,
    ) -> pd.DataFrame:
    '''Combine etfs, forecasts and information to create a master table'''

    merge1 = pd.merge(etfs, etf_information, left_on='name', right_on='ETF Name', how='left', suffixes=('_source', ''))
    merge2 = pd.merge(merge1, forecast, on='name', how='left', suffixes=('', '_forecast'))
    merge3 = pd.merge(merge2, etf_historical_meta, on='name', how='left')
    merge4 = pd.merge(merge3, current_holdings, on=['symbol_ft', 'stock_exchange'], how='left')
    return merge4


def clean_etf_summary(etf_combined_data: pd.DataFrame) -> pd.DataFrame:
    '''select & rename columns, and extract the days highs and lows'''

    # Filter columns to keep
    summary = etf_combined_data.filter(
        [
            'title',
            'long_title',
            'subtitle',
            'currency',
            'isa_eligible',
            'isin',
            'stock_exchange',
            'symbol_ft',
            'fractional_enabled',
            'plus_only',
            'asset_class',
            'Todays Range',
            'Dividend Yield',
            'name_source',
            'yhat_lower',
            'yhat_upper',
            'yhat',
            'name',
            'volatility',
            'age',
            'shares_held',
        ], 
        axis=1,
    )

    # replace missing shares held with 0
    summary.shares_held.fillna(0, inplace=True)

    # Rename columns
    summary.rename(
        {'name_source': 'symbol_investpy', 'name': 'filename'},
        axis='columns',
        inplace=True
    )

    # Split day range to extract high and low
    summary[['day_low', 'day_high']] = (
        summary['Todays Range']
        .str.split(' - ', 1, expand=True)
        .replace({',': ''}, regex=True)
        .astype('float32')
    )
    summary.drop(['Todays Range'], axis='columns', inplace=True)

    # tidy column names
    summary.columns = [c.lower().replace(' ', '_') for c in summary.columns]

    return summary


def enrich_etf_summary(clean_etf_summary: pd.DataFrame, buy_params: Dict) -> pd.DataFrame:
    '''Create computed columns which can be used directly for determining whether to sell or buy'''
    
    vf = buy_params['volatility_factor']

    summary = (
        clean_etf_summary
        .eval('pct_over_yhat_upper = (day_high - yhat_upper) / yhat_upper')
        .eval('pct_below_yhat_lower = (yhat_lower - day_low) / yhat_lower')
        .eval('sell_flag = day_high > yhat_upper')
        .eval('buy_flag = day_low < yhat_lower')
        .assign(dividend_decimal = lambda df: df.dividend_yield.str.strip('%').astype(float).fillna(0) / 100)
        .eval(f'expected_return = 100 * {vf} * volatility + 100 * dividend_decimal')
    )

    return summary



def sell_etf(summary: pd.DataFrame, current_holdings: pd.DataFrame) -> pd.DataFrame:
    '''filters and ranks ETFs to sell'''

    # merge current holdings back on to summary to keep sight of missing stocks
    sell = pd.merge(summary, current_holdings, on=['symbol_ft', 'stock_exchange'], how='right')

    # rank output by percentage over yhat upper
    sell.sort_values(by=['pct_over_yhat_upper'], ascending=False, inplace=True)

    return sell


def buy_etf(summary: pd.DataFrame, buy_params: Dict) -> pd.DataFrame:
    '''filters and ranks ETFs to buy'''

    age = buy_params['min_age']

    # filter those to buy only
    buy = summary[summary.buy_flag & (summary.age > age)].copy()

    # rank output
    buy.sort_values(by=['expected_return', 'pct_below_yhat_lower'], ascending=False, inplace=True)

    return buy






    


