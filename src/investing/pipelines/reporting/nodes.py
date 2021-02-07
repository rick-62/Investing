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
    etf_historical_meta: pd.DataFrame
    ) -> pd.DataFrame:
    '''Combine etfs, forecasts and information to create a master table'''

    merge1 = pd.merge(etfs, etf_information, left_on='name', right_on='ETF Name', how='left', suffixes=('_source', ''))
    merge2 = pd.merge(merge1, forecast, on='name', how='left', suffixes=('', '_forecast'))
    merge3 = pd.merge(merge2, etf_historical_meta, on='name', how='left')
    return merge3


def clean_etf_summary(etf_combined_data: pd.DataFrame) -> pd.DataFrame:
    '''select & rename columns, and extract the days highs and lows'''

    print(etf_combined_data.info())
    # Filter columns to keep
    summary = etf_combined_data.filter(
        [
            'title',
            'long_title',
            'subtitle',
            'currency',
            'isa_eligible',
            'isin',
            'mic',
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
        ], 
        axis=1,
    )

    # Rename columns
    summary.rename(
        {'name_source': 'symbol_investpy', 'name': 'filename'},
        axis='columns',
        inplace=True
    )

    # Split day range to extract high and low
    summary[['day_low', 'day_high']] = summary['Todays Range'].str.split(' - ', 1, expand=True).replace({',': ''}, regex=True).astype('float32')
    summary.drop(['Todays Range'], axis='columns', inplace=True)

    # tidy column names
    summary.columns = [c.lower().replace(' ', '_') for c in summary.columns]

    return summary


def enrich_etf_summary(summary: pd.DataFrame) -> pd.DataFrame:
    '''Create computed columns which can be used directly for determining whether to sell or buy'''
    
    summary.eval('pct_over_yhat_upper = (day_high - yhat_upper) / yhat_upper', inplace=True)




def sell_etf(summary: pd.DataFrame) -> pd.DataFrame:
    '''filters and ranks ETFs to sell'''

    # select those to sell only where potential price greater than upper forecast
    sell = summary[summary.day_high > summary.yhat_upper].copy()

    # create percentage difference of price above expected
    sell.eval('pct_over_yhat_upper = (day_high - yhat_upper) / yhat_upper', inplace=True)

    # rank output by percentage over yhat upper
    sell.sort_values(by=['pct_over_yhat_upper'], ascending=False, inplace=True)

    return sell


def buy_etf(summary: pd.DataFrame) -> pd.DataFrame:
    '''filters and ranks ETFs to buy'''
    pass






# create age and buy-sell-hold flags, change from trend, typical annual ROI and how oversold/undersold
# create two outputs: buy and sell ranked datasets based on viable options
# Rank based on:
    # buy = age > 5yrs, ROI, change from trend or how undersold


    


