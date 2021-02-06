import logging
from typing import Any, Dict, ItemsView

import pandas as pd

log = logging.getLogger(__name__)



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


def combine_etf_outputs(forecast: pd.DataFrame, etfs: pd.DataFrame, etf_information: pd.DataFrame) -> pd.DataFrame:
    '''Combine etfs, forecasts and information to create a master table'''

    merge1 = pd.merge(etfs, etf_information, left_on='name', right_on='ETF Name', how='left', suffixes=('_source', ''))
    merge2 = pd.merge(merge1, forecast, on='name', how='left', suffixes=('', '_forecast'))
    return merge2


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
            'name'
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


def sell_etf(summary: pd.DataFrame) -> pd.DataFrame:
    '''filters and ranks ETFs to sell'''

    # select those to sell only where potential price greater than upper forecast
    sell = summary[summary.day_high > summary.yhat_upper].copy()

    # create percentage difference of price above expected
    sell.eval('pct_over_yhat_upper = (day_high - yhat_upper) / yhat_upper', inplace=True)

    # rank output by percentage over yhat upper
    sell.sort_values(by=['pct_over_yhat_upper'], ascending=False, inplace=True)

    return sell






# create age and buy-sell-hold flags, change from trend, typical annual ROI and how oversold/undersold
# create two outputs: buy and sell ranked datasets based on viable options
# Rank based on:
    # buy = age > 5yrs, ROI, change from trend or how undersold


    


