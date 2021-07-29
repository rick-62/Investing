# Copyright 2020 QuantumBlack Visual Analytics Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
# NONINFRINGEMENT. IN NO EVENT WILL THE LICENSOR OR OTHER CONTRIBUTORS
# BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# The QuantumBlack Visual Analytics Limited ("QuantumBlack") name and logo
# (either separately or in combination, "QuantumBlack Trademarks") are
# trademarks of QuantumBlack. The License does not grant you any right or
# license to the QuantumBlack Trademarks. You may not use the QuantumBlack
# Trademarks or any confusingly similar mark as a trademark for your product,
# or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.
"""
This is a boilerplate pipeline 'P210_eng_stock_objects'
generated using Kedro 0.17.0
"""

import holidays

import pandas as pd
import numpy as np

from typing import Dict
from scipy import stats

from functools import lru_cache
from investing.common.technical_indicators import TA

class Stock():

    today = pd.to_datetime('today')
    eng_hols = holidays.England(years=range(1990, today.year + 1)).keys()
    distributions = ['nct', 'johnsonsu', 'tukeylambda',]

    def __init__(self, symbol, historic, dividends, information) -> None:
        self.symbol = symbol
        self.time_series = self._load_historic(historic)
        self.daily_change = self._calculate_daily_valid_price_difference()
        self._fill_missing_time_series()
        self._load_dividends(dividends)
        self.asset_class = information['Asset Class']


    def _load_historic(self, df):
        
        # convert date format and set index
        df.date = pd.to_datetime(df.date)
        df.set_index('date', inplace=True)

        # create new column to track missing data
        df['missing'] = False

        # set up complete list of dates
        weekdays = set(pd.bdate_range(min(df.index), end=Stock.today).date)
        business_dates = pd.DataFrame(index = weekdays - Stock.eng_hols).sort_index()

        # join to complete list of dates, to fill in missing rows
        df = business_dates.join(df, how='left')
        df['missing'].fillna(True, inplace=True)

        return df

    def _load_dividends(self, div):
        for key, value in div.items():
            setattr(self, key, value)

    def _calculate_daily_valid_price_difference(self, price_column='close'):

        daily_change = pd.DataFrame(index=self.time_series.index)

        daily_change['change'] = np.divide(
            self.time_series[price_column], self.time_series[price_column].shift(1)) - 1

        return daily_change.dropna()['change']


    def fit_distribution(self, custom_dates=None, attr_name=None):

        dc = self.daily_change

        if custom_dates:
            dc = dc.loc[dc.index.intersection(pd.to_datetime(custom_dates))]
        
        results = {}
        for i in Stock.distributions:
            dist = getattr(stats, i)
            param = dist.fit(dc)
            pvalue = stats.kstest(dc, i, args=param)[1]
            results[pvalue] = dist(*param)
        
        best_dist = results[max(results.keys())]

        if attr_name:
            setattr(self, attr_name, best_dist)
        
        return best_dist


    def mean_empirical_return(self, periods: int = 20):
        return (np.mean(self.daily_change) + 1) ** periods


    def mean_distribution_return(self, periods: int = 20):
        dist = self.fit_distribution()
        return (dist.mean() + 1) ** periods


    def _fill_missing_time_series(self, column='close'):
        self.time_series[column].fillna(method='ffill', inplace=True)
        self.time_series[column].fillna(method='bfill', inplace=True)


    def apply_technical_indicator(self, name: str, **params):

        fn = getattr(TA, name)
        series = fn(self.time_series, **params)

        self.time_series[series.name] = series

    def is_dividend_upcoming(self, start, end):
        try: 
            date = pd.to_datetime(self.approx_next_dividend_date)
            return (date >= start) & (date <= end)
        except AttributeError:
            return False

    def upcoming_dividend(self, start, end):
        if self.is_dividend_upcoming(start, end):
            return self.approx_next_dividend_pct
        else:
            return 0

    def latest(self, column_name):
        return self.time_series[column_name].tail(1)[0]

###############################################################################

def create_stock_objects(historic: Dict, info: Dict, dividends: pd.DataFrame):

    dividends.set_index('symbol', inplace=True)

    stocks = {}

    for symbol, data in historic.items():

        df_historic = data()
        information = info[
            [d for d in info.keys() if symbol.split('.')[0] in d][0]]()
        
        try:
            div = dividends.loc[symbol].to_dict()
        except KeyError:
            div = {}

        stocks[symbol] = Stock(symbol, df_historic, div, information)
        
    return stocks


