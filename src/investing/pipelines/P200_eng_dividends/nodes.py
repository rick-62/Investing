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
This is a boilerplate pipeline 'P200_eng_dividends'
generated using Kedro 0.17.0
"""

import pandas as pd

from typing import Dict

def extract_latest_dividend_data(historic: Dict):
    
    df_dividend = pd.DataFrame()
    df_dividend.index.name = 'symbol'

    year_ago_date = pd.to_datetime('today') - pd.to_timedelta(380, unit='d')

    for symbol, partition in historic.items():

        df = partition()
        df.date = pd.to_datetime(df.date)
        div = df[df.dividend > 0][['date', 'close', 'dividend']]


        # how often is dividend paid (most recently)
        div['last_year'] = div.date >= year_ago_date
        count_last_years_dividends = sum(div.last_year)

        # verify dividends, otherwise skip and continue remaining
        if count_last_years_dividends == 0:
            continue
        else:
            df_dividend.loc[symbol, 'annual_dividends'] = sum(div.last_year)

        # when is next dividend payment likely to be?
        df_dividend.loc[symbol, 'approx_next_dividend_date'] = \
            min(div.date[div.last_year]) + pd.to_timedelta(365, unit='d')

        # adjust dividend amounts, readjusting for incorrect magnitudes
        div['pct'] = 100 * div.dividend / div.close  # adjusted for GBX
        div.loc[div.pct > 0.1, 'pct'] = div.pct[div.pct > 0.1] / 100  # no single dividend is greater than 10%
        
        # how much is next dividend payment likely to be? (as percentage of previous dividend close)
        df_dividend.loc[symbol, 'approx_next_dividend_pct'] = \
            div.pct[div.date == min(div.date[div.last_year])].iloc[0]
                
        # what is estimated dividend annual yield
        df_dividend.loc[symbol, 'dividend_yield'] = sum(div.loc[div.last_year, 'pct'])

    return df_dividend.reset_index()





