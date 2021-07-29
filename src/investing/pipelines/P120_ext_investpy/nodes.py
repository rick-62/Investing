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
This is a boilerplate pipeline 'P120_ext_investpy'
generated using Kedro 0.17.0
"""

import logging
import time
import warnings

import investpy
import pandas as pd

from typing import Dict

log = logging.getLogger(__name__)


def load_investpy_stock_lists():
    '''Retrieve stock lists as dataframe tables, from investpy library'''

    stocks = investpy.get_stocks()
    etfs = investpy.get_etfs()
    indices = investpy.get_indices()

    return stocks, etfs, indices


def create_freetrade_investpy_etf_list(freetrade: pd.DataFrame, investpy_etfs: pd.DataFrame):
    '''
    Join Freetrade list with investpy stock, 
    to create relevant list of etfs
    Joining keys: ISIN, currency and stock_exchange
    '''
    return pd.merge(
        freetrade, investpy_etfs, 
        on=['isin', 'currency', 'stock_exchange'], 
        how='inner', suffixes=('_ft', '')
        )


def download_etf_investpy_historic(etfs: pd.DataFrame, from_date: str, sleep: Dict) -> Dict:
    '''Download investpy ETF historical data, for Freetrade ETFs'''

    today = pd.to_datetime('today').strftime("%d/%m/%Y")

    parts = {}
    
    warnings.filterwarnings('ignore')

    for _, row in etfs.iterrows():

        file_name = f"etf_{row.symbol_ft}_{row['isin']}"

        time.sleep(sleep['normal'])

        try:
            parts[file_name] = investpy.get_etf_historical_data(
                row['name'], row.country, 
                stock_exchange=row.stock_exchange, 
                from_date=from_date, to_date=today, 
                as_json=False, order='ascending'
            )

            log.debug(f"Download complete (ETF investpy historic): {file_name}")

        except:
            log.warning(f"Download FAILED (ETF investpy historic): {file_name}")
            time.sleep(sleep['error'])

    warnings.filterwarnings('default')

    log.info(f"{len(parts)} downloaded")
    
    return parts


def download_etf_investpy_information(etfs: pd.DataFrame, sleep: Dict) -> Dict:
    '''
    Get latest investpy ETF information, for Freetrade ETFs: 
    latest price range, market cap etc.
    '''

    info = {}

    for _, row in etfs.iterrows():

        file_name = f"etf_{row.symbol_ft}_{row['isin']}"

        time.sleep(sleep['normal'])

        try:
            info[file_name] = investpy.get_etf_information(row['name'], row.country, as_json=True)
            log.debug(f"Download complete (ETF investpy information): {file_name}")

        except:
            log.warning(f"Download FAILED (ETF investpy information): {file_name}")
            time.sleep(sleep['error'])

    warnings.filterwarnings('default')

    log.info(f"{len(info)} downloaded")
    
    return info

