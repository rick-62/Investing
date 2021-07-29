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
This is a boilerplate pipeline 'P110_ext_portfolio'
generated using Kedro 0.17.0
"""

import pandas as pd
from typing import Dict


def cleanse_portfolio(portfolio: pd.DataFrame, portfolio_remap: Dict) -> pd.DataFrame:

    # remove .L from symbols (for joining to Freetrade)
    portfolio['symbol_ft'] = portfolio.Symbol.str.replace(r'.L$', '', regex=True)

    # remap exchange column, to match column used in ETFs
    portfolio['stock_exchange'] = portfolio.Exchange.replace(portfolio_remap)

    # remove blank rows (Type is 'Buy', 'Sell' or 'Dividend')
    portfolio[~portfolio.Type.isna()]

    return portfolio


def extract_current_holdings(portfolio: pd.DataFrame) -> pd.DataFrame:

    # consider only Type buy or sell with intention of extracting only current holdings
    holdings = portfolio[portfolio.Type.isin(['Buy', 'Sell'])].copy()

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