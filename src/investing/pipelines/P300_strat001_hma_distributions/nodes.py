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
This is a boilerplate pipeline 'P300_strat001_hma_distributions'
generated using Kedro 0.17.0
"""

import warnings

import pandas as pd
import numpy as np

from collections import Counter
from typing import Counter, Dict, List

warnings.filterwarnings("ignore")


def pre_selected(
    stock: object, asset_class: str = "Equity", row_threshold: int = 1000
) -> bool:
    # apply general filter to select only equities and above threshold row count
    return (stock.asset_class == asset_class) and (
        len(stock.daily_change) > row_threshold
    )


def value_or_momentum(hma: float, trama: float, price: float) -> str:
    # identify which are value and which momentum
    if price > hma:
        return "momentum"
    elif (trama < hma) and (trama > price):
        return "value"
    else:
        return None


def recent_hma_breach(
    time_series: pd.DataFrame, hma_col_name: str, price_col_name: str, period: int = 20
) -> bool:
    recent_data = time_series.tail(period)
    hma = recent_data[hma_col_name]
    price = recent_data[price_col_name]
    return any(hma.tail(period) < price.tail(period)) and any(
        hma.tail(period) > price.tail(period)
    )


def calculate_stop_loss(
    roc: float, hma: float, price: float, period: int = 20
) -> float:
    # cap stop loss at current amount (i.e. if negative)
    stop_loss = hma * (1 + roc / 100) ** period if roc > 0 else hma
    # do not return stop loss if stop loss is not below price
    return stop_loss if stop_loss < price else None


def create_custom_dates(time_series: pd.DataFrame, query: str, offset: int = 0):
    return [
        date + pd.DateOffset(offset) for date in time_series.query(query).index.tolist()
    ]


def loop_through_stocks(
    data: Dict,
    hma_period: int,
    trama_period: int,
    rebalance_period: int,
    roc_column_name: str,
    etfs: List[str],
) -> List[object]:

    hma_period = hma_period
    trama_period = trama_period
    hma_column_name = f"{hma_period} period HMA."
    trama_column_name = f"{trama_period} period TRAMA."

    eligible_stocks = []
    stop_losses = []

    # for each stock
    for symbol in etfs:
        print(symbol)

        stock = data[symbol]()
        stock.apply_technical_indicator("HMA", period=hma_period)
        stock.apply_technical_indicator("TRAMA", period=trama_period)
        stock.apply_technical_indicator("ROC", period=1, column=hma_column_name)

        # print(symbol)

        if not pre_selected(stock):
            continue

        # print(f"{symbol} has been pre-selected")

        stock.itype = value_or_momentum(
            hma=stock.latest(hma_column_name),
            trama=stock.latest(trama_column_name),
            price=stock.latest("close"),
        )

        # HMA line is momentum stop loss
        stock.stop_loss = calculate_stop_loss(
            roc=stock.latest(roc_column_name),
            hma=stock.latest(hma_column_name),
            price=stock.latest("close"),
            period=rebalance_period,
        )

        # create an output of stop losses to update current holdings
        if stock.stop_loss:
            stop_losses.append(stock)

        # for momentum stocks calculate distribution above HMA
        if stock.itype == "momentum":

            stock.fit_distribution(
                custom_dates=create_custom_dates(
                    stock.time_series,
                    query=f"close > `{hma_column_name}`",
                    offset=rebalance_period,
                ),
                attr_name="dist",
            )

            # if stop loss triggered, skip stock
            if not stock.stop_loss:
                continue

            # skip stock if not a recent crossover of HMA, avoiding peaks by only investing at beginning of uptrend
            if not recent_hma_breach(
                stock.time_series,
                hma_col_name=hma_column_name,
                price_col_name="close",
                period=rebalance_period,
            ):
                continue

        # for value stocks calculate total distribution
        elif stock.itype == "value":
            stock.fit_distribution(
                custom_dates=create_custom_dates(
                    stock.time_series,
                    query=f"(close < `{trama_column_name}`) and (`{trama_column_name}` < `{hma_column_name}`)",
                    offset=rebalance_period,
                ),
                attr_name="dist",
            )

        # skip if not appropiate time to invest
        else:
            continue

        print(f"{symbol} added to eligible stocks")
        eligible_stocks.append(stock)

    # print all stop losses
    print("\n\nStop Losses:")
    for stock in stop_losses:
        print(stock.symbol, "\t", stock.stop_loss)

    # return all stocks which are eligible
    return eligible_stocks


class Cash:

    symbol = "Cash"
    stop_loss = 0

    class dist:
        def rvs(n):
            return np.zeros(n)

    @staticmethod
    def upcoming_dividend(start, end):
        return 0


def balance_portfolio(stocks: List[object], rebalance_period: int, top: int, n: int):

    # add cash
    stocks.append(Cash)

    # dates
    today = pd.to_datetime("today")
    start = today + pd.to_timedelta(7 - today.weekday(), unit="d")  # following Monday
    end = start + pd.to_timedelta(
        7 + rebalance_period, unit="d"
    )  # 4 x 7 - 1 (ends Sunday)

    def simulate(minmax):
        cnt = Counter(  # get count of max dist, per index
            getattr(np, minmax)(  # index of max/min, per element (length: n)
                [
                    (1 + stock.dist.rvs(n * rebalance_period))
                    .reshape((n, rebalance_period))
                    .prod(axis=1)
                    + (stock.upcoming_dividend(start, end))  # prod per days (length: n)
                    for stock in stocks
                ],
                axis=0,
            )
        )
        return cnt

    # be able to convert symbol names from index, after simulation
    symbol_map = {i: stock for i, stock in enumerate(stocks)}

    # subtract losses from wins
    result = simulate("argmax") - simulate("argmin")

    # get total of amounts
    denom = sum(result.values())

    # sort and portion stocks
    portfolio = sorted(
        [(symbol_map[key], value / denom) for key, value in result.items()],
        key=lambda x: x[1],
        reverse=True,
    )

    # print results
    for stock, pct in portfolio:
        pct = min(pct, 0.20)  # max 20% per investment
        if stock.stop_loss:
            print(
                stock.symbol,
                f"\t{round(pct, 3)*100:.2f}%",
                f"\tstop loss = {stock.stop_loss:.2f}",
            )
        else:
            print(stock.symbol, f"\t{round(pct, 3)*100:.2f}%")
