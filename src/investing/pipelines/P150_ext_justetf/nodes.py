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
This is a boilerplate pipeline 'P150_ext_justetf'
generated using Kedro 0.17.0
"""

import logging
import re
import time
from typing import Dict, List

import pandas as pd
import requests
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)


# verify webpage is in sitemap5.xml
def _download_justetf_webpage(isin: str, headers: Dict) -> requests.Response:
    '''
    Downloads single JustETF ETF summary page, and returns Requests response object.
    Error checking and logging excluded within this function.
    '''
    response = requests.get(
        url=f'https://www.justetf.com/en/etf-profile.html?isin={isin}', 
        headers=headers
    )
    return response


def _extract_quote_data(soup: BeautifulSoup) -> tuple:
    '''Extract latest quote and currency from JustETF parsed response'''
    quote_currency, latest_quote = (
        soup.find("div", text=re.compile(r"Quote")).parent.find(class_="val")
        .text.replace('\n', ' ').strip().split()
    )
    return quote_currency, latest_quote


def _extract_latest_quote_date(soup: BeautifulSoup) -> str:
    '''Extract date of latest quote from JustETF parsed response'''
    date_latest_quote = (
        soup.find("div", text=re.compile(r"Quote")).parent.find(class_="vallabel")
        .text.replace('\n', ' ').strip().split()[-1]
    )
    return date_latest_quote


def _extract_dividend_data(soup: BeautifulSoup) -> tuple:
    '''
    Extracts previous 12 month dividend and currency from JustETF parsed response..
    If ETF has no dividends, sets variables to None.
    '''
    try:
        dividend_currency, one_year_dividend = (
            soup.body.find('td', text=re.compile(r'Dividends \(last 12 months\)')).parent.find(class_ = 'val2')
            .text.split(None)
        )
    except Exception:
        dividend_currency = None
        one_year_dividend = None
    return dividend_currency, one_year_dividend


def _extract_expense_ratio(soup: BeautifulSoup) -> tuple:
    '''Extract expense ratio and frequency from JustETF parsed response'''
    expense_ratio, expense_ratio_frequency = (
        soup.body.find('div', text=re.compile('Total expense ratio')).parent.find(class_ = 'val')
        .text.split(None)
    )
    return expense_ratio, expense_ratio_frequency


def _scrape_key_data_from_justetf_response(response: requests.Response) -> Dict:
    '''Extracts only required data from a single JustETF response'''

    soup = BeautifulSoup(response.text, 'html5lib')
    
    quote_currency, latest_quote = _extract_quote_data(soup)
    date_latest_quote = _extract_latest_quote_date(soup)
    dividend_currency, one_year_dividend = _extract_dividend_data(soup)
    expense_ratio, expense_ratio_frequency = _extract_expense_ratio(soup)
    
    return {
        'quote_currency': quote_currency,
        'latest_quote': latest_quote,
        'date_latest_quote': date_latest_quote,
        'dividend_currency': dividend_currency,
        'one_year_dividend': one_year_dividend,
        'expense_ratio': expense_ratio,
        'expense_ratio_frequency': expense_ratio_frequency
    }


# ------------------------- #
#           NODES           #
# ------------------------- #

def filter_freetrade_stocks(df: pd.DataFrame) -> pd.DataFrame:
    '''
    filters ETF stocks only
    filters isa eligible stocks only
    filters dividend eligible ETFs (non-ACCumulating) only
    '''
    df.query(f'ETF_flag and isa_eligible', inplace=True)
    return df[~df["description"].str.contains("ACC")]


def verify_sample_justetf_webscrape(headers: Dict) -> None:
    '''Performs live test of sample webpage extracted from JustETF'''
    sample_response = _download_justetf_webpage(isin='LU1781541096', headers=headers)
    sample_response.raise_for_status()
    sample_data = _scrape_key_data_from_justetf_response(sample_response)
    assert not sample_data['dividend_currency'] is None
    assert not sample_data['one_year_dividend'] is None
    assert sample_data['quote_currency']  == 'GBP' 
    assert sample_data['expense_ratio'].endswith('%')
    return True


def download_pages_from_justetf(etf_isins: frozenset, headers: Dict, dummy: None=None, sleep: int=1) -> Dict:
    '''
    Downloads responses from JustETF using stock ISIN references.
    Requires frozenset of etf ISIN references and HTTP header.
    dummy is not required and is simply to ensure testing node runs prior.
    optional sleep argument ensures reasonable time between requests.
    '''
    responses = {}

    for isin in etf_isins:
        time.sleep(sleep)
        response = _download_justetf_webpage(isin, headers=headers)
        if response.status_code == 200:
            responses[isin] = response
        else:
            log.warning(f'JustETF scrape - {response.status_code} - {response.reason}')
    return responses


def scrape_key_data_from_justetf_responses(responses: Dict[str, requests.Response]) -> Dict:
    '''Extracts key data from JustETF responses'''
    return {
        isin: _scrape_key_data_from_justetf_response(response()) 
        for isin, response in responses.items()
        }


def create_etf_dividend_summary(justetf_data: Dict) -> pd.DataFrame:
    '''Converts key data from JustETF to dataframe, converts field types and calculates new fields'''
    etf_summary = (
        pd.DataFrame.from_dict(justetf_data, orient='index', dtype='float')
        .assign(ISIN = lambda x: x.index)
        .assign(expense_ratio = lambda x: x['expense_ratio'].str[:-1].astype(float))
        .assign(date_latest_quote = lambda x: pd.to_datetime(x['date_latest_quote']))
        .eval('dividend_yield = 100 * one_year_dividend / latest_quote')
        .eval('net_yield = dividend_yield - expense_ratio')
    )
    return etf_summary




    








