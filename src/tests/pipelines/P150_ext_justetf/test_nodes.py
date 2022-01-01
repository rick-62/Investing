import pickle
from functools import lru_cache
from pathlib import Path

import pandas as pd
import pytest
from bs4 import BeautifulSoup

from investing.pipelines.P150_ext_justetf.nodes import (
    _extract_quote_data,
    _extract_latest_quote_date,
    _extract_expense_ratio,
    _extract_dividend_data,
    create_etf_dividend_summary,
    filter_freetrade_stocks,
)

this_path = Path(__file__).parent.absolute()

@lru_cache(maxsize=2)
def load_sample_response_data(file_name: str) -> BeautifulSoup:
    with open(Path(this_path, file_name), "rb") as pkl:
        response = pickle.load(pkl)
    assert len(response.text) > 1000
    return BeautifulSoup(response.text, 'xml')


@pytest.fixture(params=[
    load_sample_response_data("IE00B3VSSL01_no_dividend.pkl"), 
    load_sample_response_data("IE00BVDPJP67_has_dividend.pkl"),
    ])
def generic_soup(request):
    return request.param


class TestDataExtraction:

    def test__extract_quote_data(self, generic_soup):
        quote_currency, latest_quote = _extract_quote_data(generic_soup)
        assert quote_currency == "GBP"
        whole, decimal = latest_quote.split(".")
        assert whole.isnumeric()
        assert decimal.isnumeric()
        assert int(decimal) < 100

    def test__extract_latest_quote_date(self, generic_soup):
        date_latest_quote = _extract_latest_quote_date(generic_soup)
        d, m, y = date_latest_quote.split(".")
        assert 0 < int(d) <= 31
        assert 0 < int(m) <= 12
        assert int(y) > 20

    def test__extract_expense_ratio(self, generic_soup):
        expense_ratio, expense_ratio_frequency = _extract_expense_ratio(generic_soup)
        assert expense_ratio[-1] == "%"
        assert 0 <= int(expense_ratio[0]) < 2
        assert 0 <= int(expense_ratio[2:4]) < 100

    def test__extract_dividend_data_with_no_dividend(self):
        soup = load_sample_response_data("IE00B3VSSL01_no_dividend.pkl")
        dividend_currency, one_year_dividend = _extract_dividend_data(soup)
        assert dividend_currency is None
        assert one_year_dividend is None

    def test__extract_dividend_data_with_dividend(self):
        soup = load_sample_response_data("IE00BVDPJP67_has_dividend.pkl")
        dividend_currency, one_year_dividend = _extract_dividend_data(soup)
        assert dividend_currency == "GBP"
        whole, decimal = one_year_dividend.split(".")
        assert whole.isnumeric()
        assert decimal.isnumeric()
        assert int(decimal) < 100


def test_filter_freetrade_stocks():
    '''Test ensures only eligible ETFs filtered'''
    fake_data = pd.DataFrame(
        {
            'isin': ['LU1781541096', 'IE0005042456', 'GB0000234222'],
            'isa_eligible': [True, False, True],
            'ETF_flag': [True, True, False],
            'description': ['FTSE100', 'ACC', 'OTHER']
        }
    )
    df = filter_freetrade_stocks(fake_data)
    assert df['isin'].equals(pd.Series(['LU1781541096']))


def test_create_etf_dividend_summary():
    fake_data = {
        'LU1781541096': {
            'quote_currency': 'GBP',
            'latest_quote': '6.78',
            'date_latest_quote': '30.12.21',
            'dividend_currency': 'GBP',
            'one_year_dividend': '0.10',
            'expense_ratio': '0.40%',
            'expense_ratio_frequency': 'p.a.'
        },
        'IE0005042456': {
            'quote_currency': 'GBP',
            'latest_quote': '51.10',
            'date_latest_quote': '30.12.21',
            'dividend_currency': None,
            'one_year_dividend': None,
            'expense_ratio': '0.56%',
            'expense_ratio_frequency': 'p.a.'
        },
    }
    df = create_etf_dividend_summary(fake_data)
    assert df.dtypes['latest_quote'] == 'float64'                 
    assert df.dtypes['date_latest_quote'] == 'datetime64[ns]'
    assert df.dtypes['one_year_dividend'] == 'float64'
    assert df.dtypes['expense_ratio'] == 'float64'
    assert df.dtypes['dividend_yield'] == 'float64'
    assert df.dtypes['net_yield'] == 'float64'
    null_values = df.isna().sum()
    assert null_values['dividend_yield'] == 1
    assert null_values['net_yield'] == 1
    assert null_values['one_year_dividend'] == 1
    assert null_values['latest_quote'] == 0
    assert null_values['expense_ratio'] == 0
    assert null_values['ISIN'] == 0





