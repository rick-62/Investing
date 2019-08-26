
import statistics
import matplotlib.pyplot as plt
from pytrade import datareader
from datetime import datetime as dt
import pandas as pd

def get(stocklist=[], latest_flag=False, refresh_data=False):

    stocks = {}
    for symbol in stocklist:
        stocks[symbol] = Stock(symbol)
    
    if latest_flag:
        refresh_latest(stocks)

    if refresh_data:
        refresh_historic(stocks)

    return stocks


# can be implemented as part of a new object, containing all the stocks...

def refresh_latest(stocks={}):
    latest = datareader.latest(stocks.keys())
    for symbol, obj in stocks.items():
        obj._apply_latest_data(latest[symbol])
    return stocks

def refresh_historic(stocks={}):
    for obj in stocks.values():
        obj.refresh_historic()
    return stocks


class Stock:

    def __init__(self, symbol, **kwargs):
        self.kwargs = kwargs
        self.symbol = symbol
        self.historic_data = self._fetch_historic_data()
    
    def _fetch_historic_data(self, force_download=False):
        """Retrieves historic data and returns dataframe"""
        try:
            df = datareader.history(self.symbol, 
                                    force_download=force_download) 
        except FileNotFoundError:
            df = datareader.history(self.symbol, 
                                    force_download=True) 

        return df

    def refresh_historic(self):
        """Download latest historic data and update self"""
        self.historic_data = self._fetch_historic_data(force_download=True)

    def _apply_latest_data(self, data):
        self.latest_data = data

    @property
    def name(self):
        return self.latest_data['name']

    @property
    def latest_price(self):
        return float(self.latest_data['price'])

    @property
    def currency(self):
        return self.latest_data['currency']

    @property
    def latest_date(self):
        date_as_str = self.latest_data['last_trade_time']
        date_as_dto = dt.strptime(date_as_str, '%Y-%m-%d %H:%M:%S')
        return date_as_dto.date()

    @property
    def start_date(self):
        return min(self.historic_data.index).date()

    @property
    def age(self):
        return (self.latest_date - self.start_date).days / 365

    def median_return(self, years):
        df = self.historic_data
        close_xyrs = df.Close[ df.index.year > max(df.index.year) - years ]
        inc_lst = []
        for yr in set(close_xyrs.index.year):
            yr_close = df.Close[ df.index.year == yr ]
            increase = ( max(yr_close) - min(yr_close) ) / min(yr_close)
            inc_lst.append(increase)
        return statistics.median(inc_lst)
    

        

    




    



    
            



    

    
    

    




