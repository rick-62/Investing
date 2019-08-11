
import storage
import datetime
import pandas as pd

def get(symbol, force_download=False, local_only=True):
    '''
    returns stock as object, containing historic data
    and useful attributes.

    symbol : name of stock symbol (str)
    force_download : force redownload data (bool)
    local_only : don't download any missing or new data (bool)
    '''

    return Stock(symbol, force_download=force_download, local_only=local_only)

def get_latest_stock_data(symbols):
    return storage.get_latest_data(symbols)

def symbols():
    df = pd.read_csv(storage.STOCKLST, index_col='i')
    return df.Ticker


class Stock:
    
    def __init__(self, symbol, **kwargs):
        self.kwargs = kwargs
        self.symbol = symbol
        self.historic_data = self.fetch_historic_data()
    
    def fetch_historic_data(self):
        return storage.get_data(self.symbol, **self.kwargs)


    @property
    def latest_date(self):
        return self.historic_data.index.max().date()

    @property
    def latest_data(self):
        today = datetime.date.today()
        latest = self.latest_date
        return latest == today - datetime.timedelta(days=1)

    def update(self):
        if self.latest_data:
            return False
        else:
            storage.get_data(self.symbol, force_download=True)
            return True

    @property
    def latest_price(self):
        return self.historic_data.loc[ self.latest_date, 'Close']



    
            



    

    
    

    




