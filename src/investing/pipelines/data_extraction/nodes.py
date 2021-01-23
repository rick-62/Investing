
import investpy

def get_stock_lists():

    stocks = investpy.get_stocks()
    etfs = investpy.get_etfs()
    indices = investpy.get_indices()

    return stocks, etfs, indices

