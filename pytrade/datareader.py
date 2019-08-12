'''
Handles the downloading, storage and retrieval of requested historic stock data.
'''

import json
import os
import requests
import pandas as pd


DATA_DIR = 'datastore'
HIST_DIR = 'datastore/historic'
STOCKLST = 'datastore/freetrade_stocks.csv'
HIST_URL = 'https://api.worldtradingdata.com/api/v1/history?symbol={}&sort={}&api_token={}&output={}'
STCK_URL = 'https://api.worldtradingdata.com/api/v1/stock?symbol={}&api_token={}'


# create directories if don't exist
try:
    os.makedirs(HIST_DIR)
except FileExistsError:
    pass

# get api token from separate json file
with open('tokens.json', 'r') as f:
    TOKEN = json.load(f)['worldtradingdata']


def history(symbol, force_download=False, source='worldtradingdata'):
    # historic data
    pass

def stocklist(platform='freetrade', type='etf'):
    # list of stocks - stored on master csv/xl sheet
    pass

def stock(symbol, force_download=False, source='worldtradingdata'):
    # latest data for a stock as json/dict, including some meta data
    # store locally as JSON
    pass

def _download_historic_data(symbol, sort='oldest'):
    '''
    retrieves historic data for a target stock, 
    using World Trading Data as a source. 
    
    data returned as a pandas dataframe.
    
    symbol: requires symbol for stock
    sort: specify how the data is ordered
    '''
    # print('downloading')
    url = HIST_URL.format(symbol, sort, TOKEN, 'csv')
    
    data = pd.read_csv(url, sep=',', index_col='Date', parse_dates=True)

    if not _store_data(symbol, data):
        print('Data not stored: Unknown reason')
    
    return data
    

def get_latest_data(symbols=[]):

    stocks = []

    # get latest data for 5 stocks at a time
    n = 5
    for i in range(0, len(symbols), n):
        chunk = symbols[i:i+n]
        chunk = ','.join([s+'.L' for s in chunk])
        url = STCK_URL.format(chunk, TOKEN)
        r = requests.get(url)
        stocks += r.json()['data']

    stocks_dct = {ele['symbol'][:-2]: ele for ele in stocks}

    return stocks_dct    



def _store_data(symbol, data, file_type='csv'):
    '''
    Stores data (pandas dataframe) in an appropiate format.
    Success returned as Boolean.

    symbol: symbol used for file name
    data: Pandas dataframe of data to store
    file_type: currently only csv
    '''
    # print('storing')
    directory = '{}/{}.{}'.format(HIST_DIR, symbol, file_type)
    
    if file_type == 'csv':
        data.to_csv(directory)
    else:
        return False

    return True


def _retrieve_data(symbol, force_download=False, file_type='csv'):
    # print('retrieving')
    directory = '{}/{}.{}'.format(HIST_DIR, symbol, file_type)

    if force_download:
        # print('forced downloading')
        return _download_historic_data(symbol)

    try:
        # print('trying to read csv')
        return pd.read_csv(directory, sep=',', index_col='Date', parse_dates=True)
    except FileNotFoundError:
        # print('failed to read csv')
        return _download_historic_data(symbol)


def get_data(symbol, force_download=False, file_type='csv', **kwargs):

    if symbol.startswith('^'):
        xsymbol = symbol
    else:
        xsymbol = symbol + '.L'

    data = _retrieve_data(xsymbol, force_download=force_download, file_type=file_type)

    return data