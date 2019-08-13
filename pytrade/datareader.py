'''
Handles the downloading, storage and retrieval of requested historic stock data.
'''

import json
import os
import requests
import pandas as pd
from functools import lru_cache

CONFIG = 'config.json'
TOKENS = 'tokens.json'


@lru_cache(maxsize=8)
def token(source='worldtradingdata'):
    '''Retrieves api token for source data and return it as a string'''
    with open(TOKENS, 'r') as f:
        return json.load(f)[source]


@lru_cache(maxsize=32)
def config(*args):
    '''
    Extracts target config data, from config json.

    Arguments make up path to target
    e.g. config("platform", "freetrade", "stocklist")
    returns "datastore/freetrade_stocks.csv" 
    '''
    with open(CONFIG, 'r') as f:
        parent = json.load(f)['config']
    
    for child in args:
        parent = parent.get(child)
    
    return parent

    
def history(symbol, force_download=False, source='worldtradingdata'):

    if force_download:
        base_url = config('source', source, 'history_url')
        url = base_url.format(symbol, TOKEN, 'csv')
        r = requests.get(url)

        # needs completing
        with open('sample_data.csv', 'w+') as f:
            f.write(r.text)

        # then return data as pandas dataframe
        
    else:
        # retrieve data - maybe same function for saving
        # then return data as pandas dataframe 
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