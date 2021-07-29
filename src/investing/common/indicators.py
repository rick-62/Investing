import numpy as np
import pandas as pd
import warnings
import scipy.ndimage

from typing import List, Optional

np.seterr(invalid='ignore')

def suppress_runtime_warning():
    def decorate(func):
        def call(*args, **kwargs):
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                result = func(*args, **kwargs)
            return result
        return call
    return decorate


def RSI(close, n):
    """Relative Strength Index: Compares magnitude of recent gains and losses"""

    # convert close to float
    close = close.astype(float)

    # calc difference between adjacent prices
    deltas = np.diff(close)

    # seed is a list of first n differences
    seed = deltas[: n + 1]

    # separately sum increases & decreases and average over n
    up = seed[seed >= 0].sum() / n
    down = -seed[seed < 0].sum() / n

    # proportion of increases to decreases
    if down == 0:
        rs = 1
    else:
        rs = up / down

        # create list of length prices filled with zeros
    rsi = np.zeros_like(close)

    # first n values calculated using this formula
    rsi[:n] = 100. - 100. / (1. + rs)

    # calculate remaining rsi values on at a time
    for i in range(n, len(close)):
        delta = deltas[i - 1]  # next delta in delta list

        # determine if increase or decrease
        if delta > 0:
            upval = delta
            downval = 0.
        else:
            upval = 0.
            downval = -delta

        # sum with new values and create new average
        up = (up * (n - 1) + upval) / n
        down = (down * (n - 1) + downval) / n

        # proportion of increases to decreases
        if down == 0 or np.isnan(down):
            rs = 1
        else:
            rs = up / down

        # next rsi calculated
        rsi[i] = 100. - 100. / (1. + rs)

    # replaces first n values with nan alternatives
    rsi[:n] = np.nan

    return rsi

def CCI(high, low, close, win):
    """Commodity Channel Index: compares current price to average"""
    pt = np.divide(high + low + close, 3)
    sma = _simple_moving_average(pt, win)
    tpt = _simple_moving_std(pt, win)
    cci = (pt - sma) / (0.015 * tpt)
    return cci

@suppress_runtime_warning()
def STOK(high, low, close, n):
    """Stochastic Oscillator"""
    high_win = _simple_moving_max(high, n)
    low_win = _simple_moving_min(low, n)
    k = np.divide(close - low_win, np.subtract(high_win, low_win))
    return 100 * k

def OBV(close, volume):
    """On-Balance Volume"""
    obv = [0]
    deltas = np.diff(close)
    for i, d in enumerate(deltas):
        if d == 0:
            obv.append(obv[-1])
        elif d > 0:
            obv.append(obv[-1] + volume[i+1])
        elif d < 0:
            obv.append(obv[-1] - volume[i+1])
    return np.array(obv)

def OBVMA(close, volume, n):
    """On-Balance Volume Moving Average"""
    return _simple_moving_average(OBV(close, volume), n)

@suppress_runtime_warning()
def WilliamsPctRange(high, low, close, n):
    """Returns William's % range"""
    low_n = _rolling_min(low, n, dir=1)
    high_n = _rolling_max(high, n, dir=1)
    return -100 * (high_n - close) / (high_n - low_n)

def MoneyFlow(high, low, close, volume, n):
    """Returns Money Flow Index based on RSI"""
    pt = np.divide(high + low + close, 3)
    mfr = np.multiply(pt, volume)
    return RSI(mfr, n)

def PPO(close, n, m):
    """Returns Percentage Price Oscillator using EMA"""
    ppo = np.multiply(np.divide(_ema(close, n) - _ema(close, m),
                                _ema(close, m)), 100)
    return ppo

def RelativeEMA(close, n):
    """Returns EMA relative to price"""
    multiplier = 2 / (n + 1)
    shifted_ema = _shift(_ema(close, n), 1, len(close))
    return np.multiply(close - shifted_ema, multiplier) + shifted_ema

def EMACD(close, n, m):
    """Calculates Exponential Moving Average Difference"""
    ema1 = _ema(close, n)
    ema2 = _ema(close, m)
    return 100 * (ema1 - ema2) / ema2

def PROC(close, n):
    """Price Rate of Change"""
    shifted_close = _shift(close, n, len(close))
    return np.multiply(
        np.divide(close - shifted_close, shifted_close), 100)

def ATR(high, low, close, n):
    """Average True Range, to measure volatility range (stop loss)"""
    high_low = high - low
    high_close = np.abs(high - close.shift())
    low_close = np.abs(low - close.shift())
    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    true_range = np.max(ranges, axis=1)
    return _rolling_sum(true_range, n, dir=1) / n

def Weekday(date):
    """Day of the week as an integer"""
    weekdays = pd.DataFrame(date)
    return np.array(weekdays.date.dt.weekday_name)

def Y(close, threshold=0, polarity='pos', days=1):
    if polarity == 'neg':
        return _diff(num=_rolling_min(close, days), den=close) < threshold
    elif polarity == 'pos':
        return _diff(num=_rolling_max(close, days), den=close) > threshold
    else:
        raise NameError


def count_true(y):
    """Counts number of instances where value went over the threshold"""
    return np.sum(y)


def gaussian_filter(a, sigma):
    """Smoothing filter applied to raw stock data"""
    return scipy.ndimage.gaussian_filter(a, sigma)


def _rolling_window(a, win):
    """Efficiently creates rolling arrays to perform functions on"""
    shape = a.shape[:-1] + (a.shape[-1] - win + 1, win)
    a = np.array(a) # strides removed from Pandas Series so requires converting
    strides = a.strides + (a.strides[-1],)
    return np.lib.stride_tricks.as_strided(a, shape=shape, strides=strides)

def _simple_moving_average(a, win):
    """Uses the rolling window function to return rolling mean"""
    return _shift(np.mean(_rolling_window(a, win), axis=1), win, len(a))


def _ema(a, win):
    """Calculates Exponential Moving Average"""
    weights = np.exp(np.linspace(-1., 0., win))
    weights /= weights.sum()
    ema = np.convolve(a, weights)[:len(a)]
    ema[:win] = ema[win]
    return ema


def _simple_moving_std(a, win):
    """Uses the rolling window function to return rolling std"""
    return _shift(np.std(_rolling_window(a, win), axis=1), win, len(a))


def _simple_moving_max(a, win):
    """Uses the rolling window function to return rolling max"""
    return _shift(np.max(_rolling_window(a, win), axis=1), win - 1, len(a))


def _simple_moving_min(a, win):
    """Uses the rolling window function to return rolling max"""
    return _shift(np.min(_rolling_window(a, win), axis=1), win - 1, len(a))


def _simple_moving_median(a, win):
    """Uses the rolling window function to return rolling median"""
    return _shift(np.median(_rolling_window(a, win), axis=1), win, len(a))    


def _rolling_max(a, days, dir=-1):
    """Uses the rolling window function to return rolling maximum"""
    shift = dir * (days - 1)
    return _shift(np.nanmax(_rolling_window(a, days), axis=1), shift, len(a))


def _rolling_min(a, days, dir=-1):
    """Uses the rolling window function to return rolling minimum"""
    shift = dir * (days - 1)
    return _shift(np.nanmin(_rolling_window(a, days), axis=1), shift, len(a))


def _rolling_sum(a, days, dir=-1):
    """Uses the rolling window function to return rolling minimum"""
    shift = dir * (days - 1)
    return _shift(np.sum(_rolling_window(a, days), axis=1), shift, len(a))

def _rolling_median(a, days, dir=-1):
    """Uses the rolling window function to return rolling median"""
    shift = dir * (days - 1)
    return _shift(np.median(_rolling_window(a, days), axis=1), shift, len(a))


def _shift(arr, num, len_, fill_value=np.nan):
    """specific for padding/shifting days and windows"""
    result = np.empty(len_)
    if num > 0: 
        result[:num] = fill_value
        result[num:] = arr[:len_-num]
    elif num < 0:
        result[num:] = fill_value
        result[:num] = arr[-len_-num:]
    else:
        result = arr
    return result


def _diff(num, den):
    """calculates % diff between two series and returns series"""
    return 100 * (num - den) / den

def _convert_str_to_date(array):
    return pd.to_datetime(array, format='%Y-%m-%d')


def _scaling(arr, lower, upper):
    """normalises an array between the upper and lower"""
    return (upper - lower) * ( arr - np.min(arr) ) / ( np.max(arr) - np.min(arr) )

def TRAMA(series: pd.Series, n: int):

    def hh():
        _max = series.rolling(window=n).max()
        return np.maximum(np.sign(_max - _max.shift(1)), 0).fillna(0)

    def ll():
        _min = series.rolling(window=n).min()
        return np.maximum(np.sign(_min.shift(1) - _min), 0).fillna(0)

    def tc():
        def squared(x): return np.power(x, 2)
        return squared(
            np.logical_or( ll(), hh() ).astype('uint8')
            .rolling(window=n).mean()
        )

    ama = []
    for a, b in zip(series, tc()):
        if not ama:
            nxt = a + b
        else:
            nxt = ama[-1] + b * (a - ama[-1])
        if np.isnan(nxt):
            ama.append(a)
        else:
            ama.append(nxt)

    return ama

def weighted_moving_average(series: List[float], lookback: Optional[int] = None) -> float:
    if not lookback:
        lookback = len(series)
    if len(series) == 0:
        return 0
    assert 0 < lookback <= len(series)

    wma = 0
    lookback_offset = len(series) - lookback
    for index in range(lookback + lookback_offset - 1, lookback_offset - 1, -1):
        weight = index - lookback_offset + 1
        wma += series[index] * weight
    return wma / ((lookback ** 2 + lookback) / 2)


def hull_moving_average_point(series: List[float], lookback: int) -> float:
    assert lookback > 0
    hma_series = []
    for k in range(int(lookback ** 0.5), -1, -1):
        s = series[:-k or None]
        wma_half = weighted_moving_average(s, min(lookback // 2, len(s)))
        wma_full = weighted_moving_average(s, min(lookback, len(s)))
        hma_series.append(wma_half * 2 - wma_full)
    return weighted_moving_average(hma_series)






    


    