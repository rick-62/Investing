
import pandas as pd
import numpy as np

from finta import TA

@classmethod        
def TRAMA(cls, ohlc: pd.DataFrame, period: int, column='close') -> pd.Series:    

    def hh():
        _max = ohlc[column].rolling(window=period).max()
        return np.maximum(np.sign(_max - _max.shift(1)), 0).fillna(0)

    def ll():
        _min = ohlc[column].rolling(window=period).min()
        return np.maximum(np.sign(_min.shift(1) - _min), 0).fillna(0)

    def tc():
        return np.power(
            np.logical_or( ll(), hh() ).astype('uint8')
            .rolling(window=period).mean(), 2)

    ama = []
    for a, b in zip(ohlc[column], tc()):
        if not ama:
            nxt = a + b
        else:
            nxt = ama[-1] + b * (a - ama[-1])
        if np.isnan(nxt):
            ama.append(a)
        else:
            ama.append(nxt)

    return pd.Series(ama, index=ohlc.index, name="{0} period TRAMA.".format(period))


# monkey-patch custom technical indicators
TA.TRAMA = TRAMA


