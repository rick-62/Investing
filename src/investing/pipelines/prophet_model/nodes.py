
import logging
import warnings
from typing import Dict, Any

import matplotlib.pyplot as plt
import pandas as pd
from fbprophet import Prophet

plt.style.use('fivethirtyeight')

log = logging.getLogger(__name__)

def _transform_model_input(data: pd.DataFrame) -> pd.DataFrame:
    '''Prepare data prior to fitting to Prophet model'''
    data.rename(columns={'Close': 'y', 'Date': 'ds'}, inplace=True)
    data.index = pd.to_datetime(data['ds'])
    return data

def _fit_prophet_model(data: pd.DataFrame) -> pd.DataFrame:
    '''Fit and make future prediction using Prophet model'''
    model = Prophet()
    model.fit(data)
    forecast = model.make_future_dataframe(periods=90, freq='D')
    forecast = model.predict(forecast)
    forecast.set_index('ds', inplace=True)
    return forecast

def apply_prophet_model(datasets: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
    '''Loop through historic data and apply prophet model'''

    forecasts = {}

    i = 0
    for name, data in datasets.items():
        i += 1
        if i > 4: break
        _data = _transform_model_input(data())
        forecasts[name] = _fit_prophet_model(_data)

    return forecasts






