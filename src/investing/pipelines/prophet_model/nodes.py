import logging
import os
import warnings
from multiprocessing import Pool
from typing import Any, Dict, ItemsView

import pandas as pd
from fbprophet import Prophet

log = logging.getLogger(__name__)

logging.getLogger("fbprophet").setLevel(
    logging.ERROR
)  # ignore model convergance log message; only raise ERRORs
warnings.simplefilter("ignore", DeprecationWarning)  # ignore depracation warnings


def _transform_model_input(data: pd.DataFrame) -> pd.DataFrame:
    """Prepare data prior to fitting to Prophet model"""
    data.rename(columns={"Close": "y", "Date": "ds"}, inplace=True)
    data.index = pd.to_datetime(data["ds"])
    return data


def _fit_prophet_model(data: pd.DataFrame) -> pd.DataFrame:
    """Fit and make future prediction using Prophet model"""
    model = Prophet()
    with suppress_stdout_stderr():
        model.fit(data)
    forecast = model.make_future_dataframe(periods=90, freq="D")
    forecast = model.predict(forecast)
    forecast.set_index("ds", inplace=True)
    return forecast


def _transform_fit(item):

    name, data = item
    # try:
    _data = _transform_model_input(data())
    return (name, _fit_prophet_model(_data))
    # except:
    #     log.error(f"{name} failed Prophet processing")


def apply_prophet_model(datasets: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
    """Loop through historic data and apply prophet model"""

    try:
        pool = Pool(os.cpu_count() - 1)
        forecasts = pool.map(_transform_fit, datasets.items())
        log.info(f"{len(forecasts)} processed with Prophet")

    finally:
        pool.close()
        pool.join()

    return dict(forecasts)


############################################################


# from https://stackoverflow.com/questions/11130156/suppress-stdout-stderr-print-from-python-functions
class suppress_stdout_stderr(object):
    """
    A context manager for doing a "deep suppression" of stdout and stderr in
    Python, i.e. will suppress all print, even if the print originates in a
    compiled C/Fortran sub-function.
       This will not suppress raised exceptions, since exceptions are printed
    to stderr just before a script exits, and after the context manager has
    exited (at least, I think that is why it lets exceptions through).

    """

    def __init__(self):
        # Open a pair of null files
        self.null_fds = [os.open(os.devnull, os.O_RDWR) for x in range(2)]
        # Save the actual stdout (1) and stderr (2) file descriptors.
        self.save_fds = (os.dup(1), os.dup(2))

    def __enter__(self):
        # Assign the null pointers to stdout and stderr.
        os.dup2(self.null_fds[0], 1)
        os.dup2(self.null_fds[1], 2)

    def __exit__(self, *_):
        # Re-assign the real stdout/stderr back to (1) and (2)
        os.dup2(self.save_fds[0], 1)
        os.dup2(self.save_fds[1], 2)
        # Close the null files
        os.close(self.null_fds[0])
        os.close(self.null_fds[1])
