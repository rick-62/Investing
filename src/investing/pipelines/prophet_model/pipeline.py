from kedro.pipeline import node, Pipeline

from .nodes import (
    apply_prophet_model,
)

def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        [
            node(
                func=apply_prophet_model,
                inputs='etf_historical',
                outputs='etf_forecasts',
                name='prophet_etfs'
            ),
        ]
    )