from kedro.pipeline import node, Pipeline

from .nodes import (
    extract_prophet_output,
    combine_etf_outputs,
    clean_etf_summary,
    sell_etf,
)

def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        [
            node(
                func=extract_prophet_output,
                inputs='etf_forecasts',
                outputs='etf_forecast_master',
                name='extract_etfs_forecast'
            ),
            node(
                func=combine_etf_outputs,
                inputs=['etf_forecast_master', 'etfs', 'etf_information'],
                outputs='etf_combined_data',
                name='etf_combined_data'
            ),
            node(
                func=clean_etf_summary,
                inputs='etf_combined_data',
                outputs='etf_summary',
                name='etf_summary'
            ),
            node(
                func=sell_etf,
                inputs='etf_summary',
                outputs='etf_sell',
                name='etf_sell'
            ),
        ]
    )