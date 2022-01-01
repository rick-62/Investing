from kedro.pipeline import node, Pipeline

from .nodes import (
    extract_historical_meta,
    extract_prophet_output,
    combine_etf_outputs,
    clean_etf_summary,
    enrich_etf_summary,
    sell_etf,
    buy_etf,
)


def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        [
            node(
                func=extract_historical_meta,
                inputs="etf_historical",
                outputs="etf_historical_meta",
                name="extract_etfs_historical_meta",
            ),
            node(
                func=extract_prophet_output,
                inputs="etf_forecasts",
                outputs="etf_forecast_master",
                name="extract_etfs_forecast",
            ),
            node(
                func=combine_etf_outputs,
                inputs=[
                    "etf_forecast_master",
                    "etfs",
                    "etf_information",
                    "etf_historical_meta",
                    "current_holdings",
                ],
                outputs="etf_combined_data",
                name="etf_combined_data",
            ),
            node(
                func=clean_etf_summary,
                inputs="etf_combined_data",
                outputs="etf_summary_cleaned",
                name="etf_summary_cleaned",
            ),
            node(
                func=enrich_etf_summary,
                inputs=["etf_summary_cleaned", "params:buy_params"],
                outputs="etf_summary",
                name="etf_summary",
            ),
            node(
                func=sell_etf,
                inputs=["etf_summary", "current_holdings"],
                outputs="etf_sell",
                name="etf_sell",
            ),
            node(
                func=buy_etf,
                inputs=["etf_summary", "params:buy_params"],
                outputs="etf_buy",
                name="etf_buy",
            ),
        ]
    )
