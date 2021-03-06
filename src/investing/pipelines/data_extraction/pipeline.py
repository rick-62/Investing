from kedro.pipeline import node, Pipeline

from .nodes import (
    get_stock_lists,
    download_etfs_historical,
    cleanse_freetrade,
    cleanse_investments,
    join_freetrade_etfs,
    download_etf_information,
    combine_etf_information,
    extract_current_holdings,
)

def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        [
            node(
                func=get_stock_lists,
                inputs=None,
                outputs=['investpy_stocks', 'investpy_etfs', 'investpy_indices'],
                name='investpy_stock_lists'
            ),
            node(
                func=cleanse_investments,
                inputs=['investments_raw', 'params:portfolio_remap'],
                outputs='investments',
                name='investments_cleansed'
            ),
            node(
                func=cleanse_freetrade,
                inputs=['freetrade', 'params:mic_remap'],
                outputs='freetrade_cleansed'
            ),
            node(
                func=extract_current_holdings,
                inputs='investments',
                outputs='current_holdings',
                name='current_holdings'
            ),
            node(
                func=combine_etf_information,
                inputs='etf_information_raw',
                outputs='etf_information',
                name='combine_etfs_information'
            ),
            node(
                func=join_freetrade_etfs,
                inputs=['freetrade_cleansed', 'investpy_etfs'], 
                outputs='etfs'
            ),
            node(
                func=download_etfs_historical,
                inputs=['etfs', 'params:from_date'],
                outputs='etf_historical',
                name='download_etfs_historical'
            ),
            node(
                func=download_etf_information,
                inputs='etfs',
                outputs='etf_information_raw',
                name='download_etfs_informaton'
            ),

        ]
    )