from kedro.pipeline import node, Pipeline

from .nodes import (
    get_stock_lists,
    download_etfs_historical,
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
                func=download_etfs_historical,
                inputs=['etfs', 'params:from_date'],
                outputs='etfs_historical',
                name='download_etfs_historical'
            )

        ]
    )