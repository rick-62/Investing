from kedro.pipeline import node, Pipeline

from .nodes import (
    cleanse_freetrade,
    join_freetrade_etfs,
)

def create_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=cleanse_freetrade,
                inputs=['freetrade', 'params:mic_remap'],
                outputs='freetrade_cleansed'
            ),
            node(
                func=join_freetrade_etfs,
                inputs=['freetrade_cleansed', 'investpy_etfs'],
                outputs='etfs'
            ),
        ]
    )