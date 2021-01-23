from kedro.pipeline import node, Pipeline

# from .nodes import

def create_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=lambda x: x,
                inputs=None,
                outputs=None,
                name='placeholder'
            ),
        ]
    )