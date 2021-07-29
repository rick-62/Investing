# Copyright 2020 QuantumBlack Visual Analytics Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
# NONINFRINGEMENT. IN NO EVENT WILL THE LICENSOR OR OTHER CONTRIBUTORS
# BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# The QuantumBlack Visual Analytics Limited ("QuantumBlack") name and logo
# (either separately or in combination, "QuantumBlack Trademarks") are
# trademarks of QuantumBlack. The License does not grant you any right or
# license to the QuantumBlack Trademarks. You may not use the QuantumBlack
# Trademarks or any confusingly similar mark as a trademark for your product,
# or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.

"""
This is a boilerplate pipeline 'P120_ext_investpy'
generated using Kedro 0.17.0
"""

from kedro.pipeline import Pipeline, node
from .nodes import (
    load_investpy_stock_lists,
    create_freetrade_investpy_etf_list,
    download_etf_investpy_historic,
    download_etf_investpy_information
)

def create_pipeline(**kwargs):
    return Pipeline([
        node(
            func=load_investpy_stock_lists,
            inputs=None,
            outputs=[
                'raw_csv_investpy_stocks', 
                'raw_csv_investpy_etfs', 
                'raw_csv_investpy_indices'
                ],
            name='P120_load_investpy_stock_lists'
        ),
        node(
            func=create_freetrade_investpy_etf_list,
            inputs=[
                'int_csv_freetrade_cleansed',
                'raw_csv_investpy_etfs'
                ],
            outputs='int_csv_freetrade_investpy_etf_list',
            name='P120_create_freetrade_investpy_etf_list'
        ),
        node(
            func=download_etf_investpy_historic,
            inputs=[
                'int_csv_freetrade_investpy_etf_list',
                'params:from_date',
                'params:sleep'
            ],
            outputs='raw_parts_investpy_etf_historic',
            name='P120_download_etf_investpy_historic'
        ),
        node(
            func=download_etf_investpy_information,
            inputs=[
                'int_csv_freetrade_investpy_etf_list',
                'params:sleep'
            ],
            outputs='raw_parts_investpy_etf_information',
            name='P120_download_etf_investpy_information'
        )

    ])

