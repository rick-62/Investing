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
This is a boilerplate pipeline 'P150_ext_justetf'
generated using Kedro 0.17.0
"""

from kedro.pipeline import Pipeline, node

from .nodes import (
    filter_freetrade_stocks,
    verify_sample_justetf_webscrape,
    download_pages_from_justetf,
    scrape_key_data_from_justetf_reponses,
    create_etf_dividend_summary,
)


def create_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=filter_freetrade_stocks,
                inputs="int_csv_freetrade_cleansed",
                outputs="inmem_freetrade_filtered",
                name="filter_freetrade_stocks"
            ),
            node(
                func=lambda df: frozenset(df['isin']),
                inputs="inmem_freetrade_filtered",
                outputs="inmem_etf_isins",
                name="extract_Freetrade_ETF_isins",
            ),
            node(
                func=verify_sample_justetf_webscrape,
                inputs="params:http_headers",
                outputs="inmem_dummy",
                name="test_justetf_scrape"
            ),
            node(
                func= download_pages_from_justetf,
                inputs=["inmem_etf_isins", "params:http_headers", "inmem_dummy"],
                outputs="raw_dict_justetf_responses",
                name="scrape_justetf",
            ),
            node(
                func=scrape_key_data_from_justetf_reponses,
                inputs="raw_dict_justetf_responses",
                outputs="inmem_justetf_data",
                name="extract_justetf_data"
            ),
            node(
                func=create_etf_dividend_summary,
                inputs="inmem_justetf_data",
                outputs="int_justetf_dividend_summary",
                name="create_justetf_dividend_summary",
            ),
        ]
    )
