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

"""Project hooks."""
import warnings
from typing import Any, Dict, Iterable, Optional

from kedro.config import ConfigLoader
from kedro.config import TemplatedConfigLoader 
from kedro.framework.hooks import hook_impl
from kedro.io import DataCatalog
from kedro.pipeline import Pipeline
from kedro.versioning import Journal

from investing.pipelines.P100_ext_freetrade import pipeline as P100_ext_freetrade
from investing.pipelines.P110_ext_portfolio import pipeline as P110_ext_portfolio
from investing.pipelines.P120_ext_investpy import pipeline as P120_ext_investpy
from investing.pipelines.P130_ext_alpha_vantage import pipeline as P130_ext_alpha_vantage
from investing.pipelines.P200_eng_dividends import pipeline as P200_eng_dividends
from investing.pipelines.P210_eng_stock_objects import pipeline as P210_eng_stock_objects
from investing.pipelines.P300_strat001_hma_distributions import pipeline as P300_strat001_hma_distributions

warnings.filterwarnings("ignore", category=DeprecationWarning)      # ignore depracation warnings

class ProjectHooks:
    @hook_impl
    def register_pipelines(self) -> Dict[str, Pipeline]:
        """Register the project's pipeline.

        Returns:
            A mapping from a pipeline name to a ``Pipeline`` object.

        """

        pipelines = {
            "P100_ext_freetrade": P100_ext_freetrade.create_pipeline(),
            "P110_ext_portfolio": P110_ext_portfolio.create_pipeline(),
            "P120_ext_investpy": P120_ext_investpy.create_pipeline(),
            "P130_ext_alpha_vantage": P130_ext_alpha_vantage.create_pipeline(),
            "P200_eng_dividends": P200_eng_dividends.create_pipeline(),
            "P210_eng_stock_objects": P210_eng_stock_objects.create_pipeline(),
            "P300_strat001_hma_distributions": P300_strat001_hma_distributions.create_pipeline(),
            }

        default = {
            "__default__": (
                pipelines["P100_ext_freetrade"] + 
                pipelines["P110_ext_portfolio"] + 
                pipelines["P120_ext_investpy"] + 
                pipelines["P130_ext_alpha_vantage"] + 
                pipelines["P200_eng_dividends"] + 
                pipelines["P210_eng_stock_objects"] + 
                pipelines["P300_strat001_hma_distributions"] 
            )
        }

        return {**pipelines, **default}


    @hook_impl
    def register_config_loader(self, conf_paths: Iterable[str]) -> ConfigLoader:
        return TemplatedConfigLoader(
            conf_paths,
            globals_pattern="*globals.yml",  # read the globals dictionary from project config
            globals_dict={  # extra keys to add to the globals dictionary, take precedence over globals_pattern
            },
        )

    @hook_impl
    def register_catalog(
        self,
        catalog: Optional[Dict[str, Dict[str, Any]]],
        credentials: Dict[str, Dict[str, Any]],
        load_versions: Dict[str, str],
        save_version: str,
        journal: Journal,
    ) -> DataCatalog:
        return DataCatalog.from_config(
            catalog, credentials, load_versions, save_version, journal
        )
