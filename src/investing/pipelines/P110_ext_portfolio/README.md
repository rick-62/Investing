# Pipeline P110_ext_portfolio

> *Note:* This is a `README.md` boilerplate generated using `Kedro 0.17.0`.

## Overview

This pipeline manages the import of our portfolio, capturing all previous investments as well as all curreny holdings.

This process is currently manual and requires the data to be exported from *My Stocks Portfolio*:
settings > Import/Export and Sync Data > Export CSV to External Storage > Export... > SAVE

Save the csv into the data folder in OneDrive:
01_raw > manual > portfolio.csv


## Pipeline inputs

data
- raw_csv_portfolio

params
- portfolio_remap

## Pipeline outputs

- inmem_portfolio_cleansed
- primary_csv_current_holdings
