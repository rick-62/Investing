# Pipeline P100_ext_freetrade

> *Note:* This is a `README.md` boilerplate generated using `Kedro 0.17.0`.

## Overview

This pipeline manages the import of a master list of available stocks from Freetrade.

This process is currently manual and requires the data to be downloaded from the Freetrade website, via Google Sheets. The first sheet must be copied over at least once and from then on can be occasionally copied over, when new stocks become available. 

**from:**
https://docs.google.com/spreadsheets/d/14Ep-CmoqWxrMU8HshxthRcdRW8IsXvh3n2-ZHVCzqzQ/edit#gid=1855920257

**into:**
data > 01_raw > manual > freetrade_stocks.csv


## Pipeline inputs

data
- raw_csv_portfolio

params
- portfolio_remap


## Pipeline outputs

- inmem_portfolio_cleansed
- primary_csv_current_holdings
