# Pipeline P130_ext_alpha_vantage

> *Note:* This is a `README.md` boilerplate generated using `Kedro 0.17.0`.

## Overview

This pipeline downloads all the Alpha Vantage data.

## Pipeline inputs

data
- int_csv_freetrade_investpy_etf_list

params
- sleep
- alpha_vantage_access_key

## Pipeline outputs

- raw_parts_alphavantage_etf_historic
- int_parts_alphavantage_etf_historic_cleansed
