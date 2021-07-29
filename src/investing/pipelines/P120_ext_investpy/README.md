# Pipeline P120_ext_investpy

> *Note:* This is a `README.md` boilerplate generated using `Kedro 0.17.0`.

## Overview

This pipeline downloads all the investpy (investing.com) data.

## Pipeline inputs

data
- int_csv_freetrade_cleansed

params
- from_date
- sleep

## Pipeline outputs

- raw_csv_investpy_stocks
- raw_csv_investpy_etfs
- raw_csv_investpy_indices
- int_csv_freetrade_investpy_etf_list
- raw_parts_investpy_etf_historic
- raw_parts_investpy_etf_information
