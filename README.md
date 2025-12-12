
# Multi-touch Attribution Pipeline (Production)

This repository contains a production-grade implementation of Multi-touch Attribution using **Markov Chains** on the **Databricks Lakehouse Platform**.

## Overview
This project processes ad impression and conversion data to assign credit to various marketing channels. It compares heuristic methods (First-Touch, Last-Touch) with data-driven **Markov Chains**.

## Features
*   **Production Package Structure**: Logic is modularized in `src/multitouch`.
*   **Automated Testing**: Unit tests for data generation and ingestion using `pytest`.
*   **Databricks Jobs**: Defined as code `databricks.yml` (Databricks Asset Bundle).
*   **CI/CD**: GitHub Actions workflow for linting and testing.
*   **Lakehouse Architecture**: Uses Delta Lake for Bronze, Silver, and Gold layers.

## Installation & Development
1.  **Clone the repo**:
    ```bash
    git clone https://github.com/YOUR_USERNAME/multitouch-attribution-pipeline.git
    cd multitouch-attribution-pipeline
    ```

2.  **Install dependencies**:
    ```bash
    pip install -r requirements.txt
    pip install -e .
    ```

3.  **Run Tests**:
    ```bash
    pytest tests/
    ```

## deployment
Deploy to Databricks using the Databricks CLI:
```bash
databricks bundle deploy
```


* Broadly speaking, heuristic methods are rule-based and consist of both `single-touch` and `multi-touch` approaches. Single-touch methods, such as `first-touch` and `last-touch`, assign credit to the first channel, or the last channel, associated with a conversion. Multi-touch methods, such as `linear` and `time-decay`, assign credit to multiple channels associated with a conversion. In the case of linear, credit is assigned uniformly across all channels, whereas for time-decay, an increasing amount of credit is assigned to the channels that appear closer in time to the conversion event.

* In contrast to heuristic methods, data-driven methods determine assignment using probabilites and statistics. Examples of data-driven methods include `Markov Chains` and `SHAP`. In this series of notebooks, we cover the use of Markov Chains and include a comparison to a few heuristic methods.

## About This Series of Notebooks

* This series of notebooks is intended to help you use multi-touch attribution to optimize your marketing spend.

* In support of this goal, we will:
 * Generate synthetic ad impression and conversion data.
 * Create a streaming pipeline for processing ad impression and conversion data in near real-time.
 * Create a batch pipeline for managing summary tables used for reporting, ad hoc queries, and decision support.
 * Calculate channel attribution using Markov Chains.
 * Create a dashboard for monitoring campaign performance and optimizing marketing spend.

&copy; 2022 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.

|Library Name|Library license | Library License URL | Library Source URL |
|---|---|---|---|
|Matplotlib|Python Software Foundation (PSF) License |https://matplotlib.org/stable/users/license.html|https://github.com/matplotlib/matplotlib|
|Numpy|BSD-3-Clause License|https://github.com/numpy/numpy/blob/master/LICENSE.txt|https://github.com/numpy/numpy|
|Pandas|BSD 3-Clause License|https://github.com/pandas-dev/pandas/blob/master/LICENSE|https://github.com/pandas-dev/pandas|
|Python|Python Software Foundation (PSF) |https://github.com/python/cpython/blob/master/LICENSE|https://github.com/python/cpython|
|Seaborn|BSD-3-Clause License|https://github.com/mwaskom/seaborn/blob/master/LICENSE|https://github.com/mwaskom/seaborn|
|Spark|Apache-2.0 License |https://github.com/apache/spark/blob/master/LICENSE|https://github.com/apache/spark|

To run this accelerator, clone this repo into a Databricks workspace. Attach the RUNME notebook to any cluster running a DBR 11.0 or later runtime, and execute the notebook via Run-All. A multi-step-job describing the accelerator pipeline will be created, and the link will be provided. Execute the multi-step-job to see how the pipeline runs.

The job configuration is written in the RUNME notebook in json format. The cost associated with running the accelerator is the user's responsibility.
