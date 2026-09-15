# Data Directory

This directory contains datasets used by the Real-Time Fraud Detection Pipeline.

## Required Dataset

To run the pipeline locally or train the ML model, download the **Credit Card Fraud Detection** dataset:

- **Source:** [Kaggle - Credit Card Fraud Detection](https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud)
- **Target File:** `creditcard.csv` (~150 MB)
- **Placement:** Place the downloaded `creditcard.csv` directly into this directory:
  ```
  data/
  └── creditcard.csv
  ```

> **Note:** `*.csv` files in this directory are excluded by `.gitignore` to keep the git repository lightweight and stay within GitHub's 100MB file size limit.
