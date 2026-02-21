# processors.py 4:32 PM Feb 21, 2026

import os
import sys
from pathlib import Path

# Resolve project root for absolute imports
root = Path(__file__).resolve().parent.parent
if str(root) not in sys.path:
    sys.path.append(str(root))

import pandas as pd
from src.storage import DataVault

class DataCleaner:
    def __init__(self):
        self.vault = DataVault()

    def clean_futures_data(self, symbol_filename: str):
        # Implementation of Bronze to Silver transition logic
        # Handles data validation, gap filling, and feature derivation
        print(f"Starting cleaning process for: {symbol_filename}")

        try:
            df = self.vault.load_df(layer="bronze", filename=symbol_filename)
        except FileNotFoundError:
            print(f"Error: Could not find {symbol_filename} in bronze folder.")
            return None

        # Data integrity checks: removal of duplicate indices and chronological sorting
        initial_rows = len(df)
        df = df[~df.index.duplicated(keep='first')]
        df = df.sort_index()

        # Application of forward-fill logic to ensure continuous time series
        df = df.ffill()

        # Quantitative feature engineering: derivation of periodic returns
        df['Returns'] = df['Close'].pct_change()

        # Persist refined dataset to Silver layer
        clean_name = symbol_filename.replace("_raw", "")
        clean_filename = f"{clean_name}_cleaned"

        self.vault.save_df(df, layer="silver", filename=clean_filename)

        print(f"Processed {initial_rows} rows. Saved to Silver as {clean_filename}")
        return df

if __name__ == "__main__":
    cleaner = DataCleaner()

    # Flush Silver layer to maintain idempotency in development environment
    cleaner.vault.clear_layer("silver")

    # Orchestrate cleaning for the core asset universe
    assets_to_clean = ["ES_F_raw", "NQ_F_raw", "CL_F_raw"]

    for asset in assets_to_clean:
        cleaner.clean_futures_data(asset)