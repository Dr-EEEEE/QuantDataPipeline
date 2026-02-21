# strategies.py 4:42 PM Feb 21, 2026

import os
import sys
from pathlib import Path

# Resolve project root for package availability
root = Path(__file__).resolve().parent.parent
if str(root) not in sys.path:
    sys.path.append(str(root))

import pandas as pd
from src.storage import DataVault

class SG_TrendStrategy:
    def __init__(self):
        self.vault = DataVault()

    def generate_signals(self, symbol_filename):
        # Implementation of trend-following logic using dual moving average crossovers
        # Parameters: Fast SMA (20-period), Slow SMA (120-period)
        print(f"Applying SG Trend Indicator to {symbol_filename}")

        # Retrieve processed time series from Silver layer
        df = self.vault.load_df(layer="silver", filename=symbol_filename)

        # Calculation of statistical technical indicators
        df['SMA_20'] = df['Close'].rolling(window=20).mean()
        df['SMA_120'] = df['Close'].rolling(window=120).mean()

        # Signal generation logic: binary state based on mean reversion/momentum thresholds
        df['Signal'] = 0
        df.loc[df['SMA_20'] > df['SMA_120'], 'Signal'] = 1
        df.loc[df['SMA_20'] < df['SMA_120'], 'Signal'] = -1

        # Persist analytical results to Gold layer for final reporting and backtesting
        gold_filename = symbol_filename.replace("_clean", "_gold")
        if "_gold" not in gold_filename:
            gold_filename = f"{gold_filename}_gold"

        self.vault.save_df(df, layer="gold", filename=gold_filename)

        print(f"Strategy processing complete for {gold_filename}")
        return df

if __name__ == "__main__":
    # Execution block for strategy derivation across core asset universe
    strat = SG_TrendStrategy()

    # Define analytical pipeline scope based on Silver layer availability
    symbols_to_process = ["ES_F_cleaned", "NQ_F_cleaned", "CL_F_cleaned"]

    for file_name in symbols_to_process:
        try:
            strat.generate_signals(file_name)
        except Exception as e:
            print(f"Could not process {file_name}: {e}")

    print("Process finished for all symbols")