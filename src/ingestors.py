import os
import sys
from pathlib import Path

# Resolve project root for absolute imports
root = Path(__file__).resolve().parent.parent
if str(root) not in sys.path:
    sys.path.append(str(root))

import pandas as pd
import yfinance as yf
from src.storage import DataVault

class YahooIngestor:
    def __init__(self):
        self.vault = DataVault()

    def fetch_data(self, symbol: str):
        # Extract ten year daily historical window for specified ticker
        print(f"Fetching 10 years of data for {symbol} from Yahoo Finance")

        df = yf.download(symbol, period="10y", interval="1d", progress=False)

        if df.empty:
            print(f"Warning: No data found for {symbol}")
            return None

        # Sanitize symbols for filesystem compatibility and persist to Bronze layer
        safe_name = symbol.replace("=", "_").replace("^", "").replace("/", "_")
        filename = f"{safe_name}_raw"

        self.vault.save_df(df, layer="bronze", filename=filename)
        return df

class IBKRIngestor:
    def __init__(self):
        self.vault = DataVault()

    def fetch_data(self, symbol: str):
        # Placeholder for Interactive Brokers API integration
        print(f"IBKR Ingestor: Attempting to pull 10 year history for {symbol}")
        print("IBKR API connection not established")
        return None

class TradeStationIngestor:
    def __init__(self):
        self.vault = DataVault()

    def fetch_data(self, symbol: str):
        # Placeholder for TradeStation API integration
        print(f"TradeStation Ingestor: Requested 10 year data for {symbol}")
        print("TradeStation API credentials missing")
        return None

if __name__ == "__main__":
    ingestor = YahooIngestor()
    print("Ingestor initialized. Use main.py to run the full symbol list")