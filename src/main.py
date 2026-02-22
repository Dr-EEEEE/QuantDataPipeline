import os
import sys
from pathlib import Path

# Resolve project root for package availability
root = Path(__file__).resolve().parent.parent
if str(root) not in sys.path:
    sys.path.append(str(root))

from src.ingestors import YahooIngestor, IBKRIngestor, TradeStationIngestor
from src.processors import DataCleaner

def run_pipeline(symbol: str, source: str = "yahoo"):
    # Unified orchestration logic for cross-source data ingestion and processing
    print(f"Starting Pipeline for: {symbol} using {source}")

    # Factory-style selection for data provider interfaces
    if source == "yahoo":
        ingestor = YahooIngestor()
    elif source == "ibkr":
        ingestor = IBKRIngestor()
    elif source == "tradestation":
        ingestor = TradeStationIngestor()
    else:
        print(f"Unknown source: {source}")
        return

    # Phase 1: Ingestion and persistence to Bronze layer
    ingestor.fetch_data(symbol)

    # Phase 2: Refinement and persistence to Silver layer
    cleaner = DataCleaner()
    # Normalize naming conventions for downstream processing
    raw_filename = f"{symbol.replace('=', '_').replace('^', '').replace('/', '_')}_raw"
    cleaner.clean_futures_data(raw_filename)

    print(f"Finished processing {symbol}")

if __name__ == "__main__":
    # Central configuration for asset universe and active data feed
    active_source = "yahoo"
    symbols_to_track = ["ES=F", "NQ=F", "CL=F"]

    for sym in symbols_to_track:
        try:
            run_pipeline(sym, source=active_source)
        except Exception as e:
            print(f"Failed to process {sym}: {e}")

    print("Pipeline execution complete. Check data/silver folder.")