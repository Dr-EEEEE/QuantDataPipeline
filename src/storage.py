# storage.py 4:06 PM Feb 21, 2026

import os
import shutil
from pathlib import Path
import pandas as pd

class DataVault:
    def __init__(self):
        # Resolve project root and initialize data directory structure
        self.base_path = Path(__file__).resolve().parent.parent / "data"
        self.layers = ["bronze", "silver", "gold"]
        self._setup_folders()

    def _setup_folders(self):
        # Maintain directory hierarchy for Medallion architecture
        for layer in self.layers:
            (self.base_path / layer).mkdir(parents=True, exist_ok=True)

    def clear_layer(self, layer: str):
        # Purge existing files in specified layer to maintain idempotent runs
        layer_path = self.base_path / layer
        if layer_path.exists():
            print(f"Cleaning layer: {layer}... removing old files.")
            for file in layer_path.glob("*"):
                try:
                    if file.is_file():
                        file.unlink()
                except Exception as e:
                    print(f"Error deleting {file}: {e}")

    def save_df(self, df, layer, filename):
        # Serialize DataFrame to Parquet for efficient storage and schema retention
        path = self.base_path / layer / f"{filename}.parquet"
        df.to_parquet(path)
        print(f"Saved: {path}")

    def load_df(self, layer, filename):
        # Retrieve persisted data from specified architectural layer
        path = self.base_path / layer / f"{filename}.parquet"
        if not path.exists():
            raise FileNotFoundError(f"No data found at {path}")
        return pd.read_parquet(path)