import os
import sys
import shutil
from pathlib import Path
import matplotlib.pyplot as plt
import pandas as pd

# Resolve project root for package availability
root = Path(__file__).resolve().parent.parent
if str(root) not in sys.path:
    sys.path.append(str(root))

from src.storage import DataVault

# Metadata mapping for standardized reporting and visualization titles
ASSET_NAMES = {
    "ES=F": "S&P 500 E-mini Futures",
    "NQ=F": "Nasdaq 100 E-mini Futures",
    "CL=F": "Crude Oil Futures"
}

class PipelineVisualizer:
    def __init__(self):
        self.vault = DataVault()
        self.output_dir = root / "plots"

        # Maintain idempotent plot directory by purging historical artifacts
        if self.output_dir.exists():
            print("Cleaning up old plots...")
            shutil.rmtree(self.output_dir)

        self.output_dir.mkdir(exist_ok=True)

    def plot_trend_strategy(self, symbol):
        # Generate technical analysis visualization for SG Trend Indicator logic
        full_name = ASSET_NAMES.get(symbol, symbol)

        # Retrieve analytical output from Gold layer
        filename = f"{symbol.replace('=', '_').replace('^', '').replace('/', '_')}_gold"
        try:
            df = self.vault.load_df(layer="gold", filename=filename)
        except FileNotFoundError:
            print(f"Error: Gold data for {symbol} not found. Ensure strategies.py execution.")
            return

        # Initialize canvas for multi-series time-series visualization
        plt.figure(figsize=(14, 7))
        plt.plot(df.index, df['Close'], label='Price', color='black', alpha=0.3, lw=1)
        plt.plot(df.index, df['SMA_20'], label='20-Day SMA (Fast)', color='#1f77b4', lw=1.5)
        plt.plot(df.index, df['SMA_120'], label='120-Day SMA (Slow)', color='#ff7f0e', lw=2)

        # Implementation of conditional background shading to denote regime shifts
        # Green indicates Long bias; Red indicates Short bias
        plt.fill_between(df.index, df['Close'].min(), df['Close'].max(),
                         where=(df['Signal'] == 1), color='green', alpha=0.1, label='Bullish Regime')
        plt.fill_between(df.index, df['Close'].min(), df['Close'].max(),
                         where=(df['Signal'] == -1), color='red', alpha=0.1, label='Bearish Regime')

        # Refined chart aesthetics for professional distribution
        plt.title(f"Societe Generale Trend Indicator\nAsset: {full_name}",
                  fontsize=16, fontweight='bold', pad=20)

        plt.xlabel("Date", fontsize=12)
        plt.ylabel("Price (USD)", fontsize=12)
        plt.legend(loc='upper left', frameon=True, shadow=True)
        plt.grid(True, which='both', linestyle='--', alpha=0.4)

        # Export high-resolution PNG artifact to plots directory
        clean_name = symbol.replace('=', '_').replace('^', '').replace('/', '_')
        save_path = self.output_dir / f"{clean_name}_Trend_Chart.png"

        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Chart successfully saved to: {save_path}")

if __name__ == "__main__":
    # Orchestrate visualization generation for the defined asset universe
    viz = PipelineVisualizer()
    symbols = ["ES=F", "NQ=F", "CL=F"]
    for sym in symbols:
        viz.plot_trend_strategy(sym)