import os
import sys
from pathlib import Path

# Resolve project root for package availability
root = Path(__file__).resolve().parent.parent
if str(root) not in sys.path:
    sys.path.append(str(root))

import pandas as pd
from src.storage import DataVault

def run_integrity_tests():
    # Validation suite to ensure data quality and schema adherence in the Gold layer
    vault = DataVault()
    test_file = "ES_F_gold"

    print(f"Starting integrity tests for {test_file}")

    try:
        # Retrieve analytical output for validation
        df = vault.load_df(layer="gold", filename=test_file)

        # Dataset presence validation
        assert not df.empty, "Test Failed: Dataframe is empty"
        print("Test 1 Passed: Dataframe contains data")

        # Schema validation: Ensure critical analytical features exist
        required_cols = ['Close', 'Returns', 'SMA_20', 'SMA_120', 'Signal']
        for col in required_cols:
            assert col in df.columns, f"Test Failed: Missing column {col}"
        print("Test 2 Passed: All required columns present")

        # Logical consistency check: Validate signal continuity after indicator lookback window
        # Accounting for 120-period initialization lag for technical indicators
        null_count = df['Signal'].iloc[120:].isnull().sum()
        assert null_count == 0, "Test Failed: Found null signals in active trading window"
        print("Test 3 Passed: No null signals in trading window")

        print("All integrity tests passed successfully")

    except Exception as e:
        print(f"Integrity check failed: {e}")

if __name__ == "__main__":
    run_integrity_tests()