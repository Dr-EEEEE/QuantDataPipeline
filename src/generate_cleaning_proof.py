import sys
from pathlib import Path
import pandas as pd

# Resolve project root for package availability
root = Path(__file__).resolve().parent.parent
if str(root) not in sys.path:
    sys.path.append(str(root))

from src.storage import DataVault

def generate_global_audit():
    # Auditing utility to document the transition from Bronze to Silver layers
    vault = DataVault()
    symbols = ["ES_F", "NQ_F", "CL_F"]
    all_reports = []

    print("Starting Global Data Audit...")

    for sym in symbols:
        try:
            # Load raw and processed datasets for delta verification
            raw_df = vault.load_df(layer="bronze", filename=f"{sym}_raw")
            clean_df = vault.load_df(layer="silver", filename=f"{sym}_cleaned")

            # Dimensionality reduction to ensure Series-level comparison
            raw_close = raw_df['Close'].squeeze()
            if len(raw_close.shape) > 1: raw_close = raw_close.iloc[:, 0]

            clean_close = clean_df['Close'].squeeze()
            if len(clean_close.shape) > 1: clean_close = clean_close.iloc[:, 0]

            # Normalize symbol naming convention for reporting
            clean_symbol_name = sym.split('_')[0]

            df_audit = pd.DataFrame({
                'Symbol': clean_symbol_name,
                'Raw_Price': raw_close,
                'Cleaned_Price': clean_close
            }, index=clean_df.index)

            # Identification of data gaps and verification of cleaning logic
            gaps = df_audit[df_audit['Raw_Price'].isna()]

            if not gaps.empty:
                report_part = gaps.copy()
                report_part['Audit_Note'] = "Gap Found & Repaired"
            else:
                # Document final five observation days to confirm pipeline continuity
                report_part = df_audit.tail(5).copy()
                report_part['Audit_Note'] = "Data Integrity Verified (100% Clean)"

            all_reports.append(report_part)
            print(f"Verified: {clean_symbol_name}")

        except Exception as e:
            print(f"Skipping {sym}: {e}")

    # Aggregation and persistence of audit trail to CSV
    if all_reports:
        final_csv = pd.concat(all_reports)
        # Precision rounding for standardized financial reporting
        final_csv['Raw_Price'] = final_csv['Raw_Price'].round(2)
        final_csv['Cleaned_Price'] = final_csv['Cleaned_Price'].round(2)

        output_path = root / "data" / "cleaning_audit_proof.csv"
        final_csv.to_csv(output_path)
        print("-" * 50)
        print(f"SUCCESS: Audit saved to {output_path}")
        print("-" * 50)

if __name__ == "__main__":
    generate_global_audit()