import os
import sys
from pathlib import Path
from datetime import datetime

# Resolve project root for package availability
root = Path(__file__).resolve().parent.parent
if str(root) not in sys.path:
    sys.path.append(str(root))

from src.storage import DataVault

class PipelineReporter:
    def __init__(self):
        self.vault = DataVault()
        self.report_dir = root / "reports"
        # Initialize directory for audit artifacts
        self.report_dir.mkdir(exist_ok=True)

    def generate_status_report(self, symbols):
        # Compilation of end-to-end pipeline execution metrics and asset health
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        report_file = self.report_dir / "pipeline_status.txt"

        with open(report_file, "w") as f:
            f.write("QUANT DATA PIPELINE - AUDIT REPORT\n")
            f.write(f"Generated at: {timestamp}\n")
            f.write("========================================\n\n")

            for sym in symbols:
                try:
                    # Validate against terminal Gold layer output
                    filename = f"{sym.replace('=', '_').replace('^', '').replace('/', '_')}_gold"
                    df = self.vault.load_df(layer="gold", filename=filename)

                    row_count = len(df)
                    start_dt = df.index.min()
                    end_dt = df.index.max()

                    f.write(f"ASSET: {sym}\n")
                    f.write(f"STATUS: Active\n")
                    f.write(f"TOTAL ROWS: {row_count}\n")
                    f.write(f"COVERAGE: {start_dt} to {end_dt}\n")
                    f.write("--------------------\n")

                except Exception as e:
                    f.write(f"ASSET: {sym}\n")
                    f.write(f"STATUS: FAILED\n")
                    f.write(f"ERROR: {str(e)}\n")
                    f.write("--------------------\n")

            f.write("\nEND OF REPORT\n")

        print(f"Report successfully saved to: {report_file}")

if __name__ == "__main__":
    # Execution block for terminal status audit
    reporter = PipelineReporter()
    reporter.generate_status_report(["ES=F", "NQ=F", "CL=F"])