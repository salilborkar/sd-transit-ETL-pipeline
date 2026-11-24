import pandas as pd
from utils import load_raw_df, inspect_df, logger,p, get_logging
from pathlib import Path

logger = get_logging(__name__)

def run_transform(df:pd.dataFrame | None = None) -> Path:
    if df is None:
        df = load_raw_df() #if transform.py is run alone
    df.columns = df.columns.str.lower().str.replace(' ', '_')

    # 1. Strip commas and leading/trailing spaces (convert to str first just in case)
    cleaned_val = df["value"].astype(str).str.replace(",", "", regex=False).str.strip()

    # 2. Cast to numeric; unparseable strings (like "N/A" or "-") become NaN instead of crashing
    df["value"] = pd.to_numeric(cleaned_val, errors="coerce")
    df["month"] = pd.to_datetime(df["month"], format="%B %Y")
    # Generates strings like "2025Q3" or "Q3 2025" from the parsed date
    derived_quarter = "Q" + df["month"].dt.quarter.astype(str) + " " + df["month"].dt.year.astype(str)
    df["quarter"] = df["quarter"].fillna(derived_quarter)

    inspect_df(df, stage="Cleaned Transform")

    p.mkdir(parents=True, exist_ok=True)
    output_path = p / "sd_transit_clean.parquet"
    df.to_parquet(output_path, index=False)
    logger.info(f"Saved cleaned Parquet file to {output_path}")
    return output_path #hands the file to load.py
if __name__ == "__main__":
    df = run_transform()

