#locate the file available and add an unmodified file with timestamp in /data/raw
#e.g. transit_2026_11_23.json

import pandas as pd
from utils import get_raw_files, load_raw_df, inspect_df, get_logging

logger = get_logging(__name__)

def run_extract() -> pd.DataFrame:
    df = load_raw_df()
    logger.info(f"Extracting {len(df)} rows")
    inspect_df(df,stage="Raw Extract")
    return df
if __name__ == "__main__":
    run_extract()