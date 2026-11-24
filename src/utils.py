from pathlib import Path
import pandas as pd
import logging

proj_root = Path(__file__).resolve().parent.parent
r = proj_root / "data" /"raw"
p = proj_root / "data" /"processed"

#get raw file
def get_raw_files()->Path:
    csv_file = list(r.glob("*.csv"))
    if not csv_file:
        raise FileNotFoundError("No CSV files found in data directory {rp}")
    return csv_file[0]

#read raw csv into a dataframe
def load_raw_df()->pd.DataFrame:
    return pd.read_csv(get_raw_files())

def inspect_df(df:pd.DataFrame, stage: str = "DataFrame")->None:
    """Print shape, schema, null counts, and sample rows for a DataFrame."""
    logger.info(f"\n--- {stage} Inspection ---")
    logger.info("Shape (rows, columns):", df.shape)
    logger.info("\nColumn Types:\n", df.dtypes)
    logger.info("\nMissing Values:\n", df.isna().sum())
    logger.info("\nFirst 3 Rows:\n", df.head(3))
    logger.info("-" * 30)

def get_logging(name:str)->logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger(name)
logger = get_logging(__name__)
