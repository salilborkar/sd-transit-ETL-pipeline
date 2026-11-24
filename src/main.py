import time
from utils import get_logging, logger
from extract import run_extract
from transform import run_transform
from load import run_load

logger = get_logging("pipeline from main")

def run_pipeline() -> None:
    start = time.perf_counter()
    logger.info(f"starting pipeline...")

    raw_df = run_extract()
    parquet_path = run_transform(raw_df)
    run_load(parquet_path)

    elapsed = time.perf_counter() - start
    logger.info(f"pipeline finished in {elapsed:.2f} seconds")

if __name__ == "__main__":
    run_pipeline()