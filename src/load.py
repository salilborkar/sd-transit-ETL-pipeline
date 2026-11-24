import duckdb
from utils import proj_root,p, logger, get_logging
from pathlib import Path

logger = get_logging(__name__)

def run_load(parquet_path:Path | None = None) -> None:
    if parquet_path is None:
        parquet_path = p / "sd_transit_clean.parquet"
    db_path = proj_root / "data" / "analytics.duckdb"

    with duckdb.connect(db_path) as db:
        db.execute(f""" CREATE OR REPLACE TABLE sd_transit_ridership AS
         SELECT * FROM read_parquet('{parquet_path}')
        """)
        categories = db.execute("""
                                SELECT category, COUNT(*) AS row_count, SUM(value) AS total_val
                                FROM sd_transit_ridership
                                GROUP BY category
                                """).df()
        logger.info(f"Loaded clean data from {parquet_path} to DuckDB and ran analytical queries")
        logger.info("\n--- Category Breakdown ---")
        logger.info(categories)
if __name__ == "__main__":
    run_load()