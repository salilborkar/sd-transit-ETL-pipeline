# San Diego Transit Ridership ETL Pipeline

An end-to-end, modular Python data engineering pipeline that ingests raw public transit ridership records from the San Diego Metropolitan Transit System (MTS), enforces strict data types and schema hygiene, converts the data into compressed columnar **Apache Parquet** format, and loads it into an embedded **DuckDB** analytical warehouse for SQL querying.

---

## Table of Contents
1. [Executive Summary & Project Purpose](#executive-summary--project-purpose)
2. [System Architecture & Data Flow](#system-architecture--data-flow)
3. [Project Structure & Module Breakdown](#project-structure--module-breakdown)
4. [Key Architectural Decisions & Trade-offs](#key-architectural-decisions--trade-offs)
5. [Data Quality & Schema Caveats (The EAV Trap)](#data-quality--schema-caveats-the-eav-trap)
6. [Where This Project Falls Short (Current Limitations)](#where-this-project-falls-short-current-limitations)
7. [Scaling Roadmap: From Local Prototype to Enterprise Production](#scaling-roadmap-from-local-prototype-to-enterprise-production)
8. [Local Setup & Execution Guide](#local-setup--execution-guide)

---

## Executive Summary & Project Purpose

### Why This Project Exists
Raw civic and operational datasets are rarely ready for business intelligence or machine learning out of the box. In this dataset (2,275 records covering San Diego transit operators such as Circuit in Chula Vista), every field arrives as an untyped text string (`str`), numeric values risk formatting corruption, date fields cannot be sorted chronologically, and hundreds of quarter labels are missing.

### Core Advantages
* **Zero-Cost Local Lakehouse:** Replicates modern cloud data lakehouse patterns (Raw Landing Zone $\rightarrow$ Typed Columnar Storage $\rightarrow$ Analytical SQL Warehouse) entirely on a single machine with zero cloud infrastructure bills or external database daemons.
* **Idempotent Execution:** Running the pipeline multiple times produces the exact same clean state without duplicating records.
* **Separation of Concerns:** Ingestion, transformation, warehouse loading, and shared utilities are decoupled into independent, testable modules.
* **Auditability & Observability:** Every pipeline stage emits structured, timestamped logs and schema diagnostics (`shape`, `dtypes`, null counts).

---

## System Architecture & Data Flow

```text
+-----------------------+      +-----------------------------------+      +--------------------------------+

|   data/raw/*.csv      |      |       data/processed/             |      |     data/analytics.duckdb      |
| (Untyped Civic CSV)   |      |   sd_transit_clean.parquet        |      |  Table: sd_transit_ridership   |
+-----------+-----------+      +-----------------+-----------------+      +---------------+----------------+
            |                                    ^                                        ^
            v                                    |                                        |
+-----------+-----------+      +-----------------+-----------------+      +---------------+----------------+

|    src/extract.py     | ---> |         src/transform.py          | ---> |          src/load.py           |
| - File discovery      |      | - snake_case column normalization |      | - Idempotent table replacement |
| - Schema profiling    |      | - Numeric casting (float64)       |      | - Grain/EAV validation check   |
+-----------------------+      | - Date parsing (datetime64)       |      | - Window-function SQL analytics|
            ^                  | - Missing quarter backfilling     |      +--------------------------------+
            |                  +-----------------------------------+                      ^

            |                                    ^                                        |
            +------------------------------------+----------------------------------------+
                                                 |
                                    +------------+------------+

                                    |       src/main.py       |
                                    | (Orchestrator & Timer)  |
                                    +-------------------------+
                                                 ^
                                                 |
                                    +------------+------------+

                                    |       src/utils.py      |
                                    | (Paths, Logging, Audit) |
                                    +-------------------------+
```

---

## Project Structure & Module Breakdown

```text
sd-transit-pipeline/
├── .gitignore               # Excludes .venv, local data files, and .duckdb binaries from Git
├── README.md                # System documentation and architectural guide
├── requirements.txt         # Pinned Python dependencies (pandas, pyarrow, duckdb)
├── data/
│   ├── raw/                 # Immutable landing zone for source CSV files
│   ├── processed/           # Cleaned, strongly typed Parquet output
│   └── analytics.duckdb     # Embedded analytical database file
└── src/
    ├── utils.py             # Shared configuration, path anchoring, logging, and profiling
    ├── extract.py           # Stage 1: Raw file discovery and initial data audit
    ├── transform.py         # Stage 2: Data cleaning, type casting, enrichment, and Parquet export
    ├── load.py              # Stage 3: DuckDB ingestion and analytical SQL execution
    └── main.py              # Pipeline orchestrator chaining Stages 1 -> 2 -> 3
```

### How Each Module Works & Why `extract`, `transform`, and `load` Are Chained in `main.py`

1. **`src/utils.py` (The Shared Foundation):**
   * **Path Anchoring (`PROJECT_ROOT`):** Uses `Path(__file__).resolve().parent.parent` so file paths resolve reliably regardless of which working directory a user or scheduler invokes the script from.
   * **Centralized Logging (`get_logger`):** Standardizes log formatting (`Timestamp | Level | Module | Message`) across all files.
   * **Data Profiling (`inspect_df`):** Reusable diagnostic function that prints row/column dimensions, column data types, missing value counts, and sample rows before and after transformations.
   * **Raw Loader (`load_raw_df`):** Safely discovers `.csv` files via `pathlib.Path.glob()` and raises an explicit `FileNotFoundError` if the raw landing zone is empty.

2. **`src/extract.py` (`run_extract()`):**
   * Acts as a **discovery and profiling gate**. It reads the raw CSV without mutating it, logs row counts, profiles the raw schema via `inspect_df()`, and returns a `pandas.DataFrame`.

3. **`src/transform.py` (`run_transform(df=None)`):**
   * Standardizes headers to `snake_case` (`location_name`, `month`, etc.).
   * Strips formatting characters (commas/whitespace) from `value` and casts it to `float64` using `pd.to_numeric(..., errors="coerce")`.
   * Parses string dates (`"July 2025"`) into true `datetime64[us]` objects so queries sort chronologically rather than alphabetically.
   * Derives missing `quarter` labels (backfilling 976 null rows) directly from the parsed `month` timestamp.
   * Writes the cleaned dataset to `data/processed/sd_transit_clean.parquet` without row index bloat (`index=False`) and returns the output `Path`.

4. **`src/load.py` (`run_load(parquet_path=None)`):**
   * Connects to `data/analytics.duckdb` using a context manager (`with duckdb.connect(...)`) to guarantee clean connection closure.
   * Executes `CREATE OR REPLACE TABLE sd_transit_ridership AS SELECT * FROM read_parquet(...)` for idempotent loading.
   * Runs a category grain check and executes analytical SQL queries (such as month-over-month ridership percentage growth using `LAG()` window functions and Common Table Expressions).

5. **`src/main.py` (`run_pipeline()`):**
   * **Why Chaining Matters:** Each stage (`extract.py`, `transform.py`, `load.py`) is wrapped in a callable function guarded by `if __name__ == "__main__":`. This dual design allows engineers to run or debug any individual step in isolation (`python src/transform.py`) while allowing `main.py` to chain them sequentially in memory (`raw_df = run_extract()` $\rightarrow$ `parquet_path = run_transform(raw_df)` $\rightarrow$ `run_load(parquet_path)`). Passing the DataFrame in memory avoids redundant disk reads and tracks end-to-end runtime via `time.perf_counter()`.

---

## Key Architectural Decisions & Trade-offs

### 1. Why Convert CSV to Apache Parquet?

| Dimension | Raw CSV | Apache Parquet (`sd_transit_clean.parquet`) |
| :--- | :--- | :--- |
| **Storage Layout** | Row-oriented plain text | Column-oriented binary format |
| **Schema Enforcement** | None (all columns default to `str`) | Embedded metadata preserves `datetime64` and `float64` |
| **Query Performance** | Must scan every row and every column | Column pruning & predicate pushdown read only queried columns |
| **Compression** | Uncompressed; large disk footprint | Built-in Snappy/ZSTD compression + dictionary encoding |

* **Trade-off:** Parquet files are not human-readable in a standard text editor or Excel without a viewer or query engine, whereas CSVs can be opened anywhere. Keeping `data/raw/*.csv` untouched preserves human inspectability while `data/processed/*.parquet` optimizes downstream compute.

### 2. Why DuckDB Over PostgreSQL, SQLite, or Cloud Warehouses?
* **OLAP vs. OLTP:** SQLite and PostgreSQL are row-oriented transactional (OLTP) databases optimized for single-row lookups and concurrent writes. **DuckDB** is an in-process, vectorized Online Analytical Processing (OLAP) engine built specifically for fast aggregations (`SUM`, `GROUP BY`, window functions) over columnar data.
* **Native Parquet Integration**: DuckDB queries Parquet files directly via read_parquet() without slow row-by-row INSERT loops.
* **Zero Operational Overhead**: Runs inside the Python process with no background server, Docker daemon, or cloud credentials required.
* **Trade-off**: DuckDB locks the .duckdb file during write operations and is designed for single-node analytical workloads—not high-concurrency web applications or multi-user transactional writes.

### 3. Data Quality & Schema Caveats (The EAV Trap)
* A critical finding uncovered during the load.py grain audit is that the source dataset uses an Entity-Attribute-Value (EAV) / long-format schema rather than a wide flat table. Multiple distinct metrics are stacked inside the same value column and differentiated by category

* Rule for Downstream Consumers: Every SQL query must filter on a single category (e.g., WHERE category = 'Hourly Passengers'). Summing across the entire table without a category filter will double- or triple-count ridership totals.

### 4. Where This Project Falls Short (Current Limitations)

While ideal for local analytics and prototyping, this architecture has intentional boundaries:
1. Manual Ingestion: Relies on a manually downloaded CSV placed in data/raw/ rather than automated API polling or webhook triggers.
2. Full-Refresh Overwrites: CREATE OR REPLACE TABLE rebuilds the entire dataset on every run. For billions of rows, full overwrites waste compute compared to incremental/partitioned loads.
3. Single-Machine Memory Ceiling: Pandas loads the entire CSV into local RAM during transformation; datasets exceeding available system memory will trigger Out-Of-Memory (OOM) crashes.
4. No Automated Test Suite or Schema Contracts: Lacks unit tests (pytest) and formal schema validation gates (e.g., Pandera or Great Expectations) to halt bad data before it reaches DuckDB.
5. No Automated Scheduling or Alerting: Requires manual CLI invocation (python src/main.py) with no retry policies or Slack/email failure alerts.

### 5. Scaling Roadmap: From Local Prototype to Enterprise Production

* If this dataset grew from 2,275 rows to 500M+ rows (terabyte scale) with daily automated feeds across multiple transit agencies, the codebase would evolve as follows:

| Layer | Current Local Implementation | Enterprise Large-Scale Architecture | Required Codebase Changes |
| :--- | :--- | :--- | :--- |
| **1. Ingestion (`extract.py`)** | Local file globbing (`data/raw/`) | Automated REST API / SFTP landing in **AWS S3** | Replace `Path.glob()` with `httpx` + `boto3` |
| **2. Compute (`transform.py`)** | Single-threaded `pandas` in RAM | **Polars** or **Apache Spark** | Swap `pandas` for lazy execution (`polars.scan_csv()`) |



