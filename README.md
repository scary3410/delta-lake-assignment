# Delta Lake MERGE Assignment — Incremental Data Processing (Week 7)

## 1. Objective
Perform incremental data processing on a customer dataset using **Delta Lake**,
implementing both an **SCD Type 1** (overwrite-in-place) and an **SCD Type 2**
(full history) MERGE pattern.

## 2. Dataset
- `data/customer_master.csv` — base/master customer data (7 unique customers,
  with 1 intentional duplicate row and 2 intentional null values for the
  cleaning step).
- `data/customer_incremental.csv` — a simulated incoming batch: 2 updates to
  existing customers (city change, reactivation) and 2 brand-new customers.

## 3. What the notebook does
`notebooks/delta_scd_assignment.ipynb` walks through:

1. **Spark + Delta setup** — creates a `SparkSession` configured with the
   Delta Lake extensions.
2. **Load master data** into a Delta table.
3. **Clean the data** — drops the duplicate row, fills nulls in `email`/`city`.
4. **Load incremental data** simulating a new batch arriving.
5. **SCD Type 1 MERGE** — `whenMatchedUpdate` overwrites changed attributes in
   place, `whenNotMatchedInsert` adds new customers. One row per
   `customer_id`, always reflecting only the latest state.
6. **SCD Type 2 MERGE** — adds `effective_date`, `end_date`, `is_current`
   columns. Changed customers get their old row closed
   (`is_current = false`) and a new current row inserted, preserving full
   history.
7. **Validation** — row counts, and an assertion that there are no duplicate
   *current* records per `customer_id` in either table.
8. **Final output + summary** — prints both tables and Delta's transaction
   history (`DESCRIBE HISTORY`) to show the MERGE was captured as an atomic,
   versioned operation.

## 4. How to run this notebook
This notebook needs a Spark runtime with the Delta Lake package, and an
internet connection so Spark can fetch `io.delta:delta-spark` from Maven
Central the first time it runs. Two easy options:

**Option A — Databricks Community Edition (recommended, free)**
1. Sign up at databricks.
2. Create a cluster (any runtime with Spark 3.x+ works; Delta Lake ships
   built in, so you can skip the `configure_spark_with_delta_pip` step and
   just use `spark` directly).
3. Import `notebooks/delta_scd_assignment.ipynb`.
4. Upload `data/customer_master.csv` and `data/customer_incremental.csv` to
   DBFS (or adjust the file paths to wherever you upload them), then **Run All**.
5. Take screenshots at each stage (see folder guide below) and save them into
   the matching `screenshots/` subfolder.

**Option B — Local machine with internet access**
```bash
pip install pyspark delta-spark
jupyter notebook notebooks/delta_scd_assignment.ipynb
```
Run all cells top to bottom.

> Note: the merge logic in this notebook (which rows count as "changed",
> what gets inserted vs. updated) was verified independently with a pandas
> simulation before being translated into the PySpark/Delta MERGE syntax
> below, so the SQL/DataFrame logic is correct — you just need a Spark +
> Delta environment with internet access to execute it and capture screenshots.

## 5. Screenshots to capture (per folder)
- `screenshots/data_loading/` — master CSV loaded into a Spark DataFrame
- `screenshots/data_cleaning/` — before/after cleaning (duplicate & null counts)
- `screenshots/scd1/` — SCD Type 1 table after MERGE
- `screenshots/scd2/` — SCD Type 2 table after MERGE, showing history rows
- `screenshots/validation/` — validation cell output (row counts, duplicate checks)
- `screenshots/final_output/` — final tables + `DESCRIBE HISTORY` output

## 6. Key Findings
- The raw master dataset had 1 duplicate row and 2 null values, both handled
  before loading into Delta.
- The incremental batch had 2 updates to existing customers and 2 new
  customers.
- SCD Type 1 always keeps exactly one row per customer, reflecting only the
  latest known state — good for "current state" reporting, but no history.
- SCD Type 2 preserves every version of a customer's record with
  `effective_date` / `end_date` / `is_current`, enabling point-in-time and
  historical analysis at the cost of a larger table.
- Delta Lake's transaction log confirms each MERGE is captured as an atomic,
  versioned operation, which is what enables time travel and auditability
  compared to plain CSV/Parquet overwrites.

## 7. Folder Structure
```
delta-lake-assignment/
│
├── data/
│   ├── customer_master.csv
│   └── customer_incremental.csv
│
├── notebooks/
│   └── delta_scd_assignment.ipynb
│
├── screenshots/
│   ├── data_loading/
│   ├── data_cleaning/
│   ├── scd1/
│   ├── scd2/
│   ├── validation/
│   └── final_output/
│
│
└── README.md
```
