# =========================================
# 03 SILVER - SCD TYPE 2 MERGE (SNAPSHOT)
# Works even if some business columns (e.g. email) are missing in staging
# =========================================

from pyspark.sql import functions as F

stg_table    = "etl_demo.silver_staging.customer_snapshot_stg"
silver_table = "etl_demo.silver.customer_dim"

spark.sql("CREATE SCHEMA IF NOT EXISTS etl_demo.silver")

s = spark.table(stg_table)

snapshot_date = s.select(F.max("snapshot_date").alias("d")).collect()[0]["d"]
print("📌 Running SCD2 MERGE for snapshot_date:", snapshot_date)

# Detect which business columns exist in staging
business_cols = [c for c in ["name", "city", "email"] if c in s.columns]
print("Using business columns:", business_cols)

# ---- Create target table if first run (only includes columns we support; email can be null later)
# We'll create with all 3 columns, but if email isn't present we'll insert NULL.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {silver_table} (
  customer_id   STRING,
  name          STRING,
  city          STRING,
  email         STRING,
  row_hash      STRING,
  valid_from    DATE,
  valid_to      DATE,
  is_current    BOOLEAN,
  _created_time TIMESTAMP,
  _updated_time TIMESTAMP
)
USING DELTA
""")

# ---- Prepare source with NULLs for missing columns
def col_or_null(c: str):
    return F.col(c).cast("string") if c in s.columns else F.lit(None).cast("string")

src = (
    s.select(
        F.col("customer_id").cast("string").alias("customer_id"),
        col_or_null("name").alias("name"),
        col_or_null("city").alias("city"),
        col_or_null("email").alias("email"),
        F.col("row_hash").cast("string").alias("row_hash"),
        F.col("snapshot_date").cast("date").alias("snapshot_date")
    )
)

src.createOrReplaceTempView("src")

# ---- 1) Close changed current records
spark.sql(f"""
MERGE INTO {silver_table} AS tgt
USING src AS src
ON tgt.customer_id = src.customer_id
AND tgt.is_current = true
WHEN MATCHED AND tgt.row_hash <> src.row_hash THEN
  UPDATE SET
    tgt.valid_to = src.snapshot_date,
    tgt.is_current = false,
    tgt._updated_time = current_timestamp()
""")

# ---- 2) Insert new current records (new OR changed)
spark.sql(f"""
INSERT INTO {silver_table}
SELECT
  src.customer_id,
  src.name,
  src.city,
  src.email,
  src.row_hash,
  src.snapshot_date AS valid_from,
  CAST(NULL AS DATE) AS valid_to,
  true AS is_current,
  current_timestamp() AS _created_time,
  current_timestamp() AS _updated_time
FROM src
LEFT ANTI JOIN {silver_table} tgt
  ON tgt.customer_id = src.customer_id AND tgt.is_current = true
""")

print("✅ Silver SCD2 DONE:", silver_table)
