# =========================================
# STAGING PREPARE (LAST SNAPSHOT ONLY)
# - read bronze
# - derive snapshot_date
# - keep ONLY latest snapshot
# - dedupe
# - compute row_hash
# - write clean staging table
# =========================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ---- TABLES ----
bronze_table = "etl_demo.bronze.customer_bronze"
stg_table    = "etl_demo.silver_staging.customer_snapshot_stg"

# ---- 1) READ BRONZE ----
df = spark.table(bronze_table)

# ---- 2) DERIVE snapshot_date ----
# If CSV already has snapshot_date column -> use it
if "snapshot_date" in df.columns:
    df2 = df.withColumn("snapshot_date", F.to_date(F.col("snapshot_date")))
else:
    # Extract from file name: snapshot_YYYY_MM_DD.csv
    df2 = (
        df.withColumn(
            "snapshot_date_str",
            F.regexp_extract(
                F.col("_file_path"),
                r"snapshot_(\d{4}_\d{2}_\d{2})\.csv$",
                1
            )
        )
        .withColumn(
            "snapshot_date",
            F.to_date(F.regexp_replace("snapshot_date_str", "_", "-"))
        )
        .drop("snapshot_date_str")
    )

# ---- 3) BASIC DATA QUALITY (HARD FILTER FOR STAGING) ----
df_valid = (
    df2
    .filter(F.col("customer_id").isNotNull())
    .filter(F.col("snapshot_date").isNotNull())
)

# ---- 4) FIND LATEST SNAPSHOT_DATE ----
latest_snapshot_date = (
    df_valid
    .agg(F.max("snapshot_date").alias("max_snapshot_date"))
    .collect()[0]["max_snapshot_date"]
)

print("📌 Latest snapshot_date:", latest_snapshot_date)

# ---- 5) KEEP ONLY LATEST SNAPSHOT ----
df_latest = df_valid.filter(F.col("snapshot_date") == F.lit(latest_snapshot_date))

# ---- 6) DEDUPLICATION (PER CUSTOMER PER SNAPSHOT) ----
w = (
    Window
    .partitionBy("customer_id", "snapshot_date")
    .orderBy(F.col("_ingest_time").desc())
)

df_dedup = (
    df_latest
    .withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

# ---- 7) HASH FOR CHANGE DETECTION ----
business_cols = [c for c in ["name", "city", "email"] if c in df_dedup.columns]

df_stg = (
    df_dedup
    .withColumn(
        "row_hash",
        F.sha2(
            F.concat_ws(
                "||",
                *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in business_cols]
            ),
            256
        )
    )
    .select(
        "customer_id",
        *business_cols,
        "snapshot_date",
        "row_hash",
        "_ingest_time",
        "_file_path"
    )
)

# ---- 8) WRITE STAGING (OVERWRITE IS EXPECTED) ----
spark.sql("CREATE SCHEMA IF NOT EXISTS etl_demo.silver_staging")

(
    df_stg.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(stg_table)
)

print("✅ STAGING READY:", stg_table)
print("Rows in staging:", spark.table(stg_table).count())

