# =========================================
# 04 GOLD - Views + Daily KPI table + Run log
# =========================================

import uuid
from pyspark.sql import functions as F

run_id = str(uuid.uuid4())
pipeline = "gold"

silver_table = "etl_demo.silver.customer_dim"

spark.sql("CREATE SCHEMA IF NOT EXISTS etl_demo.gold")
spark.sql("CREATE SCHEMA IF NOT EXISTS etl_demo.control")

started_at = spark.sql("SELECT current_timestamp() AS ts").collect()[0]["ts"]

# Write STARTED log
spark.createDataFrame(
    [(pipeline, run_id, None, started_at, None, "STARTED", None, None)],
    ["pipeline","run_id","snapshot_date","started_at","finished_at","status","rows_written","message"]
).createOrReplaceTempView("log_start")

spark.sql("""
INSERT INTO etl_demo.control.run_log
SELECT * FROM log_start
""")

try:
    # ---- 1) Gold "current" view
    spark.sql(f"""
    CREATE OR REPLACE VIEW etl_demo.gold.customer_current AS
    SELECT
      customer_id,
      name,
      city,
      email,
      valid_from
    FROM {silver_table}
    WHERE is_current = true
    """)

    # ---- 2) Latest date in silver
    s = spark.table(silver_table)
    latest_date = s.select(F.max("valid_from").alias("d")).collect()[0]["d"]

    # KPI table
    spark.sql("""
    CREATE TABLE IF NOT EXISTS etl_demo.gold.customer_daily_kpi (
      kpi_date         DATE,
      active_customers BIGINT,
      new_customers    BIGINT,
      changed_customers BIGINT,
      closed_records   BIGINT,
      run_time         TIMESTAMP
    )
    USING DELTA
    """)

    # Compute KPIs
    active_customers = s.filter(F.col("is_current") == True).count()

    first_dates = s.groupBy("customer_id").agg(F.min("valid_from").alias("first_valid_from"))

    new_customers = first_dates.filter(F.col("first_valid_from") == F.lit(latest_date)).count()

    changed_customers = (
        s.filter(F.col("valid_from") == F.lit(latest_date))
         .join(first_dates, on="customer_id", how="inner")
         .filter(F.col("first_valid_from") < F.lit(latest_date))
         .select("customer_id").distinct()
         .count()
    )

    closed_records = s.filter(F.col("valid_to") == F.lit(latest_date)).count()

    kpi_df = spark.createDataFrame(
        [(latest_date, active_customers, new_customers, changed_customers, closed_records)],
        ["kpi_date", "active_customers", "new_customers", "changed_customers", "closed_records"]
    ).withColumn("run_time", F.current_timestamp())

    kpi_df.createOrReplaceTempView("kpi_src")

    # Idempotent upsert (1 row per date)
    spark.sql("""
    MERGE INTO etl_demo.gold.customer_daily_kpi AS tgt
    USING kpi_src AS src
    ON tgt.kpi_date = src.kpi_date
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

    # Finish log SUCCESS
    finished_at = spark.sql("SELECT current_timestamp() AS ts").collect()[0]["ts"]
    rows_written = 1
    msg = "gold view + kpi upsert ok"

    spark.createDataFrame(
        [(pipeline, run_id, latest_date, started_at, finished_at, "SUCCESS", rows_written, msg)],
        ["pipeline","run_id","snapshot_date","started_at","finished_at","status","rows_written","message"]
    ).createOrReplaceTempView("log_end")

    spark.sql("""
    MERGE INTO etl_demo.control.run_log AS t
    USING log_end AS s
    ON t.pipeline = s.pipeline AND t.run_id = s.run_id
    WHEN MATCHED THEN UPDATE SET *
    """)

    print("✅ GOLD done. Latest date:", latest_date)

except Exception as e:
    finished_at = spark.sql("SELECT current_timestamp() AS ts").collect()[0]["ts"]
    err = str(e)[:2000]

    spark.createDataFrame(
        [(pipeline, run_id, None, started_at, finished_at, "FAILED", 0, err)],
        ["pipeline","run_id","snapshot_date","started_at","finished_at","status","rows_written","message"]
    ).createOrReplaceTempView("log_fail")

    spark.sql("""
    MERGE INTO etl_demo.control.run_log AS t
    USING log_fail AS s
    ON t.pipeline = s.pipeline AND t.run_id = s.run_id
    WHEN MATCHED THEN UPDATE SET *
    """)

    raise
