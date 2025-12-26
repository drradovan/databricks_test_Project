# =========================================
# 04 GOLD - Views + Daily KPI table
# Source: etl_demo.silver.customer_dim (SCD2)
# =========================================

from pyspark.sql import functions as F

silver_table = "etl_demo.silver.customer_dim"

spark.sql("CREATE SCHEMA IF NOT EXISTS etl_demo.gold")

# ---- 1) Gold "current" view (no data duplication)
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

print("✅ Created view: etl_demo.gold.customer_current")

# ---- 2) Gold daily KPI table
# We compute KPIs for the latest snapshot date present in Silver
s = spark.table(silver_table)

latest_date = s.select(F.max("valid_from").alias("d")).collect()[0]["d"]
print("📌 Latest valid_from date in silver:", latest_date)

# Create KPI table if not exists
spark.sql("""
CREATE TABLE IF NOT EXISTS etl_demo.gold.customer_daily_kpi (
  kpi_date       DATE,
  active_customers BIGINT,
  new_customers    BIGINT,
  changed_customers BIGINT,
  closed_records   BIGINT,
  run_time       TIMESTAMP
)
USING DELTA
""")

# Active customers at end of day = current rows whose valid_from <= date (and not closed before)
# But for snapshot-style loads, current rows represent latest snapshot.
active_customers = (
    s.filter(F.col("is_current") == True)
     .count()
)

# New customers on latest_date = rows whose valid_from == latest_date AND no prior history for that customer
# A customer is "new" if this is their first record in silver.
first_dates = (
    s.groupBy("customer_id")
     .agg(F.min("valid_from").alias("first_valid_from"))
)

new_customers = (
    first_dates.filter(F.col("first_valid_from") == F.lit(latest_date))
               .count()
)

# Changed customers on latest_date = customers that have a record with valid_from == latest_date
# and ALSO had an older record (min(valid_from) < latest_date)
changed_customers = (
    s.filter(F.col("valid_from") == F.lit(latest_date))
     .join(first_dates, on="customer_id", how="inner")
     .filter(F.col("first_valid_from") < F.lit(latest_date))
     .select("customer_id").distinct()
     .count()
)

# Closed records on latest_date = rows whose valid_to == latest_date (we closed them at this snapshot)
closed_records = (
    s.filter(F.col("valid_to") == F.lit(latest_date))
     .count()
)

kpi_df = spark.createDataFrame(
    [(latest_date, active_customers, new_customers, changed_customers, closed_records)],
    ["kpi_date", "active_customers", "new_customers", "changed_customers", "closed_records"]
).withColumn("run_time", F.current_timestamp())

# Upsert KPI for that date (idempotent)
kpi_df.createOrReplaceTempView("kpi_src")

spark.sql("""
MERGE INTO etl_demo.gold.customer_daily_kpi AS tgt
USING kpi_src AS src
ON tgt.kpi_date = src.kpi_date
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

print("✅ Upserted KPI row into etl_demo.gold.customer_daily_kpi")
