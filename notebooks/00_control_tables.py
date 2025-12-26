# =========================================
# 00 CONTROL TABLES (run log)
# =========================================

spark.sql("CREATE SCHEMA IF NOT EXISTS etl_demo.control")

spark.sql("""
CREATE TABLE IF NOT EXISTS etl_demo.control.run_log (
  pipeline      STRING,          -- 'bronze' / 'staging' / 'silver' / 'gold'
  run_id        STRING,
  snapshot_date DATE,
  started_at    TIMESTAMP,
  finished_at   TIMESTAMP,
  status        STRING,          -- 'STARTED'/'SUCCESS'/'FAILED'
  rows_written  BIGINT,
  message       STRING
)
USING DELTA
""")

print("✅ control.run_log ready")
