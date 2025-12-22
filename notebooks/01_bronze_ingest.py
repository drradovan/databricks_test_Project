# =========================================
# Bronze ingest – Auto Loader (Databricks Free)
# =========================================

from pyspark.sql.functions import current_timestamp, col

# ---- PODESI PUTANJE ----
input_path = "dbfs:/Volumes/etl_demo/bronze/customer_volume/in"
checkpoint_path = "dbfs:/Volumes/etl_demo/bronze/customer_volume/_checkpoints/bronze"
schema_location = "dbfs:/Volumes/etl_demo/bronze/customer_volume/_schema"

bronze_table = "etl_demo.bronze.customer_bronze"

def run_bronze_autoloader():
    df_stream = (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaLocation", schema_location)
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            .option("cloudFiles.rescuedDataColumn", "_rescued_data")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(input_path)
    )

    df_out = (
        df_stream
            .withColumn("_ingest_time", current_timestamp())
            .withColumn("_file_path", col("_metadata.file_path"))
    )

    query = (
        df_out.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_path)
            .trigger(availableNow=True)   # Free-friendly
            .toTable(bronze_table)
    )

    query.awaitTermination()
    print("✅ Bronze ingest finished")

# ---- POZIV ----
run_bronze_autoloader()

