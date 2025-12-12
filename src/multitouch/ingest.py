
from pyspark.sql.functions import col, to_timestamp

def ingest_data(spark, raw_data_path, bronze_tbl_path):
    """
    Ingests data using Auto Loader (cloudFiles) and writes to Delta.
    """
    # Infer schema from raw data (assuming csv with header)
    # Note: in production, schema should likely be explicit, but following original logic
    try:
        schema = spark.read.csv(raw_data_path, header=True).schema
    except Exception as e:
        # Fallback if path doesn't exist yet or is empty, though usually it should exist
        print(f"Warning: Could not infer schema from {raw_data_path}: {e}")
        return None

    # Read Stream
    # Note: cloudFiles options are specific to Databricks
    raw_data_df = spark.readStream.format("cloudFiles") \
                .option("cloudFiles.validateOptions", "false") \
                .option("cloudFiles.format", "csv") \
                .option("header", "true") \
                .option("cloudFiles.region", "us-west-2") \
                .option("cloudFiles.includeExistingFiles", "true") \
                .schema(schema) \
                .load(raw_data_path) 
    
    # Transform
    raw_data_df = raw_data_df.withColumn("time", to_timestamp(col("time"),"yyyy-MM-dd HH:mm:ss"))\
                  .withColumn("conversion", col("conversion").cast("int"))
                  
    # Write Stream (Trigger once)
    query = raw_data_df.writeStream.format("delta") \
      .trigger(once=True) \
      .option("checkpointLocation", bronze_tbl_path+"/checkpoint") \
      .start(bronze_tbl_path)
    
    return query

def register_bronze_table(spark, database_name, bronze_tbl_path, reset=True):
    """
    Registers the Delta table in the Metastore.
    """
    if reset:
        spark.sql(f'DROP DATABASE IF EXISTS {database_name} CASCADE')
        
    spark.sql(f'CREATE DATABASE IF NOT EXISTS {database_name}')
    
    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS `{database_name}`.bronze
      USING DELTA 
      LOCATION '{bronze_tbl_path}'
      """)
