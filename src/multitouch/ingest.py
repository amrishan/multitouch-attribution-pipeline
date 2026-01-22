
from pyspark.sql.functions import col, to_timestamp


def ingest_data(spark, raw_data_path, bronze_tbl_path, table_name=None):
    """
    Ingests data using Auto Loader (cloudFiles) and writes to Delta.
    
    Args:
        spark: SparkSession
        raw_data_path: Input path for Auto Loader
        bronze_tbl_path: Path for checkpointing (and output if table_name is None)
        table_name: Optional. If provided, writes to a Managed Table using .toTable()
    """
    # Infer schema from raw data (assuming parquet)
    try:
        # Parquet files carry their own schema
        # In cloudFiles (Autoloader) with parquet, schema inference is often automatic or we can sample.
        # But keeping structure:
        schema = spark.read.parquet(raw_data_path).schema
    except Exception as e:
        # Fallback if path doesn't exist yet or is empty, though usually it should exist
        print(f"Warning: Could not infer schema from {raw_data_path}: {e}")
        return None

    # Read Stream
    # Note: cloudFiles options are specific to Databricks
    raw_data_df = spark.readStream.format("cloudFiles") \
                .option("cloudFiles.validateOptions", "false") \
                .option("cloudFiles.format", "parquet") \
                .option("cloudFiles.region", "us-east-2") \
                .option("cloudFiles.includeExistingFiles", "true") \
                .schema(schema) \
                .load(raw_data_path) 
    
    # Transform
    raw_data_df = raw_data_df.withColumn("time", to_timestamp(col("time"),"yyyy-MM-dd HH:mm:ss"))\
                  .withColumn("conversion", col("conversion").cast("int"))
                  
    # Write Stream
    writer = raw_data_df.writeStream.format("delta") \
      .trigger(once=True) \
      .option("checkpointLocation", bronze_tbl_path+"/checkpoint")
    
    if table_name:
        # Write to managed table
        query = writer.toTable(table_name)
    else:
        # Legacy: Write to path
        query = writer.start(bronze_tbl_path)
    
    return query

def register_bronze_table(spark, database_name, bronze_tbl_path, reset=True):
    """
    Registers the Delta table in the Metastore.
    Deprecated if using ingest_data with table_name (Managed Tables).
    """
    if reset:
        spark.sql(f'DROP DATABASE IF EXISTS {database_name} CASCADE')
        
    spark.sql(f'CREATE DATABASE IF NOT EXISTS {database_name}')
    

    # Only create external table if NOT using managed tables approach
    # logic here assumes if we call this, we want an external table.
    # UPDATE: For Unity Catalog compatibility, we default to managed tables (no LOCATION)
    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS `{database_name}`.bronze
      USING DELTA 
      """)
