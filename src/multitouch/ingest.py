
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
    # Determine target
    target = table_name if table_name else f"delta.`{bronze_tbl_path}`"

    # Infer schema from raw data (needed for table creation if not exists)
    try:
        schema = spark.read.parquet(raw_data_path).schema
    except Exception as e:
        print(f"Warning: Could not infer schema from {raw_data_path}: {e}")
        # If we can't infer schema, we might fail to create table if it doesn't exist.
        # But we can try to proceed if table exists.
        schema = None

    # Ensure table exists (COPY INTO requires it)
    try:
        if schema:
            if table_name:
                # Managed Table (Unity Catalog): Write empty DF to initialize table if it doesn't exist
                # This automatically handles schema and location management by UC
                # Note: saveAsTable with mode("ignore") will do the CREATE IF NOT EXISTS equivalent
                spark.createDataFrame([], schema).write.format("delta").mode("ignore").saveAsTable(table_name)
            else:
                # Legacy: Path based
                spark.createDataFrame([], schema).write.format("delta").mode("ignore").save(bronze_tbl_path)
    except Exception as e:
        print(f"Note: Table initialization skipped or failed: {e}")

    # COPY INTO command with transformations
    # Note: We use to_timestamp with pattern to match original logic
    sql = f"""
    COPY INTO {target}
    FROM (
        SELECT 
            to_timestamp(time, 'yyyy-MM-dd HH:mm:ss') as time,
            cast(conversion as int) as conversion,
            * except(time, conversion)
        FROM '{raw_data_path}'
    )
    FILEFORMAT = PARQUET
    COPY_OPTIONS ('mergeSchema' = 'true', 'force' = 'true')
    """
    
    return spark.sql(sql)

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
