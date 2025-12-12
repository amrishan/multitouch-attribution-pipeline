
# Databricks notebook source
# MAGIC %pip install -e ../

# COMMAND ----------

from multitouch.config import ProjectConfig
from multitouch.ingest import ingest_data, register_bronze_table

# COMMAND ----------

dbutils.widgets.text("project_dir", "/dbfs/tmp/multitouch_attribution")
dbutils.widgets.text("database_name", "multi_touch_attribution")

project_dir_arg = dbutils.widgets.get("project_dir")
database_name_arg = dbutils.widgets.get("database_name")

config = ProjectConfig(project_directory=project_dir_arg, database_name=database_name_arg)

# COMMAND ----------

# Ingest Data
print(f"Reading from {config.raw_data_path}")
query = ingest_data(spark, config.raw_data_path, config.bronze_tbl_path)

if query:
    query.awaitTermination()

# COMMAND ----------

# Register Table
register_bronze_table(spark, config.database_name, config.bronze_tbl_path)

# COMMAND ----------

# Verify
spark.sql(f"USE {config.database_name}")
display(spark.table("bronze"))