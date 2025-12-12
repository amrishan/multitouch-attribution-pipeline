
# Databricks notebook source
# MAGIC %pip install -e ../

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview
# MAGIC Refactored notebook for Data Generation. Uses `src.multitouch` package.

# COMMAND ----------

from multitouch.config import ProjectConfig
from multitouch.data_generation import generate_synthetic_data
import os

# COMMAND ----------

dbutils.widgets.text("project_dir", "/dbfs/tmp/multitouch_attribution")
dbutils.widgets.text("database_name", "multi_touch_attribution")

project_dir_arg = dbutils.widgets.get("project_dir")
database_name_arg = dbutils.widgets.get("database_name")

config = ProjectConfig(project_directory=project_dir_arg, database_name=database_name_arg)

# COMMAND ----------

print(f"Generating data to {config.data_gen_path}")
# Ensure directory exists
os.makedirs(os.path.dirname(config.data_gen_path), exist_ok=True)

generate_synthetic_data(config.data_gen_path)

print("Data generation complete.")

# COMMAND ----------
# Optional: Display loaded data
display(spark.read.format('csv').option('header','true').load(config.raw_data_path))