


# Databricks notebook source
# MAGIC %pip install -e ../

# COMMAND ----------

import sys
import os

# Robustly find the repo root directory
# In Databricks Repos, we can often rely on ".." relative to the notebook path in notebooks/
# but sys.path needs absolute path.
current_notebook_dir = os.getcwd()
repo_root = os.path.dirname(current_notebook_dir)
src_path = os.path.join(repo_root, "src")

# Explicitly append to system path to ensure the module is found
if src_path not in sys.path:
    sys.path.append(src_path)
    print(f"Added {src_path} to sys.path")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview
# MAGIC Refactored notebook for Data Generation. Uses `src.multitouch` package.

# COMMAND ----------

from multitouch.config import ProjectConfig
from multitouch.data_generation import generate_synthetic_data
import os

# COMMAND ----------


# Note: Using FileStore because Public DBFS Root is typically disabled on free tier.
# Alternatively, use a Workspace path dynamically if preferred (requires code updates)
import os
username = os.environ.get('USER', 'your-email')
default_path = f"/Workspace/Users/{username}/multitouch_attribution"

# Check if we are likely in a workspace environment
if os.path.exists("/Workspace/Users"):
   dbutils.widgets.text("project_dir", default_path)
else:
   dbutils.widgets.text("project_dir", "/dbfs/FileStore/multitouch_attribution")

dbutils.widgets.text("database_name", "multi_touch_attribution")

project_dir_arg = dbutils.widgets.get("project_dir")
database_name_arg = dbutils.widgets.get("database_name")

config = ProjectConfig(project_directory=project_dir_arg, database_name=database_name_arg)

# COMMAND ----------


print(f"Generating data to {config.data_gen_path}")
# Ensure directory exists
if config.is_workspace_path:
    # Workspace files supported via os.makedirs
    os.makedirs(os.path.dirname(config.data_gen_path), exist_ok=True)
else:
    # DBFS
    print(f"Creating directory in DBFS: {config.raw_data_path}")
    dbutils.fs.mkdirs(config.raw_data_path)

generate_synthetic_data(config.data_gen_path)

print("Data generation complete.")

# COMMAND ----------
# Optional: Display loaded data
display(spark.read.format('csv').option('header','true').load(config.raw_data_path))