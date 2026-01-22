

# Databricks notebook source
# MAGIC %pip install -e ../

# COMMAND ----------

import sys
import os

# Robustly find the repo root directory
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
# MAGIC Refactored for package structure. uses `src.multitouch`.

# COMMAND ----------

from multitouch.config import ProjectConfig
import matplotlib.pyplot as plt
import seaborn as sns

# COMMAND ----------



dbutils.widgets.text("project_dir", "/Volumes/mycatalog/multi_touch_attribution/raw")
# Note: Using User-specified Volume
dbutils.widgets.text("database_name", "multi_touch_attribution")

project_dir_arg = dbutils.widgets.get("project_dir")
database_name_arg = dbutils.widgets.get("database_name")

config = ProjectConfig(project_directory=project_dir_arg, database_name=database_name_arg)

# COMMAND ----------

spark.sql(f"USE {config.database_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create a Gold-level User Journey Table

# COMMAND ----------

# Step 1: Calculate Visit Order (Inner Layer)
print("Step 1: Calculating Visit Order...")
spark.sql("""
CREATE OR REPLACE TEMP VIEW sub_visit_order AS
SELECT
  uid,
  channel,
  time,
  conversion,
  dense_rank() OVER (
    PARTITION BY uid
    ORDER BY
      time asc
  ) as visit_order
FROM
  bronze
""")
print("Step 1 Complete. Sample output (sub_visit_order):")
spark.sql("SELECT * FROM sub_visit_order LIMIT 5").show(truncate=False)


# Step 2: Aggregate User Paths (Middle Layer)
print("Step 2: Aggregating User Paths...")
spark.sql("""
CREATE OR REPLACE TEMP VIEW sub_aggregated AS
SELECT
  uid,
  concat_ws(' > ', collect_list(channel)) AS path,
  element_at(collect_list(channel), 1) AS first_interaction,
  element_at(collect_list(channel), -1) AS last_interaction,
  element_at(collect_list(conversion), -1) AS conversion,
  collect_list(visit_order) AS visiting_order
FROM
  sub_visit_order
GROUP BY
  uid
""")
print("Step 2 Complete. Sample output (sub_aggregated):")
spark.sql("SELECT * FROM sub_aggregated LIMIT 5").show(truncate=False)


# Step 3: Final User Journey View (Outer Layer)
print("Step 3: Creating User Journey View...")
spark.sql("""
CREATE OR REPLACE TEMP VIEW user_journey_view AS
SELECT
  uid,
  CASE
    WHEN conversion == 1 then concat('Start > ', path, ' > Conversion')
    ELSE concat('Start > ', path, ' > Null')
  END AS path,
  first_interaction,
  last_interaction,
  conversion,
  visiting_order
FROM
  sub_aggregated
""")
print("Step 3 Complete. Sample output (user_journey_view):")
spark.sql("SELECT * FROM user_journey_view LIMIT 5").show(truncate=False)

# COMMAND ----------

# Create gold table
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS `{config.database_name}`.gold_user_journey
  USING DELTA 
  LOCATION '{config.gold_user_journey_tbl_path}'
  AS SELECT * from user_journey_view
""")

# COMMAND ----------

display(spark.table("gold_user_journey"))

# COMMAND ----------

# Optimize
spark.sql(f"OPTIMIZE `{config.database_name}`.gold_user_journey ZORDER BY uid")

# COMMAND ----------

# Attribution view
spark.sql("""
CREATE OR REPLACE TEMP VIEW attribution_view AS
SELECT
  'first_touch' AS attribution_model,
  first_interaction AS channel,
  round(count(*) / (
     SELECT COUNT(*)
     FROM gold_user_journey
     WHERE conversion = 1),2) AS attribution_percent
FROM gold_user_journey
WHERE conversion = 1
GROUP BY first_interaction
UNION
SELECT
  'last_touch' AS attribution_model,
  last_interaction AS channel,
  round(count(*) /(
      SELECT COUNT(*)
      FROM gold_user_journey
      WHERE conversion = 1),2) AS attribution_percent
FROM gold_user_journey
WHERE conversion = 1
GROUP BY last_interaction
""")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- Gold Table: User Journey
# MAGIC -- Attribution model needs journey data
# MAGIC CREATE OR REPLACE TABLE gold_user_journey
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT 
# MAGIC   uid,
# MAGIC   collect_list(channel) as touch_points
# MAGIC FROM 
# MAGIC   silver_user_interactions
# MAGIC GROUP BY 
# MAGIC   uid

# COMMAND ----------

# Gold Attribution table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS gold_attribution
USING DELTA
LOCATION '{config.gold_attribution_tbl_path}'
AS
SELECT * FROM attribution_view
""")

# COMMAND ----------

display(spark.table("gold_attribution"))