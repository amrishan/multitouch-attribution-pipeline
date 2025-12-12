

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


dbutils.widgets.text("project_dir", "/dbfs/FileStore/multitouch_attribution")
# Note: Using FileStore
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

# Create temp view
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW user_journey_view AS
SELECT
  sub2.uid AS uid,CASE
    WHEN sub2.conversion == 1 then concat('Start > ', sub2.path, ' > Conversion')
    ELSE concat('Start > ', sub2.path, ' > Null')
  END AS path,
  sub2.first_interaction AS first_interaction,
  sub2.last_interaction AS last_interaction,
  sub2.conversion AS conversion,
  sub2.visiting_order AS visiting_order
FROM
  (
    SELECT
      sub.uid AS uid,
      concat_ws(' > ', collect_list(sub.channel)) AS path,
      element_at(collect_list(sub.channel), 1) AS first_interaction,
      element_at(collect_list(sub.channel), -1) AS last_interaction,
      element_at(collect_list(sub.conversion), -1) AS conversion,
      collect_list(sub.visit_order) AS visiting_order
    FROM
      (
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
      ) AS sub
    GROUP BY
      sub.uid
  ) AS sub2;
""")

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