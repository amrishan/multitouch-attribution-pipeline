
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

from multitouch.config import ProjectConfig
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
sns.set(font_scale = 1.4)

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
# MAGIC ## Step 2: Create and Populate Ad Spend Table

# COMMAND ----------

# Create table
spark.sql(f'''
  CREATE OR REPLACE TABLE gold_ad_spend (
    campaign_id STRING, 
    total_spend_in_dollars FLOAT, 
    channel_spend MAP<STRING, FLOAT>, 
    campaign_start_date TIMESTAMP)
  USING DELTA
  LOCATION '{config.gold_ad_spend_tbl_path}'
  ''')

# COMMAND ----------

dbutils.widgets.text("adspend", "10000", "Campaign Budget in $")
adspend_val = dbutils.widgets.get("adspend")

# COMMAND ----------

# Insert synthetic data
spark.sql(f'''
INSERT INTO TABLE gold_ad_spend
VALUES ("3d65f7e92e81480cac52a20dfdf64d5b", {adspend_val},
          MAP('Social Network', .2,
              'Search Engine Marketing', .2,  
              'Google Display Network', .2, 
              'Affiliates', .2, 
              'Email', .2), 
         make_timestamp(2020, 5, 17, 0, 0, 0));
''')

# COMMAND ----------

# Step 2.5: Explode
ad_spend_df = spark.sql('select explode(channel_spend) as (channel, pct_spend), \
                         round(total_spend_in_dollars * pct_spend, 2) as dollar_spend \
                         from gold_ad_spend')

ad_spend_df.createOrReplaceTempView("exploded_gold_ad_spend")
display(ad_spend_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: View Campaign Performance

# COMMAND ----------

# Base conversion rate
spark.sql(f'''
CREATE OR REPLACE TABLE base_conversion_rate
USING DELTA AS
SELECT count(*) as count,
  CASE 
    WHEN conversion == 0 
    THEN 'Impression'
    ELSE 'Conversion'
  END AS interaction_type
FROM
  gold_user_journey
GROUP BY
  conversion;
''')

# COMMAND ----------

base_conversion_rate_pd = spark.table("base_conversion_rate").toPandas()

pie, ax = plt.subplots(figsize=[20,9])
labels = base_conversion_rate_pd['interaction_type']
plt.pie(x=base_conversion_rate_pd['count'], autopct="%.1f%%", explode=[0.05]*2, labels=labels, pctdistance=0.5)
plt.title("Base Conversion Rate");
plt.show()

# COMMAND ----------

# Conversions by date
spark.sql(f'''
CREATE OR REPLACE TABLE conversions_by_date 
USING DELTA AS
SELECT count(*) AS count,
  'Conversion' AS interaction_type,
  date(time) AS date
FROM bronze
WHERE conversion = 1
GROUP BY date
ORDER BY date;
''')

# COMMAND ----------

conversions_by_date_pd = spark.table("conversions_by_date").toPandas()

plt.figure(figsize=(20,9))
pt = sns.lineplot(x='date',y='count',data=conversions_by_date_pd)

pt.tick_params(labelsize=20)
pt.set_xlabel('Date')
pt.set_ylabel('Number of Conversions')
plt.title("Conversions by Date");
plt.show()

# COMMAND ----------

# Attribution by model type
spark.sql(f'''
CREATE OR REPLACE TABLE attribution_by_model_type 
USING DELTA AS
SELECT attribution_model, channel, round(attribution_percent * (
    SELECT count(*) FROM gold_user_journey WHERE conversion = 1)) AS conversions_attributed
FROM gold_attribution;
''')

# COMMAND ----------

attribution_by_model_type_pd = spark.table("attribution_by_model_type").toPandas()

pt = sns.catplot(x='channel',y='conversions_attributed',hue='attribution_model',data=attribution_by_model_type_pd, kind='bar', aspect=4, legend=True)
if hasattr(pt, 'fig'):
    pt.fig.set_figwidth(20)
    pt.fig.set_figheight(9)

plt.tick_params(labelsize=15)
plt.ylabel("Number of Conversions")
plt.xlabel("Channels")
plt.title("Channel Performance");
plt.show()

# COMMAND ----------

# CPA Summary
spark.sql(f'''
CREATE OR REPLACE TABLE cpa_summary 
USING DELTA
AS
SELECT
  spending.channel,
  spending.dollar_spend,
  attribution_count.attribution_model,
  attribution_count.conversions_attributed,
  round(spending.dollar_spend / attribution_count.conversions_attributed,2) AS CPA_in_Dollars
FROM
  (SELECT explode(channel_spend) AS (channel, spend),
   round(total_spend_in_dollars * spend, 2) AS dollar_spend
   FROM gold_ad_spend) AS spending
JOIN
  (SELECT attribution_model, channel, round(attribution_percent * (
      SELECT count(*) FROM gold_user_journey WHERE conversion = 1)) AS conversions_attributed
   FROM gold_attribution) AS attribution_count
ON spending.channel = attribution_count.channel;
''')

# COMMAND ----------

cpa_summary_pd = spark.table("cpa_summary").toPandas()

pt = sns.catplot(x='channel', y='CPA_in_Dollars',hue='attribution_model',data=cpa_summary_pd, kind='bar', aspect=4, ci=None)
plt.title("Cost of Acquisition by Channel")
if hasattr(pt, 'fig'):
    pt.fig.set_figwidth(20)
    pt.fig.set_figheight(9)

plt.tick_params(labelsize=15)
plt.ylabel("CPA in $")
plt.xlabel("Channels")
plt.title("Channel Cost per Acquisition");
plt.show()

# COMMAND ----------

# Optimization
spark.sql(f'''
CREATE OR REPLACE TABLE spend_optimization_view 
USING DELTA
AS
SELECT
  a.channel,
  a.pct_spend,
  b.attribution_percent,
  b.attribution_percent / a.pct_spend as ROAS,
  a.dollar_spend,
  round(
    (b.attribution_percent / a.pct_spend) * a.dollar_spend,
    2
  ) as proposed_dollar_spend
FROM
  exploded_gold_ad_spend a
  JOIN gold_attribution b on a.channel = b.channel
  and attribution_model = 'markov_chain';
''')

spark.sql(f'''
CREATE
OR REPLACE TABLE spend_optimization_final 
USING DELTA AS
SELECT
  channel,
  'current_spending' AS spending,
  dollar_spend as budget
 FROM exploded_gold_ad_spend
UNION
SELECT
  channel,
  'proposed_spending' AS spending,
  proposed_dollar_spend as budget
FROM
  spend_optimization_view;  
''')

# COMMAND ----------

spend_optimization_final_pd = spark.table("spend_optimization_final").toPandas()

pt = sns.catplot(x='channel', y='budget', hue='spending', data=spend_optimization_final_pd, kind='bar', aspect=4, ci=None)

plt.tick_params(labelsize=15)
if hasattr(pt, 'fig'):
    pt.fig.set_figwidth(20)
    pt.fig.set_figheight(9)
plt.title("Spend Optimization per Channel")
plt.ylabel("Budget in $")
plt.xlabel("Channels")
plt.show()