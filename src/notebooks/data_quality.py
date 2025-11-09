# Databricks notebook source
from pyspark.sql import functions as F
from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQRowRule, DQDatasetRule
from databricks.sdk import WorkspaceClient

# COMMAND ----------

# MAGIC %run ./utilities

# COMMAND ----------

workspace_client = WorkspaceClient()
dq_engine = DQEngine(workspace_client)

# COMMAND ----------

# MAGIC %md
# MAGIC Checks on Games Fact - Primary key check on review ID as we are using append mode. AI generated score should be between the limits specified in scoring prompt (-5, 5)

# COMMAND ----------

checks = [
  DQDatasetRule(
    criticality="error",
    check_func=check_funcs.is_unique,
    columns=[GameConstants.REVIEW_ID],
  ),
  DQRowRule(
    criticality="error",
    check_func=check_funcs.is_in_range,
    column=GameConstants.WEIGHTED_SCORE,
    check_func_kwargs={"min_limit": -5, "max_limit": 5}
  ),
]

# COMMAND ----------

input_df = load_data(layer="fact", table_name="reviews")
checks_applied = dq_engine.apply_checks(input_df, checks)
errors = checks_applied.filter(F.col("_errors").isNotNull())

if errors.count() > 0:
    display(errors)
    raise Exception("Data quality checks failed")

# COMMAND ----------

# MAGIC %md
# MAGIC Foreign Key checks between Linkage and Dimension tables - The dimension IDs in linkage tables should reference primary key of corresponding dimension tables

# COMMAND ----------

dimensions = ['categories', 'genres', 'developers', 'publishers']
ids = ['category_id', 'genre_id', 'developer_id', 'publisher_id']

for dim, dim_id in zip(dimensions, ids):
  dim_table = load_data(layer="dim", table_name=dim)
  linkage_table = load_data(layer="linkage", table_name=dim)

  integrity_check = [
      DQDatasetRule(
      criticality="error",
      check_func=check_funcs.foreign_key,
      columns=[dim_id],
      check_func_kwargs={"ref_df_name": "linkage", "ref_columns": [GameConstants.DIM_ID]}
    ),
  ]

  ref_dfs = {"linkage": dim_table}

  checks_applied = dq_engine.apply_checks(linkage_table, integrity_check, ref_dfs=ref_dfs)
  errors = checks_applied.filter(F.col("_errors").isNotNull())
  
  if errors.count() > 0:
    display(errors)
    raise Exception("Data quality checks failed")
