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
