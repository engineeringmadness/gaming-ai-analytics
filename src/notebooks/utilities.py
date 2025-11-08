# Databricks notebook source
from pyspark.sql import DataFrame
import requests
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql import functions as F

# COMMAND ----------

class GameConstants:
  GAME_TYPES = ['game', 'demo', 'dlc']
  GAME_ID = 'appid'
  REVIEW_ID = 'recommendationid'

# COMMAND ----------

def save_data(layer: str, table_name: str, df: DataFrame):
    catalog = dbutils.widgets.get('catalog')
    schema = dbutils.widgets.get('environment')
    df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.{layer}_{table_name}")

# COMMAND ----------

def table_exists(table_name: str) -> bool:
    catalog = dbutils.widgets.get('catalog')
    schema = dbutils.widgets.get('environment')
    tables_df = spark.sql(f"SHOW TABLES IN {catalog}.{schema}")
    tables = [row.tableName for row in tables_df.collect()]
    return table_name in tables

# COMMAND ----------


