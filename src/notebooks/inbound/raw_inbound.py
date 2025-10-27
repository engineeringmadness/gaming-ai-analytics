# Databricks notebook source
from pyspark.sql import DataFrame

# COMMAND ----------

def save_layer(layer: str, table_name: str, df: DataFrame):
    catalog = dbutils.widgets.get('catalog')
    schema = dbutils.widgets.get('environment')
    df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.{layer}_{table_name}")

# COMMAND ----------

raw_zone = dbutils.widgets.get("raw_location")
csv_files = dbutils.fs.ls(raw_zone)
display(csv_files)

# COMMAND ----------

for file in csv_files:
    print(f"Processing file in raw zone: {file.name} ==============")
    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .option("sep", ",")
        .option("multiline", True)
        .option("escape", '"')
        .option("encoding", "UTF-8")
        .csv(file.path)
    )

    table_name = file.name.split(".")[0]
    save_layer("inbound", table_name, df)
