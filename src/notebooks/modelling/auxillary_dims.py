# Databricks notebook source
from pyspark.sql import DataFrame
import requests
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../utilities

# COMMAND ----------

def generate_translations_for_name(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "name",
        F.expr(
            """ai_query('databricks-llama-4-maverick',
                CONCAT(
                'Task: Translate the following text TO English.
                    - If the input is already in English, output it exactly as is.
                    - Output ONLY the translated (or original) text. No introductions, explanations, or additional commentary.
                    - If translation is impossible (e.g., not text or gibberish), output exactly: "NA"
                    
                Input: ', name))"""
        ),
    )

# COMMAND ----------

raw_zone = dbutils.widgets.get("raw_location")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("environment")
csv_files = dbutils.fs.ls(raw_zone)
display(csv_files)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Work on Fixed dimensions i.e. Genre, Category, Publisher, Developer

# COMMAND ----------

standard_dims = ['developers.csv', 'publishers.csv']
dims_to_translate = ['categories.csv', 'genres.csv']
linkage_tables = ['application_developers.csv', 'application_publishers.csv', 'application_categories.csv', 'application_genres.csv']

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
    # Fixed dimensions don't need to be repopulated
    if not table_exists(table_name):
        if file.name in linkage_tables:
            df.createOrReplaceTempView(f"linkage_{table_name.replace('application_', '')}")
        elif file.name in dims_to_translate:
            print('Fixing dimension values')
            df = generate_translations_for_name(df)
            df.createOrReplaceTempView(f"stage_{table_name}")
        elif file.name in standard_dims:
            df.createOrReplaceTempView(f"stage_{table_name}")

# COMMAND ----------

flatten_dimensions = {
    "linkage_categories": {
        "reference_table": "stage_categories",
        "key": "category_id"
    },
    "linkage_genres": {
        "reference_table": "stage_genres",
        "key": "genre_id"
    },
    "linkage_developers": {
        "reference_table": "stage_developers",
        "key": "developer_id"
    },
    "linkage_publishers": {
        "reference_table": "stage_publishers",
        "key": "publisher_id"
    },
}

for key, value in flatten_dimensions.items():
    print(f"Flattening {key} ===================")
    link = spark.sql(f"SELECT * FROM {key}")
    ref = spark.sql(f"SELECT * FROM {value['reference_table']}")
    joined_df = link.join(ref, (link[value['key']] == ref[GameConstants.DIM_ID]), how='left')
    save_data(layer="dim", table_name=key.replace('linkage_', ''), df=joined_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the main dimension - Application (aka Game)

# COMMAND ----------

games_df =  (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .option("sep", ",")
        .option("multiline", True)
        .option("escape", '"')
        .option("encoding", "UTF-8")
        .csv(f"{raw_zone}/applications.csv")
    )

# COMMAND ----------

remove_non_games = games_df.filter(F.col("type").isin(GameConstants.GAME_TYPES))

# COMMAND ----------

# If game is free then its initial price should be 0 (NULL), It cannot be free and have non zero price
remove_invalid_records = (
    remove_non_games.fillna(
        {"mat_initial_price": 0, "mat_final_price": 0, "mat_currency": "USD"}
    )
    .withColumn("on_sale", F.col("mat_initial_price") > F.col("mat_final_price"))
    .filter(
        (F.col("is_free") & (F.col("mat_initial_price") == 0)) | (~F.col("is_free"))
    )
)

# COMMAND ----------

#Fix column names
final_df = remove_invalid_records.select(
                        F.col(GameConstants.GAME_ID),
                        F.col("name"),
                        F.col("release_date"),
                        F.col("mat_supports_windows").alias("supports_windows"),
                        F.col("mat_supports_mac").alias("supports_mac"),
                        F.col("mat_supports_linux").alias("supports_linux"),
                        F.col("mat_final_price").alias("sale_price"),
                        F.col("mat_currency"),
                        F.col("metacritic_score"),
                        F.col("on_sale"),
                        F.col("updated_at")
                      )

# COMMAND ----------

save_data(layer='dim', table_name='games', df=final_df)
