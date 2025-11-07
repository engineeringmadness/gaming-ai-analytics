# Databricks notebook source
from pyspark.sql import DataFrame
import requests
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../utilities

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scoring Rubrik
# MAGIC
# MAGIC - Very Positive Review gets +5
# MAGIC - Somewhat Positive Review gets + 2
# MAGIC - Each Neutral Review gets + 1
# MAGIC - Somewhat Negative Review gets -2
# MAGIC - Very Bad Review gets -5
# MAGIC - Unable to understand review gets 0
# MAGIC - Reviewers who received game for free get their score weighted to 0.5

# COMMAND ----------

def generate_sentiment_score(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "sentiment_score",
        F.expr(
            """ai_query('databricks-llama-4-maverick',
                CONCAT(
                'Role: You are a data labeller for a game review company
                Task: Based on review determine a numerical score with following guidelines
                    - Very Positive Review gets +5
                    - Somewhat Positive Review gets + 2
                    - Each Neutral Review gets + 1
                    - Somewhat Negative Review gets -2
                    - Very Bad Review gets -5
                    - Unable to understand review gets 0
                    - If you feed parts of the review fit different scoring categories select the most positive one
                IMPORTANT: Output ONLY one single final score value. No introductions, explanations, or additional commentary. 
                Please no explanations needed only one single number
                Review: ', review_text))"""
        ),
    )

# COMMAND ----------

raw_zone = dbutils.widgets.get("raw_location")

# COMMAND ----------

reviews_df =  (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .option("sep", ",")
        .option("multiline", True)
        .option("escape", '"')
        .option("encoding", "UTF-8")
        .csv(f"{raw_zone}/reviews.csv")
    )

# COMMAND ----------

# Remove people who haven't played the game prior to reviewing it
# Remove reviews from alpha phase
without_spam = reviews_df.filter(
    (F.col("author_playtime_at_review") > 0) & (F.col("author_playtime_forever") > 1)
).filter(F.col("written_during_early_access") == False)

# COMMAND ----------

with_scores = generate_sentiment_score(without_spam).withColumn(
    "weighted_score",
    F.when(F.col("received_for_free"), F.col("sentiment_score") * 0.5).otherwise(
        F.col("sentiment_score") * 1
    ),
)

# COMMAND ----------

final_df = with_scores.select(
                        F.col(GameConstants.GAME_ID),
                        F.col(GameConstants.REVIEW_ID),
                        F.col("language"),
                        F.col("timestamp_updated").alias("updated_at"),
                        F.col("received_for_free").alias("sponsored_review"),
                        F.col("comment_count"),
                        F.col("author_playtime_forever"),
                        F.col("author_playtime_at_review"),
                        F.col("weighted_score"))

# COMMAND ----------

save_data(layer='fact', table_name='reviews', df=final_df)
