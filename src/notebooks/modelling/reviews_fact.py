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
        F.when(((F.col("review_text").isNull()) | (F.col("review_text") == "")), 0).otherwise(
            F.expr(
                """ai_query('databricks-llama-4-maverick',
                    CONCAT(
                    'Analyze this game review and return only a single integer score. Do not write anything else.\n',
                    'Scoring rules:\n',
                    '- Very Positive: 5\n',
                    '- Somewhat Positive: 2\n',
                    '- Neutral: 1\n',
                    '- Somewhat Negative: -2\n',
                    '- Very Negative: -5\n',
                    '- Unable to understand: 0\n',
                    'If review has mixed sentiments, use the most positive one.\n',
                    'Review: ', review_text, '\n',
                    'Score:'))"""
            ),
        )
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

final_df = without_spam.select(
                        F.col(GameConstants.GAME_ID),
                        F.col(GameConstants.REVIEW_ID),
                        F.col("language"),
                        F.col("timestamp_updated").alias("updated_at"),
                        F.col("received_for_free").alias("sponsored_review"),
                        F.col("comment_count"),
                        F.col("author_playtime_forever"),
                        F.col("author_playtime_at_review"),
                        F.col("review_text"))

# COMMAND ----------

# Each review ID is unique and review text is not changing so once its processed by scoring system it need not to be reprocessed
if table_exists('fact_reviews'):
    existing_data = load_data(layer="fact", table_name="reviews").select(GameConstants.REVIEW_ID)
    final_df = final_df.join(existing_data, on=GameConstants.REVIEW_ID, how="leftanti")

# COMMAND ----------

with_scores = generate_sentiment_score(final_df)

# COMMAND ----------

fixed_score = with_scores.withColumn(
    "fixed_score",
    F.when(
        F.expr("try_cast(sentiment_score as int)").isNotNull(),
        F.col("sentiment_score").cast("int")
    ).otherwise(F.lit(0)),
)

# COMMAND ----------

weighted_scores = fixed_score.withColumn(
    "weighted_score",
    F.when(F.col("sponsored_review"), F.col("fixed_score") * 0.5).otherwise(
        F.col("fixed_score")
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC AI Scoring is a slow process and limited by quota of serverless compute allowed in Databricks Free Edition.
# MAGIC Hence we use batches of size 10k to generate fact_reviews

# COMMAND ----------

total_size = final_df.count()
batch_size = int(dbutils.widgets.get("batch_size"))
batch_num = 0
num_of_batches = round(total_size / batch_size)
print(f"Batches left: {num_of_batches}")

# COMMAND ----------

df = weighted_scores.limit(batch_size)
save_data(layer='fact', table_name='reviews', df=df, mode='append')
