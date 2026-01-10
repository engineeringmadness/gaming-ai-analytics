# Databricks notebook source
from pyspark.sql import DataFrame
import requests
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql import functions as F
import json
import re

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

# Get OpenAI API configuration from job parameters
openai_api_key = access_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
openai_api_url = dbutils.widgets.get("AI_ENDPOINT")

# COMMAND ----------

def call_openai_sentiment_api(review_text):
    """Call OpenAI API to get sentiment score using structured JSON output"""
    if not review_text or review_text.strip() == "":
        return 0

    # Escape special characters in the review text
    escaped_text = review_text.replace('"', '\\"').replace("\n", " ").replace("\r", " ")

    prompt = f"""Analyze this game review and return a JSON object with the sentiment score.
Scoring rules:
- Very Positive: 5
- Somewhat Positive: 2
- Neutral: 1
- Somewhat Negative: -2
- Very Negative: -5
- Unable to understand: 0

Review: {escaped_text}

Return ONLY a JSON object in this exact format: {{"score": <integer>}}"""

    headers = {
        "Authorization": f"Bearer {openai_api_key}",
        "Content-Type": "application/json"
    }

    data = {
        "model": "gpt-3.5-turbo",
        "messages": [
            {"role": "system", "content": "You are a sentiment analysis system. You must return only a JSON object with a single integer score based on the review provided."},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.0,
        "max_tokens": 20,  # Increased to accommodate JSON structure
        "response_format": { "type": "json_object" }  # Force JSON output
    }

    try:
        response = requests.post(openai_api_url, headers=headers, json=data, timeout=30)
        response.raise_for_status()

        result = response.json()
        content = result['choices'][0]['message']['content'].strip()

        # Parse JSON response and extract score
        json_response = json.loads(content)
        if 'score' in json_response and isinstance(json_response['score'], int):
            return json_response['score']
        else:
            # Fallback: try to extract any integer from the response
            match = re.search(r'(-?\d+)', content)
            if match:
                return int(match.group(1))
            else:
                return 0

    except json.JSONDecodeError as e:
        print(f"Error parsing JSON response: {e}")
        return 0
    except Exception as e:
        print(f"Error calling OpenAI API: {e}")
        return 0

# Register the UDF
sentiment_score_udf = udf(call_openai_sentiment_api, IntegerType())

# COMMAND ----------

def generate_sentiment_score(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "sentiment_score",
        F.when(((F.col("review_text").isNull()) | (F.col("review_text") == "")), 0).otherwise(
            sentiment_score_udf(F.col("review_text"))
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

# Since sentiment_score is already an integer from the UDF, we can directly use it
weighted_scores = with_scores.withColumn(
    "weighted_score",
    F.when(F.col("sponsored_review"), F.col("sentiment_score") * 0.5).otherwise(
        F.col("sentiment_score")
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
