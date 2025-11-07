-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Data Exploration

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Dimensions Analysis
-- MAGIC
-- MAGIC **Fixed** would be those that are not changing once game is created
-- MAGIC - Genre - Fixed - Do require translation for readability -> Use AI
-- MAGIC - Publisher - Fixed
-- MAGIC - Platform - Maybe in future a new port of game is released so not fixed but once changed we only need to pick latest value
-- MAGIC - Developer - Fixed
-- MAGIC - Category - Fixed - Translate and attach as tags into the table with list agg
-- MAGIC

-- COMMAND ----------

SELECT * FROM steam.raw.inbound_application_categories mapp
JOIN steam.raw.inbound_categories cats ON mapp.category_id = cats.id ORDER BY appid

-- COMMAND ----------

SELECT DISTINCT name FROM steam.raw.inbound_genres

-- COMMAND ----------

SELECT
  id,
  name,
  ai_query(
    'databricks-llama-4-maverick',
    CONCAT('Translate the provided text English and ONLY output the translated output: ', name)
  ) AS readable_name
FROM steam.raw.inbound_genres
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Relationships
-- MAGIC - Application => Platforms will be one to many
-- MAGIC - Application => Developer will be one to one
-- MAGIC - Application => Publisher will be one to one
-- MAGIC - Application => Genre will be one to many but fixed
-- MAGIC - Application => Categories will be one to many but fixed

-- COMMAND ----------

SELECT
app.*,
dev.name as developer,
pub.name as publisher
FROM steam.raw.inbound_applications app
LEFT JOIN steam.raw.inbound_application_developers dev_map ON app.appid = dev_map.appid
LEFT JOIN steam.raw.inbound_application_publishers pub_map ON app.appid = pub_map.appid
LEFT JOIN steam.raw.inbound_developers dev ON dev.id = dev_map.developer_id
LEFT JOIN steam.raw.inbound_publishers pub ON pub.id = pub_map.publisher_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Enhance platform dimension with more metadata from internet

-- COMMAND ----------

SELECT * FROM steam.raw.inbound_application_platforms
