-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS ${catalog}.${environment};

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${environment}.dim_genre
SELECT
  id,
  ai_query(
    'databricks-llama-4-maverick',
    CONCAT('Translate the provided text English, 
            IF input is already in english then ONLY output it as it is,
            ONLY output the translated output if you cannot translate the input then just output NA : ', 
            name)
  ) AS genre_name
FROM ${catalog}.${source}.inbound_genres;

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${environment}.dim_category
SELECT
  id,
  ai_query(
    'databricks-llama-4-maverick',
    CONCAT('Translate the provided text English, 
            IF input is already in english then ONLY output it as it is,
            ONLY output the translated output if you cannot translate the input then just output NA : ', 
            name)
  ) AS category_name
FROM ${catalog}.${source}.inbound_categories;
