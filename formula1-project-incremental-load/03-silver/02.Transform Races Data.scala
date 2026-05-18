// Databricks notebook source
// MAGIC %md
// MAGIC # Transform Races Data
// MAGIC
// MAGIC 1. Read bronze `races` table
// MAGIC 1. Keep only the columns required for analytics (Drop `url` column)
// MAGIC 1. Standardise column names using snake_case (`raceName` → `race_name`, `circuitId` → `circuit_id`)
// MAGIC 1. Rename columns to make them more meaningful (`date` → `race_date`)
// MAGIC 1. Remove duplicate records
// MAGIC 1. Transform values of column `race_name` to Title Case
// MAGIC 1. Write the transformed data to silver `races` table
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC #### Entity Relationship Diagram - Formula1 Schema
// MAGIC
// MAGIC ![Formula1 Raw Data.png](../../z-course-images/formula1-raw-data-erd.png "Formula1 Raw Data.png")

// COMMAND ----------


dbutils.widgets.text("p_batch_id", "")
val v_batch_id = dbutils.widgets.get("p_batch_id")

// COMMAND ----------

// MAGIC %run "../00-common/01.environment-config"

// COMMAND ----------

// MAGIC %run "../00-common/03.silver-helpers"

// COMMAND ----------

val bronze_table = catalog_name+"."+ bronze_schema + ".races"
val silver_table = catalog_name+"."+ silver_schema + ".races"

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 1 - Read bronze `races` table

// COMMAND ----------

//val races_df1= spark.table(bronze_table)
val races_df = spark.table(bronze_table).filter(col("batch_id") === v_batch_id)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 2 - Keep only the columns required for analytics (Drop url column)

// COMMAND ----------

val races_selected_df =races_df.select(
"season",
"round",
"raceName",
"date",
"circuitId",
"ingestion_timestamp",
"source_file",
"batch_id"
)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 3 & 4 - Standardise Column Names
// MAGIC - Standardise column names using snake_case (`circuitId` → `circuit_id`, `raceName` → `race_name`)
// MAGIC - Rename columns to make them more meaningful (`date` → `race_date`)

// COMMAND ----------

val races_renamed_df = races_selected_df
.withColumnRenamed("raceName","race_name")
.withColumnRenamed("circuitId", "circuit_id")
.withColumnRenamed("date", "race_date")

// COMMAND ----------


display(races_renamed_df)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 5 - Remove duplicate records

// COMMAND ----------


val races_distinct_df = races_renamed_df.dropDuplicates("season","round")

// COMMAND ----------


display(races_distinct_df)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 6 - Transform values of column `race_name` to Title Case

// COMMAND ----------

val races_final_df = 
    races_distinct_df
        .withColumn("race_name",initcap(col("race_name")))


// COMMAND ----------


display(races_final_df)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 7 - Write the transformed data to silver `races` table

// COMMAND ----------


write_to_silver(
    races_final_df,
    silver_table,
    "t.season = s.season AND t.round = s.round",
    Seq(
        "race_name",
        "race_date",
        "circuit_id",
        "ingestion_timestamp",
        "source_file",
        "batch_id"
    )
)

// COMMAND ----------

display(spark.table(silver_table))
