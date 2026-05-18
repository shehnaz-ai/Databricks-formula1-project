// Databricks notebook source
// MAGIC %md
// MAGIC # Transform Results Data
// MAGIC 1. Read bronze `results` table
// MAGIC 1. Keep only the columns required for analytics (Drop `url` column)
// MAGIC 1. Standardise column names using snake_case (`constructorId` → `constructor_id`, `driverId` → `driver_id`, `raceName` → `race_name`, `positionText` → `finish_position_text`)
// MAGIC 1. Rename columns to make them more meaningful (`date` → `race_date`, `grid` → `grid_position`, `laps` → `completed_laps`, `number` → `car_number`, `position` → `finish_position`)
// MAGIC 1. Filter out rows where `season`, `round`, `custructor_id` or `driver_id` is null (business key validation)
// MAGIC 1. Remove duplicate records
// MAGIC 1. Transform values of column `race_name` to Title Case
// MAGIC 1. Write the transformed data to silver `results` table

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC #### Entity Relationship Diagram - Formula1 Bronze Schema
// MAGIC
// MAGIC ![Formula1 Raw Data.png](../../z-course-images/formula1-raw-data-erd.png "Formula1 Raw Data.png")

// COMMAND ----------


dbutils.widgets.text("p_batch_id", "")
val v_batch_id = dbutils.widgets.get("p_batch_id")

// COMMAND ----------

// MAGIC %run ../00-common/01.environment-config

// COMMAND ----------

// MAGIC %run ../00-common/03.silver-helpers

// COMMAND ----------

val bronze_table = catalog_name+"."+ bronze_schema + ".results"
val silver_table = catalog_name+"."+ silver_schema + ".results"

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 1 & 4 - Read bronze `results` table, select only the required columns and standardise column names

// COMMAND ----------


val results_df = 
  spark.table(bronze_table)
       .filter(col("batch_id") === v_batch_id)
       .select("season",
                "round",
                "constructorId",
                "driverId",
                "date",
                "raceName",
                "grid",
                "laps",
                "number",
                "points",
                "position",
                "positionText",
                "status",
                "ingestion_timestamp",
                "source_file",
                "batch_id")
.withColumnRenamed("constructorId", "constructor_id")
.withColumnRenamed("driverId", "driver_id")
.withColumnRenamed("raceName", "race_name")
.withColumnRenamed("date", "race_date")
.withColumnRenamed("grid", "grid_position")
.withColumnRenamed("laps", "completed_laps")
.withColumnRenamed("number", "car_number")
.withColumnRenamed("position", "final_position")
.withColumnRenamed("positionText", "final_position_text")



// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 5 & 6 Apply Data Quality Checks 
// MAGIC - Filter out rows where `season`, `round`, `custructor_id` or `driver_id` is null (business key validation)
// MAGIC - Remove duplicate records

// COMMAND ----------

val results_valid_df = 
    results_df
       .filter(
  col("season").isNotNull &&
  col("round").isNotNull &&
  col("constructor_id").isNotNull &&
  col("driver_id").isNotNull)
.dropDuplicates("season", "round", "constructor_id", "driver_id")
.withColumn("race_name", initcap(col("race_name")))

// COMMAND ----------


println(results_df.count() - results_valid_df.count())

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 7 - Transform values of column `race_name` to Title Case

// COMMAND ----------

val results_final_df = 
    results_valid_df
        .withColumn("race_name", initcap(col("race_name")))

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 8 - Write the transformed data to silver `results` table

// COMMAND ----------


write_to_silver(
results_final_df,silver_table,
"t.season = s.season AND t.round = s.round AND t.constructor_id = s.constructor_id AND t.driver_id = s.driver_id",
  Seq(
        "race_name",
        "race_date",
        "grid_position",
        "completed_laps",
        "car_number",
        "points",
        "final_position",
        "final_position_text",
        "status",
        "ingestion_timestamp",
        "source_file",
        "batch_id"
  )
)

// COMMAND ----------


display(spark.table(silver_table))
