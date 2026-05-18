// Databricks notebook source
// MAGIC %md
// MAGIC # Transform Drivers Data
// MAGIC
// MAGIC 1. Read bronze `drivers` table
// MAGIC 1. Keep only the columns required for analytics (Drop `url` column)
// MAGIC 1. Standardise column names using snake_case (`driverId` → `driver_id`, `dateOfbirth` → `date_of_birth`)
// MAGIC 1. Concatenate `name.givenName` and `name.familyName` to create a new column called `driver_name` and transform the value to Title Case
// MAGIC 1. Remove duplicate records
// MAGIC 1. Transform values of column `nationality` to Title Case
// MAGIC 1. Write the transformed data to silver `drivers` table
// MAGIC

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

// MAGIC %run "../00-common/01.environment-config"

// COMMAND ----------

// MAGIC %run "../00-common/03.silver-helpers"

// COMMAND ----------

val bronze_table = catalog_name+"."+ bronze_schema + ".drivers"
val silver_table = catalog_name+"."+ silver_schema + ".drivers"

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 1 - Read bronze `drivers` table

// COMMAND ----------

val drivers_df = 
    spark.table(bronze_table)
         .filter(col("batch_id") === v_batch_id)


// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 2 - Keep only the columns required for analytics (Drop url column)

// COMMAND ----------

val drivers_dropped_df = drivers_df.drop(col("url"))

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 3 - Standardise Column Names
// MAGIC - Standardise column names using snake_case (`driverId` → `driver_id`, `dateOfBirth` → `date_of_birth`)

// COMMAND ----------

val drivers_renamed_df = 
    drivers_dropped_df
       .withColumnRenamed("driverId","driver_id")
        .withColumnRenamed("dateOfBirth", "date_of_birth")


// COMMAND ----------

display(drivers_renamed_df)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 4 - Concatenate name.givenName and name.familyName to create a new column called driver_name

// COMMAND ----------


val drivers_concatenated_df = 
  drivers_renamed_df
      .withColumn("driver_name",initcap(concat_ws(" ",col("name.givenName"),col("name.familyName")))).drop("name")


// COMMAND ----------

display(drivers_concatenated_df)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 5 - Remove duplicate records

// COMMAND ----------

val drivers_distinct_df = drivers_concatenated_df.dropDuplicates("driver_id")

// COMMAND ----------


display(drivers_distinct_df)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 6 - Transform values of column `nationality` to Title Case

// COMMAND ----------

val drivers_final_df = 
    drivers_distinct_df
        .withColumn("nationality", initcap(col("nationality")))


// COMMAND ----------

display(drivers_final_df)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 7 - Write the transformed data to silver `drivers` table

// COMMAND ----------

write_to_silver(
  drivers_final_df,
  silver_table,
  "t.driver_id = s.driver_id",
  Seq(
        "driver_name",
        "date_of_birth",
        "nationality",
        "ingestion_timestamp",
        "source_file",
        "batch_id"
  )
)

// COMMAND ----------

display(spark.table(silver_table))
