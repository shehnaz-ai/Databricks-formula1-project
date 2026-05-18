// Databricks notebook source
// MAGIC %md
// MAGIC # Transform Circuits Data
// MAGIC
// MAGIC 1. Read bronze `circuits` table
// MAGIC 1. Keep only the columns required for analytics (Drop `url` column)
// MAGIC 1. Standardise column names using snake_case (`circuitId` → `circuit_id`, `circuitName` → `circuit_name`)
// MAGIC 1. Rename columns to make them more meaningful (`lat` → `latitude`, `long` → `longitude`)
// MAGIC 1. Filter out rows where `circuit_id` is null (business key validation)
// MAGIC 1. Remove duplicate records
// MAGIC 1. Transform values of columns `circuit_name` and `locality` to Title Case
// MAGIC 1. Write the transformed data to silver `circuits` table
// MAGIC
// MAGIC > Below changes are required to implement Incremental Load Processing
// MAGIC 1. Accept batch_id as a parameter to the notebook
// MAGIC 1. Process data for only the batch_id being passed in (i.e., filter reading from bronze using the batch_id)
// MAGIC 1. Add created_timestamp, updated_timestamp and batch_id to the silver table. 
// MAGIC 1. Merge the processed data to the silver table
// MAGIC     - created_timestamp should only be populated at the time of inserting/ creating the record. It should not be updated during the merge update.
// MAGIC     - Ensure that we are not overwriting the data in silver table by older bronze data (re-run scenario)

// COMMAND ----------

// MAGIC %md
// MAGIC ![incremental-data-processing-medallion.png](../../z-course-images/incremental-data-processing-medallion.png "Incremental Data Processing")

// COMMAND ----------

// MAGIC %md
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

import org.apache.spark.sql.functions._

// COMMAND ----------

val bronze_table = catalog_name +"."+ bronze_schema+".circuits"
val silver_table = catalog_name + "." + silver_schema+".circuits"

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 1 - Read bronze `circuits` table

// COMMAND ----------

val circuits_df1= spark.table(bronze_table)

// COMMAND ----------

val circuits_df = 
    circuits_df1.filter((col("batch_id") === v_batch_id ))

// COMMAND ----------

display(circuits_df)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 2 - Keep only the columns required for analytics (Drop url column)

// COMMAND ----------

// MAGIC %python
// MAGIC # circuits_selected_df = circuits_df.select(
// MAGIC #     "circuitId",
// MAGIC #     "circuitName",
// MAGIC #     "lat",
// MAGIC #     "long",
// MAGIC #     "locality",
// MAGIC #     "country",
// MAGIC #     "ingestion_timestamp",
// MAGIC #     "source_file"
// MAGIC # )

// COMMAND ----------

val circuits_selected_df =
circuits_df.select(
  "circuitId","circuitName","lat","long","locality","country","ingestion_timestamp","source_file","batch_id"
)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 3 & 4 - Standardise Column Names
// MAGIC - Standardise column names using snake_case (`circuitId` → `circuit_id`, `circuitName` → `circuit_name`)
// MAGIC - Rename columns to make them more meaningful (`lat` → `latitude`, `long` → `longitude`)

// COMMAND ----------

// MAGIC %python
// MAGIC # circuits_renamed_df = (
// MAGIC #     circuits_selected_df
// MAGIC #         .withColumnRenamed("circuitId", "circuit_id")
// MAGIC #         .withColumnRenamed("circuitName", "circuit_name")
// MAGIC #         .withColumnRenamed("lat", "latitude")
// MAGIC #         .withColumnRenamed("long", "longitude")
// MAGIC # )

// COMMAND ----------


val circuits_renamed_df = circuits_selected_df
.withColumnRenamed("circuitId","circuit_id")
.withColumnRenamed("circuitName", "circuit_name")
.withColumnRenamed("lat", "latitude")
.withColumnRenamed("long", "longitude")

// COMMAND ----------

display(circuits_renamed_df)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 5 - Filter out rows where circuit_id is null (business key validation)

// COMMAND ----------

// MAGIC %python
// MAGIC # circuits_valid_df = circuits_renamed_df.filter(
// MAGIC #     "circuit_id IS NOT NULL"
// MAGIC # )

// COMMAND ----------

val circuits_valid_df= circuits_renamed_df.filter(col("circuit_id").isNotNull)

// COMMAND ----------

display(circuits_valid_df)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 6 - Remove duplicate records

// COMMAND ----------

// MAGIC %python
// MAGIC # circuits_distinct_df = circuits_valid_df.distinct()

// COMMAND ----------

val circuits_distinct_df = circuits_valid_df.dropDuplicates("circuit_id")

// COMMAND ----------

display(circuits_distinct_df)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 7 - Transform values of columns `circuit_name` and `locality` to Title Case

// COMMAND ----------


val circuits_final_df = 
    circuits_distinct_df
        .withColumn("circuit_name", initcap(col("circuit_name")))
        .withColumn("locality", initcap(col("locality")))


// COMMAND ----------

display(circuits_final_df)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 8 - Write the transformed data to silver `circuits` table

// COMMAND ----------

// MAGIC %python
// MAGIC # circuits_final_df = (
// MAGIC #     circuits_final_df
// MAGIC #         .withColumn("created_timestamp", F.current_timestamp())
// MAGIC #         .withColumn("updated_timestamp", F.current_timestamp())
// MAGIC # )

// COMMAND ----------

// MAGIC %python
// MAGIC # if not spark.catalog.tableExists(silver_table):
// MAGIC #     (
// MAGIC #         circuits_final_df
// MAGIC #             .write
// MAGIC #             .format("delta")
// MAGIC #             .mode("overwrite")
// MAGIC #             .saveAsTable(silver_table)
// MAGIC #     )
// MAGIC
// MAGIC # else:
// MAGIC #     from delta.tables import DeltaTable
// MAGIC
// MAGIC #     delta_table = DeltaTable.forName(spark, silver_table)
// MAGIC #     (
// MAGIC #         delta_table.alias("t")
// MAGIC #         .merge (
// MAGIC #             circuits_final_df.alias("s"),
// MAGIC #             "t.circuit_id = s.circuit_id"
// MAGIC #         )
// MAGIC #         .whenMatchedUpdate(
// MAGIC #             condition="s.batch_id >= t.batch_id",
// MAGIC #             set = {
// MAGIC #                     "circuit_name": "s.circuit_name",
// MAGIC #                     "latitude": "s.latitude",
// MAGIC #                     "longitude": "s.longitude",
// MAGIC #                     "locality": "s.locality",
// MAGIC #                     "country": "s.country",
// MAGIC #                     "ingestion_timestamp": "s.ingestion_timestamp",
// MAGIC #                     "source_file": "s.source_file",
// MAGIC #                     "batch_id": "s.batch_id",
// MAGIC #                     "updated_timestamp": "s.updated_timestamp"              
// MAGIC #             }
// MAGIC #         )
// MAGIC #         .whenNotMatchedInsertAll()
// MAGIC #         .execute()
// MAGIC #     )

// COMMAND ----------

write_to_silver(circuits_final_df,silver_table,
    "t.circuit_id == s.circuit_id",Seq(
        "circuit_name",
        "latitude",
        "longitude",
        "locality",
        "country",
        "ingestion_timestamp",
        "source_file",
        "batch_id"        
    )
)

// COMMAND ----------

display(spark.table(silver_table))
