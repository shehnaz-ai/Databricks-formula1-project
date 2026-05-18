// Databricks notebook source
// MAGIC %md
// MAGIC # Ingest races.csv file
// MAGIC 1. Read the file using spark dataframe reader API
// MAGIC 1. Add Metadata Columns 
// MAGIC     - Source File
// MAGIC     - Ingestion Timestamp
// MAGIC 1. Write to bronze delta table    

// COMMAND ----------


dbutils.widgets.text("p_batch_id", "")
val v_batch_id = dbutils.widgets.get("p_batch_id")

// COMMAND ----------

// MAGIC %run "../00-common/01.environment-config"

// COMMAND ----------

// MAGIC %run "../00-common/02.bronze-helpers"

// COMMAND ----------


val source_file =landing_folder_path +"/"+ v_batch_id + "/races.csv"
val table_name =catalog_name + "." + bronze_schema + ".races"

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 1 - Read the CSV file using the dataframe reader API

// COMMAND ----------

import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DateType}

val races_schema=StructType(Seq(
  StructField("season", IntegerType),
  StructField("round", IntegerType),
  StructField("url", StringType),
  StructField("raceName", StringType),
  StructField("date", DateType),
  StructField("circuitId", StringType),
))

// COMMAND ----------

val races_df = 
    spark.read
         .format("csv")
         .option("header", "true")
         .option("mode", "FAILFAST")
         .schema(races_schema)
         .load(source_file)


// COMMAND ----------

display(races_df)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 2 - Add Metadata Columns
// MAGIC - Source File
// MAGIC - Ingestion Timestamp

// COMMAND ----------

val races_final_df = add_ingestion_metadata(races_df)

// COMMAND ----------

display(races_final_df)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 3 - Write to bronze delta table

// COMMAND ----------

write_to_bronze (races_final_df,table_name,v_batch_id
)

// COMMAND ----------


display(spark.table(table_name))
