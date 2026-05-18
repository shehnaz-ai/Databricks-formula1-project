// Databricks notebook source
// MAGIC %md
// MAGIC # Ingest results.json file
// MAGIC 1. Read the all the files from the results folder using spark dataframe reader API
// MAGIC 1. Define and enforce schema 
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

// Define source_file and table_name
val source_file = landing_folder_path+"/"+v_batch_id+"/results"
val table_name = catalog_name+"."+ bronze_schema+ ".results"

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 1 - Read the JSON file using the dataframe reader API

// COMMAND ----------

// Define the schema
import org.apache.spark.sql.types.{StructType, StructField, IntegerType, StringType, FloatType, DateType}

val results_schema = StructType(Seq(
    StructField("date", DateType),
    StructField("raceName", StringType),
    StructField("round", IntegerType),
    StructField("season", IntegerType),
    StructField("url", StringType),
    StructField("constructorId", StringType),
    StructField("driverId", StringType),
    StructField("grid", IntegerType),
    StructField("laps", IntegerType),
    StructField("number", IntegerType),
    StructField("points", FloatType),
    StructField("position", IntegerType),
    StructField("positionText", StringType),
    StructField("status", StringType)
))


val results_df=spark.read.format("json")
.option("mode", "FAILFAST")
.schema(results_schema)
.load(source_file)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 2 - Add Metadata Columns
// MAGIC - Source File
// MAGIC - Ingestion Timestamp

// COMMAND ----------

val results_final_df = add_ingestion_metadata(results_df)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 3 - Write to bronze delta table

// COMMAND ----------

write_to_bronze (results_final_df,table_name,v_batch_id
)

// COMMAND ----------

display(spark.table(table_name))
