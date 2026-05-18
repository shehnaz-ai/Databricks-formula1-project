// Databricks notebook source
// MAGIC %md
// MAGIC # Ingest drivers.json file
// MAGIC 1. Read the file using spark dataframe reader API
// MAGIC 1. Define and enforce schema (preserve the nested structure)
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

//Define source_file and table_name
val source_file = landing_folder_path +"/"+ v_batch_id + "/drivers.json"
val table_name = catalog_name + "." + bronze_schema + ".drivers"

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 1 - Read the JSON file using the dataframe reader API

// COMMAND ----------

// Define the schema
import org.apache.spark.sql.types.{StructType, StructField, StringType, DateType}

val name_schema = StructType(Seq(
    StructField("givenName", StringType),
    StructField("familyName", StringType)
))

val drivers_schema=StructType(Seq(
  StructField("driverId", StringType),
  StructField("name", name_schema),
  StructField("dateOfBirth", DateType),
  StructField("nationality", StringType),
    StructField("url", StringType),
))

// COMMAND ----------

//Read data from the drivers file
val drivers_df=spark.read.format("json")
.option("mode", "FAILFAST")
.schema(drivers_schema)
.load(source_file)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 2 - Add Metadata Columns
// MAGIC - Source File
// MAGIC - Ingestion Timestamp

// COMMAND ----------

val drivers_final_df = add_ingestion_metadata(drivers_df)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 3 - Write to bronze delta table

// COMMAND ----------


write_to_bronze (
   drivers_final_df,
   table_name,
   v_batch_id
)

// COMMAND ----------


display(spark.table(table_name))
