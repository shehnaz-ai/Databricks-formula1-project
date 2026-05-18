// Databricks notebook source
// MAGIC %md
// MAGIC # Ingest constructors.json file
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


val source_file = landing_folder_path +"/"+ v_batch_id + "/constructors.json"
val table_name =catalog_name + "." + bronze_schema + ".constructors"

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 1 - Read the JSON file using the dataframe reader API

// COMMAND ----------

val constructors_schema = """constructorId STRING, 
                         name STRING, 
                         nationality STRING, 
                         url STRING
                         """

// COMMAND ----------

val constructors_df = 
    spark.read
       .format("json")
       .schema(constructors_schema)
       .option("mode", "FAILFAST")
       .load(source_file)


// COMMAND ----------

display(constructors_df)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 2 - Add Metadata Columns
// MAGIC - Source File
// MAGIC - Ingestion Timestamp

// COMMAND ----------

val constructors_final_df = add_ingestion_metadata(constructors_df)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 3 - Write to bronze delta table

// COMMAND ----------


write_to_bronze (constructors_final_df,table_name,v_batch_id)

// COMMAND ----------

display(spark.table(table_name))
