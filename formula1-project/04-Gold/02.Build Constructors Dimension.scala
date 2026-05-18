// Databricks notebook source
// MAGIC %md
// MAGIC # Build Constructor Dimension
// MAGIC
// MAGIC 1.Read silver `constructors` table
// MAGIC 1. Read gold `ref_nationality_region` table
// MAGIC 1. Join the data from `constructors` with `ref_nationality_region` using `nationality`
// MAGIC 1. Select the required columns
// MAGIC      - constructors.constructor_id
// MAGIC      - constructors.constructor_name
// MAGIC      - constructors.nationality
// MAGIC      - ref_nationality_region.region
// MAGIC 1. Write the transformed data to gold `dim_constructors` table
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC #### Entity Relationship Diagram - Formula1 Silver Schema
// MAGIC
// MAGIC ![Formula1 Silver Data.png](../../z-course-images/formula1-silver-data-erd.png "Formula1 Silver Data.png")

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC #### Entity Relationship Diagram - Formula1 Gold Schema
// MAGIC
// MAGIC ![Formula1 Gold Data.png](../../z-course-images/formula1-gold-data-erd.png "Formula1 Gold Data.png")

// COMMAND ----------

// MAGIC %run "../00-common/01.environment-config"

// COMMAND ----------

import org.apache.spark.sql.functions.{col}

// COMMAND ----------

val target_table = catalog_name + "." + gold_schema + ".dim_constructors"

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 1 - Read source tables
// MAGIC - `silver.constructors`
// MAGIC - `gold.ref_nationality_region`

// COMMAND ----------

val constructors_df = spark.table( catalog_name +"."+ silver_schema+".constructors")
val ref_nationality_region_df = spark.table( catalog_name +"."+ gold_schema+ ".ref_nationality_region")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 2 - Join `constructors` with `nationality_region_df` using `nationality`
// MAGIC Select the following columns   
// MAGIC  1. constructors.constructor_id 
// MAGIC  1. constructors.constructor_name 
// MAGIC  1. constructors.nationality 
// MAGIC  1. ref_nationality_region.region
// MAGIC

// COMMAND ----------

val dim_constructors_df = constructors_df
                .join(ref_nationality_region_df, constructors_df("nationality") === ref_nationality_region_df("nationality"), "left")
                .select (constructors_df("constructor_id"),
                    constructors_df("constructor_name"),
                    constructors_df("nationality"),
                    ref_nationality_region_df("region")
                )
  

// COMMAND ----------


display(dim_constructors_df)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 3 - Write the transformed data to the `gold` `dim_races` table

// COMMAND ----------


    dim_constructors_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(target_table)


// COMMAND ----------


display(spark.table(target_table))
