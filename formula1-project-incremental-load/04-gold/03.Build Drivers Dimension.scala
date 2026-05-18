// Databricks notebook source
// MAGIC %md
// MAGIC # Build Drivers Dimension
// MAGIC
// MAGIC 1. Read silver `drivers` table
// MAGIC 1. Read gold `ref_nationality_region` table
// MAGIC 1. Join the data from `drivers` with `ref_nationality_region` using `nationality`
// MAGIC 1. Select the required columns
// MAGIC     - drivers.driver_id
// MAGIC     - drivers.driver_name
// MAGIC     - drivers.date_of_birth
// MAGIC     - drivers.nationality
// MAGIC     - ref_nationality_region.region
// MAGIC 1. Write the transformed data to gold `dim_drivers` table
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


dbutils.widgets.text("p_batch_id", "")
val v_batch_id = dbutils.widgets.get("p_batch_id")

// COMMAND ----------

// MAGIC %run ../00-common/01.environment-config

// COMMAND ----------

// MAGIC %run ../00-common/04.gold-helpers

// COMMAND ----------


val target_table =  catalog_name + "." + gold_schema + ".dim_drivers"

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 1 - Read source tables
// MAGIC - `silver.drivers`
// MAGIC - `gold.ref_nationality_region`

// COMMAND ----------


val drivers_df = 
    spark.table(catalog_name + "." + silver_schema + ".drivers")
         .filter(col("batch_id") === v_batch_id)


// COMMAND ----------


val ref_nationality_region_df = spark.table(catalog_name + "." + gold_schema + ".ref_nationality_region")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 2 - Join `drivers` with `nationality_region_df` using `nationality`
// MAGIC Select the following columns   
// MAGIC 1. drivers.driver_id
// MAGIC 1. drivers.driver_name
// MAGIC 1. drivers.date_of_birth
// MAGIC 1. drivers.nationality
// MAGIC 1. ref_nationality_region.region

// COMMAND ----------


val dim_drivers_df =
    drivers_df
         .join(ref_nationality_region_df, drivers_df("nationality") === ref_nationality_region_df("nationality"), "left")
                .select (drivers_df("driver_id"),
                    drivers_df("driver_name"),
                    drivers_df("date_of_birth"),
                    drivers_df("nationality"),
                    ref_nationality_region_df("region")
                )


// COMMAND ----------


display(dim_drivers_df)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 3 - Write the transformed data to the `gold` `dim_drivers` table

// COMMAND ----------


write_to_gold(
dim_drivers_df,
target_table,
"t.driver_id = s.driver_id",
Seq(
        "driver_name",
        "date_of_birth",
        "nationality",
        "nationality_region"
)
)

// COMMAND ----------


display(spark.table(target_table))
