// Databricks notebook source
// MAGIC %md
// MAGIC # Build Nationality Region Reference
// MAGIC
// MAGIC 1. Create a dataframe with list of nationalities and corresponding geographic regions
// MAGIC 1. Write the dataframe to gold `ref_nationality_region` table
// MAGIC

// COMMAND ----------

// MAGIC %run "../00-common/01.environment-config"

// COMMAND ----------


val target_table = catalog_name + "."+ gold_schema+ ".ref_nationality_region"

// COMMAND ----------

import org.apache.spark.sql.functions.{col}

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 1 - Create a dataframe with list of nationalities and corresponding geographic regions

// COMMAND ----------

import org.apache.spark.sql.{Row}
import org.apache.spark.sql.types.{StructType,StructField,StringType,IntegerType,DateType}
//Manually curated nationality → region mapping
val nationality_region_map_rows =Seq(
Row("British", "Europe"),
    Row("Italian",           "Europe"),
    Row("French",            "Europe"),
    Row("German",            "Europe"),
    Row("Swiss",             "Europe"),
    Row("Dutch",             "Europe"),
    Row("Belgium",           "Europe"),
    Row("Belgian",           "Europe"),
    Row("Irish",             "Europe"),
    Row("Spanish",           "Europe"),
    Row("Austrian",          "Europe"),
    Row("East German",       "Europe"),
    Row("Russian",           "Europe"),
    Row("Finnish",           "Europe"),
    Row("Polish",            "Europe"),
    Row("Portuguese",        "Europe"),
    Row("Hungarian",         "Europe"),
    Row("Danish",            "Europe"),
    Row("Czech",             "Europe"),
    Row("Liechtensteiner",   "Europe"),
    Row("Monegasque",        "Europe"),
    Row("Swedish",           "Europe"),
    Row("Argentine-italian", "Europe"),
    Row("American-italian",  "Europe"),

    //North America
    Row("American",          "North America"),
    Row("Canadian",          "North America"),
    Row("Mexican",           "North America"),

    //South America
    Row("Brazilian",         "South America"),
    Row("Chilean",           "South America"),
    Row("Argentine",         "South America"),
    Row("Uruguayan",         "South America"),
    Row("Venezuelan",        "South America"),
    Row("Colombian",         "South America"),

    // Africa
    Row("South African",     "Africa"),
    Row("Rhodesian",         "Africa"),

    //Asia
    Row("Indian",            "Asia"),
    Row("Chinese",           "Asia"),
    Row("Japanese",          "Asia"),
    Row("Malaysian",         "Asia"),
    Row("Hong Kong",         "Asia"),
    Row("Indonesian",        "Asia"),
    Row("Thai",              "Asia"),

    //Oceania
    Row("Australian",        "Oceania"),
    Row("New Zealand",       "Oceania"),
    Row("New Zealander",     "Oceania"),
)
val schema = StructType(Seq(
  StructField("nationality", StringType, nullable = true),
  StructField("region", StringType, nullable = true)
))
val ref_nationality_region_df = spark.createDataFrame(spark.sparkContext.parallelize(nationality_region_map_rows),schema)



// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 2 - Write the dataframe to the `gold` `ref_nationality_region` table

// COMMAND ----------


    ref_nationality_region_df
        .write
        .format("delta")
        .mode("overwrite")             
        .saveAsTable(target_table)


// COMMAND ----------


display(spark.table(target_table))
