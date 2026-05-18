// Databricks notebook source
// MAGIC %md
// MAGIC # Create New Batch

// COMMAND ----------

// MAGIC %md
// MAGIC ![Incremental Data Processing](../../z-course-images/formula1-incremental-data-processing.png "Incremental Data Processing")

// COMMAND ----------

// MAGIC %run "../00-common/01.environment-config"

// COMMAND ----------


val control_table = catalog_name+"."+control_schema+".batch_control"


// COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
val v_batch_id = dbutils.widgets.get("p_batch_id")

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame,SparkSession,Row}
import org.apache.spark.sql.types.{StructType,StructField,StringType}

import spark.implicits._
// Define schema explicitly
val schema = StructType(Seq(
  StructField("batch_id", StringType, nullable = false),
  StructField("status", StringType, nullable = false)
))

if (Option(v_batch_id).exists(_.nonEmpty)) {
   val in_progress_df = 
        spark.createDataFrame(
  spark.sparkContext.parallelize(Seq(Row(v_batch_id, "in_progress"))),
  schema)
.withColumn("created_timestamp", current_timestamp())
.withColumn("updated_timestamp", current_timestamp())
    
  in_progress_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(control_table)

  println(s"Marked batch $v_batch_id as in_progress")
} else {
  throw new Exception("batch_id is missing")
}
