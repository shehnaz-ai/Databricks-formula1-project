// Databricks notebook source
// MAGIC %md
// MAGIC # Complete Batch

// COMMAND ----------

// MAGIC %md
// MAGIC ![Incremental Data Processing](../../z-course-images/formula1-incremental-data-processing.png "Incremental Data Processing")

// COMMAND ----------

// MAGIC %run ../00-common/01.environment-config

// COMMAND ----------


val control_table = catalog_name+ "." + control_schema+ ".batch_control"

// COMMAND ----------


dbutils.widgets.text("p_batch_id", "")
val v_batch_id = dbutils.widgets.get("p_batch_id")

// COMMAND ----------

import io.delta.tables._
import org.apache.spark.sql.functions._

if (Option(v_batch_id).exists(_.nonEmpty))
{
  val delta_table = DeltaTable.forName(spark, control_table)

    val sourceDF = Seq((v_batch_id))
    .toDF("batch_id")
    .withColumn("status", lit("completed"))
    .withColumn("updated_timestamp", current_timestamp())

    // Merge into control table
  delta_table.as("t")
    .merge(
      sourceDF.as("s"),
      "t.batch_id = s.batch_id AND t.status = 'in_progress'"
    )
    .whenMatched()
    .update(Map(
      "status" -> col("s.status"),
      "updated_timestamp" -> col("s.updated_timestamp")
    ))
    .execute()

  println(s"Marked batch $v_batch_id as completed")
}
  else
  throw new Exception("batch_id is missing")

