// Databricks notebook source
import org.apache.spark.sql.functions._
import io.delta.tables._
import org.apache.spark.sql.{DataFrame,Column}


def write_to_silver(
    input_df:DataFrame,
    target_table:String,
    merge_condition:String,
    columns_to_update:Seq[String]
)={ 
    val final_df = 
        input_df
        .withColumn("created_timestamp", current_timestamp())
        .withColumn("updated_timestamp", current_timestamp())

    if(!spark.catalog.tableExists(target_table))
        {
            final_df.write
                .format("delta")
                .mode("overwrite")
                .saveAsTable(target_table)
        }
    else{
       val  delta_table = DeltaTable.forName(spark, target_table)
       val updateMap: Map[String, Column] =
      columns_to_update.map(c => c -> col(s"s.$c")).toMap +
      ("updated_timestamp" -> col("s.updated_timestamp"))
    
     delta_table.as("t")
      .merge(final_df.as("s"), merge_condition)
      .whenMatched(expr("s.batch_id >= t.batch_id"))
      .update(updateMap)                // ✅ Scala API
      .whenNotMatched()
      .insertAll()                      // ✅ Scala API
      .execute()
    }
}       

