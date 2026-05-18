// Databricks notebook source
import org.apache.spark.sql.functions.{col,current_timestamp,lit}
import org.apache.spark.sql.DataFrame

// COMMAND ----------


//Helper function to add the file metadata for ingestion (source file and ingestion timestamp)
def add_ingestion_metadata(df:DataFrame):DataFrame=
        df.withColumn("ingestion_timestamp", current_timestamp())
          .withColumn("source_file",col("_metadata.file_path"))



// COMMAND ----------

def write_to_bronze (input_df:DataFrame,target_table:String,batch_id:String)={
val  final_df = input_df.withColumn("batch_id", lit(batch_id))
final_df.write
.format("delta")
.mode("overwrite")
.partitionBy("batch_id")
.option("replaceWhere",s"batch_id = '$batch_id'")
.saveAsTable(target_table)
}
   
