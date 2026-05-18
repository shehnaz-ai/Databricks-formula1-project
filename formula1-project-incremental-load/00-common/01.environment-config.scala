// Databricks notebook source

// Unity Catalog Object Names
val catalog_name = "formula1_incr"
val bronze_schema = "bronze"
val silver_schema = "silver"
val gold_schema = "gold"
val control_schema = "control"

// COMMAND ----------

val landing_folder_path = "/Volumes/formula1_incr/landing/files"
