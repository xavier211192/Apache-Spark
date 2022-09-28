# Databricks notebook source
# DBTITLE 1,Auto Loader Default Trigger
from pyspark.sql.functions import *

source_format = "csv"
source_dir = "dbfs:/FileStore/tables/flightdelaysraw/"
target_table = "DailyDelays"
checkpoint = "dbfs:/FileStore/tables/silver/checkpoints/"

#Read data from a source directory using Auto Loader syntax
source_df = (spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", source_format)
             .option("cloudFiles.inferColumnTypes", "True")
             .option("cloudFiles.schemaLocation", checkpoint)
             .load(source_dir))

#Apply a simple transformation
transform_df = (source_df.withColumn("d_date",to_date(col("d_date"),"yyyy-MM-dd")))

#Load data into a Delta Lake table using default trigger for streaming.
#In this setting, streaming query is always active looking for new data at the source
(transform_df.writeStream
     .option("checkpointLocation", checkpoint)
     .option("mergeSchema", "True")
     .table(target_table))

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from DailyDelays limit 10

# COMMAND ----------

# DBTITLE 1,Auto Loader Trigger Once
from pyspark.sql.functions import *

source_format = "csv"
source_dir = "dbfs:/FileStore/tables/flightdelaysraw/"
target_table = "DailyDelays"
checkpoint = "dbfs:/FileStore/tables/silver/checkpoints/"

#Read data from a source directory using Auto Loader syntax
source_df = (spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", source_format)
             .option("cloudFiles.inferColumnTypes", "True")
             .option("cloudFiles.schemaLocation", checkpoint)
             .load(source_dir))

#Apply a simple transformation
transform_df = (source_df.withColumn("d_date",to_date(col("d_date"),"yyyy-MM-dd")))

#Load data into a Delta Lake table using Trigger Once method.
#This will ensure that the streaming query executes only once and automatically stops.
(transform_df.writeStream
     .trigger(once=True)
     .option("checkpointLocation", checkpoint)
     .option("mergeSchema", "True")
     .table(target_table))

# COMMAND ----------


