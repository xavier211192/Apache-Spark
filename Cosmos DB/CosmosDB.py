# Databricks notebook source
# Replace the URI and Primary key with the values you copied from the Keys section of your Cosmos DB account
URI = "your-URI"
PrimaryKey = "your-Key"

# COMMAND ----------

# DBTITLE 1,Write Config
#Create a write Config
CosmosDatabase = "AdventureWorks"
CosmosCollection = "ratings"

cosmosConfig = {
  "Endpoint": URI,
  "Masterkey": PrimaryKey,
  "Database": CosmosDatabase,
  "Collection": CosmosCollection,
  "Upsert": "false"
}

# COMMAND ----------

#Load data into a data frame
#link to sample data https://github.com/xavier211192/Apache-Spark/blob/main/Cosmos%20DB/ratings.csv
ratingsDF = spark.read.format("csv")\
            .option("header","true")\
            .load("/FileStore/tables/ratings.csv")

# COMMAND ----------

# DBTITLE 1,Write data
#Write data to Cosmos
(ratingsDF.write
  .mode("overwrite")
  .format("com.microsoft.azure.cosmosdb.spark")
  .options(**cosmosConfig)
  .save())

# COMMAND ----------

# DBTITLE 1,Read Config
CosmosDatabase = "AdventureWorks"
CosmosCollection = "ratings"
query = "Select top 10 * from ratings"
readConfig = {
  "Endpoint": URI,
  "Masterkey": PrimaryKey,
  "Database": CosmosDatabase,
  "Collection": CosmosCollection,
  "query_custom": query
}

# COMMAND ----------

# DBTITLE 1,Read data
readDF = (spark.read
  .format("com.microsoft.azure.cosmosdb.spark")
  .options(**readConfig)
  .load())

# COMMAND ----------

# DBTITLE 1,Bonus: Update Data
#Register the data you read as a sql table
readDF.createOrReplaceTempView("readDF")

# COMMAND ----------

readDF.columns

# COMMAND ----------

# DBTITLE 1,Update data into a SQL view
# MAGIC %sql
# MAGIC -- Updating all ratings to value 5
# MAGIC --It is important to have all the columns that we read from Cosmos DB since Cosmos DB creates additional metadata columns
# MAGIC Create or replace temp view updatedDF as
# MAGIC Select _attachments,
# MAGIC  _etag,
# MAGIC  _rid,
# MAGIC  _self,
# MAGIC  _ts,
# MAGIC  id,
# MAGIC  product_id,
# MAGIC  5 as rating,
# MAGIC  user_id
# MAGIC FROM readDF

# COMMAND ----------

# DBTITLE 1,Load the updated SQL view into a dataframe
updatedDF = sqlContext.table('updatedDF')
display(updatedDF)

# COMMAND ----------

# DBTITLE 1,Write updated data into Cosmos DB
#Write updated data to Cosmos
(updatedDF.write
  .mode("overwrite")
  .format("com.microsoft.azure.cosmosdb.spark")
  .options(**cosmosConfig)
  .save())
