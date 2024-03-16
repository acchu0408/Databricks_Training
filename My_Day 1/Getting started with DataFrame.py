# Databricks notebook source
data = ([(1, 'a', 10), (2, 'b', 20)])
schema = "id int, name string, age string"
df = spark.createDataFrame(data, schema)

# COMMAND ----------

df.show()

# COMMAND ----------

DBFS:
    Databricks file system: abstraction of your cloud storage, linux
    https://docs.databricks.com/en/dbfs/index.html#what-is-the-databricks-file-system-dbfs

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

# MAGIC %fs

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/raw/

# COMMAND ----------

#creates a new folder processed
dbutils.fs.mkdirs("dbfs:/FileStore/processed")

# COMMAND ----------

#moves a file from one location to other
dbutils.fs.mv("dbfs:/FileStore/processed/emp.csv","dbfs:/FileStore/tables/raw/")

# COMMAND ----------

#deleted the folder
dbutils.fs.rm("dbfs:/FileStore/processed")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read/Write

# COMMAND ----------

Reference: https://spark.apache.org/docs/latest/api/python/reference/index.html

# COMMAND ----------

#reads a csv file from DBFS
df=spark.read.option("header",True).option("inferSchema",True).csv("dbfs:/FileStore/tables/raw/emp.csv")

# COMMAND ----------

#reads a csv file from DBFS
df=spark.read.csv("dbfs:/FileStore/tables/raw/emp.csv",header=True,inferSchema=True)

# COMMAND ----------

#display dataframe
df.display()
df.printSchema()

# COMMAND ----------

#saves the dataframe as a table in the mentioned schema
df.write.saveAsTable("shreyas.emp_demo")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM shreyas.emp_demo

# COMMAND ----------


