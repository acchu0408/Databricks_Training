# Databricks notebook source
# Used to fetch the variables from other notebook
%run "/Workspace/Users/shreyas.achari@mmc.com/Day 1/includes"

# COMMAND ----------

df=spark.read.option("header",True).option("inferSchema",True).json("dbfs:/FileStore/tables/raw/iot1.json")

# COMMAND ----------

# raw_file_path is the variable present in some other notebook. We are fetching it using the "%run" command in the first cell
df=spark.read.json(f"{raw_files_path}iot1.json")

# COMMAND ----------

df.write.saveAsTable("shreyas.json_demo")

# COMMAND ----------

# %sql
# DROP TABLE shreyas.json_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM shreyas.json_demo

# COMMAND ----------


