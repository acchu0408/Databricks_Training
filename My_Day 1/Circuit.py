# Databricks notebook source
# Used to fetch the variables from other notebook
%run "/Workspace/Users/shreyas.achari@mmc.com/Day 1/includes"

# COMMAND ----------

df=spark.read.option("inferschema", True).option("header", True).csv(f"{input_formula1_path}circuits.csv")

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/formula1/circuits.csv",header=True,inferSchema=True)

# COMMAND ----------

display(df)

# COMMAND ----------

df.select("circuitId","name").display()

# COMMAND ----------

from pyspark.sql.functions import *
df.select(col("circuitId").alias("circuit_id"),"name").display()

# COMMAND ----------

df.select("circuitId",col("name"),df.location,df["country"]).display()

# COMMAND ----------

df.select(concat("location"," ","country")).display()

# COMMAND ----------

df.select(concat("location",lit(", "),"country").alias("location & county")).display()

# COMMAND ----------

df.withColumnRenamed("circuitId","circuit_id").display()

# COMMAND ----------

df.withColumnRenamed("circuitId","circuit_id").display()

# COMMAND ----------

df.withColumnRenamed("circuitId","circuit_id").withColumnRenamed("circuitRef","circuit_ref").display()

# COMMAND ----------

df.columns

# COMMAND ----------

new_column=['circuit_id',
 'circuit_ref',
 'name',
 'location',
 'country',
 'latitude',
 'longitute',
 'altitude',
 'url']

# COMMAND ----------

df1=df.toDF(*new_column)

# COMMAND ----------

df2=df1.drop("url")

# COMMAND ----------

df2.withColumn("ingestion_date",current_date()).display()

# COMMAND ----------

df2.withColumn("newcolumn",lit("formula1")).display()

# COMMAND ----------

df2\
.withColumn("location&country",concat("location", lit(" "),"country"))\
.drop("country","location")\
.display()

# COMMAND ----------

(df2
.withColumn("location&country",concat("location", lit(" "),"country"))
.drop("country","location")
.display())

# COMMAND ----------

df2.withColumn("country",upper("country")).display()

# COMMAND ----------

Task: 
    to get a new column with current timestamp: column name should be ingetion_date

# COMMAND ----------

df2.withColumn("ingestion_date",current_timestamp()).display()

# COMMAND ----------


