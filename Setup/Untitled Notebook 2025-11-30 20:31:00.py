# Databricks notebook source
# MAGIC %sql
# MAGIC create catalog if not exists ecommerce;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use catalog ecommerce;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create schema if not exists ecommerce.bronze;
# MAGIC create schema if not exists ecommerce.silver;
# MAGIC create schema if not exists ecommerce.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show databases from ecommerce;

# COMMAND ----------

