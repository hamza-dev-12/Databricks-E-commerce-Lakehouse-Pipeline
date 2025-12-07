# Databricks notebook source
import pyspark.sql.types as sparkTypes
import pyspark.sql.functions as sparkFun

# COMMAND ----------

catalog_name = 'ecommerce'  

brand_schema = sparkTypes.StructType([
  sparkTypes.StructField('brand_id', sparkTypes.StringType(), False),
  sparkTypes.StructField('brand_name', sparkTypes.StringType(), True),
  sparkTypes.StructField('category_code', sparkTypes.StringType(), True)
])

# COMMAND ----------

raw_data_path = '/Volumes/ecommerce/source_data/raw/brands/*.csv'

df = spark.read.option("header", "true").option("delimeter", ",").schema(brand_schema).csv(raw_data_path)

df = df.withColumn("_source_file", sparkFun.col("_metadata.file_path")).withColumn("ingested_at", sparkFun.current_timestamp())

display(df.show(5))

# COMMAND ----------

df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.bronze.brz_brands")

# COMMAND ----------

category_schema = sparkTypes.StructType([
    sparkTypes.StructField("category_code", sparkTypes.StringType(), False),
    sparkTypes.StructField("category_name", sparkTypes.StringType(), True)
])

raw_data_path = '/Volumes/ecommerce/source_data/raw/category/*.csv'

df_raw = spark.read.option("header", "true").option("delimeter", ",").schema(category_schema).csv(raw_data_path)

df_raw = df_raw.withColumn(
    "_ingested_at",
    sparkFun.current_timestamp()
).withColumn(
    "_source_file",
    sparkFun.col("_metadata.file_path")
)

df_raw.write.mode("overwrite").saveAsTable(f"{catalog_name}.bronze.brz_category")


# COMMAND ----------

products_schema = sparkTypes.StructType([
    sparkTypes.StructField("product_id", sparkTypes.StringType(), False),
    sparkTypes.StructField("sku", sparkTypes.StringType(), True),
    sparkTypes.StructField("category_code", sparkTypes.StringType(), True),
    sparkTypes.StructField("brand_code", sparkTypes.StringType(), True),
    sparkTypes.StructField("color", sparkTypes.StringType(), True),
    sparkTypes.StructField("size", sparkTypes.StringType(), True),
    sparkTypes.StructField("material", sparkTypes.StringType(), True),
    sparkTypes.StructField("weight_grams", sparkTypes.StringType(), True),
    sparkTypes.StructField("length_cm", sparkTypes.StringType(), True),
    sparkTypes.StructField("width_cm", sparkTypes.FloatType(), True),
    sparkTypes.StructField("height_cm", sparkTypes.FloatType(), True),
    sparkTypes.StructField("rating_count", sparkTypes.IntegerType(), True),
    sparkTypes.StructField("file_name", sparkTypes.StringType(), False),
    sparkTypes.StructField("ingest_timestamp", sparkTypes.TimestampType(), False),
    
])

raw_data_path = '/Volumes/ecommerce/source_data/raw/products/*.csv'

df_products = spark.read.option("header", "true").option("delimeter", ",").schema(products_schema).csv(raw_data_path)

df_products = df_products.withColumn("_source_file", sparkFun.col("_metadata.file_path")).withColumn("ingested_at", sparkFun.current_timestamp())

df_products.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.bronze.brz_products")

# COMMAND ----------

customer_schema = sparkTypes.StructType([
    sparkTypes.StructField("customer_id", sparkTypes.StringType(), False),
    sparkTypes.StructField("phone", sparkTypes.StringType(), True),
    sparkTypes.StructField("country_code", sparkTypes.StringType(), True),
    sparkTypes.StructField("country", sparkTypes.StringType(), True),
    sparkTypes.StructField("state", sparkTypes.StringType(), True)
])

raw_data_path = '/Volumes/ecommerce/source_data/raw/customers/*.csv'

df_customers = spark.read.option("header", "true").option("delimeter", ",").schema(customer_schema).csv(raw_data_path)
df_customers = df_customers.withColumn("_source_file", sparkFun.col("_metadata.file_path")).withColumn("ingested_at", sparkFun.current_timestamp())
df_customers.write.mode("overwrite").saveAsTable(f"{catalog_name}.bronze.brz_customers")

# COMMAND ----------

date_schema = sparkTypes.StructType([
    sparkTypes.StructField("date", sparkTypes.StringType(), True),
    sparkTypes.StructField("year", sparkTypes.IntegerType(), True),
    sparkTypes.StructField("day_name", sparkTypes.StringType(), True),
    sparkTypes.StructField("quarter", sparkTypes.IntegerType(), True),
    sparkTypes.StructField("week_of_year", sparkTypes.IntegerType(), True)
])

raw_data_path = '/Volumes/ecommerce/source_data/raw/date/*.csv'

df_date = spark.read.option("header", "true").option("delimeter", ",").schema(date_schema).csv(raw_data_path)
df_date = df_date.withColumn("_ingested_at", sparkFun.current_timestamp()).withColumn("_source_file", sparkFun.col("_metadata.file_path"))
df_date.write.mode("overwrite").saveAsTable(f"{catalog_name}.bronze.brz_calendar")

# COMMAND ----------

