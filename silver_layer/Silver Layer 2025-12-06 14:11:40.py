# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, FloatType

catalog_name = 'ecommerce'

# COMMAND ----------

df_bronze = spark.table(f"{catalog_name}.bronze.brz_brands")
df_bronze.show(10)

# COMMAND ----------

df_silver = df_bronze.withColumn('brand_name',
                                 F.trim(F.col('brand_name')))

df_silver.show(10)

# COMMAND ----------



# COMMAND ----------

df_silver = df_silver.withColumn('brand_code', F.regexp_replace(
    F.col('brand_id'),
    r'[^a-zA-Z0-9\s]',
    ''
))

df_silver.show(10)


# COMMAND ----------

df_silver = df_silver.drop('brand_id')

df_silver.show()

# COMMAND ----------

anomolies = {
    "GROCERY": "GRCY",
    "BOOKS": "BKS",
    "TOYS": "TOY"
}


df_silver = df_silver.replace(anomolies, subset="category_code")

df_silver.select("category_code").distinct().show()

# COMMAND ----------

df_silver.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.silver.slv_brands")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Category

# COMMAND ----------

catalog_name = "ecommerce"
schema = "bronze"
table_name = "brz_category"

df_bronze = spark.table(f"{catalog_name}.{schema}.{table_name}")
df_bronze.show()

# COMMAND ----------

df_bronze.groupBy("category_code").count().filter(F.col("count") > 1).show()

# COMMAND ----------

df_silver = df_bronze.dropDuplicates(["category_code"])

df_silver.groupBy("category_code").count().show()

# COMMAND ----------

df_silver = df_silver.withColumn(
    "category_code",
    F.upper(F.col("category_code"))
)

df_silver.show()

# COMMAND ----------

df_silver.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.silver.slv_category")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Products

# COMMAND ----------

df_bronze = spark.table(f"{catalog_name}.bronze.brz_products")

row_count, column_count = df_bronze.count(), len(df_bronze.columns)

print(f"row_count: {row_count}, column_count: {column_count}")

# COMMAND ----------

df_bronze.select("weight_grams").show(5, truncate=False)

# COMMAND ----------

df_silver = df_bronze.withColumn(
    "weight_grams",
    F.regexp_replace(F.col("weight_grams"), "g", "").cast(IntegerType())
)

df_silver.select("weight_grams").show(5, truncate=False)

# COMMAND ----------

df_silver.select("length_cm").show(2, truncate=False)

# COMMAND ----------

df_silver = df_silver.withColumn(
    "length_cm",
    F.regexp_replace(F.col("length_cm"), ",", ".").cast(FloatType())
)

df_silver.select("length_cm").show(3)

# COMMAND ----------

df_silver.select("category_code", "brand_code").show(3)

# COMMAND ----------

df_silver = df_silver.withColumn(
    "category_code",
    F.upper(F.col("category_code"))
).withColumn(
    "brand_code",
    F.upper(F.col("brand_code"))
)

df_silver.select("category_code", "brand_code").show(3)

# COMMAND ----------

df_silver.select("material").show(12)

# COMMAND ----------

spelling = {
    "Coton": "Cotton",
    "Alumium": "Aluminum",
    "Ruber": "Rubber"
}

df_silver_checked = df_silver.replace(spelling, subset=["material"])

df_silver_checked.select("material").show(3)

# COMMAND ----------

df_silver_checked.select("rating_count").filter(F.col("rating_count") < 0).show(2)

# COMMAND ----------

df_silver = df_silver_checked.withColumn(
    "rating_count",
    F.when(F.col("rating_count").isNotNull(), F.abs("rating_count")).otherwise(F.lit(0))
)

df_silver.filter(F.col("rating_count") < 0).show()

# COMMAND ----------

#writing delta table

df_silver.write.\
    format("delta").\
        mode("overwrite").\
            option("mergeSchema", "true").\
                saveAsTable(f"{catalog_name}.silver.slv_products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customers

# COMMAND ----------

df_bronze = spark.table(f"{catalog_name}.bronze.brz_customers")

row_count, column_count = df_bronze.count(), len(df_bronze.columns)

print(f"row_count: {row_count}, column_count: {column_count}")

# COMMAND ----------

df_silver = df_bronze.dropna(subset=["customer_id"])

row_count = df_silver.count()
print(f"{row_count}: is the row count")

# COMMAND ----------

df_silver = df_silver.fillna("Not Available", subset=['phone'])

df_silver.filter(F.col("phone") == "Not Available").show(2)

# COMMAND ----------

df_silver.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.silver.slv_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calendar/Date

# COMMAND ----------

df_bronze = spark.read.table(f"{catalog_name}.bronze.brz_calendar")

row_count, column_count = df_bronze.count(), len(df_bronze.columns)

print(f"row_count: {row_count}, column_count: {column_count}")

# COMMAND ----------

df_bronze.printSchema()

# COMMAND ----------

df_silver = df_bronze.withColumn(
    "date",
    F.to_date(F.col("date"), "dd-MM-yyyy")
)

df_silver.printSchema()

# COMMAND ----------

df_silver = df_silver.dropDuplicates(["date"])

row_count = df_silver.count()

print('updated row_count: ', row_count)

# COMMAND ----------

df_silver = df_silver.withColumn("day_name", F.initcap(F.col("day_name")))

df_silver.show(5)

# COMMAND ----------

df_silver = df_silver.withColumn("week_of_year",
                                 F.abs(F.col("week_of_year")))

df_silver.show(4)

# COMMAND ----------

df_silver = df_silver.withColumn(
    "quarter",
    F.concat(F.lit("Q"), F.col("quarter"), F.lit("-"), F.col("year"))
).withColumn(
    "week_of_year",
    F.concat(F.lit("Week"), F.col("week_of_year"), F.lit("-"), F.col("year"))
)

# COMMAND ----------

df_silver = df_silver.withColumnRenamed("week_of_year", "week")

# COMMAND ----------

df_silver.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.silver.slv_calendar")

# COMMAND ----------

""