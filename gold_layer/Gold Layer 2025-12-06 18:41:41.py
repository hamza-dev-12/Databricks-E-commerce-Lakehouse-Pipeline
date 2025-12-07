# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType, FloatType
from pyspark.sql import Row

# COMMAND ----------

catalog_name = "ecommerce"

# COMMAND ----------

df_products = spark.table(f"{catalog_name}.silver.slv_products")
df_brands = spark.read.table(f"{catalog_name}.silver.slv_brands")
df_category = spark.table(f"{catalog_name}.silver.slv_category")

# COMMAND ----------

df_products.createOrReplaceTempView("v_products")
df_brands.createOrReplaceTempView("v_brands")
df_category.createOrReplaceTempView("v_category")

# COMMAND ----------

display(spark.sql("select * from v_products limit 5"))

# COMMAND ----------

display(spark.sql("select * from v_brands limit 5"))

# COMMAND ----------

spark.sql("use catalog "+catalog_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace table gold.gold_dim_products as
# MAGIC
# MAGIC with brands_categories as (
# MAGIC   select
# MAGIC   b.brand_name,
# MAGIC   b.brand_code,
# MAGIC   c.category_name,
# MAGIC   c.category_code
# MAGIC
# MAGIC   from v_brands b
# MAGIC   inner join v_category c on b.category_code = c.category_code
# MAGIC )
# MAGIC
# MAGIC select
# MAGIC p.product_id,
# MAGIC p.sku,
# MAGIC p.category_code,
# MAGIC coalesce(bc.category_name, 'Not Available') as category_name,
# MAGIC p.brand_code,
# MAGIC coalesce(bc.brand_name, 'Not Available') as brand_name,
# MAGIC p.color,
# MAGIC p.size,
# MAGIC p.material,
# MAGIC p.weight_grams,
# MAGIC p.length_cm,
# MAGIC p.width_cm,
# MAGIC p.height_cm,
# MAGIC p.rating_count,
# MAGIC p.file_name,
# MAGIC p.ingest_timestamp
# MAGIC from v_products p
# MAGIC left join brands_categories bc on p.brand_code = bc.brand_code;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * 
# MAGIC from gold.gold_dim_products
# MAGIC limit 5;

# COMMAND ----------

# India states
india_region = {
    "MH": "West", "GJ": "West", "RJ": "West",
    "KA": "South", "TN": "South", "TS": "South", "AP": "South", "KL": "South",
    "UP": "North", "WB": "North", "DL": "North"
}
# Australia states
australia_region = {
    "VIC": "SouthEast", "WA": "West", "NSW": "East", "QLD": "NorthEast"
}

# United Kingdom states
uk_region = {
    "ENG": "England", "WLS": "Wales", "NIR": "Northern Ireland", "SCT": "Scotland"
}

# United States states
us_region = {
    "MA": "NorthEast", "FL": "South", "NJ": "NorthEast", "CA": "West", 
    "NY": "NorthEast", "TX": "South"
}

# UAE states
uae_region = {
    "AUH": "Abu Dhabi", "DU": "Dubai", "SHJ": "Sharjah"
}

# Singapore states
singapore_region = {
    "SG": "Singapore"
}

# Canada states
canada_region = {
    "BC": "West", "AB": "West", "ON": "East", "QC": "East", "NS": "East", "IL": "Other"
}

# Combine into a master dictionary
country_state_map = {
    "India": india_region,
    "Australia": australia_region,
    "United Kingdom": uk_region,
    "United States": us_region,
    "United Arab Emirates": uae_region,
    "Singapore": singapore_region,
    "Canada": canada_region
}  


# COMMAND ----------

rows = []
for country, states in country_state_map.items():
    for state_code, region in states.items():
        rows.append(Row(country=country, state=state_code, region=region))
rows[:10]   

# COMMAND ----------

df_region_mapping = spark.createDataFrame(rows)

# Optional: show mapping
df_region_mapping.show(truncate=False)

# COMMAND ----------

df_silver = spark.read.table(f"{catalog_name}.silver.slv_customers")
display(df_silver.limit(5))

# COMMAND ----------

df_gold = df_silver.join(
    df_region_mapping,
    ['country', 'state'],
    "left"
)

df_gold = df_gold.fillna("Other", subset=["region"])

display(df_gold.show(5))

# COMMAND ----------

df_gold.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.gold.gld_dim_customers")

# COMMAND ----------

df_silver = spark.read.table(f"{catalog_name}.silver.slv_calendar")
display(df_silver.show(5))

# COMMAND ----------

df_gold = df_silver.withColumn(
    "date_id",
    F.date_format(F.col("date"), "yyyyMMdd").cast(IntegerType())
).withColumn(
    "month_name",
    F.date_format(F.col("date"), "MMMM")
).withColumn(
    "is_weekend",
    F.when(F.col("day_name").isin(["Saturday", "Sunday"]), 1).otherwise(0)
) 

display(df_gold.show(5))

# COMMAND ----------

desired_columns_order = ["date_id", "date", "year", "month_name", "day_name", "is_weekend", "quarter", "week", "_ingested_at", "_source_file"]

df_gold = df_gold.select(desired_columns_order)

display(df_gold.limit(5))

# COMMAND ----------

# write table to gold layer
df_gold.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.gld_dim_date")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE EXTENDED ecommerce.gold.gld_dim_date;

# COMMAND ----------

